//! Durable keep-file intent, checked downloads and conditional publication.

use super::{join, Files};
use crate::{
    operation_data::{Data, KeepFileWork},
    operations::{Output, Progress},
    FileReadError, SyncError,
};
use coven_database::{DbError, FileLocation, FileRef, OperationRecord, Provenance};
use coven_foundation::files::{DownloadFile, DownloadLocation, FileError, ObservationError};
use std::{
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    sync::Arc,
};

impl Files {
    pub(crate) async fn record_keeps(
        &self,
        files: &[FileRef],
        destinations: &HashMap<String, PathBuf>,
    ) -> Result<(), SyncError> {
        self.inner.check_open()?;
        let mut requests = Vec::new();
        let mut paths = BTreeSet::new();
        let mut rows = BTreeSet::new();
        for file in files {
            if !rows.insert(file.encode()?) {
                return Err(DbError::FileBatchDuplicate {
                    namespace: file.namespace().into(),
                    id: file.id(),
                }
                .into());
            }
            let provenance = self.inner.database.provenance(file).await?;
            if matches!(file.location(), FileLocation::OnDevice(_)) {
                self.inner
                    .database
                    .open_local(file)
                    .await
                    .map_err(FileReadError::from)?;
                continue;
            }
            let destination = match provenance {
                Provenance::AppProvided => None,
                Provenance::UserProvided => {
                    let path = destinations
                        .get(&file.id())
                        .ok_or_else(|| SyncError::DestinationRequired { id: file.id() })?
                        .clone();
                    let path = join(tokio::task::spawn_blocking(move || {
                        DownloadFile::check_destination(&path)
                    }))
                    .await
                    .map_err(disk_error)?;
                    if !paths.insert(path.clone()) {
                        return Err(SyncError::DestinationRepeated { path });
                    }
                    Some(path)
                }
            };
            let data = Data::KeepFile(KeepFileWork {
                reference: file.encode()?,
                obsolete: false,
            });
            requests.push((
                file.clone(),
                destination,
                data.new_operation("keep_files_on_this_device")?,
            ));
        }
        self.inner.database.start_keeps(requests).await?;
        Ok(())
    }

    pub(crate) async fn keep_step(
        &self,
        record: &OperationRecord,
        mut data: Data,
    ) -> Result<Progress, SyncError> {
        let Data::KeepFile(work) = &data else {
            return Err(DbError::DamagedDatabase.into());
        };
        let file = FileRef::decode(&work.reference)?;
        if work.obsolete {
            self.inner.database.clean_files().await?;
            return Err(changed(&file));
        }
        if record.last_step == 2 {
            self.inner.database.complete_keep(record.id).await?;
            return Ok(Progress::Finished(Output::Unit));
        }
        let result = self.keep_current(record, &data, &file).await;
        if matches!(
            &result,
            Err(SyncError::Database(DbError::FileRefChanged { .. })
                | SyncError::File(FileReadError::Database(DbError::FileRefChanged { .. })))
        ) {
            let Data::KeepFile(work) = &mut data else {
                unreachable!()
            };
            work.obsolete = true;
            self.inner
                .database
                .abandon_keep(data.update(record, record.last_step)?)
                .await?;
        }
        result
    }

    async fn keep_current(
        &self,
        record: &OperationRecord,
        data: &Data,
        file: &FileRef,
    ) -> Result<Progress, SyncError> {
        self.inner.check_open()?;
        self.inner.database.validate(file).await?;
        let location = self.inner.database.keep_location(record.id).await?;
        let lease = Arc::new(self.inner.database.keep_lease().await?);
        match record.last_step {
            0 => {
                let directory = self.inner.directory.clone();
                let target = location.clone();
                let captured = file.clone();
                let retained = lease.clone();
                let published = join(tokio::task::spawn_blocking(move || {
                    let _lease = retained;
                    let download = directory.download(&target).map_err(FileReadError::from)?;
                    download
                        .recover_publication(|reader| captured.matches_content(reader))
                        .map_err(observation_error)
                }))
                .await?;
                if published {
                    self.inner
                        .database
                        .advance_keep(data.update(record, 1)?)
                        .await?;
                    return Ok(Progress::Advanced);
                }
                let stream = self.open_file_stream(file).await?;
                let directory = self.inner.directory.clone();
                let target = location.clone();
                let retained = lease.clone();
                let mut writer = join(tokio::task::spawn_blocking(move || {
                    let _lease = retained;
                    let download = directory.download(&target).map_err(FileReadError::from)?;
                    download.remove_staging()?;
                    Ok::<_, FileReadError>(download.create_writer()?)
                }))
                .await?;
                let mut range = stream.read_range(0, file.plaintext_size())?;
                while let Some(bytes) = range.next().await? {
                    writer.append(&bytes).await.map_err(disk_error)?;
                }
                // The range reader checks the row's hash before yielding its
                // last buffer, including for an empty file. Only then publish.
                writer.finish().await.map_err(disk_error)?;
                let directory = self.inner.directory.clone();
                join(tokio::task::spawn_blocking(move || {
                    let _lease = lease;
                    let download = directory.download(&location).map_err(FileReadError::from)?;
                    download.publish().map_err(disk_error)
                }))
                .await?;
                self.inner
                    .database
                    .advance_keep(data.update(record, 1)?)
                    .await?;
                Ok(Progress::Advanced)
            }
            1 => {
                let prepared = match &location {
                    DownloadLocation::AppProvided(_) => {
                        let directory = self.inner.directory.clone();
                        let location = location.clone();
                        let file = file.clone();
                        join(tokio::task::spawn_blocking(move || {
                            let _lease = lease;
                            let download =
                                directory.download(&location).map_err(FileReadError::from)?;
                            let reader = download.open_reader().map_err(DbError::from)?;
                            if !file.matches_content(&reader).map_err(DbError::from)? {
                                return Err(FileReadError::Integrity { id: file.id() }.into());
                            }
                            Ok::<_, SyncError>(())
                        }))
                        .await?;
                        None
                    }
                    DownloadLocation::UserProvided { path, .. } => {
                        let prepared = coven_database::prepare_user_file(path, |_| {}).await?;
                        Some(prepared)
                    }
                };
                // The ordinary database write checks the original reference
                // again inside its transaction, then records step 2 atomically.
                self.inner
                    .database
                    .finish_keep(file, prepared, data.update(record, 2)?)
                    .await?;
                Ok(Progress::Advanced)
            }
            _ => Err(DbError::DamagedDatabase.into()),
        }
    }

    pub(crate) async fn discard_keep(
        &self,
        record: &OperationRecord,
        mut data: Data,
    ) -> Result<(), SyncError> {
        let Data::KeepFile(work) = &mut data else {
            return Err(DbError::DamagedDatabase.into());
        };
        work.obsolete = true;
        self.inner
            .database
            .abandon_keep(data.update(record, record.last_step)?)
            .await?;
        self.inner.database.complete_keep(record.id).await?;
        Ok(())
    }
}

fn changed(file: &FileRef) -> SyncError {
    DbError::FileRefChanged {
        table: file.table().into(),
        key: file.key().clone(),
    }
    .into()
}
fn disk_error(error: FileError) -> SyncError {
    match error {
        FileError::Io { path, source, .. }
            if source.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            SyncError::DestinationExists { path }
        }
        error => FileReadError::Disk(error).into(),
    }
}

fn observation_error(error: ObservationError) -> SyncError {
    match error {
        ObservationError::File(error) => disk_error(error),
        error => DbError::from(error).into(),
    }
}
