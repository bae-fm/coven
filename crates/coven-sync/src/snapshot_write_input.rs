//! Keep shared device-log downloads on disk until an atomic snapshot reload.

use super::{io, StoreLogSync};
use crate::{replay_cache::ReplayCache, snapshot_data::SavedWrite, SyncError};
use coven_database::StoreLog;
use coven_format::{
    write::WriteHeader,
    write_stream::{PartDecoder, WriteHeaderFrame},
};
use coven_foundation::files::FileWriter;
use coven_merge::Audience;
use coven_storage::StoredObject;
use std::collections::BTreeSet;

impl StoreLogSync {
    pub(super) async fn open_write_header(
        &self,
        object: &StoredObject,
    ) -> Result<WriteHeaderFrame, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let ring = self.store_keys.read()?;
        let result = crate::write_object::open(storage, object, ring.as_ref(), &self.reads).await;
        if let Err(SyncError::Damaged(damage)) = &result {
            crate::write_object::record_damage(&self.database, storage, object, &damage.failure)
                .await?;
        }
        Ok(result?.header)
    }

    pub(super) async fn open_snapshot_write(
        &self,
        object: &StoredObject,
        log: &StoreLog,
        replays: &mut ReplayCache<'_>,
        readable: &BTreeSet<Audience>,
        names: &mut impl Iterator<Item = String>,
    ) -> Result<SavedWrite, SyncError> {
        let result = self
            .download_snapshot_write(object, log, replays, readable, names)
            .await;
        let result = match result {
            Err(SyncError::Format(error)) => crate::write_object::checked(&object.path, Err(error)),
            Err(SyncError::Database(error)) => {
                Err(crate::write_object::database_failure(&object.path, error))
            }
            result => result,
        };
        if let Err(SyncError::Damaged(damage)) = &result {
            crate::write_object::record_damage(
                &self.database,
                self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                object,
                &damage.failure,
            )
            .await?;
        }
        result
    }

    async fn download_snapshot_write(
        &self,
        object: &StoredObject,
        log: &StoreLog,
        replays: &mut ReplayCache<'_>,
        readable: &BTreeSet<Audience>,
        names: &mut impl Iterator<Item = String>,
    ) -> Result<SavedWrite, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let ring = self.store_keys.read()?;
        let crate::write_object::ReadyWrite {
            opened,
            author,
            parts: eligible,
        } = crate::write_object::download(
            storage,
            object,
            ring.as_ref(),
            &self.reads,
            log,
            replays,
            || Ok(self.operation_member()?.member_id()),
        )
        .await?
        .map_err(coven_database::SnapshotError::WriteWaiting)
        .map_err(coven_database::DbError::Snapshot)?;
        let ring = ring.as_ref().expect("opened header has its store key");
        let mut files = Vec::new();
        let mut targets = Vec::new();
        for (part, eligible) in opened.header.parts.iter().zip(eligible) {
            if eligible && readable.contains(&part.audience) {
                let name = names.next().expect("reserved log part files");
                let writer = self
                    .directory
                    .file(
                        coven_foundation::files::FileArea::AppProvided,
                        &io::name(&name)?,
                    )
                    .create_writer(self.directory.lock_read_only()?)?;
                files.push(Some(name));
                targets.push(Some((writer, PartDecoder::new(part.clone())?)));
            } else {
                files.push(None);
                targets.push(None);
            }
        }
        let header = opened.header.encode()?;
        let primary = SnapshotWriteParts {
            header: opened.header.header.clone(),
            targets,
        };
        let opens = files.iter().map(Option::is_some).collect::<Vec<_>>();
        let (references, input) =
            crate::stream_input::ChannelParts::new(opened.header.clone(), &opens);
        let transfer = async {
            let mut sink = crate::stream_input::WithReferences {
                primary,
                references,
            };
            crate::write_object::finish(storage, object, ring, &author, opened, &mut sink).await
        };
        let (transferred, checked) =
            tokio::join!(transfer, self.database.write_file_references(input));
        transferred?;
        self.reads.keep_files(object, checked?);
        Ok(SavedWrite {
            header,
            parts: files,
        })
    }
}

struct SnapshotWriteParts {
    header: WriteHeader,
    targets: Vec<Option<(FileWriter, PartDecoder)>>,
}
impl crate::write_object::PartSink for SnapshotWriteParts {
    fn opens(&self, part: usize) -> bool {
        self.targets[part].is_some()
    }
    async fn chunk(&mut self, part: usize, bytes: Vec<u8>) -> Result<(), SyncError> {
        let (writer, decoder) = self.targets[part].as_mut().expect("opened part");
        for frame in decoder.chunk(&bytes)? {
            if let coven_format::dismissal::WriteFrame::Dismissal(dismissal) = frame {
                dismissal.validate_past(&self.header)?;
            }
        }
        writer.append(&bytes).await?;
        Ok(())
    }
    async fn end(&mut self, part: usize) -> Result<(), SyncError> {
        if let Some((writer, decoder)) = self.targets[part].take() {
            decoder.finish()?;
            writer.finish().await?;
        }
        Ok(())
    }
}
