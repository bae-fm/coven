//! Preparing cloud files to be made device-only on this device (§17.3):
//! download each to the path the app names under the folder the person
//! picked, reporting how far the downloads have come, for the write that
//! switches them all to this device. A prepare that fails leaves nothing
//! behind.

use crate::files::Files;
use crate::FileReadError;
use coven_database::{
    DbError, DeviceOnlyCopy, FileHasher, FilePlace, FileRef, FolderCopy, PreparedDeviceOnly,
};
use coven_foundation::{
    files::{CopyFolder, CopyPathError, FileError as DiskError, FlushBatch, Flushed, RelativePath},
    id_source::FileId,
    progress::{Progress, ProgressReport},
};
use std::path::{Path, PathBuf};
use tokio::sync::watch;

/// A step of preparing files to be made device-only (§17.3, E8), which a
/// host draws a bar for: one step for each file downloaded, in order. Done
/// and total count the file's bytes; done plus `before`, out of `all`, is how
/// far the whole call has come.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceOnlyStep {
    /// Downloading one file to its copy.
    Downloading {
        /// The file.
        file: FileId,
        /// The bytes of the files downloaded before it.
        before: u64,
        /// The bytes of every file the call downloads.
        all: u64,
    },
}

/// Why no file of a call was made device-only (E8). Whatever the call made
/// is removed before it returns, or, if removing it failed too, at the next
/// open.
#[derive(Debug, thiserror::Error)]
pub enum DeviceOnlyError {
    /// The call names one file twice.
    #[error("file {file} is named twice")]
    RepeatedFile {
        /// The file.
        file: FileId,
    },
    /// A copy can't go where the app named it.
    #[error(transparent)]
    Path(#[from] CopyPathError),
    /// This device may not make a file device-only, or the write switching
    /// them failed: no record of the file here (`FileAbsent`), as for a
    /// file of a circle this member was never in; one not in the cloud
    /// (`NotInCloud`); an app file (`NamespaceKind`); a file of a circle
    /// this member has left (`NotInCircle`); or another member's row of an
    /// `owner_only` table referencing it (`NotOwner`).
    #[error(transparent)]
    Database(#[from] DbError),
    /// Downloading a file failed.
    #[error("downloading file {file} failed: {error}")]
    Download {
        /// The file.
        file: FileId,
        /// The read's failure.
        #[source]
        error: FileReadError,
    },
    /// Writing a folder or a copy failed.
    #[error(transparent)]
    Disk(#[from] DiskError),
    /// The app cancelled the call before its files switched.
    #[error("cancelled")]
    Cancelled,
    /// The write switching the files failed other than in the database, as
    /// in a failed rollback, keeping its cause.
    #[error(transparent)]
    Write(Box<coven_database::CovenError>),
    /// Removing what the call made failed too. What stays is recorded, and
    /// the next open removes it.
    #[error("{error}; removing the copies failed: {cleanup}")]
    Cleanup {
        /// The call's failure.
        error: Box<DeviceOnlyError>,
        /// The removal's.
        #[source]
        cleanup: DbError,
    },
}

/// A failure of the write switching the files, as the call reports it.
impl From<coven_database::CovenError> for DeviceOnlyError {
    fn from(error: coven_database::CovenError) -> Self {
        match error {
            coven_database::CovenError::Database(error) => Self::Database(error),
            error => Self::Write(Box::new(error)),
        }
    }
}

impl Files {
    /// Download each cloud file to its path under `folder`, for one write
    /// to switch them all to this device only, each copy becoming the
    /// original this device records (§17.3). Each download checks the
    /// file's content hash, and each copy is written under a temporary name
    /// in its final folder and renamed once synced. Each name is written as
    /// the one rule for every platform makes it, which keeps it inside the
    /// folder. A path through a symbolic link below the folder, where
    /// something is already, or naming what another path names once
    /// written, case and Unicode form aside, is refused. `progress` hears
    /// each file's download and how many of its bytes, and of the call's,
    /// are down. Until it returns, `cancel` stops the call; a failure or a
    /// cancel removes every copy and folder the call made.
    pub async fn prepare_device_only(
        &self,
        folder: &Path,
        files: Vec<(FileId, RelativePath)>,
        cancel: &watch::Receiver<bool>,
        progress: watch::Sender<Option<Progress<DeviceOnlyStep>>>,
    ) -> Result<PreparedDeviceOnly, DeviceOnlyError> {
        self.inner.check_open()?;
        let mut ids = Vec::new();
        let mut paths = Vec::new();
        for (file, path) in files {
            if ids.contains(&file) {
                return Err(DeviceOnlyError::RepeatedFile { file });
            }
            ids.push(file);
            paths.push(path);
        }
        // Refused before any byte is downloaded; the write checks again.
        let refs = self.inner.database.device_only_files(ids).await?;
        let folder = CopyFolder::open(folder.to_owned()).await?;
        let plan = folder.plan(paths).await?;
        let copies: Vec<FolderCopy> = plan
            .files
            .iter()
            .map(|path| FolderCopy {
                path: path.clone(),
                temporary: path
                    .with_file_name(format!(".coven-{}.download", self.inner.ids.new_id())),
                folders: plan
                    .folders
                    .iter()
                    .filter(|folder| path.starts_with(folder))
                    .cloned()
                    .collect(),
            })
            .collect();
        // Held until the call ends however it ends: dropping the call's
        // future drops it too, and the next write removes what it made.
        let reservation = self.inner.database.record_copies(copies.clone()).await?;
        let mut cancel = cancel.clone();
        let mut report = ProgressReport::new(progress, self.inner.clock.clone());
        let error =
            match download(self, &folder, &plan.folders, &refs, &copies, &mut cancel, &mut report)
                .await
            {
                Ok((switches, flushed)) => {
                    return Ok(self.inner.database.prepared_device_only(
                        reservation,
                        switches,
                        flushed,
                    )?)
                }
                Err(error) => error,
            };
        let made = copies.into_iter().map(|copy| copy.path).collect();
        let abandoned = self.inner.database.abandon_copies(made).await;
        drop(reservation);
        match abandoned {
            Ok(()) => Err(error),
            Err(cleanup) => Err(DeviceOnlyError::Cleanup { error: Box::new(error), cleanup }),
        }
    }
}

/// Make the plan's folders, then download each file to its copy and
/// place it, returning the switches the write makes and the flush of
/// the copies' batch: their folders and the drive's cache, once for the
/// call, so every copy is on the drive before the write records it
/// (Appendix C.3). A cancel is heard while waiting for bytes and between
/// steps, never inside one, so every name a step made is in its record
/// when the call removes them.
async fn download(
    handle: &Files,
    folder: &CopyFolder,
    folders: &[PathBuf],
    files: &[FileRef],
    copies: &[FolderCopy],
    cancel: &mut watch::Receiver<bool>,
    report: &mut ProgressReport<DeviceOnlyStep>,
) -> Result<(Vec<DeviceOnlyCopy>, Flushed), DeviceOnlyError> {
    for made in folders {
        check(cancel)?;
        // A folder someone made since planning is theirs, and stays.
        if folder.create_folder(made.clone()).await? {
            handle.inner.database.made_folder(made).await?;
        }
    }
    let mut switches = Vec::new();
    let mut batch = FlushBatch::new();
    let all = files.iter().map(FileRef::size).sum();
    let mut before = 0;
    for (file, copy) in files.iter().zip(copies) {
        check(cancel)?;
        report.begin(DeviceOnlyStep::Downloading { file: file.id(), before, all }, file.size());
        let mut done = 0;
        let download = |error| DeviceOnlyError::Download { file: file.id(), error };
        let FilePlace::Cloud { location, .. } = file.place() else {
            return Err(DbError::NotInCloud { file: file.id() }.into());
        };
        let location = location.clone();
        let stream = handle.open_file_stream(file).await.map_err(download)?;
        let mut writer = folder.create_copy(copy.temporary.clone()).await?;
        // A whole read in order checks the content hash before its last
        // bytes are handed over (§17.7). The bytes written are hashed as
        // they go, for a later move of the copy to check against (§17.3).
        let mut range = stream.read_range(0, file.size()).map_err(download)?;
        let mut hashes = FileHasher::new();
        loop {
            let bytes = tokio::select! {
                biased;
                () = cancelled(cancel) => return Err(DeviceOnlyError::Cancelled),
                bytes = range.next() => bytes.map_err(download)?,
            };
            let Some(bytes) = bytes else { break };
            hashes.update(&bytes);
            writer.append(&bytes).await?;
            done += bytes.len() as u64;
            report.done(done);
        }
        before += file.size();
        writer.publish(&copy.path, &mut batch).await?;
        let observed = folder.observe(&copy.path).await.map_err(DbError::from)?;
        handle.inner.database.place_copy(&observed).await?;
        switches.push(DeviceOnlyCopy {
            file: file.id(),
            location,
            path: copy.path.clone(),
            hashes: hashes.finish(),
        });
    }
    check(cancel)?;
    Ok((switches, batch.flush().await?))
}

fn check(cancel: &watch::Receiver<bool>) -> Result<(), DeviceOnlyError> {
    if *cancel.borrow() {
        Err(DeviceOnlyError::Cancelled)
    } else {
        Ok(())
    }
}

/// Resolves once `cancel` says to stop; never, once its sender is gone.
async fn cancelled(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow_and_update() {
            return;
        }
        if cancel.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}
