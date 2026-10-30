//! Making cloud files device-only on this device (§17.3): download each to
//! the path the app names under the folder the person picked, then switch
//! them all to this device in one write. A call that doesn't reach that
//! write leaves nothing behind.

use crate::files::Files;
use crate::FileReadError;
use coven_database::{DbError, DeviceOnlyCopy, FilePlace, FileRef, FolderCopy};
use coven_foundation::{
    files::{observe_metadata, CopyFolder, CopyPathError, FileError as DiskError, RelativePath},
    id_source::FileId,
};
use std::path::{Path, PathBuf};
use tokio::sync::watch;

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
    /// them failed: no record of the file here (`FileAbsent`), one not in
    /// the cloud (`NotInCloud`), an app file (`NamespaceKind`), a circle
    /// this member isn't in (`NotInCircle`), or another member's row of an
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

impl Files {
    /// Download each cloud file to its path under `folder`, then switch them
    /// all to this device only in one write, each copy becoming the original
    /// this device records (§17.3). Each download checks the file's content
    /// hash, and each copy is written under a temporary name in its final
    /// folder and renamed once synced. A path that leaves the folder can't be
    /// named; one through a symbolic link below it, or where something is
    /// already, is refused. Until the write commits, `cancel` stops the call;
    /// a failure or a cancel removes every copy and folder the call made.
    pub async fn make_device_only(
        &self,
        folder: &Path,
        files: Vec<(FileId, RelativePath)>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<(), DeviceOnlyError> {
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
        let result = match self
            .download(&folder, &plan.folders, &refs, &copies, &mut cancel)
            .await
        {
            Ok(switches) => self
                .inner
                .database
                .make_device_only(switches)
                .await
                .map_err(DeviceOnlyError::from),
            Err(error) => Err(error),
        };
        let Err(error) = result else {
            return Ok(());
        };
        let made = copies.into_iter().map(|copy| copy.path).collect();
        let abandoned = self.inner.database.abandon_copies(made).await;
        drop(reservation);
        match abandoned {
            Ok(()) => Err(error),
            Err(cleanup) => Err(DeviceOnlyError::Cleanup {
                error: Box::new(error),
                cleanup,
            }),
        }
    }

    /// Make the plan's folders, then download each file to its copy and
    /// place it, returning the switches the write makes. A cancel is heard
    /// while waiting for bytes and between steps, never inside one, so
    /// every name a step made is in its record when the call removes them.
    async fn download(
        &self,
        folder: &CopyFolder,
        folders: &[PathBuf],
        files: &[FileRef],
        copies: &[FolderCopy],
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<Vec<DeviceOnlyCopy>, DeviceOnlyError> {
        for made in folders {
            check(cancel)?;
            // A folder someone made since planning is theirs, and stays.
            if folder.create_folder(made.clone()).await? {
                self.inner.database.made_folder(made).await?;
            }
        }
        let mut switches = Vec::new();
        for (file, copy) in files.iter().zip(copies) {
            check(cancel)?;
            let download = |error| DeviceOnlyError::Download {
                file: file.id(),
                error,
            };
            let FilePlace::Cloud { location, .. } = file.place() else {
                return Err(DbError::NotInCloud { file: file.id() }.into());
            };
            let location = location.clone();
            let stream = self.open_file_stream(file).await.map_err(download)?;
            let mut writer = folder.create_copy(copy.temporary.clone()).await?;
            // A whole read in order checks the content hash before its last
            // bytes are handed over (§17.6).
            let mut range = stream.read_range(0, file.size()).map_err(download)?;
            loop {
                let bytes = tokio::select! {
                    biased;
                    () = cancelled(cancel) => return Err(DeviceOnlyError::Cancelled),
                    bytes = range.next() => bytes.map_err(download)?,
                };
                let Some(bytes) = bytes else { break };
                writer.append(&bytes).await?;
            }
            writer.publish(&copy.path).await?;
            let observed = observe_metadata(&copy.path).await.map_err(DbError::from)?;
            self.inner.database.place_copy(&observed).await?;
            switches.push(DeviceOnlyCopy {
                file: file.id(),
                location,
                path: copy.path.clone(),
            });
        }
        check(cancel)?;
        Ok(switches)
    }
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
