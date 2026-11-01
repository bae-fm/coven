//! Pins, budgets and downloads of stored files.

use crate::{
    files::file_read::UploadedFile,
    files::{Files, FilesInner},
    FileReadError,
};
use coven_database::{CacheReservation, DbError, FilePlace, FileRef, PreparedPin};
use coven_foundation::files::{FileArea, FileName, FileWriter, StoreDirAccess};
use futures_util::{StreamExt, TryStreamExt};

/// What preparing one file's pin found: a whole copy written for it, or one
/// already kept.
enum PreparedFile {
    Copy(CacheReservation),
    Kept(FileRef),
}

impl Files {
    /// Prepare a pin of stored files (§17.8): download what each lacks, up
    /// to the download limit at once, into a whole cache copy written,
    /// synced and checked against its content hash, or note a checked whole
    /// copy already kept. Nothing is pinned until a write commits the
    /// result with its `pin` step. Each file's download failure is its
    /// stored bytes' pending record, as for any pin; a download that
    /// succeeds clears it. A file only on one device, this one included
    /// while it uploads, is refused: only stored bytes are kept. So is one
    /// whose record goes, or leaves its stored bytes, while they download
    /// (`NoLongerStored`).
    pub async fn prepare_pin(&self, files: &[FileRef]) -> Result<PreparedPin, FileReadError> {
        self.inner.check_open()?;
        let _pins = self.inner.pins.lock().await;
        let mut current = Vec::new();
        for file in files {
            let file = self.inner.current(file).await?;
            if let FilePlace::DeviceOnly { device } = file.place() {
                return Err(FileReadError::DeviceOnly { file: file.id(), device });
            }
            current.push(file);
        }
        let limit = self.transfer_limits().downloads.get();
        let prepared: Vec<PreparedFile> = futures_util::stream::iter(current)
            .map(|file| async move {
                let work = async {
                    if self.inner.database.whole_kept(&file).await? {
                        return Ok(PreparedFile::Kept(file.clone()));
                    }
                    let opened = UploadedFile::open(self.inner.clone(), file.clone()).await?;
                    Ok(PreparedFile::Copy(opened.keep_whole().await?))
                }
                .await;
                self.inner.complete_pin(&file, work).await
            })
            .buffer_unordered(limit)
            .try_collect()
            .await?;
        let (mut copies, mut kept) = (Vec::new(), Vec::new());
        for file in prepared {
            match file {
                PreparedFile::Copy(copy) => copies.push(copy),
                PreparedFile::Kept(file) => kept.push(file),
            }
        }
        Ok(self.inner.database.prepared_pin(copies, kept)?)
    }
    /// Prepare a pin and commit it in a write of its own, as an app does,
    /// for tests.
    #[cfg(test)]
    pub(crate) async fn pin(&self, files: &[FileRef]) -> Result<(), FileReadError> {
        let prepared = self.prepare_pin(files).await?;
        Ok(self.inner.database.commit_pin(prepared).await?)
    }
    /// The namespace's budget, declared or since set (E8).
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<u64, DbError> {
        self.inner.check_open()?;
        self.inner.database.budget(namespace).await
    }
}
impl FilesInner {
    /// Reserve a whole cache file of `file` and create it for writing. The
    /// writer syncs its bytes on finishing; the reservation then publishes
    /// them, and a dropped one leaves them to their recorded removal.
    pub(crate) async fn create_cache_file(
        &self,
        file: &FileRef,
    ) -> Result<(CacheReservation, FileWriter), FileReadError> {
        let name = FileName::new(self.ids.new_id().to_string()).expect("UUID cache name");
        let reservation = self.database.reserve_cache_file(file, name.clone()).await?;
        let target = self.directory.file(FileArea::Cache, &name);
        let directory = self.directory.clone();
        // Cleanup cannot release the name while creation still runs after a
        // cancelled await; the blocking task retains its reservation.
        let joined = tokio::task::spawn_blocking(move || {
            let writer = (|| -> Result<_, FileReadError> {
                let lock = directory.lock_read_only()?;
                Ok(target.create_writer(lock)?)
            })();
            (reservation, writer)
        })
        .await;
        // The pool dropping the creation unstarted drops the reservation
        // with it, which leaves the name to its recorded removal.
        let (reservation, writer) =
            coven_foundation::unwinding::blocking("create a cache file", joined)
                .map_err(coven_foundation::files::FileError::from)?;
        Ok((reservation, writer?))
    }
}
