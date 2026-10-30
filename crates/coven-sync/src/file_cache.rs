//! Pins, budgets and downloads of stored files.

use crate::{
    files::file_read::UploadedFile,
    files::{Files, FilesInner},
    FileReadError,
};
use coven_database::{CacheReservation, DbError, FilePlace, FileRef};
use coven_foundation::files::{FileArea, FileName, FileWriter, StoreDirAccess};
use futures_util::{StreamExt, TryStreamExt};

impl Files {
    /// Download missing chunks and retain complete stored files regardless
    /// of budgets. Progress is the committed coverage `pin_progress` reads. A
    /// file only on one device, this one included while it uploads, is
    /// refused: only stored bytes are kept (§17.7).
    pub async fn pin(&self, files: &[FileRef]) -> Result<(), FileReadError> {
        self.inner.check_open()?;
        let _pins = self.inner.pins.lock().await;
        let mut current = Vec::new();
        for file in files {
            let file = self.inner.current(file).await?;
            if let FilePlace::DeviceOnly { device } = file.place() {
                return Err(FileReadError::DeviceOnly {
                    file: file.id(),
                    device,
                });
            }
            current.push(file);
        }
        let limit = self.transfer_limits().downloads.get();
        futures_util::stream::iter(current)
            .map(|file| async move {
                let work = async {
                    if self.inner.database.pin_complete(&file).await? {
                        return Ok(());
                    }
                    UploadedFile::open(self.inner.clone(), file.clone())
                        .await?
                        .keep_whole()
                        .await
                }
                .await;
                self.inner.complete_download(&file, work, true).await
            })
            .buffer_unordered(limit)
            .try_for_each(|()| std::future::ready(Ok(())))
            .await
    }
    /// Release budget exemptions, then apply each affected namespace budget.
    /// A file only on one device has none to release.
    pub async fn unpin(&self, files: &[FileRef]) -> Result<(), FileReadError> {
        self.inner.check_open()?;
        let _pins = self.inner.pins.lock().await;
        let _cache = self.inner.cache.write().await;
        for file in files {
            let file = self.inner.current(file).await?;
            if let FilePlace::Cloud { .. } = file.place() {
                self.inner.database.unpin(&file).await?;
                self.inner.trim(file.namespace()).await;
            }
        }
        Ok(())
    }
    /// Remove cached bytes only, including pinned bytes; storage is untouched.
    /// A file only on one device has none.
    pub async fn evict_file(&self, file: &FileRef) -> Result<(), FileReadError> {
        self.inner.check_open()?;
        let _pins = self.inner.pins.lock().await;
        let _cache = self.inner.cache.write().await;
        let file = self.inner.current(file).await?;
        if let FilePlace::Cloud { .. } = file.place() {
            self.inner.database.evict(&file).await?;
        }
        Ok(())
    }
    /// Set the budget and immediately evict this namespace's unpinned LRU bytes.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        self.inner.check_open()?;
        let _cache = self.inner.cache.write().await;
        self.inner.database.set_budget(namespace, max_bytes).await?;
        self.inner.database.trim_cache(namespace).await?;
        Ok(())
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
        let (reservation, writer) = crate::files::join(tokio::task::spawn_blocking(move || {
            let writer = (|| -> Result<_, FileReadError> {
                let lock = directory.lock_read_only()?;
                Ok(target.create_writer(lock)?)
            })();
            (reservation, writer)
        }))
        .await;
        Ok((reservation, writer?))
    }
}
