//! Pins, budgets and downloads of stored files.

use crate::{
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
    /// (`NoLongerStored`). While the network is metered and the app moves
    /// files only on unmetered networks, the downloads wait before asking
    /// storage for bytes (§17.8).
    pub async fn prepare_pin(&self, files: &[FileRef]) -> Result<PreparedPin, FileReadError> {
        self.inner.check_open()?;
        let (mut copies, mut kept) = (Vec::new(), Vec::new());
        let mut waiting = files.to_vec();
        loop {
            // A metered network the app keeps files off holds the downloads
            // (§17.8): the wait holds no lock, so a connection change or the
            // store's close goes on meanwhile. A file whose download it
            // stopped starts again here; the ones done stay done.
            self.inner.read_unmetered_only().await?;
            self.inner.until_downloads_unheld().await?;
            let _pins = self.inner.pins.lock().await;
            self.inner.check_open()?;
            let mut current = Vec::new();
            for file in &waiting {
                let file = self.inner.current(file).await?;
                if let FilePlace::DeviceOnly { device } = file.place() {
                    return Err(FileReadError::DeviceOnly { file: file.id(), device });
                }
                current.push(file);
            }
            let limit = self.transfer_limits().downloads.get();
            let prepared: Vec<Result<PreparedFile, FileRef>> = futures_util::stream::iter(current)
                .map(|file| async move {
                    let work = async {
                        if self.inner.database.whole_kept(&file).await? {
                            return Ok(Some(PreparedFile::Kept(file.clone())));
                        }
                        Ok(self.inner.download_whole(file.clone()).await?.map(PreparedFile::Copy))
                    }
                    .await;
                    // A held download failed at nothing: no record, and no
                    // condition settled.
                    match work {
                        Ok(None) => Ok(Err(file)),
                        Ok(Some(prepared)) => {
                            self.inner.complete_pin(&file, Ok(prepared)).await.map(Ok)
                        }
                        Err(error) => {
                            self.inner.complete_pin::<PreparedFile>(&file, Err(error)).await.map(Ok)
                        }
                    }
                })
                .buffer_unordered(limit)
                .try_collect()
                .await?;
            waiting.clear();
            for file in prepared {
                match file {
                    Ok(PreparedFile::Copy(copy)) => copies.push(copy),
                    Ok(PreparedFile::Kept(file)) => kept.push(file),
                    Err(held) => waiting.push(held),
                }
            }
            if waiting.is_empty() {
                return Ok(self.inner.database.prepared_pin(copies, kept)?);
            }
        }
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
        let directory = self.directory.clone();
        // Cleanup cannot release the name while creation still runs after a
        // cancelled await; the blocking task retains its reservation.
        // A panic in the creation, or the pool dropping it unstarted, drops
        // the reservation with it, which leaves the name to its recorded
        // removal. The reservation comes back with the writer, or with why
        // it wasn't made, so a caller that stopped waiting drops both, and
        // only a panic goes to the sink.
        let (reservation, writer) = coven_foundation::task::blocking_completing(
            "create a cache file",
            self.database.bugs().clone(),
            move || {
                let writer =
                    directory.create_writer(FileArea::Cache, &name).map_err(FileReadError::from);
                Ok::<_, std::convert::Infallible>((reservation, writer))
            },
        )
        .await
        .map_err(coven_foundation::files::FileError::from)?
        .unwrap_or_else(|never| match never {});
        Ok((reservation, writer?))
    }
}
