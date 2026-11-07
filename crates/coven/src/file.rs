//! Application file calls delegate to the composed sync file owner.
use crate::*;

impl CovenHandle {
    /// The file's status as its record now stands (§17.6): one this device
    /// uploads from local facts, one only on another device from its record,
    /// and a stored one by a single-object status call. Network failures and
    /// denied access are errors, not Missing.
    pub async fn file_status(&self, file: &FileRef) -> Result<FileStatus, FileReadError> {
        self.files.file_status(file).await
    }
    /// Prepares keeping stored files whole on this device regardless of the
    /// cache budget (§17.8): downloads what each lacks into a whole copy,
    /// checked against its content hash, and pins nothing; a write commits
    /// the result with `CovenWrite::pin`. A file only on one device, this
    /// one included while it uploads, is refused with
    /// `FileReadError::DeviceOnly`, and one whose record goes, or leaves its
    /// stored bytes, while they download keeps nothing and fails with
    /// `DbError::NoLongerStored`. A prepare that stops, dropped or ended by
    /// a crash, keeps the bytes it downloaded of each file, apart from the
    /// cache, and the next prepare of the file continues from them; the
    /// app's unpin of the file ends them, and a prepare it ends fails with
    /// `DbError::PinChanged`. Which files are downloading is in
    /// `coven_downloading`, how far each has got on the download watch.
    pub async fn prepare_pin(&self, files: &[FileRef]) -> Result<PreparedPin, FileReadError> {
        self.files.prepare_pin(files).await
    }
    /// Prepares a pin and commits it in a write of its own.
    pub async fn pin(&self, files: &[FileRef]) -> Result<(), FileReadError> {
        let prepared = self.prepare_pin(files).await?;
        self.write(move |write| write.pin(prepared)).await.map_err(FileReadError::from)
    }
    /// Verified cache coverage for these fixed references, in one snapshot.
    pub async fn pin_progress(&self, files: Vec<FileRef>) -> CovenResult<PinProgress> {
        self.database.pin_progress(files).await.map_err(CovenError::from)
    }
    /// The same pre-built cache query with the same fixed references.
    pub fn subscribe_pin_progress(
        &self,
        files: Vec<FileRef>,
    ) -> LiveQuery<PinProgress, CovenError> {
        self.database.subscribe_pin_progress(files)
    }
    /// Release pins and apply cache budgets, in a write of its own
    /// (`CovenWrite::unpin`).
    pub async fn unpin(&self, files: &[FileId]) -> CovenResult<()> {
        let files = files.to_vec();
        self.write(move |write| Ok(write.unpin(&files)?)).await
    }
    /// Whether each file is pinned, one answer per file in order, or `None`
    /// for a file this device has no record of. The answer is whether
    /// `coven_pins` lists it.
    pub async fn files_pinned(&self, files: &[FileId]) -> CovenResult<Vec<Option<bool>>> {
        self.database.files_pinned(files.to_vec()).await.map_err(CovenError::from)
    }
    /// The same internal query, live. To watch different files, drop this
    /// query and subscribe with the new files.
    pub fn subscribe_files_pinned(
        &self,
        files: &[FileId],
    ) -> LiveQuery<Vec<Option<bool>>, CovenError> {
        self.database.subscribe_files_pinned(files.to_vec())
    }
    /// Remove cache copies only, in a write of its own
    /// (`CovenWrite::evict_file`).
    pub async fn evict_file(&self, file: FileId) -> CovenResult<()> {
        self.write(move |write| Ok(write.evict_file(file)?)).await
    }
    /// Set and enforce a namespace's independent byte budget, in a write of
    /// its own (`CovenWrite::set_cache_budget`).
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> CovenResult<()> {
        let namespace = namespace.to_owned();
        self.write(move |write| Ok(write.set_cache_budget(&namespace, max_bytes)?)).await
    }
    /// The namespace's budget, declared or since set.
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<u64, DbError> {
        self.files.get_cache_budget(namespace).await
    }
}
#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
