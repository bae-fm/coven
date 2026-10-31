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
    /// Keeps stored files whole on this device regardless of the cache
    /// budget, downloading what is missing before the call returns. A file
    /// only on one device, this one included while it uploads, is refused
    /// with `FileReadError::DeviceOnly` (§17.8), and one whose record goes,
    /// or leaves its stored bytes, while they download keeps nothing and
    /// fails with `DbError::NoLongerStored`. Progress is read from
    /// `pin_progress()`.
    pub async fn pin(&self, files: &[FileRef]) -> Result<(), FileReadError> {
        self.files.pin(files).await
    }
    /// Verified cache coverage for these fixed references, in one snapshot.
    pub async fn pin_progress(&self, files: Vec<FileRef>) -> CovenResult<PinProgress> {
        self.database.pin_progress(files).await
    }
    /// The same pre-built cache query with the same fixed references.
    pub fn subscribe_pin_progress(&self, files: Vec<FileRef>) -> LiveQuery<PinProgress> {
        self.database.subscribe_pin_progress(files)
    }
    /// Release pins and apply cache budgets.
    pub async fn unpin(&self, files: &[FileRef]) -> Result<(), FileReadError> {
        self.files.unpin(files).await
    }
    /// Whether each file is pinned, one answer per file in order, or `None`
    /// for a file this device has no record of. The answer is whether
    /// `coven_pins` lists it.
    pub async fn files_pinned(&self, files: &[FileId]) -> CovenResult<Vec<Option<bool>>> {
        self.database.files_pinned(files.to_vec()).await
    }
    /// The same internal query, live. To watch different files, drop this
    /// query and subscribe with the new files.
    pub fn subscribe_files_pinned(&self, files: &[FileId]) -> LiveQuery<Vec<Option<bool>>> {
        self.database.subscribe_files_pinned(files.to_vec())
    }
    /// Remove cache copies only, without changing storage or local originals.
    pub async fn evict_file(&self, file: &FileRef) -> Result<(), FileReadError> {
        self.files.evict_file(file).await
    }
    /// Set and enforce a namespace's independent byte budget.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        self.files.set_cache_budget(namespace, max_bytes).await
    }
    /// The namespace's budget, declared or since set.
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<u64, DbError> {
        self.files.get_cache_budget(namespace).await
    }
}
#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
