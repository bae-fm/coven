//! Application file calls delegate to the composed sync file owner.
use crate::*;

impl CovenHandle {
    /// Calls single-object status, then checks the uploader's current state
    /// and any source failure observed by this installation (§17.4). Checks the reference
    /// against its row. Network failures and denied access are errors, not Missing.
    pub async fn file_status(&self, file: &FileRef) -> Result<FileStatus, FileReadError> {
        self.files.file_status(file).await
    }
    /// Queue, pause setting and provider-confirmed session progress in one
    /// committed database snapshot, available while disconnected, with the
    /// files this device is sending now (E7).
    pub async fn uploads(&self) -> CovenResult<UploadQueue> {
        self.files
            .uploads_in_flight()
            .project(self.database.upload_queue().await?)
    }
    /// The same pre-built query, through the app's live-query mechanism. It
    /// also answers again as an upload starts or ends.
    pub fn subscribe_uploads(&self) -> LiveQuery<UploadQueue> {
        let sending = self.files.uploads_in_flight();
        self.database
            .subscribe_upload_queue()
            .rerun_on(sending.changes())
            .process(move |state| sending.project(state))
    }
    /// Optionally retry recoverable uploads early; provider cooldowns still
    /// hold them and individual failures stay in pending() (§17.7).
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError> {
        self.files.retry_uploads_now().await
    }
    /// Pause uploads, or resume them. A paused upload keeps its place,
    /// including a provider upload session in progress; the setting persists.
    pub async fn set_uploads_paused(&self, paused: bool) -> CovenResult<()> {
        Ok(self.files.set_uploads_paused(paused).await?)
    }
    /// Keeps files whole on this device regardless of the cache budget,
    /// downloading what is missing before the call returns; a file still
    /// queued here keeps its pin through its upload. Progress is read from
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
    /// Whether each row's file is pinned, one answer per key in order, or
    /// `None` for a key with no row carrying a file.
    pub async fn rows_pinned(
        &self,
        table: &str,
        keys: Vec<RowKey>,
    ) -> CovenResult<Vec<Option<bool>>> {
        self.database.rows_pinned(table, keys).await
    }
    /// The same internal query, live. To watch different keys, drop this
    /// query and subscribe with the new keys.
    pub fn subscribe_rows_pinned(
        &self,
        table: &str,
        keys: Vec<RowKey>,
    ) -> LiveQuery<Vec<Option<bool>>> {
        self.database.subscribe_rows_pinned(table, keys)
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
