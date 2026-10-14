//! Application file calls delegate to the composed sync file owner.
use crate::*;

impl CovenHandle {
    /// Calls single-object status, then checks the uploader's current state
    /// and its signed undeliverable-file report (§16.1). Checks the reference
    /// against its row. Network failures and denied access are errors, not Missing.
    pub async fn file_status(&self, file: &FileRef) -> Result<FileStatus, FileReadError> {
        self.files.file_status(file).await
    }
    /// Queue, pause setting and provider-confirmed session progress in one
    /// committed database snapshot, available while disconnected (E7).
    pub async fn uploads(&self) -> CovenResult<UploadQueue> {
        UploadQueue::from_state(self.database.upload_queue().await?)
    }
    /// The same pre-built query, through the app's live-query mechanism.
    pub fn subscribe_uploads(&self) -> LiveQuery<UploadQueue> {
        self.database
            .subscribe_upload_queue()
            .process(UploadQueue::from_state)
    }
    /// Attempt waiting uploads now, overriding their ordinary backoff and
    /// app-action waits; a provider cooldown still holds them (§16.5).
    /// Individual failures stay in pending().
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError> {
        self.files.retry_uploads_now().await
    }
    /// Pause uploads, or resume them. A paused upload keeps its place,
    /// including a provider upload session in progress; the setting persists.
    pub async fn set_uploads_paused(&self, paused: bool) -> CovenResult<()> {
        Ok(self.files.set_uploads_paused(paused).await?)
    }
    /// Keeps uploaded files whole on this device regardless of the cache
    /// budget, downloading what is missing before the call returns. Progress
    /// is read from `pin_progress()`.
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
    /// `None` for a key with no row carrying a file. A file not yet uploaded
    /// reads as not pinned.
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
    /// An unset budget evicts nothing.
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
        self.files.get_cache_budget(namespace).await
    }
    /// Progress of downloading every file declared to download as soon as
    /// its row arrives, over the requested set's verified coverage.
    pub async fn eager_cache_fill_status(&self) -> CovenResult<EagerCacheFillStatus> {
        self.database.eager_cache_fill_status().await
    }
    /// The same query over cache reservations, cancellation and verified ranges.
    pub fn subscribe_eager_cache_fill_status(&self) -> LiveQuery<EagerCacheFillStatus> {
        self.database.subscribe_eager_cache_fill_status()
    }
    /// Records cancellation of the current eager requests without stopping sync.
    pub async fn cancel_eager_cache_fill(&self) -> CovenResult<()> {
        Ok(self.files.cancel_eager_cache_fill().await?)
    }
}
#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
