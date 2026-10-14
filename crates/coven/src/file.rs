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
    /// Retain uploaded files regardless of the namespace's cache budget.
    pub async fn pin(
        &self,
        files: &[FileRef],
        on_progress: &(dyn Fn(PinProgress) + Send + Sync),
    ) -> Result<(), FileReadError> {
        self.files.pin(files, on_progress).await
    }
    /// Release pins and apply cache budgets.
    pub async fn unpin(&self, files: &[FileRef]) -> Result<(), FileReadError> {
        self.files.unpin(files).await
    }
    /// Pin state in key order; absent files produce `None`.
    pub async fn rows_pinned(
        &self,
        table: &str,
        keys: Vec<RowKey>,
    ) -> Result<Vec<Option<bool>>, FileReadError> {
        self.files.rows_pinned(table, keys).await
    }
    /// Observe row and pin changes, with a replaceable row request.
    pub fn subscribe_rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> RowsPinnedLiveQuery {
        self.files.subscribe_rows_pinned(table, keys)
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
    /// Observe eager downloads triggered by committed file rows.
    pub fn subscribe_eager_cache_fill_status(
        &self,
    ) -> tokio::sync::watch::Receiver<EagerCacheFillStatus> {
        self.files.subscribe_eager_cache_fill_status()
    }
    /// Stop current eager downloads without affecting uploads.
    pub fn cancel_eager_cache_fill(&self) {
        self.files.cancel_eager_cache_fill()
    }
}
#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
