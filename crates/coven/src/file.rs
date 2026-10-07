//! Application file calls delegate to the composed sync file owner.
use crate::*;

impl CovenHandle {
    /// Record downloads that keep uploaded files on this device. User-provided
    /// files require a previously absent destination, keyed by their file id.
    /// The call returns after recording; permanent failures remain operations.
    pub async fn keep_files_on_this_device(
        &self,
        files: &[FileRef],
        destinations: &std::collections::HashMap<String, std::path::PathBuf>,
    ) -> Result<(), OperationError> {
        self.operations
            .keep_files_on_this_device(files, destinations)
            .await
    }
    /// Durably queue local files; uploading proceeds after this call returns.
    pub async fn upload_files(&self, files: &[FileRef]) -> Result<(), OperationError> {
        self.files.upload_files(files).await
    }
    /// Current and subsequent upload queue states.
    pub fn subscribe_uploads(&self) -> UploadsLiveQuery {
        self.files.subscribe_uploads()
    }
    /// Attempt waiting uploads without waiting for their backoff.
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError> {
        self.files.retry_uploads_now().await
    }
    /// Pause or resume file transfers at provider part boundaries.
    pub fn set_uploads_paused(&self, paused: bool) {
        self.files.set_uploads_paused(paused)
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
    /// Whether every requested file is pinned; an empty list is pinned.
    pub async fn is_pinned(&self, files: &[FileRef]) -> Result<bool, FileReadError> {
        self.files.is_pinned(files).await
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
