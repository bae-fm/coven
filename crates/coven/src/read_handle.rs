//! The read-only application surface caches files under a shared deletion guard.

use crate::*;
use coven_crypto::custody::StoreKeys;
use coven_database::DatabaseReadHandle;
use std::sync::{Arc, Mutex};

/// A handle that reads synced rows and maintains its local file cache (§5, E1).
/// Its shared store lock prevents deletion until all its connections close.
///
/// ```compile_fail
/// async fn no_writes(handle: &coven::CovenReadHandle) {
///     handle.write(|_| Ok(())).await;
/// }
/// ```
/// ```compile_fail
/// fn no_live_query(handle: &coven::CovenReadHandle) {
///     handle.subscribe(|_| Ok(()));
/// }
/// ```
#[derive(Clone)]
pub struct CovenReadHandle {
    database: DatabaseReadHandle,
    files: coven_sync::Files,
    keys: Arc<Mutex<Option<StoreKeys>>>,
}

impl CovenReadHandle {
    pub(crate) fn new(
        database: DatabaseReadHandle,
        keys: StoreKeys,
        files: coven_sync::Files,
    ) -> Self {
        Self {
            database,
            files,
            keys: Arc::new(Mutex::new(Some(keys))),
        }
    }
    /// A read of one consistent snapshot, run when awaited.
    pub fn read<F, R>(&self, read: F) -> Read<'_, F>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.database.read(read)
    }
    /// The file a row carries, as of its current file version.
    pub async fn file_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<FileRef, DbError> {
        self.database.file_ref(table, key).await
    }
    /// The path, size and modification time recorded for a row's original.
    pub async fn user_file(
        &self,
        table: &str,
        key: impl Into<RowKey>,
    ) -> Result<Option<UserFile>, DbError> {
        self.database.user_file(table, key).await
    }
    /// Reads a whole file, checking it against its row.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError> {
        let stream = self.open_file_stream(file).await?;
        stream.read_at(0, stream.plaintext_size()).await
    }
    /// Opens a file for reading ranges (§16.3). Opening checks the file
    /// against its row once; keep the stream for as long as the file is read.
    /// Its shared store lock prevents deletion until it and its I/O finish.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError> {
        self.files.open_file_stream(file).await
    }
    /// Decrypts app data with the retained store key its header names.
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        self.keys
            .lock()
            .expect("custody lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?
            .open_app_data(sealed, aad)
    }
    /// Closes read and cache connections, releases their shared store lock,
    /// and drops unlocked keys on all clones. File streams retain their own locks.
    pub async fn close(&self) -> Result<(), DbError> {
        let handle = self.clone();
        crate::coven::completion(tokio::spawn(async move {
            handle.files.close().await;
            let custody = handle.keys.clone();
            crate::coven::blocking(move || {
                custody.lock().expect("custody lock poisoned").take();
            })
            .await;
            handle.database.close().await
        }))
        .await
    }
}
