//! The read-only application surface retains a shared deletion guard.

use crate::*;
use coven_crypto::custody::StoreKeys;
use coven_database::DatabaseReadHandle;
use std::sync::{Arc, Mutex};

/// A handle that only reads an already open store (§5, §20.1).
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
    keys: Arc<Mutex<Option<StoreKeys>>>,
}

impl CovenReadHandle {
    pub(crate) fn new(database: DatabaseReadHandle, keys: StoreKeys) -> Self {
        Self {
            database,
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
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError> {
        Ok(FileStream::new(self.database.open_local_file(file).await?))
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
    /// Closes every connection and drops unlocked key material on all clones.
    pub async fn close(&self) -> Result<(), DbError> {
        let handle = self.clone();
        crate::coven::completion(tokio::spawn(async move {
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
