//! The read-only application surface caches files under a shared deletion guard.

use crate::*;
use coven_database::DatabaseReadHandle;

/// A handle that reads synced rows and maintains its local file cache (§4, E1).
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
}

impl CovenReadHandle {
    pub(crate) fn new(database: DatabaseReadHandle, files: coven_sync::Files) -> Self {
        Self { database, files }
    }
    /// The store's current name from this device's committed state (E9).
    pub async fn store_name(&self) -> CovenResult<String> {
        self.database.store_name().await
    }

    /// A read of one consistent snapshot, run when awaited.
    pub fn read<F, R>(&self, read: F) -> Read<'_, F>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.database.read(read)
    }
    /// The SQLite settings this handle's connections have in effect (§4).
    pub async fn sqlite_settings(&self) -> Result<SqliteSettingsInEffect, DbError> {
        self.database.sqlite_settings().await
    }
    /// The file a row references, by the id the row holds.
    pub async fn file_ref(&self, file: FileId) -> Result<FileRef, DbError> {
        self.database.file_ref(file).await
    }
    /// The path, size and modification time recorded for a file's original.
    pub async fn user_file(&self, file: FileId) -> Result<Option<UserFile>, DbError> {
        self.database.user_file(file).await
    }
    /// Reads a whole file. A stored file is checked against its content hash.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError> {
        let stream = self.open_file_stream(file).await?;
        stream.read_at(0, stream.plaintext_size()).await
    }
    /// Opens a file for reading ranges (§17.7), as its record stands when it
    /// opens; keep the stream for as long as the file is read. Its shared
    /// store lock prevents deletion until it and its I/O finish.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError> {
        self.files.open_file_stream(file).await
    }
    /// Closes read and cache connections on all clones and releases their shared
    /// store lock. File streams retain their own locks.
    pub async fn close(&self) -> Result<(), DbError> {
        let handle = self.clone();
        crate::coven::completion(tokio::spawn(async move {
            handle.files.close().await;
            handle.database.close().await
        }))
        .await
    }
}
