//! The read-only application surface caches files under a shared deletion guard.

use crate::*;
use coven_database::{Database, ReadOnly};

/// A handle that reads synced rows and maintains its local file cache (§4, E1).
/// Its shared store lock prevents deletion until all its connections close.
/// Its SQL may not read `coven_sync_status`: it can't tell whether the writer
/// that last wrote the row still runs (E5).
///
/// ```compile_fail
/// async fn no_writes(handle: &coven::CovenReadHandle) {
///     handle.write(|_| Ok::<_, coven::CovenError>(())).await;
/// }
/// ```
/// ```compile_fail
/// fn no_live_query(handle: &coven::CovenReadHandle) {
///     handle.subscribe(|_| Ok::<_, coven::CovenError>(()));
/// }
/// ```
#[derive(Clone)]
pub struct CovenReadHandle {
    database: Database<ReadOnly>,
    files: coven_sync::Files<ReadOnly>,
}

impl CovenReadHandle {
    pub(crate) fn new(database: Database<ReadOnly>, files: coven_sync::Files<ReadOnly>) -> Self {
        Self { database, files }
    }
    /// The store's current name from this device's committed state (E9).
    pub async fn store_name(&self) -> CovenResult<String> {
        self.database.store_name().await.map_err(CovenError::from)
    }

    /// A read of one consistent snapshot, run when awaited.
    pub fn read<F, R, E>(&self, read: F) -> Read<'_, F, ReadOnly>
    where
        F: FnOnce(SqlReadContext<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: From<DbError> + task::CarriesBug + Send + 'static,
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
    /// Reads a whole file, in one place of `lease`. A stored file is checked
    /// against its content hash.
    pub async fn read_file(
        &self,
        file: &FileRef,
        lease: &FileLease,
    ) -> Result<Vec<u8>, FileReadError> {
        let stream = self.open_file_stream(file, lease).await?;
        stream.read_at(0, stream.plaintext_size()).await
    }
    /// Opens a file for reading ranges (§17.7), as its record stands when it
    /// opens; keep the stream for as long as the file is read. The stream
    /// holds one place of `lease` until it drops. Its shared store lock
    /// prevents deletion until it and its I/O finish.
    pub async fn open_file_stream(
        &self,
        file: &FileRef,
        lease: &FileLease,
    ) -> Result<FileStream<ReadOnly>, FileReadError> {
        self.files.open_file_stream(file, lease).await
    }
    /// Closes read and cache connections on all clones and releases their
    /// storage client and shared store lock, while the app still holds the
    /// handle. From its first poll every clone refuses new calls with
    /// `StoreClosed`; the closing itself runs as its own task, so dropping
    /// this future doesn't stop it (E1, E15). File streams retain their own
    /// locks.
    pub async fn close(&self) -> Result<(), DbError> {
        self.database.begin_close();
        let handle = self.clone();
        let bugs = self.database.bugs().clone();
        let closing = async move {
            handle.files.close().await;
            handle.database.close().await
        };
        crate::task::spawn_completing("close the read-only store", bugs, closing).await?
    }
}
