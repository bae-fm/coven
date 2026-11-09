//! Syncing the database file through SQLite's own handle to it (§4).

use super::DatabaseConnection;
use crate::DbError;

impl DatabaseConnection {
    /// Sync the database file as this connection's checkpoints do: with
    /// `F_FULLFSYNC` on macOS and iOS when `checkpoint_fullfsync` or
    /// `fullfsync` is on, as SQLite's own checkpoint sync is, and a plain sync
    /// otherwise. It goes through SQLite's own handle to the file, because
    /// closing any other handle to it would drop every lock SQLite holds on
    /// it in this process.
    pub(crate) fn sync_database_file(&self) -> Result<(), DbError> {
        let flush = |pragma: &str| -> Result<bool, DbError> {
            Ok(self.query_row::<i64, _>(&format!("PRAGMA {pragma}"), [], |r| r.get(0))? != 0)
        };
        let flags = if flush("checkpoint_fullfsync")? || flush("fullfsync")? {
            rusqlite::ffi::SQLITE_SYNC_FULL
        } else {
            rusqlite::ffi::SQLITE_SYNC_NORMAL
        };
        let mut file: *mut rusqlite::ffi::sqlite3_file = std::ptr::null_mut();
        // SAFETY: this owner exclusively uses its live connection, and
        // SQLITE_FCNTL_FILE_POINTER writes one pointer to `file`.
        let code = unsafe {
            rusqlite::ffi::sqlite3_file_control(
                self.connection.handle(),
                c"main".as_ptr(),
                rusqlite::ffi::SQLITE_FCNTL_FILE_POINTER,
                (&raw mut file).cast(),
            )
        };
        failed(code)?;
        // SAFETY: SQLite hands out the main database's file, open while the
        // connection is, with the methods SQLite set as it opened it.
        let sync = unsafe { file.as_ref().and_then(|file| file.pMethods.as_ref()) }
            .and_then(|methods| methods.xSync)
            .ok_or(DbError::Invariant { what: "an open connection's database file syncs" })?;
        // SAFETY: the file is the connection's own, which no one else uses
        // while this owner holds the connection.
        failed(unsafe { sync(file, flags) })
    }
}

/// SQLite's result `code` as a result.
fn failed(code: std::ffi::c_int) -> Result<(), DbError> {
    if code == rusqlite::ffi::SQLITE_OK {
        return Ok(());
    }
    Err(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None).into())
}
