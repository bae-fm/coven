//! A connection's page cache: how much it holds, and giving it back (§4).

use super::DatabaseConnection;

impl DatabaseConnection {
    /// The bytes this connection's page cache holds now, SQLite's
    /// `SQLITE_DBSTATUS_CACHE_USED`. It counts retained pages even when
    /// global memory accounting is disabled.
    pub(crate) fn cache_used(&self) -> rusqlite::Result<i32> {
        let (mut used, mut high) = (0, 0);
        // SAFETY: this owner exclusively uses its live connection.
        let code = unsafe {
            rusqlite::ffi::sqlite3_db_status(
                self.connection.handle(),
                rusqlite::ffi::SQLITE_DBSTATUS_CACHE_USED,
                &mut used,
                &mut high,
                0,
            )
        };
        if code != rusqlite::ffi::SQLITE_OK {
            return Err(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None));
        }
        Ok(used)
    }

    /// Give back the cached pages no statement is using, as
    /// `sqlite3_db_release_memory` does. Pages a transaction in progress
    /// changed and hasn't written out stay; the next read loads what it needs
    /// again.
    pub(crate) fn release_memory(&self) -> rusqlite::Result<()> {
        self.connection.release_memory()
    }
}

#[cfg(test)]
#[path = "page_cache_tests.rs"]
mod tests;
