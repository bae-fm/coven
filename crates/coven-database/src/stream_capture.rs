//! Bounded observation for streamed transactions, including WITHOUT ROWID tables.

use super::DatabaseConnection;
use crate::{observation::RowChange, DbError};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

pub(super) struct StreamCapture<'a> {
    database: &'a DatabaseConnection,
    tables: Arc<Mutex<BTreeSet<String>>>,
}
impl<'a> StreamCapture<'a> {
    pub(super) fn begin(database: &'a DatabaseConnection) -> Result<Self, DbError> {
        let tables = Arc::new(Mutex::new(BTreeSet::new()));
        let observed = tables.clone();
        database.connection.preupdate_hook(Some(
            move |_, schema: &str, table: &str, _: &rusqlite::hooks::PreUpdateCase| {
                if schema == "main" {
                    observed
                        .lock()
                        .expect("stream observation lock poisoned")
                        .insert(table.to_ascii_lowercase());
                }
            },
        ))?;
        Ok(Self { database, tables })
    }
    pub(super) fn tables(&self) -> BTreeSet<String> {
        self.tables
            .lock()
            .expect("stream observation lock poisoned")
            .clone()
    }

    pub(super) fn changes(&self) -> Result<Vec<RowChange>, DbError> {
        crate::change_capture::table_changes(self.database, self.tables())
    }
}
impl Drop for StreamCapture<'_> {
    fn drop(&mut self) {
        self.database
            .connection
            .preupdate_hook(
                None::<fn(rusqlite::hooks::Action, &str, &str, &rusqlite::hooks::PreUpdateCase)>,
            )
            .expect("remove stream observation hook");
    }
}

impl DatabaseConnection {
    /// Keep a streamed transaction's page cache bounded: past 4 MiB of
    /// retained pages, spill and release them without committing.
    pub(super) fn bound_stream_cache(&self) -> rusqlite::Result<()> {
        if !self.streaming.get() {
            return Ok(());
        }
        let (mut used, mut high) = (0, 0);
        // SAFETY: this owner exclusively uses its live connection. CACHE_USED
        // counts retained pages even when global memory accounting is disabled.
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
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                None,
            ));
        }
        if used > 4 * 1024 * 1024 {
            // cache_size is a suggestion. Spill uncommitted pages without
            // committing the transaction, then release reusable cache pages.
            self.connection.cache_flush()?;
            self.connection.release_memory()?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "stream_capture_tests.rs"]
mod tests;
