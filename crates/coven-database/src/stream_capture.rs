//! Bounded observation for streamed transactions, including WITHOUT ROWID
//! tables: applying another device's slot, loading a snapshot, and coven's
//! upload and claim bookkeeping.

use super::pre_update_changes::{PreUpdateChanges, TableShapes};
use super::DatabaseConnection;
use crate::{observation::RowChange, DbError};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

/// What a streamed transaction's pre-update hook keeps for live queries.
enum Recorded {
    /// On the writer that serves live queries: each table's changed
    /// columns, and the rows' images where a waiting live query narrows it.
    Rows { shapes: Arc<TableShapes>, changes: PreUpdateChanges },
    /// On a connection that knows no table's shape: the tables changed.
    Tables(BTreeSet<String>),
}

pub(super) struct StreamCapture<'a> {
    database: &'a DatabaseConnection,
    recorded: Arc<Mutex<Recorded>>,
}

impl<'a> StreamCapture<'a> {
    /// Observe `database`'s streamed transaction. A writer that serves live
    /// queries knows its tables' shapes and the columns waiting live queries
    /// narrow by; it keeps those values of each changed row, up to a bound
    /// per table, as a local write does.
    pub(super) fn begin(database: &'a DatabaseConnection) -> Result<Self, DbError> {
        let recorded = Arc::new(Mutex::new(match &database.observation {
            Some(observation) => {
                let shapes = Arc::clone(&observation.tables);
                let scoped = match &database.handle {
                    Some(handle) => shapes.scoped(&handle.scoped_columns()),
                    None => std::collections::HashMap::new(),
                };
                Recorded::Rows { shapes, changes: PreUpdateChanges::new(scoped) }
            }
            None => Recorded::Tables(BTreeSet::new()),
        }));
        let observed = Arc::clone(&recorded);
        database.connection.preupdate_hook(Some(
            move |_, schema: &str, table: &str, case: &rusqlite::hooks::PreUpdateCase| {
                if schema != "main" {
                    return;
                }
                match &mut *observed.lock().expect("stream observation lock poisoned") {
                    Recorded::Rows { shapes, changes } => match shapes.position(table) {
                        Some(position) => changes.record(shapes, position, case),
                        None => changes.unshaped(table),
                    },
                    Recorded::Tables(tables) => {
                        tables.insert(table.to_ascii_lowercase());
                    }
                }
            },
        ))?;
        Ok(Self { database, recorded })
    }

    /// What the transaction changed, as live queries read it.
    pub(super) fn changes(&self) -> Result<Vec<RowChange>, DbError> {
        let recorded = std::mem::replace(
            &mut *self.recorded.lock().expect("stream observation lock poisoned"),
            Recorded::Tables(BTreeSet::new()),
        );
        match recorded {
            Recorded::Rows { shapes, changes } => Ok(changes.finish(&shapes)),
            Recorded::Tables(tables) => crate::change_capture::table_changes(self.database, tables),
        }
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
        if self.cache_used()? > 4 * 1024 * 1024 {
            // cache_size is a suggestion. Spill uncommitted pages without
            // committing the transaction, then release reusable cache pages.
            self.connection.cache_flush()?;
            self.release_memory()?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "stream_capture_tests.rs"]
mod tests;
