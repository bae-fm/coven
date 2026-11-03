//! Bounded observation for streamed transactions, including WITHOUT ROWID
//! tables: applying another device's slot, loading a snapshot, and coven's
//! upload and claim bookkeeping.

use super::pre_update_changes::{PreUpdateChanges, TableShapes};
use super::write_transaction::WriteTransaction;
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

/// What a streamed transaction changed, as its capture saw it.
pub(super) enum StreamChanges {
    /// The changed rows, as live queries read them.
    Rows(Vec<RowChange>),
    /// The tables changed, on a connection that knows no table's shape:
    /// the connection reads their changes (`change_capture::table_changes`).
    Tables(BTreeSet<String>),
}

pub(super) struct StreamCapture {
    recorded: Arc<Mutex<Recorded>>,
}

impl StreamCapture {
    /// Observe a streamed transaction. A writer that serves live queries
    /// knows its tables' shapes and the columns waiting live queries narrow
    /// by; it keeps those values of each changed row, up to a bound per
    /// table, as a local write does.
    pub(super) fn begin(transaction: &mut WriteTransaction<'_>) -> Result<Self, DbError> {
        let recorded =
            Arc::new(Mutex::new(transaction.within(|database| match &database.observation {
                Some(observation) => {
                    let shapes = Arc::clone(&observation.tables);
                    let scoped = match &database.handle {
                        Some(handle) => shapes.scoped(&handle.scoped_columns()),
                        None => std::collections::HashMap::new(),
                    };
                    Recorded::Rows { shapes, changes: PreUpdateChanges::new(scoped) }
                }
                None => Recorded::Tables(BTreeSet::new()),
            })));
        let observed = Arc::clone(&recorded);
        transaction.observe_operations(
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
        )?;
        Ok(Self { recorded })
    }

    /// What the transaction changed, as the capture recorded it.
    pub(super) fn changes(&self) -> StreamChanges {
        let recorded = std::mem::replace(
            &mut *self.recorded.lock().expect("stream observation lock poisoned"),
            Recorded::Tables(BTreeSet::new()),
        );
        match recorded {
            Recorded::Rows { shapes, changes } => StreamChanges::Rows(changes.finish(&shapes)),
            Recorded::Tables(tables) => StreamChanges::Tables(tables),
        }
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
