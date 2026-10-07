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
    pub(super) fn changes(&self) -> Result<Vec<RowChange>, DbError> {
        let mut tables = self
            .tables
            .lock()
            .expect("stream observation lock poisoned")
            .clone();
        for table in tables.clone() {
            if let Some(parent) = crate::change_capture::shadow_parent(self.database, &table)? {
                tables.insert(parent);
            }
        }
        Ok(tables
            .into_iter()
            .map(|table| RowChange {
                table,
                column: String::new(),
                keys: None,
            })
            .collect())
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

#[cfg(test)]
#[path = "stream_capture_tests.rs"]
mod tests;
