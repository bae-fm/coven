//! A local write's app phase, observed one SQLite row operation at a time.
//!
//! Sessions record only net changes. A write's steps need each operation, in
//! order (§9.6, D5). A connection has one pre-update hook, which a session
//! would otherwise own, so the app phase records through this hook and no
//! session runs until it ends.

use super::DatabaseConnection;
use crate::sql_value::SqlValue;
use crate::DbError;
use rusqlite::hooks::{Action, PreUpdateCase};
use std::sync::{Arc, Mutex};

/// One row operation of the app phase.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Event {
    /// The table, as SQLite names it.
    pub(crate) table: String,
    /// Insert, update or delete.
    pub(crate) action: Action,
    /// Every column before the operation, in declaration order; `None` for a
    /// virtual generated column. Empty for an insert.
    pub(crate) old: Vec<Option<SqlValue>>,
    /// Every column after the operation; empty for a delete.
    pub(crate) new: Vec<Option<SqlValue>>,
    /// The rowid before and after, meaningful only for rowid tables.
    pub(crate) rowids: (i64, i64),
}

pub(super) struct EventCapture<'a> {
    database: &'a DatabaseConnection,
    events: Arc<Mutex<Vec<Event>>>,
}

impl<'a> EventCapture<'a> {
    pub(super) fn begin(database: &'a DatabaseConnection) -> Result<Self, DbError> {
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&events);
        database.connection.preupdate_hook(Some(
            move |action: Action, schema: &str, table: &str, case: &PreUpdateCase| {
                if schema != "main" {
                    return;
                }
                let old = |accessor: &rusqlite::hooks::PreUpdateOldValueAccessor| {
                    (0..accessor.get_column_count())
                        .map(|i| accessor.get_old_column_value(i).ok().map(SqlValue::from))
                        .collect::<Vec<_>>()
                };
                let new = |accessor: &rusqlite::hooks::PreUpdateNewValueAccessor| {
                    (0..accessor.get_column_count())
                        .map(|i| accessor.get_new_column_value(i).ok().map(SqlValue::from))
                        .collect::<Vec<_>>()
                };
                let event = match case {
                    PreUpdateCase::Insert(accessor) => Event {
                        table: table.to_owned(),
                        action,
                        old: Vec::new(),
                        new: new(accessor),
                        rowids: (accessor.get_new_row_id(), accessor.get_new_row_id()),
                    },
                    PreUpdateCase::Delete(accessor) => Event {
                        table: table.to_owned(),
                        action,
                        old: old(accessor),
                        new: Vec::new(),
                        rowids: (accessor.get_old_row_id(), accessor.get_old_row_id()),
                    },
                    PreUpdateCase::Update {
                        old_value_accessor,
                        new_value_accessor,
                    } => Event {
                        table: table.to_owned(),
                        action,
                        old: old(old_value_accessor),
                        new: new(new_value_accessor),
                        rowids: (
                            old_value_accessor.get_old_row_id(),
                            new_value_accessor.get_new_row_id(),
                        ),
                    },
                    PreUpdateCase::Unknown => return,
                };
                recorded
                    .lock()
                    .expect("event capture lock poisoned")
                    .push(event);
            },
        ))?;
        Ok(Self { database, events })
    }

    /// The operations so far, in order.
    pub(super) fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .expect("event capture lock poisoned")
            .clone()
    }
}

impl Drop for EventCapture<'_> {
    fn drop(&mut self) {
        self.database
            .connection
            .preupdate_hook(None::<fn(Action, &str, &str, &PreUpdateCase)>)
            .expect("remove event capture hook");
    }
}

#[cfg(test)]
#[path = "event_capture_tests.rs"]
mod tests;
