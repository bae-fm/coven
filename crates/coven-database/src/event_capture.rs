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
use std::collections::{BTreeMap, BTreeSet};
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
    /// Reference columns explicitly assigned NULL while already displaying NULL.
    pub(crate) null_assignments: BTreeSet<String>,
}

#[derive(Default)]
struct CapturedEvents {
    operations: Vec<(i32, Event)>,
    /// A marker rowid may be reused after a statement rolls back. Its latest
    /// insertion identifies the operation the surviving SQL row belongs to.
    assignments: BTreeMap<i64, (usize, String)>,
}

pub(super) struct EventCapture<'a> {
    database: &'a DatabaseConnection,
    events: Arc<Mutex<CapturedEvents>>,
}

impl<'a> EventCapture<'a> {
    pub(super) fn begin(database: &'a DatabaseConnection) -> Result<Self, DbError> {
        let events = Arc::new(Mutex::new(CapturedEvents::default()));
        let recorded = Arc::clone(&events);
        database.connection.preupdate_hook(Some(
            move |action: Action, schema: &str, table: &str, case: &PreUpdateCase| {
                if schema == "temp" && table == super::reference_assignments::TABLE {
                    if let PreUpdateCase::Insert(accessor) = case {
                        let table = accessor.get_new_column_value(0).expect("marker table");
                        let column = accessor.get_new_column_value(1).expect("marker column");
                        let mut events = recorded.lock().expect("event capture lock poisoned");
                        let index = events
                            .operations
                            .iter()
                            .enumerate()
                            .rev()
                            .find(|(_, (depth, event))| {
                                *depth + 1 == accessor.get_query_depth()
                                    && event.table == table.as_str().expect("marker table name")
                                    && event.action == Action::SQLITE_UPDATE
                            })
                            .expect("assignment trigger follows its row update")
                            .0;
                        events.assignments.insert(
                            accessor.get_new_row_id(),
                            (index, column.as_str().expect("marker column name").into()),
                        );
                    }
                    return;
                }
                if schema != "main" {
                    return;
                }
                let depth = match case {
                    PreUpdateCase::Insert(a) => a.get_query_depth(),
                    PreUpdateCase::Delete(a) => a.get_query_depth(),
                    PreUpdateCase::Update {
                        new_value_accessor, ..
                    } => new_value_accessor.get_query_depth(),
                    PreUpdateCase::Unknown => return,
                };
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
                        null_assignments: BTreeSet::new(),
                        action,
                        old: Vec::new(),
                        new: new(accessor),
                        rowids: (accessor.get_new_row_id(), accessor.get_new_row_id()),
                    },
                    PreUpdateCase::Delete(accessor) => Event {
                        table: table.to_owned(),
                        null_assignments: BTreeSet::new(),
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
                        null_assignments: BTreeSet::new(),
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
                    .operations
                    .push((depth, event));
            },
        ))?;
        Ok(Self { database, events })
    }

    /// Only SQLite's surviving marker rows count. Pre-update hooks also report
    /// operations subsequently rolled back by a failed statement.
    pub(super) fn complete_assignments(&self, surviving: &[i64]) {
        let mut captured = self.events.lock().expect("event capture lock poisoned");
        for id in surviving {
            let (index, column) = captured.assignments[id].clone();
            captured.operations[index].1.null_assignments.insert(column);
        }
    }

    /// The operations so far, in order.
    pub(super) fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .expect("event capture lock poisoned")
            .operations
            .iter()
            .map(|(_, event)| event.clone())
            .collect()
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
