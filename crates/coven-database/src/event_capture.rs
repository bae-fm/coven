//! A local write's app phase, observed one SQLite row operation at a time.
//!
//! Sessions record only net changes. A write's steps need each operation, in
//! order (§9.6, D5). A connection has one pre-update hook, which a session
//! would otherwise own, so the app phase records through this hook and no
//! session runs until it ends. Only a synced table's operations are kept
//! whole; a device-local table's are kept as live queries need them.

use super::pre_update_changes::{new_value, old_value};
use super::write_transaction::WriteTransaction;
use crate::sql_value::SqlValue;
use crate::DbError;
use coven_foundation::locks::lock_unpoisoned;
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
    /// virtual generated column, which keeps no value. Empty for an insert.
    pub(crate) old: Vec<Option<SqlValue>>,
    /// Every column after the operation; empty for a delete.
    pub(crate) new: Vec<Option<SqlValue>>,
    /// The rowid before and after, meaningful only for rowid tables.
    pub(crate) rowids: (i64, i64),
}

struct CapturedEvents {
    operations: Vec<(i32, Event)>,
    local: super::pre_update_changes::PreUpdateChanges,
}

/// A panic in the hook fails the transaction it observes before it commits
/// (`write_transaction.rs`), so what a panicking hook call left half-recorded
/// is never committed: the capture's lock is taken as it is.
pub(super) struct EventCapture {
    shapes: Arc<super::pre_update_changes::TableShapes>,
    events: Arc<Mutex<CapturedEvents>>,
}

impl EventCapture {
    /// Record `transaction`'s row operations until it stops observing them.
    pub(super) fn begin(transaction: &mut WriteTransaction<'_>) -> Result<Self, DbError> {
        let (tables, scoped) = transaction.within(|database| {
            let tables = Arc::clone(
                &database
                    .observation
                    .as_ref()
                    .ok_or(DbError::Invariant { what: "a local write's writer serves its handle" })?
                    .tables,
            );
            let scoped = match &database.handle {
                Some(handle) => tables.scoped(&handle.scoped_columns()),
                None => std::collections::HashMap::new(),
            };
            Ok::<_, DbError>((tables, scoped))
        })?;
        let events = Arc::new(Mutex::new(CapturedEvents {
            operations: Vec::new(),
            local: super::pre_update_changes::PreUpdateChanges::new(scoped),
        }));
        let recorded = Arc::clone(&events);
        let shapes = Arc::clone(&tables);
        transaction.observe_operations(
            move |action: Action, schema: &str, table: &str, case: &PreUpdateCase| {
                if schema != "main" {
                    return Ok(());
                }
                match tables.position(table) {
                    Some(position) if tables.synced(position) => {}
                    Some(position) => {
                        return lock_unpoisoned(&recorded).local.record(&tables, position, case);
                    }
                    None => {
                        lock_unpoisoned(&recorded).local.unshaped(table);
                        return Ok(());
                    }
                }
                let depth = match case {
                    PreUpdateCase::Insert(a) => a.get_query_depth(),
                    PreUpdateCase::Delete(a) => a.get_query_depth(),
                    PreUpdateCase::Update { new_value_accessor, .. } => {
                        new_value_accessor.get_query_depth()
                    }
                    PreUpdateCase::Unknown => return Ok(()),
                };
                let count = |columns: i32| 0..usize::try_from(columns).unwrap_or(0);
                let old = |accessor: &rusqlite::hooks::PreUpdateOldValueAccessor| {
                    count(accessor.get_column_count())
                        .map(|i| Ok(old_value(accessor, i)?.map(SqlValue::from)))
                        .collect::<Result<Vec<_>, DbError>>()
                };
                let new = |accessor: &rusqlite::hooks::PreUpdateNewValueAccessor| {
                    count(accessor.get_column_count())
                        .map(|i| Ok(new_value(accessor, i)?.map(SqlValue::from)))
                        .collect::<Result<Vec<_>, DbError>>()
                };
                let event = match case {
                    PreUpdateCase::Insert(accessor) => Event {
                        table: table.to_owned(),
                        action,
                        old: Vec::new(),
                        new: new(accessor)?,
                        rowids: (accessor.get_new_row_id(), accessor.get_new_row_id()),
                    },
                    PreUpdateCase::Delete(accessor) => Event {
                        table: table.to_owned(),
                        action,
                        old: old(accessor)?,
                        new: Vec::new(),
                        rowids: (accessor.get_old_row_id(), accessor.get_old_row_id()),
                    },
                    PreUpdateCase::Update { old_value_accessor, new_value_accessor } => Event {
                        table: table.to_owned(),
                        action,
                        old: old(old_value_accessor)?,
                        new: new(new_value_accessor)?,
                        rowids: (
                            old_value_accessor.get_old_row_id(),
                            new_value_accessor.get_new_row_id(),
                        ),
                    },
                    PreUpdateCase::Unknown => return Ok(()),
                };
                lock_unpoisoned(&recorded).operations.push((depth, event));
                Ok(())
            },
        )?;
        Ok(Self { shapes, events })
    }

    /// The device-local tables' changes, as live queries read them.
    pub(super) fn local_changes(&self) -> Vec<crate::observation::RowChange> {
        std::mem::take(&mut lock_unpoisoned(&self.events).local).finish(&self.shapes)
    }

    /// The synced tables' operations so far, in order.
    pub(super) fn events(&self) -> Vec<Event> {
        lock_unpoisoned(&self.events).operations.iter().map(|(_, event)| event.clone()).collect()
    }
}

#[cfg(test)]
#[path = "event_capture_tests.rs"]
mod tests;
