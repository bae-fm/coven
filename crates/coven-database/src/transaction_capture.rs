//! Transaction observation through SQLite sessions.

use super::write_transaction::WriteTransaction;
use super::{DatabaseConnection, WriterObservation};
use crate::{observation::RowChange, DbError};
use coven_foundation::locks::lock_unpoisoned;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

// rusqlite does not expose the session ROWID option. Keep raw handles here,
// inside the SQLite owner; the borrow prevents the connection outliving capture.
pub(super) struct ChangeCapture<'a> {
    session: *mut rusqlite::ffi::sqlite3_session,
    database: &'a DatabaseConnection,
    supplemental: &'a BTreeMap<String, Option<String>>,
    hook_changes: Arc<Mutex<BTreeSet<String>>>,
}

/// The supplemental tables a transaction's update hook saw change: those
/// whose rows a session can't key (`change_capture::supplemental_tables`).
/// A panic in the hook fails the transaction before it commits, so the
/// lock is taken as it is.
pub(super) struct SupplementalChanges(Arc<Mutex<BTreeSet<String>>>);

impl SupplementalChanges {
    /// Watch `transaction`'s supplemental tables, which only a serving
    /// writer knows; none are watched where there are none.
    pub(super) fn observe(transaction: &mut WriteTransaction<'_>) -> Result<Self, DbError> {
        let changes = Arc::new(Mutex::new(BTreeSet::new()));
        let tables = transaction.within(|database| match &database.observation {
            Some(observation) if !observation.supplemental.is_empty() => {
                Some(Arc::clone(&observation.supplemental))
            }
            _ => None,
        });
        let Some(tables) = tables else {
            return Ok(Self(changes));
        };
        // A session's pre-update hook disables SQLite's truncate-delete
        // optimization, so even DELETE without WHERE reaches this hook.
        let seen = Arc::clone(&changes);
        transaction.observe_rows(move |_, schema: &str, table: &str, _| {
            if schema == "main" {
                let table = table.to_ascii_lowercase();
                if tables.contains_key(&table) {
                    lock_unpoisoned(&seen).insert(table);
                }
            }
        })?;
        Ok(Self(changes))
    }
}

impl<'a> ChangeCapture<'a> {
    pub(super) fn begin(
        database: &'a DatabaseConnection,
        observation: &'a WriterObservation,
        supplemental: SupplementalChanges,
    ) -> Result<Self, DbError> {
        let mut session = std::ptr::null_mut();
        // SAFETY: the borrowed owner retains its connection until this guard drops.
        sqlite_ok(unsafe {
            rusqlite::ffi::sqlite3session_create(
                database.connection.handle(),
                c"main".as_ptr(),
                &mut session,
            )
        })?;
        let capture = Self {
            session,
            database,
            supplemental: &observation.supplemental,
            hook_changes: supplemental.0,
        };
        let mut rowid: std::ffi::c_int = 1;
        // SAFETY: the live session has no attached tables yet and rowid is an int.
        sqlite_ok(unsafe {
            rusqlite::ffi::sqlite3session_object_config(
                session,
                rusqlite::ffi::SQLITE_SESSION_OBJCONFIG_ROWID,
                (&mut rowid as *mut std::ffi::c_int).cast(),
            )
        })?;
        // SAFETY: the filter is a plain function reading only its table name,
        // valid for the session's life; null attaches every current and
        // subsequently created main table the filter admits.
        unsafe {
            rusqlite::ffi::sqlite3session_table_filter(
                session,
                Some(observed_table),
                std::ptr::null_mut(),
            );
        }
        sqlite_ok(unsafe { rusqlite::ffi::sqlite3session_attach(session, std::ptr::null()) })?;
        Ok(capture)
    }

    pub(super) fn take_changes(&mut self) -> Result<Vec<RowChange>, DbError> {
        let _scope = self.database.authorization.internal();
        let copied = self.changeset()?;
        let scoped = self
            .database
            .handle
            .as_ref()
            .map(crate::observation::CommitObserver::scoped_columns)
            .unwrap_or_default();
        let mut changes = crate::change_capture::changes(self.database, &copied, &scoped)?;
        for table in std::mem::take(&mut *lock_unpoisoned(&self.hook_changes)) {
            if let Some(parent) = &self.supplemental[&table] {
                changes.push(RowChange {
                    table: parent.clone(),
                    column: String::new(),
                    rows: None,
                });
            }
            changes.push(RowChange { table, column: String::new(), rows: None });
        }
        Ok(changes)
    }
}

impl ChangeCapture<'_> {
    /// The session's changeset so far, inside the transaction it captures.
    fn changeset(&mut self) -> Result<Vec<u8>, DbError> {
        self.database.require_transaction()?;
        let mut length = 0;
        let mut bytes = std::ptr::null_mut();
        // SAFETY: this guard exclusively owns the live session; SQLite allocates
        // the result, which we copy then free even when reporting a failure.
        let result = unsafe {
            rusqlite::ffi::sqlite3session_changeset(self.session, &mut length, &mut bytes)
        };
        let copied = if result == rusqlite::ffi::SQLITE_OK && length > 0 {
            // SAFETY: successful changeset generation returned length initialized bytes.
            unsafe { std::slice::from_raw_parts(bytes.cast::<u8>(), length as usize) }.to_vec()
        } else {
            Vec::new()
        };
        // SAFETY: SQLite allocated this buffer and sqlite3_free accepts null.
        unsafe { rusqlite::ffi::sqlite3_free(bytes) };
        sqlite_ok(result)?;
        Ok(copied)
    }
}

impl Drop for ChangeCapture<'_> {
    fn drop(&mut self) {
        // SAFETY: the guard owns this session and still borrows its live connection.
        unsafe { rusqlite::ffi::sqlite3session_delete(self.session) };
    }
}

/// The session records changes only to the tables a live query can read.
unsafe extern "C" fn observed_table(
    _: *mut std::ffi::c_void,
    table: *const std::ffi::c_char,
) -> std::ffi::c_int {
    // SAFETY: SQLite passes the table's NUL-terminated name for this call.
    let table = unsafe { std::ffi::CStr::from_ptr(table) };
    table.to_str().map_or(1, |name| std::ffi::c_int::from(crate::observation::observed(name)))
}

fn sqlite_ok(code: std::ffi::c_int) -> Result<(), DbError> {
    if code == rusqlite::ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None).into())
    }
}

#[cfg(test)]
#[path = "transaction_capture_tests.rs"]
mod tests;
