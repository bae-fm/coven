//! Transaction observation through SQLite sessions.

use super::{DatabaseConnection, WriterObservation};
use crate::{observation::RowChange, DbError};
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

impl<'a> ChangeCapture<'a> {
    pub(super) fn begin(
        database: &'a DatabaseConnection,
        observation: &'a WriterObservation,
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
            hook_changes: Arc::new(Mutex::new(BTreeSet::new())),
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
        // SAFETY: null attaches every current and subsequently created main table.
        sqlite_ok(unsafe { rusqlite::ffi::sqlite3session_attach(session, std::ptr::null()) })?;
        if !observation.supplemental.is_empty() {
            // A session's pre-update hook disables SQLite's truncate-delete
            // optimization, so even DELETE without WHERE reaches this hook.
            let tables = Arc::clone(&observation.supplemental);
            let changes = Arc::clone(&capture.hook_changes);
            database
                .connection
                .update_hook(Some(move |_, schema: &str, table: &str, _| {
                    if schema == "main" {
                        let table = table.to_ascii_lowercase();
                        if tables.contains_key(&table) {
                            changes
                                .lock()
                                .expect("update hook capture lock poisoned")
                                .insert(table);
                        }
                    }
                }))?;
        }
        Ok(capture)
    }

    pub(super) fn take_changes(&mut self) -> Result<Vec<RowChange>, DbError> {
        self.database.require_transaction()?;
        let _scope = self.database.authorization.internal();
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
        let mut changes = crate::change_capture::changes(self.database, &copied)?;
        for table in std::mem::take(
            &mut *self
                .hook_changes
                .lock()
                .expect("update hook capture lock poisoned"),
        ) {
            if let Some(parent) = &self.supplemental[&table] {
                changes.push(RowChange {
                    table: parent.clone(),
                    column: String::new(),
                    keys: None,
                });
            }
            changes.push(RowChange {
                table,
                column: String::new(),
                keys: None,
            });
        }
        Ok(changes)
    }
}

impl Drop for ChangeCapture<'_> {
    fn drop(&mut self) {
        self.database
            .connection
            .update_hook(None::<fn(rusqlite::hooks::Action, &str, &str, i64)>)
            .expect("remove transaction update hook");
        // SAFETY: the guard owns this session and still borrows its live connection.
        unsafe { rusqlite::ffi::sqlite3session_delete(self.session) };
    }
}

fn sqlite_ok(code: std::ffi::c_int) -> Result<(), DbError> {
    if code == rusqlite::ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None).into())
    }
}
