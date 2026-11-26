//! Transaction observation through SQLite sessions.

use super::HostSql;
use crate::authorization::HostError;
use crate::authorization::Policy;
use crate::authorization::PolicyError;
use crate::net_changes::Captured;
use crate::UnrecordedTables;
use std::sync::Arc;

// rusqlite does not expose the session ROWID option. Keep raw handles here,
// inside the SQLite owner; the borrow prevents the connection outliving capture.
pub(super) struct ChangeCapture<'a, P: Policy> {
    session: *mut rusqlite::ffi::sqlite3_session,
    database: &'a HostSql<P>,
    /// The tables the session doesn't record. The session's filter reads
    /// them through a pointer, so the capture keeps them until after it
    /// deletes the session.
    unrecorded: Arc<UnrecordedTables>,
}

impl<'a, P: Policy> ChangeCapture<'a, P> {
    /// Begins recording the transaction's changes. A table with no primary
    /// key refuses the capture, since a session can't tell its rows apart
    /// (`table_shapes.rs`).
    pub(super) fn begin(database: &'a HostSql<P>) -> Result<Self, PolicyError<P>> {
        database.table_shapes()?;
        let unrecorded = Arc::clone(
            database
                .handle
                .as_ref()
                .ok_or(crate::Error::Invariant { what: "a capturing connection has its handle" })?
                .unrecorded(),
        );
        let mut session = std::ptr::null_mut();
        // SAFETY: the borrowed owner retains its connection until this guard drops.
        sqlite_ok(unsafe {
            rusqlite::ffi::sqlite3session_create(
                database.connection.handle(),
                c"main".as_ptr(),
                &mut session,
            )
        })?;
        let capture = Self { session, database, unrecorded };
        // SAFETY: the filter reads only its table name and its context, the
        // tables `capture` holds, which outlive the session; null attaches
        // every current and subsequently created main table the filter admits.
        unsafe {
            rusqlite::ffi::sqlite3session_table_filter(
                session,
                Some(recorded_table),
                Arc::as_ptr(&capture.unrecorded).cast_mut().cast(),
            );
        }
        sqlite_ok(unsafe { rusqlite::ffi::sqlite3session_attach(session, std::ptr::null()) })?;
        Ok(capture)
    }

    pub(super) fn take_changes(&mut self) -> Result<Captured, PolicyError<P>> {
        let _scope = self.database.authorization.internal();
        let copied = self.changeset()?;
        let kept =
            self.database.handle.as_ref().map(crate::Handle::kept_columns).unwrap_or_default();
        let changes = crate::change_capture::changes(self.database, &copied, &kept)?;
        Ok(changes)
    }
}

impl<P: Policy> ChangeCapture<'_, P> {
    /// The session's changeset so far, inside the transaction it captures.
    fn changeset(&mut self) -> Result<Vec<u8>, PolicyError<P>> {
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

impl<P: Policy> Drop for ChangeCapture<'_, P> {
    fn drop(&mut self) {
        // SAFETY: the guard owns this session and still borrows its live connection.
        unsafe { rusqlite::ffi::sqlite3session_delete(self.session) };
    }
}

/// The session records changes only to the tables a live query can read.
unsafe extern "C" fn recorded_table(
    unrecorded: *mut std::ffi::c_void,
    table: *const std::ffi::c_char,
) -> std::ffi::c_int {
    // SAFETY: the context is the `UnrecordedTables` the capture holds, which
    // outlives its session.
    let unrecorded = unsafe { &*unrecorded.cast::<UnrecordedTables>() };
    // SAFETY: SQLite passes the table's NUL-terminated name for this call.
    let table = unsafe { std::ffi::CStr::from_ptr(table) };
    table.to_str().map_or(1, |name| std::ffi::c_int::from(unrecorded.recorded(name)))
}

fn sqlite_ok<F: HostError, R: HostError>(code: std::ffi::c_int) -> Result<(), crate::Error<F, R>> {
    if code == rusqlite::ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None).into())
    }
}

#[cfg(test)]
#[path = "transaction_capture_tests.rs"]
mod tests;
