use std::ffi::{c_int, c_void};
use std::ptr;

use crate::DbError;

/// SQLite's Rust wrapper exposes sessions but not the ROWID configuration.
/// This owner calls the same C API so ordinary tables without an explicit
/// primary key are represented with SQLite's synthetic `_rowid_` key.
pub(crate) struct ChangeCapture {
    session: *mut rusqlite::ffi::sqlite3_session,
    schema_version: i64,
}

impl ChangeCapture {
    pub(crate) fn begin(
        database: *mut rusqlite::ffi::sqlite3,
        schema_version: i64,
    ) -> Result<Self, DbError> {
        let mut session = ptr::null_mut();
        let main = c"main";
        // SAFETY: DatabaseCore keeps `database` alive on its connection worker
        // until this owner is finished, and `session` is deleted in Drop.
        sqlite_ok(unsafe {
            rusqlite::ffi::sqlite3session_create(database, main.as_ptr(), &mut session)
        })?;
        let mut capture_rowid: c_int = 1;
        // SAFETY: `session` was created above and this configuration is applied
        // before attaching any table, as SQLite requires.
        let configured = unsafe {
            rusqlite::ffi::sqlite3session_object_config(
                session,
                rusqlite::ffi::SQLITE_SESSION_OBJCONFIG_ROWID,
                (&mut capture_rowid as *mut c_int).cast::<c_void>(),
            )
        };
        if let Err(error) = sqlite_ok(configured) {
            // SAFETY: creation succeeded and ownership has not escaped.
            unsafe { rusqlite::ffi::sqlite3session_delete(session) };
            return Err(error);
        }
        // SAFETY: a null table name attaches every current and later table.
        let attached = unsafe { rusqlite::ffi::sqlite3session_attach(session, ptr::null()) };
        if let Err(error) = sqlite_ok(attached) {
            // SAFETY: creation succeeded and ownership has not escaped.
            unsafe { rusqlite::ffi::sqlite3session_delete(session) };
            return Err(error);
        }
        Ok(Self {
            session,
            schema_version,
        })
    }

    pub(crate) fn schema_version(&self) -> i64 {
        self.schema_version
    }

    pub(crate) fn take_changeset(&mut self) -> Result<Vec<u8>, DbError> {
        let mut length = 0;
        let mut bytes = ptr::null_mut();
        // SAFETY: this owner uniquely holds a live session and frees the
        // SQLite-allocated result after copying it.
        sqlite_ok(unsafe {
            rusqlite::ffi::sqlite3session_changeset(self.session, &mut length, &mut bytes)
        })?;
        let copied = if length == 0 {
            Vec::new()
        } else {
            // SAFETY: SQLite returned `length` initialized bytes on success.
            unsafe { std::slice::from_raw_parts(bytes.cast::<u8>(), length as usize) }.to_vec()
        };
        // SAFETY: SQLite owns this allocation and accepts null for an empty set.
        unsafe { rusqlite::ffi::sqlite3_free(bytes) };
        Ok(copied)
    }
}

impl Drop for ChangeCapture {
    fn drop(&mut self) {
        // SAFETY: this owner is the only owner of the created session.
        unsafe { rusqlite::ffi::sqlite3session_delete(self.session) };
    }
}

fn sqlite_ok(code: c_int) -> Result<(), DbError> {
    if code == rusqlite::ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            None,
        )))
    }
}
