//! Raw SQLite session/changeset FFI for the gate: value readers, the changegroup
//! wrapper, the changeset-iterator walk, and the [`ChangeRow`] a walk yields.

use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

use rusqlite::ffi;
use tracing::{debug, warn};

use super::model::truthy;
use super::GateError;

/// Read a changeset/sqlite value as a text string, or `None` for NULL. Mirrors
/// `sqlite3_value_text` so gate reads match the rest of the engine.
unsafe fn value_to_string(val: *mut ffi::sqlite3_value) -> Option<String> {
    let vtype = ffi::sqlite3_value_type(val);
    if vtype == ffi::SQLITE_NULL as c_int {
        return None;
    }
    let text = ffi::sqlite3_value_text(val);
    if text.is_null() {
        return None;
    }
    Some(
        CStr::from_ptr(text as *const c_char)
            .to_string_lossy()
            .into_owned(),
    )
}

/// The new value at `col` for the change at the iterator's current position
/// (`None` if absent — e.g. an unchanged column in an update — or NULL).
unsafe fn extract_new_value(iter: *mut ffi::sqlite3_changeset_iter, col: c_int) -> Option<String> {
    extract_value(iter, col, ffi::sqlite3changeset_new)
}

/// The old value at `col` for the change at the iterator's current position
/// (`None` if absent — e.g. an unchanged column in an update — or NULL).
unsafe fn extract_old_value(iter: *mut ffi::sqlite3_changeset_iter, col: c_int) -> Option<String> {
    extract_value(iter, col, ffi::sqlite3changeset_old)
}

type ChangesetValueReader = unsafe extern "C" fn(
    *mut ffi::sqlite3_changeset_iter,
    c_int,
    *mut *mut ffi::sqlite3_value,
) -> c_int;

unsafe fn extract_value(
    iter: *mut ffi::sqlite3_changeset_iter,
    col: c_int,
    read_value: ChangesetValueReader,
) -> Option<String> {
    let mut val: *mut ffi::sqlite3_value = ptr::null_mut();
    let rc = read_value(iter, col, &mut val);
    if rc != ffi::SQLITE_OK as c_int || val.is_null() {
        return None;
    }
    value_to_string(val)
}

/// A changegroup: accumulates changes (by iterator position or whole changeset)
/// and concatenates/dedups them into one output changeset.
pub(super) struct Changegroup {
    raw: *mut ffi::sqlite3_changegroup,
}

impl Changegroup {
    pub(super) fn new() -> Result<Self, GateError> {
        let mut raw: *mut ffi::sqlite3_changegroup = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3changegroup_new(&mut raw) };
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_new", rc));
        }
        Ok(Changegroup { raw })
    }

    /// Tell the changegroup the schema of `db` so it can dedup rows by primary
    /// key across `add_change` calls from differently-sourced changesets.
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection.
    pub(super) unsafe fn set_schema(&self, db: *mut ffi::sqlite3) -> Result<(), GateError> {
        let main = CString::new("main").unwrap();
        let rc = ffi::sqlite3changegroup_schema(self.raw, db, main.as_ptr());
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_schema", rc));
        }
        Ok(())
    }

    /// Append the change at the iterator's current position.
    ///
    /// # Safety
    /// `iter` must point at a valid current change (a `SQLITE_ROW` step).
    pub(super) unsafe fn add_change(
        &self,
        iter: *mut ffi::sqlite3_changeset_iter,
    ) -> Result<(), GateError> {
        let rc = ffi::sqlite3changegroup_add_change(self.raw, iter);
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_add_change", rc));
        }
        Ok(())
    }

    /// Append a complete changeset.
    pub(super) fn add_changeset(&self, changeset: &[u8]) -> Result<(), GateError> {
        let rc = unsafe {
            ffi::sqlite3changegroup_add(
                self.raw,
                changeset.len() as c_int,
                changeset.as_ptr() as *mut c_void,
            )
        };
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_add", rc));
        }
        Ok(())
    }

    /// Concatenate everything added so far into one changeset's bytes.
    pub(super) fn output(&self) -> Result<Vec<u8>, GateError> {
        let mut len: c_int = 0;
        let mut buf: *mut c_void = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3changegroup_output(self.raw, &mut len, &mut buf) };
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_output", rc));
        }
        Ok(unsafe { copy_sqlite_bytes_and_free(buf, len) })
    }
}

impl Drop for Changegroup {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3changegroup_delete(self.raw) };
    }
}

/// SQLite hands session/changegroup output back in sqlite3-managed memory; copy
/// the bytes before freeing the buffer.
unsafe fn copy_sqlite_bytes_and_free(buf: *mut c_void, len: c_int) -> Vec<u8> {
    let bytes = if buf.is_null() || len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(buf as *const u8, len as usize).to_vec()
    };
    if !buf.is_null() {
        ffi::sqlite3_free(buf);
    }
    bytes
}
/// Walk `changeset`, reading each change as a [`ChangeRow`] and handing it — with
/// the live iterator, which the caller needs for `add_change` — to `f`. Owns the
/// `start`/`next`/`finalize` FFI boilerplate so each caller writes only its
/// per-row action; `f` returning `Ok(())` early is this walk's "skip this row".
/// A finalize failure surfaces only when the walk itself succeeded — a walk error
/// is the more specific cause and takes precedence.
pub(super) unsafe fn for_each_change(
    changeset: &[u8],
    mut f: impl FnMut(*mut ffi::sqlite3_changeset_iter, ChangeRow) -> Result<(), GateError>,
) -> Result<(), GateError> {
    if changeset.is_empty() {
        return Ok(());
    }
    let mut iter: *mut ffi::sqlite3_changeset_iter = ptr::null_mut();
    let rc = ffi::sqlite3changeset_start(
        &mut iter,
        changeset.len() as c_int,
        changeset.as_ptr() as *mut c_void,
    );
    if rc != ffi::SQLITE_OK as c_int {
        return Err(GateError::Ffi("sqlite3changeset_start", rc));
    }
    let walk = loop {
        let step = ffi::sqlite3changeset_next(iter);
        if step == ffi::SQLITE_DONE as c_int {
            break Ok(());
        }
        if step != ffi::SQLITE_ROW as c_int {
            break Err(GateError::Ffi("sqlite3changeset_next", step));
        }
        let row = ChangeRow::read(iter);
        if let Err(e) = f(iter, row) {
            break Err(e);
        }
    };
    let fin = ffi::sqlite3changeset_finalize(iter);
    match walk {
        // Clean walk, failed finalize: the finalize failure is the cycle's outcome.
        Ok(()) if fin != ffi::SQLITE_OK as c_int => {
            Err(GateError::Ffi("sqlite3changeset_finalize", fin))
        }
        Ok(()) => Ok(()),
        // The walk already failed (the more specific cause): return it, but don't
        // swallow a finalize failure silently — log it alongside.
        Err(e) => {
            if fin != ffi::SQLITE_OK as c_int {
                warn!(
                    rc = fin,
                    "gate: changeset finalize failed after a walk error"
                );
            }
            Err(e)
        }
    }
}

/// Every DELETE in the changeset, keyed by `(table, primary key)`, holding the
/// row's old column values. `was_shared` (in the outbound pass) reads these to
/// resolve a deleted row's
/// pre-deletion gate state — its gate terminus is gone from the live db, so the
/// old values in the changeset are the only record that it was shared.
pub(super) unsafe fn collect_deletes(
    changeset: &[u8],
) -> Result<HashMap<(String, String), ChangeRow>, GateError> {
    let mut deleted = HashMap::new();
    for_each_change(changeset, |_iter, row| {
        if row.op == ffi::SQLITE_DELETE {
            match row.pk() {
                Some(pk) => {
                    deleted.insert((row.table.clone(), pk.to_string()), row);
                }
                None => debug!(
                    table = %row.table,
                    "gate: delete row has no primary key; not tracked for pre-delete resolution"
                ),
            }
        }
        Ok(())
    })?;
    Ok(deleted)
}
// ---- one change extracted from a changeset iterator ------------------------

/// A single change at a changeset iterator's current position, with its table,
/// op, and the columns needed for gating. We read columns eagerly so the
/// iterator can advance.
pub(super) struct ChangeRow {
    pub(super) table: String,
    pub(super) op: c_int,
    /// New values (insert/update); `None` per column = absent or NULL.
    pub(super) new: Vec<Option<String>>,
    /// Old values (delete/update); `None` per column = absent or NULL.
    pub(super) old: Vec<Option<String>>,
}

impl ChangeRow {
    /// Read the current change. Does not advance the iterator.
    pub(super) unsafe fn read(iter: *mut ffi::sqlite3_changeset_iter) -> Self {
        let mut table_ptr: *const c_char = ptr::null();
        let mut ncol: c_int = 0;
        let mut op: c_int = 0;
        let mut indirect: c_int = 0;
        ffi::sqlite3changeset_op(iter, &mut table_ptr, &mut ncol, &mut op, &mut indirect);
        let table = CStr::from_ptr(table_ptr)
            .to_str()
            .expect("SQLite table names are always UTF-8")
            .to_string();

        let mut new = Vec::with_capacity(ncol as usize);
        let mut old = Vec::with_capacity(ncol as usize);
        for c in 0..ncol {
            new.push(extract_new_value(iter, c));
            old.push(extract_old_value(iter, c));
        }
        ChangeRow {
            table,
            op,
            new,
            old,
        }
    }

    /// Primary key (column 0), following op semantics.
    pub(super) fn pk(&self) -> Option<&str> {
        match self.op {
            x if x == ffi::SQLITE_DELETE => self.old.first().and_then(|v| v.as_deref()),
            _ => self
                .new
                .first()
                .and_then(|v| v.as_deref())
                .or_else(|| self.old.first().and_then(|v| v.as_deref())),
        }
    }

    /// The FK value at `col`, following op semantics (new for insert/update,
    /// old for delete). `None` if absent (e.g. unchanged in an update).
    pub(super) fn fk_value(&self, col: usize) -> Option<&str> {
        match self.op {
            x if x == ffi::SQLITE_DELETE => self.old.get(col).and_then(|v| v.as_deref()),
            _ => self
                .new
                .get(col)
                .and_then(|v| v.as_deref())
                .or_else(|| self.old.get(col).and_then(|v| v.as_deref())),
        }
    }

    pub(super) fn new_truth(&self, col: usize) -> Option<bool> {
        self.new.get(col).and_then(|v| v.as_deref()).map(truthy)
    }

    pub(super) fn old_truth(&self, col: usize) -> Option<bool> {
        self.old.get(col).and_then(|v| v.as_deref()).map(truthy)
    }

    /// Effective gate truth for the row, following op semantics. For an update
    /// where the gate column is unchanged, the changeset omits it from both
    /// old and new; we treat absence as "unknown" → caller resolves from db.
    pub(super) fn effective_truth(&self, col: usize) -> Option<bool> {
        match self.op {
            x if x == ffi::SQLITE_DELETE => self.old_truth(col),
            _ => self.new_truth(col).or_else(|| self.old_truth(col)),
        }
    }
}
