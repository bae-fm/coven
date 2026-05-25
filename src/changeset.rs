//! Changeset walking: the single primitive for inspecting SQLite changesets.
//!
//! coven uses it internally (to find blobs a changeset references); the host
//! uses it to map row-changes to its own domain events. It is the crate's only
//! changeset iterator.

use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;

use libsqlite3_sys as ffi;

use crate::sync::session_ext::value_to_string;

/// The operation type for a changeset entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

/// One row change extracted from a changeset.
///
/// `columns` holds the row's column values in schema order. The value chosen
/// per column follows the changeset's old/new semantics:
/// - `Insert`: the new value.
/// - `Delete`: the old value.
/// - `Update`: the old value if present, else the new value (so unchanged
///   columns — primary keys, foreign keys — are still available).
///
/// `None` means SQL NULL or a column absent from the changeset.
#[derive(Debug, Clone)]
pub struct RowChange {
    pub table: String,
    pub op: ChangeOp,
    pub columns: Vec<Option<String>>,
}

impl RowChange {
    /// The primary key (column 0).
    pub fn pk(&self) -> Option<&str> {
        self.col(0)
    }

    /// A column value by index.
    pub fn col(&self, i: usize) -> Option<&str> {
        self.columns.get(i).and_then(|c| c.as_deref())
    }
}

/// Walk a changeset and return every row change with its column values.
///
/// Returns an empty vec for an empty changeset.
pub fn walk(changeset_bytes: &[u8]) -> Result<Vec<RowChange>, String> {
    if changeset_bytes.is_empty() {
        return Ok(Vec::new());
    }

    let mut changes = Vec::new();

    unsafe {
        let mut iter: *mut ffi::sqlite3_changeset_iter = ptr::null_mut();
        let rc = ffi::sqlite3changeset_start(
            &mut iter,
            changeset_bytes.len() as c_int,
            changeset_bytes.as_ptr() as *mut c_void,
        );
        if rc != ffi::SQLITE_OK as c_int {
            return Err(format!("sqlite3changeset_start failed (rc={rc})"));
        }

        loop {
            let step = ffi::sqlite3changeset_next(iter);
            if step == ffi::SQLITE_DONE as c_int {
                break;
            }
            if step != ffi::SQLITE_ROW as c_int {
                ffi::sqlite3changeset_finalize(iter);
                return Err(format!("sqlite3changeset_next failed (rc={step})"));
            }

            let mut table: *const c_char = ptr::null();
            let mut ncol: c_int = 0;
            let mut op: c_int = 0;
            let mut indirect: c_int = 0;
            ffi::sqlite3changeset_op(iter, &mut table, &mut ncol, &mut op, &mut indirect);

            let table_name = CStr::from_ptr(table)
                .to_str()
                .expect("SQLite table names are always UTF-8")
                .to_string();

            let change_op = match op {
                ffi::SQLITE_INSERT => ChangeOp::Insert,
                ffi::SQLITE_UPDATE => ChangeOp::Update,
                ffi::SQLITE_DELETE => ChangeOp::Delete,
                _ => continue,
            };

            let columns = (0..ncol)
                .map(|c| extract_col(iter, c, change_op))
                .collect();

            changes.push(RowChange {
                table: table_name,
                op: change_op,
                columns,
            });
        }

        let rc = ffi::sqlite3changeset_finalize(iter);
        if rc != ffi::SQLITE_OK as c_int {
            return Err(format!("sqlite3changeset_finalize failed (rc={rc})"));
        }
    }

    Ok(changes)
}

/// Extract a column value following the op's old/new semantics.
unsafe fn extract_col(
    iter: *mut ffi::sqlite3_changeset_iter,
    col: c_int,
    op: ChangeOp,
) -> Option<String> {
    match op {
        ChangeOp::Insert => extract_new_value(iter, col),
        ChangeOp::Delete => extract_old_value(iter, col),
        ChangeOp::Update => extract_old_value(iter, col).or_else(|| extract_new_value(iter, col)),
    }
}

unsafe fn extract_new_value(iter: *mut ffi::sqlite3_changeset_iter, col: c_int) -> Option<String> {
    let mut val: *mut ffi::sqlite3_value = ptr::null_mut();
    let rc = ffi::sqlite3changeset_new(iter, col, &mut val);
    if rc != ffi::SQLITE_OK as c_int || val.is_null() {
        return None;
    }
    value_to_string(val)
}

unsafe fn extract_old_value(iter: *mut ffi::sqlite3_changeset_iter, col: c_int) -> Option<String> {
    let mut val: *mut ffi::sqlite3_value = ptr::null_mut();
    let rc = ffi::sqlite3changeset_old(iter, col, &mut val);
    if rc != ffi::SQLITE_OK as c_int || val.is_null() {
        return None;
    }
    value_to_string(val)
}
