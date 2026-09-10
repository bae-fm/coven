//! Raw SQLite session/changeset FFI for the gate: value readers, the changegroup
//! wrapper, the changeset-iterator walk, and the [`ChangeRow`] a walk yields.

use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

use rusqlite::ffi;
use rusqlite::types::{Value, ValueRef};
use tracing::{debug, warn};

use super::model::truthy;
use super::GateError;

/// Borrow the native cell in its existing SQL type without converting it.
///
/// # Safety
/// `value` must be non-null and valid for the returned borrow. The caller must
/// consume that borrow before advancing/finalizing the iterator or converting
/// the native cell to another type.
unsafe fn value_ref<'a>(value: *mut ffi::sqlite3_value) -> Result<ValueRef<'a>, GateError> {
    Ok(match ffi::sqlite3_value_type(value) {
        ffi::SQLITE_NULL => ValueRef::Null,
        ffi::SQLITE_INTEGER => ValueRef::Integer(ffi::sqlite3_value_int64(value)),
        ffi::SQLITE_FLOAT => ValueRef::Real(ffi::sqlite3_value_double(value)),
        kind @ (ffi::SQLITE_TEXT | ffi::SQLITE_BLOB) => {
            let data = if kind == ffi::SQLITE_TEXT {
                ffi::sqlite3_value_text(value).cast()
            } else {
                ffi::sqlite3_value_blob(value)
            };
            if kind == ffi::SQLITE_TEXT && data.is_null() {
                return Err(GateError::Ffi("read changeset text", ffi::SQLITE_NOMEM));
            }
            let len = ffi::sqlite3_value_bytes(value) as usize;
            let bytes = if len == 0 {
                &[][..]
            } else if data.is_null() {
                return Err(GateError::Ffi("read changeset bytes", ffi::SQLITE_NOMEM));
            } else {
                std::slice::from_raw_parts(data.cast::<u8>(), len)
            };
            if kind == ffi::SQLITE_TEXT {
                ValueRef::Text(bytes)
            } else {
                ValueRef::Blob(bytes)
            }
        }
        _ => return Err(GateError::Ffi("read changeset type", ffi::SQLITE_MISMATCH)),
    })
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
) -> Result<(bool, Option<String>), GateError> {
    let mut val: *mut ffi::sqlite3_value = ptr::null_mut();
    let rc = read_value(iter, col, &mut val);
    if rc != ffi::SQLITE_OK as c_int {
        return Err(GateError::Ffi("read gate cell", rc));
    }
    if val.is_null() {
        return Ok((false, None));
    }
    Ok((true, crate::value_ref_to_string(value_ref(val)?)))
}

/// A changegroup: accumulates changes by iterator position and deduplicates them
/// into one output changeset.
pub(crate) struct Changegroup {
    raw: *mut ffi::sqlite3_changegroup,
}

impl Changegroup {
    pub(crate) fn new() -> Result<Self, GateError> {
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
    /// `db` must remain a valid, open sqlite3 connection until this group is
    /// dropped. SQLite retains it to resolve table schemas during later adds.
    pub(crate) unsafe fn set_schema(&self, db: *mut ffi::sqlite3) -> Result<(), GateError> {
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
    pub(crate) unsafe fn add_change(
        &self,
        iter: *mut ffi::sqlite3_changeset_iter,
    ) -> Result<(), GateError> {
        let rc = ffi::sqlite3changegroup_add_change(self.raw, iter);
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_add_change", rc));
        }
        Ok(())
    }

    /// Append a complete changeset without decoding its rows in Rust.
    pub(crate) fn add_changeset(&self, bytes: &[u8]) -> Result<(), GateError> {
        let length = c_int::try_from(bytes.len())
            .map_err(|_| GateError::Ffi("append changeset", ffi::SQLITE_TOOBIG))?;
        let rc = unsafe {
            ffi::sqlite3changegroup_add(self.raw, length, bytes.as_ptr().cast_mut().cast())
        };
        if rc != ffi::SQLITE_OK {
            return Err(GateError::Ffi("sqlite3changegroup_add", rc));
        }
        Ok(())
    }

    /// Encode one UPDATE against the schema configured on this group. `None`
    /// omits a cell; `Some(Value::Null)` records SQL NULL. Non-primary-key cells
    /// must appear on both sides, including equal values that belong to an
    /// atomic group of columns. Primary keys appear only on the old side.
    ///
    /// Add each row once when equal cells must survive: SQLite concatenation of
    /// two updates to the same row removes cells with equal final old/new values.
    pub(crate) fn add_update(
        &self,
        table: &str,
        old: &[Option<Value>],
        new: &[Option<Value>],
        indirect: bool,
    ) -> Result<(), GateError> {
        if old.len() != new.len() || c_int::try_from(old.len()).is_err() {
            return Err(GateError::Ffi("encode UPDATE columns", ffi::SQLITE_RANGE));
        }
        let table = CString::new(table)
            .map_err(|_| GateError::Ffi("encode UPDATE table name", ffi::SQLITE_MISUSE))?;
        let mut message = ptr::null_mut();
        // SQLite copies the table name and each value before the call returns;
        // the group owns its native storage for the entire operation.
        let rc = unsafe {
            ffi::sqlite3changegroup_change_begin(
                self.raw,
                ffi::SQLITE_UPDATE,
                table.as_ptr(),
                c_int::from(indirect),
                &mut message,
            )
        };
        change_result("begin typed UPDATE", rc, message)?;
        let values = (|| {
            for (is_new, cells) in [(false, old), (true, new)] {
                for (index, cell) in cells.iter().enumerate() {
                    if let Some(value) = cell {
                        self.add_value(is_new, index as c_int, value)?;
                    }
                }
            }
            Ok(())
        })();
        let mut message = ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3changegroup_change_finish(
                self.raw,
                c_int::from(values.is_err()),
                &mut message,
            )
        };
        let finished = change_result("finish typed UPDATE", rc, message);
        match (values, finished) {
            (Ok(()), result) => result,
            (Err(operation), Ok(())) => Err(operation),
            (Err(operation), Err(cleanup)) => Err(GateError::Cleanup {
                operation: Box::new(operation),
                cleanup: Box::new(cleanup),
            }),
        }
    }

    fn add_value(&self, is_new: bool, column: c_int, value: &Value) -> Result<(), GateError> {
        let is_new = c_int::from(is_new);
        let length = |len| {
            c_int::try_from(len)
                .map_err(|_| GateError::Ffi("encode UPDATE value", ffi::SQLITE_TOOBIG))
        };
        let rc = unsafe {
            match value {
                Value::Null => ffi::sqlite3changegroup_change_null(self.raw, is_new, column),
                Value::Integer(value) => {
                    ffi::sqlite3changegroup_change_int64(self.raw, is_new, column, *value)
                }
                Value::Real(value) => {
                    ffi::sqlite3changegroup_change_double(self.raw, is_new, column, *value)
                }
                Value::Text(value) => ffi::sqlite3changegroup_change_text(
                    self.raw,
                    is_new,
                    column,
                    value.as_ptr().cast(),
                    length(value.len())?,
                ),
                Value::Blob(value) => ffi::sqlite3changegroup_change_blob(
                    self.raw,
                    is_new,
                    column,
                    value.as_ptr().cast(),
                    length(value.len())?,
                ),
            }
        };
        if rc != ffi::SQLITE_OK {
            return Err(GateError::Ffi("encode UPDATE value", rc));
        }
        Ok(())
    }

    /// Concatenate everything added so far into one changeset's bytes.
    pub(crate) fn output(&self) -> Result<Vec<u8>, GateError> {
        let mut len: c_int = 0;
        let mut buf: *mut c_void = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3changegroup_output(self.raw, &mut len, &mut buf) };
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_output", rc));
        }
        Ok(unsafe { copy_sqlite_bytes_and_free(buf, len) })
    }
}

fn change_result(operation: &str, rc: c_int, message: *mut c_char) -> Result<(), GateError> {
    let message = if message.is_null() {
        None
    } else {
        // Both callers pass the buffer returned by SQLite's typed change API.
        let text = unsafe { CStr::from_ptr(message).to_string_lossy().into_owned() };
        unsafe { ffi::sqlite3_free(message.cast()) };
        Some(text)
    };
    if rc == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(GateError::Session {
            operation: operation.to_string(),
            source: rusqlite::Error::SqliteFailure(ffi::Error::new(rc), message),
        })
    }
}

impl Drop for Changegroup {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3changegroup_delete(self.raw) };
    }
}

/// The paired typed cells and indirect marker of an UPDATE iterator position.
pub(crate) type UpdateValues = (Vec<Option<Value>>, Vec<Option<Value>>, bool);

/// Read an UPDATE without converting its integer, real or blob cells to text.
///
/// # Safety
/// `iter` must be a live changeset iterator positioned on a row, not a
/// foreign-key conflict iterator. The returned values own their bytes.
pub(crate) unsafe fn update_values(
    iter: *mut ffi::sqlite3_changeset_iter,
) -> Result<UpdateValues, GateError> {
    let mut table = ptr::null();
    let mut columns = 0;
    let mut operation = 0;
    let mut indirect = 0;
    let rc = ffi::sqlite3changeset_op(
        iter,
        &mut table,
        &mut columns,
        &mut operation,
        &mut indirect,
    );
    if rc != ffi::SQLITE_OK {
        return Err(GateError::Ffi("read UPDATE operation", rc));
    }
    if operation != ffi::SQLITE_UPDATE || columns < 0 {
        return Err(GateError::Ffi("read UPDATE values", ffi::SQLITE_MISUSE));
    }
    let read = |reader: ChangesetValueReader| {
        (0..columns)
            .map(|column| {
                let mut value = ptr::null_mut();
                let rc = reader(iter, column, &mut value);
                if rc != ffi::SQLITE_OK {
                    return Err(GateError::Ffi("read UPDATE cell", rc));
                }
                if value.is_null() {
                    return Ok(None);
                }
                let borrowed = value_ref(value)?;
                let typed = Value::try_from(borrowed).map_err(|error| GateError::Session {
                    operation: "read typed UPDATE cell".to_string(),
                    source: rusqlite::Error::FromSqlConversionFailure(
                        column as usize,
                        borrowed.data_type(),
                        Box::new(error),
                    ),
                })?;
                Ok(Some(typed))
            })
            .collect::<Result<Vec<_>, GateError>>()
    };
    Ok((
        read(ffi::sqlite3changeset_old)?,
        read(ffi::sqlite3changeset_new)?,
        indirect != 0,
    ))
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
pub(crate) unsafe fn for_each_change(
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
        let row = match ChangeRow::read(iter) {
            Ok(row) => row,
            Err(error) => break Err(error),
        };
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
/// row's old column values. `deleted_row_audience` reads these to resolve a
/// deleted row's pre-deletion audience — its gate terminus is gone from the live
/// db, so the old values in the changeset are the only record of what it was.
pub(crate) unsafe fn collect_deletes(
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
#[derive(Clone)]
pub(crate) struct ChangeRow {
    pub table: String,
    pub op: c_int,
    /// New values (insert/update); presence is recorded separately so `None`
    /// means SQL NULL when the corresponding presence entry is true.
    pub new: Vec<Option<String>>,
    new_present: Vec<bool>,
    /// Old values (delete/update); presence is recorded separately so `None`
    /// means SQL NULL when the corresponding presence entry is true.
    pub old: Vec<Option<String>>,
    old_present: Vec<bool>,
}

impl ChangeRow {
    /// Read the current change. Does not advance the iterator.
    pub(crate) unsafe fn read(iter: *mut ffi::sqlite3_changeset_iter) -> Result<Self, GateError> {
        let mut table_ptr: *const c_char = ptr::null();
        let mut ncol: c_int = 0;
        let mut op: c_int = 0;
        let mut indirect: c_int = 0;
        let rc = ffi::sqlite3changeset_op(iter, &mut table_ptr, &mut ncol, &mut op, &mut indirect);
        if rc != ffi::SQLITE_OK {
            return Err(GateError::Ffi("read gate operation", rc));
        }
        let table = CStr::from_ptr(table_ptr)
            .to_str()
            .expect("SQLite table names are always UTF-8")
            .to_string();

        let mut new = Vec::with_capacity(ncol as usize);
        let mut new_present = Vec::with_capacity(ncol as usize);
        let mut old = Vec::with_capacity(ncol as usize);
        let mut old_present = Vec::with_capacity(ncol as usize);
        for c in 0..ncol {
            let (present, value) = if op == ffi::SQLITE_DELETE {
                (false, None)
            } else {
                extract_value(iter, c, ffi::sqlite3changeset_new)?
            };
            new_present.push(present);
            new.push(value);
            let (present, value) = if op == ffi::SQLITE_INSERT {
                (false, None)
            } else {
                extract_value(iter, c, ffi::sqlite3changeset_old)?
            };
            old_present.push(present);
            old.push(value);
        }
        Ok(ChangeRow {
            table,
            op,
            new,
            new_present,
            old,
            old_present,
        })
    }

    pub(crate) fn new_value(&self, col: usize) -> Option<Option<&str>> {
        self.new_present.get(col).and_then(|present| {
            present.then(|| self.new.get(col).and_then(|value| value.as_deref()))
        })
    }

    pub(crate) fn old_value(&self, col: usize) -> Option<Option<&str>> {
        self.old_present.get(col).and_then(|present| {
            present.then(|| self.old.get(col).and_then(|value| value.as_deref()))
        })
    }

    /// Primary key (column 0), following op semantics.
    pub(crate) fn pk(&self) -> Option<&str> {
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
    pub(crate) fn fk_value(&self, col: usize) -> Option<&str> {
        match self.op {
            x if x == ffi::SQLITE_DELETE => self.old.get(col).and_then(|v| v.as_deref()),
            _ => self
                .new
                .get(col)
                .and_then(|v| v.as_deref())
                .or_else(|| self.old.get(col).and_then(|v| v.as_deref())),
        }
    }

    pub(crate) fn new_truth(&self, col: usize) -> Option<bool> {
        self.new.get(col).and_then(|v| v.as_deref()).map(truthy)
    }

    pub(crate) fn old_truth(&self, col: usize) -> Option<bool> {
        self.old.get(col).and_then(|v| v.as_deref()).map(truthy)
    }

    /// Effective gate truth for the row, following op semantics. For an update
    /// where the gate column is unchanged, the changeset omits it from both
    /// old and new; we treat absence as "unknown" → caller resolves from db.
    pub(crate) fn effective_truth(&self, col: usize) -> Option<bool> {
        match self.op {
            x if x == ffi::SQLITE_DELETE => self.old_truth(col),
            _ => self.new_truth(col).or_else(|| self.old_truth(col)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn typed_update_preserves_equal_cells_through_partition_and_apply() {
        let connection = Connection::open_in_memory().expect("open typed changeset database");
        connection.execute_batch(
            "CREATE TABLE rows (id TEXT PRIMARY KEY, text_value TEXT, size INTEGER, real_value REAL, data BLOB, absent TEXT);
             INSERT INTO rows VALUES ('row', 'before', 4, 1.5, X'0001FF', NULL);",
        ).expect("create typed row");
        let group = Changegroup::new().expect("create typed group");
        unsafe { group.set_schema(connection.handle()) }.expect("configure schema");
        let old = vec![
            Some(Value::Text("row".into())),
            Some(Value::Text("before".into())),
            Some(Value::Integer(4)),
            Some(Value::Real(1.5)),
            Some(Value::Blob(vec![0, 1, 255])),
            Some(Value::Null),
        ];
        let mut new = old.clone();
        new[0] = None;
        new[1] = Some(Value::Text("after\0text".into()));
        group
            .add_update("rows", &old, &new, true)
            .expect("encode equal cells");
        let captured = group.output().expect("encoded output");
        let partition = Changegroup::new().expect("create partition group");
        let mut count = 0;
        unsafe {
            for_each_change(&captured, |iter, _| {
                let (actual_old, actual_new, indirect) = update_values(iter)?;
                assert_eq!(actual_old, old);
                assert_eq!(actual_new, new);
                assert!(indirect);
                count += 1;
                partition.add_change(iter)
            })
        }
        .expect("partition typed UPDATE");
        assert_eq!(count, 1);
        let partitioned = partition.output().expect("partition output");
        assert_eq!(partitioned, captured);
        connection
            .apply_strm(&mut &partitioned[..], None::<fn(&str) -> bool>, |_, _| {
                rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT
            })
            .expect("apply paired equal cells");
        let values = connection
            .query_row("SELECT * FROM rows", [], |row| {
                (0..old.len())
                    .map(|column| row.get::<_, Value>(column))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .expect("read applied values");
        let mut expected = new;
        expected[0] = old[0].clone();
        assert_eq!(values.into_iter().map(Some).collect::<Vec<_>>(), expected);
    }

    #[test]
    fn typed_update_rejects_unpaired_cells_without_poisoning_the_group() {
        let connection = Connection::open_in_memory().expect("open typed changeset database");
        connection
            .execute_batch("CREATE TABLE rows (id TEXT PRIMARY KEY, value INTEGER)")
            .expect("create schema");
        let group = Changegroup::new().expect("create typed group");
        unsafe { group.set_schema(connection.handle()) }.expect("configure schema");
        let old = [Some(Value::Text("row".into())), Some(Value::Integer(1))];
        assert!(group
            .add_update("rows", &old, &[None, None], false)
            .is_err());
        assert!(group.output().expect("output after rejection").is_empty());
        let new = [None, Some(Value::Integer(2))];
        group
            .add_update("rows", &old, &new, false)
            .expect("encode after rejection");
        let output = group.output().expect("output after retry");
        unsafe {
            for_each_change(&output, |iter, _| {
                let (actual_old, actual_new, indirect) = update_values(iter)?;
                assert_eq!(actual_old, old);
                assert_eq!(actual_new, new);
                assert!(!indirect);
                Ok(())
            })
        }
        .expect("read retried UPDATE");
    }

    #[test]
    fn changeset_values_distinguish_unchanged_columns_from_sql_null() {
        let connection = Connection::open_in_memory().expect("open changeset database");
        connection
            .execute_batch(
                "CREATE TABLE rows (
                     id TEXT PRIMARY KEY,
                     parent_id TEXT NOT NULL,
                     nullable TEXT,
                     changed TEXT NOT NULL
                 ) STRICT;
                 INSERT INTO rows VALUES ('row', 'parent', 'present', 'before');",
            )
            .expect("create changeset row");
        let mut session = rusqlite::session::Session::new(&connection).expect("create session");
        session.attach(Some("rows")).expect("attach rows");
        connection
            .execute(
                "UPDATE rows SET nullable = NULL, changed = 'after' WHERE id = 'row'",
                [],
            )
            .expect("update row");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract changeset");

        let mut rows = Vec::new();
        unsafe {
            for_each_change(&changeset, |_iter, row| {
                rows.push(row);
                Ok(())
            })
        }
        .expect("walk changeset");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].new_value(1), None);
        assert_eq!(rows[0].new_value(2), Some(None));
        assert_eq!(rows[0].new_value(3), Some(Some("after")));
    }
}
