/// Apply a changeset to a database with the production conflict handler.
///
/// Within a single changeset, SQLite defers FK checks -- parent and child
/// rows in the same changeset are applied in recording order. Cross-changeset
/// FK dependencies are handled by applying changesets in seq order (parents
/// are always in earlier changesets than children).
///
/// If a FK violation remains after applying a changeset, the conflict handler
/// reports it via `FOREIGN_KEY` type and the tracker notes it for the caller.
///
/// For `release_files` DATA conflicts where incoming wins, the local
/// `encryption_nonce` value is restored after applying because that column
/// is device-specific.
use std::collections::HashMap;
use std::ffi::{c_char, c_int, CStr, CString};
use std::ptr;

use libsqlite3_sys as ffi;

use super::conflict::{lww_conflict_handler, ConflictTracker, TableSchema};
use super::session::SyncError;
use super::session_ext::{apply_changeset_with_context, Changeset};

/// Result of applying a changeset.
pub struct ApplyResult {
    /// True if any FK constraint violations were reported. The caller may
    /// want to retry this changeset after applying other changesets that
    /// contain the missing parent rows.
    pub had_fk_violations: bool,
}

/// Snapshot of device-specific columns for a single release_files row.
struct DeviceLocalSnapshot {
    /// Local `encryption_nonce` as raw bytes (None means NULL).
    encryption_nonce: Option<Vec<u8>>,
}

/// Apply a changeset to the given database connection using LWW conflict
/// resolution.
///
/// Builds schema info from the database to look up `_updated_at` column
/// indices dynamically, so future migrations that add columns are safe.
///
/// # Safety
/// `db` must be a valid, open sqlite3 connection pointer.
pub unsafe fn apply_changeset_lww(
    db: *mut ffi::sqlite3,
    changeset: &Changeset,
) -> Result<ApplyResult, SyncError> {
    let synced = super::session::synced_tables();
    let table_refs: Vec<&str> = synced.iter().map(String::as_str).collect();
    let schema = TableSchema::from_db(db, &table_refs);
    let mut tracker = ConflictTracker::new();

    // Snapshot device-specific columns from all existing release_files rows.
    // This is read before applying so we have the original local values
    // regardless of what the changeset overwrites.
    let snapshots = snapshot_device_local_columns(db);

    apply_changeset_with_context(db, changeset, |ct, ctx| {
        lww_conflict_handler(ct, ctx, &schema, &mut tracker)
    })
    .map_err(SyncError::ChangesetApply)?;

    // Restore device-specific columns on release_files rows where incoming won.
    for row_id in &tracker.release_file_restore_ids {
        if let Some(snap) = snapshots.get(row_id.as_str()) {
            restore_device_local_columns(db, row_id, snap);
        }
    }

    Ok(ApplyResult {
        had_fk_violations: tracker.had_constraint_conflict,
    })
}

/// Read `encryption_nonce` for all existing release_files rows.
unsafe fn snapshot_device_local_columns(
    db: *mut ffi::sqlite3,
) -> HashMap<String, DeviceLocalSnapshot> {
    let mut map = HashMap::new();

    let sql = "SELECT id, encryption_nonce FROM release_files";
    let c_sql = CString::new(sql).unwrap();
    let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut());

    // Table might not exist yet (e.g. in a partial test schema).
    // In that case, just return an empty map.
    if rc != ffi::SQLITE_OK as c_int {
        return map;
    }

    while ffi::sqlite3_step(stmt) == ffi::SQLITE_ROW as c_int {
        // Column 0: id (TEXT)
        let id_ptr = ffi::sqlite3_column_text(stmt, 0);
        if id_ptr.is_null() {
            continue;
        }
        let id = CStr::from_ptr(id_ptr as *const c_char)
            .to_str()
            .expect("SQLite text columns are always UTF-8")
            .to_string();

        // Column 1: encryption_nonce (BLOB, nullable)
        let encryption_nonce = if ffi::sqlite3_column_type(stmt, 1) == ffi::SQLITE_NULL as c_int {
            None
        } else {
            let blob_ptr = ffi::sqlite3_column_blob(stmt, 1);
            let blob_len = ffi::sqlite3_column_bytes(stmt, 1) as usize;
            if blob_ptr.is_null() || blob_len == 0 {
                None
            } else {
                let slice = std::slice::from_raw_parts(blob_ptr as *const u8, blob_len);
                Some(slice.to_vec())
            }
        };

        map.insert(id, DeviceLocalSnapshot { encryption_nonce });
    }

    ffi::sqlite3_finalize(stmt);
    map
}

/// Restore local `encryption_nonce` on a release_files row after an incoming
/// changeset overwrote it.
unsafe fn restore_device_local_columns(
    db: *mut ffi::sqlite3,
    row_id: &str,
    snap: &DeviceLocalSnapshot,
) {
    let sql = "UPDATE release_files SET encryption_nonce = ?1 WHERE id = ?2";
    let c_sql = CString::new(sql).unwrap();
    let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
    assert_eq!(
        rc,
        ffi::SQLITE_OK as c_int,
        "prepare restore_device_local_columns failed"
    );

    // Bind encryption_nonce (param 1) as BLOB
    match &snap.encryption_nonce {
        Some(bytes) => {
            ffi::sqlite3_bind_blob(
                stmt,
                1,
                bytes.as_ptr() as *const _,
                bytes.len() as c_int,
                ffi::SQLITE_TRANSIENT(),
            );
        }
        None => {
            ffi::sqlite3_bind_null(stmt, 1);
        }
    }

    // Bind row id (param 2)
    let c_id = CString::new(row_id).unwrap();
    ffi::sqlite3_bind_text(
        stmt,
        2,
        c_id.as_ptr(),
        row_id.len() as c_int,
        ffi::SQLITE_TRANSIENT(),
    );

    let step = ffi::sqlite3_step(stmt);
    assert_eq!(
        step,
        ffi::SQLITE_DONE as c_int,
        "restore_device_local_columns step failed"
    );

    ffi::sqlite3_finalize(stmt);
}
