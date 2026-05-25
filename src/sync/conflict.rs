/// Production conflict handler for changeset application.
///
/// Uses row-level Last-Writer-Wins (LWW) based on the `_updated_at` column,
/// which contains HLC timestamps that sort lexicographically = causally.
///
/// The `_updated_at` column index is looked up dynamically from the schema
/// (via `TableSchema`) so adding columns to the end of a table is safe.
///
/// For `release_files`, the `encryption_nonce` column is device-specific.
/// When incoming wins a DATA conflict on that table, the row ID is recorded
/// so the caller can restore the local value afterward.
use std::collections::HashMap;
use std::ffi::{c_char, c_int, CStr, CString};
use std::ptr;

use libsqlite3_sys as ffi;
use tracing::warn;

use super::session_ext::{ConflictAction, ConflictContext, ConflictType};

/// Column indices for a synced table, looked up from `PRAGMA table_info`.
pub struct TableColumns {
    /// Index of the `_updated_at` column.
    pub updated_at: usize,
}

/// Schema info for all synced tables: maps table name to column indices.
pub struct TableSchema {
    tables: HashMap<String, TableColumns>,
}

impl TableSchema {
    /// Build schema info by querying `PRAGMA table_info` for each synced table.
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection pointer.
    pub unsafe fn from_db(db: *mut ffi::sqlite3, synced_tables: &[&str]) -> Self {
        let mut tables = HashMap::new();

        for &table in synced_tables {
            let sql = format!("PRAGMA table_info({table})");
            let c_sql = CString::new(sql).unwrap();
            let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
            let rc = ffi::sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
            assert_eq!(
                rc,
                ffi::SQLITE_OK as c_int,
                "PRAGMA table_info failed for {table}"
            );

            let mut updated_at = None;

            while ffi::sqlite3_step(stmt) == ffi::SQLITE_ROW as c_int {
                let col_index = ffi::sqlite3_column_int(stmt, 0) as usize;
                let name_ptr = ffi::sqlite3_column_text(stmt, 1);
                if name_ptr.is_null() {
                    continue;
                }
                let name = CStr::from_ptr(name_ptr as *const c_char)
                    .to_str()
                    .expect("SQLite column names are always UTF-8");

                if name == "_updated_at" {
                    updated_at = Some(col_index);
                }
            }

            ffi::sqlite3_finalize(stmt);

            let updated_at = updated_at.unwrap_or_else(|| {
                panic!("synced table {table} has no _updated_at column");
            });

            tables.insert(table.to_string(), TableColumns { updated_at });
        }

        TableSchema { tables }
    }

    /// Look up column info for a table. Panics if the table was not in the
    /// synced tables list passed to `from_db`.
    pub fn get(&self, table: &str) -> &TableColumns {
        self.tables.get(table).unwrap_or_else(|| {
            panic!("unknown synced table in conflict handler: {table}");
        })
    }
}

/// Tracks state across conflict handler invocations within a single apply.
#[derive(Default)]
pub struct ConflictTracker {
    /// True if any FK constraint violations were reported.
    pub had_constraint_conflict: bool,
    /// Row IDs in `release_files` where incoming won a DATA conflict and
    /// device-specific columns need to be restored afterward.
    pub release_file_restore_ids: Vec<String>,
}

impl ConflictTracker {
    pub fn new() -> Self {
        Self::default()
    }
}

/// The production conflict handler for `apply_changeset_with_context`.
///
/// Rules:
/// - **DATA** (same row, both sides edited): compare `_updated_at`. Newer wins.
///   For `release_files`, records the row ID so device-specific columns can
///   be restored by the caller.
/// - **NOTFOUND** (row deleted locally, incoming UPDATE): OMIT (delete wins).
/// - **CONFLICT** (row exists, incoming INSERT): compare `_updated_at`. Newer wins.
/// - **CONSTRAINT** (FK violation): OMIT and track for retry.
/// - **FOREIGN_KEY**: OMIT (deferred FK check failure, handled by retry).
pub fn lww_conflict_handler(
    conflict_type: ConflictType,
    ctx: &ConflictContext,
    schema: &TableSchema,
    tracker: &mut ConflictTracker,
) -> ConflictAction {
    match conflict_type {
        ConflictType::Data => {
            let table = ctx.table_name();
            let cols = schema.get(table);

            let incoming = ctx.new_value(cols.updated_at);
            let local = ctx.conflict_value(cols.updated_at);

            match (incoming.as_deref(), local.as_deref()) {
                (Some(inc), Some(loc)) if inc > loc => {
                    // Incoming wins. For release_files, record the row ID
                    // so device-specific columns can be restored after apply.
                    if table == "release_files" {
                        if let Some(row_id) = ctx.conflict_value(0) {
                            tracker.release_file_restore_ids.push(row_id);
                        }
                    }

                    ConflictAction::Replace
                }
                (Some(_), Some(_)) => ConflictAction::Omit,
                _ => {
                    warn!(
                        table,
                        "DATA conflict without _updated_at values, keeping local"
                    );
                    ConflictAction::Omit
                }
            }
        }

        ConflictType::NotFound => {
            // Row was deleted locally, incoming changeset has an UPDATE.
            // Delete wins.
            ConflictAction::Omit
        }

        ConflictType::Conflict => {
            // Row already exists locally but incoming changeset has an INSERT
            // (duplicate PK). Compare _updated_at to decide which version wins.
            let table = ctx.table_name();
            let cols = schema.get(table);

            let incoming = ctx.new_value(cols.updated_at);
            let local = ctx.conflict_value(cols.updated_at);

            match (incoming.as_deref(), local.as_deref()) {
                (Some(inc), Some(loc)) if inc > loc => ConflictAction::Replace,
                (Some(_), Some(_)) => ConflictAction::Omit,
                _ => {
                    warn!(table, "CONFLICT without _updated_at values, keeping local");
                    ConflictAction::Omit
                }
            }
        }

        ConflictType::Constraint => {
            tracker.had_constraint_conflict = true;
            ConflictAction::Omit
        }

        ConflictType::ForeignKey => {
            tracker.had_constraint_conflict = true;
            ConflictAction::Omit
        }
    }
}
