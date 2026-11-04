//! Coven's local schema. `application_id` identifies these tables independently
//! of the application's `user_version`.
//!
//! Unsigned merge counters are big-endian eight-byte blobs. SQLite's signed
//! integer range must not truncate device ids, generations or write numbers.
//! Present displayed values live in app tables; a written value that reads
//! differently (a NULL-substituted reference) is kept by cell. Generated
//! columns have no cells: SQLite computes them. Metadata and lost values use
//! coven-format's snapshot field encodings inside blobs.
//! Loss targets name their row directly: a write excluded by a schema change
//! (§18.6) need not have an accepted generation in `_coven_rows`.
//! An upload's `record` is one plaintext value: the write header frame followed
//! by its audience parts' row-frame streams. Its first attempt fixes the format
//! and key ids in `_coven_upload_keys`, encoded as the D9 write prefix. Each
//! attempted slot keeps its exact ciphertext in the claim journal before storage
//! is called. `_coven_uploads` retains its rowid for queue order and incremental
//! BLOB reads.
//! Cell keys start with the row; column-wide cell edits occur only in migrations.
//! An original change has no admission row until judged here, then this
//! device's `ChangeVerdict`. Consuming a slot does not establish admission.
//! Write-id indexes serve SQLite's incoming foreign-key checks on INSERT with
//! RETURNING, which EXPLAIN QUERY PLAN includes alongside explicit lookups.

pub(crate) const VERSION: u32 = 1;
pub(crate) const CACHE_HEADER: i64 = -1;
pub(crate) const CACHE_COMPLETE_FILE: i64 = -2;
// D2 timestamps contain six clock bytes, two counter bytes, then the device id.
pub(crate) const WRITE_DEVICE_SQL: &str = "substr(timestamp,9,8)";
/// Whether removal rules took out the merge row `r` names, through its
/// `r.table_name`, `r.key` and `r.audience`: an active rules loss holds its
/// values. An active one is always at the row's current generation
/// (format.md D7.2), so the predicate needs no generation.
pub(crate) const RULES_REMOVED_SQL: &str = "(EXISTS(SELECT 1 FROM _coven_lost AS removed
    WHERE removed.table_name=r.table_name AND removed.key=r.key AND removed.audience=r.audience
      AND removed.frozen=0 AND removed.replacement_kind='rules'))";
/// Whether the files-area path `?1` is named: by a file on this device
/// only, or as the copy of a queued upload or a pin request (Appendix C.3).
pub(crate) const FILE_NAMED_SQL: &str = "(EXISTS(SELECT 1 FROM _coven_device_files WHERE path=?1)
    OR EXISTS(SELECT 1 FROM _coven_file_uploads WHERE copy=?1)
    OR EXISTS(SELECT 1 FROM _coven_pin_requests WHERE copy=?1))";
/// Whether the cache-area path `?1` is in use: by an entry, a range being
/// written, or bytes recorded for removal (Appendix C.3).
pub(crate) const CACHE_NAME_USED_SQL: &str = "(EXISTS(SELECT 1 FROM _coven_cache WHERE path=?1)
    OR EXISTS(SELECT 1 FROM _coven_cache_writes WHERE path=?1)
    OR EXISTS(SELECT 1 FROM _coven_file_removals WHERE path=?1 AND area='cache'))";

/// App objects cannot use either reserved namespace (Appendix C.1).
pub(crate) fn reserved_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("_coven_") || name.starts_with("coven_")
}

// This is also the ownership checker's source of reserved table names.
#[macro_use]
#[path = "internal_schema_ownership.rs"]
mod ownership;
#[macro_use]
#[path = "internal_schema_sequence.rs"]
mod sequence;
// Each domain's tables, beside the domain that writes them.
#[macro_use]
#[path = "authorship_schema.rs"]
mod authorship_schema;
#[macro_use]
#[path = "captured_changes_schema.rs"]
mod captured_changes_schema;
#[macro_use]
#[path = "claim_schema.rs"]
mod claim_schema;
#[macro_use]
#[path = "database_file_retention_schema.rs"]
mod database_file_retention_schema;
#[macro_use]
#[path = "database_operations_schema.rs"]
mod database_operations_schema;
#[macro_use]
#[path = "device_reset_schema.rs"]
mod device_reset_schema;
#[macro_use]
#[path = "file_cache_schema.rs"]
mod file_cache_schema;
#[macro_use]
#[path = "file_database_copies_schema.rs"]
mod file_database_copies_schema;
#[macro_use]
#[path = "file_moves_schema.rs"]
mod file_moves_schema;
#[macro_use]
#[path = "file_pins_schema.rs"]
mod file_pins_schema;
#[macro_use]
#[path = "file_protection_schema.rs"]
mod file_protection_schema;
#[macro_use]
#[path = "file_queue_schema.rs"]
mod file_queue_schema;
#[macro_use]
#[path = "file_removals_schema.rs"]
mod file_removals_schema;
#[macro_use]
#[path = "file_table_schema.rs"]
mod file_table_schema;
#[macro_use]
#[path = "migration_receive_schema.rs"]
mod migration_receive_schema;
#[macro_use]
#[path = "migration_run_schema.rs"]
mod migration_run_schema;
#[macro_use]
#[path = "object_removals_schema.rs"]
mod object_removals_schema;
#[macro_use]
#[path = "operation_schema.rs"]
mod operation_schema;
#[macro_use]
#[path = "pending_schema.rs"]
mod pending_schema;
#[macro_use]
#[path = "provider_access_schema.rs"]
mod provider_access_schema;
#[macro_use]
#[path = "published_schema.rs"]
mod published_schema;
#[macro_use]
#[path = "raise_capture_schema.rs"]
mod raise_capture_schema;
#[macro_use]
#[path = "reload_gate_schema.rs"]
mod reload_gate_schema;
#[macro_use]
#[path = "retry_schema.rs"]
mod retry_schema;
#[macro_use]
#[path = "row_queries_schema.rs"]
mod row_queries_schema;
#[macro_use]
#[path = "sequence_apply_schema.rs"]
mod sequence_apply_schema;
#[macro_use]
#[path = "sequence_schema.rs"]
mod sequence_schema;
#[macro_use]
#[path = "source_names_schema.rs"]
mod source_names_schema;
#[macro_use]
#[path = "sql_versions_schema.rs"]
mod sql_versions_schema;
#[macro_use]
#[path = "store_state_schema.rs"]
mod store_state_schema;
#[macro_use]
#[path = "upload_schema.rs"]
mod upload_schema;
#[macro_use]
#[path = "user_file_schema.rs"]
mod user_file_schema;
#[macro_use]
#[path = "write_boundary_schema.rs"]
mod write_boundary_schema;
#[macro_use]
#[path = "write_commit_schema.rs"]
mod write_commit_schema;
#[macro_use]
#[path = "write_headers_schema.rs"]
mod write_headers_schema;
#[macro_use]
#[path = "written_values_schema.rs"]
mod written_values_schema;

macro_rules! coven_tables {
    ($visit:ident) => {
        coven_data_reloads_tables!($visit);
        coven_file_protection_tables!($visit);
        coven_pending_tables!($visit);
        coven_file_columns_tables!($visit);
        coven_device_reset_tables!($visit);
        coven_SQL_versions_tables!($visit);
        coven_store_state_tables!($visit);
        coven_write_boundaries_tables!($visit);
        coven_raises_tables!($visit);
        coven_writes_tables!($visit);
        coven_write_headers_tables!($visit);
        $visit!(_coven_authored_parts, "
            CREATE TABLE _coven_authored_parts (
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                audience TEXT NOT NULL,
                header BLOB NOT NULL,
                complete INTEGER NOT NULL CHECK(complete IN (0,1)),
                PRIMARY KEY(write_id,audience)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_authored_changes, "
            CREATE TABLE _coven_authored_changes (
                write_id BLOB NOT NULL,
                audience TEXT NOT NULL,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                record BLOB NOT NULL,
                PRIMARY KEY(write_id,audience,table_name,key),
                FOREIGN KEY(write_id,audience) REFERENCES _coven_authored_parts(write_id,audience)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_authored_changes_audience ON _coven_authored_changes(audience,write_id,table_name,key);
        ");
        // This device's verdict on an original change, beside its record for
        // the reason `_coven_upload_keys` is beside an upload's.
        $visit!(_coven_authored_admissions, "
            CREATE TABLE _coven_authored_admissions (
                write_id BLOB NOT NULL,
                audience TEXT NOT NULL,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                admission BLOB NOT NULL,
                PRIMARY KEY(write_id,audience,table_name,key),
                FOREIGN KEY(write_id,audience,table_name,key) REFERENCES _coven_authored_changes(write_id,audience,table_name,key) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        coven_ownership_tables!($visit);
        coven_published_tables!($visit);
        coven_sequence_tables!($visit);
        coven_claim_tables!($visit);
        coven_conversion_schemas_tables!($visit);
        coven_source_names_tables!($visit);
        // A synced column, or a column a breaking migration dropped, which
        // has a row only while a frozen lost value names it (§18.7): the
        // deletion or move of the last one deletes it.
        $visit!(_coven_columns, "
            CREATE TABLE _coven_columns (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                synced INTEGER NOT NULL CHECK(synced IN (0,1)),
                UNIQUE(table_name, column_name)
            ) STRICT;
        ");
        $visit!(_coven_rows, "
            CREATE TABLE _coven_rows (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                generation BLOB NOT NULL CHECK(length(generation) = 8 AND generation > x'0000000000000000'),
                write_id INTEGER NOT NULL REFERENCES _coven_writes(id),
                UNIQUE(table_name, key, audience, generation)
            ) STRICT;
            CREATE INDEX _coven_rows_audience ON _coven_rows(audience,table_name,key,generation);
            CREATE INDEX _coven_rows_write ON _coven_rows(write_id);
        ");
        $visit!(_coven_cells, "
            CREATE TABLE _coven_cells (
                column_id INTEGER NOT NULL REFERENCES _coven_columns(id),
                row_id INTEGER NOT NULL REFERENCES _coven_rows(id),
                write_id INTEGER NOT NULL REFERENCES _coven_writes(id),
                PRIMARY KEY(row_id, column_id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_cells_write ON _coven_cells(write_id);
        ");
        coven_written_values_tables!($visit);
        $visit!(_coven_references, "
            CREATE TABLE _coven_references (
                row_id INTEGER NOT NULL REFERENCES _coven_rows(id),
                column_id INTEGER NOT NULL REFERENCES _coven_columns(id),
                foreign_key_id INTEGER NOT NULL REFERENCES _coven_foreign_keys(id),
                parent_table TEXT NOT NULL,
                parent_key BLOB NOT NULL,
                parent_audience TEXT NOT NULL,
                parent_generation BLOB NOT NULL CHECK(length(parent_generation)=8),
                PRIMARY KEY(row_id, column_id, foreign_key_id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_references_parent ON _coven_references(parent_table,parent_key,parent_audience,foreign_key_id,row_id);
            CREATE INDEX _coven_references_column ON _coven_references(column_id,row_id);
            CREATE INDEX _coven_references_key ON _coven_references(foreign_key_id,row_id);
        ");
        $visit!(
            _coven_constraints,
            "
            CREATE TABLE _coven_constraints (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                UNIQUE(table_name, identity)
            ) STRICT;
        "
        );
        $visit!(
            _coven_foreign_keys,
            "
            CREATE TABLE _coven_foreign_keys (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                UNIQUE(table_name, identity)
            ) STRICT;
        "
        );
        $visit!(_coven_claims, "
            CREATE TABLE _coven_claims (
                row_id INTEGER NOT NULL REFERENCES _coven_rows(id),
                constraint_id INTEGER NOT NULL REFERENCES _coven_constraints(id),
                audience TEXT NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY(row_id, constraint_id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_claims_value ON _coven_claims(constraint_id,audience,value,row_id);
        ");
        $visit!(_coven_lost, "
            CREATE TABLE _coven_lost (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                generation BLOB NOT NULL CHECK(length(generation) = 8),
                column_id INTEGER REFERENCES _coven_columns(id),
                value BLOB NOT NULL,
                set_by BLOB NOT NULL,
                frozen INTEGER NOT NULL DEFAULT 0 CHECK(frozen IN (0,1)),
                replacement_kind TEXT NOT NULL CHECK(replacement_kind IN ('write', 'rules', 'excluded', 'dependency', 'unauthorized')),
                replaced_by BLOB NOT NULL,
                CHECK((replacement_kind='write') = (column_id IS NOT NULL)),
                CHECK(replacement_kind NOT IN ('excluded','dependency','unauthorized') OR frozen=1)
            ) STRICT;
            CREATE INDEX _coven_lost_row ON _coven_lost(audience,table_name,key,generation,column_id);
            CREATE INDEX _coven_lost_app_key ON _coven_lost(table_name,key);
            CREATE UNIQUE INDEX _coven_lost_live_cell ON _coven_lost(table_name,key,audience,column_id,set_by) WHERE frozen=0 AND replacement_kind='write';
            CREATE INDEX _coven_lost_removed ON _coven_lost(table_name,key,audience) WHERE frozen=0 AND replacement_kind='rules';
            CREATE INDEX _coven_lost_column ON _coven_lost(column_id);
            CREATE TRIGGER _coven_lost_column_deleted AFTER DELETE ON _coven_lost
            WHEN OLD.column_id IS NOT NULL BEGIN
                DELETE FROM _coven_columns WHERE id=OLD.column_id AND synced=0
                    AND NOT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=OLD.column_id);
            END;
            CREATE TRIGGER _coven_lost_column_moved AFTER UPDATE OF column_id ON _coven_lost
            WHEN OLD.column_id IS NOT NULL AND OLD.column_id IS NOT NEW.column_id BEGIN
                DELETE FROM _coven_columns WHERE id=OLD.column_id AND synced=0
                    AND NOT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=OLD.column_id);
            END;
        ");
        coven_uploads_tables!($visit);
        coven_user_originals_tables!($visit);
        coven_device_only_copies_tables!($visit);
        coven_file_removals_tables!($visit);
        coven_pin_requests_tables!($visit);
        coven_file_moves_tables!($visit);
        coven_file_uploads_tables!($visit);
        coven_file_retention_tables!($visit);
        coven_cache_tables!($visit);
        coven_provider_access_tables!($visit);
        coven_operations_tables!($visit);
        coven_object_removals_tables!($visit);
        coven_ended_requests_tables!($visit);
        coven_retries_tables!($visit);
    };
}

pub(crate) fn initial_schema() -> String {
    let mut sql = String::new();
    macro_rules! append {
        (_coven_writes, $sql:literal) => {
            sql.push_str(&format!($sql));
        };
        (_coven_cache, $sql:literal) => {
            sql.push_str(&format!($sql));
        };
        ($table:ident, $sql:literal) => {
            sql.push_str($sql);
        };
    }
    coven_tables!(append);
    sql.push_str(files::SCHEMA);
    sql.push_str(&pins::schema());
    sql.push_str(upload_queue::SCHEMA);
    sql.push_str(sync_status::SCHEMA);
    sql.push_str(retry_effect::SCHEMA);
    sql.push_str(views::SCHEMA);
    sql
}

/// Canonical lowercase hyphenated text of a 16-byte UUID, as `UuidText`
/// binds one.
macro_rules! uuid_text {
    ($blob:literal) => {
        concat!(
            "lower(substr(hex(",
            $blob,
            "),1,8)||'-'||substr(hex(",
            $blob,
            "),9,4)||'-'||",
            "substr(hex(",
            $blob,
            "),13,4)||'-'||substr(hex(",
            $blob,
            "),17,4)||'-'||",
            "substr(hex(",
            $blob,
            "),21))"
        )
    };
}

#[path = "internal_schema_files.rs"]
mod files;
pub(crate) use files::FILES;

#[path = "internal_schema_pins.rs"]
mod pins;
pub(crate) use pins::PINS;

#[path = "internal_schema_upload_queue.rs"]
mod upload_queue;
pub(crate) use upload_queue::UPLOAD_QUEUE;

#[path = "internal_schema_sync_status.rs"]
pub(crate) mod sync_status;
pub(crate) use sync_status::SYNC_STATUS;

#[path = "internal_schema_retry_effect.rs"]
pub(crate) mod retry_effect;
pub(crate) use retry_effect::RETRY_EFFECT;

/// Coven's tables the app's local triggers may follow (§9.4): coven changes
/// their rows in the statements that change what they say, its files table
/// as each record is made, stored or deleted here or merged from another
/// device, its sync status and retry effect as they change, and the app's
/// AFTER triggers on them write only local tables.
pub(crate) const FOLLOWED: [&str; 5] = [FILES, PINS, UPLOAD_QUEUE, SYNC_STATUS, RETRY_EFFECT];

/// Coven's tables that describe this process's own sync, which only the
/// handle holding the store's writer lock may read.
pub(crate) const WRITER_STATE: [&str; 2] = [SYNC_STATUS, RETRY_EFFECT];

/// Whether `name` is one of the tables the app's triggers may follow.
pub(crate) fn followed(name: &str) -> bool {
    FOLLOWED.iter().any(|table| table.eq_ignore_ascii_case(name))
}

#[path = "internal_schema_views.rs"]
mod views;
pub(crate) use views::app_readable;

#[path = "input_database_schema.rs"]
mod input;
pub(crate) use input::initial_input_schema;

#[cfg(test)]
#[path = "internal_schema_tests.rs"]
mod tests;
