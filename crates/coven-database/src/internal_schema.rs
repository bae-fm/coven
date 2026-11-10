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
/// written, a pin's partial download, or bytes recorded for removal
/// (Appendix C.3).
pub(crate) const CACHE_NAME_USED_SQL: &str = "(EXISTS(SELECT 1 FROM _coven_cache WHERE path=?1)
    OR EXISTS(SELECT 1 FROM _coven_cache_partials WHERE path=?1)
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
#[path = "forget_records_schema.rs"]
mod forget_records_schema;
#[macro_use]
#[path = "joined_invitee_schema.rs"]
mod joined_invitee_schema;
#[macro_use]
#[path = "loss_record_schema.rs"]
mod loss_record_schema;
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
#[path = "ownership_schema.rs"]
mod ownership_schema;
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
#[path = "write_sources_schema.rs"]
mod write_sources_schema;
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
        coven_joined_invitee_tables!($visit);
        coven_write_boundaries_tables!($visit);
        coven_raises_tables!($visit);
        coven_merge_store_tables!($visit);
        coven_write_headers_tables!($visit);
        coven_authored_sources_tables!($visit);
        coven_forgetting_tables!($visit);
        coven_ownership_tables!($visit);
        coven_published_tables!($visit);
        coven_sequence_tables!($visit);
        coven_claim_tables!($visit);
        coven_conversion_schemas_tables!($visit);
        coven_source_names_tables!($visit);
        coven_written_values_tables!($visit);
        coven_losses_tables!($visit);
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
    sql.push_str(row_queries_schema::COLUMN_RELEASE);
    for followed in FOLLOWED {
        sql.push_str(followed.table);
    }
    sql.push_str(&pins::source());
    sql.push_str(upload_queue::TRIGGERS);
    sql.push_str(views::SCHEMA);
    sql
}

#[path = "internal_schema_files.rs"]
mod files;
pub(crate) use files::FILES;

#[path = "internal_schema_pins.rs"]
mod pins;
pub(crate) use pins::PINS;

#[path = "internal_schema_pin_requests.rs"]
mod pin_requests;
pub(crate) use pin_requests::PIN_REQUESTS;

#[path = "internal_schema_moves.rs"]
mod moves;
pub(crate) use moves::MOVES;

#[path = "internal_schema_upload_queue.rs"]
mod upload_queue;
pub(crate) use upload_queue::UPLOAD_QUEUE;

#[path = "internal_schema_sync_status.rs"]
pub(crate) mod sync_status;
pub(crate) use sync_status::SYNC_STATUS;

#[path = "internal_schema_retry_effect.rs"]
pub(crate) mod retry_effect;
pub(crate) use retry_effect::RETRY_EFFECT;

/// One of coven's tables the app's local triggers may follow: its name, and
/// the SQL that makes it as every store holds it, which an app's test of its
/// own schema runs too (`followed_tables_sql`).
pub(crate) struct FollowedTable {
    pub(crate) name: &'static str,
    pub(crate) table: &'static str,
}

/// Coven's tables the app's local triggers may follow (§9.4): coven changes
/// their rows in the statements that change what they say, its files table
/// as each record is made, stored or deleted here or merged from another
/// device, its sync status and retry effect as they change, and the app's
/// AFTER triggers on them write only local tables.
pub(crate) const FOLLOWED: [FollowedTable; 7] = [
    FollowedTable { name: FILES, table: files::TABLE },
    FollowedTable { name: PINS, table: pins::TABLE },
    FollowedTable { name: PIN_REQUESTS, table: pin_requests::TABLE },
    FollowedTable { name: MOVES, table: moves::TABLE },
    FollowedTable { name: UPLOAD_QUEUE, table: upload_queue::TABLE },
    FollowedTable { name: SYNC_STATUS, table: sync_status::TABLE },
    FollowedTable { name: RETRY_EFFECT, table: retry_effect::TABLE },
];

/// Coven's tables that describe this process's own sync, which only the
/// handle holding the store's writer lock may read.
pub(crate) const WRITER_STATE: [&str; 2] = [SYNC_STATUS, RETRY_EFFECT];

/// Whether `name` is one of the tables the app's triggers may follow.
pub(crate) fn followed(name: &str) -> bool {
    FOLLOWED.iter().any(|table| table.name.eq_ignore_ascii_case(name))
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
