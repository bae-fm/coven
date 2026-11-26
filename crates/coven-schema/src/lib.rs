//! Coven's local schema as SQL text, with no connection and no runtime: the `CREATE` statements of
//! every table and view and of coven's triggers, each in the file of the domain that writes the
//! table. coven-database runs this text to make a store, and
//! [`readable_objects_sql`] returns the part that app SQL may read, which a build script that
//! checks an app's SQL against coven's objects takes without compiling coven's runtime.
//!
//! Unsigned merge counters are big-endian eight-byte blobs. SQLite's signed
//! integer range must not truncate device ids, generations or write numbers.
//! Present displayed values live in app tables; a written value that reads
//! differently (a NULL-substituted reference) is kept by cell. Generated
//! columns have no cells: SQLite computes them. Metadata and lost values use
//! coven-format's snapshot field encodings inside blobs.

mod shared_text;
mod sql_functions;

pub use sql_functions::{SqlFunction, SQL_FUNCTIONS};

/// The chunk number of a cache entry's header.
pub const CACHE_HEADER: i64 = -1;
/// The chunk number of a cache entry that holds the whole file.
pub const CACHE_COMPLETE_FILE: i64 = -2;
/// D2 timestamps contain six clock bytes, two counter bytes, then the device id: the SQL that
/// reads the device id of a write's timestamp.
pub const WRITE_DEVICE_SQL: &str = "substr(timestamp,9,8)";

// This is also the ownership checker's source of reserved table names.
#[macro_use]
#[path = "ownership_tables.rs"]
mod ownership;
#[macro_use]
#[path = "sequence_tables.rs"]
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
#[path = "local_bytes_schema.rs"]
mod local_bytes_schema;
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

mod files;
#[path = "input_database_schema.rs"]
mod input;
mod moves;
mod pending;
mod pin_requests;
mod pins;
mod retry_effect;
mod sync_status;
mod upload_queue;
mod views;

pub use files::FILES;
pub use input::initial_input_schema;
pub use moves::MOVES;
pub use pending::PENDING;
pub use pin_requests::PIN_REQUESTS;
pub use pins::PINS;
pub use retry_effect::RETRY_EFFECT;
pub use sync_status::SYNC_STATUS;
pub use upload_queue::UPLOAD_QUEUE;
pub use views::READABLE;

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
        coven_local_bytes_tables!($visit);
        coven_provider_access_tables!($visit);
        coven_operations_tables!($visit);
        coven_object_removals_tables!($visit);
        coven_ended_requests_tables!($visit);
        coven_retries_tables!($visit);
    };
}

/// The SQL that makes every table, view and trigger of a new store.
pub fn initial_schema() -> String {
    let mut sql = String::new();
    macro_rules! append {
        (_coven_writes, $sql:literal) => {
            sql.push_str(&format!($sql));
        };
        (_coven_cache, $sql:literal) => {
            sql.push_str(&format!($sql));
        };
        (_coven_cache_budgets, $sql:literal) => {
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
    sql.push_str(pending::SOURCE);
    sql.push_str(file_database_copies_schema::VIEWS);
    sql.push_str(views::SCHEMA);
    sql
}

/// One of coven's tables the app's local triggers may follow: its name, and
/// the SQL that makes it as every store holds it, which an app's test of its
/// own schema makes outside any store too.
pub struct FollowedTable {
    /// The table's name.
    pub name: &'static str,
    /// The SQL that makes the table.
    pub table: &'static str,
}

/// Coven's tables the app's local triggers may follow (§9.4): coven changes
/// their rows in the statements that change what they say, its files table
/// as each record is made, stored or deleted here or merged from another
/// device, its sync status and retry effect as they change, and the app's
/// AFTER triggers on them write only local tables.
pub const FOLLOWED: [FollowedTable; 8] = [
    FollowedTable { name: FILES, table: files::TABLE },
    FollowedTable { name: PINS, table: pins::TABLE },
    FollowedTable { name: PIN_REQUESTS, table: pin_requests::TABLE },
    FollowedTable { name: MOVES, table: moves::TABLE },
    FollowedTable { name: UPLOAD_QUEUE, table: upload_queue::TABLE },
    FollowedTable { name: PENDING, table: pending::TABLE },
    FollowedTable { name: SYNC_STATUS, table: sync_status::TABLE },
    FollowedTable { name: RETRY_EFFECT, table: retry_effect::TABLE },
];

/// Coven's own tables that `readable_objects_sql` makes:
/// - each table coven's views read;
/// - `_coven_user_chunks`, which `user_file::record_original!` writes when
///   it records a user's original;
/// - `_coven_undoable_files`, which `coven_user_files` reads;
/// - `_coven_write_pause`, which `operation::journal!` writes when it
///   journals an operation that pauses writes;
/// - `_coven_moves` and `_coven_pin_requests`, which the pending records,
///   the uploads and the retry waits reference: their foreign keys need
///   the tables;
/// - `_coven_cache`, whose rows the triggers of `_coven_cache_budgets`
///   count, and `_coven_copy_sizes` and `_coven_capture_sizes`, whose rows
///   the triggers of `coven_library_bytes` count: SQLite makes a trigger
///   only on a table that exists.
pub const OUTSIDE_STORE_TABLES: [&str; 27] = [
    "_coven_user_files",
    "_coven_user_chunks",
    "_coven_undoable_files",
    "_coven_write_pause",
    "_coven_moves",
    "_coven_pin_requests",
    "_coven_uploads",
    "_coven_file_uploads",
    "_coven_upload_files",
    "_coven_upload_pause",
    "_coven_unmetered_only",
    "_coven_retries",
    "_coven_pending",
    "_coven_stored_files",
    "_coven_operations",
    "_coven_app_operations",
    "_coven_members",
    "_coven_devices",
    "_coven_circles",
    "_coven_circle_deletions",
    "_coven_circle_members",
    "_coven_cache",
    "_coven_cache_budgets",
    "_coven_cache_partials",
    "_coven_copy_sizes",
    "_coven_capture_sizes",
    "_coven_copies",
];

/// The SQL that makes each object app SQL may read as every store makes it,
/// and the tables of coven's own in `OUTSIDE_STORE_TABLES`, for an app's
/// test outside any store (`make_readable_objects` of coven-database).
///
/// The SQL makes coven's triggers that keep `coven_pending`, because a test
/// saves pending records as a store does (`pending::save_outside_store`).
/// It also makes each trigger that a made table's own SQL holds, such as
/// those of `_coven_pin_requests`; no test write fires them. The SQL makes
/// no other trigger that keeps a followed table: outside a store the test
/// writes those tables' rows.
pub fn readable_objects_sql() -> String {
    let mut sql = String::new();
    macro_rules! outside_store_table {
        (_coven_cache, $sql:literal) => {
            sql.push_str(&format!($sql));
        };
        (_coven_cache_budgets, $sql:literal) => {
            sql.push_str(&format!($sql));
        };
        ($table:ident, $sql:literal) => {
            let table = stringify!($table);
            if OUTSIDE_STORE_TABLES.contains(&table) || READABLE.contains(&table) {
                sql.push_str($sql);
            }
        };
    }
    coven_tables!(outside_store_table);
    for followed in FOLLOWED {
        sql.push_str(followed.table);
    }
    sql.push_str(pending::SOURCE);
    sql.push_str(file_database_copies_schema::VIEWS);
    sql.push_str(views::SCHEMA);
    sql
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
