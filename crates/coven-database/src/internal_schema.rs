//! Coven's local schema. `application_id` identifies these tables independently
//! of the application's `user_version`.
//!
//! The SQL of the tables and views is in coven-schema, which has no runtime; this module holds
//! what coven-database does with it. Unsigned merge counters are big-endian eight-byte blobs.
//! SQLite's signed integer range must not truncate device ids, generations or write numbers.
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

pub(crate) const VERSION: u32 = 2;
pub(crate) use coven_schema::{CACHE_COMPLETE_FILE, CACHE_HEADER, FOLLOWED, WRITE_DEVICE_SQL};
/// Coven's tables no live query reads: the merge state, authorship, creation,
/// publication and claim records each write keeps beside its rows. A write records no
/// change to them, and recording one costs as much as an app row's (§4); a
/// live query that read one would be a bug coven refuses at once.
pub(crate) const UNRECORDED: [&str; 23] = [
    "_coven_authored_admissions",
    "_coven_authored_changes",
    "_coven_authored_parts",
    "_coven_cells",
    "_coven_claims",
    "_coven_creations",
    "_coven_foreign_keys",
    "_coven_part_leaves",
    "_coven_part_proofs",
    "_coven_published",
    "_coven_published_creations",
    "_coven_published_dropped",
    "_coven_published_file_refs",
    "_coven_published_losses",
    "_coven_published_rows",
    "_coven_published_writes",
    "_coven_references",
    "_coven_rows",
    "_coven_unnamed_writes",
    "_coven_write_evidence",
    "_coven_write_headers",
    "_coven_constraints",
    "_coven_written_values",
];
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

#[cfg(test)]
pub(crate) use coven_schema::OUTSIDE_STORE_TABLES;
pub(crate) use coven_schema::{initial_input_schema, initial_schema};
pub(crate) use coven_schema::{FILES, RETRY_EFFECT, SYNC_STATUS};

#[path = "internal_schema_files.rs"]
mod files;
#[path = "internal_schema_pending.rs"]
mod pending;
#[path = "internal_schema_pins.rs"]
mod pins;
#[path = "internal_schema_upload_queue.rs"]
mod upload_queue;

#[path = "internal_schema_sync_status.rs"]
pub(crate) mod sync_status;

#[path = "internal_schema_retry_effect.rs"]
pub(crate) mod retry_effect;

/// Coven's tables that describe this process's own sync, which only the
/// handle holding the store's writer lock may read. A read-only handle's
/// policy refuses app SQL that reads either, directly or through a view such
/// as `coven_sync_state`.
pub(crate) const WRITER_STATE: [&str; 2] = [SYNC_STATUS, RETRY_EFFECT];

/// Whether `name` is one of the tables the app's triggers may follow.
pub(crate) fn followed(name: &str) -> bool {
    FOLLOWED.iter().any(|table| table.name.eq_ignore_ascii_case(name))
}

#[path = "internal_schema_views.rs"]
mod views;
#[cfg(test)]
pub(crate) use coven_schema::READABLE;
pub(crate) use views::{app_readable, whole_views};

#[cfg(test)]
#[path = "internal_schema_tests.rs"]
mod tests;
