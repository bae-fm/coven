//! coven's bookkeeping schema, the `item_keys` synced table, and the
//! cloud-outbox row types.
//!
//! coven owns three device-local bookkeeping tables — `sync_cursors`,
//! `sync_state`, `cloud_outbox` — plus the library-global synced table
//! `item_keys`, all created by `MIGRATION_SQL`, which
//! [`crate::database::Database::open`] runs against the connection coven owns.
//! The host no longer implements any of this; it runs its own SQL through
//! [`crate::database::Database::call`] and reads/writes the outbox through the
//! [`crate::database::Database`] API.
//!
//! Unlike the bookkeeping tables, `item_keys` is content every member needs, so
//! coven injects it into the synced-table set (see
//! [`crate::database::Database::open`]) and it rides both sync paths: the
//! changeset capture session records `mint_item_key` INSERTs, and the snapshot
//! preserves it (it is in the synced set, so `clear_non_synced` keeps its rows).

/// The coven-owned synced table holding per-item content keys. Injected into the
/// synced-table set by [`crate::database::Database::open`], so it is captured,
/// snapshotted, and applied like any synced table — but is owned by coven, not
/// the host. The `_updated_at` HLC stamp satisfies the synced-table contract;
/// rows are immutable (idempotent INSERT) so LWW never has to pick a winner.
pub(crate) const ITEM_KEYS_TABLE: &str = "item_keys";

/// SQL that creates coven's bookkeeping tables and the `item_keys` synced table,
/// run by `Database::open` before the host's own migration. Idempotent
/// (`IF NOT EXISTS`).
pub(crate) const MIGRATION_SQL: &str = "\
CREATE TABLE IF NOT EXISTS sync_cursors (
    device_id TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cloud_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation TEXT NOT NULL CHECK (operation IN ('upload', 'delete')),
    file_id TEXT NOT NULL,
    cloud_key TEXT NOT NULL,
    source_path TEXT,
    -- The blob's encryption scope (master / derived / item), serialized so the
    -- async drain resolves it to a key long after the enqueue site is gone.
    -- NULL for a delete entry, which touches no key. Local bookkeeping; this
    -- table does not sync.
    scope TEXT,
    created_at TEXT NOT NULL,
    min_seq INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TEXT,
    UNIQUE(operation, cloud_key)
);

CREATE TABLE IF NOT EXISTS item_keys (
    item_id TEXT PRIMARY KEY,
    key BLOB NOT NULL,
    _updated_at TEXT NOT NULL
);
";

/// A pending cloud blob operation from the `cloud_outbox` table.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub id: i64,
    pub operation: OutboxOperation,
    pub file_id: String,
    pub cloud_key: String,
    pub source_path: Option<String>,
    /// The blob's encryption scope, named by the host at enqueue, for an upload
    /// entry. `process_uploads` resolves it to a key at drain (looking up
    /// `item_keys` for a [`crate::blob::BlobScope::Item`] scope), since the upload
    /// runs long after the enqueue site is gone. `None` for a delete entry — a
    /// delete touches no key, so it stores no scope.
    pub scope: Option<crate::blob::BlobScope>,
    pub created_at: String,
    pub min_seq: Option<u64>,
    /// How many times an upload of this entry has failed. `0` for a freshly
    /// queued entry.
    pub attempt_count: i64,
    /// The error from the most recent failed attempt, if any.
    pub last_error: Option<String>,
    /// RFC 3339 timestamp of the most recent attempt, if any. Drives retry
    /// backoff.
    pub last_attempt_at: Option<String>,
}

/// Type of cloud blob operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxOperation {
    Upload,
    Delete,
}

impl OutboxOperation {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "upload" => Some(OutboxOperation::Upload),
            "delete" => Some(OutboxOperation::Delete),
            _ => None,
        }
    }
}
