//! coven's bookkeeping schema and the cloud-outbox row types.
//!
//! coven owns three bookkeeping tables — `sync_cursors`, `sync_state`,
//! `cloud_outbox` — created by `MIGRATION_SQL`, which [`crate::database::Database::open`]
//! runs against the connection coven owns. The host no longer implements any of
//! this; it runs its own SQL through [`crate::database::Database::call`] and
//! reads/writes the outbox through the [`crate::database::Database`] API.

/// SQL that creates coven's bookkeeping tables, run by `Database::open` before
/// the host's own migration. Idempotent (`IF NOT EXISTS`).
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
    -- The 32-byte key this blob is encrypted under (NULL falls back to the
    -- library master key). Local bookkeeping; this table does not sync.
    content_key BLOB,
    created_at TEXT NOT NULL,
    min_seq INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TEXT,
    UNIQUE(operation, cloud_key)
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
    /// The 32-byte key this blob's bytes are encrypted under. `None` falls back
    /// to the library master key. The host supplies it at enqueue time and
    /// persists it on the row, since the upload drains long after the enqueue
    /// site is gone.
    pub content_key: Option<[u8; 32]>,
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
