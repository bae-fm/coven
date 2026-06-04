//! Host database integration for sync bookkeeping.
//!
//! coven owns three bookkeeping tables — `sync_cursors`, `sync_state`,
//! `cloud_outbox` — created by applying [`MIGRATION_SQL`] to the host's
//! database. The host implements [`SyncBookkeeping`] (coven calls these during
//! a sync cycle) and [`RawDbHandle`] (the session extension attaches to the
//! host's write connection). coven imposes no SQLite driver this way.

use std::collections::HashMap;

use async_trait::async_trait;

/// SQL that creates coven's bookkeeping tables. The host applies this alongside
/// its own schema migration. Idempotent (`IF NOT EXISTS`).
///
/// `IF NOT EXISTS` only guarantees the *fresh-table* shape: a host whose
/// `cloud_outbox` already exists from an earlier coven version will not gain
/// columns added here (e.g. `attempt_count`, `last_error`, `last_attempt_at`).
/// Such hosts must add the new columns through their own `ALTER TABLE`
/// migration path.
pub const MIGRATION_SQL: &str = "\
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
    created_at TEXT NOT NULL,
    min_seq INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TEXT,
    UNIQUE(operation, cloud_key)
);
";

/// An error from the host's bookkeeping implementation.
#[derive(Debug, thiserror::Error)]
#[error("sync bookkeeping error: {0}")]
pub struct DbError(pub String);

/// A pending cloud blob operation from the `cloud_outbox` table.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub id: i64,
    pub operation: OutboxOperation,
    pub file_id: String,
    pub cloud_key: String,
    pub source_path: Option<String>,
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

/// Bookkeeping the host's database performs against coven's tables. coven calls
/// these during a sync cycle; the host's implementation runs the SQL.
#[async_trait]
pub trait SyncBookkeeping: Send + Sync {
    /// Read a value from `sync_state` by key.
    async fn get_sync_state(&self, key: &str) -> Result<Option<String>, DbError>;

    /// The lexicographic `MAX(_updated_at)` across all of the host's synced
    /// tables, or `None` when no synced rows exist. coven seeds its
    /// `_updated_at` register from this on construction so the clock cannot mint
    /// a stamp behind a row already on disk — even one whose stamp was minted
    /// between sync cycles and so never reached the flushed high-water mark. The
    /// host runs `SELECT MAX(_updated_at) FROM <table>` across its synced tables
    /// and returns the overall max (coven imposes no SQLite driver).
    async fn max_synced_updated_at(&self) -> Result<Option<String>, DbError>;

    /// Write a value to `sync_state`.
    async fn set_sync_state(&self, key: &str, value: &str) -> Result<(), DbError>;

    /// All per-device cursors from `sync_cursors` as `device_id -> last_seq`.
    async fn get_all_sync_cursors(&self) -> Result<HashMap<String, u64>, DbError>;

    /// Upsert a single device cursor.
    async fn set_sync_cursor(&self, device_id: &str, seq: u64) -> Result<(), DbError>;

    /// Pending `upload` entries from `cloud_outbox`, oldest first.
    async fn get_pending_cloud_uploads(&self) -> Result<Vec<OutboxEntry>, DbError>;

    /// Pending `delete` entries from `cloud_outbox`.
    async fn get_pending_cloud_deletes(&self) -> Result<Vec<OutboxEntry>, DbError>;

    /// Whether any `upload` entries remain (gates changeset push).
    async fn has_pending_cloud_uploads(&self) -> Result<bool, DbError>;

    /// Remove a `cloud_outbox` entry by id.
    async fn remove_cloud_outbox_entry(&self, id: i64) -> Result<(), DbError>;

    /// Record a failed upload attempt for an entry: increment its
    /// `attempt_count` and set `last_error` and `last_attempt_at`. The entry
    /// stays queued for retry.
    async fn record_cloud_upload_failure(
        &self,
        id: i64,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError>;
}

/// Access to the host's raw write connection, which the session extension
/// attaches to. The same connection the host writes through.
#[async_trait]
pub trait RawDbHandle: Send + Sync {
    /// Acquire the raw sqlite3 write connection pointer the session extension
    /// attaches to. The same connection the host writes through.
    ///
    /// # Safety
    /// The pointer must outlive all sync sessions; the caller serializes session
    /// operations on it.
    async fn raw_write_handle(&self) -> Result<*mut libsqlite3_sys::sqlite3, DbError>;
}

/// The full database surface coven needs from the host: bookkeeping plus the
/// raw write handle. Blanket-implemented for any type providing both.
pub trait SyncDb: SyncBookkeeping + RawDbHandle {}
impl<T: SyncBookkeeping + RawDbHandle> SyncDb for T {}
