//! coven's bookkeeping schema, the `item_keys` synced table, and the
//! cloud-outbox row types.
//!
//! coven owns five device-local bookkeeping tables — `sync_cursors`,
//! `sync_state`, `cloud_outbox`, `local_blob_refs`, `blob_make_remote_intents` — plus
//! the library-global synced table `item_keys`, all created by `MIGRATION_SQL`,
//! which
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
    operation TEXT NOT NULL CHECK (operation IN ('upload', 'delete', 'cancel')),
    -- The blob's file id, which an upload reports progress under. NULL for a
    -- delete or cancel entry, which carry no file id.
    file_id TEXT,
    cloud_key TEXT NOT NULL,
    source_path TEXT,
    -- The blob's encryption scope (master / derived / item), serialized so the
    -- async drain resolves it to a key long after the enqueue site is gone.
    -- NULL for a delete or cancel entry, which touch no key. Local bookkeeping;
    -- this table does not sync.
    scope TEXT,
    -- Whether a successful upload should also populate coven's protected cache
    -- folder (storage/pinned/<id>) from the plaintext, so the blob is kept local
    -- and budget-exempt with no later cloud round-trip. Upload-only and honestly
    -- 0 for a delete or cancel (they retain nothing), so unlike scope/source_path
    -- it has a meaningful default rather than NULL.
    retain_pinned INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TEXT,
    UNIQUE(operation, cloud_key)
);

-- Device-local map from a blob id to an external user-owned file coven reads
-- but does not own (a user-provided Local blob plays the user's own file). A weaker
-- storage class than the cache: validate-on-read by presence + size, never
-- self-heal. Device-local like the bookkeeping tables — no `_updated_at`, never
-- in the synced-table set, never snapshotted — so external refs stay on the one
-- device that registered them and never cross to a peer.
CREATE TABLE IF NOT EXISTS local_blob_refs (
    blob_id   TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    path      TEXT NOT NULL,   -- absolute external path coven reads but does NOT own
    size      INTEGER NOT NULL -- plaintext length; validate-on-read
);

-- Device-local marker for an in-flight make_remote (Local → Remote): coven owns
-- the transition, so this row makes it durable. It is set in the same transaction
-- that enqueues the root's user-provided blob uploads, and removed in the same
-- transaction that flips the gate true once the last upload lands (the single commit
-- point). It is a pure presence marker: its existence tells the upload drain's
-- completion check that an uploaded blob's root is a make_remote to finish (vs. an
-- orphan from a cancelled make_remote to tombstone), and it makes completion +
-- cancel idempotent across a restart. The pin choice rides each upload row's
-- `retain_pinned`, not this marker. Device-local like the other bookkeeping tables —
-- no `_updated_at`, never synced or snapshotted.
CREATE TABLE IF NOT EXISTS blob_make_remote_intents (
    root_table TEXT NOT NULL,
    root_id    TEXT NOT NULL,
    PRIMARY KEY (root_table, root_id)
);

CREATE TABLE IF NOT EXISTS item_keys (
    item_id TEXT PRIMARY KEY,
    key BLOB NOT NULL,
    _updated_at TEXT NOT NULL
);
";

/// An external user-owned file a blob id resolves to, read back from a
/// `local_blob_refs` row. The blob's plaintext lives at `path` (an absolute file
/// coven references but does not own); `size` is its registered plaintext length,
/// against which a read validates the file by presence + size. The `namespace`
/// stays on the row but is not part of the read shape, so it is not carried here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalBlob {
    /// Absolute path to the external file coven reads but does not own.
    pub path: std::path::PathBuf,
    /// The file's plaintext length at registration. A read fails loud if the
    /// file's current length differs (truncated, replaced) — validate-on-read.
    pub size: u64,
}

/// A pending cloud blob operation from the `cloud_outbox` table.
///
/// The fields the operations share live here; the operation-specific ones live in
/// [`OutboxOperation`]. The `cloud_outbox` table is flat (the operation-specific
/// `scope`/`source_path` columns are nullable), but a row reads back as exactly one
/// variant, so a drain matches on `operation` and never sees a column that doesn't
/// belong to it (no upload-only `scope` on a delete or cancel).
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub id: i64,
    pub cloud_key: String,
    pub created_at: String,
    /// How many times an upload of this entry has failed. `0` for a freshly
    /// queued entry.
    pub attempt_count: i64,
    /// The error from the most recent failed attempt, if any.
    pub last_error: Option<String>,
    /// RFC 3339 timestamp of the most recent attempt, if any. Drives retry
    /// backoff.
    pub last_attempt_at: Option<String>,
    /// The operation and its operation-specific fields.
    pub operation: OutboxOperation,
}

/// A cloud blob operation and the fields only that operation carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxOperation {
    /// Upload a local blob. Carries the local source, the host-named encryption
    /// scope (resolved to a key at drain — looking up `item_keys` for a
    /// [`crate::blob::BlobScope::Item`] scope, since the upload runs long after
    /// the enqueue site is gone), the `file_id` the upload reports progress
    /// under, and whether to keep the uploaded blob pinned in the local cache.
    Upload {
        file_id: String,
        /// Local plaintext source. `None` means the blob lives at coven's
        /// default storage path for `file_id`.
        source_path: Option<String>,
        /// The blob's encryption scope, named by the host at enqueue. An upload
        /// always has one — a delete, which touches no key, has none.
        scope: crate::blob::BlobScope,
        /// Whether the drain populates the protected cache folder
        /// (`storage/pinned/<id>`) from the plaintext on a successful upload, so a
        /// pinned managed blob is kept local and budget-exempt with no later cloud
        /// round-trip. `false` populates nothing on write — the evictable
        /// `storage/cache/<id>` fills on a later read-miss instead.
        retain_pinned: bool,
    },
    /// Delete a cloud blob. The drain turns it into a signed cloud tombstone (the
    /// deletion's durable record); a later GC reclaims the blob once a convergence
    /// grace has passed, so a peer that still references it isn't stranded. See
    /// [`crate::blob::delete`]. Carries no extra fields.
    Delete,
    /// Cancel the tombstone for `cloud_key`: remove the `blob_tombstones/{key}`
    /// object so a GC pass won't reclaim a blob that has just been (re-)uploaded
    /// to that key. Queued by the upload drain only when its inline cancel of the
    /// tombstone fails, so the cancel survives that failure and a restart and is
    /// retried each cycle until the tombstone is gone (see
    /// [`crate::blob::delete::drain_tombstone_cancels`]). Carries no extra fields.
    Cancel,
}
