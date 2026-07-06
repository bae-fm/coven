//! coven's bookkeeping schema, the `item_keys` synced table, and the
//! cloud-outbox row types.
//!
//! coven owns eight device-local bookkeeping tables — `sync_cursors`,
//! `sync_state`, `cloud_outbox`, `local_blob_refs`, `blob_make_remote_intents`,
//! `local_cleanup_intents`, `published_blob_drop_intents`,
//! `pending_changesets` — plus the library-global synced table `item_keys`, all
//! created by [`apply_coven_schema`], which coven runs against the connection it owns
//! during open. The host does not implement any of this; native app SQL goes through
//! [`crate::CovenHandle::sql`] or [`crate::CovenHandle::write`].
//!
//! Unlike the bookkeeping tables, `item_keys` is content every member needs, so
//! coven injects it into the synced-table set during open and it rides both sync
//! paths: the changeset capture session records `mint_item_key` INSERTs, and the
//! snapshot preserves it (it is in the synced set, so `clear_non_synced` keeps
//! its rows).

/// The coven-owned synced table holding per-item content keys. Injected into the
/// synced-table set during open, so it is captured, snapshotted, and applied
/// like any synced table — but is owned by coven, not the host. The `_updated_at`
/// HLC stamp satisfies the synced-table contract; rows are immutable
/// (idempotent INSERT) so LWW never has to pick a winner.
pub(crate) const ITEM_KEYS_TABLE: &str = stringify!(item_keys);

macro_rules! coven_tables {
    ($visit:ident) => {
        $visit!(
            sync_cursors,
            "
    device_id TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL
"
        );
        $visit!(
            sync_state,
            "
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
"
        );
        $visit!(
            cloud_outbox,
            "
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
"
        );
        $visit!(
            local_blob_refs,
            "
    blob_id   TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    path      TEXT NOT NULL,   -- absolute external path coven reads but does NOT own
    size      INTEGER NOT NULL -- plaintext length; validate-on-read
"
        );
        $visit!(
            blob_make_remote_intents,
            "
    root_table TEXT NOT NULL,
    root_id    TEXT NOT NULL,
    retain_pinned INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (root_table, root_id)
"
        );
        $visit!(
            local_cleanup_intents,
            "
    namespace TEXT NOT NULL,
    blob_id   TEXT NOT NULL,
    PRIMARY KEY (namespace, blob_id)
"
        );
        $visit!(
            published_blob_drop_intents,
            "
    seq INTEGER NOT NULL,
    namespace TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    size INTEGER NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('drop', 'cache', 'pin')),
    PRIMARY KEY (seq, namespace, blob_id)
"
        );
        $visit!(
            pending_changesets,
            "
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    changeset BLOB NOT NULL
"
        );
        $visit!(
            item_keys,
            "
    item_id TEXT PRIMARY KEY,
    key BLOB NOT NULL,
    _updated_at TEXT NOT NULL
"
        );
    };
}

/// Creates coven's bookkeeping tables and the `item_keys` synced table before
/// the host's own migrations. Idempotent (`IF NOT EXISTS`).
pub(crate) fn apply_coven_schema(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    macro_rules! apply_table {
        ($name:ident, $columns:literal) => {
            conn.execute_batch(concat!(
                "CREATE TABLE IF NOT EXISTS ",
                stringify!($name),
                " (",
                $columns,
                ");"
            ))?;
        };
    }

    coven_tables!(apply_table);
    Ok(())
}

/// Whether `name` is a table coven owns for sync bookkeeping or library-global
/// key material. Hosts may not declare these as synced tables.
pub(crate) fn is_reserved_table_name(name: &str) -> bool {
    macro_rules! matches_table {
        ($table:ident, $columns:literal) => {
            if name == stringify!($table) {
                return true;
            }
        };
    }

    coven_tables!(matches_table);
    false
}

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
    /// How many times an upload of this entry has failed. `0` for a freshly
    /// queued entry.
    pub attempt_count: i64,
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
        /// (`storage/pinned/<namespace>/<id>`) from the plaintext on a successful
        /// upload, so a pinned Remote blob is kept local and budget-exempt with no
        /// later cloud round-trip. `false` populates nothing on write — the evictable
        /// `storage/cache/<namespace>/<id>` fills on a later read-miss instead.
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
