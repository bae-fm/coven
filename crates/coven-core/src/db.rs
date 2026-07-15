//! coven's bookkeeping schema and the cloud-outbox row types.
//!
//! coven owns its device-local bookkeeping tables — `protocol_state`,
//! `materialized_commits`, `snapshot_coverage`, `store_writes`,
//! `outbound_membership_mutation`, `outbound_store_snapshot`,
//! `cloud_outbox`, `local_blob_refs`, `blob_make_remote_intents`,
//! `local_cleanup_intents`, `store_write_blob_leases`, and `blob_uploaders` — all
//! created STRICT by [`apply_coven_schema`], which coven
//! runs against the connection it owns during open. The host does not implement
//! any of this; app SQL goes through [`crate::CovenHandle::sql`] or
//! [`crate::CovenHandle::write`].

macro_rules! coven_tables {
    ($visit:ident) => {
        $visit!(
            protocol_state,
            "
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
"
        );
        $visit!(
            materialized_commits,
            "
    device_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    commit_hash TEXT NOT NULL CHECK (length(commit_hash) = 64),
    PRIMARY KEY (device_id, seq)
"
        );
        $visit!(
            snapshot_coverage,
            "
    device_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL CHECK (seq > 0),
    commit_hash TEXT NOT NULL CHECK (length(commit_hash) = 64),
    snapshot_hash TEXT NOT NULL CHECK (length(snapshot_hash) = 64)
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
    seq INTEGER NOT NULL CHECK (seq > 0),
    namespace TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    disposition TEXT NOT NULL CHECK (disposition IN ('drop', 'cache', 'pin')),
    PRIMARY KEY (seq, namespace, blob_id)
"
        );
        $visit!(
            store_writes,
            "
    ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
    write_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (json_valid(status)),
    affected_rows TEXT NOT NULL CHECK (json_valid(affected_rows)),
    changeset BLOB NOT NULL,
    inverse_changeset BLOB NOT NULL,
    base TEXT NOT NULL CHECK (json_valid(base)),
    blob_facts TEXT NOT NULL CHECK (json_valid(blob_facts)),
    prepared TEXT CHECK (prepared IS NULL OR json_valid(prepared))
"
        );
        $visit!(
            store_write_blob_leases,
            "
    write_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    PRIMARY KEY (write_id, namespace, blob_id),
    FOREIGN KEY (write_id) REFERENCES store_writes(write_id)
"
        );
        $visit!(
            store_write_partitions,
            "
    write_id TEXT NOT NULL,
    audience TEXT NOT NULL,
    control_coord TEXT,
    changeset BLOB NOT NULL,
    PRIMARY KEY (write_id, audience),
    FOREIGN KEY (write_id) REFERENCES store_writes(write_id) ON DELETE CASCADE,
    CHECK (
        (audience = 'store' AND control_coord IS NULL)
        OR
        (audience NOT IN ('store', 'local') AND json_valid(control_coord))
    )
"
        );
        $visit!(
            outbound_membership_mutation,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    intent_hash TEXT NOT NULL CHECK (length(intent_hash) = 64),
    plan_bytes BLOB NOT NULL,
    progress_bytes BLOB NOT NULL
"
        );
        $visit!(
            outbound_store_snapshot,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    snapshot_hash TEXT NOT NULL CHECK (length(snapshot_hash) = 64),
    image_hash TEXT NOT NULL CHECK (length(image_hash) = 64),
    image_bytes BLOB NOT NULL,
    meta_bytes BLOB NOT NULL
"
        );
        $visit!(
            published_store_acks,
            "
    revision INTEGER PRIMARY KEY CHECK (revision > 0),
    ack_hash TEXT NOT NULL UNIQUE CHECK (length(ack_hash) = 64)
"
        );
        $visit!(
            outbound_store_acks,
            "
    revision INTEGER PRIMARY KEY CHECK (revision > 0),
    ack_hash TEXT NOT NULL UNIQUE CHECK (length(ack_hash) = 64),
    previous_ack_hash TEXT,
    ack_bytes BLOB NOT NULL
"
        );
        $visit!(
            local_store_protocol_root,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    store_root_hash TEXT NOT NULL CHECK (length(store_root_hash) = 64),
    store_protocol_root_bytes BLOB NOT NULL,
    published INTEGER NOT NULL CHECK (published IN (0, 1))
"
        );
        $visit!(
            local_store_device_registration,
            "
    revision INTEGER PRIMARY KEY CHECK (revision > 0),
    registration_hash TEXT NOT NULL UNIQUE CHECK (length(registration_hash) = 64),
    previous_registration_hash TEXT,
    state TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    registration_bytes BLOB NOT NULL,
    activation_commit_bytes BLOB,
    activation_head_bytes BLOB,
    published INTEGER NOT NULL CHECK (published IN (0, 1)),
    CHECK ((activation_commit_bytes IS NULL) = (activation_head_bytes IS NULL))
"
        );
        $visit!(
            store_device_registration_activations,
            "
    device_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    registration_hash TEXT NOT NULL CHECK (length(registration_hash) = 64),
    previous_registration_hash TEXT,
    state TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    author_pubkey TEXT NOT NULL,
    registration_bytes BLOB NOT NULL,
    stream_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    commit_hash TEXT NOT NULL CHECK (length(commit_hash) = 64),
    PRIMARY KEY (device_id, revision),
    UNIQUE (device_id, registration_hash),
    UNIQUE (stream_id, seq, device_id)
"
        );
        $visit!(
            blob_uploaders,
            "
    namespace TEXT NOT NULL,
    blob_id   TEXT NOT NULL,
    -- Hex public key of the member that uploaded this blob (its cloud key sits
    -- under `{namespace}/{uploader}/…`). Recorded from an authenticated source
    -- only: at pull (the signed changeset's author, who uploads the blobs its rows
    -- introduce) and at our own enqueue (ourselves). Never discovered by scanning
    -- an untrusted listing — a missing record is a fail-loud dispatch error, not a
    -- cue to search. Not a synced table, but preserved into a snapshot (unlike the
    -- per-device bookkeeping tables) because a blob's uploader is a member-global
    -- fact the same for every device, so a snapshot-bootstrapped device inherits
    -- authoritative uploaders from the Owner-signed snapshot rather than scanning.
    uploader  TEXT NOT NULL,
    PRIMARY KEY (namespace, blob_id)
"
        );
        $visit!(
            circle_control_activations,
            "
    circle_id TEXT NOT NULL,
    control_coord TEXT NOT NULL CHECK (json_valid(control_coord)),
    stream_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    commit_hash TEXT NOT NULL CHECK (length(commit_hash) = 64),
    control_bytes BLOB NOT NULL,
    PRIMARY KEY (circle_id, control_coord),
    UNIQUE (circle_id, stream_id, seq)
"
        );
        $visit!(
            circle_operations,
            "
    operation_id TEXT PRIMARY KEY,
    circle_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (json_valid(status)),
    payload BLOB NOT NULL
"
        );
        $visit!(
            circle_access_cache,
            "
    circle_id TEXT NOT NULL,
    control_coord TEXT NOT NULL CHECK (json_valid(control_coord)),
    owner_pubkey TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('active', 'inactive')),
    access_bytes BLOB NOT NULL,
    PRIMARY KEY (circle_id, control_coord, owner_pubkey),
    FOREIGN KEY (circle_id, control_coord)
        REFERENCES circle_control_activations(circle_id, control_coord)
"
        );
        $visit!(
            circle_roster_cache,
            "
    circle_id TEXT NOT NULL,
    control_coord TEXT NOT NULL CHECK (json_valid(control_coord)),
    roster_bytes BLOB NOT NULL,
    PRIMARY KEY (circle_id, control_coord),
    FOREIGN KEY (circle_id, control_coord)
        REFERENCES circle_control_activations(circle_id, control_coord)
"
        );
        $visit!(
            circle_metadata_cache,
            "
    circle_id TEXT NOT NULL,
    control_coord TEXT NOT NULL CHECK (json_valid(control_coord)),
    metadata_bytes BLOB NOT NULL,
    PRIMARY KEY (circle_id, control_coord),
    FOREIGN KEY (circle_id, control_coord)
        REFERENCES circle_control_activations(circle_id, control_coord)
"
        );
        $visit!(
            circle_snapshot_coverage,
            "
    circle_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    commit_hash TEXT NOT NULL CHECK (length(commit_hash) = 64),
    snapshot_hash TEXT NOT NULL CHECK (length(snapshot_hash) = 64),
    PRIMARY KEY (circle_id, device_id)
"
        );
        $visit!(
            circle_acks,
            "
    circle_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    ack_hash TEXT NOT NULL CHECK (length(ack_hash) = 64),
    ack_bytes BLOB NOT NULL,
    published INTEGER NOT NULL CHECK (published IN (0, 1)),
    PRIMARY KEY (circle_id, device_id, revision),
    UNIQUE (circle_id, ack_hash)
"
        );
    };
}

macro_rules! coven_routing_tables {
    ($visit:ident) => {
        $visit!(
            _coven_audience,
            "
    routing_id TEXT PRIMARY KEY,
    circle_id TEXT,
    _updated_at TEXT NOT NULL
"
        );
        $visit!(
            _coven_row_routes,
            "
    routing_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    _updated_at TEXT NOT NULL,
    UNIQUE (table_name, row_id)
"
        );
    };
}

/// Creates Coven's bookkeeping tables after the fresh host schema has passed
/// sync-routing validation, inside the same open transaction. Idempotent (`IF
/// NOT EXISTS`). STRICT: every column here is already TEXT/INTEGER/BLOB, so
/// STRICT only forecloses a future column drifting off its declared affinity.
pub(crate) fn apply_coven_schema(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    macro_rules! apply_table {
        ($name:ident, $columns:literal) => {
            conn.execute_batch(concat!(
                "CREATE TABLE IF NOT EXISTS ",
                stringify!($name),
                " (",
                $columns,
                ") STRICT;"
            ))?;
        };
    }

    coven_tables!(apply_table);
    Ok(())
}

/// Create the MergeConcurrent audience mirror and private route map. The
/// initializer calls this only when the routing contract has a scoped graph.
pub(crate) fn apply_coven_routing_schema(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    macro_rules! apply_table {
        ($name:ident, $columns:literal) => {
            conn.execute_batch(concat!(
                "CREATE TABLE ",
                stringify!($name),
                " (",
                $columns,
                ") STRICT, WITHOUT ROWID;"
            ))?;
        };
    }

    coven_routing_tables!(apply_table);
    Ok(())
}

/// Whether `name` is a table coven owns for sync bookkeeping. Hosts may not
/// declare these as synced tables.
pub(crate) fn is_reserved_table_name(name: &str) -> bool {
    macro_rules! matches_table {
        ($table:ident, $columns:literal) => {
            if name == stringify!($table) {
                return true;
            }
        };
    }

    coven_tables!(matches_table);
    coven_routing_tables!(matches_table);
    false
}

/// The name of every table [`apply_coven_schema`] creates, for a test to assert a
/// schema property (STRICT) holds across all of them without re-listing the set
/// by hand.
#[cfg(test)]
pub(crate) fn table_names() -> Vec<&'static str> {
    let mut names = Vec::new();
    macro_rules! collect_name {
        ($name:ident, $columns:literal) => {
            names.push(stringify!($name));
        };
    }

    coven_tables!(collect_name);
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every bookkeeping table [`apply_coven_schema`] creates is declared STRICT
    /// — the same guarantee the synced-table contract now requires of the host's
    /// own tables, so coven does not exempt itself from the invariant it enforces
    /// on the host.
    #[test]
    fn every_bookkeeping_table_is_strict() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        apply_coven_schema(&conn).expect("apply coven schema");
        for name in table_names() {
            let sql = format!(
                "PRAGMA table_list({})",
                crate::sync::session::quote_ident(name)
            );
            let mut stmt = conn.prepare(&sql).expect("prepare table_list");
            let strict: i64 = stmt
                .query_row([], |row| row.get(5))
                .unwrap_or_else(|e| panic!("PRAGMA table_list({name}): {e}"));
            assert_eq!(strict, 1, "{name} must be STRICT");
        }
    }

    #[test]
    fn routing_tables_are_strict_without_rowid() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        apply_coven_routing_schema(&conn).expect("apply routing schema");
        for name in ["_coven_audience", "_coven_row_routes"] {
            let sql = format!(
                "PRAGMA table_list({})",
                crate::sync::session::quote_ident(name)
            );
            let (wr, strict): (i64, i64) = conn
                .query_row(&sql, [], |row| Ok((row.get(4)?, row.get(5)?)))
                .expect("table_list");
            assert_eq!((wr, strict), (1, 1), "{name}");
        }
    }
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
    /// scope (resolved to a key at drain, since the upload runs long after the
    /// enqueue site is gone), the `file_id` the upload reports progress under,
    /// and whether to keep the uploaded blob pinned in the local cache.
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
