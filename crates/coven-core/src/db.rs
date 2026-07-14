//! coven's bookkeeping schema and the cloud-outbox row types.
//!
//! coven owns ten device-local bookkeeping tables — `sync_cursors`,
//! `sync_state`, `cloud_outbox`, `local_blob_refs`, `blob_make_remote_intents`,
//! `local_cleanup_intents`, `published_blob_drop_intents`, `pending_changesets`,
//! `blob_uploaders`, `blob_location_versions` — all created STRICT by
//! [`apply_coven_schema`], which coven
//! runs against the connection it owns during open. The host does not implement
//! any of this; native app SQL goes through [`crate::CovenHandle::sql`] or
//! [`crate::CovenHandle::write`].

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
    operation TEXT NOT NULL CHECK (operation IN ('upload', 'delete')),
    -- The blob's file id, which an upload reports progress under. NULL for a
    -- delete entry, which carries no file id.
    file_id TEXT,
    cloud_key TEXT NOT NULL,
    source_path TEXT,
    -- Author-signed plaintext hash this upload must match. Required for uploads.
    expected_hash TEXT,
    -- The blob's encryption scope (master / derived / item), serialized so the
    -- async drain resolves it to a key long after the enqueue site is gone.
    -- NULL for delete entries, which touch no encryption key. Local bookkeeping;
    -- this table does not sync.
    scope TEXT,
    -- Whether a successful upload should also populate coven's protected cache
    -- folder (storage/pinned/<namespace>/<id>/<content_hash>) from the plaintext, so the blob is kept local
    -- and budget-exempt with no later cloud round-trip. Upload-only and honestly
    -- 0 for a delete (it retains nothing), so unlike scope/source_path
    -- it has a meaningful default rather than NULL.
    retain_pinned INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TEXT,
    CHECK (operation != 'upload' OR expected_hash IS NOT NULL),
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
    content_hash TEXT NOT NULL,
    PRIMARY KEY (namespace, blob_id, content_hash)
"
        );
        $visit!(
            published_blob_drop_intents,
            "
    seq INTEGER NOT NULL,
    namespace TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    size INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('drop', 'cache', 'pin')),
    PRIMARY KEY (seq, namespace, blob_id, content_hash)
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
            blob_uploaders,
            "
    namespace TEXT NOT NULL,
    blob_id   TEXT NOT NULL,
    -- Exact immutable cloud location: member prefix, object generation, and the
    -- HLC version that orders replacements. Recorded only from signed changeset metadata or our own upload;
    -- never inferred by scanning an untrusted listing. Preserved into snapshots
    -- so restored devices inherit the exact location of every active blob.
    uploader  TEXT NOT NULL,
    generation TEXT NOT NULL,
    version TEXT NOT NULL,
    PRIMARY KEY (namespace, blob_id)
"
        );
        $visit!(
            blob_location_versions,
            "
    namespace TEXT NOT NULL,
    blob_id   TEXT NOT NULL,
    version   TEXT NOT NULL,
    PRIMARY KEY (namespace, blob_id)
"
        );
    };
}

/// Creates coven's bookkeeping tables before the host's own migrations.
/// Idempotent (`IF NOT EXISTS`). STRICT: every column here is already
/// TEXT/INTEGER/BLOB, so STRICT only forecloses a future column drifting off
/// its declared affinity.
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

#[derive(Debug, Eq, PartialEq)]
struct SchemaObject {
    kind: String,
    name: String,
    table: String,
    sql: Option<String>,
}

/// Whether the database already contains any part of coven's bookkeeping
/// schema. A database with none is a fresh store; a database with any must have
/// the complete current schema before open is allowed to touch it.
pub(crate) fn has_coven_schema(conn: &rusqlite::Connection) -> Result<bool, String> {
    for table in table_names() {
        let exists = conn
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
                [table],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|error| format!("inspect bookkeeping schema for {table}: {error}"))?;
        if exists {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Require every coven-owned table and index to match the schema produced by
/// [`apply_coven_schema`]. This deliberately has no migration path: a partly old,
/// partly new bookkeeping schema cannot be interpreted without guessing which
/// invariants its rows satisfy.
pub(crate) fn validate_coven_schema(conn: &rusqlite::Connection) -> Result<(), String> {
    let expected_conn = rusqlite::Connection::open_in_memory()
        .map_err(|error| format!("open current bookkeeping schema fixture: {error}"))?;
    apply_coven_schema(&expected_conn)
        .map_err(|error| format!("create current bookkeeping schema fixture: {error}"))?;

    let expected = schema_objects(&expected_conn)?;
    let actual = schema_objects(conn)?;
    if expected == actual {
        return Ok(());
    }

    for found in &actual {
        match expected
            .iter()
            .find(|candidate| candidate.kind == found.kind && candidate.name == found.name)
        {
            None => {
                return Err(format!(
                    "bookkeeping schema is non-current: unexpected {} {} for table {}: {found:?}",
                    found.kind, found.name, found.table
                ));
            }
            Some(object) if found != object => {
                return Err(format!(
                    "bookkeeping schema is non-current at {} {} for table {}; expected {object:?}, found {found:?}",
                    object.kind, object.name, object.table
                ));
            }
            Some(_) => {}
        }
    }

    let missing = expected
        .iter()
        .find(|object| {
            !actual
                .iter()
                .any(|candidate| candidate.kind == object.kind && candidate.name == object.name)
        })
        .expect("unequal schemas contain a missing object");
    Err(format!(
        "bookkeeping schema is non-current: missing {} {} for table {}; expected {missing:?}",
        missing.kind, missing.name, missing.table
    ))
}

fn schema_objects(conn: &rusqlite::Connection) -> Result<Vec<SchemaObject>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT type, name, tbl_name, sql
             FROM sqlite_schema
             WHERE type IN ('table', 'index')
             ORDER BY type, name",
        )
        .map_err(|error| format!("prepare bookkeeping schema inspection: {error}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(SchemaObject {
                kind: row.get(0)?,
                name: row.get(1)?,
                table: row.get(2)?,
                sql: row
                    .get::<_, Option<String>>(3)?
                    .map(|sql| canonical_sql(&sql)),
            })
        })
        .map_err(|error| format!("inspect bookkeeping schema: {error}"))?;

    let mut objects = Vec::new();
    for row in rows {
        let object = row.map_err(|error| format!("read bookkeeping schema: {error}"))?;
        if is_reserved_table_name(&object.table) {
            objects.push(object);
        }
    }
    Ok(objects)
}

fn canonical_sql(sql: &str) -> String {
    let mut canonical = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut quoted = false;
    while let Some(ch) = chars.next() {
        if !quoted && ch == '-' && chars.peek() == Some(&'-') {
            chars.next();
            for comment in chars.by_ref() {
                if comment == '\n' {
                    break;
                }
            }
            continue;
        }
        if ch == '\'' {
            quoted = !quoted;
            canonical.push(ch);
        } else if quoted {
            canonical.push(ch);
        } else if !ch.is_whitespace() {
            canonical.extend(ch.to_lowercase());
        }
    }
    canonical
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
    false
}

/// The name of every table [`apply_coven_schema`] creates, for a test to assert a
/// schema property (STRICT) holds across all of them without re-listing the set
/// by hand.
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
    fn fresh_blob_location_schema_requires_generation_version_and_upload_hash() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        apply_coven_schema(&conn).unwrap();
        let mut stmt = conn.prepare("PRAGMA table_info(blob_uploaders)").unwrap();
        let not_null = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(1)?, row.get::<_, i64>(3)?))
            })
            .unwrap()
            .collect::<Result<std::collections::HashMap<_, _>, _>>()
            .unwrap();
        assert_eq!(not_null.get("generation"), Some(&1));
        assert_eq!(not_null.get("version"), Some(&1));
        assert!(conn.execute(
            "INSERT INTO blob_uploaders (namespace, blob_id, uploader) VALUES ('photos', 'p1', 'aa11')",
            [],
        ).is_err());
        assert!(conn
            .execute(
                "INSERT INTO cloud_outbox (operation, file_id, cloud_key, scope, created_at) \
             VALUES ('upload', 'p1', 'photos/key', 'master', 'stamp')",
                [],
            )
            .is_err());
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
/// belong to it (no upload-only `scope` on a delete).
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
        /// Author-signed plaintext hash this upload must match.
        expected_hash: String,
        /// The blob's encryption scope, named by the host at enqueue. An upload
        /// always has one — a delete, which touches no key, has none.
        scope: crate::blob::BlobScope,
        /// Whether the drain populates the protected cache folder
        /// (`storage/pinned/<namespace>/<id>/<content_hash>`) from the plaintext on a successful
        /// upload, so a pinned Remote blob is kept local and budget-exempt with no
        /// later cloud round-trip. `false` populates nothing on write — the evictable
        /// `storage/cache/<namespace>/<id>/<content_hash>` fills on a later read-miss instead.
        retain_pinned: bool,
    },
    /// Delete a cloud blob. The drain turns it into a signed cloud tombstone (the
    /// deletion's durable record); a later GC reclaims the blob once a convergence
    /// grace has passed, so a peer that still references it isn't stranded. See
    /// [`crate::blob::delete`]. Carries no extra fields.
    Delete,
}
