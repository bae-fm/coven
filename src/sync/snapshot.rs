/// Snapshots and garbage collection for the sync system.
///
/// Periodically, a device creates a full snapshot of the database via
/// `VACUUM INTO`, seals it through the home's [`CloudCipher`], and uploads it as
/// `snapshot.db{suffix}` (`.enc` for an encrypted home, no suffix for a
/// plaintext one). This allows new devices to bootstrap without replaying the
/// entire changeset history, and enables GC of old changesets.
///
/// Snapshot creation policy: after every N changesets (default 100) or
/// T hours (default 24) since the last snapshot.
use std::collections::HashMap;
use std::path::Path;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::cloud_storage::CloudCipher;
use super::session::SyncedTable;
use super::storage::{StorageError, SyncStorage};

/// Default: create a snapshot after this many changesets since the last one.
const SNAPSHOT_CHANGESET_THRESHOLD: u64 = 100;

/// Default: create a snapshot after this many hours since the last one.
const SNAPSHOT_HOURS_THRESHOLD: u64 = 24;

/// Error type for snapshot operations.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("VACUUM INTO failed: {0}")]
    VacuumFailed(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage error: {0}")]
    Bucket(#[from] StorageError),
    #[error("decryption failed: {0}")]
    Decryption(String),
    /// No synced tables were registered, so we cannot determine which tables
    /// are safe to share. Emitting a snapshot here would either leak every
    /// local-only table or clear the whole DB — both wrong, so we refuse.
    #[error("no synced tables registered; refusing to emit an all-cleared snapshot")]
    NoSyncedTables,
    /// Scoping the snapshot copy down to shareable data failed (sqlite FFI):
    /// either clearing local-only tables or applying the row-level gate that
    /// excludes gated-false subtrees (the changeset gate cuts them too).
    #[error("failed to scope snapshot down to shareable data: {0}")]
    ClearFailed(String),
}

/// Metadata stored alongside a snapshot in `snapshot_meta.json{suffix}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// Per-device cursors at snapshot time: device_id -> head seq.
    /// A bootstrapping device uses these as initial sync_cursors.
    pub cursors: HashMap<String, u64>,
    /// RFC 3339 timestamp when the snapshot was created.
    pub created_at: String,
}

/// Result of bootstrapping from a snapshot.
#[derive(Debug)]
pub struct BootstrapResult {
    /// Per-device cursors from the snapshot metadata.
    /// The bootstrapping device should use these as initial sync_cursors.
    pub cursors: HashMap<String, u64>,
}

/// Create a snapshot of the database as bytes sealed for storage.
///
/// Uses `VACUUM INTO` to create a clean copy of the database at a temp path,
/// then clears every non-synced table's data from that copy, reads the bytes,
/// seals them through the home's [`CloudCipher`] (encrypting for an encrypted
/// home, verbatim for a plaintext one), and returns the sealed blob.
///
/// A snapshot is restored byte-for-byte as the joining device's `library.db`
/// (no migration rebuild), so it must carry only data that is eligible to
/// cross devices — the host's declared synced tables. Local-only tables
/// (per-device paths, caches) and per-device sync bookkeeping must not ride
/// along; their schemas are kept, but their rows are deleted from the copy.
///
/// `conn` is the owned live connection; `tables` is the host's synced set.
pub fn create_snapshot(
    conn: &Connection,
    temp_dir: &Path,
    tables: &[SyncedTable],
    cipher: &CloudCipher,
) -> Result<Vec<u8>, SnapshotError> {
    // A snapshot with no synced set would either leak every local-only table or
    // clear the whole DB — both wrong. Refuse before doing any work.
    if tables.is_empty() {
        return Err(SnapshotError::NoSyncedTables);
    }

    let snapshot_path = temp_dir.join("snapshot.db");
    let path_str = snapshot_path
        .to_str()
        .expect("temp path should be valid UTF-8");

    // Remove any leftover snapshot file from a previous failed attempt.
    let _ = std::fs::remove_file(&snapshot_path);

    // VACUUM INTO creates a clean, defragmented copy of the live database.
    let vacuum = format!("VACUUM INTO '{}'", path_str.replace('\'', "''"));
    if let Err(e) = conn.execute_batch(&vacuum) {
        if let Err(rm) = std::fs::remove_file(&snapshot_path) {
            warn!(error = %rm, "failed to remove temp snapshot after VACUUM error");
        }
        return Err(SnapshotError::VacuumFailed(e.to_string()));
    }

    // The copy is a whole-DB byte image, so it still holds every local-only
    // table's data. Strip those before reading: open the copy as its own
    // connection and DELETE from every table outside the synced set.
    if let Err(e) = clear_local_only_tables(&snapshot_path, tables) {
        if let Err(rm) = std::fs::remove_file(&snapshot_path) {
            warn!(error = %rm, "failed to remove temp snapshot after clear error");
        }
        return Err(e);
    }

    // Read the cleared snapshot file and seal it for storage.
    let plaintext = std::fs::read(&snapshot_path)?;
    let _ = std::fs::remove_file(&snapshot_path);

    let sealed = cipher.seal(&plaintext);

    info!(
        plaintext_size = plaintext.len(),
        sealed_size = sealed.len(),
        "created snapshot"
    );

    Ok(sealed)
}

/// Delete every non-synced table's rows from the snapshot copy at `path`,
/// keeping all table schemas intact.
///
/// Opens `path` as its own connection (the copy must be edited in isolation from
/// the live DB). Errors if any step fails — a snapshot that silently dropped
/// synced data, or silently kept local-only data, is worse than no snapshot.
fn clear_local_only_tables(path: &Path, synced: &[SyncedTable]) -> Result<(), SnapshotError> {
    let conn = Connection::open(path)
        .map_err(|e| SnapshotError::ClearFailed(format!("failed to open snapshot copy: {e}")))?;
    clear_non_synced(&conn, synced)?;
    conn.close()
        .map_err(|(_, e)| SnapshotError::ClearFailed(format!("failed to close snapshot copy: {e}")))
}

/// On the snapshot-copy connection, scope it down to exactly what is eligible to
/// cross devices, then VACUUM to reclaim the freed pages:
///
/// 1. Table-level: DELETE every user table not in `synced` — local-only tables
///    keep their schema, lose their rows.
/// 2. Row-level: within the synced tables, DELETE the rows the gate excludes
///    (gated-false roots and their FK-descendants), so a private subtree does
///    not ride the snapshot to a restoring peer. This is the same exclusion the
///    outbound changeset gate applies; both reuse [`crate::sync::gate::Gates`].
fn clear_non_synced(conn: &Connection, synced: &[SyncedTable]) -> Result<(), SnapshotError> {
    for table in list_user_tables(conn)? {
        if synced.iter().any(|t| t.name() == table) {
            continue;
        }
        conn.execute_batch(&format!(
            "DELETE FROM {}",
            crate::sync::session::quote_ident(&table)
        ))
        .map_err(|e| SnapshotError::ClearFailed(format!("clear {table}: {e}")))?;
    }

    // The snapshot is a second propagation channel: the changeset gate cuts
    // gated-false rows on the wire, so the snapshot must drop them too or a
    // private subtree leaks to a restoring device. Reuse the changeset gate's
    // model rather than re-deriving the FK walk.
    let gates = crate::sync::gate::Gates::from_tables(conn, synced)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;
    gates
        .delete_gated_false(conn)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;

    // Reclaim the pages freed by the DELETEs so the blob shrinks.
    conn.execute_batch("VACUUM")
        .map_err(|e| SnapshotError::ClearFailed(format!("vacuum: {e}")))?;
    Ok(())
}

/// List user table names (excluding sqlite internal `sqlite_%` tables).
fn list_user_tables(conn: &Connection) -> Result<Vec<String>, SnapshotError> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        .map_err(|e| SnapshotError::ClearFailed(format!("prepare table list: {e}")))?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| SnapshotError::ClearFailed(format!("query table list: {e}")))?;
    let mut tables = Vec::new();
    for row in rows {
        tables.push(row.map_err(|e| SnapshotError::ClearFailed(format!("step table list: {e}")))?);
    }
    Ok(tables)
}

/// Upload a snapshot to the sync storage and update the device head.
///
/// Also uploads per-device cursor metadata (`snapshot_meta.json{suffix}`) so
/// that bootstrapping devices know where each device was at snapshot time, and
/// GC can safely delete only changesets covered by the snapshot.
pub async fn push_snapshot(
    storage: &dyn SyncStorage,
    sealed_snapshot: Vec<u8>,
    device_id: &str,
    applied_cursors: HashMap<String, u64>,
    current_seq: u64,
    clock: &dyn crate::clock::Clock,
) -> Result<(), SnapshotError> {
    let size = sealed_snapshot.len();
    let timestamp = clock.now().to_rfc3339();

    // Upload snapshot (overwrites previous).
    storage.put_snapshot(sealed_snapshot).await?;

    // The snapshot DB is a VACUUM of this device's live database, so its
    // metadata must describe exactly what THIS device has applied — never
    // other devices' published heads, which may be ahead of what we pulled.
    // Claiming coverage we don't have lets GC delete un-snapshotted changesets
    // that no future restore can recover.
    let mut cursors = applied_cursors;
    // Our own current_seq is included (our head hasn't been updated yet).
    cursors.insert(device_id.to_string(), current_seq);

    let meta = SnapshotMeta {
        cursors: cursors.clone(),
        created_at: timestamp.clone(),
    };
    let meta_json =
        serde_json::to_vec(&meta).map_err(|e| SnapshotError::Io(std::io::Error::other(e)))?;

    storage.put_snapshot_meta(meta_json).await?;

    // Update the head to record this snapshot's coverage (snapshot_seq).
    storage
        .put_head(device_id, current_seq, Some(current_seq), &timestamp)
        .await?;

    info!(
        device_id,
        snapshot_seq = current_seq,
        size,
        "pushed snapshot to sync storage"
    );

    Ok(())
}

/// Check whether it's time to create a new snapshot.
///
/// Returns true if:
/// - `changesets_since_snapshot` >= the changeset threshold (100), OR
/// - `hours_since_snapshot` >= the time threshold (24h), OR
/// - No snapshot has ever been created (`last_snapshot_seq` is None)
///   AND at least one changeset has been pushed.
pub fn should_create_snapshot(
    local_seq: u64,
    last_snapshot_seq: Option<u64>,
    hours_since_snapshot: Option<u64>,
) -> bool {
    // Never created a snapshot, and we have at least one changeset.
    let Some(snap_seq) = last_snapshot_seq else {
        return local_seq > 0;
    };

    let changesets_since = local_seq.saturating_sub(snap_seq);
    if changesets_since >= SNAPSHOT_CHANGESET_THRESHOLD {
        return true;
    }

    if let Some(hours) = hours_since_snapshot {
        if hours >= SNAPSHOT_HOURS_THRESHOLD && changesets_since > 0 {
            return true;
        }
    }

    false
}

/// Delete changesets that are superseded by a snapshot.
///
/// Reads snapshot metadata to get per-device cursors at snapshot time.
/// For each device, only deletes changesets with seq <= the device's cursor
/// in the snapshot. This ensures changesets pushed AFTER the snapshot are
/// preserved, even if their seq is below another device's snapshot seq.
///
/// Devices that don't appear in the snapshot metadata are skipped entirely
/// (they appeared after the snapshot was created).
pub async fn garbage_collect(storage: &dyn SyncStorage) -> Result<GcResult, SnapshotError> {
    // Read snapshot metadata.
    let meta_json = match storage.get_snapshot_meta().await {
        Ok(data) => data,
        Err(StorageError::NotFound(_)) => {
            // No snapshot metadata -- nothing to GC.
            info!("no snapshot metadata found, skipping GC");
            return Ok(GcResult {
                deleted: 0,
                errors: 0,
            });
        }
        Err(e) => return Err(SnapshotError::Bucket(e)),
    };

    let meta: SnapshotMeta = serde_json::from_slice(&meta_json)
        .map_err(|e| SnapshotError::Io(std::io::Error::other(e)))?;

    let heads = storage.list_heads().await?;
    let mut deleted = 0u64;
    let mut errors = 0u64;

    for head in &heads {
        // Only GC changesets up to what the snapshot covers for THIS device.
        let safe_seq = match meta.cursors.get(&head.device_id) {
            Some(&seq) => seq,
            None => continue, // Device appeared after snapshot -- don't touch.
        };

        let seqs = match storage.list_changesets(&head.device_id).await {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    device_id = %head.device_id,
                    error = %e,
                    "failed to list changesets for GC, skipping device"
                );
                errors += 1;
                continue;
            }
        };

        for seq in seqs {
            if seq > safe_seq {
                continue;
            }

            match storage.delete_changeset(&head.device_id, seq).await {
                Ok(()) => deleted += 1,
                Err(e) => {
                    warn!(
                        device_id = %head.device_id,
                        seq,
                        error = %e,
                        "failed to delete changeset during GC"
                    );
                    errors += 1;
                }
            }
        }
    }

    info!(deleted, errors, "garbage collection complete");

    Ok(GcResult { deleted, errors })
}

/// Result of a garbage collection run.
#[derive(Debug, PartialEq, Eq)]
pub struct GcResult {
    /// Number of changesets successfully deleted.
    pub deleted: u64,
    /// Number of errors encountered (logged but not fatal).
    pub errors: u64,
}

/// Bootstrap a new device from a snapshot.
///
/// Downloads the snapshot, opens it through the home's [`CloudCipher`]
/// (decrypting for an encrypted home, verbatim for a plaintext one), and writes
/// the plaintext database to `target_path`. The caller should then open this as
/// their local database and pull any changesets newer than the per-device
/// cursors in the result.
///
/// Returns a `BootstrapResult` with per-device cursors so the caller knows
/// where to start pulling changesets from each device.
pub async fn bootstrap_from_snapshot(
    storage: &dyn SyncStorage,
    cipher: &CloudCipher,
    target_path: &Path,
) -> Result<BootstrapResult, SnapshotError> {
    // Download both the snapshot blob and its per-device cursor metadata
    // before touching disk. `push_snapshot` writes the metadata immediately
    // after the snapshot blob, so its absence here means the bucket is in
    // a torn state (e.g., a previous push failed between the two uploads).
    // We refuse to bootstrap from incomplete data, and we fetch metadata
    // first so we don't leave a half-written DB on the target path.
    let meta_json = storage
        .get_snapshot_meta()
        .await
        .map_err(SnapshotError::Bucket)?;
    let meta: SnapshotMeta = serde_json::from_slice(&meta_json)
        .map_err(|e| SnapshotError::Io(std::io::Error::other(e)))?;
    let cursors = meta.cursors;

    let sealed = storage.get_snapshot().await?;
    let plaintext = cipher
        .open(&sealed)
        .map_err(|e| SnapshotError::Decryption(e.to_string()))?;

    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(target_path, &plaintext)?;

    info!(
        num_devices = cursors.len(),
        db_size = plaintext.len(),
        path = %target_path.display(),
        "bootstrapped from snapshot"
    );

    Ok(BootstrapResult { cursors })
}

/// `sync_state` key holding `"1"` while the snapshot blob reconciliation has not
/// yet fully succeeded for this library, absent once it has. A bootstrap sets it;
/// each sync cycle runs the reconciliation while it is set and clears it on the
/// first run that lands every referenced blob. See [`reconcile_snapshot_blobs`].
pub(crate) const SNAPSHOT_BLOB_BACKFILL_PENDING: &str = "snapshot_blob_backfill_pending";

/// Download the blob files the DB at `db_path` references but whose local file is
/// absent, returning true once every referenced blob is on local disk.
///
/// `bootstrap_from_snapshot` writes only the catalog DB; the incremental pull
/// that follows starts past the snapshot's per-device cursors, so the original
/// INSERT changesets that carried each row's image/torrent blob (seq <= cursor)
/// are never re-walked and the per-changeset blob download never fires for them.
/// Without this reconciliation a bootstrapped device has the rows but none of the
/// files they point at (a synced album shows a placeholder cover). Audio is
/// unaffected: a host's [`crate::blob::BlobPlan`] excludes audio from the blobs
/// it wants local (it streams on demand), so `blobs_in_db` does not list it.
///
/// Reads the blobs the host's plan finds in the DB at `db_path`, then downloads
/// each via the same [`crate::sync::pull::download_blobs`] path the incremental
/// pull uses (skipping any whose local file already exists). A failed download is
/// logged there and reflected in the returned flag; the bootstrap that calls this
/// records the not-yet-complete state in [`SNAPSHOT_BLOB_BACKFILL_PENDING`], and
/// each subsequent sync cycle re-runs this until it returns true, so a blob whose
/// object was not yet in the cloud (or whose download hit a transient error) at
/// bootstrap is fetched on a later cycle rather than lost. A clear flag means no
/// cycle runs this, so a caught-up library pays nothing.
///
/// `blobs_in_db` is a read-only enumeration the host's plan runs against a
/// short-lived connection to the same on-disk DB the `db` actor owns; `db` is
/// still needed because `download_blobs` resolves each blob's scope through it
/// (an `Item`-scoped blob reads its key from the `item_keys` rows). At bootstrap
/// capture is suspended and the pull has not started; in a cycle this runs after
/// the pull's span has resumed capture, and is read-only either way, so it does
/// not re-record rows or race the actor.
pub(crate) async fn reconcile_snapshot_blobs(
    db: &crate::database::Database,
    db_path: &Path,
    storage: &dyn SyncStorage,
    blob_plan: &dyn crate::blob::BlobPlan,
) -> Result<bool, crate::database::DbError> {
    let blobs = {
        let conn = Connection::open(db_path).map_err(crate::database::DbError::from)?;
        blob_plan
            .blobs_in_db(&conn)
            .map_err(crate::database::DbError::from)?
    };

    if blobs.is_empty() {
        return Ok(true);
    }

    let total = blobs.len();
    // No in-changeset key map here: a snapshot's blobs take their keys from the
    // `item_keys` rows the snapshot itself carried into this DB, so resolution
    // goes through the DB (issue #111's pull path uses the map for keys minted in
    // the changeset being applied; the bootstrap has no such changeset).
    let all_ok =
        crate::sync::pull::download_blobs(db, blobs, storage, &std::collections::HashMap::new())
            .await;
    if all_ok {
        info!(total, "snapshot blob reconciliation complete");
    } else {
        warn!(
            total,
            "some snapshot blob files are not yet local; a later sync cycle reconciles them"
        );
    }
    Ok(all_ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::EncryptionService;
    use crate::sync::apply::apply_changeset_lww;
    use crate::sync::storage::{DeviceHead, MinSchemaVersion};
    use async_trait::async_trait;
    use rusqlite::session::Session as RqSession;
    use rusqlite::{Connection, OptionalExtension};
    use std::collections::HashMap;
    use std::sync::Mutex;

    // ---- in-process db helpers (rusqlite, the new `&Connection` API) ----

    /// The synthetic synced set the snapshot tests scope by.
    fn synced_tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("notes").gated_by("shared"),
            SyncedTable::new("note_tags"),
            SyncedTable::new("note_photos"),
        ]
    }

    /// A fresh in-memory connection with `foreign_keys=ON` and the synthetic
    /// notes/note_tags/note_photos schema.
    fn synced_conn() -> Connection {
        let c = Connection::open_in_memory().expect("open in-memory");
        c.execute_batch(
            "PRAGMA foreign_keys = ON;
            CREATE TABLE notes (
                id TEXT PRIMARY KEY, title TEXT NOT NULL, body TEXT,
                shared INTEGER NOT NULL DEFAULT 0,
                _updated_at TEXT NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TABLE note_tags (
                id TEXT PRIMARY KEY, note_id TEXT NOT NULL, tag TEXT NOT NULL,
                _updated_at TEXT NOT NULL, created_at TEXT NOT NULL,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            );
            CREATE TABLE note_photos (
                id TEXT PRIMARY KEY, note_id TEXT NOT NULL, kind TEXT NOT NULL,
                _updated_at TEXT NOT NULL, created_at TEXT NOT NULL,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            );",
        )
        .expect("create schema");
        c
    }

    /// Open a SQLite database file by path as a standalone connection.
    fn open_db_at(path: &Path) -> Connection {
        Connection::open(path).expect("open db file")
    }

    fn exec(c: &Connection, sql: &str) {
        c.execute_batch(sql)
            .unwrap_or_else(|e| panic!("exec failed for {sql}: {e}"));
    }

    fn query_text(c: &Connection, sql: &str) -> String {
        c.query_row(sql, [], |r| r.get::<_, String>(0))
            .unwrap_or_else(|e| panic!("query_text failed for {sql}: {e}"))
    }

    fn query_int(c: &Connection, sql: &str) -> i64 {
        c.query_row(sql, [], |r| r.get::<_, i64>(0))
            .unwrap_or_else(|e| panic!("query_int failed for {sql}: {e}"))
    }

    fn row_exists(c: &Connection, sql: &str) -> bool {
        c.query_row(sql, [], |_| Ok(()))
            .optional()
            .unwrap_or_else(|e| panic!("row_exists failed for {sql}: {e}"))
            .is_some()
    }

    /// Apply a changeset's bytes with the production LWW path scoped to the test
    /// synced set.
    fn apply(c: &Connection, bytes: &[u8]) {
        apply_changeset_lww(c, bytes, &synced_tables(), crate::sync::hlc::now_wall_ms())
            .expect("apply changeset");
    }

    /// Record a changeset over the synced tables while `body` runs SQL against a
    /// fresh schema-only connection. Returns the recorded bytes.
    fn changeset_bytes_for(body: impl FnOnce(&Connection)) -> Vec<u8> {
        let c = synced_conn();
        let mut session = RqSession::new(&c).expect("session");
        for t in synced_tables() {
            session.attach(Some(t.name())).expect("attach");
        }
        body(&c);
        let mut buf = Vec::new();
        session.changeset_strm(&mut buf).expect("changeset");
        buf
    }

    /// Full-featured mock storage for snapshot tests.
    struct MockSyncStorage {
        changesets: Mutex<HashMap<String, Vec<u8>>>,
        heads: Mutex<HashMap<String, (u64, Option<u64>)>>,
        snapshot: Mutex<Option<Vec<u8>>>,
        snapshot_meta: Mutex<Option<Vec<u8>>>,
        min_schema_version: Mutex<Option<u32>>,
    }

    impl MockSyncStorage {
        fn new() -> Self {
            MockSyncStorage {
                changesets: Mutex::new(HashMap::new()),
                heads: Mutex::new(HashMap::new()),
                snapshot: Mutex::new(None),
                snapshot_meta: Mutex::new(None),
                min_schema_version: Mutex::new(None),
            }
        }

        /// Helper to add a changeset directly.
        fn add_changeset(&self, device_id: &str, seq: u64, data: Vec<u8>) {
            let key = format!("{device_id}/{seq}");
            self.changesets.lock().unwrap().insert(key, data);

            let mut heads = self.heads.lock().unwrap();
            let entry = heads.entry(device_id.to_string()).or_insert((0, None));
            if seq > entry.0 {
                entry.0 = seq;
            }
        }

        /// Count remaining changesets.
        fn changeset_count(&self) -> usize {
            self.changesets.lock().unwrap().len()
        }

        /// Get stored snapshot data.
        fn get_stored_snapshot(&self) -> Option<Vec<u8>> {
            self.snapshot.lock().unwrap().clone()
        }

        /// Get stored snapshot metadata.
        fn get_stored_snapshot_meta(&self) -> Option<Vec<u8>> {
            self.snapshot_meta.lock().unwrap().clone()
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl SyncStorage for MockSyncStorage {
        async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError> {
            let heads = self.heads.lock().unwrap();
            Ok(heads
                .iter()
                .map(|(id, (seq, snap))| DeviceHead {
                    device_id: id.clone(),
                    seq: *seq,
                    snapshot_seq: *snap,
                    last_sync: None,
                    author_pubkey: String::new(),
                })
                .collect())
        }

        async fn get_changeset(&self, device_id: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
            let key = format!("{device_id}/{seq}");
            let cs = self.changesets.lock().unwrap();
            cs.get(&key).cloned().ok_or(StorageError::NotFound(key))
        }

        async fn put_changeset(
            &self,
            device_id: &str,
            seq: u64,
            data: Vec<u8>,
        ) -> Result<(), StorageError> {
            let key = format!("{device_id}/{seq}");
            self.changesets.lock().unwrap().insert(key, data);
            Ok(())
        }

        async fn put_head(
            &self,
            device_id: &str,
            seq: u64,
            snapshot_seq: Option<u64>,
            _timestamp: &str,
        ) -> Result<(), StorageError> {
            let mut heads = self.heads.lock().unwrap();
            let entry = heads.entry(device_id.to_string()).or_insert((0, None));
            entry.0 = seq;
            if snapshot_seq.is_some() {
                entry.1 = snapshot_seq;
            }
            Ok(())
        }

        async fn put_blob(
            &self,
            _namespace: &str,
            _id: &str,
            _scope: crate::blob::ResolvedScope,
            _cloud_path: Option<&str>,
            _data: Vec<u8>,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn get_blob(
            &self,
            namespace: &str,
            id: &str,
            _scope: crate::blob::ResolvedScope,
            _cloud_path: Option<&str>,
        ) -> Result<Vec<u8>, StorageError> {
            Err(StorageError::NotFound(format!("{namespace}/{id}")))
        }

        async fn put_snapshot(&self, data: Vec<u8>) -> Result<(), StorageError> {
            *self.snapshot.lock().unwrap() = Some(data);
            Ok(())
        }

        async fn get_snapshot(&self) -> Result<Vec<u8>, StorageError> {
            self.snapshot
                .lock()
                .unwrap()
                .clone()
                .ok_or(StorageError::NotFound("snapshot.db".into()))
        }

        async fn delete_changeset(&self, device_id: &str, seq: u64) -> Result<(), StorageError> {
            let key = format!("{device_id}/{seq}");
            self.changesets.lock().unwrap().remove(&key);
            Ok(())
        }

        async fn list_changesets(&self, device_id: &str) -> Result<Vec<u64>, StorageError> {
            let prefix = format!("{device_id}/");
            let cs = self.changesets.lock().unwrap();
            let mut seqs: Vec<u64> = cs
                .keys()
                .filter_map(|k| k.strip_prefix(&prefix).and_then(|s| s.parse().ok()))
                .collect();
            seqs.sort();
            Ok(seqs)
        }

        async fn get_min_schema_version(&self) -> Result<Option<MinSchemaVersion>, StorageError> {
            Ok(self
                .min_schema_version
                .lock()
                .unwrap()
                .map(|version| MinSchemaVersion {
                    version,
                    author_pubkey: String::new(),
                }))
        }

        async fn set_min_schema_version(&self, version: u32) -> Result<(), StorageError> {
            *self.min_schema_version.lock().unwrap() = Some(version);
            Ok(())
        }

        async fn put_membership_entry(
            &self,
            _author_pubkey: &str,
            _seq: u64,
            _data: Vec<u8>,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn get_membership_entry(
            &self,
            author_pubkey: &str,
            seq: u64,
        ) -> Result<Vec<u8>, StorageError> {
            Err(StorageError::NotFound(format!(
                "membership/{author_pubkey}/{seq}"
            )))
        }

        async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError> {
            Ok(vec![])
        }

        async fn put_wrapped_key(
            &self,
            _user_pubkey: &str,
            _data: Vec<u8>,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn get_wrapped_key(&self, user_pubkey: &str) -> Result<Vec<u8>, StorageError> {
            Err(StorageError::NotFound(format!("keys/{user_pubkey}")))
        }

        async fn delete_wrapped_key(&self, _user_pubkey: &str) -> Result<(), StorageError> {
            Ok(())
        }

        async fn put_snapshot_meta(&self, data: Vec<u8>) -> Result<(), StorageError> {
            *self.snapshot_meta.lock().unwrap() = Some(data);
            Ok(())
        }

        async fn get_snapshot_meta(&self) -> Result<Vec<u8>, StorageError> {
            self.snapshot_meta
                .lock()
                .unwrap()
                .clone()
                .ok_or(StorageError::NotFound("snapshot_meta.json".into()))
        }
    }

    /// An encrypted-home cipher over a fixed key, the default the snapshot tests
    /// run against. Plaintext-home snapshot round-tripping is covered end-to-end
    /// through the real cycle in `delete_propagation_tests`.
    fn test_encryption() -> CloudCipher {
        CloudCipher::Encrypted(EncryptionService::new_with_key(&[0x42u8; 32]))
    }

    // ---- should_create_snapshot tests ----

    #[test]
    fn snapshot_policy_no_previous_snapshot_with_changes() {
        assert!(should_create_snapshot(1, None, None));
        assert!(should_create_snapshot(50, None, None));
    }

    #[test]
    fn snapshot_policy_no_previous_snapshot_no_changes() {
        assert!(!should_create_snapshot(0, None, None));
    }

    #[test]
    fn snapshot_policy_below_threshold() {
        // 10 changesets since last snapshot, only 1 hour elapsed.
        assert!(!should_create_snapshot(60, Some(50), Some(1)));
    }

    #[test]
    fn snapshot_policy_changeset_threshold_reached() {
        // Exactly 100 changesets since snapshot.
        assert!(should_create_snapshot(150, Some(50), Some(1)));
        // Over 100.
        assert!(should_create_snapshot(200, Some(50), Some(1)));
    }

    #[test]
    fn snapshot_policy_time_threshold_reached() {
        // Only 10 changesets but 24+ hours have passed.
        assert!(should_create_snapshot(60, Some(50), Some(24)));
        assert!(should_create_snapshot(60, Some(50), Some(48)));
    }

    #[test]
    fn snapshot_policy_time_threshold_no_new_changes() {
        // 24 hours but zero changesets since snapshot.
        assert!(!should_create_snapshot(50, Some(50), Some(24)));
    }

    // ---- create_snapshot tests ----

    #[test]
    fn create_snapshot_produces_encrypted_db() {
        let c = synced_conn();
        exec(
            &c,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'Note One', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&c, temp.path(), &synced_tables(), &enc).expect("create_snapshot");

        assert!(!encrypted.is_empty());
        let plaintext = enc.open(&encrypted).expect("open should succeed");
        assert!(!plaintext.is_empty());
        assert!(
            plaintext.starts_with(b"SQLite format 3\0"),
            "snapshot should be a valid SQLite database"
        );
    }

    #[test]
    fn create_snapshot_contains_data() {
        let c = synced_conn();
        exec(
            &c,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        );
        exec(
            &c,
            "INSERT INTO note_tags (id, tag, note_id, _updated_at, created_at) \
             VALUES ('al1', 'Album One', 'a1', '0000000001000-0000-dev1', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let enc = test_encryption();
        let encrypted = create_snapshot(&c, temp.path(), &synced_tables(), &enc).expect("snapshot");
        let plaintext = enc.open(&encrypted).expect("open");

        let db_path = temp.path().join("verify.db");
        std::fs::write(&db_path, &plaintext).unwrap();
        let db2 = open_db_at(&db_path);

        assert_eq!(
            query_text(&db2, "SELECT title FROM notes WHERE id = 'a1'"),
            "Artist One"
        );
        assert_eq!(
            query_text(&db2, "SELECT tag FROM note_tags WHERE id = 'al1'"),
            "Album One"
        );
    }

    /// A snapshot is a propagation channel between devices, so it carries only
    /// synced-table data. A non-synced table (here `device_local`, holding a
    /// filesystem path meaningful only on the device that wrote it) keeps its
    /// schema in the restored DB but none of its rows: the schema survives so
    /// the table still opens, while its device-local rows never cross to a
    /// restoring peer.
    #[tokio::test]
    async fn snapshot_does_not_carry_local_only_tables_to_a_restoring_device() {
        // --- Device A: a synced table + a device-local table ---
        let db_a = synced_conn();
        exec(
            &db_a,
            "CREATE TABLE device_local (note_id TEXT PRIMARY KEY, local_path TEXT NOT NULL)",
        );
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n1', 'Gish', 1, '0000000001000-0000-devA', '2026-01-01')",
        );
        exec(
            &db_a,
            "INSERT INTO device_local (note_id, local_path) \
             VALUES ('n1', '/Users/dima/Torrents/Gish (1991)')",
        );

        let temp = tempfile::tempdir().unwrap();
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db_a, temp.path(), &synced_tables(), &enc).expect("snapshot");

        let storage = MockSyncStorage::new();
        push_snapshot(
            &storage,
            encrypted,
            "devA",
            HashMap::new(),
            1,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push_snapshot");

        let target = temp.path().join("device_b.db");
        bootstrap_from_snapshot(&storage, &enc, &target)
            .await
            .expect("bootstrap_from_snapshot");
        let db_b = open_db_at(&target);

        // Synced data SHOULD cross.
        assert_eq!(
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'"),
            "Gish",
            "synced-table data must survive a snapshot restore",
        );
        // Device-local data must NOT cross.
        assert!(
            !row_exists(&db_b, "SELECT 1 FROM device_local WHERE note_id = 'n1'"),
            "device-local row leaked to a peer via the snapshot",
        );
        // The table SCHEMA is preserved (only its rows are cleared).
        assert!(
            row_exists(
                &db_b,
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='device_local'",
            ),
            "non-synced table schema must survive: snapshot DELETEs rows, never DROPs tables",
        );
        assert_eq!(
            query_int(&db_b, "SELECT COUNT(*) FROM device_local"),
            0,
            "non-synced table must be empty in the restored DB",
        );
    }

    /// The snapshot is a second propagation channel, so it must honor the same
    /// row-level gate the outbound changeset does: a gated-false root (`notes`
    /// with `shared = 0`) and its FK-descendants (`note_tags`) are private and
    /// must never cross to a restoring device, while a gated-true root and its
    /// descendants must. The table-level clear keeps the `notes`/`note_tags`
    /// *schema* (both are synced tables); this verifies the *rows* are
    /// gate-scoped.
    #[tokio::test]
    async fn snapshot_does_not_carry_gated_false_rows_to_a_restoring_device() {
        let db_a = synced_conn();

        // A shared note with a child tag (both must cross).
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('pub', 'Public', 1, '0000000001000-0000-devA', '2026-01-01')",
        );
        exec(
            &db_a,
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
             VALUES ('pub_t', 'pub', 'green', '0000000001000-0000-devA', '2026-01-01')",
        );
        // A private note with its own child tag (neither may cross).
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('priv', 'Private', 0, '0000000002000-0000-devA', '2026-01-01')",
        );
        exec(
            &db_a,
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
             VALUES ('priv_t', 'priv', 'red', '0000000002000-0000-devA', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db_a, temp.path(), &synced_tables(), &enc).expect("snapshot");

        let storage = MockSyncStorage::new();
        push_snapshot(
            &storage,
            encrypted,
            "devA",
            HashMap::new(),
            1,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push_snapshot");

        let target = temp.path().join("device_b.db");
        bootstrap_from_snapshot(&storage, &enc, &target)
            .await
            .expect("bootstrap_from_snapshot");
        let db_b = open_db_at(&target);

        assert!(
            row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'pub'"),
            "a gated-true note must survive the snapshot restore",
        );
        assert!(
            row_exists(&db_b, "SELECT 1 FROM note_tags WHERE id = 'pub_t'"),
            "a gated-true note's FK-child must survive the snapshot restore",
        );
        assert!(
            !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'priv'"),
            "a gated-false note leaked to a peer via the snapshot",
        );
        assert!(
            !row_exists(&db_b, "SELECT 1 FROM note_tags WHERE id = 'priv_t'"),
            "a gated-false note's FK-descendant leaked to a peer via the snapshot",
        );
    }

    /// coven's own bookkeeping tables (`sync_state`, `sync_cursors`,
    /// `cloud_outbox`) are per-device: a peer's cursors, pending outbox, and HLC
    /// high-water must NOT ride a snapshot to a restoring device — inheriting them
    /// would make the new device think it had already pulled the snapshotter's
    /// peers, or replay the snapshotter's blob queue. They are not in the synced
    /// set, so the table-level clear must strip their rows while keeping the
    /// schemas (so the restored DB opens and coven can immediately write its own
    /// fresh bookkeeping). This guards that present-but-empty invariant, which the
    /// other snapshot tests miss because their schema omits coven's tables.
    #[tokio::test]
    async fn snapshot_does_not_carry_bookkeeping_tables_to_a_restoring_device() {
        let db_a = synced_conn();
        // Add coven's bookkeeping tables (the snapshot source normally has them;
        // the synthetic test schema doesn't) and populate every one.
        exec(&db_a, crate::db::MIGRATION_SQL);
        exec(
            &db_a,
            "INSERT INTO sync_state (key, value) VALUES \
             ('local_seq', '42'), ('hlc_high_water', '0000000009000-0000-devA')",
        );
        exec(
            &db_a,
            "INSERT INTO sync_cursors (device_id, last_seq) VALUES ('devB', 7), ('devC', 3)",
        );
        exec(
            &db_a,
            &format!(
                "INSERT INTO cloud_outbox (operation, file_id, cloud_key, scope, created_at) \
                 VALUES ('upload', 'f1', 'blobs/f1', '{}', '2026-01-01')",
                crate::blob::BlobScope::Master.to_outbox_str()
            ),
        );
        // A synced row that SHOULD cross, to prove the snapshot still carries data.
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n1', 'Keep me', 1, '0000000001000-0000-devA', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db_a, temp.path(), &synced_tables(), &enc).expect("snapshot");

        let storage = MockSyncStorage::new();
        push_snapshot(
            &storage,
            encrypted,
            "devA",
            HashMap::new(),
            1,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push_snapshot");

        let target = temp.path().join("device_b.db");
        bootstrap_from_snapshot(&storage, &enc, &target)
            .await
            .expect("bootstrap_from_snapshot");
        let db_b = open_db_at(&target);

        // Synced data still crosses.
        assert_eq!(
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'"),
            "Keep me",
            "synced data must survive alongside the bookkeeping clear",
        );

        // Each bookkeeping table is present (schema kept) but empty (rows cleared).
        for table in ["sync_state", "sync_cursors", "cloud_outbox"] {
            assert!(
                row_exists(
                    &db_b,
                    &format!("SELECT 1 FROM sqlite_master WHERE type='table' AND name='{table}'"),
                ),
                "bookkeeping table {table} schema must survive the snapshot restore",
            );
            assert_eq!(
                query_int(&db_b, &format!("SELECT COUNT(*) FROM {table}")),
                0,
                "{table} must be empty in the restored DB — a peer's bookkeeping \
                 (cursors/outbox/clock) must never ride a snapshot to a new device",
            );
        }
    }

    // ---- push_snapshot tests ----

    #[tokio::test]
    async fn push_snapshot_uploads_and_updates_head() {
        let storage = MockSyncStorage::new();
        let data = vec![1, 2, 3, 4, 5];

        // The snapshotting device has applied dev-2 up to seq 15.
        let applied = HashMap::from([("dev-2".to_string(), 15)]);

        push_snapshot(
            &storage,
            data.clone(),
            "dev-1",
            applied,
            42,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push_snapshot should succeed");

        // Snapshot should be stored.
        assert_eq!(storage.get_stored_snapshot(), Some(data));

        // Head should be updated with snapshot_seq.
        let heads = storage.list_heads().await.unwrap();
        let dev1_head = heads.iter().find(|h| h.device_id == "dev-1").unwrap();
        assert_eq!(dev1_head.seq, 42);
        assert_eq!(dev1_head.snapshot_seq, Some(42));

        // Snapshot metadata reflects the applied cursors plus our own seq.
        let meta_json = storage
            .get_stored_snapshot_meta()
            .expect("metadata should be written");
        let meta: SnapshotMeta = serde_json::from_slice(&meta_json).unwrap();
        assert_eq!(meta.cursors.get("dev-1"), Some(&42));
        assert_eq!(meta.cursors.get("dev-2"), Some(&15));
        assert_eq!(meta.cursors.len(), 2);
    }

    // ---- garbage_collect tests ----

    #[tokio::test]
    async fn gc_deletes_changesets_per_device_cursors() {
        let storage = MockSyncStorage::new();

        // Device A: changesets 1-5.
        for seq in 1..=5 {
            storage.add_changeset("dev-a", seq, vec![seq as u8]);
        }
        // Device B: changesets 1-3.
        for seq in 1..=3 {
            storage.add_changeset("dev-b", seq, vec![seq as u8]);
        }

        assert_eq!(storage.changeset_count(), 8);

        // Snapshot metadata: dev-a was at seq 3, dev-b was at seq 2.
        let meta = SnapshotMeta {
            cursors: HashMap::from([("dev-a".to_string(), 3), ("dev-b".to_string(), 2)]),
            created_at: "2026-02-10T00:00:00Z".to_string(),
        };
        storage
            .put_snapshot_meta(serde_json::to_vec(&meta).unwrap())
            .await
            .unwrap();

        let result = garbage_collect(&storage).await.expect("gc");

        // dev-a: 1,2,3 deleted (<=3), dev-b: 1,2 deleted (<=2)
        assert_eq!(result.deleted, 5);
        assert_eq!(result.errors, 0);
        assert_eq!(storage.changeset_count(), 3); // dev-a: 4,5 + dev-b: 3

        // Verify remaining changesets.
        let remaining_a = storage.list_changesets("dev-a").await.unwrap();
        assert_eq!(remaining_a, vec![4, 5]);

        let remaining_b = storage.list_changesets("dev-b").await.unwrap();
        assert_eq!(remaining_b, vec![3]);
    }

    #[tokio::test]
    async fn gc_with_no_changesets_to_delete() {
        let storage = MockSyncStorage::new();
        storage.add_changeset("dev-a", 10, vec![10]);

        // Snapshot metadata says dev-a was at seq 5 -- changeset 10 is newer.
        let meta = SnapshotMeta {
            cursors: HashMap::from([("dev-a".to_string(), 5)]),
            created_at: "2026-02-10T00:00:00Z".to_string(),
        };
        storage
            .put_snapshot_meta(serde_json::to_vec(&meta).unwrap())
            .await
            .unwrap();

        let result = garbage_collect(&storage).await.expect("gc");

        assert_eq!(result.deleted, 0);
        assert_eq!(storage.changeset_count(), 1);
    }

    #[tokio::test]
    async fn gc_with_empty_bucket() {
        let storage = MockSyncStorage::new();
        // No snapshot metadata -- GC should be a no-op.

        let result = garbage_collect(&storage).await.expect("gc");

        assert_eq!(result.deleted, 0);
        assert_eq!(result.errors, 0);
    }

    // ---- bootstrap_from_snapshot tests ----

    #[tokio::test]
    async fn bootstrap_downloads_decrypts_and_writes_db() {
        // First create a snapshot from a real database.
        let db = synced_conn();
        exec(
            &db,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        );
        let temp = tempfile::tempdir().unwrap();
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db, temp.path(), &synced_tables(), &enc).expect("snapshot");

        let storage = MockSyncStorage::new();
        storage.put_snapshot(encrypted).await.unwrap();
        let meta = SnapshotMeta {
            cursors: HashMap::from([("dev-1".to_string(), 10), ("dev-2".to_string(), 7)]),
            created_at: "2026-02-10T00:00:00Z".to_string(),
        };
        storage
            .put_snapshot_meta(serde_json::to_vec(&meta).unwrap())
            .await
            .unwrap();

        let target = temp.path().join("bootstrapped.db");
        let result = bootstrap_from_snapshot(&storage, &enc, &target)
            .await
            .expect("bootstrap");

        assert_eq!(result.cursors.get("dev-1"), Some(&10));
        assert_eq!(result.cursors.get("dev-2"), Some(&7));
        assert_eq!(result.cursors.len(), 2);
        assert!(target.exists());

        let db2 = open_db_at(&target);
        assert_eq!(
            query_text(&db2, "SELECT title FROM notes WHERE id = 'a1'"),
            "Artist One"
        );
    }

    #[tokio::test]
    async fn bootstrap_fails_when_no_snapshot_exists() {
        let storage = MockSyncStorage::new();
        let enc = test_encryption();
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("nope.db");

        let result = bootstrap_from_snapshot(&storage, &enc, &target).await;

        assert!(result.is_err());
        assert!(!target.exists());
    }

    // ---- Integration: create, push, bootstrap, verify ----

    #[tokio::test]
    async fn full_snapshot_round_trip() {
        let db = synced_conn();
        exec(
            &db,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        );
        exec(
            &db,
            "INSERT INTO note_tags (id, tag, note_id, _updated_at, created_at) \
             VALUES ('al1', 'Album One', 'a1', '0000000001000-0000-dev1', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let enc = test_encryption();
        let storage = MockSyncStorage::new();

        let encrypted =
            create_snapshot(&db, temp.path(), &synced_tables(), &enc).expect("snapshot");
        push_snapshot(
            &storage,
            encrypted,
            "dev-1",
            HashMap::new(),
            5,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push");

        let target = temp.path().join("device2.db");
        let result = bootstrap_from_snapshot(&storage, &enc, &target)
            .await
            .expect("bootstrap");
        assert_eq!(result.cursors.get("dev-1"), Some(&5));

        let db2 = open_db_at(&target);
        assert_eq!(
            query_text(&db2, "SELECT title FROM notes WHERE id = 'a1'"),
            "Artist One"
        );
        assert_eq!(
            query_text(&db2, "SELECT tag FROM note_tags WHERE id = 'al1'"),
            "Album One"
        );
    }

    /// Verify that a snapshot + subsequent changesets produces the same state
    /// as applying all changesets from scratch.
    #[tokio::test]
    async fn snapshot_plus_changesets_equals_full_replay() {
        let enc = test_encryption();
        let temp = tempfile::tempdir().unwrap();

        // --- Phase 1: create data, snapshot, then more data ---
        let db_source = synced_conn();

        // Initial data (before snapshot), captured as cs1.
        let mut session1 = RqSession::new(&db_source).expect("session");
        for t in synced_tables() {
            session1.attach(Some(t.name())).expect("attach");
        }
        exec(
            &db_source,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        );
        exec(
            &db_source,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a2', 'Artist Two', 1, '0000000002000-0000-dev1', '2026-01-01')",
        );
        let mut cs1_bytes = Vec::new();
        session1.changeset_strm(&mut cs1_bytes).expect("cs1");
        drop(session1);

        // Snapshot after cs1.
        let snapshot_encrypted =
            create_snapshot(&db_source, temp.path(), &synced_tables(), &enc).expect("snapshot");

        // More data after snapshot, captured as cs2.
        let mut session2 = RqSession::new(&db_source).expect("session2");
        for t in synced_tables() {
            session2.attach(Some(t.name())).expect("attach");
        }
        exec(
            &db_source,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a3', 'Artist Three', 1, '0000000003000-0000-dev1', '2026-01-01')",
        );
        exec(
            &db_source,
            "UPDATE notes SET title = 'Artist One Updated' WHERE id = 'a1'",
        );
        let mut cs2_bytes = Vec::new();
        session2.changeset_strm(&mut cs2_bytes).expect("cs2");
        drop(session2);

        // --- Path A: bootstrap from snapshot + apply cs2 ---
        let snapshot_plain = enc.open(&snapshot_encrypted).unwrap();
        let path_a = temp.path().join("path_a.db");
        std::fs::write(&path_a, &snapshot_plain).unwrap();
        let db_a = open_db_at(&path_a);
        apply(&db_a, &cs2_bytes);

        // --- Path B: fresh DB + apply cs1 + apply cs2 ---
        let db_b = synced_conn();
        apply(&db_b, &cs1_bytes);
        apply(&db_b, &cs2_bytes);

        // --- Compare: both paths should have identical data ---
        let count_a = query_int(&db_a, "SELECT COUNT(*) FROM notes");
        let count_b = query_int(&db_b, "SELECT COUNT(*) FROM notes");
        assert_eq!(count_a, count_b, "artist count should match");
        assert_eq!(count_a, 3);

        assert_eq!(
            query_text(&db_a, "SELECT title FROM notes WHERE id = 'a1'"),
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'a1'")
        );
        assert_eq!(
            query_text(&db_a, "SELECT title FROM notes WHERE id = 'a1'"),
            "Artist One Updated"
        );
        assert_eq!(
            query_text(&db_a, "SELECT title FROM notes WHERE id = 'a3'"),
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'a3'")
        );
        assert_eq!(
            query_text(&db_a, "SELECT title FROM notes WHERE id = 'a3'"),
            "Artist Three"
        );
    }

    // ---- new safety tests ----

    /// Device A creates snapshot when Device B is at seq 30. Device B later
    /// pushes seq 31-35. GC must NOT delete Device B's 31-35.
    #[tokio::test]
    async fn gc_does_not_delete_post_snapshot_changesets() {
        let storage = MockSyncStorage::new();

        // Device A: changesets 1-50. Device B: changesets 1-35.
        for seq in 1..=50 {
            storage.add_changeset("dev-a", seq, vec![seq as u8]);
        }
        for seq in 1..=35 {
            storage.add_changeset("dev-b", seq, vec![seq as u8]);
        }

        // Snapshot taken when dev-a was at 50, dev-b was at 30.
        // (Dev-b pushed 31-35 after the snapshot.)
        let meta = SnapshotMeta {
            cursors: HashMap::from([("dev-a".to_string(), 50), ("dev-b".to_string(), 30)]),
            created_at: "2026-02-10T00:00:00Z".to_string(),
        };
        storage
            .put_snapshot_meta(serde_json::to_vec(&meta).unwrap())
            .await
            .unwrap();

        let result = garbage_collect(&storage).await.expect("gc");

        // dev-a: all 50 deleted (<=50), dev-b: 1-30 deleted (<=30)
        assert_eq!(result.deleted, 80);
        assert_eq!(result.errors, 0);

        // dev-b's 31-35 must survive.
        let remaining_b = storage.list_changesets("dev-b").await.unwrap();
        assert_eq!(remaining_b, vec![31, 32, 33, 34, 35]);

        // dev-a has nothing remaining.
        let remaining_a = storage.list_changesets("dev-a").await.unwrap();
        assert!(remaining_a.is_empty());
    }

    /// Device C appears after snapshot was created. GC should not touch
    /// any of Device C's changesets.
    #[tokio::test]
    async fn gc_ignores_device_not_in_snapshot_meta() {
        let storage = MockSyncStorage::new();

        // Device A: changesets 1-5 (present in snapshot).
        for seq in 1..=5 {
            storage.add_changeset("dev-a", seq, vec![seq as u8]);
        }
        // Device C: changesets 1-3 (NOT in snapshot metadata).
        for seq in 1..=3 {
            storage.add_changeset("dev-c", seq, vec![seq as u8]);
        }

        // Snapshot only knows about dev-a.
        let meta = SnapshotMeta {
            cursors: HashMap::from([("dev-a".to_string(), 5)]),
            created_at: "2026-02-10T00:00:00Z".to_string(),
        };
        storage
            .put_snapshot_meta(serde_json::to_vec(&meta).unwrap())
            .await
            .unwrap();

        let result = garbage_collect(&storage).await.expect("gc");

        // Only dev-a's changesets should be deleted.
        assert_eq!(result.deleted, 5);
        assert_eq!(result.errors, 0);

        // dev-c's changesets are untouched.
        let remaining_c = storage.list_changesets("dev-c").await.unwrap();
        assert_eq!(remaining_c, vec![1, 2, 3]);
    }

    /// A snapshot blob without its accompanying `snapshot_meta.json.enc`
    /// is a torn bucket state (e.g., a previous push failed between the
    /// snapshot upload and the metadata upload). Bootstrap must refuse
    /// rather than seed cursors from a heuristic on `heads`.
    #[tokio::test]
    async fn bootstrap_fails_when_snapshot_meta_missing() {
        let db = synced_conn();
        exec(
            &db,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        );
        let temp = tempfile::tempdir().unwrap();
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db, temp.path(), &synced_tables(), &enc).expect("snapshot");

        // Put snapshot in storage WITHOUT metadata (torn-bucket simulation).
        let storage = MockSyncStorage::new();
        storage.put_snapshot(encrypted).await.unwrap();
        storage
            .put_head("dev-1", 20, Some(15), "2026-02-10T00:00:00Z")
            .await
            .unwrap();

        let target = temp.path().join("torn.db");
        let err = bootstrap_from_snapshot(&storage, &enc, &target)
            .await
            .expect_err("bootstrap must refuse torn bucket");
        assert!(
            matches!(err, SnapshotError::Bucket(StorageError::NotFound(_))),
            "expected Bucket(NotFound), got {err:?}",
        );
        assert!(
            !target.exists(),
            "no DB should be written when metadata is missing"
        );
    }

    // ---- snapshot cursor honesty (the overclaim bug) ----

    /// The core regression: a device that snapshots a DB it has NOT fully
    /// caught up to must record cursors describing what the snapshot DB
    /// actually contains — never another device's published head. If it
    /// overclaims, GC deletes the un-snapshotted changeset and no future
    /// restore can ever recover it.
    #[tokio::test]
    async fn snapshot_meta_reflects_applied_not_published() {
        let enc = test_encryption();
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();

        // Owner device M inserts a note and is at applied seq K = 1.
        let k = 1u64;
        let cs_insert = changeset_bytes_for(|db| {
            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Release Draft', 1, '0000000001000-0000-M', '2026-01-01')",
            );
        });
        storage.add_changeset("M", k, cs_insert.clone());

        // M later pushes a "manage release" UPDATE as seq K+1 = 2, raising M's
        // head to 2 — but this edit is NOT in any snapshot yet.
        let cs_update = changeset_bytes_for(|db| {
            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Release Draft', 1, '0000000001000-0000-M', '2026-01-01')",
            );
            exec(
                db,
                "UPDATE notes SET title = 'Release Managed', \
                 _updated_at = '0000000002000-0000-M' WHERE id = 'n1'",
            );
        });
        storage.add_changeset("M", k + 1, cs_update.clone());

        // Device B is behind: it has applied M only up to K. B snapshots its state.
        let db_b = synced_conn();
        apply(&db_b, &cs_insert);
        let snapshot =
            create_snapshot(&db_b, temp.path(), &synced_tables(), &enc).expect("snapshot");

        let applied = HashMap::from([("M".to_string(), k)]);
        push_snapshot(
            &storage,
            snapshot,
            "B",
            applied,
            0,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push");

        let meta_json = storage.get_stored_snapshot_meta().expect("meta");
        let meta: SnapshotMeta = serde_json::from_slice(&meta_json).unwrap();
        assert_eq!(
            meta.cursors.get("M"),
            Some(&k),
            "snapshot meta must reflect applied seq K, not published head K+1"
        );

        garbage_collect(&storage).await.expect("gc");
        storage
            .get_changeset("M", k + 1)
            .await
            .expect("K+1 must survive GC");

        let target = temp.path().join("device_c.db");
        let boot = bootstrap_from_snapshot(&storage, &enc, &target)
            .await
            .expect("bootstrap");
        let c_cursor = *boot.cursors.get("M").unwrap_or(&0);

        let db_c = open_db_at(&target);
        for seq in storage.list_changesets("M").await.unwrap() {
            if seq <= c_cursor {
                continue;
            }
            let bytes = storage.get_changeset("M", seq).await.unwrap();
            apply(&db_c, &bytes);
        }
        assert_eq!(
            query_text(&db_c, "SELECT title FROM notes WHERE id = 'n1'"),
            "Release Managed",
            "device C must receive the post-snapshot edit"
        );
    }

    /// End-to-end: owner inserts + snapshots, B bootstraps, owner pushes an
    /// UPDATE, B pulls it, B snapshots (honest meta), C bootstraps + pulls and
    /// also has the update. All through the real snapshot/GC/bootstrap funcs.
    #[tokio::test]
    async fn multi_device_managed_edit_reaches_restore() {
        let enc = test_encryption();
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();

        // Owner inserts a note (seq 1) and snapshots its applied state.
        let cs1 = changeset_bytes_for(|db| {
            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Draft', 1, '0000000001000-0000-owner', '2026-01-01')",
            );
        });
        storage.add_changeset("owner", 1, cs1.clone());

        let db_owner = synced_conn();
        apply(&db_owner, &cs1);
        let snap1 = create_snapshot(&db_owner, temp.path(), &synced_tables(), &enc).expect("snap1");
        push_snapshot(
            &storage,
            snap1,
            "owner",
            HashMap::new(),
            1,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push snap1");

        // Device B bootstraps and has the note.
        let b_path = temp.path().join("b.db");
        let b_boot = bootstrap_from_snapshot(&storage, &enc, &b_path)
            .await
            .expect("b bootstrap");
        let db_b = open_db_at(&b_path);
        assert_eq!(
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'"),
            "Draft"
        );

        // Owner pushes a row-UPDATE changeset (seq 2).
        let cs2 = changeset_bytes_for(|db| {
            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Draft', 1, '0000000001000-0000-owner', '2026-01-01')",
            );
            exec(
                db,
                "UPDATE notes SET title = 'Published', \
                 _updated_at = '0000000002000-0000-owner' WHERE id = 'n1'",
            );
        });
        storage.add_changeset("owner", 2, cs2.clone());

        // B pulls the update (everything past its bootstrap cursor).
        let mut b_cursors = b_boot.cursors.clone();
        let b_owner_cursor = *b_cursors.get("owner").unwrap_or(&0);
        for seq in storage.list_changesets("owner").await.unwrap() {
            if seq <= b_owner_cursor {
                continue;
            }
            let bytes = storage.get_changeset("owner", seq).await.unwrap();
            apply(&db_b, &bytes);
            b_cursors.insert("owner".to_string(), seq);
        }
        assert_eq!(
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'"),
            "Published"
        );

        // B snapshots its now-current state with honest cursors {owner: 2}.
        let snap2 = create_snapshot(&db_b, temp.path(), &synced_tables(), &enc).expect("snap2");
        push_snapshot(
            &storage,
            snap2,
            "B",
            b_cursors.clone(),
            0,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push snap2");

        // Device C bootstraps + pulls and must also have the update.
        let c_path = temp.path().join("c.db");
        let c_boot = bootstrap_from_snapshot(&storage, &enc, &c_path)
            .await
            .expect("c bootstrap");
        let db_c = open_db_at(&c_path);
        let c_owner_cursor = *c_boot.cursors.get("owner").unwrap_or(&0);
        for seq in storage.list_changesets("owner").await.unwrap() {
            if seq <= c_owner_cursor {
                continue;
            }
            let bytes = storage.get_changeset("owner", seq).await.unwrap();
            apply(&db_c, &bytes);
        }
        assert_eq!(
            query_text(&db_c, "SELECT title FROM notes WHERE id = 'n1'"),
            "Published",
            "device C must receive the managed edit through B's snapshot + pull"
        );
    }

    /// GC deletes only seqs <= the snapshot's accurate cursor; a changeset
    /// pushed after the snapshot (absent from the snapshot DB) survives.
    #[tokio::test]
    async fn gc_never_deletes_changeset_absent_from_snapshot() {
        let storage = MockSyncStorage::new();
        for seq in 1..=3 {
            storage.add_changeset("M", seq, vec![seq as u8]);
        }

        // Snapshot honestly covers M only through seq 2.
        let applied = HashMap::from([("M".to_string(), 2)]);
        push_snapshot(
            &storage,
            vec![0u8; 4],
            "M",
            applied,
            2,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push");

        garbage_collect(&storage).await.expect("gc");

        assert_eq!(storage.list_changesets("M").await.unwrap(), vec![3]);
    }

    /// After bootstrap, the returned cursors never exceed what the snapshot DB
    /// actually contains — they equal the applied state the snapshot was taken
    /// from.
    #[tokio::test]
    async fn bootstrap_cursors_match_snapshot_contents() {
        let enc = test_encryption();
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();

        // Snapshot taken from a state where M is applied through seq 7.
        let db = synced_conn();
        exec(
            &db,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n1', 'A', 1, '0000000001000-0000-M', '2026-01-01')",
        );
        let snap = create_snapshot(&db, temp.path(), &synced_tables(), &enc).expect("snap");

        let applied = HashMap::from([("M".to_string(), 7)]);
        push_snapshot(
            &storage,
            snap,
            "self",
            applied,
            0,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push");

        let target = temp.path().join("boot.db");
        let boot = bootstrap_from_snapshot(&storage, &enc, &target)
            .await
            .expect("bootstrap");
        assert_eq!(boot.cursors.get("M"), Some(&7));
    }

    /// A device that snapshots while another device's head is ahead writes
    /// cursors equal to its applied state, not the ahead head.
    #[tokio::test]
    async fn behind_device_snapshot_does_not_overclaim() {
        let storage = MockSyncStorage::new();

        // Device M's head is ahead at seq 9.
        storage.add_changeset("M", 9, vec![9]);

        // The snapshotting device B has only applied M through seq 4.
        let applied = HashMap::from([("M".to_string(), 4)]);
        push_snapshot(
            &storage,
            vec![0u8; 4],
            "B",
            applied,
            0,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push");

        let meta_json = storage.get_stored_snapshot_meta().expect("meta");
        let meta: SnapshotMeta = serde_json::from_slice(&meta_json).unwrap();
        assert_eq!(
            meta.cursors.get("M"),
            Some(&4),
            "must record applied seq 4, not M's ahead head 9"
        );
    }
}
