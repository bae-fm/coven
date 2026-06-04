/// Snapshots and garbage collection for the sync system.
///
/// Periodically, a device creates a full snapshot of the database via
/// `VACUUM INTO`, encrypts it, and uploads as `snapshot.db.enc`. This
/// allows new devices to bootstrap without replaying the entire changeset
/// history, and enables GC of old changesets.
///
/// Snapshot creation policy: after every N changesets (default 100) or
/// T hours (default 24) since the last snapshot.
use std::collections::HashMap;
use std::ffi::CString;
use std::path::Path;

use libsqlite3_sys as ffi;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::storage::{StorageError, SyncStorage};
use crate::encryption::EncryptionService;

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

/// Metadata stored alongside a snapshot in `snapshot_meta.json.enc`.
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

/// Create a snapshot of the database as encrypted bytes.
///
/// Uses `VACUUM INTO` to create a clean copy of the database at a temp path,
/// then clears every non-synced table's data from that copy, reads the bytes,
/// encrypts, and returns the encrypted blob.
///
/// A snapshot is restored byte-for-byte as the joining device's `library.db`
/// (no migration rebuild), so it must carry only data that is eligible to
/// cross devices — the host's registered synced tables. Local-only tables
/// (per-device paths, caches) and per-device sync bookkeeping must not ride
/// along; their schemas are kept, but their rows are deleted from the copy.
///
/// # Safety
/// `db` must be a valid, open sqlite3 connection pointer.
pub unsafe fn create_snapshot(
    db: *mut ffi::sqlite3,
    temp_dir: &Path,
    encryption: &EncryptionService,
) -> Result<Vec<u8>, SnapshotError> {
    // A snapshot with no synced set would either leak every local-only table or
    // clear the whole DB — both wrong. Refuse before doing any work, and read
    // the synced set once here so the clearing helper stays pure.
    let synced = crate::sync::session::synced_tables();
    if synced.is_empty() {
        return Err(SnapshotError::NoSyncedTables);
    }

    let snapshot_path = temp_dir.join("snapshot.db");
    let path_str = snapshot_path
        .to_str()
        .expect("temp path should be valid UTF-8");

    // Remove any leftover snapshot file from a previous failed attempt.
    let _ = std::fs::remove_file(&snapshot_path);

    // VACUUM INTO creates a clean, defragmented copy of the database.
    let sql = format!("VACUUM INTO '{}'", path_str.replace('\'', "''"));
    let c_sql = CString::new(sql).expect("SQL should not contain null bytes");
    let rc = ffi::sqlite3_exec(
        db,
        c_sql.as_ptr(),
        None,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    if rc != ffi::SQLITE_OK {
        let err = ffi::sqlite3_errmsg(db);
        let msg = if err.is_null() {
            format!("sqlite3 error code {rc}")
        } else {
            std::ffi::CStr::from_ptr(err).to_string_lossy().into_owned()
        };
        if let Err(rm) = std::fs::remove_file(&snapshot_path) {
            warn!(error = %rm, "failed to remove temp snapshot after VACUUM error");
        }
        return Err(SnapshotError::VacuumFailed(msg));
    }

    // The copy is a whole-DB byte image, so it still holds every local-only
    // table's data. Strip those before reading: open the copy as its own
    // connection and DELETE from every table outside the synced set.
    if let Err(e) = clear_local_only_tables(&snapshot_path, synced) {
        if let Err(rm) = std::fs::remove_file(&snapshot_path) {
            warn!(error = %rm, "failed to remove temp snapshot after clear error");
        }
        return Err(e);
    }

    // Read the cleared snapshot file and encrypt.
    let plaintext = std::fs::read(&snapshot_path)?;
    let _ = std::fs::remove_file(&snapshot_path);

    let encrypted = encryption.encrypt(&plaintext);

    info!(
        plaintext_size = plaintext.len(),
        encrypted_size = encrypted.len(),
        "created snapshot"
    );

    Ok(encrypted)
}

/// Tables whose data is preserved in a snapshot regardless of the synced set:
/// the migration ledger must survive so the restored DB opens without
/// re-migrating.
const SNAPSHOT_PRESERVED_TABLE: &str = "_sqlx_migrations";

/// Delete every non-synced table's rows from the snapshot copy at `path`,
/// keeping all table schemas intact. The migration ledger
/// (`_sqlx_migrations`) is also preserved so the restored DB opens directly.
///
/// Opens `path` as its own sqlite3 connection (the copy must be edited in
/// isolation from the live DB). The synced set is passed in by the caller (the
/// only reader of the process-global). Errors if any FFI step fails — a
/// snapshot that silently dropped synced data, or silently kept local-only
/// data, is worse than no snapshot.
unsafe fn clear_local_only_tables(
    path: &Path,
    synced: &[crate::sync::session::SyncedTable],
) -> Result<(), SnapshotError> {
    let db = open_snapshot_db(path)?;
    let result = clear_non_synced(db, synced);
    let close_rc = ffi::sqlite3_close(db);
    result?;
    if close_rc != ffi::SQLITE_OK {
        return Err(SnapshotError::ClearFailed(format!(
            "sqlite3_close failed (rc={close_rc})"
        )));
    }
    Ok(())
}

/// Open the snapshot copy as a standalone read-write connection.
unsafe fn open_snapshot_db(path: &Path) -> Result<*mut ffi::sqlite3, SnapshotError> {
    let c_path = CString::new(path.to_str().expect("temp path should be valid UTF-8"))
        .expect("path should not contain null bytes");
    let mut db: *mut ffi::sqlite3 = std::ptr::null_mut();
    let rc = ffi::sqlite3_open(c_path.as_ptr(), &mut db);
    if rc != ffi::SQLITE_OK {
        // sqlite3_open may allocate a handle even on failure; free it.
        ffi::sqlite3_close(db);
        return Err(SnapshotError::ClearFailed(format!(
            "failed to open snapshot copy (rc={rc})"
        )));
    }
    Ok(db)
}

/// On the already-open snapshot connection `db`, scope the copy down to exactly
/// what is eligible to cross devices, then VACUUM to reclaim the freed pages:
///
/// 1. Table-level: DELETE every user table that is neither in `synced` nor the
///    preserved migration ledger — local-only tables keep their schema, lose
///    their rows.
/// 2. Row-level: within the synced tables, DELETE the rows the gate excludes
///    (gated-false roots and their FK-descendants), so a private subtree does
///    not ride the snapshot to a restoring peer. This is the same exclusion the
///    outbound changeset gate applies; both reuse [`crate::sync::gate::Gates`].
unsafe fn clear_non_synced(
    db: *mut ffi::sqlite3,
    synced: &[crate::sync::session::SyncedTable],
) -> Result<(), SnapshotError> {
    for table in list_user_tables(db)? {
        if synced.iter().any(|t| t.name() == table) || table == SNAPSHOT_PRESERVED_TABLE {
            continue;
        }
        // Quote the identifier, doubling any embedded quotes.
        let stmt = format!("DELETE FROM \"{}\"", table.replace('"', "\"\""));
        exec_or_err(db, &stmt)?;
    }

    // The snapshot is a second propagation channel: the changeset gate cuts
    // gated-false rows on the wire, so the snapshot must drop them too or a
    // private subtree leaks to a restoring device. Reuse the changeset gate's
    // model rather than re-deriving the FK walk.
    let gates = crate::sync::gate::Gates::from_tables(db, synced)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;
    gates
        .delete_gated_false(db)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;

    // Reclaim the pages freed by the DELETEs so the blob shrinks.
    exec_or_err(db, "VACUUM")?;
    Ok(())
}

/// List user table names (excluding sqlite internal `sqlite_%` tables) on `db`.
unsafe fn list_user_tables(db: *mut ffi::sqlite3) -> Result<Vec<String>, SnapshotError> {
    let sql = c"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'";
    let mut stmt: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
    if rc != ffi::SQLITE_OK {
        return Err(SnapshotError::ClearFailed(errmsg(
            db,
            "prepare table list",
            rc,
        )));
    }

    let mut tables = Vec::new();
    loop {
        let step = ffi::sqlite3_step(stmt);
        if step == ffi::SQLITE_ROW {
            let text = ffi::sqlite3_column_text(stmt, 0);
            if text.is_null() {
                // A NULL `name` in `sqlite_master WHERE type='table'` is
                // corruption, not an empty result set — refuse rather than
                // silently skip a table whose rows we then can't scope.
                ffi::sqlite3_finalize(stmt);
                return Err(SnapshotError::ClearFailed(
                    "sqlite_master row has NULL table name".to_string(),
                ));
            }
            let name = match std::ffi::CStr::from_ptr(text as *const std::ffi::c_char).to_str() {
                Ok(name) => name.to_string(),
                Err(e) => {
                    // Table names are ASCII in practice; non-UTF-8 means the
                    // catalog is corrupt. Surface it instead of mangling the
                    // name and DELETE-ing (or sparing) the wrong table.
                    ffi::sqlite3_finalize(stmt);
                    return Err(SnapshotError::ClearFailed(format!(
                        "table name is not valid UTF-8: {e}"
                    )));
                }
            };
            tables.push(name);
        } else if step == ffi::SQLITE_DONE {
            break;
        } else {
            ffi::sqlite3_finalize(stmt);
            return Err(SnapshotError::ClearFailed(errmsg(
                db,
                "step table list",
                step,
            )));
        }
    }
    ffi::sqlite3_finalize(stmt);
    Ok(tables)
}

/// Run a statement on `db`, surfacing any error.
unsafe fn exec_or_err(db: *mut ffi::sqlite3, sql: &str) -> Result<(), SnapshotError> {
    let c_sql = CString::new(sql).expect("SQL should not contain null bytes");
    let rc = ffi::sqlite3_exec(
        db,
        c_sql.as_ptr(),
        None,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    if rc != ffi::SQLITE_OK {
        return Err(SnapshotError::ClearFailed(errmsg(db, sql, rc)));
    }
    Ok(())
}

/// Render the connection's last error message, falling back to the rc.
unsafe fn errmsg(db: *mut ffi::sqlite3, context: &str, rc: i32) -> String {
    let err = ffi::sqlite3_errmsg(db);
    if err.is_null() {
        format!("{context}: sqlite3 error code {rc}")
    } else {
        let msg = std::ffi::CStr::from_ptr(err).to_string_lossy();
        format!("{context}: {msg}")
    }
}

/// Upload a snapshot to the sync storage and update the device head.
///
/// Also uploads per-device cursor metadata (`snapshot_meta.json.enc`) so that
/// bootstrapping devices know where each device was at snapshot time, and GC
/// can safely delete only changesets covered by the snapshot.
pub async fn push_snapshot(
    storage: &dyn SyncStorage,
    encrypted_snapshot: Vec<u8>,
    device_id: &str,
    applied_cursors: HashMap<String, u64>,
    current_seq: u64,
    clock: &dyn crate::clock::Clock,
) -> Result<(), SnapshotError> {
    let size = encrypted_snapshot.len();
    let timestamp = clock.now().to_rfc3339();

    // Upload snapshot (overwrites previous).
    storage.put_snapshot(encrypted_snapshot).await?;

    // The snapshot DB is a VACUUM of this device's live database, so its
    // metadata must describe exactly what THIS device has applied — never
    // other devices' published heads, which may be ahead of what we pulled.
    // Claiming coverage we don't have lets GC delete un-snapshotted changesets
    // that no future restore can recover.
    let mut cursors = applied_cursors;
    // Our own current_seq is included (our head hasn't been updated yet).
    cursors.insert(device_id.to_string(), current_seq);

    let meta = SnapshotMeta {
        cursors,
        created_at: timestamp.clone(),
    };
    let meta_json =
        serde_json::to_vec(&meta).map_err(|e| SnapshotError::Io(std::io::Error::other(e)))?;

    storage.put_snapshot_meta(meta_json).await?;

    // Update head with snapshot_seq.
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
/// Downloads `snapshot.db.enc`, decrypts, and writes the plaintext database
/// to `target_path`. The caller should then open this as their local database
/// and pull any changesets newer than the per-device cursors in the result.
///
/// Returns a `BootstrapResult` with per-device cursors so the caller knows
/// where to start pulling changesets from each device.
pub async fn bootstrap_from_snapshot(
    storage: &dyn SyncStorage,
    encryption: &EncryptionService,
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

    let encrypted = storage.get_snapshot().await?;
    let plaintext = encryption
        .decrypt(&encrypted)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::session::SyncSession;
    use crate::sync::storage::DeviceHead;
    use crate::sync::test_helpers::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

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

    #[async_trait]
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
            _scope: crate::blob::BlobScope,
            _data: Vec<u8>,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn get_blob(
            &self,
            namespace: &str,
            id: &str,
            _scope: crate::blob::BlobScope,
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
                .ok_or(StorageError::NotFound("snapshot.db.enc".into()))
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

        async fn get_min_schema_version(&self) -> Result<Option<u32>, StorageError> {
            Ok(*self.min_schema_version.lock().unwrap())
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
                .ok_or(StorageError::NotFound("snapshot_meta.json.enc".into()))
        }
    }

    fn test_encryption() -> EncryptionService {
        EncryptionService::new_with_key(&[0x42u8; 32])
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
        unsafe {
            let db = open_memory_db();
            create_synced_schema(db);

            exec(
                db,
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'Note One', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            );

            let temp = tempfile::tempdir().unwrap();
            let enc = test_encryption();

            let encrypted =
                create_snapshot(db, temp.path(), &enc).expect("create_snapshot should succeed");

            // Should be non-empty encrypted bytes.
            assert!(!encrypted.is_empty());

            // Should be decryptable.
            let plaintext = enc.decrypt(&encrypted).expect("decrypt should succeed");
            assert!(!plaintext.is_empty());

            // The plaintext should be a valid SQLite database (starts with "SQLite format 3\0").
            assert!(
                plaintext.starts_with(b"SQLite format 3\0"),
                "snapshot should be a valid SQLite database"
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn create_snapshot_contains_data() {
        unsafe {
            let db = open_memory_db();
            create_synced_schema(db);

            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
            );
            exec(
                db,
                "INSERT INTO note_tags (id, tag, note_id, _updated_at, created_at) \
                 VALUES ('al1', 'Album One', 'a1', '0000000001000-0000-dev1', '2026-01-01')",
            );

            let temp = tempfile::tempdir().unwrap();
            let enc = test_encryption();

            let encrypted = create_snapshot(db, temp.path(), &enc).expect("snapshot");
            let plaintext = enc.decrypt(&encrypted).expect("decrypt");

            // Write to file and open to verify contents.
            let db_path = temp.path().join("verify.db");
            std::fs::write(&db_path, &plaintext).unwrap();

            let db2 = {
                let c_path = CString::new(db_path.to_str().unwrap()).unwrap();
                let mut ptr: *mut ffi::sqlite3 = std::ptr::null_mut();
                let rc = ffi::sqlite3_open(c_path.as_ptr(), &mut ptr);
                assert_eq!(rc, ffi::SQLITE_OK);
                ptr
            };

            let name = query_text(db2, "SELECT title FROM notes WHERE id = 'a1'");
            assert_eq!(name, "Artist One");

            let title = query_text(db2, "SELECT tag FROM note_tags WHERE id = 'al1'");
            assert_eq!(title, "Album One");

            ffi::sqlite3_close(db2);
            ffi::sqlite3_close(db);
        }
    }

    /// A snapshot is a propagation channel between devices, so it carries only
    /// synced-table data. A non-synced table (here `device_local`, holding a
    /// filesystem path meaningful only on the device that wrote it) keeps its
    /// schema in the restored DB but none of its rows: the schema survives so
    /// the table still opens, while its device-local rows never cross to a
    /// restoring peer.
    #[tokio::test]
    async fn snapshot_does_not_carry_local_only_tables_to_a_restoring_device() {
        unsafe {
            // --- Device A: a synced table + a device-local table ---
            let db_a = open_memory_db();
            create_synced_schema(db_a); // `notes` is synced (has `_updated_at`)
            exec(
                db_a,
                "CREATE TABLE device_local (note_id TEXT PRIMARY KEY, local_path TEXT NOT NULL)",
            );
            exec(
                db_a,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Gish', 1, '0000000001000-0000-devA', '2026-01-01')",
            );
            exec(
                db_a,
                "INSERT INTO device_local (note_id, local_path) \
                 VALUES ('n1', '/Users/dima/Torrents/Gish (1991)')",
            );

            let temp = tempfile::tempdir().unwrap();
            let enc = test_encryption();
            let encrypted = create_snapshot(db_a, temp.path(), &enc).expect("snapshot");

            // --- Upload (this device -> cloud) ---
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

            // --- Restore on a fresh device (a peer adds the cloud home) ---
            let target = temp.path().join("device_b.db");
            bootstrap_from_snapshot(&storage, &enc, &target)
                .await
                .expect("bootstrap_from_snapshot");

            let db_b = {
                let c_path = CString::new(target.to_str().unwrap()).unwrap();
                let mut ptr: *mut ffi::sqlite3 = std::ptr::null_mut();
                let rc = ffi::sqlite3_open(c_path.as_ptr(), &mut ptr);
                assert_eq!(rc, ffi::SQLITE_OK);
                ptr
            };

            // Synced data SHOULD cross.
            assert_eq!(
                query_text(db_b, "SELECT title FROM notes WHERE id = 'n1'"),
                "Gish",
                "synced-table data must survive a snapshot restore",
            );

            // Device-local data must NOT cross.
            assert!(
                !row_exists(db_b, "SELECT 1 FROM device_local WHERE note_id = 'n1'"),
                "device-local row leaked to a peer via the snapshot: a non-synced \
                 table's data must never cross devices",
            );

            // The table SCHEMA is preserved (only its rows are cleared), so the
            // restored DB can still open it — it simply has no rows.
            assert!(
                row_exists(
                    db_b,
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='device_local'",
                ),
                "non-synced table schema must survive: snapshot DELETEs rows, never DROPs tables",
            );
            assert_eq!(
                query_int(db_b, "SELECT COUNT(*) FROM device_local"),
                0,
                "non-synced table must be empty in the restored DB",
            );

            ffi::sqlite3_close(db_b);
            ffi::sqlite3_close(db_a);
        }
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
        unsafe {
            let db_a = open_memory_db();
            create_synced_schema(db_a); // `notes` gated by `shared`; note_tags FK-child

            // A shared note with a child tag (both must cross).
            exec(
                db_a,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('pub', 'Public', 1, '0000000001000-0000-devA', '2026-01-01')",
            );
            exec(
                db_a,
                "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('pub_t', 'pub', 'green', '0000000001000-0000-devA', '2026-01-01')",
            );
            // A private note with its own child tag (neither may cross).
            exec(
                db_a,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('priv', 'Private', 0, '0000000002000-0000-devA', '2026-01-01')",
            );
            exec(
                db_a,
                "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('priv_t', 'priv', 'red', '0000000002000-0000-devA', '2026-01-01')",
            );

            let temp = tempfile::tempdir().unwrap();
            let enc = test_encryption();
            let encrypted = create_snapshot(db_a, temp.path(), &enc).expect("snapshot");

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

            let db_b = {
                let c_path = CString::new(target.to_str().unwrap()).unwrap();
                let mut ptr: *mut ffi::sqlite3 = std::ptr::null_mut();
                let rc = ffi::sqlite3_open(c_path.as_ptr(), &mut ptr);
                assert_eq!(rc, ffi::SQLITE_OK);
                ptr
            };

            // The shared root and its descendant cross.
            assert!(
                row_exists(db_b, "SELECT 1 FROM notes WHERE id = 'pub'"),
                "a gated-true note must survive the snapshot restore",
            );
            assert!(
                row_exists(db_b, "SELECT 1 FROM note_tags WHERE id = 'pub_t'"),
                "a gated-true note's FK-child must survive the snapshot restore",
            );

            // The private root and its descendant must NOT cross.
            assert!(
                !row_exists(db_b, "SELECT 1 FROM notes WHERE id = 'priv'"),
                "a gated-false note leaked to a peer via the snapshot",
            );
            assert!(
                !row_exists(db_b, "SELECT 1 FROM note_tags WHERE id = 'priv_t'"),
                "a gated-false note's FK-descendant leaked to a peer via the snapshot",
            );

            ffi::sqlite3_close(db_b);
            ffi::sqlite3_close(db_a);
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
        unsafe {
            // First create a snapshot from a real database.
            let db = open_memory_db();
            create_synced_schema(db);

            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
            );

            let temp = tempfile::tempdir().unwrap();
            let enc = test_encryption();

            let encrypted = create_snapshot(db, temp.path(), &enc).expect("snapshot");
            ffi::sqlite3_close(db);

            // Put snapshot in mock storage with metadata.
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

            // Bootstrap a new database.
            let target = temp.path().join("bootstrapped.db");
            let result = bootstrap_from_snapshot(&storage, &enc, &target)
                .await
                .expect("bootstrap");

            // Should have per-device cursors from metadata.
            assert_eq!(result.cursors.get("dev-1"), Some(&10));
            assert_eq!(result.cursors.get("dev-2"), Some(&7));
            assert_eq!(result.cursors.len(), 2);
            assert!(target.exists());

            // Open the bootstrapped DB and verify data.
            let c_path = CString::new(target.to_str().unwrap()).unwrap();
            let mut db2: *mut ffi::sqlite3 = std::ptr::null_mut();
            let rc = ffi::sqlite3_open(c_path.as_ptr(), &mut db2);
            assert_eq!(rc, ffi::SQLITE_OK);

            let name = query_text(db2, "SELECT title FROM notes WHERE id = 'a1'");
            assert_eq!(name, "Artist One");

            ffi::sqlite3_close(db2);
        }
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
        unsafe {
            // Device 1 creates some data.
            let db = open_memory_db();
            create_synced_schema(db);

            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
            );
            exec(
                db,
                "INSERT INTO note_tags (id, tag, note_id, _updated_at, created_at) \
                 VALUES ('al1', 'Album One', 'a1', '0000000001000-0000-dev1', '2026-01-01')",
            );

            let temp = tempfile::tempdir().unwrap();
            let enc = test_encryption();
            let storage = MockSyncStorage::new();

            // Create and push snapshot at seq 5.
            let encrypted = create_snapshot(db, temp.path(), &enc).expect("snapshot");
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

            ffi::sqlite3_close(db);

            // Device 2 bootstraps.
            let target = temp.path().join("device2.db");
            let result = bootstrap_from_snapshot(&storage, &enc, &target)
                .await
                .expect("bootstrap");

            assert_eq!(result.cursors.get("dev-1"), Some(&5));

            // Open and verify.
            let c_path = CString::new(target.to_str().unwrap()).unwrap();
            let mut db2: *mut ffi::sqlite3 = std::ptr::null_mut();
            let rc = ffi::sqlite3_open(c_path.as_ptr(), &mut db2);
            assert_eq!(rc, ffi::SQLITE_OK);

            let name = query_text(db2, "SELECT title FROM notes WHERE id = 'a1'");
            assert_eq!(name, "Artist One");

            let title = query_text(db2, "SELECT tag FROM note_tags WHERE id = 'al1'");
            assert_eq!(title, "Album One");

            // Device 2 can now pull only changesets > per-device cursors.
            // (Not tested here since pull is already tested in pull_tests.rs.)

            ffi::sqlite3_close(db2);
        }
    }

    /// Verify that a snapshot + subsequent changesets produces the same state
    /// as applying all changesets from scratch.
    #[tokio::test]
    async fn snapshot_plus_changesets_equals_full_replay() {
        unsafe {
            let enc = test_encryption();
            let temp = tempfile::tempdir().unwrap();

            // --- Phase 1: create data, snapshot, then more data ---

            let db_source = open_memory_db();
            create_synced_schema(db_source);

            // Initial data (before snapshot).
            let session1 = SyncSession::start(db_source).expect("session");
            exec(
                db_source,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
            );
            exec(
                db_source,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('a2', 'Artist Two', 1, '0000000002000-0000-dev1', '2026-01-01')",
            );
            let cs1 = session1.changeset().unwrap().unwrap();
            let cs1_bytes = cs1.as_bytes().to_vec();
            drop(session1);

            // Create snapshot after cs1.
            let snapshot_encrypted =
                create_snapshot(db_source, temp.path(), &enc).expect("snapshot");

            // More data after snapshot.
            let session2 = SyncSession::start(db_source).expect("session2");
            exec(
                db_source,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('a3', 'Artist Three', 1, '0000000003000-0000-dev1', '2026-01-01')",
            );
            exec(
                db_source,
                "UPDATE notes SET title = 'Artist One Updated' \
                 WHERE id = 'a1'",
            );
            let cs2 = session2.changeset().unwrap().unwrap();
            let cs2_bytes = cs2.as_bytes().to_vec();
            drop(session2);

            ffi::sqlite3_close(db_source);

            // --- Path A: bootstrap from snapshot + apply cs2 ---

            let snapshot_plain = enc.decrypt(&snapshot_encrypted).unwrap();
            let path_a = temp.path().join("path_a.db");
            std::fs::write(&path_a, &snapshot_plain).unwrap();

            let db_a = {
                let c = CString::new(path_a.to_str().unwrap()).unwrap();
                let mut p: *mut ffi::sqlite3 = std::ptr::null_mut();
                ffi::sqlite3_open(c.as_ptr(), &mut p);
                p
            };

            let cs2_obj = crate::sync::session_ext::Changeset::from_bytes(&cs2_bytes);
            crate::sync::apply::apply_changeset_lww(db_a, &cs2_obj).expect("apply cs2");

            // --- Path B: fresh DB + apply cs1 + apply cs2 ---

            let db_b = open_memory_db();
            create_synced_schema(db_b);

            let cs1_obj = crate::sync::session_ext::Changeset::from_bytes(&cs1_bytes);
            crate::sync::apply::apply_changeset_lww(db_b, &cs1_obj).expect("apply cs1");

            let cs2_obj2 = crate::sync::session_ext::Changeset::from_bytes(&cs2_bytes);
            crate::sync::apply::apply_changeset_lww(db_b, &cs2_obj2).expect("apply cs2");

            // --- Compare: both paths should have identical data ---

            let count_a = query_int(db_a, "SELECT COUNT(*) FROM notes");
            let count_b = query_int(db_b, "SELECT COUNT(*) FROM notes");
            assert_eq!(count_a, count_b, "artist count should match");
            assert_eq!(count_a, 3);

            let name_a = query_text(db_a, "SELECT title FROM notes WHERE id = 'a1'");
            let name_b = query_text(db_b, "SELECT title FROM notes WHERE id = 'a1'");
            assert_eq!(name_a, name_b);
            assert_eq!(name_a, "Artist One Updated");

            let name_a3 = query_text(db_a, "SELECT title FROM notes WHERE id = 'a3'");
            let name_b3 = query_text(db_b, "SELECT title FROM notes WHERE id = 'a3'");
            assert_eq!(name_a3, name_b3);
            assert_eq!(name_a3, "Artist Three");

            ffi::sqlite3_close(db_a);
            ffi::sqlite3_close(db_b);
        }
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
        unsafe {
            let db = open_memory_db();
            create_synced_schema(db);

            exec(
                db,
                "INSERT INTO notes (id, title, _updated_at, created_at) \
                 VALUES ('a1', 'Artist One', '0000000001000-0000-dev1', '2026-01-01')",
            );

            let temp = tempfile::tempdir().unwrap();
            let enc = test_encryption();

            let encrypted = create_snapshot(db, temp.path(), &enc).expect("snapshot");
            ffi::sqlite3_close(db);

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
                "no DB should be written when metadata is missing",
            );
        }
    }

    // ---- snapshot cursor honesty (the overclaim bug) ----

    /// Open a SQLite database file by path. Caller owns the returned handle.
    unsafe fn open_db_at(path: &Path) -> *mut ffi::sqlite3 {
        let c = CString::new(path.to_str().unwrap()).unwrap();
        let mut p: *mut ffi::sqlite3 = std::ptr::null_mut();
        let rc = ffi::sqlite3_open(c.as_ptr(), &mut p);
        assert_eq!(rc, ffi::SQLITE_OK);
        p
    }

    /// Produce a signed-free changeset's raw bytes by recording the SQL run
    /// inside `body` against a fresh schema-only DB, and return those bytes.
    /// This is what device M would push as a changeset blob.
    unsafe fn changeset_bytes_for(body: impl FnOnce(*mut ffi::sqlite3)) -> Vec<u8> {
        init_synced_tables();
        let db = open_memory_db();
        create_synced_schema(db);
        let session = SyncSession::start(db).expect("session");
        body(db);
        let cs = session.changeset().unwrap().unwrap();
        let bytes = cs.as_bytes().to_vec();
        drop(session);
        ffi::sqlite3_close(db);
        bytes
    }

    /// The core regression: a device that snapshots a DB it has NOT fully
    /// caught up to must record cursors describing what the snapshot DB
    /// actually contains — never another device's published head. If it
    /// overclaims, GC deletes the un-snapshotted changeset and no future
    /// restore can ever recover it.
    #[tokio::test]
    async fn snapshot_meta_reflects_applied_not_published() {
        unsafe {
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

            // M later pushes a "manage release" UPDATE as seq K+1 = 2, raising
            // M's head to 2 — but this edit is NOT in any snapshot yet.
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

            // Device B is behind: it has applied M only up to K, and its DB
            // lacks the K+1 edit. B builds a snapshot DB of its applied state.
            let db_b = open_memory_db();
            create_synced_schema(db_b);
            let cs_insert_obj = crate::sync::session_ext::Changeset::from_bytes(&cs_insert);
            crate::sync::apply::apply_changeset_lww(db_b, &cs_insert_obj).expect("apply insert");
            let snapshot = create_snapshot(db_b, temp.path(), &enc).expect("snapshot");
            ffi::sqlite3_close(db_b);

            // B pushes the snapshot with ITS applied cursors {M: K}.
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

            // The metadata must claim only K for M, not M's head K+1.
            let meta_json = storage.get_stored_snapshot_meta().expect("meta");
            let meta: SnapshotMeta = serde_json::from_slice(&meta_json).unwrap();
            assert_eq!(
                meta.cursors.get("M"),
                Some(&k),
                "snapshot meta must reflect applied seq K, not published head K+1"
            );

            // GC must NOT delete M's K+1 changeset (it is not in the snapshot).
            garbage_collect(&storage).await.expect("gc");
            storage
                .get_changeset("M", k + 1)
                .await
                .expect("K+1 must survive GC");

            // A fresh device C bootstraps from the snapshot and pulls M's
            // changesets newer than its bootstrap cursor — it must end up
            // with the "manage release" edit.
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
                let obj = crate::sync::session_ext::Changeset::from_bytes(&bytes);
                crate::sync::apply::apply_changeset_lww(db_c, &obj).expect("apply pulled");
            }

            let title = query_text(db_c, "SELECT title FROM notes WHERE id = 'n1'");
            assert_eq!(
                title, "Release Managed",
                "device C must receive the post-snapshot edit"
            );
            ffi::sqlite3_close(db_c);
        }
    }

    /// End-to-end: owner inserts + snapshots, B bootstraps, owner pushes an
    /// UPDATE, B pulls it, B snapshots (honest meta), C bootstraps + pulls and
    /// also has the update. All through the real snapshot/GC/bootstrap funcs.
    #[tokio::test]
    async fn multi_device_managed_edit_reaches_restore() {
        unsafe {
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

            let db_owner = open_memory_db();
            create_synced_schema(db_owner);
            let cs1_obj = crate::sync::session_ext::Changeset::from_bytes(&cs1);
            crate::sync::apply::apply_changeset_lww(db_owner, &cs1_obj).expect("apply cs1");
            let snap1 = create_snapshot(db_owner, temp.path(), &enc).expect("snap1");
            ffi::sqlite3_close(db_owner);

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
                query_text(db_b, "SELECT title FROM notes WHERE id = 'n1'"),
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
                let obj = crate::sync::session_ext::Changeset::from_bytes(&bytes);
                crate::sync::apply::apply_changeset_lww(db_b, &obj).expect("b apply");
                b_cursors.insert("owner".to_string(), seq);
            }
            assert_eq!(
                query_text(db_b, "SELECT title FROM notes WHERE id = 'n1'"),
                "Published"
            );

            // B snapshots its now-current state with honest cursors {owner: 2}.
            let snap2 = create_snapshot(db_b, temp.path(), &enc).expect("snap2");
            ffi::sqlite3_close(db_b);
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
                let obj = crate::sync::session_ext::Changeset::from_bytes(&bytes);
                crate::sync::apply::apply_changeset_lww(db_c, &obj).expect("c apply");
            }
            assert_eq!(
                query_text(db_c, "SELECT title FROM notes WHERE id = 'n1'"),
                "Published",
                "device C must receive the managed edit through B's snapshot + pull"
            );
            ffi::sqlite3_close(db_c);
        }
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
        unsafe {
            let enc = test_encryption();
            let temp = tempfile::tempdir().unwrap();
            let storage = MockSyncStorage::new();

            // Snapshot taken from a state where M is applied through seq 7.
            let db = open_memory_db();
            create_synced_schema(db);
            exec(
                db,
                "INSERT INTO notes (id, title, _updated_at, created_at) \
                 VALUES ('n1', 'A', '0000000001000-0000-M', '2026-01-01')",
            );
            let snap = create_snapshot(db, temp.path(), &enc).expect("snap");
            ffi::sqlite3_close(db);

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
