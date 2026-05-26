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
/// reads the bytes, encrypts, and returns the encrypted blob.
///
/// # Safety
/// `db` must be a valid, open sqlite3 connection pointer.
pub unsafe fn create_snapshot(
    db: *mut ffi::sqlite3,
    temp_dir: &Path,
    encryption: &EncryptionService,
) -> Result<Vec<u8>, SnapshotError> {
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
        let _ = std::fs::remove_file(&snapshot_path);
        return Err(SnapshotError::VacuumFailed(msg));
    }

    // Read the snapshot file and encrypt.
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

/// Upload a snapshot to the sync storage and update the device head.
///
/// Also uploads per-device cursor metadata (`snapshot_meta.json.enc`) so that
/// bootstrapping devices know where each device was at snapshot time, and GC
/// can safely delete only changesets covered by the snapshot.
pub async fn push_snapshot(
    storage: &dyn SyncStorage,
    encrypted_snapshot: Vec<u8>,
    device_id: &str,
    current_seq: u64,
    clock: &dyn crate::clock::Clock,
) -> Result<(), SnapshotError> {
    let size = encrypted_snapshot.len();
    let timestamp = clock.now().to_rfc3339();

    // Upload snapshot (overwrites previous).
    storage.put_snapshot(encrypted_snapshot).await?;

    // Read all heads and build per-device cursor map for snapshot metadata.
    let heads = storage.list_heads().await?;
    let mut cursors: HashMap<String, u64> =
        heads.iter().map(|h| (h.device_id.clone(), h.seq)).collect();
    // Ensure our own current_seq is included (our head hasn't been updated yet).
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
                "INSERT INTO notes (id, title, _updated_at, created_at) \
                 VALUES ('a1', 'Artist One', '0000000001000-0000-dev1', '2026-01-01')",
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

    // ---- push_snapshot tests ----

    #[tokio::test]
    async fn push_snapshot_uploads_and_updates_head() {
        let storage = MockSyncStorage::new();
        // Simulate another device that already has a head.
        storage
            .put_head("dev-2", 15, None, "2026-02-10T00:00:00Z")
            .await
            .unwrap();
        let data = vec![1, 2, 3, 4, 5];

        push_snapshot(
            &storage,
            data.clone(),
            "dev-1",
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

        // Snapshot metadata should contain cursors for both devices.
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
                "INSERT INTO notes (id, title, _updated_at, created_at) \
                 VALUES ('a1', 'Artist One', '0000000001000-0000-dev1', '2026-01-01')",
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
                "INSERT INTO notes (id, title, _updated_at, created_at) \
                 VALUES ('a1', 'Artist One', '0000000001000-0000-dev1', '2026-01-01')",
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
            push_snapshot(&storage, encrypted, "dev-1", 5, &crate::clock::SystemClock)
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
                "INSERT INTO notes (id, title, _updated_at, created_at) \
                 VALUES ('a1', 'Artist One', '0000000001000-0000-dev1', '2026-01-01')",
            );
            exec(
                db_source,
                "INSERT INTO notes (id, title, _updated_at, created_at) \
                 VALUES ('a2', 'Artist Two', '0000000002000-0000-dev1', '2026-01-01')",
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
                "INSERT INTO notes (id, title, _updated_at, created_at) \
                 VALUES ('a3', 'Artist Three', '0000000003000-0000-dev1', '2026-01-01')",
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
}
