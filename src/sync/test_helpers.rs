/// Shared test helpers for sync module tests.
///
/// These operate on raw sqlite3 connections via libsqlite3-sys.
use std::collections::HashMap;
use std::ffi::{c_char, c_int, CStr, CString};
use std::ptr;
use std::sync::Mutex;

use async_trait::async_trait;
use libsqlite3_sys as ffi;

use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::sync::envelope::{self, ChangesetEnvelope};
use crate::sync::membership::{
    sign_membership_entry, MemberRole, MembershipAction, MembershipEntry,
};
use crate::sync::session::{SyncSession, SyncedTable};
use crate::sync::storage::{DeviceHead, StorageError, SyncStorage};

/// Open an in-memory sqlite3 database via libsqlite3-sys directly.
pub unsafe fn open_memory_db() -> *mut ffi::sqlite3 {
    let mut db: *mut ffi::sqlite3 = ptr::null_mut();
    let rc = ffi::sqlite3_open(c":memory:".as_ptr(), &mut db);
    assert_eq!(rc, ffi::SQLITE_OK as c_int, "Failed to open in-memory DB");
    db
}

/// Execute a SQL statement on a raw connection.
pub unsafe fn exec(db: *mut ffi::sqlite3, sql: &str) {
    let c_sql = CString::new(sql).unwrap();
    let rc = ffi::sqlite3_exec(db, c_sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());
    assert_eq!(rc, ffi::SQLITE_OK as c_int, "exec failed for: {sql}");
}

/// Query a single integer value.
pub unsafe fn query_int(db: *mut ffi::sqlite3, sql: &str) -> i64 {
    let c_sql = CString::new(sql).unwrap();
    let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
    assert_eq!(rc, ffi::SQLITE_OK as c_int, "prepare failed for: {sql}");

    let step = ffi::sqlite3_step(stmt);
    assert_eq!(step, ffi::SQLITE_ROW as c_int, "expected a row for: {sql}");

    let val = ffi::sqlite3_column_int64(stmt, 0);
    ffi::sqlite3_finalize(stmt);
    val
}

/// Query a single text value.
pub unsafe fn query_text(db: *mut ffi::sqlite3, sql: &str) -> String {
    let c_sql = CString::new(sql).unwrap();
    let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
    assert_eq!(rc, ffi::SQLITE_OK as c_int, "prepare failed for: {sql}");

    let step = ffi::sqlite3_step(stmt);
    assert_eq!(step, ffi::SQLITE_ROW as c_int, "expected a row for: {sql}");

    let ptr = ffi::sqlite3_column_text(stmt, 0);
    let val = CStr::from_ptr(ptr as *const c_char)
        .to_string_lossy()
        .into_owned();
    ffi::sqlite3_finalize(stmt);
    val
}

/// Query whether a row exists.
pub unsafe fn row_exists(db: *mut ffi::sqlite3, sql: &str) -> bool {
    let c_sql = CString::new(sql).unwrap();
    let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
    assert_eq!(rc, ffi::SQLITE_OK as c_int, "prepare failed for: {sql}");
    let step = ffi::sqlite3_step(stmt);
    ffi::sqlite3_finalize(stmt);
    step == ffi::SQLITE_ROW as c_int
}

/// The synthetic, domain-free schema the sync tests run against. Three synced
/// tables exercising the engine's generic mechanics: a *gated root* (`notes`,
/// gated by its `shared` boolean), a child with a foreign key (`note_tags`,
/// which inherits the gate and exercises FK-violation retry), and a blob-bearing
/// child (`note_photos`, also FK-to-`notes`, so it inherits the gate too).
pub fn test_synced_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes").gated_by("shared"),
        SyncedTable::new("note_tags"),
        SyncedTable::new("note_photos"),
    ]
}

/// Declare [`test_synced_tables`] as the synced set (idempotent; first call wins).
pub fn init_synced_tables() {
    crate::sync::session::set_synced_tables(&test_synced_tables());
}

pub unsafe fn create_synced_schema(db: *mut ffi::sqlite3) {
    // The synced set is process-global; register it here so any test that
    // builds this schema (and then snapshots) has a synced set to scope by.
    // `create_snapshot` requires one and refuses to emit an all-cleared blob
    // otherwise.
    init_synced_tables();
    exec(db, "PRAGMA foreign_keys = ON");
    exec(
        db,
        "CREATE TABLE notes (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT,
            shared INTEGER NOT NULL DEFAULT 0,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        )",
    );
    exec(
        db,
        "CREATE TABLE note_tags (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        )",
    );
    exec(
        db,
        "CREATE TABLE note_photos (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        )",
    );
}

/// Capture a changeset's bytes after running `stmts` against `db`.
pub unsafe fn capture_bytes(db: *mut ffi::sqlite3, stmts: &[&str]) -> Vec<u8> {
    let session = SyncSession::start(db).expect("start session");
    for s in stmts {
        exec(db, s);
    }
    session
        .changeset()
        .expect("changeset")
        .expect("non-empty")
        .as_bytes()
        .to_vec()
}

/// A temp dir plus a [`LibraryDir`] rooted at it. The returned `TempDir` must be
/// held for the directory to outlive the test.
pub fn temp_library_dir() -> (tempfile::TempDir, LibraryDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = LibraryDir::new(tmp.path());
    (tmp, dir)
}

/// A signed founder (first) membership entry for `kp`.
pub fn founder_entry(kp: &UserKeypair, timestamp: &str) -> MembershipEntry {
    let pk_hex = hex::encode(kp.public_key);
    let mut entry = MembershipEntry {
        action: MembershipAction::Add,
        user_pubkey: pk_hex.clone(),
        role: MemberRole::Owner,
        timestamp: timestamp.to_string(),
        author_pubkey: pk_hex,
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, kp);
    entry
}

/// A signed entry where `author` adds/removes `subject` with `role`.
pub fn make_entry(
    author: &UserKeypair,
    action: MembershipAction,
    subject: &UserKeypair,
    role: MemberRole,
    timestamp: &str,
) -> MembershipEntry {
    let mut entry = MembershipEntry {
        action,
        user_pubkey: hex::encode(subject.public_key),
        role,
        timestamp: timestamp.to_string(),
        author_pubkey: hex::encode(author.public_key),
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, author);
    entry
}

/// In-memory mock of SyncStorage for tests.
/// Stores changesets as plaintext (no encryption in tests).
pub struct MockSyncStorage {
    /// Changesets: key = "changes/{device_id}/{seq}" -> packed envelope bytes.
    objects: Mutex<HashMap<String, Vec<u8>>>,
    /// Heads: device_id -> seq.
    heads: Mutex<HashMap<String, u64>>,
    /// The single shared snapshot blob (`snapshot.db.enc`).
    snapshot: Mutex<Option<Vec<u8>>>,
    /// The snapshot's per-device cursor metadata (`snapshot_meta.json.enc`).
    snapshot_meta: Mutex<Option<Vec<u8>>>,
    /// Minimum schema version marker (None = no minimum set).
    min_schema_version: Mutex<Option<u32>>,
}

impl MockSyncStorage {
    pub fn new() -> Self {
        MockSyncStorage {
            objects: Mutex::new(HashMap::new()),
            heads: Mutex::new(HashMap::new()),
            snapshot: Mutex::new(None),
            snapshot_meta: Mutex::new(None),
            min_schema_version: Mutex::new(None),
        }
    }

    /// Store a changeset in the mock storage (simulates what push would do).
    pub fn store_changeset(
        &self,
        device_id: &str,
        seq: u64,
        changeset_bytes: &[u8],
        schema_version: u32,
    ) {
        let env = ChangesetEnvelope {
            device_id: device_id.to_string(),
            seq,
            schema_version,
            message: String::new(),
            timestamp: "2026-02-10T00:00:00Z".to_string(),
            changeset_size: changeset_bytes.len(),
            author_pubkey: None,
            signature: None,
        };
        let packed = envelope::pack(&env, changeset_bytes);
        self.store_packed(device_id, seq, packed);
    }

    /// Store a pre-packed envelope (already signed/packed by the caller) and
    /// advance the device head. For tests that need a specific signature or
    /// envelope timestamp `store_changeset`'s synthetic envelope can't express.
    pub fn put_changeset_packed(&self, device_id: &str, seq: u64, packed: Vec<u8>) {
        self.store_packed(device_id, seq, packed);
    }

    /// Insert packed envelope bytes at `changes/{device_id}/{seq}` and advance
    /// the device head to `seq`.
    fn store_packed(&self, device_id: &str, seq: u64, packed: Vec<u8>) {
        let key = format!("changes/{device_id}/{seq}");
        self.objects.lock().unwrap().insert(key, packed);
        self.heads
            .lock()
            .unwrap()
            .insert(device_id.to_string(), seq);
    }
}

#[async_trait]
impl SyncStorage for MockSyncStorage {
    async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError> {
        let heads = self.heads.lock().unwrap();
        Ok(heads
            .iter()
            .map(|(id, &seq)| DeviceHead {
                device_id: id.clone(),
                seq,
                snapshot_seq: None,
                last_sync: None,
            })
            .collect())
    }

    async fn get_changeset(&self, device_id: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        let key = format!("changes/{device_id}/{seq}");
        let objects = self.objects.lock().unwrap();
        objects
            .get(&key)
            .cloned()
            .ok_or(StorageError::NotFound(key))
    }

    async fn put_changeset(
        &self,
        device_id: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = format!("changes/{device_id}/{seq}");
        self.objects.lock().unwrap().insert(key, data);
        Ok(())
    }

    async fn put_head(
        &self,
        device_id: &str,
        seq: u64,
        _snapshot_seq: Option<u64>,
        _timestamp: &str,
    ) -> Result<(), StorageError> {
        self.heads
            .lock()
            .unwrap()
            .insert(device_id.to_string(), seq);
        Ok(())
    }

    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        _scope: crate::blob::BlobScope,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = format!("{namespace}/{id}");
        self.objects.lock().unwrap().insert(key, data);
        Ok(())
    }

    async fn get_blob(
        &self,
        namespace: &str,
        id: &str,
        _scope: crate::blob::BlobScope,
    ) -> Result<Vec<u8>, StorageError> {
        let key = format!("{namespace}/{id}");
        let objects = self.objects.lock().unwrap();
        objects
            .get(&key)
            .cloned()
            .ok_or(StorageError::NotFound(key))
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

    async fn delete_changeset(&self, _device_id: &str, _seq: u64) -> Result<(), StorageError> {
        Ok(())
    }

    async fn list_changesets(&self, _device_id: &str) -> Result<Vec<u64>, StorageError> {
        Ok(vec![])
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
        author_pubkey: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = format!("membership/{author_pubkey}/{seq}");
        self.objects.lock().unwrap().insert(key, data);
        Ok(())
    }

    async fn get_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let key = format!("membership/{author_pubkey}/{seq}");
        let objects = self.objects.lock().unwrap();
        objects
            .get(&key)
            .cloned()
            .ok_or(StorageError::NotFound(key))
    }

    async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError> {
        let objects = self.objects.lock().unwrap();
        let mut entries = Vec::new();

        for key in objects.keys() {
            if let Some(rest) = key.strip_prefix("membership/") {
                if let Some(slash_pos) = rest.rfind('/') {
                    let author = &rest[..slash_pos];
                    if let Ok(seq) = rest[slash_pos + 1..].parse::<u64>() {
                        entries.push((author.to_string(), seq));
                    }
                }
            }
        }

        Ok(entries)
    }

    async fn put_wrapped_key(&self, user_pubkey: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let key = format!("keys/{user_pubkey}.enc");
        self.objects.lock().unwrap().insert(key, data);
        Ok(())
    }

    async fn get_wrapped_key(&self, user_pubkey: &str) -> Result<Vec<u8>, StorageError> {
        let key = format!("keys/{user_pubkey}.enc");
        let objects = self.objects.lock().unwrap();
        objects
            .get(&key)
            .cloned()
            .ok_or(StorageError::NotFound(key))
    }

    async fn delete_wrapped_key(&self, user_pubkey: &str) -> Result<(), StorageError> {
        let key = format!("keys/{user_pubkey}.enc");
        self.objects.lock().unwrap().remove(&key);
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

#[async_trait]
impl CloudHome for MockSyncStorage {
    async fn write(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.objects.lock().unwrap().insert(key.to_string(), data);
        Ok(())
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        let data = self.read(key).await?;
        Ok(data[start as usize..end as usize].to_vec())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        let objects = self.objects.lock().unwrap();
        Ok(objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        Ok(self.objects.lock().unwrap().contains_key(key))
    }

    async fn grant_access(&self, _member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        Ok(CloudHomeJoinInfo::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key: "test-access-key".to_string(),
            secret_key: "test-secret-key".to_string(),
            key_prefix: None,
        })
    }

    async fn revoke_access(&self, _member_id: &str) -> Result<(), CloudHomeError> {
        Ok(())
    }
}

/// A blob plan that references no blobs — for cycle/pull tests not exercising blobs.
pub struct NoopBlobPlan;

impl crate::blob::BlobPlan for NoopBlobPlan {
    fn blobs_to_push(&self, _changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        Vec::new()
    }
    fn blobs_to_pull(&self, _changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        Vec::new()
    }
}

/// A blob plan that maps `note_photos` rows to blobs under `dir`. `kind = "cover"`
/// uses a per-note derived key; everything else uses the master key.
pub struct PhotoBlobPlan {
    pub dir: std::path::PathBuf,
}

impl PhotoBlobPlan {
    fn refs(&self, changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        use crate::blob::{BlobRef, BlobScope};
        use crate::changeset::ChangeOp;
        changes
            .iter()
            .filter(|c| c.table == "note_photos" && c.op != ChangeOp::Delete)
            .filter_map(|c| {
                let id = c.pk()?.to_string();
                let note_id = c.col(1)?.to_string();
                let scope = if c.col(2) == Some("cover") {
                    BlobScope::Derived(note_id)
                } else {
                    BlobScope::Master
                };
                Some(BlobRef {
                    namespace: "photos".to_string(),
                    local_path: self.dir.join(&id),
                    id,
                    scope,
                })
            })
            .collect()
    }
}

impl crate::blob::BlobPlan for PhotoBlobPlan {
    fn blobs_to_push(&self, changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        self.refs(changes)
    }
    fn blobs_to_pull(&self, changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        self.refs(changes)
    }
}
