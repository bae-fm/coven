/// Shared test helpers for sync module tests.
///
/// These drive a real [`Database`] (one owned connection on its actor thread)
/// over an in-memory connection carrying the synthetic test schema, so tests
/// exercise the engine through the same path production does.
use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension};

use crate::database::{Database, DbError};
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::sync::apply::apply_changeset_lww;
use crate::sync::envelope::{self, ChangesetEnvelope};
use crate::sync::membership::{
    sign_membership_entry, MemberRole, MembershipAction, MembershipChain, MembershipEntry,
};
use crate::sync::pull::pull_changes;
use crate::sync::session::SyncedTable;
use crate::sync::storage::{DeviceHead, StorageError, SyncStorage};

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

/// Create the synthetic test schema on a connection. Used as the host `migrate`
/// closure for [`open_test_db`].
pub fn create_synced_schema(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE notes (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT,
            shared INTEGER NOT NULL DEFAULT 0,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE note_tags (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        );
        CREATE TABLE note_photos (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        );",
    )
    .map_err(DbError::from)
}

/// Open a [`Database`] over a fresh in-memory connection with the synthetic test
/// schema and the [`test_synced_tables`] synced set. The returned stamper is
/// dropped (tests stamp `_updated_at` literally in their SQL).
pub fn open_test_db() -> Database {
    open_test_db_with(test_synced_tables())
}

/// Like [`open_test_db`] but with an explicit synced set and schema builder, for
/// tests that exercise a different schema (gate tests).
pub fn open_test_db_schema(
    tables: Vec<SyncedTable>,
    migrate: impl FnOnce(&Connection) -> Result<(), DbError>,
) -> Database {
    // `:memory:` is unique per connection; the Database owns exactly one.
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        tables,
        "test-device".to_string(),
        migrate,
    )
    .expect("open test database");
    db
}

fn open_test_db_with(tables: Vec<SyncedTable>) -> Database {
    open_test_db_schema(tables, create_synced_schema)
}

/// Open a test [`Database`] over the synthetic schema with a caller-supplied
/// register clock (so a test can control the wall clock), plus an extra `seed`
/// step run after the schema is created (to plant `sync_state` rows or seeded
/// `notes` rows before `Database::open` reads its floor).
pub fn open_test_db_with_hlc(
    hlc: std::sync::Arc<crate::sync::hlc::Hlc>,
    seed: impl FnOnce(&Connection) -> Result<(), DbError>,
) -> Database {
    let (db, _stamper) = Database::open_with_hlc(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        hlc,
        |conn| {
            create_synced_schema(conn)?;
            seed(conn)
        },
    )
    .expect("open test database with hlc");
    db
}

/// Run a write statement on the test database (blocking on the current runtime).
pub async fn exec(db: &Database, sql: &str) {
    let sql = sql.to_string();
    db.call(move |conn| conn.execute_batch(&sql).map_err(DbError::from))
        .await
        .unwrap_or_else(|e| panic!("exec failed: {e}"));
}

/// Query a single text value from the test database.
pub async fn query_text(db: &Database, sql: &str) -> String {
    let sql = sql.to_string();
    db.call(move |conn| {
        conn.query_row(&sql, [], |r| r.get::<_, String>(0))
            .map_err(DbError::from)
    })
    .await
    .unwrap_or_else(|e| panic!("query_text failed: {e}"))
}

/// Whether a row exists for `sql` (a `SELECT 1 ...`).
pub async fn row_exists(db: &Database, sql: &str) -> bool {
    let sql = sql.to_string();
    db.call(move |conn| {
        conn.query_row(&sql, [], |_| Ok(()))
            .optional()
            .map(|o| o.is_some())
            .map_err(DbError::from)
    })
    .await
    .unwrap_or_else(|e| panic!("row_exists failed: {e}"))
}

/// Run `stmts` against the test database, then capture and return the recorded
/// changeset bytes, re-attaching the capture session for the next capture.
pub async fn capture_bytes(db: &Database, stmts: &[&str]) -> Vec<u8> {
    for s in stmts {
        exec(db, s).await;
    }
    let bytes = db
        .take_changeset_and_suspend()
        .await
        .expect("capture changeset");
    db.resume_session().await.expect("resume session");
    bytes
}

/// Apply a changeset to the test database with the production LWW path, scoped to
/// `tables`. Suspends the capture session around the apply so the applied rows
/// are not re-recorded (mirrors the cycle's lifecycle).
pub async fn apply_to_db(db: &Database, bytes: &[u8], tables: &[SyncedTable]) {
    db.take_changeset_and_suspend()
        .await
        .expect("suspend before apply");
    let bytes = bytes.to_vec();
    let tables = tables.to_vec();
    db.call(move |conn| apply_changeset_lww(conn, &bytes, &tables).map(|_| ()))
        .await
        .expect("apply changeset");
    db.resume_session().await.expect("resume after apply");
}

/// A temp dir plus a [`LibraryDir`] rooted at it. The returned `TempDir` must be
/// held for the directory to outlive the test.
pub fn temp_library_dir() -> (tempfile::TempDir, LibraryDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = LibraryDir::new(tmp.path());
    (tmp, dir)
}

/// Hex-encoded ed25519 public key, as membership entries and the wrapped-key
/// store identify a member.
pub fn pubkey_hex(kp: &UserKeypair) -> String {
    hex::encode(kp.public_key)
}

/// A fresh membership chain whose only member is `owner`, the founding owner,
/// stamped at the standard test founding time. A test that needs a different
/// founding timestamp builds the chain from `founder_entry` directly.
pub fn bootstrap_chain(owner: &UserKeypair) -> MembershipChain {
    let mut chain = MembershipChain::new();
    chain
        .add_entry(founder_entry(owner, "0000000001000-0000-dev1"))
        .unwrap();
    chain
}

/// A signed founder (first) membership entry for `kp`.
pub fn founder_entry(kp: &UserKeypair, timestamp: &str) -> MembershipEntry {
    let pk_hex = pubkey_hex(kp);
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
        user_pubkey: pubkey_hex(subject),
        role,
        timestamp: timestamp.to_string(),
        author_pubkey: pubkey_hex(author),
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
    /// Published device heads, keyed by device_id.
    heads: Mutex<HashMap<String, DeviceHead>>,
    /// The single shared snapshot blob (`snapshot.db{suffix}`).
    snapshot: Mutex<Option<Vec<u8>>>,
    /// The snapshot's per-device cursor metadata (`snapshot_meta.json{suffix}`).
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
        let mut heads = self.heads.lock().unwrap();
        heads
            .entry(device_id.to_string())
            .or_insert_with(|| DeviceHead {
                device_id: device_id.to_string(),
                seq,
                snapshot_seq: None,
                last_sync: None,
            })
            .seq = seq;
    }
}

#[async_trait]
impl SyncStorage for MockSyncStorage {
    async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError> {
        Ok(self.heads.lock().unwrap().values().cloned().collect())
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
        self.heads.lock().unwrap().insert(
            device_id.to_string(),
            DeviceHead {
                device_id: device_id.to_string(),
                seq,
                snapshot_seq: None,
                last_sync: None,
            },
        );
        Ok(())
    }

    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        _scope: crate::blob::ResolvedScope,
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
        _scope: crate::blob::ResolvedScope,
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
            .ok_or(StorageError::NotFound("snapshot.db".into()))
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
            .ok_or(StorageError::NotFound("snapshot_meta.json".into()))
    }
}

#[async_trait]
impl CloudHome for MockSyncStorage {
    async fn write(
        &self,
        key: &str,
        data: Vec<u8>,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        let total = data.len() as u64;
        self.objects.lock().unwrap().insert(key.to_string(), data);
        progress(total);
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

/// A [`BlobUploadObserver`](crate::blob::BlobUploadObserver) that breaks the
/// outbox drain after every upload by returning
/// [`DrainControl::Publish`](crate::blob::DrainControl), modeling a host that
/// flips a gate column on the moment a unit's blobs land. The other callbacks are
/// no-ops. Shared by the cycle and outbox drain tests.
pub struct PublishingObserver;

#[async_trait]
impl crate::blob::BlobUploadObserver for PublishingObserver {
    async fn on_blob_upload_started(&self, _file_id: &str) {}
    async fn on_blob_uploaded(&self, _file_id: &str) -> crate::blob::DrainControl {
        crate::blob::DrainControl::Publish
    }
    async fn on_blob_upload_failed(&self, _file_id: &str, _error: &str) {}
}

/// Pull into `db` the way the protocol requires: suspend its capture session
/// first (so applying remote rows isn't re-recorded as a local change), pull +
/// apply, then resume. Returns the updated cursors and the pull result.
pub async fn pull_into(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    cursors: &HashMap<String, u64>,
    library_dir: &crate::library_dir::LibraryDir,
    blob_plan: &dyn crate::blob::BlobPlan,
) -> (HashMap<String, u64>, crate::sync::pull::PullResult) {
    db.take_changeset_and_suspend()
        .await
        .expect("suspend before pull");
    let r = pull_changes(
        db,
        &test_synced_tables(),
        storage,
        device_id,
        cursors,
        library_dir,
        blob_plan,
    )
    .await
    .expect("pull");
    db.resume_session().await.expect("resume after pull");
    r
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
    fn blobs_in_db(&self, _conn: &Connection) -> rusqlite::Result<Vec<crate::blob::BlobRef>> {
        Ok(Vec::new())
    }
}

/// Build one [`crate::blob::BlobRef`] for a `note_photos` row from its
/// `(id, note_id, kind)`, under `dir` in `namespace`, scoping via
/// `scope_for(kind, note_id)`. The single per-row builder both the changeset and
/// the DB enumeration paths call, so a photo blob's path and scope are derived
/// one way regardless of whether the row came from a [`crate::changeset::RowChange`]
/// or a `SELECT`.
pub fn note_photo_ref(
    id: &str,
    note_id: &str,
    kind: &str,
    dir: &std::path::Path,
    namespace: &str,
    scope_for: &dyn Fn(&str, &str) -> crate::blob::BlobScope,
) -> crate::blob::BlobRef {
    crate::blob::BlobRef {
        namespace: namespace.to_string(),
        local_path: dir.join(id),
        id: id.to_string(),
        scope: scope_for(kind, note_id),
    }
}

/// Map every `note_photos` INSERT row-change to a [`crate::blob::BlobRef`] under
/// `dir` in `namespace`, scoping each via `scope_for(kind, note_id)`. The
/// changeset-driven analogue of [`note_photos_refs_from_db`]; both build each ref
/// through [`note_photo_ref`]. Filtered to INSERTs (matching bae's real
/// `BlobPlan`): a blob's bytes never change on update, so only an INSERT carries
/// one, and an INSERT records every column — so `id`/`note_id`/`kind` are always
/// present, where a partial UPDATE could leave `kind` absent. Their absence on an
/// INSERT is a malformed test fixture, surfaced loudly rather than silently
/// skipped.
pub fn note_photos_refs(
    changes: &[crate::changeset::RowChange],
    dir: &std::path::Path,
    namespace: &str,
    scope_for: &dyn Fn(&str, &str) -> crate::blob::BlobScope,
) -> Vec<crate::blob::BlobRef> {
    use crate::changeset::ChangeOp;
    changes
        .iter()
        .filter(|c| c.table == "note_photos" && c.op == ChangeOp::Insert)
        .map(|c| {
            let id = c.pk().expect("note_photos row has a primary key");
            let note_id = c.col(1).expect("note_photos row has a note_id at column 1");
            let kind = c.col(2).expect("note_photos row has a kind at column 2");
            note_photo_ref(id, note_id, kind, dir, namespace, scope_for)
        })
        .collect()
}

/// Map every `note_photos` row currently in `conn` to a [`crate::blob::BlobRef`],
/// the snapshot-bootstrap analogue of [`note_photos_refs`]: the rows the
/// bootstrapped DB already holds, rather than an incoming changeset's. Both build
/// each ref through [`note_photo_ref`], so a photo blob lands at the same path and
/// scope however it was discovered.
pub fn note_photos_refs_from_db(
    conn: &Connection,
    dir: &std::path::Path,
    namespace: &str,
    scope_for: &dyn Fn(&str, &str) -> crate::blob::BlobScope,
) -> rusqlite::Result<Vec<crate::blob::BlobRef>> {
    let mut stmt = conn.prepare("SELECT id, note_id, kind FROM note_photos")?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;
    let mut refs = Vec::new();
    for row in rows {
        let (id, note_id, kind) = row?;
        refs.push(note_photo_ref(
            &id, &note_id, &kind, dir, namespace, scope_for,
        ));
    }
    Ok(refs)
}

/// A blob plan that maps `note_photos` rows to blobs under `dir`. `kind = "cover"`
/// uses a per-note derived key; everything else uses the master key.
pub struct PhotoBlobPlan {
    pub dir: std::path::PathBuf,
}

impl PhotoBlobPlan {
    /// The scope every `note_photos` row maps to, regardless of discovery path:
    /// a cover photo uses a per-note derived key, everything else the master key.
    fn scope_for(kind: &str, note_id: &str) -> crate::blob::BlobScope {
        if kind == "cover" {
            crate::blob::BlobScope::Derived(note_id.to_string())
        } else {
            crate::blob::BlobScope::Master
        }
    }

    fn refs(&self, changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        note_photos_refs(changes, &self.dir, "photos", &Self::scope_for)
    }
}

impl crate::blob::BlobPlan for PhotoBlobPlan {
    fn blobs_to_push(&self, changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        self.refs(changes)
    }
    fn blobs_to_pull(&self, changes: &[crate::changeset::RowChange]) -> Vec<crate::blob::BlobRef> {
        self.refs(changes)
    }
    fn blobs_in_db(&self, conn: &Connection) -> rusqlite::Result<Vec<crate::blob::BlobRef>> {
        note_photos_refs_from_db(conn, &self.dir, "photos", &Self::scope_for)
    }
}
