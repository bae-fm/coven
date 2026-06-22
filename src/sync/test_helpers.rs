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
use crate::sync::signed_control::{HeadJson, MinSchemaVersionJson};
use crate::sync::storage::{DeviceHead, MinSchemaVersion, StorageError, SyncStorage};

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
///
/// Used only by the native-only register-clock tests (`hlc_register_tests`).
#[cfg(not(target_arch = "wasm32"))]
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
    let receiver_wall_ms = db.receive_wall_ms();
    db.call(move |conn| apply_changeset_lww(conn, &bytes, &tables, receiver_wall_ms).map(|_| ()))
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

/// A signed founder (first) membership entry for `kp`. Thin alias over the
/// production [`crate::sync::membership::founder_entry`] so tests and production
/// build the founder identically.
pub fn founder_entry(kp: &UserKeypair, timestamp: &str) -> MembershipEntry {
    crate::sync::membership::founder_entry(kp, timestamp)
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

/// The object key [`MockSyncStorage`] stores a blob under. A plain-scheme blob
/// (one carrying a `cloud_path`) delegates to [`CloudSyncStorage::blob_key`] so it
/// is exactly the readable key production writes; an obfuscated one keys flat by id
/// as `{namespace}/{id}` (the mock deliberately doesn't shard — a flat id key is
/// unambiguous for tests and never needs to match production's `{ab}/{cd}` layout).
fn blob_key(namespace: &str, id: &str, cloud_path: Option<&str>) -> String {
    match cloud_path {
        Some(path) => crate::sync::cloud_storage::CloudSyncStorage::blob_key(
            crate::sync::cloud_storage::BlobPathScheme::Plain,
            namespace,
            id,
            Some(path),
        )
        .expect("plain blob_key with a cloud_path is always Ok"),
        None => format!("{namespace}/{id}"),
    }
}

/// In-memory mock of SyncStorage for tests.
///
/// Stores changesets as plaintext (no encryption in tests) but signs and verifies
/// the head and min_schema control objects through the same
/// [`crate::sync::signed_control`] helpers production uses, so tests exercise a
/// faithful sign-on-write / verify-on-read path rather than a parallel one.
pub struct MockSyncStorage {
    /// Changesets: key = "changes/{device_id}/{seq}" -> packed envelope bytes.
    objects: Mutex<HashMap<String, Vec<u8>>>,
    /// Published device heads, keyed by device_id -> signed `HeadJson` JSON bytes
    /// (exactly what the cloud stores, minus the at-rest cipher).
    heads: Mutex<HashMap<String, Vec<u8>>>,
    /// The single shared snapshot blob (`snapshot.db{suffix}`).
    snapshot: Mutex<Option<Vec<u8>>>,
    /// The snapshot's per-device cursor metadata (`snapshot_meta.json{suffix}`).
    snapshot_meta: Mutex<Option<Vec<u8>>>,
    /// Signed `min_schema_version.json` bytes (None = no minimum set).
    min_schema_version: Mutex<Option<Vec<u8>>>,
    /// The device identity this mock signs its head/min_schema with. Defaults to a
    /// fresh keypair; a membership test that needs the head/floor attributed to a
    /// specific member constructs the mock with [`Self::with_keypair`].
    keypair: UserKeypair,
    /// When set, `list_membership_entries` returns an error — to exercise the
    /// fail-closed path where membership can't even be listed (#88), instead of
    /// silently disabling authorization for the cycle.
    fail_membership_list: std::sync::atomic::AtomicBool,
    /// `(author_pubkey, seq)` entries the LIST omits but a keyed GET still serves.
    /// Simulates the eventual-consistency window where a freshly-written
    /// membership entry isn't in the LIST yet, but a direct GET (read-after-write
    /// consistent) resolves it — the exact lag issue #84's grant-coordinate fetch
    /// is built for.
    hidden_from_listing: Mutex<std::collections::HashSet<(String, u64)>>,
}

impl MockSyncStorage {
    pub fn new() -> Self {
        Self::with_keypair(UserKeypair::generate())
    }

    /// A mock whose head and min_schema control objects are signed by `keypair`.
    /// Lets a membership test attribute the head it publishes (and any floor it
    /// sets) to a specific member, the way a real device signs its own.
    pub fn with_keypair(keypair: UserKeypair) -> Self {
        MockSyncStorage {
            objects: Mutex::new(HashMap::new()),
            heads: Mutex::new(HashMap::new()),
            snapshot: Mutex::new(None),
            snapshot_meta: Mutex::new(None),
            min_schema_version: Mutex::new(None),
            keypair,
            fail_membership_list: std::sync::atomic::AtomicBool::new(false),
            hidden_from_listing: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Make `list_membership_entries` fail, so a test can assert the cycle fails
    /// closed (refuses to apply changesets) when membership can't be listed,
    /// rather than falling open to "no chain, accept everything" (#88).
    pub fn fail_membership_listing(&self) {
        self.fail_membership_list
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Hide a stored membership entry from `list_membership_entries` while leaving
    /// `get_membership_entry` able to serve it — the eventual-consistency window
    /// where the LIST that rebuilds the chain lags an entry a direct (keyed,
    /// read-after-write consistent) GET already resolves. Lets a test stage a
    /// member's authorizing Add, hide it from the LIST, and prove issue #84's
    /// grant-coordinate fetch recovers the changeset instead of dropping it.
    pub fn hide_membership_from_listing(&self, author_pubkey: &str, seq: u64) {
        self.hidden_from_listing
            .lock()
            .unwrap()
            .insert((author_pubkey.to_string(), seq));
    }

    /// Remove a blob object from the mock cloud, keyed the same flat
    /// `{namespace}/{id}` way [`Self::put_blob`] stores a no-`cloud_path` blob. Lets
    /// a cache test delete the cloud copy after a read populated the local cache, to
    /// prove a second read is served from disk (a re-fetch would now fail).
    pub async fn delete_blob_object(&self, namespace: &str, id: &str) {
        let key = blob_key(namespace, id, None);
        assert!(
            self.objects.lock().unwrap().remove(&key).is_some(),
            "delete_blob_object: no mock cloud blob at {key} to delete (test-setup bug)",
        );
    }

    /// Store a changeset in the mock storage (simulates what push would do). The
    /// changeset itself is left unsigned (for tests exercising unsigned-changeset
    /// rejection); the device head it advances is signed by the mock's keypair.
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
            membership_grant: None,
            signature: None,
        };
        let packed = envelope::pack(&env, changeset_bytes);
        self.store_packed(device_id, seq, packed);
    }

    /// Store a pre-packed envelope (already signed/packed by the caller) and
    /// advance the device head. For tests that need a specific signature or
    /// envelope timestamp `store_changeset`'s synthetic envelope can't express.
    ///
    /// Used only by the native-only register-clock tests (`hlc_register_tests`).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn put_changeset_packed(&self, device_id: &str, seq: u64, packed: Vec<u8>) {
        self.store_packed(device_id, seq, packed);
    }

    /// Insert packed envelope bytes at `changes/{device_id}/{seq}` and advance the
    /// device head to `seq`, signing the head with the mock's keypair.
    fn store_packed(&self, device_id: &str, seq: u64, packed: Vec<u8>) {
        let key = format!("changes/{device_id}/{seq}");
        self.objects.lock().unwrap().insert(key, packed);
        self.publish_head(device_id, seq, None);
    }

    /// Publish a signed head for `device_id` at `seq`, exactly as `put_head` does,
    /// but reusable by the changeset-fabrication helpers. Signs with the mock's
    /// keypair and stores the resulting `HeadJson` JSON bytes.
    fn publish_head(&self, device_id: &str, seq: u64, snapshot_seq: Option<u64>) {
        let head = HeadJson::signed(device_id, seq, snapshot_seq, None, &self.keypair);
        let bytes = serde_json::to_vec(&head).expect("serialize head");
        self.heads
            .lock()
            .unwrap()
            .insert(device_id.to_string(), bytes);
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl SyncStorage for MockSyncStorage {
    async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError> {
        let heads = self.heads.lock().unwrap();
        let mut out = Vec::new();
        for (device_id, bytes) in heads.iter() {
            let head: HeadJson = serde_json::from_slice(bytes)
                .map_err(|e| StorageError::S3(format!("parse head {device_id}: {e}")))?;
            // Mirror the cloud: a head whose signature doesn't verify against its
            // slot is skipped, not returned — and logged, like production.
            if !head.verify(device_id) {
                tracing::warn!("skipping head {device_id} with an invalid signature");
                continue;
            }
            out.push(DeviceHead {
                device_id: device_id.clone(),
                seq: head.seq,
                snapshot_seq: head.snapshot_seq,
                last_sync: head.last_sync,
                author_pubkey: head.author_pubkey,
            });
        }
        Ok(out)
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
        snapshot_seq: Option<u64>,
        _timestamp: &str,
    ) -> Result<(), StorageError> {
        self.publish_head(device_id, seq, snapshot_seq);
        Ok(())
    }

    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        _scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = blob_key(namespace, id, cloud_path);
        self.objects.lock().unwrap().insert(key, data);
        Ok(())
    }

    async fn get_blob(
        &self,
        namespace: &str,
        id: &str,
        _scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
    ) -> Result<Vec<u8>, StorageError> {
        let key = blob_key(namespace, id, cloud_path);
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

    async fn get_min_schema_version(&self) -> Result<Option<MinSchemaVersion>, StorageError> {
        let stored = self.min_schema_version.lock().unwrap();
        let Some(bytes) = stored.as_ref() else {
            return Ok(None);
        };
        let parsed: MinSchemaVersionJson = serde_json::from_slice(bytes)
            .map_err(|e| StorageError::S3(format!("parse min_schema_version: {e}")))?;
        // Mirror the cloud: an unverifiable floor is treated as absent, and logged.
        if !parsed.verify() {
            tracing::warn!("ignoring min_schema_version with an invalid signature");
            return Ok(None);
        }
        Ok(Some(MinSchemaVersion {
            version: parsed.min_schema_version,
            author_pubkey: parsed.author_pubkey,
        }))
    }

    async fn set_min_schema_version(&self, version: u32) -> Result<(), StorageError> {
        let payload = MinSchemaVersionJson::signed(version, &self.keypair);
        let bytes = serde_json::to_vec(&payload).expect("serialize min_schema_version");
        *self.min_schema_version.lock().unwrap() = Some(bytes);
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
        if self
            .fail_membership_list
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StorageError::S3("injected membership-list failure".into()));
        }
        let objects = self.objects.lock().unwrap();
        let hidden = self.hidden_from_listing.lock().unwrap();
        let mut entries = Vec::new();

        for key in objects.keys() {
            if let Some(rest) = key.strip_prefix("membership/") {
                if let Some(slash_pos) = rest.rfind('/') {
                    let author = &rest[..slash_pos];
                    if let Ok(seq) = rest[slash_pos + 1..].parse::<u64>() {
                        // Omit entries a test marked as not-yet-visible to the LIST
                        // (issue #84's eventual-consistency lag); a keyed GET still
                        // serves them.
                        if !hidden.contains(&(author.to_string(), seq)) {
                            entries.push((author.to_string(), seq));
                        }
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
    blob_source: &dyn crate::blob::BlobSource,
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
        blob_source,
    )
    .await
    .expect("pull");
    db.resume_session().await.expect("resume after pull");
    r
}

/// Like [`pull_into`] but returns the `pull_changes` result without unwrapping, so
/// a test can assert a [`crate::sync::pull::PullError`]. Suspends/resumes the
/// capture session around it just like `pull_into`.
#[allow(clippy::type_complexity)]
pub async fn pull_into_result(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    cursors: &HashMap<String, u64>,
    library_dir: &crate::library_dir::LibraryDir,
    blob_source: &dyn crate::blob::BlobSource,
) -> Result<(HashMap<String, u64>, crate::sync::pull::PullResult), crate::sync::pull::PullError> {
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
        blob_source,
    )
    .await;
    db.resume_session().await.expect("resume after pull");
    r
}

/// A blob source that references no blobs — for cycle/pull tests not exercising blobs.
pub struct NoopBlobSource;

impl crate::blob::BlobSource for NoopBlobSource {
    fn blobs_for_change(&self, _change: &crate::changeset::RowChange) -> Vec<crate::blob::BlobRef> {
        Vec::new()
    }
    fn blobs_in_db(&self, _conn: &Connection) -> rusqlite::Result<Vec<crate::blob::BlobRef>> {
        Ok(Vec::new())
    }
}

/// Build one [`crate::blob::BlobRef`] for a `note_photos` row from its
/// `(id, note_id, kind)`, under `dir` in `namespace`, scoping via
/// `scope_for(kind, note_id)` and tagging the retention class `sync`. The single
/// per-row builder both the changeset and the DB enumeration paths call, so a
/// photo blob's path, scope, and class are derived one way regardless of whether
/// the row came from a [`crate::changeset::RowChange`] or a `SELECT`.
pub fn note_photo_ref(
    id: &str,
    note_id: &str,
    kind: &str,
    dir: &std::path::Path,
    namespace: &str,
    scope_for: &dyn Fn(&str, &str) -> crate::blob::BlobScope,
    sync: crate::blob::BlobSync,
) -> crate::blob::BlobRef {
    crate::blob::BlobRef {
        namespace: namespace.to_string(),
        local_path: dir.join(id),
        id: id.to_string(),
        scope: scope_for(kind, note_id),
        // These helpers exercise the hashed (default) scheme; a plain-scheme test
        // constructs its own ref with a `cloud_path`.
        cloud_path: None,
        sync,
    }
}

/// Map every `note_photos` INSERT row-change to a [`crate::blob::BlobRef`] under
/// `dir` in `namespace`, scoping each via `scope_for(kind, note_id)` and tagging
/// them all with the retention class `sync`. The changeset-driven analogue of
/// [`note_photos_refs_from_db`]; both build each ref through [`note_photo_ref`].
/// Filtered to INSERTs (matching bae's real `BlobSource`): a blob's bytes never
/// change on update, so only an INSERT carries one, and an INSERT records every
/// column — so `id`/`note_id`/`kind` are always present, where a partial UPDATE
/// could leave `kind` absent. Their absence on an INSERT is a malformed test
/// fixture, surfaced loudly rather than silently skipped.
pub fn note_photos_refs(
    changes: &[crate::changeset::RowChange],
    dir: &std::path::Path,
    namespace: &str,
    scope_for: &dyn Fn(&str, &str) -> crate::blob::BlobScope,
    sync: crate::blob::BlobSync,
) -> Vec<crate::blob::BlobRef> {
    use crate::changeset::ChangeOp;
    changes
        .iter()
        .filter(|c| c.table == "note_photos" && c.op == ChangeOp::Insert)
        .map(|c| {
            let id = c.pk().expect("note_photos row has a primary key");
            let note_id = c.col(1).expect("note_photos row has a note_id at column 1");
            let kind = c.col(2).expect("note_photos row has a kind at column 2");
            note_photo_ref(id, note_id, kind, dir, namespace, scope_for, sync)
        })
        .collect()
}

/// Map every `note_photos` row currently in `conn` to a [`crate::blob::BlobRef`]
/// of class `sync`, the snapshot-bootstrap analogue of [`note_photos_refs`]: the
/// rows the bootstrapped DB already holds, rather than an incoming changeset's.
/// Both build each ref through [`note_photo_ref`], so a photo blob lands at the
/// same path, scope, and class however it was discovered.
pub fn note_photos_refs_from_db(
    conn: &Connection,
    dir: &std::path::Path,
    namespace: &str,
    scope_for: &dyn Fn(&str, &str) -> crate::blob::BlobScope,
    sync: crate::blob::BlobSync,
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
            &id, &note_id, &kind, dir, namespace, scope_for, sync,
        ));
    }
    Ok(refs)
}

/// A blob source that maps `note_photos` rows to `Mirrored` blobs under `dir`
/// (photos are part of having the library, downloaded on pull). `kind = "cover"`
/// uses a per-note derived key; everything else uses the master key.
pub struct PhotoBlobSource {
    pub dir: std::path::PathBuf,
}

impl PhotoBlobSource {
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
        note_photos_refs(
            changes,
            &self.dir,
            "photos",
            &Self::scope_for,
            crate::blob::BlobSync::Mirrored,
        )
    }
}

impl crate::blob::BlobSource for PhotoBlobSource {
    fn blobs_for_change(&self, change: &crate::changeset::RowChange) -> Vec<crate::blob::BlobRef> {
        self.refs(std::slice::from_ref(change))
    }
    fn blobs_in_db(&self, conn: &Connection) -> rusqlite::Result<Vec<crate::blob::BlobRef>> {
        note_photos_refs_from_db(
            conn,
            &self.dir,
            "photos",
            &Self::scope_for,
            crate::blob::BlobSync::Mirrored,
        )
    }
}
