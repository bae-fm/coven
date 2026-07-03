/// Shared test helpers for sync module tests.
///
/// These drive a real [`Database`] over an in-memory connection carrying the
/// synthetic test schema, so tests exercise the engine through the same path
/// production does.
use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension};

use crate::database::{Database, DbError};
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::migration::Migration;
use crate::storage::cloud::{BoxPartSink, CloudHome, CloudHomeError, CloudHomeJoinInfo, PartSink};
use crate::sync::apply::apply_changeset_lww;
use crate::sync::envelope::{self, ChangesetEnvelope};
use crate::sync::membership::{
    sign_membership_entry, MemberRole, MembershipAction, MembershipChain, MembershipEntry,
};
use crate::sync::pull::pull_changes;
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::sync::signed_control::{HeadJson, MinSchemaVersionJson};
use crate::sync::storage::{DeviceHead, MinSchemaVersion, StorageError, SyncStorage};

/// The synthetic, domain-free schema the sync tests run against. Three synced
/// tables exercising the engine's generic mechanics: a *gated root* (`notes`,
/// gated by its `shared` boolean), a child with a foreign key (`note_tags`,
/// which inherits the gate and exercises FK-violation retry), and a child that
/// CAN carry a blob (`note_photos`, also FK-to-`notes`, so it inherits the gate).
/// `note_photos` carries no blob here; blob tests declare one with
/// [`test_synced_tables_with_blob`].
pub fn test_synced_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes").gated_by("shared"),
        SyncedTable::new("note_tags"),
        SyncedTable::new("note_photos"),
    ]
}

/// [`test_synced_tables`] with `note_photos` declared blob-bearing per `decl`, for
/// tests exercising the blob push/pull/backfill paths. The blob id is the
/// `note_photos` primary key; `note_photos.cloud_path` holds a readable key for
/// plain-scheme tests.
pub fn test_synced_tables_with_blob(decl: BlobDecl) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes").gated_by("shared"),
        SyncedTable::new("note_tags"),
        SyncedTable::new("note_photos").carries_blob(decl),
    ]
}

/// [`test_synced_tables`] with TWO blob-bearing children of the gated `notes` root:
/// `note_photos` per `photo_decl` (a release file, user-provided) and `note_covers`
/// per `cover_decl` (a host-provided asset). Both inherit the `notes` gate, so a
/// make_remote of a note carries both — the user-provided file through the durable
/// outbox and the host-provided cover through the inline push — exercising the
/// per-provenance split in one subtree.
pub fn test_synced_tables_with_user_and_host_blobs(
    photo_decl: BlobDecl,
    cover_decl: BlobDecl,
) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes").gated_by("shared"),
        SyncedTable::new("note_tags"),
        SyncedTable::new("note_photos").carries_blob(photo_decl),
        SyncedTable::new("note_covers").carries_blob(cover_decl),
    ]
}

/// Open a test [`Database`] over the synthetic schema with `note_photos` declared
/// blob-bearing per `decl`.
pub fn open_test_db_with_blob(decl: BlobDecl) -> Database {
    open_test_db_schema(test_synced_tables_with_blob(decl), test_migrations())
}

/// Open a read-test [`Database`] whose `note_photos` child carries a blob in
/// `namespace`, so [`crate::blob::cache::read_blob`]'s locality dispatch can resolve a
/// blob in that namespace up to its gated `notes` root. The decl's namespace MUST
/// match the blobs the test reads (the read path resolves the carrying table from the
/// blob's namespace); its provenance/fill don't matter to that resolution (the read
/// reads the row → root → gate, and takes provenance off the `BlobRef`), so this fixes
/// them. Pair with [`plant_blob_row`].
pub fn read_test_db(namespace: &str) -> Database {
    open_test_db_with_blob(BlobDecl::new(
        namespace,
        crate::blob::Provenance::UserProvided,
        crate::blob::CacheFill::CacheLazy,
    ))
}

/// Plant the backing row [`crate::blob::cache::read_blob`] resolves a blob's locality
/// from: a gated `notes` root with `shared = remote` and a `note_photos` child whose
/// id is `blob_id`. `remote = true` ⇒ the blob resolves **Remote** (cache/cloud);
/// `remote = false` ⇒ **Local** (and the read then dispatches on the `BlobRef`'s
/// provenance — external file vs local store). Requires a db whose `note_photos`
/// carries a blob (e.g. [`read_test_db`] / [`open_test_db_with_blob`]).
pub async fn plant_blob_row(db: &Database, blob_id: &str, remote: bool) {
    let note = format!("note-{blob_id}");
    let blob_id = blob_id.to_string();
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES (?1, 'read-test', ?2, '0000000001000-0000-dev1', '2026-01-01')",
            (note.as_str(), remote as i64),
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES (?1, ?2, 'attach', '0000000001000-0000-dev1', '2026-01-01')",
            (blob_id.as_str(), note.as_str()),
        )
        .map_err(DbError::from)?;
        Ok(())
    })
    .await
    .expect("plant blob row");
}

/// Flip the gate on a blob's planted `notes` root — `shared = remote` for the row
/// [`plant_blob_row`] created — so a read re-resolves the blob's locality. Models the
/// gate side of a make_remote (Local → Remote) / make_local (Remote → Local) without
/// running the whole transition.
pub async fn set_blob_remote(db: &Database, blob_id: &str, remote: bool) {
    let note = format!("note-{blob_id}");
    db.call(move |conn| {
        conn.execute(
            "UPDATE notes SET shared = ?1 WHERE id = ?2",
            (remote as i64, note.as_str()),
        )
        .map_err(DbError::from)?;
        Ok(())
    })
    .await
    .expect("flip blob gate");
}

/// Open a test [`Database`] with both `note_photos` (per `photo_decl`) and
/// `note_covers` (per `cover_decl`) declared blob-bearing — the schema for the
/// per-provenance transition tests.
pub fn open_test_db_with_user_and_host_blobs(
    photo_decl: BlobDecl,
    cover_decl: BlobDecl,
) -> Database {
    open_test_db_schema(
        test_synced_tables_with_user_and_host_blobs(photo_decl, cover_decl),
        test_migrations(),
    )
}

/// The synthetic test schema as a single-migration ladder, so a test db opens at
/// `schema_version() == 1`. The host-schema ladder for every `open_test_db*`
/// helper.
pub fn test_migrations() -> Vec<Migration> {
    vec![Migration::run(1, "test-schema", create_synced_schema)]
}

/// Create the synthetic test schema on a connection. Run as the host migration
/// step for [`open_test_db`] (see [`test_migrations`]).
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
            cloud_path TEXT,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        );
        CREATE TABLE note_covers (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            cloud_path TEXT,
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

/// Like [`open_test_db`] but with an explicit synced set and migration ladder, for
/// tests that exercise a different schema (gate tests).
pub fn open_test_db_schema(tables: Vec<SyncedTable>, migrations: Vec<Migration>) -> Database {
    // `:memory:` is unique per connection; the Database owns exactly one.
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        tables,
        "test-device".to_string(),
        &migrations,
    )
    .expect("open test database");
    db
}

fn open_test_db_with(tables: Vec<SyncedTable>) -> Database {
    open_test_db_schema(tables, test_migrations())
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
    seed: impl Fn(&Connection) -> Result<(), DbError> + Send + Sync + 'static,
) -> Database {
    let migrations = vec![Migration::run(1, "test-schema", move |conn| {
        create_synced_schema(conn)?;
        seed(conn)
    })];
    let (db, _stamper) = Database::open_with_hlc(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        hlc,
        &migrations,
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
/// changeset bytes. Capture stays enabled (the batch is reset), so a later
/// `capture_bytes` returns only the writes since this one.
pub async fn capture_bytes(db: &Database, stmts: &[&str]) -> Vec<u8> {
    for s in stmts {
        exec(db, s).await;
    }
    db.take_changeset().await.expect("capture changeset")
}

/// Apply a changeset to the test database with the production LWW path, scoped to
/// `tables`. Goes through `apply_changeset` so capture is disabled around only the
/// apply (the applied rows are not re-recorded), mirroring the cycle's lifecycle.
pub async fn apply_to_db(db: &Database, bytes: &[u8], tables: &[SyncedTable]) {
    let bytes = bytes.to_vec();
    let tables = tables.to_vec();
    let receiver_wall_ms = db.receive_wall_ms();
    db.apply_changeset(move |conn| {
        apply_changeset_lww(conn, &bytes, &tables, receiver_wall_ms).map(|_| ())
    })
    .await
    .expect("apply changeset");
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
    hex::encode(kp.public_key())
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
        provider_account_email: None,
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
    /// Snapshot generations and their pointer, keyed exactly as the cloud lays
    /// them out (`snapshot/{author}/{seq}.db`, `snapshot/{author}/{seq}_meta.json`,
    /// `snapshot/current.json`). Keying each device's generations under its own
    /// `{author}` — not a flat seq — is what lets a test exercise the real
    /// globally-unique keyspace and atomic-publish behavior.
    snapshot_objects: Mutex<HashMap<String, Vec<u8>>>,
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
    membership_list_count: std::sync::atomic::AtomicUsize,
    membership_get_count: std::sync::atomic::AtomicUsize,
    /// `(author_pubkey, seq)` entries the LIST omits but a keyed GET still serves.
    /// Simulates the eventual-consistency window where a freshly-written
    /// membership entry isn't in the LIST yet, but a direct GET (read-after-write
    /// consistent) resolves it — the exact lag issue #84's grant-coordinate fetch
    /// is built for.
    hidden_from_listing: Mutex<std::collections::HashSet<(String, u64)>>,
    fail_blob_puts: std::sync::atomic::AtomicUsize,
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
            snapshot_objects: Mutex::new(HashMap::new()),
            min_schema_version: Mutex::new(None),
            keypair,
            fail_membership_list: std::sync::atomic::AtomicBool::new(false),
            membership_list_count: std::sync::atomic::AtomicUsize::new(0),
            membership_get_count: std::sync::atomic::AtomicUsize::new(0),
            hidden_from_listing: Mutex::new(std::collections::HashSet::new()),
            fail_blob_puts: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Make `list_membership_entries` fail, so a test can assert the cycle fails
    /// closed (refuses to apply changesets) when membership can't be listed,
    /// rather than falling open to "no chain, accept everything" (#88).
    pub fn fail_membership_listing(&self) {
        self.fail_membership_list
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn membership_list_count(&self) -> usize {
        self.membership_list_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn membership_get_count(&self) -> usize {
        self.membership_get_count
            .load(std::sync::atomic::Ordering::SeqCst)
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

    pub fn fail_next_blob_puts(&self, count: usize) {
        self.fail_blob_puts
            .store(count, std::sync::atomic::Ordering::SeqCst);
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

    /// The DB image of the live snapshot generation — the one the pointer names —
    /// or None if no snapshot has been published. Lets a test assert what
    /// `bootstrap_from_snapshot` would adopt without re-deriving the pointer→seq→db
    /// resolution itself.
    pub async fn current_snapshot_db(&self) -> Option<Vec<u8>> {
        let (author, seq) = self.current_snapshot_target().await?;
        self.snapshot_objects
            .lock()
            .unwrap()
            .get(&format!("snapshot/{author}/{seq}.db"))
            .cloned()
    }

    /// The live snapshot generation's author and sequence, or None if no pointer
    /// has been published.
    pub async fn current_snapshot_target(&self) -> Option<(String, u64)> {
        let pointer_bytes = self
            .snapshot_objects
            .lock()
            .unwrap()
            .get("snapshot/current.json")
            .cloned()?;
        let pointer: crate::sync::signed_control::SnapshotPointerJson =
            serde_json::from_slice(&pointer_bytes).expect("parse pointer");
        Some((pointer.author_pubkey, pointer.seq))
    }

    /// The sequence the live snapshot pointer names, or None if no pointer has
    /// been published.
    pub async fn current_snapshot_seq(&self) -> Option<u64> {
        self.current_snapshot_target().await.map(|(_, seq)| seq)
    }

    /// The metadata of the live snapshot generation — the one the pointer names —
    /// or None if no snapshot has been published.
    pub async fn current_snapshot_meta(&self) -> Option<Vec<u8>> {
        let (author, seq) = self.current_snapshot_target().await?;
        self.snapshot_objects
            .lock()
            .unwrap()
            .get(&format!("snapshot/{author}/{seq}_meta.json"))
            .cloned()
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

    /// Publish a head for `device_id` signed by `keypair` — so the head's author is
    /// that device's own key rather than the mock's default identity. Lets a
    /// changeset-GC test give each device a head attributed to its own membership
    /// key (the author changeset reclamation matches each device's ack against).
    pub fn publish_head_as(&self, device_id: &str, seq: u64, keypair: &UserKeypair) {
        let head = HeadJson::signed(device_id, seq, None, None, keypair);
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
                .map_err(|e| StorageError::Parse(format!("parse head {device_id}: {e}")))?;
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
        if self
            .fail_blob_puts
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
        {
            return Err(StorageError::Storage(format!(
                "forced blob upload failure for {namespace}/{id}"
            )));
        }
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

    async fn read_blob_range(
        &self,
        namespace: &str,
        id: &str,
        _scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
        source_size: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StorageError> {
        // The mock stores blobs as plaintext (no at-rest cipher in tests), so the
        // plaintext range is exactly the stored byte range — the same slice the
        // real `BlobRangeReader` would yield over a `Plaintext` home, which its
        // own cloud_storage tests exercise against `InMemoryCloudHome` with real
        // encryption. The bounds checks mirror `BlobRangeReader::read` so a miss
        // here behaves identically whether the cloud is the mock or a real home.
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.checked_add(len).ok_or_else(|| {
            StorageError::Storage(format!("blob range overflow: offset={offset}, len={len}"))
        })?;
        if end > source_size {
            return Err(StorageError::Storage(format!(
                "blob range {offset}..{end} exceeds blob size {source_size}"
            )));
        }
        let key = blob_key(namespace, id, cloud_path);
        let objects = self.objects.lock().unwrap();
        let stored = objects.get(&key).ok_or(StorageError::NotFound(key))?;
        Ok(stored[offset as usize..end as usize].to_vec())
    }

    async fn put_snapshot(
        &self,
        author: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.snapshot_objects
            .lock()
            .unwrap()
            .insert(format!("snapshot/{author}/{seq}.db"), data);
        Ok(())
    }

    async fn get_snapshot(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        let key = format!("snapshot/{author}/{seq}.db");
        self.snapshot_objects
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or(StorageError::NotFound(key))
    }

    async fn delete_changeset(&self, device_id: &str, seq: u64) -> Result<(), StorageError> {
        let key = format!("changes/{device_id}/{seq}");
        self.objects.lock().unwrap().remove(&key);
        Ok(())
    }

    async fn list_changesets(&self, device_id: &str) -> Result<Vec<u64>, StorageError> {
        let prefix = format!("changes/{device_id}/");
        let objects = self.objects.lock().unwrap();
        let mut seqs = Vec::new();
        for key in objects.keys() {
            let Some(seq_str) = key.strip_prefix(&prefix) else {
                continue;
            };
            match seq_str.parse::<u64>() {
                Ok(seq) => seqs.push(seq),
                Err(e) => tracing::warn!("skipping changeset key {key} with non-numeric seq: {e}"),
            }
        }
        seqs.sort_unstable();
        Ok(seqs)
    }

    async fn put_ack(&self, device_id: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let key = format!("acks/{device_id}.json");
        self.objects.lock().unwrap().insert(key, data);
        Ok(())
    }

    async fn get_ack(&self, device_id: &str) -> Result<Vec<u8>, StorageError> {
        let key = format!("acks/{device_id}.json");
        let objects = self.objects.lock().unwrap();
        objects
            .get(&key)
            .cloned()
            .ok_or(StorageError::NotFound(key))
    }

    async fn get_min_schema_version(&self) -> Result<Option<MinSchemaVersion>, StorageError> {
        let stored = self.min_schema_version.lock().unwrap();
        let Some(bytes) = stored.as_ref() else {
            return Ok(None);
        };
        let parsed: MinSchemaVersionJson = serde_json::from_slice(bytes)
            .map_err(|e| StorageError::Parse(format!("parse min_schema_version: {e}")))?;
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
        self.membership_get_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let key = format!("membership/{author_pubkey}/{seq}");
        let objects = self.objects.lock().unwrap();
        objects
            .get(&key)
            .cloned()
            .ok_or(StorageError::NotFound(key))
    }

    async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError> {
        self.membership_list_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self
            .fail_membership_list
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StorageError::Storage(
                "injected membership-list failure".into(),
            ));
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

    async fn put_snapshot_meta(
        &self,
        author: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.snapshot_objects
            .lock()
            .unwrap()
            .insert(format!("snapshot/{author}/{seq}_meta.json"), data);
        Ok(())
    }

    async fn get_snapshot_meta(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        let key = format!("snapshot/{author}/{seq}_meta.json");
        self.snapshot_objects
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or(StorageError::NotFound(key))
    }

    async fn put_snapshot_pointer(&self, data: Vec<u8>) -> Result<(), StorageError> {
        self.snapshot_objects
            .lock()
            .unwrap()
            .insert("snapshot/current.json".to_string(), data);
        Ok(())
    }

    async fn get_snapshot_pointer(&self) -> Result<Vec<u8>, StorageError> {
        self.snapshot_objects
            .lock()
            .unwrap()
            .get("snapshot/current.json")
            .cloned()
            .ok_or(StorageError::NotFound("snapshot/current.json".into()))
    }

    async fn list_own_snapshot_generations(&self, author: &str) -> Result<Vec<u64>, StorageError> {
        // List only this author's own keyspace — the meta objects under
        // `snapshot/{author}/` — exactly as `CloudSyncStorage` does. Ownership is
        // structural: a peer's generations live under a different prefix.
        let prefix = format!("snapshot/{author}/");
        let objects = self.snapshot_objects.lock().unwrap();
        let mut seqs = Vec::new();
        for key in objects.keys() {
            let Some(rest) = key.strip_prefix(&prefix) else {
                // A different author's generation or the shared pointer — not under
                // this author's prefix, legitimately not ours; skip silently.
                continue;
            };
            // Mirror `CloudSyncStorage`: the `{seq}.db` sibling is expected and listed
            // via its meta (skip silently); a meta with a non-numeric seq, or any other
            // object under this author's prefix, is surfaced rather than dropped.
            match rest.strip_suffix("_meta.json") {
                Some(seq_str) => match seq_str.parse::<u64>() {
                    Ok(seq) => seqs.push(seq),
                    Err(e) => {
                        tracing::warn!(
                            "snapshot listing: meta key {key} has a non-numeric seq: {e}"
                        )
                    }
                },
                None if rest.ends_with(".db") => {}
                None => tracing::warn!(
                    "snapshot listing: unexpected object {key} under a snapshot author prefix"
                ),
            }
        }
        seqs.sort_unstable();
        Ok(seqs)
    }

    async fn delete_snapshot_generation(&self, author: &str, seq: u64) -> Result<(), StorageError> {
        // Remove the db first, the meta last: the meta is what `list` keys a
        // generation by, so a crash between the two leaves it still listed (and
        // re-deletable), never a meta-less db.
        let mut objects = self.snapshot_objects.lock().unwrap();
        objects.remove(&format!("snapshot/{author}/{seq}.db"));
        objects.remove(&format!("snapshot/{author}/{seq}_meta.json"));
        Ok(())
    }
}

/// A [`PartSink`] for [`MockSyncStorage`]: accumulate the streamed parts and store
/// the assembled object on `finish`, so a multipart upload round-trips like a
/// single `put_object`.
struct MockPartSink<'a> {
    storage: &'a MockSyncStorage,
    key: String,
    buf: Vec<u8>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PartSink for MockPartSink<'_> {
    fn part_size(&self) -> usize {
        4 * 1024 * 1024
    }
    async fn send_part(
        &mut self,
        part: bytes::Bytes,
        _offset: u64,
        _is_last: bool,
    ) -> Result<(), CloudHomeError> {
        self.buf.extend_from_slice(&part);
        Ok(())
    }
    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        self.storage
            .objects
            .lock()
            .unwrap()
            .insert(self.key, self.buf);
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CloudHome for MockSyncStorage {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.objects.lock().unwrap().insert(key.to_string(), data);
        Ok(())
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        _total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        Ok(Box::new(MockPartSink {
            storage: self,
            key: key.to_string(),
            buf: Vec::new(),
        }))
    }

    fn multipart_threshold(&self) -> u64 {
        8 * 1024 * 1024
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

    async fn grant_access(
        &self,
        _grant: crate::storage::cloud::CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        Ok(CloudHomeJoinInfo::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key: "test-access-key".to_string(),
            secret_key: "test-secret-key".to_string(),
            key_prefix: None,
        })
    }

    async fn revoke_access(
        &self,
        _revoke: crate::storage::cloud::CloudAccessRevoke,
    ) -> Result<(), CloudHomeError> {
        Ok(())
    }
}

/// Pull into `db` the way production does: capture stays enabled, and
/// `pull_changes` disables it around only each apply (so applied rows aren't
/// re-recorded as a local change) while a host write during the pull would still
/// be captured. Returns the updated cursors and the pull result.
pub async fn pull_into(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    cursors: &HashMap<String, u64>,
    library_dir: &crate::library_dir::LibraryDir,
) -> (HashMap<String, u64>, crate::sync::pull::PullResult) {
    pull_changes(
        db,
        db.synced_tables(),
        storage,
        device_id,
        cursors,
        library_dir,
    )
    .await
    .expect("pull")
}

/// Like [`pull_into`] but returns the `pull_changes` result without unwrapping, so
/// a test can assert a [`crate::sync::pull::PullError`].
#[allow(clippy::type_complexity)]
pub async fn pull_into_result(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    cursors: &HashMap<String, u64>,
    library_dir: &crate::library_dir::LibraryDir,
) -> Result<(HashMap<String, u64>, crate::sync::pull::PullResult), crate::sync::pull::PullError> {
    pull_changes(
        db,
        db.synced_tables(),
        storage,
        device_id,
        cursors,
        library_dir,
    )
    .await
}
