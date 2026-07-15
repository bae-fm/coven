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
use crate::encryption::MasterKeyring;
use crate::keys::{KeyError, MasterKeyCustody, UserKeypair};
use crate::migration::Migration;
use crate::storage::cloud::{BoxPartSink, CloudHome, CloudHomeError, CloudHomeJoinInfo, PartSink};
use crate::store_dir::StoreDir;
use crate::sync::apply::resolve_and_apply_changeset;
use crate::sync::membership::{MembershipChain, MembershipCoord, MembershipEntry, OwnerGrantId};
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::sync::storage::{
    ProtocolObjectListing, ProtocolObjectLocator, StorageError, SyncStorage,
};

/// In-memory [`MasterKeyCustody`] for tests, with a switch to force `persist`
/// to fail. The switch models a device whose keyring is momentarily
/// unwritable, so a test can drive a key adoption into its failure path and then
/// clear the switch to prove the retry converges. Stores the serialized form
/// (like the real `Keyring` preset), so `stored_key` reflects exactly what a
/// caller wrote.
#[derive(Default)]
pub struct TestCustody {
    value: Mutex<Option<String>>,
    fail: std::sync::atomic::AtomicBool,
}

impl TestCustody {
    pub fn set_initial_key(&self, key: [u8; 32]) {
        *self.value.lock().unwrap() = Some(
            MasterKeyring::from(crate::encryption::EncryptionService::from_key(key))
                .to_serialized(),
        );
    }

    pub fn stored_key(&self) -> Option<String> {
        self.value.lock().unwrap().clone()
    }

    /// Make the next and every subsequent `persist` fail until cleared.
    pub fn fail_writes(&self) {
        self.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Let `persist` succeed again.
    pub fn allow_writes(&self) {
        self.fail.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

impl MasterKeyCustody for TestCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        self.value
            .lock()
            .unwrap()
            .as_deref()
            .map(MasterKeyring::from_serialized)
            .transpose()
            .map_err(|e| KeyError::Crypto(e.to_string()))
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KeyError::Persistence(
                "forced keyring write failure".to_string(),
            ));
        }
        *self.value.lock().unwrap() = Some(keyring.to_serialized());
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        *self.value.lock().unwrap() = None;
        Ok(())
    }
}

/// The synthetic, domain-free schema the sync tests run against. Three synced
/// tables exercising the engine's generic mechanics: a *gated root* (`notes`,
/// gated by its `shared` boolean), a child with a foreign key (`note_tags`,
/// which inherits the gate and exercises FK-violation retry), and a child that
/// CAN carry a blob (`note_photos`, also FK-to-`notes`, so it inherits the gate).
/// `note_photos` carries no blob here; blob tests declare one with
/// [`test_synced_tables_with_blob`].
pub fn test_synced_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).gated_by("shared"),
        SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
        SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey),
    ]
}

/// [`test_synced_tables`] with `note_photos` declared blob-bearing per `decl`, for
/// tests exercising the blob push/pull/backfill paths. The blob id defaults to the
/// `note_photos` primary key; `note_photos.cloud_path` holds a readable key for
/// plain-scheme tests, and `note_photos.blob_id` is there for a decl that names a
/// blob id apart from the PK — the shape a row repointed at a new blob needs, since
/// the row keeps its primary key.
pub fn test_synced_tables_with_blob(decl: BlobDecl) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).gated_by("shared"),
        SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
        SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey)
            .carries_blob(decl),
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
        SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).gated_by("shared"),
        SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
        SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey)
            .carries_blob(photo_decl),
        SyncedTable::new("note_covers", crate::sync::session::RowIdentity::SharedKey)
            .carries_blob(cover_decl),
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

/// Like [`read_test_db`] but with a chosen `max_concurrent_downloads`, so a pin test
/// can drive the download loop concurrently. Uploads stay serial (not exercised here).
pub fn read_test_db_with_download_limit(namespace: &str, downloads: usize) -> Database {
    let tables = test_synced_tables_with_blob(BlobDecl::new(
        namespace,
        crate::blob::Provenance::UserProvided,
        crate::blob::CacheFill::CacheLazy,
    ));
    let limits = crate::blob::TransferLimits {
        uploads: std::num::NonZeroUsize::MIN,
        downloads: std::num::NonZeroUsize::new(downloads).expect("downloads limit is nonzero"),
    };
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        tables,
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        limits,
        crate::WritePolicy::MergeConcurrent,
        "test-device".to_string(),
        &test_migrations(),
    )
    .expect("open test database");
    db
}

/// Plant the backing row [`crate::blob::cache::read_blob`] resolves a blob's locality
/// from: a gated `notes` root with `shared = remote` and a `note_photos` child whose
/// id is `blob_id`, carrying `bytes`'s length and content hash so a download of those
/// exact bytes verifies. `remote = true` ⇒ the blob resolves **Remote** (cache/cloud);
/// `remote = false` ⇒ **Local** (and the read then dispatches on the `BlobRef`'s
/// provenance — external file vs local store). Requires a db whose `note_photos`
/// carries a blob (e.g. [`read_test_db`] / [`open_test_db_with_blob`]).
pub async fn plant_blob_row(db: &Database, blob_id: &str, remote: bool, bytes: &[u8]) {
    plant_blob_row_with_size_hash(
        db,
        blob_id,
        remote,
        bytes.len() as u64,
        Some(&crate::blob::content_hash(bytes)),
    )
    .await;
}

/// Plant a blob-bearing row with a caller-chosen `size` and `hash`, for the tests
/// that deliberately declare a size or hash that does not match the bytes served
/// (the size-mismatch and hash-mismatch refusals) or that never download at all
/// (a missing-blob row, `hash = None`).
pub async fn plant_blob_row_with_size_hash(
    db: &Database,
    blob_id: &str,
    remote: bool,
    size: u64,
    hash: Option<&str>,
) {
    let note = format!("note-{blob_id}");
    let blob_id = blob_id.to_string();
    let hash = hash.map(str::to_string);
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES (?1, 'read-test', ?2, '0000000001000-0000-dev1', '2026-01-01')",
            (note.as_str(), remote as i64),
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES (?1, ?2, 'attach', ?3, ?4, '0000000001000-0000-dev1', '2026-01-01')",
            rusqlite::params![blob_id.as_str(), note.as_str(), size as i64, hash],
        )
        .map_err(DbError::from)?;
        Ok(())
    })
    .await
    .expect("plant blob row");
}

/// Record `uploader` as the member that uploaded blob `(namespace, id)` — the way
/// the pull records the signed changeset's author, and the way a snapshot carries
/// the source's uploader index forward. For tests that seed a Remote blob
/// directly (no pull, no make_remote) and then read or backfill it, so the read
/// resolves the blob's prefix from the recorded uploader rather than a listing
/// scan (which no longer exists).
pub async fn record_blob_uploader(db: &Database, namespace: &str, id: &str, uploader: &str) {
    db.record_blob_uploader(namespace, id, uploader)
        .await
        .expect("record blob uploader");
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
        ) STRICT;
        CREATE TABLE note_tags (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;
        CREATE TABLE note_photos (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            hash TEXT,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            cloud_path TEXT,
            blob_id TEXT,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;
        CREATE TABLE note_covers (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            hash TEXT,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            cloud_path TEXT,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;",
    )
    .map_err(DbError::from)
}

pub fn test_sync_routing_hash() -> crate::sync::store_commit::ObjectHash {
    let conn = Connection::open_in_memory().expect("open schema-contract database");
    create_synced_schema(&conn).expect("create schema-contract tables");
    crate::sync::routing_contract::SyncRoutingContract::from_connection(
        &conn,
        &test_synced_tables(),
    )
    .expect("resolve test sync-schema contract")
    .hash()
}

/// Open a [`Database`] over a fresh in-memory connection with the synthetic test
/// schema and the [`test_synced_tables`] synced set. The returned stamper is
/// dropped (tests stamp `_updated_at` literally in their SQL).
pub fn open_test_db() -> Database {
    open_test_db_with(test_synced_tables())
}

pub fn open_serial_test_db() -> Database {
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::Serial,
        "test-device".to_string(),
        &test_migrations(),
    )
    .expect("open Serial test database");
    db
}

/// Like [`open_test_db`] but with an explicit synced set and migration ladder, for
/// tests that exercise a different schema (gate tests).
pub fn open_test_db_schema(tables: Vec<SyncedTable>, migrations: Vec<Migration>) -> Database {
    // `:memory:` is unique per connection; the Database owns exactly one.
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        tables,
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::MergeConcurrent,
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
/// step run after the host schema is created to plant host rows before
/// `Database::open` reads its floor.
///
/// Used only by the register-clock tests (`hlc_register_tests`).
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
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::MergeConcurrent,
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

pub async fn host_exec(db: &Database, sql: &str) {
    let sql = sql.to_string();
    let tables = db.synced_tables().to_vec();
    let write_id = db.new_write_id();
    let write_policy = db.write_policy();
    db.call(move |conn| {
        Database::run_internal_store_write_transaction_on(
            conn,
            &tables,
            write_policy,
            write_id,
            |tx| tx.execute_batch(&sql).map(|_| ()).map_err(DbError::from),
        )
    })
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

/// Run `stmts` as one journaled host transaction (the same path a host write
/// takes), then drain the pending-changeset journal and return the combined
/// changeset bytes. With no `stmts`, this just drains whatever the journal already
/// holds — the "clear the captured changes" idiom. Draining clears the journal, so
/// a later `capture_bytes` returns only the writes since this one.
pub async fn capture_bytes(db: &Database, stmts: &[&str]) -> Vec<u8> {
    let statements: Vec<String> = stmts
        .iter()
        .map(|statement| statement.to_string())
        .collect();
    let tables: Vec<String> = db
        .synced_tables()
        .iter()
        .map(|table| table.name().to_string())
        .collect();
    db.call(move |conn| {
        let mut session = rusqlite::session::Session::new(conn).map_err(DbError::from)?;
        for table in tables {
            session
                .attach(Some(table.as_str()))
                .map_err(DbError::from)?;
        }
        for statement in statements {
            conn.execute_batch(&statement).map_err(DbError::from)?;
        }
        let mut bytes = Vec::new();
        session.changeset_strm(&mut bytes).map_err(DbError::from)?;
        Ok(bytes)
    })
    .await
    .unwrap_or_else(|error| panic!("capture failed: {error}"))
}

/// Apply a changeset to the test database with the production conflict-resolving
/// apply path, scoped to `tables`. A plain `call`, like the cycle's apply: an apply
/// is never journaled, so the applied rows are not recorded as this device's own
/// outgoing changes.
pub async fn apply_to_db(db: &Database, bytes: &[u8], tables: &[SyncedTable]) {
    let bytes = bytes.to_vec();
    let tables = tables.to_vec();
    let receiver_wall_ms = db.receive_wall_ms();
    db.call(move |conn| {
        resolve_and_apply_changeset(conn, &bytes, &tables, receiver_wall_ms).map(|_| ())
    })
    .await
    .expect("apply changeset");
}

/// A temp dir plus a [`StoreDir`] rooted at it. The returned `TempDir` must be
/// held for the directory to outlive the test.
pub fn temp_store_dir() -> (tempfile::TempDir, StoreDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new(tmp.path());
    (tmp, dir)
}

/// Hex-encoded ed25519 public key, as membership entries and the wrapped-key
/// store identify a member.
pub fn pubkey_hex(kp: &UserKeypair) -> String {
    hex::encode(kp.public_key())
}

/// A membership chain rooted at the exact founder entry carried by Store protocol root.
pub fn bootstrap_chain(founder: MembershipEntry) -> MembershipChain {
    let mut chain = MembershipChain::new();
    chain.add_entry(founder).unwrap();
    chain
}

pub async fn append_membership_entry(
    storage: &MockSyncStorage,
    chain: &mut MembershipChain,
    author_pubkey: &str,
    seq: u64,
    entry: MembershipEntry,
) {
    let coord = entry.coord();
    assert_eq!(coord.author_pubkey, author_pubkey);
    assert_eq!(coord.seq, seq);
    chain
        .add_entry_at(coord.clone(), entry.clone())
        .expect("valid membership test chain");
    crate::sync::store_objects::append_membership_entry_object(storage, &coord, &entry)
        .await
        .expect("upload membership entry");
}

pub async fn append_membership_entry_bytes(
    storage: &dyn SyncStorage,
    author_pubkey: &str,
    seq: u64,
    data: Vec<u8>,
) -> Result<(), StorageError> {
    let entry: MembershipEntry =
        serde_json::from_slice(&data).map_err(|error| StorageError::Parse(error.to_string()))?;
    if entry.author_pubkey != author_pubkey || entry.seq != seq {
        return Err(StorageError::Parse(format!(
            "membership bytes declare {}/{}, expected {author_pubkey}/{seq}",
            entry.author_pubkey, entry.seq
        )));
    }
    let hash = crate::sync::store_commit::ObjectHash::digest(&data);
    let prefix = crate::sync::store_commit::membership_entry_semantic_prefix(
        author_pubkey,
        &entry.author_owner_grant,
        seq,
        hash,
    );
    storage
        .append_protocol_object(&prefix, ".json", data)
        .await?;
    Ok(())
}

pub async fn publish_membership_chain_head(
    storage: &MockSyncStorage,
    chain: &MembershipChain,
    signer: &UserKeypair,
) {
    crate::sync::membership_ops::publish_membership_head(storage, chain, signer)
        .await
        .expect("publish membership head");
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
            None,
            id,
            Some(path),
        )
        .expect("plain blob_key with a cloud_path is always Ok"),
        None => format!("{namespace}/{id}"),
    }
}

struct MembershipHeadReadPause {
    author_pubkey: String,
    snapshot_held: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

/// In-memory mock of SyncStorage for tests.
///
/// Stores blobs and membership objects in memory and immutable Store protocol
/// copies under their exact semantic paths.
pub struct MockSyncStorage {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    protocol_objects: Mutex<Vec<(crate::storage::cloud::AppendedObject, Vec<u8>)>>,
    next_protocol_object: std::sync::atomic::AtomicU64,
    store_commits: Mutex<HashMap<(String, u64), crate::sync::store_commit::StoreBatchCommit>>,
    store_protocol_root: crate::sync::store_commit::StoreProtocolRoot,
    /// One membership-head read to pause after snapshotting its bytes. The two
    /// notifications tell the test that the snapshot is held and release it.
    membership_head_read_pause: Mutex<Option<MembershipHeadReadPause>>,
    /// The device identity this mock uses for signed Store objects and membership.
    keypair: UserKeypair,
    /// When set, `list_membership_entries` returns an error — to exercise the
    /// fail-closed path where membership can't even be listed (#88), instead of
    /// silently disabling authorization for the cycle.
    fail_membership_list: std::sync::atomic::AtomicBool,
    /// Paired with `membership_list_count` via [`arm_put_failure`] /
    /// [`armed_put_failure_hits`] to fail exactly one numbered
    /// `list_membership_entries` call, so a test can make the cycle-start listing
    /// succeed and a later mid-cycle re-list fail (or vice versa) instead of
    /// failing every call alike.
    fail_membership_list_on: std::sync::atomic::AtomicUsize,
    membership_list_count: std::sync::atomic::AtomicUsize,
    membership_entry_read_count: std::sync::atomic::AtomicUsize,
    /// `(author_pubkey, seq)` entries the LIST omits but a keyed GET still serves.
    /// Simulates the eventual-consistency window where a freshly-written
    /// membership entry isn't in the LIST yet, but a direct GET (read-after-write
    /// consistent) resolves it — the exact lag issue #84's grant-coordinate fetch
    /// is built for.
    hidden_from_listing: Mutex<std::collections::HashSet<(String, OwnerGrantId, u64)>>,
    fail_blob_puts: std::sync::atomic::AtomicUsize,
    blob_put_count: std::sync::atomic::AtomicUsize,
    blob_put_from_file_count: std::sync::atomic::AtomicUsize,
    blob_read_to_file_count: std::sync::atomic::AtomicUsize,
    fail_blob_reads: std::sync::atomic::AtomicUsize,
    fail_blob_put_on: std::sync::atomic::AtomicUsize,
    fail_changeset_puts: std::sync::atomic::AtomicUsize,
    wrapped_key_put_count: std::sync::atomic::AtomicUsize,
    fail_wrapped_key_put_on: std::sync::atomic::AtomicUsize,
    membership_head_append_count: std::sync::atomic::AtomicUsize,
    fail_membership_head_append_on: std::sync::atomic::AtomicUsize,
    lose_membership_head_append_on: std::sync::atomic::AtomicUsize,
    membership_entry_append_count: std::sync::atomic::AtomicUsize,
    fail_membership_entry_append_on: std::sync::atomic::AtomicUsize,
    /// When armed, every `read_blob_to_file` gathers on this barrier before serving,
    /// so a test can prove the pin loop runs fetches concurrently and bounds them:
    /// with a barrier of size N, N fetches must arrive together to release it, and
    /// `read_to_file_max_inflight` records the observed peak.
    read_to_file_barrier: Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>,
    read_to_file_inflight: std::sync::atomic::AtomicUsize,
    read_to_file_max_inflight: std::sync::atomic::AtomicUsize,
}

/// Arm the `call_number`-th put (1-based) tracked by the `(count, fail_on)` atomic
/// pair to fail once: reset the running count and record which call trips. `label`
/// names the object kind in the 1-based assertion. Paired with
/// [`armed_put_failure_hits`], which the matching put method calls to test the arm.
fn arm_put_failure(
    count: &std::sync::atomic::AtomicUsize,
    fail_on: &std::sync::atomic::AtomicUsize,
    call_number: usize,
    label: &str,
) {
    assert!(call_number > 0, "{label} put call numbers are 1-based");
    count.store(0, std::sync::atomic::Ordering::SeqCst);
    fail_on.store(call_number, std::sync::atomic::Ordering::SeqCst);
}

/// Count this put and report whether it is the call [`arm_put_failure`] armed,
/// clearing the arm on a hit so only that one call fails.
fn armed_put_failure_hits(
    count: &std::sync::atomic::AtomicUsize,
    fail_on: &std::sync::atomic::AtomicUsize,
) -> bool {
    let call = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    if fail_on.load(std::sync::atomic::Ordering::SeqCst) == call {
        fail_on.store(0, std::sync::atomic::Ordering::SeqCst);
        true
    } else {
        false
    }
}

impl MockSyncStorage {
    pub fn new() -> Self {
        Self::with_keypair(UserKeypair::generate())
    }

    pub fn for_store(store_id: &str) -> Self {
        Self::with_store_and_keypair(store_id, UserKeypair::generate())
    }

    pub fn with_keypair(keypair: UserKeypair) -> Self {
        Self::with_store_and_keypair("test-store", keypair)
    }

    pub fn with_store_and_keypair(store_id: &str, keypair: UserKeypair) -> Self {
        let founder = crate::sync::membership::founder_entry(
            store_id,
            &keypair,
            "0000000000001-0000-test-founder",
        );
        let store_protocol_root = crate::sync::store_commit::StoreProtocolRoot::signed(
            store_id.to_string(),
            founder,
            1,
            test_sync_routing_hash(),
            crate::WritePolicy::MergeConcurrent,
            &keypair,
        )
        .expect("create test store protocol root");
        let store_root_hash = store_protocol_root.object_hash();
        let copy_id: crate::storage::cloud::CopyId = format!("{:064x}", 0_u64)
            .parse()
            .expect("canonical test copy id");
        let store_protocol_root_key =
            crate::sync::store_commit::store_protocol_root_copy_key(store_root_hash, copy_id);
        let store_protocol_root_object = crate::storage::cloud::AppendedObject::from_provider(
            store_protocol_root_key,
            "mock-protocol-0".to_string(),
        );
        MockSyncStorage {
            objects: Mutex::new(HashMap::new()),
            protocol_objects: Mutex::new(vec![(
                store_protocol_root_object,
                store_protocol_root.to_bytes(),
            )]),
            next_protocol_object: std::sync::atomic::AtomicU64::new(1),
            store_commits: Mutex::new(HashMap::new()),
            store_protocol_root,
            membership_head_read_pause: Mutex::new(None),
            keypair,
            fail_membership_list: std::sync::atomic::AtomicBool::new(false),
            fail_membership_list_on: std::sync::atomic::AtomicUsize::new(0),
            membership_list_count: std::sync::atomic::AtomicUsize::new(0),
            membership_entry_read_count: std::sync::atomic::AtomicUsize::new(0),
            hidden_from_listing: Mutex::new(std::collections::HashSet::new()),
            fail_blob_puts: std::sync::atomic::AtomicUsize::new(0),
            blob_put_count: std::sync::atomic::AtomicUsize::new(0),
            blob_put_from_file_count: std::sync::atomic::AtomicUsize::new(0),
            blob_read_to_file_count: std::sync::atomic::AtomicUsize::new(0),
            fail_blob_reads: std::sync::atomic::AtomicUsize::new(0),
            fail_blob_put_on: std::sync::atomic::AtomicUsize::new(0),
            fail_changeset_puts: std::sync::atomic::AtomicUsize::new(0),
            wrapped_key_put_count: std::sync::atomic::AtomicUsize::new(0),
            fail_wrapped_key_put_on: std::sync::atomic::AtomicUsize::new(0),
            membership_head_append_count: std::sync::atomic::AtomicUsize::new(0),
            fail_membership_head_append_on: std::sync::atomic::AtomicUsize::new(0),
            lose_membership_head_append_on: std::sync::atomic::AtomicUsize::new(0),
            membership_entry_append_count: std::sync::atomic::AtomicUsize::new(0),
            fail_membership_entry_append_on: std::sync::atomic::AtomicUsize::new(0),
            read_to_file_barrier: Mutex::new(None),
            read_to_file_inflight: std::sync::atomic::AtomicUsize::new(0),
            read_to_file_max_inflight: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn store_root_hash(&self) -> crate::sync::store_commit::ObjectHash {
        self.store_protocol_root.object_hash()
    }

    pub fn store_protocol_root(&self) -> crate::sync::store_commit::StoreProtocolRoot {
        self.store_protocol_root.clone()
    }

    pub fn protocol_founder_pubkey(&self) -> String {
        crate::keys::public_key_hex(&self.keypair)
    }

    pub fn protocol_founder_keypair(&self) -> UserKeypair {
        self.keypair.clone()
    }

    pub fn protocol_founder_coord(&self) -> crate::sync::membership::MembershipCoord {
        self.store_protocol_root.founder.coord()
    }

    pub async fn publish_protocol_founder_membership(
        &self,
    ) -> crate::sync::membership::MembershipChain {
        let founder = self.store_protocol_root.founder.clone();
        let coord = founder.coord();
        let chain = crate::sync::membership::MembershipChain::from_entries(vec![founder.clone()])
            .expect("validate mock protocol founder");
        crate::sync::store_objects::append_membership_entry_object(self, &coord, &founder)
            .await
            .expect("publish mock protocol founder membership");
        crate::sync::membership_ops::publish_membership_head(self, &chain, &self.keypair)
            .await
            .expect("publish mock protocol founder head");
        chain
    }

    pub fn store_commit_position(
        &self,
        device_id: &str,
        seq: u64,
    ) -> crate::sync::store_commit::CommitPosition {
        self.store_commits
            .lock()
            .unwrap()
            .get(&(device_id.to_string(), seq))
            .unwrap_or_else(|| panic!("missing Store commit {device_id}/{seq}"))
            .position()
    }

    pub async fn publish_store_snapshot(
        &self,
        db_image: Vec<u8>,
        coverage: std::collections::BTreeMap<String, crate::sync::store_commit::CommitPosition>,
        schema_version: u32,
        db: &Database,
    ) -> crate::sync::store_commit::SnapshotMeta {
        let membership = self.publish_protocol_founder_membership().await;
        crate::sync::store_snapshot::push_store_snapshot(
            self,
            self.store_root_hash(),
            crate::sync::snapshot::CreatedSnapshot {
                db_image,
                host_blobs: Vec::new(),
                publish_blobs: Vec::new(),
            },
            crate::CommitFrontier::MergeConcurrent(coverage),
            schema_version,
            &self.keypair,
            "2026-02-10T00:00:00Z".to_string(),
            Some(&membership),
            db,
        )
        .await
        .expect("publish mock Store snapshot")
    }

    fn append_test_protocol(
        &self,
        semantic_prefix: &str,
        extension: &str,
        bytes: Vec<u8>,
    ) -> ProtocolObjectLocator {
        let id = self
            .next_protocol_object
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let copy_id: crate::storage::cloud::CopyId = format!("{id:064x}")
            .parse()
            .expect("canonical test copy id");
        let logical_key = format!("{semantic_prefix}/copies/{copy_id}{extension}");
        let physical = crate::storage::cloud::AppendedObject::from_provider(
            logical_key.clone(),
            format!("mock-protocol-{id}"),
        );
        self.protocol_objects
            .lock()
            .unwrap()
            .push((physical.clone(), bytes));
        ProtocolObjectLocator::new(logical_key, physical)
    }

    /// Arm `read_blob_to_file` to gather `n` calls on a barrier before each serves,
    /// so a pin test can prove the download loop runs `n` fetches at once. Use with a
    /// blob count that is a multiple of `n` so every wave fills the barrier.
    pub fn arm_read_to_file_concurrency_probe(&self, n: usize) {
        *self.read_to_file_barrier.lock().unwrap() =
            Some(std::sync::Arc::new(tokio::sync::Barrier::new(n)));
    }

    /// The peak number of `read_blob_to_file` calls observed in flight at once while
    /// the concurrency probe was armed.
    pub fn read_to_file_max_inflight(&self) -> usize {
        self.read_to_file_max_inflight
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Make `list_membership_entries` fail, so a test can assert the cycle fails
    /// closed (refuses to apply changesets) when membership can't be listed,
    /// rather than falling open to "no chain, accept everything" (#88).
    pub fn fail_membership_listing(&self) {
        self.fail_membership_list
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Fail exactly the `call_number`-th (1-based) `list_membership_entries` call,
    /// leaving every other call to succeed normally. Lets a test isolate a
    /// mid-cycle re-list (the second call within one pull) from the cycle-start
    /// listing (the first), to exercise the reload's own fallback to the
    /// cycle-start chain without also failing cycle start itself.
    pub fn fail_membership_list_on_call(&self, call_number: usize) {
        arm_put_failure(
            &self.membership_list_count,
            &self.fail_membership_list_on,
            call_number,
            "membership-list",
        );
    }

    pub fn membership_list_count(&self) -> usize {
        self.membership_list_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn membership_entry_read_count(&self) -> usize {
        self.membership_entry_read_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn discover_membership_entries(&self) -> Vec<MembershipCoord> {
        crate::sync::membership_ops::list_membership_entries(self)
            .await
            .expect("discover membership entries")
    }

    fn membership_coords(&self, author_pubkey: &str, seq: u64) -> Vec<MembershipCoord> {
        let mut coords = self
            .protocol_objects
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(object, _)| {
                crate::sync::store_commit::parse_membership_entry_copy_key(object.logical_key())
                    .ok()
            })
            .filter(|slot| slot.author == author_pubkey && slot.sequence == seq)
            .map(|slot| MembershipCoord {
                author_pubkey: slot.author,
                author_owner_grant: slot.author_owner_grant,
                seq: slot.sequence,
                entry_hash: slot.semantic_hash,
            })
            .collect::<Vec<_>>();
        coords.sort();
        coords.dedup();
        coords
    }

    async fn unique_membership_coord(
        &self,
        author_pubkey: &str,
        seq: u64,
    ) -> Result<MembershipCoord, StorageError> {
        let matches = self.membership_coords(author_pubkey, seq);
        match matches.as_slice() {
            [coord] => Ok(coord.clone()),
            [] => Err(StorageError::NotFound(format!(
                "membership entry {author_pubkey}/{seq}"
            ))),
            _ => Err(StorageError::Parse(format!(
                "membership entry {author_pubkey}/{seq} spans multiple Owner grant streams"
            ))),
        }
    }

    pub async fn read_membership_entry_bytes(
        &self,
        author_pubkey: &str,
        seq: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let coord = self.unique_membership_coord(author_pubkey, seq).await?;
        crate::sync::store_objects::load_membership_entry_slot(
            self,
            author_pubkey,
            &coord.author_owner_grant,
            seq,
        )
        .await
        .map_err(|error| StorageError::Parse(error.to_string()))?
        .map(|entry| entry.bytes)
        .ok_or_else(|| StorageError::NotFound(format!("membership entry {author_pubkey}/{seq}")))
    }

    pub async fn append_membership_entry_bytes(
        &self,
        author_pubkey: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let entry: MembershipEntry = serde_json::from_slice(&data)
            .map_err(|error| StorageError::Parse(error.to_string()))?;
        if entry.author_pubkey != author_pubkey || entry.seq != seq {
            return Err(StorageError::Parse(format!(
                "membership bytes declare {}/{}, expected {author_pubkey}/{seq}",
                entry.author_pubkey, entry.seq
            )));
        }
        let hash = crate::sync::store_commit::ObjectHash::digest(&data);
        let prefix = crate::sync::store_commit::membership_entry_semantic_prefix(
            author_pubkey,
            &entry.author_owner_grant,
            seq,
            hash,
        );
        SyncStorage::append_protocol_object(self, &prefix, ".json", data).await?;
        Ok(())
    }

    pub async fn read_latest_membership_head_bytes(
        &self,
        author_pubkey: &str,
    ) -> Result<Vec<u8>, StorageError> {
        crate::sync::store_objects::list_membership_head_objects(self)
            .await
            .map_err(|error| StorageError::Parse(error.to_string()))?
            .heads
            .into_iter()
            .filter(|head| head.value.author_pubkey == author_pubkey)
            .max_by_key(|head| head.value.seq)
            .map(|head| head.bytes)
            .ok_or_else(|| StorageError::NotFound(format!("membership head {author_pubkey}")))
    }

    pub async fn append_membership_head_bytes(
        &self,
        author_pubkey: &str,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let head: crate::sync::membership::AuthorHead = serde_json::from_slice(&data)
            .map_err(|error| StorageError::Parse(error.to_string()))?;
        let hash = crate::sync::store_commit::ObjectHash::digest(&data);
        let prefix = crate::sync::store_commit::membership_head_semantic_prefix(
            author_pubkey,
            &head.author_owner_grant,
            head.seq,
            hash,
        );
        SyncStorage::append_protocol_object(self, &prefix, ".json", data).await?;
        Ok(())
    }

    /// Hide a stored membership entry from `list_membership_entries` while leaving
    /// `get_membership_entry` able to serve it — the eventual-consistency window
    /// where the LIST that rebuilds the chain lags an entry a direct (keyed,
    /// read-after-write consistent) GET already resolves. Lets a test stage a
    /// member's authorizing Add, hide it from the LIST, and prove issue #84's
    /// grant-coordinate fetch recovers the changeset instead of dropping it.
    pub fn hide_membership_from_listing(&self, author_pubkey: &str, seq: u64) {
        let matches = self.membership_coords(author_pubkey, seq);
        assert_eq!(
            matches.len(),
            1,
            "membership slot must identify one grant stream"
        );
        let coord = &matches[0];
        self.hidden_from_listing.lock().unwrap().insert((
            coord.author_pubkey.clone(),
            coord.author_owner_grant.clone(),
            coord.seq,
        ));
    }

    /// Remove an author's published membership head while leaving its entries
    /// stored, so a reader test can distinguish a missing required head from a
    /// lagging entry listing.
    pub fn remove_membership_head(&self, author_pubkey: &str) {
        let prefix = format!("store-v1/membership/heads/{author_pubkey}/");
        let mut objects = self.protocol_objects.lock().unwrap();
        let before = objects.len();
        objects.retain(|(object, _)| !object.logical_key().starts_with(&prefix));
        assert!(
            objects.len() < before,
            "remove_membership_head: no head for {author_pubkey}"
        );
    }

    /// Remove one membership entry from keyed storage as well as the listing.
    pub fn remove_membership_entry(&self, author_pubkey: &str, seq: u64) {
        let matches = self.membership_coords(author_pubkey, seq);
        assert_eq!(
            matches.len(),
            1,
            "membership slot must identify one grant stream"
        );
        let prefix = format!(
            "store-v1/membership/entries/{author_pubkey}/{}/{seq}/",
            matches[0].author_owner_grant
        );
        let mut objects = self.protocol_objects.lock().unwrap();
        let before = objects.len();
        objects.retain(|(object, _)| !object.logical_key().starts_with(&prefix));
        assert!(
            objects.len() < before,
            "remove_membership_entry: no entry at {author_pubkey}/{seq}"
        );
    }

    /// Pause the next read of `author_pubkey`'s membership head after cloning its
    /// current bytes. Returns `(snapshot_held, release)` notifications.
    pub fn pause_next_membership_head_read(
        &self,
        author_pubkey: &str,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        let snapshot_held = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let previous =
            self.membership_head_read_pause
                .lock()
                .unwrap()
                .replace(MembershipHeadReadPause {
                    author_pubkey: author_pubkey.to_string(),
                    snapshot_held: snapshot_held.clone(),
                    release: release.clone(),
                });
        assert!(
            previous.is_none(),
            "a membership-head read is already paused"
        );
        (snapshot_held, release)
    }

    pub fn fail_next_blob_puts(&self, count: usize) {
        self.fail_blob_puts
            .store(count, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn fail_blob_put_on_call(&self, call_number: usize) {
        assert!(call_number > 0, "blob put call numbers are 1-based");
        self.blob_put_count
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.fail_blob_put_on
            .store(call_number, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn blob_put_from_file_count(&self) -> usize {
        self.blob_put_from_file_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn blob_read_to_file_count(&self) -> usize {
        self.blob_read_to_file_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn fail_next_blob_reads(&self, count: usize) {
        self.fail_blob_reads
            .store(count, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn fail_next_changeset_puts(&self, count: usize) {
        self.fail_changeset_puts
            .store(count, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn fail_wrapped_key_put_on_call(&self, call_number: usize) {
        arm_put_failure(
            &self.wrapped_key_put_count,
            &self.fail_wrapped_key_put_on,
            call_number,
            "wrapped-key",
        );
    }

    /// Fail the `call_number`-th membership-entry put (1-based) to exercise the
    /// invite entry-upload rollback: the wrapped key is written first, then the
    /// entry upload fails and the rollback must restore or delete the slot.
    pub fn fail_membership_entry_append_on_call(&self, call_number: usize) {
        arm_put_failure(
            &self.membership_entry_append_count,
            &self.fail_membership_entry_append_on,
            call_number,
            "membership-entry",
        );
    }

    /// Fail the `call_number`-th membership-head put (1-based) to exercise the
    /// failed-publish retry path: the entry uploads but its head does not commit.
    pub fn fail_membership_head_append_on_call(&self, call_number: usize) {
        arm_put_failure(
            &self.membership_head_append_count,
            &self.fail_membership_head_append_on,
            call_number,
            "membership-head",
        );
    }

    pub fn lose_membership_head_append_result_on_call(&self, call_number: usize) {
        assert!(
            call_number > 0,
            "membership-head append call numbers are 1-based"
        );
        self.membership_head_append_count
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.lose_membership_head_append_on
            .store(call_number, std::sync::atomic::Ordering::SeqCst);
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
        self.store_changeset_with_grant(
            device_id,
            seq,
            changeset_bytes,
            schema_version,
            Some(self.protocol_founder_coord()),
        );
    }

    pub fn store_changeset_with_grant(
        &self,
        device_id: &str,
        seq: u64,
        changeset_bytes: &[u8],
        schema_version: u32,
        membership_grant: Option<MembershipCoord>,
    ) {
        self.store_changeset_signed_as(
            device_id,
            seq,
            changeset_bytes,
            schema_version,
            membership_grant,
            &self.keypair,
            &self.keypair,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn store_changeset_signed_as(
        &self,
        device_id: &str,
        seq: u64,
        changeset_bytes: &[u8],
        schema_version: u32,
        membership_grant: Option<MembershipCoord>,
        commit_signer: &UserKeypair,
        head_signer: &UserKeypair,
    ) {
        let previous = if seq == 1 {
            None
        } else {
            Some(
                self.store_commits
                    .lock()
                    .unwrap()
                    .get(&(device_id.to_string(), seq - 1))
                    .unwrap_or_else(|| panic!("missing predecessor {device_id}/{}", seq - 1))
                    .commit_hash(),
            )
        };
        let commit = crate::sync::store_commit::StoreBatchCommit::signed(
            self.store_root_hash(),
            crate::WriteId::from_generated(format!("test-{device_id}-{seq}")),
            device_id.to_string(),
            crate::sync::store_commit::StoreCommitOrder::MergeConcurrent {
                seq,
                previous_commit_hash: previous,
                dependencies: std::collections::BTreeMap::new(),
            },
            membership_grant,
            schema_version,
            changeset_bytes,
            commit_signer,
        )
        .expect("sign test Store commit");
        let head = crate::sync::store_commit::StoreDeviceHead::signed(
            self.store_root_hash(),
            device_id.to_string(),
            Some(commit.position()),
            "2026-02-10T00:00:00Z".to_string(),
            head_signer,
        )
        .expect("sign test Store head");
        self.append_test_protocol(
            &commit
                .store_package
                .as_ref()
                .expect("test Store commit has a package")
                .object_key,
            ".pkg",
            changeset_bytes.to_vec(),
        );
        self.append_test_protocol(
            &crate::sync::store_commit::commit_semantic_prefix(
                device_id,
                seq,
                commit.commit_hash(),
            ),
            ".json",
            commit.to_bytes(),
        );
        self.append_test_protocol(
            &crate::sync::store_commit::head_semantic_prefix(device_id, seq, head.head_hash()),
            ".json",
            head.to_bytes(),
        );
        self.store_commits
            .lock()
            .unwrap()
            .insert((device_id.to_string(), seq), commit);
    }
}

#[async_trait]
impl SyncStorage for MockSyncStorage {
    async fn append_protocol_object(
        &self,
        semantic_prefix: &str,
        extension: &str,
        data: Vec<u8>,
    ) -> Result<ProtocolObjectLocator, StorageError> {
        if semantic_prefix.starts_with("store-v1/membership/entries/")
            && armed_put_failure_hits(
                &self.membership_entry_append_count,
                &self.fail_membership_entry_append_on,
            )
        {
            return Err(StorageError::Storage(format!(
                "forced membership-entry append failure for {semantic_prefix}"
            )));
        }
        if semantic_prefix.starts_with("store-v1/membership/heads/")
            && armed_put_failure_hits(
                &self.membership_head_append_count,
                &self.fail_membership_head_append_on,
            )
        {
            return Err(StorageError::Storage(format!(
                "forced membership-head append failure for {semantic_prefix}"
            )));
        }
        if extension == ".pkg"
            && self
                .fail_changeset_puts
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
        {
            return Err(StorageError::Storage(format!(
                "forced Store package append failure for {semantic_prefix}"
            )));
        }
        let appended = self.append_test_protocol(semantic_prefix, extension, data);
        if semantic_prefix.starts_with("store-v1/membership/heads/") {
            let call = self
                .membership_head_append_count
                .load(std::sync::atomic::Ordering::SeqCst);
            if self
                .lose_membership_head_append_on
                .compare_exchange(
                    call,
                    0,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
            {
                return Err(StorageError::Storage(format!(
                    "membership-head append result lost after storing {semantic_prefix}"
                )));
            }
        }
        Ok(appended)
    }

    async fn list_protocol_objects(
        &self,
        prefix: &str,
    ) -> Result<ProtocolObjectListing, StorageError> {
        if prefix == "store-v1/membership/entries/" {
            let armed_call_hit =
                armed_put_failure_hits(&self.membership_list_count, &self.fail_membership_list_on);
            if self
                .fail_membership_list
                .load(std::sync::atomic::Ordering::SeqCst)
                || armed_call_hit
            {
                return Err(StorageError::Storage(
                    "injected membership-list failure".into(),
                ));
            }
        }
        let hidden = self.hidden_from_listing.lock().unwrap();
        let objects = self
            .protocol_objects
            .lock()
            .unwrap()
            .iter()
            .filter(|(physical, _)| {
                if !physical.logical_key().starts_with(prefix) {
                    return false;
                }
                if prefix != "store-v1/membership/entries/" {
                    return true;
                }
                crate::sync::store_commit::parse_membership_entry_copy_key(physical.logical_key())
                    .is_ok_and(|parsed| {
                        !hidden.contains(&(
                            parsed.author,
                            parsed.author_owner_grant,
                            parsed.sequence,
                        ))
                    })
            })
            .map(|(physical, _)| {
                ProtocolObjectLocator::new(physical.logical_key().to_string(), physical.clone())
            })
            .collect();
        Ok(ProtocolObjectListing {
            objects,
            coverage: crate::storage::cloud::ListingCoverage::CompleteAtScan,
        })
    }

    async fn read_protocol_object(
        &self,
        object: &ProtocolObjectLocator,
        _semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError> {
        let bytes = self
            .protocol_objects
            .lock()
            .unwrap()
            .iter()
            .find(|(physical, _)| physical == object.physical())
            .map(|(_, bytes)| bytes.clone())
            .ok_or_else(|| StorageError::NotFound(object.logical_key().to_string()))?;
        if crate::sync::store_commit::parse_membership_entry_copy_key(object.logical_key()).is_ok()
        {
            self.membership_entry_read_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if let Ok(parsed) =
            crate::sync::store_commit::parse_membership_head_copy_key(object.logical_key())
        {
            let pause = {
                let mut pause = self.membership_head_read_pause.lock().unwrap();
                if pause
                    .as_ref()
                    .is_some_and(|pause| pause.author_pubkey == parsed.author)
                {
                    pause.take()
                } else {
                    None
                }
            };
            if let Some(pause) = pause {
                pause.snapshot_held.notify_one();
                pause.release.notified().await;
            }
        }
        Ok(bytes)
    }

    async fn delete_protocol_object(
        &self,
        object: &ProtocolObjectLocator,
    ) -> Result<(), StorageError> {
        let mut objects = self.protocol_objects.lock().unwrap();
        let before = objects.len();
        objects.retain(|(physical, _)| physical != object.physical());
        if objects.len() == before {
            return Err(StorageError::NotFound(object.logical_key().to_string()));
        }
        Ok(())
    }

    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        _scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let call_number = self
            .blob_put_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        if self
            .fail_blob_put_on
            .load(std::sync::atomic::Ordering::SeqCst)
            == call_number
        {
            return Err(StorageError::Storage(format!(
                "forced blob upload failure for {namespace}/{id}"
            )));
        }
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

    async fn put_blob_from_file(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_path: &std::path::Path,
    ) -> Result<(), StorageError> {
        self.blob_put_from_file_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let data = crate::local_blob::read(source_path)
            .await
            .map_err(StorageError::Storage)?;
        self.put_blob(namespace, id, scope, cloud_path, data).await
    }

    async fn get_blob(
        &self,
        namespace: &str,
        _uploader: Option<&str>,
        id: &str,
        _scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
    ) -> Result<Vec<u8>, StorageError> {
        // The mock keys blobs flat by (namespace, id) and ignores the uploader
        // prefix — the real `{namespace}/{uploader}/…` layout is exercised against
        // `CloudSyncStorage` over an `InMemoryCloudHome`, where it can be observed.
        let key = blob_key(namespace, id, cloud_path);
        let objects = self.objects.lock().unwrap();
        objects
            .get(&key)
            .cloned()
            .ok_or(StorageError::NotFound(key))
    }

    async fn blob_exists(
        &self,
        namespace: &str,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<bool, StorageError> {
        let key = blob_key(namespace, id, cloud_path);
        Ok(self.objects.lock().unwrap().contains_key(&key))
    }

    async fn read_blob_range(
        &self,
        namespace: &str,
        _uploader: Option<&str>,
        id: &str,
        _scope: crate::blob::BlobScope,
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

    async fn read_blob_to_file(
        &self,
        namespace: &str,
        uploader: Option<&str>,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_size: u64,
        expected_hash: &str,
        dest: &std::path::Path,
    ) -> Result<(), StorageError> {
        self.blob_read_to_file_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self
            .fail_blob_reads
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
        {
            return Err(StorageError::Storage(format!(
                "forced blob download failure for {namespace}/{id}"
            )));
        }
        // Concurrency probe: when armed, record the peak in-flight count and gather on
        // the barrier so a fixed number of fetches must run at once to proceed. Clone
        // the Arc out of the lock first so the guard isn't held across the await.
        let barrier = self.read_to_file_barrier.lock().unwrap().clone();
        if let Some(barrier) = barrier {
            let inflight = self
                .read_to_file_inflight
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            self.read_to_file_max_inflight
                .fetch_max(inflight, std::sync::atomic::Ordering::SeqCst);
            barrier.wait().await;
            self.read_to_file_inflight
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
        let bytes = self
            .read_blob_range(
                namespace,
                uploader,
                id,
                scope,
                cloud_path,
                source_size,
                0,
                source_size,
            )
            .await?;
        // Verify the content hash before committing the file, the same authority
        // the real `CloudSyncStorage` streaming download enforces — so a
        // mock-served blob that does not match its row's hash is refused, not
        // cached.
        let actual = crate::blob::content_hash(&bytes);
        if actual != expected_hash {
            return Err(StorageError::InvalidContent(format!(
                "blob {namespace}/{id} content hash mismatch: expected {expected_hash}, got {actual}"
            )));
        }
        crate::local_blob::write_atomic(dest, &bytes)
            .await
            .map_err(StorageError::LocalFilesystem)
    }

    fn blob_path_scheme(&self) -> crate::sync::cloud_storage::BlobPathScheme {
        crate::sync::cloud_storage::BlobPathScheme::Hashed
    }

    fn blob_cloud_key(
        &self,
        namespace: &str,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<String, StorageError> {
        // The mock keys objects flatly (namespace/cloud_path or namespace/id),
        // regardless of the scheme it reports — so a tombstone cancel targets the same
        // key the object was stored under.
        Ok(blob_key(namespace, id, cloud_path))
    }

    fn own_uploader(&self) -> Option<String> {
        Some(hex::encode(self.keypair.public_key()))
    }

    async fn put_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        if armed_put_failure_hits(&self.wrapped_key_put_count, &self.fail_wrapped_key_put_on) {
            return Err(StorageError::Storage(format!(
                "forced wrapped-key upload failure for {owner_pubkey}/{recipient_pubkey}"
            )));
        }
        let key = format!("keys/{owner_pubkey}/{recipient_pubkey}.enc");
        self.objects.lock().unwrap().insert(key, data);
        Ok(())
    }

    async fn get_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<Vec<u8>, StorageError> {
        let key = format!("keys/{owner_pubkey}/{recipient_pubkey}.enc");
        let objects = self.objects.lock().unwrap();
        objects
            .get(&key)
            .cloned()
            .ok_or(StorageError::NotFound(key))
    }

    async fn delete_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<(), StorageError> {
        let key = format!("keys/{owner_pubkey}/{recipient_pubkey}.enc");
        self.objects.lock().unwrap().remove(&key);
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

#[async_trait]
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

#[async_trait]
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

    async fn set_access(
        &self,
        desired: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, CloudHomeError> {
        Ok(match desired {
            crate::storage::cloud::CloudAccessState::Present { .. } => {
                crate::storage::cloud::CloudAccessOutcome::Present(CloudHomeJoinInfo::S3 {
                    bucket: "test-bucket".to_string(),
                    region: "us-east-1".to_string(),
                    endpoint: None,
                    access_key: "test-access-key".to_string(),
                    secret_key: "test-secret-key".to_string(),
                    key_prefix: None,
                })
            }
            crate::storage::cloud::CloudAccessState::Absent { .. } => {
                crate::storage::cloud::CloudAccessOutcome::Absent(
                    crate::storage::cloud::RevokeOutcome::Unsupported,
                )
            }
        })
    }
}

/// Bind a test database to the immutable Store protocol root already carried by the
/// mock and to the device that will publish from it.
pub async fn bind_mock_store_protocol(db: &Database, storage: &MockSyncStorage, device_id: &str) {
    db.set_protocol_state(
        crate::database::STORE_ROOT_HASH_STATE_KEY,
        &storage.store_root_hash().to_string(),
    )
    .await
    .expect("bind mock Store protocol root");
    db.set_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY, device_id)
        .await
        .expect("bind mock Store device");
}

/// Append a founder-signed Store protocol root and pin its exact hash in a test
/// database. Real cloud-storage tests use this instead of inventing protocol
/// state that has no corresponding immutable object.
pub async fn publish_test_store_protocol_root(
    db: &Database,
    storage: &dyn SyncStorage,
    store_id: &str,
    device_id: &str,
    founder: &UserKeypair,
) -> crate::sync::store_commit::ObjectHash {
    let founder_entry = crate::sync::membership::founder_entry(
        store_id,
        founder,
        "0000000000001-0000-test-store-protocol-root",
    );
    let store_protocol_root = crate::sync::store_commit::StoreProtocolRoot::signed(
        store_id.to_string(),
        founder_entry,
        1,
        db.sync_routing_hash(),
        crate::WritePolicy::MergeConcurrent,
        founder,
    )
    .expect("sign test Store protocol root");
    let hash = store_protocol_root.object_hash();
    crate::sync::store_objects::append_and_verify(
        storage,
        &crate::sync::store_commit::store_protocol_root_semantic_prefix(hash),
        ".json",
        &store_protocol_root.to_bytes(),
    )
    .await
    .expect("append test Store protocol root");
    db.set_protocol_state(
        crate::database::STORE_ROOT_HASH_STATE_KEY,
        &hash.to_string(),
    )
    .await
    .expect("pin test Store protocol root");
    db.set_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY, device_id)
        .await
        .expect("bind test Store device");
    hash
}

pub async fn publish_test_serial_store_protocol_root(
    db: &Database,
    storage: &dyn SyncStorage,
    store_id: &str,
    device_id: &str,
    founder: &UserKeypair,
) -> crate::sync::store_commit::ObjectHash {
    let founder_entry = crate::sync::membership::founder_entry(
        store_id,
        founder,
        "0000000000001-0000-test-serial-store-protocol-root",
    );
    let store_protocol_root = crate::sync::store_commit::StoreProtocolRoot::signed(
        store_id.to_string(),
        founder_entry,
        1,
        db.sync_routing_hash(),
        crate::WritePolicy::Serial,
        founder,
    )
    .expect("sign test Serial Store protocol root");
    let hash = store_protocol_root.object_hash();
    crate::sync::store_objects::append_and_verify(
        storage,
        &crate::sync::store_commit::store_protocol_root_semantic_prefix(hash),
        ".json",
        &store_protocol_root.to_bytes(),
    )
    .await
    .expect("append test Serial Store protocol root");
    db.set_protocol_state(
        crate::database::STORE_ROOT_HASH_STATE_KEY,
        &hash.to_string(),
    )
    .await
    .expect("pin test Serial Store protocol root");
    db.set_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY, device_id)
        .await
        .expect("bind test Serial Store device");
    let authorization = crate::sync::membership::SerialAuthorizationState::from_founder(
        hash,
        &store_protocol_root.founder,
    )
    .expect("derive test Serial founder authorization");
    db.install_serial_root_authorization(
        store_protocol_root.founder.author_pubkey.clone(),
        authorization,
    )
    .await
    .expect("install test Serial founder authorization");
    hash
}

/// Publish one Store protocol root and its byte-identical membership founder root.
pub async fn publish_test_protocol_roots(
    storage: &dyn SyncStorage,
    store_id: &str,
    founder: &UserKeypair,
    created_at: &str,
) -> (
    crate::sync::store_commit::StoreProtocolRoot,
    crate::sync::membership::MembershipChain,
) {
    let founder_entry = crate::sync::membership::founder_entry(store_id, founder, created_at);
    let store_protocol_root = crate::sync::store_commit::StoreProtocolRoot::signed(
        store_id.to_string(),
        founder_entry.clone(),
        1,
        test_sync_routing_hash(),
        crate::WritePolicy::MergeConcurrent,
        founder,
    )
    .expect("sign test Store protocol root");
    let hash = store_protocol_root.object_hash();
    crate::sync::store_objects::append_and_verify(
        storage,
        &crate::sync::store_commit::store_protocol_root_semantic_prefix(hash),
        ".json",
        &store_protocol_root.to_bytes(),
    )
    .await
    .expect("publish test Store protocol root");
    let coord = founder_entry.coord();
    let chain = crate::sync::membership::MembershipChain::from_entries(vec![founder_entry.clone()])
        .expect("validate test founder membership");
    crate::sync::store_objects::append_membership_entry_object(storage, &coord, &founder_entry)
        .await
        .expect("publish test founder membership");
    crate::sync::membership_ops::publish_membership_head(storage, &chain, founder)
        .await
        .expect("publish test founder head");
    (store_protocol_root, chain)
}

pub async fn publish_test_founder_membership(
    storage: &dyn SyncStorage,
    store_id: &str,
    founder: &UserKeypair,
) -> crate::sync::membership::MembershipChain {
    let store_protocol_root = crate::sync::store_objects::discover_store_protocol_root(
        storage,
        store_id,
        Some(&crate::keys::public_key_hex(founder)),
    )
    .await
    .expect("load test Store protocol root")
    .value;
    let entry = store_protocol_root.founder;
    let coord = entry.coord();
    let mut chain = crate::sync::membership::MembershipChain::new();
    chain
        .add_entry_at(coord.clone(), entry.clone())
        .expect("valid test founder membership");
    crate::sync::store_objects::append_membership_entry_object(storage, &coord, &entry)
        .await
        .expect("publish test founder membership");
    crate::sync::membership_ops::publish_membership_head(storage, &chain, founder)
        .await
        .expect("publish test founder membership head");
    chain
}

#[allow(clippy::too_many_arguments)]
pub async fn push_test_store_snapshot(
    storage: &dyn SyncStorage,
    store_root_hash: crate::sync::store_commit::ObjectHash,
    db_image: Vec<u8>,
    coverage: std::collections::BTreeMap<String, crate::sync::store_commit::CommitPosition>,
    schema_version: u32,
    founder: &UserKeypair,
    membership: &crate::sync::membership::MembershipChain,
    db: &Database,
) -> crate::sync::store_commit::SnapshotMeta {
    crate::sync::store_snapshot::push_store_snapshot(
        storage,
        store_root_hash,
        crate::sync::snapshot::CreatedSnapshot {
            db_image,
            host_blobs: Vec::new(),
            publish_blobs: Vec::new(),
        },
        crate::CommitFrontier::MergeConcurrent(coverage),
        schema_version,
        founder,
        "2026-02-10T00:00:00Z".to_string(),
        Some(membership),
        db,
    )
    .await
    .expect("publish test Store snapshot")
}

pub async fn push_test_serial_store_snapshot(
    storage: &dyn SyncStorage,
    store_root_hash: crate::sync::store_commit::ObjectHash,
    db_image: Vec<u8>,
    coverage: Option<crate::sync::store_commit::CommitPosition>,
    schema_version: u32,
    founder: &UserKeypair,
    db: &Database,
) -> crate::sync::store_commit::SnapshotMeta {
    crate::sync::store_snapshot::push_store_snapshot(
        storage,
        store_root_hash,
        crate::sync::snapshot::CreatedSnapshot {
            db_image,
            host_blobs: Vec::new(),
            publish_blobs: Vec::new(),
        },
        crate::CommitFrontier::Serial(coverage),
        schema_version,
        founder,
        "2026-07-14T00:00:00Z".to_string(),
        None,
        db,
    )
    .await
    .expect("publish test Serial Store snapshot")
}

/// Pull into `db` the way production does: `pull_changes` applies each incoming
/// changeset with a plain `call` (never journaled), so applied rows aren't
/// recorded as a local change, while a host write during the pull journals
/// normally. Returns the updated positions and the pull result.
pub async fn pull_into(
    db: &Database,
    storage: &MockSyncStorage,
    device_id: &str,
    store_dir: &crate::store_dir::StoreDir,
) -> (
    std::collections::BTreeMap<String, u64>,
    crate::sync::store_pull::StorePullResult,
) {
    pull_into_result(db, storage, device_id, store_dir)
        .await
        .expect("pull")
}

pub async fn pull_into_result(
    db: &Database,
    storage: &MockSyncStorage,
    device_id: &str,
    store_dir: &crate::store_dir::StoreDir,
) -> Result<
    (
        std::collections::BTreeMap<String, u64>,
        crate::sync::store_pull::StorePullResult,
    ),
    crate::sync::store_pull::StorePullError,
> {
    bind_mock_store_protocol(db, storage, device_id).await;
    let store_root_hash = storage.store_root_hash();
    let membership = crate::sync::pull::load_cycle_membership(storage, db)
        .await
        .map_err(|error| {
            crate::sync::store_pull::StorePullError::Membership(
                crate::sync::store_pull::StorePullMembershipError::Message(error.to_string()),
            )
        })?;
    let result = crate::sync::store_pull::pull_store_commits(
        db,
        db.synced_tables(),
        storage,
        store_root_hash,
        device_id,
        store_dir,
        membership.chain.as_ref(),
    )
    .await?;
    let sequences = result
        .frontier
        .iter()
        .map(|(device_id, position)| (device_id.clone(), position.seq))
        .collect();
    Ok((sequences, result))
}

pub async fn pull_cloud_into(
    db: &Database,
    trusted_store_db: &Database,
    storage: &crate::sync::cloud_storage::CloudSyncStorage,
    device_id: &str,
    store_dir: &crate::store_dir::StoreDir,
) -> (
    std::collections::BTreeMap<String, u64>,
    crate::sync::store_pull::StorePullResult,
) {
    let store_root_hash = trusted_store_db
        .get_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
        .await
        .expect("read trusted Store protocol root")
        .expect("trusted database is bound to a Store protocol root")
        .parse()
        .expect("trusted Store protocol root hash");
    crate::sync::store_protocol_root::open_store(
        db,
        storage,
        store_root_hash,
        storage.store_id(),
        &crate::keys::public_key_hex(storage.user_keypair()),
    )
    .await
    .expect("open exact test Store");
    db.set_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY, device_id)
        .await
        .expect("bind test Store device");
    let membership = crate::sync::pull::load_cycle_membership(storage, db)
        .await
        .expect("load test Store membership");
    let result = crate::sync::store_pull::pull_store_commits(
        db,
        db.synced_tables(),
        storage,
        store_root_hash,
        device_id,
        store_dir,
        membership.chain.as_ref(),
    )
    .await
    .expect("pull exact test Store");
    let sequences = result
        .frontier
        .iter()
        .map(|(device_id, position)| (device_id.clone(), position.seq))
        .collect();
    (sequences, result)
}

/// Drive the raw engine with an injected [`SyncStorage`] in downstream tests.
/// Production runtimes can execute cycles only through initialized
/// [`crate::sync::cycle::SyncComponents`].
#[allow(clippy::too_many_arguments)]
pub async fn run_test_cycle(
    storage: &dyn SyncStorage,
    store_id: &str,
    device_id: &str,
    hlc: &crate::sync::hlc::Hlc,
    clock: &dyn crate::clock::Clock,
    db: &Database,
    cipher: &dyn crate::sync::cloud_storage::CloudCipherAccess,
    pending_rotation: &crate::sync::cloud_storage::PendingRotation,
    user_keypair: &UserKeypair,
    custody: Option<&dyn MasterKeyCustody>,
    store_dir: &StoreDir,
    cloud_home: Option<&dyn CloudHome>,
    observer: Option<&dyn crate::blob::BlobTransitionObserver>,
) -> Result<crate::sync::cycle::SyncCycleResult, String> {
    crate::sync::cycle::run_single_sync_cycle(
        storage,
        store_id,
        device_id,
        hlc,
        clock,
        db,
        cipher,
        pending_rotation,
        user_keypair,
        custody,
        store_dir,
        cloud_home,
        observer,
    )
    .await
    .map_err(|error| error.to_string())
}
