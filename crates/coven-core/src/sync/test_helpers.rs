/// Shared test helpers for sync module tests.
///
/// These drive a real [`Database`] over an in-memory connection carrying the
/// synthetic test schema, so tests exercise the engine through the same path
/// production does.
use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension};

use crate::database::{Database, DbError, PendingChangesetBatch};
use crate::encryption::MasterKeyring;
use crate::keys::{KeyError, MasterKeyCustody, UserKeypair};
use crate::migration::Migration;
use crate::storage::cloud::{BoxPartSink, CloudHome, CloudHomeError, CloudHomeJoinInfo, PartSink};
use crate::store_dir::StoreDir;
use crate::sync::apply::resolve_and_apply_changeset;
use crate::sync::envelope::{self, ChangesetEnvelope};
use crate::sync::membership::{
    sign_membership_entry, MemberRole, MembershipAction, MembershipChain, MembershipCoord,
    MembershipEntry,
};
use crate::sync::pull::pull_changes;
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::sync::signed_control::{HeadJson, MinSchemaVersionJson};
use crate::sync::storage::{DeviceHead, HeadListing, MinSchemaVersion, StorageError, SyncStorage};

/// In-memory [`MasterKeyCustody`] for tests, with a switch to force `persist`
/// to fail. The switch models a device whose keyring is momentarily
/// unwritable, so a test can drive a key adoption into its failure path and then
/// clear the switch to prove the retry converges. Stores the serialized form
/// (like the real `Keyring` preset), so `stored_key` reflects exactly what a
/// caller wrote — a raw-hex seed from [`Self::set_initial_key`] stays raw hex
/// until a real `persist` call re-serializes it to the keyring JSON format.
#[derive(Default)]
pub struct TestCustody {
    value: Mutex<Option<String>>,
    fail: std::sync::atomic::AtomicBool,
}

impl TestCustody {
    pub fn set_initial_key(&self, key: [u8; 32]) {
        *self.value.lock().unwrap() = Some(hex::encode(key));
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
        SyncedTable::new("notes").gated_by("shared"),
        SyncedTable::new("note_tags"),
        SyncedTable::new("note_photos"),
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
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
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
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
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
    db.call(move |conn| {
        Database::run_pending_journaled_transaction_on(conn, &tables, |tx| {
            tx.execute_batch(&sql).map(|_| ()).map_err(DbError::from)
        })
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
    if !stmts.is_empty() {
        let owned: Vec<String> = stmts.iter().map(|s| s.to_string()).collect();
        let tables = db.synced_tables().to_vec();
        db.call(move |conn| {
            Database::run_pending_journaled_transaction_on(conn, &tables, |tx| {
                for s in &owned {
                    tx.execute_batch(s).map_err(DbError::from)?;
                }
                Ok::<(), DbError>(())
            })
        })
        .await
        .unwrap_or_else(|e| panic!("journal writes failed: {e}"));
    }
    drain_pending_changesets(db).await
}

/// Drain the pending-changeset journal, returning the combined bytes and clearing
/// the drained rows. Empty when the journal holds nothing — the same bytes a sync
/// cycle would read from the journal and push.
async fn drain_pending_changesets(db: &Database) -> Vec<u8> {
    match db
        .pending_changeset_batch()
        .await
        .expect("read pending changesets")
    {
        PendingChangesetBatch::Empty => Vec::new(),
        PendingChangesetBatch::Pending { max_id, changeset } => {
            db.clear_pending_changesets_through(max_id)
                .await
                .expect("clear drained pending changesets");
            changeset
        }
    }
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

/// A fresh membership chain whose only member is `owner`, the founding owner,
/// stamped at the standard test founding time. A test that needs a different
/// founding timestamp builds the chain from `founder_entry` directly.
pub fn bootstrap_chain(owner: &UserKeypair) -> MembershipChain {
    let owner_pubkey = pubkey_hex(owner);
    let founder = founder_entry(owner, "0000000001000-0000-dev1");
    let mut chain = MembershipChain::new();
    chain
        .add_entry_at(
            MembershipCoord {
                author_pubkey: owner_pubkey,
                seq: 1,
            },
            founder,
        )
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
        prev_hash: None,
        role,
        timestamp: timestamp.to_string(),
        author_pubkey: pubkey_hex(author),
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, author);
    entry
}

pub fn make_linked_entry(
    chain: &MembershipChain,
    author: &UserKeypair,
    action: MembershipAction,
    subject: &UserKeypair,
    role: MemberRole,
    timestamp: &str,
) -> MembershipEntry {
    let author_pubkey = pubkey_hex(author);
    let mut entry = make_entry(author, action, subject, role, timestamp);
    entry.prev_hash = chain.latest_author_hash(&author_pubkey);
    sign_membership_entry(&mut entry, author);
    entry
}

pub async fn append_membership_entry(
    storage: &MockSyncStorage,
    chain: &mut MembershipChain,
    author_pubkey: &str,
    seq: u64,
    entry: MembershipEntry,
) {
    chain
        .add_entry_at(
            MembershipCoord {
                author_pubkey: author_pubkey.to_string(),
                seq,
            },
            entry.clone(),
        )
        .expect("valid membership test chain");
    storage
        .put_membership_entry(author_pubkey, seq, serde_json::to_vec(&entry).unwrap())
        .await
        .expect("upload membership entry");
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
    /// Every `(device_id, timestamp)` passed to [`SyncStorage::put_head`], in call
    /// order. Lets a test assert the timestamp each writer records is RFC 3339, the
    /// format the trait's `put_head` contract specifies.
    head_puts: Mutex<Vec<(String, String)>>,
    /// Snapshot generations and their pointer, keyed exactly as the cloud lays
    /// them out (`snapshot/{author}/{publish_id}.db`,
    /// `snapshot/{author}/{publish_id}_meta.json`, `snapshot/current.json`). Keying
    /// each device's generations under its own `{author}` — not a flat id — is what
    /// lets a test exercise the real globally-unique keyspace and atomic-publish
    /// behavior.
    snapshot_objects: Mutex<HashMap<String, Vec<u8>>>,
    /// Signed `min_schema_version.json` bytes (None = no minimum set).
    min_schema_version: Mutex<Option<Vec<u8>>>,
    /// Per-author signed membership heads, keyed by author pubkey -> head bytes.
    /// An author absent from this map has not committed a head (its entries are
    /// uncommitted).
    membership_heads: Mutex<HashMap<String, Vec<u8>>>,
    /// One membership-head read to pause after snapshotting its bytes. The two
    /// notifications tell the test that the snapshot is held and release it.
    membership_head_read_pause: Mutex<Option<MembershipHeadReadPause>>,
    /// The device identity this mock signs its head/min_schema with. Defaults to a
    /// fresh keypair; a membership test that needs the head/floor attributed to a
    /// specific member constructs the mock with [`Self::with_keypair`].
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
    membership_get_count: std::sync::atomic::AtomicUsize,
    /// `(author_pubkey, seq)` entries the LIST omits but a keyed GET still serves.
    /// Simulates the eventual-consistency window where a freshly-written
    /// membership entry isn't in the LIST yet, but a direct GET (read-after-write
    /// consistent) resolves it — the exact lag issue #84's grant-coordinate fetch
    /// is built for.
    hidden_from_listing: Mutex<std::collections::HashSet<(String, u64)>>,
    fail_blob_puts: std::sync::atomic::AtomicUsize,
    blob_put_count: std::sync::atomic::AtomicUsize,
    blob_put_from_file_count: std::sync::atomic::AtomicUsize,
    blob_read_to_file_count: std::sync::atomic::AtomicUsize,
    fail_blob_put_on: std::sync::atomic::AtomicUsize,
    fail_changeset_puts: std::sync::atomic::AtomicUsize,
    wrapped_key_put_count: std::sync::atomic::AtomicUsize,
    fail_wrapped_key_put_on: std::sync::atomic::AtomicUsize,
    membership_head_put_count: std::sync::atomic::AtomicUsize,
    fail_membership_head_put_on: std::sync::atomic::AtomicUsize,
    membership_entry_put_count: std::sync::atomic::AtomicUsize,
    fail_membership_entry_put_on: std::sync::atomic::AtomicUsize,
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

    /// A mock whose head and min_schema control objects are signed by `keypair`.
    /// Lets a membership test attribute the head it publishes (and any floor it
    /// sets) to a specific member, the way a real device signs its own.
    pub fn with_keypair(keypair: UserKeypair) -> Self {
        MockSyncStorage {
            objects: Mutex::new(HashMap::new()),
            heads: Mutex::new(HashMap::new()),
            head_puts: Mutex::new(Vec::new()),
            snapshot_objects: Mutex::new(HashMap::new()),
            min_schema_version: Mutex::new(None),
            membership_heads: Mutex::new(HashMap::new()),
            membership_head_read_pause: Mutex::new(None),
            keypair,
            fail_membership_list: std::sync::atomic::AtomicBool::new(false),
            fail_membership_list_on: std::sync::atomic::AtomicUsize::new(0),
            membership_list_count: std::sync::atomic::AtomicUsize::new(0),
            membership_get_count: std::sync::atomic::AtomicUsize::new(0),
            hidden_from_listing: Mutex::new(std::collections::HashSet::new()),
            fail_blob_puts: std::sync::atomic::AtomicUsize::new(0),
            blob_put_count: std::sync::atomic::AtomicUsize::new(0),
            blob_put_from_file_count: std::sync::atomic::AtomicUsize::new(0),
            blob_read_to_file_count: std::sync::atomic::AtomicUsize::new(0),
            fail_blob_put_on: std::sync::atomic::AtomicUsize::new(0),
            fail_changeset_puts: std::sync::atomic::AtomicUsize::new(0),
            wrapped_key_put_count: std::sync::atomic::AtomicUsize::new(0),
            fail_wrapped_key_put_on: std::sync::atomic::AtomicUsize::new(0),
            membership_head_put_count: std::sync::atomic::AtomicUsize::new(0),
            fail_membership_head_put_on: std::sync::atomic::AtomicUsize::new(0),
            membership_entry_put_count: std::sync::atomic::AtomicUsize::new(0),
            fail_membership_entry_put_on: std::sync::atomic::AtomicUsize::new(0),
            read_to_file_barrier: Mutex::new(None),
            read_to_file_inflight: std::sync::atomic::AtomicUsize::new(0),
            read_to_file_max_inflight: std::sync::atomic::AtomicUsize::new(0),
        }
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

    /// Remove an author's published membership head while leaving its entries
    /// stored, so a reader test can distinguish a missing required head from a
    /// lagging entry listing.
    pub fn remove_membership_head(&self, author_pubkey: &str) {
        assert!(
            self.membership_heads
                .lock()
                .unwrap()
                .remove(author_pubkey)
                .is_some(),
            "remove_membership_head: no head for {author_pubkey}",
        );
    }

    /// Remove one membership entry from keyed storage as well as the listing.
    pub fn remove_membership_entry(&self, author_pubkey: &str, seq: u64) {
        let key = format!("membership/{author_pubkey}/{seq}");
        assert!(
            self.objects.lock().unwrap().remove(&key).is_some(),
            "remove_membership_entry: no entry at {author_pubkey}/{seq}",
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
    pub fn fail_membership_entry_put_on_call(&self, call_number: usize) {
        arm_put_failure(
            &self.membership_entry_put_count,
            &self.fail_membership_entry_put_on,
            call_number,
            "membership-entry",
        );
    }

    /// Fail the `call_number`-th membership-head put (1-based) to exercise the
    /// failed-publish retry path: the entry uploads but its head does not commit.
    pub fn fail_membership_head_put_on_call(&self, call_number: usize) {
        arm_put_failure(
            &self.membership_head_put_count,
            &self.fail_membership_head_put_on,
            call_number,
            "membership-head",
        );
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
    /// `bootstrap_from_snapshot` would adopt without re-deriving the
    /// pointer→publish_id→db resolution itself.
    pub async fn current_snapshot_db(&self) -> Option<Vec<u8>> {
        let (author, publish_id) = self.current_snapshot_target().await?;
        self.snapshot_objects
            .lock()
            .unwrap()
            .get(&format!("snapshot/{author}/{publish_id}.db"))
            .cloned()
    }

    /// The live snapshot generation's author and publish id, or None if no pointer
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
        Some((pointer.author_pubkey, pointer.publish_id))
    }

    /// The publish id the live snapshot pointer names, or None if no pointer has
    /// been published.
    pub async fn current_snapshot_publish_id(&self) -> Option<u64> {
        self.current_snapshot_target()
            .await
            .map(|(_, publish_id)| publish_id)
    }

    /// The metadata of the live snapshot generation — the one the pointer names —
    /// or None if no snapshot has been published.
    pub async fn current_snapshot_meta(&self) -> Option<Vec<u8>> {
        let (author, publish_id) = self.current_snapshot_target().await?;
        self.snapshot_objects
            .lock()
            .unwrap()
            .get(&format!("snapshot/{author}/{publish_id}_meta.json"))
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
        // Sign with the mock's keypair, as production always does: the author is
        // what the pull records as each introduced blob's uploader (there is no
        // listing scan), so a blob a signed changeset introduces is downloadable
        // and later readable. A test that specifically needs an unsigned envelope
        // (a pre-initialization unanchored path or an unsigned-rejection path) uses
        // [`store_unsigned_changeset`].
        let packed = envelope::pack_signed(
            device_id,
            seq,
            schema_version,
            "",
            "2026-02-10T00:00:00Z",
            &self.keypair,
            None,
            changeset_bytes,
        );
        self.store_packed(device_id, seq, packed);
    }

    /// Store an unsigned changeset envelope (`author_pubkey`/`signature` both
    /// `None`), for tests that exercise the pre-initialization unanchored path or
    /// assert pull rejects an unsigned changeset once a membership chain exists.
    pub fn store_unsigned_changeset(
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
        self.publish_head(device_id, seq);
    }

    /// Publish a signed head for `device_id` at `seq`, exactly as `put_head` does,
    /// but reusable by the changeset-fabrication helpers. Signs with the mock's
    /// keypair and stores the resulting `HeadJson` JSON bytes.
    fn publish_head(&self, device_id: &str, seq: u64) {
        let head = HeadJson::signed(device_id, seq, None, &self.keypair);
        let bytes = serde_json::to_vec(&head).expect("serialize head");
        self.heads
            .lock()
            .unwrap()
            .insert(device_id.to_string(), bytes);
    }

    /// Every `(device_id, timestamp)` passed to [`SyncStorage::put_head`], in call
    /// order — for asserting the timestamp each head writer records is RFC 3339.
    pub fn head_put_timestamps(&self) -> Vec<(String, String)> {
        self.head_puts.lock().unwrap().clone()
    }

    /// Publish a head for `device_id` signed by `keypair` — so the head's author is
    /// that device's own key rather than the mock's default identity. Lets a
    /// changeset-GC test give each device a head attributed to its own membership
    /// key (the author changeset reclamation matches each device's ack against).
    pub fn publish_head_as(&self, device_id: &str, seq: u64, keypair: &UserKeypair) {
        let head = HeadJson::signed(device_id, seq, None, keypair);
        let bytes = serde_json::to_vec(&head).expect("serialize head");
        self.heads
            .lock()
            .unwrap()
            .insert(device_id.to_string(), bytes);
    }

    /// Place an object in a device's head slot that cannot be parsed as a
    /// `HeadJson`, mirroring a slot the cloud reader can't open/parse/verify. The
    /// slot is a valid device-head key, so `list_heads` counts it toward
    /// `unreadable` — letting a test exercise reclamation failing closed.
    pub fn publish_unreadable_head(&self, device_id: &str) {
        self.heads
            .lock()
            .unwrap()
            .insert(device_id.to_string(), b"not a head".to_vec());
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl SyncStorage for MockSyncStorage {
    async fn list_heads(&self) -> Result<HeadListing, StorageError> {
        let heads = self.heads.lock().unwrap();
        let mut out = Vec::new();
        let mut unreadable = 0usize;
        for (device_id, bytes) in heads.iter() {
            // Mirror the cloud: a head that can't be parsed or verified is skipped
            // (not fatal) and counted, so reclamation can fail closed on a
            // present-but-unreadable slot while the pull stays tolerant.
            let head: HeadJson = match serde_json::from_slice(bytes) {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!("skipping head {device_id} that does not parse: {e}");
                    unreadable += 1;
                    continue;
                }
            };
            if !head.verify(device_id) {
                tracing::warn!("skipping head {device_id} with an invalid signature");
                unreadable += 1;
                continue;
            }
            out.push(DeviceHead {
                device_id: device_id.clone(),
                seq: head.seq,
                last_sync: head.last_sync,
                author_pubkey: head.author_pubkey,
            });
        }
        out.sort_by(|a, b| a.device_id.cmp(&b.device_id));
        Ok(HeadListing {
            heads: out,
            unreadable,
        })
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
        if self
            .fail_changeset_puts
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
        {
            return Err(StorageError::Storage(format!(
                "forced changeset publish failure for {device_id}/{seq}"
            )));
        }
        let key = format!("changes/{device_id}/{seq}");
        self.objects.lock().unwrap().insert(key, data);
        Ok(())
    }

    async fn put_head(
        &self,
        device_id: &str,
        seq: u64,
        timestamp: &str,
    ) -> Result<(), StorageError> {
        self.head_puts
            .lock()
            .unwrap()
            .push((device_id.to_string(), timestamp.to_string()));
        // Record the timestamp on the head the way the cloud does, so `list_heads`
        // returns it as the device's `last_sync` — the field the sync-status view
        // reads.
        let head = HeadJson::signed(device_id, seq, Some(timestamp.to_string()), &self.keypair);
        let bytes = serde_json::to_vec(&head).expect("serialize head");
        self.heads
            .lock()
            .unwrap()
            .insert(device_id.to_string(), bytes);
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
            return Err(StorageError::Storage(format!(
                "blob {namespace}/{id} content hash mismatch: expected {expected_hash}, got {actual}"
            )));
        }
        crate::local_blob::write_atomic(dest, &bytes)
            .await
            .map_err(StorageError::Storage)
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

    async fn put_snapshot(
        &self,
        author: &str,
        publish_id: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.snapshot_objects
            .lock()
            .unwrap()
            .insert(format!("snapshot/{author}/{publish_id}.db"), data);
        Ok(())
    }

    async fn get_snapshot(&self, author: &str, publish_id: u64) -> Result<Vec<u8>, StorageError> {
        let key = format!("snapshot/{author}/{publish_id}.db");
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
        if armed_put_failure_hits(
            &self.membership_entry_put_count,
            &self.fail_membership_entry_put_on,
        ) {
            return Err(StorageError::Storage(format!(
                "forced membership-entry upload failure for {author_pubkey}/{seq}"
            )));
        }
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

    async fn put_membership_head(
        &self,
        author_pubkey: &str,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        if armed_put_failure_hits(
            &self.membership_head_put_count,
            &self.fail_membership_head_put_on,
        ) {
            return Err(StorageError::Storage(format!(
                "forced membership-head upload failure for {author_pubkey}"
            )));
        }
        self.membership_heads
            .lock()
            .unwrap()
            .insert(author_pubkey.to_string(), data);
        Ok(())
    }

    async fn get_membership_head(&self, author_pubkey: &str) -> Result<Vec<u8>, StorageError> {
        let bytes = self
            .membership_heads
            .lock()
            .unwrap()
            .get(author_pubkey)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(format!("membership/{author_pubkey}/head")))?;
        let pause = {
            let mut pause = self.membership_head_read_pause.lock().unwrap();
            if pause
                .as_ref()
                .is_some_and(|pause| pause.author_pubkey == author_pubkey)
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
        Ok(bytes)
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

    async fn put_snapshot_meta(
        &self,
        author: &str,
        publish_id: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.snapshot_objects
            .lock()
            .unwrap()
            .insert(format!("snapshot/{author}/{publish_id}_meta.json"), data);
        Ok(())
    }

    async fn get_snapshot_meta(
        &self,
        author: &str,
        publish_id: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let key = format!("snapshot/{author}/{publish_id}_meta.json");
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
        let mut publish_ids = Vec::new();
        for key in objects.keys() {
            let Some(rest) = key.strip_prefix(&prefix) else {
                // A different author's generation or the shared pointer — not under
                // this author's prefix, legitimately not ours; skip silently.
                continue;
            };
            // Mirror `CloudSyncStorage`: the `{publish_id}.db` sibling is expected and
            // listed via its meta (skip silently); a meta with a non-numeric publish
            // id, or any other object under this author's prefix, is surfaced rather
            // than dropped.
            match rest.strip_suffix("_meta.json") {
                Some(id_str) => match id_str.parse::<u64>() {
                    Ok(publish_id) => publish_ids.push(publish_id),
                    Err(e) => {
                        tracing::warn!(
                            "snapshot listing: meta key {key} has a non-numeric publish id: {e}"
                        )
                    }
                },
                None if rest.ends_with(".db") => {}
                None => tracing::warn!(
                    "snapshot listing: unexpected object {key} under a snapshot author prefix"
                ),
            }
        }
        publish_ids.sort_unstable();
        Ok(publish_ids)
    }

    async fn delete_snapshot_generation(
        &self,
        author: &str,
        publish_id: u64,
    ) -> Result<(), StorageError> {
        // Remove the db first, the meta last: the meta is what `list` keys a
        // generation by, so a crash between the two leaves it still listed (and
        // re-deletable), never a meta-less db.
        let mut objects = self.snapshot_objects.lock().unwrap();
        objects.remove(&format!("snapshot/{author}/{publish_id}.db"));
        objects.remove(&format!("snapshot/{author}/{publish_id}_meta.json"));
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
    ) -> Result<crate::storage::cloud::RevokeOutcome, CloudHomeError> {
        Ok(crate::storage::cloud::RevokeOutcome::Unsupported)
    }
}

/// Pull into `db` the way production does: `pull_changes` applies each incoming
/// changeset with a plain `call` (never journaled), so applied rows aren't
/// recorded as a local change, while a host write during the pull journals
/// normally. Returns the updated cursors and the pull result.
pub async fn pull_into(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    store_dir: &crate::store_dir::StoreDir,
) -> (HashMap<String, u64>, crate::sync::pull::PullResult) {
    let membership = crate::sync::pull::load_cycle_membership(storage, db)
        .await
        .expect("load cycle membership");
    pull_changes(
        db,
        db.synced_tables(),
        storage,
        device_id,
        store_dir,
        membership.chain,
        membership.pinned_owner,
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
    store_dir: &crate::store_dir::StoreDir,
) -> Result<(HashMap<String, u64>, crate::sync::pull::PullResult), crate::sync::pull::PullError> {
    let membership = crate::sync::pull::load_cycle_membership(storage, db).await?;
    pull_changes(
        db,
        db.synced_tables(),
        storage,
        device_id,
        store_dir,
        membership.chain,
        membership.pinned_owner,
    )
    .await
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
}

pub async fn publish_test_founder(
    storage: &dyn SyncStorage,
    owner: &UserKeypair,
    timestamp: &str,
) -> Result<(), String> {
    crate::sync::membership_ops::write_founder_entry(storage, owner, timestamp).await
}
