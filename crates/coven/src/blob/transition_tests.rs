//! Tests for the coven-owned make-Remote / make-Local transitions.
//!
//! These drive the real transition owners (`LocalBlobTransitions`,
//! `BlobTransitionJournal`, and `ConnectedBlobTransitions`) and the upload
//! drain's completion flip against a real [`Database`] and a [`TestStore`] that serves as both
//! the sync storage and the cloud home. A `Plaintext` cipher + `Plain` blob-path
//! scheme keep what the drain writes and what a read fetches byte-identical through
//! the mock, so a blob round-trips as plaintext across devices.
//!
//! The synthetic schema stands in for a release: `notes` is the gated root (a
//! release), `note_photos` is its user-provided blob-bearing child (a release file),
//! and `note_covers` is its host-provided asset child (a cover). Making a note
//! Remote uploads its blobs and flips `shared` on; making it Local materializes them
//! back and flips it off (the gate retract removes the subtree from peers).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use tokio::sync::watch;

use crate::blob::transition::BlobTransitionJournal;
use crate::blob::{BlobTransitionObserver, CacheFill, Provenance, RowBlobRef};
use crate::clock::SystemClock;
use crate::database::Database;
use crate::database::StoreDatabase;
use crate::keys::UserKeypair;
use crate::migration::Migration;
use crate::protocol::store_commit::ObjectHash;
use crate::storage::cloud::CloudHome;
use crate::storage::SyncStorage;
use crate::storage::{CloudCipher, PendingRotation};
use crate::store_dir::StoreDir;
use crate::sync::cycle::{run_single_sync_cycle, SyncCycleResult};
use crate::sync::hlc::Hlc;
use crate::sync::session::{BlobDecl, RowIdentity, SyncedTable};
use crate::sync::test_helpers::{
    exact_tombstone_key, host_exec as exec, open_test_db, open_test_db_schema,
    open_test_db_with_blob, open_test_db_with_user_and_host_blobs, plaintext_cipher, query_text,
    register_external_blob, remote_root_db, row_exists, temp_store_dir, TestStore,
};

async fn make_remote(
    db: &Database,
    store_dir: &StoreDir,
    root_table: &str,
    root_id: &str,
    pin: bool,
) -> Result<(), crate::blob::transition::MakeRemoteError> {
    crate::sync::test_owner_graph::TestOwnerGraph::new(StoreDatabase::new(db), store_dir.clone())
        .local_transitions()
        .make_remote(root_table, root_id, pin)
        .await
}

async fn read_blob(
    db: &Database,
    store_dir: &StoreDir,
    storage: Option<std::sync::Arc<dyn SyncStorage>>,
    reference: &RowBlobRef,
) -> Result<Vec<u8>, crate::sync::BlobCacheError> {
    crate::sync::test_owner_graph::TestOwnerGraph::new(StoreDatabase::new(db), store_dir.clone())
        .blob_access(storage)
        .read(reference)
        .await
}

#[allow(clippy::too_many_arguments)]
async fn make_local(
    db: &Database,
    storage: std::sync::Arc<dyn SyncStorage>,
    store_dir: &StoreDir,
    routing_encryption: Option<crate::encryption::EncryptionService>,
    observer: Option<Arc<dyn BlobTransitionObserver>>,
    root_table: &str,
    root_id: &str,
    dest: &HashMap<String, PathBuf>,
    cancel: &watch::Receiver<bool>,
) -> Result<(), crate::blob::transition::MakeLocalError> {
    crate::sync::test_owner_graph::TestOwnerGraph::new(StoreDatabase::new(db), store_dir.clone())
        .connected_blob_transitions(storage, routing_encryption, observer)
        .make_local(root_table, root_id, dest, cancel)
        .await
}

async fn cancel_make_remote(
    database: &StoreDatabase,
    root_table: &str,
    root_id: &str,
) -> Result<(), crate::blob::transition::MakeRemoteError> {
    BlobTransitionJournal::new(database.clone())
        .cancel_make_remote(root_table, root_id)
        .await
}

async fn drain_uploads(
    db: &Database,
    storage: &TestStore,
    store_dir: &StoreDir,
    clock: &dyn crate::clock::Clock,
    hlc: &Hlc,
    routing_encryption: Option<&crate::encryption::EncryptionService>,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<crate::blob::upload::DrainOutcome, crate::database::DbError> {
    let store = storage
        .bind_device(db, &storage.signer)
        .await
        .map_err(crate::database::DbError::Message)?;
    store
        .authorize_writer()
        .await
        .map_err(|error| crate::database::DbError::Message(error.to_string()))?
        .drain_uploads(store_dir, clock, hlc, routing_encryption, observer)
        .await
}

fn exact_cache_path(store_dir: &StoreDir, reference: &RowBlobRef) -> PathBuf {
    let stored = reference.stored().expect("Remote row has exact storage");
    store_dir
        .cache_blob_path(
            stored.locator().namespace(),
            stored.locator().locator_hash(),
        )
        .expect("build exact locator cache path")
}

fn exact_pinned_path(store_dir: &StoreDir, reference: &RowBlobRef) -> PathBuf {
    let stored = reference.stored().expect("Remote row has exact storage");
    store_dir
        .pinned_blob_path(
            stored.locator().namespace(),
            stored.locator().locator_hash(),
        )
        .expect("build exact locator pinned path")
}

/// The blob declaration for `note_photos`: a release file — user-provided ·
/// `CacheLazy`, keyed by the readable cloud path (browsable home), master-scoped.
fn photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::UserProvided, CacheFill::CacheLazy)
        .with_cloud_path_column("cloud_path")
}

/// The blob declaration for `note_covers`: a host-provided · `CacheEager` asset,
/// keyed by the readable cloud path, master-scoped.
fn cover_decl() -> BlobDecl {
    BlobDecl::new("covers", Provenance::HostProvided, CacheFill::CacheEager)
        .with_cloud_path_column("cloud_path")
}

/// A host-provided cover declared `CacheLazy`, so a make_remote WITHOUT a pin drops
/// its local-store copy (rather than moving it into the eager cache like `cover_decl`).
fn cover_lazy_decl() -> BlobDecl {
    BlobDecl::new("covers", Provenance::HostProvided, CacheFill::CacheLazy)
        .with_cloud_path_column("cloud_path")
}

fn scoped_blob_transition_db() -> Database {
    open_test_db_schema(
        vec![
            SyncedTable::new("notes", RowIdentity::SharedKey).gated_by("shared"),
            SyncedTable::new("note_tags", RowIdentity::SharedKey),
            SyncedTable::new("note_photos", RowIdentity::SharedKey).carries_blob(photo_decl()),
            SyncedTable::new("note_covers", RowIdentity::SharedKey).carries_blob(cover_decl()),
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience"),
        ],
        vec![Migration::run(1, "scoped-blob-transition", |conn| {
            crate::sync::test_helpers::create_synced_schema(conn)?;
            conn.execute_batch(
                "CREATE TABLE accounts (
                    id TEXT PRIMARY KEY,
                    audience TEXT,
                    _updated_at TEXT NOT NULL
                ) STRICT;",
            )
            .map_err(crate::database::DbError::from)
        })],
    )
}

async fn create_store(db: &Database, signer: UserKeypair) -> TestStore {
    TestStore::create(db, "test-store", signer)
        .await
        .expect("create exact test Store for the test database")
}

async fn invite_and_activate_peer(
    storage: &TestStore,
    observer_db: &Database,
    peer_db: &Database,
    peer: &UserKeypair,
) -> crate::sync::test_helpers::TestDevice {
    storage
        .invite_member(
            observer_db,
            &storage.signer,
            &Hlc::new(
                "peer-invitation".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
            ),
            &crate::sync::test_helpers::pubkey_hex(peer),
            None,
            crate::protocol::membership::MemberRole::Member,
            &crate::encryption::EncryptionService::from_key([42; 32]),
            "Test Store",
        )
        .await
        .expect("invite peer identity");
    storage
        .activate_joined_device(observer_db, peer_db, peer, "2026-07-16T00:00:00Z")
        .await
        .expect("activate peer Store device");
    storage
        .bind_device(peer_db, peer)
        .await
        .expect("bind activated peer Store device")
}

async fn photo_ref(db: &Database, id: &str) -> RowBlobRef {
    db.row_blob_ref("note_photos", id)
        .await
        .expect("load exact photo row blob reference")
}

async fn cover_ref(db: &Database, id: &str) -> RowBlobRef {
    db.row_blob_ref("note_covers", id)
        .await
        .expect("load exact cover row blob reference")
}

async fn remote_blob_exists(storage: &TestStore, reference: &RowBlobRef) -> bool {
    match reference.stored() {
        Some(stored) => storage.verify_blob_object(stored).await.is_ok(),
        None => false,
    }
}

async fn external_blob(
    db: &Database,
    table: &str,
    row_id: &str,
) -> Option<crate::database::ExternalBlob> {
    let blobs = crate::database::StoreDatabase::new(db);
    let reference = blobs
        .row_blob_ref(table, row_id)
        .await
        .expect("load row blob reference for external ownership");
    blobs
        .external_blob_for_row(&reference)
        .await
        .expect("load exact external blob ownership")
}

/// Records the transition observer's completion + materialize callbacks.
#[derive(Default)]
struct Recorder {
    made_remote: Mutex<Vec<(String, String)>>,
    made_local: Mutex<Vec<(String, String)>>,
    materialized: Mutex<Vec<(String, u64, u64)>>,
}

#[async_trait]
impl BlobTransitionObserver for Recorder {
    async fn on_blob_upload_started(&self, _blob_id: &str) {}
    async fn on_blob_uploaded(&self, _blob_id: &str) {}
    async fn on_blob_upload_failed(&self, _blob_id: &str, _error: &str) {}
    async fn on_root_made_remote(&self, root_table: &str, root_id: &str) {
        self.made_remote
            .lock()
            .unwrap()
            .push((root_table.to_string(), root_id.to_string()));
    }
    async fn on_root_made_local(&self, root_table: &str, root_id: &str) {
        self.made_local
            .lock()
            .unwrap()
            .push((root_table.to_string(), root_id.to_string()));
    }
    async fn on_blob_materialize_progress(
        &self,
        _root_table: &str,
        _root_id: &str,
        blob_id: &str,
        done: u64,
        total: u64,
    ) {
        self.materialized
            .lock()
            .unwrap()
            .push((blob_id.to_string(), done, total));
    }
}

/// Run one real sync cycle for `device`, with the mock wired as both storage and
/// cloud home so the upload drain, the gate, the tombstone drain, and the GC all run.
#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    storage: &TestStore,
    _device: &str,
    hlc: &Hlc,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    kp: &UserKeypair,
    lib: &StoreDir,
    observer: Option<&dyn BlobTransitionObserver>,
) -> SyncCycleResult {
    storage.open_into(db).await.expect("open exact test Store");
    let store_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read exact local Store device")
        .expect("test database has an activated Store device");
    // A fresh gate each call: none of these transition tests exercise a rotation
    // this device can't adopt.
    let pending_rotation = PendingRotation::none();
    let result = run_single_sync_cycle(
        storage.storage.clone(),
        &store_device_id,
        hlc,
        &SystemClock,
        db,
        cipher,
        &pending_rotation,
        kp,
        None,
        lib,
        Some(storage.home.as_ref()),
        observer,
    )
    .await
    .expect("cycle");
    result
}

/// Like [`run_cycle`] but surfaces the cycle result instead of unwrapping it, so a
/// test can drive a cycle expected to fail (e.g. a pull rejected by a schema floor).
#[allow(clippy::too_many_arguments)]
async fn try_run_cycle(
    storage: &TestStore,
    _device: &str,
    hlc: &Hlc,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    kp: &UserKeypair,
    lib: &StoreDir,
) -> Result<SyncCycleResult, String> {
    storage.open_into(db).await.expect("open exact test Store");
    let store_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "test database has no activated Store device".to_string())?;
    let pending_rotation = PendingRotation::none();
    run_single_sync_cycle(
        storage.storage.clone(),
        &store_device_id,
        hlc,
        &SystemClock,
        db,
        cipher,
        &pending_rotation,
        kp,
        None,
        lib,
        Some(storage.home.as_ref()),
        None,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Insert the gated note + its blob-bearing photo row, `shared` (Remote) or not,
/// carrying `bytes`'s length and content hash so a peer that later downloads the
/// blob verifies it. The two seeders below differ only in this flag and where the
/// blob's bytes live.
async fn seed_release_rows(
    db: &Database,
    note_id: &str,
    photo_id: &str,
    cloud_path: &str,
    shared: u8,
    bytes: &[u8],
) {
    crate::sync::test_helpers::exec(
        db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('{note_id}', 'Release', NULL, {shared}, '0000000001000-0000-A', '2026-01-01')"
        ),
    )
    .await;
    crate::sync::test_helpers::exec(
        db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('{photo_id}', '{note_id}', 'image', {}, '{}', '0000000001000-0000-A', '2026-01-01', '{cloud_path}')",
            bytes.len(),
            crate::blob::content_hash(bytes),
        ),
    )
    .await;
}

/// Insert a Local release: a gated-off note plus a blob-bearing photo with an
/// external source file registered for it. Returns the external source path.
async fn seed_local_release(
    db: &Database,
    user_dir: &std::path::Path,
    note_id: &str,
    photo_id: &str,
    cloud_path: &str,
    bytes: &[u8],
) -> PathBuf {
    seed_release_rows(db, note_id, photo_id, cloud_path, 0, bytes).await;
    std::fs::create_dir_all(user_dir).unwrap();
    let src = user_dir.join(format!("{photo_id}.jpg"));
    std::fs::write(&src, bytes).unwrap();
    register_external_blob(db, "note_photos", photo_id, &src).await;
    src
}

/// Add a second blob-bearing photo (with its external source registered) to a
/// release already seeded by [`seed_local_release`] — for the multi-blob tests.
/// Returns the external source path.
async fn add_local_photo(
    db: &Database,
    user_dir: &std::path::Path,
    note_id: &str,
    photo_id: &str,
    cloud_path: &str,
    bytes: &[u8],
) -> PathBuf {
    exec(
        db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('{photo_id}', '{note_id}', 'image', {}, '{}', '0000000001000-0000-A', '2026-01-01', '{cloud_path}')",
            bytes.len(),
            crate::blob::content_hash(bytes),
        ),
    )
    .await;
    let src = user_dir.join(format!("{photo_id}.jpg"));
    std::fs::write(&src, bytes).unwrap();
    register_external_blob(db, "note_photos", photo_id, &src).await;
    src
}

/// Insert a Remote release: a gated-on note plus a photo whose blob is already in
/// the cloud (plaintext, at the readable key the `Plain` scheme derives).
async fn seed_remote_release(
    storage: &TestStore,
    db: &Database,
    store_dir: &StoreDir,
    hlc: &Hlc,
    routing_encryption: Option<&crate::encryption::EncryptionService>,
    note_id: &str,
    photo_id: &str,
    cloud_path: &str,
    bytes: &[u8],
) {
    seed_release_rows(db, note_id, photo_id, cloud_path, 0, bytes).await;
    crate::store_dir::StoreDir::store_local_blob(store_dir, "fixture_sources", photo_id, bytes)
        .await
        .expect("write exact remote fixture source");
    let source = store_dir
        .local_blob_path("fixture_sources", photo_id)
        .expect("build exact remote fixture source path");
    register_external_blob(db, "note_photos", photo_id, &source).await;
    storage.open_into(db).await.expect("open exact test Store");
    make_remote(db, store_dir, "notes", note_id, false)
        .await
        .expect("queue exact remote fixture upload");
    let outcome = drain_uploads(
        db,
        storage,
        store_dir,
        &SystemClock,
        hlc,
        routing_encryption,
        None,
    )
    .await
    .expect("create exact remote fixture blob");
    assert_eq!(outcome.uploaded, 1);
    assert!(outcome.yielded_for_publish);
    assert!(
        storage
            .publish_pending(db, store_dir)
            .await
            .expect("publish exact remote fixture"),
        "remote fixture publishes its Store write",
    );
}

async fn shared_flag(db: &Database, note_id: &str) -> i64 {
    let v = query_text(
        db,
        &format!("SELECT CAST(shared AS TEXT) FROM notes WHERE id = '{note_id}'"),
    )
    .await;
    v.parse().unwrap()
}

/// The note's gate stamp (`_updated_at`), to prove a refused transition leaves the
/// gate row — value and causal stamp — untouched.
async fn gate_stamp(db: &Database, note_id: &str) -> String {
    query_text(
        db,
        &format!("SELECT _updated_at FROM notes WHERE id = '{note_id}'"),
    )
    .await
}

async fn pending_uploads(db: &Database) -> usize {
    db.get_pending_cloud_uploads().await.unwrap().len()
}

async fn created_upload_blob(db: &Database, blob_id: &str) -> crate::blob::locator::StoredBlobRef {
    db.get_pending_cloud_uploads()
        .await
        .expect("load exact upload journals")
        .into_iter()
        .find_map(|entry| match entry.operation {
            crate::database::OutboxOperation::Upload {
                row,
                state: crate::database::OutboxUploadState::Created { stored, .. },
                ..
            } if row.blob().id == blob_id => Some(stored),
            crate::database::OutboxOperation::Upload { .. }
            | crate::database::OutboxOperation::Delete { .. } => None,
        })
        .expect("blob has a Created exact upload journal")
}

async fn pending_deletes(db: &Database) -> Vec<String> {
    db.get_pending_cloud_deletes()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| match entry.operation {
            crate::database::OutboxOperation::Delete { stored } => {
                stored.locator().blob_id().to_string()
            }
            crate::database::OutboxOperation::Upload { .. } => {
                panic!("pending delete query returned an upload")
            }
        })
        .collect()
}

async fn has_intent(db: &Database, root_table: &str, root_id: &str) -> bool {
    let (rt, ri) = (root_table.to_string(), root_id.to_string());
    db.call(move |conn| Database::make_remote_intent_exists(conn, &rt, &ri))
        .await
        .unwrap()
}

async fn scoped_store_state(db: &Database) -> [i64; 4] {
    let mut counts = [0; 4];
    for (index, table) in [
        "store_writes",
        "store_write_partitions",
        "_coven_row_routes",
        "_coven_audience",
    ]
    .into_iter()
    .enumerate()
    {
        counts[index] = query_text(db, &format!("SELECT CAST(COUNT(*) AS TEXT) FROM {table}"))
            .await
            .parse()
            .expect("scoped Store table count is an integer");
    }
    counts
}

async fn assert_scoped_flip_journaled_atomically(
    db: &Database,
    expected_changes: &[(&str, crate::changeset::ChangeOp)],
) {
    assert!(
        row_exists(
            db,
            "SELECT 1 FROM store_write_partitions AS partition \
             JOIN store_writes AS write USING (write_id) \
             WHERE partition.audience = 'store'",
        )
        .await,
        "the audience partition and its parent write commit together",
    );
    assert!(
        !row_exists(db, "SELECT 1 FROM _coven_row_routes LIMIT 1").await
            && !row_exists(db, "SELECT 1 FROM _coven_audience LIMIT 1").await,
        "a boolean-gated Store row does not invent scoped row routes or mirrors",
    );
    let changesets = db
        .call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT partition.changeset
                     FROM store_write_partitions AS partition
                     JOIN store_writes AS write USING (write_id)
                     WHERE partition.audience = 'store'
                     ORDER BY write.ordinal DESC",
                )
                .map_err(crate::database::DbError::from)?;
            let changesets = statement
                .query_map([], |row| row.get::<_, Vec<u8>>(0))
                .map_err(crate::database::DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(crate::database::DbError::from)?;
            Ok(changesets)
        })
        .await
        .expect("read routed Store partitions");
    let all_changes = changesets
        .iter()
        .map(|changeset| {
            crate::database::walk_changeset(changeset).expect("walk routed Store partition")
        })
        .collect::<Vec<_>>();
    let changes = all_changes
        .into_iter()
        .find(|changes| {
            expected_changes.iter().all(|(table, op)| {
                changes
                    .iter()
                    .any(|change| change.table == *table && change.op == *op)
            })
        })
        .unwrap_or_else(|| {
            panic!("no Store partition contains the expected changes {expected_changes:?}")
        });
    let mut tables: Vec<String> = changes.into_iter().map(|row| row.table).collect();
    tables.sort();
    tables.dedup();
    assert!(
        tables.iter().any(|table| table == "notes"),
        "the partition contains the flipped root; tables were {tables:?}",
    );
    assert!(
        !tables
            .iter()
            .any(|table| matches!(table.as_str(), "_coven_audience" | "_coven_row_routes")),
        "the Store partition contains no unrelated scoped routing rows: {tables:?}",
    );
}

/// Insert a `published_blob_drop_intents` row directly, to reconstruct the durable
/// bookkeeping a crash leaves when a drain applies a disposition but dies before
/// clearing its intent.
async fn insert_published_drop_intent(
    db: &Database,
    seq: i64,
    namespace: &str,
    blob_id: &str,
    bytes: &[u8],
    locator_hash: ObjectHash,
    disposition: &str,
) {
    let (ns, id, disp) = (
        namespace.to_string(),
        blob_id.to_string(),
        disposition.to_string(),
    );
    let size = bytes.len() as i64;
    let plaintext_hash = ObjectHash::digest(bytes).to_string();
    let locator_hash = locator_hash.to_string();
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO published_blob_drop_intents \
             (seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![seq, ns, id, size, plaintext_hash, locator_hash, disp],
        )
        .map(|_| ())
        .map_err(crate::database::DbError::from)
    })
    .await
    .expect("insert published blob drop intent");
}

#[tokio::test]
async fn published_drop_intents_preserve_distinct_locators_for_one_logical_id() {
    let db = open_test_db();
    let first = ObjectHash::digest(b"first locator");
    let second = ObjectHash::digest(b"second locator");

    insert_published_drop_intent(&db, 1, "covers", "shared-id", b"first", first, "cache").await;
    insert_published_drop_intent(&db, 1, "covers", "shared-id", b"second", second, "pin").await;

    let count = db
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM published_blob_drop_intents
                 WHERE seq = 1 AND namespace = 'covers' AND blob_id = 'shared-id'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("count exact drop intents");
    assert_eq!(count, 2);
}

async fn drop_intent_present(db: &Database, blob_id: &str) -> bool {
    row_exists(
        db,
        &format!("SELECT 1 FROM published_blob_drop_intents WHERE blob_id = '{blob_id}'"),
    )
    .await
}

async fn publish_fixture_position(
    storage: &TestStore,
    db: &Database,
    store_dir: &StoreDir,
    note_id: &str,
) -> u64 {
    exec(
        db,
        &format!(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('{note_id}', 'fixture position', 1, '0000000001000-0000-A', '2026-01-01')"
        ),
    )
    .await;
    assert!(storage
        .publish_pending(db, store_dir)
        .await
        .expect("publish fixture Store position"));
    storage
        .founder_device()
        .await
        .expect("retain fixture Store device")
        .latest_local_store_position()
        .await
        .expect("read fixture Store position")
        .expect("fixture Store write has an exact position")
        .coord
        .sequence()
}

// ===========================================================================
// Multi-device make_remote / make_local
// ===========================================================================

/// A imports a Local release and makes it Remote. Device B receives the subtree
/// ONLY after the blob is up (the gate stays off until the flip), A keeps the blob
/// pinned and the external source left in place, and B fetches the CacheLazy blob on read.
#[tokio::test]
async fn multi_device_make_remote_publishes_only_after_blobs_are_up() {
    let kp_a = UserKeypair::generate();
    let db_a = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db_a, kp_a.clone()).await;
    let enc = plaintext_cipher();
    let hlc_a = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp_a, lib_a) = temp_store_dir();
    let bytes = b"PHOTO-BYTES-one-file".to_vec();

    let src = seed_local_release(
        &db_a,
        &tmp_a.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;

    // A cycle while the note is gated off: nothing reaches a peer.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;
    let db_b = open_test_db_with_blob(photo_decl());
    let (_tmp_b, lib_b) = temp_store_dir();
    let kp_b = UserKeypair::generate();
    let peer = invite_and_activate_peer(&storage, &db_a, &db_b, &kp_b).await;
    peer.pull_store(&lib_b).await.expect("pull peer Store");
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a gated-off (Local) release does not reach a peer",
    );

    // A makes it Remote: enqueue the upload + intent, then the next cycle's drain
    // uploads the blob and flips the gate.
    make_remote(&db_a, &lib_a, "notes", "n1", true)
        .await
        .expect("make_remote");
    let recorder = Recorder::default();
    let result = run_cycle(
        &storage,
        "A",
        &hlc_a,
        &db_a,
        &enc,
        &kp_a,
        &lib_a,
        Some(&recorder),
    )
    .await;

    // The flip completed this cycle: the gate is on, the intent is gone, the
    // external ref is dropped, the user's source file is left in place, the blob
    // is pinned, and the drain broke to publish.
    assert_eq!(shared_flag(&db_a, "n1").await, 1, "the release is Remote");
    assert!(
        !has_intent(&db_a, "notes", "n1").await,
        "the intent is cleared"
    );
    assert!(
        external_blob(&db_a, "note_photos", "photoaaa")
            .await
            .is_none(),
        "the external ref is dropped on completion",
    );
    assert!(
        src.exists(),
        "the user-provided source file is left in place post-commit"
    );
    let pinned = exact_pinned_path(&lib_a, &photo_ref(&db_a, "photoaaa").await);
    assert_eq!(
        std::fs::read(&pinned).unwrap(),
        bytes,
        "A keeps the Remote blob pinned (plaintext)",
    );
    assert!(
        result.resume_drain_promptly,
        "completing a make_remote breaks the drain so the cycle publishes the subtree",
    );
    assert_eq!(
        *recorder.made_remote.lock().unwrap(),
        vec![("notes".to_string(), "n1".to_string())],
        "on_root_made_remote fires for the completed make_remote",
    );

    // B pulls and now gets the subtree, and fetches the CacheLazy blob on read.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;
    peer.pull_store(&lib_b).await.expect("pull peer Store");
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B receives the release once its blobs are up and the gate flips",
    );
    let fetched = read_blob(
        &db_b,
        &lib_b,
        Some(storage.storage.clone()),
        &photo_ref(&db_b, "photoaaa").await,
    )
    .await
    .expect("B fetches the CacheLazy blob");
    assert_eq!(fetched, bytes, "B reads the original photo from the cloud");
}

/// The mutable upload facts attached to the exact pending Local row version.
async fn pending_upload_state(db: &Database, id: &str) -> (PathBuf, bool) {
    let expected = db
        .row_blob_ref("note_photos", id)
        .await
        .expect("load exact pending upload row");
    let mut matching = db
        .get_pending_cloud_uploads()
        .await
        .expect("load pending exact row uploads")
        .into_iter()
        .filter_map(|entry| match entry.operation {
            crate::database::OutboxOperation::Upload {
                row,
                source_path,
                retain_pinned,
                ..
            } if row == expected => Some((source_path, retain_pinned)),
            crate::database::OutboxOperation::Upload { .. }
            | crate::database::OutboxOperation::Delete { .. } => None,
        });
    let state = matching.next().expect("exact row has a pending upload");
    assert!(
        matching.next().is_none(),
        "an exact Local row version owns at most one pending upload"
    );
    state
}

/// A second make_remote on the same still-Local root, before any cycle drains the
/// first one's queued upload, must carry its new pin choice through to the queued
/// blob: the enqueue upserts the row's `retain_pinned` rather than leaving the stale
/// value, so the drained upload pins.
#[tokio::test]
async fn re_enqueue_updates_the_pending_upload_pin() {
    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let enc = plaintext_cipher();
    let kp = storage.protocol_founder_keypair();
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"PHOTO-repin".to_vec();

    seed_local_release(
        &db,
        &tmp.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;

    make_remote(&db, &lib, "notes", "n1", false)
        .await
        .expect("make_remote pin=false");
    assert!(
        !pending_upload_state(&db, "photoaaa").await.1,
        "the first make_remote queued the upload unpinned",
    );

    // A second make_remote with a pin, before the upload drains.
    make_remote(&db, &lib, "notes", "n1", true)
        .await
        .expect("make_remote pin=true");
    assert!(
        pending_upload_state(&db, "photoaaa").await.1,
        "the re-enqueue must update the queued upload's pin to the new call's value",
    );

    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert_eq!(shared_flag(&db, "n1").await, 1, "the release is Remote");
    assert!(
        exact_pinned_path(&lib, &photo_ref(&db, "photoaaa").await).exists(),
        "the drained upload pins, honoring the second make_remote's choice",
    );
}

/// Re-registering a blob's external source before the upload drains, then a second
/// make_remote, must repoint the queued upload at the new path: the enqueue upserts
/// `source_path`, so the drain reads the current file rather than the stale (here
/// removed) one it would otherwise retry forever.
#[tokio::test]
async fn re_enqueue_updates_the_pending_upload_source_path() {
    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let enc = plaintext_cipher();
    let kp = storage.protocol_founder_keypair();
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"PHOTO-relocate".to_vec();
    let user_dir = tmp.path().join("user");

    let src1 =
        seed_local_release(&db, &user_dir, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes).await;
    make_remote(&db, &lib, "notes", "n1", false)
        .await
        .expect("first make_remote");
    assert_eq!(
        pending_upload_state(&db, "photoaaa").await.0,
        src1,
        "the upload is queued against the original source",
    );

    // The user moves the file: re-register it at a new path and remove the old one.
    let src2 = user_dir.join("relocated.jpg");
    std::fs::write(&src2, &bytes).unwrap();
    register_external_blob(&db, "note_photos", "photoaaa", &src2).await;
    std::fs::remove_file(&src1).unwrap();

    make_remote(&db, &lib, "notes", "n1", false)
        .await
        .expect("second make_remote");
    assert_eq!(
        pending_upload_state(&db, "photoaaa").await.0,
        src2,
        "the re-enqueue repoints the queued upload at the new source",
    );

    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert_eq!(
        shared_flag(&db, "n1").await,
        1,
        "the drain read the re-registered path and completed the make_remote",
    );
    assert!(
        remote_blob_exists(&storage, &photo_ref(&db, "photoaaa").await).await,
        "the blob uploaded from the new path",
    );
}

#[tokio::test]
async fn cancel_make_remote_after_completion_enqueues_no_deletes() {
    let db = open_test_db_with_blob(photo_decl());
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(&db, UserKeypair::generate()).await;
    let enc = plaintext_cipher();
    let kp = storage.protocol_founder_keypair();
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"PHOTO-BYTES-completed-remote".to_vec();

    seed_local_release(
        &db,
        &tmp.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;
    make_remote(&db, &lib, "notes", "n1", false)
        .await
        .expect("make_remote");
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert_eq!(shared_flag(&db, "n1").await, 1, "the root is Remote");
    assert!(
        !has_intent(&db, "notes", "n1").await,
        "completion deleted the make_remote intent",
    );
    let remote = photo_ref(&db, "photoaaa").await;
    storage
        .verify_blob_object(
            remote
                .stored()
                .expect("Remote row has exact blob authority"),
        )
        .await
        .expect("the published blob exists in cloud");

    cancel_make_remote(&store_database, "notes", "n1")
        .await
        .expect_err("a completed make_remote has no transition left to cancel");

    assert!(
        pending_deletes(&db).await.is_empty(),
        "a cancel racing after completion must not tombstone published blobs",
    );
    storage
        .verify_blob_object(
            remote
                .stored()
                .expect("Remote row keeps exact blob authority"),
        )
        .await
        .expect("the cloud blob remains present");
}

/// A makes a Remote release Local. B's subtree is DELETEd (gate retract) and the
/// cloud blob is tombstoned, while A keeps the external file and reads from it.
#[tokio::test]
async fn multi_device_make_local_retracts_peer_and_tombstones_cloud() {
    tokio::spawn(async {
        let kp_a = UserKeypair::generate();
        let db_a = open_test_db_with_blob(photo_decl());
        let storage = create_store(&db_a, kp_a.clone()).await;
        let enc = plaintext_cipher();
        let kp_b = UserKeypair::generate();
        let hlc_a = Hlc::new(
            "A".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
        );
        let hlc_b = Hlc::new(
            "B".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
        );
        let db_b = open_test_db_with_blob(photo_decl());
        let (tmp_a, lib_a) = temp_store_dir();
        let (_tmp_b, lib_b) = temp_store_dir();
        let bytes = b"MANAGED-PHOTO-going-back-local".to_vec();

        let peer = Box::pin(invite_and_activate_peer(&storage, &db_a, &db_b, &kp_b)).await;
        Box::pin(seed_remote_release(
            &storage,
            &db_a,
            &lib_a,
            &hlc_a,
            None,
            "n1",
            "photoaaa",
            "cv/photoaaa.jpg",
            &bytes,
        ))
        .await;
        // B is a registered reader that has not acknowledged A yet, so A's snapshot
        // cycle keeps A's release changeset for B to pull — reclamation is paused until
        // every current device acks. Without a peer head A would be single-device and
        // the snapshot-covered changeset would be reclaimed, leaving nothing for B's
        // incremental pull.
        // A pushes the Remote release; B pulls it.
        Box::pin(run_cycle(
            &storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None,
        ))
        .await;
        peer.pull_store(&lib_b).await.expect("pull peer Store");
        assert!(
            row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
            "B has the Remote release",
        );
        let remote_blob = photo_ref(&db_a, "photoaaa")
            .await
            .stored()
            .cloned()
            .expect("Remote photo has exact storage authority");

        // A makes it Local to a chosen folder.
        let dest_dir = tmp_a.path().join("dest");
        let dest_path = dest_dir.join("photoaaa.jpg");
        let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
        let (_cancel_tx, cancel) = watch::channel(false);
        let recorder = Arc::new(Recorder::default());
        let reads_before_make_local = storage.home.exact_stream_read_count();
        Box::pin(make_local(
            &db_a,
            storage.storage.clone(),
            &lib_a,
            None,
            Some(recorder.clone()),
            "notes",
            "n1",
            &dest,
            &cancel,
        ))
        .await
        .expect("make_local");

        assert_eq!(
            storage.home.exact_stream_read_count(),
            reads_before_make_local + 1,
            "make_local materializes the Remote blob through the file download path",
        );
        assert_eq!(shared_flag(&db_a, "n1").await, 0, "A's release is Local");
        assert_eq!(
            std::fs::read(&dest_path).unwrap(),
            bytes,
            "the file is materialized to the chosen folder",
        );
        assert_eq!(
            external_blob(&db_a, "note_photos", "photoaaa")
                .await
                .expect("materialized photo has external ownership")
                .path,
            dest_path,
            "A now reads the blob from the external file",
        );
        assert_eq!(
            pending_deletes(&db_a).await,
            vec!["photoaaa".to_string()],
            "the cloud blob's delete is enqueued in the same commit as the flip",
        );
        assert_eq!(
            *recorder.made_local.lock().unwrap(),
            vec![("notes".to_string(), "n1".to_string())],
        );
        assert_eq!(
            *recorder.materialized.lock().unwrap(),
            vec![("photoaaa".to_string(), 1, 1)],
            "materialize progress reported once for the single blob",
        );

        // A's retract cycle: the gate flip false emits DELETEs and the tombstone drain
        // writes the cloud tombstone.
        Box::pin(run_cycle(
            &storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None,
        ))
        .await;
        assert!(
            storage
                .home
                .exists(&exact_tombstone_key(&remote_blob))
                .await
                .unwrap(),
            "the cloud blob is tombstoned",
        );

        // B's next cycle pulls the retract: its subtree disappears.
        Box::pin(run_cycle(
            &storage, "B", &hlc_b, &db_b, &enc, &kp_b, &lib_b, None,
        ))
        .await;
        assert!(
            !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
            "B's subtree is removed by the gate retract",
        );

        // A still reads the photo from its external file (no cloud copy needed).
        let read = read_blob(
            &db_a,
            &lib_a,
            Some(storage.storage.clone()),
            &photo_ref(&db_a, "photoaaa").await,
        )
        .await
        .expect("A reads from its external file");
        assert_eq!(read, bytes, "A plays its own local file");
    })
    .await
    .expect("multi-device make-local task");
}

#[tokio::test]
async fn scoped_make_local_without_routing_encryption_mutates_nothing() {
    let db = scoped_blob_transition_db();
    let storage = create_store(&db, UserKeypair::generate()).await;
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    storage.open_into(&db).await.expect("open exact test Store");
    let (tmp, lib) = temp_store_dir();
    let bytes = b"scoped-managed-photo".to_vec();
    let routing_encryption = crate::encryption::EncryptionService::from_key([5; 32]);
    seed_remote_release(
        &storage,
        &db,
        &lib,
        &hlc,
        Some(&routing_encryption),
        "n-scoped",
        "photoscoped",
        "cv/photoscoped.jpg",
        &bytes,
    )
    .await;
    let store_state_before = scoped_store_state(&db).await;

    let gate_stamp_before = gate_stamp(&db, "n-scoped").await;
    let dest_dir = tmp.path().join("destination");
    let dest_path = dest_dir.join("photoscoped.jpg");
    let dest: HashMap<String, PathBuf> = [("photoscoped".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    let recorder = Arc::new(Recorder::default());

    let error = make_local(
        &db,
        storage.storage.clone(),
        &lib,
        None,
        Some(recorder.clone()),
        "notes",
        "n-scoped",
        &dest,
        &cancel,
    )
    .await
    .expect_err("a scoped transition requires routing encryption");

    assert!(
        error
            .to_string()
            .contains("requires the Store generation-1 routing key"),
        "the missing routing key is surfaced: {error}"
    );
    assert_eq!(
        storage.home.exact_stream_read_count(),
        0,
        "the transition must reject before reading or materializing the cloud blob",
    );
    assert!(
        !dest_dir.exists() && !dest_path.exists(),
        "the transition must reject before creating the host destination",
    );
    assert!(
        recorder.materialized.lock().unwrap().is_empty()
            && recorder.made_local.lock().unwrap().is_empty(),
        "the host observer sees no progress or completion",
    );
    assert_eq!(
        shared_flag(&db, "n-scoped").await,
        1,
        "the root remains Remote",
    );
    assert_eq!(
        gate_stamp(&db, "n-scoped").await,
        gate_stamp_before,
        "the gate and its causal stamp are untouched",
    );
    assert!(
        external_blob(&db, "note_photos", "photoscoped")
            .await
            .is_none(),
        "no external reference is registered",
    );
    assert!(
        pending_deletes(&db).await.is_empty(),
        "no cloud deletion is enqueued",
    );
    assert_eq!(scoped_store_state(&db).await, store_state_before);

    make_local(
        &db,
        storage.storage.clone(),
        &lib,
        Some(routing_encryption),
        Some(recorder.clone()),
        "notes",
        "n-scoped",
        &dest,
        &cancel,
    )
    .await
    .expect("retry scoped make_local with routing encryption");
    assert_eq!(storage.home.exact_stream_read_count(), 1);
    assert_eq!(std::fs::read(&dest_path).unwrap(), b"scoped-managed-photo");
    assert_eq!(shared_flag(&db, "n-scoped").await, 0);
    assert!(
        external_blob(&db, "note_photos", "photoscoped")
            .await
            .is_some(),
        "the successful flip registers the materialized external file",
    );
    assert_eq!(
        recorder.materialized.lock().unwrap().as_slice(),
        [("photoscoped".to_string(), 1, 1)]
    );
    assert_eq!(
        recorder.made_local.lock().unwrap().as_slice(),
        [("notes".to_string(), "n-scoped".to_string())],
    );
    assert_scoped_flip_journaled_atomically(
        &db,
        &[
            ("notes", crate::changeset::ChangeOp::Delete),
            ("note_photos", crate::changeset::ChangeOp::Delete),
        ],
    )
    .await;
}

#[tokio::test]
async fn scoped_user_upload_completion_without_routing_encryption_mutates_nothing() {
    let db = scoped_blob_transition_db();
    let storage = create_store(&db, UserKeypair::generate()).await;
    let exact_creates_before = storage.home.exact_create_count();
    storage.open_into(&db).await.expect("open exact test Store");
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"scoped-user-photo";
    let routing_encryption = crate::encryption::EncryptionService::from_key([7; 32]);
    crate::sync::test_helpers::exec(
        &db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n-user-scoped', 'Scoped user fixture', NULL, 0, \
                     '0000000001000-0000-A', '2026-01-01'); \
             INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('photo-user-scoped', 'n-user-scoped', 'image', {}, '{}', \
                     '0000000001000-0000-A', '2026-01-01', 'cv/photo-user-scoped.jpg');",
            bytes.len(),
            crate::blob::content_hash(bytes),
        ),
    )
    .await;
    let user_dir = tmp.path().join("user");
    std::fs::create_dir_all(&user_dir).unwrap();
    let source = user_dir.join("photo-user-scoped.jpg");
    std::fs::write(&source, bytes).unwrap();
    register_external_blob(&db, "note_photos", "photo-user-scoped", &source).await;
    make_remote(&db, &lib, "notes", "n-user-scoped", false)
        .await
        .expect("queue scoped user-provided make_remote");
    let stamp_before = gate_stamp(&db, "n-user-scoped").await;
    let store_state_before = scoped_store_state(&db).await;

    let error = match drain_uploads(&db, &storage, &lib, &SystemClock, &hlc, None, None).await {
        Ok(_) => panic!("a scoped upload completion requires routing encryption before upload"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("requires the Store generation-1 routing key"),
        "the missing routing key is surfaced: {error}",
    );
    assert_eq!(
        storage.home.exact_create_count(),
        exact_creates_before,
        "the blob is not uploaded before routing validation",
    );
    assert!(
        !storage
            .home
            .exists("photos/cv/photo-user-scoped.jpg")
            .await
            .unwrap(),
        "the cloud is untouched",
    );
    assert!(source.exists(), "the user-owned source remains in place");
    assert!(
        external_blob(&db, "note_photos", "photo-user-scoped")
            .await
            .is_some(),
        "the external reference remains registered",
    );
    assert_eq!(pending_uploads(&db).await, 1, "the upload remains queued");
    assert!(
        has_intent(&db, "notes", "n-user-scoped").await,
        "the transition intent remains queued",
    );
    assert_eq!(shared_flag(&db, "n-user-scoped").await, 0);
    assert_eq!(gate_stamp(&db, "n-user-scoped").await, stamp_before);
    assert_eq!(scoped_store_state(&db).await, store_state_before);

    let outcome = drain_uploads(
        &db,
        &storage,
        &lib,
        &SystemClock,
        &hlc,
        Some(&routing_encryption),
        None,
    )
    .await
    .expect("retry scoped upload completion with routing encryption");
    assert_eq!(outcome.uploaded, 1);
    assert!(outcome.yielded_for_publish);
    assert_eq!(shared_flag(&db, "n-user-scoped").await, 1);
    assert_eq!(pending_uploads(&db).await, 1);
    assert!(has_intent(&db, "notes", "n-user-scoped").await);
    assert!(
        external_blob(&db, "note_photos", "photo-user-scoped")
            .await
            .is_none(),
        "the successful flip drops the external reference",
    );
    assert_scoped_flip_journaled_atomically(
        &db,
        &[
            ("notes", crate::changeset::ChangeOp::Insert),
            ("note_photos", crate::changeset::ChangeOp::Insert),
        ],
    )
    .await;
    assert!(storage
        .publish_pending(&db, &lib)
        .await
        .expect("publish scoped user make_remote Store write"));
    assert_eq!(pending_uploads(&db).await, 0);
    assert!(!has_intent(&db, "notes", "n-user-scoped").await);
}

#[tokio::test]
async fn scoped_host_completion_without_routing_encryption_mutates_nothing() {
    let db = scoped_blob_transition_db();
    let storage = create_store(&db, UserKeypair::generate()).await;
    let exact_creates_before = storage.home.exact_create_count();
    storage.open_into(&db).await.expect("open exact test Store");
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (_tmp, lib) = temp_store_dir();
    let bytes = b"scoped-host-cover";
    let routing_encryption = crate::encryption::EncryptionService::from_key([9; 32]);
    crate::sync::test_helpers::exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n-host-scoped', 'Scoped host fixture', NULL, 0, \
                 '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    crate::sync::test_helpers::exec(
        &db,
        &format!(
            "INSERT INTO note_covers \
             (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('cover-host-scoped', 'n-host-scoped', {}, '{}', \
                     '0000000001000-0000-A', '2026-01-01', 'cv/cover-host-scoped.jpg')",
            bytes.len(),
            crate::blob::content_hash(bytes),
        ),
    )
    .await;
    crate::store_dir::StoreDir::store_local_blob(&lib, "covers", "cover-host-scoped", bytes)
        .await
        .expect("store host-provided fixture");
    make_remote(&db, &lib, "notes", "n-host-scoped", false)
        .await
        .expect("queue scoped host-provided make_remote");
    let stamp_before = gate_stamp(&db, "n-host-scoped").await;
    let store_state_before = scoped_store_state(&db).await;

    let error = match drain_uploads(&db, &storage, &lib, &SystemClock, &hlc, None, None).await {
        Err(error) => error,
        Ok(_) => panic!("a scoped host completion requires routing encryption before upload"),
    };

    assert!(
        error
            .to_string()
            .contains("requires the Store generation-1 routing key"),
        "the missing routing key is surfaced: {error}",
    );
    assert_eq!(
        storage.home.exact_create_count(),
        exact_creates_before,
        "the host-provided blob is not uploaded before routing validation",
    );
    assert!(
        !storage
            .home
            .exists("covers/cv/cover-host-scoped.jpg")
            .await
            .unwrap(),
        "the cloud is untouched",
    );
    assert!(
        lib.local_blob_path("covers", "cover-host-scoped")
            .unwrap()
            .exists(),
        "the local-store blob remains in place",
    );
    assert_eq!(shared_flag(&db, "n-host-scoped").await, 0);
    assert_eq!(gate_stamp(&db, "n-host-scoped").await, stamp_before);
    assert!(
        has_intent(&db, "notes", "n-host-scoped").await,
        "the transition intent remains queued",
    );
    assert_eq!(scoped_store_state(&db).await, store_state_before);

    let completed = drain_uploads(
        &db,
        &storage,
        &lib,
        &SystemClock,
        &hlc,
        Some(&routing_encryption),
        None,
    )
    .await
    .expect("retry scoped host completion with routing encryption");
    assert_eq!(completed.uploaded, 1);
    assert!(completed.yielded_for_publish);
    assert_eq!(storage.home.exact_create_count(), exact_creates_before + 1);
    assert_eq!(shared_flag(&db, "n-host-scoped").await, 1);
    assert!(
        has_intent(&db, "notes", "n-host-scoped").await,
        "the publishing intent remains until its exact Store write activates",
    );
    assert_scoped_flip_journaled_atomically(
        &db,
        &[
            ("notes", crate::changeset::ChangeOp::Insert),
            ("note_covers", crate::changeset::ChangeOp::Insert),
        ],
    )
    .await;
    assert!(
        storage
            .publish_pending(&db, &lib)
            .await
            .expect("publish scoped host make_remote Store write"),
        "the host completion produces an exact Store write",
    );
    assert!(
        !has_intent(&db, "notes", "n-host-scoped").await,
        "Store write activation consumes the durable publishing intent",
    );
}

// ===========================================================================
// Host-provided lifecycle
// ===========================================================================

/// A release with a user-provided photo file AND a host-provided cover, through both
/// transitions. make_remote journals both exact row versions, uploads both, and
/// flips the gate only after both objects exist. A peer
/// pulls the cover eagerly (`CacheEager`) into its cache. make_local: the photo goes
/// back to its dest (external ref) and the cover back to the local store (NO dest),
/// both cloud copies tombstoned.
#[tokio::test]
async fn host_provided_cover_rides_the_inline_push_through_both_transitions() {
    let kp_a = UserKeypair::generate();
    let db_a = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let storage = create_store(&db_a, kp_a.clone()).await;
    let enc = plaintext_cipher();
    let hlc_a = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp_a, lib_a) = temp_store_dir();
    let kp_b = UserKeypair::generate();
    let db_b = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let (_tmp_b, lib_b) = temp_store_dir();
    let photo = b"PHOTO-BYTES".to_vec();
    let cover = b"RELEASE-COVER".to_vec();

    // Seed a gated-off release: a note + a user-provided photo (external ref) + a
    // host-provided cover (in the local store).
    let src = seed_local_release(
        &db_a,
        &tmp_a.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &photo,
    )
    .await;
    exec(
        &db_a,
        &format!(
            "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coveraaa', 'n1', 13, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/cover-coveraaa.jpg')",
            crate::blob::content_hash(&cover),
        ),
    )
    .await;
    crate::store_dir::StoreDir::store_local_blob(&lib_a, "covers", "coveraaa", &cover)
        .await
        .expect("store the host-provided cover in the local store");

    let peer = invite_and_activate_peer(&storage, &db_a, &db_b, &kp_b).await;

    // A cycle while gated off: nothing reaches a peer.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;

    // make_remote: the photo drains, the gate flips, and this cycle's inline push
    // uploads the cover from the local store and keeps the requested pin.
    make_remote(&db_a, &lib_a, "notes", "n1", true)
        .await
        .expect("make_remote");
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;

    assert_eq!(shared_flag(&db_a, "n1").await, 1, "the release is Remote");
    assert!(
        remote_blob_exists(&storage, &cover_ref(&db_a, "coveraaa").await).await,
        "the host-provided cover is uploaded to the cloud",
    );
    assert!(
        exact_pinned_path(&lib_a, &cover_ref(&db_a, "coveraaa").await).exists(),
        "the cover's local-store copy moved into the pinned cache",
    );
    assert!(
        !lib_a
            .local_blob_path("covers", "coveraaa")
            .unwrap()
            .exists(),
        "the cover is no longer in the local store (it is Remote now)",
    );

    // B pulls: the cover (CacheEager) lands in B's cache; the photo (CacheLazy) does not.
    peer.pull_store(&lib_b).await.expect("pull peer Store");
    assert!(
        exact_cache_path(&lib_b, &cover_ref(&db_b, "coveraaa").await).exists(),
        "B fetches the CacheEager cover eagerly into its cache",
    );
    assert!(
        !exact_cache_path(&lib_b, &photo_ref(&db_b, "photoaaa").await).exists()
            && !exact_pinned_path(&lib_b, &photo_ref(&db_b, "photoaaa").await).exists(),
        "B does not fetch the CacheLazy photo on pull",
    );
    assert_eq!(
        read_blob(
            &db_b,
            &lib_b,
            Some(storage.storage.clone()),
            &cover_ref(&db_b, "coveraaa").await
        )
        .await
        .expect("B reads the cover"),
        cover,
        "B's cover bytes match",
    );

    // make_local: the photo back to its dest (external ref), the cover back to the
    // local store (no dest), both cloud copies tombstoned.
    let dest_path = tmp_a.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    make_local(
        &db_a,
        storage.storage.clone(),
        &lib_a,
        None,
        None,
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect("make_local");

    assert_eq!(
        shared_flag(&db_a, "n1").await,
        0,
        "the release is Local again"
    );
    assert_eq!(
        std::fs::read(&dest_path).unwrap(),
        photo,
        "the user-provided photo is materialized to its required dest",
    );
    assert_eq!(
        external_blob(&db_a, "note_photos", "photoaaa")
            .await
            .expect("materialized photo has external ownership")
            .path,
        dest_path,
        "the photo is registered as an external ref",
    );
    assert!(
        lib_a
            .local_blob_path("covers", "coveraaa")
            .unwrap()
            .exists(),
        "the host-provided cover is back in the local store (no dest needed)",
    );
    assert!(
        external_blob(&db_a, "note_covers", "coveraaa")
            .await
            .is_none(),
        "the host-provided cover registers NO external ref",
    );
    let mut deletes = pending_deletes(&db_a).await;
    deletes.sort();
    assert_eq!(
        deletes,
        vec!["coveraaa".to_string(), "photoaaa".to_string(),],
        "both cloud copies are tombstoned in the make_local commit",
    );
    // The source the user provided is untouched: make_remote uploads a copy and
    // drops the external ref, but never deletes the user's original.
    assert!(
        src.exists(),
        "the user-provided source is preserved on make_remote"
    );
}

#[tokio::test]
async fn host_provided_only_make_remote_flips_gate_and_consumes_durable_pin_intent() {
    let db_a = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let storage = create_store(&db_a, UserKeypair::generate()).await;
    let exact_creates_before = storage.home.exact_create_count();
    let enc = plaintext_cipher();
    let kp_a = storage.protocol_founder_keypair();
    let hlc_a = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (_tmp_a, lib_a) = temp_store_dir();
    let cover = b"HOST-ONLY-COVER".to_vec();

    exec(
        &db_a,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n-host', 'Host Only', NULL, 0, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db_a,
        &format!(
            "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coverhost', 'n-host', 15, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/host-coverhost.jpg')",
            crate::blob::content_hash(&cover),
        ),
    )
    .await;
    crate::store_dir::StoreDir::store_local_blob(&lib_a, "covers", "coverhost", &cover)
        .await
        .expect("store host-provided cover");

    let before = read_blob(
        &db_a,
        &lib_a,
        Some(storage.storage.clone()),
        &cover_ref(&db_a, "coverhost").await,
    )
    .await
    .expect("read Local host-provided cover");
    assert_eq!(before, cover);

    make_remote(&db_a, &lib_a, "notes", "n-host", true)
        .await
        .expect("make host-provided-only root remote");
    assert_eq!(
        shared_flag(&db_a, "n-host").await,
        0,
        "host-provided-only make_remote leaves the gate off until the blob uploads"
    );
    assert!(
        has_intent(&db_a, "notes", "n-host").await,
        "the pin choice is durable until inline upload consumes it"
    );
    assert_eq!(pending_uploads(&db_a).await, 1);
    assert!(
        !remote_blob_exists(&storage, &cover_ref(&db_a, "coverhost").await).await,
        "the host-provided blob is not published before the cycle uploads it"
    );

    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;

    assert_eq!(
        shared_flag(&db_a, "n-host").await,
        1,
        "the gate flips after the host-provided blob lands"
    );
    assert!(
        remote_blob_exists(&storage, &cover_ref(&db_a, "coverhost").await).await,
        "inline push uploads the host-provided blob"
    );
    assert!(storage.home.exact_create_count() > exact_creates_before);
    assert!(
        !has_intent(&db_a, "notes", "n-host").await,
        "inline upload consumes the make_remote intent"
    );
    assert!(
        exact_pinned_path(&lib_a, &cover_ref(&db_a, "coverhost").await).exists(),
        "pin=true keeps the host-provided blob in the protected cache"
    );
    assert!(
        !lib_a
            .local_blob_path("covers", "coverhost")
            .unwrap()
            .exists(),
        "after Remote upload the local store no longer holds the blob"
    );
    let after = read_blob(
        &db_a,
        &lib_a,
        Some(storage.storage.clone()),
        &cover_ref(&db_a, "coverhost").await,
    )
    .await
    .expect("read Remote host-provided cover");
    assert_eq!(after, cover);
}

/// A crash between a host-provided make_remote's gate flip and its local-store
/// disposition must not strand the blob. The flip commits the disposition as a
/// durable intent, so a cycle whose push fails (the flip is committed, the drain
/// that applies the disposition never runs) leaves the disposition pending — the
/// local-store copy untouched — and the recovery cycle's prepared-write retry drains
/// it, removing both local-store copies while retaining the requested pin.
#[tokio::test]
async fn host_provided_make_remote_disposition_survives_crash_before_drain() {
    let db = open_test_db_with_user_and_host_blobs(photo_decl(), cover_lazy_decl());
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(&db, UserKeypair::generate()).await;
    let enc = plaintext_cipher();
    let kp = storage.protocol_founder_keypair();
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (_tmp, lib) = temp_store_dir();
    let pin_bytes = b"PINNED-COVER".to_vec();
    let drop_bytes = b"DROPPED-COVER".to_vec();

    // Two host-provided-only releases: one made Remote with a pin (Pin disposition),
    // one without (Drop disposition, because the cover is CacheLazy).
    for (note, cover, path, bytes) in [
        ("n-pin", "cover-pin", "cv/cover-pin.jpg", &pin_bytes),
        ("n-drop", "cover-drop", "cv/cover-drop.jpg", &drop_bytes),
    ] {
        exec(
            &db,
            &format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('{note}', 'Release', NULL, 0, '0000000001000-0000-A', '2026-01-01')"
            ),
        )
        .await;
        exec(
            &db,
            &format!(
                "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
                 VALUES ('{cover}', '{note}', {}, '{}', '0000000001000-0000-A', '2026-01-01', '{path}')",
                bytes.len(),
                crate::blob::content_hash(bytes),
            ),
        )
        .await;
        crate::store_dir::StoreDir::store_local_blob(&lib, "covers", cover, bytes)
            .await
            .expect("store host-provided cover");
    }
    make_remote(&db, &lib, "notes", "n-pin", true)
        .await
        .expect("make_remote pin");
    make_remote(&db, &lib, "notes", "n-drop", false)
        .await
        .expect("make_remote drop");

    // Each upload drain stops after one root becomes publishable. Both flips now
    // own exact Created handoffs and durable local-store cleanup intents.
    drain_uploads(&db, &storage, &lib, &SystemClock, &hlc, None, None)
        .await
        .expect("create pinned exact blob and flip its root");
    drain_uploads(&db, &storage, &lib, &SystemClock, &hlc, None, None)
        .await
        .expect("create unpinned exact blob and flip its root");

    // The crash: Store publication fails, so neither cleanup intent may apply.
    storage.home.fail_exact_create_before_call(1);
    let failed = try_run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib).await;
    assert!(failed.is_err(), "the Store package append fails");

    assert_eq!(
        shared_flag(&db, "n-pin").await,
        1,
        "the pin release flipped"
    );
    assert_eq!(
        shared_flag(&db, "n-drop").await,
        1,
        "the drop release flipped"
    );
    assert!(
        row_exists(
            &db,
            "SELECT 1 FROM blob_make_remote_intents AS intent \
             JOIN store_writes AS write ON write.write_id = intent.write_id \
             WHERE intent.root_id = 'n-pin' AND intent.state = 'publishing' \
               AND write.prepared IS NOT NULL",
        )
        .await,
        "the prepared Store write durably owns the pin disposition",
    );
    assert!(
        exact_pinned_path(&lib, &cover_ref(&db, "cover-pin").await).exists(),
        "upload durability created the requested pin before the gate flipped",
    );
    assert!(
        lib.local_blob_path("covers", "cover-pin").unwrap().exists(),
        "the pinned cover remains in the local store until Store publication",
    );
    assert!(
        lib.local_blob_path("covers", "cover-drop")
            .unwrap()
            .exists(),
        "the dropped cover's local copy is still present until the drain runs",
    );

    // Recovery republishes the prepared write and applies both dispositions.
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    let store_cache =
        crate::sync::store::blob::StoreBlobCache::new(store_database.clone(), lib.clone());

    assert!(
        exact_pinned_path(&lib, &cover_ref(&db, "cover-pin").await).exists(),
        "recovery pins the retained cover",
    );
    assert!(
        store_cache
            .all_pinned(std::slice::from_ref(&cover_ref(&db, "cover-pin").await))
            .await
            .unwrap(),
        "is_pinned reports the recovered pin",
    );
    assert!(
        !lib.local_blob_path("covers", "cover-pin").unwrap().exists(),
        "the pinned cover no longer sits in the local store",
    );
    assert!(
        !lib.local_blob_path("covers", "cover-drop")
            .unwrap()
            .exists(),
        "recovery drops the un-pinned cover's local copy",
    );
    assert!(
        !store_cache
            .all_pinned(std::slice::from_ref(&cover_ref(&db, "cover-drop").await))
            .await
            .unwrap(),
        "the dropped cover is not pinned",
    );
    assert!(remote_blob_exists(&storage, &cover_ref(&db, "cover-pin").await).await);
    assert!(remote_blob_exists(&storage, &cover_ref(&db, "cover-drop").await).await);
}

/// The drain applies a disposition (copy to the destination, drop the local-store
/// source) and then clears its intent in a separate commit. A crash in that window
/// leaves the blob correctly placed but the intent uncleared. Re-draining must
/// recognize the completed work — the blob already in pinned/ — and clear the intent,
/// not keep failing every cycle because the source it would copy is gone.
#[tokio::test]
async fn drain_clears_a_pin_disposition_already_applied_before_its_intent() {
    let db = open_test_db();
    let storage = create_store(&db, UserKeypair::generate()).await;
    let enc = plaintext_cipher();
    let kp = storage.protocol_founder_keypair();
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (_tmp, lib) = temp_store_dir();
    let bytes = b"ALREADY-PINNED".to_vec();

    let pinned = lib
        .pinned_blob_path(
            "covers",
            crate::sync::test_helpers::test_cache_locator_hash("cov-pin"),
        )
        .unwrap();
    tokio::fs::create_dir_all(pinned.parent().unwrap())
        .await
        .unwrap();
    crate::storage::StagedBlobFile::write_for_test(&pinned, &bytes)
        .await
        .unwrap();
    let sequence = publish_fixture_position(&storage, &db, &lib, "pin-position").await;
    insert_published_drop_intent(
        &db,
        sequence as i64,
        "covers",
        "cov-pin",
        &bytes,
        crate::sync::test_helpers::test_cache_locator_hash("cov-pin"),
        "pin",
    )
    .await;

    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert!(
        !drop_intent_present(&db, "cov-pin").await,
        "the drain recognizes the completed pin and clears its intent",
    );
    assert!(pinned.exists(), "the pin stays intact");
}

/// Sibling of the pin case for the cache disposition: the blob already sits in cache/
/// with its source dropped, so re-draining recognizes it and clears the intent.
#[tokio::test]
async fn drain_clears_a_cache_disposition_already_applied_before_its_intent() {
    let db = open_test_db();
    let storage = create_store(&db, UserKeypair::generate()).await;
    let enc = plaintext_cipher();
    let kp = storage.protocol_founder_keypair();
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (_tmp, lib) = temp_store_dir();
    let bytes = b"ALREADY-CACHED".to_vec();

    let cached = lib
        .cache_blob_path(
            "covers",
            crate::sync::test_helpers::test_cache_locator_hash("cov-cache"),
        )
        .unwrap();
    tokio::fs::create_dir_all(cached.parent().unwrap())
        .await
        .unwrap();
    crate::storage::StagedBlobFile::write_for_test(&cached, &bytes)
        .await
        .unwrap();
    let sequence = publish_fixture_position(&storage, &db, &lib, "cache-position").await;
    insert_published_drop_intent(
        &db,
        sequence as i64,
        "covers",
        "cov-cache",
        &bytes,
        crate::sync::test_helpers::test_cache_locator_hash("cov-cache"),
        "cache",
    )
    .await;

    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert!(
        !drop_intent_present(&db, "cov-cache").await,
        "the drain recognizes the completed cache write and clears its intent",
    );
    assert!(cached.exists(), "the cache copy stays intact");
}

/// A disposition whose blob is in neither the local store nor its destination is a
/// genuine loss, not a completed apply: the drain must keep failing loud (the intent
/// stays pending) rather than clearing it as if the work had been done.
#[tokio::test]
async fn drain_keeps_a_disposition_whose_blob_is_genuinely_lost() {
    let db = open_test_db();
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(&db, UserKeypair::generate()).await;
    let (_tmp, lib) = temp_store_dir();

    let sequence = publish_fixture_position(&storage, &db, &lib, "lost-position").await;
    insert_published_drop_intent(
        &db,
        sequence as i64,
        "covers",
        "cov-lost",
        b"missing",
        crate::sync::test_helpers::test_cache_locator_hash("cov-lost"),
        "pin",
    )
    .await;

    let error = crate::sync::test_owner_graph::local_blob_access(store_database, lib.clone())
        .drain_published_blob_drop_intents(sequence)
        .await
        .expect_err("a lost disposition fails the drain");
    assert!(error.contains("missing from both"), "{error}");

    assert!(
        drop_intent_present(&db, "cov-lost").await,
        "a disposition missing from both the local store and its destination stays pending",
    );
    assert!(
        !lib.pinned_blob_path(
            "covers",
            crate::sync::test_helpers::test_cache_locator_hash("cov-lost")
        )
        .unwrap()
        .exists(),
        "no destination copy was conjured",
    );
}

#[tokio::test]
async fn remote_root_host_provided_blob_uploads_before_peer_reads_the_row() {
    let kp_a = UserKeypair::generate();
    let kp_b = UserKeypair::generate();
    let db_a = remote_root_db(cover_decl());
    let storage = create_store(&db_a, kp_a.clone()).await;
    let enc = plaintext_cipher();
    let hlc_a = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (_tmp_a, lib_a) = temp_store_dir();
    let cover = b"REMOTE-ROOT-HOST-BLOB".to_vec();

    exec(
        &db_a,
        "INSERT INTO notes (id, title, _updated_at, created_at) \
         VALUES ('n-remote-root', 'Remote Root', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db_a,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coverrrr', 'n-remote-root', 'cover', 21, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/remote-root-coverrrr.jpg')",
            crate::blob::content_hash(&cover),
        ),
    )
    .await;
    crate::store_dir::StoreDir::store_local_blob(&lib_a, "covers", "coverrrr", &cover)
        .await
        .expect("store host-provided blob");

    let db_b = remote_root_db(cover_decl());
    let (_tmp_b, lib_b) = temp_store_dir();
    let peer = invite_and_activate_peer(&storage, &db_a, &db_b, &kp_b).await;
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;
    assert!(
        remote_blob_exists(
            &storage,
            &db_a
                .row_blob_ref("note_photos", "coverrrr")
                .await
                .expect("load exact remote-root cover reference"),
        )
        .await,
        "the host-provided blob is uploaded before the row changeset is pushed"
    );

    peer.pull_store(&lib_b).await.expect("pull peer Store");
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n-remote-root'").await,
        "the peer receives the remote-root row"
    );
    assert!(
        exact_cache_path(
            &lib_b,
            &db_b
                .row_blob_ref("note_photos", "coverrrr")
                .await
                .expect("load exact remote-root cover reference"),
        )
        .exists(),
        "the peer eagerly caches the host-provided blob"
    );
    let got = read_blob(
        &db_b,
        &lib_b,
        Some(storage.storage.clone()),
        &db_b
            .row_blob_ref("note_photos", "coverrrr")
            .await
            .expect("load exact remote-root cover reference"),
    )
    .await
    .expect("peer reads the remote-root blob");
    assert_eq!(got, cover);
}

#[tokio::test]
async fn make_remote_rejects_remote_root() {
    let db = remote_root_db(cover_decl());
    let (_tmp, lib) = temp_store_dir();
    exec(
        &db,
        "INSERT INTO notes (id, title, _updated_at, created_at) \
         VALUES ('n-remote-root', 'Remote Root', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coverrrr', 'n-remote-root', 'cover', 11, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/remote-root-coverrrr.jpg')",
            crate::blob::content_hash(b"REMOTE-ROOT"),
        ),
    )
    .await;
    crate::store_dir::StoreDir::store_local_blob(&lib, "covers", "coverrrr", b"REMOTE-ROOT")
        .await
        .expect("store host-provided blob");

    let err = make_remote(&db, &lib, "notes", "n-remote-root", true)
        .await
        .expect_err("remote roots have no make_remote transition");
    assert!(
        matches!(err, crate::blob::transition::MakeRemoteError::RemoteRoot(_)),
        "make_remote rejects a remote root specifically: {err:?}"
    );
}

#[tokio::test]
async fn make_local_rejects_remote_root() {
    let db = remote_root_db(cover_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let (tmp, lib) = temp_store_dir();
    exec(
        &db,
        "INSERT INTO notes (id, title, _updated_at, created_at) \
         VALUES ('n-remote-root', 'Remote Root', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coverrrr', 'n-remote-root', 'cover', 11, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/remote-root-coverrrr.jpg')",
            crate::blob::content_hash(b"REMOTE-ROOT"),
        ),
    )
    .await;
    let dest: HashMap<String, PathBuf> =
        [("coverrrr".to_string(), tmp.path().join("dest/coverrrr.jpg"))].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = make_local(
        &db,
        storage.storage.clone(),
        &lib,
        None,
        None,
        "notes",
        "n-remote-root",
        &dest,
        &cancel,
    )
    .await
    .expect_err("remote roots have no make_local transition");
    assert!(
        matches!(err, crate::blob::transition::MakeLocalError::RemoteRoot(_)),
        "make_local rejects a remote root specifically: {err:?}"
    );
}

#[tokio::test]
async fn cancel_make_remote_rejects_remote_root() {
    let db = remote_root_db(cover_decl());
    let store_database = StoreDatabase::new(&db);

    let err = cancel_make_remote(&store_database, "notes", "n-remote-root")
        .await
        .expect_err("remote roots have no cancelable make_remote transition");
    assert!(
        matches!(err, crate::blob::transition::MakeRemoteError::RemoteRoot(_)),
        "cancel_make_remote rejects a remote root specifically: {err:?}"
    );
}

/// make_remote on a root already Remote is refused at the API: no intent is
/// recorded and the gate row is untouched. Without the precondition, path 1
/// (host-provided-only) would insert an intent whose completion re-flips the
/// already-on gate with a fresh stamp — a spurious full-subtree re-publish.
#[tokio::test]
async fn make_remote_rejects_already_remote_root() {
    let db = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let (_tmp, lib) = temp_store_dir();

    // A host-provided-only root already Remote (gate on): a note plus a cover row.
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n-host', 'Host Only', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        &format!(
            "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coverhost', 'n-host', 15, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/host-coverhost.jpg')",
            crate::blob::content_hash(&[0; 15]),
        ),
    )
    .await;

    let stamp_before = gate_stamp(&db, "n-host").await;

    let err = make_remote(&db, &lib, "notes", "n-host", true)
        .await
        .expect_err("a root already Remote has no make_remote transition");
    assert!(
        matches!(
            err,
            crate::blob::transition::MakeRemoteError::AlreadyRemote(_, _)
        ),
        "make_remote refuses an already-Remote root specifically: {err:?}"
    );

    assert!(
        !has_intent(&db, "notes", "n-host").await,
        "a refused make_remote records no intent",
    );
    assert_eq!(shared_flag(&db, "n-host").await, 1, "the gate stays on");
    assert_eq!(
        gate_stamp(&db, "n-host").await,
        stamp_before,
        "the gate stamp is untouched — no spurious re-publish",
    );
}

/// make_remote verifies every user-provided source before enqueuing a single
/// upload: if a source file's on-disk length no longer matches the size recorded
/// on its blob row (truncated after registration — an interrupted copy, a partial
/// write), the whole transition aborts with `MakeRemoteError::Source` and queues
/// nothing. Without this up-front check the drain would upload a short, corrupt
/// blob and flip the gate over it. Proves the abort is atomic: no upload is
/// enqueued, no intent is recorded, the gate stays Local (row and causal stamp
/// untouched), and the user's source file is left in place — neither consumed nor
/// deleted. coven's own-layer counterpart to bae's
/// `test_manage_truncated_source_aborts_before_enqueue`.
#[tokio::test]
async fn make_remote_aborts_when_source_size_no_longer_matches() {
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let bytes = b"PHOTO-BYTES-full-length".to_vec();

    let src = seed_local_release(
        &db,
        &tmp.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;

    // Truncate the source on disk so its length no longer matches the size the
    // blob row recorded at registration — the drift the pre-enqueue check catches.
    let truncated_len = 5u64;
    let f = std::fs::OpenOptions::new().write(true).open(&src).unwrap();
    f.set_len(truncated_len).unwrap();
    drop(f);
    assert_eq!(src.metadata().unwrap().len(), truncated_len);

    let stamp_before = gate_stamp(&db, "n1").await;

    let err = make_remote(&db, &lib, "notes", "n1", true)
        .await
        .expect_err("a source whose length drifted from its blob row aborts make_remote");
    assert!(
        matches!(
            &err,
            crate::blob::transition::MakeRemoteError::Source { blob_id, .. }
                if blob_id.as_str() == "photoaaa"
        ),
        "make_remote aborts on the source-verification check for the drifted blob: {err:?}"
    );

    // The abort is atomic: nothing was enqueued and the gate never flipped.
    assert_eq!(
        pending_uploads(&db).await,
        0,
        "the source check aborts before a single upload is enqueued",
    );
    assert!(
        !has_intent(&db, "notes", "n1").await,
        "an aborted make_remote records no intent",
    );
    assert_eq!(shared_flag(&db, "n1").await, 0, "the release stays Local");
    assert_eq!(
        gate_stamp(&db, "n1").await,
        stamp_before,
        "the gate row and its causal stamp are untouched",
    );

    // The failed transition neither consumed nor deleted the user's source, and
    // left its external ref registered — the release is exactly as it was.
    assert!(src.exists(), "the source file is left in place");
    assert_eq!(
        src.metadata().unwrap().len(),
        truncated_len,
        "the source file is untouched by the aborted transition",
    );
    assert!(
        external_blob(&db, "note_photos", "photoaaa")
            .await
            .is_some(),
        "the external blob ref survives the aborted transition",
    );
}

/// make_local on a root already Local is refused at the API before any
/// materialization: nothing is registered, no delete is queued, the gate row is
/// untouched. Without the precondition, make_local would try to read the blob from
/// the cloud and fail deep in materialization with a misleading cloud-read error.
#[tokio::test]
async fn make_local_rejects_already_local_root() {
    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let (tmp, lib) = temp_store_dir();
    let bytes = b"already-local".to_vec();

    // A Local release (gate off) with its blob at a registered external file.
    seed_local_release(
        &db,
        &tmp.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;

    let stamp_before = gate_stamp(&db, "n1").await;
    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = make_local(
        &db,
        storage.storage.clone(),
        &lib,
        None,
        None,
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect_err("a root already Local has no make_local transition");
    assert!(
        matches!(
            err,
            crate::blob::transition::MakeLocalError::AlreadyLocal(_, _)
        ),
        "make_local refuses an already-Local root specifically: {err:?}"
    );

    assert_eq!(shared_flag(&db, "n1").await, 0, "the gate stays off");
    assert_eq!(
        gate_stamp(&db, "n1").await,
        stamp_before,
        "the gate stamp is untouched",
    );
    assert!(
        pending_deletes(&db).await.is_empty(),
        "no cloud delete is queued",
    );
    assert!(!dest_path.exists(), "no file is materialized");
}

// ===========================================================================
// Cancel
// ===========================================================================

/// Cancelling an in-flight make_remote clears the intent and upload journals and
/// exact-deletes any object that already landed. The gate never flips.
#[tokio::test]
async fn cancel_make_remote_clears_pending_and_exact_deletes_uploaded() {
    let db = open_test_db_with_blob(photo_decl());
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(&db, UserKeypair::generate()).await;
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let user = tmp.path().join("user");

    // Two photos under one release.
    let _src1 = seed_local_release(&db, &user, "n1", "photoaaa", "cv/photoaaa.jpg", b"first").await;
    let src2 = add_local_photo(&db, &user, "n1", "photobbb", "cv/photobbb.jpg", b"second").await;

    make_remote(&db, &lib, "notes", "n1", true)
        .await
        .expect("make_remote");
    assert_eq!(pending_uploads(&db).await, 2, "both uploads queued");

    // Drain with photobbb's source removed: photoaaa uploads (not the last, no flip), photobbb fails.
    std::fs::remove_file(&src2).unwrap();
    drain_uploads(&db, &storage, &lib, &SystemClock, &hlc, None, None)
        .await
        .expect("partial drain");
    assert_eq!(
        shared_flag(&db, "n1").await,
        0,
        "not flipped — photobbb never uploaded"
    );
    let uploaded = created_upload_blob(&db, "photoaaa").await;
    storage
        .verify_blob_object(&uploaded)
        .await
        .expect("photoaaa is in the cloud");
    assert!(
        has_intent(&db, "notes", "n1").await,
        "the make_remote is still in flight"
    );

    // Cancel: mark the intent, then let the upload drain exact-delete the created
    // object and consume both upload journals atomically with the intent.
    cancel_make_remote(&store_database, "notes", "n1")
        .await
        .expect("cancel make_remote");
    drain_uploads(&db, &storage, &lib, &SystemClock, &hlc, None, None)
        .await
        .expect("drain cancelled make_remote cleanup");
    assert_eq!(shared_flag(&db, "n1").await, 0, "the release stays Local");
    assert!(
        !has_intent(&db, "notes", "n1").await,
        "the intent is cleared"
    );
    assert_eq!(pending_uploads(&db).await, 0, "no uploads remain");
    assert!(pending_deletes(&db).await.is_empty());
    assert!(storage.verify_blob_object(&uploaded).await.is_err());
    assert!(
        !lib.pinned_blob_path("photos", uploaded.locator().locator_hash())
            .unwrap()
            .exists(),
        "the orphan's pinned cache copy is dropped",
    );
}

#[tokio::test]
async fn cancel_make_remote_deletes_every_same_locator_exact_object() {
    let db = open_test_db_with_blob(photo_decl().with_id_column("blob_id"));
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(&db, UserKeypair::generate()).await;
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"same-locator-created-journals";
    let hash = crate::blob::content_hash(bytes);

    exec(
        &db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'Release', NULL, 0, '0000000001000-0000-A', '2026-01-01'); \
             INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path, blob_id) \
             VALUES ('photo-a', 'n1', 'image', {}, '{hash}', \
                     '0000000001000-0000-A', '2026-01-01', 'cv/shared.bin', 'sharedblob'), \
                    ('photo-b', 'n1', 'image', {}, '{hash}', \
                     '0000000001000-0000-A', '2026-01-01', 'cv/shared.bin', 'sharedblob')",
            bytes.len(),
            bytes.len(),
        ),
    )
    .await;
    let user_dir = tmp.path().join("user");
    std::fs::create_dir_all(&user_dir).unwrap();
    let photo_source = user_dir.join("photo.bin");
    let cover_source = user_dir.join("cover.bin");
    std::fs::write(&photo_source, bytes).unwrap();
    std::fs::write(&cover_source, bytes).unwrap();
    register_external_blob(&db, "note_photos", "photo-a", &photo_source).await;
    register_external_blob(&db, "note_photos", "photo-b", &cover_source).await;

    make_remote(&db, &lib, "notes", "n1", false)
        .await
        .expect("queue both same-locator uploads");
    storage.open_into(&db).await.expect("open exact test Store");
    let (registration_ref, registration) = crate::database::StoreDatabase::new(&db)
        .local_blob_write_authority()
        .await
        .expect("load local blob write authority");
    let authority = crate::storage::BlobWriteAuthority::new(&registration_ref, &registration)
        .expect("validate local blob write authority");
    let entries = db
        .get_pending_cloud_uploads()
        .await
        .expect("load same-locator upload journals");
    assert_eq!(entries.len(), 2);
    let mut created = Vec::new();
    for pending in entries {
        let crate::database::OutboxOperation::Upload {
            row,
            source_path,
            state: crate::database::OutboxUploadState::Pending,
            ..
        } = &pending.operation
        else {
            panic!("new make_remote journal is Pending");
        };
        let protection = storage
            .storage
            .store_blob_protection()
            .expect("load blob protection");
        let locator = match &protection {
            crate::storage::BlobSpoolProtection::Opaque(encryption) => {
                crate::blob::locator::BlobLocator::opaque(
                    row.blob().namespace.clone(),
                    row.blob().id.clone(),
                    registration_ref.clone(),
                    crate::blob::locator::RemoteAudience::Store,
                    row.blob().scope.clone(),
                    encryption.seal_key_fingerprint(),
                    row.plaintext_size(),
                    row.plaintext_hash(),
                )
            }
            crate::storage::BlobSpoolProtection::Browsable => {
                crate::blob::locator::BlobLocator::browsable(
                    row.blob().namespace.clone(),
                    row.blob().id.clone(),
                    registration_ref.clone(),
                    row.blob()
                        .cloud_path
                        .clone()
                        .expect("browsable row has a cloud path"),
                    row.plaintext_size(),
                    row.plaintext_hash(),
                )
            }
        }
        .expect("build shared locator");
        let spool = lib.outbound_blob_spool_path(locator.locator_hash());
        storage
            .storage
            .seal_blob_to_spool(&locator, &authority, protection, source_path, &spool)
            .await
            .expect("seal shared-locator blob");
        let slot = storage
            .storage
            .allocate_blob_slot(&locator, &authority)
            .await
            .expect("allocate distinct exact blob slot");
        let stored = storage
            .storage
            .prepare_blob_object(&locator, &authority, slot, &spool)
            .await
            .expect("prepare exact blob");
        db.mark_cloud_upload_prepared(
            &pending,
            crate::protocol::audience_package::PackageAudience::Store,
            stored.clone(),
            spool.clone(),
        )
        .await
        .expect("record Prepared exact handoff");
        storage
            .storage
            .create_blob_object_from_file(
                &stored,
                &authority,
                &spool,
                &crate::storage::cloud::no_progress(),
            )
            .await
            .expect("create exact blob");
        let prepared = db
            .get_pending_cloud_uploads()
            .await
            .expect("reload Prepared handoff")
            .into_iter()
            .find(|entry| entry.id == pending.id)
            .expect("Prepared handoff remains queued");
        db.mark_cloud_upload_created(&prepared)
            .await
            .expect("record Created exact handoff");
        created.push(stored);
    }
    assert_eq!(created[0].locator(), created[1].locator());
    assert_ne!(created[0].object(), created[1].object());

    cancel_make_remote(&store_database, "notes", "n1")
        .await
        .expect("cancel same-locator uploads");
    drain_uploads(&db, &storage, &lib, &SystemClock, &hlc, None, None)
        .await
        .expect("drain exact cancellation cleanup");

    assert_eq!(pending_uploads(&db).await, 0, "both handoffs are cleared");
    assert!(!has_intent(&db, "notes", "n1").await);
    for stored in &created {
        storage
            .verify_blob_object(stored)
            .await
            .expect_err("each exact object is deleted");
    }
}

/// A queued upload without its owning make_remote intent is inconsistent durable
/// state. The drain leaves its exact object and journal intact and reports the
/// semantic failure instead of guessing that either one is disposable.
#[tokio::test]
async fn drain_orphan_upload_fails_loud_and_preserves_exact_state() {
    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();

    let src = seed_local_release(
        &db,
        &tmp.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        b"orphan-bytes",
    )
    .await;
    // Enqueue an upload with no intent to model impossible durable state directly.
    let row = photo_ref(&db, "photoaaa").await;
    db.call(move |conn| {
        crate::database::CloudOutboxRecords::new(conn).enqueue_upload(
            "notes",
            "n1",
            &row,
            &src,
            true,
            "0000000001000-0000-A",
        )
    })
    .await
    .unwrap();

    let deletes_before = storage.home.exact_delete_count();
    let outcome = drain_uploads(&db, &storage, &lib, &SystemClock, &hlc, None, None)
        .await
        .expect("drain");

    assert_eq!(outcome.failures.failures().len(), 1);
    assert!(
        outcome.failures.failures()[0]
            .cause
            .to_string()
            .contains("make_remote intent"),
        "the missing owner is reported",
    );
    assert_eq!(shared_flag(&db, "n1").await, 0, "no intent means no flip");
    assert!(pending_deletes(&db).await.is_empty());
    assert_eq!(storage.home.exact_delete_count(), deletes_before);
    assert_eq!(pending_uploads(&db).await, 1);
    let created = created_upload_blob(&db, "photoaaa").await;
    storage
        .verify_blob_object(&created)
        .await
        .expect("the exact created object is preserved with its journal");
    assert!(
        lib.pinned_blob_path("photos", created.locator().locator_hash())
            .unwrap()
            .exists(),
        "the durable cache copy is preserved with the exact object",
    );
}

/// Cancelling a make_local before the commit deletes the partial dest copies and
/// leaves the release Remote with nothing tombstoned.
#[tokio::test]
async fn cancel_make_local_before_commit_stays_remote() {
    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"still-managed".to_vec();

    seed_remote_release(
        &storage,
        &db,
        &lib,
        &hlc,
        None,
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    // Already cancelled (initial value true) before the first materialize.
    let (_cancel_tx, cancel) = watch::channel(true);

    let err = make_local(
        &db,
        storage.storage.clone(),
        &lib,
        None,
        None,
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect_err("a cancelled make_local aborts");
    assert!(matches!(
        err,
        crate::blob::transition::MakeLocalError::Cancelled
    ));

    assert_eq!(shared_flag(&db, "n1").await, 1, "the release stays Remote");
    assert!(
        external_blob(&db, "note_photos", "photoaaa")
            .await
            .is_none(),
        "no external ref registered"
    );
    assert!(pending_deletes(&db).await.is_empty(), "nothing tombstoned");
    assert!(!dest_path.exists(), "no partial dest copy left behind");
}

/// A make_local that can't write a dest file aborts before the commit: the release
/// stays Remote, the cloud blob is untouched, and no delete is queued.
#[tokio::test]
async fn make_local_dest_failure_stays_remote_no_tombstones() {
    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"managed-bytes".to_vec();

    seed_remote_release(
        &storage,
        &db,
        &lib,
        &hlc,
        None,
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;

    // Block the dest: make the dest's parent dir a FILE, so create_dir_all fails.
    let blocker = tmp.path().join("blocker");
    std::fs::write(&blocker, b"x").unwrap();
    let dest_path = blocker.join("photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path)].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = make_local(
        &db,
        storage.storage.clone(),
        &lib,
        None,
        None,
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect_err("the dest write fails");
    assert!(matches!(
        err,
        crate::blob::transition::MakeLocalError::Write { .. }
    ));

    assert_eq!(shared_flag(&db, "n1").await, 1, "the release stays Remote");
    assert!(
        external_blob(&db, "note_photos", "photoaaa")
            .await
            .is_none(),
        "no external ref"
    );
    assert!(pending_deletes(&db).await.is_empty(), "no tombstone queued");
    assert!(
        storage
            .verify_blob_object(
                photo_ref(&db, "photoaaa")
                    .await
                    .stored()
                    .expect("Remote photo has exact storage"),
            )
            .await
            .is_ok(),
        "the cloud blob is untouched",
    );
}

/// A database failure after materialization removes every file created by the
/// operation. The root remains Remote and the cloud blob remains authoritative.
#[tokio::test]
async fn make_local_commit_failure_removes_materialized_files() {
    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"materialized-before-commit-failure".to_vec();

    seed_remote_release(
        &storage,
        &db,
        &lib,
        &hlc,
        None,
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;
    db.call(|connection| {
        connection
            .execute_batch(
                "CREATE TRIGGER reject_make_local_gate_update
                 BEFORE UPDATE OF shared ON notes
                 WHEN NEW.id = 'n1' AND NEW.shared = 0
                 BEGIN
                     SELECT RAISE(ABORT, 'forced make_local commit failure');
                 END;",
            )
            .map_err(crate::database::DbError::from)
    })
    .await
    .expect("install make_local commit failure");

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let error = make_local(
        &db,
        storage.storage.clone(),
        &lib,
        None,
        None,
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect_err("the gate update fails after materialization");

    assert!(
        matches!(error, crate::blob::transition::MakeLocalError::Db(_)),
        "the database failure surfaces: {error:?}",
    );
    assert!(!dest_path.exists(), "the materialized file is rolled back");
    assert_eq!(shared_flag(&db, "n1").await, 1, "the root stays Remote");
    assert!(
        external_blob(&db, "note_photos", "photoaaa")
            .await
            .is_none(),
        "no external ownership is registered",
    );
    assert!(pending_deletes(&db).await.is_empty(), "no delete is queued");
    storage
        .verify_blob_object(
            photo_ref(&db, "photoaaa")
                .await
                .stored()
                .expect("Remote photo has exact storage"),
        )
        .await
        .expect("the cloud blob remains intact");
}

/// A non-UTF-8 destination path aborts make_local before the cloud delete: the path
/// conversion fails loud (`NonUtf8Dest`) rather than lossily rewriting the dest,
/// registering a wrong external ref, and tombstoning the cloud copy. The release
/// stays Remote, nothing is registered, no tombstone is queued, the cloud is intact.
#[cfg(unix)]
#[tokio::test]
async fn make_local_non_utf8_dest_stays_remote_no_tombstones() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"managed-bytes".to_vec();

    seed_remote_release(
        &storage,
        &db,
        &lib,
        &hlc,
        None,
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;

    // A dest whose filename is not valid UTF-8: `to_str()` returns None, so the
    // conversion must fail loud instead of lossily rewriting the path. Kept under the
    // temp dir so the rolled-back partial is contained.
    let bad = tmp.path().join(OsStr::from_bytes(b"photo-\xff\xfe.jpg"));
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), bad)].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = make_local(
        &db,
        storage.storage.clone(),
        &lib,
        None,
        None,
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect_err("a non-UTF-8 dest aborts");
    assert!(matches!(
        err,
        crate::blob::transition::MakeLocalError::NonUtf8Dest { .. }
    ));

    assert_eq!(shared_flag(&db, "n1").await, 1, "the release stays Remote");
    assert!(
        external_blob(&db, "note_photos", "photoaaa")
            .await
            .is_none(),
        "no external ref registered"
    );
    assert!(pending_deletes(&db).await.is_empty(), "no tombstone queued");
    assert!(
        storage
            .verify_blob_object(
                photo_ref(&db, "photoaaa")
                    .await
                    .stored()
                    .expect("Remote photo has exact storage"),
            )
            .await
            .is_ok(),
        "the cloud blob is untouched",
    );
}

// ===========================================================================
// Crash idempotency
// ===========================================================================

/// Crash mid-make_remote (some blobs uploaded, the gate not flipped): the release stays
/// validly Local-uploading, and re-running the drain reaches a publishable Remote
/// write once the remaining exact object lands.
#[tokio::test]
async fn make_remote_crash_before_flip_redrain_converges() {
    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let user = tmp.path().join("user");

    seed_local_release(&db, &user, "n1", "photoaaa", "cv/photoaaa.jpg", b"first").await;
    let src2 = add_local_photo(&db, &user, "n1", "photobbb", "cv/photobbb.jpg", b"second").await;

    make_remote(&db, &lib, "notes", "n1", true)
        .await
        .expect("make_remote");

    // "Crash" after photoaaa uploads but before completion: remove photobbb's source so the
    // first drain uploads only photoaaa, leaving the make_remote in flight.
    std::fs::remove_file(&src2).unwrap();
    drain_uploads(&db, &storage, &lib, &SystemClock, &hlc, None, None)
        .await
        .expect("partial drain");
    assert_eq!(shared_flag(&db, "n1").await, 0, "still Local-uploading");
    assert!(
        has_intent(&db, "notes", "n1").await,
        "the make_remote marker survives"
    );
    assert_eq!(
        pending_uploads(&db).await,
        2,
        "the Created photoaaa handoff and Pending photobbb journal both remain authoritative"
    );

    // Re-run the drain after photobbb's source is back. Clear photobbb's
    // failed-attempt backoff first (a restart/retry re-attempts past the window);
    // the drain then completes and flips.
    std::fs::write(&src2, b"second").unwrap();
    db.reset_cloud_outbox_backoff().await.unwrap();
    drain_uploads(&db, &storage, &lib, &SystemClock, &hlc, None, None)
        .await
        .expect("resume drain");
    assert_eq!(shared_flag(&db, "n1").await, 1, "converged to Remote");
    assert!(
        has_intent(&db, "notes", "n1").await,
        "the publishing intent remains until the exact Store write activates"
    );
    assert_eq!(
        pending_uploads(&db).await,
        2,
        "Created handoffs remain until Store publication activates them"
    );
    assert!(
        storage
            .publish_pending(&db, &lib)
            .await
            .expect("publish completed make_remote"),
        "the completed transition has a Store write to publish",
    );
    assert!(!has_intent(&db, "notes", "n1").await);
    assert_eq!(pending_uploads(&db).await, 0);
    assert!(external_blob(&db, "note_photos", "photoaaa")
        .await
        .is_none());
    assert!(external_blob(&db, "note_photos", "photobbb")
        .await
        .is_none());
}

/// An aborted make_local (here via cancel) leaves the release Remote; retrying from
/// scratch converges to Local with the cloud delete enqueued — re-materialize +
/// re-commit is idempotent.
#[tokio::test]
async fn make_local_abort_then_retry_converges() {
    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"materialize-me".to_vec();

    seed_remote_release(
        &storage,
        &db,
        &lib,
        &hlc,
        None,
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();

    // First attempt is cancelled before the commit (the "crash"): still Remote.
    let (_cancel_tx, cancelled) = watch::channel(true);
    let err = make_local(
        &db,
        storage.storage.clone(),
        &lib,
        None,
        None,
        "notes",
        "n1",
        &dest,
        &cancelled,
    )
    .await
    .expect_err("aborted");
    assert!(
        matches!(err, crate::blob::transition::MakeLocalError::Cancelled),
        "the abort surfaces Cancelled, got {err:?}"
    );
    assert_eq!(
        shared_flag(&db, "n1").await,
        1,
        "still Remote after the abort"
    );

    // Retry from scratch: converges to Local with the file materialized and the
    // cloud delete enqueued.
    let (_fresh_tx, fresh) = watch::channel(false);
    make_local(
        &db,
        storage.storage.clone(),
        &lib,
        None,
        None,
        "notes",
        "n1",
        &dest,
        &fresh,
    )
    .await
    .expect("retry make_local");
    assert_eq!(shared_flag(&db, "n1").await, 0, "converged to Local");
    assert_eq!(std::fs::read(&dest_path).unwrap(), bytes);
    assert_eq!(pending_deletes(&db).await, vec!["photoaaa".to_string()],);
}

// ===========================================================================
// Round trip
// ===========================================================================

/// make_remote → make_local → make_remote on one device. The second make_remote
/// creates a new exact object. The old object's tombstone remains valid and cannot
/// reclaim the replacement.
#[tokio::test]
async fn round_trip_make_remote_make_local_make_remote() {
    let db = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db, UserKeypair::generate()).await;
    let enc = plaintext_cipher();
    let kp = storage.protocol_founder_keypair();
    let hlc = Hlc::new(
        "A".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
    );
    let (tmp, lib) = temp_store_dir();
    let bytes = b"round-trip-photo".to_vec();

    // Start Local, make it Remote.
    seed_local_release(
        &db,
        &tmp.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;
    make_remote(&db, &lib, "notes", "n1", true)
        .await
        .expect("make_remote 1");
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;
    assert_eq!(shared_flag(&db, "n1").await, 1, "Remote after make_remote");
    let first_remote = photo_ref(&db, "photoaaa")
        .await
        .stored()
        .cloned()
        .expect("first Remote photo has exact storage authority");

    // Make it Local again.
    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    make_local(
        &db,
        storage.storage.clone(),
        &lib,
        None,
        None,
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect("make_local");
    // The retract cycle writes the tombstone.
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;
    assert_eq!(shared_flag(&db, "n1").await, 0, "Local after make_local");
    assert!(
        storage
            .home
            .exists(&exact_tombstone_key(&first_remote))
            .await
            .unwrap(),
        "the make_local tombstoned the cloud blob",
    );

    // Second make_remote: the external file is uploaded to a new exact object and
    // the gate flips back on.
    make_remote(&db, &lib, "notes", "n1", true)
        .await
        .expect("make_remote 2");
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;
    assert_eq!(
        shared_flag(&db, "n1").await,
        1,
        "Remote again after the second make_remote"
    );
    assert!(
        external_blob(&db, "note_photos", "photoaaa")
            .await
            .is_none(),
        "external ref cleared"
    );
    let second_remote = photo_ref(&db, "photoaaa")
        .await
        .stored()
        .cloned()
        .expect("second Remote photo has exact storage authority");
    assert_ne!(second_remote.object(), first_remote.object());
    assert!(
        storage
            .home
            .exists(&exact_tombstone_key(&first_remote))
            .await
            .unwrap(),
        "the old exact object's tombstone remains valid",
    );
    storage
        .verify_blob_object(&second_remote)
        .await
        .expect("the replacement exact blob is in the cloud");
}
