//! Tests for the coven-owned make-Remote / make-Local transitions.
//!
//! These drive the real transition functions ([`make_remote`],
//! [`cancel_make_remote`], [`make_local`]) and the upload drain's completion
//! flip against a real [`Database`] and a [`MockSyncStorage`] that serves as both
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

use async_trait::async_trait;
use tokio::sync::{watch, Notify};

use crate::blob::transition::{cancel_make_remote, make_local, make_remote};
use crate::blob::upload::drain_uploads;
use crate::blob::{
    cache, local_files, BlobRef, BlobScope, BlobTransitionObserver, CacheFill, Provenance,
};
use crate::clock::SystemClock;
use crate::database::Database;
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::cycle::{run_single_sync_cycle, SyncCycleResult};
use crate::sync::hlc::Hlc;
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    host_exec as exec, open_test_db, open_test_db_schema, open_test_db_with_blob,
    open_test_db_with_user_and_host_blobs, query_text, row_exists, temp_store_dir,
    test_blob_location, test_migrations, MockSyncStorage,
};

/// The uploader these browsable-home tests pass to the make_remote/cancel paths.
/// A `Plain` home carries no uploader segment, so the value is ignored — it exists
/// only to satisfy the parameter the hashed layout needs.
const SELF_UPLOADER: &str = "self-uploader";

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

fn remote_root_db(decl: BlobDecl) -> Database {
    open_test_db_schema(
        vec![
            SyncedTable::new("notes").remote_root(),
            SyncedTable::new("note_tags"),
            SyncedTable::new("note_photos").carries_blob(decl),
        ],
        test_migrations(),
    )
}

fn plaintext() -> RwLock<CloudCipher> {
    RwLock::new(CloudCipher::Plaintext)
}

/// The `BlobRef` a host builds to read a release file (matching `photo_decl`).
fn photo_ref(id: &str, cloud_path: &str) -> BlobRef {
    BlobRef {
        namespace: "photos".to_string(),
        id: id.to_string(),
        scope: BlobScope::Master,
        cloud_path: Some(cloud_path.to_string()),
        provenance: Provenance::UserProvided,
        fill: CacheFill::CacheLazy,
    }
}

/// The `BlobRef` a host builds to read a cover (matching `cover_decl`).
fn cover_ref(id: &str, cloud_path: &str) -> BlobRef {
    BlobRef {
        namespace: "covers".to_string(),
        id: id.to_string(),
        scope: BlobScope::Master,
        cloud_path: Some(cloud_path.to_string()),
        provenance: Provenance::HostProvided,
        fill: CacheFill::CacheEager,
    }
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

#[derive(Default)]
struct PauseAfterFirstMaterialize {
    callbacks: AtomicUsize,
    reached: Notify,
    resume: Notify,
}

#[async_trait]
impl BlobTransitionObserver for PauseAfterFirstMaterialize {
    async fn on_blob_upload_started(&self, _blob_id: &str) {}
    async fn on_blob_uploaded(&self, _blob_id: &str) {}
    async fn on_blob_upload_failed(&self, _blob_id: &str, _error: &str) {}
    async fn on_root_made_remote(&self, _root_table: &str, _root_id: &str) {}
    async fn on_root_made_local(&self, _root_table: &str, _root_id: &str) {}
    async fn on_blob_materialize_progress(
        &self,
        _root_table: &str,
        _root_id: &str,
        _blob_id: &str,
        _done: u64,
        _total: u64,
    ) {
        if self.callbacks.fetch_add(1, Ordering::SeqCst) == 0 {
            self.reached.notify_one();
            self.resume.notified().await;
        }
    }
}

/// Run one real sync cycle for `device`, with the mock wired as both storage and
/// cloud home so the upload drain, the gate, the tombstone drain, and the GC all run.
#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    storage: &MockSyncStorage,
    device: &str,
    hlc: &Hlc,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    kp: &UserKeypair,
    lib: &StoreDir,
    observer: Option<&dyn BlobTransitionObserver>,
) -> SyncCycleResult {
    // A fresh gate each call: none of these transition tests exercise a rotation
    // this device can't adopt.
    let pending_rotation = PendingRotation::none();
    run_single_sync_cycle(
        storage,
        "test-lib",
        device,
        hlc,
        &SystemClock,
        db,
        cipher,
        &pending_rotation,
        kp,
        None,
        lib,
        Some(storage as &dyn crate::storage::cloud::CloudHome),
        observer,
    )
    .await
    .expect("cycle")
}

/// Like [`run_cycle`] but surfaces the cycle result instead of unwrapping it, so a
/// test can drive a cycle expected to fail (e.g. a pull rejected by a schema floor).
#[allow(clippy::too_many_arguments)]
async fn try_run_cycle(
    storage: &MockSyncStorage,
    device: &str,
    hlc: &Hlc,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    kp: &UserKeypair,
    lib: &StoreDir,
) -> Result<SyncCycleResult, String> {
    let pending_rotation = PendingRotation::none();
    run_single_sync_cycle(
        storage,
        "test-lib",
        device,
        hlc,
        &SystemClock,
        db,
        cipher,
        &pending_rotation,
        kp,
        None,
        lib,
        Some(storage as &dyn crate::storage::cloud::CloudHome),
        None,
    )
    .await
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
    exec(
        db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('{note_id}', 'Release', NULL, {shared}, '0000000001000-0000-A', '2026-01-01')"
        ),
    )
    .await;
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
    db.register_external_blob(photo_id, "photos", &src, bytes.len() as u64)
        .await
        .expect("register external blob");
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
    db.register_external_blob(photo_id, "photos", &src, bytes.len() as u64)
        .await
        .unwrap();
    src
}

/// Insert a Remote release: a gated-on note plus a photo whose blob is already in
/// the cloud (plaintext, at the readable key the `Plain` scheme derives).
async fn seed_remote_release(
    storage: &MockSyncStorage,
    db: &Database,
    note_id: &str,
    photo_id: &str,
    cloud_path: &str,
    bytes: &[u8],
) {
    seed_release_rows(db, note_id, photo_id, cloud_path, 1, bytes).await;
    let location = test_blob_location(&storage.own_uploader().expect("mock uploader"), 1000);
    storage
        .put_blob(
            "photos",
            &location,
            photo_id,
            BlobScope::Master,
            Some(cloud_path),
            bytes.to_vec(),
        )
        .await
        .expect("seed cloud blob");
    // Record who uploaded it — the pull that introduced the row would record the
    // changeset author; here the row is seeded directly, so record the mock's
    // uploader so the read resolves the blob's prefix without a listing scan.
    db.record_blob_location("photos", photo_id, &location)
        .await
        .expect("record seeded blob location");
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

async fn pending_deletes(db: &Database) -> Vec<String> {
    db.get_pending_cloud_deletes()
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.cloud_key)
        .collect()
}

async fn recorded_blob_key(
    db: &Database,
    namespace: &str,
    blob_id: &str,
    cloud_path: &str,
) -> String {
    let location = db
        .blob_location(namespace, blob_id)
        .await
        .expect("read blob location")
        .expect("blob location is recorded");
    CloudSyncStorage::blob_key(
        BlobPathScheme::Plain,
        namespace,
        &location,
        blob_id,
        Some(cloud_path),
    )
    .expect("generated blob key")
}

async fn has_intent(db: &Database, root_table: &str, root_id: &str) -> bool {
    let (rt, ri) = (root_table.to_string(), root_id.to_string());
    db.call(move |conn| Database::make_remote_intent_exists(conn, &rt, &ri))
        .await
        .unwrap()
}

/// Insert a `published_blob_drop_intents` row directly, to reconstruct the durable
/// bookkeeping a crash leaves when a drain applies a disposition but dies before
/// clearing its intent.
async fn insert_published_drop_intent(
    db: &Database,
    seq: i64,
    namespace: &str,
    blob_id: &str,
    size: u64,
    disposition: &str,
) {
    let (ns, id, disp) = (
        namespace.to_string(),
        blob_id.to_string(),
        disposition.to_string(),
    );
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO published_blob_drop_intents (seq, namespace, blob_id, size, disposition) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![seq, ns, id, size as i64, disp],
        )
        .map(|_| ())
        .map_err(crate::database::DbError::from)
    })
    .await
    .expect("insert published blob drop intent");
}

async fn drop_intent_present(db: &Database, blob_id: &str) -> bool {
    row_exists(
        db,
        &format!("SELECT 1 FROM published_blob_drop_intents WHERE blob_id = '{blob_id}'"),
    )
    .await
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
    let storage = MockSyncStorage::with_keypair(kp_a.clone());
    let enc = plaintext();
    let hlc_a = Hlc::new("A".to_string());
    let db_a = open_test_db_with_blob(photo_decl());
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
    crate::sync::test_helpers::pull_into(&db_b, &storage, "B", &lib_b).await;
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a gated-off (Local) release does not reach a peer",
    );

    // A makes it Remote: enqueue the upload + intent, then the next cycle's drain
    // uploads the blob and flips the gate.
    make_remote(
        &db_a,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc_a,
        "notes",
        "n1",
        true,
    )
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
        db_a.external_blob("photoaaa").await.unwrap().is_none(),
        "the external ref is dropped on completion",
    );
    assert!(
        src.exists(),
        "the user-provided source file is left in place post-commit"
    );
    let pinned = lib_a.pinned_blob_path("photos", "photoaaa").unwrap();
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
    crate::sync::test_helpers::pull_into(&db_b, &storage, "B", &lib_b).await;
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B receives the release once its blobs are up and the gate flips",
    );
    let fetched = cache::read_blob(
        &db_b,
        &lib_b,
        Some(&storage),
        &photo_ref("photoaaa", "cv/photoaaa.jpg"),
    )
    .await
    .expect("B fetches the CacheLazy blob");
    assert_eq!(fetched, bytes, "B reads the original photo from the cloud");
}

/// Whether the blob `(namespace, id)` has a pending upload row in the outbox whose
/// `retain_pinned` is set — the per-row pin the drain honors.
async fn upload_retains_pin(db: &Database, id: &str) -> bool {
    use rusqlite::OptionalExtension;
    let id = id.to_string();
    db.call(move |conn| {
        Ok(conn
            .query_row(
                "SELECT retain_pinned FROM cloud_outbox \
                 WHERE operation = 'upload' AND file_id = ?1",
                [id],
                |r| r.get::<_, bool>(0),
            )
            .optional()?
            .unwrap_or(false))
    })
    .await
    .unwrap()
}

/// The `source_path` recorded on the blob's pending upload row.
async fn upload_source_path(db: &Database, id: &str) -> Option<String> {
    use rusqlite::OptionalExtension;
    let id = id.to_string();
    db.call(move |conn| {
        Ok(conn
            .query_row(
                "SELECT source_path FROM cloud_outbox \
                 WHERE operation = 'upload' AND file_id = ?1",
                [id],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    })
    .await
    .unwrap()
}

async fn upload_cloud_key(db: &Database, id: &str) -> String {
    use rusqlite::OptionalExtension;
    let id = id.to_string();
    db.call(move |conn| {
        Ok(conn
            .query_row(
                "SELECT cloud_key FROM cloud_outbox \
                 WHERE operation = 'upload' AND file_id = ?1",
                [id],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .expect("pending upload has an exact cloud key"))
    })
    .await
    .unwrap()
}

/// A second make_remote on the same still-Local root, before any cycle drains the
/// first one's queued upload, must carry its new pin choice through to the queued
/// blob: the enqueue upserts the row's `retain_pinned` rather than leaving the stale
/// value, so the drained upload pins.
#[tokio::test]
async fn re_enqueue_updates_the_pending_upload_pin() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp = UserKeypair::generate();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
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

    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        false,
    )
    .await
    .expect("make_remote pin=false");
    assert!(
        !upload_retains_pin(&db, "photoaaa").await,
        "the first make_remote queued the upload unpinned",
    );

    // A second make_remote with a pin, before the upload drains.
    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        true,
    )
    .await
    .expect("make_remote pin=true");
    assert!(
        upload_retains_pin(&db, "photoaaa").await,
        "the re-enqueue must update the queued upload's pin to the new call's value",
    );

    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert_eq!(shared_flag(&db, "n1").await, 1, "the release is Remote");
    assert!(
        lib.pinned_blob_path("photos", "photoaaa").unwrap().exists(),
        "the drained upload pins, honoring the second make_remote's choice",
    );
}

/// Re-registering a blob's external source before the upload drains, then a second
/// make_remote, must repoint the queued upload at the new path: the enqueue upserts
/// `source_path`, so the drain reads the current file rather than the stale (here
/// removed) one it would otherwise retry forever.
#[tokio::test]
async fn re_enqueue_updates_the_pending_upload_source_path() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp = UserKeypair::generate();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let bytes = b"PHOTO-relocate".to_vec();
    let user_dir = tmp.path().join("user");

    let src1 =
        seed_local_release(&db, &user_dir, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes).await;
    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        false,
    )
    .await
    .expect("first make_remote");
    assert_eq!(
        upload_source_path(&db, "photoaaa").await.as_deref(),
        src1.to_str(),
        "the upload is queued against the original source",
    );

    // The user moves the file: re-register it at a new path and remove the old one.
    let src2 = user_dir.join("relocated.jpg");
    std::fs::write(&src2, &bytes).unwrap();
    db.register_external_blob("photoaaa", "photos", &src2, bytes.len() as u64)
        .await
        .expect("re-register external blob");
    std::fs::remove_file(&src1).unwrap();

    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        false,
    )
    .await
    .expect("second make_remote");
    assert_eq!(
        upload_source_path(&db, "photoaaa").await.as_deref(),
        src2.to_str(),
        "the re-enqueue repoints the queued upload at the new source",
    );

    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert_eq!(
        shared_flag(&db, "n1").await,
        1,
        "the drain read the re-registered path and completed the make_remote",
    );
    assert!(
        storage
            .exists(&recorded_blob_key(&db, "photos", "photoaaa", "cv/photoaaa.jpg",).await)
            .await
            .unwrap(),
        "the blob uploaded from the new path",
    );
}

/// Record a make_remote intent directly, for the state a peer's independent flip
/// leaves: the root's gate is already true while this device's intent (with its pin
/// choice) is still live, which `make_remote` itself would refuse as AlreadyRemote.
async fn insert_intent(db: &Database, root_table: &str, root_id: &str, pin: bool) {
    let (rt, ri) = (root_table.to_string(), root_id.to_string());
    db.call(move |conn| Database::insert_make_remote_intent_on(conn, &rt, &ri, pin))
        .await
        .expect("insert make_remote intent");
}

/// Queue a user-provided upload and push it deep into backoff so the drain skips it
/// every cycle — a make_remote whose user blob never lands, keeping the intent live
/// and the pre-capture completion path (which requires no pending user upload) out
/// of the picture, so the inline push is the intent's consumer.
async fn queue_stuck_upload(db: &Database, file_id: &str, cloud_key: &str) {
    db.enqueue_upload(
        file_id,
        cloud_key,
        Some("/nonexistent"),
        BlobScope::Master,
        false,
        &crate::blob::content_hash(b"unavailable"),
        "0000000001000-0000-A",
    )
    .await
    .expect("enqueue upload");
    let file_id = file_id.to_string();
    db.call(move |conn| {
        conn.execute(
            "UPDATE cloud_outbox SET attempt_count = 9, last_attempt_at = '2999-01-01T00:00:00Z' \
             WHERE operation = 'upload' AND file_id = ?1",
            [file_id],
        )
        .map(|_| ())
        .map_err(crate::database::DbError::from)
    })
    .await
    .expect("force upload into backoff");
}

/// The inline-push path consumes a make_remote intent when it uploads a
/// host-provided blob whose root's intent is still live. That consumption must not
/// commit before the cycle durably records the pin disposition: it is deferred to the
/// push-state transaction. A cycle that fails after the inline upload (here a pull
/// rejected by a schema floor) must therefore leave the intent live, and the retry
/// records the pinned disposition and clears it.
#[tokio::test]
async fn inline_intent_consumption_survives_a_failed_cycle_then_records_the_pin() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp = UserKeypair::generate();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let (_tmp, lib) = temp_store_dir();
    let photo = b"PHOTO-stuck".to_vec();
    let cover = b"COVER-inline".to_vec();

    // A published Remote release with a user photo already in the cloud (no external
    // ref). The gate is on; nothing here has a cover yet.
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Release', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('photoaaa', 'n1', 'image', {}, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/photoaaa.jpg')",
            photo.len(),
            crate::blob::content_hash(&photo),
        ),
    )
    .await;
    let location = test_blob_location(&storage.own_uploader().expect("mock uploader"), 1000);
    storage
        .put_blob(
            "photos",
            &location,
            "photoaaa",
            BlobScope::Master,
            Some("cv/photoaaa.jpg"),
            photo.clone(),
        )
        .await
        .expect("seed cloud photo");
    db.record_blob_location("photos", "photoaaa", &location)
        .await
        .expect("record photo uploader");

    // Baseline: publish the release. Nothing is deferred (no cover, no intent).
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    // The state a peer's flip + a live local make_remote leaves: a live pinned intent
    // and a stuck user upload that keeps the pre-capture completion path skipping this
    // root. Then the host adds a cover (host-provided) under the already-shared root.
    insert_intent(&db, "notes", "n1", true).await;
    let photo_key = recorded_blob_key(&db, "photos", "photoaaa", "cv/photoaaa.jpg").await;
    queue_stuck_upload(&db, "photoaaa", &photo_key).await;
    exec(
        &db,
        &format!(
            "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coveraaa', 'n1', {}, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/cover-coveraaa.jpg')",
            cover.len(),
            crate::blob::content_hash(&cover),
        ),
    )
    .await;
    local_files::store(&lib, "covers", "coveraaa", &cover)
        .await
        .expect("store host-provided cover");

    // The cycle fails at the pull: a schema floor above this device's version. The
    // inline push has already uploaded the cover and consumed the intent in memory.
    storage
        .set_min_schema_version(db.schema_version() + 1)
        .await
        .expect("set schema floor");
    let failed = try_run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib).await;
    assert!(failed.is_err(), "the cycle fails at the pull");

    assert!(
        storage
            .exists(&recorded_blob_key(&db, "covers", "coveraaa", "cv/cover-coveraaa.jpg",).await)
            .await
            .unwrap(),
        "the inline push uploaded the cover before the pull failed",
    );
    assert!(
        has_intent(&db, "notes", "n1").await,
        "the intent survives a cycle that failed before recording the disposition",
    );
    assert!(
        !lib.pinned_blob_path("covers", "coveraaa").unwrap().exists(),
        "the pin disposition was not recorded, so nothing pinned the cover yet",
    );

    // Retry with the floor lifted: the cycle completes, records the pin disposition in
    // the same transaction it consumes the intent, and the drain applies the pin.
    storage
        .set_min_schema_version(db.schema_version())
        .await
        .expect("lift schema floor");
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert!(
        !has_intent(&db, "notes", "n1").await,
        "the completed retry consumed the intent",
    );
    assert!(
        lib.pinned_blob_path("covers", "coveraaa").unwrap().exists(),
        "the retry recorded and applied the pinned disposition",
    );
}

#[tokio::test]
async fn cancel_make_remote_after_completion_enqueues_no_deletes() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp = UserKeypair::generate();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
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
    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        false,
    )
    .await
    .expect("make_remote");
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert_eq!(shared_flag(&db, "n1").await, 1, "the root is Remote");
    assert!(
        !has_intent(&db, "notes", "n1").await,
        "completion deleted the make_remote intent",
    );
    assert!(
        storage
            .exists(&recorded_blob_key(&db, "photos", "photoaaa", "cv/photoaaa.jpg",).await)
            .await
            .unwrap(),
        "the published blob exists in cloud",
    );

    cancel_make_remote(&db, &lib, BlobPathScheme::Plain, &hlc, "notes", "n1")
        .await
        .expect("cancel after completion");

    assert!(
        pending_deletes(&db).await.is_empty(),
        "a cancel racing after completion must not tombstone published blobs",
    );
    assert!(
        storage
            .exists(&recorded_blob_key(&db, "photos", "photoaaa", "cv/photoaaa.jpg",).await)
            .await
            .unwrap(),
        "the cloud blob remains present",
    );
}

#[tokio::test]
async fn make_local_rejects_same_length_corrupt_cache_before_tombstoning_cloud() {
    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let bytes = b"managed-cloud-bytes".to_vec();
    let corrupt = vec![b'x'; bytes.len()];

    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes).await;
    let cache_path = lib.cache_blob_path("photos", "photoaaa").unwrap();
    std::fs::create_dir_all(cache_path.parent().unwrap()).unwrap();
    std::fs::write(&cache_path, &corrupt).unwrap();

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    let cloud_key = recorded_blob_key(&db, "photos", "photoaaa", "cv/photoaaa.jpg").await;

    make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
        None,
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect("make_local must fetch the signed bytes instead of publishing corrupt cache bytes");

    assert_eq!(std::fs::read(&dest_path).unwrap(), bytes);
    assert_eq!(storage.blob_read_to_file_count(), 1);
    assert_eq!(shared_flag(&db, "n1").await, 0);
    assert_eq!(pending_deletes(&db).await, vec![cloud_key]);
}

/// A makes a Remote release Local. B's subtree is DELETEd (gate retract) and the
/// cloud blob is tombstoned, while A keeps the external file and reads from it.
#[tokio::test]
async fn multi_device_make_local_retracts_peer_and_tombstones_cloud() {
    let kp_a = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(kp_a.clone());
    let enc = plaintext();
    let kp_b = UserKeypair::generate();
    let hlc_a = Hlc::new("A".to_string());
    let hlc_b = Hlc::new("B".to_string());
    let db_a = open_test_db_with_blob(photo_decl());
    let db_b = open_test_db_with_blob(photo_decl());
    let (tmp_a, lib_a) = temp_store_dir();
    let (_tmp_b, lib_b) = temp_store_dir();
    let bytes = b"MANAGED-PHOTO-going-back-local".to_vec();

    seed_remote_release(&storage, &db_a, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes).await;
    // B is a known peer (it has a head) that has not acked yet, so A's snapshot
    // cycle keeps A's release changeset for B to pull — reclamation is paused until
    // every current device acks. Without a peer head A would be single-device and
    // the snapshot-covered changeset would be reclaimed, leaving nothing for B's
    // incremental pull.
    storage
        .put_head("B", 0, "2024-01-01T00:00:00Z")
        .await
        .expect("seed peer head");

    // A pushes the Remote release; B pulls it.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;
    crate::sync::test_helpers::pull_into(&db_b, &storage, "B", &lib_b).await;
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B has the Remote release",
    );

    // A makes it Local to a chosen folder.
    let dest_dir = tmp_a.path().join("dest");
    let dest_path = dest_dir.join("photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    let recorder = Recorder::default();
    let photo_key = recorded_blob_key(&db_a, "photos", "photoaaa", "cv/photoaaa.jpg").await;
    make_local(
        &db_a,
        &storage,
        &lib_a,
        BlobPathScheme::Plain,
        &hlc_a,
        Some(&recorder),
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect("make_local");

    assert_eq!(
        storage.blob_read_to_file_count(),
        1,
        "make_local materializes the Remote blob through the file download path",
    );
    assert_eq!(shared_flag(&db_a, "n1").await, 0, "A's release is Local");
    assert_eq!(
        std::fs::read(&dest_path).unwrap(),
        bytes,
        "the file is materialized to the chosen folder",
    );
    assert_eq!(
        db_a.external_blob("photoaaa").await.unwrap().unwrap().path,
        dest_path,
        "A now reads the blob from the external file",
    );
    assert_eq!(
        pending_deletes(&db_a).await,
        vec![photo_key.clone()],
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
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;
    assert!(
        storage
            .exists(&format!("blob_tombstones/{photo_key}"))
            .await
            .unwrap(),
        "the cloud blob is tombstoned",
    );

    // B's next cycle pulls the retract: its subtree disappears.
    run_cycle(&storage, "B", &hlc_b, &db_b, &enc, &kp_b, &lib_b, None).await;
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B's subtree is removed by the gate retract",
    );

    // A still reads the photo from its external file (no cloud copy needed).
    let read = cache::read_blob(
        &db_a,
        &lib_a,
        Some(&storage),
        &photo_ref("photoaaa", "cv/photoaaa.jpg"),
    )
    .await
    .expect("A reads from its external file");
    assert_eq!(read, bytes, "A plays its own local file");
}

// ===========================================================================
// Host-provided lifecycle (the cover rides the inline push, not the outbox)
// ===========================================================================

/// A release with a user-provided photo file AND a host-provided cover, through both
/// transitions. make_remote: the photo uploads via the outbox and flips the gate;
/// the gate flip re-emits the subtree and the cycle's inline push uploads the cover
/// (host-provided) from the local store and pins that copy. A peer
/// pulls the cover eagerly (`CacheEager`) into its cache. make_local: the photo goes
/// back to its dest (external ref) and the cover back to the local store (NO dest),
/// both cloud copies tombstoned.
#[tokio::test]
async fn host_provided_cover_rides_the_inline_push_through_both_transitions() {
    let kp_a = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(kp_a.clone());
    let enc = plaintext();
    let hlc_a = Hlc::new("A".to_string());
    let db_a = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let (tmp_a, lib_a) = temp_store_dir();
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
    local_files::store(&lib_a, "covers", "coveraaa", &cover)
        .await
        .expect("store the host-provided cover in the local store");

    // A cycle while gated off: nothing reaches a peer.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;

    // make_remote: the photo drains, the gate flips, and this cycle's inline push
    // uploads the cover from the local store and keeps the requested pin.
    make_remote(
        &db_a,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc_a,
        "notes",
        "n1",
        true,
    )
    .await
    .expect("make_remote");
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;

    assert_eq!(shared_flag(&db_a, "n1").await, 1, "the release is Remote");
    assert!(
        storage
            .exists(&recorded_blob_key(&db_a, "covers", "coveraaa", "cv/cover-coveraaa.jpg",).await)
            .await
            .unwrap(),
        "the host-provided cover is uploaded to the cloud",
    );
    assert!(
        lib_a
            .pinned_blob_path("covers", "coveraaa")
            .unwrap()
            .exists(),
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
    let db_b = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let (_tmp_b, lib_b) = temp_store_dir();
    crate::sync::test_helpers::pull_into(&db_b, &storage, "B", &lib_b).await;
    assert!(
        lib_b
            .cache_blob_path("covers", "coveraaa")
            .unwrap()
            .exists(),
        "B fetches the CacheEager cover eagerly into its cache",
    );
    assert!(
        !lib_b
            .cache_blob_path("photos", "photoaaa")
            .unwrap()
            .exists()
            && !lib_b
                .pinned_blob_path("photos", "photoaaa")
                .unwrap()
                .exists(),
        "B does not fetch the CacheLazy photo on pull",
    );
    assert_eq!(
        cache::read_blob(
            &db_b,
            &lib_b,
            Some(&storage),
            &cover_ref("coveraaa", "cv/cover-coveraaa.jpg")
        )
        .await
        .expect("B reads the cover"),
        cover,
        "B's cover bytes match",
    );

    let photo_key = CloudSyncStorage::blob_key(
        BlobPathScheme::Plain,
        "photos",
        &db_a
            .blob_location("photos", "photoaaa")
            .await
            .unwrap()
            .unwrap(),
        "photoaaa",
        Some("cv/photoaaa.jpg"),
    )
    .unwrap();
    let cover_key = CloudSyncStorage::blob_key(
        BlobPathScheme::Plain,
        "covers",
        &db_a
            .blob_location("covers", "coveraaa")
            .await
            .unwrap()
            .unwrap(),
        "coveraaa",
        Some("cv/cover-coveraaa.jpg"),
    )
    .unwrap();

    // make_local: the photo back to its dest (external ref), the cover back to the
    // local store (no dest), both cloud copies tombstoned.
    let dest_path = tmp_a.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    make_local(
        &db_a,
        &storage,
        &lib_a,
        BlobPathScheme::Plain,
        &hlc_a,
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
        db_a.external_blob("photoaaa").await.unwrap().unwrap().path,
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
        db_a.external_blob("coveraaa").await.unwrap().is_none(),
        "the host-provided cover registers NO external ref",
    );
    let mut deletes = pending_deletes(&db_a).await;
    deletes.sort();
    assert_eq!(
        deletes,
        {
            let mut expected = vec![cover_key, photo_key];
            expected.sort();
            expected
        },
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
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp_a = UserKeypair::generate();
    let hlc_a = Hlc::new("A".to_string());
    let db_a = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
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
    local_files::store(&lib_a, "covers", "coverhost", &cover)
        .await
        .expect("store host-provided cover");

    let before = cache::read_blob(
        &db_a,
        &lib_a,
        Some(&storage),
        &cover_ref("coverhost", "cv/host-coverhost.jpg"),
    )
    .await
    .expect("read Local host-provided cover");
    assert_eq!(before, cover);

    make_remote(
        &db_a,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc_a,
        "notes",
        "n-host",
        true,
    )
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
    assert_eq!(pending_uploads(&db_a).await, 0);
    assert!(
        storage.list("covers/").await.unwrap().is_empty(),
        "the host-provided blob is not published before the cycle uploads it"
    );

    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;

    assert_eq!(
        shared_flag(&db_a, "n-host").await,
        1,
        "the gate flips after the host-provided blob lands"
    );
    assert!(
        storage
            .exists(
                &recorded_blob_key(&db_a, "covers", "coverhost", "cv/host-coverhost.jpg",).await
            )
            .await
            .unwrap(),
        "inline push uploads the host-provided blob"
    );
    assert_eq!(
        storage.blob_put_from_file_count(),
        1,
        "inline push uploads the host-provided blob through the file upload path",
    );
    assert!(
        !has_intent(&db_a, "notes", "n-host").await,
        "inline upload consumes the make_remote intent"
    );
    assert!(
        lib_a
            .pinned_blob_path("covers", "coverhost")
            .unwrap()
            .exists(),
        "pin=true keeps the host-provided blob in the protected cache"
    );
    assert!(
        !lib_a
            .local_blob_path("covers", "coverhost")
            .unwrap()
            .exists(),
        "after Remote upload the local store no longer holds the blob"
    );
    let after = cache::read_blob(
        &db_a,
        &lib_a,
        Some(&storage),
        &cover_ref("coverhost", "cv/host-coverhost.jpg"),
    )
    .await
    .expect("read Remote host-provided cover");
    assert_eq!(after, cover);
}

/// A crash between a host-provided make_remote's gate flip and its local-store
/// disposition must not strand the blob. The flip commits the disposition as a
/// durable intent, so a cycle whose push fails (the flip is committed, the drain
/// that applies the disposition never runs) leaves the disposition pending — the
/// local copy untouched — and the recovery cycle's staged retry drains it: the
/// pinned blob reaches `pinned/` and the dropped blob's local copy is gone.
#[tokio::test]
async fn host_provided_make_remote_disposition_survives_crash_before_drain() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp = UserKeypair::generate();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_user_and_host_blobs(photo_decl(), cover_lazy_decl());
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
        local_files::store(&lib, "covers", cover, bytes)
            .await
            .expect("store host-provided cover");
    }
    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n-pin",
        true,
    )
    .await
    .expect("make_remote pin");
    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n-drop",
        false,
    )
    .await
    .expect("make_remote drop");

    // The crash: the changeset push fails, so the gate flip commits (with its
    // disposition intent) but the drain that applies the disposition never runs.
    storage.fail_next_changeset_puts(1);
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

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
            "SELECT 1 FROM published_blob_drop_intents WHERE blob_id = 'cover-pin'",
        )
        .await,
        "the pin disposition is committed durably with the flip, not applied in memory",
    );
    assert!(
        !lib.pinned_blob_path("covers", "cover-pin")
            .unwrap()
            .exists(),
        "the disposition is deferred to the drain, so the pin has not been applied yet",
    );
    assert!(
        lib.local_blob_path("covers", "cover-drop")
            .unwrap()
            .exists(),
        "the dropped cover's local copy is still present until the drain runs",
    );

    // Recovery: the staged changeset re-pushes and the drain applies both dispositions.
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert!(
        lib.pinned_blob_path("covers", "cover-pin")
            .unwrap()
            .exists(),
        "recovery pins the retained cover",
    );
    assert!(
        cache::is_pinned(&lib, "covers", "cover-pin").await.unwrap(),
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
        !cache::is_pinned(&lib, "covers", "cover-drop")
            .await
            .unwrap(),
        "the dropped cover is not pinned",
    );
    assert!(
        storage
            .exists(&recorded_blob_key(&db, "covers", "cover-pin", "cv/cover-pin.jpg").await)
            .await
            .unwrap()
            && storage
                .exists(&recorded_blob_key(&db, "covers", "cover-drop", "cv/cover-drop.jpg").await,)
                .await
                .unwrap(),
        "both covers are published to the cloud",
    );
}

/// The drain applies a disposition (copy to the destination, drop the local-store
/// source) and then clears its intent in a separate commit. A crash in that window
/// leaves the blob correctly placed but the intent uncleared. Re-draining must
/// recognize the completed work — the blob already in pinned/ — and clear the intent,
/// not keep failing every cycle because the source it would copy is gone.
#[tokio::test]
async fn drain_clears_a_pin_disposition_already_applied_before_its_intent() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp = UserKeypair::generate();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db();
    let (_tmp, lib) = temp_store_dir();
    let bytes = b"ALREADY-PINNED".to_vec();

    let pinned = lib.pinned_blob_path("covers", "cov-pin").unwrap();
    crate::local_blob::create_dir_all(pinned.parent().unwrap())
        .await
        .unwrap();
    crate::local_blob::write_atomic(&pinned, &bytes)
        .await
        .unwrap();
    db.set_sync_state("local_seq", "1").await.unwrap();
    insert_published_drop_intent(&db, 1, "covers", "cov-pin", bytes.len() as u64, "pin").await;

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
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp = UserKeypair::generate();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db();
    let (_tmp, lib) = temp_store_dir();
    let bytes = b"ALREADY-CACHED".to_vec();

    let cached = lib.cache_blob_path("covers", "cov-cache").unwrap();
    crate::local_blob::create_dir_all(cached.parent().unwrap())
        .await
        .unwrap();
    crate::local_blob::write_atomic(&cached, &bytes)
        .await
        .unwrap();
    db.set_sync_state("local_seq", "1").await.unwrap();
    insert_published_drop_intent(&db, 1, "covers", "cov-cache", bytes.len() as u64, "cache").await;

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
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp = UserKeypair::generate();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db();
    let (_tmp, lib) = temp_store_dir();

    db.set_sync_state("local_seq", "1").await.unwrap();
    insert_published_drop_intent(&db, 1, "covers", "cov-lost", 7, "pin").await;

    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;

    assert!(
        drop_intent_present(&db, "cov-lost").await,
        "a disposition missing from both the local store and its destination stays pending",
    );
    assert!(
        !lib.pinned_blob_path("covers", "cov-lost").unwrap().exists(),
        "no destination copy was conjured",
    );
}

#[tokio::test]
async fn remote_root_host_provided_blob_uploads_before_peer_reads_the_row() {
    let kp_a = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(kp_a.clone());
    let enc = plaintext();
    let hlc_a = Hlc::new("A".to_string());
    let db_a = remote_root_db(cover_decl());
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
    local_files::store(&lib_a, "covers", "coverrrr", &cover)
        .await
        .expect("store host-provided blob");

    storage
        .put_head("B", 0, "2024-01-01T00:00:00Z")
        .await
        .expect("seed peer head");
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;
    assert!(
        storage
            .exists(
                &recorded_blob_key(&db_a, "covers", "coverrrr", "cv/remote-root-coverrrr.jpg",)
                    .await
            )
            .await
            .unwrap(),
        "the host-provided blob is uploaded before the row changeset is pushed"
    );

    let db_b = remote_root_db(cover_decl());
    let (_tmp_b, lib_b) = temp_store_dir();
    crate::sync::test_helpers::pull_into(&db_b, &storage, "B", &lib_b).await;
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n-remote-root'").await,
        "the peer receives the remote-root row"
    );
    assert!(
        lib_b
            .cache_blob_path("covers", "coverrrr")
            .unwrap()
            .exists(),
        "the peer eagerly caches the host-provided blob"
    );
    let got = cache::read_blob(
        &db_b,
        &lib_b,
        Some(&storage),
        &cover_ref("coverrrr", "cv/remote-root-coverrrr.jpg"),
    )
    .await
    .expect("peer reads the remote-root blob");
    assert_eq!(got, cover);
}

#[tokio::test]
async fn make_remote_rejects_remote_root() {
    let hlc = Hlc::new("A".to_string());
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
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at, cloud_path) \
         VALUES ('coverrrr', 'n-remote-root', 'cover', '0000000001000-0000-A', '2026-01-01', 'cv/remote-root-coverrrr.jpg')",
    )
    .await;
    local_files::store(&lib, "covers", "coverrrr", b"REMOTE-ROOT")
        .await
        .expect("store host-provided blob");

    let err = make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n-remote-root",
        true,
    )
    .await
    .expect_err("remote roots have no make_remote transition");
    assert!(
        matches!(err, crate::blob::transition::MakeRemoteError::RemoteRoot(_)),
        "make_remote rejects a remote root specifically: {err:?}"
    );
}

#[tokio::test]
async fn make_local_rejects_remote_root() {
    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = remote_root_db(cover_decl());
    let (tmp, lib) = temp_store_dir();
    exec(
        &db,
        "INSERT INTO notes (id, title, _updated_at, created_at) \
         VALUES ('n-remote-root', 'Remote Root', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at, cloud_path) \
         VALUES ('coverrrr', 'n-remote-root', 'cover', '0000000001000-0000-A', '2026-01-01', 'cv/remote-root-coverrrr.jpg')",
    )
    .await;
    let location = test_blob_location(&storage.own_uploader().unwrap(), 1000);
    storage
        .put_blob(
            "covers",
            &location,
            "coverrrr",
            BlobScope::Master,
            Some("cv/remote-root-coverrrr.jpg"),
            b"REMOTE-ROOT".to_vec(),
        )
        .await
        .expect("seed remote blob");
    db.record_blob_location("covers", "coverrrr", &location)
        .await
        .unwrap();
    let dest: HashMap<String, PathBuf> =
        [("coverrrr".to_string(), tmp.path().join("dest/coverrrr.jpg"))].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
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
    let hlc = Hlc::new("A".to_string());
    let db = remote_root_db(cover_decl());
    let (_tmp, lib) = temp_store_dir();

    let err = cancel_make_remote(
        &db,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
        "notes",
        "n-remote-root",
    )
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
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let (_tmp, _lib) = temp_store_dir();

    // A host-provided-only root already Remote (gate on): a note plus a cover row.
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n-host', 'Host Only', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        "INSERT INTO note_covers (id, note_id, size, _updated_at, created_at, cloud_path) \
         VALUES ('coverhost', 'n-host', 15, '0000000001000-0000-A', '2026-01-01', 'cv/host-coverhost.jpg')",
    )
    .await;

    let stamp_before = gate_stamp(&db, "n-host").await;

    let err = make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n-host",
        true,
    )
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
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, _lib) = temp_store_dir();
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

    let err = make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        true,
    )
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
        db.external_blob("photoaaa").await.unwrap().is_some(),
        "the external blob ref survives the aborted transition",
    );
}

#[tokio::test]
async fn make_remote_aborts_when_source_bytes_change_without_changing_length() {
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, _lib) = temp_store_dir();
    let bytes = b"registered-content".to_vec();
    let src = seed_local_release(
        &db,
        &tmp.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.jpg",
        &bytes,
    )
    .await;
    std::fs::write(&src, vec![b'x'; bytes.len()]).expect("replace source at equal length");

    let error = make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        true,
    )
    .await
    .expect_err("same-length content drift must abort before enqueue");

    assert!(matches!(
        error,
        crate::blob::transition::MakeRemoteError::Source { .. }
    ));
    assert_eq!(pending_uploads(&db).await, 0);
    assert!(!has_intent(&db, "notes", "n1").await);
    assert_eq!(shared_flag(&db, "n1").await, 0);
    assert_eq!(std::fs::read(src).unwrap(), vec![b'x'; bytes.len()]);
}

/// make_local on a root already Local is refused at the API before any
/// materialization: nothing is registered, no delete is queued, the gate row is
/// untouched. Without the precondition, make_local would try to read the blob from
/// the cloud and fail deep in materialization with a misleading cloud-read error.
#[tokio::test]
async fn make_local_rejects_already_local_root() {
    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
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
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
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

/// Cancelling an in-flight make_remote clears the intent and the still-pending uploads,
/// and tombstones any blob that already landed. The gate never flips.
#[tokio::test]
async fn cancel_make_remote_clears_pending_and_tombstones_uploaded() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let user = tmp.path().join("user");

    // Two photos under one release.
    let _src1 = seed_local_release(&db, &user, "n1", "photoaaa", "cv/photoaaa.jpg", b"first").await;
    let src2 = add_local_photo(&db, &user, "n1", "photobbb", "cv/photobbb.jpg", b"second").await;

    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        true,
    )
    .await
    .expect("make_remote");
    assert_eq!(pending_uploads(&db).await, 2, "both uploads queued");

    // Drain with photobbb's source removed: photoaaa uploads (not the last, no flip), photobbb fails.
    std::fs::remove_file(&src2).unwrap();
    drain_uploads(
        &db,
        &storage,
        &enc,
        &PendingRotation::none(),
        "test-lib",
        &lib,
        &SystemClock,
        &hlc,
        None,
    )
    .await
    .expect("partial drain");
    assert_eq!(
        shared_flag(&db, "n1").await,
        0,
        "not flipped — photobbb never uploaded"
    );
    let uploaded_key = recorded_blob_key(&db, "photos", "photoaaa", "cv/photoaaa.jpg").await;
    let pending_key = upload_cloud_key(&db, "photobbb").await;
    assert!(
        storage.exists(&uploaded_key).await.unwrap(),
        "photoaaa is in the cloud"
    );
    assert!(
        has_intent(&db, "notes", "n1").await,
        "the make_remote is still in flight"
    );
    // Cancel: the gate stays off, photoaaa (already uploaded) is tombstoned and its pinned
    // copy dropped, photobbb's pending upload is removed, the intent is cleared.
    cancel_make_remote(&db, &lib, BlobPathScheme::Plain, &hlc, "notes", "n1")
        .await
        .expect("cancel make_remote");
    assert_eq!(shared_flag(&db, "n1").await, 0, "the release stays Local");
    assert!(
        !has_intent(&db, "notes", "n1").await,
        "the intent is cleared"
    );
    assert_eq!(pending_uploads(&db).await, 0, "no uploads remain");
    let mut deletes = pending_deletes(&db).await;
    deletes.sort();
    let mut expected_deletes = vec![uploaded_key, pending_key];
    expected_deletes.sort();
    assert_eq!(
        deletes, expected_deletes,
        "every upload key owned by the cancelled operation is tombstoned",
    );
    assert!(
        !lib.pinned_blob_path("photos", "photoaaa").unwrap().exists(),
        "the orphan's pinned cache copy is dropped",
    );
}

/// The drain's cancel-in-gap path: an upload whose gated root has no make_remote
/// intent (a make_remote cancelled while this blob was in flight) is tombstoned and its cache
/// dropped, not flipped.
#[tokio::test]
async fn drain_orphan_upload_is_tombstoned_when_intent_gone() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
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
    let orphan_location = test_blob_location(&storage.own_uploader().unwrap(), 1000);
    let orphan_key = CloudSyncStorage::blob_key(
        BlobPathScheme::Plain,
        "photos",
        &orphan_location,
        "photoaaa",
        Some("cv/photoaaa.jpg"),
    )
    .unwrap();
    // Enqueue the upload with NO intent (models a make_remote whose intent + pending row
    // were cancelled, but this blob was already in flight in the drain).
    db.enqueue_upload(
        "photoaaa",
        &orphan_key,
        Some(src.to_str().unwrap()),
        BlobScope::Master,
        true,
        &crate::blob::content_hash(b"orphan-bytes"),
        "0000000001000-0000-A",
    )
    .await
    .unwrap();

    drain_uploads(
        &db,
        &storage,
        &enc,
        &PendingRotation::none(),
        "test-lib",
        &lib,
        &SystemClock,
        &hlc,
        None,
    )
    .await
    .expect("drain");

    assert_eq!(shared_flag(&db, "n1").await, 0, "no intent ⇒ no flip");
    assert_eq!(
        pending_deletes(&db).await,
        vec![orphan_key],
        "the orphan blob is tombstoned",
    );
    assert!(
        !lib.pinned_blob_path("photos", "photoaaa").unwrap().exists(),
        "the orphan's cache copy is dropped",
    );
}

/// Cancelling a make_local before the commit deletes the partial dest copies and
/// leaves the release Remote with nothing tombstoned.
#[tokio::test]
async fn cancel_make_local_before_commit_stays_remote() {
    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let bytes = b"still-managed".to_vec();

    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes).await;

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    // Already cancelled (initial value true) before the first materialize.
    let (_cancel_tx, cancel) = watch::channel(true);

    let err = make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
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
        db.external_blob("photoaaa").await.unwrap().is_none(),
        "no external ref registered"
    );
    assert!(pending_deletes(&db).await.is_empty(), "nothing tombstoned");
    assert!(!dest_path.exists(), "no partial dest copy left behind");
}

/// A make_local that can't write a dest file aborts before the commit: the release
/// stays Remote, the cloud blob is untouched, and no delete is queued.
#[tokio::test]
async fn make_local_dest_failure_stays_remote_no_tombstones() {
    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let bytes = b"managed-bytes".to_vec();

    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes).await;

    // Block the dest: make the dest's parent dir a FILE, so create_dir_all fails.
    let blocker = tmp.path().join("blocker");
    std::fs::write(&blocker, b"x").unwrap();
    let dest_path = blocker.join("photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path)].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
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
        db.external_blob("photoaaa").await.unwrap().is_none(),
        "no external ref"
    );
    assert!(pending_deletes(&db).await.is_empty(), "no tombstone queued");
    let location = db
        .blob_location("photos", "photoaaa")
        .await
        .unwrap()
        .unwrap();
    assert!(
        storage
            .get_blob(
                "photos",
                &location,
                "photoaaa",
                BlobScope::Master,
                Some("cv/photoaaa.jpg")
            )
            .await
            .is_ok(),
        "the cloud blob is untouched",
    );
}

#[tokio::test]
async fn make_local_db_commit_failure_retains_a_replaced_user_destination() {
    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let bytes = b"managed-bytes".to_vec();
    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes).await;

    db.call(|conn| {
        conn.execute_batch(
            "CREATE TRIGGER reject_make_local \
             BEFORE UPDATE OF shared ON notes \
             BEGIN SELECT RAISE(ABORT, 'forced make_local commit failure'); END;",
        )
        .map_err(crate::database::DbError::from)
    })
    .await
    .expect("install commit failure trigger");

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    let observer = PauseAfterFirstMaterialize::default();
    let replacement = b"replacement-owned-by-another-process".to_vec();
    let make_local = make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
        Some(&observer),
        "notes",
        "n1",
        &dest,
        &cancel,
    );
    let replace_destination = async {
        observer.reached.notified().await;
        std::fs::remove_file(&dest_path).expect("remove Coven's published file");
        std::fs::write(&dest_path, &replacement).expect("publish another process's replacement");
        observer.resume.notify_one();
    };
    let (result, ()) = tokio::join!(make_local, replace_destination);
    let error = result.expect_err("database commit failure must abort make_local");

    assert!(matches!(
        &error,
        crate::blob::transition::MakeLocalError::PartialMaterialization {
            retained_paths,
            ..
        } if retained_paths == &vec![dest_path.clone()]
    ));
    assert_eq!(
        std::fs::read(&dest_path).unwrap(),
        replacement,
        "rollback must not unlink a caller-owned pathname after it has been published",
    );
    assert_eq!(shared_flag(&db, "n1").await, 1);
    assert!(db.external_blob("photoaaa").await.unwrap().is_none());
    assert!(pending_deletes(&db).await.is_empty());
}

#[tokio::test]
async fn make_local_db_commit_failure_removes_host_local_store_file() {
    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(cover_decl());
    let (_tmp, lib) = temp_store_dir();
    let bytes = b"managed-cover".to_vec();
    seed_release_rows(&db, "n1", "coveraaa", "cv/coveraaa.jpg", 1, &bytes).await;
    let location = test_blob_location(&storage.own_uploader().unwrap(), 1000);
    storage
        .put_blob(
            "covers",
            &location,
            "coveraaa",
            BlobScope::Master,
            Some("cv/coveraaa.jpg"),
            bytes,
        )
        .await
        .unwrap();
    db.record_blob_location("covers", "coveraaa", &location)
        .await
        .unwrap();
    db.call(|conn| {
        conn.execute_batch(
            "CREATE TRIGGER reject_host_make_local \
             BEFORE UPDATE OF shared ON notes \
             BEGIN SELECT RAISE(ABORT, 'forced make_local commit failure'); END;",
        )
        .map_err(crate::database::DbError::from)
    })
    .await
    .unwrap();

    let (_cancel_tx, cancel) = watch::channel(false);
    let error = make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
        None,
        "notes",
        "n1",
        &HashMap::new(),
        &cancel,
    )
    .await
    .expect_err("database commit failure aborts host make_local");

    assert!(matches!(
        error,
        crate::blob::transition::MakeLocalError::Db(_)
    ));
    assert!(!lib.local_blob_path("covers", "coveraaa").unwrap().exists());
    assert_eq!(shared_flag(&db, "n1").await, 1);
    assert!(pending_deletes(&db).await.is_empty());
}

#[tokio::test]
async fn make_local_refuses_to_replace_an_existing_destination() {
    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let bytes = b"managed-bytes".to_vec();
    let existing = b"unrelated-user-bytes".to_vec();
    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes).await;

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    std::fs::create_dir_all(dest_path.parent().unwrap()).unwrap();
    std::fs::write(&dest_path, &existing).unwrap();
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
        None,
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect_err("an existing destination is owned by the user");

    assert!(err.to_string().contains("already exists"));
    assert_eq!(std::fs::read(&dest_path).unwrap(), existing);
    assert_eq!(shared_flag(&db, "n1").await, 1);
    assert!(db.external_blob("photoaaa").await.unwrap().is_none());
    assert!(pending_deletes(&db).await.is_empty());
    let location = db
        .blob_location("photos", "photoaaa")
        .await
        .unwrap()
        .unwrap();
    assert!(storage
        .get_blob(
            "photos",
            &location,
            "photoaaa",
            BlobScope::Master,
            Some("cv/photoaaa.jpg")
        )
        .await
        .is_ok());
}

#[tokio::test]
async fn make_local_retains_the_published_path_when_two_blobs_share_a_destination() {
    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let first = b"first-blob".to_vec();
    let second = b"second-blob".to_vec();
    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.jpg", &first).await;
    exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('photobbb', 'n1', 'image', {}, '{}', '0000000001001-0000-A', '2026-01-01', 'cv/photobbb.jpg')",
            second.len(),
            crate::blob::content_hash(&second),
        ),
    )
    .await;
    let second_location = test_blob_location(&storage.own_uploader().unwrap(), 1001);
    storage
        .put_blob(
            "photos",
            &second_location,
            "photobbb",
            BlobScope::Master,
            Some("cv/photobbb.jpg"),
            second.clone(),
        )
        .await
        .unwrap();
    db.record_blob_location("photos", "photobbb", &second_location)
        .await
        .unwrap();

    let destination = tmp.path().join("dest/shared.jpg");
    let dest: HashMap<String, PathBuf> = [
        ("photoaaa".to_string(), destination.clone()),
        ("photobbb".to_string(), destination.clone()),
    ]
    .into();
    let (_cancel_tx, cancel) = watch::channel(false);
    let error = make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
        None,
        "notes",
        "n1",
        &dest,
        &cancel,
    )
    .await
    .expect_err("one destination cannot own two blobs");

    assert!(matches!(
        &error,
        crate::blob::transition::MakeLocalError::PartialMaterialization {
            retained_paths,
            ..
        } if retained_paths == &vec![destination.clone()]
    ));
    let retained = std::fs::read(&destination).unwrap();
    assert!(
        retained == first || retained == second,
        "the published destination contains whichever valid blob materialized first",
    );
    assert_eq!(shared_flag(&db, "n1").await, 1);
    assert!(pending_deletes(&db).await.is_empty());
    let first_location = db
        .blob_location("photos", "photoaaa")
        .await
        .unwrap()
        .unwrap();
    assert!(storage
        .get_blob(
            "photos",
            &first_location,
            "photoaaa",
            BlobScope::Master,
            Some("cv/photoaaa.jpg")
        )
        .await
        .is_ok());
    assert!(storage
        .get_blob(
            "photos",
            &second_location,
            "photobbb",
            BlobScope::Master,
            Some("cv/photobbb.jpg")
        )
        .await
        .is_ok());
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

    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let bytes = b"managed-bytes".to_vec();

    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes).await;

    // A dest whose filename is not valid UTF-8: `to_str()` returns None, so the
    // conversion must fail loud instead of lossily rewriting the path. Kept under the
    // temp dir so the rolled-back partial is contained.
    let bad = tmp.path().join(OsStr::from_bytes(b"photo-\xff\xfe.jpg"));
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), bad)].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
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
        db.external_blob("photoaaa").await.unwrap().is_none(),
        "no external ref registered"
    );
    assert!(pending_deletes(&db).await.is_empty(), "no tombstone queued");
    let location = db
        .blob_location("photos", "photoaaa")
        .await
        .unwrap()
        .unwrap();
    assert!(
        storage
            .get_blob(
                "photos",
                &location,
                "photoaaa",
                BlobScope::Master,
                Some("cv/photoaaa.jpg")
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
/// validly Local-uploading, and re-running the drain converges to Remote once
/// the remaining blob lands — no half-state, the re-upload is an idempotent overwrite.
#[tokio::test]
async fn make_remote_crash_before_flip_redrain_converges() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let user = tmp.path().join("user");

    seed_local_release(&db, &user, "n1", "photoaaa", "cv/photoaaa.jpg", b"first").await;
    let src2 = add_local_photo(&db, &user, "n1", "photobbb", "cv/photobbb.jpg", b"second").await;

    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        true,
    )
    .await
    .expect("make_remote");

    // "Crash" after photoaaa uploads but before completion: remove photobbb's source so the
    // first drain uploads only photoaaa, leaving the make_remote in flight.
    std::fs::remove_file(&src2).unwrap();
    drain_uploads(
        &db,
        &storage,
        &enc,
        &PendingRotation::none(),
        "test-lib",
        &lib,
        &SystemClock,
        &hlc,
        None,
    )
    .await
    .expect("partial drain");
    assert_eq!(shared_flag(&db, "n1").await, 0, "still Local-uploading");
    assert!(
        has_intent(&db, "notes", "n1").await,
        "the make_remote marker survives"
    );
    assert_eq!(
        pending_uploads(&db).await,
        1,
        "photobbb's upload is still queued"
    );

    // Re-run the drain after photobbb's source is back. Clear photobbb's
    // failed-attempt backoff first (a restart/retry re-attempts past the window);
    // the drain then completes and flips.
    std::fs::write(&src2, b"second").unwrap();
    db.reset_cloud_outbox_backoff().await.unwrap();
    drain_uploads(
        &db,
        &storage,
        &enc,
        &PendingRotation::none(),
        "test-lib",
        &lib,
        &SystemClock,
        &hlc,
        None,
    )
    .await
    .expect("resume drain");
    assert_eq!(shared_flag(&db, "n1").await, 1, "converged to Remote");
    assert!(
        !has_intent(&db, "notes", "n1").await,
        "the intent is cleared"
    );
    assert_eq!(pending_uploads(&db).await, 0, "the queue is drained");
    assert!(db.external_blob("photoaaa").await.unwrap().is_none());
    assert!(db.external_blob("photobbb").await.unwrap().is_none());
}

/// An aborted make_local (here via cancel) leaves the release Remote; retrying from
/// scratch converges to Local with the cloud delete enqueued — re-materialize +
/// re-commit is idempotent.
#[tokio::test]
async fn make_local_abort_then_retry_converges() {
    let storage = MockSyncStorage::new();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_store_dir();
    let bytes = b"materialize-me".to_vec();

    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes).await;

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let photo_key = recorded_blob_key(&db, "photos", "photoaaa", "cv/photoaaa.jpg").await;

    // First attempt is cancelled before the commit (the "crash"): still Remote.
    let (_cancel_tx, cancelled) = watch::channel(true);
    let err = make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
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
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
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
    assert_eq!(pending_deletes(&db).await, vec![photo_key],);
}

// ===========================================================================
// Round trip
// ===========================================================================

/// make_remote → make_local → make_remote on one device. The second make_remote
/// uploads a new immutable generation while the first generation remains condemned.
#[tokio::test]
async fn round_trip_make_remote_make_local_make_remote() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp = UserKeypair::generate();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
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
    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        true,
    )
    .await
    .expect("make_remote 1");
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;
    assert_eq!(shared_flag(&db, "n1").await, 1, "Remote after make_remote");
    let first_location = db
        .blob_location("photos", "photoaaa")
        .await
        .unwrap()
        .unwrap();
    let first_key = CloudSyncStorage::blob_key(
        BlobPathScheme::Plain,
        "photos",
        &first_location,
        "photoaaa",
        Some("cv/photoaaa.jpg"),
    )
    .unwrap();

    // Make it Local again.
    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    make_local(
        &db,
        &storage,
        &lib,
        BlobPathScheme::Plain,
        &hlc,
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
    assert_eq!(shared_flag(&db, "n1").await, 0, "Local after make_local");
    assert!(
        storage
            .exists(&format!("blob_tombstones/{first_key}"))
            .await
            .unwrap(),
        "the make_local tombstoned the cloud blob",
    );

    // Second make_remote: the external file is uploaded to a new generation and
    // the gate flips back on without touching the old tombstone.
    make_remote(
        &db,
        BlobPathScheme::Plain,
        SELF_UPLOADER,
        &hlc,
        "notes",
        "n1",
        true,
    )
    .await
    .expect("make_remote 2");
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;
    assert_eq!(
        shared_flag(&db, "n1").await,
        1,
        "Remote again after the second make_remote"
    );
    assert!(
        db.external_blob("photoaaa").await.unwrap().is_none(),
        "external ref cleared"
    );
    let second_location = db
        .blob_location("photos", "photoaaa")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first_location, second_location);
    assert!(
        storage
            .exists(&format!("blob_tombstones/{first_key}"))
            .await
            .unwrap(),
        "the old generation remains condemned",
    );
    assert!(
        storage
            .get_blob(
                "photos",
                &second_location,
                "photoaaa",
                BlobScope::Master,
                Some("cv/photoaaa.jpg")
            )
            .await
            .is_ok(),
        "the blob is back in the cloud",
    );
}
