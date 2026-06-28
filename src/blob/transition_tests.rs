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
use std::sync::{Mutex, RwLock};

use async_trait::async_trait;
use tokio::sync::watch;

use crate::blob::transition::{cancel_make_remote, make_local, make_remote};
use crate::blob::upload::drain_uploads;
use crate::blob::{
    cache, local_files, BlobRef, BlobScope, BlobTransitionObserver, CacheFill, Provenance,
    ResolvedScope,
};
use crate::clock::SystemClock;
use crate::database::Database;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::CloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher};
use crate::sync::cycle::{run_single_sync_cycle, SyncCycleResult};
use crate::sync::hlc::Hlc;
use crate::sync::session::BlobDecl;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    exec, open_test_db_with_blob, open_test_db_with_user_and_host_blobs, query_text, row_exists,
    temp_library_dir, MockSyncStorage,
};

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
    lib: &LibraryDir,
    observer: Option<&dyn BlobTransitionObserver>,
) -> SyncCycleResult {
    run_single_sync_cycle(
        storage,
        "test-lib",
        device,
        hlc,
        &SystemClock,
        db,
        cipher,
        kp,
        lib,
        Some(storage as &dyn crate::storage::cloud::CloudHome),
        observer,
    )
    .await
    .expect("cycle")
}

/// Insert the gated note + its blob-bearing photo row, `shared` (Remote) or not.
/// The two seeders below differ only in this flag and where the blob's bytes live.
async fn seed_release_rows(
    db: &Database,
    note_id: &str,
    photo_id: &str,
    cloud_path: &str,
    shared: u8,
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
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at, cloud_path) \
             VALUES ('{photo_id}', '{note_id}', 'audio', '0000000001000-0000-A', '2026-01-01', '{cloud_path}')"
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
    seed_release_rows(db, note_id, photo_id, cloud_path, 0).await;
    std::fs::create_dir_all(user_dir).unwrap();
    let src = user_dir.join(format!("{photo_id}.flac"));
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
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at, cloud_path) \
             VALUES ('{photo_id}', '{note_id}', 'audio', '0000000001000-0000-A', '2026-01-01', '{cloud_path}')"
        ),
    )
    .await;
    let src = user_dir.join(format!("{photo_id}.flac"));
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
    seed_release_rows(db, note_id, photo_id, cloud_path, 1).await;
    storage
        .put_blob(
            "photos",
            photo_id,
            ResolvedScope::Master,
            Some(cloud_path),
            bytes.to_vec(),
        )
        .await
        .expect("seed cloud blob");
}

async fn shared_flag(db: &Database, note_id: &str) -> i64 {
    let v = query_text(
        db,
        &format!("SELECT CAST(shared AS TEXT) FROM notes WHERE id = '{note_id}'"),
    )
    .await;
    v.parse().unwrap()
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

async fn has_intent(db: &Database, root_table: &str, root_id: &str) -> bool {
    let (rt, ri) = (root_table.to_string(), root_id.to_string());
    db.call(move |conn| Database::make_remote_intent_exists(conn, &rt, &ri))
        .await
        .unwrap()
}

// ===========================================================================
// Multi-device make_remote / make_local
// ===========================================================================

/// A imports a Local release and makes it Remote. Device B receives the subtree
/// ONLY after the blob is up (the gate stays off until the flip), A keeps the blob
/// pinned and the external source deleted, and B fetches the CacheLazy blob on read.
#[tokio::test]
async fn multi_device_make_remote_publishes_only_after_blobs_are_up() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp_a = UserKeypair::generate();
    let hlc_a = Hlc::new("A".to_string());
    let db_a = open_test_db_with_blob(photo_decl());
    let (tmp_a, lib_a) = temp_library_dir();
    let bytes = b"RELEASE-AUDIO-BYTES-one-file".to_vec();

    let src = seed_local_release(
        &db_a,
        &tmp_a.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.flac",
        &bytes,
    )
    .await;

    // A cycle while the note is gated off: nothing reaches a peer.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;
    let db_b = open_test_db_with_blob(photo_decl());
    let (_tmp_b, lib_b) = temp_library_dir();
    crate::sync::test_helpers::pull_into(&db_b, &storage, "B", &HashMap::new(), &lib_b).await;
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a gated-off (Local) release does not reach a peer",
    );

    // A makes it Remote: enqueue the upload + intent, then the next cycle's drain
    // uploads the blob and flips the gate.
    make_remote(&db_a, BlobPathScheme::Plain, &hlc_a, "notes", "n1", true)
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
    // external ref is dropped, the source file is deleted, the blob is pinned, and
    // the drain broke to publish.
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
        !src.exists(),
        "the external source file is deleted post-commit"
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
    crate::sync::test_helpers::pull_into(&db_b, &storage, "B", &HashMap::new(), &lib_b).await;
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B receives the release once its blobs are up and the gate flips",
    );
    let fetched = cache::read_blob(
        &db_b,
        &lib_b,
        Some(&storage),
        &photo_ref("photoaaa", "cv/photoaaa.flac"),
    )
    .await
    .expect("B fetches the CacheLazy blob");
    assert_eq!(fetched, bytes, "B reads the original audio from the cloud");
}

/// A makes a Remote release Local. B's subtree is DELETEd (gate retract) and the
/// cloud blob is tombstoned, while A keeps the external file and reads from it.
#[tokio::test]
async fn multi_device_make_local_retracts_peer_and_tombstones_cloud() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp_a = UserKeypair::generate();
    let kp_b = UserKeypair::generate();
    let hlc_a = Hlc::new("A".to_string());
    let hlc_b = Hlc::new("B".to_string());
    let db_a = open_test_db_with_blob(photo_decl());
    let db_b = open_test_db_with_blob(photo_decl());
    let (tmp_a, lib_a) = temp_library_dir();
    let (_tmp_b, lib_b) = temp_library_dir();
    let bytes = b"MANAGED-AUDIO-going-back-local".to_vec();

    seed_remote_release(
        &storage,
        &db_a,
        "n1",
        "photoaaa",
        "cv/photoaaa.flac",
        &bytes,
    )
    .await;
    // B is a known peer (it has a head) that has not acked yet, so A's snapshot
    // cycle keeps A's release changeset for B to pull — reclamation is paused until
    // every current device acks. Without a peer head A would be single-device and
    // the snapshot-covered changeset would be reclaimed, leaving nothing for B's
    // incremental pull.
    storage
        .put_head("B", 0, None, "2024-01-01T00:00:00Z")
        .await
        .expect("seed peer head");

    // A pushes the Remote release; B pulls it.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;
    crate::sync::test_helpers::pull_into(&db_b, &storage, "B", &HashMap::new(), &lib_b).await;
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B has the Remote release",
    );

    // A makes it Local to a chosen folder.
    let dest_dir = tmp_a.path().join("dest");
    let dest_path = dest_dir.join("photoaaa.flac");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    let recorder = Recorder::default();
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
        vec!["photos/cv/photoaaa.flac".to_string()],
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
            .exists("blob_tombstones/photos/cv/photoaaa.flac")
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

    // A still reads the audio from its external file (no cloud copy needed).
    let read = cache::read_blob(
        &db_a,
        &lib_a,
        Some(&storage),
        &photo_ref("photoaaa", "cv/photoaaa.flac"),
    )
    .await
    .expect("A reads from its external file");
    assert_eq!(read, bytes, "A plays its own local file");
}

// ===========================================================================
// Host-provided lifecycle (the cover rides the inline push, not the outbox)
// ===========================================================================

/// A release with a user-provided audio file AND a host-provided cover, through both
/// transitions. make_remote: the audio uploads via the outbox and flips the gate;
/// the gate flip re-emits the subtree and the cycle's inline push uploads the cover
/// (host-provided) from the local store and moves that copy into the cache. A peer
/// pulls the cover eagerly (`CacheEager`) into its cache. make_local: the audio goes
/// back to its dest (external ref) and the cover back to the local store (NO dest),
/// both cloud copies tombstoned.
#[tokio::test]
async fn host_provided_cover_rides_the_inline_push_through_both_transitions() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp_a = UserKeypair::generate();
    let hlc_a = Hlc::new("A".to_string());
    let db_a = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let (tmp_a, lib_a) = temp_library_dir();
    let audio = b"RELEASE-AUDIO".to_vec();
    let cover = b"RELEASE-COVER".to_vec();

    // Seed a gated-off release: a note + a user-provided audio (external ref) + a
    // host-provided cover (in the local store).
    let src = seed_local_release(
        &db_a,
        &tmp_a.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.flac",
        &audio,
    )
    .await;
    exec(
        &db_a,
        "INSERT INTO note_covers (id, note_id, _updated_at, created_at, cloud_path) \
         VALUES ('coveraaa', 'n1', '0000000001000-0000-A', '2026-01-01', 'cv/cover.jpg')",
    )
    .await;
    local_files::store(&lib_a, "covers", "coveraaa", &cover)
        .await
        .expect("store the host-provided cover in the local store");

    // A cycle while gated off: nothing reaches a peer.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;

    // make_remote: the audio drains, the gate flips, and this cycle's inline push
    // uploads the cover from the local store and moves it into the cache.
    make_remote(&db_a, BlobPathScheme::Plain, &hlc_a, "notes", "n1", true)
        .await
        .expect("make_remote");
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc, &kp_a, &lib_a, None).await;

    assert_eq!(shared_flag(&db_a, "n1").await, 1, "the release is Remote");
    assert!(
        storage.exists("covers/cv/cover.jpg").await.unwrap(),
        "the host-provided cover is uploaded to the cloud",
    );
    assert!(
        lib_a
            .cache_blob_path("covers", "coveraaa")
            .unwrap()
            .exists(),
        "the cover's local-store copy moved into the evictable cache",
    );
    assert!(
        !lib_a
            .local_blob_path("covers", "coveraaa")
            .unwrap()
            .exists(),
        "the cover is no longer in the local store (it is Remote now)",
    );

    // B pulls: the cover (CacheEager) lands in B's cache; the audio (CacheLazy) does not.
    let db_b = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let (_tmp_b, lib_b) = temp_library_dir();
    crate::sync::test_helpers::pull_into(&db_b, &storage, "B", &HashMap::new(), &lib_b).await;
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
        "B does not fetch the CacheLazy audio on pull",
    );
    assert_eq!(
        cache::read_blob(
            &db_b,
            &lib_b,
            Some(&storage),
            &cover_ref("coveraaa", "cv/cover.jpg")
        )
        .await
        .expect("B reads the cover"),
        cover,
        "B's cover bytes match",
    );

    // make_local: the audio back to its dest (external ref), the cover back to the
    // local store (no dest), both cloud copies tombstoned.
    let dest_path = tmp_a.path().join("dest/photoaaa.flac");
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
        audio,
        "the user-provided audio is materialized to its required dest",
    );
    assert_eq!(
        db_a.external_blob("photoaaa").await.unwrap().unwrap().path,
        dest_path,
        "the audio is registered as an external ref",
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
        vec![
            "covers/cv/cover.jpg".to_string(),
            "photos/cv/photoaaa.flac".to_string(),
        ],
        "both cloud copies are tombstoned in the make_local commit",
    );
    // The source the user imported from is gone (deleted post make_remote upload).
    assert!(
        !src.exists(),
        "the original imported audio was deleted on make_remote"
    );
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
    let (tmp, lib) = temp_library_dir();
    let user = tmp.path().join("user");

    // Two photos under one release.
    let _src1 =
        seed_local_release(&db, &user, "n1", "photoaaa", "cv/photoaaa.flac", b"first").await;
    let src2 = add_local_photo(&db, &user, "n1", "photobbb", "cv/photobbb.flac", b"second").await;

    make_remote(&db, BlobPathScheme::Plain, &hlc, "notes", "n1", true)
        .await
        .expect("make_remote");
    assert_eq!(pending_uploads(&db).await, 2, "both uploads queued");

    // Drain with photobbb's source removed: photoaaa uploads (not the last, no flip), photobbb fails.
    std::fs::remove_file(&src2).unwrap();
    drain_uploads(&db, &storage, &enc, &lib, &SystemClock, &hlc, None)
        .await
        .expect("partial drain");
    assert_eq!(
        shared_flag(&db, "n1").await,
        0,
        "not flipped — photobbb never uploaded"
    );
    assert!(
        storage.exists("photos/cv/photoaaa.flac").await.unwrap(),
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
    assert_eq!(
        pending_deletes(&db).await,
        vec!["photos/cv/photoaaa.flac".to_string()],
        "the already-uploaded orphan is tombstoned",
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
    let (tmp, lib) = temp_library_dir();

    let src = seed_local_release(
        &db,
        &tmp.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.flac",
        b"orphan-bytes",
    )
    .await;
    // Enqueue the upload with NO intent (models a make_remote whose intent + pending row
    // were cancelled, but this blob was already in flight in the drain).
    db.enqueue_upload(
        "photoaaa",
        "photos/cv/photoaaa.flac",
        Some(src.to_str().unwrap()),
        BlobScope::Master,
        true,
        "0000000001000-0000-A",
    )
    .await
    .unwrap();

    drain_uploads(&db, &storage, &enc, &lib, &SystemClock, &hlc, None)
        .await
        .expect("drain");

    assert_eq!(shared_flag(&db, "n1").await, 0, "no intent ⇒ no flip");
    assert_eq!(
        pending_deletes(&db).await,
        vec!["photos/cv/photoaaa.flac".to_string()],
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
    let (tmp, lib) = temp_library_dir();
    let bytes = b"still-managed".to_vec();

    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.flac", &bytes).await;

    let dest_path = tmp.path().join("dest/photoaaa.flac");
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
    let (tmp, lib) = temp_library_dir();
    let bytes = b"managed-bytes".to_vec();

    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.flac", &bytes).await;

    // Block the dest: make the dest's parent dir a FILE, so create_dir_all fails.
    let blocker = tmp.path().join("blocker");
    std::fs::write(&blocker, b"x").unwrap();
    let dest_path = blocker.join("photoaaa.flac");
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
    assert!(
        storage
            .get_blob(
                "photos",
                "photoaaa",
                ResolvedScope::Master,
                Some("cv/photoaaa.flac")
            )
            .await
            .is_ok(),
        "the cloud blob is untouched",
    );
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
    let (tmp, lib) = temp_library_dir();
    let bytes = b"managed-bytes".to_vec();

    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.flac", &bytes).await;

    // A dest whose filename is not valid UTF-8: `to_str()` returns None, so the
    // conversion must fail loud instead of lossily rewriting the path. Kept under the
    // temp dir so the rolled-back partial is contained.
    let bad = tmp.path().join(OsStr::from_bytes(b"photo-\xff\xfe.flac"));
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
    assert!(
        storage
            .get_blob(
                "photos",
                "photoaaa",
                ResolvedScope::Master,
                Some("cv/photoaaa.flac")
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
    let (tmp, lib) = temp_library_dir();
    let user = tmp.path().join("user");

    seed_local_release(&db, &user, "n1", "photoaaa", "cv/photoaaa.flac", b"first").await;
    let src2 = add_local_photo(&db, &user, "n1", "photobbb", "cv/photobbb.flac", b"second").await;

    make_remote(&db, BlobPathScheme::Plain, &hlc, "notes", "n1", true)
        .await
        .expect("make_remote");

    // "Crash" after photoaaa uploads but before completion: remove photobbb's source so the
    // first drain uploads only photoaaa, leaving the make_remote in flight.
    std::fs::remove_file(&src2).unwrap();
    drain_uploads(&db, &storage, &enc, &lib, &SystemClock, &hlc, None)
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
    drain_uploads(&db, &storage, &enc, &lib, &SystemClock, &hlc, None)
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
    let (tmp, lib) = temp_library_dir();
    let bytes = b"materialize-me".to_vec();

    seed_remote_release(&storage, &db, "n1", "photoaaa", "cv/photoaaa.flac", &bytes).await;

    let dest_path = tmp.path().join("dest/photoaaa.flac");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();

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
    assert_eq!(
        pending_deletes(&db).await,
        vec!["photos/cv/photoaaa.flac".to_string()],
    );
}

// ===========================================================================
// Round trip
// ===========================================================================

/// make_remote → make_local → make_remote on one device. The second make_remote re-uploads
/// to the same cloud key, whose drain cancels the make_local's tombstone, and flips the gate back
/// on. The release ends Remote with the blob in the cloud and no external ref.
#[tokio::test]
async fn round_trip_make_remote_make_local_make_remote() {
    let storage = MockSyncStorage::new();
    let enc = plaintext();
    let kp = UserKeypair::generate();
    let hlc = Hlc::new("A".to_string());
    let db = open_test_db_with_blob(photo_decl());
    let (tmp, lib) = temp_library_dir();
    let bytes = b"round-trip-audio".to_vec();

    // Start Local, make it Remote.
    seed_local_release(
        &db,
        &tmp.path().join("user"),
        "n1",
        "photoaaa",
        "cv/photoaaa.flac",
        &bytes,
    )
    .await;
    make_remote(&db, BlobPathScheme::Plain, &hlc, "notes", "n1", true)
        .await
        .expect("make_remote 1");
    run_cycle(&storage, "A", &hlc, &db, &enc, &kp, &lib, None).await;
    assert_eq!(shared_flag(&db, "n1").await, 1, "Remote after make_remote");

    // Make it Local again.
    let dest_path = tmp.path().join("dest/photoaaa.flac");
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
            .exists("blob_tombstones/photos/cv/photoaaa.flac")
            .await
            .unwrap(),
        "the make_local tombstoned the cloud blob",
    );

    // Second make_remote: the external file (now at dest) is re-uploaded, the drain cancels
    // the tombstone, and the gate flips back on.
    make_remote(&db, BlobPathScheme::Plain, &hlc, "notes", "n1", true)
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
    assert!(
        !storage
            .exists("blob_tombstones/photos/cv/photoaaa.flac")
            .await
            .unwrap(),
        "the re-upload cancelled the leftover tombstone",
    );
    assert!(
        storage
            .get_blob(
                "photos",
                "photoaaa",
                ResolvedScope::Master,
                Some("cv/photoaaa.flac")
            )
            .await
            .is_ok(),
        "the blob is back in the cloud",
    );
}
