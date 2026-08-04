//! Tests for the coven-owned make-Remote / make-Local transitions.
//!
//! These drive the real transition owners (`LocalBlobTransitions` and
//! `ConnectedBlobTransitions`) and the upload
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
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::watch;

use crate::blob::transition::LocalBlobTransitions;
use crate::clock::SystemClock;
use crate::database::Database;
use crate::database::StoreDatabase;
use crate::keys::UserKeypair;
use crate::protocol::blob::{BlobTransitionObserver, CacheFill, Provenance, RowBlobRef};
use crate::protocol::store_commit::ObjectHash;
use crate::storage::cloud::CloudHome;
use crate::storage::SyncStorage;
use crate::store_dir::StoreDir;
use crate::sync::cycle::DeferredLocalBlobDisposition;
use crate::sync::session::{BlobDecl, RowIdentity, SyncedTable};
use crate::sync::test_helpers::{
    open_test_db, open_test_db_schema, open_test_db_with_blob,
    open_test_db_with_user_and_host_blobs, remote_root_db, temp_store_dir, TestStore,
};
use crate::sync::test_owner_graph::TestOwnerGraph;
use crate::Migration;

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

async fn create_store(
    db: &Database,
    signer: UserKeypair,
    home: std::sync::Arc<crate::InMemoryCloudHome>,
) -> std::sync::Arc<TestStore> {
    TestStore::create(db, "test-store", signer, home)
        .await
        .expect("create exact test Store for the test database")
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

async fn shared_flag(db: &Database, note_id: &str) -> i64 {
    let v = db
        .query_test_text(&format!(
            "SELECT CAST(shared AS TEXT) FROM notes WHERE id = '{note_id}'"
        ))
        .await;
    v.parse().unwrap()
}

/// The note's gate stamp (`_updated_at`), to prove a refused transition leaves the
/// gate row — value and causal stamp — untouched.
async fn gate_stamp(db: &Database, note_id: &str) -> String {
    db.query_test_text(&format!(
        "SELECT _updated_at FROM notes WHERE id = '{note_id}'"
    ))
    .await
}

async fn pending_uploads(db: &Database) -> usize {
    db.get_pending_cloud_uploads().await.unwrap().len()
}

async fn created_upload_blob(
    db: &Database,
    blob_id: &str,
) -> crate::protocol::blob::locator::StoredBlobRef {
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

async fn assert_scoped_flip_journaled_atomically(
    db: &Database,
    expected_changes: &[(&str, crate::changeset::ChangeOp)],
) {
    assert!(
        db.has_store_partition_for_test()
            .await
            .expect("inspect Store partition"),
        "the audience partition and its parent write commit together",
    );
    let has_routes = db
        .table_has_rows_for_test(crate::database::DatabaseTestTable::named(
            "_coven_row_routes",
        ))
        .await
        .expect("inspect scoped routes");
    let has_mirrors = db
        .table_has_rows_for_test(crate::database::DatabaseTestTable::named("_coven_audience"))
        .await
        .expect("inspect scoped mirrors");
    assert!(
        !has_routes && !has_mirrors,
        "a boolean-gated Store row does not invent scoped row routes or mirrors",
    );
    let changesets = db
        .store_partition_changesets_for_test()
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

#[tokio::test]
async fn published_drop_intents_preserve_distinct_locators_for_one_logical_id() {
    let db = open_test_db();
    let first = ObjectHash::digest(b"first locator");
    let second = ObjectHash::digest(b"second locator");

    db.insert_published_blob_drop_intent_for_test(
        1,
        "covers",
        "shared-id",
        b"first",
        first,
        DeferredLocalBlobDisposition::Cache,
    )
    .await
    .expect("insert first published blob drop intent");
    db.insert_published_blob_drop_intent_for_test(
        1,
        "covers",
        "shared-id",
        b"second",
        second,
        DeferredLocalBlobDisposition::Pin,
    )
    .await
    .expect("insert second published blob drop intent");

    let count = db
        .test_sql(|database| database.published_blob_drop_intent_count(1, "covers", "shared-id"))
        .await
        .expect("count exact drop intents");
    assert_eq!(count, 2);
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
    let store_database_a = StoreDatabase::new(&db_a);
    let storage = create_store(
        &db_a,
        kp_a.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp_a, lib_a) = temp_store_dir();
    let owners_a = TestOwnerGraph::new(StoreDatabase::new(&db_a), lib_a.clone());
    let bytes = b"PHOTO-BYTES-one-file".to_vec();

    let src = owners_a
        .seed_local_release(
            &tmp_a.path().join("user"),
            "n1",
            "photoaaa",
            "cv/photoaaa.jpg",
            &bytes,
        )
        .await;

    // A cycle while the note is gated off: nothing reaches a peer.
    storage
        .run_founder_cycle(&lib_a, None)
        .await
        .expect("run founder cycle");
    let db_b = open_test_db_with_blob(photo_decl());
    let (_tmp_b, lib_b) = temp_store_dir();
    let owners_b = TestOwnerGraph::new(StoreDatabase::new(&db_b), lib_b.clone());
    let kp_b = UserKeypair::generate();
    let peer = storage
        .invite_and_activate_peer(&db_a, &db_b, &kp_b)
        .await
        .expect("invite and activate peer Store device");
    peer.pull_store(&lib_b).await.expect("pull peer Store");
    assert!(
        !db_b
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await,
        "a gated-off (Local) release does not reach a peer",
    );

    // A makes it Remote: enqueue the upload + intent, then the next cycle's drain
    // uploads the blob and flips the gate.
    owners_a
        .make_remote("notes", "n1", true)
        .await
        .expect("make_remote");
    let recorder = Recorder::default();
    let result = storage
        .run_founder_cycle(&lib_a, Some(&recorder))
        .await
        .expect("run founder cycle");

    // The flip completed this cycle: the gate is on, the intent is gone, the
    // external ref is dropped, the user's source file is left in place, the blob
    // is pinned, and the drain broke to publish.
    assert_eq!(shared_flag(&db_a, "n1").await, 1, "the release is Remote");
    assert!(
        !db_a
            .make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent"),
        "the intent is cleared"
    );
    assert!(
        store_database_a
            .external_blob("note_photos", "photoaaa")
            .await
            .expect("load exact external blob ownership")
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
    storage
        .run_founder_cycle(&lib_a, None)
        .await
        .expect("run founder cycle");
    peer.pull_store(&lib_b).await.expect("pull peer Store");
    assert!(
        db_b.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await,
        "B receives the release once its blobs are up and the gate flips",
    );
    let fetched = owners_b
        .read_blob(Some(storage.clone()), &photo_ref(&db_b, "photoaaa").await)
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
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(StoreDatabase::new(&db), lib.clone());
    let bytes = b"PHOTO-repin".to_vec();

    owners
        .seed_local_release(
            &tmp.path().join("user"),
            "n1",
            "photoaaa",
            "cv/photoaaa.jpg",
            &bytes,
        )
        .await;

    owners
        .make_remote("notes", "n1", false)
        .await
        .expect("make_remote pin=false");
    assert!(
        !pending_upload_state(&db, "photoaaa").await.1,
        "the first make_remote queued the upload unpinned",
    );

    // A second make_remote with a pin, before the upload drains.
    owners
        .make_remote("notes", "n1", true)
        .await
        .expect("make_remote pin=true");
    assert!(
        pending_upload_state(&db, "photoaaa").await.1,
        "the re-enqueue must update the queued upload's pin to the new call's value",
    );

    storage
        .run_founder_cycle(&lib, None)
        .await
        .expect("run founder cycle");

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
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(StoreDatabase::new(&db), lib.clone());
    let bytes = b"PHOTO-relocate".to_vec();
    let user_dir = tmp.path().join("user");

    let src1 = owners
        .seed_local_release(&user_dir, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes)
        .await;
    owners
        .make_remote("notes", "n1", false)
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
    crate::database::StoreDatabase::new(&db)
        .register_external_blob_for_test("note_photos", "photoaaa", &src2)
        .await;
    std::fs::remove_file(&src1).unwrap();

    owners
        .make_remote("notes", "n1", false)
        .await
        .expect("second make_remote");
    assert_eq!(
        pending_upload_state(&db, "photoaaa").await.0,
        src2,
        "the re-enqueue repoints the queued upload at the new source",
    );

    storage
        .run_founder_cycle(&lib, None)
        .await
        .expect("run founder cycle");

    assert_eq!(
        shared_flag(&db, "n1").await,
        1,
        "the drain read the re-registered path and completed the make_remote",
    );
    assert!(
        storage
            .contains_blob_object(&photo_ref(&db, "photoaaa").await)
            .await,
        "the blob uploaded from the new path",
    );
}

#[tokio::test]
async fn cancel_make_remote_after_completion_enqueues_no_deletes() {
    let db = open_test_db_with_blob(photo_decl());
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let bytes = b"PHOTO-BYTES-completed-remote".to_vec();

    owners
        .seed_local_release(
            &tmp.path().join("user"),
            "n1",
            "photoaaa",
            "cv/photoaaa.jpg",
            &bytes,
        )
        .await;
    owners
        .make_remote("notes", "n1", false)
        .await
        .expect("make_remote");
    storage
        .run_founder_cycle(&lib, None)
        .await
        .expect("run founder cycle");

    assert_eq!(shared_flag(&db, "n1").await, 1, "the root is Remote");
    assert!(
        !db.make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent"),
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

    LocalBlobTransitions::new(store_database, lib.clone())
        .cancel_make_remote("notes", "n1")
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
        let store_database_a = StoreDatabase::new(&db_a);
        let home = crate::sync::test_helpers::test_cloud_home();
        let storage = create_store(&db_a, kp_a.clone(), home.clone()).await;
        let kp_b = UserKeypair::generate();
        let db_b = open_test_db_with_blob(photo_decl());
        let (tmp_a, lib_a) = temp_store_dir();
        let owners_a = TestOwnerGraph::new(store_database_a.clone(), lib_a.clone());
        let (_tmp_b, lib_b) = temp_store_dir();
        let bytes = b"MANAGED-PHOTO-going-back-local".to_vec();

        let peer = Box::pin(storage.invite_and_activate_peer(&db_a, &db_b, &kp_b))
            .await
            .expect("invite and activate peer Store device");
        Box::pin(owners_a.seed_remote_release(
            &storage,
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
        Box::pin(storage.run_founder_cycle(&lib_a, None))
            .await
            .expect("run founder cycle");
        peer.pull_store(&lib_b).await.expect("pull peer Store");
        assert!(
            db_b.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
                .await,
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
        let reads_before_make_local = home.exact_stream_read_count();
        Box::pin(owners_a.make_local(
            storage.clone(),
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
            home.exact_stream_read_count(),
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
            store_database_a
                .external_blob("note_photos", "photoaaa")
                .await
                .expect("load exact external blob ownership")
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
        Box::pin(storage.run_founder_cycle(&lib_a, None))
            .await
            .expect("run founder cycle");
        assert!(
            storage.contains_blob_tombstone(&remote_blob).await.unwrap(),
            "the cloud blob is tombstoned",
        );

        // B's next cycle pulls the retract: its subtree disappears.
        Box::pin(peer.run_cycle(&lib_b, None))
            .await
            .expect("run peer cycle");
        assert!(
            !db_b
                .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
                .await,
            "B's subtree is removed by the gate retract",
        );

        // A still reads the photo from its external file (no cloud copy needed).
        let read = owners_a
            .read_blob(Some(storage.clone()), &photo_ref(&db_a, "photoaaa").await)
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
    let store_database = StoreDatabase::new(&db);
    let home = crate::sync::test_helpers::test_cloud_home();
    let storage = create_store(&db, UserKeypair::generate(), home.clone()).await;
    storage.open_into(&db).await.expect("open exact test Store");
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let bytes = b"scoped-managed-photo".to_vec();
    let routing_encryption = crate::encryption::EncryptionService::from_key([5; 32]);
    owners
        .seed_remote_release(
            &storage,
            Some(&routing_encryption),
            "n-scoped",
            "photoscoped",
            "cv/photoscoped.jpg",
            &bytes,
        )
        .await;
    let store_state_before = db
        .test_sql(|database| database.scoped_store_state_counts())
        .await
        .expect("read scoped Store state");

    let gate_stamp_before = gate_stamp(&db, "n-scoped").await;
    let dest_dir = tmp.path().join("destination");
    let dest_path = dest_dir.join("photoscoped.jpg");
    let dest: HashMap<String, PathBuf> = [("photoscoped".to_string(), dest_path.clone())].into();
    let (_cancel_tx, cancel) = watch::channel(false);
    let recorder = Arc::new(Recorder::default());

    let error = owners
        .make_local(
            storage.clone(),
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
        home.exact_stream_read_count(),
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
        store_database
            .external_blob("note_photos", "photoscoped")
            .await
            .expect("load exact external blob ownership")
            .is_none(),
        "no external reference is registered",
    );
    assert!(
        pending_deletes(&db).await.is_empty(),
        "no cloud deletion is enqueued",
    );
    assert_eq!(
        db.test_sql(|database| database.scoped_store_state_counts())
            .await
            .expect("read scoped Store state"),
        store_state_before
    );

    owners
        .make_local(
            storage.clone(),
            Some(routing_encryption),
            Some(recorder.clone()),
            "notes",
            "n-scoped",
            &dest,
            &cancel,
        )
        .await
        .expect("retry scoped make_local with routing encryption");
    assert_eq!(home.exact_stream_read_count(), 1);
    assert_eq!(std::fs::read(&dest_path).unwrap(), b"scoped-managed-photo");
    assert_eq!(shared_flag(&db, "n-scoped").await, 0);
    assert!(
        store_database
            .external_blob("note_photos", "photoscoped")
            .await
            .expect("load exact external blob ownership")
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
    let store_database = StoreDatabase::new(&db);
    let home = crate::sync::test_helpers::test_cloud_home();
    let storage = create_store(&db, UserKeypair::generate(), home.clone()).await;
    let exact_creates_before = home.exact_create_count();
    storage.open_into(&db).await.expect("open exact test Store");
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let bytes = b"scoped-user-photo";
    let routing_encryption = crate::encryption::EncryptionService::from_key([7; 32]);
    db.execute_test_sql(&format!(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n-user-scoped', 'Scoped user fixture', NULL, 0, \
                     '0000000001000-0000-A', '2026-01-01'); \
             INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('photo-user-scoped', 'n-user-scoped', 'image', {}, '{}', \
                     '0000000001000-0000-A', '2026-01-01', 'cv/photo-user-scoped.jpg');",
        bytes.len(),
        crate::protocol::blob::content_hash(bytes),
    ))
    .await;
    let user_dir = tmp.path().join("user");
    std::fs::create_dir_all(&user_dir).unwrap();
    let source = user_dir.join("photo-user-scoped.jpg");
    std::fs::write(&source, bytes).unwrap();
    crate::database::StoreDatabase::new(&db)
        .register_external_blob_for_test("note_photos", "photo-user-scoped", &source)
        .await;
    owners
        .make_remote("notes", "n-user-scoped", false)
        .await
        .expect("queue scoped user-provided make_remote");
    let stamp_before = gate_stamp(&db, "n-user-scoped").await;
    let store_state_before = db
        .test_sql(|database| database.scoped_store_state_counts())
        .await
        .expect("read scoped Store state");

    let error = match storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
    {
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
        home.exact_create_count(),
        exact_creates_before,
        "the blob is not uploaded before routing validation",
    );
    assert!(
        !home
            .exists("photos/cv/photo-user-scoped.jpg")
            .await
            .unwrap(),
        "the cloud is untouched",
    );
    assert!(source.exists(), "the user-owned source remains in place");
    assert!(
        store_database
            .external_blob("note_photos", "photo-user-scoped")
            .await
            .expect("load exact external blob ownership")
            .is_some(),
        "the external reference remains registered",
    );
    assert_eq!(pending_uploads(&db).await, 1, "the upload remains queued");
    assert!(
        db.make_remote_intent_exists_for_test("notes", "n-user-scoped")
            .await
            .expect("inspect make_remote intent"),
        "the transition intent remains queued",
    );
    assert_eq!(shared_flag(&db, "n-user-scoped").await, 0);
    assert_eq!(gate_stamp(&db, "n-user-scoped").await, stamp_before);
    assert_eq!(
        db.test_sql(|database| database.scoped_store_state_counts())
            .await
            .expect("read scoped Store state"),
        store_state_before
    );

    let outcome = storage
        .drain_uploads(
            &StoreDatabase::new(&db),
            &lib,
            &SystemClock,
            Some(&routing_encryption),
            None,
        )
        .await
        .expect("retry scoped upload completion with routing encryption");
    assert_eq!(outcome.uploaded(), 1);
    assert!(outcome.yielded_for_publish());
    assert_eq!(shared_flag(&db, "n-user-scoped").await, 1);
    assert_eq!(pending_uploads(&db).await, 1);
    assert!(db
        .make_remote_intent_exists_for_test("notes", "n-user-scoped")
        .await
        .expect("inspect make_remote intent"));
    assert!(
        store_database
            .external_blob("note_photos", "photo-user-scoped")
            .await
            .expect("load exact external blob ownership")
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
    assert!(!db
        .make_remote_intent_exists_for_test("notes", "n-user-scoped")
        .await
        .expect("inspect make_remote intent"));
}

#[tokio::test]
async fn scoped_host_completion_without_routing_encryption_mutates_nothing() {
    let db = scoped_blob_transition_db();
    let store_database = StoreDatabase::new(&db);
    let home = crate::sync::test_helpers::test_cloud_home();
    let storage = create_store(&db, UserKeypair::generate(), home.clone()).await;
    let exact_creates_before = home.exact_create_count();
    storage.open_into(&db).await.expect("open exact test Store");
    let (_tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database, lib.clone());
    let bytes = b"scoped-host-cover";
    let routing_encryption = crate::encryption::EncryptionService::from_key([9; 32]);
    db.execute_test_sql(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n-host-scoped', 'Scoped host fixture', NULL, 0, \
                 '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    db.execute_test_sql(&format!(
        "INSERT INTO note_covers \
             (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('cover-host-scoped', 'n-host-scoped', {}, '{}', \
                     '0000000001000-0000-A', '2026-01-01', 'cv/cover-host-scoped.jpg')",
        bytes.len(),
        crate::protocol::blob::content_hash(bytes),
    ))
    .await;
    crate::store_dir::StoreDir::store_local_blob(&lib, "covers", "cover-host-scoped", bytes)
        .await
        .expect("store host-provided fixture");
    owners
        .make_remote("notes", "n-host-scoped", false)
        .await
        .expect("queue scoped host-provided make_remote");
    let stamp_before = gate_stamp(&db, "n-host-scoped").await;
    let store_state_before = db
        .test_sql(|database| database.scoped_store_state_counts())
        .await
        .expect("read scoped Store state");

    let error = match storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
    {
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
        home.exact_create_count(),
        exact_creates_before,
        "the host-provided blob is not uploaded before routing validation",
    );
    assert!(
        !home
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
        db.make_remote_intent_exists_for_test("notes", "n-host-scoped")
            .await
            .expect("inspect make_remote intent"),
        "the transition intent remains queued",
    );
    assert_eq!(
        db.test_sql(|database| database.scoped_store_state_counts())
            .await
            .expect("read scoped Store state"),
        store_state_before
    );

    let completed = storage
        .drain_uploads(
            &StoreDatabase::new(&db),
            &lib,
            &SystemClock,
            Some(&routing_encryption),
            None,
        )
        .await
        .expect("retry scoped host completion with routing encryption");
    assert_eq!(completed.uploaded(), 1);
    assert!(completed.yielded_for_publish());
    assert_eq!(home.exact_create_count(), exact_creates_before + 1);
    assert_eq!(shared_flag(&db, "n-host-scoped").await, 1);
    assert!(
        db.make_remote_intent_exists_for_test("notes", "n-host-scoped")
            .await
            .expect("inspect make_remote intent"),
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
        !db.make_remote_intent_exists_for_test("notes", "n-host-scoped")
            .await
            .expect("inspect make_remote intent"),
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
    let store_database_a = StoreDatabase::new(&db_a);
    let storage = create_store(
        &db_a,
        kp_a.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp_a, lib_a) = temp_store_dir();
    let owners_a = TestOwnerGraph::new(store_database_a.clone(), lib_a.clone());
    let kp_b = UserKeypair::generate();
    let db_b = open_test_db_with_user_and_host_blobs(photo_decl(), cover_decl());
    let (_tmp_b, lib_b) = temp_store_dir();
    let owners_b = TestOwnerGraph::new(StoreDatabase::new(&db_b), lib_b.clone());
    let photo = b"PHOTO-BYTES".to_vec();
    let cover = b"RELEASE-COVER".to_vec();

    // Seed a gated-off release: a note + a user-provided photo (external ref) + a
    // host-provided cover (in the local store).
    let src = owners_a
        .seed_local_release(
            &tmp_a.path().join("user"),
            "n1",
            "photoaaa",
            "cv/photoaaa.jpg",
            &photo,
        )
        .await;
    db_a.execute_test_sql(&format!(
            "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coveraaa', 'n1', 13, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/cover-coveraaa.jpg')",
            crate::protocol::blob::content_hash(&cover),
        ),
    )
    .await;
    crate::store_dir::StoreDir::store_local_blob(&lib_a, "covers", "coveraaa", &cover)
        .await
        .expect("store the host-provided cover in the local store");

    let peer = storage
        .invite_and_activate_peer(&db_a, &db_b, &kp_b)
        .await
        .expect("invite and activate peer Store device");

    // A cycle while gated off: nothing reaches a peer.
    storage
        .run_founder_cycle(&lib_a, None)
        .await
        .expect("run founder cycle");

    // make_remote: the photo drains, the gate flips, and this cycle's inline push
    // uploads the cover from the local store and keeps the requested pin.
    owners_a
        .make_remote("notes", "n1", true)
        .await
        .expect("make_remote");
    storage
        .run_founder_cycle(&lib_a, None)
        .await
        .expect("run founder cycle");

    assert_eq!(shared_flag(&db_a, "n1").await, 1, "the release is Remote");
    assert!(
        storage
            .contains_blob_object(&cover_ref(&db_a, "coveraaa").await)
            .await,
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
        owners_b
            .read_blob(Some(storage.clone()), &cover_ref(&db_b, "coveraaa").await)
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
    owners_a
        .make_local(storage.clone(), None, None, "notes", "n1", &dest, &cancel)
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
        store_database_a
            .external_blob("note_photos", "photoaaa")
            .await
            .expect("load exact external blob ownership")
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
        store_database_a
            .external_blob("note_covers", "coveraaa")
            .await
            .expect("load exact external blob ownership")
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
    let store_database_a = StoreDatabase::new(&db_a);
    let home = crate::sync::test_helpers::test_cloud_home();
    let storage = create_store(&db_a, UserKeypair::generate(), home.clone()).await;
    let exact_creates_before = home.exact_create_count();
    let (_tmp_a, lib_a) = temp_store_dir();
    let owners_a = TestOwnerGraph::new(store_database_a, lib_a.clone());
    let cover = b"HOST-ONLY-COVER".to_vec();

    db_a.execute_test_sql(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n-host', 'Host Only', NULL, 0, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    db_a.execute_test_sql(&format!(
            "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coverhost', 'n-host', 15, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/host-coverhost.jpg')",
            crate::protocol::blob::content_hash(&cover),
        ),
    )
    .await;
    crate::store_dir::StoreDir::store_local_blob(&lib_a, "covers", "coverhost", &cover)
        .await
        .expect("store host-provided cover");

    let before = owners_a
        .read_blob(Some(storage.clone()), &cover_ref(&db_a, "coverhost").await)
        .await
        .expect("read Local host-provided cover");
    assert_eq!(before, cover);

    owners_a
        .make_remote("notes", "n-host", true)
        .await
        .expect("make host-provided-only root remote");
    assert_eq!(
        shared_flag(&db_a, "n-host").await,
        0,
        "host-provided-only make_remote leaves the gate off until the blob uploads"
    );
    assert!(
        db_a.make_remote_intent_exists_for_test("notes", "n-host")
            .await
            .expect("inspect make_remote intent"),
        "the pin choice is durable until inline upload consumes it"
    );
    assert_eq!(pending_uploads(&db_a).await, 1);
    assert!(
        !storage
            .contains_blob_object(&cover_ref(&db_a, "coverhost").await)
            .await,
        "the host-provided blob is not published before the cycle uploads it"
    );

    storage
        .run_founder_cycle(&lib_a, None)
        .await
        .expect("run founder cycle");

    assert_eq!(
        shared_flag(&db_a, "n-host").await,
        1,
        "the gate flips after the host-provided blob lands"
    );
    assert!(
        storage
            .contains_blob_object(&cover_ref(&db_a, "coverhost").await)
            .await,
        "inline push uploads the host-provided blob"
    );
    assert!(home.exact_create_count() > exact_creates_before);
    assert!(
        !db_a
            .make_remote_intent_exists_for_test("notes", "n-host")
            .await
            .expect("inspect make_remote intent"),
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
    let after = owners_a
        .read_blob(Some(storage.clone()), &cover_ref(&db_a, "coverhost").await)
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
    let home = crate::sync::test_helpers::test_cloud_home();
    let storage = create_store(&db, UserKeypair::generate(), home.clone()).await;
    let (_tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let pin_bytes = b"PINNED-COVER".to_vec();
    let drop_bytes = b"DROPPED-COVER".to_vec();

    // Two host-provided-only releases: one made Remote with a pin (Pin disposition),
    // one without (Drop disposition, because the cover is CacheLazy).
    for (note, cover, path, bytes) in [
        ("n-pin", "cover-pin", "cv/cover-pin.jpg", &pin_bytes),
        ("n-drop", "cover-drop", "cv/cover-drop.jpg", &drop_bytes),
    ] {
        db.execute_test_sql(&format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('{note}', 'Release', NULL, 0, '0000000001000-0000-A', '2026-01-01')"
        ))
        .await;
        db.execute_test_sql(&format!(
                "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
                 VALUES ('{cover}', '{note}', {}, '{}', '0000000001000-0000-A', '2026-01-01', '{path}')",
                bytes.len(),
                crate::protocol::blob::content_hash(bytes),
            ),
        )
        .await;
        crate::store_dir::StoreDir::store_local_blob(&lib, "covers", cover, bytes)
            .await
            .expect("store host-provided cover");
    }
    owners
        .make_remote("notes", "n-pin", true)
        .await
        .expect("make_remote pin");
    owners
        .make_remote("notes", "n-drop", false)
        .await
        .expect("make_remote drop");

    // Each upload drain stops after one root becomes publishable. Both flips now
    // own exact Created handoffs and durable local-store cleanup intents.
    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("create pinned exact blob and flip its root");
    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("create unpinned exact blob and flip its root");

    // The crash: Store publication fails, so neither cleanup intent may apply.
    home.fail_exact_create_before_call(1);
    let failed = storage.run_founder_cycle(&lib, None).await;
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
        db.test_row_exists(
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
    storage
        .run_founder_cycle(&lib, None)
        .await
        .expect("run founder cycle");

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
    assert!(
        storage
            .contains_blob_object(&cover_ref(&db, "cover-pin").await)
            .await
    );
    assert!(
        storage
            .contains_blob_object(&cover_ref(&db, "cover-drop").await)
            .await
    );
}

/// The drain applies a disposition (copy to the destination, drop the local-store
/// source) and then clears its intent in a separate commit. A crash in that window
/// leaves the blob correctly placed but the intent uncleared. Re-draining must
/// recognize the completed work — the blob already in pinned/ — and clear the intent,
/// not keep failing every cycle because the source it would copy is gone.
#[tokio::test]
async fn drain_clears_a_pin_disposition_already_applied_before_its_intent() {
    let db = open_test_db();
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
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
    crate::local_file::AtomicStagedFile::write_for_test(&pinned, &bytes)
        .await
        .unwrap();
    let sequence = storage.publish_fixture_position(&lib, "pin-position").await;
    db.insert_published_blob_drop_intent_for_test(
        sequence,
        "covers",
        "cov-pin",
        &bytes,
        crate::sync::test_helpers::test_cache_locator_hash("cov-pin"),
        DeferredLocalBlobDisposition::Pin,
    )
    .await
    .expect("insert published pin disposition");

    storage
        .run_founder_cycle(&lib, None)
        .await
        .expect("run founder cycle");

    assert!(
        !db.published_blob_drop_intent_exists_for_test("cov-pin")
            .await
            .expect("inspect published blob drop intent"),
        "the drain recognizes the completed pin and clears its intent",
    );
    assert!(pinned.exists(), "the pin stays intact");
}

/// Sibling of the pin case for the cache disposition: the blob already sits in cache/
/// with its source dropped, so re-draining recognizes it and clears the intent.
#[tokio::test]
async fn drain_clears_a_cache_disposition_already_applied_before_its_intent() {
    let db = open_test_db();
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
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
    crate::local_file::AtomicStagedFile::write_for_test(&cached, &bytes)
        .await
        .unwrap();
    let sequence = storage
        .publish_fixture_position(&lib, "cache-position")
        .await;
    db.insert_published_blob_drop_intent_for_test(
        sequence,
        "covers",
        "cov-cache",
        &bytes,
        crate::sync::test_helpers::test_cache_locator_hash("cov-cache"),
        DeferredLocalBlobDisposition::Cache,
    )
    .await
    .expect("insert published cache disposition");

    storage
        .run_founder_cycle(&lib, None)
        .await
        .expect("run founder cycle");

    assert!(
        !db.published_blob_drop_intent_exists_for_test("cov-cache")
            .await
            .expect("inspect published blob drop intent"),
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
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (_tmp, lib) = temp_store_dir();

    let sequence = storage
        .publish_fixture_position(&lib, "lost-position")
        .await;
    db.insert_published_blob_drop_intent_for_test(
        sequence,
        "covers",
        "cov-lost",
        b"missing",
        crate::sync::test_helpers::test_cache_locator_hash("cov-lost"),
        DeferredLocalBlobDisposition::Pin,
    )
    .await
    .expect("insert lost published pin disposition");

    let error = crate::sync::test_owner_graph::local_blob_access(store_database, lib.clone())
        .drain_published_blob_drop_intents(sequence)
        .await
        .expect_err("a lost disposition fails the drain");
    assert!(error.contains("missing from both"), "{error}");

    assert!(
        db.published_blob_drop_intent_exists_for_test("cov-lost")
            .await
            .expect("inspect published blob drop intent"),
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
    let storage = create_store(
        &db_a,
        kp_a.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (_tmp_a, lib_a) = temp_store_dir();
    let cover = b"REMOTE-ROOT-HOST-BLOB".to_vec();

    db_a.execute_test_host_write(
        "INSERT INTO notes (id, title, _updated_at, created_at) \
         VALUES ('n-remote-root', 'Remote Root', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    db_a.execute_test_host_write(&format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coverrrr', 'n-remote-root', 'cover', 21, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/remote-root-coverrrr.jpg')",
            crate::protocol::blob::content_hash(&cover),
        ),
    )
    .await;
    crate::store_dir::StoreDir::store_local_blob(&lib_a, "covers", "coverrrr", &cover)
        .await
        .expect("store host-provided blob");

    let db_b = remote_root_db(cover_decl());
    let (_tmp_b, lib_b) = temp_store_dir();
    let owners_b = TestOwnerGraph::new(StoreDatabase::new(&db_b), lib_b.clone());
    let peer = storage
        .invite_and_activate_peer(&db_a, &db_b, &kp_b)
        .await
        .expect("invite and activate peer Store device");
    storage
        .run_founder_cycle(&lib_a, None)
        .await
        .expect("run founder cycle");
    storage
        .run_founder_cycle(&lib_a, None)
        .await
        .expect("run founder cycle");
    assert!(
        storage
            .contains_blob_object(
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
        db_b.test_row_exists("SELECT 1 FROM notes WHERE id = 'n-remote-root'")
            .await,
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
    let got = owners_b
        .read_blob(
            Some(storage.clone()),
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
    let owners = TestOwnerGraph::new(StoreDatabase::new(&db), lib.clone());
    db.execute_test_sql(
        "INSERT INTO notes (id, title, _updated_at, created_at) \
         VALUES ('n-remote-root', 'Remote Root', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    db.execute_test_sql(&format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coverrrr', 'n-remote-root', 'cover', 11, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/remote-root-coverrrr.jpg')",
            crate::protocol::blob::content_hash(b"REMOTE-ROOT"),
        ),
    )
    .await;
    crate::store_dir::StoreDir::store_local_blob(&lib, "covers", "coverrrr", b"REMOTE-ROOT")
        .await
        .expect("store host-provided blob");

    let err = owners
        .make_remote("notes", "n-remote-root", true)
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
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(StoreDatabase::new(&db), lib.clone());
    db.execute_test_sql(
        "INSERT INTO notes (id, title, _updated_at, created_at) \
         VALUES ('n-remote-root', 'Remote Root', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    db.execute_test_sql(&format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coverrrr', 'n-remote-root', 'cover', 11, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/remote-root-coverrrr.jpg')",
            crate::protocol::blob::content_hash(b"REMOTE-ROOT"),
        ),
    )
    .await;
    let dest: HashMap<String, PathBuf> =
        [("coverrrr".to_string(), tmp.path().join("dest/coverrrr.jpg"))].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = owners
        .make_local(
            storage.clone(),
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
    let (_temp, store_dir) = temp_store_dir();

    let err = LocalBlobTransitions::new(store_database, store_dir)
        .cancel_make_remote("notes", "n-remote-root")
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
    let owners = TestOwnerGraph::new(StoreDatabase::new(&db), lib.clone());

    // A host-provided-only root already Remote (gate on): a note plus a cover row.
    db.execute_test_sql(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n-host', 'Host Only', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    db.execute_test_sql(&format!(
            "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at, cloud_path) \
             VALUES ('coverhost', 'n-host', 15, '{}', '0000000001000-0000-A', '2026-01-01', 'cv/host-coverhost.jpg')",
            crate::protocol::blob::content_hash(&[0; 15]),
        ),
    )
    .await;

    let stamp_before = gate_stamp(&db, "n-host").await;

    let err = owners
        .make_remote("notes", "n-host", true)
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
        !db.make_remote_intent_exists_for_test("notes", "n-host")
            .await
            .expect("inspect make_remote intent"),
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
    let store_database = StoreDatabase::new(&db);
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let bytes = b"PHOTO-BYTES-full-length".to_vec();

    let src = owners
        .seed_local_release(
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

    let err = owners
        .make_remote("notes", "n1", true)
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
        !db.make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent"),
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
        store_database
            .external_blob("note_photos", "photoaaa")
            .await
            .expect("load exact external blob ownership")
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
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(StoreDatabase::new(&db), lib.clone());
    let bytes = b"already-local".to_vec();

    // A Local release (gate off) with its blob at a registered external file.
    owners
        .seed_local_release(
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

    let err = owners
        .make_local(storage.clone(), None, None, "notes", "n1", &dest, &cancel)
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
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let user = tmp.path().join("user");

    // Two photos under one release.
    let _src1 = owners
        .seed_local_release(&user, "n1", "photoaaa", "cv/photoaaa.jpg", b"first")
        .await;
    let src2 = user.join("photobbb.jpg");
    std::fs::write(&src2, b"second").unwrap();
    db.add_local_photo_for_test("n1", "photobbb", "cv/photobbb.jpg", b"second", &src2)
        .await;

    owners
        .make_remote("notes", "n1", true)
        .await
        .expect("make_remote");
    assert_eq!(pending_uploads(&db).await, 2, "both uploads queued");

    // Drain with photobbb's source removed: photoaaa uploads (not the last, no flip), photobbb fails.
    std::fs::remove_file(&src2).unwrap();
    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
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
        db.make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent"),
        "the make_remote is still in flight"
    );

    // Cancel: mark the intent, then let the upload drain exact-delete the created
    // object and consume both upload journals atomically with the intent.
    LocalBlobTransitions::new(store_database, lib.clone())
        .cancel_make_remote("notes", "n1")
        .await
        .expect("cancel make_remote");
    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("drain cancelled make_remote cleanup");
    assert_eq!(shared_flag(&db, "n1").await, 0, "the release stays Local");
    assert!(
        !db.make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent"),
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
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let bytes = b"same-locator-created-journals";
    let hash = crate::protocol::blob::content_hash(bytes);

    db.execute_test_sql(&format!(
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
    ))
    .await;
    let user_dir = tmp.path().join("user");
    std::fs::create_dir_all(&user_dir).unwrap();
    let photo_source = user_dir.join("photo.bin");
    let cover_source = user_dir.join("cover.bin");
    std::fs::write(&photo_source, bytes).unwrap();
    std::fs::write(&cover_source, bytes).unwrap();
    crate::database::StoreDatabase::new(&db)
        .register_external_blob_for_test("note_photos", "photo-a", &photo_source)
        .await;
    crate::database::StoreDatabase::new(&db)
        .register_external_blob_for_test("note_photos", "photo-b", &cover_source)
        .await;

    owners
        .make_remote("notes", "n1", false)
        .await
        .expect("queue both same-locator uploads");
    storage.open_into(&db).await.expect("open exact test Store");
    let registration = crate::database::StoreDatabase::new(&db)
        .local_blob_write_authority()
        .await
        .expect("load local blob write authority");
    let authority = crate::protocol::objects::BlobWriteAuthority::new(&registration);
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
            .store_blob_protection()
            .expect("load blob protection");
        let locator = match &protection {
            crate::protocol::objects::BlobSpoolProtection::Opaque(encryption) => {
                crate::protocol::blob::locator::BlobLocator::opaque(
                    row.blob().namespace.clone(),
                    row.blob().id.clone(),
                    registration.reference().clone(),
                    crate::protocol::blob::locator::RemoteAudience::Store,
                    row.blob().scope.clone(),
                    encryption.seal_key_fingerprint(),
                    row.plaintext_size(),
                    row.plaintext_hash(),
                )
            }
            crate::protocol::objects::BlobSpoolProtection::Browsable => {
                crate::protocol::blob::locator::BlobLocator::browsable(
                    row.blob().namespace.clone(),
                    row.blob().id.clone(),
                    registration.reference().clone(),
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
            .seal_blob_to_spool(&locator, &authority, protection, source_path, &spool)
            .await
            .expect("seal shared-locator blob");
        let slot = storage
            .allocate_blob_slot(&locator, &authority)
            .await
            .expect("allocate distinct exact blob slot");
        let stored = storage
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

    LocalBlobTransitions::new(store_database, lib.clone())
        .cancel_make_remote("notes", "n1")
        .await
        .expect("cancel same-locator uploads");
    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("drain exact cancellation cleanup");

    assert_eq!(pending_uploads(&db).await, 0, "both handoffs are cleared");
    assert!(!db
        .make_remote_intent_exists_for_test("notes", "n1")
        .await
        .expect("inspect make_remote intent"));
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
    let home = crate::sync::test_helpers::test_cloud_home();
    let storage = create_store(&db, UserKeypair::generate(), home.clone()).await;
    let (tmp, lib) = temp_store_dir();

    let src = TestOwnerGraph::new(StoreDatabase::new(&db), lib.clone())
        .seed_local_release(
            &tmp.path().join("user"),
            "n1",
            "photoaaa",
            "cv/photoaaa.jpg",
            b"orphan-bytes",
        )
        .await;
    // Enqueue an upload with no intent to model impossible durable state directly.
    let row = photo_ref(&db, "photoaaa").await;
    db.test_sql(move |database| {
        database.enqueue_blob_upload("notes", "n1", &row, &src, true, "0000000001000-0000-A")
    })
    .await
    .unwrap();

    let deletes_before = home.exact_delete_count();
    let outcome = storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("drain");

    assert_eq!(outcome.failures().failures().len(), 1);
    assert!(
        outcome.failures().failures()[0]
            .cause
            .to_string()
            .contains("make_remote intent"),
        "the missing owner is reported",
    );
    assert_eq!(shared_flag(&db, "n1").await, 0, "no intent means no flip");
    assert!(pending_deletes(&db).await.is_empty());
    assert_eq!(home.exact_delete_count(), deletes_before);
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
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let bytes = b"still-managed".to_vec();

    owners
        .seed_remote_release(&storage, None, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes)
        .await;

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();
    // Already cancelled (initial value true) before the first materialize.
    let (_cancel_tx, cancel) = watch::channel(true);

    let err = owners
        .make_local(storage.clone(), None, None, "notes", "n1", &dest, &cancel)
        .await
        .expect_err("a cancelled make_local aborts");
    assert!(matches!(
        err,
        crate::blob::transition::MakeLocalError::Cancelled
    ));

    assert_eq!(shared_flag(&db, "n1").await, 1, "the release stays Remote");
    assert!(
        store_database
            .external_blob("note_photos", "photoaaa")
            .await
            .expect("load exact external blob ownership")
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
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let bytes = b"managed-bytes".to_vec();

    owners
        .seed_remote_release(&storage, None, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes)
        .await;

    // Block the dest: make the dest's parent dir a FILE, so create_dir_all fails.
    let blocker = tmp.path().join("blocker");
    std::fs::write(&blocker, b"x").unwrap();
    let dest_path = blocker.join("photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path)].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = owners
        .make_local(storage.clone(), None, None, "notes", "n1", &dest, &cancel)
        .await
        .expect_err("the dest write fails");
    assert!(matches!(
        err,
        crate::blob::transition::MakeLocalError::Write { .. }
    ));

    assert_eq!(shared_flag(&db, "n1").await, 1, "the release stays Remote");
    assert!(
        store_database
            .external_blob("note_photos", "photoaaa")
            .await
            .expect("load exact external blob ownership")
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
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let bytes = b"materialized-before-commit-failure".to_vec();

    owners
        .seed_remote_release(&storage, None, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes)
        .await;
    db.test_sql(|connection| {
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

    let error = owners
        .make_local(storage.clone(), None, None, "notes", "n1", &dest, &cancel)
        .await
        .expect_err("the gate update fails after materialization");

    assert!(
        matches!(error, crate::blob::transition::MakeLocalError::Db(_)),
        "the database failure surfaces: {error:?}",
    );
    assert!(!dest_path.exists(), "the materialized file is rolled back");
    assert_eq!(shared_flag(&db, "n1").await, 1, "the root stays Remote");
    assert!(
        store_database
            .external_blob("note_photos", "photoaaa")
            .await
            .expect("load exact external blob ownership")
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
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let bytes = b"managed-bytes".to_vec();

    owners
        .seed_remote_release(&storage, None, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes)
        .await;

    // A dest whose filename is not valid UTF-8: `to_str()` returns None, so the
    // conversion must fail loud instead of lossily rewriting the path. Kept under the
    // temp dir so the rolled-back partial is contained.
    let bad = tmp.path().join(OsStr::from_bytes(b"photo-\xff\xfe.jpg"));
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), bad)].into();
    let (_cancel_tx, cancel) = watch::channel(false);

    let err = owners
        .make_local(storage.clone(), None, None, "notes", "n1", &dest, &cancel)
        .await
        .expect_err("a non-UTF-8 dest aborts");
    assert!(matches!(
        err,
        crate::blob::transition::MakeLocalError::NonUtf8Dest { .. }
    ));

    assert_eq!(shared_flag(&db, "n1").await, 1, "the release stays Remote");
    assert!(
        store_database
            .external_blob("note_photos", "photoaaa")
            .await
            .expect("load exact external blob ownership")
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
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let user = tmp.path().join("user");

    owners
        .seed_local_release(&user, "n1", "photoaaa", "cv/photoaaa.jpg", b"first")
        .await;
    let src2 = user.join("photobbb.jpg");
    std::fs::write(&src2, b"second").unwrap();
    db.add_local_photo_for_test("n1", "photobbb", "cv/photobbb.jpg", b"second", &src2)
        .await;

    owners
        .make_remote("notes", "n1", true)
        .await
        .expect("make_remote");

    // "Crash" after photoaaa uploads but before completion: remove photobbb's source so the
    // first drain uploads only photoaaa, leaving the make_remote in flight.
    std::fs::remove_file(&src2).unwrap();
    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("partial drain");
    assert_eq!(shared_flag(&db, "n1").await, 0, "still Local-uploading");
    assert!(
        db.make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent"),
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
    storage
        .drain_uploads(&StoreDatabase::new(&db), &lib, &SystemClock, None, None)
        .await
        .expect("resume drain");
    assert_eq!(shared_flag(&db, "n1").await, 1, "converged to Remote");
    assert!(
        db.make_remote_intent_exists_for_test("notes", "n1")
            .await
            .expect("inspect make_remote intent"),
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
    assert!(!db
        .make_remote_intent_exists_for_test("notes", "n1")
        .await
        .expect("inspect make_remote intent"));
    assert_eq!(pending_uploads(&db).await, 0);
    assert!(store_database
        .external_blob("note_photos", "photoaaa")
        .await
        .expect("load exact external blob ownership")
        .is_none());
    assert!(store_database
        .external_blob("note_photos", "photobbb")
        .await
        .expect("load exact external blob ownership")
        .is_none());
}

/// An aborted make_local (here via cancel) leaves the release Remote; retrying from
/// scratch converges to Local with the cloud delete enqueued — re-materialize +
/// re-commit is idempotent.
#[tokio::test]
async fn make_local_abort_then_retry_converges() {
    let db = open_test_db_with_blob(photo_decl());
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database, lib.clone());
    let bytes = b"materialize-me".to_vec();

    owners
        .seed_remote_release(&storage, None, "n1", "photoaaa", "cv/photoaaa.jpg", &bytes)
        .await;

    let dest_path = tmp.path().join("dest/photoaaa.jpg");
    let dest: HashMap<String, PathBuf> = [("photoaaa".to_string(), dest_path.clone())].into();

    // First attempt is cancelled before the commit (the "crash"): still Remote.
    let (_cancel_tx, cancelled) = watch::channel(true);
    let err = owners
        .make_local(
            storage.clone(),
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
    owners
        .make_local(storage.clone(), None, None, "notes", "n1", &dest, &fresh)
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
    let store_database = StoreDatabase::new(&db);
    let storage = create_store(
        &db,
        UserKeypair::generate(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (tmp, lib) = temp_store_dir();
    let owners = TestOwnerGraph::new(store_database.clone(), lib.clone());
    let bytes = b"round-trip-photo".to_vec();

    // Start Local, make it Remote.
    owners
        .seed_local_release(
            &tmp.path().join("user"),
            "n1",
            "photoaaa",
            "cv/photoaaa.jpg",
            &bytes,
        )
        .await;
    owners
        .make_remote("notes", "n1", true)
        .await
        .expect("make_remote 1");
    storage
        .run_founder_cycle(&lib, None)
        .await
        .expect("run founder cycle");
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
    owners
        .make_local(storage.clone(), None, None, "notes", "n1", &dest, &cancel)
        .await
        .expect("make_local");
    // The retract cycle writes the tombstone.
    storage
        .run_founder_cycle(&lib, None)
        .await
        .expect("run founder cycle");
    storage
        .run_founder_cycle(&lib, None)
        .await
        .expect("run founder cycle");
    assert_eq!(shared_flag(&db, "n1").await, 0, "Local after make_local");
    assert!(
        storage
            .contains_blob_tombstone(&first_remote)
            .await
            .unwrap(),
        "the make_local tombstoned the cloud blob",
    );

    // Second make_remote: the external file is uploaded to a new exact object and
    // the gate flips back on.
    owners
        .make_remote("notes", "n1", true)
        .await
        .expect("make_remote 2");
    storage
        .run_founder_cycle(&lib, None)
        .await
        .expect("run founder cycle");
    assert_eq!(
        shared_flag(&db, "n1").await,
        1,
        "Remote again after the second make_remote"
    );
    assert!(
        store_database
            .external_blob("note_photos", "photoaaa")
            .await
            .expect("load exact external blob ownership")
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
            .contains_blob_tombstone(&first_remote)
            .await
            .unwrap(),
        "the old exact object's tombstone remains valid",
    );
    storage
        .verify_blob_object(&second_remote)
        .await
        .expect("the replacement exact blob is in the cloud");
}
