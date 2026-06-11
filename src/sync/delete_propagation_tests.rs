//! End-to-end blob-delete propagation across two devices: an owner that deletes
//! a blob, and a joined device that only pulls afterward.
//!
//! A blob delete in the cloud outbox is held until every peer's published head
//! proves that peer has pulled past the deletion's `min_seq` — otherwise a peer
//! still holding the referencing row would later fetch a blob that is gone. That
//! gate reads the peer's cursor out of the peer's *published head*. A device only
//! republishes its head when it has a changeset of its own to push, so a device
//! that pulls the deletion but writes nothing never advertises its advanced
//! cursor — and the deleter defers the blob delete forever. This drives the real
//! `run_single_sync_cycle` for both devices and asserts the delete actually
//! drains once the joined device has pulled past it.

use std::sync::RwLock;

use crate::clock::SystemClock;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::CloudHome;
use crate::sync::cycle::{run_single_sync_cycle, SyncCycleResult};
use crate::sync::hlc::Hlc;
use crate::sync::join::open_db_and_pull;
use crate::sync::snapshot::bootstrap_from_snapshot;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

const T0: &str = "2024-06-01T00:00:00Z";

/// Run one real sync cycle for `device_id` against the shared storage, with the
/// cloud home wired so the outbox drains and `process_deletes` runs.
#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    storage: &MockSyncStorage,
    device_id: &str,
    hlc: &Hlc,
    db: &Database,
    enc: &RwLock<EncryptionService>,
    kp: &UserKeypair,
    lib: &LibraryDir,
) -> Result<SyncCycleResult, String> {
    run_single_sync_cycle(
        storage,
        device_id,
        hlc,
        &SystemClock,
        db,
        enc,
        kp,
        lib,
        Some(storage as &dyn CloudHome),
        &NoopBlobPlan,
        None,
    )
    .await
}

/// A blob delete must reach the cloud once the joined device has pulled past the
/// deletion — even though that device writes nothing of its own after pulling.
#[tokio::test]
async fn blob_delete_drains_after_joined_device_pulls_without_writing() {
    let enc = EncryptionService::new_with_key(&[11u8; 32]);
    let enc_a = RwLock::new(enc.clone());
    let enc_b = RwLock::new(enc.clone());
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();
    let kp_a = UserKeypair::generate();
    let kp_b = UserKeypair::generate();

    // Owner A, with its own db + library dir, driven through the real cycle.
    let db_a = open_test_db();
    let (_tmp_a, lib_a) = temp_library_dir();
    let hlc_a = Hlc::new("A".to_string());

    // A's first cycle on an empty library pushes the initial snapshot (so B can
    // join from it). Nothing local yet, so no changeset.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc_a, &kp_a, &lib_a)
        .await
        .expect("A initial snapshot cycle");

    // A imports a shared note and pushes it as changeset A/1.
    exec(
        &db_a,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc_a, &kp_a, &lib_a)
        .await
        .expect("A push n1");

    // Device B joins: bootstrap from A's snapshot, then pull A/1. open_db_and_pull
    // persists B's cursors and snapshot_seq, so B's later cycles treat it as a
    // joined device (no initial-sync snapshot of its own).
    let (_tmp_b, lib_b) = temp_library_dir();
    let boot = bootstrap_from_snapshot(&storage, &enc, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        "B",
        &storage,
        &boot.cursors,
        &lib_b,
        &NoopBlobPlan,
    )
    .await
    .expect("B open_db_and_pull");
    let (db_b, _stamper_b) =
        Database::open(&lib_b.db_path(), tables.clone(), "B".to_string(), |_c| {
            Ok(())
        })
        .expect("open B db");
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "Album Title",
        "B must receive the imported note through bootstrap + pull",
    );

    let hlc_b = Hlc::new("B".to_string());

    // B writes a note of its own and pushes B/1 — this publishes B's head with its
    // pull cursor for A. B is now a known peer with a cursor A can read, exactly
    // the situation of a second device that has synced before.
    exec(
        &db_b,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n2', 'Other Title', NULL, 1, '0000000002000-0000-B', '2026-01-01')",
    )
    .await;
    run_cycle(&storage, "B", &hlc_b, &db_b, &enc_b, &kp_b, &lib_b)
        .await
        .expect("B push n2");

    // A pulls B/1 so A knows B as a peer.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc_a, &kp_a, &lib_a)
        .await
        .expect("A pull B/1");

    // A deletes the note and queues its blob for cloud deletion, mirroring the
    // host: read local_seq as the floor, then enqueue. The blob is already in the
    // cloud (uploaded when A imported it).
    let blob_key = "storage/blob1";
    storage
        .put_blob(
            "storage",
            "blob1",
            crate::blob::ResolvedScope::Master,
            b"audio-bytes".to_vec(),
        )
        .await
        .expect("seed cloud blob");
    exec(&db_a, "DELETE FROM notes WHERE id = 'n1'").await;
    let min_seq = db_a.local_seq().await.expect("read local_seq");
    db_a.enqueue_delete(blob_key, min_seq, T0)
        .await
        .expect("enqueue blob delete");

    // A pushes the deletion as A/2 and runs process_deletes. B has not pulled the
    // deletion yet, so its published cursor for A is still behind min_seq — the
    // delete must be deferred and the blob must remain.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc_a, &kp_a, &lib_a)
        .await
        .expect("A push deletion");
    assert!(
        storage.exists(blob_key).await.expect("exists check"),
        "the blob must NOT be deleted before the peer has pulled past the deletion",
    );
    assert_eq!(
        db_a.get_pending_cloud_deletes()
            .await
            .expect("pending")
            .len(),
        1,
        "the delete stays queued while the peer trails",
    );

    // B pulls the deletion. It has no local change of its own this cycle — the
    // exact pull-only case that strands the delete when the head is not republished.
    run_cycle(&storage, "B", &hlc_b, &db_b, &enc_b, &kp_b, &lib_b)
        .await
        .expect("B pull deletion");
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B must have pulled the deletion (its copy of the note is gone)",
    );

    // A runs once more: now B's published head proves it pulled past min_seq, so
    // the blob delete is safe and must actually fire.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc_a, &kp_a, &lib_a)
        .await
        .expect("A process deletes");
    assert!(
        !storage.exists(blob_key).await.expect("exists check"),
        "the blob delete must reach the cloud once the joined device pulled past it",
    );
    assert!(
        db_a.get_pending_cloud_deletes()
            .await
            .expect("pending")
            .is_empty(),
        "the outbox delete must be drained, not stranded",
    );
}
