//! The sync cycle defers a changeset push while cloud uploads are still pending
//! — remote devices must not learn about releases whose audio isn't in the cloud
//! yet. But a deferred changeset must *survive* to a later cycle: it was taken
//! out of the capture session (`take_changeset_and_suspend`), so dropping it on
//! defer loses those mutations forever, and the device's `local_seq` never
//! advances past them even after the uploads finish.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::clock::SystemClock;
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::pull::pull_changes;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

/// Run one sync cycle for device "M" against the mock storage with no cloud home.
async fn run_cycle_m(
    storage: &MockSyncStorage,
    db: &Database,
    enc: &RwLock<EncryptionService>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &LibraryDir,
) {
    run_single_sync_cycle(
        storage,
        "M",
        hlc,
        &SystemClock,
        db,
        enc,
        keypair,
        ld,
        None,
        &NoopBlobPlan,
        None,
    )
    .await
    .expect("cycle");
}

/// Seed one pending `upload` outbox entry so `has_pending_cloud_uploads` is true.
/// The source path doesn't exist, so the cycle's upload pass can't drain it —
/// the entry stays pending, which is what we want for the deferral.
async fn seed_pending_upload(db: &Database) {
    let scope = crate::blob::BlobScope::Master.to_outbox_str();
    db.call(move |conn| {
        conn.execute(
            &format!(
                "INSERT INTO cloud_outbox \
                 (id, operation, file_id, cloud_key, source_path, scope, created_at, attempt_count) \
                 VALUES (1, 'upload', 'f1', 'storage/aa/bb/f1', '/nonexistent/f1', '{scope}', \
                         '2024-01-01T00:00:00Z', 0)"
            ),
            [],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .expect("seed pending upload");
}

#[tokio::test]
async fn changeset_deferred_for_pending_uploads_is_not_lost() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(EncryptionService::new_with_key(&[5u8; 32]));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // A pending cloud upload makes the cycle defer any changeset push.
    seed_pending_upload(&db).await;

    // A local change captured into the session — the equivalent of flipping a
    // release to managed=1 after enqueuing its audio for upload. `shared=1` so
    // the gate keeps it (an unshared row would be cut and never sync).
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    // Cycle 1: the upload is still pending, so the push is deferred — nothing in
    // the cloud yet.
    run_single_sync_cycle(
        &storage,
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &keypair,
        &ld,
        None,
        &NoopBlobPlan,
        None,
    )
    .await
    .expect("cycle 1");
    assert!(
        storage.get_changeset("M", 1).await.is_err(),
        "while uploads are pending the changeset push must be deferred",
    );

    // The upload completes; the outbox drains.
    exec(&db, "DELETE FROM cloud_outbox WHERE operation = 'upload'").await;

    // Cycle 2: no pending uploads, and no *new* local changes — the only thing to
    // push is the change captured in cycle 1. It must reach the cloud, not have
    // been dropped with cycle 1's capture session.
    run_single_sync_cycle(
        &storage,
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &keypair,
        &ld,
        None,
        &NoopBlobPlan,
        None,
    )
    .await
    .expect("cycle 2");
    assert!(
        storage.get_changeset("M", 1).await.is_ok(),
        "a changeset deferred for pending uploads must be pushed once uploads \
         complete, not dropped",
    );
}

#[tokio::test]
async fn deferred_changesets_accumulate_across_cycles_then_all_reach_a_peer() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(EncryptionService::new_with_key(&[6u8; 32]));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // A pending upload keeps every push deferred across the next two cycles.
    seed_pending_upload(&db).await;

    // First shared note, captured and deferred (staged).
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'One', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    // A SECOND shared note while uploads are still pending. It must ACCUMULATE
    // into the staged changeset, not overwrite the first — otherwise n1 is lost,
    // exactly the empty-catalog failure (manage one album, edit another, the
    // first never syncs).
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n2', 'Two', NULL, 1, '0000000002000-0000-M', '2026-01-01')",
    )
    .await;
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_err(),
        "both changesets stay deferred while uploads are pending",
    );

    // Uploads finish; the accumulated changeset pushes.
    exec(&db, "DELETE FROM cloud_outbox WHERE operation = 'upload'").await;
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_ok(),
        "the accumulated changeset must push once uploads complete",
    );

    // A fresh peer pulls and must receive BOTH notes — proving the second
    // deferral accumulated onto the first rather than clobbering it.
    let db_b = open_test_db();
    db_b.take_changeset_and_suspend().await.expect("suspend B");
    pull_changes(
        &db_b,
        &test_synced_tables(),
        &storage,
        "B",
        &HashMap::new(),
        &ld,
        &NoopBlobPlan,
    )
    .await
    .expect("B pull");
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "One",
        "the first deferred note must survive accumulation and reach the peer",
    );
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n2'").await,
        "Two",
        "the second deferred note must also reach the peer",
    );
}
