//! "Join then sync" end-to-end: a device that bootstraps from a snapshot and
//! then runs the real [`run_single_sync_cycle`] must not republish — and thereby
//! clobber — the shared snapshot.
//!
//! `multi_device_managed_edit_reaches_restore` (in `snapshot.rs`) covers
//! bootstrap plus a *hand-rolled* pull and snapshot push; it never drives the
//! real cycle, so it can't see that the cycle, run on a just-joined device whose
//! `sync_state` the join path left empty, trips `is_initial_sync` and overwrites
//! the owner's snapshot with its own.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::clock::SystemClock;
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::join::open_db_and_pull;
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::snapshot::{bootstrap_from_snapshot, create_snapshot, push_snapshot};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

#[tokio::test]
async fn joined_device_first_cycle_does_not_clobber_the_shared_snapshot() {
    let enc = EncryptionService::new_with_key(&[7u8; 32]);
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();

    // Owner A leaves the cloud in the shape "empty library, then import" produces:
    // an empty snapshot at seq 0 (the initial-sync snapshot of the empty library),
    // and the imported row as changeset A/1. `should_create_snapshot` does not
    // refresh the snapshot for a single sub-threshold changeset, so the catalog
    // lives in the changeset — not the snapshot.
    let db_a = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let (tables_c, enc_c) = (tables.clone(), enc.clone());
    let empty_snap = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c, &enc_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner empty snapshot");
    push_snapshot(&storage, empty_snap, "A", HashMap::new(), 0, &SystemClock)
        .await
        .expect("push empty snapshot");

    let cs1 = capture_bytes(
        &db_a,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Album Title', NULL, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 1, &cs1, SCHEMA_VERSION);

    // The shared snapshot before B joins. B must not replace it.
    let snapshot_before = storage.get_snapshot().await.expect("snapshot present");

    // Device B joins through the real path: bootstrap from the snapshot, then
    // pull the changesets published after it.
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

    let (db_b, _stamper) =
        Database::open(&lib_b.db_path(), tables.clone(), "B".to_string(), |_c| {
            Ok(())
        })
        .expect("open B db");
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "Album Title",
        "B must receive the imported row through bootstrap + pull",
    );

    // B's first real sync cycle, with no local changes of its own.
    let enc_lock = RwLock::new(enc.clone());
    let keypair = UserKeypair::generate();
    let b_hlc = Hlc::new("B".to_string());
    run_single_sync_cycle(
        &storage,
        "B",
        &b_hlc,
        &SystemClock,
        &db_b,
        &enc_lock,
        &keypair,
        &lib_b,
        None,
        &NoopBlobPlan,
        None,
    )
    .await
    .expect("B sync cycle");

    // A just-joined device with no local changes must leave the shared snapshot
    // untouched. Today the join path persists no `snapshot_seq`, so the cycle
    // trips `is_initial_sync` and overwrites the owner's snapshot with B's own.
    let snapshot_after = storage.get_snapshot().await.expect("snapshot present");
    assert_eq!(
        snapshot_after, snapshot_before,
        "a just-joined device's first cycle must not republish/clobber the shared snapshot",
    );
}
