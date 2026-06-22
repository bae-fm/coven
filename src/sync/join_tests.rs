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
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::join::open_db_and_pull;
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::snapshot::{
    bootstrap_from_snapshot, create_snapshot, push_snapshot, SNAPSHOT_BLOB_BACKFILL_PENDING,
};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

#[tokio::test]
async fn joined_device_first_cycle_does_not_clobber_the_shared_snapshot() {
    let enc = CloudCipher::Encrypted(EncryptionService::new_with_key(&[7u8; 32]));
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
        None,
        &storage,
        &boot.cursors,
        &lib_b,
        &NoopBlobSource,
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
        &NoopBlobSource,
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

/// A device that bootstraps from a snapshot must receive not just the catalog
/// rows but the blob *files* those rows reference. The snapshot is a whole-DB
/// image carrying a `note_photos` row, but no per-row blob file; the pull that
/// follows starts past the snapshot's cursors, so the INSERT changeset that first
/// carried the photo (seq <= cursor) is never re-walked and the per-changeset
/// blob download never fires for it. Without the bootstrap backfill, the
/// bootstrapped device has the photo row but the file at its `local_path` is
/// absent — a synced album renders a placeholder cover. Asserts the file lands.
#[tokio::test]
async fn bootstrap_backfills_blob_files_for_snapshot_rows() {
    let enc = CloudCipher::Encrypted(EncryptionService::new_with_key(&[9u8; 32]));
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();

    // Owner A: a shared note with a cover photo, both captured into the snapshot.
    let db_a = open_test_db();
    exec(
        &db_a,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db_a,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('photo1', 'n1', 'cover', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;

    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let (tables_c, enc_c) = (tables.clone(), enc.clone());
    let snapshot = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c, &enc_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    push_snapshot(&storage, snapshot, "A", HashMap::new(), 1, &SystemClock)
        .await
        .expect("push snapshot");

    // The cover blob exists in the cloud (uploaded when A first imported the
    // album), keyed `photos/photo1` as `PhotoBlobSource` maps a cover row. The
    // mock ignores the scope, so any resolved scope seeds it.
    storage
        .put_blob(
            "photos",
            "photo1",
            crate::blob::ResolvedScope::Derived("n1".to_string()),
            None,
            b"cover-bytes".to_vec(),
        )
        .await
        .expect("seed cover blob");

    // Device B bootstraps from the snapshot, then runs the real bootstrap path
    // with a plan that maps `note_photos` rows to blobs under B's library dir.
    let (_tmp_b, lib_b) = temp_library_dir();
    let blob_dir = lib_b.join("photos");
    let plan = PhotoBlobSource {
        dir: blob_dir.clone(),
    };
    let expected_blob = blob_dir.join("photo1");

    let boot = bootstrap_from_snapshot(&storage, &enc, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        "B",
        None,
        &storage,
        &boot.cursors,
        &lib_b,
        &plan,
    )
    .await
    .expect("B open_db_and_pull");

    assert!(
        expected_blob.exists(),
        "the cover blob file must be backfilled to {} after bootstrap",
        expected_blob.display(),
    );
    assert_eq!(
        std::fs::read(&expected_blob).expect("read backfilled blob"),
        b"cover-bytes",
        "the backfilled file must hold the blob's plaintext bytes",
    );
}

/// Read the snapshot-blob-backfill pending flag (true while a bootstrap could not
/// land every referenced blob) from a library's `sync_state`.
async fn backfill_pending(db: &Database) -> bool {
    db.get_sync_state(SNAPSHOT_BLOB_BACKFILL_PENDING)
        .await
        .expect("read backfill flag")
        .is_some_and(|v| !v.is_empty())
}

/// A blob whose download fails at bootstrap (its object isn't in the cloud yet)
/// must not be lost: the bootstrap records that the reconciliation is incomplete,
/// and a later sync cycle re-runs it once the object is available. This is the
/// retry the bootstrap's swallow-and-continue relies on — before it existed, a
/// transient failure stranded the file permanently, because the pull only
/// downloads blobs for changesets past the per-device cursor and bootstrap
/// advanced that cursor past the INSERT that carried this one.
#[tokio::test]
async fn snapshot_blob_backfill_retries_on_a_later_cycle() {
    let enc = CloudCipher::Encrypted(EncryptionService::new_with_key(&[11u8; 32]));
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();

    // Owner A: a shared note with a cover photo, both captured into the snapshot.
    let db_a = open_test_db();
    exec(
        &db_a,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db_a,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('photo1', 'n1', 'cover', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;

    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let (tables_c, enc_c) = (tables.clone(), enc.clone());
    let snapshot = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c, &enc_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    push_snapshot(&storage, snapshot, "A", HashMap::new(), 1, &SystemClock)
        .await
        .expect("push snapshot");

    // Unlike the happy-path test above, the cover blob is NOT in the cloud yet at
    // bootstrap time (e.g. A's upload of it hadn't landed). So the bootstrap's
    // download attempt fails.
    let (_tmp_b, lib_b) = temp_library_dir();
    let blob_dir = lib_b.join("photos");
    let plan = PhotoBlobSource {
        dir: blob_dir.clone(),
    };
    let expected_blob = blob_dir.join("photo1");

    let boot = bootstrap_from_snapshot(&storage, &enc, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        "B",
        None,
        &storage,
        &boot.cursors,
        &lib_b,
        &plan,
    )
    .await
    .expect("B open_db_and_pull");

    // After bootstrap the file is absent and the pending flag is set: the catalog
    // landed, the blob did not, and the cycle must reconcile it later.
    assert!(
        !expected_blob.exists(),
        "the cover blob must be absent after a bootstrap whose download failed",
    );
    let (db_b, _stamper) =
        Database::open(&lib_b.db_path(), tables.clone(), "B".to_string(), |_c| {
            Ok(())
        })
        .expect("open B db");
    assert!(
        backfill_pending(&db_b).await,
        "bootstrap must record the backfill as pending when a blob did not land",
    );

    // The object becomes available in the cloud (A's upload landed).
    storage
        .put_blob(
            "photos",
            "photo1",
            crate::blob::ResolvedScope::Derived("n1".to_string()),
            None,
            b"cover-bytes".to_vec(),
        )
        .await
        .expect("seed cover blob");

    // A normal sync cycle now reconciles the missing blob and clears the flag.
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
        &plan,
        None,
    )
    .await
    .expect("B sync cycle");

    assert!(
        expected_blob.exists(),
        "a later sync cycle must land the previously-missing blob at {}",
        expected_blob.display(),
    );
    assert_eq!(
        std::fs::read(&expected_blob).expect("read reconciled blob"),
        b"cover-bytes",
        "the reconciled file must hold the blob's plaintext bytes",
    );
    assert!(
        !backfill_pending(&db_b).await,
        "the cycle that lands every blob must clear the pending flag",
    );
}
