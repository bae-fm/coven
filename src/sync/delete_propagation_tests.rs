//! End-to-end blob-delete behavior across two devices: an owner that deletes a
//! release, and a joined device that pulls it afterward.
//!
//! coven deletes a queued cloud blob as soon as the deletion is queued and the
//! cloud is reachable — it does not hold the delete until peers have synced past
//! it. This drives the real `run_single_sync_cycle` for both devices and asserts
//! the blob is removed on the deleting device's own cycle, before the joined
//! device has pulled the deletion, and that the joined device still receives the
//! row's removal on its own next cycle.

use std::sync::RwLock;

use crate::clock::SystemClock;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::CloudHome;
use crate::sync::cloud_storage::CloudCipher;
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
    cipher: &RwLock<CloudCipher>,
    kp: &UserKeypair,
    lib: &LibraryDir,
) -> Result<SyncCycleResult, String> {
    run_single_sync_cycle(
        storage,
        device_id,
        hlc,
        &SystemClock,
        db,
        cipher,
        kp,
        lib,
        Some(storage as &dyn CloudHome),
        &NoopBlobPlan,
        None,
    )
    .await
}

/// The blob delete reaches the cloud on the deleting device's own cycle, before
/// the joined device has pulled the deletion — no peer-sync wait — and the joined
/// device still receives the row removal on its own next cycle.
#[tokio::test]
async fn blob_delete_fires_immediately_without_waiting_for_peers() {
    let cipher = CloudCipher::Encrypted(EncryptionService::new_with_key(&[11u8; 32]));
    let enc_a = RwLock::new(cipher.clone());
    let enc_b = RwLock::new(cipher.clone());
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

    // Device B joins: bootstrap from A's snapshot, then pull A/1.
    let (_tmp_b, lib_b) = temp_library_dir();
    let boot = bootstrap_from_snapshot(&storage, &cipher, &lib_b.db_path())
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

    // A deletes the note and queues its blob for cloud deletion, mirroring the
    // host. The blob is already in the cloud (uploaded when A imported it).
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
    db_a.enqueue_delete(blob_key, T0)
        .await
        .expect("enqueue blob delete");

    // A pushes the deletion as A/2 and runs process_deletes. B has NOT pulled the
    // deletion yet, but the blob must be removed anyway — the delete does not wait
    // on the peer.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc_a, &kp_a, &lib_a)
        .await
        .expect("A push deletion + drain delete");
    assert!(
        !storage.exists(blob_key).await.expect("exists check"),
        "the blob delete must reach the cloud on the deleting device's own cycle",
    );
    assert!(
        db_a.get_pending_cloud_deletes()
            .await
            .expect("pending")
            .is_empty(),
        "the outbox delete is drained immediately, not held",
    );

    // The delete fired while B still holds the referencing row — proof it did not
    // wait for B to sync.
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B still holds the note when the blob was already deleted (no peer wait)",
    );

    // B's own next cycle pulls the deletion: the row removal reaches it
    // independently of the blob delete.
    run_cycle(&storage, "B", &hlc_b, &db_b, &enc_b, &kp_b, &lib_b)
        .await
        .expect("B pull deletion");
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B receives the row removal on its own cycle",
    );
}

/// A plaintext home round-trips a real library through the real cycle: device A,
/// over a `CloudCipher::Plaintext` home, runs a cycle on a small library; the
/// snapshot it pushes is stored in the clear (a valid SQLite image, not
/// ciphertext). Device B, also plaintext, bootstraps from that snapshot and reads
/// A's rows, then pulls A's later changeset and sees the update — proving the
/// plaintext snapshot + changeset path works end to end through the cycle.
#[tokio::test]
async fn plaintext_home_snapshot_and_changeset_round_trip_through_the_cycle() {
    let cipher_a = RwLock::new(CloudCipher::Plaintext);
    let cipher_b = RwLock::new(CloudCipher::Plaintext);
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();
    let kp_a = UserKeypair::generate();
    let kp_b = UserKeypair::generate();

    // Owner A imports a shared note, then runs a cycle. A library with data but no
    // changeset yet trips the initial-sync path, so the cycle pushes a snapshot.
    let db_a = open_test_db();
    let (_tmp_a, lib_a) = temp_library_dir();
    let hlc_a = Hlc::new("A".to_string());
    exec(
        &db_a,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Plain Album', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    run_cycle(&storage, "A", &hlc_a, &db_a, &cipher_a, &kp_a, &lib_a)
        .await
        .expect("A initial snapshot cycle");

    // The snapshot is stored in the clear: a valid SQLite image, not ciphertext.
    let at_rest = storage.get_snapshot().await.expect("snapshot pushed");
    assert!(
        at_rest.starts_with(b"SQLite format 3\0"),
        "a plaintext home stores the snapshot as a bare SQLite image, not ciphertext",
    );

    // Device B bootstraps from the plaintext snapshot — `CloudCipher::Plaintext`
    // opens it verbatim — and reads A's row.
    let (_tmp_b, lib_b) = temp_library_dir();
    let boot = bootstrap_from_snapshot(&storage, &CloudCipher::Plaintext, &lib_b.db_path())
        .await
        .expect("B bootstrap from plaintext snapshot");
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
        "Plain Album",
        "B reads A's row out of the plaintext snapshot",
    );

    // A edits the row and pushes it as a plaintext changeset; B pulls it.
    exec(
        &db_a,
        "UPDATE notes SET title = 'Plain Album (Deluxe)', \
         _updated_at = '0000000002000-0000-A' WHERE id = 'n1'",
    )
    .await;
    run_cycle(&storage, "A", &hlc_a, &db_a, &cipher_a, &kp_a, &lib_a)
        .await
        .expect("A push update changeset");

    // The changeset is stored in the clear too.
    let cs_at_rest = storage
        .get_changeset("A", 1)
        .await
        .expect("A's changeset present");
    assert!(
        !cs_at_rest.is_empty(),
        "A's plaintext changeset is stored under its bare key",
    );

    let hlc_b = Hlc::new("B".to_string());
    run_cycle(&storage, "B", &hlc_b, &db_b, &cipher_b, &kp_b, &lib_b)
        .await
        .expect("B pull the update");
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "Plain Album (Deluxe)",
        "B receives A's update through the plaintext changeset round-trip",
    );
}
