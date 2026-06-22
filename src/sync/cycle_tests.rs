//! Blob-before-row ordering is the host's job, enforced per row by the gate
//! column: the host keeps a blob-bearing row's gate column off until that row's
//! blobs upload, then flips it on (in `on_blob_uploaded`), so the changeset gate
//! — and the snapshot, which runs the same gate — only ever carry rows whose
//! blobs are in the cloud. The sync cycle does not hold the whole changeset back
//! on a global "any upload pending" flag.
//!
//! These tests pin that contract: a pending upload does not hold back an
//! already-shareable (gated-true) changeset or snapshot, a gated-false row is
//! withheld until its gate flips, and a `DrainControl::Publish` from the observer
//! lets the cycle publish a just-completed unit mid-batch (surfaced as
//! `resume_drain_promptly` so the loop runs the next cycle promptly).

use std::collections::HashMap;
use std::sync::RwLock;

use crate::blob::{BlobScope, BlobUploadObserver};
use crate::clock::SystemClock;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::CloudHome;
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

const T0: &str = "2024-01-01T00:00:00Z";

/// Run one sync cycle for device "M" with no cloud home (no outbox drain).
async fn run_cycle_m(
    storage: &MockSyncStorage,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
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
        cipher,
        keypair,
        ld,
        None,
        &NoopBlobPlan,
        None,
    )
    .await
    .expect("cycle");
}

/// Queue a pending upload whose source file doesn't exist, so the cycle's drain
/// can't clear it — the entry stays pending, modeling a slow or stuck upload
/// while we assert the changeset/snapshot aren't held back by it.
async fn seed_pending_upload(db: &Database) {
    db.enqueue_upload(
        "f1",
        "storage/aa/bb/f1",
        Some("/nonexistent/f1"),
        BlobScope::Master,
        T0,
    )
    .await
    .expect("seed pending upload");
}

/// Queue a pending upload whose source file exists, so the cycle's drain uploads
/// it for real.
async fn seed_real_upload(db: &Database, file_id: &str, source: &str) {
    db.enqueue_upload(
        file_id,
        &format!("storage/{file_id}"),
        Some(source),
        BlobScope::Master,
        T0,
    )
    .await
    .expect("seed real upload");
}

/// A pending cloud upload does not hold back a gated-true changeset: the gate
/// column decides per-row visibility, so a row that is shareable now reaches
/// peers without waiting for unrelated uploads to finish. The gate still cuts a
/// gated-false row, which is what withholds a not-yet-uploaded unit.
#[tokio::test]
async fn pending_upload_does_not_hold_back_a_gated_true_changeset() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[5u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // A slow/stuck upload for some OTHER unit is pending the whole time.
    seed_pending_upload(&db).await;

    // One shareable note (its blobs are up → gate on) and one still-private note
    // (its blobs aren't up yet → gate off; the host hasn't flipped it).
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('pub', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('priv', 'NotYet', NULL, 0, '0000000002000-0000-M', '2026-01-01')",
    )
    .await;

    // The changeset pushes despite the pending upload — no global deferral.
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_ok(),
        "a gated-true changeset must push even while an unrelated upload is pending",
    );

    // A fresh peer pulls: it gets the shareable row, never the gated-false one.
    let db_b = open_test_db();
    pull_into(&db_b, &storage, "B", &HashMap::new(), &ld, &NoopBlobPlan).await;
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'pub'").await,
        "Shareable",
        "the shareable note reaches the peer",
    );
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'priv'").await,
        "a gated-false row is still withheld — that is what holds a not-yet-uploaded unit",
    );
}

/// A gated-false row is withheld until its gate flips on, then it propagates: the
/// per-row gate, not a global flag, is what holds a not-yet-uploaded unit. (A
/// host flips the gate in `on_blob_uploaded` once the row's blobs land.)
#[tokio::test]
async fn gated_false_row_propagates_once_its_gate_flips() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[8u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // A note whose blobs aren't up yet: gate off.
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    let db_b = open_test_db();
    pull_into(&db_b, &storage, "B", &HashMap::new(), &ld, &NoopBlobPlan).await;
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a gated-false row must not reach a peer",
    );

    // The blobs land; the host flips the gate on. The next cycle re-emits the
    // now-shareable row.
    exec(
        &db,
        "UPDATE notes SET shared = 1, _updated_at = '0000000003000-0000-M' WHERE id = 'n1'",
    )
    .await;
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    // n1 was gated-false in cycle 1 (cut → no changeset pushed), so the flip
    // re-emits it at seq 1. Re-pull from empty cursors to pick it up wherever it
    // landed.
    pull_into(&db_b, &storage, "B", &HashMap::new(), &ld, &NoopBlobPlan).await;
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "Album Title",
        "once its gate flips on, the row reaches the peer",
    );
}

/// The snapshot is the second propagation channel and runs the same row-level
/// gate (`delete_gated_false`), so a pending upload does not withhold it: the
/// snapshot carries the gated-true rows and excludes the gated-false ones, which
/// is the blob-before-row guarantee at snapshot granularity.
#[tokio::test]
async fn snapshot_is_not_withheld_by_pending_uploads() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[9u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // local_seq past 0 with no snapshot yet → the snapshot policy fires this cycle.
    db.set_sync_state("local_seq", "1")
        .await
        .expect("seed local_seq");
    seed_pending_upload(&db).await;

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        SyncStorage::get_snapshot(&storage).await.is_ok(),
        "the snapshot must push even while an upload is pending — the gate, not a \
         global flag, decides what it carries",
    );
}

/// When the observer breaks the drain mid-batch (Publish), the cycle reports
/// `resume_drain_promptly`: it uploaded one blob and left the rest queued, so the
/// loop runs the next cycle promptly to keep draining + publishing per unit.
#[tokio::test]
async fn cycle_reports_resume_drain_promptly_when_drain_breaks_mid_batch() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[3u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let cloud = InMemoryCloudHome::new();

    // Two real uploads queued; the shared observer publishes after each upload,
    // so the drain breaks after the first.
    let a = tmp.path().join("a.bin");
    let b = tmp.path().join("b.bin");
    std::fs::write(&a, b"aaaa").unwrap();
    std::fs::write(&b, b"bbbb").unwrap();
    seed_real_upload(&db, "fa", a.to_str().unwrap()).await;
    seed_real_upload(&db, "fb", b.to_str().unwrap()).await;

    let result = run_single_sync_cycle(
        &storage,
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &keypair,
        &ld,
        Some(&cloud as &dyn CloudHome),
        &NoopBlobPlan,
        Some(&PublishingObserver as &dyn BlobUploadObserver),
    )
    .await
    .expect("cycle");

    assert!(
        result.resume_drain_promptly,
        "after the drain breaks mid-batch with entries still queued, the cycle \
         signals the loop to run again promptly",
    );
    assert!(
        cloud.get("storage/fa").is_some(),
        "the first blob uploaded this cycle",
    );
    assert!(
        cloud.get("storage/fb").is_none(),
        "the second blob is left for the next cycle",
    );
}

/// Founder-at-creation + owner anchoring (issue #102): the first cloud connect of
/// a created library writes the founder Owner entry and pins the owner; later
/// connects anchor the chain to that pinned owner; and a wiped or refounded chain
/// is refused as a takeover attempt.
#[tokio::test]
async fn ensure_owner_anchored_chain_founds_pins_and_refuses_tampering() {
    use crate::sync::cycle::ensure_owner_anchored_chain;
    use crate::sync::membership::founder_entry;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let hlc = Hlc::new("owner-dev".to_string());
    let db = open_test_db();

    // First connect: empty storage, no pinned owner → found + pin.
    let storage = MockSyncStorage::new();
    let chain = ensure_owner_anchored_chain(&storage, &db, &owner, &hlc)
        .await
        .expect("first connect founds the library");
    assert!(chain.is_founded_by(&owner_pk));
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the owner is pinned in sync_state",
    );
    assert_eq!(
        storage.list_membership_entries().await.unwrap().len(),
        1,
        "the founder entry is written to storage",
    );

    // Second connect on the same storage + db: anchors fine (founder == owner).
    let again = ensure_owner_anchored_chain(&storage, &db, &owner, &hlc)
        .await
        .expect("re-connect anchors to the pinned owner");
    assert!(again.is_founded_by(&owner_pk));

    // Wiped membership/* with the owner still pinned → refuse (do not re-found).
    let wiped = MockSyncStorage::new();
    assert!(
        ensure_owner_anchored_chain(&wiped, &db, &owner, &hlc)
            .await
            .is_err(),
        "an empty chain with a pinned owner is tampering, not a fresh library",
    );

    // Refounded under an attacker's key with the owner pinned → refuse.
    let attacker = UserKeypair::generate();
    let forged = MockSyncStorage::new();
    let forged_founder = founder_entry(&attacker, "2026-03-01T00:00:00Z");
    forged
        .put_membership_entry(
            &hex::encode(attacker.public_key),
            1,
            serde_json::to_vec(&forged_founder).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        ensure_owner_anchored_chain(&forged, &db, &owner, &hlc)
            .await
            .is_err(),
        "a chain refounded under a different key is a takeover attempt",
    );
}

/// Founding writes the cloud founder entry before pinning the owner, so a crash
/// between the two leaves a chain founded by our own key with no pin. The next
/// connect completes the pin (the founder is provably ours). A chain founded by a
/// DIFFERENT key with no pin is a first-connect takeover seed and is refused — the
/// branch that previously adopted any founder on trust.
#[tokio::test]
async fn ensure_owner_anchored_chain_completes_own_founding_but_refuses_foreign() {
    use crate::sync::cycle::ensure_owner_anchored_chain;
    use crate::sync::membership_ops::{write_founder_entry, OWNER_PUBKEY_STATE_KEY};

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let hlc = Hlc::new("owner-dev".to_string());

    // Cloud-first crash: our founder is in storage, but the pin never landed. The
    // next connect completes it (founder == our key) and anchors.
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    write_founder_entry(&storage, &owner, "0000000001000-0000-owner")
        .await
        .unwrap();
    let chain = ensure_owner_anchored_chain(&storage, &db, &owner, &hlc)
        .await
        .expect("completes our own half-done founding");
    assert!(chain.is_founded_by(&owner_pk));
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk),
        "the pin is completed from our own founder",
    );

    // Foreign chain, no pin: an attacker seeded a chain under their own key before
    // we ever connected. We neither founded it nor pinned an owner → refuse.
    let attacker = UserKeypair::generate();
    let fresh_db = open_test_db();
    let seeded = MockSyncStorage::new();
    write_founder_entry(&seeded, &attacker, "0000000001000-0000-attacker")
        .await
        .unwrap();
    assert!(
        ensure_owner_anchored_chain(&seeded, &fresh_db, &owner, &hlc)
            .await
            .is_err(),
        "a foreign chain with no pinned owner must be refused, not adopted on trust",
    );
}
