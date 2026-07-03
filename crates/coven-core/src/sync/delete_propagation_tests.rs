//! End-to-end blob-delete behavior across two devices: an owner that deletes a
//! release, and a joined device that pulls it afterward.
//!
//! A queued cloud blob deletion is recorded as a signed tombstone and the blob is
//! held for a convergence grace, so a peer that still references the row is never
//! stranded by an immediate delete. This drives the real `run_single_sync_cycle`
//! for both devices and asserts the deleting device's cycle writes a tombstone
//! (the blob survives), the joined device can still read the blob until it pulls
//! the row removal, and a GC past the grace finally reclaims the blob.

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
/// cloud home wired so the upload drain, the tombstone drain, and the tombstone GC
/// all run.
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
        "test-lib",
        device_id,
        hlc,
        &SystemClock,
        db,
        cipher,
        kp,
        None,
        lib,
        Some(storage as &dyn CloudHome),
        None,
    )
    .await
}

/// A blob deletion does NOT strand a peer that still references the row: the
/// deleting device's cycle writes a signed tombstone (not an immediate delete), so
/// the blob stays readable through the convergence grace while a lagging peer
/// catches up. The peer pulls the row removal on its own cycle, and only a GC past
/// the grace reclaims the blob. Driven end to end through the real cycle.
#[tokio::test]
async fn blob_deletion_does_not_strand_a_peer_then_reclaims_past_the_grace() {
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
    let boot = bootstrap_from_snapshot(&storage, "test-lib", &cipher, None, 1, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        None,
        &storage,
        &boot.cursors,
        &lib_b,
    )
    .await
    .expect("B open_db_and_pull");
    let (db_b, _stamper_b) = Database::open(
        &lib_b.db_path(),
        tables.clone(),
        "B".to_string(),
        &test_migrations(),
    )
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
            None,
            b"audio-bytes".to_vec(),
        )
        .await
        .expect("seed cloud blob");
    exec(&db_a, "DELETE FROM notes WHERE id = 'n1'").await;
    db_a.enqueue_delete(blob_key, T0)
        .await
        .expect("enqueue blob delete");

    // A pushes the deletion as A/2 and runs the tombstone drain + GC. The blob must
    // STILL be present: the drain records the deletion as a tombstone, and the GC
    // (running at ~now, well inside the grace) does not yet reclaim it — so a peer
    // still inside the grace isn't stranded.
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc_a, &kp_a, &lib_a)
        .await
        .expect("A push deletion + write tombstone");
    assert!(
        storage.exists(blob_key).await.expect("exists check"),
        "the blob is kept through the grace, not deleted on the deleting device's cycle",
    );
    assert!(
        storage
            .exists("blob_tombstones/storage/blob1.enc")
            .await
            .expect("tombstone exists check"),
        "the deletion is recorded as a signed cloud tombstone",
    );
    assert!(
        db_a.get_pending_cloud_deletes()
            .await
            .expect("pending")
            .is_empty(),
        "the outbox delete row is drained into a tombstone",
    );

    // B still holds the referencing row AND can still read the blob — it is not
    // stranded while it remains inside the convergence grace.
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B still holds the note (it hasn't pulled the removal yet)",
    );
    assert!(
        storage
            .get_blob("storage", "blob1", crate::blob::ResolvedScope::Master, None)
            .await
            .is_ok(),
        "B can still read the blob the row points at — no strand",
    );

    // B's own next cycle pulls the deletion: the row removal reaches it.
    run_cycle(&storage, "B", &hlc_b, &db_b, &enc_b, &kp_b, &lib_b)
        .await
        .expect("B pull deletion");
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "B receives the row removal on its own cycle",
    );

    // Once every peer has converged and the grace has passed, a GC reclaims the
    // blob and the tombstone. Drive the same production GC the cycle calls, with a
    // clock past the grace.
    let past_grace = crate::clock::FixedClock(
        chrono::Utc::now() + crate::blob::delete::BLOB_TOMBSTONE_GRACE + chrono::Duration::days(1),
    );
    // This library is chain-less (no `membership/*` founded in this test), so the
    // GC authorizes on the verified signature alone — the open-library path, with no
    // pinned owner. The test below exercises the owner-anchored path.
    let reclaimed = crate::blob::delete::gc_tombstones(
        &storage,
        &storage,
        &enc_a,
        "test-lib",
        None,
        &past_grace,
    )
    .await
    .expect("GC past the grace");
    assert_eq!(
        reclaimed, 1,
        "the blob is reclaimed once the grace has passed"
    );
    assert!(
        !storage.exists(blob_key).await.expect("exists check"),
        "the blob is gone after the grace",
    );
    assert!(
        !storage
            .exists("blob_tombstones/storage/blob1.enc")
            .await
            .expect("tombstone exists check"),
        "the tombstone is gone after reclaiming the blob",
    );
}

/// End-to-end against a REAL, owner-anchored membership chain: a chain founded by A
/// with the device pinned to A. An authorized member's tombstone, run through the
/// real cycle, reclaims past the grace — and then, after the chain is wiped and
/// refounded under an attacker's key, the attacker's tombstone leaves the blob,
/// because the GC anchors authorization to the pinned owner A. The other end-to-end
/// test runs only the chain-less path; this one drives the owner-anchored path the
/// production cycle uses.
#[tokio::test]
async fn gc_against_a_real_chain_reclaims_for_a_member_but_refuses_a_refounded_chain() {
    use crate::sync::membership::{MemberRole, MembershipAction};
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let cipher = CloudCipher::Encrypted(EncryptionService::new_with_key(&[11u8; 32]));
    let enc_a = RwLock::new(cipher.clone());
    let storage = MockSyncStorage::new();
    let kp_a = UserKeypair::generate(); // founder + Owner
    let kp_b = UserKeypair::generate(); // a real Member A adds
    let owner_pk = pubkey_hex(&kp_a);

    // Found a real chain in the bucket: A (Owner, seq 1), then A adds B as a Member
    // (seq 2). Both entries are authored by A, so they live under `membership/{A}/*`
    // where `list_membership_entries` finds them.
    let founder = founder_entry(&kp_a, "0000000001000-0000-A");
    storage
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .expect("put founder");
    let add_b = make_entry(
        &kp_a,
        MembershipAction::Add,
        &kp_b,
        MemberRole::Member,
        "0000000002000-0000-A",
    );
    storage
        .put_membership_entry(&owner_pk, 2, serde_json::to_vec(&add_b).unwrap())
        .await
        .expect("put add-b");

    // A's device pins A as the owner — set on found/join/restore in production, here
    // the value the cycle's GC and pull read from `sync_state`.
    let db_a = open_test_db();
    db_a.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .expect("pin owner");
    let (_tmp_a, lib_a) = temp_library_dir();
    let hlc_a = Hlc::new("A".to_string());

    // A's initial snapshot cycle, then import a note (A is the authorized Owner, so
    // the pull anchored to the pinned owner accepts A's own head/changeset).
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc_a, &kp_a, &lib_a)
        .await
        .expect("A initial snapshot cycle");
    exec(
        &db_a,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc_a, &kp_a, &lib_a)
        .await
        .expect("A push n1");

    // The blob the note points at, in the cloud. A deletes the note and queues the
    // blob delete; the cycle writes the tombstone (signed by A, an authorized
    // member) and holds the blob through the grace.
    let blob_key = "storage/blob1";
    storage
        .put_blob(
            "storage",
            "blob1",
            crate::blob::ResolvedScope::Master,
            None,
            b"audio-bytes".to_vec(),
        )
        .await
        .expect("seed cloud blob");
    exec(&db_a, "DELETE FROM notes WHERE id = 'n1'").await;
    db_a.enqueue_delete(blob_key, T0)
        .await
        .expect("enqueue blob delete");
    run_cycle(&storage, "A", &hlc_a, &db_a, &enc_a, &kp_a, &lib_a)
        .await
        .expect("A push deletion + write tombstone");
    assert!(
        storage.exists(blob_key).await.expect("exists"),
        "the blob is held through the grace, not deleted on the deleting cycle",
    );
    assert!(
        storage
            .exists("blob_tombstones/storage/blob1.enc")
            .await
            .expect("tombstone exists"),
        "the member's deletion is recorded as a signed tombstone",
    );

    // Past the grace, the production GC anchored to the pinned owner A reclaims the
    // blob: A's tombstone is authorized by the real chain.
    let owner_pin = db_a
        .get_sync_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .expect("read pin");
    let past_grace = crate::clock::FixedClock(
        chrono::Utc::now() + crate::blob::delete::BLOB_TOMBSTONE_GRACE + chrono::Duration::days(1),
    );
    let reclaimed = crate::blob::delete::gc_tombstones(
        &storage,
        &storage,
        &enc_a,
        "test-lib",
        owner_pin.as_deref(),
        &past_grace,
    )
    .await
    .expect("GC past the grace");
    assert_eq!(reclaimed, 1, "the authorized member's tombstone reclaims");
    assert!(
        !storage.exists(blob_key).await.expect("exists"),
        "the blob is gone after the authorized reclaim",
    );

    // ---- The refounded-chain negative, end to end ----
    // A second blob, with an authorized tombstone (by B, a real Member). Then the
    // attacker wipes `membership/*`, refounds under their own key, and plants a
    // tombstone of their own. The GC, still anchored to the pinned owner A, must
    // leave the victim blob: the refounded chain is not founded by A.
    let victim = "storage/blob2";
    storage
        .put_blob(
            "storage",
            "blob2",
            crate::blob::ResolvedScope::Master,
            None,
            b"more-audio".to_vec(),
        )
        .await
        .expect("seed victim blob");

    // Wipe the real chain (delete both membership entries) and refound under the
    // attacker's key — a self-signed Owner that passes `MembershipChain::validate`
    // but is founded by the wrong key.
    storage
        .delete(&format!("membership/{owner_pk}/1"))
        .await
        .expect("wipe founder");
    storage
        .delete(&format!("membership/{owner_pk}/2"))
        .await
        .expect("wipe add-b");
    let attacker = UserKeypair::generate();
    let forged = founder_entry(&attacker, "0000000009000-0000-evil");
    storage
        .put_membership_entry(
            &pubkey_hex(&attacker),
            1,
            serde_json::to_vec(&forged).unwrap(),
        )
        .await
        .expect("plant forged founder");

    // The attacker signs a tombstone for the victim blob, backdated past the grace,
    // and seals it under the library cipher exactly as the drain would so the GC can
    // open it.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let forged_tombstone = crate::blob::delete::BlobTombstoneJson::signed(
        "test-lib",
        victim.to_string(),
        deleted_at.to_string(),
        &attacker,
    );
    let (sealed, suffix) = {
        let guard = enc_a.read().unwrap();
        (
            guard.seal(&serde_json::to_vec(&forged_tombstone).unwrap()),
            guard.suffix(),
        )
    };
    storage
        .write(
            &format!("blob_tombstones/{victim}{suffix}"),
            crate::storage::cloud::BlobBody::from_bytes(sealed),
            &crate::storage::cloud::no_progress(),
        )
        .await
        .expect("plant forged tombstone");

    let n = crate::blob::delete::gc_tombstones(
        &storage,
        &storage,
        &enc_a,
        "test-lib",
        owner_pin.as_deref(),
        &past_grace,
    )
    .await
    .expect("GC over refounded chain");
    assert_eq!(n, 0, "a refounded chain's tombstone reclaims nothing");
    assert!(
        storage.exists(victim).await.expect("exists"),
        "the victim blob survives a wiped-and-refounded-chain takeover",
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
    let at_rest = storage
        .current_snapshot_db()
        .await
        .expect("snapshot pushed");
    assert!(
        at_rest.starts_with(b"SQLite format 3\0"),
        "a plaintext home stores the snapshot as a bare SQLite image, not ciphertext",
    );

    // Device B bootstraps from the plaintext snapshot — `CloudCipher::Plaintext`
    // opens it verbatim — and reads A's row.
    let (_tmp_b, lib_b) = temp_library_dir();
    let boot = bootstrap_from_snapshot(
        &storage,
        "test-lib",
        &CloudCipher::Plaintext,
        None,
        1,
        &lib_b.db_path(),
    )
    .await
    .expect("B bootstrap from plaintext snapshot");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        None,
        &storage,
        &boot.cursors,
        &lib_b,
    )
    .await
    .expect("B open_db_and_pull");
    let (db_b, _stamper_b) = Database::open(
        &lib_b.db_path(),
        tables.clone(),
        "B".to_string(),
        &test_migrations(),
    )
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

    // The update changeset is stored in the clear too. It is A's seq 2: the seq-1
    // insert was covered by the initial snapshot (a single-device library, so the
    // floor is the snapshot cursor) and reclaimed, while this post-snapshot update
    // is above the snapshot cursor and persists for B to pull.
    let cs_at_rest = storage
        .get_changeset("A", 2)
        .await
        .expect("A's update changeset present");
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
