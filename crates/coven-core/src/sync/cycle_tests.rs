//! Blob-before-row ordering is enforced per row by the gate column: a blob-bearing
//! row's gate column stays off until its blobs upload, then coven flips it on (the
//! manage completion in the upload drain), so the changeset gate — and the snapshot,
//! which runs the same gate — only ever carry rows whose blobs are in the cloud. The
//! sync cycle does not hold the whole changeset back on a global "any upload
//! pending" flag.
//!
//! These tests pin that contract: a pending upload does not hold back an
//! already-shareable (gated-true) changeset or snapshot, and a gated-false row is
//! withheld until its gate flips. The completion flip + its mid-batch publish
//! (`resume_drain_promptly`) are covered in `blob::transition_tests`.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::blob::{BlobScope, CacheFill, Provenance};
use crate::clock::SystemClock;
use crate::database::{Database, PendingChangesetBatch};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::cycle::{self, run_single_sync_cycle};
use crate::sync::hlc::Hlc;
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::sync::signed_control::AckJson;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

const T0: &str = "2024-01-01T00:00:00Z";

/// The synthetic test db opens with a single migration, so its
/// [`Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

/// Run one sync cycle for device "M" with no cloud home (no outbox drain).
async fn run_cycle_m(
    storage: &MockSyncStorage,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &StoreDir,
) {
    run_single_sync_cycle(
        storage,
        "test-lib",
        "M",
        hlc,
        &SystemClock,
        db,
        cipher,
        &PendingRotation::none(),
        keypair,
        None,
        ld,
        None,
        None,
    )
    .await
    .expect("cycle");
}

async fn make_remote_intent_present(db: &Database, root_table: &str, root_id: &str) -> bool {
    let (rt, ri) = (root_table.to_string(), root_id.to_string());
    db.call(move |conn| Database::make_remote_intent_exists(conn, &rt, &ri))
        .await
        .expect("make_remote intent lookup")
}

async fn pending_changeset_count(db: &Database) -> i64 {
    db.call(|conn| {
        conn.query_row("SELECT COUNT(*) FROM pending_changesets", [], |row| {
            row.get(0)
        })
        .map_err(crate::database::DbError::from)
    })
    .await
    .expect("pending changeset count")
}

async fn blob_exists_at_recorded_location(
    db: &Database,
    storage: &MockSyncStorage,
    namespace: &str,
    id: &str,
) -> bool {
    let Some(location) = db
        .blob_location(namespace, id)
        .await
        .expect("read blob location")
    else {
        return false;
    };
    storage
        .blob_exists(namespace, &location, id, None)
        .await
        .expect("exists check")
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
        false,
        &crate::blob::content_hash(b"missing"),
        T0,
    )
    .await
    .expect("seed pending upload");
}

/// A pending cloud upload does not hold back a gated-true changeset: the gate
/// column decides per-row visibility, so a row that is shareable now reaches
/// peers without waiting for unrelated uploads to finish. The gate still cuts a
/// gated-false row, which is what withholds a not-yet-uploaded unit.
#[tokio::test]
async fn pending_upload_does_not_hold_back_a_gated_true_changeset() {
    let keypair = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(keypair.clone());
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [5u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    // A slow/stuck upload for some OTHER unit is pending the whole time.
    seed_pending_upload(&db).await;

    // The cycle below pushes a changeset and then snapshots it, and changeset
    // reclamation runs after the snapshot. Seed a peer that has not acked so the
    // store is multi-device with an un-acked member: its missing ack pins the
    // reclaim floor at 0, so the freshly pushed changeset is kept (a peer might
    // still need it), exactly as a real fleet behaves until everyone acks. Without
    // this peer M would be the only device and the snapshot-covered changeset would
    // be reclaimed, which is correct but not what this gate-focused test asserts.
    storage
        .put_head("peer-lagging", 0, T0)
        .await
        .expect("seed an un-acked peer head");

    // One shareable note (its blobs are up → gate on) and one still-private note
    // (its blobs aren't up yet → gate off; the host hasn't flipped it).
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('pub', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
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
    pull_into(&db_b, &storage, "B", &ld).await;
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
/// per-row gate, not a global flag, is what holds a not-yet-uploaded unit. (coven
/// flips the gate when a manage's blobs land; here the flip is written directly.)
#[tokio::test]
async fn gated_false_row_propagates_once_its_gate_flips() {
    let keypair = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(keypair.clone());
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [8u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    // A note whose blobs aren't up yet: gate off.
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    let db_b = open_test_db();
    pull_into(&db_b, &storage, "B", &ld).await;
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a gated-false row must not reach a peer",
    );

    // The blobs land; the host flips the gate on. The next cycle re-emits the
    // now-shareable row.
    host_exec(
        &db,
        "UPDATE notes SET shared = 1, _updated_at = '0000000003000-0000-M' WHERE id = 'n1'",
    )
    .await;
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    // n1 was gated-false in cycle 1 (cut → no changeset pushed), so the flip
    // re-emits it at seq 1. Re-pull from empty cursors to pick it up wherever it
    // landed.
    pull_into(&db_b, &storage, "B", &ld).await;
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
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [9u8; 32],
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
        SyncStorage::get_snapshot_pointer(&storage).await.is_ok(),
        "the snapshot must publish even while an upload is pending — the gate, not a \
         global flag, decides what it carries",
    );
}

#[tokio::test]
async fn initial_snapshot_uploads_remote_root_host_blobs_before_publish() {
    let storage = MockSyncStorage::new();
    let db = open_test_db_schema(
        vec![
            SyncedTable::new("notes").remote_root(),
            SyncedTable::new("note_tags"),
            SyncedTable::new("note_photos").carries_blob(BlobDecl::new(
                "photos",
                Provenance::HostProvided,
                CacheFill::CacheEager,
            )),
        ],
        test_migrations(),
    );
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [11u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "cover1", b"cover")
        .await
        .expect("store host-provided blob");
    // Drain the seed rows out of the pending journal so the cycle sees no outgoing
    // changeset and takes the initial-snapshot path; the rows still reach the cloud
    // via the snapshot, which reads them straight from the db.
    let _ = capture_bytes(&db, &[]).await;

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    assert!(
        blob_exists_at_recorded_location(&db, &storage, "photos", "cover1").await,
        "the blob referenced by the initial snapshot is uploaded before the pointer publishes",
    );
    assert!(
        SyncStorage::get_snapshot_pointer(&storage).await.is_ok(),
        "the snapshot pointer publishes after its referenced blob exists",
    );
}

#[tokio::test]
async fn initial_snapshot_does_not_publish_when_host_blob_upload_fails() {
    let storage = MockSyncStorage::new();
    let db = open_test_db_schema(
        vec![
            SyncedTable::new("notes").remote_root(),
            SyncedTable::new("note_tags"),
            SyncedTable::new("note_photos").carries_blob(BlobDecl::new(
                "photos",
                Provenance::HostProvided,
                CacheFill::CacheEager,
            )),
        ],
        test_migrations(),
    );
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [12u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "cover1", b"cover")
        .await
        .expect("store host-provided blob");
    // Drain the seed rows out of the pending journal so the cycle takes the
    // initial-snapshot path (the rows reach the cloud via the snapshot).
    let _ = capture_bytes(&db, &[]).await;
    storage.fail_next_blob_puts(1);

    let failed = match run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    {
        Ok(_) => panic!("snapshot publish should fail when a referenced blob cannot upload"),
        Err(error) => error,
    };

    assert!(
        failed.contains("forced blob upload failure"),
        "cycle surfaces the blob upload failure: {failed}",
    );
    assert!(
        SyncStorage::get_snapshot_pointer(&storage).await.is_err(),
        "the snapshot pointer is not published when a referenced blob upload fails",
    );
}

// The drain's break-to-publish is now driven by a manage *completion* (coven flips
// the gate the moment the last blob lands), not by an observer signal. It is covered
// end-to-end in `blob::transition_tests` — `resume_drain_promptly` after a manage
// completes, with another root's blob left queued.

/// Founder-at-creation + owner anchoring (issue #102): the first cloud connect of
/// a created store writes the founder Owner entry and pins the owner; later
/// connects anchor the chain to that pinned owner; and a wiped or refounded chain
/// is refused as a takeover attempt.
#[tokio::test]
async fn ensure_owner_anchored_chain_founds_pins_and_refuses_tampering() {
    use crate::sync::cycle::ensure_owner_anchored_chain;
    use crate::sync::membership::founder_entry;
    use crate::sync::membership_ops::{
        download_chain, write_founder_entry, OWNER_PUBKEY_STATE_KEY,
    };

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let hlc = Hlc::new("owner-dev".to_string());
    let db = open_test_db();

    // First connect: empty storage, no pinned owner → found + pin.
    let storage = MockSyncStorage::new();
    ensure_owner_anchored_chain(&storage, &db, &owner, &hlc)
        .await
        .expect("first connect founds the store");
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the owner is pinned in sync_state",
    );
    let entries = storage.list_membership_entries().await.unwrap();
    assert_eq!(entries.len(), 1, "the founder entry is written to storage");
    assert!(
        download_chain(&storage, &entries)
            .await
            .unwrap()
            .is_founded_by(&owner_pk),
        "the persisted chain is founded by the owner",
    );

    // Second connect on the same storage + db: anchors fine (founder == owner).
    ensure_owner_anchored_chain(&storage, &db, &owner, &hlc)
        .await
        .expect("re-connect anchors to the pinned owner");
    let entries = storage.list_membership_entries().await.unwrap();
    assert!(
        download_chain(&storage, &entries)
            .await
            .unwrap()
            .is_founded_by(&owner_pk),
        "the persisted chain is still founded by the owner after re-connect",
    );
    let owner_before = db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap();
    let floor_key = format!("membership_head_seq/{owner_pk}");
    let floor_before = db.get_sync_state(&floor_key).await.unwrap();

    // Wiped membership/* with the owner still pinned → refuse (do not re-found).
    let wiped = MockSyncStorage::new();
    assert!(
        ensure_owner_anchored_chain(&wiped, &db, &owner, &hlc)
            .await
            .is_err(),
        "an empty chain with a pinned owner is tampering, not a fresh store",
    );
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        owner_before,
    );
    assert_eq!(db.get_sync_state(&floor_key).await.unwrap(), floor_before);

    // Refounded under an attacker's key with the owner pinned → refuse.
    let attacker = UserKeypair::generate();
    let forged = MockSyncStorage::new();
    let forged_founder = founder_entry(&attacker, "2026-03-01T00:00:00Z");
    forged
        .put_membership_entry(
            &hex::encode(attacker.public_key()),
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
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        owner_before,
    );
    assert_eq!(db.get_sync_state(&floor_key).await.unwrap(), floor_before);

    let committed_foreign = MockSyncStorage::new();
    write_founder_entry(&committed_foreign, &attacker, "0000000001000-0000-attacker")
        .await
        .unwrap();
    assert!(
        ensure_owner_anchored_chain(&committed_foreign, &db, &owner, &hlc)
            .await
            .is_err(),
        "a committed foreign founder must not replace the pinned owner",
    );
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        owner_before,
    );
    assert_eq!(db.get_sync_state(&floor_key).await.unwrap(), floor_before);
}

/// Founding writes the cloud founder entry before pinning the owner, so a crash
/// between the two leaves a chain founded by our own key with no pin. The next
/// connect completes the pin (the founder is provably ours). A chain founded by a
/// DIFFERENT key with no pin is a first-connect takeover seed and is refused — the
/// branch that previously adopted any founder on trust.
#[tokio::test]
async fn ensure_owner_anchored_chain_completes_own_founding_but_refuses_foreign() {
    use crate::sync::cycle::ensure_owner_anchored_chain;
    use crate::sync::membership_ops::{
        download_chain, write_founder_entry, OWNER_PUBKEY_STATE_KEY,
    };

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let hlc = Hlc::new("owner-dev".to_string());

    // Cloud-first crash: our founder is in storage, but the pin never landed. The
    // next connect completes it (founder == our key) and anchors.
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    write_founder_entry(&storage, &owner, "0000000001000-0000-owner")
        .await
        .unwrap();
    ensure_owner_anchored_chain(&storage, &db, &owner, &hlc)
        .await
        .expect("completes our own half-done founding");
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the pin is completed from our own founder",
    );
    let entries = storage.list_membership_entries().await.unwrap();
    assert!(
        download_chain(&storage, &entries)
            .await
            .unwrap()
            .is_founded_by(&owner_pk),
        "the persisted chain is founded by the owner",
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

fn cloud_objects(home: &InMemoryCloudHome) -> BTreeMap<String, Vec<u8>> {
    home.keys()
        .into_iter()
        .map(|key| {
            let bytes = home.get(&key).expect("listed cloud object");
            (key, bytes)
        })
        .collect()
}

#[tokio::test]
async fn initializing_plaintext_storage_commits_and_pins_its_founder() {
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let db = open_test_db();
    let cipher = CloudCipher::Plaintext;
    let storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        cipher.clone(),
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );

    cycle::init_sync_over_storage(&db, storage)
        .await
        .expect("initialize plaintext storage");

    assert!(
        home.get(&format!("membership/{owner_pk}/1")).is_some(),
        "plaintext initialization publishes the founder entry",
    );
    assert!(
        home.get(&format!("membership/{owner_pk}/head")).is_some(),
        "plaintext initialization publishes the signed founder head",
    );
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    assert_eq!(
        db.get_sync_state(&format!("membership_head_seq/{owner_pk}"))
            .await
            .unwrap(),
        Some("1".to_string()),
        "the owner pin and committed head floor are both persisted",
    );
}

#[tokio::test]
async fn initialization_completes_a_listed_self_founder_without_a_head() {
    use crate::sync::membership::founder_entry;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    let founder_bytes = serde_json::to_vec(&founder_entry(
        &owner,
        "0000000001000-0000-interrupted-founder",
    ))
    .unwrap();
    storage
        .put_membership_entry(&owner_pk, 1, founder_bytes.clone())
        .await
        .unwrap();

    let db = open_test_db();
    cycle::init_sync_over_storage(&db, storage)
        .await
        .expect("complete the interrupted self-founding operation");

    assert_eq!(
        home.get(&format!("membership/{owner_pk}/1")),
        Some(founder_bytes),
        "the existing self-founder entry is preserved byte-for-byte",
    );
    assert!(home.get(&format!("membership/{owner_pk}/head")).is_some());
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    assert_eq!(
        db.get_sync_state(&format!("membership_head_seq/{owner_pk}"))
            .await
            .unwrap(),
        Some("1".to_string()),
    );
}

#[tokio::test]
async fn initialization_ignores_a_listed_foreign_founder_without_a_head() {
    use crate::sync::membership::founder_entry;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let attacker = UserKeypair::generate();
    let attacker_pk = pubkey_hex(&attacker);
    let attacker_storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        attacker.clone(),
    );
    let foreign_bytes = serde_json::to_vec(&founder_entry(
        &attacker,
        "0000000001000-0000-uncommitted-foreign-founder",
    ))
    .unwrap();
    attacker_storage
        .put_membership_entry(&attacker_pk, 1, foreign_bytes.clone())
        .await
        .unwrap();

    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner,
    );
    let db = open_test_db();
    cycle::init_sync_over_storage(&db, storage)
        .await
        .expect("unsigned foreign entries do not establish store ownership");

    assert_eq!(
        home.get(&format!("membership/{attacker_pk}/1")),
        Some(foreign_bytes),
        "initialization does not rewrite an unrelated uncommitted entry",
    );
    assert!(home
        .get(&format!("membership/{attacker_pk}/head"))
        .is_none());
    assert!(home.get(&format!("membership/{owner_pk}/head")).is_some());
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    assert_eq!(
        db.get_sync_state(&format!("membership_head_seq/{owner_pk}"))
            .await
            .unwrap(),
        Some("1".to_string()),
    );
}

#[tokio::test]
async fn initialization_pins_a_committed_self_founder_without_cloud_rewrite() {
    use crate::sync::membership_ops::{write_founder_entry, OWNER_PUBKEY_STATE_KEY};

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    write_founder_entry(&storage, &owner, "0000000001000-0000-founder")
        .await
        .unwrap();
    let cloud_before = cloud_objects(&home);

    let db = open_test_db();
    cycle::init_sync_over_storage(&db, storage)
        .await
        .expect("accept the identity's committed founder");

    assert_eq!(cloud_objects(&home), cloud_before);
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    assert_eq!(
        db.get_sync_state(&format!("membership_head_seq/{owner_pk}"))
            .await
            .unwrap(),
        Some("1".to_string()),
    );
}

#[tokio::test]
async fn plaintext_initialization_refuses_a_committed_foreign_founder_without_mutation() {
    use crate::sync::membership_ops::{write_founder_entry, OWNER_PUBKEY_STATE_KEY};

    let home = InMemoryCloudHome::new();
    let attacker = UserKeypair::generate();
    let attacker_storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        attacker.clone(),
    );
    write_founder_entry(&attacker_storage, &attacker, "0000000001000-0000-attacker")
        .await
        .unwrap();
    let cloud_before = cloud_objects(&home);

    let victim = UserKeypair::generate();
    let victim_pk = pubkey_hex(&victim);
    let db = open_test_db();
    let cipher = CloudCipher::Plaintext;
    let victim_storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        cipher.clone(),
        BlobPathScheme::Plain,
        "test-lib",
        victim.clone(),
    );

    assert!(
        cycle::init_sync_over_storage(&db, victim_storage)
            .await
            .is_err(),
        "a committed foreign founder prevents initialization",
    );
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
    assert_eq!(
        db.get_sync_state(&format!("membership_head_seq/{victim_pk}"))
            .await
            .unwrap(),
        None,
    );
    let cloud_after = cloud_objects(&home);
    assert_eq!(cloud_after, cloud_before, "cloud objects are unchanged");
}

#[tokio::test]
async fn initialization_rejects_incoherent_cipher_and_blob_path_scheme() {
    for (cipher, blob_paths) in [
        (CloudCipher::Plaintext, BlobPathScheme::Hashed),
        (
            CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            BlobPathScheme::Plain,
        ),
    ] {
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let db = open_test_db();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            cipher.clone(),
            blob_paths,
            "test-lib",
            owner.clone(),
        );
        db.set_sync_state(crate::sync::cloud_storage::PENDING_ROTATION_STATE_KEY, "9")
            .await
            .unwrap();
        let pending_rotation = storage.shared_pending_rotation();

        assert!(
            cycle::init_sync_over_storage(&db, storage).await.is_err(),
            "incoherent at-rest representation must be refused",
        );
        assert!(home.is_empty(), "the cloud is unchanged");
        assert_eq!(
            db.get_sync_state("owner_pubkey").await.unwrap(),
            None,
            "the local owner is not pinned",
        );
        assert_eq!(
            pending_rotation.pending_generation(),
            None,
            "the in-memory pending-rotation marker is not restored",
        );
        assert_eq!(
            db.get_sync_state(crate::sync::cloud_storage::PENDING_ROTATION_STATE_KEY)
                .await
                .unwrap(),
            Some("9".to_string()),
            "the durable pending-rotation state is unchanged",
        );
    }
}

// ---- Host writes journal; applies never do ----

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;

use crate::sync::storage::{MinSchemaVersion, StorageError};

/// A [`SyncStorage`] that injects a host write at a cycle `await` point — the
/// moment the cycle fetches an incoming changeset to apply — by running a host
/// INSERT through the same `Database` the cycle holds, once, before delegating
/// `get_changeset` to the inner mock.
///
/// This models the real hazard in issue #92: a host edit committed while the
/// cycle is in its network phase. The write goes through the actor's one
/// connection (the only door) at an `await` the cycle is parked on, and the host
/// write path appends it to the durable pending-changeset journal for the next
/// cycle.
struct HostWriteInjector {
    inner: MockSyncStorage,
    db: Database,
    /// The INSERT to run, once, the first time the cycle fetches a changeset.
    write_sql: String,
    fired: AtomicBool,
}

impl HostWriteInjector {
    fn new(inner: MockSyncStorage, db: Database, write_sql: &str) -> Self {
        Self {
            inner,
            db,
            write_sql: write_sql.to_string(),
            fired: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl SyncStorage for HostWriteInjector {
    async fn get_changeset(&self, device_id: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        // Fire the host write exactly once, at this `await` inside the pull's
        // network phase.
        if !self.fired.swap(true, Ordering::SeqCst) {
            host_exec(&self.db, &self.write_sql).await;
        }
        self.inner.get_changeset(device_id, seq).await
    }

    async fn list_heads(&self) -> Result<crate::sync::storage::HeadListing, StorageError> {
        self.inner.list_heads().await
    }
    async fn put_changeset(
        &self,
        device_id: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner.put_changeset(device_id, seq, data).await
    }
    async fn put_head(
        &self,
        device_id: &str,
        seq: u64,
        timestamp: &str,
    ) -> Result<(), StorageError> {
        self.inner.put_head(device_id, seq, timestamp).await
    }
    async fn put_blob(
        &self,
        namespace: &str,
        location: &crate::blob::CloudBlobLocation,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner
            .put_blob(namespace, location, id, scope, cloud_path, data)
            .await
    }
    async fn put_blob_from_file(
        &self,
        namespace: &str,
        location: &crate::blob::CloudBlobLocation,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_path: &std::path::Path,
    ) -> Result<(), StorageError> {
        self.inner
            .put_blob_from_file(namespace, location, id, scope, cloud_path, source_path)
            .await
    }
    async fn get_blob(
        &self,
        namespace: &str,
        location: &crate::blob::CloudBlobLocation,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
    ) -> Result<Vec<u8>, StorageError> {
        self.inner
            .get_blob(namespace, location, id, scope, cloud_path)
            .await
    }
    async fn blob_exists(
        &self,
        namespace: &str,
        location: &crate::blob::CloudBlobLocation,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<bool, StorageError> {
        self.inner
            .blob_exists(namespace, location, id, cloud_path)
            .await
    }
    async fn read_blob_range(
        &self,
        namespace: &str,
        location: &crate::blob::CloudBlobLocation,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_size: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StorageError> {
        self.inner
            .read_blob_range(
                namespace,
                location,
                id,
                scope,
                cloud_path,
                source_size,
                offset,
                len,
            )
            .await
    }
    async fn read_blob_to_file(
        &self,
        namespace: &str,
        location: &crate::blob::CloudBlobLocation,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_size: u64,
        expected_hash: &str,
        dest: &std::path::Path,
    ) -> Result<(), StorageError> {
        self.inner
            .read_blob_to_file(
                namespace,
                location,
                id,
                scope,
                cloud_path,
                source_size,
                expected_hash,
                dest,
            )
            .await
    }
    fn blob_path_scheme(&self) -> crate::sync::cloud_storage::BlobPathScheme {
        self.inner.blob_path_scheme()
    }
    fn blob_cloud_key(
        &self,
        namespace: &str,
        location: &crate::blob::CloudBlobLocation,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<String, crate::sync::storage::StorageError> {
        self.inner
            .blob_cloud_key(namespace, location, id, cloud_path)
    }
    fn own_uploader(&self) -> Option<String> {
        self.inner.own_uploader()
    }
    async fn put_snapshot(
        &self,
        author: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner.put_snapshot(author, seq, data).await
    }
    async fn get_snapshot(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        self.inner.get_snapshot(author, seq).await
    }
    async fn delete_changeset(&self, device_id: &str, seq: u64) -> Result<(), StorageError> {
        self.inner.delete_changeset(device_id, seq).await
    }
    async fn list_changesets(&self, device_id: &str) -> Result<Vec<u64>, StorageError> {
        self.inner.list_changesets(device_id).await
    }
    async fn put_ack(&self, device_id: &str, data: Vec<u8>) -> Result<(), StorageError> {
        self.inner.put_ack(device_id, data).await
    }
    async fn get_ack(&self, device_id: &str) -> Result<Vec<u8>, StorageError> {
        self.inner.get_ack(device_id).await
    }
    async fn get_min_schema_version(&self) -> Result<Option<MinSchemaVersion>, StorageError> {
        self.inner.get_min_schema_version().await
    }
    async fn set_min_schema_version(&self, version: u32) -> Result<(), StorageError> {
        self.inner.set_min_schema_version(version).await
    }
    async fn put_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner
            .put_membership_entry(author_pubkey, seq, data)
            .await
    }
    async fn get_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
    ) -> Result<Vec<u8>, StorageError> {
        self.inner.get_membership_entry(author_pubkey, seq).await
    }
    async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError> {
        self.inner.list_membership_entries().await
    }
    async fn put_membership_head(
        &self,
        author_pubkey: &str,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner.put_membership_head(author_pubkey, data).await
    }
    async fn get_membership_head(&self, author_pubkey: &str) -> Result<Vec<u8>, StorageError> {
        self.inner.get_membership_head(author_pubkey).await
    }
    async fn put_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner
            .put_wrapped_key(owner_pubkey, recipient_pubkey, data)
            .await
    }
    async fn get_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<Vec<u8>, StorageError> {
        self.inner
            .get_wrapped_key(owner_pubkey, recipient_pubkey)
            .await
    }
    async fn delete_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<(), StorageError> {
        self.inner
            .delete_wrapped_key(owner_pubkey, recipient_pubkey)
            .await
    }
    async fn put_snapshot_meta(
        &self,
        author: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner.put_snapshot_meta(author, seq, data).await
    }
    async fn get_snapshot_meta(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        self.inner.get_snapshot_meta(author, seq).await
    }
    async fn put_snapshot_pointer(&self, data: Vec<u8>) -> Result<(), StorageError> {
        self.inner.put_snapshot_pointer(data).await
    }
    async fn get_snapshot_pointer(&self) -> Result<Vec<u8>, StorageError> {
        self.inner.get_snapshot_pointer().await
    }
    async fn list_own_snapshot_generations(&self, author: &str) -> Result<Vec<u64>, StorageError> {
        self.inner.list_own_snapshot_generations(author).await
    }
    async fn delete_snapshot_generation(&self, author: &str, seq: u64) -> Result<(), StorageError> {
        self.inner.delete_snapshot_generation(author, seq).await
    }
}

/// A host write made WHILE a cycle is in its push/pull network phase
/// must land in the device's NEXT outgoing changeset. It is recorded by the same
/// durable journal path as any other host write.
///
/// Setup: a peer "A" has a changeset in shared storage. Device "M" runs a cycle
/// that pulls it; the storage wrapper injects a host INSERT into M at the
/// `get_changeset` await inside the pull. We then assert the
/// injected row is (a) present locally on M and (b) carried in M's next outgoing
/// changeset — proven by pulling that changeset into a fresh peer.
///
/// Mutation proof: route the injected write through raw `Database::call` instead
/// of the host journal. The row commits locally, but it is absent from M's next
/// changeset and assertion (b) fails.
#[tokio::test]
async fn host_write_during_pull_lands_in_next_outgoing_changeset() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [4u8; 32],
    )));

    // A peer A has published one changeset (an insert of note 'a1') to shared
    // storage, so M's cycle has something to fetch — the await we inject at.
    let inner = MockSyncStorage::with_keypair(keypair.clone());
    let a_src = open_test_db();
    let a_cs = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    inner.store_changeset("A", 1, &a_cs, SCHEMA_VERSION);

    // M's database. The injector runs this INSERT into M at the get_changeset
    // await, mid-pull.
    let db_m = open_test_db();
    let storage = HostWriteInjector::new(
        inner,
        db_m.clone(),
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('m_mid', 'WrittenMidCycle', NULL, 1, '0000000002000-0000-M', '2026-01-01')",
    );

    // Cycle 1: M pulls A's changeset; the host write fires mid-pull.
    run_cycle_m_storage(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    // (a) The injected row is present locally on M.
    assert_eq!(
        query_text(&db_m, "SELECT title FROM notes WHERE id = 'm_mid'").await,
        "WrittenMidCycle",
        "the mid-cycle host write committed to M's local db",
    );

    // (b) The injected row is in M's NEXT outgoing changeset. Cycle 2 drains the
    // pending journal row written during cycle 1 and pushes it. A fresh peer C
    // pulls M's output and must receive 'm_mid'.
    run_cycle_m_storage(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    let db_c = open_test_db();
    pull_into(&db_c, &storage, "C", &ld).await;
    assert_eq!(
        query_text(&db_c, "SELECT title FROM notes WHERE id = 'm_mid'").await,
        "WrittenMidCycle",
        "the mid-cycle host write reached a peer via M's next outgoing changeset",
    );
}

/// The other half of the journal invariant: an APPLIED row must NOT echo. After
/// M applies a peer's changeset, M's own next outgoing changeset must not carry the
/// applied rows — an apply is a plain connection write, never journaled, so the
/// applied rows are never recorded as M's own outgoing changes.
///
/// Mutation proof: route the apply through the pending journal (wrap it in a
/// `run_pending_journaled_transaction_on`). The applied rows then enter the journal
/// and re-ship on M's next changeset, so device C receives note 'a1' attributed to
/// M and the assertion fails.
#[tokio::test]
async fn applied_rows_do_not_echo_into_next_outgoing_changeset() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [6u8; 32],
    )));

    // Peer A publishes a changeset; M pulls and applies it in cycle 1.
    let storage = MockSyncStorage::with_keypair(keypair.clone());
    let a_src = open_test_db();
    let a_cs = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 1, &a_cs, SCHEMA_VERSION);

    let db_m = open_test_db();
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;
    assert_eq!(
        query_text(&db_m, "SELECT title FROM notes WHERE id = 'a1'").await,
        "FromA",
        "M applied A's changeset",
    );

    // Cycle 2: M pushes whatever it captured since. The applied row must not be in
    // it. A third device C pulls from a storage view containing only M's stream
    // and must not receive 'a1' from M.
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    let m_only = MockSyncStorage::new();
    let m_sequences = storage
        .list_changesets("M")
        .await
        .expect("list M changesets");
    for seq in &m_sequences {
        let packed = storage
            .get_changeset("M", *seq)
            .await
            .expect("read M changeset");
        m_only
            .put_changeset("M", *seq, packed)
            .await
            .expect("copy M changeset");
    }
    m_only
        .put_head("M", m_sequences.last().copied().unwrap_or(0), T0)
        .await
        .expect("publish isolated M head");
    let db_c = open_test_db();
    pull_into(&db_c, &m_only, "C", &ld).await;
    assert!(
        !row_exists(&db_c, "SELECT 1 FROM notes WHERE id = 'a1'").await,
        "the row M applied from A must NOT echo back through M's own changeset \
         (an apply is never journaled)",
    );
}

#[tokio::test]
async fn captured_changeset_retries_after_host_provided_blob_upload_failure() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [8u8; 32],
    )));
    let storage = MockSyncStorage::new();
    storage
        .put_head("peer-lagging", 0, T0)
        .await
        .expect("seed an un-acked peer head");
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");

    storage.fail_next_blob_puts(1);
    let failed = match run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    {
        Ok(_) => panic!("blob upload should fail before publish"),
        Err(error) => error,
    };
    assert!(
        failed.contains("forced blob upload failure"),
        "cycle surfaces the blob upload failure: {failed}"
    );
    assert!(
        pending_changeset_count(&db).await > 0,
        "the pending changesets remain queued for retry"
    );
    assert!(
        !blob_exists_at_recorded_location(&db, &storage, "photos", "hponly").await,
        "the failed blob upload did not publish the blob"
    );

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert_eq!(
        pending_changeset_count(&db).await,
        0,
        "the pending changesets clear once the retry publishes"
    );
    assert!(
        blob_exists_at_recorded_location(&db, &storage, "photos", "hponly").await,
        "the retried pending changeset uploads the host-provided blob"
    );
}

/// A committed store-key rotation this device has not adopted pauses sealing
/// without taking down the cycle: a pending changeset that references a
/// host-provided blob stays queued (the inline host-blob upload in
/// `service::sync` step 3 is gated on `rotation_pending`), the cycle completes,
/// and the first cycle after adoption publishes the changeset and uploads its
/// blob. Without the gate this cycle would abort at `cipher_for_seal` before the
/// pull.
#[tokio::test]
async fn rotation_pending_defers_a_host_blob_changeset_until_adoption() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    // The live cipher is generation 1; the cloud has committed generation 2.
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [8u8; 32],
    )));
    let storage = MockSyncStorage::new();
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");

    let pending_rotation = PendingRotation::none();
    pending_rotation.mark_committed(2);

    run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &pending_rotation,
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    .expect("the cycle completes; a pending rotation pauses sealing, it does not abort");

    assert!(
        pending_changeset_count(&db).await > 0,
        "the host-blob changeset stays queued while sealing is paused",
    );
    assert!(
        !blob_exists_at_recorded_location(&db, &storage, "photos", "hponly").await,
        "no host-provided blob is sealed to the cloud while sealing is paused",
    );

    // Adoption clears the pause (a fresh, unmarked rotation gate); the first cycle
    // after publishes the queued changeset and uploads its blob.
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert_eq!(
        pending_changeset_count(&db).await,
        0,
        "the queued changeset publishes on the first cycle after adoption",
    );
    assert!(
        blob_exists_at_recorded_location(&db, &storage, "photos", "hponly").await,
        "the host-provided blob uploads on the first cycle after adoption",
    );
}

/// The sibling of the host-blob-changeset case for the other newly-gated seal
/// path: a ready host-provided make_remote intent. With a rotation pending,
/// `complete_host_provided_make_remotes` is skipped — the root's gate does not
/// flip, its blob is not sealed, and the intent stays queued — yet the cycle
/// completes. The first cycle after adoption flips the gate, uploads the blob,
/// and consumes the intent. Without the gate this cycle would abort at
/// `cipher_for_seal` before the pull.
#[tokio::test]
async fn rotation_pending_defers_a_ready_make_remote_intent_until_adoption() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [9u8; 32],
    )));
    let storage = MockSyncStorage::new();
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Release', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"cover"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");
    crate::blob::transition::make_remote(
        &db,
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        "self-uploader",
        &hlc,
        "notes",
        "n1",
        false,
    )
    .await
    .expect("queue the host-provided make_remote intent");

    let pending_rotation = PendingRotation::none();
    pending_rotation.mark_committed(2);

    run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &pending_rotation,
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    .expect("the cycle completes; a pending rotation pauses sealing, it does not abort");

    assert_eq!(
        query_text(
            &db,
            "SELECT CAST(shared AS TEXT) FROM notes WHERE id = 'n1'"
        )
        .await,
        "0",
        "the make_remote gate does not flip while sealing is paused",
    );
    assert!(
        make_remote_intent_present(&db, "notes", "n1").await,
        "the make_remote intent stays queued while sealing is paused",
    );
    assert!(
        !blob_exists_at_recorded_location(&db, &storage, "photos", "hponly").await,
        "no host-provided blob is sealed to the cloud while sealing is paused",
    );

    // Adoption clears the pause; the first cycle after completes the intent.
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert_eq!(
        query_text(
            &db,
            "SELECT CAST(shared AS TEXT) FROM notes WHERE id = 'n1'"
        )
        .await,
        "1",
        "the make_remote gate flips on the first cycle after adoption",
    );
    assert!(
        !make_remote_intent_present(&db, "notes", "n1").await,
        "completing the make_remote consumes its intent",
    );
    assert!(
        blob_exists_at_recorded_location(&db, &storage, "photos", "hponly").await,
        "the host-provided blob uploads on the first cycle after adoption",
    );
}

#[tokio::test]
async fn captured_changeset_retry_recognizes_first_blob_uploaded_before_second_failed() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [18u8; 32],
    )));
    let storage = MockSyncStorage::new();
    storage
        .put_head("peer-lagging", 0, T0)
        .await
        .expect("seed an un-acked peer head");
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    ));
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('firstblob', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01'); \
             INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('secondblob', 'n1', 'cover', 6, '{}', '0000000001001-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"first"),
            crate::blob::content_hash(b"second"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "firstblob", b"first")
        .await
        .expect("store first host-provided blob");
    crate::blob::local_files::store(&ld, "photos", "secondblob", b"second")
        .await
        .expect("store second host-provided blob");

    storage.fail_blob_put_on_call(2);
    let failed = match run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    {
        Ok(_) => panic!("second blob upload should fail before publish"),
        Err(error) => error,
    };
    assert!(
        failed.contains("forced blob upload failure for photos/secondblob"),
        "cycle surfaces the second blob upload failure: {failed}"
    );
    assert!(
        blob_exists_at_recorded_location(&db, &storage, "photos", "firstblob").await,
        "the first blob reached cloud before the second upload failed"
    );
    assert!(
        crate::blob::local_files::read(
            &ld,
            "photos",
            "firstblob",
            5,
            &crate::blob::content_hash(b"first"),
        )
        .await
        .expect("read first local")
        .is_some(),
        "the first local copy remains because the changeset was not published"
    );

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_ok(),
        "the retry publishes instead of wedging on the first blob's missing local copy"
    );
}

#[tokio::test]
async fn already_uploaded_host_blob_publishes_without_local_copy_or_reupload() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [19u8; 32],
    )));
    let storage = MockSyncStorage::new();
    storage
        .put_head("peer-lagging", 0, T0)
        .await
        .expect("seed an un-acked peer head");
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    ));
    let location = test_blob_location(&storage.own_uploader().unwrap(), 1000);
    storage
        .put_blob(
            "photos",
            &location,
            "remoteonly",
            crate::blob::BlobScope::Master,
            None,
            b"already durable".to_vec(),
        )
        .await
        .expect("plant remote blob");
    db.record_blob_location("photos", "remoteonly", &location)
        .await
        .unwrap();
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('remoteonly', 'n1', 'cover', 15, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"already durable"),
        ),
    )
    .await;

    storage.fail_next_blob_puts(1);
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_ok(),
        "an already-durable cloud blob publishes without reading a local copy"
    );
}

#[tokio::test]
async fn fresh_push_failure_keeps_cache_lazy_local_copy_until_retry_publishes() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [20u8; 32],
    )));
    let storage = MockSyncStorage::new();
    storage
        .put_head("peer-lagging", 0, T0)
        .await
        .expect("seed an un-acked peer head");
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    ));
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('lazyblob', 'n1', 'cover', 4, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"lazy"),
        ),
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "lazyblob", b"lazy")
        .await
        .expect("store cache-lazy host-provided blob");

    storage.fail_next_changeset_puts(1);
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_err(),
        "the first push attempt did not publish the changeset"
    );
    assert!(
        crate::blob::local_files::read(
            &ld,
            "photos",
            "lazyblob",
            4,
            &crate::blob::content_hash(b"lazy"),
        )
        .await
        .expect("read lazy local")
        .is_some(),
        "the local copy remains until the changeset is published"
    );

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_ok(),
        "the staged retry publishes the changeset"
    );
    assert!(
        crate::blob::local_files::read(
            &ld,
            "photos",
            "lazyblob",
            4,
            &crate::blob::content_hash(b"lazy"),
        )
        .await
        .expect("read lazy local after publish")
        .is_none(),
        "the local copy drops after the staged retry commits"
    );
}

/// Every head the cycle wrote for our own device records its `last_sync` as an
/// RFC 3339 wall-clock string — the format the `put_head` contract specifies —
/// never the HLC string form the changeset envelope uses. An HLC string
/// (`0000000001000-0000-M`) fails RFC 3339 parsing, so this distinguishes them.
fn assert_own_head_timestamps_are_rfc3339(storage: &MockSyncStorage, device_id: &str) {
    let stamps = storage.head_put_timestamps();
    let ours: Vec<&String> = stamps
        .iter()
        .filter(|(d, _)| d == device_id)
        .map(|(_, ts)| ts)
        .collect();
    assert!(
        !ours.is_empty(),
        "the cycle wrote at least one head for {device_id}",
    );
    for ts in ours {
        assert!(
            chrono::DateTime::parse_from_rfc3339(ts).is_ok(),
            "head last_sync must be RFC 3339, got {ts:?}",
        );
    }
}

/// The main-push and post-pull republish head writers stamp the head with an
/// RFC 3339 `last_sync`.
#[tokio::test]
async fn push_cycle_writes_rfc3339_head_timestamps() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [21u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // A lagging peer keeps the pushed changeset from being reclaimed by the
    // snapshot the cycle also creates (local_seq 1, no snapshot yet), so the
    // main-push head writer's changeset is still present to assert against.
    storage
        .put_head("peer-lagging", 0, T0)
        .await
        .expect("seed an un-acked peer head");
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_ok(),
        "the cycle pushed a changeset, exercising the main-push head writer",
    );
    assert_own_head_timestamps_are_rfc3339(&storage, "M");
}

/// The snapshot head writer stamps the head with an RFC 3339 `last_sync`.
#[tokio::test]
async fn snapshot_cycle_writes_rfc3339_head_timestamp() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [22u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // local_seq past 0 with no snapshot yet → the snapshot policy fires this cycle.
    db.set_sync_state("local_seq", "1")
        .await
        .expect("seed local_seq");

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        SyncStorage::get_snapshot_pointer(&storage).await.is_ok(),
        "the cycle published a snapshot, exercising the snapshot head writer",
    );
    assert_own_head_timestamps_are_rfc3339(&storage, "M");
}

/// The staged-retry head writer stamps the head with an RFC 3339 `last_sync`.
#[tokio::test]
async fn staged_retry_writes_rfc3339_head_timestamp() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [23u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // A lagging peer keeps the retried changeset from being reclaimed by the
    // snapshot the retry cycle also creates, so it is still present to assert on.
    storage
        .put_head("peer-lagging", 0, T0)
        .await
        .expect("seed an un-acked peer head");
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    // The first push fails at the changeset put, so the changeset stages for retry
    // and no head is written for it yet.
    storage.fail_next_changeset_puts(1);
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        cycle::read_staged_changeset(&ld).await.unwrap().is_some(),
        "the changeset stages after the push write fails",
    );

    // The next cycle retries the staged push, exercising the staged-retry writer.
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_ok(),
        "the staged retry publishes the changeset",
    );
    assert_own_head_timestamps_are_rfc3339(&storage, "M");
}

#[tokio::test]
async fn missing_committed_stage_holds_sequence_and_pending_journal() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [24u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'First', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    storage.fail_next_changeset_puts(1);
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    let staged = cycle::read_staged_changeset(&ld)
        .await
        .expect("read staged changeset")
        .expect("staged changeset exists");
    storage
        .put_changeset("M", 1, staged.clone())
        .await
        .expect("simulate committed cloud changeset");
    storage
        .put_head("M", 1, T0)
        .await
        .expect("simulate committed cloud head");
    crate::local_blob::remove_file(&cycle::staging_path(&ld))
        .await
        .expect("remove stage");

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n2', 'Later', NULL, 1, '0000000002000-0000-M', '2026-01-01')",
    )
    .await;
    let retry = run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await;

    assert!(
        retry.is_err(),
        "a missing staged payload must stop the cycle"
    );
    assert_eq!(
        db.get_sync_state("staged_seq").await.unwrap().as_deref(),
        Some("1")
    );
    assert_eq!(db.get_sync_state("local_seq").await.unwrap(), None);
    assert!(matches!(
        db.pending_changeset_batch().await.unwrap(),
        PendingChangesetBatch::Pending { .. }
    ));
    assert_eq!(storage.get_changeset("M", 1).await.unwrap(), staged);
}

#[tokio::test]
async fn staged_pending_journal_marker_without_sequence_fails_closed() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [26u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    db.set_sync_state("staged_pending_changeset_id", "1")
        .await
        .expect("seed orphan pending marker");

    let error = run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    .expect_err("orphan pending marker must stop the cycle");
    assert!(
        error.contains("pending-journal marker exists without staged_seq"),
        "{error}"
    );
    assert!(storage.get_changeset("M", 1).await.is_err());
}

#[tokio::test]
async fn staged_sequence_without_pending_marker_rejects_a_nonempty_journal() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [27u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'First', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.set_sync_state("staged_seq", "1")
        .await
        .expect("seed staged sequence");

    let error = run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    .expect_err("missing pending marker must stop over journal rows");
    assert!(
        error.contains("staged_seq exists without its pending-journal marker"),
        "{error}"
    );
    assert!(matches!(
        db.pending_changeset_batch().await.unwrap(),
        PendingChangesetBatch::Pending { .. }
    ));
    assert!(storage.get_changeset("M", 1).await.is_err());
}

#[tokio::test]
async fn readable_tampering_of_a_staged_changeset_preserves_its_retry_state() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [25u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'First', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    storage.fail_next_changeset_puts(1);
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    let staged = cycle::read_staged_changeset(&ld)
        .await
        .expect("read staged changeset")
        .expect("staged changeset exists");
    let (mut envelope, changeset) = crate::sync::envelope::unpack(&staged).expect("unpack stage");
    envelope.message.push_str(" tampered");
    let tampered = crate::sync::envelope::pack(&envelope, &changeset);
    crate::local_blob::write_atomic_durable(&cycle::staging_path(&ld), &tampered)
        .await
        .expect("replace stage with readable tampering");

    let retry = run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    .expect_err("tampered stage must fail before publication");

    assert!(retry.contains("signature does not verify"), "{retry}");
    assert!(storage.get_changeset("M", 1).await.is_err());
    assert_eq!(
        db.get_sync_state("staged_seq").await.unwrap().as_deref(),
        Some("1")
    );
    assert_eq!(db.get_sync_state("local_seq").await.unwrap(), None);
    assert!(db
        .get_sync_state("staged_pending_changeset_id")
        .await
        .unwrap()
        .is_some());
    assert!(matches!(
        db.pending_changeset_batch().await.unwrap(),
        PendingChangesetBatch::Pending { .. }
    ));
    assert_eq!(
        cycle::read_staged_changeset(&ld).await.unwrap(),
        Some(tampered)
    );
}

#[tokio::test]
async fn staged_changeset_retry_rechecks_user_provided_blob_before_publish() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [10u8; 32],
    )));
    let storage = MockSyncStorage::new();
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let location = test_blob_location(&storage.own_uploader().unwrap(), 1000);
    storage
        .put_blob(
            "audio",
            &location,
            "audio1",
            crate::blob::BlobScope::Master,
            None,
            b"AUDIO".to_vec(),
        )
        .await
        .expect("plant remote user-provided blob");
    db.record_blob_location("audio", "audio1", &location)
        .await
        .unwrap();
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('audio1', 'n1', 'audio', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
            crate::blob::content_hash(b"AUDIO"),
        ),
    )
    .await;

    storage.fail_next_changeset_puts(1);
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        cycle::read_staged_changeset(&ld).await.unwrap().is_some(),
        "the packed changeset remains staged after the publish write fails"
    );
    assert!(
        storage.get_changeset("M", 1).await.is_err(),
        "the failed publish did not write the changeset"
    );

    storage
        .delete_blob_object("audio", &location, "audio1")
        .await;
    let retry = run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await;
    let err = match retry {
        Err(err) => err,
        Ok(_) => panic!("staged retry must recheck the remote user-provided blob"),
    };

    assert!(
        err.contains("user-provided blob audio/audio1 is absent from remote storage"),
        "staged retry surfaces the missing blob: {err}",
    );
    assert!(
        storage.get_changeset("M", 1).await.is_err(),
        "the retry aborts before writing the changeset"
    );
    assert!(
        storage
            .list_heads()
            .await
            .expect("list heads")
            .heads
            .into_iter()
            .all(|head| head.seq == 0),
        "the retry aborts before advancing a head"
    );
}

#[tokio::test]
async fn outgoing_stage_failure_keeps_pending_batch_for_retry() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [9u8; 32],
    )));
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    storage
        .put_head("peer-lagging", 0, T0)
        .await
        .expect("seed an un-acked peer head");

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('stage-fail', 'Stage Fail', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    let store_root = ld.as_ref().to_path_buf();
    let writable_permissions = std::fs::metadata(&store_root)
        .expect("store dir metadata")
        .permissions();
    let mut readonly_permissions = writable_permissions.clone();
    readonly_permissions.set_readonly(true);
    std::fs::set_permissions(&store_root, readonly_permissions).expect("block staging writes");
    let failed = match run_single_sync_cycle(
        &storage,
        "test-lib",
        "M",
        &hlc,
        &SystemClock,
        &db,
        &enc,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld,
        None,
        None,
    )
    .await
    {
        Ok(_) => panic!("outgoing staging should fail"),
        Err(error) => error,
    };
    assert!(
        failed.contains("Failed to stage outgoing changeset"),
        "cycle surfaces the outgoing staging failure: {failed}"
    );
    assert_eq!(
        pending_changeset_count(&db).await,
        1,
        "the pending batch remains queued when outgoing staging fails"
    );
    assert!(
        storage.get_changeset("M", 1).await.is_err(),
        "the unstaged changeset is not published"
    );

    std::fs::set_permissions(&store_root, writable_permissions).expect("unblock staging writes");
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_ok(),
        "the same pending batch publishes after staging is available"
    );
    assert_eq!(
        pending_changeset_count(&db).await,
        0,
        "the pending batch clears after the retry publishes"
    );
}

/// Like [`run_cycle_m`] but over an arbitrary `&dyn SyncStorage` (e.g. the
/// host-write injector), still with no cloud home (no outbox drain, no auth
/// refresh).
async fn run_cycle_m_storage(
    storage: &dyn SyncStorage,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &StoreDir,
) {
    run_single_sync_cycle(
        storage,
        "test-lib",
        "M",
        hlc,
        &SystemClock,
        db,
        cipher,
        &PendingRotation::none(),
        keypair,
        None,
        ld,
        None,
        None,
    )
    .await
    .expect("cycle");
}

// ---- changeset reclamation through a real cycle ----

/// A changeset that becomes both snapshot-covered and acked by every current
/// device is reclaimed by the cycle that publishes the snapshot. Peer A has pushed
/// `changes/A/1`; M runs one cycle that pulls it (so M acks A->1), snapshots
/// (covering A->1), and then reclaims — so `changes/A/1` is gone afterward.
///
/// The mock is built with M's keypair so the head it signs for M and the ack M
/// publishes share an author, the same identity a real device's storage and ack
/// share — which is what lets reclamation honor M's ack against M's head.
#[tokio::test]
async fn cycle_reclaims_a_fully_acked_changeset() {
    let keypair = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(keypair.clone());
    let db_m = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [11u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    // Peer A's changeset 1 (a shareable note).
    let a_src = open_test_db();
    let a_cs = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 1, &a_cs, SCHEMA_VERSION);

    // M's cycle pulls A->1, acks A->1, snapshots covering A->1, then reclaims.
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    assert!(
        storage.get_changeset("A", 1).await.is_err(),
        "a snapshot-covered, fully-acked changeset is reclaimed by the cycle",
    );
}

/// A changeset a behind peer still needs is NOT reclaimed, even after a snapshot
/// covers it. Peer A has pushed `changes/A/1` and `changes/A/2`; a peer B is parked
/// at A->1 (its ack reports only A->1). M pulls both, snapshots covering A->2, and
/// reclaims — but B's ack pins the floor at 1, so `changes/A/2` survives and B pulls
/// it forward.
#[tokio::test]
async fn cycle_keeps_a_behind_peers_changeset() {
    let keypair = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(keypair.clone());
    let db_m = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [12u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    // Peer A's two changesets (two independent shareable notes).
    let a_src = open_test_db();
    let cs1 = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'Title Alpha', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 1, &cs1, SCHEMA_VERSION);
    let cs2 = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a2', 'Title Beta', NULL, 1, '0000000002000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 2, &cs2, SCHEMA_VERSION);

    // A behind peer B: a head (so it counts as a current device) and an ack that
    // reports it has pulled A only through seq 1. Both are signed by the mock's
    // keypair so B's ack author matches B's head author.
    storage
        .put_head("B", 0, T0)
        .await
        .expect("seed behind peer head");
    let b_ack = AckJson::signed("B", BTreeMap::from([("A".to_string(), 1u64)]), &keypair);
    storage
        .put_ack("B", serde_json::to_vec(&b_ack).expect("serialize ack"))
        .await
        .expect("seed behind peer ack");

    // M's cycle pulls A->2, acks A->2, snapshots covering A->2, then reclaims. The
    // floor is min(snapshot A->2, min(M ack A->2, B ack A->1)) = 1.
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    assert!(
        storage.get_changeset("A", 1).await.is_err(),
        "the changeset below the floor (everyone has it) is reclaimed",
    );
    assert!(
        storage.get_changeset("A", 2).await.is_ok(),
        "the changeset the behind peer still needs is kept",
    );

    // And the behind peer pulls the kept changeset forward.
    let db_b = open_test_db();
    exec(
        &db_b,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('a1', 'Title Alpha', NULL, 1, \
                 '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db_b,
        "INSERT INTO sync_cursors (device_id, last_seq) VALUES ('A', 1)",
    )
    .await;
    pull_into(&db_b, &storage, "B", &ld).await;
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'a2'").await,
        "the behind peer pulls the kept changeset forward",
    );
}

/// Owner-only snapshots (#161), write side: a Member device with local data and a
/// pinned owner pushes its changeset but does NOT author a snapshot. The catalog
/// image is owner-only — a Member may push bounded changesets, not restate the whole
/// catalog — yet its rows still reach the cloud through the changeset push.
#[tokio::test]
async fn member_device_does_not_create_a_snapshot() {
    use crate::sync::membership::{MemberRole, MembershipAction, MembershipChain};
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [5u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();

    // The owner founds the chain and adds this device as a write-capable Member.
    let owner_pk = pubkey_hex(&owner);
    let mut chain = MembershipChain::new();
    let founder = founder_entry(&owner, "0000000001000-0000-owner");
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "0000000002000-0000-owner",
    );
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // This device pins the owner (set on join in production) — an opaque store.
    db.set_sync_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .expect("pin owner");

    // Local data: a shareable note (gate on). With no prior snapshot, pushing it
    // trips `should_create_snapshot`, so the owner gate is the only thing that can
    // stop the snapshot.
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    run_cycle_m(&storage, &db, &cipher, &member, &hlc, &ld).await;

    // The Member's row reached the cloud as a changeset...
    assert_eq!(
        SyncStorage::list_changesets(&storage, "M").await.unwrap(),
        vec![1],
        "the member's rows still propagate via the changeset push",
    );
    // ...but no catalog snapshot was authored — the pointer is absent, not merely
    // unreadable.
    assert!(
        matches!(
            SyncStorage::get_snapshot_pointer(&storage).await,
            Err(crate::sync::storage::StorageError::NotFound(_))
        ),
        "a non-owner device must not author a catalog snapshot (no pointer written)",
    );
}

/// The mirror of the above: an Owner device with local data and itself pinned as the
/// owner DOES author the snapshot — the founder/initial-sync path a freshly-founded
/// store bootstraps from is preserved by the gate's owner branch.
#[tokio::test]
async fn owner_device_creates_a_snapshot() {
    use crate::sync::membership::MembershipChain;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [6u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let mut chain = MembershipChain::new();
    let founder = founder_entry(&owner, "0000000001000-0000-owner");
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    db.set_sync_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .expect("pin owner");

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    run_cycle_m(&storage, &db, &cipher, &owner, &hlc, &ld).await;

    SyncStorage::get_snapshot_pointer(&storage)
        .await
        .expect("an owner device must author the catalog snapshot");
}
