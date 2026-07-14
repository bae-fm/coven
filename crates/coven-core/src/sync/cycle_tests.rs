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
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::{test_utils::InMemoryCloudHome, CloudHome};
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::cycle::{self, run_single_sync_cycle};
use crate::sync::hlc::Hlc;
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::sync::storage::SyncStorage;
use crate::sync::store_commit::{
    CommitPosition, ProtocolGenesis, SnapshotMeta, StoreAck, StoreDeviceHead,
    StoreDeviceRegistration, StoreDeviceRegistrationState,
};
use crate::sync::test_helpers::*;

const T0: &str = "2024-01-01T00:00:00Z";

/// The synthetic test db opens with a single migration, so its
/// [`Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

fn cycle_cloud_storage(
    home: Arc<dyn CloudHome>,
    cipher: CloudCipher,
    blob_paths: BlobPathScheme,
    store_id: &str,
    keypair: UserKeypair,
) -> CloudSyncStorage {
    CloudSyncStorage::new(home, cipher, blob_paths, store_id, keypair)
        .with_copy_ids(Arc::new(crate::storage::cloud::RandomCopyIdGenerator))
}

/// Run one sync cycle for device "M" with no cloud home (no outbox drain).
async fn run_cycle_m(
    storage: &MockSyncStorage,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &StoreDir,
) {
    run_cycle_m_result(storage, db, cipher, keypair, hlc, ld)
        .await
        .expect("cycle");
}

async fn run_cycle_m_result(
    storage: &MockSyncStorage,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &StoreDir,
) -> Result<(), String> {
    bind_mock_store_protocol(db, storage, "M").await;
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
    .map(|_| ())
}

async fn store_package_exists(storage: &MockSyncStorage, device_id: &str, seq: u64) -> bool {
    let Some(commit) = crate::sync::store_objects::load_commit_slot(
        storage,
        storage.protocol_genesis_hash(),
        device_id,
        seq,
    )
    .await
    .expect("load Store commit slot") else {
        return false;
    };
    crate::sync::store_objects::load_package(storage, &commit.value)
        .await
        .expect("load Store package")
        .is_some()
}

async fn store_snapshot_metas(storage: &MockSyncStorage) -> Vec<SnapshotMeta> {
    crate::sync::store_objects::list_snapshot_metas(storage, storage.protocol_genesis_hash())
        .await
        .expect("list Store snapshots")
        .metas
        .into_iter()
        .map(|meta| meta.value)
        .collect()
}

async fn store_heads(storage: &MockSyncStorage) -> Vec<StoreDeviceHead> {
    crate::sync::store_objects::list_visible_heads(storage, storage.protocol_genesis_hash())
        .await
        .expect("list Store heads")
        .heads
        .into_iter()
        .map(|head| head.value)
        .collect()
}

async fn publish_mock_founder_membership(
    storage: &MockSyncStorage,
) -> crate::sync::membership::MembershipChain {
    let founder = storage.protocol_genesis().founder;
    let founder_coord = founder.coord();
    let mut chain = crate::sync::membership::MembershipChain::new();
    chain
        .add_entry_at(founder_coord.clone(), founder.clone())
        .expect("valid protocol founder membership");
    crate::sync::store_objects::append_membership_entry_object(storage, &founder_coord, &founder)
        .await
        .expect("publish protocol founder membership");
    crate::sync::membership_ops::publish_membership_head(
        storage,
        &chain,
        &storage.protocol_founder_keypair(),
    )
    .await
    .expect("publish protocol founder membership head");
    chain
}

async fn append_active_store_device(
    storage: &MockSyncStorage,
    device_id: &str,
    signer: &UserKeypair,
) {
    let registration = StoreDeviceRegistration::signed(
        storage.protocol_genesis_hash(),
        device_id.to_string(),
        1,
        None,
        StoreDeviceRegistrationState::Active,
        signer,
    )
    .expect("sign active Store device registration");
    crate::sync::store_objects::append_and_verify(
        storage,
        &crate::sync::store_commit::registration_semantic_prefix(
            device_id,
            1,
            registration.registration_hash(),
        ),
        ".json",
        &registration.to_bytes(),
    )
    .await
    .expect("append active Store device registration");
}

async fn append_store_ack(
    storage: &MockSyncStorage,
    device_id: &str,
    frontier: BTreeMap<String, CommitPosition>,
    signer: &UserKeypair,
) {
    let ack = StoreAck::signed(
        storage.protocol_genesis_hash(),
        device_id.to_string(),
        1,
        None,
        frontier,
        T0.to_string(),
        signer,
    )
    .expect("sign Store acknowledgement");
    crate::sync::store_objects::append_and_verify(
        storage,
        &crate::sync::store_commit::ack_semantic_prefix(device_id, 1, ack.ack_hash()),
        ".json",
        &ack.to_bytes(),
    )
    .await
    .expect("append Store acknowledgement");
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
        store_package_exists(&storage, "M", 1).await,
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
    // re-emits it at seq 1. Re-pull from empty positions to pick it up wherever it
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
    db.set_protocol_state("local_seq", "1")
        .await
        .expect("seed local_seq");
    seed_pending_upload(&db).await;

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        !store_snapshot_metas(&storage).await.is_empty(),
        "the snapshot must publish even while an upload is pending — the gate, not a \
         global flag, decides what it carries",
    );
}

#[tokio::test]
async fn initial_snapshot_uploads_remote_root_host_blobs_before_publish() {
    let storage = MockSyncStorage::new();
    let db = open_test_db_schema(
        vec![
            SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).remote_root(),
            SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
            SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey)
                .carries_blob(BlobDecl::new(
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
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('cover1', 'n1', 'cover', 5, '0000000001000-0000-M', '2026-01-01')",
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
        storage.exists("photos/cover1").await.expect("exists check"),
        "the blob referenced by the initial snapshot is uploaded before the pointer publishes",
    );
    assert!(
        !store_snapshot_metas(&storage).await.is_empty(),
        "the snapshot metadata publishes after its referenced blob exists",
    );
}

#[tokio::test]
async fn initial_snapshot_does_not_publish_when_host_blob_upload_fails() {
    let storage = MockSyncStorage::new();
    let db = open_test_db_schema(
        vec![
            SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey).remote_root(),
            SyncedTable::new("note_tags", crate::sync::session::RowIdentity::SharedKey),
            SyncedTable::new("note_photos", crate::sync::session::RowIdentity::SharedKey)
                .carries_blob(BlobDecl::new(
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
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('cover1', 'n1', 'cover', 5, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "cover1", b"cover")
        .await
        .expect("store host-provided blob");
    // Drain the seed rows out of the pending journal so the cycle takes the
    // initial-snapshot path (the rows reach the cloud via the snapshot).
    let _ = capture_bytes(&db, &[]).await;
    storage.fail_next_blob_puts(1);
    bind_mock_store_protocol(&db, &storage, "M").await;

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
        store_snapshot_metas(&storage).await.is_empty(),
        "snapshot metadata is not published when a referenced blob upload fails",
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
    let db = open_test_db();

    // First connect: empty storage, no pinned owner → found + pin.
    let storage = MockSyncStorage::with_keypair(owner.clone());
    let genesis = storage.protocol_genesis();
    ensure_owner_anchored_chain(&storage, &db, &genesis, &owner)
        .await
        .expect("first connect founds the store");
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the owner is pinned in protocol_state",
    );
    let entries = storage.discover_membership_entries().await;
    assert_eq!(entries.len(), 1, "the founder entry is written to storage");
    assert!(
        download_chain(&storage, &entries)
            .await
            .unwrap()
            .is_founded_by(&owner_pk),
        "the persisted chain is founded by the owner",
    );

    // Second connect on the same storage + db: anchors fine (founder == owner).
    ensure_owner_anchored_chain(&storage, &db, &genesis, &owner)
        .await
        .expect("re-connect anchors to the pinned owner");
    let entries = storage.discover_membership_entries().await;
    assert!(
        download_chain(&storage, &entries)
            .await
            .unwrap()
            .is_founded_by(&owner_pk),
        "the persisted chain is still founded by the owner after re-connect",
    );
    let owner_before = db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap();
    let floor_key = format!("membership_head_seq/{owner_pk}");
    let floor_before = db.get_protocol_state(&floor_key).await.unwrap();

    // Wiped membership/* with the owner still pinned → refuse (do not re-found).
    let wiped = MockSyncStorage::with_keypair(owner.clone());
    let wiped_genesis = wiped.protocol_genesis();
    assert!(
        ensure_owner_anchored_chain(&wiped, &db, &wiped_genesis, &owner)
            .await
            .is_err(),
        "an empty chain with a pinned owner is tampering, not a fresh store",
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        owner_before,
    );
    assert_eq!(
        db.get_protocol_state(&floor_key).await.unwrap(),
        floor_before
    );

    // Refounded under an attacker's key with the owner pinned → refuse.
    let attacker = UserKeypair::generate();
    let forged = MockSyncStorage::with_keypair(attacker.clone());
    let forged_genesis = forged.protocol_genesis();
    let forged_founder = founder_entry("test-store", &attacker, "2026-03-01T00:00:00Z");
    forged
        .append_membership_entry_bytes(
            &hex::encode(attacker.public_key()),
            1,
            serde_json::to_vec(&forged_founder).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        ensure_owner_anchored_chain(&forged, &db, &forged_genesis, &owner)
            .await
            .is_err(),
        "a chain refounded under a different key is a takeover attempt",
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        owner_before,
    );
    assert_eq!(
        db.get_protocol_state(&floor_key).await.unwrap(),
        floor_before
    );

    let committed_foreign = MockSyncStorage::with_keypair(attacker.clone());
    let committed_foreign_genesis = committed_foreign.protocol_genesis();
    write_founder_entry(
        &committed_foreign,
        "test-store",
        &attacker,
        "0000000001000-0000-attacker",
    )
    .await
    .unwrap();
    assert!(
        ensure_owner_anchored_chain(&committed_foreign, &db, &committed_foreign_genesis, &owner,)
            .await
            .is_err(),
        "a committed foreign founder must not replace the pinned owner",
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        owner_before,
    );
    assert_eq!(
        db.get_protocol_state(&floor_key).await.unwrap(),
        floor_before
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
    use crate::sync::membership_ops::{
        download_chain, write_founder_entry, OWNER_PUBKEY_STATE_KEY,
    };

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    // Cloud-first crash: our founder is in storage, but the pin never landed. The
    // next connect completes it (founder == our key) and anchors.
    let db = open_test_db();
    let storage = MockSyncStorage::with_keypair(owner.clone());
    let genesis = storage.protocol_genesis();
    write_founder_entry(&storage, "test-store", &owner, &genesis.founder.created_at)
        .await
        .unwrap();
    ensure_owner_anchored_chain(&storage, &db, &genesis, &owner)
        .await
        .expect("completes our own half-done founding");
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the pin is completed from our own founder",
    );
    let entries = storage.discover_membership_entries().await;
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
    let seeded = MockSyncStorage::with_keypair(attacker.clone());
    let seeded_genesis = seeded.protocol_genesis();
    write_founder_entry(
        &seeded,
        "test-store",
        &attacker,
        "0000000001000-0000-attacker",
    )
    .await
    .unwrap();
    assert!(
        ensure_owner_anchored_chain(&seeded, &fresh_db, &seeded_genesis, &owner)
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
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        cipher.clone(),
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );

    cycle::init_sync_over_storage(&db, storage, cycle::StoreInitialization::CreateStore)
        .await
        .expect("initialize plaintext storage");

    let entry_prefix = format!("store-v1/membership/entries/{owner_pk}/");
    let head_prefix = format!("store-v1/membership/heads/{owner_pk}/");
    let entry_keys: Vec<_> = home
        .appended_keys()
        .into_iter()
        .filter(|key| key.starts_with(&entry_prefix))
        .collect();
    let head_keys: Vec<_> = home
        .appended_keys()
        .into_iter()
        .filter(|key| key.starts_with(&head_prefix))
        .collect();
    assert_eq!(
        entry_keys.len(),
        1,
        "initialization publishes one founder entry"
    );
    assert_eq!(
        head_keys.len(),
        1,
        "initialization publishes one founder head"
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    let head_slot = crate::sync::store_commit::parse_membership_head_copy_key(&head_keys[0])
        .expect("parse founder head copy key");
    let founder_head: crate::sync::membership::AuthorHead = serde_json::from_slice(
        &home
            .get_appended(&head_keys[0])
            .expect("read founder head copy"),
    )
    .expect("parse founder head copy");
    let floor: crate::sync::membership::MembershipCoord = serde_json::from_str(
        &db.get_protocol_state(&format!(
            "membership_head_seq/{}",
            head_slot.author_owner_grant
        ))
        .await
        .unwrap()
        .expect("persisted exact founder floor"),
    )
    .expect("parse persisted founder floor");
    assert_eq!(
        floor,
        crate::sync::membership::MembershipCoord {
            author_pubkey: head_slot.author,
            author_owner_grant: head_slot.author_owner_grant,
            seq: head_slot.sequence,
            entry_hash: founder_head.tip_hash,
        },
        "the owner pin and exact committed head floor are both persisted",
    );
}

#[tokio::test]
async fn initialization_refuses_a_founder_entry_without_its_genesis() {
    use crate::sync::membership::founder_entry;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    let db = open_test_db();
    let founder = founder_entry("test-lib", &owner, "0000000001000-0000-interrupted-founder");
    let genesis = ProtocolGenesis::signed(
        "test-lib".to_string(),
        founder.clone(),
        db.schema_version(),
        &owner,
    )
    .expect("sign interrupted protocol genesis");
    db.stage_protocol_genesis(genesis)
        .await
        .expect("stage interrupted protocol genesis");
    let founder_bytes = serde_json::to_vec(&founder).expect("serialize founder");
    crate::sync::test_helpers::append_membership_entry_bytes(
        &storage,
        &owner_pk,
        1,
        founder_bytes.clone(),
    )
    .await
    .unwrap();

    let error =
        match cycle::init_sync_over_storage(&db, storage, cycle::StoreInitialization::CreateStore)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("a nonempty Store layout without genesis must fail loud"),
        };
    assert!(
        matches!(error, cycle::InitSyncError::ProtocolGenesis(ref message) if message.contains("nonempty but has no supported genesis")),
        "{error}"
    );

    let entry_prefix = format!("store-v1/membership/entries/{owner_pk}/");
    let entry_keys: Vec<_> = home
        .appended_keys()
        .into_iter()
        .filter(|key| key.starts_with(&entry_prefix))
        .collect();
    assert_eq!(entry_keys.len(), 1);
    assert_eq!(home.get_appended(&entry_keys[0]), Some(founder_bytes));
    assert!(home
        .appended_keys()
        .iter()
        .all(|key| !key.starts_with(&format!("store-v1/membership/heads/{owner_pk}/"))));
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
}

#[tokio::test]
async fn initialization_refuses_a_foreign_founder_without_genesis() {
    use crate::sync::membership::founder_entry;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let attacker = UserKeypair::generate();
    let attacker_pk = pubkey_hex(&attacker);
    let attacker_storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        attacker.clone(),
    );
    let foreign_bytes = serde_json::to_vec(&founder_entry(
        "test-lib",
        &attacker,
        "0000000001000-0000-uncommitted-foreign-founder",
    ))
    .unwrap();
    crate::sync::test_helpers::append_membership_entry_bytes(
        &attacker_storage,
        &attacker_pk,
        1,
        foreign_bytes.clone(),
    )
    .await
    .unwrap();

    let owner = UserKeypair::generate();
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner,
    );
    let db = open_test_db();
    let error =
        match cycle::init_sync_over_storage(&db, storage, cycle::StoreInitialization::CreateStore)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("a nonempty Store layout without genesis must fail loud"),
        };
    assert!(
        matches!(error, cycle::InitSyncError::ProtocolGenesis(ref message) if message.contains("nonempty but has no supported genesis")),
        "{error}"
    );

    let entry_prefix = format!("store-v1/membership/entries/{attacker_pk}/");
    let entry_keys: Vec<_> = home
        .appended_keys()
        .into_iter()
        .filter(|key| key.starts_with(&entry_prefix))
        .collect();
    assert_eq!(entry_keys.len(), 1);
    assert_eq!(home.get_appended(&entry_keys[0]), Some(foreign_bytes));
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
}

#[tokio::test]
async fn initialization_pins_a_committed_self_founder_without_cloud_rewrite() {
    use crate::sync::membership::{founder_entry, MembershipChain};
    use crate::sync::membership_ops::{publish_membership_head, OWNER_PUBKEY_STATE_KEY};

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    let db = open_test_db();
    let founder = founder_entry("test-lib", &owner, "0000000001000-0000-founder");
    let genesis = ProtocolGenesis::signed(
        "test-lib".to_string(),
        founder.clone(),
        db.schema_version(),
        &owner,
    )
    .expect("sign protocol genesis");
    let genesis_hash = genesis.object_hash();
    db.stage_protocol_genesis(genesis.clone())
        .await
        .expect("stage protocol genesis");
    crate::sync::store_objects::append_and_verify(
        &storage,
        &crate::sync::store_commit::genesis_semantic_prefix(genesis_hash),
        ".json",
        &genesis.to_bytes(),
    )
    .await
    .expect("publish protocol genesis");
    db.complete_protocol_genesis(genesis_hash)
        .await
        .expect("complete protocol genesis");

    let coord = founder.coord();
    let mut chain = MembershipChain::new();
    chain
        .add_entry_at(coord, founder.clone())
        .expect("add protocol founder");
    crate::sync::test_helpers::append_membership_entry_bytes(
        &storage,
        &owner_pk,
        1,
        serde_json::to_vec(&founder).expect("serialize protocol founder"),
    )
    .await
    .expect("publish protocol founder");
    publish_membership_head(&storage, &chain, &owner)
        .await
        .expect("publish protocol founder head");
    let cloud_before = cloud_objects(&home);

    cycle::init_sync_over_storage(&db, storage, cycle::StoreInitialization::CreateStore)
        .await
        .expect("accept the identity's committed founder");

    assert_eq!(cloud_objects(&home), cloud_before);
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    let floor: crate::sync::membership::MembershipCoord = serde_json::from_str(
        &db.get_protocol_state(&format!(
            "membership_head_seq/{}",
            founder.author_owner_grant
        ))
        .await
        .unwrap()
        .expect("persist exact founder floor"),
    )
    .expect("parse exact founder floor");
    assert_eq!(floor, founder.coord());
}

#[tokio::test]
async fn plaintext_initialization_refuses_a_committed_foreign_founder_without_mutation() {
    use crate::sync::membership_ops::{write_founder_entry, OWNER_PUBKEY_STATE_KEY};

    let home = InMemoryCloudHome::new();
    let attacker = UserKeypair::generate();
    let attacker_storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        attacker.clone(),
    );
    write_founder_entry(
        &attacker_storage,
        "test-lib",
        &attacker,
        "0000000001000-0000-attacker",
    )
    .await
    .unwrap();
    let cloud_before = cloud_objects(&home);

    let victim = UserKeypair::generate();
    let victim_pk = pubkey_hex(&victim);
    let db = open_test_db();
    let cipher = CloudCipher::Plaintext;
    let victim_storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        cipher.clone(),
        BlobPathScheme::Plain,
        "test-lib",
        victim.clone(),
    );

    assert!(
        cycle::init_sync_over_storage(&db, victim_storage, cycle::StoreInitialization::CreateStore)
            .await
            .is_err(),
        "a committed foreign founder prevents initialization",
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
    assert_eq!(
        db.get_protocol_state(&format!("membership_head_seq/{victim_pk}"))
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
        let storage = cycle_cloud_storage(
            Arc::new(home.clone()),
            cipher.clone(),
            blob_paths,
            "test-lib",
            owner.clone(),
        );
        db.set_protocol_state(crate::sync::cloud_storage::PENDING_ROTATION_STATE_KEY, "9")
            .await
            .unwrap();
        let pending_rotation = storage.shared_pending_rotation();

        assert!(
            cycle::init_sync_over_storage(&db, storage, cycle::StoreInitialization::CreateStore)
                .await
                .is_err(),
            "incoherent at-rest representation must be refused",
        );
        assert!(home.is_empty(), "the cloud is unchanged");
        assert_eq!(
            db.get_protocol_state("owner_pubkey").await.unwrap(),
            None,
            "the local owner is not pinned",
        );
        assert_eq!(
            pending_rotation.pending_generation(),
            None,
            "the in-memory pending-rotation marker is not restored",
        );
        assert_eq!(
            db.get_protocol_state(crate::sync::cloud_storage::PENDING_ROTATION_STATE_KEY)
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

use crate::sync::storage::StorageError;

/// A [`SyncStorage`] that injects a host write at a cycle `await` point — the
/// moment the cycle fetches an incoming changeset to apply — by running a host
/// INSERT through the same `Database` the cycle holds, once, before delegating
/// the immutable package read to the inner mock.
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
    async fn append_protocol_object(
        &self,
        semantic_prefix: &str,
        extension: &str,
        data: Vec<u8>,
    ) -> Result<crate::sync::storage::ProtocolObjectLocator, StorageError> {
        self.inner
            .append_protocol_object(semantic_prefix, extension, data)
            .await
    }

    async fn list_protocol_objects(
        &self,
        prefix: &str,
    ) -> Result<crate::sync::storage::ProtocolObjectListing, StorageError> {
        self.inner.list_protocol_objects(prefix).await
    }

    async fn read_protocol_object(
        &self,
        object: &crate::sync::storage::ProtocolObjectLocator,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError> {
        if semantic_prefix.starts_with("store-v1/packages/")
            && !self.fired.swap(true, Ordering::SeqCst)
        {
            host_exec(&self.db, &self.write_sql).await;
        }
        self.inner
            .read_protocol_object(object, semantic_prefix)
            .await
    }

    async fn delete_protocol_object(
        &self,
        object: &crate::sync::storage::ProtocolObjectLocator,
    ) -> Result<(), StorageError> {
        self.inner.delete_protocol_object(object).await
    }

    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner
            .put_blob(namespace, id, scope, cloud_path, data)
            .await
    }
    async fn put_blob_from_file(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_path: &std::path::Path,
    ) -> Result<(), StorageError> {
        self.inner
            .put_blob_from_file(namespace, id, scope, cloud_path, source_path)
            .await
    }
    async fn get_blob(
        &self,
        namespace: &str,
        uploader: Option<&str>,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
    ) -> Result<Vec<u8>, StorageError> {
        self.inner
            .get_blob(namespace, uploader, id, scope, cloud_path)
            .await
    }
    async fn blob_exists(
        &self,
        namespace: &str,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<bool, StorageError> {
        self.inner.blob_exists(namespace, id, cloud_path).await
    }
    async fn read_blob_range(
        &self,
        namespace: &str,
        uploader: Option<&str>,
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
                uploader,
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
        uploader: Option<&str>,
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
                uploader,
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
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<String, crate::sync::storage::StorageError> {
        self.inner.blob_cloud_key(namespace, id, cloud_path)
    }
    fn own_uploader(&self) -> Option<String> {
        self.inner.own_uploader()
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
}

/// A host write made WHILE a cycle is in its push/pull network phase
/// must land in the device's NEXT outgoing changeset. It is recorded by the same
/// durable journal path as any other host write.
///
/// Setup: a peer "A" has a changeset in shared storage. Device "M" runs a cycle
/// that pulls it; the storage wrapper injects a host INSERT into M at the
/// immutable package-read await inside the pull. We then assert the
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

    // M's database. The injector runs this INSERT into M at the package-read
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
    pull_into(&db_c, &storage.inner, "C", &ld).await;
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

    // Cycle 2 has no host write to stage. The applied row must not create an M
    // commit because apply bypasses the pending journal.
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;
    assert!(
        crate::sync::store_objects::load_commit_slot(
            &storage,
            storage.protocol_genesis_hash(),
            "M",
            1,
        )
        .await
        .expect("load M Store commit slot")
        .is_none(),
        "the row M applied from A must not create an outgoing M commit",
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
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('hponly', 'n1', 'cover', 5, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");

    storage.fail_next_blob_puts(1);
    bind_mock_store_protocol(&db, &storage, "M").await;
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
        !storage.exists("photos/hponly").await.expect("exists check"),
        "the failed blob upload did not publish the blob"
    );

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert_eq!(
        pending_changeset_count(&db).await,
        0,
        "the pending changesets clear once the retry publishes"
    );
    assert!(
        storage.exists("photos/hponly").await.expect("exists check"),
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
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('hponly', 'n1', 'cover', 5, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "hponly", b"cover")
        .await
        .expect("store host-provided blob");

    let pending_rotation = PendingRotation::none();
    pending_rotation.mark_committed(2);
    bind_mock_store_protocol(&db, &storage, "M").await;

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
        !storage.exists("photos/hponly").await.expect("exists check"),
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
        storage.exists("photos/hponly").await.expect("exists check"),
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
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('hponly', 'n1', 'cover', 5, '0000000001000-0000-M', '2026-01-01')",
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
    bind_mock_store_protocol(&db, &storage, "M").await;

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
        !storage.exists("photos/hponly").await.expect("exists check"),
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
        storage.exists("photos/hponly").await.expect("exists check"),
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
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('firstblob', 'n1', 'cover', 5, '0000000001000-0000-M', '2026-01-01'); \
         INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('secondblob', 'n1', 'cover', 6, '0000000001001-0000-M', '2026-01-01')",
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "firstblob", b"first")
        .await
        .expect("store first host-provided blob");
    crate::blob::local_files::store(&ld, "photos", "secondblob", b"second")
        .await
        .expect("store second host-provided blob");

    storage.fail_blob_put_on_call(2);
    bind_mock_store_protocol(&db, &storage, "M").await;
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
        storage
            .exists("photos/firstblob")
            .await
            .expect("exists check"),
        "the first blob reached cloud before the second upload failed"
    );
    assert!(
        crate::blob::local_files::read(&ld, "photos", "firstblob", 5)
            .await
            .expect("read first local")
            .is_some(),
        "the first local copy remains because the changeset was not published"
    );

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        store_package_exists(&storage, "M", 1).await,
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
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    ));
    storage
        .put_blob(
            "photos",
            "remoteonly",
            crate::blob::BlobScope::Master,
            None,
            b"already durable".to_vec(),
        )
        .await
        .expect("plant remote blob");
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('remoteonly', 'n1', 'cover', 15, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    storage.fail_next_blob_puts(1);
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        store_package_exists(&storage, "M", 1).await,
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
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('lazyblob', 'n1', 'cover', 4, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    crate::blob::local_files::store(&ld, "photos", "lazyblob", b"lazy")
        .await
        .expect("store cache-lazy host-provided blob");

    storage.fail_next_changeset_puts(1);
    let error = run_cycle_m_result(&storage, &db, &enc, &keypair, &hlc, &ld)
        .await
        .expect_err("the first Store package append fails");
    assert!(
        error.contains("forced Store package append failure"),
        "cycle surfaces the Store package append failure: {error}",
    );
    assert!(
        !store_package_exists(&storage, "M", 1).await,
        "the first push attempt does not publish the Store package"
    );
    assert!(
        db.oldest_outbound_store_batch()
            .await
            .expect("read outbound Store queue")
            .is_some(),
        "the exact outbound Store batch remains durable",
    );
    assert!(
        crate::blob::local_files::read(&ld, "photos", "lazyblob", 4)
            .await
            .expect("read lazy local")
            .is_some(),
        "the local copy remains until the changeset is published"
    );

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        store_package_exists(&storage, "M", 1).await,
        "the staged retry publishes the Store package"
    );
    assert!(
        crate::blob::local_files::read(&ld, "photos", "lazyblob", 4)
            .await
            .expect("read lazy local after publish")
            .is_none(),
        "the local copy drops after the staged retry commits"
    );
}

/// Every immutable head the cycle wrote for our own device records its publish
/// time as an RFC 3339 wall-clock string, never the HLC string used to order
/// row writes. An HLC string
/// (`0000000001000-0000-M`) fails RFC 3339 parsing, so this distinguishes them.
async fn assert_own_head_timestamps_are_rfc3339(storage: &MockSyncStorage, device_id: &str) {
    let heads = store_heads(storage).await;
    let ours: Vec<&str> = heads
        .iter()
        .filter(|head| head.device_id == device_id)
        .map(|head| head.published_at.as_str())
        .collect();
    assert!(
        !ours.is_empty(),
        "the cycle wrote at least one head for {device_id}",
    );
    for timestamp in ours {
        assert!(
            chrono::DateTime::parse_from_rfc3339(timestamp).is_ok(),
            "head publish time must be RFC 3339, got {timestamp:?}",
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

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        store_package_exists(&storage, "M", 1).await,
        "the cycle pushed a Store package and its immutable head",
    );
    assert_own_head_timestamps_are_rfc3339(&storage, "M").await;
}

/// Snapshot metadata records its creation time as RFC 3339.
#[tokio::test]
async fn snapshot_cycle_writes_rfc3339_metadata_timestamp() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [22u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // local_seq past 0 with no snapshot yet → the snapshot policy fires this cycle.
    db.set_protocol_state("local_seq", "1")
        .await
        .expect("seed local_seq");

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    let snapshots = store_snapshot_metas(&storage).await;
    assert_eq!(snapshots.len(), 1, "the cycle published one snapshot");
    assert!(
        chrono::DateTime::parse_from_rfc3339(&snapshots[0].created_at).is_ok(),
        "snapshot creation time must be RFC 3339, got {:?}",
        snapshots[0].created_at,
    );
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

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    // The first push fails at the changeset put, so the changeset stages for retry
    // and no head is written for it yet.
    storage.fail_next_changeset_puts(1);
    run_cycle_m_result(&storage, &db, &enc, &keypair, &hlc, &ld)
        .await
        .expect_err("the first Store package append fails");
    assert!(
        db.oldest_outbound_store_batch()
            .await
            .expect("read outbound Store queue")
            .is_some(),
        "the exact Store batch remains durable after append failure",
    );

    // The next cycle retries the staged push, exercising the staged-retry writer.
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        store_package_exists(&storage, "M", 1).await,
        "the staged retry publishes the Store package",
    );
    assert_own_head_timestamps_are_rfc3339(&storage, "M").await;
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
    storage
        .put_blob(
            "audio",
            "audio1",
            crate::blob::BlobScope::Master,
            None,
            b"AUDIO".to_vec(),
        )
        .await
        .expect("plant remote user-provided blob");
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('audio1', 'n1', 'audio', '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    storage.fail_next_changeset_puts(1);
    run_cycle_m_result(&storage, &db, &enc, &keypair, &hlc, &ld)
        .await
        .expect_err("the first Store package append fails");
    assert!(
        db.oldest_outbound_store_batch()
            .await
            .expect("read outbound Store queue")
            .is_some(),
        "the exact Store batch remains queued after append failure"
    );
    assert!(!store_package_exists(&storage, "M", 1).await);

    storage.delete_blob_object("audio", "audio1").await;
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
        err.contains("outbound blob audio/audio1 is absent from storage"),
        "staged retry surfaces the missing blob: {err}",
    );
    assert!(!store_package_exists(&storage, "M", 1).await);
    assert!(
        store_heads(&storage)
            .await
            .into_iter()
            .all(|head| head.device_id != "M"),
        "the retry aborts before publishing an M head"
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
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('stage-fail', 'Stage Fail', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    db.call(|conn| {
        conn.execute_batch(
            "CREATE TEMP TRIGGER fail_outbound_stage \
             BEFORE INSERT ON outbound_store_batches \
             BEGIN SELECT RAISE(ABORT, 'injected Store staging failure'); END;",
        )
        .map_err(crate::database::DbError::from)
    })
    .await
    .expect("install Store staging fault");
    bind_mock_store_protocol(&db, &storage, "M").await;
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
        failed.contains("injected Store staging failure"),
        "cycle surfaces the outgoing staging failure: {failed}"
    );
    assert_eq!(
        pending_changeset_count(&db).await,
        1,
        "the pending batch remains queued when outgoing staging fails"
    );
    assert!(!store_package_exists(&storage, "M", 1).await);

    db.call(|conn| {
        conn.execute_batch("DROP TRIGGER fail_outbound_stage")
            .map_err(crate::database::DbError::from)
    })
    .await
    .expect("remove Store staging fault");
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        store_package_exists(&storage, "M", 1).await,
        "the same pending batch publishes after staging succeeds"
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
    storage: &HostWriteInjector,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &StoreDir,
) {
    bind_mock_store_protocol(db, &storage.inner, "M").await;
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

/// A package that becomes both snapshot-covered and acknowledged by every active
/// device is reclaimed by the cycle that publishes the snapshot. Peer A has
/// pushed A/1; M pulls it, acknowledges it, snapshots it, and reclaims its package.
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
    publish_mock_founder_membership(&storage).await;

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
    storage.store_changeset_with_grant(
        "A",
        1,
        &a_cs,
        SCHEMA_VERSION,
        Some(storage.protocol_founder_coord()),
    );

    // M's cycle pulls A->1, acks A->1, snapshots covering A->1, then reclaims.
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    assert!(
        !store_package_exists(&storage, "A", 1).await,
        "a snapshot-covered, fully-acknowledged package is reclaimed by the cycle",
    );
}

/// Reclamation refuses the whole snapshot proof while any active device is behind
/// that snapshot. Peer A has pushed A/1 and A/2; active device B acknowledges only
/// A/1. M snapshots A/2, so both packages remain and B can pull A/2.
#[tokio::test]
async fn cycle_preserves_packages_until_every_device_covers_the_snapshot() {
    let keypair = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(keypair.clone());
    let db_m = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [12u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());
    publish_mock_founder_membership(&storage).await;

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
    storage.store_changeset_with_grant(
        "A",
        1,
        &cs1,
        SCHEMA_VERSION,
        Some(storage.protocol_founder_coord()),
    );
    let cs2 = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a2', 'Title Beta', NULL, 1, '0000000002000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_with_grant(
        "A",
        2,
        &cs2,
        SCHEMA_VERSION,
        Some(storage.protocol_founder_coord()),
    );

    // B is an active device for the owner and reports the exact A/1 commit hash.
    append_active_store_device(&storage, "B", &keypair).await;
    append_store_ack(
        &storage,
        "B",
        BTreeMap::from([("A".to_string(), storage.store_commit_position("A", 1))]),
        &keypair,
    )
    .await;

    // M's cycle pulls A/2, acknowledges A/2, and snapshots A/2. B does not cover
    // that snapshot position, so the reclamation proof is incomplete.
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    assert!(
        store_package_exists(&storage, "A", 1).await,
        "reclamation does not delete part of a snapshot whose proof is incomplete",
    );
    assert!(
        store_package_exists(&storage, "A", 2).await,
        "the package the behind peer still needs is kept",
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
    let position = storage.store_commit_position("A", 1);
    db_b.call(move |conn| {
        conn.execute(
            "INSERT INTO materialized_commits (device_id, seq, commit_hash) \
                 VALUES ('A', 1, ?1)",
            [position.commit_hash.to_string()],
        )
        .map_err(crate::database::DbError::from)?;
        Ok(())
    })
    .await
    .expect("seed B's exact A/1 materialized position");
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
    use crate::sync::membership::{MemberRole, MembershipChain};
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(owner.clone());
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [5u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    let member = UserKeypair::generate();

    // The owner founds the chain and adds this device as a write-capable Member.
    let owner_pk = pubkey_hex(&owner);
    let mut chain = MembershipChain::new();
    let founder = storage.protocol_genesis().founder;
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "0000000002000-0000-owner".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // This device pins the owner (set on join in production) — an opaque store.
    db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
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

    // The Member's row reached the cloud as an immutable Store package.
    assert!(
        store_package_exists(&storage, "M", 1).await,
        "the member's rows still propagate via the Store commit",
    );
    // No catalog snapshot metadata was authored.
    assert!(
        store_snapshot_metas(&storage).await.is_empty(),
        "a non-owner device must not author catalog snapshot metadata",
    );
}

/// The mirror of the above: an Owner device with local data and itself pinned as the
/// owner DOES author the snapshot — the founder/initial-sync path a freshly-founded
/// store bootstraps from is preserved by the gate's owner branch.
#[tokio::test]
async fn owner_device_creates_a_snapshot() {
    use crate::sync::membership::MembershipChain;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(owner.clone());
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [6u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    let owner_pk = pubkey_hex(&owner);
    let mut chain = MembershipChain::new();
    let founder = storage.protocol_genesis().founder;
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .expect("pin owner");

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    run_cycle_m(&storage, &db, &cipher, &owner, &hlc, &ld).await;

    assert_eq!(
        store_snapshot_metas(&storage).await.len(),
        1,
        "an owner device must author catalog snapshot metadata",
    );
}
