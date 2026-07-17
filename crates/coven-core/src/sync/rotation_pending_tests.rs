//! A device that has not adopted a store-key rotation the cloud has already
//! committed must seal nothing new for the cloud — not a changeset, not a blob,
//! not a tombstone, not a snapshot — until it adopts. Confidentiality after a
//! member removal rests entirely on the rotation: the removed member keeps its
//! S3 credential and residual bucket read, so anything this device seals under
//! the superseded generation in the meantime is readable to them.
//!
//! These drive the real [`CloudSyncStorage`] over an [`InMemoryCloudHome`] (not
//! the plaintext-shaped `TestStore` other sync tests use), because the
//! point here is to observe actual sealed bytes at rest: whether an object
//! reaches the cloud at all, and whether the removed member's superseded key
//! can open it.

use std::sync::Arc;

use async_trait::async_trait;

use crate::clock::SystemClock;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::{
    BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeError, CloudHomeJoinInfo,
};
use crate::sync::cloud_storage::{
    BlobPathScheme, CloudCipher, CloudCipherAccess, CloudSyncStorage,
};
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::membership::MemberRole;
use crate::sync::membership_ops::{
    invite_member, invite_member_with_coordination, remove_member, remove_member_with_coordination,
    MembershipOpsError, OWNER_PUBKEY_STATE_KEY,
};
use crate::sync::store_commit::{
    StoreBatchCommitRef, StoreDeviceRegistration, StoreRootRef, StoreSerialHeadState,
};
use crate::sync::test_helpers::{
    host_exec, open_serial_test_db, open_test_db, pubkey_hex, temp_store_dir, TestCustody,
};

const LIB_ID: &str = "rotation-pending-test";
const DEVICE_ID: &str = "owner-device";

fn storage_for(home: &InMemoryCloudHome, key: [u8; 32], keypair: &UserKeypair) -> CloudSyncStorage {
    CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key(key)),
        BlobPathScheme::Hashed,
        LIB_ID,
        keypair.clone(),
    )
    .expect("build exact test cloud storage")
}

async fn create_test_store(
    db: &crate::database::Database,
    storage: &CloudSyncStorage,
    owner: &UserKeypair,
) -> StoreRootRef {
    crate::sync::store_protocol_root::create_store(db, storage, LIB_ID, owner)
        .await
        .expect("create exact test Store");
    db.local_store_root_ref()
        .await
        .expect("read exact test Store root")
        .expect("created test Store root is present")
}

async fn local_store_device_id(db: &crate::database::Database) -> String {
    db.get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device id")
        .expect("local Store device registration is active")
}

async fn founder_registration(
    storage: &CloudSyncStorage,
    root: &StoreRootRef,
) -> StoreDeviceRegistration {
    crate::sync::store_objects::load_founder_registration(storage, root)
        .await
        .expect("load exact founder Store device registration")
        .value
}

/// `InMemoryCloudHome` refuses `grant_access` — it models a backend with no
/// concept of a per-member cloud account, which is not what these tests are
/// about. This forwards every other call straight through to the same backing
/// store and returns a dummy S3 grant, exactly so `invite_member`'s access-grant
/// step (irrelevant here — these tests are about the store-key rotation, not
/// provider access control) does not stand in the way of building the chain.
struct GrantingCloudHome(InMemoryCloudHome);

#[async_trait]
impl CloudHome for GrantingCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.0.put_object(key, data).await
    }
    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        self.0.open_multipart(key, total_len).await
    }
    fn multipart_threshold(&self) -> u64 {
        self.0.multipart_threshold()
    }
    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.0.read(key).await
    }
    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        self.0.read_range(key, start, end).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        self.0.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.0.delete(key).await
    }
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        self.0.exists(key).await
    }
    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        match desired {
            CloudAccessState::Present { .. } => {
                Ok(CloudAccessOutcome::Present(CloudHomeJoinInfo::S3 {
                    bucket: "test-bucket".to_string(),
                    region: "us-east-1".to_string(),
                    endpoint: None,
                    access_key: "test-access-key".to_string(),
                    secret_key: "test-secret-key".to_string(),
                    key_prefix: None,
                }))
            }
            absent => self.0.set_access(absent).await,
        }
    }
}

#[tokio::test]
async fn public_serial_invite_activates_one_control_only_commit() {
    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let key = [0x41; 32];
    let storage =
        storage_for(&home, key, &owner).with_test_serial_coordination(Arc::new(home.clone()));
    let db = open_serial_test_db();
    let root = create_test_store(&db, &storage, &owner).await;
    let device_id = local_store_device_id(&db).await;
    let registration = founder_registration(&storage, &root).await;
    let granting_home = GrantingCloudHome(home.clone());
    let code = invite_member_with_coordination(
        &storage,
        &granting_home,
        &owner,
        &Hlc::new(DEVICE_ID.to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &EncryptionService::from_key(key),
        LIB_ID,
        "Serial Store",
        &db,
        Some(crate::sync::membership_ops::SerialMembershipContext {
            coordination: storage.serial_coordination().unwrap(),
            device_id: device_id.clone(),
        }),
    )
    .await
    .expect("public Serial invitation");
    let commit_ref = match code.membership_floor {
        crate::join_code::MembershipFloor::Serial(Some(reference)) => reference,
        crate::join_code::MembershipFloor::Serial(None) => {
            panic!("Serial invitation returned the root floor")
        }
        crate::join_code::MembershipFloor::MergeConcurrent(_) => {
            panic!("Serial invitation returned a causal membership floor")
        }
    };
    let head = storage
        .serial_coordination()
        .unwrap()
        .read_head(crate::sync::store_commit::serial_head_key())
        .await
        .unwrap();
    let head = crate::sync::store_commit::StoreSerialHead::parse(
        &head.bytes,
        root.store_root_hash,
        &registration,
    )
    .unwrap();
    assert!(matches!(
        &head.state,
        StoreSerialHeadState::Commit { commit, .. } if commit == &commit_ref
    ));
    let commit = crate::sync::store_objects::load_commit_ref(
        &storage,
        root.store_root_hash,
        &commit_ref,
        &registration,
    )
    .await
    .unwrap()
    .value;
    commit_ref
        .verify_commit(&commit)
        .expect("prepared exact commit ref verifies its signed commit");
    assert!(matches!(
        commit.control(),
        Some(crate::sync::store_commit::StoreControl::SerialMembership { .. })
    ));
    assert!(
        crate::sync::store_objects::load_store_package(&storage, &commit_ref, &commit)
            .await
            .unwrap()
            .is_none()
    );
    assert!(commit.store_package().is_none());
    assert!(db
        .serial_membership_state()
        .await
        .unwrap()
        .unwrap()
        .can_write(&pubkey_hex(&member)));

    let custody = TestCustody::default();
    custody.set_initial_key(key);
    let cipher = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();
    remove_member_with_coordination(
        &storage,
        &granting_home,
        &owner,
        &Hlc::new(DEVICE_ID.to_string()),
        &pubkey_hex(&member),
        LIB_ID,
        &EncryptionService::from_key(key),
        &custody,
        &cipher,
        &pending_rotation,
        &db,
        Some(crate::sync::membership_ops::SerialMembershipContext {
            coordination: storage.serial_coordination().unwrap(),
            device_id,
        }),
    )
    .await
    .expect("public Serial removal and rotation");
    assert!(!db
        .serial_membership_state()
        .await
        .unwrap()
        .unwrap()
        .can_write(&pubkey_hex(&member)));
    assert_eq!(db.serial_key_generation().await.unwrap(), Some(2));
    let head = storage
        .serial_coordination()
        .unwrap()
        .read_head(crate::sync::store_commit::serial_head_key())
        .await
        .unwrap();
    let head = crate::sync::store_commit::StoreSerialHead::parse(
        &head.bytes,
        root.store_root_hash,
        &registration,
    )
    .unwrap();
    let StoreSerialHeadState::Commit {
        commit: removal_ref,
        ..
    } = head.state
    else {
        panic!("Serial removal did not publish a commit head")
    };
    assert_eq!(removal_ref.coord.sequence(), 2);
    let removal = crate::sync::store_objects::load_commit_ref(
        &storage,
        root.store_root_hash,
        &removal_ref,
        &registration,
    )
    .await
    .unwrap()
    .value;
    assert!(matches!(
        removal.control(),
        Some(
            crate::sync::store_commit::StoreControl::SerialMembershipAndKeyRotation {
                generation: 2,
                ..
            }
        )
    ));
    assert!(
        crate::sync::store_objects::load_store_package(&storage, &removal_ref, &removal)
            .await
            .unwrap()
            .is_none()
    );
}

async fn insert_shareable_row(db: &crate::database::Database, id: &str, stamp: &str) {
    host_exec(
        db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('{id}', 'title', NULL, 1, '{stamp}', '2026-01-01')"
        ),
    )
    .await;
}

async fn mark_snapshot_floor(db: &crate::database::Database) {
    db.set_protocol_state("snapshot_seq", "0")
        .await
        .expect("persist snapshot floor");
}

/// Found a store with `owner` as its sole owner, add `member`, then remove
/// `member` while `custody` is failing — so the cloud rotation commits (to
/// generation 2) but this device's local adoption fails. Returns the storage
/// whose cipher and pending-rotation marker a later cycle or a retried removal
/// reads, and the `Hlc` used throughout.
async fn found_add_and_fail_to_adopt_a_removal(
    db: &crate::database::Database,
    home: &InMemoryCloudHome,
    owner: &UserKeypair,
    member: &UserKeypair,
    custody: &TestCustody,
    old_key: [u8; 32],
) -> (CloudSyncStorage, Hlc, StoreRootRef) {
    let storage = storage_for(home, old_key, owner);
    let store_root = create_test_store(db, &storage, owner).await;
    db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(owner))
        .await
        .expect("pin test Store founder");
    let hlc = Hlc::new(DEVICE_ID.to_string());
    let granting_home = GrantingCloudHome(home.clone());
    invite_member(
        &storage,
        &granting_home,
        owner,
        &hlc,
        &pubkey_hex(member),
        None,
        MemberRole::Member,
        &EncryptionService::from_key(old_key),
        LIB_ID,
        "Test Store",
        db,
    )
    .await
    .expect("invite member");

    custody.set_initial_key(old_key);
    custody.fail_writes();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();
    let err = remove_member(
        &storage,
        storage.cloud_home(),
        owner,
        &hlc,
        &pubkey_hex(member),
        LIB_ID,
        &EncryptionService::from_key(old_key),
        custody,
        &cipher_lock,
        &pending_rotation,
        db,
    )
    .await
    .expect_err("adoption fails while custody is unwritable");
    assert!(
        matches!(
            err,
            MembershipOpsError::RotationCommittedAdoptionFailed { .. }
        ),
        "the failure is the rotation-committed/adoption-failed variant, got {err:?}",
    );

    (storage, hlc, store_root)
}

/// The defect this closes: today, a device whose adoption fails keeps sealing
/// new changesets under the superseded generation — the removed member's key
/// still opens them. Driving `remove_member` into that failure, writing a row,
/// and running a cycle must instead produce no cloud object at all, with the
/// cycle reporting the rotation-pending state rather than silently sealing or
/// hard-failing the whole cycle.
#[tokio::test]
async fn a_device_that_failed_to_adopt_a_rotation_seals_nothing_new() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let old_key: [u8; 32] = [40u8; 32];
    let custody = TestCustody::default();
    let home = InMemoryCloudHome::new();
    let db = open_test_db();

    let (storage, hlc, _store_root_hash) =
        found_add_and_fail_to_adopt_a_removal(&db, &home, &owner, &member, &custody, old_key).await;
    let device_id = local_store_device_id(&db).await;

    db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .unwrap();
    // Seed the snapshot floor so this device's first cycle takes the ordinary
    // changeset path rather than the initial-sync snapshot coven pushes when a
    // store has pre-existing local data and no snapshot yet — these tests are
    // about the changeset push specifically.
    mark_snapshot_floor(&db).await;
    insert_shareable_row(&db, "n1", "0000000005000-0000-owner-device").await;

    let (_tmp, store_dir) = temp_store_dir();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();
    let result = run_single_sync_cycle(
        &storage,
        LIB_ID,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &cipher_lock,
        &pending_rotation,
        &owner,
        Some(&custody),
        &store_dir,
        Some(storage.cloud_home()),
        None,
    )
    .await
    .expect("the cycle reports rotation-pending rather than hard-failing");

    let pending = result
        .rotation_pending
        .expect("the cycle reports the rotation-pending state");
    assert_eq!(pending.committed_generation, 2);
    assert_eq!(pending.live_generation, 1);

    assert_eq!(
        db.get_protocol_state("local_seq").await.unwrap(),
        None,
        "the pending Store write stays queued while key adoption is incomplete",
    );
}

/// Retrying the removal (idempotent: the member is already gone, so it re-derives
/// and re-adopts the same generation) clears the gate, and the changeset that
/// stayed queued through the stuck cycle now drains — sealed under the rotated
/// generation, not the one the removed member holds.
#[tokio::test]
async fn retrying_the_removal_adopts_the_rotation_and_drains_the_pending_changeset() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let old_key: [u8; 32] = [41u8; 32];
    let custody = TestCustody::default();
    let home = InMemoryCloudHome::new();
    let db = open_test_db();

    let (storage, hlc, store_root) =
        found_add_and_fail_to_adopt_a_removal(&db, &home, &owner, &member, &custody, old_key).await;
    let device_id = local_store_device_id(&db).await;

    db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .unwrap();
    // Seed the snapshot floor so this device's first cycle takes the ordinary
    // changeset path rather than the initial-sync snapshot coven pushes when a
    // store has pre-existing local data and no snapshot yet — these tests are
    // about the changeset push specifically.
    mark_snapshot_floor(&db).await;
    insert_shareable_row(&db, "n1", "0000000005000-0000-owner-device").await;

    let (_tmp, store_dir) = temp_store_dir();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();

    // Still stuck: a cycle now seals nothing.
    run_single_sync_cycle(
        &storage,
        LIB_ID,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &cipher_lock,
        &pending_rotation,
        &owner,
        Some(&custody),
        &store_dir,
        Some(storage.cloud_home()),
        None,
    )
    .await
    .expect("still-pending cycle");
    assert_eq!(
        db.latest_local_store_position().await.unwrap(),
        None,
        "the queued Store write has no published commit while rotation is pending",
    );

    // Retry the removal now that custody is writable again.
    custody.allow_writes();
    remove_member(
        &storage,
        storage.cloud_home(),
        &owner,
        &hlc,
        &pubkey_hex(&member),
        LIB_ID,
        &EncryptionService::from_key(old_key),
        &custody,
        &cipher_lock,
        &pending_rotation,
        &db,
    )
    .await
    .expect("retrying the removal converges");
    assert_eq!(
        pending_rotation.pending_generation(),
        None,
        "adoption clears the gate",
    );

    // The queued changeset now drains, sealed under the rotated generation.
    let result = run_single_sync_cycle(
        &storage,
        LIB_ID,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &cipher_lock,
        &pending_rotation,
        &owner,
        Some(&custody),
        &store_dir,
        Some(storage.cloud_home()),
        None,
    )
    .await
    .expect("cycle after adoption");
    assert!(result.rotation_pending.is_none());

    let commit_ref = db
        .latest_local_store_position()
        .await
        .expect("read published Store position")
        .expect("published Store write has an exact commit reference");
    let registration = founder_registration(&storage, &store_root).await;
    assert_generation_two_opens_but_generation_one_does_not(
        &home,
        &cipher_lock,
        old_key,
        &store_root,
        &commit_ref,
        &registration,
        &owner,
        &member,
    )
    .await;
}

/// The other remedy: without ever retrying the removal, the next sync cycle's
/// own refresh discovers the rotation from this device's own re-wrapped
/// `keys/{owner}/{owner}` and adopts it, clearing the gate the same way.
#[tokio::test]
async fn the_next_sync_cycle_adopts_the_rotation_and_drains_the_pending_changeset() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let old_key: [u8; 32] = [42u8; 32];
    let custody = TestCustody::default();
    let home = InMemoryCloudHome::new();
    let db = open_test_db();

    let (storage, hlc, store_root) =
        found_add_and_fail_to_adopt_a_removal(&db, &home, &owner, &member, &custody, old_key).await;
    let device_id = local_store_device_id(&db).await;

    db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .unwrap();
    // Seed the snapshot floor so this device's first cycle takes the ordinary
    // changeset path rather than the initial-sync snapshot coven pushes when a
    // store has pre-existing local data and no snapshot yet — these tests are
    // about the changeset push specifically.
    mark_snapshot_floor(&db).await;
    insert_shareable_row(&db, "n1", "0000000005000-0000-owner-device").await;

    let (_tmp, store_dir) = temp_store_dir();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();

    run_single_sync_cycle(
        &storage,
        LIB_ID,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &cipher_lock,
        &pending_rotation,
        &owner,
        Some(&custody),
        &store_dir,
        Some(storage.cloud_home()),
        None,
    )
    .await
    .expect("still-pending cycle");
    assert_eq!(
        db.latest_local_store_position().await.unwrap(),
        None,
        "the queued Store write has no published commit while rotation is pending",
    );

    // No retried removal — custody just becomes writable again, and the next
    // cycle's own refresh adopts the rotation.
    custody.allow_writes();
    let result = run_single_sync_cycle(
        &storage,
        LIB_ID,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &cipher_lock,
        &pending_rotation,
        &owner,
        Some(&custody),
        &store_dir,
        Some(storage.cloud_home()),
        None,
    )
    .await
    .expect("cycle that adopts the rotation");
    assert!(result.rotation_pending.is_none());
    assert_eq!(pending_rotation.pending_generation(), None);

    let commit_ref = db
        .latest_local_store_position()
        .await
        .expect("read published Store position")
        .expect("published Store write has an exact commit reference");
    let registration = founder_registration(&storage, &store_root).await;
    assert_generation_two_opens_but_generation_one_does_not(
        &home,
        &cipher_lock,
        old_key,
        &store_root,
        &commit_ref,
        &registration,
        &owner,
        &member,
    )
    .await;
}

/// The removed member's generation-one key must not open the exact commit that
/// published the queued package, while the current cipher does. The package may
/// already be reclaimed after this device acknowledges its materialized commit.
async fn assert_generation_two_opens_but_generation_one_does_not(
    home: &InMemoryCloudHome,
    cipher: &dyn CloudCipherAccess,
    old_key: [u8; 32],
    store_root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    registration: &StoreDeviceRegistration,
    current_reader: &UserKeypair,
    removed_reader: &UserKeypair,
) {
    let current_storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        cipher.snapshot(),
        BlobPathScheme::Hashed,
        LIB_ID,
        current_reader.clone(),
    )
    .expect("build current-reader exact storage");
    let commit = crate::sync::store_objects::load_commit_ref(
        &current_storage,
        store_root.store_root_hash,
        commit_ref,
        registration,
    )
    .await
    .expect("the current cipher opens the exact Store commit")
    .value;
    assert!(
        commit.store_package().is_some(),
        "the published Store commit names the queued package",
    );

    let removed_member_storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key(old_key)),
        BlobPathScheme::Hashed,
        LIB_ID,
        removed_reader.clone(),
    )
    .expect("build removed-reader exact storage");
    assert!(
        crate::sync::store_objects::load_commit_ref(
            &removed_member_storage,
            store_root.store_root_hash,
            commit_ref,
            registration,
        )
        .await
        .is_err(),
        "the removed member's generation-one key must not open post-adoption content",
    );
}
