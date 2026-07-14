//! A device that has not adopted a store-key rotation the cloud has already
//! committed must seal nothing new for the cloud — not a changeset, not a blob,
//! not a tombstone, not a snapshot — until it adopts. Confidentiality after a
//! member removal rests entirely on the rotation: the removed member keeps its
//! S3 credential and residual bucket read, so anything this device seals under
//! the superseded generation in the meantime is readable to them.
//!
//! These drive the real [`CloudSyncStorage`] over an [`InMemoryCloudHome`] (not
//! the plaintext-shaped `MockSyncStorage` other sync tests use), because the
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
    BoxPartSink, CloudAccessGrant, CloudAccessRevoke, CloudHome, CloudHomeError, CloudHomeJoinInfo,
    RevokeOutcome,
};
use crate::sync::cloud_storage::{
    cloud_aad_context, BlobPathScheme, CloudCipher, CloudCipherAccess, CloudSyncStorage,
};
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::membership::MemberRole;
use crate::sync::membership_ops::{
    invite_member, remove_member, write_founder_entry, MembershipOpsError, OWNER_PUBKEY_STATE_KEY,
};
use crate::sync::test_helpers::{host_exec, open_test_db, pubkey_hex, temp_store_dir, TestCustody};

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
}

/// `InMemoryCloudHome` refuses `grant_access` — it models a backend with no
/// concept of a per-member cloud account, which is not what these tests are
/// about. This forwards every other call straight through to the same backing
/// store and returns a dummy S3 grant, exactly so `invite_member`'s access-grant
/// step (irrelevant here — these tests are about the store-key rotation, not
/// provider access control) does not stand in the way of building the chain.
struct GrantingCloudHome(InMemoryCloudHome);

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
    async fn grant_access(
        &self,
        _grant: CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        Ok(CloudHomeJoinInfo::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key: "test-access-key".to_string(),
            secret_key: "test-secret-key".to_string(),
            key_prefix: None,
        })
    }
    async fn revoke_access(
        &self,
        revoke: CloudAccessRevoke,
    ) -> Result<RevokeOutcome, CloudHomeError> {
        self.0.revoke_access(revoke).await
    }
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

/// Every `changes/` object currently in `home`.
fn changeset_keys(home: &InMemoryCloudHome) -> Vec<String> {
    home.keys()
        .into_iter()
        .filter(|k| k.starts_with("changes/"))
        .collect()
}

/// Found a store with `owner` as its sole owner, add `member`, then remove
/// `member` while `custody` is failing — so the cloud rotation commits (to
/// generation 2) but this device's local adoption fails. Returns the storage
/// whose cipher and pending-rotation marker a later cycle or a retried removal
/// reads, and the `Hlc` used throughout.
async fn found_add_and_fail_to_adopt_a_removal(
    home: &InMemoryCloudHome,
    owner: &UserKeypair,
    member: &UserKeypair,
    custody: &TestCustody,
    old_key: [u8; 32],
) -> (CloudSyncStorage, Hlc) {
    let storage = storage_for(home, old_key, owner);
    let hlc = Hlc::new(DEVICE_ID.to_string());

    write_founder_entry(&storage, owner, &hlc.now().to_string())
        .await
        .expect("found store");
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

    (storage, hlc)
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

    let (storage, hlc) =
        found_add_and_fail_to_adopt_a_removal(&home, &owner, &member, &custody, old_key).await;

    let db = open_test_db();
    db.set_sync_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .unwrap();
    // Seed the snapshot floor so this device's first cycle takes the ordinary
    // changeset path rather than the initial-sync snapshot coven pushes when a
    // store has pre-existing local data and no snapshot yet — these tests are
    // about the changeset push specifically.
    db.set_sync_state("snapshot_seq", "0").await.unwrap();
    insert_shareable_row(&db, "n1", "0000000005000-0000-owner-device").await;

    let (_tmp, store_dir) = temp_store_dir();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();
    let result = run_single_sync_cycle(
        &storage,
        LIB_ID,
        DEVICE_ID,
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

    assert!(
        changeset_keys(&home).is_empty(),
        "no changeset reaches the cloud while adoption is pending: {:?}",
        changeset_keys(&home),
    );
    assert_eq!(
        db.get_sync_state("local_seq").await.unwrap(),
        None,
        "local_seq does not advance — the pending changeset stays queued, not lost",
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

    let (storage, hlc) =
        found_add_and_fail_to_adopt_a_removal(&home, &owner, &member, &custody, old_key).await;

    let db = open_test_db();
    db.set_sync_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .unwrap();
    // Seed the snapshot floor so this device's first cycle takes the ordinary
    // changeset path rather than the initial-sync snapshot coven pushes when a
    // store has pre-existing local data and no snapshot yet — these tests are
    // about the changeset push specifically.
    db.set_sync_state("snapshot_seq", "0").await.unwrap();
    insert_shareable_row(&db, "n1", "0000000005000-0000-owner-device").await;

    let (_tmp, store_dir) = temp_store_dir();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();

    // Still stuck: a cycle now seals nothing.
    run_single_sync_cycle(
        &storage,
        LIB_ID,
        DEVICE_ID,
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
    assert!(changeset_keys(&home).is_empty(), "still nothing sealed");

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
        DEVICE_ID,
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

    let keys = changeset_keys(&home);
    assert_eq!(keys.len(), 1, "the pending changeset drains now: {keys:?}");
    assert_generation_two_opens_but_generation_one_does_not(&home, &keys[0], &cipher_lock, old_key);
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

    let (storage, hlc) =
        found_add_and_fail_to_adopt_a_removal(&home, &owner, &member, &custody, old_key).await;

    let db = open_test_db();
    db.set_sync_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .unwrap();
    // Seed the snapshot floor so this device's first cycle takes the ordinary
    // changeset path rather than the initial-sync snapshot coven pushes when a
    // store has pre-existing local data and no snapshot yet — these tests are
    // about the changeset push specifically.
    db.set_sync_state("snapshot_seq", "0").await.unwrap();
    insert_shareable_row(&db, "n1", "0000000005000-0000-owner-device").await;

    let (_tmp, store_dir) = temp_store_dir();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();

    run_single_sync_cycle(
        &storage,
        LIB_ID,
        DEVICE_ID,
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
    assert!(changeset_keys(&home).is_empty(), "still nothing sealed");

    // No retried removal — custody just becomes writable again, and the next
    // cycle's own refresh adopts the rotation.
    custody.allow_writes();
    let result = run_single_sync_cycle(
        &storage,
        LIB_ID,
        DEVICE_ID,
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

    let keys = changeset_keys(&home);
    assert_eq!(keys.len(), 1, "the pending changeset drains now: {keys:?}");
    assert_generation_two_opens_but_generation_one_does_not(&home, &keys[0], &cipher_lock, old_key);
}

/// The removed member's generation-one key must not open the changeset now at
/// `key`, while the current (post-rotation) cipher does — and this is checked
/// against the one and only changeset object either test produced, so there is
/// no generation-one object in between for the removed member to have read.
fn assert_generation_two_opens_but_generation_one_does_not(
    home: &InMemoryCloudHome,
    key: &str,
    cipher: &dyn CloudCipherAccess,
    old_key: [u8; 32],
) {
    let sealed = home.get(key).expect("changeset object present at rest");
    let aad = cloud_aad_context(LIB_ID, key);

    let current = cipher.snapshot();
    current
        .open(sealed.clone(), &aad)
        .expect("the current (post-rotation) cipher opens the changeset");

    let removed_members_key = CloudCipher::Encrypted(EncryptionService::from_key(old_key));
    assert!(
        removed_members_key.open(sealed, &aad).is_err(),
        "the removed member's generation-one key must not open post-adoption content",
    );
}
