//! Per-cycle authorization/decryption refresh.
//!
//! Each sync cycle re-reads membership and the rotatable store key before doing
//! other work, so a running device picks up a membership change or a
//! key rotation made by another device without a restart. Loaded only once at
//! init/join, a removed member would keep acting on stale authorization and
//! a non-rotating device would keep using a dead store key after a rotation it
//! didn't perform, silently diverging.
//!
//! Each test proves fail-before/pass-after: the assertion that passes with the
//! refresh in place fails when the corresponding refresh step is dropped (the
//! "mutation" each test documents).

use std::sync::{Arc, RwLock};

use crate::sync::store::authorization::load_wrapped_store_key;
use crate::sync::store::MembershipOpsError;
use crate::sync::test_helpers::{pubkey_hex, TestCustody, TestStore};
use coven_foundation::clock::SystemClock;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::MasterKeyCustody;
use coven_keys::keys::UserKeypair;
use coven_protocol::membership::{MemberRole, MembershipChain};
use coven_protocol::wrapped_store_key::{WrappedStoreKey, WrappedStoreKeyRef};
use coven_storage::CloudSyncObjectStorage;
use coven_storage::{CloudCipher, CloudSyncCipherStateAccess, PendingRotation};

const LIB_ID: &str = "lib-refresh-test";

trait RefreshTestStoreOps {
    #[allow(clippy::too_many_arguments)]
    async fn remove_member_with_local_state_for_test(
        &self,
        user_keypair: &UserKeypair,
        public_key_hex: &str,
        current_encryption: &EncryptionService,
        master_keys: &dyn MasterKeyCustody,
        cipher: &dyn coven_storage::CloudSyncCipherStateAccess,
        pending_rotation: &PendingRotation,
        db: &coven_database::Database,
        db_store_dir: coven_foundation::store_dir::StoreDir,
    ) -> Result<String, MembershipOpsError>;

    async fn revoke_member_durable(
        &self,
        db: &coven_database::Database,
        db_store_dir: coven_foundation::store_dir::StoreDir,
        owner_keypair: &UserKeypair,
        revokee_pubkey: &str,
        timestamp: &str,
        current_encryption: &EncryptionService,
        pending_rotation: &PendingRotation,
    ) -> Result<EncryptionService, MembershipOpsError>;

    async fn admit_exact_member(
        &self,
        owner_db: &coven_database::Database,
        owner_db_store_dir: coven_foundation::store_dir::StoreDir,
        owner: &UserKeypair,
        member: &UserKeypair,
        role: MemberRole,
        encryption: &EncryptionService,
    ) -> MembershipChain;

    async fn load_exact_chain(
        &self,
        db: &coven_database::Database,
        db_store_dir: coven_foundation::store_dir::StoreDir,
    ) -> MembershipChain;

    async fn create_unreferenced_wrapped_key(
        &self,
        cloud_storage: &coven_storage::CloudSyncConnection,
        owner_db: &coven_database::Database,
        owner_db_store_dir: coven_foundation::store_dir::StoreDir,
        recipient: &UserKeypair,
        encryption: &EncryptionService,
        signer: &UserKeypair,
    ) -> WrappedStoreKeyRef;
}

impl RefreshTestStoreOps for std::sync::Arc<TestStore> {
    async fn remove_member_with_local_state_for_test(
        &self,
        user_keypair: &UserKeypair,
        public_key_hex: &str,
        current_encryption: &EncryptionService,
        master_keys: &dyn MasterKeyCustody,
        cipher: &dyn coven_storage::CloudSyncCipherStateAccess,
        pending_rotation: &PendingRotation,
        db: &coven_database::Database,
        db_store_dir: coven_foundation::store_dir::StoreDir,
    ) -> Result<String, MembershipOpsError> {
        self.bind_device(db, db_store_dir.clone(), user_keypair)
            .await
            .map_err(MembershipOpsError::Store)?
            .remove_member(
                public_key_hex,
                current_encryption,
                master_keys,
                cipher,
                pending_rotation,
            )
            .await
    }

    async fn revoke_member_durable(
        &self,
        db: &coven_database::Database,
        db_store_dir: coven_foundation::store_dir::StoreDir,
        owner_keypair: &UserKeypair,
        revokee_pubkey: &str,
        timestamp: &str,
        current_encryption: &EncryptionService,
        pending_rotation: &PendingRotation,
    ) -> Result<EncryptionService, MembershipOpsError> {
        let device = self
            .bind_device(db, db_store_dir.clone(), owner_keypair)
            .await
            .map_err(MembershipOpsError::Store)?;
        let mut writer = device
            .authorize_writer()
            .await
            .map_err(MembershipOpsError::Store)?;
        writer
            .revoke_member_without_local_adoption_for_test(
                revokee_pubkey,
                timestamp,
                current_encryption,
                pending_rotation,
            )
            .await
    }

    async fn admit_exact_member(
        &self,
        owner_db: &coven_database::Database,
        owner_db_store_dir: coven_foundation::store_dir::StoreDir,
        owner: &UserKeypair,
        member: &UserKeypair,
        role: MemberRole,
        encryption: &EncryptionService,
    ) -> MembershipChain {
        self.admit_member(
            owner_db,
            owner_db_store_dir.clone(),
            owner,
            &pubkey_hex(member),
            None,
            role,
            encryption,
            "Refresh Test Store",
        )
        .await
        .expect("publish exact membership admission");
        let device = self
            .open_into(owner_db, owner_db_store_dir.clone())
            .await
            .expect("reload exact membership after admission");
        device
            .membership_for_test()
            .await
            .expect("read exact membership after admission")
    }

    async fn load_exact_chain(
        &self,
        db: &coven_database::Database,
        db_store_dir: coven_foundation::store_dir::StoreDir,
    ) -> MembershipChain {
        let device = self
            .open_into(db, db_store_dir.clone())
            .await
            .expect("load exact refresh membership chain");
        device
            .membership_for_test()
            .await
            .expect("read exact refresh membership chain")
    }

    async fn create_unreferenced_wrapped_key(
        &self,
        cloud_storage: &coven_storage::CloudSyncConnection,
        owner_db: &coven_database::Database,
        owner_db_store_dir: coven_foundation::store_dir::StoreDir,
        recipient: &UserKeypair,
        encryption: &EncryptionService,
        signer: &UserKeypair,
    ) -> WrappedStoreKeyRef {
        let recipient_pubkey = pubkey_hex(recipient);
        let wrapped = WrappedStoreKey::seal_keyring(
            &self.root().store_root_id.to_string(),
            &recipient_pubkey,
            &recipient.to_x25519_public_key(),
            encryption,
            signer,
        )
        .expect("seal wrapped Store key");
        let prepared = self
            .bind_device(owner_db, owner_db_store_dir.clone(), signer)
            .await
            .expect("bind wrapped-key publication Store")
            .prepare_wrapped_key(&recipient_pubkey, wrapped)
            .await
            .expect("prepare exact wrapped Store key");
        cloud_storage
            .create_protocol_object(&prepared.object)
            .await
            .expect("create exact wrapped Store key");
        prepared.reference
    }
}

struct ExactStoreFixture {
    store: Arc<TestStore>,
    cloud_storage: Arc<coven_storage::CloudSyncConnection>,
    db: coven_database::Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
}

async fn exact_store(owner: &UserKeypair, encryption: &EncryptionService) -> ExactStoreFixture {
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let (store, cloud_storage) = Box::pin(TestStore::create_encrypted_with_connection(
        &owner_db,
        owner_db_store_dir.clone(),
        LIB_ID,
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
        encryption.clone(),
    ))
    .await
    .expect("create exact refresh Store");
    Box::pin(store.open_into(&owner_db, owner_db_store_dir.clone()))
        .await
        .expect("open exact refresh Store on owner device");
    ExactStoreFixture {
        store,
        cloud_storage,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    }
}

#[derive(Clone)]
struct MembershipReadCounter {
    reads: Arc<std::sync::atomic::AtomicUsize>,
}

impl MembershipReadCounter {
    fn new() -> Self {
        Self {
            reads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl crate::sync::test_helpers::StorageInterceptor for MembershipReadCounter {
    async fn before_protocol_read(
        &self,
        read: crate::sync::test_helpers::ProtocolRead,
        semantic_prefix: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        if read != crate::sync::test_helpers::ProtocolRead::Object
            && semantic_prefix.starts_with("store-v1/membership/heads/")
        {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }
}

/// A non-rotating running device B adopts a rotated store key on its next cycle,
/// with no restart. Device A removes a member, which rotates the key and re-wraps
/// an immutable key object for B and names it in the removal authority; B's next
/// the next sync cycle reads that exact object, authenticates it, and swaps
/// its live cipher — so it can now decrypt content sealed under the new key, and
/// its keyring holds the new key for the next restart.
///
/// Mutation proof: drop the wrapped-key re-fetch from the refresh and B
/// keeps its old cipher — the post-rotation key never reaches it and the final
/// fingerprint assertion fails. (Asserted here by checking B's live cipher and
/// keyring both hold the rotated key, which only the adoption can produce.)
#[tokio::test]
async fn non_rotating_device_adopts_rotated_key_without_restart() {
    let owner = UserKeypair::generate(); // device A, the founder/owner
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate(); // the member A will remove
    let old_key: [u8; 32] = [11u8; 32];

    let encryption = EncryptionService::from_key(old_key);
    let ExactStoreFixture {
        store: storage,
        cloud_storage: _,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&owner, &encryption).await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &device_b,
            MemberRole::Member,
            &encryption,
        )
        .await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &victim,
            MemberRole::Member,
            &encryption,
        )
        .await;

    // B's local state: pinned owner + its keyring holds the OLD key + its live
    // cipher is the OLD key. This is the just-joined steady state.
    let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_b = crate::sync::test_helpers::open_test_db(db_b_store_dir.clone());
    let running_b = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &db_b,
            db_b_store_dir.clone(),
            &device_b,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);

    // Sanity: before the rotation, B's refresh is a no-op — it already holds the
    // current key, so the cycle leaves the cipher unchanged.
    running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await
        .expect("pre-rotation cycle");
    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B has encrypted storage")
            .seal_key(),
        old_key,
        "before any rotation B keeps the key it joined with",
    );

    // Device A removes the victim, rotates the key, and activates B's new exact wrap.
    let new_key = storage
        .revoke_member_durable(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &pubkey_hex(&victim),
            "0000000004000-0000-A",
            &encryption,
            &PendingRotation::none(),
        )
        .await
        .expect("revoke rotates the key");
    assert_ne!(
        new_key.key_bytes(),
        old_key,
        "removal rotates to a fresh key"
    );

    // --- B's NEXT cycle, no restart: it must adopt the rotated key. ---
    running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await
        .expect("post-rotation cycle");

    // B's live cipher now holds the rotated key (it can decrypt what A seals under
    // it this cycle), and its keyring was updated so a restart reads the new key —
    // the two halves `apply_key_rotation` performs.
    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B retains encrypted storage")
            .seal_key(),
        new_key.key_bytes(),
        "B adopted the rotated key into its live cipher without a restart",
    );
    assert_eq!(
        ks_b.stored_key().as_deref(),
        Some(new_key.to_keyring_string().unwrap().as_str()),
        "B persisted the rotated key to its keyring, so its restart reads the current key",
    );

    // The chain supplies the exact refs; no path search chooses the key.
    let (reunwrapped, _) = storage
        .bind_device_in(&db_b, db_b_store_dir.clone(), &device_b)
        .await
        .expect("bind refreshed member Store")
        .membership_keyring_facts()
        .await
        .expect("B can unwrap its re-wrapped key");
    assert_eq!(reunwrapped, new_key.key_bytes());
}

#[tokio::test]
async fn admission_after_rotation_uses_the_membership_selected_keyring() {
    let owner = UserKeypair::generate();
    let removed_member = UserKeypair::generate();
    let admitted_member = UserKeypair::generate();
    let initial = EncryptionService::from_key([52u8; 32]);
    let ExactStoreFixture {
        store: storage,
        cloud_storage,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&owner, &initial).await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &removed_member,
            MemberRole::Member,
            &initial,
        )
        .await;
    let custody = TestCustody::default();
    custody.set_initial_key(initial.key_bytes());
    let cipher = RwLock::new(CloudCipher::Encrypted(initial.clone()));
    let pending_rotation = PendingRotation::none();
    let rotated = storage
        .revoke_member_durable(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &pubkey_hex(&removed_member),
            "0000000004000-0000-owner",
            &initial,
            &pending_rotation,
        )
        .await
        .expect("remove member and rotate the Store key");
    cipher
        .adopt_key_rotation(&rotated, &custody)
        .expect("owner adopts the activated rotation");
    storage
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
        .await
        .expect("bind rotation owner")
        .complete_revoke_rotation_adoption_for_test(&pending_rotation, rotated.current_generation())
        .await
        .expect("owner completes the activated removal journal");

    let admission = storage
        .admit_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &pubkey_hex(&admitted_member),
            None,
            MemberRole::Member,
            &initial,
            "Refresh Test Store",
        )
        .await
        .expect("publish post-rotation admission");
    let mut history = crate::sync::store::HistoryConstructionAuthority::admission()
        .open_pinned(&*cloud_storage, &admission.store_root)
        .await
        .expect("open admission history");
    let chain = history
        .load_exact_anchored_membership(
            &admission.membership_floor.0,
            Some(&admission.owner_pubkey),
        )
        .await
        .expect("load admission membership");
    let admitted_keyring =
        crate::sync::store::StoreKeyrings::new(&*cloud_storage, admission.store_root.clone())
            .open_containing(&admitted_member, &chain, &admission.wrapped_key)
            .await
            .expect("admitted member opens the activated exact wrap");
    let sealed = rotated.seal_app_data(b"current Store data", b"post-rotation admission");
    assert_eq!(
        admitted_keyring
            .open_app_data(&sealed, b"post-rotation admission")
            .expect("admit retains the current Store key"),
        b"current Store data",
    );
}

/// Creating a wrapped key cannot make it active. Until membership authority
/// names its exact reference, refresh ignores it and continues with the
/// authorized keyring.
#[tokio::test]
async fn unreferenced_wrapped_key_does_not_change_or_pause_the_cycle() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let old_key: [u8; 32] = [31u8; 32];
    let rotated_key: [u8; 32] = [32u8; 32];

    // The cloud is the owner's, so a changeset it serves is authored by a member
    // the pull will authorize against the chain.
    let encryption = EncryptionService::from_key(old_key);
    let ExactStoreFixture {
        store: storage,
        cloud_storage,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&owner, &encryption).await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &device_b,
            MemberRole::Member,
            &encryption,
        )
        .await;
    let chain = storage
        .load_exact_chain(&owner_db, owner_db_store_dir.clone())
        .await;
    let pending_keyring = EncryptionService::from_key(old_key)
        .with_appended_generation(2, rotated_key)
        .unwrap();
    let unreferenced = storage
        .create_unreferenced_wrapped_key(
            &cloud_storage,
            &owner_db,
            owner_db_store_dir.clone(),
            &device_b,
            &pending_keyring,
            &owner,
        )
        .await;
    assert!(
        !chain
            .active_wrapped_keys_for(&pubkey_hex(&device_b))
            .contains(&unreferenced),
        "creating an exact object does not activate it",
    );

    let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_b = crate::sync::test_helpers::open_test_db(db_b_store_dir.clone());
    let running_b = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &db_b,
            db_b_store_dir.clone(),
            &device_b,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");

    // A peer changeset waiting to be pulled, so the cycle proving it "completes"
    // also proves the pull ran and applied it while sealing was paused.
    let peer_src_store_dir = crate::sync::test_helpers::test_store_dir();
    let peer_src = crate::sync::test_helpers::open_test_db(peer_src_store_dir.clone());
    let peer_cs = peer_src
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('peer1', 'FromOwner', NULL, 1, '0000000005000-0000-A', '2026-01-01')",
        ])
        .await;
    storage
        .publish_changeset("owner-device", 4, &peer_cs, db_b.schema_version())
        .await
        .expect("publish exact owner changeset");

    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let result = running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await
        .expect("an unreferenced wrapped key does not affect the cycle");

    assert_eq!(
        result.changesets_applied, 1,
        "the pull still applies a peer's changeset",
    );
    assert_eq!(result.rotation_pending, None);
    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B retains encrypted storage")
            .seal_key(),
        old_key,
        "an unreferenced key must not replace the live cipher",
    );
    load_wrapped_store_key(
        &*cloud_storage,
        storage.root().store_root_hash,
        &unreferenced,
    )
    .await
    .expect("the ignored exact object remains readable by its exact reference");
}

#[tokio::test]
async fn replayed_pre_rotation_wrapped_key_is_not_adopted() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate();
    let old_key: [u8; 32] = [12u8; 32];

    let encryption = EncryptionService::from_key(old_key);
    let ExactStoreFixture {
        store: storage,
        cloud_storage,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&owner, &encryption).await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &device_b,
            MemberRole::Member,
            &encryption,
        )
        .await;
    let chain = storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &victim,
            MemberRole::Member,
            &encryption,
        )
        .await;
    let old_reference = chain
        .active_wrapped_keys_for(&pubkey_hex(&device_b))
        .into_iter()
        .next()
        .expect("admit activates the initial exact wrapped key");

    let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_b = crate::sync::test_helpers::open_test_db(db_b_store_dir.clone());
    let running_b = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &db_b,
            db_b_store_dir.clone(),
            &device_b,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let new_key = storage
        .revoke_member_durable(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &pubkey_hex(&victim),
            "0000000004000-0000-A",
            &encryption,
            &PendingRotation::none(),
        )
        .await
        .expect("revoke rotates key");

    running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await
        .expect("adopt generation 2");
    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B adopted an encrypted keyring")
            .current_generation(),
        new_key.current_generation()
    );

    load_wrapped_store_key(
        &*cloud_storage,
        storage.root().store_root_hash,
        &old_reference,
    )
    .await
    .expect("the retained pre-rotation object remains readable");

    running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await
        .expect("replayed old wrapped key is ignored");

    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B retains encrypted storage")
            .seal_key(),
        new_key.key_bytes(),
        "a replayed older wrapped key must not roll the device back",
    );
    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B retains encrypted storage")
            .current_generation(),
        new_key.current_generation(),
        "the accepted generation floor remains at the rotated generation",
    );
}

#[tokio::test]
async fn readmitting_member_supersedes_old_wrap_and_merges_same_generation_key() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let current_key: [u8; 32] = [13u8; 32];
    let replacement_key: [u8; 32] = [14u8; 32];

    let current = EncryptionService::from_key(current_key);
    let replacement = EncryptionService::from_key(replacement_key);
    let expected = current
        .merged_with(&replacement)
        .expect("same-generation keyrings merge");
    let ExactStoreFixture {
        store: storage,
        cloud_storage: _,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&owner, &current).await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &device_b,
            MemberRole::Member,
            &current,
        )
        .await;
    let chain = storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &device_b,
            MemberRole::Member,
            &replacement,
        )
        .await;
    assert_eq!(
        chain.active_wrapped_keys_for(&pubkey_hex(&device_b)).len(),
        1,
        "the replacement grant is the sole authority for its wrapped key",
    );

    let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_b = crate::sync::test_helpers::open_test_db(db_b_store_dir.clone());
    let running_b = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &db_b,
            db_b_store_dir.clone(),
            &device_b,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(current_key);
    running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await
        .expect("replacement same-generation wrapped key is merged");

    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B retains encrypted storage")
            .seal_key(),
        expected.key_bytes(),
        "same-generation forks select the greatest fingerprint independent of arrival order",
    );
    let persisted = ks_b
        .stored_key()
        .expect("merged same-generation keyring persisted");
    let persisted = coven_keys::encryption::MasterKeyring::from_serialized(&persisted)
        .expect("parse persisted merged keyring");
    let persisted: EncryptionService = persisted.into();
    assert_eq!(
        persisted.key_count(),
        2,
        "both same-generation keys are retained"
    );
}

#[tokio::test]
async fn second_owner_rotation_is_adoptable_by_existing_members() {
    let founder = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate();
    let old_key: [u8; 32] = [15u8; 32];

    let encryption = EncryptionService::from_key(old_key);
    let ExactStoreFixture {
        store: storage,
        cloud_storage: _,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&founder, &encryption).await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &founder,
            &second_owner,
            MemberRole::Member,
            &encryption,
        )
        .await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &founder,
            &device_b,
            MemberRole::Member,
            &encryption,
        )
        .await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &founder,
            &victim,
            MemberRole::Member,
            &encryption,
        )
        .await;
    let second_owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let second_owner_db =
        crate::sync::test_helpers::open_test_db(second_owner_db_store_dir.clone());
    let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_b = crate::sync::test_helpers::open_test_db(db_b_store_dir.clone());
    storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &second_owner,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    let running_b = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &db_b,
            db_b_store_dir.clone(),
            &device_b,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    storage
        .promote_active_member_fixture(
            &owner_db,
            owner_db_store_dir.clone(),
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &founder,
            &second_owner,
            &encryption,
        )
        .await
        .expect("promote active second Owner");
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let new_key = Box::pin(storage.revoke_member_durable(
        &second_owner_db,
        second_owner_db_store_dir.clone(),
        &second_owner,
        &pubkey_hex(&victim),
        "0000000005000-0000-B",
        &encryption,
        &PendingRotation::none(),
    ))
    .await
    .expect("second owner can revoke");

    Box::pin(running_b.run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None))
        .await
        .expect("existing member adopts a current owner's rotation");

    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B retains encrypted storage")
            .seal_key(),
        new_key.key_bytes()
    );
}

#[tokio::test]
async fn rotation_after_concurrent_rotations_retains_every_authorized_key() {
    let founder = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let remaining_member = UserKeypair::generate();
    let first_victim = UserKeypair::generate();
    let second_victim = UserKeypair::generate();
    let initial = EncryptionService::from_key([41u8; 32]);

    let ExactStoreFixture {
        store: storage,
        cloud_storage: _,
        db: founder_db,
        db_store_dir: founder_db_store_dir,
    } = exact_store(&founder, &initial).await;
    storage
        .admit_exact_member(
            &founder_db,
            founder_db_store_dir.clone(),
            &founder,
            &second_owner,
            MemberRole::Member,
            &initial,
        )
        .await;
    let second_owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let second_owner_db =
        crate::sync::test_helpers::open_test_db(second_owner_db_store_dir.clone());
    storage
        .activate_joined_device(
            &founder_db,
            founder_db_store_dir.clone(),
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &second_owner,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    storage
        .promote_active_member_fixture(
            &founder_db,
            founder_db_store_dir.clone(),
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &founder,
            &second_owner,
            &initial,
        )
        .await
        .expect("promote active second Owner");
    storage
        .admit_exact_member(
            &founder_db,
            founder_db_store_dir.clone(),
            &founder,
            &remaining_member,
            MemberRole::Member,
            &initial,
        )
        .await;
    storage
        .admit_exact_member(
            &founder_db,
            founder_db_store_dir.clone(),
            &founder,
            &first_victim,
            MemberRole::Member,
            &initial,
        )
        .await;
    storage
        .admit_exact_member(
            &founder_db,
            founder_db_store_dir.clone(),
            &founder,
            &second_victim,
            MemberRole::Member,
            &initial,
        )
        .await;

    let founder_device = storage
        .bind_device_in(&founder_db, founder_db_store_dir.clone(), &founder)
        .await
        .expect("bind founder before either concurrent rotation");
    let second_owner_device = storage
        .bind_device_in(
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &second_owner,
        )
        .await
        .expect("bind second Owner before either concurrent rotation");
    let mut founder_writer = founder_device
        .authorize_writer()
        .await
        .expect("authorize founder at the shared membership cut");
    let mut second_owner_writer = second_owner_device
        .authorize_writer()
        .await
        .expect("authorize second Owner at the shared membership cut");
    let founder_pending_rotation = PendingRotation::none();
    let founder_rotation = founder_writer
        .revoke_member_without_local_adoption_for_test(
            &pubkey_hex(&first_victim),
            "0000000005000-0000-founder",
            &initial,
            &founder_pending_rotation,
        )
        .await
        .expect("founder publishes one rotation fork");
    second_owner_writer
        .revoke_member_without_local_adoption_for_test(
            &pubkey_hex(&second_victim),
            "0000000005000-0000-second-owner",
            &initial,
            &PendingRotation::none(),
        )
        .await
        .expect("second owner publishes the concurrent rotation fork");

    let founder_custody = TestCustody::default();
    founder_custody.set_initial_key(initial.key_bytes());
    let founder_cipher = RwLock::new(CloudCipher::Encrypted(initial.clone()));
    founder_cipher
        .adopt_key_rotation(&founder_rotation, &founder_custody)
        .expect("founder adopts its activated rotation fork");
    founder_device
        .complete_revoke_rotation_adoption_for_test(
            &founder_pending_rotation,
            founder_rotation.current_generation(),
        )
        .await
        .expect("founder completes its activated removal journal");

    let (_, authority_key_count) = storage
        .bind_device_in(&founder_db, founder_db_store_dir.clone(), &founder)
        .await
        .expect("bind founder Store keyring")
        .membership_keyring_facts()
        .await
        .expect("founder unwraps both concurrent rotation forks");
    assert_eq!(authority_key_count, 3);

    let next_rotation = Box::pin(storage.revoke_member_durable(
        &founder_db,
        founder_db_store_dir.clone(),
        &founder,
        &pubkey_hex(&remaining_member),
        "0000000006000-0000-founder",
        &founder_rotation,
        &founder_pending_rotation,
    ))
    .await
    .expect("founder rotates after observing both forks");

    assert_eq!(
        next_rotation.key_count(),
        authority_key_count + 1,
        "a later rotation extends the membership-selected keyring rather than one device's fork",
    );
}

#[tokio::test]
async fn removed_owner_key_is_not_adopted() {
    let founder = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let founder_pk = pubkey_hex(&founder);
    let current_key: [u8; 32] = [16u8; 32];

    let encryption = EncryptionService::from_key(current_key);
    let ExactStoreFixture {
        store: storage,
        cloud_storage: _,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&founder, &encryption).await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &founder,
            &second_owner,
            MemberRole::Member,
            &encryption,
        )
        .await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &founder,
            &device_b,
            MemberRole::Member,
            &encryption,
        )
        .await;
    let second_owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let second_owner_db =
        crate::sync::test_helpers::open_test_db(second_owner_db_store_dir.clone());
    let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_b = crate::sync::test_helpers::open_test_db(db_b_store_dir.clone());
    storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &second_owner,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    let running_b = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &db_b,
            db_b_store_dir.clone(),
            &device_b,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    storage
        .promote_active_member_fixture(
            &owner_db,
            owner_db_store_dir.clone(),
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &founder,
            &second_owner,
            &encryption,
        )
        .await
        .expect("promote active second Owner");
    let rotated = storage
        .revoke_member_durable(
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &second_owner,
            &founder_pk,
            "0000000004000-0000-B",
            &encryption,
            &PendingRotation::none(),
        )
        .await
        .expect("second owner removes founder through exact membership graph");

    let ks_b = TestCustody::default();
    ks_b.set_initial_key(rotated.key_bytes());
    running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await
        .expect("refresh ignores refs authored by a removed owner");
    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B retains encrypted storage")
            .seal_key(),
        rotated.key_bytes(),
        "a removed owner's retained historical wrap is not active",
    );
}

/// A valid exact object signed by an attacker has no authority. Refresh does not
/// discover objects by listing paths, so the object is ignored unless a valid
/// membership entry names its exact reference.
#[tokio::test]
async fn refresh_ignores_an_unreferenced_attacker_wrapped_key() {
    let owner = UserKeypair::generate();
    let attacker = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let real_key: [u8; 32] = [22u8; 32];

    let encryption = EncryptionService::from_key(real_key);
    let ExactStoreFixture {
        store: storage,
        cloud_storage,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&owner, &encryption).await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &device_b,
            MemberRole::Member,
            &encryption,
        )
        .await;

    let forged_key: [u8; 32] = [0xCDu8; 32];
    let forged = storage
        .create_unreferenced_wrapped_key(
            &cloud_storage,
            &owner_db,
            owner_db_store_dir.clone(),
            &device_b,
            &EncryptionService::from_key(forged_key),
            &attacker,
        )
        .await;

    // B holds its real key (live + keyring) and pins the owner.
    let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_b = crate::sync::test_helpers::open_test_db(db_b_store_dir.clone());
    let running_b = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &db_b,
            db_b_store_dir.clone(),
            &device_b,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(real_key);
    running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await
        .expect("an unreferenced attacker object does not affect refresh");

    // Critically, B did NOT swap its cipher to the attacker's key.
    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B retains encrypted storage")
            .seal_key(),
        real_key,
        "B keeps its real key; the unauthenticated forged key is never adopted",
    );
    assert_ne!(
        running_b
            .current_keyring_for_test()
            .expect("B retains encrypted storage")
            .seal_key(),
        forged_key,
        "the attacker's key was rejected",
    );
    load_wrapped_store_key(&*cloud_storage, storage.root().store_root_hash, &forged)
        .await
        .expect("the ignored attacker object exists at its exact reference");
}

/// For an owner-pinned store, a chain the refresh can't load must abort the
/// cycle, never fall open to "no chain, act anyway". A required exact membership
/// object that cannot be read aborts B's cycle rather than letting it push or
/// judge under no authorization.
#[tokio::test]
async fn refresh_fails_closed_when_the_chain_cannot_be_loaded() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let key: [u8; 32] = [44u8; 32];

    let encryption = EncryptionService::from_key(key);
    let ExactStoreFixture {
        store: storage,
        cloud_storage,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&owner, &encryption).await;
    let chain = storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &device_b,
            MemberRole::Member,
            &encryption,
        )
        .await;

    let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_b = crate::sync::test_helpers::open_test_db(db_b_store_dir.clone());
    let running_b = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &db_b,
            db_b_store_dir.clone(),
            &device_b,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    let owner_device = storage
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
        .await
        .expect("bind exact membership owner");
    let head = owner_device
        .load_membership_head_for_test(
            chain
                .head_refs()
                .first()
                .expect("exact membership chain has an active head"),
        )
        .await
        .expect("load exact active membership head");
    cloud_storage
        .delete_protocol_object(&head.body.entry.object)
        .await
        .expect("remove exact membership entry before refresh");
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(key);
    let result = running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await;
    assert!(
        result.is_err(),
        "an exact membership-object failure under a pinned owner must abort the cycle, not fall open: {result:?}",
    );
}

/// One sync cycle traverses each exact membership stream once. The chain is loaded
/// and anchored at the top of the cycle. Pull advances that chain from membership
/// controls it has already verified and materialized, then threads the result to
/// post-pull authorization sites without another storage traversal. The storage
/// wrapper counts exact membership-head slot reads.
///
/// Mutation proof: route any of those sites through its own membership load and
/// the count rises above the exact traversal count.
#[tokio::test]
async fn one_cycle_loads_exact_membership_once() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let key: [u8; 32] = [7u8; 32];

    let encryption = EncryptionService::from_key(key);
    let ExactStoreFixture {
        store: storage,
        cloud_storage: _,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&owner, &encryption).await;
    let chain = storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &device_b,
            MemberRole::Member,
            &encryption,
        )
        .await;

    // B's steady state: owner pinned + an encrypted cipher on the shared key, so the
    // cycle's refresh and pull both run against the anchored chain rather than
    // short-circuiting as a plaintext no-op. B is a Member, so it authors no
    // snapshot — whose reclaim would be a separate, out-of-scope membership read.
    let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_b = crate::sync::test_helpers::open_test_db(db_b_store_dir.clone());
    let running_b = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &db_b,
            db_b_store_dir.clone(),
            &device_b,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(key);
    let stream_count = chain
        .entries()
        .iter()
        .map(|entry| {
            (
                entry.author_pubkey.clone(),
                entry.author_owner_grant.clone(),
                entry.stream_id,
            )
        })
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let counter = MembershipReadCounter::new();
    running_b
        .run_cycle_with_interceptor(
            &SystemClock,
            Some(Arc::new(ks_b.clone())),
            None,
            counter.clone(),
        )
        .await
        .expect("B's cycle");
    assert_eq!(
        counter.reads(),
        chain.entries().len() + stream_count,
        "one cycle reads every exact membership head and each stream's terminal empty slot once",
    );
}

fn cipher_generation(cipher: &RwLock<CloudCipher>) -> u64 {
    match &*cipher.read().unwrap() {
        CloudCipher::Encrypted(enc) => enc.current_generation(),
        CloudCipher::Plaintext => panic!("expected an encrypted cipher"),
    }
}

/// Removing a member commits the cloud key rotation before this device adopts the
/// rotated key locally. When the local keyring write fails, the removal is durable
/// — the member is out and the store is rotated for everyone — but this device's
/// live cipher stays on the superseded generation. That half-applied state is its
/// own typed error, marks this device's rotation-pending gate (so it seals
/// nothing new for the cloud in the meantime — see `rotation_pending_tests`), and
/// both remedies it names converge without losing the rotation: the device's next
/// sync cycle adopts the key from its own `keys/{self}` wrap, and retrying the
/// removal re-derives and re-adopts it.
#[tokio::test]
async fn removal_rotation_stays_resumable_when_local_adoption_fails() {
    let owner = UserKeypair::generate(); // this device — performs the removal
    let member = UserKeypair::generate(); // the member being removed
    let old_key: [u8; 32] = [11u8; 32];

    let encryption = EncryptionService::from_key(old_key);
    let ExactStoreFixture {
        store: storage,
        cloud_storage: _,
        db,
        db_store_dir,
    } = exact_store(&owner, &encryption).await;
    storage
        .admit_exact_member(
            &db,
            db_store_dir.clone(),
            &owner,
            &member,
            MemberRole::Member,
            &encryption,
        )
        .await;

    // This device's steady state: keyring and live cipher hold the pre-rotation key.
    let ks = TestCustody::default();
    ks.set_initial_key(old_key);
    let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let pending_rotation = PendingRotation::none();

    // The keyring is momentarily unwritable, so local adoption fails after the
    // cloud rotation commits.
    ks.fail_writes();
    let err = Box::pin(
        RefreshTestStoreOps::remove_member_with_local_state_for_test(
            &storage,
            &owner,
            &pubkey_hex(&member),
            &encryption,
            &ks,
            &cipher,
            &pending_rotation,
            &db,
            db_store_dir.clone(),
        ),
    )
    .await
    .expect_err("local adoption fails while the keyring is unwritable");
    assert!(
        matches!(
            err,
            MembershipOpsError::RotationCommittedAdoptionFailed { .. }
        ),
        "the failure is the rotation-committed/adoption-failed variant, got {err:?}",
    );

    // The cloud rotation committed: the member is durably removed from the
    // committed chain even though this device could not adopt the new key.
    let committed = storage.load_exact_chain(&db, db_store_dir.clone()).await;
    assert!(
        !committed
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&member)),
        "the member is durably removed despite the failed local adoption",
    );

    // The failed adoption left this device's live cipher and keyring on the
    // pre-rotation generation — no half-applied swap.
    assert_eq!(
        cipher_generation(&cipher),
        1,
        "the failed adoption did not swap the live cipher",
    );
    assert_eq!(
        ks.stored_key(),
        Some(
            coven_keys::encryption::MasterKeyring::from(EncryptionService::from_key(old_key))
                .to_serialized(),
        ),
        "the failed adoption did not persist a new key",
    );

    // The failed adoption leaves the exact durable gate armed, so every seal
    // through this storage refuses until the removal operation completes.
    assert_eq!(
        pending_rotation.pending_generation(),
        Some(2),
        "the failed adoption marks the committed generation as pending",
    );

    // Authorization refresh may adopt the selected key bytes, but it leaves the
    // local removal journal and its gate for the exact removal retry to complete.
    {
        let ks_refresh = TestCustody::default();
        ks_refresh.set_initial_key(old_key);
        let running_owner = storage
            .bind_device(&db, db_store_dir.clone(), &owner)
            .await
            .expect("bind removal owner to its retained sync storage");
        Box::pin(running_owner.run_cycle_with(
            &SystemClock,
            Some(Arc::new(ks_refresh.clone())),
            None,
        ))
        .await
        .expect("refresh cycle");
        assert_eq!(
            running_owner
                .current_keyring_for_test()
                .expect("owner retains encrypted storage")
                .current_generation(),
            2,
            "authorization refresh adopts the rotated key",
        );
        assert_eq!(
            running_owner.pending_rotation_generation_for_test(),
            Some(2),
            "authorization refresh retains the local removal gate",
        );
    }

    // Retrying the exact removal closes its journal and durable gate now that
    // custody is writable.
    ks.allow_writes();
    let fingerprint = Box::pin(
        RefreshTestStoreOps::remove_member_with_local_state_for_test(
            &storage,
            &owner,
            &pubkey_hex(&member),
            &encryption,
            &ks,
            &cipher,
            &pending_rotation,
            &db,
            db_store_dir.clone(),
        ),
    )
    .await
    .expect("retrying the removal converges");
    assert!(
        !fingerprint.is_empty(),
        "the retry returns the key fingerprint"
    );
    assert_eq!(
        cipher_generation(&cipher),
        2,
        "retrying the removal adopts the rotated key",
    );
    assert_eq!(
        pending_rotation.pending_generation(),
        None,
        "the retried removal clears this device's own rotation-pending gate",
    );
}
