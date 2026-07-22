//! Per-cycle authorization/decryption refresh.
//!
//! `run_single_sync_cycle` re-reads membership and the rotatable store key at
//! the top of every cycle, so a running device picks up a membership change or a
//! key rotation made by another device without a restart. Loaded only once at
//! init/join, a removed member would keep acting on stale authorization and
//! a non-rotating device would keep using a dead store key after a rotation it
//! didn't perform, silently diverging.
//!
//! Each test proves fail-before/pass-after: the assertion that passes with the
//! refresh in place fails when the corresponding refresh step is dropped (the
//! "mutation" each test documents).

use std::sync::RwLock;

use crate::clock::SystemClock;
use crate::encryption::EncryptionService;
use crate::keys::{MasterKeyCustody, UserKeypair};
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{CloudCipher, PendingRotation};
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::membership::{MemberRole, MembershipChain};
use crate::sync::storage::SyncStorage;
use crate::sync::store::membership::{
    apply_key_rotation, invite_member, remove_member, MembershipOpsError,
};
use crate::sync::store::membership::{
    revoke_member_durable, signed_wrapped_keyring_for_test, unwrap_store_keyring_for_refs,
};
use crate::sync::test_helpers::{
    capture_bytes, open_test_db, pubkey_hex, temp_store_dir, TestCustody, TestStore,
};
use crate::sync::wrapped_store_key::{
    load_wrapped_store_key, prepare_wrapped_store_key, WrappedStoreKeyRef,
};

const LIB_ID: &str = "lib-refresh-test";

async fn exact_store(owner: &UserKeypair) -> (TestStore, crate::database::Database) {
    let owner_db = open_test_db();
    let storage = Box::pin(TestStore::create(&owner_db, LIB_ID, owner.clone()))
        .await
        .expect("create exact refresh Store");
    Box::pin(storage.open_into(&owner_db))
        .await
        .expect("open exact refresh Store on owner device");
    (storage, owner_db)
}

async fn invite_exact_member(
    storage: &TestStore,
    owner_db: &crate::database::Database,
    owner: &UserKeypair,
    member: &UserKeypair,
    role: MemberRole,
    encryption: &EncryptionService,
) -> MembershipChain {
    invite_member(
        &storage.storage,
        storage.home.as_ref(),
        owner,
        &Hlc::new("refresh-owner".to_string()),
        &pubkey_hex(member),
        None,
        role,
        encryption,
        LIB_ID,
        "Refresh Test Store",
        owner_db,
    )
    .await
    .expect("publish exact membership invitation");
    storage
        .open_into(owner_db)
        .await
        .expect("reload exact membership after invitation")
}

async fn activate_joined_device(
    storage: &TestStore,
    owner_db: &crate::database::Database,
    joining_db: &crate::database::Database,
    joining_identity: &UserKeypair,
) {
    crate::sync::test_helpers::install_active_device_fixture(
        storage,
        owner_db,
        joining_db,
        joining_identity,
        "0000000001000-0000-refresh",
    )
    .await
    .expect("install active exact device fixture");
}

async fn load_exact_chain(storage: &TestStore, db: &crate::database::Database) -> MembershipChain {
    storage
        .open_into(db)
        .await
        .expect("load exact refresh membership chain")
}

async fn create_unreferenced_wrapped_key(
    storage: &TestStore,
    recipient: &UserKeypair,
    encryption: &EncryptionService,
    signer: &UserKeypair,
) -> WrappedStoreKeyRef {
    let recipient_pubkey = pubkey_hex(recipient);
    let wrapped = signed_wrapped_keyring_for_test(
        &storage.root.store_root_id.to_string(),
        &recipient_pubkey,
        &recipient.to_x25519_public_key(),
        encryption,
        signer,
    );
    let prepared = prepare_wrapped_store_key(
        &storage.storage,
        storage.root.store_root_hash,
        &recipient_pubkey,
        wrapped,
    )
    .await
    .expect("prepare exact wrapped Store key");
    storage
        .storage
        .create_protocol_object(&prepared.object)
        .await
        .expect("create exact wrapped Store key");
    prepared.reference
}

/// Run one real sync cycle for `device_id` over the test Store's encrypted home,
/// using the same backing storage for protocol objects and wrapped-key reads.
async fn run_cycle(
    storage: &TestStore,
    db: &crate::database::Database,
    cipher: &RwLock<CloudCipher>,
    pending_rotation: &PendingRotation,
    keypair: &UserKeypair,
    device_id: &str,
    ld: &StoreDir,
    custody: Option<&dyn MasterKeyCustody>,
) -> Result<super::cycle::SyncCycleResult, String> {
    run_cycle_with_storage(
        &storage.storage,
        storage,
        db,
        cipher,
        pending_rotation,
        keypair,
        device_id,
        ld,
        custody,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_cycle_with_storage(
    sync_storage: &dyn SyncStorage,
    storage: &TestStore,
    db: &crate::database::Database,
    cipher: &RwLock<CloudCipher>,
    pending_rotation: &PendingRotation,
    keypair: &UserKeypair,
    device_id: &str,
    ld: &StoreDir,
    custody: Option<&dyn MasterKeyCustody>,
) -> Result<super::cycle::SyncCycleResult, String> {
    let exact_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "refresh device has no exact Store registration".to_string())?;
    let hlc = Hlc::new(device_id.to_string());
    run_single_sync_cycle(
        sync_storage,
        &exact_device_id,
        &hlc,
        &SystemClock,
        db,
        cipher,
        pending_rotation,
        keypair,
        custody,
        ld,
        Some(storage.home.as_ref()),
        None,
    )
    .await
    .map_err(|error| error.to_string())
}

struct MembershipReadCounter<'a> {
    inner: &'a crate::sync::cloud_storage::CloudSyncStorage,
    reads: std::sync::atomic::AtomicUsize,
}

impl<'a> MembershipReadCounter<'a> {
    fn new(inner: &'a crate::sync::cloud_storage::CloudSyncStorage) -> Self {
        Self {
            inner,
            reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl SyncStorage for MembershipReadCounter<'_> {
    fn store_blob_protection(
        &self,
    ) -> Result<crate::sync::storage::BlobSpoolProtection, crate::sync::storage::StorageError> {
        self.inner.store_blob_protection()
    }

    async fn provider_binding(
        &self,
    ) -> Result<crate::sync::storage::ResolvedProviderBinding, crate::sync::storage::StorageError>
    {
        self.inner.provider_binding().await
    }

    async fn allocate_protocol_slot(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<crate::storage::cloud::ObjectSlot, crate::sync::storage::StorageError> {
        self.inner
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
    }

    fn prepare_protocol_object(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        slot: crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<crate::sync::storage::PreparedExactObject, crate::sync::storage::StorageError> {
        self.inner
            .prepare_protocol_object(context, slot, semantic_prefix, data)
    }

    async fn create_protocol_object(
        &self,
        prepared: &crate::sync::storage::PreparedExactObject,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner.create_protocol_object(prepared).await
    }

    async fn read_protocol_object(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        object: &crate::sync::storage::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, crate::sync::storage::StorageError> {
        self.inner
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    async fn read_protocol_slot(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, crate::sync::storage::ExactObjectRef), crate::sync::storage::StorageError>
    {
        if semantic_prefix.starts_with("store-v1/membership/heads/") {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        (Vec<u8>, crate::sync::storage::PreparedExactObject),
        crate::sync::storage::StorageError,
    > {
        if semantic_prefix.starts_with("store-v1/membership/heads/") {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn delete_protocol_object(
        &self,
        object: &crate::sync::storage::ExactObjectRef,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner.delete_protocol_object(object).await
    }

    async fn allocate_blob_slot(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
    ) -> Result<crate::storage::cloud::ObjectSlot, crate::sync::storage::StorageError> {
        self.inner.allocate_blob_slot(locator, authority).await
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        protection: crate::sync::storage::BlobSpoolProtection,
        plaintext_file: &std::path::Path,
        spool_file: &std::path::Path,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner
            .seal_blob_to_spool(locator, authority, protection, plaintext_file, spool_file)
            .await
    }

    async fn prepare_blob_object(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        slot: crate::storage::cloud::ObjectSlot,
        stored_file: &std::path::Path,
    ) -> Result<crate::blob::locator::StoredBlobRef, crate::sync::storage::StorageError> {
        self.inner
            .prepare_blob_object(locator, authority, slot, stored_file)
            .await
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        stored_file: &std::path::Path,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner
            .create_blob_object_from_file(blob, authority, stored_file, progress)
            .await
    }

    async fn verify_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner.verify_blob_object(blob).await
    }

    async fn stage_exact_blob_download(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        dest: &std::path::Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, crate::sync::storage::StorageError> {
        self.inner.stage_exact_blob_download(blob, dest).await
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        protection: crate::sync::storage::BlobSpoolProtection,
        dest: &std::path::Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, crate::sync::storage::StorageError> {
        self.inner
            .stage_verified_blob_plaintext(blob, protection, dest)
            .await
    }

    async fn delete_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner.delete_blob_object(blob).await
    }
}

/// A non-rotating running device B adopts a rotated store key on its next cycle,
/// with no restart. Device A removes a member, which rotates the key and re-wraps
/// an immutable key object for B and names it in the removal authority; B's next
/// `run_single_sync_cycle` reads that exact object, authenticates it, and swaps
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
    let (storage, owner_db) = exact_store(&owner).await;
    invite_exact_member(
        &storage,
        &owner_db,
        &owner,
        &device_b,
        MemberRole::Member,
        &encryption,
    )
    .await;
    let mut chain = invite_exact_member(
        &storage,
        &owner_db,
        &owner,
        &victim,
        MemberRole::Member,
        &encryption,
    )
    .await;

    // B's local state: pinned owner + its keyring holds the OLD key + its live
    // cipher is the OLD key. This is the just-joined steady state.
    let db_b = open_test_db();
    let (_tmp_b, ld_b) = temp_store_dir();
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));

    // Sanity: before the rotation, B's refresh is a no-op — it already holds the
    // current key, so the cycle leaves the cipher unchanged.
    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("pre-rotation cycle");
    assert_eq!(
        cipher_key(&cipher_b),
        old_key,
        "before any rotation B keeps the key it joined with",
    );

    // Device A removes the victim, rotates the key, and activates B's new exact wrap.
    let new_key = revoke_member_durable(
        &storage.storage,
        storage.home.as_ref(),
        storage.root.store_root_hash,
        &mut chain,
        &owner,
        &pubkey_hex(&victim),
        &storage.root.store_root_id.to_string(),
        "0000000004000-0000-A",
        &encryption,
        &PendingRotation::none(),
        &owner_db,
    )
    .await
    .expect("revoke rotates the key");
    assert_ne!(
        new_key.key_bytes(),
        old_key,
        "removal rotates to a fresh key"
    );

    // --- B's NEXT cycle, no restart: it must adopt the rotated key. ---
    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("post-rotation cycle");

    // B's live cipher now holds the rotated key (it can decrypt what A seals under
    // it this cycle), and its keyring was updated so a restart reads the new key —
    // the two halves `apply_key_rotation` performs.
    assert_eq!(
        cipher_key(&cipher_b),
        new_key.key_bytes(),
        "B adopted the rotated key into its live cipher without a restart",
    );
    assert_eq!(
        ks_b.stored_key().as_deref(),
        Some(new_key.to_keyring_string().unwrap().as_str()),
        "B persisted the rotated key to its keyring, so its restart reads the current key",
    );

    // The chain supplies the exact refs; no path search chooses the key.
    let references = chain.active_wrapped_keys_for(&pubkey_hex(&device_b));
    let reunwrapped = unwrap_store_keyring_for_refs(
        &storage.storage,
        storage.root.store_root_hash,
        &device_b,
        &storage.root.store_root_id.to_string(),
        &references,
    )
    .await
    .expect("B can unwrap its re-wrapped key")
    .key_bytes();
    assert_eq!(reunwrapped, new_key.key_bytes());
}

#[tokio::test]
async fn invitation_after_rotation_uses_the_membership_selected_keyring() {
    let owner = UserKeypair::generate();
    let removed_member = UserKeypair::generate();
    let invited_member = UserKeypair::generate();
    let initial = EncryptionService::from_key([52u8; 32]);
    let (storage, owner_db) = exact_store(&owner).await;
    let mut chain = invite_exact_member(
        &storage,
        &owner_db,
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
    let rotated = revoke_member_durable(
        &storage.storage,
        storage.home.as_ref(),
        storage.root.store_root_hash,
        &mut chain,
        &owner,
        &pubkey_hex(&removed_member),
        &storage.root.store_root_id.to_string(),
        "0000000004000-0000-owner",
        &initial,
        &pending_rotation,
        &owner_db,
    )
    .await
    .expect("remove member and rotate the Store key");
    apply_key_rotation(rotated.clone(), &custody, &cipher)
        .expect("owner adopts the activated rotation");
    super::store::membership::complete_revoke_rotation_adoption(
        &owner_db,
        &pending_rotation,
        rotated.current_generation(),
    )
    .await
    .expect("owner completes the activated removal journal");

    let chain = invite_exact_member(
        &storage,
        &owner_db,
        &owner,
        &invited_member,
        MemberRole::Member,
        &initial,
    )
    .await;
    let invited_keyring = unwrap_store_keyring_for_refs(
        &storage.storage,
        storage.root.store_root_hash,
        &invited_member,
        &storage.root.store_root_id.to_string(),
        &chain
            .wrapped_key_authority_for(&pubkey_hex(&invited_member))
            .expect("invited member has complete wrapped-key authority"),
    )
    .await
    .expect("invited member opens the activated exact wrap");
    let sealed = rotated.seal_app_data(b"current Store data", b"post-rotation invite");
    assert_eq!(
        invited_keyring
            .open_app_data(&sealed, b"post-rotation invite")
            .expect("invitation retains the current Store key"),
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
    let (storage, owner_db) = exact_store(&owner).await;
    invite_exact_member(
        &storage,
        &owner_db,
        &owner,
        &device_b,
        MemberRole::Member,
        &encryption,
    )
    .await;
    let chain = load_exact_chain(&storage, &owner_db).await;
    let pending_keyring = EncryptionService::from_key(old_key)
        .with_appended_generation(2, rotated_key)
        .unwrap();
    let unreferenced =
        create_unreferenced_wrapped_key(&storage, &device_b, &pending_keyring, &owner).await;
    assert!(
        !chain
            .active_wrapped_keys_for(&pubkey_hex(&device_b))
            .contains(&unreferenced),
        "creating an exact object does not activate it",
    );

    let db_b = open_test_db();
    let (_tmp_b, ld_b) = temp_store_dir();
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;

    // A peer changeset waiting to be pulled, so the cycle proving it "completes"
    // also proves the pull ran and applied it while sealing was paused.
    let peer_src = open_test_db();
    let peer_cs = capture_bytes(
        &peer_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('peer1', 'FromOwner', NULL, 1, '0000000005000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage
        .publish_changeset("owner-device", 4, &peer_cs, db_b.schema_version())
        .await
        .expect("publish exact owner changeset");

    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));

    let result = run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("an unreferenced wrapped key does not affect the cycle");

    assert_eq!(
        result.changesets_applied, 1,
        "the pull still applies a peer's changeset",
    );
    assert_eq!(result.rotation_pending, None);
    assert_eq!(
        cipher_key(&cipher_b),
        old_key,
        "an unreferenced key must not replace the live cipher",
    );
    load_wrapped_store_key(
        &storage.storage,
        storage.root.store_root_hash,
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
    let (storage, owner_db) = exact_store(&owner).await;
    invite_exact_member(
        &storage,
        &owner_db,
        &owner,
        &device_b,
        MemberRole::Member,
        &encryption,
    )
    .await;
    let mut chain = invite_exact_member(
        &storage,
        &owner_db,
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
        .expect("invitation activates the initial exact wrapped key");

    let db_b = open_test_db();
    let (_tmp_b, ld_b) = temp_store_dir();
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));

    let new_key = revoke_member_durable(
        &storage.storage,
        storage.home.as_ref(),
        storage.root.store_root_hash,
        &mut chain,
        &owner,
        &pubkey_hex(&victim),
        &storage.root.store_root_id.to_string(),
        "0000000004000-0000-A",
        &encryption,
        &PendingRotation::none(),
        &owner_db,
    )
    .await
    .expect("revoke rotates key");

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("adopt generation 2");
    assert_eq!(cipher_generation(&cipher_b), new_key.current_generation());

    load_wrapped_store_key(
        &storage.storage,
        storage.root.store_root_hash,
        &old_reference,
    )
    .await
    .expect("the retained pre-rotation object remains readable");

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("replayed old wrapped key is ignored");

    assert_eq!(
        cipher_key(&cipher_b),
        new_key.key_bytes(),
        "a replayed older wrapped key must not roll the device back",
    );
    assert_eq!(
        cipher_generation(&cipher_b),
        new_key.current_generation(),
        "the accepted generation floor remains at the rotated generation",
    );
}

#[tokio::test]
async fn reinviting_member_supersedes_old_wrap_and_merges_same_generation_key() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let current_key: [u8; 32] = [13u8; 32];
    let replacement_key: [u8; 32] = [14u8; 32];

    let current = EncryptionService::from_key(current_key);
    let replacement = EncryptionService::from_key(replacement_key);
    let expected = current.merged_with(&replacement);
    let (storage, owner_db) = exact_store(&owner).await;
    invite_exact_member(
        &storage,
        &owner_db,
        &owner,
        &device_b,
        MemberRole::Member,
        &current,
    )
    .await;
    let chain = invite_exact_member(
        &storage,
        &owner_db,
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

    let db_b = open_test_db();
    let (_tmp_b, ld_b) = temp_store_dir();
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(current_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        current_key,
    )));

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("replacement same-generation wrapped key is merged");

    assert_eq!(
        cipher_key(&cipher_b),
        expected.key_bytes(),
        "same-generation forks select the greatest fingerprint independent of arrival order",
    );
    let persisted = ks_b
        .stored_key()
        .expect("merged same-generation keyring persisted");
    let persisted = crate::encryption::MasterKeyring::from_serialized(&persisted)
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
    let (storage, owner_db) = exact_store(&founder).await;
    invite_exact_member(
        &storage,
        &owner_db,
        &founder,
        &second_owner,
        MemberRole::Member,
        &encryption,
    )
    .await;
    invite_exact_member(
        &storage,
        &owner_db,
        &founder,
        &device_b,
        MemberRole::Member,
        &encryption,
    )
    .await;
    invite_exact_member(
        &storage,
        &owner_db,
        &founder,
        &victim,
        MemberRole::Member,
        &encryption,
    )
    .await;
    let second_owner_db = open_test_db();
    let db_b = open_test_db();
    activate_joined_device(&storage, &owner_db, &second_owner_db, &second_owner).await;
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
    crate::sync::test_helpers::promote_active_member_fixture(
        &storage,
        &owner_db,
        &second_owner_db,
        &founder,
        &second_owner,
        &encryption,
    )
    .await
    .expect("promote active second Owner");
    let mut chain = load_exact_chain(&storage, &owner_db).await;

    let (_tmp_b, ld_b) = temp_store_dir();
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));

    let new_key = Box::pin(revoke_member_durable(
        &storage.storage,
        storage.home.as_ref(),
        storage.root.store_root_hash,
        &mut chain,
        &second_owner,
        &pubkey_hex(&victim),
        &storage.root.store_root_id.to_string(),
        "0000000005000-0000-B",
        &encryption,
        &PendingRotation::none(),
        &second_owner_db,
    ))
    .await
    .expect("second owner can revoke");

    Box::pin(run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    ))
    .await
    .expect("existing member adopts a current owner's rotation");

    assert_eq!(cipher_key(&cipher_b), new_key.key_bytes());
}

#[tokio::test]
async fn rotation_after_concurrent_rotations_retains_every_authorized_key() {
    let founder = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let remaining_member = UserKeypair::generate();
    let first_victim = UserKeypair::generate();
    let second_victim = UserKeypair::generate();
    let initial = EncryptionService::from_key([41u8; 32]);

    let (storage, founder_db) = exact_store(&founder).await;
    invite_exact_member(
        &storage,
        &founder_db,
        &founder,
        &second_owner,
        MemberRole::Member,
        &initial,
    )
    .await;
    let second_owner_db = open_test_db();
    activate_joined_device(&storage, &founder_db, &second_owner_db, &second_owner).await;
    crate::sync::test_helpers::promote_active_member_fixture(
        &storage,
        &founder_db,
        &second_owner_db,
        &founder,
        &second_owner,
        &initial,
    )
    .await
    .expect("promote active second Owner");
    invite_exact_member(
        &storage,
        &founder_db,
        &founder,
        &remaining_member,
        MemberRole::Member,
        &initial,
    )
    .await;
    invite_exact_member(
        &storage,
        &founder_db,
        &founder,
        &first_victim,
        MemberRole::Member,
        &initial,
    )
    .await;
    let base_chain = invite_exact_member(
        &storage,
        &founder_db,
        &founder,
        &second_victim,
        MemberRole::Member,
        &initial,
    )
    .await;

    let mut founder_fork = base_chain.clone();
    let mut second_owner_fork = base_chain;
    let founder_pending_rotation = PendingRotation::none();
    let founder_rotation = Box::pin(revoke_member_durable(
        &storage.storage,
        storage.home.as_ref(),
        storage.root.store_root_hash,
        &mut founder_fork,
        &founder,
        &pubkey_hex(&first_victim),
        &storage.root.store_root_id.to_string(),
        "0000000005000-0000-founder",
        &initial,
        &founder_pending_rotation,
        &founder_db,
    ))
    .await
    .expect("founder publishes one rotation fork");
    Box::pin(revoke_member_durable(
        &storage.storage,
        storage.home.as_ref(),
        storage.root.store_root_hash,
        &mut second_owner_fork,
        &second_owner,
        &pubkey_hex(&second_victim),
        &storage.root.store_root_id.to_string(),
        "0000000005000-0000-second-owner",
        &initial,
        &PendingRotation::none(),
        &second_owner_db,
    ))
    .await
    .expect("second owner publishes the concurrent rotation fork");

    let founder_custody = TestCustody::default();
    founder_custody.set_initial_key(initial.key_bytes());
    let founder_cipher = RwLock::new(CloudCipher::Encrypted(initial.clone()));
    apply_key_rotation(founder_rotation.clone(), &founder_custody, &founder_cipher)
        .expect("founder adopts its activated rotation fork");
    super::store::membership::complete_revoke_rotation_adoption(
        &founder_db,
        &founder_pending_rotation,
        founder_rotation.current_generation(),
    )
    .await
    .expect("founder completes its activated removal journal");

    let mut merged_chain = load_exact_chain(&storage, &founder_db).await;
    let authority_refs = merged_chain
        .wrapped_key_authority_for(&pubkey_hex(&founder))
        .expect("merged membership selects the founder's exact wraps");
    let authority_keyring = unwrap_store_keyring_for_refs(
        &storage.storage,
        storage.root.store_root_hash,
        &founder,
        &storage.root.store_root_id.to_string(),
        &authority_refs,
    )
    .await
    .expect("founder unwraps both concurrent rotation forks");
    assert_eq!(authority_keyring.key_count(), 3);

    let next_rotation = Box::pin(revoke_member_durable(
        &storage.storage,
        storage.home.as_ref(),
        storage.root.store_root_hash,
        &mut merged_chain,
        &founder,
        &pubkey_hex(&remaining_member),
        &storage.root.store_root_id.to_string(),
        "0000000006000-0000-founder",
        &founder_rotation,
        &founder_pending_rotation,
        &founder_db,
    ))
    .await
    .expect("founder rotates after observing both forks");

    assert_eq!(
        next_rotation.key_count(),
        authority_keyring.key_count() + 1,
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
    let (storage, owner_db) = exact_store(&founder).await;
    invite_exact_member(
        &storage,
        &owner_db,
        &founder,
        &second_owner,
        MemberRole::Member,
        &encryption,
    )
    .await;
    invite_exact_member(
        &storage,
        &owner_db,
        &founder,
        &device_b,
        MemberRole::Member,
        &encryption,
    )
    .await;
    let second_owner_db = open_test_db();
    let db_b = open_test_db();
    activate_joined_device(&storage, &owner_db, &second_owner_db, &second_owner).await;
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
    crate::sync::test_helpers::promote_active_member_fixture(
        &storage,
        &owner_db,
        &second_owner_db,
        &founder,
        &second_owner,
        &encryption,
    )
    .await
    .expect("promote active second Owner");
    let mut chain = load_exact_chain(&storage, &owner_db).await;
    let (_tmp_b, ld_b) = temp_store_dir();
    let rotated = revoke_member_durable(
        &storage.storage,
        storage.home.as_ref(),
        storage.root.store_root_hash,
        &mut chain,
        &second_owner,
        &founder_pk,
        &storage.root.store_root_id.to_string(),
        "0000000004000-0000-B",
        &encryption,
        &PendingRotation::none(),
        &second_owner_db,
    )
    .await
    .expect("second owner removes founder through exact membership graph");

    let ks_b = TestCustody::default();
    ks_b.set_initial_key(rotated.key_bytes());
    let cipher_b = RwLock::new(CloudCipher::Encrypted(rotated.clone()));

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("refresh ignores refs authored by a removed owner");
    assert_eq!(
        cipher_key(&cipher_b),
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
    let (storage, owner_db) = exact_store(&owner).await;
    invite_exact_member(
        &storage,
        &owner_db,
        &owner,
        &device_b,
        MemberRole::Member,
        &encryption,
    )
    .await;

    let forged_key: [u8; 32] = [0xCDu8; 32];
    let forged = create_unreferenced_wrapped_key(
        &storage,
        &device_b,
        &EncryptionService::from_key(forged_key),
        &attacker,
    )
    .await;

    // B holds its real key (live + keyring) and pins the owner.
    let db_b = open_test_db();
    let (_tmp_b, ld_b) = temp_store_dir();
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(real_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        real_key,
    )));

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("an unreferenced attacker object does not affect refresh");

    // Critically, B did NOT swap its cipher to the attacker's key.
    assert_eq!(
        cipher_key(&cipher_b),
        real_key,
        "B keeps its real key; the unauthenticated forged key is never adopted",
    );
    assert_ne!(
        cipher_key(&cipher_b),
        forged_key,
        "the attacker's key was rejected",
    );
    load_wrapped_store_key(&storage.storage, storage.root.store_root_hash, &forged)
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
    let (storage, owner_db) = exact_store(&owner).await;
    let chain = invite_exact_member(
        &storage,
        &owner_db,
        &owner,
        &device_b,
        MemberRole::Member,
        &encryption,
    )
    .await;

    let db_b = open_test_db();
    let (_tmp_b, ld_b) = temp_store_dir();
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
    let head = crate::sync::store::membership::load_exact_membership_head(
        &storage.storage,
        &storage.root,
        chain
            .head_refs()
            .first()
            .expect("exact membership chain has an active head"),
    )
    .await
    .expect("load exact active membership head");
    storage
        .delete_protocol_object(&head.body.entry.object)
        .await
        .expect("remove exact membership entry before refresh");
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(key)));

    let result = run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await;
    assert!(
        result.is_err(),
        "an exact membership-object failure under a pinned owner must abort the cycle, not fall open: {result:?}",
    );
}

/// One sync cycle traverses each exact membership stream once. The chain is loaded
/// and anchored a single time at the top of the cycle and threaded to every
/// authorization site — the refresh, the pull, the outgoing write-grant, the
/// snapshot-author check, and the tombstone GC — so the whole cycle judges one
/// chain state. The storage wrapper counts exact membership-head slot reads.
///
/// Mutation proof: route any of those sites through its own membership load and
/// the count rises above the exact traversal count.
#[tokio::test]
async fn one_cycle_loads_exact_membership_once() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let key: [u8; 32] = [7u8; 32];

    let encryption = EncryptionService::from_key(key);
    let (storage, owner_db) = exact_store(&owner).await;
    let chain = invite_exact_member(
        &storage,
        &owner_db,
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
    let db_b = open_test_db();
    let (_tmp_b, ld_b) = temp_store_dir();
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(key)));

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
    let counter = MembershipReadCounter::new(&storage.storage);
    run_cycle_with_storage(
        &counter,
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("B's cycle");
    assert_eq!(
        counter.reads(),
        chain.entries().len() + stream_count,
        "one cycle reads every exact membership head and each stream's terminal empty slot once",
    );
}

fn cipher_key(cipher: &RwLock<CloudCipher>) -> [u8; 32] {
    match &*cipher.read().unwrap() {
        CloudCipher::Encrypted(enc) => enc.key_bytes(),
        CloudCipher::Plaintext => panic!("expected an encrypted cipher"),
    }
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
    let (storage, db) = exact_store(&owner).await;
    invite_exact_member(
        &storage,
        &db,
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
    let hlc = Hlc::new("A".to_string());

    // The keyring is momentarily unwritable, so local adoption fails after the
    // cloud rotation commits.
    ks.fail_writes();
    let err = Box::pin(remove_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &hlc,
        &pubkey_hex(&member),
        &encryption,
        &ks,
        &cipher,
        &pending_rotation,
        &db,
    ))
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
    let committed = load_exact_chain(&storage, &db).await;
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
            crate::encryption::MasterKeyring::from(EncryptionService::from_key(old_key))
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
        let cipher_refresh =
            RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
        let pending_rotation_refresh = PendingRotation::none();
        let (_tmp, ld) = temp_store_dir();
        Box::pin(run_cycle(
            &storage,
            &db,
            &cipher_refresh,
            &pending_rotation_refresh,
            &owner,
            "A",
            &ld,
            Some(&ks_refresh),
        ))
        .await
        .expect("refresh cycle");
        assert_eq!(
            cipher_generation(&cipher_refresh),
            2,
            "authorization refresh adopts the rotated key",
        );
        assert!(matches!(
            pending_rotation_refresh
                .check(&cipher_refresh.read().unwrap().clone())
                .map_err(|pending| pending.state),
            Err(super::cloud_storage::RotationPendingState::LocalCommitted { generation: 2 })
        ));
    }

    // Retrying the exact removal closes its journal and durable gate now that
    // custody is writable.
    ks.allow_writes();
    let fingerprint = Box::pin(remove_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &hlc,
        &pubkey_hex(&member),
        &encryption,
        &ks,
        &cipher,
        &pending_rotation,
        &db,
    ))
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
