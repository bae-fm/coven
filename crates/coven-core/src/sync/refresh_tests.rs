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
use crate::sync::cloud_storage::{CloudCipher, PendingRotation, PENDING_ROTATION_STATE_KEY};
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::invite::{
    revoke_member_durable, signed_wrapped_key_for_test, signed_wrapped_keyring_for_test,
    unwrap_store_keyring_for_owners_with_activation,
};
use crate::sync::membership::{MemberRole, MembershipChain, MembershipCoord};
use crate::sync::membership_ops::{invite_member, remove_member, MembershipOpsError};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    capture_bytes, open_test_db, pubkey_hex, temp_store_dir, TestCustody, TestStore,
};
use crate::sync::wrapped_store_key::WrappedKeyActivation;

const LIB_ID: &str = "lib-refresh-test";

async fn exact_store(owner: &UserKeypair) -> (TestStore, crate::database::Database) {
    let owner_db = open_test_db();
    let storage = TestStore::create(&owner_db, LIB_ID, owner.clone())
        .await
        .expect("create exact refresh Store");
    storage
        .open_into(&owner_db)
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
        LIB_ID,
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

    async fn put_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
        data: Vec<u8>,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner
            .put_wrapped_key(owner_pubkey, recipient_pubkey, data)
            .await
    }

    async fn get_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<Vec<u8>, crate::sync::storage::StorageError> {
        self.inner
            .get_wrapped_key(owner_pubkey, recipient_pubkey)
            .await
    }

    async fn delete_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner
            .delete_wrapped_key(owner_pubkey, recipient_pubkey)
            .await
    }
}

/// A non-rotating running device B adopts a rotated store key on its next cycle,
/// with no restart. Device A removes a member, which rotates the key and re-wraps
/// B's `keys/{A}/{B}` under the new key; B's next `run_single_sync_cycle` re-reads
/// that wrapped key, authenticates it against the current Owner set, and swaps
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
    let owner_pk = pubkey_hex(&owner);
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

    // The owner wraps the OLD key for B (what B adopted at join), signed by the
    // owner B pins, so B's refresh can authenticate it.
    let b_x = device_b.to_x25519_public_key();
    let wrapped_old = signed_wrapped_key_for_test(
        &storage.root.store_root_id.to_string(),
        &pubkey_hex(&device_b),
        &b_x,
        &old_key,
        &owner,
    );
    storage
        .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&device_b), wrapped_old)
        .await
        .unwrap();

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

    // --- Device A removes the victim: rotates the key, re-wraps B's keys/{A}/{B}. ---
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

    // And the rotated key B now holds is exactly the one the owner re-wrapped for
    // it — i.e. B can independently unwrap keys/{A}/{B} to the same bytes.
    let visible_activations: Vec<_> = chain
        .author_heads()
        .into_iter()
        .map(WrappedKeyActivation::MergeConcurrent)
        .collect();
    let reunwrapped = unwrap_store_keyring_for_owners_with_activation(
        storage.home.as_ref(),
        &device_b,
        &storage.root.store_root_id.to_string(),
        std::iter::once(owner_pk.as_str()),
        Some(&visible_activations),
    )
    .await
    .expect("B can unwrap its re-wrapped key")
    .key_bytes();
    assert_eq!(reunwrapped, new_key.key_bytes());
}

/// The mid-removal crash state: an owner overwrote this device's wrap with a
/// rotated keyring whose activation (the Remove entry) is not yet visible, then
/// crashed before uploading that entry. The device cannot adopt the rotation —
/// but it holds a working old keyring and must not be wedged. The cycle
/// COMPLETES: the refresh marks the rotation pending (at the wrap's committed
/// generation, read from the signed envelope without opening the sealed box),
/// the pull still applies a peer's changeset, the live cipher is untouched, and
/// the pause is persisted. Nothing new is sealed — the seal paths are gated on
/// `rotation_pending`.
#[tokio::test]
async fn inactive_removal_key_pauses_sealing_but_completes_the_cycle() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
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
    let chain = invite_exact_member(
        &storage,
        &owner_db,
        &owner,
        &victim,
        MemberRole::Member,
        &encryption,
    )
    .await;

    let activation = MembershipCoord {
        author_pubkey: owner_pk.clone(),
        author_owner_grant: chain
            .active_owner_grant(&owner_pk)
            .expect("founder Owner grant"),
        stream_id: chain
            .preferred_author_stream(
                &owner_pk,
                &chain
                    .active_owner_grant(&owner_pk)
                    .expect("founder Owner grant"),
            )
            .expect("founder author stream"),
        seq: 4,
        entry_hash: crate::sync::store_commit::ObjectHash::digest(b"pending removal"),
    };
    let pending_keyring = EncryptionService::from_key(old_key)
        .with_appended_generation(2, rotated_key)
        .unwrap();
    let b_x = device_b.to_x25519_public_key();
    let pending_wrapped = signed_wrapped_keyring_for_test(
        &storage.root.store_root_id.to_string(),
        &pubkey_hex(&device_b),
        &b_x,
        &pending_keyring,
        &owner,
        Some(activation),
    );
    storage
        .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&device_b), pending_wrapped)
        .await
        .unwrap();

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
    let peer_sequence = u64::try_from(chain.entries().len())
        .expect("membership entry count fits Store sequence")
        .checked_add(1)
        .expect("Store sequence does not overflow");
    storage
        .publish_changeset(
            "owner-device",
            peer_sequence,
            &peer_cs,
            db_b.schema_version(),
        )
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
    .expect("the cycle completes; an inactive removal key pauses sealing, it does not abort");

    assert_eq!(
        result.changesets_applied, 1,
        "the pull still applies a peer's changeset while sealing is paused",
    );
    let pending = result
        .rotation_pending
        .expect("the inactive removal key marks the rotation pending");
    assert_eq!(pending.committed_generation, 2);
    assert_eq!(pending.live_generation, 1);
    assert_eq!(
        cipher_key(&cipher_b),
        old_key,
        "the inactive key must not replace the live cipher",
    );
    assert_eq!(
        db_b.get_protocol_state(PENDING_ROTATION_STATE_KEY)
            .await
            .unwrap(),
        Some("2".to_string()),
        "the pause is persisted so a restart does not forget it and seal under the old generation",
    );
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

    let b_x = device_b.to_x25519_public_key();
    let wrapped_old = signed_wrapped_key_for_test(
        &storage.root.store_root_id.to_string(),
        &pubkey_hex(&device_b),
        &b_x,
        &old_key,
        &owner,
    );
    storage
        .put_wrapped_key(
            &pubkey_hex(&owner),
            &pubkey_hex(&device_b),
            wrapped_old.clone(),
        )
        .await
        .unwrap();

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

    storage
        .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&device_b), wrapped_old)
        .await
        .unwrap();

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
async fn same_generation_wrapped_key_is_merged_and_converges_deterministically() {
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

    let b_x = device_b.to_x25519_public_key();
    let same_generation_replacement = signed_wrapped_key_for_test(
        &storage.root.store_root_id.to_string(),
        &pubkey_hex(&device_b),
        &b_x,
        &replacement_key,
        &owner,
    );
    storage
        .put_wrapped_key(
            &pubkey_hex(&owner),
            &pubkey_hex(&device_b),
            same_generation_replacement,
        )
        .await
        .unwrap();

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
    .expect("same-generation wrapped key is merged");

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
        MemberRole::Owner,
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
    let mut chain = invite_exact_member(
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
    let (_tmp_b, ld_b) = temp_store_dir();
    activate_joined_device(&storage, &owner_db, &second_owner_db, &second_owner).await;
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
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
async fn removed_owner_key_is_not_adopted() {
    let founder = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let founder_pk = pubkey_hex(&founder);
    let current_key: [u8; 32] = [16u8; 32];
    let removed_owner_key: [u8; 32] = [17u8; 32];

    let encryption = EncryptionService::from_key(current_key);
    let (storage, owner_db) = exact_store(&founder).await;
    invite_exact_member(
        &storage,
        &owner_db,
        &founder,
        &second_owner,
        MemberRole::Owner,
        &encryption,
    )
    .await;
    let mut chain = invite_exact_member(
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
    let (_tmp_b, ld_b) = temp_store_dir();
    activate_joined_device(&storage, &owner_db, &second_owner_db, &second_owner).await;
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
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
        &second_owner_db,
    )
    .await
    .expect("second owner removes founder through exact membership graph");

    let b_x = device_b.to_x25519_public_key();
    let removed_owner_wrapped = signed_wrapped_key_for_test(
        &storage.root.store_root_id.to_string(),
        &pubkey_hex(&device_b),
        &b_x,
        &removed_owner_key,
        &founder,
    );
    // The removed founder, using residual bucket write, drops its wrap into the
    // current owner's prefix — where B's scan will read it. B authenticates the
    // wrap against that prefix's owner (second_owner), so the founder's signature
    // fails and the key is refused.
    storage
        .put_wrapped_key(
            &pubkey_hex(&second_owner),
            &pubkey_hex(&device_b),
            removed_owner_wrapped,
        )
        .await
        .unwrap();

    let ks_b = TestCustody::default();
    ks_b.set_initial_key(rotated.key_bytes());
    let cipher_b = RwLock::new(CloudCipher::Encrypted(rotated.clone()));

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
        "a key signed by a removed owner must fail closed, not be adopted",
    );
    assert_eq!(
        cipher_key(&cipher_b),
        rotated.key_bytes(),
        "the removed owner's key must not replace the current key",
    );
}

/// Wrapped-key adoption refuses a forged wrap — one not signed by the owner whose
/// prefix it sits under. A bucket writer drops a key they chose, sealed to B's
/// public key and signed by an attacker, into the current owner's `keys/{owner}/`
/// prefix. B's refresh scans the current owners and authenticates each wrap
/// against its prefix owner, so it does NOT adopt the attacker's key; the cycle
/// fails closed and B keeps its real key.
#[tokio::test]
async fn refresh_rejects_a_forged_wrapped_key() {
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

    // The attacker forges B's wrapped key: a key THEY chose, sealed to B's real
    // public key, signed by the attacker (not the owner), dropped into the current
    // owner's prefix where B's scan will read it.
    let forged_key: [u8; 32] = [0xCDu8; 32];
    let b_x = device_b.to_x25519_public_key();
    let forged = signed_wrapped_key_for_test(
        &storage.root.store_root_id.to_string(),
        &pubkey_hex(&device_b),
        &b_x,
        &forged_key,
        &attacker,
    );
    storage
        .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&device_b), forged)
        .await
        .unwrap();

    // B holds its real key (live + keyring) and pins the owner.
    let db_b = open_test_db();
    let (_tmp_b, ld_b) = temp_store_dir();
    activate_joined_device(&storage, &owner_db, &db_b, &device_b).await;
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(real_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        real_key,
    )));

    // B's cycle aborts (refuses the forged key) rather than adopting it.
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
        "a forged wrapped key must abort the cycle, not be adopted: {result:?}",
    );

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
    let head = crate::sync::membership_ops::load_exact_membership_head(
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
        .delete_protocol_object(&head.entry.object)
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
async fn removal_rotation_commits_even_when_local_adoption_fails_then_both_remedies_converge() {
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
        LIB_ID,
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

    // The failed adoption marks this device's own rotation-pending gate — the
    // structural half of the fix: every seal for the cloud through this same
    // storage now refuses until one of the two remedies below clears it (see
    // `rotation_pending_tests` for the end-to-end proof that nothing seals in
    // the meantime).
    assert_eq!(
        pending_rotation.pending_generation(),
        Some(2),
        "the failed adoption marks the committed generation as pending",
    );

    // Remedy 1 — the next sync cycle: a still-stale device (generation 1) adopts
    // the rotated key from its own `keys/{owner}/{owner}` wrap, no retry needed.
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
            "the next sync cycle adopts the rotated key",
        );
    }

    // Remedy 2 — retry the removal: the member is already removed, so the retry
    // re-derives the rotated keyring from the current owner set and adopts it now
    // that the keyring is writable again.
    ks.allow_writes();
    let fingerprint = Box::pin(remove_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &hlc,
        &pubkey_hex(&member),
        LIB_ID,
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
