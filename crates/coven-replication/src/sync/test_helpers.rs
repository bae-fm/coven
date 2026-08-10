/// Shared test helpers for sync module tests.
///
/// These drive a real [`Database`] over an in-memory connection carrying the
/// synthetic test schema, so tests exercise the engine through the same path
/// production does.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use coven_foundation::store_dir::StoreDir;
use coven_keys::encryption::MasterKeyring;
use coven_keys::keys::{KeyError, MasterKeyCustody, UserKeypair};
#[cfg(test)]
use coven_protocol::store_commit::ObjectHash;
use coven_storage::CloudSyncObjectStorage;

/// The synthetic store's schema and `Database` constructors, which the database
/// layer owns and its own tests open directly.
pub use coven_database::synthetic_store::*;
pub use coven_foundation::store_dir::temp_store_dir;
pub use coven_storage::cloud::test_utils::{test_cloud_home, test_cloud_home_with_binding};

pub fn staged_snapshot_image(bytes: &[u8]) -> coven_database::SnapshotDatabaseImage {
    let file = tempfile::NamedTempFile::new().expect("create staged snapshot fixture path");
    let path = file.path().to_path_buf();
    file.close().expect("release staged snapshot fixture path");
    coven_database::SnapshotDatabaseImage::create(path, bytes)
        .expect("write staged snapshot fixture")
}

#[cfg(test)]
pub fn test_cache_locator_hash(label: &str) -> ObjectHash {
    ObjectHash::digest(label.as_bytes())
}

/// In-memory [`MasterKeyCustody`] for tests, with a switch to force `persist`
/// to fail. The switch models a device whose keyring is momentarily
/// unwritable, so a test can drive a key adoption into its failure path and then
/// clear the switch to prove the retry converges. Stores the serialized form
/// (like the real `Keyring` preset), so `stored_key` reflects exactly what a
/// caller wrote.
#[derive(Clone, Default)]
pub struct TestCustody {
    value: Arc<Mutex<Option<String>>>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl TestCustody {
    pub fn set_initial_key(&self, key: [u8; 32]) {
        *self.value.lock().unwrap() = Some(
            MasterKeyring::from(coven_keys::encryption::EncryptionService::from_key(key))
                .to_serialized(),
        );
    }

    pub fn stored_key(&self) -> Option<String> {
        self.value.lock().unwrap().clone()
    }

    /// Make the next and every subsequent `persist` fail until cleared.
    pub fn fail_writes(&self) {
        self.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Let `persist` succeed again.
    pub fn allow_writes(&self) {
        self.fail.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

impl MasterKeyCustody for TestCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        self.value
            .lock()
            .unwrap()
            .as_deref()
            .map(MasterKeyring::from_serialized)
            .transpose()
            .map_err(|e| KeyError::Crypto(e.to_string()))
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KeyError::Persistence(
                "forced keyring write failure".to_string(),
            ));
        }
        *self.value.lock().unwrap() = Some(keyring.to_serialized());
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        *self.value.lock().unwrap() = None;
        Ok(())
    }
}

/// Copy the payload files one store directory holds into another.
///
/// A store is a directory, not a file: rows name payload files beside the
/// database, so a test that copies the database with `VACUUM INTO` and opens the
/// copy has to bring those files along, exactly as a device carries its whole
/// store directory rather than one file out of it.
pub fn copy_payload_spool(
    from: &coven_foundation::store_dir::StoreDir,
    to: &coven_foundation::store_dir::StoreDir,
) {
    let source = from.payload_spool_dir();
    let destination = to.payload_spool_dir();
    std::fs::create_dir_all(&destination).expect("create the copied payload spool directory");
    for entry in std::fs::read_dir(&source).expect("read the payload spool being copied") {
        let entry = entry.expect("payload spool entry");
        std::fs::copy(entry.path(), destination.join(entry.file_name()))
            .expect("copy one payload into the copied store directory");
    }
}

/// Hex-encoded ed25519 public key, as membership entries and the wrapped-key
/// store identify a member.
pub fn pubkey_hex(kp: &UserKeypair) -> String {
    coven_keys::keys::public_key_hex(kp)
}

/// Ed25519 identity derived from exact test-owned seed bytes.
pub fn user_keypair_from_seed(seed: [u8; 32]) -> UserKeypair {
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
    UserKeypair::from_signing_key_bytes(&signing_key.to_keypair_bytes())
        .expect("seed-derived signing key is valid")
}

/// Grants a Dropbox shared-folder membership to whichever peer account asks —
/// the provider-side step a cross-principal admission needs before the joining
/// device can write to the store's namespace.
pub struct TestDropboxAccessAdministrator {
    pub namespace_id: String,
}

#[async_trait::async_trait]
impl crate::sync::store::DeviceProviderAccessAdministrator for TestDropboxAccessAdministrator {
    async fn grant_member_access(
        &self,
        _member_pubkey: &str,
        _provider_account_email: Option<&str>,
        peer: &coven_protocol::objects::ProviderDeviceBinding,
    ) -> Result<coven_protocol::provider::ProviderAccessLocator, crate::sync::store::DeviceJoinError>
    {
        let coven_protocol::objects::ProviderPrincipalId::Dropbox { account_id } = &peer.principal
        else {
            return Err(crate::sync::store::DeviceJoinError::Provider(
                "test Dropbox access administrator received a non-Dropbox peer".to_string(),
            ));
        };
        Ok(
            coven_protocol::provider::ProviderAccessLocator::DropboxSharedFolderMember {
                namespace_id: self.namespace_id.clone(),
                account_id: account_id.clone(),
            },
        )
    }
}

pub struct TestStore {
    home: std::sync::Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
    pub root: coven_protocol::store_commit::StoreRootRef,
    signer: UserKeypair,
    founder: TestDevice,
    producers: Arc<tokio::sync::Mutex<TestStoreProducers>>,
    founder_store_dir: TestStoreDir,
}

/// Why a test pull did not produce a result. Keeps the three steps a test pull
/// runs — opening the store, authorizing the writer, running the cycle — apart,
/// so a test asserting on one of them cannot pass on another.
#[derive(Debug, thiserror::Error)]
pub enum TestPullError {
    #[error("open Store: {0}")]
    Open(String),
    #[error("authorize Store writer: {0}")]
    Authorize(#[from] crate::sync::store::StoreWriterAuthorizationError),
    #[error("pull: {0}")]
    Pull(#[from] crate::sync::cycle::SyncCycleFailure),
}

mod test_device {
    use super::*;

    pub struct TestDeviceSigningAuthority {
        registration: coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
        device_signer: UserKeypair,
    }

    impl TestDeviceSigningAuthority {
        pub fn registration_ref(
            &self,
        ) -> &coven_protocol::store_commit::StoreDeviceRegistrationRef {
            self.registration.reference()
        }

        pub fn registration(&self) -> &coven_protocol::store_commit::StoreDeviceRegistration {
            self.registration.value()
        }

        pub fn referenced_registration(
            &self,
        ) -> &coven_protocol::store_commit::ReferencedStoreDeviceRegistration {
            &self.registration
        }

        #[allow(clippy::too_many_arguments)]
        pub fn sign_device_join_attempt_for_test(
            &self,
            store_root: coven_protocol::store_commit::StoreRootRef,
            attempt_id: coven_protocol::store_commit::DeviceJoinAttemptId,
            attempt_slot: coven_protocol::objects::ObjectSlot,
            expected_registration: coven_protocol::store_commit::StoreDeviceRegistration,
            registration_slot: coven_protocol::objects::ObjectSlot,
            outcome_slot: coven_protocol::objects::ObjectSlot,
            bootstrap_cut: coven_protocol::store_commit::StoreHistoryCut,
            membership: coven_protocol::circle_control::StoreMembershipStateRef,
            provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId,
            provider_approval: coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
            provider_response: coven_protocol::store_commit::device_join_exchange::DeviceProviderResponseReservation,
            owner_grant: coven_protocol::membership::MembershipGrantId,
        ) -> Result<
            coven_protocol::store_commit::DeviceJoinAttempt,
            coven_protocol::store_commit::StoreProtocolError,
        > {
            coven_protocol::store_commit::DeviceJoinAttempt::signed(
                store_root,
                attempt_id,
                attempt_slot,
                expected_registration,
                registration_slot,
                outcome_slot,
                bootstrap_cut,
                membership,
                provider_admin_grant,
                provider_approval,
                provider_response,
                self.registration.reference().clone(),
                owner_grant,
                self.registration.value(),
                &self.device_signer,
            )
        }

        pub fn sign_provider_admission_approval_without_shape_validation_for_test(
            &self,
            request: coven_protocol::store_commit::device_join_exchange::DeviceProviderAccessRequest,
            access_grant: coven_protocol::provider::ActivatedStoreMemberProviderAccessGrant,
            admission: coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionChallenge,
        ) -> coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval
        {
            coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval::signed_without_shape_validation_for_test(
                request,
                access_grant,
                admission,
                &self.device_signer,
            )
        }

        pub fn sign_device_head_for_test(
            &self,
            store_root_hash: coven_protocol::store_commit::ObjectHash,
            commit: coven_protocol::store_commit::StoreBatchCommitRef,
            successor: coven_protocol::store_commit::SuccessorLink,
        ) -> Result<
            coven_protocol::store_commit::StoreDeviceHead,
            coven_protocol::store_commit::StoreProtocolError,
        > {
            coven_protocol::store_commit::StoreDeviceHead::signed(
                store_root_hash,
                self.registration.reference().clone(),
                commit,
                successor,
                &self.device_signer,
            )
        }

        pub fn sign_reclaim_receipt_for_test(
            &self,
            store_root_hash: coven_protocol::store_commit::ObjectHash,
            authorization: coven_protocol::reclaim::ReclaimAuthorizationRef,
            provider_admin_state: coven_protocol::circle_control::StoreMembershipStateRef,
            provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId,
        ) -> Result<
            coven_protocol::reclaim::ReclaimReceipt,
            coven_protocol::store_commit::StoreProtocolError,
        > {
            coven_protocol::reclaim::ReclaimReceipt::signed(
                store_root_hash,
                authorization,
                provider_admin_state,
                provider_admin_grant,
                self.registration.reference().clone(),
                self.registration.value(),
                &self.device_signer,
            )
        }
    }

    /// One device's store directory, shared by its database and Store object.
    #[derive(Clone)]
    pub struct TestStoreDir {
        dir: StoreDir,
    }

    impl TestStoreDir {
        pub fn from_database(database: &SyntheticDatabase) -> Self {
            Self {
                dir: database.store_dir.clone(),
            }
        }

        pub fn from_store_dir(dir: StoreDir) -> Self {
            Self { dir }
        }

        pub fn dir(&self) -> &StoreDir {
            &self.dir
        }
    }

    #[derive(Clone)]
    pub struct TestDevice {
        db: coven_database::StoreDatabase,
        store: std::sync::Arc<crate::sync::store::Store>,
        store_dir: TestStoreDir,
        pub device_id: String,
        storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
        identity: UserKeypair,
    }

    impl TestDevice {
        pub async fn create(
            db: &SyntheticDatabase,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            founder_timestamp: &str,
            identity: UserKeypair,
        ) -> Result<Self, String> {
            Self::create_with_database(
                coven_database::StoreDatabase::new(db),
                TestStoreDir::from_database(db),
                storage,
                founder_timestamp,
                identity,
            )
            .await
        }

        pub async fn create_with_database(
            database: coven_database::StoreDatabase,
            store_dir: TestStoreDir,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            founder_timestamp: &str,
            identity: UserKeypair,
        ) -> Result<Self, String> {
            let initialized = crate::sync::store::Store::create(
                database.clone(),
                storage.clone(),
                store_dir.dir().clone(),
                founder_timestamp,
                &identity,
            )
            .await
            .map_err(|error| error.to_string())?;
            Ok(Self {
                db: database,
                store: std::sync::Arc::new(initialized.store),
                store_dir,
                device_id: initialized.device_id,
                storage,
                identity,
            })
        }

        pub async fn open_with_database(
            database: coven_database::StoreDatabase,
            store_dir: TestStoreDir,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            root: &coven_protocol::store_commit::StoreRootRef,
            identity: &UserKeypair,
        ) -> Result<Self, String> {
            let initialized = crate::sync::store::Store::open(
                database.clone(),
                storage.clone(),
                store_dir.dir().clone(),
                root,
                identity,
            )
            .await
            .map_err(|error| error.to_string())?;
            Ok(Self {
                db: database,
                store: std::sync::Arc::new(initialized.store),
                store_dir,
                device_id: initialized.device_id,
                storage,
                identity: identity.clone(),
            })
        }

        pub async fn load(
            db: &SyntheticDatabase,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            identity: UserKeypair,
        ) -> Result<Self, crate::sync::store::StoreError> {
            Self::load_with_database(
                coven_database::StoreDatabase::new(db),
                storage,
                identity,
                TestStoreDir::from_database(db),
            )
            .await
        }

        pub async fn load_with_database(
            database: coven_database::StoreDatabase,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            identity: UserKeypair,
            store_dir: TestStoreDir,
        ) -> Result<Self, crate::sync::store::StoreError> {
            let store = crate::sync::store::Store::load(
                database.clone(),
                storage.clone(),
                store_dir.dir().clone(),
                identity.clone(),
            )
            .await?;
            let device_id = database
                .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
                .await?
                .ok_or(crate::sync::store::StoreError::MissingState {
                    key: coven_database::LOCAL_DEVICE_ID_STATE_KEY,
                })?;
            Ok(Self {
                db: database,
                store: std::sync::Arc::new(store),
                store_dir,
                device_id,
                storage,
                identity,
            })
        }

        /// The directory this device's durable local state lives under —
        /// stable across every rebinding of the device.
        pub fn store_dir(&self) -> &StoreDir {
            self.store_dir.dir()
        }

        pub fn test_store_dir(&self) -> &TestStoreDir {
            &self.store_dir
        }

        pub fn adopt_key_rotation(
            &self,
            encryption: &coven_keys::encryption::EncryptionService,
            custody: &dyn coven_keys::keys::MasterKeyCustody,
        ) -> Result<String, coven_keys::keys::KeyError> {
            self.storage
                .adopt_key_rotation_for_test(encryption, custody)
        }

        pub fn store_root(&self) -> &coven_protocol::store_commit::StoreRootRef {
            self.store.store_root()
        }

        pub async fn authorize_writer(
            &self,
        ) -> Result<crate::sync::store::AuthorizedWriterOperation<'_>, crate::sync::store::StoreError>
        {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
        }

        pub async fn membership_for_test(
            &self,
        ) -> Result<coven_protocol::membership::MembershipChain, crate::sync::store::StoreError>
        {
            self.store.membership_for_test().await
        }

        pub async fn latest_local_store_position(
            &self,
        ) -> Result<
            Option<coven_protocol::store_commit::StoreBatchCommitRef>,
            crate::sync::store::StoreError,
        > {
            self.store.latest_local_store_position().await
        }

        pub async fn load_commit_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
            crate::sync::store::StoreError,
        > {
            self.store.load_commit_for_test(reference).await
        }

        pub async fn load_membership_head_for_test(
            &self,
            reference: &coven_protocol::membership::MembershipHeadRef,
        ) -> Result<coven_protocol::membership::AuthorHead, crate::sync::store::StoreError>
        {
            self.store.load_membership_head_for_test(reference).await
        }

        pub async fn load_exact_materialized_commit(
            &self,
            stream_id: &str,
            sequence: u64,
        ) -> Result<
            Option<(
                coven_protocol::store_commit::StoreBatchCommitRef,
                coven_protocol::store_commit::VerifiedStoreBatchCommit,
            )>,
            String,
        > {
            self.store
                .load_exact_materialized_commit(stream_id, sequence)
                .await
        }

        pub fn device_join_transport(
            &self,
        ) -> crate::sync::store::device_join::transport::StoreDeviceJoinTransport<'_> {
            self.store.device_join_transport()
        }

        pub fn circles(&self) -> crate::sync::store::StoreCircleCommands<'_> {
            self.store.circles()
        }

        pub async fn circle_epoch_access(
            &self,
            circle_id: coven_protocol::circle::CircleId,
            expected_control: coven_protocol::circle::CircleControlCoord,
        ) -> Result<
            Option<coven_protocol::circle_activation::CircleEpochAccess>,
            coven_database::DbError,
        > {
            self.store
                .circle_epoch_access(circle_id, expected_control)
                .await
        }

        pub async fn discard_blocked_write(
            &self,
            write_id: coven_protocol::write::WriteId,
        ) -> Result<Vec<coven_protocol::write::WriteId>, crate::sync::store::StoreError> {
            self.store.discard_blocked_write(write_id).await
        }

        pub async fn restore_membership(
            &self,
        ) -> Result<
            crate::sync::store::StoreRestoreMembership,
            crate::sync::store::MembershipOpsError,
        > {
            self.store.restore_membership().await
        }

        pub async fn owner_recovery_for_test(
            &self,
        ) -> Result<crate::sync::store::RestoringStore<'_>, String> {
            self.store.owner_recovery_for_test().await
        }

        pub async fn begin_device_join(
            &self,
            member_pubkey: &str,
        ) -> Result<crate::sync::DeviceJoinOffer, crate::sync::DeviceJoinError> {
            self.store.begin_device_join(member_pubkey).await
        }

        pub async fn begin_owner_promotion_for_device(
            &self,
            device_id: coven_protocol::StoreDeviceId,
        ) -> Result<
            coven_protocol::store_commit::OwnerPromotionRequest,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store.begin_owner_promotion_for_device(device_id).await
        }

        pub async fn begin_owner_promotion(
            &self,
            member_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            coven_protocol::store_commit::OwnerPromotionRequest,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store.begin_owner_promotion(member_registration).await
        }

        pub async fn accept_owner_promotion(
            &self,
            request: coven_protocol::store_commit::OwnerPromotionRequest,
        ) -> Result<
            coven_protocol::store_commit::OwnerPromotionAcceptance,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store.accept_owner_promotion(request).await
        }

        pub async fn finalize_owner_promotion(
            &self,
            encryption: &coven_keys::encryption::EncryptionService,
            acceptance: coven_protocol::store_commit::OwnerPromotionAcceptance,
        ) -> Result<
            coven_protocol::circle_control::StoreMembershipStateRef,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store
                .finalize_owner_promotion(encryption, acceptance)
                .await
        }

        pub async fn blob_protection_for_test(
            &self,
            authority: &coven_protocol::blob::RowBlobAuthority,
            stored: &coven_protocol::blob::locator::StoredBlobRef,
        ) -> Result<coven_protocol::objects::BlobSpoolProtection, String> {
            self.store.blob_protection_for_test(authority, stored).await
        }

        pub async fn announcement_stream_id_for_test(
            &self,
        ) -> Result<coven_protocol::membership::AuthorStreamId, crate::sync::store::StoreError>
        {
            self.store.announcement_stream_id_for_test().await
        }

        pub async fn sign_device_head_for_test(
            &self,
            commit: coven_protocol::store_commit::StoreBatchCommitRef,
            successor: coven_protocol::store_commit::SuccessorLink,
        ) -> Result<coven_protocol::store_commit::StoreDeviceHead, crate::sync::store::StoreError>
        {
            self.store
                .sign_device_head_for_test(commit, successor)
                .await
        }

        pub async fn owner_promotion_target_for_test(
            &self,
        ) -> Result<
            coven_protocol::store_commit::StoreDeviceRegistrationRef,
            crate::sync::store::StoreError,
        > {
            self.store.owner_promotion_target_for_test().await
        }

        pub async fn observe_excluded_candidate_head_for_test(
            &self,
            candidate: &coven_protocol::store_commit::StoreDeviceHead,
            candidate_commit: &coven_protocol::store_commit::StoreBatchCommit,
            candidate_object: &coven_protocol::objects::ExactObjectRef,
        ) -> Result<
            crate::sync::store::ExcludedCandidateHeadObservation,
            crate::sync::store::StoreError,
        > {
            self.store
                .observe_excluded_candidate_head_for_test(
                    candidate,
                    candidate_commit,
                    candidate_object,
                )
                .await
        }

        pub async fn cleanup_merge_candidate_for_test(
            &self,
            write_id: coven_protocol::write::WriteId,
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store.cleanup_merge_candidate_for_test(write_id).await
        }

        pub async fn resign_snapshot_meta_for_test(
            &self,
            meta: coven_protocol::store_commit::SnapshotMeta,
        ) -> Result<coven_protocol::store_commit::SnapshotMeta, crate::sync::store::StoreError>
        {
            self.store.resign_snapshot_meta_for_test(meta).await
        }

        pub async fn parse_local_snapshot_meta_for_test(
            &self,
            bytes: &[u8],
            reference: &coven_protocol::store_commit::StoreSnapshotRef,
        ) -> Result<coven_protocol::store_commit::SnapshotMeta, crate::sync::store::StoreError>
        {
            self.store
                .parse_local_snapshot_meta_for_test(bytes, reference)
                .await
        }

        pub async fn prepare_operation_plan_for_test(
            &self,
        ) -> Result<crate::sync::store::StoreOperationCommitPlan, crate::sync::store::StoreError>
        {
            self.store.prepare_operation_plan_for_test().await
        }

        pub async fn authorize_retained_outbound_for_test(
            &self,
            order: &coven_protocol::store_commit::StoreCommitOrder,
            candidate_membership_heads: &[coven_protocol::membership::MembershipHeadRef],
        ) -> Result<crate::sync::store::MergeOutboundAuthorization, crate::sync::store::StoreError>
        {
            self.store
                .authorize_retained_outbound_for_test(order, candidate_membership_heads)
                .await
        }

        pub async fn complete_revoke_rotation_adoption_for_test(
            &self,
            pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
            adopted_generation: u64,
        ) -> Result<(), crate::sync::store::InviteError> {
            self.store
                .complete_revoke_rotation_adoption_for_test(pending_rotation, adopted_generation)
                .await
        }

        pub async fn retained_merge_replay_inputs_for_test(
            &self,
        ) -> Result<Vec<coven_database::OwnedVerifiedMergeMaterialization>, coven_database::DbError>
        {
            self.store.retained_merge_replay_inputs_for_test().await
        }

        pub async fn resolved_store_device_state_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreDeviceStateRef,
        ) -> Result<coven_protocol::store_commit::ResolvedStoreDeviceState, coven_database::DbError>
        {
            self.store
                .resolved_store_device_state_for_test(reference)
                .await
        }

        pub async fn retained_merge_materialization_for_test(
            &self,
            reference: coven_protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<coven_database::OwnedVerifiedMergeMaterialization, coven_database::DbError>
        {
            self.store
                .retained_merge_materialization_for_test(reference)
                .await
        }

        pub async fn prepare_conflict_resolution_plan_for_test(
            &self,
            candidate_membership_heads: &[coven_protocol::membership::MembershipHeadRef],
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .prepare_conflict_resolution_plan_for_test(candidate_membership_heads)
                .await
        }

        pub async fn load_membership_at_exact_heads_for_test(
            &self,
            heads: &[coven_protocol::membership::MembershipHeadRef],
            resolutions: &[coven_protocol::membership::StoreMembershipConflictResolutionRef],
        ) -> Result<coven_protocol::membership::MembershipChain, crate::sync::store::StoreError>
        {
            self.store
                .load_membership_at_exact_heads_for_test(heads, resolutions)
                .await
        }

        pub async fn project_membership_for_test(
            &self,
            candidate_heads: &[coven_protocol::membership::MembershipHeadRef],
        ) -> Result<coven_protocol::membership::MembershipChain, crate::sync::store::StoreError>
        {
            self.store
                .project_membership_for_test(candidate_heads)
                .await
        }

        pub async fn assert_deep_membership_projection_for_test(
            &self,
            heads: &[coven_protocol::membership::MembershipHeadRef],
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .assert_deep_membership_projection_for_test(heads)
                .await
        }

        pub async fn verify_device_join_attempt_for_test(
            &self,
            reference: &coven_protocol::store_commit::DeviceJoinAttemptRef,
            owner: &coven_protocol::store_commit::StoreDeviceRegistration,
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .verify_device_join_attempt_for_test(reference, owner)
                .await
        }

        pub async fn exact_next_announcement_slot_for_test(
            &self,
            registration_ref: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
            registration: &coven_protocol::store_commit::StoreDeviceRegistration,
            previous: Option<&coven_protocol::store_commit::StoreBatchCommitRef>,
        ) -> Result<
            (
                coven_protocol::objects::ObjectSlot,
                Option<coven_protocol::store_commit::StoreDeviceHeadRef>,
            ),
            crate::sync::store::StoreError,
        > {
            self.store
                .exact_next_announcement_slot_for_test(registration_ref, registration, previous)
                .await
        }

        pub async fn load_registration_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            coven_protocol::store_commit::StoreDeviceRegistration,
            crate::sync::store::StoreError,
        > {
            self.store.load_registration_for_test(reference).await
        }

        pub async fn verify_snapshots_for_acknowledgement_for_test(
            &self,
            snapshots: &[coven_database::PublishedStoreSnapshot],
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .verify_snapshots_for_acknowledgement_for_test(snapshots)
                .await
        }

        pub async fn open_circle_package_for_test(
            &self,
            access: &coven_protocol::circle_activation::CircleEpochAccess,
            commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
            reference: &coven_protocol::store_commit::CirclePackageRef,
        ) -> Result<Vec<u8>, crate::sync::store::StoreError> {
            self.store
                .open_circle_package_for_test(access, commit, reference)
                .await
        }

        #[allow(clippy::too_many_arguments)]
        pub async fn pull_readiness_for_test(
            &self,
            coverage: &coven_protocol::store_commit::CommitFrontier,
            frontier: &std::collections::BTreeMap<
                String,
                coven_protocol::store_commit::StoreBatchCommitRef,
            >,
            device_state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
            exclusion_freezes: &[coven_protocol::store_commit::StoreDeviceProposalAck],
            commit_ref: &coven_protocol::store_commit::StoreBatchCommitRef,
            commit: &coven_protocol::store_commit::StoreBatchCommit,
        ) -> Result<crate::sync::store::Readiness, crate::sync::store::StorePullError> {
            self.store
                .pull_readiness_for_test(
                    coverage,
                    frontier,
                    device_state,
                    exclusion_freezes,
                    commit_ref,
                    commit,
                )
                .await
        }

        pub async fn verified_merge_membership_prefix_for_test(
            &self,
            references: impl IntoIterator<Item = coven_protocol::store_commit::StoreBatchCommitRef>,
            predecessors: impl IntoIterator<Item = coven_protocol::store_commit::StoreBatchCommitRef>,
        ) -> Result<
            crate::sync::store::VerifiedMergeMembershipPrefix,
            crate::sync::store::StorePullError,
        > {
            self.store
                .verified_merge_membership_prefix_for_test(references, predecessors)
                .await
        }

        pub async fn retained_merge_history_frontier_for_test(
            &self,
            references: Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
        ) -> Result<Vec<coven_database::RetainedMergeHistoryCheckpoint>, coven_database::DbError>
        {
            self.store
                .retained_merge_history_frontier_for_test(references)
                .await
        }

        pub async fn verified_circle_activation_for_test(
            &self,
            circle_id: coven_protocol::circle::CircleId,
            control: coven_protocol::circle::CircleControlCoord,
        ) -> Result<
            Option<coven_protocol::circle_activation::VerifiedCircleReference>,
            coven_database::DbError,
        > {
            self.store
                .verified_circle_activation_for_test(circle_id, control)
                .await
        }

        pub async fn finalized_circle_close_outcome_for_test(
            &self,
            circle_id: coven_protocol::circle::CircleId,
        ) -> Result<
            coven_protocol::circle::CircleEpochCloseOutcome,
            crate::sync::store::CircleOperationError,
        > {
            self.store
                .finalized_circle_close_outcome_for_test(circle_id)
                .await
        }

        pub async fn circle_package_is_retained_for_replay_for_test(
            &self,
            target: coven_protocol::store_commit::CirclePackageRef,
            activation: coven_protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<bool, coven_database::DbError> {
            self.store
                .circle_package_is_retained_for_replay_for_test(target, activation)
                .await
        }

        pub async fn load_circle_acknowledgement_for_test(
            &self,
            reference: &coven_protocol::store_commit::CircleAckRef,
        ) -> Result<coven_protocol::store_commit::CircleAck, crate::sync::store::StoreAckError>
        {
            self.store
                .load_circle_acknowledgement_for_test(reference)
                .await
        }

        pub async fn load_applicable_circle_packages_for_test(
            &self,
            verified: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
            activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
            author: &coven_protocol::store_commit::StoreDeviceRegistration,
            local_store_membership: coven_protocol::membership::LocalStoreMembership,
        ) -> Result<
            Vec<crate::sync::store::LoadedCirclePackage>,
            crate::sync::store::CirclePackageReadError,
        > {
            self.store
                .load_applicable_circle_packages_for_test(
                    verified,
                    activations,
                    author,
                    local_store_membership,
                )
                .await
        }

        pub fn protocol_root_for_test(&self) -> &coven_protocol::store_commit::StoreProtocolRoot {
            self.store.protocol_root_for_test()
        }

        pub async fn prepare_acknowledgement_activation_for_test(
            &self,
            acknowledgement: coven_protocol::store_commit::StoreAckRef,
            candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        ) -> Result<(), coven_database::DbError> {
            self.store
                .prepare_acknowledgement_activation_for_test(acknowledgement, candidate)
                .await
        }

        pub async fn prepare_merge_history_successor_for_test(
            &self,
            verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
            recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
            evidence: crate::sync::store::MergeHistorySuccessorEvidence,
        ) -> Result<crate::sync::store::PreparedMergeHistorySuccessor, crate::sync::store::StoreError>
        {
            self.store
                .prepare_merge_history_successor_for_test(
                    verified_commit,
                    recovery_author,
                    evidence,
                )
                .await
        }

        pub async fn prepare_device_join_bootstrap_for_test(
            &self,
            coverage: &coven_protocol::store_commit::StoreHistoryCut,
            attempt_activation: &coven_protocol::store_commit::StoreBatchCommitRef,
            membership_state: &coven_protocol::circle_control::StoreMembershipStateRef,
        ) -> Result<coven_database::DeviceJoinBootstrapPlan, crate::sync::store::StoreError>
        {
            self.store
                .prepare_device_join_bootstrap_for_test(
                    coverage,
                    attempt_activation,
                    membership_state,
                )
                .await
        }

        pub async fn load_store_package_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<
            Option<coven_protocol::objects::VerifiedObject<Vec<u8>>>,
            crate::sync::store::StoreError,
        > {
            self.store.load_store_package_for_test(reference).await
        }

        pub async fn load_store_ack_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreAckRef,
            registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        ) -> Result<coven_protocol::store_commit::StoreAck, crate::sync::store::StoreError>
        {
            self.store
                .load_store_ack_for_test(reference, registration)
                .await
        }

        pub async fn load_head_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreDeviceHeadRef,
            registration: &coven_protocol::store_commit::StoreDeviceRegistration,
            commit: &coven_protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<coven_protocol::store_commit::StoreDeviceHead, crate::sync::store::StoreError>
        {
            self.store
                .load_head_for_test(reference, registration, commit)
                .await
        }

        #[allow(clippy::too_many_arguments)]
        pub async fn remove_member(
            &self,
            public_key_hex: &str,
            encryption: &coven_keys::encryption::EncryptionService,
            master_keys: &dyn coven_keys::keys::MasterKeyCustody,
            cipher: &dyn coven_storage::CloudSyncCipherStateAccess,
            pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
        ) -> Result<String, crate::sync::store::MembershipOpsError> {
            self.store
                .remove_member(
                    public_key_hex,
                    encryption,
                    master_keys,
                    cipher,
                    pending_rotation,
                )
                .await
        }

        pub async fn authorize_device_provider_access(
            &self,
            request: coven_protocol::store_commit::device_join_exchange::DeviceProviderAccessRequest,
            access_administrator: Option<
                &dyn crate::sync::store::DeviceProviderAccessAdministrator,
            >,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .authorize_device_provider_access(request, access_administrator)
                .await
        }

        pub async fn publish_device_provider_challenge(
            &self,
            bootstrap: coven_protocol::store_commit::device_join_exchange::ProvisionalDeviceBootstrap,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::ProviderReadyDeviceBootstrap,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .publish_device_provider_challenge(bootstrap)
                .await
        }

        pub async fn complete_device_provider_admission(
            &self,
            readiness: coven_protocol::store_commit::device_join_exchange::DeviceJoinReadiness,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionCompletion,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .complete_device_provider_admission(readiness)
                .await
        }

        pub async fn close_device_provider_admission(
            &self,
            cancellation: coven_protocol::store_commit::device_join_exchange::DeviceJoinCancellation,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::ProviderAdminJoinTerminal,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .close_device_provider_admission(cancellation)
                .await
        }

        pub async fn revoke_device_provider_admission_writes(
            &self,
            cancellation: coven_protocol::store_commit::device_join_exchange::DeviceJoinCancellation,
            revocation_executor: &dyn crate::sync::store::DeviceJoinWriteRevocationExecutor,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::ProviderAdminJoinTerminal,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .revoke_device_provider_admission_writes(cancellation, revocation_executor)
                .await
        }

        pub async fn abandon_device_join(
            &self,
            offer: coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinAbandonment,
            crate::sync::DeviceJoinError,
        > {
            self.store.abandon_device_join(offer).await
        }

        pub async fn accept_device_registration_request(
            &self,
            request: coven_protocol::store_commit::device_join_exchange::DeviceRegistrationRequest,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::ProvisionalDeviceBootstrap,
            crate::sync::DeviceJoinError,
        > {
            self.store.accept_device_registration_request(request).await
        }

        pub async fn cancel_device_join(
            &self,
            attempt: coven_protocol::store_commit::DeviceJoinAttemptRef,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinCancellation,
            crate::sync::DeviceJoinError,
        > {
            self.store.cancel_device_join(attempt).await
        }

        pub async fn finalize_device_join(
            &self,
            completion: coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionCompletion,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinActivation,
            crate::sync::DeviceJoinError,
        > {
            self.store.finalize_device_join(completion).await
        }

        pub async fn complete_owner_device_join_cleanup(
            &self,
            activation: coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupActivation,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupActivation,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .complete_owner_device_join_cleanup(activation)
                .await
        }

        pub async fn revoke_joining_device_writes(
            &self,
            cancellation: coven_protocol::store_commit::device_join_exchange::DeviceJoinCancellation,
            revocation_executor: &dyn crate::sync::store::DeviceJoinWriteRevocationExecutor,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::JoinerJoinTerminal,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .revoke_joining_device_writes(cancellation, revocation_executor)
                .await
        }

        pub async fn prepare_device_join_cleanup(
            &self,
            cancellation: coven_protocol::store_commit::device_join_exchange::DeviceJoinCancellation,
            administrator_terminal: coven_protocol::store_commit::device_join_exchange::ProviderAdminJoinTerminal,
            joiner_terminal: coven_protocol::store_commit::device_join_exchange::JoinerJoinTerminal,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupReceipt,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .prepare_device_join_cleanup(cancellation, administrator_terminal, joiner_terminal)
                .await
        }

        pub async fn activate_device_join_cleanup(
            &self,
            receipt: coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupReceipt,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupActivation,
            crate::sync::DeviceJoinError,
        > {
            self.store.activate_device_join_cleanup(receipt).await
        }

        pub async fn device_exclusion_operations_for_test(
            &self,
        ) -> Result<
            Vec<crate::sync::store::StoreDeviceExclusionOperationInfo>,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.device_exclusion_operations_for_test().await
        }

        pub async fn stage_uploaded_device_exclusion_proposal_for_test(
            &self,
        ) -> Result<
            coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store
                .stage_uploaded_device_exclusion_proposal_for_test()
                .await
        }

        pub async fn propose_device_exclusion(
            &self,
            target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            crate::sync::store::StoreDeviceExclusionResult,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.propose_device_exclusion(target).await
        }

        pub async fn cancel_device_exclusion(
            &self,
            proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        ) -> Result<
            crate::sync::store::StoreDeviceExclusionResult,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.cancel_device_exclusion(proposal).await
        }

        pub async fn finalize_device_exclusion(
            &self,
            proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        ) -> Result<
            crate::sync::store::StoreDeviceExclusionResult,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.finalize_device_exclusion(proposal).await
        }

        pub async fn pending_device_join_observation_for_test(
            &self,
            pending: &crate::sync::store::DeviceJoinJournalDatabase,
            offer: &coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        ) -> Result<crate::sync::store::PendingDeviceJoinObservation<'_>, String> {
            self.store
                .pending_device_join_observation_for_test(pending, offer)
                .await
                .map_err(|error| error.to_string())
        }

        pub async fn open_pending_device_join_for_test(
            &self,
            pending: &crate::sync::store::DeviceJoinJournalDatabase,
            identity: &UserKeypair,
            offer: coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        ) -> Result<crate::sync::store::PendingDeviceJoinAuthority<'_>, String> {
            self.store
                .open_pending_device_join_for_test(pending, identity, offer)
                .await
                .map_err(|error| error.to_string())
        }

        pub async fn prepare_snapshot_bootstrap_for_test(
            &self,
            membership_floor: &coven_protocol::membership::MembershipFloor,
            binary_schema_version: u32,
            target_path: &std::path::Path,
            restorer_identity: &UserKeypair,
        ) -> Result<
            crate::sync::store::PreparedSnapshotBootstrap<'_>,
            crate::sync::store::SnapshotError,
        > {
            self.store
                .prepare_snapshot_bootstrap_for_test(
                    membership_floor,
                    binary_schema_version,
                    target_path,
                    restorer_identity,
                )
                .await
        }

        #[allow(clippy::too_many_arguments)]
        pub async fn invite_member(
            &self,
            member_pubkey: &str,
            invitee_email: Option<&str>,
            role: coven_protocol::membership::MemberRole,
            encryption: &coven_keys::encryption::EncryptionService,
            store_id: &str,
            store_name: &str,
        ) -> Result<crate::sync::store::MemberInvitation, crate::sync::store::MembershipOpsError>
        {
            self.store
                .invite_member(
                    member_pubkey,
                    invitee_email,
                    role,
                    encryption,
                    store_id,
                    store_name,
                )
                .await
        }

        pub async fn drain_uploads(
            &self,
            store_dir: &StoreDir,
            clock: &dyn coven_foundation::clock::Clock,
            routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
            observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
        ) -> Result<coven_protocol::blob::DrainOutcome, coven_database::DbError> {
            self.store
                .with_test_store_dir(store_dir.clone())
                .authorize_writer()
                .await
                .map_err(|error| coven_database::DbError::Message(error.to_string()))?
                .drain_uploads(clock, routing_encryption, observer)
                .await
        }

        pub async fn publish_pending_store_database(
            &self,
            store_dir: &StoreDir,
        ) -> Result<bool, String> {
            let store = self.store.with_test_store_dir(store_dir.clone());
            let mut writer = store
                .authorize_writer()
                .await
                .map_err(|error| error.to_string())?;
            let prepared = writer
                .prepare_pending_store_write()
                .await
                .map_err(|error| error.to_string())?;
            let published = writer
                .drain_store_writes()
                .await
                .map_err(|error| error.to_string())?;
            if published > 0 {
                crate::sync::test_owner_graph::local_blob_access(
                    self.db.clone(),
                    store_dir.clone(),
                )
                .drain_published_blob_drop_intents(u64::MAX)
                .await?;
                coven_database::LocalBlobCleanup::new(&self.db)
                    .drain()
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok(prepared || published > 0)
        }

        pub async fn publish_fixture_position(&self, store_dir: &StoreDir, note_id: &str) -> u64 {
            self.db
                .insert_fixture_position_for_test(note_id)
                .await
                .expect("insert fixture Store position");
            assert!(self
                .publish_pending_store_database(store_dir)
                .await
                .expect("publish fixture Store position"));
            self.latest_local_store_position()
                .await
                .expect("read fixture Store position")
                .expect("fixture Store write has an exact position")
                .coord
                .sequence()
        }

        pub async fn publish_exact_remote_blob_binding(
            &self,
            store_dir: &StoreDir,
            root_id: &str,
            row_id: &str,
            bytes: &[u8],
        ) -> coven_protocol::blob::locator::StoredBlobRef {
            let local = self
                .db
                .row_blob_ref("note_photos", row_id)
                .await
                .expect("load exact Local row blob reference");
            let source = store_dir
                .local_blob_path(&local.blob().namespace, &local.blob().id)
                .expect("resolve host blob source");
            coven_foundation::local_file::AtomicStagedFile::write_for_test(&source, bytes)
                .await
                .expect("write host blob source");
            crate::sync::test_owner_graph::TestOwnerGraph::new(self.db.clone(), store_dir.clone())
                .make_remote("notes", root_id, false)
                .await
                .expect("start exact make_remote");
            let clock = coven_foundation::clock::FixedClock(
                chrono::DateTime::parse_from_rfc3339("2024-06-01T01:00:00Z")
                    .expect("valid exact blob publication time")
                    .with_timezone(&chrono::Utc),
            );
            let outcome = self
                .drain_uploads(store_dir, &clock, None, None)
                .await
                .expect("drain exact blob upload");
            assert_eq!(outcome.uploaded(), 1);
            assert!(self
                .publish_pending_store_database(store_dir)
                .await
                .expect("publish exact remote blob binding"));
            self.db
                .row_blob_ref("note_photos", row_id)
                .await
                .expect("load exact Remote row blob reference")
                .stored()
                .cloned()
                .expect("Remote row owns an exact stored blob reference")
        }

        pub async fn activated_store_device_registration_for_test(
            &self,
            reference: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
            coven_database::DbError,
        > {
            self.db.activated_store_device_registration(reference).await
        }

        pub fn schema_version(&self) -> u32 {
            self.db.schema_version()
        }

        pub async fn device_authority_for_test(
            &self,
        ) -> Result<TestDeviceSigningAuthority, String> {
            let registration = self
                .db
                .activated_store_device_registration_records()
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .find(|registration| registration.value().device_id.to_string() == self.device_id)
                .ok_or_else(|| "test device registration is not active".to_string())?;
            let device_signer = registration
                .value()
                .device_signer(&self.identity)
                .map_err(|error| error.to_string())?;
            Ok(TestDeviceSigningAuthority {
                registration,
                device_signer,
            })
        }

        pub async fn publish_changeset_for_test(
            &self,
            sequence: u64,
            changeset: Vec<u8>,
            schema_version: u32,
        ) -> Result<coven_protocol::store_commit::StoreBatchCommitRef, String> {
            if schema_version != self.db.schema_version() {
                return Err(format!(
                    "test changeset schema version {schema_version} differs from producer schema {}",
                    self.db.schema_version()
                ));
            }
            let before = self
                .latest_local_store_position()
                .await
                .map_err(|error| error.to_string())?;
            let expected = before
                .as_ref()
                .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
            if sequence != expected {
                return Err(format!(
                    "test producer expected sequence {expected}, got {sequence}"
                ));
            }
            self.db
                .enqueue_store_changeset_for_test(changeset)
                .await
                .map_err(|error| error.to_string())?;
            let mut writer = self
                .authorize_writer()
                .await
                .map_err(|error| error.to_string())?;
            let published = writer
                .publish_pending_store_writes()
                .await
                .map_err(|error| error.to_string())?;
            if published == 0 {
                return Err("test changeset did not prepare a Store commit".to_string());
            }
            writer
                .latest_local_store_position()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "published test changeset has no Store position".to_string())
        }

        pub async fn publish_changeset_after_for_test(
            &self,
            store_dir: &StoreDir,
            changeset: Vec<u8>,
            previous_sequence: u64,
        ) -> Result<coven_protocol::store_commit::StoreBatchCommitRef, String> {
            let before = self
                .latest_local_store_position()
                .await
                .map_err(|error| error.to_string())?;
            let actual_previous_sequence = before
                .as_ref()
                .map_or(0, |position| position.coord.sequence());
            if actual_previous_sequence != previous_sequence {
                return Err(format!(
                    "Store position is {actual_previous_sequence}, expected {previous_sequence}"
                ));
            }
            self.db
                .enqueue_store_changeset_for_test(changeset)
                .await
                .map_err(|error| error.to_string())?;
            let store = self.store.with_test_store_dir(store_dir.clone());
            let mut writer = store
                .authorize_writer()
                .await
                .map_err(|error| error.to_string())?;
            if !writer
                .prepare_pending_store_write()
                .await
                .map_err(|error| error.to_string())?
            {
                return Err("test changeset did not prepare a Store commit".to_string());
            }
            writer
                .drain_store_writes()
                .await
                .map_err(|error| error.to_string())?;
            writer
                .latest_local_store_position()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "published test changeset has no Store position".to_string())
        }

        pub async fn create_exact_opaque_blob(
            &self,
            namespace: &str,
            id: &str,
            bytes: &[u8],
        ) -> coven_protocol::blob::locator::StoredBlobRef {
            let registration = self
                .db
                .local_blob_write_authority()
                .await
                .expect("load exact blob write authority");
            let authority = coven_protocol::objects::BlobWriteAuthority::new(&registration);
            let protection = coven_keys::encryption::EncryptionService::from_key([42; 32]);
            let locator = coven_protocol::blob::locator::BlobLocator::opaque(
                namespace,
                id,
                authority.reference.clone(),
                coven_protocol::blob::locator::RemoteAudience::Store,
                coven_protocol::blob::BlobScope::Master,
                protection.seal_key_fingerprint(),
                bytes.len() as u64,
                coven_protocol::store_commit::ObjectHash::digest(bytes),
            )
            .expect("build exact blob locator");
            let temp = tempfile::tempdir().expect("create exact blob spool directory");
            let plaintext = temp.path().join("plaintext");
            let spool = temp.path().join("stored");
            coven_foundation::local_file::AtomicStagedFile::write_for_test(&plaintext, bytes)
                .await
                .expect("write exact blob plaintext");
            let slot = self
                .storage
                .allocate_blob_slot(&locator, &authority)
                .await
                .expect("allocate exact blob slot");
            let spool_stage = self
                .store_dir
                .dir()
                .stage_atomic_file(&spool)
                .await
                .expect("create exact blob spool stage");
            self.storage
                .seal_blob_to_spool(
                    &locator,
                    &authority,
                    coven_protocol::objects::BlobSpoolProtection::Opaque(protection),
                    &plaintext,
                    spool_stage,
                )
                .await
                .expect("seal exact blob");
            let stored = self
                .storage
                .prepare_blob_object(&locator, &authority, slot, &spool)
                .await
                .expect("prepare exact blob object");
            self.storage
                .create_blob_object_from_file(
                    &stored,
                    &authority,
                    &spool,
                    &coven_storage::cloud::no_progress(),
                )
                .await
                .expect("create exact blob object");
            stored
        }

        pub async fn create_exact_browsable_blob(
            &self,
            namespace: &str,
            id: &str,
            cloud_path: &str,
            bytes: &[u8],
        ) -> coven_protocol::blob::locator::StoredBlobRef {
            let registration = self
                .db
                .local_blob_write_authority()
                .await
                .expect("load browsable blob write authority");
            let authority = coven_protocol::objects::BlobWriteAuthority::new(&registration);
            let locator = coven_protocol::blob::locator::BlobLocator::browsable(
                namespace,
                id,
                authority.reference.clone(),
                cloud_path,
                bytes.len() as u64,
                coven_protocol::store_commit::ObjectHash::digest(bytes),
            )
            .expect("build browsable blob locator");
            let temp = tempfile::tempdir().expect("create browsable blob spool directory");
            let plaintext = temp.path().join("plaintext");
            let spool = temp.path().join("stored");
            coven_foundation::local_file::AtomicStagedFile::write_for_test(&plaintext, bytes)
                .await
                .expect("write browsable blob plaintext");
            let slot = self
                .storage
                .allocate_blob_slot(&locator, &authority)
                .await
                .expect("allocate browsable blob slot");
            let spool_stage = self
                .store_dir
                .dir()
                .stage_atomic_file(&spool)
                .await
                .expect("create browsable blob spool stage");
            self.storage
                .seal_blob_to_spool(
                    &locator,
                    &authority,
                    coven_protocol::objects::BlobSpoolProtection::Browsable,
                    &plaintext,
                    spool_stage,
                )
                .await
                .expect("stage browsable blob");
            let stored = self
                .storage
                .prepare_blob_object(&locator, &authority, slot, &spool)
                .await
                .expect("prepare browsable blob object");
            self.storage
                .create_blob_object_from_file(
                    &stored,
                    &authority,
                    &spool,
                    &coven_storage::cloud::no_progress(),
                )
                .await
                .expect("create browsable blob object");
            stored
        }

        pub async fn run_cycle(
            &self,
            store_dir: &StoreDir,
            observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        {
            self.run_cycle_with(
                &coven_foundation::clock::SystemClock,
                None,
                store_dir,
                observer,
            )
            .await
        }

        pub async fn run_cycle_with(
            &self,
            clock: &dyn coven_foundation::clock::Clock,
            master_keys: Option<&dyn coven_keys::keys::MasterKeyCustody>,
            store_dir: &StoreDir,
            observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        {
            self.run_cycle_with_storage(
                self.store.clone(),
                self.storage.clone(),
                clock,
                master_keys,
                store_dir,
                observer,
            )
            .await
        }

        pub async fn run_cycle_with_interceptor<I>(
            &self,
            clock: &dyn coven_foundation::clock::Clock,
            master_keys: Option<&dyn coven_keys::keys::MasterKeyCustody>,
            store_dir: &StoreDir,
            observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
            interceptor: I,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        where
            I: super::StorageInterceptor + 'static,
        {
            let storage = std::sync::Arc::new(super::InterceptedStorage::new(
                self.storage.clone(),
                interceptor,
            ));
            let store_storage: std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage> =
                storage.clone();
            let store = std::sync::Arc::new(self.store.with_test_storage(store_storage));
            self.run_cycle_with_storage(store, storage, clock, master_keys, store_dir, observer)
                .await
        }

        async fn run_cycle_with_storage<S>(
            &self,
            store: std::sync::Arc<crate::sync::store::Store>,
            storage: std::sync::Arc<S>,
            clock: &dyn coven_foundation::clock::Clock,
            master_keys: Option<&dyn coven_keys::keys::MasterKeyCustody>,
            store_dir: &StoreDir,
            observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        where
            S: crate::sync::cycle::CloudSyncCycleConnection + 'static,
        {
            let local_blob_access = crate::sync::test_owner_graph::local_blob_access(
                self.db.clone(),
                store_dir.clone(),
            );
            let store = std::sync::Arc::new(store.with_test_store_dir(store_dir.clone()));
            let components = crate::sync::cycle::SyncComponents::from_retained_test_device(
                store,
                self.db.clone(),
                local_blob_access,
                storage,
                self.storage.store_id().to_string(),
                self.device_id.clone(),
            );
            components.run_cycle(clock, master_keys, observer).await
        }

        pub fn current_encryption_for_test(
            &self,
        ) -> Option<coven_keys::encryption::EncryptionService> {
            self.storage.current_encryption()
        }

        pub fn mark_rotation_committed_for_test(&self, generation: u64) -> Result<(), String> {
            self.storage.mark_rotation_committed_for_test(generation)
        }

        pub fn pending_rotation_generation_for_test(&self) -> Option<u64> {
            self.storage.pending_rotation_generation_for_test()
        }

        pub fn clear_rotation_gate_for_test(&self) {
            self.storage.clear_rotation_gate_for_test();
        }

        pub async fn create_circle(
            &self,
            metadata_stamp: &str,
            name: &str,
        ) -> Result<coven_protocol::CircleId, crate::sync::store::CircleOperationError> {
            self.store
                .circles()
                .create_circle(metadata_stamp, name)
                .await
        }

        /// The writer authority every Circle helper below has to take before it
        /// can name its operation, with the one error shape they all report.
        async fn circle_writer(
            &self,
        ) -> Result<
            crate::sync::store::AuthorizedWriterOperation<'_>,
            crate::sync::store::CircleOperationError,
        > {
            self.store.authorize_writer().await.map_err(|error| {
                crate::sync::store::CircleOperationError::InvalidState(error.to_string())
            })
        }

        #[cfg(test)]
        pub(crate) async fn prepare_circle_operation(
            &self,
            metadata_stamp: &str,
            name: &str,
        ) -> Result<
            crate::sync::store::circles::PreparedCircleJournal,
            crate::sync::store::CircleOperationError,
        > {
            self.circle_writer()
                .await?
                .circles()
                .prepare_create_for_test(metadata_stamp, name)
                .await
        }

        pub async fn publish_circle_epoch_close_response(
            &self,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.circle_writer()
                .await?
                .circles()
                .publish_circle_epoch_close_responses()
                .await
        }

        pub async fn publish_circle_operation(
            &self,
            operation_id: &coven_protocol::circle::CircleOperationId,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            let routing_key = coven_protocol::circle::derive_row_routing_key(
                &coven_keys::encryption::EncryptionService::from_key([42; 32]),
                self.store.store_root().store_root_hash,
            )
            .expect("derive Circle test routing key");
            self.circle_writer()
                .await?
                .circles()
                .publish_prepared_operation_for_test(operation_id, Some(&routing_key))
                .await
        }

        pub async fn resume_circle_operations(
            &self,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            let routing_key = coven_protocol::circle::derive_row_routing_key(
                &coven_keys::encryption::EncryptionService::from_key([42; 32]),
                self.store.store_root().store_root_hash,
            )
            .expect("derive Circle test routing key");
            self.circle_writer()
                .await?
                .circles()
                .resume_circle_operations(Some(&routing_key))
                .await
        }

        pub async fn retry_circle_operation(
            &self,
            operation_id: &coven_protocol::circle::CircleOperationId,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.store
                .circles()
                .retry_circle_operation(
                    operation_id,
                    Some(&coven_keys::encryption::EncryptionService::from_key(
                        [42; 32],
                    )),
                )
                .await
        }

        pub async fn prepare_pending_store_write(
            &self,
            store_dir: &StoreDir,
        ) -> Result<bool, crate::sync::store::StoreError> {
            self.store
                .with_test_store_dir(store_dir.clone())
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                })?
                .prepare_pending_store_write()
                .await
        }

        #[cfg(test)]
        pub async fn prepare_blocked_transfer_candidate(
            &self,
            label: &str,
        ) -> (tempfile::TempDir, StoreDir, coven_protocol::write::WriteId) {
            let statement = format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('{label}', 'pending', NULL, 1, \
                     '0000000002000-0000-{label}', '2026-07-18')"
            );
            self.db
                .run_host_store_write_for_test(None, None, move |transaction| {
                    transaction
                        .execute_batch(&statement)
                        .map_err(coven_database::DbError::from)
                })
                .await
                .expect("capture transfer candidate host write");
            let (temporary, store_dir) = temp_store_dir();
            assert!(self
                .prepare_pending_store_write(&store_dir)
                .await
                .expect("prepare transfer candidate"));
            let candidate = self
                .db
                .oldest_prepared_store_write()
                .await
                .expect("load transfer candidate")
                .expect("transfer candidate exists");
            let write_id = candidate.commit.value.write_id.clone();
            self.db
                .set_write_status(
                    &write_id,
                    coven_protocol::write::WriteStatus::Blocked(
                        coven_protocol::write::WriteBlock::InvalidProtocolState {
                            reason: "exercise restored author-exclusion evidence".to_string(),
                        },
                    ),
                )
                .await
                .expect("block transfer candidate");
            (temporary, store_dir, write_id)
        }

        #[cfg(test)]
        pub async fn prepare_store_operation_plan_for_test(
            &self,
        ) -> Result<crate::sync::store::StoreOperationCommitPlan, crate::sync::store::StoreError>
        {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                })?
                .prepare_plan()
                .await
        }

        pub async fn drain_store_writes(&self) -> Result<u64, crate::sync::store::StoreError> {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                })?
                .drain_store_writes()
                .await
        }

        pub async fn reclaim_packages(
            &self,
        ) -> Result<crate::sync::store::StoreReclaimResult, crate::sync::store::StoreReclaimError>
        {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::StoreReclaimError::Authorization(error.to_string())
                })?
                .reclaim_packages()
                .await
        }

        pub async fn abandon_merge_candidate(
            &self,
            write_id: coven_protocol::write::WriteId,
        ) -> Result<crate::sync::store::MergeCandidateAbandonment, crate::sync::store::StoreError>
        {
            self.store.abandon_merge_candidate(write_id).await
        }

        pub async fn prepare_merge_candidate_abandonment(
            &self,
            write_id: coven_protocol::write::WriteId,
        ) -> Result<bool, crate::sync::store::StoreError> {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                })?
                .prepare_merge_candidate_abandonment(write_id)
                .await
        }

        pub async fn prepare_peer_exclusion(
            &self,
            target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> coven_protocol::store_commit::StoreDeviceExclusionProposalRef {
            let proposal = match self
                .propose_device_exclusion(target)
                .await
                .expect("propose peer exclusion")
            {
                crate::sync::store::StoreDeviceExclusionResult::ProposalActivated {
                    proposal,
                    ..
                } => proposal,
                result => panic!("unexpected exclusion proposal result: {result:?}"),
            };
            let freezes = self
                .db
                .store_device_exclusion_freezes()
                .await
                .expect("read owner exclusion freeze");
            assert_eq!(freezes.len(), 1);
            assert_eq!(freezes[0].proposal, proposal);
            assert_eq!(&freezes[0].proposal.target, target);
            let frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
                self.db
                    .materialized_frontier()
                    .await
                    .expect("read owner exclusion frontier"),
            )
            .expect("shape owner exclusion frontier");
            let acknowledgement = self
                .stage_acknowledgement(frontier, "2026-07-18T00:01:00Z".to_string())
                .await
                .expect("stage owner exclusion acknowledgement");
            let coven_protocol::store_commit::StoreAckExclusionState { proposal_freezes } =
                acknowledgement.exclusions.clone();
            assert_eq!(proposal_freezes, freezes);
            assert_eq!(
                self.drain_acknowledgements()
                    .await
                    .expect("publish owner exclusion acknowledgement"),
                1
            );
            proposal
        }

        pub async fn activate_peer_exclusion(
            &self,
            proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        ) -> coven_protocol::store_commit::StoreDeviceExclusionRef {
            let result = self
                .finalize_device_exclusion(proposal)
                .await
                .expect("finalize peer exclusion");
            let crate::sync::store::StoreDeviceExclusionResult::OutcomeActivated {
                outcome:
                    coven_protocol::store_commit::StoreDeviceExclusionOutcomeRef::Excluded(exclusion),
                ..
            } = result
            else {
                panic!("unexpected exclusion result: {result:?}")
            };
            assert!(self
                .db
                .store_device_exclusion_freezes()
                .await
                .expect("read released owner exclusion freeze")
                .is_empty());
            exclusion
        }

        pub async fn finalize_peer_exclusion(
            &self,
            target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> coven_protocol::store_commit::StoreDeviceExclusionRef {
            let proposal = self.prepare_peer_exclusion(target).await;
            self.activate_peer_exclusion(&proposal).await
        }

        pub async fn prepare_circle_object(
            &self,
            context: &coven_protocol::objects::ProtocolObjectContext,
            semantic_prefix: &str,
            extension: &str,
            bytes: Vec<u8>,
        ) -> Result<
            coven_protocol::objects::PreparedExactObject,
            crate::sync::store::CircleOperationError,
        > {
            self.circle_writer()
                .await?
                .circles()
                .prepare_circle_object_for_test(context, semantic_prefix, extension, bytes)
                .await
        }

        pub async fn prepare_circle_object_at(
            &self,
            context: &coven_protocol::objects::ProtocolObjectContext,
            slot: coven_protocol::objects::ObjectSlot,
            semantic_prefix: &str,
            bytes: Vec<u8>,
        ) -> Result<
            coven_protocol::objects::PreparedExactObject,
            crate::sync::store::CircleOperationError,
        > {
            self.circle_writer()
                .await?
                .circles()
                .prepare_circle_object_at_for_test(context, slot, semantic_prefix, bytes)
        }

        pub async fn prepare_circle_activation_objects(
            &self,
            draft: coven_protocol::circle::CircleTransitionDraft,
            history: &crate::sync::store::CircleTransitionHistory,
            candidate_family: coven_protocol::store_commit::CandidateFamilyId,
        ) -> Result<
            (
                coven_protocol::circle::PreparedCircleTransition,
                coven_protocol::store_commit::CircleActivationObjects,
                std::collections::BTreeMap<String, coven_protocol::objects::PreparedExactObject>,
                Option<coven_protocol::objects::ExactObjectRef>,
                Vec<coven_protocol::store_commit::StreamActivation>,
            ),
            crate::sync::store::CircleOperationError,
        > {
            self.circle_writer()
                .await?
                .circles()
                .prepare_circle_activation_objects_for_test(draft, history, candidate_family)
                .await
        }

        pub async fn sign_circle_commit(
            &self,
            old_commit: &coven_protocol::store_commit::StoreBatchCommit,
            coord: coven_protocol::store_commit::StoreCommitCoord,
            reference: coven_protocol::store_commit::CircleControlRef,
            stream_activations: Vec<coven_protocol::store_commit::StreamActivation>,
        ) -> Result<
            coven_protocol::store_commit::StoreBatchCommit,
            crate::sync::store::CircleOperationError,
        > {
            self.circle_writer()
                .await?
                .circles()
                .sign_circle_commit_for_test(old_commit, coord, reference, stream_activations)
        }

        pub async fn rename_circle(
            &self,
            metadata_stamp: &str,
            circle_id: coven_protocol::CircleId,
            name: &str,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.store
                .circles()
                .rename_circle(metadata_stamp, circle_id, name)
                .await
        }

        pub async fn delete_circle(
            &self,
            circle_id: coven_protocol::CircleId,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.store.circles().delete_circle(circle_id).await
        }

        pub async fn load_circle_activations(
            &self,
            commit_ref: &coven_protocol::store_commit::StoreBatchCommitRef,
            commit: &coven_protocol::store_commit::StoreBatchCommit,
            author: &coven_protocol::store_commit::StoreDeviceRegistration,
        ) -> Result<
            coven_protocol::circle_activation::VerifiedCircleActivations,
            crate::sync::store::CircleOperationError,
        > {
            let routing_key = coven_protocol::circle::derive_row_routing_key(
                &coven_keys::encryption::EncryptionService::from_key([42; 32]),
                commit.store_root_hash,
            )
            .expect("derive Circle test routing key");
            self.store
                .load_circle_activations_for_test(commit_ref, commit, author, Some(&routing_key))
                .await
        }

        pub async fn circle_blob_opening_error(
            &self,
            authority: &coven_protocol::blob::RowBlobAuthority,
            stored: &coven_protocol::blob::locator::StoredBlobRef,
        ) -> String {
            match self.store.blob_protection_for_test(authority, stored).await {
                Ok(_) => panic!("invalid Circle blob authority must fail"),
                Err(error) => error,
            }
        }

        pub async fn load_circle_snapshot_refs(
            &self,
            circle_id: coven_protocol::CircleId,
            access: &coven_protocol::circle_activation::CircleEpochAccess,
        ) -> Result<
            Vec<(
                coven_protocol::store_commit::CircleSnapshotRef,
                coven_protocol::store_commit::CircleSnapshotMeta,
            )>,
            String,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| error.to_string())?
                .circles()
                .snapshots()
                .load_circle_snapshot_refs_for_test(circle_id, access)
                .await
                .map_err(|error| error.to_string())
        }

        pub async fn membership(
            &self,
        ) -> Result<coven_protocol::membership::MembershipChain, String> {
            self.store
                .membership_for_test()
                .await
                .map_err(|error| error.to_string())
        }

        pub fn protocol_root(&self) -> &coven_protocol::store_commit::StoreProtocolRoot {
            self.store.protocol_root_for_test()
        }

        #[cfg(test)]
        pub async fn prepare_wrapped_key(
            &self,
            recipient: &str,
            value: coven_protocol::wrapped_store_key::WrappedStoreKey,
        ) -> Result<coven_protocol::wrapped_store_key::PreparedWrappedStoreKey, String> {
            self.store
                .prepare_wrapped_key_for_test(recipient, value)
                .await
        }

        #[cfg(test)]
        pub async fn open_membership_keyring(
            &self,
        ) -> Result<coven_keys::encryption::EncryptionService, String> {
            self.store.open_membership_keyring_for_test().await
        }

        pub async fn publish_snapshot(
            &self,
            db_image: Vec<u8>,
            coverage: coven_protocol::store_commit::CommitFrontier,
        ) -> Result<coven_protocol::store_commit::SnapshotMeta, String> {
            self.publish_snapshot_at(db_image, coverage, "2026-07-16T00:00:00Z")
                .await
                .map_err(|error| error.to_string())
        }

        pub async fn publish_snapshot_at(
            &self,
            db_image: Vec<u8>,
            coverage: coven_protocol::store_commit::CommitFrontier,
            created_at: &str,
        ) -> Result<coven_protocol::store_commit::SnapshotMeta, crate::sync::store::SnapshotError>
        {
            self.store
                .publish_snapshot_for_test(
                    coven_database::CreatedSnapshot {
                        db_image: staged_snapshot_image(&db_image),
                        blobs: Vec::new(),
                    },
                    coverage,
                    created_at.to_string(),
                )
                .await
        }

        pub async fn resume_snapshot_publication(
            &self,
        ) -> Result<
            Option<coven_protocol::store_commit::SnapshotMeta>,
            crate::sync::store::SnapshotError,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::SnapshotError::PublicationState(error.to_string())
                })?
                .resume_snapshot_publication()
                .await
        }

        pub async fn publish_acknowledgement(
            &self,
            frontier: coven_protocol::store_commit::CommitFrontier,
        ) -> Result<(), String> {
            self.store
                .stage_acknowledgement_for_test(frontier, "2026-07-16T00:00:01Z".to_string())
                .await
                .map_err(|error| error.to_string())?;
            let published = self
                .store
                .drain_acknowledgements_for_test()
                .await
                .map_err(|error| error.to_string())?;
            if published != 1 {
                return Err(format!(
                "snapshot acknowledgement fixture published {published} acknowledgements instead of one"
            ));
            }
            Ok(())
        }

        pub async fn stage_acknowledgement(
            &self,
            frontier: coven_protocol::store_commit::CommitFrontier,
            sync_time: String,
        ) -> Result<coven_protocol::store_commit::StoreAck, String> {
            self.store
                .stage_acknowledgement_for_test(frontier, sync_time)
                .await
                .map_err(|error| error.to_string())
        }

        pub async fn materialized_frontier(
            &self,
        ) -> Result<
            std::collections::BTreeMap<String, coven_protocol::store_commit::StoreBatchCommitRef>,
            String,
        > {
            self.db
                .materialized_frontier()
                .await
                .map_err(|error| error.to_string())
        }

        pub async fn drain_acknowledgements(&self) -> Result<u64, String> {
            self.store
                .drain_acknowledgements_for_test()
                .await
                .map_err(|error| error.to_string())
        }

        #[cfg(test)]
        pub async fn stage_acknowledgement_exact(
            &self,
            frontier: coven_protocol::store_commit::CommitFrontier,
            sync_time: String,
        ) -> Result<coven_protocol::store_commit::StoreAck, crate::sync::store::StoreAckError>
        {
            self.store
                .stage_acknowledgement_for_test(frontier, sync_time)
                .await
        }

        #[cfg(test)]
        pub async fn acknowledgement_frontier(
            &self,
        ) -> Result<coven_protocol::store_commit::CommitFrontier, crate::sync::store::StoreAckError>
        {
            coven_protocol::store_commit::CommitFrontier::from_refs(
                self.db.materialized_frontier().await?,
            )
            .map_err(crate::sync::store::StoreAckError::Protocol)
        }

        #[cfg(test)]
        pub async fn stage_current_acknowledgement(
            &self,
            sync_time: &str,
        ) -> Result<coven_protocol::store_commit::StoreAck, crate::sync::store::StoreAckError>
        {
            let frontier = self.acknowledgement_frontier().await?;
            self.stage_acknowledgement_exact(frontier, sync_time.to_string())
                .await
        }

        #[cfg(any(test, feature = "test-utils"))]
        pub fn typed_device_id(&self) -> coven_protocol::store_commit::StoreDeviceId {
            self.device_id
                .parse()
                .expect("TestDevice retains a valid Store device id")
        }

        #[cfg(test)]
        pub async fn prepare_acknowledgement_candidate_for_test(
            &self,
            outbound: &coven_database::OutboundStoreAck,
        ) -> coven_protocol::prepared_commit::PreparedStoreOperationCommit {
            let mut writer = self
                .authorize_writer()
                .await
                .expect("authorize acknowledgement writer");
            let plan = writer
                .prepare_plan()
                .await
                .expect("prepare acknowledgement activation");
            plan.common()
                .validate_acknowledgement(&outbound.ack.value)
                .expect("acknowledgement matches activation predecessor");
            let candidate = writer
                .prepare_candidate(
                    plan,
                    crate::sync::store::StoreOperationBatch::Acknowledgement {
                        reference: outbound.reference.clone(),
                        value: outbound.ack.value.clone(),
                        circle_acknowledgements: Vec::new(),
                    },
                )
                .await
                .expect("prepare acknowledgement candidate");
            self.prepare_acknowledgement_activation_for_test(
                outbound.reference.clone(),
                candidate.clone(),
            )
            .await
            .expect("persist acknowledgement candidate");
            candidate
        }

        #[cfg(test)]
        pub async fn drain_acknowledgements_exact(
            &self,
        ) -> Result<u64, crate::sync::store::StoreAckError> {
            self.store.drain_acknowledgements_for_test().await
        }

        #[cfg(test)]
        pub async fn stage_circle_acknowledgements(
            &self,
            frontier: &coven_protocol::store_commit::CommitFrontier,
            sync_time: &str,
        ) -> Result<(), crate::sync::store::StoreAckError> {
            self.store
                .stage_circle_acknowledgements_for_test(frontier, sync_time)
                .await
        }

        pub async fn load_commit_ancestry_until(
            &self,
            start: coven_protocol::store_commit::StoreBatchCommitRef,
            coverage: &coven_protocol::store_commit::CommitFrontier,
        ) -> Result<
            Vec<(
                coven_protocol::store_commit::StoreBatchCommitRef,
                coven_protocol::store_commit::VerifiedStoreBatchCommit,
            )>,
            String,
        > {
            self.store
                .load_commit_ancestry_until_for_test(start, coverage)
                .await
                .map_err(|error| error.to_string())
        }

        pub async fn export_activated_device_continuation(
            &self,
        ) -> Result<coven_protocol::recovery::ActivatedContinuation, String> {
            self.store
                .export_activated_device_continuation_for_test()
                .await
                .map_err(|error| error.to_string())
        }

        pub async fn latest_store_position(
            &self,
        ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, String> {
            self.store
                .latest_local_store_position()
                .await
                .map_err(|error| error.to_string())
        }

        pub async fn pull_store(
            &self,
            store_dir: &StoreDir,
        ) -> Result<
            (
                std::collections::BTreeMap<String, u64>,
                crate::sync::store::StorePullResult,
            ),
            TestPullError,
        > {
            let routing_encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
            self.pull_store_with_encryption(store_dir, &routing_encryption)
                .await
        }

        pub async fn pull_store_with_encryption(
            &self,
            store_dir: &StoreDir,
            routing_encryption: &coven_keys::encryption::EncryptionService,
        ) -> Result<
            (
                std::collections::BTreeMap<String, u64>,
                crate::sync::store::StorePullResult,
            ),
            TestPullError,
        > {
            let store = self.store.with_test_store_dir(store_dir.clone());
            let mut authorization = store.authorize_writer().await?;
            let result = authorization.pull(Some(routing_encryption)).await?;
            let sequences = result
                .frontier
                .iter()
                .map(|(stream, reference)| (stream.clone(), reference.coord.sequence()))
                .collect();
            Ok((sequences, result))
        }
    }
}

pub use test_device::{TestDevice, TestDeviceSigningAuthority, TestStoreDir};

struct TestStoreProducers {
    unassigned: Option<TestDevice>,
    by_name: HashMap<String, TestDevice>,
}

impl TestStore {
    pub async fn bind_founder_device(
        &self,
        database: &SyntheticDatabase,
    ) -> Result<TestDevice, String> {
        self.bind_device(database, &self.signer).await
    }

    pub async fn open_store_with_identity(
        &self,
        database: &SyntheticDatabase,
        store_dir: StoreDir,
        identity: &UserKeypair,
    ) -> Result<crate::sync::store::Store, String> {
        self.open_store_with_storage(
            coven_database::StoreDatabase::new(database),
            self.storage.clone(),
            store_dir,
            identity,
        )
        .await
    }

    pub async fn open_store_with_storage(
        &self,
        database: coven_database::StoreDatabase,
        storage: Arc<dyn coven_storage::CloudSyncObjectStorage>,
        store_dir: StoreDir,
        identity: &UserKeypair,
    ) -> Result<crate::sync::store::Store, String> {
        crate::sync::store::Store::open(database, storage, store_dir, &self.root, identity)
            .await
            .map(|initialized| initialized.store)
            .map_err(|error| error.to_string())
    }

    pub async fn open_founder_store_with_storage(
        &self,
        database: coven_database::StoreDatabase,
        storage: Arc<dyn coven_storage::CloudSyncObjectStorage>,
        store_dir: StoreDir,
    ) -> Result<crate::sync::store::Store, String> {
        self.open_store_with_storage(database, storage, store_dir, &self.signer)
            .await
    }

    pub fn tombstone_deletions(&self) -> Vec<String> {
        self.home.deletes_seen()
    }

    pub fn stored_tombstone_bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.home.get(key)
    }

    pub async fn plant_tombstone_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.storage().write_provider_object(key, bytes).await
    }

    /// Plants a typed tombstone through the exact plaintext Store layout while
    /// bypassing the signing drain, so deletion tests can exercise rejected
    /// signatures and Store identities.
    pub async fn plant_tombstone(&self, tombstone: &crate::blob::delete::BlobTombstoneJson) {
        let key = exact_tombstone_key(&tombstone.stored);
        let bytes = serde_json::to_vec(tombstone).expect("serialize tombstone");
        self.plant_tombstone_bytes(&key, bytes)
            .await
            .expect("plant tombstone");
    }

    pub fn fail_exact_delete_on_call(&self, call: usize) {
        self.home.fail_exact_delete_on_call(call);
    }

    pub fn fail_nth_exact_delete_of(
        &self,
        slots: &[&coven_protocol::objects::ObjectSlot],
        call: usize,
    ) {
        self.home.fail_nth_exact_delete_of(slots, call);
    }

    pub fn sort_provider_listings(&self) {
        self.home.sort_listings();
    }

    pub fn provider_object_is_absent(&self, logical_key: &str) -> bool {
        self.home.get(logical_key).is_none()
    }

    pub fn arm_provider_write_failures(&self) {
        self.home.arm_write_failures();
    }

    pub fn fail_exact_create_before_call(&self, call: usize) {
        self.home.fail_exact_create_before_call(call);
    }

    pub fn fail_exact_create_after_call(&self, call: usize) {
        self.home.fail_exact_create_after_call(call);
    }

    pub fn pause_after_exact_create_call(
        &self,
        call: usize,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        self.home.pause_after_exact_create_call(call)
    }

    pub async fn pull_with_storage_for_test(
        &self,
        database: &SyntheticDatabase,
        storage: Arc<dyn coven_storage::CloudSyncObjectStorage>,
        store_dir: &StoreDir,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<crate::sync::store::StorePullResult, crate::sync::cycle::SyncCycleFailure> {
        let store = crate::sync::store::Store::load(
            coven_database::StoreDatabase::new(database),
            storage,
            store_dir.clone(),
            self.signer.clone(),
        )
        .await
        .map_err(|error| crate::sync::cycle::SyncCycleFailure::from(error.to_string()))?;
        store
            .authorize_writer()
            .await
            .map_err(|error| crate::sync::cycle::SyncCycleFailure::from(error.to_string()))?
            .pull(routing_encryption)
            .await
    }

    pub async fn founder_recovery_authority(
        &self,
    ) -> coven_protocol::recovery::OwnerRecoveryAuthority {
        let device = self.founder_device().await.expect("load founder Store");
        let protocol_root = device.protocol_root_for_test();
        let owner_grant = protocol_root.descriptor.founder_grant.clone();
        let activation = coven_protocol::store_commit::OwnerRecoveryActivationId::derive(
            &self.root,
            &coven_keys::keys::public_key_hex(&self.signer),
            &owner_grant,
            &protocol_root.descriptor.founder_recovery,
        )
        .expect("derive founder recovery activation");
        coven_protocol::recovery::OwnerRecoveryAuthority {
            owner_identity_secret: hex::encode(self.signer.to_keypair_bytes()),
            owner_grant: owner_grant.clone(),
            recovery: coven_protocol::store_commit::OwnerRecoveryCursor {
                owner_grant,
                position: coven_protocol::store_commit::OwnerRecoveryPosition::BeforeFirst {
                    activation,
                },
            },
            published_at: "2026-07-17T00:00:00Z".to_string(),
        }
    }

    pub async fn run_founder_cycle(
        &self,
        store_dir: &StoreDir,
        observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
    ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure> {
        self.founder.run_cycle(store_dir, observer).await
    }

    pub async fn publish_fixture_position(&self, store_dir: &StoreDir, note_id: &str) -> u64 {
        self.founder
            .publish_fixture_position(store_dir, note_id)
            .await
    }

    pub async fn create_exact_opaque_blob(
        &self,
        namespace: &str,
        id: &str,
        bytes: &[u8],
    ) -> coven_protocol::blob::locator::StoredBlobRef {
        self.founder
            .create_exact_opaque_blob(namespace, id, bytes)
            .await
    }

    pub async fn create_exact_browsable_blob(
        &self,
        namespace: &str,
        id: &str,
        cloud_path: &str,
        bytes: &[u8],
    ) -> coven_protocol::blob::locator::StoredBlobRef {
        self.founder
            .create_exact_browsable_blob(namespace, id, cloud_path, bytes)
            .await
    }

    pub async fn publish_exact_remote_blob_binding(
        &self,
        store_dir: &StoreDir,
        root_id: &str,
        row_id: &str,
        bytes: &[u8],
    ) -> coven_protocol::blob::locator::StoredBlobRef {
        self.founder
            .publish_exact_remote_blob_binding(store_dir, root_id, row_id, bytes)
            .await
    }

    pub async fn pull_into_result(
        &self,
        db: &SyntheticDatabase,
        store_dir: &StoreDir,
    ) -> Result<
        (
            std::collections::BTreeMap<String, u64>,
            crate::sync::store::StorePullResult,
        ),
        TestPullError,
    > {
        let device = Box::pin(self.open_into(db))
            .await
            .map_err(TestPullError::Open)?;
        device.pull_store(store_dir).await
    }

    pub async fn pull_into(
        &self,
        db: &SyntheticDatabase,
        store_dir: &StoreDir,
    ) -> (
        std::collections::BTreeMap<String, u64>,
        crate::sync::store::StorePullResult,
    ) {
        self.pull_into_result(db, store_dir)
            .await
            .expect("pull exact test Store")
    }

    pub async fn promote_active_member_fixture(
        &self,
        owner_db: &SyntheticDatabase,
        member_db: &SyntheticDatabase,
        owner: &UserKeypair,
        member: &UserKeypair,
        encryption: &coven_keys::encryption::EncryptionService,
    ) -> Result<coven_protocol::circle_control::StoreMembershipStateRef, String> {
        let owner_device = self.bind_device(owner_db, owner).await?;
        let member_device = self.bind_device(member_db, member).await?;
        let request = owner_device
            .begin_owner_promotion_for_device(member_device.typed_device_id())
            .await
            .map_err(|error| format!("begin Owner promotion: {error}"))?;
        let acceptance = member_device
            .accept_owner_promotion(request)
            .await
            .map_err(|error| format!("accept Owner promotion: {error}"))?;
        let finalized = owner_device
            .finalize_owner_promotion(encryption, acceptance)
            .await
            .map_err(|error| format!("finalize Owner promotion: {error}"))?;
        let (_temp, store_dir) = temp_store_dir();
        let (_, pull) = member_device
            .pull_store_with_encryption(&store_dir, encryption)
            .await
            .map_err(|error| error.to_string())?;
        if !pull.held_positions.is_empty() {
            return Err(format!(
                "Owner promotion pull held signed positions: {:?}",
                pull.held_positions
            ));
        }
        Ok(finalized)
    }

    fn storage_for_device(
        &self,
        identity: UserKeypair,
    ) -> Result<std::sync::Arc<coven_storage::CloudSyncConnection>, String> {
        if identity.public_key() == self.signer.public_key() {
            return Ok(self.storage.clone());
        }
        coven_storage::CloudSyncConnection::new(
            self.home.clone(),
            self.storage.cipher_snapshot(),
            self.storage.blob_path_scheme(),
            self.storage.store_id(),
            identity,
        )
        .map(std::sync::Arc::new)
        .map_err(|error| error.to_string())
    }

    pub async fn create(
        db: &SyntheticDatabase,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<Arc<Self>, String> {
        Box::pin(Self::create_with_protection(
            db,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Encrypted(
                coven_keys::encryption::EncryptionService::from_key([42; 32]),
            ),
            coven_storage::BlobPathScheme::Hashed,
        ))
        .await
    }

    pub async fn create_encrypted(
        db: &SyntheticDatabase,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
        encryption: coven_keys::encryption::EncryptionService,
    ) -> Result<Arc<Self>, String> {
        Self::create_with_protection(
            db,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Encrypted(encryption),
            coven_storage::BlobPathScheme::Hashed,
        )
        .await
    }

    pub async fn create_with_database(
        database: coven_database::StoreDatabase,
        store_dir: TestStoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<Arc<Self>, String> {
        Box::pin(Self::create_with_protection_database(
            database,
            store_dir,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Encrypted(
                coven_keys::encryption::EncryptionService::from_key([42; 32]),
            ),
            coven_storage::BlobPathScheme::Hashed,
        ))
        .await
    }

    /// A store whose home keeps blobs **browsable**: stored in the clear under
    /// readable paths. The counterpart of [`Self::create`], whose home is opaque
    /// (sealed under the store key, hashed paths). The pair is fixed per home,
    /// so a test that needs the browsable verification story needs this store.
    pub async fn create_browsable(
        db: &SyntheticDatabase,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<Arc<Self>, String> {
        Box::pin(Self::create_with_protection(
            db,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Plaintext,
            coven_storage::BlobPathScheme::Plain,
        ))
        .await
    }

    async fn create_with_protection(
        db: &SyntheticDatabase,
        store_id: &str,
        signer: UserKeypair,
        home: std::sync::Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
        cipher: coven_storage::CloudCipher,
        blob_paths: coven_storage::BlobPathScheme,
    ) -> Result<Arc<Self>, String> {
        Self::create_with_protection_database(
            coven_database::StoreDatabase::new(db),
            TestStoreDir::from_database(db),
            store_id,
            signer,
            home,
            cipher,
            blob_paths,
        )
        .await
    }

    async fn create_with_protection_database(
        database: coven_database::StoreDatabase,
        store_dir: TestStoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: std::sync::Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
        cipher: coven_storage::CloudCipher,
        blob_paths: coven_storage::BlobPathScheme,
    ) -> Result<Arc<Self>, String> {
        let storage = std::sync::Arc::new(
            coven_storage::CloudSyncConnection::new(
                home.clone(),
                cipher,
                blob_paths,
                store_id,
                signer.clone(),
            )
            .map_err(|error| error.to_string())?,
        );
        let founder = TestDevice::create_with_database(
            database,
            store_dir,
            storage.clone(),
            store_id,
            signer.clone(),
        )
        .await?;
        let root = founder.store_root().clone();
        let founder_store_dir = founder.test_store_dir().clone();
        Ok(Arc::new(Self {
            home,
            storage,
            root,
            signer,
            founder: founder.clone(),
            producers: Arc::new(tokio::sync::Mutex::new(TestStoreProducers {
                unassigned: Some(founder),
                by_name: HashMap::new(),
            })),
            founder_store_dir,
        }))
    }

    pub fn protocol_founder_pubkey(&self) -> String {
        coven_keys::keys::public_key_hex(&self.signer)
    }

    /// The storage handle tests hand to code that takes a [`CloudSyncObjectStorage`].
    pub fn storage(&self) -> std::sync::Arc<coven_storage::CloudSyncConnection> {
        self.storage.clone()
    }

    pub async fn create_exact_protocol_object(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
        bytes: &[u8],
    ) -> Result<coven_protocol::objects::ExactObjectRef, String> {
        let slot = self
            .storage
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
            .map_err(|error| error.to_string())?;
        let prepared = self
            .storage
            .prepare_protocol_object(context, slot, semantic_prefix, bytes.to_vec())
            .map_err(|error| error.to_string())?;
        self.storage
            .create_protocol_object(&prepared)
            .await
            .map_err(|error| error.to_string())?;
        Ok(prepared.reference().clone())
    }

    /// Publish one object from the bytes and reference that identify it, for
    /// tests holding a candidate that carries references rather than uploads.
    pub async fn publish_exact_protocol_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
        bytes: Vec<u8>,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        let prepared = coven_protocol::objects::PreparedExactObject::new(object.clone(), bytes)?;
        self.storage.create_protocol_object(&prepared).await
    }

    pub async fn publish_prepared_protocol_object(
        &self,
        prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.storage.create_protocol_object(prepared).await
    }

    pub async fn read_exact_protocol_object(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        object: &coven_protocol::objects::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        self.storage
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    pub async fn contains_blob_object(&self, reference: &coven_protocol::blob::RowBlobRef) -> bool {
        match reference.stored() {
            Some(stored) => self
                .contains_stored_blob_object(stored)
                .await
                .unwrap_or_else(|error| panic!("verify exact blob object: {error}")),
            None => false,
        }
    }

    pub async fn contains_stored_blob_object(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, coven_protocol::objects::StorageError> {
        match self.storage.verify_blob_object(stored).await {
            Ok(()) => Ok(true),
            Err(coven_protocol::objects::StorageError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn contains_blob_tombstone(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, coven_storage::cloud::CloudHomeError> {
        let key =
            crate::blob::delete::tombstone_key_for_test(stored, &self.storage.cipher_snapshot());
        coven_storage::cloud::CloudHome::exists(self.home.as_ref(), &key).await
    }

    pub async fn contains_circle_snapshot_image(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        meta: &coven_protocol::store_commit::CircleSnapshotMeta,
    ) -> Result<bool, String> {
        let access = self
            .founder
            .circle_epoch_access(circle_id, meta.control.clone())
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "the Circle snapshot control has no retained access".to_string())?;
        let context = access.protocol_context(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CircleSnapshotImage,
        );
        let prefix = coven_protocol::store_commit::semantic_prefix_from_exact_object(
            &meta.bootstrap.image.object,
            coven_protocol::objects::ProtectedObjectDomain::CircleSnapshotImage.extension(),
        )
        .map_err(|error| error.to_string())?;
        match self
            .storage
            .read_protocol_object(&context, &meta.bootstrap.image.object, &prefix)
            .await
        {
            Ok(_) => Ok(true),
            Err(coven_protocol::objects::StorageError::NotFound(_)) => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }

    pub async fn circle_package_in(
        &self,
        commit_ref: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> coven_protocol::store_commit::CirclePackageRef {
        let commit = self
            .founder
            .load_commit_for_test(commit_ref)
            .await
            .expect("load the exact Circle package commit");
        let [package] = commit.value().circle_packages() else {
            panic!("the commit must carry exactly one Circle package");
        };
        package.clone()
    }

    pub async fn circle_package_object_present(
        &self,
        package: &coven_protocol::store_commit::CirclePackageRef,
        activation: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> bool {
        let access = self
            .founder
            .circle_epoch_access(package.circle_id, package.control.clone())
            .await
            .expect("resolve Circle package access")
            .expect("the package's control stays retained after its epoch closed");
        let context = access.protocol_context(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CirclePackage,
        );
        let prefix = coven_protocol::store_commit::circle_package_semantic_prefix(
            package.circle_id,
            package.package.candidate_family,
            &activation.coord.stream_id.to_string(),
            activation.coord.sequence(),
            package.package.content_hash,
        );
        match self
            .storage
            .read_protocol_object(&context, &package.package.object, &prefix)
            .await
        {
            Ok(_) => true,
            Err(coven_protocol::objects::StorageError::NotFound(_)) => false,
            Err(error) => panic!("read the exact Circle package object: {error}"),
        }
    }

    pub async fn publish_competing_store_head(
        &self,
        journal: &coven_protocol::circle_journal::CircleOperationJournal,
    ) -> (
        coven_protocol::objects::ExactObjectRef,
        coven_protocol::objects::ExactObjectRef,
    ) {
        let candidate = journal.commit().expect("parse candidate Store commit");
        let coord = journal.operation().commit_ref.coord.clone();
        let head = &journal.operation().policy.head;
        let registration = self
            .founder
            .activated_store_device_registration_for_test(candidate.author_registration.clone())
            .await
            .expect("load candidate author registration");
        let device_signer = registration
            .value()
            .device_signer(&self.signer)
            .expect("derive candidate device signer");
        let schema_version = self.founder.schema_version();
        let package = coven_protocol::audience_package::AudiencePackage::store(
            self.root.store_root_hash,
            candidate.candidate_family(),
            candidate.write_id.clone(),
            coord.clone(),
            schema_version,
            b"competing valid package".to_vec(),
            Vec::new(),
        )
        .expect("construct competing package");
        let package_bytes = package.to_bytes();
        let package_prefix = coven_protocol::store_commit::package_semantic_prefix(
            candidate.candidate_family(),
            &coord.stream_id.to_string(),
            candidate.seq(),
            coven_protocol::store_commit::ObjectHash::digest(&package_bytes),
        );
        let package_context = coven_protocol::objects::ProtocolObjectContext::store_encrypted(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StorePackage,
        );
        let package_slot = self
            .storage
            .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
            .await
            .expect("reserve competing package slot");
        let package_prepared = self
            .storage
            .prepare_protocol_object(
                &package_context,
                package_slot,
                &package_prefix,
                package_bytes.clone(),
            )
            .expect("prepare competing package");
        self.storage
            .create_protocol_object(&package_prepared)
            .await
            .expect("publish competing package");
        let membership = self
            .founder
            .membership()
            .await
            .expect("load competing commit membership");
        let predecessor = membership
            .write_grant_authority(&registration.value().author_pubkey)
            .expect("competing author has an active write grant");
        let winner = coven_protocol::store_commit::StoreBatchCommit::signed_operations(
            self.root.store_root_hash,
            candidate.write_id.clone(),
            coord.clone(),
            candidate.author_registration.clone(),
            registration.value(),
            candidate.order.clone(),
            candidate.membership_state.clone(),
            candidate.device_state.clone(),
            coven_protocol::store_commit::StoreOperationMembershipAuthority { predecessor },
            coven_protocol::store_commit::StoreCommitOperationsInput {
                store_package: Some(coven_protocol::store_commit::StorePackageInput {
                    candidate_family: candidate.candidate_family(),
                    schema_version,
                    bytes: &package_bytes,
                    object: package_prepared.reference().clone(),
                }),
                ..coven_protocol::store_commit::StoreCommitOperationsInput::empty()
            },
            &device_signer,
        )
        .expect("sign competing commit");
        let commit_prefix = coven_protocol::store_commit::commit_semantic_prefix(
            winner.candidate_family(),
            &coord.stream_id.to_string(),
            winner.seq(),
            winner.commit_hash(),
        );
        let commit_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreCommit,
        );
        let commit_slot = self
            .storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .expect("reserve competing commit slot");
        let commit_prepared = self
            .storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                winner.to_bytes(),
            )
            .expect("prepare competing commit");
        self.storage
            .create_protocol_object(&commit_prepared)
            .await
            .expect("publish competing commit");
        let winner_ref = coven_protocol::store_commit::StoreBatchCommitRef::from_commit(
            &winner,
            coord,
            commit_prepared.reference().clone(),
        )
        .expect("reference competing commit");
        assert_ne!(winner_ref, journal.operation().commit_ref);
        let winner_head = coven_protocol::store_commit::StoreDeviceHead::signed(
            self.root.store_root_hash,
            candidate.author_registration.clone(),
            winner_ref,
            head.successor.clone(),
            &device_signer,
        )
        .expect("sign competing head");
        let head_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let head_slot = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .expect("candidate carries a prepared Store head")
            .slot()
            .clone();
        let head_prefix = coven_protocol::store_commit::head_slot_prefix(
            &candidate.author_registration.device_id.to_string(),
            candidate.seq(),
        );
        let head_prepared = self
            .storage
            .prepare_protocol_object(
                &head_context,
                head_slot,
                &head_prefix,
                winner_head.to_bytes(),
            )
            .expect("prepare competing head");
        self.storage
            .create_protocol_object(&head_prepared)
            .await
            .expect("publish competing head");
        (
            commit_prepared.reference().clone(),
            head_prepared.reference().clone(),
        )
    }

    pub async fn publish_third_candidate_winner(
        &self,
        peer_db: &SyntheticDatabase,
        candidate: &coven_database::BlockedMergeCandidate,
    ) {
        let registration = coven_database::StoreDatabase::new(peer_db)
            .activated_store_device_registration(
                candidate.commit.value().author_registration.clone(),
            )
            .await
            .expect("load third-winner device registration");
        let device_signer = registration
            .value()
            .device_signer(&self.signer)
            .expect("derive third-winner device signer");
        let coord = candidate.head.commit.coord.clone();
        let candidate_family = candidate.commit.value().candidate_family();
        let package = coven_protocol::audience_package::AudiencePackage::store(
            self.root.store_root_hash,
            candidate_family,
            candidate.commit.value().write_id.clone(),
            coord.clone(),
            peer_db.schema_version(),
            b"third winner package".to_vec(),
            Vec::new(),
        )
        .expect("construct third winner package");
        let coven_protocol::store_commit::StoreCommitCoord {
            stream_id,
            sequence,
        } = coord.clone();
        let package_bytes = package.to_bytes();
        let package_context = coven_protocol::objects::ProtocolObjectContext::store_encrypted(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StorePackage,
        );
        let package_prefix = coven_protocol::store_commit::package_semantic_prefix(
            candidate_family,
            &stream_id.to_string(),
            sequence,
            coven_protocol::store_commit::ObjectHash::digest(&package_bytes),
        );
        let package_slot = self
            .storage
            .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
            .await
            .expect("allocate third winner package slot");
        let package_prepared = self
            .storage
            .prepare_protocol_object(
                &package_context,
                package_slot,
                &package_prefix,
                package_bytes.clone(),
            )
            .expect("prepare third winner package");
        let third = coven_protocol::store_commit::StoreBatchCommit::signed_operations(
            self.root.store_root_hash,
            candidate.commit.value().write_id.clone(),
            coord.clone(),
            candidate.commit.value().author_registration.clone(),
            registration.value(),
            candidate.commit.value().order.clone(),
            candidate.commit.value().membership_state.clone(),
            candidate.commit.value().device_state.clone(),
            candidate
                .commit
                .value()
                .operations_membership_authority()
                .expect("load third winner membership authority"),
            coven_protocol::store_commit::StoreCommitOperationsInput {
                store_package: Some(coven_protocol::store_commit::StorePackageInput {
                    candidate_family,
                    schema_version: peer_db.schema_version(),
                    bytes: &package_bytes,
                    object: package_prepared.reference().clone(),
                }),
                ..coven_protocol::store_commit::StoreCommitOperationsInput::empty()
            },
            &device_signer,
        )
        .expect("sign third ordinary winner");
        let commit_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreCommit,
        );
        let commit_prefix = coven_protocol::store_commit::commit_semantic_prefix(
            third.candidate_family(),
            &stream_id.to_string(),
            sequence,
            third.commit_hash(),
        );
        let commit_slot = self
            .storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .expect("allocate third winner commit slot");
        let third_prepared = self
            .storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                third.to_bytes(),
            )
            .expect("prepare third winner commit");
        self.storage
            .create_protocol_object(&third_prepared)
            .await
            .expect("publish third winner commit");
        let third_ref = coven_protocol::store_commit::StoreBatchCommitRef::from_commit(
            &third,
            coord,
            third_prepared.reference().clone(),
        )
        .expect("reference third winner commit");
        let third_head = coven_protocol::store_commit::StoreDeviceHead::signed(
            self.root.store_root_hash,
            candidate.commit.value().author_registration.clone(),
            third_ref,
            candidate.head.successor.clone(),
            &device_signer,
        )
        .expect("sign third winner head");
        let head_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let head_prefix = coven_protocol::store_commit::head_slot_prefix(
            &candidate
                .commit
                .value()
                .author_registration
                .device_id
                .to_string(),
            sequence,
        );
        let head_prepared = self
            .storage
            .prepare_protocol_object(
                &head_context,
                candidate.head_object.slot().clone(),
                &head_prefix,
                third_head.to_bytes(),
            )
            .expect("prepare third winner head");
        self.storage
            .create_protocol_object(&head_prepared)
            .await
            .expect("publish third winner head");
    }

    pub async fn overwrite_membership_head(
        &self,
        reference: &coven_protocol::membership::MembershipHeadRef,
        head: &coven_protocol::membership::AuthorHead,
    ) {
        self.storage
            .delete_protocol_object(&reference.object)
            .await
            .expect("delete exact head before replacement");
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreMembershipHead,
        );
        let prefix = coven_protocol::store_commit::membership_head_slot_prefix(
            &reference.coord.author_pubkey,
            &reference.coord.author_owner_grant,
            reference.coord.stream_id,
            reference.coord.seq,
        );
        let prepared = self
            .storage
            .prepare_protocol_object(
                &context,
                reference.object.slot().clone(),
                &prefix,
                serde_json::to_vec(head).expect("serialize replacement head"),
            )
            .expect("prepare replacement head");
        self.storage
            .create_protocol_object(&prepared)
            .await
            .expect("write replacement head");
    }

    pub async fn delete_membership_head_for_test(
        &self,
        reference: &coven_protocol::membership::MembershipHeadRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.storage.delete_protocol_object(&reference.object).await
    }

    pub async fn pending_device_join_observation(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        offer: &coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<crate::sync::store::PendingDeviceJoinObservation<'_>, String> {
        self.founder
            .pending_device_join_observation_for_test(pending, offer)
            .await
    }

    pub async fn open_pending_device_join(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        identity: &UserKeypair,
        offer: coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<crate::sync::store::PendingDeviceJoinAuthority<'_>, String> {
        self.founder
            .open_pending_device_join_for_test(pending, identity, offer)
            .await
    }

    pub async fn prepare_snapshot_bootstrap<'a>(
        &'a self,
        membership_floor: &coven_protocol::membership::MembershipFloor,
        binary_schema_version: u32,
        target_path: &std::path::Path,
        restorer_identity: &UserKeypair,
    ) -> Result<crate::sync::store::PreparedSnapshotBootstrap<'a>, crate::sync::store::SnapshotError>
    {
        self.founder
            .prepare_snapshot_bootstrap_for_test(
                membership_floor,
                binary_schema_version,
                target_path,
                restorer_identity,
            )
            .await
    }

    pub async fn bind_device(
        &self,
        db: &SyntheticDatabase,
        identity: &UserKeypair,
    ) -> Result<TestDevice, String> {
        TestDevice::load_with_database(
            coven_database::StoreDatabase::new(db),
            self.storage_for_device(identity.clone())?,
            identity.clone(),
            TestStoreDir::from_database(db),
        )
        .await
        .map_err(|error| error.to_string())
    }

    pub async fn drain_uploads(
        &self,
        database: &coven_database::StoreDatabase,
        store_dir: &coven_foundation::store_dir::StoreDir,
        clock: &dyn coven_foundation::clock::Clock,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
    ) -> Result<coven_protocol::blob::DrainOutcome, coven_database::DbError> {
        let store = self
            .bind_store_device(database, &self.signer)
            .await
            .map_err(coven_database::DbError::Message)?;
        store
            .drain_uploads(store_dir, clock, routing_encryption, observer)
            .await
    }

    pub async fn activate_joined_device(
        &self,
        observer_db: &SyntheticDatabase,
        joining_db: &SyntheticDatabase,
        joining_identity: &UserKeypair,
        published_at: &str,
    ) -> Result<TestDevice, String> {
        let observer = self.bind_device(observer_db, &self.signer).await?;
        self.activate_joined_device_with_observer(
            observer,
            joining_db,
            joining_identity,
            published_at,
        )
        .await
    }

    async fn activate_joined_device_with_observer(
        &self,
        observer: TestDevice,
        joining_db: &SyntheticDatabase,
        joining_identity: &UserKeypair,
        published_at: &str,
    ) -> Result<TestDevice, String> {
        let joining_database = coven_database::StoreDatabase::new(joining_db);
        let activated_database = joining_database.clone();
        let pending_dir = tempfile::tempdir().map_err(|error| error.to_string())?;
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .map_err(|error| error.to_string())?;
        let offer = observer
            .begin_device_join(&pubkey_hex(joining_identity))
            .await
            .map_err(|error| format!("begin device join: {error}"))?;
        let mut pending_join = observer
            .open_pending_device_join_for_test(&pending, joining_identity, offer)
            .await
            .map_err(|error| format!("open pending device join: {error}"))?;
        let access_request = pending_join
            .prepare_provider_access_request()
            .await
            .map_err(|error| format!("prepare provider access request: {error}"))?;
        let approval = observer
            .authorize_device_provider_access(access_request, None)
            .await
            .map_err(|error| format!("authorize device provider access: {error}"))?;
        let registration_request = pending_join
            .prepare_registration_request(approval)
            .await
            .map_err(|error| format!("prepare device registration request: {error}"))?;
        let provisional = observer
            .accept_device_registration_request(registration_request)
            .await
            .map_err(|error| format!("accept device registration request: {error}"))?;
        let provider_ready = observer
            .publish_device_provider_challenge(provisional)
            .await
            .map_err(|error| format!("publish device provider challenge: {error}"))?;
        let bootstrap_store_dir = TestStoreDir::from_database(joining_db);
        let mut joining = pending_join
            .begin_joining_store(joining_database, bootstrap_store_dir.dir())
            .await
            .map_err(|error| format!("begin joining Store: {error}"))?;
        let routing_encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        let bootstrap_pull = joining
            .pull_store_history(Some(&routing_encryption))
            .await
            .map_err(|error| format!("pull joining Store history: {error}"))?;
        if !bootstrap_pull.held_positions.is_empty() {
            return Err(format!(
                "device join bootstrap pull held signed positions: {:?}",
                bootstrap_pull.held_positions
            ));
        }
        let readiness = joining
            .bootstrap(provider_ready, published_at)
            .await
            .map_err(|error| format!("bootstrap joining Store: {error}"))?;
        let completion = observer
            .complete_device_provider_admission(readiness)
            .await
            .map_err(|error| format!("complete device provider admission: {error}"))?;
        let activation = observer
            .finalize_device_join(completion)
            .await
            .map_err(|error| format!("finalize device join: {error}"))?;
        joining
            .complete(activation)
            .await
            .map_err(|error| format!("complete joining Store: {error}"))?;
        TestDevice::load_with_database(
            activated_database,
            self.storage_for_device(joining_identity.clone())?,
            joining_identity.clone(),
            bootstrap_store_dir,
        )
        .await
        .map_err(|error| error.to_string())
    }

    pub async fn bind_store_device(
        &self,
        database: &coven_database::StoreDatabase,
        identity: &UserKeypair,
    ) -> Result<TestDevice, String> {
        if identity.public_key() != self.signer.public_key() {
            return Err("custom Store database binding requires the founder identity".to_string());
        }
        TestDevice::load_with_database(
            database.clone(),
            self.storage_for_device(identity.clone())?,
            identity.clone(),
            self.founder_store_dir.clone(),
        )
        .await
        .map_err(|error| error.to_string())
    }

    pub async fn invite_member(
        &self,
        db: &SyntheticDatabase,
        identity: &UserKeypair,
        member_pubkey: &str,
        invitee_email: Option<&str>,
        role: coven_protocol::membership::MemberRole,
        encryption: &coven_keys::encryption::EncryptionService,
        store_name: &str,
    ) -> Result<crate::sync::store::MemberInvitation, crate::sync::store::MembershipOpsError> {
        let device = self.bind_device(db, identity).await.map_err(|error| {
            crate::sync::store::MembershipOpsError::Chain(
                crate::sync::store::AnchoredChainError::LoadFailed(error),
            )
        })?;
        device
            .invite_member(
                member_pubkey,
                invitee_email,
                role,
                encryption,
                self.storage.store_id(),
                store_name,
            )
            .await
    }

    pub async fn invite_and_activate_peer(
        &self,
        observer_db: &SyntheticDatabase,
        peer_db: &SyntheticDatabase,
        peer: &UserKeypair,
    ) -> Result<TestDevice, String> {
        self.invite_member(
            observer_db,
            &self.signer,
            &pubkey_hex(peer),
            None,
            coven_protocol::membership::MemberRole::Member,
            &coven_keys::encryption::EncryptionService::from_key([42; 32]),
            "Test Store",
        )
        .await
        .map_err(|error| format!("invite peer identity: {error}"))?;
        self.activate_joined_device(observer_db, peer_db, peer, "2026-07-16T00:00:00Z")
            .await
    }

    pub async fn remove_member(
        &self,
        db: &SyntheticDatabase,
        identity: &UserKeypair,
        member_pubkey: &str,
        encryption: &coven_keys::encryption::EncryptionService,
        master_keys: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<String, crate::sync::store::MembershipOpsError> {
        let device = self.bind_device(db, identity).await.map_err(|error| {
            crate::sync::store::MembershipOpsError::Chain(
                crate::sync::store::AnchoredChainError::LoadFailed(error),
            )
        })?;
        device
            .remove_member(
                member_pubkey,
                encryption,
                master_keys,
                self.storage.as_ref(),
                self.storage.as_ref(),
            )
            .await
    }

    pub async fn device_id(&self, name: &str) -> Result<String, String> {
        Ok(self.ensure_producer(name).await?.device_id)
    }

    pub async fn founder_device(&self) -> Result<TestDevice, String> {
        Ok(self.founder.clone())
    }

    pub async fn next_commit_sequence(&self, name: &str) -> Result<u64, String> {
        self.ensure_producer(name)
            .await?
            .latest_local_store_position()
            .await
            .map_err(|error| error.to_string())?
            .map_or(Ok(1), |reference| {
                reference
                    .coord
                    .sequence()
                    .checked_add(1)
                    .ok_or_else(|| "test producer sequence exhausted u64".to_string())
            })
    }

    pub async fn founder_device_authority(&self) -> Result<TestDeviceSigningAuthority, String> {
        let device = self.ensure_producer("founder").await?;
        device.device_authority_for_test().await
    }

    async fn ensure_producer(&self, name: &str) -> Result<TestDevice, String> {
        {
            let producers = self.producers.lock().await;
            if let Some(producer) = producers.by_name.get(name) {
                return Ok(producer.clone());
            }
        }

        let unassigned = {
            let mut producers = self.producers.lock().await;
            producers.unassigned.take()
        };
        let producer = match unassigned {
            Some(producer) => producer,
            None => {
                let db = open_test_db();
                let observer = {
                    let producers = self.producers.lock().await;
                    producers
                        .by_name
                        .values()
                        .next()
                        .ok_or_else(|| "test Store has no active device observer".to_string())?
                        .clone()
                };
                self.activate_joined_device_with_observer(
                    observer,
                    &db,
                    &self.signer,
                    "2026-07-16T00:00:00Z",
                )
                .await?
            }
        };
        let mut producers = self.producers.lock().await;
        if producers
            .by_name
            .insert(name.to_string(), producer)
            .is_some()
        {
            return Err(format!("test producer {name:?} was registered twice"));
        }
        Ok(producers
            .by_name
            .get(name)
            .expect("inserted test producer exists")
            .clone())
    }

    pub async fn open_into(&self, db: &SyntheticDatabase) -> Result<TestDevice, String> {
        TestDevice::open_with_database(
            coven_database::StoreDatabase::new(db),
            TestStoreDir::from_database(db),
            self.storage_for_device(self.signer.clone())?,
            &self.root,
            &self.signer,
        )
        .await
    }

    pub async fn open_into_store_database(
        &self,
        database: &coven_database::StoreDatabase,
    ) -> Result<TestDevice, String> {
        TestDevice::open_with_database(
            database.clone(),
            self.founder_store_dir.clone(),
            self.storage_for_device(self.signer.clone())?,
            &self.root,
            &self.signer,
        )
        .await
    }

    pub async fn publish_pending(
        &self,
        db: &SyntheticDatabase,
        store_dir: &StoreDir,
    ) -> Result<bool, String> {
        self.publish_pending_store_database(&coven_database::StoreDatabase::new(db), store_dir)
            .await
    }

    pub async fn publish_pending_store_database(
        &self,
        database: &coven_database::StoreDatabase,
        store_dir: &StoreDir,
    ) -> Result<bool, String> {
        let device = self.bind_store_device(database, &self.signer).await?;
        device.publish_pending_store_database(store_dir).await
    }

    #[cfg(test)]
    pub fn install_cross_principal_device<'a>(
        &'a self,
        local_database: coven_database::StoreDatabase,
        identity: &'a UserKeypair,
        peer_account_id: &'a str,
        published_at: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + 'a>> {
        Box::pin(async move {
            let observer = self.founder.clone();
            let provider_binding =
                coven_storage::CloudSyncObjectStorage::provider_binding(&*self.storage)
                    .await
                    .map_err(|error| error.to_string())?;
            let coven_protocol::objects::StoreProviderBinding::Dropbox { namespace_id } =
                &provider_binding.store
            else {
                return Err("cross-principal test Store is not Dropbox".to_string());
            };
            let namespace_id = namespace_id.clone();
            let peer_binding = coven_protocol::objects::ResolvedProviderBinding {
                store: provider_binding.store.clone(),
                device: coven_protocol::objects::ProviderDeviceBinding {
                    principal: coven_protocol::objects::ProviderPrincipalId::Dropbox {
                        account_id: peer_account_id.to_string(),
                    },
                },
            };
            let peer_home = std::sync::Arc::new(
                self.home
                    .as_ref()
                    .clone()
                    .with_provider_binding(peer_binding),
            );
            let peer_storage: std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage> =
                std::sync::Arc::new(
                    coven_storage::CloudSyncConnection::new(
                        peer_home.clone(),
                        coven_storage::CloudCipher::Encrypted(
                            coven_keys::encryption::EncryptionService::from_key([42; 32]),
                        ),
                        coven_storage::BlobPathScheme::Hashed,
                        "cross-principal-test-store",
                        identity.clone(),
                    )
                    .map_err(|error| error.to_string())?,
                );
            let pending_dir = tempfile::tempdir().map_err(|error| error.to_string())?;
            let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
                pending_dir.path().join("pending-device-join.sqlite"),
            )
            .map_err(|error| error.to_string())?;
            let offer = observer
                .begin_device_join(&pubkey_hex(identity))
                .await
                .map_err(|error| error.to_string())?;
            let join_history =
                crate::sync::store::HistoryConstructionAuthority::for_pending_device_join()
                    .open_pinned(peer_storage.as_ref(), &offer.store_root)
                    .await
                    .map_err(|error| error.to_string())?;
            let observation = crate::sync::store::PendingDeviceJoinObservation::new(
                &pending,
                &peer_storage,
                join_history,
                offer.attempt_id,
            );
            let mut pending_join =
                crate::sync::store::PendingDeviceJoinAuthority::open(observation, identity, offer)
                    .await
                    .map_err(|error| error.to_string())?;
            let access_request = pending_join
                .prepare_provider_access_request()
                .await
                .map_err(|error| error.to_string())?;
            let access_administrator = TestDropboxAccessAdministrator { namespace_id };
            let approval = observer
                .authorize_device_provider_access(access_request, Some(&access_administrator))
                .await
                .map_err(|error| error.to_string())?;
            if !matches!(
                approval.admission,
                coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionChallenge::CrossPrincipal(_)
            ) {
                return Err(
                    "distinct provider principals produced same-principal admission".into(),
                );
            }
            let registration_request = pending_join
                .prepare_registration_request(approval)
                .await
                .map_err(|error| error.to_string())?;
            let provisional = observer
                .accept_device_registration_request(registration_request)
                .await
                .map_err(|error| error.to_string())?;
            let provider_ready = observer
                .publish_device_provider_challenge(provisional)
                .await
                .map_err(|error| error.to_string())?;
            let (_store_dir_temp, store_dir) = temp_store_dir();
            let mut joining = pending_join
                .begin_joining_store(local_database, &store_dir)
                .await
                .map_err(|error| error.to_string())?;
            let readiness = joining
                .bootstrap(provider_ready, published_at)
                .await
                .map_err(|error| error.to_string())?;
            if !matches!(
                readiness.provider,
                coven_protocol::store_commit::device_join_exchange::DeviceProviderReadiness::CrossPrincipal(_)
            ) {
                return Err(
                    "distinct provider principals produced same-principal readiness".into(),
                );
            }
            let completion = observer
                .complete_device_provider_admission(readiness)
                .await
                .map_err(|error| error.to_string())?;
            if !matches!(
                completion.admission,
                coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission::CrossPrincipal(_)
            ) {
                return Err(
                    "distinct provider principals produced same-principal completion".into(),
                );
            }
            let activation = observer
                .finalize_device_join(completion)
                .await
                .map_err(|error| error.to_string())?;
            joining
                .complete(activation)
                .await
                .map_err(|error| error.to_string())?;
            Ok(())
        })
    }

    #[cfg(test)]
    pub async fn push_circle_snapshots(
        &self,
        db: &SyntheticDatabase,
        temp_dir: std::path::PathBuf,
        schema_version: u32,
        created_at: &str,
        store_routing: &coven_keys::encryption::EncryptionService,
    ) -> Result<coven_protocol::store_commit::CircleSnapshotMeta, crate::sync::store::SnapshotError>
    {
        self.bind_device(db, &self.signer)
            .await
            .map_err(crate::sync::store::SnapshotError::PublicationState)?
            .authorize_writer()
            .await
            .map_err(|error| {
                crate::sync::store::SnapshotError::PublicationState(error.to_string())
            })?
            .circles()
            .snapshots()
            .author_one_circle_snapshot_for_test(
                temp_dir,
                schema_version,
                created_at,
                store_routing,
            )
            .await
    }

    #[cfg(test)]
    pub async fn load_circle_snapshot_metas(
        &self,
        db: &SyntheticDatabase,
        circle_id: coven_protocol::circle::CircleId,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
    ) -> Result<
        Vec<coven_protocol::store_commit::CircleSnapshotMeta>,
        crate::sync::store::SnapshotError,
    > {
        self.bind_device(db, &self.signer)
            .await
            .map_err(crate::sync::store::SnapshotError::PublicationState)?
            .authorize_writer()
            .await
            .map_err(|error| {
                crate::sync::store::SnapshotError::PublicationState(error.to_string())
            })?
            .circles()
            .snapshots()
            .load_circle_snapshot_metas_for_test(circle_id, access)
            .await
    }

    #[cfg(test)]
    pub async fn verify_standalone_circle_snapshot_image(
        &self,
        db: &SyntheticDatabase,
        circle_id: coven_protocol::circle::CircleId,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
        store_routing: &coven_keys::encryption::EncryptionService,
    ) -> Result<(), crate::sync::store::SnapshotError> {
        self.bind_device(db, &self.signer)
            .await
            .map_err(crate::sync::store::SnapshotError::PublicationState)?
            .authorize_writer()
            .await
            .map_err(|error| {
                crate::sync::store::SnapshotError::PublicationState(error.to_string())
            })?
            .circles()
            .snapshots()
            .verify_standalone_circle_snapshot_image_for_test(circle_id, access, store_routing)
            .await
    }

    #[cfg(test)]
    pub async fn circle_snapshot_is_stable(
        &self,
        db: &SyntheticDatabase,
        circle_id: coven_protocol::circle::CircleId,
        snapshot_cut: &coven_protocol::store_commit::CommitFrontier,
    ) -> Result<bool, crate::sync::store::SnapshotError> {
        self.bind_device(db, &self.signer)
            .await
            .map_err(crate::sync::store::SnapshotError::PublicationState)?
            .authorize_writer()
            .await
            .map_err(|error| {
                crate::sync::store::SnapshotError::PublicationState(error.to_string())
            })?
            .circles()
            .snapshots()
            .circle_snapshot_is_stable(circle_id, snapshot_cut)
            .await
    }

    #[cfg(test)]
    pub async fn load_circle_acknowledgement(
        &self,
        db: &SyntheticDatabase,
        reference: &coven_protocol::store_commit::CircleAckRef,
    ) -> Result<coven_protocol::store_commit::CircleAck, crate::sync::store::StoreAckError> {
        self.bind_device(db, &self.signer)
            .await
            .map_err(crate::sync::store::StoreAckError::InvalidOutbound)?
            .load_circle_acknowledgement_for_test(reference)
            .await
    }

    #[cfg(test)]
    pub async fn read_circle_snapshot_image(
        &self,
        selected: &coven_protocol::store_commit::CircleSnapshotMeta,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        let context = access.protocol_context(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CircleSnapshotImage,
        );
        self.storage
            .read_protocol_object(
                &context,
                &selected.bootstrap.image.object,
                &coven_protocol::store_commit::circle_snapshot_image_semantic_prefix(
                    selected.circle_id,
                    &selected.author_registration.device_id.to_string(),
                    selected.bootstrap.image.image_hash,
                ),
            )
            .await
    }

    #[cfg(test)]
    pub async fn circle_snapshot_meta_is_unreadable(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        encryption: coven_keys::encryption::EncryptionService,
    ) -> bool {
        let context = coven_protocol::objects::ProtocolObjectContext::circle(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CircleSnapshotMeta,
            encryption,
        );
        let prefix = coven_protocol::store_commit::circle_snapshot_slot_prefix(
            circle_id,
            &self.founder.device_id,
            0,
        );
        let slot = coven_protocol::objects::ObjectSlot::logical(format!("{prefix}.json"))
            .expect("valid generation-zero Circle snapshot slot");
        self.storage
            .read_protocol_slot(&context, &slot, &prefix)
            .await
            .is_err()
    }

    #[cfg(test)]
    pub fn store_root_hash(&self) -> coven_protocol::store_commit::ObjectHash {
        self.root.store_root_hash
    }

    #[cfg(test)]
    pub async fn publish_changeset(
        &self,
        name: &str,
        sequence: u64,
        changeset: &[u8],
        schema_version: u32,
    ) -> Result<coven_protocol::store_commit::StoreBatchCommitRef, String> {
        let device = self.ensure_producer(name).await?;
        device
            .publish_changeset_for_test(sequence, changeset.to_vec(), schema_version)
            .await
    }

    #[cfg(test)]
    pub async fn publish_founder_changeset(
        &self,
        store_dir: &StoreDir,
        changeset: Vec<u8>,
        previous_sequence: u64,
    ) -> Result<coven_protocol::store_commit::StoreBatchCommitRef, String> {
        self.founder
            .publish_changeset_after_for_test(store_dir, changeset, previous_sequence)
            .await
    }
}

/// A plaintext cloud cipher — the default for tests that are not exercising
/// sealing.
#[cfg(test)]
pub fn plaintext_cipher() -> std::sync::RwLock<coven_storage::CloudCipher> {
    std::sync::RwLock::new(coven_storage::CloudCipher::Plaintext)
}

/// The cloud key a tombstone for `stored` is written under.
#[cfg(any(test, feature = "test-utils"))]
pub fn exact_tombstone_key(stored: &coven_protocol::blob::locator::StoredBlobRef) -> String {
    crate::blob::delete::tombstone_key_for_test(stored, &coven_storage::CloudCipher::Plaintext)
}

/// Which protocol read an interceptor hook is running ahead of.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolRead {
    Object,
    Slot,
    PreparedSlot,
}

#[cfg(any(test, feature = "test-utils"))]
pub enum ProviderObjectExistsInterception {
    Proceed,
    DeleteAndReportAbsent,
}

/// Test-side observation of a [`CloudSyncObjectStorage`] call.
///
/// Every hook runs before the wrapped storage does the work, and returning `Err`
/// fails the call without reaching it. All hooks default to doing nothing, so an
/// interceptor states only the operations its test is about — which is the point:
/// a test that intercepts two reads should not also have to restate the sixteen
/// operations it does not care about.
#[cfg(any(test, feature = "test-utils"))]
#[async_trait::async_trait]
pub trait StorageInterceptor: Send + Sync {
    async fn before_protocol_create(
        &self,
        _prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_protocol_read(
        &self,
        _read: ProtocolRead,
        _semantic_prefix: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_allocate(&self) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_prepare(&self) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_create(
        &self,
        _blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_stage(&self) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_provider_object_read(
        &self,
        _key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_provider_object_write(
        &self,
        _key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_provider_object_exists(
        &self,
        _key: &str,
    ) -> Result<ProviderObjectExistsInterception, coven_protocol::objects::StorageError> {
        Ok(ProviderObjectExistsInterception::Proceed)
    }

    async fn before_provider_object_delete(
        &self,
        _key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl<T> StorageInterceptor for std::sync::Arc<T>
where
    T: StorageInterceptor + ?Sized,
{
    async fn before_protocol_create(
        &self,
        prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_protocol_create(prepared).await
    }

    async fn before_protocol_read(
        &self,
        read: ProtocolRead,
        semantic_prefix: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_protocol_read(read, semantic_prefix).await
    }

    async fn before_blob_allocate(&self) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_blob_allocate().await
    }

    async fn before_blob_prepare(&self) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_blob_prepare().await
    }

    async fn before_blob_create(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_blob_create(blob).await
    }

    async fn before_blob_stage(&self) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_blob_stage().await
    }

    async fn before_provider_object_read(
        &self,
        key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_provider_object_read(key).await
    }

    async fn before_provider_object_write(
        &self,
        key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_provider_object_write(key).await
    }

    async fn before_provider_object_exists(
        &self,
        key: &str,
    ) -> Result<ProviderObjectExistsInterception, coven_protocol::objects::StorageError> {
        (**self).before_provider_object_exists(key).await
    }

    async fn before_provider_object_delete(
        &self,
        key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_provider_object_delete(key).await
    }
}

/// A [`CloudSyncObjectStorage`] that forwards every call to `inner`, giving `interceptor`
/// its chance first.
#[cfg(any(test, feature = "test-utils"))]
pub struct InterceptedStorage<S, I: StorageInterceptor>
where
    S: std::ops::Deref,
{
    inner: S,
    interceptor: I,
}

#[cfg(any(test, feature = "test-utils"))]
impl<S, I> coven_storage::CloudSyncCipherStateAccess for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: coven_storage::CloudSyncCipherStateAccess,
    I: StorageInterceptor,
{
    fn snapshot(&self) -> coven_storage::CloudCipher {
        self.inner.snapshot()
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &coven_keys::encryption::EncryptionService,
        custody: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<Option<String>, coven_keys::keys::KeyError> {
        self.inner.merge_key_rotation(new_encryption, custody)
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl<S, I> coven_storage::CloudSyncRotationStateAccess for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: coven_storage::CloudSyncRotationStateAccess,
    I: StorageInterceptor,
{
    fn mark_candidate(
        &self,
        generation: u64,
        mutation: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        self.inner.mark_candidate(generation, mutation)
    }

    fn mark_committed_mutation(
        &self,
        generation: u64,
        mutation: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        self.inner.mark_committed_mutation(generation, mutation)
    }

    fn remove_candidate(
        &self,
        generation: u64,
        mutation: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        self.inner.remove_candidate(generation, mutation)
    }

    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: coven_protocol::store_commit::ObjectHash,
        replacement: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        self.inner
            .replace_candidate_mutation(generation, previous, replacement)
    }

    fn gate(&self) -> Option<coven_protocol::objects::RotationGate> {
        self.inner.gate()
    }

    fn install_durable_gate(&self, gate: Option<coven_protocol::objects::RotationGate>) {
        self.inner.install_durable_gate(gate);
    }

    fn check(
        &self,
        cipher: &coven_storage::CloudCipher,
    ) -> Result<(), coven_protocol::objects::RotationPending> {
        self.inner.check(cipher)
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl<S, I> crate::sync::cycle::CloudSyncCycleConnection for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: crate::sync::cycle::CloudSyncCycleConnection,
    I: StorageInterceptor,
{
}

#[cfg(any(test, feature = "test-utils"))]
impl<S, I: StorageInterceptor> InterceptedStorage<S, I>
where
    S: std::ops::Deref,
{
    pub fn new(inner: S, interceptor: I) -> Self {
        Self { inner, interceptor }
    }

    pub fn interceptor(&self) -> &I {
        &self.interceptor
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait::async_trait]
impl<S, I> coven_storage::CloudSyncObjectStorage for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: coven_storage::CloudSyncObjectStorage,
    I: StorageInterceptor,
{
    fn blob_path_scheme(&self) -> coven_storage::BlobPathScheme {
        self.inner.blob_path_scheme()
    }

    async fn probe_provider(&self) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner.probe_provider().await
    }

    async fn set_member_access(
        &self,
        state: coven_storage::cloud::CloudAccessState,
    ) -> Result<coven_storage::cloud::CloudAccessOutcome, coven_protocol::objects::StorageError>
    {
        self.inner.set_member_access(state).await
    }

    async fn read_provider_object(
        &self,
        key: &str,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        self.interceptor.before_provider_object_read(key).await?;
        self.inner.read_provider_object(key).await
    }

    async fn write_provider_object(
        &self,
        key: &str,
        stored_bytes: Vec<u8>,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.interceptor.before_provider_object_write(key).await?;
        self.inner.write_provider_object(key, stored_bytes).await
    }

    async fn list_provider_objects(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, coven_protocol::objects::StorageError> {
        self.inner.list_provider_objects(prefix).await
    }

    async fn provider_object_exists(
        &self,
        key: &str,
    ) -> Result<bool, coven_protocol::objects::StorageError> {
        match self.interceptor.before_provider_object_exists(key).await? {
            ProviderObjectExistsInterception::Proceed => {
                self.inner.provider_object_exists(key).await
            }
            ProviderObjectExistsInterception::DeleteAndReportAbsent => {
                self.inner.delete_provider_object(key).await?;
                Ok(false)
            }
        }
    }

    async fn delete_provider_object(
        &self,
        key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.interceptor.before_provider_object_delete(key).await?;
        self.inner.delete_provider_object(key).await
    }

    async fn reserve_cross_principal_response_slot(
        &self,
        probe_id: coven_protocol::provider::ProviderProbeId,
    ) -> Result<coven_protocol::objects::ObjectSlot, coven_protocol::provider::ProviderProbeError>
    {
        self.inner
            .reserve_cross_principal_response_slot(probe_id)
            .await
    }

    async fn prepare_cross_principal_challenge(
        &self,
        publication_journal: &dyn coven_protocol::provider::DeviceJoinChallengePublicationJournal,
        probe_id: coven_protocol::provider::ProviderProbeId,
        store: &coven_protocol::StoreProviderBinding,
        context: &coven_protocol::provider::CrossPrincipalChallengeContext,
        administrator_signer: &dyn coven_keys::keys::DeviceSigningAuthority,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeChallenge,
        coven_protocol::provider::ProviderProbeError,
    > {
        self.inner
            .prepare_cross_principal_challenge(
                publication_journal,
                probe_id,
                store,
                context,
                administrator_signer,
            )
            .await
    }

    async fn settle_cross_principal_challenge(
        &self,
        publication_journal: &dyn coven_protocol::provider::DeviceJoinChallengePublicationJournal,
        authorization: &coven_protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
        context: &coven_protocol::provider::CrossPrincipalChallengeContext,
        store: &coven_protocol::StoreProviderBinding,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeChallenge,
        coven_protocol::provider::ProviderProbeError,
    > {
        self.inner
            .settle_cross_principal_challenge(
                publication_journal,
                authorization,
                challenge,
                context,
                store,
            )
            .await
    }

    async fn create_cross_principal_response(
        &self,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
        context: &coven_protocol::provider::CrossPrincipalResponseContext,
        store: &coven_protocol::StoreProviderBinding,
        administrator_signing_pubkey: &str,
        peer_signer: &coven_keys::keys::UserKeypair,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeResponse,
        coven_protocol::provider::ProviderProbeError,
    > {
        self.inner
            .create_cross_principal_response(
                challenge,
                context,
                store,
                administrator_signing_pubkey,
                peer_signer,
            )
            .await
    }

    async fn complete_cross_principal_probe(
        &self,
        journal: &dyn coven_protocol::provider::ProviderProbeJournal,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
        response: &coven_protocol::provider::CrossPrincipalProbeResponse,
        context: &coven_protocol::provider::CrossPrincipalResponseContext,
        store: &coven_protocol::StoreProviderBinding,
        administrator_signer: &dyn coven_keys::keys::DeviceSigningAuthority,
        peer_signing_pubkey: &str,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeReceipt,
        coven_protocol::provider::ProviderProbeError,
    > {
        self.inner
            .complete_cross_principal_probe(
                journal,
                challenge,
                response,
                context,
                store,
                administrator_signer,
                peer_signing_pubkey,
            )
            .await
    }

    async fn probe_exact_slots(
        &self,
        journal: &dyn coven_protocol::provider::ProviderProbeJournal,
        probe_id: coven_protocol::provider::ProviderProbeId,
        binding: &coven_protocol::objects::ResolvedProviderBinding,
    ) -> Result<
        coven_protocol::provider::ExactSlotProbeReceipt,
        coven_protocol::provider::ProviderProbeError,
    > {
        self.inner
            .probe_exact_slots(journal, probe_id, binding)
            .await
    }

    async fn observe_exact_slot(
        &self,
        slot: &coven_protocol::objects::ObjectSlot,
    ) -> Result<
        Option<coven_protocol::objects::ExactObjectRef>,
        coven_protocol::objects::StorageError,
    > {
        self.inner.observe_exact_slot(slot).await
    }

    async fn delete_exact_slot_and_verify_absent(
        &self,
        slot: &coven_protocol::objects::ObjectSlot,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner.delete_exact_slot_and_verify_absent(slot).await
    }

    fn store_blob_protection(
        &self,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, coven_protocol::objects::StorageError>
    {
        self.inner.store_blob_protection()
    }

    async fn provider_binding(
        &self,
    ) -> Result<
        coven_protocol::objects::ResolvedProviderBinding,
        coven_protocol::objects::StorageError,
    > {
        self.inner.provider_binding().await
    }

    async fn allocate_protocol_slot(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<coven_protocol::objects::ObjectSlot, coven_protocol::objects::StorageError> {
        self.inner
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
    }

    fn prepare_protocol_object(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        slot: coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<coven_protocol::objects::PreparedExactObject, coven_protocol::objects::StorageError>
    {
        self.inner
            .prepare_protocol_object(context, slot, semantic_prefix, data)
    }

    async fn open_prepared_protocol_object(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        prepared: &coven_protocol::objects::PreparedExactObject,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        self.inner
            .open_prepared_protocol_object(context, prepared, semantic_prefix)
            .await
    }

    async fn create_protocol_object(
        &self,
        prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.interceptor.before_protocol_create(prepared).await?;
        self.inner.create_protocol_object(prepared).await
    }

    async fn read_protocol_object(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        object: &coven_protocol::objects::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        self.interceptor
            .before_protocol_read(ProtocolRead::Object, semantic_prefix)
            .await?;
        self.inner
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    async fn read_protocol_slot(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        slot: &coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        (Vec<u8>, coven_protocol::objects::ExactObjectRef),
        coven_protocol::objects::StorageError,
    > {
        self.interceptor
            .before_protocol_read(ProtocolRead::Slot, semantic_prefix)
            .await?;
        self.inner
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        slot: &coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        (Vec<u8>, coven_protocol::objects::PreparedExactObject),
        coven_protocol::objects::StorageError,
    > {
        self.interceptor
            .before_protocol_read(ProtocolRead::PreparedSlot, semantic_prefix)
            .await?;
        self.inner
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn delete_protocol_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner.delete_protocol_object(object).await
    }

    async fn allocate_blob_slot(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
    ) -> Result<coven_protocol::objects::ObjectSlot, coven_protocol::objects::StorageError> {
        self.interceptor.before_blob_allocate().await?;
        self.inner.allocate_blob_slot(locator, authority).await
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        protection: coven_protocol::objects::BlobSpoolProtection,
        plaintext_file: &std::path::Path,
        spool: coven_foundation::local_file::AtomicStagedFile,
    ) -> Result<coven_protocol::objects::BlobSpoolWrite, coven_protocol::objects::StorageError>
    {
        self.inner
            .seal_blob_to_spool(locator, authority, protection, plaintext_file, spool)
            .await
    }

    async fn prepare_blob_object(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        slot: coven_protocol::objects::ObjectSlot,
        stored_file: &std::path::Path,
    ) -> Result<coven_protocol::blob::locator::StoredBlobRef, coven_protocol::objects::StorageError>
    {
        self.interceptor.before_blob_prepare().await?;
        self.inner
            .prepare_blob_object(locator, authority, slot, stored_file)
            .await
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        stored_file: &std::path::Path,
        progress: &coven_storage::cloud::UploadProgress<'_>,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.interceptor.before_blob_create(blob).await?;
        self.inner
            .create_blob_object_from_file(blob, authority, stored_file, progress)
            .await
    }

    async fn verify_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner.verify_blob_object(blob).await
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: coven_protocol::objects::BlobSpoolProtection,
        stage: coven_foundation::local_file::AtomicStagedFile,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, coven_protocol::objects::StorageError>
    {
        self.interceptor.before_blob_stage().await?;
        self.inner
            .stage_verified_blob_plaintext(blob, protection, stage)
            .await
    }

    async fn open_blob_range_reader(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: coven_protocol::objects::BlobSpoolProtection,
    ) -> Result<coven_storage::BlobRangeReader, coven_protocol::objects::StorageError> {
        self.inner.open_blob_range_reader(blob, protection).await
    }

    async fn delete_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner.delete_blob_object(blob).await
    }
}
