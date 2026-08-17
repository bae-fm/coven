use super::*;
use coven_database::BlockedWriteDiscard;
use coven_protocol::store_commit::StoreRootRef;
use std::sync::Arc;

mod authorized_store;
mod candidate_cleanup;
pub(crate) mod history;
mod history_construction;
pub(crate) mod keyring;
pub(crate) use keyring::load_wrapped_store_key;
mod registration;
pub(crate) mod registration_outbox;

mod store_test_support;

use crate::sync::store::device_join::transport;
pub(crate) use authorized_store::AuthorizedStore;
pub(crate) use candidate_cleanup::delete_candidate_cleanup_targets;
use history::AuthorizedStoreHistory;
pub use history_construction::HistoryConstructionAuthority;
pub use keyring::StoreKeyrings;
pub use registration::StoreRegistrationError;
use registration_outbox::RegistrationOutbox;

#[doc(hidden)]
pub struct Store {
    database: StoreDatabase,
    storage: Arc<dyn CloudSyncObjectStorage>,
    store_dir: StoreDir,
    blob_cache: crate::sync::store::blob::StoreBlobCache,
    identity: UserKeypair,
    device_id: Option<String>,
    root: crate::sync::store::protocol_root::VerifiedStoreRoot,
}

#[doc(hidden)]
pub struct StoreRestoreMembership {
    pub store_root: StoreRootRef,
    pub founder_pubkey: String,
    pub membership_floor: coven_protocol::membership::MembershipFloor,
}

pub(crate) struct InitializedStore {
    store: Store,
    device_id: String,
}

impl InitializedStore {
    pub(crate) fn new(store: Store, device_id: String) -> Self {
        Self { store, device_id }
    }

    pub(crate) fn into_parts(self) -> (Store, String) {
        (self.store, self.device_id)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreInitializationError {
    #[error("Store protocol root failed: {0}")]
    ProtocolRoot(#[from] crate::sync::store::protocol_root::StoreProtocolRootError),
    #[error("Store history verification failed: {0}")]
    History(#[from] crate::sync::store::pull::StorePullError),
    #[error("Store initialization database state failed: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("membership chain bootstrap/anchor failed: {0}")]
    MembershipAnchor(#[from] crate::sync::store::membership::AnchoredChainError),
    #[error("Store founder device installation failed: {0}")]
    Registration(#[from] crate::sync::store::authorization::registration::StoreRegistrationError),
    #[error("opening a Store for a non-founder requires an installed local device")]
    NonFounderDeviceMissing,
    #[error("initialized Store has no local device registration id")]
    LocalDeviceMissing,
    #[error("Store founder state is invalid: {0}")]
    FounderState(String),
    #[error("Store founder rollback failed: {0}")]
    FounderRollback(#[from] crate::sync::store::founder_creation::FounderRollbackError),
    #[error(transparent)]
    FounderPublicationRollback(
        #[from] crate::sync::store::founder_creation::FounderPublicationRollback,
    ),
}

impl Store {
    pub(crate) fn device_join_transport(&self) -> transport::StoreDeviceJoinTransport<'_> {
        transport::StoreDeviceJoinTransport::new(self)
    }

    pub(crate) async fn allocate_device_join_transport_bundle(
        &self,
        offer: coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<transport::DeviceJoinOfferBundle, transport::DeviceJoinTransportError> {
        let attempt_namespace = transport::attempt_namespace(offer.attempt_id);
        let context = transport::slot_context(offer.store_root.store_root_hash);
        let mut slots = std::collections::BTreeMap::new();
        let allocations = futures_util::future::join_all(
            transport::DeviceJoinTransportKind::ALL
                .into_iter()
                .map(|kind| {
                    let context = &context;
                    let attempt_namespace = &attempt_namespace;
                    async move {
                        self.storage
                            .allocate_protocol_slot(
                                context,
                                &transport::semantic_prefix(attempt_namespace, kind),
                                ".json",
                            )
                            .await
                            .map(|slot| (kind, slot))
                    }
                }),
        )
        .await;
        for allocation in allocations {
            let (kind, slot) = allocation?;
            slots.insert(kind, slot);
        }
        Ok(transport::DeviceJoinOfferBundle {
            version: coven_protocol::store_commit::STORE_PROTOCOL_VERSION,
            offer,
            transport: transport::DeviceJoinTransportParams::new(
                attempt_namespace,
                slots,
                coven_keys::encryption::MasterKeyring::generate(),
            ),
        })
    }

    pub(crate) async fn publish_device_join_transport_artifact(
        &self,
        bundle: &transport::DeviceJoinOfferBundle,
        roles: transport::DeviceJoinRoles,
        action: &crate::sync::store::DeviceJoinAction,
    ) -> Result<(), transport::DeviceJoinTransportError> {
        transport::DeviceJoinTransport::open(self.storage.as_ref(), bundle, roles)?
            .publish(action)
            .await
    }

    pub(crate) async fn await_device_join_transport_artifact<T: transport::DeviceJoinArtifact>(
        &self,
        bundle: &transport::DeviceJoinOfferBundle,
        roles: transport::DeviceJoinRoles,
        timing: transport::DeviceJoinTransportTiming,
    ) -> Result<T, transport::DeviceJoinTransportError> {
        transport::DeviceJoinTransport::open(self.storage.as_ref(), bundle, roles)?
            .await_artifact::<T>(timing)
            .await
    }

    pub(crate) async fn device_join_transport_status(
        &self,
        attempt_id: coven_protocol::store_commit::DeviceJoinAttemptId,
        role: crate::sync::store::DeviceJoinRole,
    ) -> Result<Option<crate::sync::store::DeviceJoinStatus>, transport::DeviceJoinTransportError>
    {
        Ok(self.database.device_join_status(attempt_id, role).await?)
    }

    pub(crate) async fn device_join_transport_roles(
        &self,
        offer: &coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<transport::DeviceJoinRoles, transport::DeviceJoinTransportError> {
        let local = self
            .database
            .local_activated_registration_ref()
            .await
            .map_err(crate::sync::store::DeviceJoinError::from)?
            .ok_or(crate::sync::store::DeviceJoinError::ActiveDeviceRequired)?;
        let roles = transport::DeviceJoinRoles::admitting(
            local == offer.owner_registration,
            local == offer.provider_admin.administrator,
        );
        if !roles.any() {
            return Err(crate::sync::store::DeviceJoinError::ActiveDeviceRequired.into());
        }
        Ok(roles)
    }

    pub(crate) fn circles(&self) -> StoreCircleCommands<'_> {
        StoreCircleCommands::new(self)
    }

    fn local_author_pubkey(&self) -> String {
        coven_keys::keys::public_key_hex(&self.identity)
    }

    #[doc(hidden)]
    pub(crate) fn host_write_blob_staging(
        &self,
        runtime: tokio::runtime::Handle,
    ) -> HostWriteBlobStaging {
        HostWriteBlobStaging::new(
            runtime,
            Arc::clone(&self.storage),
            self.root.reference().clone(),
            self.store_dir.clone(),
        )
    }

    pub(crate) async fn create(
        database: StoreDatabase,
        storage: Arc<dyn CloudSyncObjectStorage>,
        store_dir: StoreDir,
        founder_timestamp: &str,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let blob_cache =
            crate::sync::store::blob::StoreBlobCache::new(database.clone(), store_dir.clone());
        crate::sync::store::founder_creation::FounderStoreCreation::begin(
            database,
            storage,
            &store_dir,
            blob_cache,
            founder_timestamp,
            identity,
        )
        .await
        .execute()
        .await
    }

    pub(crate) async fn open(
        database: StoreDatabase,
        storage: Arc<dyn CloudSyncObjectStorage>,
        store_dir: StoreDir,
        expected_root: &StoreRootRef,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let root = crate::sync::store::protocol_root::VerifiedStoreRoot::open(
            &database,
            &*storage,
            expected_root,
        )
        .await?;
        let authority = HistoryConstructionAuthority::store();
        let history_verifier = authority
            .bind_verified(storage.as_ref(), root.clone())
            .await?;
        let blob_source = crate::sync::store::blob::RemoteBlobSource::authorized(
            database.clone(),
            storage.as_ref(),
            root.reference().clone(),
        );
        let keyrings = keyring::StoreKeyrings::new(storage.as_ref(), root.reference().clone());
        let blob_cache =
            crate::sync::store::blob::StoreBlobCache::new(database.clone(), store_dir.clone());
        AuthorizedStoreHistory::new(
            database,
            &storage,
            &store_dir,
            blob_cache,
            history_verifier,
            blob_source,
            keyrings,
        )
        .finish_initialization(identity)
        .await
    }

    #[doc(hidden)]
    pub async fn load(
        database: StoreDatabase,
        storage: Arc<dyn CloudSyncObjectStorage>,
        store_dir: StoreDir,
        identity: UserKeypair,
    ) -> Result<Self, StoreError> {
        let store_root =
            database
                .local_store_root_ref()
                .await?
                .ok_or(StoreError::MissingState {
                    key: commit_plan::STORE_ROOT_AUTHORITY,
                })?;
        let root = crate::sync::store::protocol_root::VerifiedStoreRoot::open(
            &database,
            &*storage,
            &store_root,
        )
        .await
        .map_err(StoreError::from)?;
        let device_id = database
            .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?;
        Ok(Self::new(
            database, storage, store_dir, identity, device_id, root,
        ))
    }

    fn new(
        database: StoreDatabase,
        storage: Arc<dyn CloudSyncObjectStorage>,
        store_dir: StoreDir,
        identity: UserKeypair,
        device_id: Option<String>,
        root: crate::sync::store::protocol_root::VerifiedStoreRoot,
    ) -> Self {
        let blob_cache =
            crate::sync::store::blob::StoreBlobCache::new(database.clone(), store_dir.clone());
        Self {
            database,
            storage,
            store_dir,
            blob_cache,
            identity,
            device_id,
            root,
        }
    }
    pub(crate) fn store_root(&self) -> &StoreRootRef {
        self.root.reference()
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.storage.blob_path_scheme()
    }

    pub(crate) async fn circle_close_status(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<coven_protocol::circle::CircleCloseStatus, CircleOperationError> {
        let (current, _) = self
            .database
            .circle_closing_context(circle_id, &self.local_author_pubkey())
            .await?;
        let coven_protocol::circle::CircleControlState::EpochClose(close) =
            current.control.value.state()
        else {
            return Err(CircleOperationError::InvalidState(
                "Circle close-status inspection received an active control".to_string(),
            ));
        };
        let context = coven_protocol::objects::ProtocolObjectContext::store_encrypted(
            current.control.value.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CircleEpochCloseResponse,
        );
        let mut participants = Vec::with_capacity(close.participants.len());
        for participant in &close.participants {
            let prefix = coven_protocol::circle::circle_epoch_close_response_semantic_prefix(
                current.control.value.circle_id,
                close.close_id,
                participant.registration.device_id,
            );
            let settlement = match self
                .storage
                .read_protocol_slot(&context, &participant.response_slot, &prefix)
                .await
            {
                Ok((bytes, _)) => {
                    match coven_protocol::circle::CircleEpochCloseResponseSlotValue::parse(&bytes)?
                    {
                        coven_protocol::circle::CircleEpochCloseResponseSlotValue::Response(_) => {
                            coven_protocol::circle::CircleCloseSettlement::Responded
                        }
                        coven_protocol::circle::CircleEpochCloseResponseSlotValue::Exclusion(_) => {
                            coven_protocol::circle::CircleCloseSettlement::Excluded
                        }
                    }
                }
                Err(coven_protocol::objects::StorageError::NotFound(_)) => {
                    coven_protocol::circle::CircleCloseSettlement::Pending
                }
                Err(error) => {
                    return Err(coven_protocol::objects::StoreObjectError::from(error).into())
                }
            };
            participants.push(coven_protocol::circle::CircleCloseParticipant {
                device_id: participant.registration.device_id,
                settlement,
            });
        }
        Ok(coven_protocol::circle::CircleCloseStatus {
            circle_id,
            close_id: close.close_id,
            participants,
        })
    }

    #[doc(hidden)]
    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<Vec<coven_protocol::write::WriteId>, crate::sync::store::StoreError> {
        if let BlockedWriteDiscard::Discarded(discarded) =
            self.database.discard_blocked_write(&write_id).await?
        {
            return Ok(discarded);
        }

        match self.abandon_merge_candidate(write_id.clone()).await? {
            crate::sync::store::merge_conflict::MergeCandidateAbandonment::NotRequired => {
                return Err(StoreError::InvalidOutbound(
                    "blocked Merge candidate has no abandonment authority".to_string(),
                ));
            }
            crate::sync::store::merge_conflict::MergeCandidateAbandonment::Abandoned => {}
            crate::sync::store::merge_conflict::MergeCandidateAbandonment::CandidateActivated => {
                return Err(StoreError::InvalidOutbound(
                    "Merge candidate activated before abandonment and cannot be discarded"
                        .to_string(),
                ));
            }
        }

        match self.database.discard_blocked_write(&write_id).await? {
            BlockedWriteDiscard::Discarded(discarded) => Ok(discarded),
            BlockedWriteDiscard::RemoteResolutionRequired => Err(StoreError::InvalidOutbound(
                "Merge candidate remains unresolved after abandonment".to_string(),
            )),
        }
    }

    pub(crate) async fn propose_device_exclusion_for_device(
        &self,
        device_id: coven_protocol::store_commit::StoreDeviceId,
    ) -> Result<
        coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        device_exclusion::StoreDeviceExclusionError,
    > {
        let mut writer = self.authorize_exclusion_writer().await?;
        device_exclusion::propose_for_device(&self.database, &mut writer, device_id).await
    }

    pub(crate) async fn cancel_device_exclusion_proposal(
        &self,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), device_exclusion::StoreDeviceExclusionError> {
        let mut writer = self.authorize_exclusion_writer().await?;
        device_exclusion::cancel_proposal(&mut writer, proposal).await
    }

    pub(crate) async fn finalize_device_exclusion_proposal(
        &self,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), device_exclusion::StoreDeviceExclusionError> {
        let mut writer = self.authorize_exclusion_writer().await?;
        device_exclusion::finalize_proposal(&mut writer, proposal).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn propose_device_exclusion(
        &self,
        target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        device_exclusion::StoreDeviceExclusionResult,
        device_exclusion::StoreDeviceExclusionError,
    > {
        let mut writer = self.authorize_exclusion_writer().await?;
        writer.device_exclusion().propose(target).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn cancel_device_exclusion(
        &self,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        device_exclusion::StoreDeviceExclusionResult,
        device_exclusion::StoreDeviceExclusionError,
    > {
        let mut writer = self.authorize_exclusion_writer().await?;
        writer.device_exclusion().cancel(proposal).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn finalize_device_exclusion(
        &self,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        device_exclusion::StoreDeviceExclusionResult,
        device_exclusion::StoreDeviceExclusionError,
    > {
        let mut writer = self.authorize_exclusion_writer().await?;
        writer.device_exclusion().exclude(proposal).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn device_exclusion_operations_for_test(
        &self,
    ) -> Result<
        Vec<device_exclusion::StoreDeviceExclusionOperationInfo>,
        device_exclusion::StoreDeviceExclusionError,
    > {
        device_exclusion::operations_for_test(&self.database).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn stage_uploaded_device_exclusion_proposal_for_test(
        &self,
    ) -> Result<
        coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        device_exclusion::StoreDeviceExclusionError,
    > {
        let mut writer = self.authorize_exclusion_writer().await?;
        device_exclusion::stage_uploaded_proposal_for_test(&self.database, &mut writer).await
    }

    async fn authorize_exclusion_writer(
        &self,
    ) -> Result<AuthorizedWriterOperation<'_>, device_exclusion::StoreDeviceExclusionError> {
        self.authorize_writer()
            .await
            .map_err(StoreError::from)
            .map_err(device_exclusion::StoreDeviceExclusionError::from)
    }

    pub(crate) async fn abandon_merge_candidate(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<crate::sync::store::merge_conflict::MergeCandidateAbandonment, StoreError> {
        if self.device_id.is_none() {
            let mut authority = self.authorize_history().await.map_err(StoreError::from)?;
            return authority
                .abandon_excluded_merge_candidate(write_id)
                .await?
                .ok_or_else(|| {
                    StoreError::InvalidOutbound(
                        "unregistered Store cannot publish Merge abandonment authority".to_string(),
                    )
                });
        }
        let mut writer = self.authorize_writer().await.map_err(StoreError::from)?;
        writer.abandon_merge_candidate(write_id).await
    }

    #[doc(hidden)]
    pub async fn members(
        &self,
    ) -> Result<Vec<coven_protocol::membership::MemberInfo>, membership::MembershipOpsError> {
        let authorization = self
            .authorize()
            .await
            .map_err(StoreError::from)
            .map_err(membership::MembershipOpsError::from)?;
        authorization.members(Some(&self.identity.public_key()))
    }

    #[doc(hidden)]
    pub async fn membership_conflict(
        &self,
    ) -> Result<
        Option<coven_protocol::membership::MembershipConflictInfo>,
        membership::MembershipOpsError,
    > {
        let authorization = self
            .authorize()
            .await
            .map_err(StoreError::from)
            .map_err(membership::MembershipOpsError::from)?;
        Ok(authorization.membership_conflict(Some(&self.identity.public_key())))
    }

    pub(crate) async fn resolve_membership_conflict(
        &self,
        choice: &coven_protocol::membership::MembershipConflictChoice,
        created_at: &str,
    ) -> Result<
        coven_protocol::membership::StoreMembershipConflictResolutionRef,
        membership::MembershipOpsError,
    > {
        let mut authorization = self
            .authorize_writer()
            .await
            .map_err(StoreError::from)
            .map_err(membership::MembershipOpsError::from)?;
        authorization
            .resolve_membership_conflict(choice, created_at)
            .await
    }

    #[doc(hidden)]
    pub async fn restore_membership(
        &self,
    ) -> Result<StoreRestoreMembership, membership::MembershipOpsError> {
        let authorization = self
            .authorize()
            .await
            .map_err(StoreError::from)
            .map_err(membership::MembershipOpsError::from)?;
        authorization.restore_membership()
    }

    async fn authorize_history(&self) -> Result<AuthorizedStoreHistory<'_>, SyncCycleFailure> {
        let authority = HistoryConstructionAuthority::store();
        let history_verifier = authority
            .bind_verified(self.storage.as_ref(), self.root.clone())
            .await
            .map_err(|error| SyncCycleFailure::operation("open Store history authority", error))?;
        let blob_source = crate::sync::store::blob::RemoteBlobSource::authorized(
            self.database.clone(),
            self.storage.as_ref(),
            self.root.reference().clone(),
        );
        let keyrings =
            keyring::StoreKeyrings::new(self.storage.as_ref(), self.root.reference().clone());
        Ok(AuthorizedStoreHistory::new(
            self.database.clone(),
            &self.storage,
            &self.store_dir,
            self.blob_cache.clone(),
            history_verifier,
            blob_source,
            keyrings,
        ))
    }

    pub(crate) async fn authorize(&self) -> Result<AuthorizedStore<'_>, SyncCycleFailure> {
        self.authorize_history()
            .await?
            .authorize_store(&self.identity, self.device_id.as_deref())
            .await
    }

    pub(crate) async fn authorize_writer(
        &self,
    ) -> Result<
        AuthorizedWriterOperation<'_>,
        crate::sync::store::commit_publication::StoreWriterAuthorizationError,
    > {
        RegistrationOutbox::new(self.database.clone(), &*self.storage)
            .drain()
            .await
            .map_err(
                crate::sync::store::commit_publication::StoreWriterAuthorizationError::Registration,
            )?;
        self.authorize()
            .await
            .map_err(crate::sync::store::commit_publication::StoreWriterAuthorizationError::StoreAuthority)?
            .into_writer()
            .await
            .map_err(crate::sync::store::commit_publication::StoreWriterAuthorizationError::Registration)
    }

    #[doc(hidden)]
    pub(crate) async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        crate::sync::store::DeviceJoinError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(crate::sync::store::DeviceJoinError::from)?;
        writer.join_operation().begin(member_pubkey).await
    }

    pub(crate) async fn begin_device_join_bundle(
        &self,
        member_pubkey: &str,
    ) -> Result<
        crate::sync::store::DeviceJoinOfferBundle,
        crate::sync::store::DeviceJoinTransportError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(crate::sync::store::DeviceJoinError::from)?;
        let offer = writer.join_operation().begin(member_pubkey).await?;
        self.device_join_transport().allocate_bundle(offer).await
    }

    pub(crate) async fn begin_owner_promotion_for_device(
        &self,
        device_id: coven_protocol::store_commit::StoreDeviceId,
    ) -> Result<
        coven_protocol::store_commit::OwnerPromotionRequest,
        owner_role_promotion::OwnerPromotionError,
    > {
        let registration = self
            .database
            .activated_store_device_registration_for_device(device_id)
            .await?
            .ok_or_else(|| {
                owner_role_promotion::OwnerPromotionError::Protocol(
                    "the target Store device is not active".to_string(),
                )
            })?;
        self.begin_owner_promotion(registration.reference().clone())
            .await
    }

    pub(crate) async fn begin_owner_promotion(
        &self,
        member_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        coven_protocol::store_commit::OwnerPromotionRequest,
        owner_role_promotion::OwnerPromotionError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(owner_role_promotion::OwnerPromotionError::from)?;
        writer.owner_promotion().begin(member_registration).await
    }

    pub(crate) async fn accept_owner_promotion(
        &self,
        request: coven_protocol::store_commit::OwnerPromotionRequest,
    ) -> Result<
        coven_protocol::store_commit::OwnerPromotionAcceptance,
        owner_role_promotion::OwnerPromotionError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(owner_role_promotion::OwnerPromotionError::from)?;
        writer.owner_promotion().accept(request).await
    }

    pub(crate) async fn finalize_owner_promotion(
        &self,
        encryption: &coven_keys::encryption::EncryptionService,
        acceptance: coven_protocol::store_commit::OwnerPromotionAcceptance,
    ) -> Result<
        coven_protocol::circle_control::StoreMembershipStateRef,
        owner_role_promotion::OwnerPromotionError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(owner_role_promotion::OwnerPromotionError::from)?;
        writer
            .owner_promotion()
            .finalize(encryption, acceptance)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn admit_member(
        &self,
        public_key_hex: &str,
        member_email: Option<&str>,
        role: coven_protocol::membership::MemberRole,
        encryption: &coven_keys::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<
        crate::sync::store::membership::MemberAdmission,
        crate::sync::store::membership::MembershipOpsError,
    > {
        let mut authorization = self
            .authorize_writer()
            .await
            .map_err(StoreError::from)
            .map_err(membership::MembershipOpsError::from)?;
        authorization
            .admit_member(
                public_key_hex,
                member_email,
                role,
                encryption,
                store_id,
                store_name,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn remove_member(
        &self,
        public_key_hex: &str,
        encryption: &coven_keys::encryption::EncryptionService,
        master_keys: &dyn coven_keys::keys::MasterKeyCustody,
        cipher: &dyn coven_storage::CloudSyncCipherStateAccess,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
    ) -> Result<String, crate::sync::store::membership::MembershipOpsError> {
        let mut authorization = self
            .authorize_writer()
            .await
            .map_err(StoreError::from)
            .map_err(membership::MembershipOpsError::from)?;
        authorization
            .remove_member(
                public_key_hex,
                encryption,
                master_keys,
                cipher,
                pending_rotation,
            )
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn circle_epoch_access(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::CircleEpochAccess>, coven_database::DbError>
    {
        self.database
            .circle_epoch_access(self.root.reference().clone(), circle_id, expected_control)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn latest_local_store_position(
        &self,
    ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, StoreError> {
        let writer = self.authorize_writer().await.map_err(StoreError::from)?;
        writer
            .latest_local_store_position()
            .await
            .map_err(Into::into)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_exact_materialized_commit(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<
        Option<(
            coven_protocol::store_commit::StoreBatchCommitRef,
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
        )>,
        StoreError,
    > {
        let Some(reference) = self
            .database
            .exact_materialized_ref(stream_id, sequence)
            .await?
        else {
            return Ok(None);
        };
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        let commit = history
            .load_commit(&reference)
            .await
            .map_err(StoreError::from)?;
        Ok(Some((reference, commit)))
    }
}

#[cfg(test)]
mod tests;
