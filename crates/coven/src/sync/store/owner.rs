use super::*;
use crate::database::BlockedWriteDiscard;
use crate::protocol::store_commit::StoreRootRef;
use std::sync::Arc;

mod authorized_history;
mod authorized_store;
mod circles;
pub(super) mod device_exclusion;
pub(super) mod device_join;
pub(crate) mod device_join_transport;
mod founder_creation;
pub(super) mod history;
mod history_construction;
mod host_write;
mod keyring;
pub(crate) use keyring::load_wrapped_store_key;
pub(super) mod owner_promotion;
pub(crate) mod pull;
mod registration;
mod registration_outbox;
mod restore;
mod verification;
mod verified_history;
pub(super) mod writer;

mod store_test_support;

use super::prepare_registration_object;
use super::protocol_root::VerifiedStoreRoot;
use authorized_history::AuthorizedStoreHistory;
pub(crate) use authorized_store::AuthorizedStore;
#[cfg(test)]
pub(crate) use circles::CirclePackageReadError;
pub(crate) use circles::{AuthorizedCircleWriter, StoreCircleCommands};
use founder_creation::FounderStoreCreation;
pub(crate) use history_construction::HistoryConstructionAuthority;
pub(crate) use host_write::HostWriteBlobStaging;
pub(crate) use keyring::StoreKeyrings;
pub(crate) use registration::StoreRegistrationError;
use registration_outbox::RegistrationOutbox;
pub(crate) use restore::RestoringStore;
use verification::StoreCommitVerifier;
#[cfg(test)]
pub(crate) use verified_history::{
    MergeHistorySuccessorEvidence, MergeOutboundAuthorization, PreparedMergeHistorySuccessor,
    VerifiedMergeMembershipPrefix,
};
pub(super) use writer::{operations, reclaim, snapshot};
pub(crate) use writer::{AuthorizedWriterOperation, StoreAckError};

#[doc(hidden)]
pub(crate) struct Store {
    database: StoreDatabase,
    storage: Arc<dyn SyncStorage>,
    store_dir: StoreDir,
    blob_cache: crate::sync::store::blob::StoreBlobCache,
    identity: UserKeypair,
    device_id: Option<String>,
    root: crate::sync::store::protocol_root::VerifiedStoreRoot,
}

#[doc(hidden)]
pub(crate) struct StoreRestoreMembership {
    pub store_root: StoreRootRef,
    pub founder_pubkey: String,
    pub membership_floor: crate::protocol::membership::MembershipFloor,
}

pub(crate) struct InitializedStore {
    pub(crate) store: Store,
    pub(crate) device_id: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreInitializationError {
    #[error("Store protocol root failed: {0}")]
    ProtocolRoot(String),
    #[error("membership chain bootstrap/anchor failed: {0}")]
    MembershipAnchor(String),
}

struct BlobDownload {
    authority: crate::protocol::blob::RowBlobAuthority,
    stored: crate::protocol::blob::locator::StoredBlobRef,
}

impl BlobDownload {
    fn from_row(reference: crate::protocol::blob::RowBlobRef) -> Result<Self, String> {
        let stored = reference
            .stored()
            .cloned()
            .ok_or_else(|| "remote eager blob row has no exact stored reference".to_string())?;
        Ok(Self {
            authority: reference.authority().clone(),
            stored,
        })
    }
}

impl Store {
    pub(crate) fn device_join_transport(
        &self,
    ) -> device_join_transport::StoreDeviceJoinTransport<'_> {
        device_join_transport::StoreDeviceJoinTransport::new(self)
    }

    async fn allocate_device_join_transport_bundle(
        &self,
        offer: crate::protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<
        device_join_transport::DeviceJoinOfferBundle,
        device_join_transport::DeviceJoinTransportError,
    > {
        let attempt_namespace = device_join_transport::attempt_namespace(offer.attempt_id);
        let context = device_join_transport::slot_context(offer.store_root.store_root_hash);
        let mut slots = std::collections::BTreeMap::new();
        for kind in device_join_transport::DeviceJoinTransportKind::ALL {
            let slot = self
                .storage
                .allocate_protocol_slot(
                    &context,
                    &device_join_transport::semantic_prefix(&attempt_namespace, kind),
                    ".json",
                )
                .await?;
            slots.insert(kind, slot);
        }
        Ok(device_join_transport::DeviceJoinOfferBundle {
            version: crate::protocol::store_commit::STORE_PROTOCOL_VERSION,
            offer,
            transport: device_join_transport::DeviceJoinTransportParams {
                version: crate::protocol::store_commit::STORE_PROTOCOL_VERSION,
                attempt_namespace,
                slots,
                seal_key: crate::encryption::MasterKeyring::generate(),
            },
        })
    }

    async fn publish_device_join_transport_artifact(
        &self,
        bundle: &device_join_transport::DeviceJoinOfferBundle,
        roles: device_join_transport::DeviceJoinRoles,
        action: &crate::sync::store::DeviceJoinAction,
    ) -> Result<(), device_join_transport::DeviceJoinTransportError> {
        device_join_transport::DeviceJoinTransport::open(self.storage.as_ref(), bundle, roles)?
            .publish(action)
            .await
    }

    async fn await_device_join_transport_artifact<T: device_join_transport::DeviceJoinArtifact>(
        &self,
        bundle: &device_join_transport::DeviceJoinOfferBundle,
        roles: device_join_transport::DeviceJoinRoles,
        timing: device_join_transport::DeviceJoinTransportTiming,
    ) -> Result<T, device_join_transport::DeviceJoinTransportError> {
        device_join_transport::DeviceJoinTransport::open(self.storage.as_ref(), bundle, roles)?
            .await_artifact::<T>(timing)
            .await
    }

    async fn device_join_transport_status(
        &self,
        attempt_id: crate::protocol::store_commit::DeviceJoinAttemptId,
        role: crate::sync::store::DeviceJoinRole,
    ) -> Result<
        Option<crate::sync::store::DeviceJoinStatus>,
        device_join_transport::DeviceJoinTransportError,
    > {
        Ok(self.database.device_join_status(attempt_id, role).await?)
    }

    async fn device_join_transport_roles(
        &self,
        offer: &crate::protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<
        device_join_transport::DeviceJoinRoles,
        device_join_transport::DeviceJoinTransportError,
    > {
        let local = self
            .database
            .local_activated_registration_ref()
            .await
            .map_err(|error| crate::sync::store::DeviceJoinError::Store(error.to_string()))?
            .ok_or(crate::sync::store::DeviceJoinError::ActiveDeviceRequired)?;
        let roles = device_join_transport::DeviceJoinRoles::admitting(
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
        crate::keys::public_key_hex(&self.identity)
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
        storage: Arc<dyn SyncStorage>,
        store_dir: StoreDir,
        founder_timestamp: &str,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let blob_cache =
            crate::sync::store::blob::StoreBlobCache::new(database.clone(), store_dir.clone());
        FounderStoreCreation::begin(
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
        storage: Arc<dyn SyncStorage>,
        store_dir: StoreDir,
        expected_root: &StoreRootRef,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let root = crate::sync::store::protocol_root::VerifiedStoreRoot::open(
            &database,
            &*storage,
            expected_root,
        )
        .await
        .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        let authority = HistoryConstructionAuthority::store();
        let history_verifier = authority
            .bind_verified(storage.as_ref(), root.clone())
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
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
    pub(crate) async fn load(
        database: StoreDatabase,
        storage: Arc<dyn SyncStorage>,
        store_dir: StoreDir,
        identity: UserKeypair,
    ) -> Result<Self, StoreError> {
        let store_root =
            database
                .local_store_root_ref()
                .await?
                .ok_or(StoreError::MissingState {
                    key: operations::STORE_ROOT_AUTHORITY,
                })?;
        let root = crate::sync::store::protocol_root::VerifiedStoreRoot::open(
            &database,
            &*storage,
            &store_root,
        )
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let device_id = database
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?;
        Ok(Self::new(
            database, storage, store_dir, identity, device_id, root,
        ))
    }

    fn new(
        database: StoreDatabase,
        storage: Arc<dyn SyncStorage>,
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

    async fn circle_close_status(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<crate::protocol::circle::CircleCloseStatus, CircleOperationError> {
        let (current, _) = self
            .database
            .circle_closing_context(circle_id, &self.local_author_pubkey())
            .await?;
        let crate::protocol::circle::CircleControlState::EpochClose(close) =
            current.control.value.state()
        else {
            return Err(CircleOperationError::InvalidState(
                "Circle close-status inspection received an active control".to_string(),
            ));
        };
        let context = crate::protocol::objects::ProtocolObjectContext::store_encrypted(
            current.control.value.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::CircleEpochCloseResponse,
        );
        let mut participants = Vec::with_capacity(close.participants.len());
        for participant in &close.participants {
            let prefix = crate::protocol::circle::circle_epoch_close_response_semantic_prefix(
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
                    match crate::protocol::circle::CircleEpochCloseResponseSlotValue::parse(&bytes)
                        .map_err(|error| {
                            CircleOperationError::InvalidState(format!(
                                "Circle epoch-close response slot for device {} failed to parse: {error}",
                                participant.registration.device_id
                            ))
                        })?
                    {
                        crate::protocol::circle::CircleEpochCloseResponseSlotValue::Response(_) => {
                            crate::protocol::circle::CircleCloseSettlement::Responded
                        }
                        crate::protocol::circle::CircleEpochCloseResponseSlotValue::Exclusion(_) => {
                            crate::protocol::circle::CircleCloseSettlement::Excluded
                        }
                    }
                }
                Err(crate::protocol::objects::StorageError::NotFound(_)) => {
                    crate::protocol::circle::CircleCloseSettlement::Pending
                }
                Err(error) => return Err(crate::protocol::objects::StoreObjectError::from(error).into()),
            };
            participants.push(crate::protocol::circle::CircleCloseParticipant {
                device_id: participant.registration.device_id,
                settlement,
            });
        }
        Ok(crate::protocol::circle::CircleCloseStatus {
            circle_id,
            close_id: close.close_id,
            participants,
        })
    }

    #[doc(hidden)]
    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, crate::sync::store::StoreError> {
        if let BlockedWriteDiscard::Discarded(discarded) =
            self.database.discard_blocked_write(&write_id).await?
        {
            return Ok(discarded);
        }

        match self.abandon_merge_candidate(write_id.clone()).await? {
            history::abandonment::MergeCandidateAbandonment::NotRequired => {
                return Err(StoreError::InvalidOutbound(
                    "blocked Merge candidate has no abandonment authority".to_string(),
                ));
            }
            history::abandonment::MergeCandidateAbandonment::Abandoned => {}
            history::abandonment::MergeCandidateAbandonment::CandidateActivated => {
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

    pub(crate) async fn abandon_merge_candidate(
        &self,
        write_id: crate::WriteId,
    ) -> Result<history::abandonment::MergeCandidateAbandonment, StoreError> {
        if self.device_id.is_none() {
            let mut authority = self
                .authorize_history()
                .await
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
            return authority
                .abandon_excluded_merge_candidate(write_id)
                .await?
                .ok_or_else(|| {
                    StoreError::InvalidOutbound(
                        "unregistered Store cannot publish Merge abandonment authority".to_string(),
                    )
                });
        }
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        writer.abandon_merge_candidate(write_id).await
    }

    #[doc(hidden)]
    pub(crate) async fn members(
        &self,
    ) -> Result<Vec<crate::protocol::membership::MemberInfo>, membership::MembershipOpsError> {
        let authorization = self.authorize().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
        authorization.members(Some(&self.identity.public_key()))
    }

    #[doc(hidden)]
    pub(crate) async fn membership_conflict(
        &self,
    ) -> Result<
        Option<crate::protocol::membership::MembershipConflictInfo>,
        membership::MembershipOpsError,
    > {
        let authorization = self.authorize().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
        Ok(authorization.membership_conflict(Some(&self.identity.public_key())))
    }

    pub(crate) async fn resolve_membership_conflict(
        &self,
        choice: &crate::protocol::membership::MembershipConflictChoice,
        created_at: &str,
    ) -> Result<
        crate::protocol::membership::StoreMembershipConflictResolutionRef,
        membership::MembershipOpsError,
    > {
        let mut authorization = self.authorize_writer().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
        authorization
            .resolve_membership_conflict(choice, created_at)
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn restore_membership(
        &self,
    ) -> Result<StoreRestoreMembership, membership::MembershipOpsError> {
        let authorization = self.authorize().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
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
    ) -> Result<AuthorizedWriterOperation<'_>, writer::StoreWriterAuthorizationError> {
        RegistrationOutbox::new(self.database.clone(), &*self.storage)
            .drain()
            .await
            .map_err(writer::StoreWriterAuthorizationError::Registration)?;
        self.authorize()
            .await
            .map_err(writer::StoreWriterAuthorizationError::StoreAuthority)?
            .into_writer()
            .await
            .map_err(writer::StoreWriterAuthorizationError::Registration)
    }

    #[doc(hidden)]
    pub(crate) async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<
        crate::protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        crate::sync::store::DeviceJoinError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| crate::sync::store::DeviceJoinError::Store(error.to_string()))?;
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
            .map_err(|error| crate::sync::store::DeviceJoinError::Store(error.to_string()))?;
        let offer = writer.join_operation().begin(member_pubkey).await?;
        self.device_join_transport().allocate_bundle(offer).await
    }

    pub(crate) async fn begin_owner_promotion_for_device(
        &self,
        device_id: crate::protocol::store_commit::StoreDeviceId,
    ) -> Result<
        crate::protocol::store_commit::OwnerPromotionRequest,
        owner_promotion::OwnerPromotionError,
    > {
        let registration = self
            .database
            .activated_store_device_registration_for_device(device_id)
            .await?
            .ok_or_else(|| {
                owner_promotion::OwnerPromotionError::Protocol(
                    "the target Store device is not active".to_string(),
                )
            })?;
        self.begin_owner_promotion(registration.reference().clone())
            .await
    }

    pub(crate) async fn begin_owner_promotion(
        &self,
        member_registration: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        crate::protocol::store_commit::OwnerPromotionRequest,
        owner_promotion::OwnerPromotionError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| owner_promotion::OwnerPromotionError::Protocol(error.to_string()))?;
        writer.owner_promotion().begin(member_registration).await
    }

    pub(crate) async fn accept_owner_promotion(
        &self,
        request: crate::protocol::store_commit::OwnerPromotionRequest,
    ) -> Result<
        crate::protocol::store_commit::OwnerPromotionAcceptance,
        owner_promotion::OwnerPromotionError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| owner_promotion::OwnerPromotionError::Protocol(error.to_string()))?;
        writer.owner_promotion().accept(request).await
    }

    pub(crate) async fn finalize_owner_promotion(
        &self,
        encryption: &crate::encryption::EncryptionService,
        acceptance: crate::protocol::store_commit::OwnerPromotionAcceptance,
    ) -> Result<
        crate::protocol::circle_control::StoreMembershipStateRef,
        owner_promotion::OwnerPromotionError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| owner_promotion::OwnerPromotionError::Protocol(error.to_string()))?;
        writer
            .owner_promotion()
            .finalize(encryption, acceptance)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: crate::protocol::membership::MemberRole,
        encryption: &crate::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<crate::join_code::InviteCode, crate::sync::store::membership::MembershipOpsError>
    {
        let mut authorization = self.authorize_writer().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
        authorization
            .invite_member(
                public_key_hex,
                invitee_email,
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
        encryption: &crate::encryption::EncryptionService,
        security: &dyn crate::sync::RotationKeyAdoption,
        cipher: &dyn crate::storage::CloudCipherAccess,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
    ) -> Result<String, crate::sync::store::membership::MembershipOpsError> {
        let mut authorization = self.authorize_writer().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
        authorization
            .remove_member(
                public_key_hex,
                encryption,
                security,
                cipher,
                pending_rotation,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn circle_epoch_access(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        expected_control: crate::protocol::circle::CircleControlCoord,
    ) -> Result<
        Option<crate::sync::store::circle_controls::CircleEpochAccess>,
        crate::database::DbError,
    > {
        self.database
            .circle_epoch_access(self.root.reference().clone(), circle_id, expected_control)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn latest_local_store_position(
        &self,
    ) -> Result<Option<crate::protocol::store_commit::StoreBatchCommitRef>, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        writer
            .latest_local_store_position()
            .await
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) async fn load_exact_materialized_commit(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<
        Option<(
            crate::protocol::store_commit::StoreBatchCommitRef,
            crate::protocol::store_commit::VerifiedStoreBatchCommit,
        )>,
        String,
    > {
        let Some(reference) = self
            .database
            .exact_materialized_ref(stream_id, sequence)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| error.to_string())?;
        let commit = history
            .load_commit(&reference)
            .await
            .map_err(|error| error.to_string())?;
        Ok(Some((reference, commit)))
    }
}

#[cfg(test)]
mod tests;
