use super::*;
use crate::database::BlockedWriteDiscard;
use crate::protocol::store_commit::StoreRootRef;

mod circle_bootstrap;
mod circles;
pub(super) mod device_exclusion;
pub(super) mod device_join;
pub(super) mod history;
mod host_write;
mod keyring;
pub(super) mod owner_promotion;
pub(crate) use history::pull;
mod registration;
mod registration_outbox;
mod restore;
mod verification;
mod verified_history;
pub(super) mod writer;

use super::prepare_registration_object;
pub(crate) use circles::{AuthorizedCircleWriter, StoreCircleCommands};
use history::{AuthorizedStoreHistory, FounderStoreInitialization};
pub(crate) use host_write::HostWriteBlobStaging;
pub(crate) use registration::StoreRegistrationError;
use registration_outbox::RegistrationOutbox;
pub(crate) use restore::RestoringStore;
use verification::StoreCommitVerifier;
pub(super) use writer::{operations, reclaim, snapshot};
pub(crate) use writer::{AuthorizedWriterOperation, StoreAckError};

#[doc(hidden)]
pub(crate) struct Store {
    database: StoreDatabase,
    storage: Arc<dyn SyncStorage>,
    identity: UserKeypair,
    device_id: Option<String>,
    store_root: StoreRootRef,
    protocol_root: crate::storage::VerifiedObject<StoreProtocolRoot>,
}

#[doc(hidden)]
pub(crate) struct StoreRestoreMembership {
    pub store_root: StoreRootRef,
    pub founder_pubkey: String,
    pub membership_floor: crate::joining::MembershipFloor,
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

struct LocalStoreDevice {
    registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    registration: crate::protocol::store_commit::StoreDeviceRegistration,
    activation: Option<crate::protocol::store_commit::StoreDeviceRegistrationActivation>,
}

pub(crate) struct AuthorizedStore<'a> {
    history: AuthorizedStoreHistory<'a>,
    storage: &'a Arc<dyn SyncStorage>,
    identity: &'a UserKeypair,
    local_device: Option<LocalStoreDevice>,
    membership: crate::protocol::membership::MembershipChain,
}

struct BlobDownload {
    authority: crate::blob::RowBlobAuthority,
    stored: crate::blob::locator::StoredBlobRef,
}

impl BlobDownload {
    fn from_row(reference: crate::blob::RowBlobRef) -> Result<Self, String> {
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
    ) -> super::device_join_transport::StoreDeviceJoinTransport<'_> {
        super::device_join_transport::StoreDeviceJoinTransport::new(
            self,
            self.database.clone(),
            self.storage.as_ref(),
        )
    }

    pub(crate) fn circles(&self) -> StoreCircleCommands<'_> {
        StoreCircleCommands::from_parts(
            self,
            self.database.clone(),
            self.storage.clone(),
            &self.identity,
            self.storage.blob_path_scheme(),
        )
    }

    #[doc(hidden)]
    pub(crate) fn host_write_blob_staging(
        &self,
        runtime: tokio::runtime::Handle,
        store_dir: StoreDir,
    ) -> HostWriteBlobStaging {
        HostWriteBlobStaging::new(
            runtime,
            Arc::clone(&self.storage),
            self.store_root.clone(),
            store_dir,
        )
    }

    pub(crate) async fn open_invitation_keyring(
        bootstrap_storage: &dyn SyncStorage,
        keypair: &UserKeypair,
        invitation: &crate::joining::InviteCode,
    ) -> Result<crate::encryption::EncryptionService, membership::InviteError> {
        let recipient = hex::encode(keypair.public_key());
        if invitation.wrapped_key.recipient_pubkey != recipient {
            return Err(membership::InviteError::Crypto(
                "invite wrapped-key ref names another recipient".to_string(),
            ));
        }
        crate::protocol::membership::validate_membership_floor(&invitation.membership_floor.0)
            .map_err(membership::InviteError::Crypto)?;
        let mut history =
            history::InvitationHistory::open(bootstrap_storage, keypair, &invitation.store_root)
                .await?;
        let chain = history
            .load_membership(&invitation.membership_floor.0, &invitation.owner_pubkey)
            .await?;
        history
            .keyring(&chain)
            .open_containing(&invitation.wrapped_key)
            .await
    }

    pub(crate) async fn create(
        database: StoreDatabase,
        storage: Arc<dyn SyncStorage>,
        founder_timestamp: &str,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let _creation = database.store_creation_permit().await;
        let mut graph = match database
            .local_store_founder_graph()
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?
        {
            Some(graph) => graph,
            None => {
                let graph = Box::pin(crate::sync::store::protocol_root::prepare_founder_graph(
                    &database,
                    &*storage,
                    founder_timestamp,
                    identity,
                ))
                .await
                .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
                database
                    .stage_store_founder_graph(graph)
                    .await
                    .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
                database
                    .local_store_founder_graph()
                    .await
                    .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?
                    .ok_or_else(|| {
                        StoreInitializationError::ProtocolRoot(
                            "staged Store founder graph is absent".to_string(),
                        )
                    })?
            }
        };
        let rollback_allowed = match &graph.registration_state {
            crate::database::LocalDeviceRegistrationState::Prepared
            | crate::database::LocalDeviceRegistrationState::Created => true,
            crate::database::LocalDeviceRegistrationState::Activated { .. } => false,
        };
        if rollback_allowed {
            Box::pin(
                crate::sync::store::protocol_root::rollback_founder_publication(
                    &database, &*storage, &graph,
                ),
            )
            .await
            .map_err(|rollback| {
                StoreInitializationError::ProtocolRoot(format!(
                    "Store founder rollback before publication: {rollback}"
                ))
            })?;
            graph = database
                .local_store_founder_graph()
                .await
                .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?
                .ok_or_else(|| {
                    StoreInitializationError::ProtocolRoot(
                        "rolled-back Store founder graph is absent".to_string(),
                    )
                })?;
        }
        let history = match FounderStoreInitialization::new(
            &database,
            &*storage,
            founder_timestamp,
            identity,
            &graph,
        )
        .publish()
        .await
        {
            Ok(history) => history,
            Err(operation) if rollback_allowed => {
                match Box::pin(
                    crate::sync::store::protocol_root::rollback_founder_publication(
                        &database, &*storage, &graph,
                    ),
                )
                .await
                {
                    Ok(()) => {
                        return Err(StoreInitializationError::ProtocolRoot(
                            operation.to_string(),
                        ));
                    }
                    Err(rollback) => {
                        return Err(StoreInitializationError::ProtocolRoot(format!(
                            "{operation}; Store founder rollback failed: {rollback}"
                        )));
                    }
                }
            }
            Err(operation) => {
                return Err(StoreInitializationError::ProtocolRoot(
                    operation.to_string(),
                ));
            }
        };
        let durable_root = database
            .local_store_root_ref()
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?
            .ok_or_else(|| {
                StoreInitializationError::ProtocolRoot(
                    "published Store founder graph has no durable exact root".to_string(),
                )
            })?;
        if history.root() != &durable_root {
            return Err(StoreInitializationError::ProtocolRoot(
                "published Store founder history differs from its durable exact root".to_string(),
            ));
        }
        history.finish_initialization(&storage, identity).await
    }

    pub(crate) async fn open(
        database: StoreDatabase,
        storage: Arc<dyn SyncStorage>,
        expected_root: &StoreRootRef,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let protocol_root = Self::open_protocol_root(&database, &*storage, expected_root)
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        let history = AuthorizedStoreHistory::from_verified_root(
            database,
            &*storage,
            expected_root,
            protocol_root,
        )
        .await
        .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        history.finish_initialization(&storage, identity).await
    }

    async fn open_protocol_root(
        database: &StoreDatabase,
        storage: &dyn SyncStorage,
        expected: &StoreRootRef,
    ) -> Result<
        crate::storage::VerifiedObject<StoreProtocolRoot>,
        crate::sync::store::protocol_root::StoreProtocolRootError,
    > {
        let verified = crate::sync::store::protocol_root::load_exact_store_protocol_root(
            storage,
            expected,
            database.sync_routing_hash(),
        )
        .await?;
        let live_binding = storage.provider_binding().await.map_err(|error| {
            crate::sync::store::protocol_root::StoreProtocolRootError::Provider(error.to_string())
        })?;
        if live_binding.store != verified.value.descriptor.provider {
            return Err(
                crate::sync::store::protocol_root::StoreProtocolRootError::Database(
                    "live provider namespace differs from the signed Store root".to_string(),
                ),
            );
        }
        if let Some(local) = database
            .latest_local_store_device_registration()
            .await
            .map_err(|error| {
                crate::sync::store::protocol_root::StoreProtocolRootError::Database(
                    error.to_string(),
                )
            })?
            .filter(|registration| registration.is_activated())
        {
            let registration = crate::protocol::store_commit::StoreDeviceRegistration::parse_at(
                &local.registration_bytes,
                expected,
                local.device_id,
            )
            .map_err(|error| {
                crate::sync::store::protocol_root::StoreProtocolRootError::Database(
                    error.to_string(),
                )
            })?;
            if registration.provider != live_binding.device {
                return Err(
                    crate::sync::store::protocol_root::StoreProtocolRootError::Database(
                        "live provider principal differs from the active Store registration"
                            .to_string(),
                    ),
                );
            }
        }
        if verified.value.descriptor.schema_version > database.schema_version() {
            return Err(
                crate::sync::store::protocol_root::StoreProtocolRootError::SchemaTooNew {
                    root_schema: verified.value.descriptor.schema_version,
                    local: database.schema_version(),
                },
            );
        }
        Ok(verified)
    }

    #[doc(hidden)]
    pub(crate) async fn load(
        database: StoreDatabase,
        storage: Arc<dyn SyncStorage>,
        identity: UserKeypair,
    ) -> Result<Self, StoreError> {
        let store_root =
            database
                .local_store_root_ref()
                .await?
                .ok_or(StoreError::MissingState {
                    key: operations::STORE_ROOT_AUTHORITY,
                })?;
        let protocol_root = Self::open_protocol_root(&database, &*storage, &store_root)
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let device_id = database
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?;
        Self::new(
            database,
            storage,
            identity,
            device_id,
            store_root,
            protocol_root,
        )
        .map_err(StoreError::InvalidOutbound)
    }

    fn new(
        database: StoreDatabase,
        storage: Arc<dyn SyncStorage>,
        identity: UserKeypair,
        device_id: Option<String>,
        store_root: StoreRootRef,
        protocol_root: crate::storage::VerifiedObject<StoreProtocolRoot>,
    ) -> Result<Self, String> {
        if store_root.store_root_hash != protocol_root.value.object_hash() {
            return Err(
                "local Store root reference differs from the verified Store root".to_string(),
            );
        }
        Ok(Self {
            database,
            storage,
            identity,
            device_id,
            store_root,
            protocol_root,
        })
    }
    pub(crate) fn store_root(&self) -> &StoreRootRef {
        &self.store_root
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.storage.blob_path_scheme()
    }

    pub(crate) fn self_uploader(&self) -> String {
        self.storage.self_uploader()
    }

    #[cfg(test)]
    pub(crate) async fn circle_package_access(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        expected_control: crate::protocol::circle::CircleControlCoord,
    ) -> Result<
        Option<crate::sync::store::circle_controls::CirclePackageAccess>,
        crate::database::DbError,
    > {
        self.database
            .circle_package_access(self.store_root.clone(), circle_id, expected_control)
            .await
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
        user_pubkey: Option<&[u8]>,
    ) -> Result<Vec<crate::protocol::membership::MemberInfo>, membership::MembershipOpsError> {
        let authorization = self.authorize().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
        authorization.members(user_pubkey)
    }

    #[doc(hidden)]
    pub(crate) async fn membership_conflict(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Result<
        Option<crate::protocol::membership::MembershipConflictInfo>,
        membership::MembershipOpsError,
    > {
        let authorization = self.authorize().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
        Ok(authorization.membership_conflict(user_pubkey))
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
        let founder_pubkey = authorization
            .membership()
            .founder_pubkey()
            .map(str::to_string)
            .ok_or(membership::MembershipOpsError::NoFounderChain)?;
        let membership_floor = authorization.membership().head_refs().to_vec();
        Ok(StoreRestoreMembership {
            store_root: self.store_root().clone(),
            founder_pubkey,
            membership_floor: crate::joining::MembershipFloor(membership_floor),
        })
    }

    async fn authorize_history(&self) -> Result<AuthorizedStoreHistory<'_>, SyncCycleFailure> {
        AuthorizedStoreHistory::from_verified_root(
            self.database.clone(),
            &*self.storage,
            &self.store_root,
            self.protocol_root.clone(),
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("open Store history authority", error))
    }

    pub(crate) async fn authorize(&self) -> Result<AuthorizedStore<'_>, SyncCycleFailure> {
        self.authorize_history()
            .await?
            .authorize_store(&self.storage, &self.identity, self.device_id.as_deref())
            .await
    }

    #[cfg(test)]
    pub(super) async fn restoring_for_test(&self) -> Result<RestoringStore<'_>, SyncCycleFailure> {
        let authorization = self.authorize().await?;
        Ok(authorization.history.bind_restore(
            authorization.storage.as_ref(),
            authorization.membership,
            authorization.identity.clone(),
            std::path::PathBuf::new(),
        ))
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

    #[cfg(test)]
    pub(crate) async fn prepare_wrapped_key_for_test(
        &self,
        recipient: &str,
        value: crate::protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<crate::protocol::wrapped_store_key::PreparedWrappedStoreKey, String> {
        let authorization = self.authorize().await.map_err(|error| error.to_string())?;
        authorization
            .keyring(authorization.membership())
            .prepare(recipient, value)
            .await
            .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    pub(crate) async fn open_membership_keyring_for_test(
        &self,
    ) -> Result<crate::encryption::EncryptionService, String> {
        let authorization = self.authorize().await.map_err(|error| error.to_string())?;
        authorization
            .keyring(authorization.membership())
            .open()
            .await
            .map_err(|error| error.to_string())
    }

    #[doc(hidden)]
    pub(crate) async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::sync::store::DeviceJoinOffer, crate::sync::store::DeviceJoinError> {
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
        let (registration, _, _) = self
            .database
            .activated_store_device_registration_for_device(device_id)
            .await?
            .ok_or_else(|| {
                owner_promotion::OwnerPromotionError::Protocol(
                    "the target Store device is not active".to_string(),
                )
            })?;
        self.begin_owner_promotion(registration).await
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

    #[cfg(test)]
    pub(crate) async fn announcement_stream_id_for_test(
        &self,
    ) -> Result<crate::protocol::membership::AuthorStreamId, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(writer.announcement_stream_id())
    }

    #[cfg(test)]
    pub(crate) async fn sign_device_head_for_test(
        &self,
        commit: crate::protocol::store_commit::StoreBatchCommitRef,
        history_summary: crate::protocol::store_commit::ObjectHash,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> Result<crate::protocol::store_commit::StoreDeviceHead, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        writer.sign_device_head_for_test(commit, history_summary, successor)
    }

    #[cfg(test)]
    pub(crate) async fn resign_snapshot_meta_for_test(
        &self,
        meta: crate::protocol::store_commit::SnapshotMeta,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        writer.resign_snapshot_meta_for_test(meta)
    }

    #[cfg(test)]
    pub(crate) async fn parse_local_snapshot_meta_for_test(
        &self,
        bytes: &[u8],
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        writer.parse_snapshot_meta_for_test(bytes, reference)
    }

    #[cfg(test)]
    pub(crate) async fn prepare_operation_plan_for_test(
        &self,
    ) -> Result<writer::operations::StoreOperationCommitPlan, StoreError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        writer.prepare_plan().await
    }

    #[cfg(test)]
    pub(crate) async fn authorize_retained_outbound_for_test(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[crate::protocol::membership::MembershipHeadRef],
    ) -> Result<verified_history::MergeOutboundAuthorization, StoreError> {
        let authorization = self
            .authorize()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let author_registration = authorization
            .local_device
            .as_ref()
            .ok_or_else(|| {
                StoreError::InvalidOutbound(
                    "retained outbound test Store has no local device".to_string(),
                )
            })?
            .registration_ref
            .clone();
        authorization
            .history
            .authorize_retained_outbound(order, candidate_membership_heads, &author_registration)
            .await
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) async fn owner_promotion_target_for_test(
        &self,
    ) -> Result<crate::protocol::store_commit::StoreDeviceRegistrationRef, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(writer.local_registration_ref_for_test())
    }

    #[cfg(test)]
    pub(crate) async fn observe_excluded_candidate_head_for_test(
        &self,
        candidate: &crate::protocol::store_commit::StoreDeviceHead,
        candidate_commit: &crate::protocol::store_commit::StoreBatchCommit,
        candidate_object: &crate::storage::ExactObjectRef,
    ) -> Result<history::abandonment::ExcludedCandidateHeadObservation, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let verified_commit = history
            .authenticate_commit_bytes(&candidate.commit, &candidate_commit.to_bytes())
            .await?;
        history
            .observe_excluded_candidate_head(candidate, &verified_commit, candidate_object)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn cleanup_merge_candidate_for_test(
        &self,
        write_id: crate::WriteId,
    ) -> Result<(), StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .cleanup_merge_candidate(write_id)
            .await
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) async fn complete_revoke_rotation_adoption_for_test(
        &self,
        pending_rotation: &crate::storage::PendingRotation,
        adopted_generation: u64,
    ) -> Result<(), membership::InviteError> {
        self.authorize_writer()
            .await
            .map_err(|error| membership::InviteError::Database(error.to_string()))?
            .complete_revoke_rotation_adoption_for_test(pending_rotation, adopted_generation)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn membership_for_test(
        &self,
    ) -> Result<crate::protocol::membership::MembershipChain, StoreError> {
        self.authorize()
            .await
            .map(|authorization| authorization.membership().clone())
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) async fn retained_merge_replay_inputs_for_test(
        &self,
    ) -> Result<Vec<crate::database::OwnedVerifiedMergeMaterialization>, crate::database::DbError>
    {
        self.database
            .retained_merge_replay_inputs(self.store_root.clone())
            .await
    }

    #[cfg(test)]
    pub(crate) async fn retained_merge_materialization_for_test(
        &self,
        reference: crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<crate::database::OwnedVerifiedMergeMaterialization, crate::database::DbError> {
        self.database
            .retained_merge_materialization(self.store_root.clone(), reference)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn prepare_conflict_resolution_plan_for_test(
        &self,
        candidate_membership_heads: &[crate::protocol::membership::MembershipHeadRef],
    ) -> Result<(), StoreError> {
        self.authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
            .prepare_conflict_resolution_plan(candidate_membership_heads)
            .await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn load_membership_head_for_test(
        &self,
        reference: &crate::protocol::membership::MembershipHeadRef,
    ) -> Result<crate::protocol::membership::AuthorHead, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .load_exact_membership_head_for_test(reference)
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) async fn load_membership_at_exact_heads_for_test(
        &self,
        heads: &[crate::protocol::membership::MembershipHeadRef],
        resolutions: &[crate::protocol::membership::StoreMembershipConflictResolutionRef],
    ) -> Result<crate::protocol::membership::MembershipChain, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .load_membership_at_exact_heads_for_test(heads, resolutions)
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) async fn project_membership_for_test(
        &self,
        candidate_heads: &[crate::protocol::membership::MembershipHeadRef],
    ) -> Result<crate::protocol::membership::MembershipChain, StoreError> {
        let history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .project_membership_to_verified_prefix(
                candidate_heads,
                &verified_history::VerifiedMergeMembershipPrefix::default(),
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) async fn assert_deep_membership_projection_for_test(
        &self,
        heads: &[crate::protocol::membership::MembershipHeadRef],
    ) -> Result<(), StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .assert_deep_membership_projection_for_test(heads)
            .await;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn verify_device_join_attempt_for_test(
        &self,
        reference: &crate::protocol::store_commit::DeviceJoinAttemptRef,
        owner: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .verify_device_join_attempt_for_test(reference, owner)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn exact_next_announcement_slot_for_test(
        &self,
        registration_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        previous: Option<&crate::protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<
        (
            crate::storage::cloud::ObjectSlot,
            Option<crate::protocol::store_commit::StoreDeviceHeadRef>,
        ),
        StoreError,
    > {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .exact_next_announcement_slot_for_test(registration_ref, registration, previous)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_commit_for_test(
        &self,
        reference: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<crate::protocol::store_commit::VerifiedStoreBatchCommit, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .load_commit(reference)
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) async fn load_commit_ancestry_until_for_test(
        &self,
        start: crate::protocol::store_commit::StoreBatchCommitRef,
        coverage: &crate::protocol::store_commit::CommitFrontier,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::StoreBatchCommitRef,
            crate::protocol::store_commit::VerifiedStoreBatchCommit,
        )>,
        StoreError,
    > {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .load_commit_ancestry_until_for_test(start, coverage)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_registration_for_test(
        &self,
        reference: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<crate::protocol::store_commit::StoreDeviceRegistration, StoreError> {
        let history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(history.load_registration(reference).await?.value)
    }

    #[cfg(test)]
    pub(crate) async fn verify_snapshots_for_acknowledgement_for_test(
        &self,
        snapshots: &[crate::database::PublishedStoreSnapshot],
    ) -> Result<(), StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .verify_snapshots_for_acknowledgement(snapshots)
            .await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn open_circle_package_for_test(
        &self,
        access: &crate::sync::store::circle_controls::CirclePackageAccess,
        commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        reference: &crate::protocol::store_commit::CirclePackageRef,
    ) -> Result<Vec<u8>, StoreError> {
        let history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .open_circle_package_for_test(access, commit, reference)
            .await
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn pull_readiness_for_test(
        &self,
        coverage: &crate::protocol::store_commit::CommitFrontier,
        frontier: &std::collections::BTreeMap<
            String,
            crate::protocol::store_commit::StoreBatchCommitRef,
        >,
        device_state: &crate::protocol::store_commit::ResolvedStoreDeviceState,
        exclusion_freezes: &[crate::protocol::store_commit::StoreDeviceProposalAck],
        commit_ref: &crate::protocol::store_commit::StoreBatchCommitRef,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
    ) -> Result<pull::Readiness, pull::StorePullError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        history
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

    #[cfg(test)]
    pub(crate) async fn verified_merge_membership_prefix_for_test(
        &self,
        references: impl IntoIterator<Item = crate::protocol::store_commit::StoreBatchCommitRef>,
        predecessors: impl IntoIterator<Item = crate::protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<verified_history::VerifiedMergeMembershipPrefix, pull::StorePullError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        history
            .verified_merge_membership_prefix_for_test(references, predecessors)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn retained_merge_history_frontier_for_test(
        &self,
        references: Vec<crate::protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<
        Vec<crate::protocol::store_commit::OpenedRetainedMergeHistorySummary>,
        crate::database::DbError,
    > {
        self.database
            .retained_merge_history_frontier(self.store_root.clone(), references)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn verified_circle_activation_for_test(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        control: crate::protocol::circle::CircleControlCoord,
    ) -> Result<
        Option<crate::sync::store::circle_controls::VerifiedCircleReference>,
        crate::database::DbError,
    > {
        self.database
            .verified_circle_activation(self.store_root.clone(), circle_id, control)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn circle_package_is_retained_for_replay_for_test(
        &self,
        target: crate::protocol::store_commit::CirclePackageRef,
        activation: crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<bool, crate::database::DbError> {
        self.database
            .circle_package_is_retained_for_replay(self.store_root.clone(), target, activation)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_circle_acknowledgement_for_test(
        &self,
        reference: &crate::protocol::store_commit::CircleAckRef,
    ) -> Result<crate::protocol::store_commit::CircleAck, StoreAckError> {
        self.authorize_history()
            .await
            .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?
            .circles()
            .acknowledgements()
            .load(reference)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_circle_activations_for_test(
        &self,
        commit_ref: &crate::protocol::store_commit::StoreBatchCommitRef,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
        author: &crate::protocol::store_commit::StoreDeviceRegistration,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
    ) -> Result<
        crate::sync::store::circle_controls::VerifiedCircleActivations,
        crate::sync::store::CircleOperationError,
    > {
        let verified = crate::protocol::store_commit::VerifiedStoreBatchCommit::parse(
            &commit.to_bytes(),
            self.store_root.store_root_hash,
            commit_ref,
            author,
        )
        .map_err(|error| {
            crate::sync::store::CircleOperationError::InvalidState(error.to_string())
        })?;
        let mut history = self.authorize_history().await.map_err(|error| {
            crate::sync::store::CircleOperationError::InvalidState(error.to_string())
        })?;
        history
            .circles()
            .activations()
            .load(&verified, &self.identity, routing_key)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_applicable_circle_packages_for_test(
        &self,
        verified: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        activations: &[crate::sync::store::circle_controls::VerifiedCircleReference],
        author: &crate::protocol::store_commit::StoreDeviceRegistration,
        local_store_membership: pull::LocalStoreMembership,
    ) -> Result<Vec<pull::LoadedCirclePackage>, circles::CirclePackageReadError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| circles::CirclePackageReadError::Invalid(error.to_string()))?;
        history
            .circles()
            .packages()
            .load_applicable(verified, activations, author, local_store_membership)
            .await
    }

    #[cfg(test)]
    pub(crate) fn protocol_root_for_test(&self) -> &StoreProtocolRoot {
        &self.protocol_root.value
    }

    #[cfg(test)]
    pub(crate) async fn export_activated_device_continuation_for_test(
        &self,
    ) -> Result<crate::restoration::ActivatedContinuation, crate::database::DbError> {
        self.database
            .export_activated_device_continuation(&self.identity)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn stage_acknowledgement_for_test(
        &self,
        frontier: crate::protocol::store_commit::CommitFrontier,
        sync_time: String,
    ) -> Result<crate::protocol::store_commit::StoreAck, writer::StoreAckError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| writer::StoreAckError::InvalidOutbound(error.to_string()))?;
        writer.stage_acknowledgement(frontier, sync_time).await
    }

    #[cfg(test)]
    pub(crate) async fn drain_acknowledgements_for_test(
        &self,
    ) -> Result<u64, writer::StoreAckError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| writer::StoreAckError::InvalidOutbound(error.to_string()))?;
        writer.drain_acknowledgements().await
    }

    #[cfg(test)]
    pub(crate) async fn prepare_acknowledgement_activation_for_test(
        &self,
        acknowledgement: crate::protocol::store_commit::StoreAckRef,
        candidate: crate::sync::store::operations::PreparedStoreOperationCommit,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .prepare_acknowledgement_activation(acknowledgement, candidate)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn stage_circle_acknowledgements_for_test(
        &self,
        frontier: &crate::protocol::store_commit::CommitFrontier,
        sync_time: &str,
    ) -> Result<(), writer::StoreAckError> {
        self.authorize_writer()
            .await
            .map_err(|error| writer::StoreAckError::InvalidOutbound(error.to_string()))?
            .circles()
            .acknowledgements()
            .stage(frontier, sync_time)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn publish_snapshot_for_test(
        &self,
        snapshot: writer::snapshot::CreatedSnapshot,
        coverage: crate::protocol::store_commit::CommitFrontier,
        created_at: String,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, writer::snapshot::SnapshotError> {
        let mut writer = self.authorize_writer().await.map_err(|error| {
            writer::snapshot::SnapshotError::PublicationState(error.to_string())
        })?;
        writer
            .push_store_snapshot(
                snapshot,
                coverage,
                self.database.schema_version(),
                created_at,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_founder_registration_for_test(
        &self,
    ) -> Result<
        crate::storage::VerifiedObject<crate::protocol::store_commit::StoreDeviceRegistration>,
        StoreError,
    > {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history.load_founder_registration_for_test().await
    }

    #[cfg(test)]
    pub(crate) async fn prepare_merge_history_successor_for_test(
        &self,
        verified_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        recovery_author: Option<&crate::protocol::store_commit::StoreDeviceRegistrationRef>,
        evidence: verified_history::MergeHistorySuccessorEvidence,
    ) -> Result<verified_history::PreparedMergeHistorySuccessor, StoreError> {
        let mut authorized = self
            .authorize()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let membership = authorized.membership().clone();
        authorized
            .history()
            .prepare_merge_history_successor_for_test(
                verified_commit,
                &membership,
                recovery_author,
                evidence,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn prepare_device_join_bootstrap_for_test(
        &self,
        coverage: &crate::protocol::store_commit::StoreHistoryCut,
        attempt_activation: &crate::protocol::store_commit::StoreBatchCommitRef,
        membership_state: &crate::protocol::circle_control::StoreMembershipStateRef,
    ) -> Result<crate::sync::store::owner::pull::DeviceJoinBootstrapPlan, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .prepare_device_join_bootstrap_for_test(coverage, attempt_activation, membership_state)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_store_package_for_test(
        &self,
        reference: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<Option<crate::storage::VerifiedObject<Vec<u8>>>, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history.load_store_package_for_test(reference).await
    }

    #[cfg(test)]
    pub(crate) async fn load_store_ack_for_test(
        &self,
        reference: &crate::protocol::store_commit::StoreAckRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<crate::protocol::store_commit::StoreAck, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .load_store_ack_for_test(reference, registration)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_head_for_test(
        &self,
        reference: &crate::protocol::store_commit::StoreDeviceHeadRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        commit: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<crate::protocol::store_commit::StoreDeviceHead, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .load_head_for_test(reference, registration, commit)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invite_member(
        &self,
        hlc: &crate::sync::hlc::Hlc,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: crate::protocol::membership::MemberRole,
        encryption: &crate::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<crate::joining::InviteCode, crate::sync::store::membership::MembershipOpsError>
    {
        let mut authorization = self.authorize_writer().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
        authorization
            .invite_member(
                hlc,
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
        hlc: &crate::sync::hlc::Hlc,
        public_key_hex: &str,
        encryption: &crate::encryption::EncryptionService,
        security: &crate::store_security::StoreSecurity,
        cipher: &dyn crate::storage::CloudCipherAccess,
        pending_rotation: &crate::storage::PendingRotation,
    ) -> Result<String, crate::sync::store::membership::MembershipOpsError> {
        let mut authorization = self.authorize_writer().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
        authorization
            .remove_member(
                hlc,
                public_key_hex,
                encryption,
                security,
                cipher,
                pending_rotation,
            )
            .await
    }
}

impl<'storage> AuthorizedStore<'storage> {
    fn circle_operation_discarder(&mut self) -> circles::CircleOperationDiscarder<'_, 'storage> {
        self.history.circle_operation_discarder()
    }

    pub(super) async fn discard_circle_operation(
        &mut self,
        operation_id: &crate::protocol::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::circle_controls::CircleOperationError> {
        self.circle_operation_discarder()
            .discard(operation_id)
            .await
    }

    #[cfg(test)]
    fn keyring<'operation>(
        &'operation self,
        membership: &'operation crate::protocol::membership::MembershipChain,
    ) -> keyring::AuthorizedMembershipKeyring<'operation, 'storage> {
        self.history.keyring(self.identity, membership)
    }

    #[cfg(test)]
    fn history(&mut self) -> &mut AuthorizedStoreHistory<'storage> {
        &mut self.history
    }

    pub(super) fn membership(&self) -> &crate::protocol::membership::MembershipChain {
        &self.membership
    }

    fn resolved_membership(
        &self,
    ) -> Result<
        &crate::protocol::membership::MembershipChain,
        crate::sync::store::membership::MembershipOpsError,
    > {
        match self.membership.conflict() {
            Some(conflict) => Err(
                crate::sync::store::membership::MembershipOpsError::SemanticConflict(Box::new(
                    conflict.clone(),
                )),
            ),
            None => Ok(&self.membership),
        }
    }

    fn members(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Result<
        Vec<crate::protocol::membership::MemberInfo>,
        crate::sync::store::membership::MembershipOpsError,
    > {
        Ok(member_info(
            self.resolved_membership()?.current_members(),
            user_pubkey,
        ))
    }

    fn membership_conflict(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Option<crate::protocol::membership::MembershipConflictInfo> {
        match self.membership.status() {
            crate::protocol::membership::MembershipStatus::Resolved(_) => None,
            crate::protocol::membership::MembershipStatus::Conflict(
                crate::protocol::membership::MembershipConflict::ConcurrentMemberAssignments {
                    conflict_hash,
                    member_pubkey,
                    conflicting_grants,
                    grants,
                    ..
                },
            ) => Some(
                crate::protocol::membership::MembershipConflictInfo::ConcurrentMemberAssignments {
                    id: conflict_hash.to_string(),
                    member_pubkey: member_pubkey.clone(),
                    choices: conflicting_grants
                        .iter()
                        .map(|(selected_grant, selected_record)| {
                            let selection = crate::protocol::membership::MembershipConflictSelection::MemberAssignment {
                                grant: selected_grant.clone(),
                            };
                            let members = member_info(
                                grants
                                    .iter()
                                    .filter_map(|(grant, state)| {
                                        (!conflicting_grants.contains_key(grant))
                                            .then(|| state.active())
                                            .flatten()
                                            .map(|record| {
                                                (
                                                    record.member_pubkey.clone(),
                                                    record.role.role(),
                                                )
                                            })
                                    })
                                    .chain(std::iter::once((
                                        selected_record.member_pubkey.clone(),
                                        selected_record.role.role(),
                                    )))
                                    .collect(),
                                user_pubkey,
                            );
                            crate::protocol::membership::MembershipConflictChoice::new(
                                membership_conflict_choice_id(&selection),
                                members,
                                *conflict_hash,
                                selection,
                            )
                        })
                        .collect(),
                },
            ),
            crate::protocol::membership::MembershipStatus::Conflict(
                crate::protocol::membership::MembershipConflict::RevocationCycle {
                    conflict_hash,
                    maximal_valid_branches,
                    ..
                },
            ) => Some(
                crate::protocol::membership::MembershipConflictInfo::RevocationCycle {
                    id: conflict_hash.to_string(),
                    choices: maximal_valid_branches
                        .iter()
                        .map(|branch| {
                            let selection = crate::protocol::membership::MembershipConflictSelection::RevocationBranch {
                                heads: branch.heads.clone(),
                            };
                            let members = member_info(
                                branch
                                    .active_grants()
                                    .map(|(_, record)| {
                                        (record.member_pubkey.clone(), record.role.role())
                                    })
                                    .collect(),
                                user_pubkey,
                            );
                            crate::protocol::membership::MembershipConflictChoice::new(
                                membership_conflict_choice_id(&selection),
                                members,
                                *conflict_hash,
                                selection,
                            )
                        })
                        .collect(),
                },
            ),
        }
    }
}

fn membership_conflict_choice_id(
    selection: &crate::protocol::membership::MembershipConflictSelection,
) -> String {
    let selection_bytes =
        serde_json::to_vec(selection).expect("membership conflict selections always serialize");
    let mut bytes = b"coven.membership-conflict-choice.v1\0".to_vec();
    bytes.extend(selection_bytes);
    crate::protocol::store_commit::ObjectHash::digest(&bytes).to_string()
}

fn member_info(
    current: Vec<(String, crate::protocol::membership::MemberRole)>,
    user_pubkey: Option<&[u8]>,
) -> Vec<crate::protocol::membership::MemberInfo> {
    let user_pubkey_hex = user_pubkey.map(hex::encode);
    current
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>()
        .into_iter()
        .map(|(pubkey, role)| crate::protocol::membership::MemberInfo {
            is_self: user_pubkey_hex.as_deref() == Some(&pubkey),
            pubkey,
            role,
        })
        .collect()
}

async fn load_local_store_device(
    database: &StoreDatabase,
    root: &StoreRootRef,
    expected_device_id: &str,
) -> Result<LocalStoreDevice, StoreError> {
    let durable = database
        .latest_local_store_device_registration()
        .await?
        .ok_or(StoreError::MissingState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        })?;
    if durable.device_id.to_string() != expected_device_id {
        return Err(StoreError::InvalidState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            reason: "local registration belongs to another device".to_string(),
        });
    }
    let registration = crate::protocol::store_commit::StoreDeviceRegistration::parse_at(
        &durable.registration_bytes,
        root,
        durable.device_id,
    )
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let registration_ref =
        crate::protocol::store_commit::StoreDeviceRegistrationRef::from_registration(
            &registration,
            durable.prepared.reference().clone(),
        );
    if registration_ref.registration_hash != durable.registration_hash {
        return Err(StoreError::InvalidOutbound(
            "local registration differs from its durable hash".to_string(),
        ));
    }
    let activation = match durable.state {
        crate::database::LocalDeviceRegistrationState::Activated { authority } => {
            let activated = database
                .activated_store_device_registration_with_authority(root, registration_ref.clone())
                .await?;
            if activated != (registration.clone(), authority.clone()) {
                return Err(StoreError::InvalidOutbound(
                    "local registration differs from its exact activation authority".to_string(),
                ));
            }
            Some(authority)
        }
        crate::database::LocalDeviceRegistrationState::Prepared
        | crate::database::LocalDeviceRegistrationState::Created => None,
    };
    Ok(LocalStoreDevice {
        registration_ref,
        registration,
        activation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::test_helpers::{open_test_db, TestStore};

    #[tokio::test]
    async fn loaded_store_authorization_retains_its_verified_root() {
        let db = open_test_db();
        let fixture = TestStore::create(&db, "retained-root-authority", UserKeypair::generate())
            .await
            .expect("create Store");
        let store = Store::load(
            crate::database::StoreDatabase::new(&db),
            fixture.storage.clone(),
            fixture.signer.clone(),
        )
        .await
        .expect("load Store");

        fixture.home.remove_exact_object(fixture.root.object.slot());

        store
            .authorize()
            .await
            .expect("authorize from the root verified while loading");
    }
}
