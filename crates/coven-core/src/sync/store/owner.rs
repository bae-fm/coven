use super::*;
use crate::sync::store_commit::StoreRootRef;

mod acknowledgements;
mod circle_operation;
mod circles;
pub(super) mod device_exclusion;
pub(super) mod device_join;
pub(super) mod history;
mod host_write;
mod keyring;
pub(super) mod owner_promotion;
pub(super) mod pull;
mod registration;
mod registration_outbox;
mod restore;
mod verification;
pub(super) mod writer;

pub use host_write::HostWriteBlobStaging;
pub use registration::StoreRegistrationError;
pub(crate) use registration::{bootstrap_pending_device, prepare_registration_for_origin};
pub use restore::RestoringStore;
use verification::StoreCommitVerifier;
pub(super) use writer::{operations, reclaim, snapshot};
pub(crate) use writer::{AuthorizedWriterOperation, StoreAckError};

#[doc(hidden)]
pub struct Store {
    database: StoreDatabase,
    storage: Arc<dyn SyncStorage>,
    identity: UserKeypair,
    device_id: Option<String>,
    store_root: StoreRootRef,
    protocol_root: crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>,
}

#[doc(hidden)]
pub struct StoreRestoreMembership {
    pub store_root: StoreRootRef,
    pub founder_pubkey: String,
    pub membership_floor: crate::join_code::MembershipFloor,
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

pub(super) struct AuthorizedStoreHistory<'a> {
    database: StoreDatabase,
    history_verifier: crate::sync::store::owner::pull::MergeHistoryVerifier<'a>,
}

struct BootstrappedStore<'storage> {
    history: AuthorizedStoreHistory<'storage>,
    membership: crate::sync::membership::MembershipChain,
    identity: UserKeypair,
}

struct LocalStoreDevice {
    registration_ref: crate::sync::store_commit::StoreDeviceRegistrationRef,
    registration: crate::sync::store_commit::StoreDeviceRegistration,
    activation: Option<crate::sync::store_commit::StoreDeviceRegistrationActivation>,
}

pub(crate) struct AuthorizedStore<'a> {
    history: AuthorizedStoreHistory<'a>,
    storage: &'a Arc<dyn SyncStorage>,
    identity: &'a UserKeypair,
    local_device: Option<LocalStoreDevice>,
    membership: crate::sync::membership::MembershipChain,
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
    #[doc(hidden)]
    pub fn host_write_blob_staging(
        self: &Arc<Self>,
        runtime: tokio::runtime::Handle,
        store_dir: StoreDir,
    ) -> HostWriteBlobStaging {
        HostWriteBlobStaging::new(runtime, self.clone(), store_dir)
    }

    pub async fn open_invitation_keyring(
        bootstrap_storage: &dyn SyncStorage,
        keypair: &UserKeypair,
        invitation: &crate::join_code::InviteCode,
    ) -> Result<crate::encryption::EncryptionService, membership::InviteError> {
        let recipient = hex::encode(keypair.public_key());
        if invitation.wrapped_key.recipient_pubkey != recipient {
            return Err(membership::InviteError::Crypto(
                "invite wrapped-key ref names another recipient".to_string(),
            ));
        }
        crate::sync::membership::validate_membership_floor(&invitation.membership_floor.0)
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
        let _creation = database.lock_store_creation().await;
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
        let protocol_root = match Box::pin(Self::publish_founder_graph(
            &database,
            &*storage,
            founder_timestamp,
            identity,
            &graph,
        ))
        .await
        {
            Ok(root) => root,
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
        let store_root = database
            .local_store_root_ref()
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?
            .ok_or_else(|| {
                StoreInitializationError::ProtocolRoot(
                    "published Store founder graph has no durable exact root".to_string(),
                )
            })?;
        Self::finish_initialization(database, storage, store_root, protocol_root, identity).await
    }

    async fn publish_founder_graph(
        database: &StoreDatabase,
        storage: &dyn SyncStorage,
        founder_timestamp: &str,
        identity: &UserKeypair,
        graph: &crate::database::DurableFounderGraph,
    ) -> Result<
        crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>,
        protocol_root::StoreProtocolRootError,
    > {
        let root = StoreRootRef {
            store_root_id: graph.root.value.descriptor.store_root_id(),
            store_root_hash: graph.root.value.object_hash(),
            object: graph.root.object.clone(),
        };
        if graph.initial_ack.value.last_sync != founder_timestamp {
            return Err(protocol_root::StoreProtocolRootError::Database(
                "durable Store founder timestamp differs from this creation request".to_string(),
            ));
        }
        let protocol_root = StoreProtocolRoot::parse_expected(
            &graph.root.bytes,
            &root,
            database.sqlite().sync_routing_hash(),
        )
        .map_err(|error| protocol_root::StoreProtocolRootError::Database(error.to_string()))?;
        if protocol_root.descriptor.founder_pubkey != crate::keys::public_key_hex(identity) {
            return Err(protocol_root::StoreProtocolRootError::Database(
                "durable Store founder differs from the creation signer".to_string(),
            ));
        }
        if protocol_root.descriptor.schema_version > database.sqlite().schema_version() {
            return Err(protocol_root::StoreProtocolRootError::SchemaTooNew {
                root_schema: protocol_root.descriptor.schema_version,
                local: database.sqlite().schema_version(),
            });
        }
        let registration_ref =
            crate::sync::store_commit::StoreDeviceRegistrationRef::from_registration(
                &graph.registration.value,
                graph.registration.object.clone(),
            );
        storage
            .create_protocol_object(&graph.root.prepared)
            .await
            .map_err(crate::sync::store_objects::StoreObjectError::from)?;
        let opened_root = protocol_root::load_exact_store_protocol_root(
            storage,
            &root,
            database.sqlite().sync_routing_hash(),
        )
        .await?;
        if opened_root.value != protocol_root {
            return Err(protocol_root::StoreProtocolRootError::Missing(
                root.store_root_hash,
            ));
        }
        let commit_verifier =
            StoreCommitVerifier::from_verified_root(storage, &root, opened_root.clone()).map_err(
                |error| protocol_root::StoreProtocolRootError::Database(error.to_string()),
            )?;
        storage
            .create_protocol_object(&graph.registration.prepared)
            .await
            .map_err(crate::sync::store_objects::StoreObjectError::from)?;
        let registration = commit_verifier
            .load_registration(&registration_ref)
            .await?
            .value;
        if registration != graph.registration.value {
            return Err(protocol_root::StoreProtocolRootError::Database(
                "founder registration readback differs from durable bytes".to_string(),
            ));
        }
        storage
            .create_protocol_object(&graph.initial_ack.prepared)
            .await
            .map_err(crate::sync::store_objects::StoreObjectError::from)?;
        let initial_ack = commit_verifier
            .load_store_ack(&graph.initial_ack_ref, &registration)
            .await?
            .value;
        if initial_ack != graph.initial_ack.value {
            return Err(protocol_root::StoreProtocolRootError::Database(
                "founder initial acknowledgement readback differs from durable bytes".to_string(),
            ));
        }
        if !matches!(
            &graph.registration_state,
            crate::database::LocalDeviceRegistrationState::Activated { .. }
        ) {
            database
                .mark_local_store_device_registration_created(
                    graph.registration.clone(),
                    graph.initial_ack_ref.clone(),
                    graph.initial_ack.clone(),
                )
                .await
                .map_err(|error| {
                    protocol_root::StoreProtocolRootError::Database(error.to_string())
                })?;
        }
        let membership = &graph.membership;
        storage
            .create_protocol_object(&membership.entry.prepared)
            .await
            .map_err(crate::sync::store_objects::StoreObjectError::from)?;
        let loaded_entry = crate::sync::store_objects::load_membership_entry_ref(
            storage,
            root.store_root_hash,
            &membership.entry_ref,
        )
        .await?
        .value;
        if loaded_entry != membership.entry.value {
            return Err(protocol_root::StoreProtocolRootError::Database(
                "founder membership entry readback differs from durable bytes".to_string(),
            ));
        }
        storage
            .create_protocol_object(&membership.head.prepared)
            .await
            .map_err(crate::sync::store_objects::StoreObjectError::from)?;
        let loaded_head = crate::sync::store_objects::load_membership_head_ref(
            storage,
            root.store_root_hash,
            &membership.head_ref,
            &registration,
        )
        .await?
        .value;
        if loaded_head != membership.head.value {
            return Err(protocol_root::StoreProtocolRootError::Database(
                "founder membership head readback differs from durable bytes".to_string(),
            ));
        }
        database
            .complete_store_founder_graph(
                root,
                registration_ref,
                graph.initial_ack_ref.clone(),
                crate::database::FounderMembershipRefs {
                    entry: membership.entry_ref.clone(),
                    head: membership.head_ref.clone(),
                },
            )
            .await
            .map_err(|error| protocol_root::StoreProtocolRootError::Database(error.to_string()))?;
        Ok(opened_root)
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
        Self::finish_initialization(
            database,
            storage,
            expected_root.clone(),
            protocol_root,
            identity,
        )
        .await
    }

    async fn open_protocol_root(
        database: &StoreDatabase,
        storage: &dyn SyncStorage,
        expected: &StoreRootRef,
    ) -> Result<
        crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>,
        crate::sync::store::protocol_root::StoreProtocolRootError,
    > {
        let verified = crate::sync::store::protocol_root::load_exact_store_protocol_root(
            storage,
            expected,
            database.sqlite().sync_routing_hash(),
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
            let registration = crate::sync::store_commit::StoreDeviceRegistration::parse_at(
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
        if verified.value.descriptor.schema_version > database.sqlite().schema_version() {
            return Err(
                crate::sync::store::protocol_root::StoreProtocolRootError::SchemaTooNew {
                    root_schema: verified.value.descriptor.schema_version,
                    local: database.sqlite().schema_version(),
                },
            );
        }
        Ok(verified)
    }

    async fn finish_initialization(
        database: StoreDatabase,
        storage: Arc<dyn SyncStorage>,
        store_root: StoreRootRef,
        protocol_root: crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let mut device_id = database
            .sqlite()
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        let identity_is_founder =
            protocol_root.value.descriptor.founder_pubkey == crate::keys::public_key_hex(identity);
        if device_id.is_none() && !identity_is_founder {
            return Err(StoreInitializationError::ProtocolRoot(
                "opening a Store for a non-founder requires an installed local device".to_string(),
            ));
        }
        let commit_verifier =
            StoreCommitVerifier::from_verified_root(&*storage, &store_root, protocol_root)
                .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        let history_verifier =
            crate::sync::store::owner::pull::MergeHistoryVerifier::from_commit_verifier(
                commit_verifier,
            )
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        let mut history = AuthorizedStoreHistory {
            database: database.clone(),
            history_verifier,
        };
        let founder_pubkey = history
            .history_verifier
            .verified_root()
            .descriptor
            .founder_pubkey
            .clone();
        history
            .load_and_install_owner_membership(&founder_pubkey)
            .await
            .map_err(|error| StoreInitializationError::MembershipAnchor(error.to_string()))?;

        if device_id.is_none() && identity_is_founder {
            registration::install_existing_founder_device(
                &database,
                history.history_verifier.commit_verifier_ref(),
                identity,
            )
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
            device_id = database
                .sqlite()
                .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                .await
                .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        }
        let device_id = device_id.ok_or_else(|| {
            StoreInitializationError::ProtocolRoot(
                "initialized Store has no local device registration id".to_string(),
            )
        })?;
        let protocol_root = history.history_verifier.verified_root_object().clone();
        drop(history);
        let store = Self::new(
            database,
            storage,
            identity.clone(),
            Some(device_id.clone()),
            store_root,
            protocol_root,
        )
        .map_err(StoreInitializationError::ProtocolRoot)?;
        Ok(InitializedStore { store, device_id })
    }

    #[doc(hidden)]
    pub async fn load(
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
            .sqlite()
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
        protocol_root: crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>,
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
    pub(crate) fn storage(&self) -> &Arc<dyn SyncStorage> {
        &self.storage
    }

    pub(crate) fn store_root(&self) -> &StoreRootRef {
        &self.store_root
    }

    pub(crate) fn database(&self) -> &StoreDatabase {
        &self.database
    }

    pub(crate) fn identity(&self) -> &UserKeypair {
        &self.identity
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.storage().blob_path_scheme()
    }

    pub(crate) fn self_uploader(&self) -> String {
        self.storage().self_uploader()
    }

    pub(crate) fn cloud_home(&self) -> &dyn CloudHome {
        self.storage().cloud_home()
    }

    #[cfg(test)]
    pub(crate) async fn circle_package_access(
        &self,
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
    ) -> Result<
        Option<crate::sync::store::circle_controls::CirclePackageAccess>,
        crate::database::DbError,
    > {
        self.database
            .circle_package_access(self.store_root.clone(), circle_id, expected_control)
            .await
    }

    #[doc(hidden)]
    pub async fn discard_blocked_write(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, crate::sync::store::StoreError> {
        if let BlockedWriteDiscard::Discarded(discarded) =
            self.database().discard_blocked_write(&write_id).await?
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

        match self.database().discard_blocked_write(&write_id).await? {
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
        history::abandonment::abandon_merge_candidate(&mut writer, write_id).await
    }

    #[doc(hidden)]
    pub async fn members(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Result<Vec<crate::sync::membership::MemberInfo>, membership::MembershipOpsError> {
        let authorization = self.authorize().await.map_err(|error| {
            membership::MembershipOpsError::Chain(membership::AnchoredChainError::LoadFailed(
                error.to_string(),
            ))
        })?;
        authorization.members(user_pubkey)
    }

    #[doc(hidden)]
    pub async fn membership_conflict(
        &self,
        user_pubkey: Option<&[u8]>,
    ) -> Result<
        Option<crate::sync::membership::MembershipConflictInfo>,
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
        choice: &crate::sync::membership::MembershipConflictChoice,
        created_at: &str,
    ) -> Result<
        crate::sync::membership::StoreMembershipConflictResolutionRef,
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
    pub async fn restore_membership(
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
            membership_floor: crate::join_code::MembershipFloor(membership_floor),
        })
    }

    async fn authorize_history(&self) -> Result<AuthorizedStoreHistory<'_>, SyncCycleFailure> {
        let commit_verifier = StoreCommitVerifier::from_verified_root(
            &*self.storage,
            &self.store_root,
            self.protocol_root.clone(),
        )
        .map_err(|error| SyncCycleFailure::operation("open Store history authority", error))?;
        let history_verifier =
            crate::sync::store::owner::pull::MergeHistoryVerifier::from_commit_verifier(
                commit_verifier,
            )
            .await
            .map_err(|error| SyncCycleFailure::operation("open Store history authority", error))?;
        Ok(AuthorizedStoreHistory {
            database: self.database.clone(),
            history_verifier,
        })
    }

    pub(crate) async fn authorize(&self) -> Result<AuthorizedStore<'_>, SyncCycleFailure> {
        let mut authority = self.authorize_history().await?;
        let owner = authority
            .database
            .validated_store_owner(&self.store_root)
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("validate Store owner authority", error)
            })?;
        let membership = authority
            .load_current_membership(&owner)
            .await
            .map_err(|error| SyncCycleFailure::operation("load membership chain", error))?;
        let local_device = match self.device_id.as_deref() {
            Some(device_id) => Some(
                load_local_store_device(
                    &authority.database,
                    authority.history_verifier.root(),
                    device_id,
                )
                .await
                .map_err(|error| {
                    SyncCycleFailure::operation("load local Store device authority", error)
                })?,
            ),
            None => None,
        };
        Ok(AuthorizedStore {
            history: authority,
            storage: &self.storage,
            identity: &self.identity,
            local_device,
            membership,
        })
    }

    pub(crate) async fn authorize_writer(
        &self,
    ) -> Result<AuthorizedWriterOperation<'_>, writer::StoreWriterAuthorizationError> {
        registration_outbox::drain(&self.database, &*self.storage)
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
        value: crate::sync::wrapped_store_key::WrappedStoreKey,
    ) -> Result<crate::sync::wrapped_store_key::PreparedWrappedStoreKey, String> {
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
    pub async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::sync::store::DeviceJoinOffer, crate::sync::store::DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| crate::sync::store::DeviceJoinError::Store(error.to_string()))?;
        let provider_admin_grant = writer
            .protocol_root()
            .descriptor
            .founder_provider_admin
            .grant_id
            .clone();
        writer
            .join_operation()
            .begin(member_pubkey, provider_admin_grant)
            .await
    }

    pub async fn begin_device_join_bundle(
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
        let provider_admin_grant = writer
            .protocol_root()
            .descriptor
            .founder_provider_admin
            .grant_id
            .clone();
        let offer = writer
            .join_operation()
            .begin(member_pubkey, provider_admin_grant)
            .await?;
        crate::sync::store::DeviceJoinOfferBundle::allocate(writer.storage(), offer).await
    }

    pub async fn begin_owner_promotion(
        &self,
        member_registration: crate::sync::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        crate::sync::store_commit::OwnerPromotionRequest,
        owner_promotion::OwnerPromotionError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| owner_promotion::OwnerPromotionError::Protocol(error.to_string()))?;
        owner_promotion::begin(&mut writer, member_registration).await
    }

    pub async fn accept_owner_promotion(
        &self,
        request: crate::sync::store_commit::OwnerPromotionRequest,
    ) -> Result<
        crate::sync::store_commit::OwnerPromotionAcceptance,
        owner_promotion::OwnerPromotionError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| owner_promotion::OwnerPromotionError::Protocol(error.to_string()))?;
        owner_promotion::accept(&mut writer, request).await
    }

    pub async fn finalize_owner_promotion(
        &self,
        encryption: &crate::encryption::EncryptionService,
        acceptance: crate::sync::store_commit::OwnerPromotionAcceptance,
    ) -> Result<
        crate::sync::circle_control::StoreMembershipStateRef,
        owner_promotion::OwnerPromotionError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| owner_promotion::OwnerPromotionError::Protocol(error.to_string()))?;
        owner_promotion::finalize(&mut writer, encryption, acceptance).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn latest_local_store_position(
        &self,
    ) -> Result<Option<crate::sync::store_commit::StoreBatchCommitRef>, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
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
            crate::sync::store_commit::StoreBatchCommitRef,
            crate::sync::store_commit::VerifiedStoreBatchCommit,
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
            .history_verifier_mut()
            .load_ref(&reference)
            .await
            .map_err(|error| error.to_string())?;
        Ok(Some((reference, commit)))
    }

    #[cfg(test)]
    pub(crate) async fn announcement_stream_id_for_test(
        &self,
    ) -> Result<crate::sync::membership::AuthorStreamId, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(writer.announcement_stream_id())
    }

    #[cfg(test)]
    pub(crate) async fn sign_device_head_for_test(
        &self,
        commit: crate::sync::store_commit::StoreBatchCommitRef,
        history_summary: crate::sync::store_commit::ObjectHash,
        successor: crate::sync::store_commit::SuccessorLink,
    ) -> Result<crate::sync::store_commit::StoreDeviceHead, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        crate::sync::store_commit::StoreDeviceHead::signed(
            writer.store_root().store_root_hash,
            writer.writer.registration_ref.clone(),
            commit,
            history_summary,
            successor,
            &writer.writer.device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) async fn resign_snapshot_meta_for_test(
        &self,
        meta: crate::sync::store_commit::SnapshotMeta,
    ) -> Result<crate::sync::store_commit::SnapshotMeta, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        if meta.store_root_hash != writer.store_root().store_root_hash
            || meta.author_registration != writer.writer.registration_ref
        {
            return Err(StoreError::InvalidOutbound(
                "snapshot test input belongs to another Store writer".to_string(),
            ));
        }
        crate::sync::store_commit::SnapshotMeta::signed(
            meta.store_root_hash,
            writer.writer.registration_ref.clone(),
            meta.generation,
            meta.predecessor,
            meta.image,
            meta.coverage,
            meta.state,
            meta.history_summary,
            meta.schema_version,
            meta.created_at,
            meta.successor,
            &writer.writer.device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) async fn parse_local_snapshot_meta_for_test(
        &self,
        bytes: &[u8],
        reference: &crate::sync::store_commit::StoreSnapshotRef,
    ) -> Result<crate::sync::store_commit::SnapshotMeta, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        crate::sync::store_commit::SnapshotMeta::parse_at(
            bytes,
            writer.store_root().store_root_hash,
            reference,
            &writer.writer.registration,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
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
        order: &crate::sync::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[crate::sync::membership::MembershipHeadRef],
    ) -> Result<pull::MergeOutboundAuthorization, StoreError> {
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
    ) -> Result<crate::sync::store_commit::StoreDeviceRegistrationRef, StoreError> {
        let writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(writer.writer.registration_ref.clone())
    }

    #[cfg(test)]
    pub(crate) async fn observe_excluded_candidate_head_for_test(
        &self,
        candidate: &crate::sync::store_commit::StoreDeviceHead,
        candidate_commit: &crate::sync::store_commit::StoreBatchCommit,
        candidate_object: &crate::sync::storage::ExactObjectRef,
    ) -> Result<history::abandonment::ExcludedCandidateHeadObservation, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let verified_commit = history
            .history_verifier_mut()
            .commit_verifier()
            .authenticate_bytes(&candidate.commit, &candidate_commit.to_bytes())
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
        pending_rotation: &crate::sync::cloud_storage::PendingRotation,
        adopted_generation: u64,
    ) -> Result<(), membership::InviteError> {
        self.authorize_writer()
            .await
            .map_err(|error| membership::InviteError::Database(error.to_string()))?
            .complete_revoke_rotation_adoption_for_test(pending_rotation, adopted_generation)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn membership_for_test(
        &self,
    ) -> Result<crate::sync::membership::MembershipChain, StoreError> {
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
        reference: crate::sync::store_commit::StoreBatchCommitRef,
    ) -> Result<crate::database::OwnedVerifiedMergeMaterialization, crate::database::DbError> {
        self.database
            .retained_merge_materialization(self.store_root.clone(), reference)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn prepare_conflict_resolution_plan_for_test(
        &self,
        candidate_membership_heads: &[crate::sync::membership::MembershipHeadRef],
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
        reference: &crate::sync::membership::MembershipHeadRef,
    ) -> Result<crate::sync::membership::AuthorHead, StoreError> {
        let history = self
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
        heads: &[crate::sync::membership::MembershipHeadRef],
        resolutions: &[crate::sync::membership::StoreMembershipConflictResolutionRef],
    ) -> Result<crate::sync::membership::MembershipChain, StoreError> {
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
        candidate_heads: &[crate::sync::membership::MembershipHeadRef],
        prefix: &crate::sync::store::owner::pull::VerifiedMergeMembershipPrefix,
    ) -> Result<crate::sync::membership::MembershipChain, StoreError> {
        let history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .project_membership_to_verified_prefix(candidate_heads, prefix)
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) async fn assert_deep_membership_projection_for_test(
        &self,
        heads: &[crate::sync::membership::MembershipHeadRef],
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
        reference: &crate::sync::store_commit::DeviceJoinAttemptRef,
        owner: &crate::sync::store_commit::StoreDeviceRegistration,
    ) -> Result<(), StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .history_verifier_mut()
            .load_verified_device_join_attempt(reference, owner)
            .await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn exact_next_announcement_slot_for_test(
        &self,
        registration_ref: &crate::sync::store_commit::StoreDeviceRegistrationRef,
        registration: &crate::sync::store_commit::StoreDeviceRegistration,
        previous: Option<&crate::sync::store_commit::StoreBatchCommitRef>,
    ) -> Result<
        (
            crate::storage::cloud::ObjectSlot,
            Option<crate::sync::store_commit::StoreDeviceHeadRef>,
        ),
        StoreError,
    > {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let previous = match previous {
            Some(reference) => Some(
                history
                    .history_verifier_mut()
                    .commit_verifier()
                    .load_ref(reference)
                    .await?,
            ),
            None => None,
        };
        history
            .history_verifier_mut()
            .commit_verifier()
            .exact_next_announcement_slot(registration_ref, registration, previous.as_ref())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_commit_for_test(
        &self,
        reference: &crate::sync::store_commit::StoreBatchCommitRef,
    ) -> Result<crate::sync::store_commit::VerifiedStoreBatchCommit, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(history
            .history_verifier_mut()
            .commit_verifier()
            .load_ref(reference)
            .await?)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_commit_ancestry_until_for_test(
        &self,
        start: crate::sync::store_commit::StoreBatchCommitRef,
        coverage: &crate::sync::store_commit::CommitFrontier,
    ) -> Result<
        Vec<(
            crate::sync::store_commit::StoreBatchCommitRef,
            crate::sync::store_commit::VerifiedStoreBatchCommit,
        )>,
        StoreError,
    > {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let mut ancestry = Vec::new();
        let mut cursor = start;
        while !coverage.0.values().any(|covered| covered == &cursor) {
            let commit = history
                .history_verifier_mut()
                .commit_verifier()
                .load_ref(&cursor)
                .await?;
            let predecessor = commit.order.predecessor().cloned().ok_or_else(|| {
                StoreError::InvalidOutbound(
                    "commit ancestry ended before snapshot coverage".to_string(),
                )
            })?;
            ancestry.push((cursor, commit));
            cursor = predecessor;
        }
        Ok(ancestry)
    }

    #[cfg(test)]
    pub(crate) async fn load_registration_for_test(
        &self,
        reference: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<crate::sync::store_commit::StoreDeviceRegistration, StoreError> {
        let history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(history
            .history_verifier
            .commit_verifier_ref()
            .load_registration(reference)
            .await?
            .value)
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
        commit: &crate::sync::store_commit::VerifiedStoreBatchCommit,
        reference: &crate::sync::store_commit::CirclePackageRef,
    ) -> Result<Vec<u8>, StoreError> {
        let history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let opened = access
            .open_package(history.storage(), commit, reference, commit.author())
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(opened.object.value)
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn pull_readiness_for_test(
        &self,
        coverage: &crate::sync::store_commit::CommitFrontier,
        frontier: &std::collections::BTreeMap<
            String,
            crate::sync::store_commit::StoreBatchCommitRef,
        >,
        device_state: &crate::sync::store_commit::ResolvedStoreDeviceState,
        exclusion_freezes: &[crate::sync::store_commit::StoreDeviceProposalAck],
        commit_ref: &crate::sync::store_commit::StoreBatchCommitRef,
        commit: &crate::sync::store_commit::StoreBatchCommit,
    ) -> Result<pull::Readiness, pull::StorePullError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        pull::readiness(
            &self.database,
            history.history_verifier_mut().commit_verifier(),
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
        references: impl IntoIterator<Item = crate::sync::store_commit::StoreBatchCommitRef>,
        predecessors: impl IntoIterator<Item = crate::sync::store_commit::StoreBatchCommitRef>,
    ) -> Result<pull::VerifiedMergeMembershipPrefix, pull::StorePullError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        history
            .history_verifier_mut()
            .verify_refs(references)
            .await?;
        pull::verified_merge_membership_prefix(
            &history.history_verifier_mut().history().commits,
            predecessors,
        )
    }

    #[cfg(test)]
    pub(crate) async fn retained_merge_history_frontier_for_test(
        &self,
        references: Vec<crate::sync::store_commit::StoreBatchCommitRef>,
    ) -> Result<
        Vec<crate::sync::store_commit::OpenedRetainedMergeHistorySummary>,
        crate::database::DbError,
    > {
        self.database
            .retained_merge_history_frontier(self.store_root.clone(), references)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn verified_circle_activation_for_test(
        &self,
        circle_id: crate::sync::circle::CircleId,
        control: crate::sync::circle::CircleControlCoord,
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
        target: crate::sync::store_commit::CirclePackageRef,
        activation: crate::sync::store_commit::StoreBatchCommitRef,
    ) -> Result<bool, crate::database::DbError> {
        self.database
            .circle_package_is_retained_for_replay(self.store_root.clone(), target, activation)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_circle_acknowledgement_for_test(
        &self,
        reference: &crate::sync::store_commit::CircleAckRef,
        control: &crate::sync::circle::CircleControlCoord,
    ) -> Result<crate::sync::store_commit::CircleAck, StoreAckError> {
        self.authorize_history()
            .await
            .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?
            .load_circle_acknowledgement(reference, control)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_circle_activations_for_test(
        &self,
        commit_ref: &crate::sync::store_commit::StoreBatchCommitRef,
        commit: &crate::sync::store_commit::StoreBatchCommit,
        author: &crate::sync::store_commit::StoreDeviceRegistration,
        routing_key: Option<&crate::sync::circle::RowRoutingKey>,
    ) -> Result<
        crate::sync::store::circle_controls::VerifiedCircleActivations,
        crate::sync::store::CircleOperationError,
    > {
        let verified = crate::sync::store_commit::VerifiedStoreBatchCommit::parse(
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
        circles::activation::load_circle_activations(
            &self.database,
            history.history_verifier_mut(),
            &verified,
            &self.identity,
            routing_key,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn load_applicable_circle_packages_for_test(
        &self,
        verified: &crate::sync::store_commit::VerifiedStoreBatchCommit,
        activations: &[crate::sync::store::circle_controls::VerifiedCircleReference],
        author: &crate::sync::store_commit::StoreDeviceRegistration,
        local_store_membership: pull::LocalStoreMembership,
    ) -> Result<Vec<pull::LoadedCirclePackage>, pull::PullCircleActivationError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| pull::PullCircleActivationError::Invalid(error.to_string()))?;
        pull::load_applicable_circle_packages(
            &self.database,
            history.history_verifier_mut().commit_verifier(),
            verified,
            activations,
            author,
            local_store_membership,
        )
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn protocol_root_for_test(&self) -> &StoreProtocolRoot {
        &self.protocol_root.value
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn export_activated_device_continuation_for_test(
        &self,
    ) -> Result<crate::sync::restore_code::ActivatedContinuation, crate::database::DbError> {
        self.database
            .export_activated_device_continuation(&self.identity)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn stage_acknowledgement_for_test(
        &self,
        frontier: crate::sync::store_commit::CommitFrontier,
        sync_time: String,
    ) -> Result<crate::sync::store_commit::StoreAck, writer::StoreAckError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| writer::StoreAckError::InvalidOutbound(error.to_string()))?;
        writer.stage_acknowledgement(frontier, sync_time).await
    }

    #[cfg(any(test, feature = "test-utils"))]
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
        acknowledgement: crate::sync::store_commit::StoreAckRef,
        candidate: crate::sync::store::operations::PreparedStoreOperationCommit,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .prepare_acknowledgement_activation(acknowledgement, candidate)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn stage_circle_acknowledgements_for_test(
        &self,
        frontier: &crate::sync::store_commit::CommitFrontier,
        sync_time: &str,
    ) -> Result<(), writer::StoreAckError> {
        self.authorize_writer()
            .await
            .map_err(|error| writer::StoreAckError::InvalidOutbound(error.to_string()))?
            .stage_circle_acknowledgements(frontier, sync_time)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn publish_snapshot_for_test(
        &self,
        snapshot: writer::snapshot::CreatedSnapshot,
        coverage: crate::sync::store_commit::CommitFrontier,
        created_at: String,
    ) -> Result<crate::sync::store_commit::SnapshotMeta, writer::snapshot::SnapshotError> {
        let mut writer = self.authorize_writer().await.map_err(|error| {
            writer::snapshot::SnapshotError::PublicationState(error.to_string())
        })?;
        writer
            .push_store_snapshot(
                snapshot,
                coverage,
                self.database.sqlite().schema_version(),
                created_at,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_founder_registration_for_test(
        &self,
    ) -> Result<
        crate::sync::store_objects::VerifiedObject<
            crate::sync::store_commit::StoreDeviceRegistration,
        >,
        StoreError,
    > {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(history
            .history_verifier_mut()
            .commit_verifier()
            .load_founder_registration()
            .await?)
    }

    #[cfg(test)]
    pub(crate) async fn prepare_merge_history_successor_for_test(
        &self,
        verified_commit: &crate::sync::store_commit::VerifiedStoreBatchCommit,
        recovery_author: Option<&crate::sync::store_commit::StoreDeviceRegistrationRef>,
        evidence: pull::MergeHistorySuccessorEvidence,
    ) -> Result<pull::PreparedMergeHistorySuccessor, StoreError> {
        let mut authorized = self
            .authorize()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let membership = authorized.membership().clone();
        let (_, state_after) = authorized
            .database()
            .store_device_state_for_order(&verified_commit.value().order)
            .await?;
        authorized
            .history()
            .prepare_merge_history_successor(
                verified_commit,
                &membership,
                recovery_author,
                state_after,
                evidence,
            )
            .await
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) async fn prepare_device_join_bootstrap_for_test(
        &self,
        coverage: &crate::sync::store_commit::StoreHistoryCut,
        attempt_activation: &crate::sync::store_commit::StoreBatchCommitRef,
        membership_state: &crate::sync::circle_control::StoreMembershipStateRef,
    ) -> Result<crate::sync::store::owner::pull::DeviceJoinBootstrapPlan, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        history
            .history_verifier_mut()
            .prepare_device_join_bootstrap(coverage, attempt_activation, membership_state)
            .await
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) async fn load_store_package_for_test(
        &self,
        reference: &crate::sync::store_commit::StoreBatchCommitRef,
    ) -> Result<Option<crate::sync::store_objects::VerifiedObject<Vec<u8>>>, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(history
            .history_verifier_mut()
            .commit_verifier()
            .load_store_package(reference)
            .await?)
    }

    #[cfg(test)]
    pub(crate) async fn load_store_ack_for_test(
        &self,
        reference: &crate::sync::store_commit::StoreAckRef,
        registration: &crate::sync::store_commit::StoreDeviceRegistration,
    ) -> Result<crate::sync::store_commit::StoreAck, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(history
            .history_verifier_mut()
            .commit_verifier()
            .load_store_ack(reference, registration)
            .await?
            .value)
    }

    #[cfg(test)]
    pub(crate) async fn load_head_for_test(
        &self,
        reference: &crate::sync::store_commit::StoreDeviceHeadRef,
        registration: &crate::sync::store_commit::StoreDeviceRegistration,
        commit: &crate::sync::store_commit::StoreBatchCommitRef,
    ) -> Result<crate::sync::store_commit::StoreDeviceHead, StoreError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(history
            .history_verifier_mut()
            .commit_verifier()
            .load_head(reference, registration, commit)
            .await?
            .value)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invite_member(
        &self,
        hlc: &crate::sync::hlc::Hlc,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: crate::sync::membership::MemberRole,
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
        custody: &dyn crate::keys::MasterKeyCustody,
        cipher: &dyn crate::sync::cloud_storage::CloudCipherAccess,
        pending_rotation: &crate::sync::cloud_storage::PendingRotation,
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
                custody,
                cipher,
                pending_rotation,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn make_local(
        &self,
        store_dir: &crate::store_dir::StoreDir,
        hlc: &crate::sync::hlc::Hlc,
        routing_encryption: Option<crate::encryption::EncryptionService>,
        observer: Option<&dyn crate::blob::BlobTransitionObserver>,
        root_table: &str,
        root_id: &str,
        destinations: &std::collections::HashMap<String, std::path::PathBuf>,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), crate::blob::transition::MakeLocalError> {
        crate::blob::transition::make_local(
            &self.database,
            self.storage.as_ref(),
            store_dir,
            hlc,
            routing_encryption,
            observer,
            root_table,
            root_id,
            destinations,
            cancel,
        )
        .await
    }
}

impl<'storage> AuthorizedStoreHistory<'storage> {
    fn database(&self) -> &StoreDatabase {
        &self.database
    }

    fn history_verifier_mut(
        &mut self,
    ) -> &mut crate::sync::store::owner::pull::MergeHistoryVerifier<'storage> {
        &mut self.history_verifier
    }

    fn storage(&self) -> &'storage dyn SyncStorage {
        self.history_verifier.storage()
    }

    fn root(&self) -> &StoreRootRef {
        self.history_verifier.root()
    }

    fn verified_root_object(
        &self,
    ) -> &crate::sync::store_objects::VerifiedObject<StoreProtocolRoot> {
        self.history_verifier.verified_root_object()
    }

    async fn verify_snapshots_for_acknowledgement(
        &mut self,
        snapshots: &[crate::database::PublishedStoreSnapshot],
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        self.history_verifier
            .verify_snapshots_for_acknowledgement(snapshots)
            .await
    }

    async fn select_acknowledgement_snapshot(
        &mut self,
        frontier: &crate::sync::store_commit::CommitFrontier,
        device_state: &crate::sync::store_commit::StoreDeviceStateRef,
    ) -> Result<
        Option<crate::sync::store_commit::StoreSnapshotLocator>,
        crate::sync::store::owner::writer::StoreAckError,
    > {
        let registrations = self
            .database
            .activated_store_device_registration_records()
            .await?;
        let storage = self.history_verifier.storage();
        let root = self.history_verifier.root().clone();
        let mut candidates = Vec::new();
        for (registration_ref, registration) in registrations {
            for snapshot in crate::sync::store::snapshot::load_store_snapshot_stream(
                storage,
                &root,
                &registration_ref,
                &registration,
            )
            .await?
            {
                if !frontier.covers(&snapshot.meta.coverage)
                    || snapshot.meta.state.devices.state_hash() != device_state.state_hash()
                    || snapshot.meta.state.devices.recovery() != device_state.recovery()
                {
                    continue;
                }
                candidates.push(snapshot);
            }
        }
        if candidates.is_empty() {
            return Ok(None);
        }
        self.verify_snapshots_for_acknowledgement(&candidates)
            .await
            .map_err(|error| {
                crate::sync::store::snapshot::SnapshotError::UnauthorizedAuthor(error.to_string())
            })?;
        Ok(
            crate::sync::store::snapshot::select_maximal_store_snapshot(candidates).map(
                |snapshot| crate::sync::store_commit::StoreSnapshotLocator {
                    author_registration: snapshot.meta.author_registration,
                    snapshot: snapshot.reference,
                },
            ),
        )
    }
}

impl<'storage> AuthorizedStore<'storage> {
    fn keyring<'operation>(
        &'operation self,
        membership: &'operation crate::sync::membership::MembershipChain,
    ) -> keyring::AuthorizedMembershipKeyring<'operation, 'storage> {
        keyring::AuthorizedMembershipKeyring::bind(
            &self.history.history_verifier,
            self.identity,
            membership,
        )
    }

    pub(super) fn history(&mut self) -> &mut AuthorizedStoreHistory<'storage> {
        &mut self.history
    }

    pub(super) async fn load_circle_acknowledgement_under_retained_controls(
        &self,
        reference: &crate::sync::store_commit::CircleAckRef,
        preferred: &crate::sync::circle::CircleControlCoord,
        retained: &[crate::sync::circle::CircleControlCoord],
    ) -> Result<crate::sync::store_commit::CircleAck, StoreAckError> {
        self.history
            .load_circle_acknowledgement_under_retained_controls(reference, preferred, retained)
            .await
    }

    pub(super) async fn stable_circle_acknowledgements_dominating(
        &self,
        circle_id: crate::sync::circle::CircleId,
        current_control: &crate::sync::circle::CircleControlCoord,
        snapshot_cut: &crate::sync::store_commit::CommitFrontier,
    ) -> Result<Option<Vec<crate::sync::store_commit::CircleAckRef>>, StoreAckError> {
        self.history
            .stable_circle_acknowledgements_dominating(circle_id, current_control, snapshot_cut)
            .await
    }

    pub(crate) fn db(&self) -> &Database {
        self.history.database.sqlite()
    }

    pub(crate) fn database(&self) -> &StoreDatabase {
        &self.history.database
    }

    pub(super) fn membership(&self) -> &crate::sync::membership::MembershipChain {
        &self.membership
    }

    fn resolved_membership(
        &self,
    ) -> Result<
        &crate::sync::membership::MembershipChain,
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
        Vec<crate::sync::membership::MemberInfo>,
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
    ) -> Option<crate::sync::membership::MembershipConflictInfo> {
        match self.membership.status() {
            crate::sync::membership::MembershipStatus::Resolved(_) => None,
            crate::sync::membership::MembershipStatus::Conflict(
                crate::sync::membership::MembershipConflict::ConcurrentMemberAssignments {
                    conflict_hash,
                    member_pubkey,
                    conflicting_grants,
                    grants,
                    ..
                },
            ) => Some(
                crate::sync::membership::MembershipConflictInfo::ConcurrentMemberAssignments {
                    id: conflict_hash.to_string(),
                    member_pubkey: member_pubkey.clone(),
                    choices: conflicting_grants
                        .iter()
                        .map(|(selected_grant, selected_record)| {
                            let selection = crate::sync::membership::MembershipConflictSelection::MemberAssignment {
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
                            crate::sync::membership::MembershipConflictChoice::new(
                                membership_conflict_choice_id(&selection),
                                members,
                                *conflict_hash,
                                selection,
                            )
                        })
                        .collect(),
                },
            ),
            crate::sync::membership::MembershipStatus::Conflict(
                crate::sync::membership::MembershipConflict::RevocationCycle {
                    conflict_hash,
                    maximal_valid_branches,
                    ..
                },
            ) => Some(
                crate::sync::membership::MembershipConflictInfo::RevocationCycle {
                    id: conflict_hash.to_string(),
                    choices: maximal_valid_branches
                        .iter()
                        .map(|branch| {
                            let selection = crate::sync::membership::MembershipConflictSelection::RevocationBranch {
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
                            crate::sync::membership::MembershipConflictChoice::new(
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

    fn require_current_owner(&self, author_pubkey: &str) -> Result<(), String> {
        if self.membership.is_owner_now(author_pubkey) {
            Ok(())
        } else {
            Err(format!("author {author_pubkey} is not a current owner"))
        }
    }

    pub(crate) fn storage(&self) -> &'storage dyn SyncStorage {
        self.history.history_verifier.storage()
    }

    pub(super) fn storage_arc(&self) -> &'storage Arc<dyn SyncStorage> {
        self.storage
    }

    pub(crate) fn store_root(&self) -> &StoreRootRef {
        self.history.history_verifier.root()
    }

    pub(crate) fn protocol_root(&self) -> &StoreProtocolRoot {
        self.history.history_verifier.verified_root()
    }
}

fn membership_conflict_choice_id(
    selection: &crate::sync::membership::MembershipConflictSelection,
) -> String {
    let selection_bytes =
        serde_json::to_vec(selection).expect("membership conflict selections always serialize");
    let mut bytes = b"coven.membership-conflict-choice.v1\0".to_vec();
    bytes.extend(selection_bytes);
    crate::sync::store_commit::ObjectHash::digest(&bytes).to_string()
}

fn member_info(
    current: Vec<(String, crate::sync::membership::MemberRole)>,
    user_pubkey: Option<&[u8]>,
) -> Vec<crate::sync::membership::MemberInfo> {
    let user_pubkey_hex = user_pubkey.map(hex::encode);
    current
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>()
        .into_iter()
        .map(|(pubkey, role)| crate::sync::membership::MemberInfo {
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
    let registration = crate::sync::store_commit::StoreDeviceRegistration::parse_at(
        &durable.registration_bytes,
        root,
        durable.device_id,
    )
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let registration_ref = crate::sync::store_commit::StoreDeviceRegistrationRef::from_registration(
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

impl AuthorizedWriterOperation<'_> {
    pub(crate) async fn drain_uploads(
        &self,
        store_dir: &StoreDir,
        clock: &dyn crate::clock::Clock,
        hlc: &crate::sync::hlc::Hlc,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        observer: Option<&dyn crate::blob::BlobTransitionObserver>,
    ) -> Result<crate::blob::upload::DrainOutcome, crate::database::DbError> {
        StoreDatabase::validate_store_write_routing(
            self.db().gates().as_ref(),
            routing_encryption,
        )?;
        let (registration_ref, registration) = self.database().local_blob_write_authority().await?;
        let authority =
            crate::sync::storage::BlobWriteAuthority::new(&registration_ref, &registration)
                .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
        crate::blob::upload::drain_uploads(
            self.database(),
            self.storage(),
            authority,
            store_dir,
            clock,
            hlc,
            routing_encryption,
            observer,
        )
        .await
    }

    pub(crate) async fn drain_tombstones(
        &self,
        cloud_home: &dyn CloudHome,
        cipher: &dyn CloudCipherAccess,
        pending_rotation: &crate::sync::cloud_storage::PendingRotation,
        store_id: &str,
        clock: &dyn crate::clock::Clock,
    ) -> Result<usize, String> {
        crate::blob::delete::drain_tombstones(
            self.db(),
            cloud_home,
            cipher,
            pending_rotation,
            store_id,
            self.identity(),
            clock,
        )
        .await
    }

    pub(crate) async fn gc_tombstones(
        &self,
        cloud_home: &dyn CloudHome,
        cipher: &dyn CloudCipherAccess,
        store_id: &str,
        clock: &dyn crate::clock::Clock,
        grace: chrono::Duration,
    ) -> Result<usize, String> {
        let activated_uploaders = self
            .database()
            .activated_store_device_registration_records()
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .collect();
        crate::blob::delete::gc_tombstones(
            self.db(),
            cloud_home,
            self.storage(),
            cipher,
            store_id,
            &crate::keys::public_key_hex(self.identity()),
            &activated_uploaders,
            &self.membership,
            clock,
            grace,
        )
        .await
    }
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
            StoreDatabase::new(&db),
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
