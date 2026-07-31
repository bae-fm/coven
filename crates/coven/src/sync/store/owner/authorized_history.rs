use super::*;
use std::collections::{BTreeMap, BTreeSet};

use super::authorized_store::LocalStoreDevice;
use super::history::{abandonment, OwnerPromotionHistory, ReclaimHistory, RestoreHistory};
use super::pull;
use super::verified_history::registration::RegistrationLoadError;
use super::verified_history::*;

pub(crate) struct AuthorizedStoreHistory<'storage> {
    database: StoreDatabase,
    history_verifier: MergeHistoryVerifier<'storage>,
    blob_source: crate::sync::store::blob::RemoteBlobSource<'storage>,
    keyrings: super::keyring::StoreKeyrings<'storage>,
}

impl<'storage> AuthorizedStoreHistory<'storage> {
    fn new(
        database: StoreDatabase,
        history_verifier: MergeHistoryVerifier<'storage>,
        blob_source: crate::sync::store::blob::RemoteBlobSource<'storage>,
        keyrings: super::keyring::StoreKeyrings<'storage>,
    ) -> Self {
        Self {
            database,
            history_verifier,
            blob_source,
            keyrings,
        }
    }

    pub(super) async fn finish_initialization(
        mut self,
        storage: &Arc<dyn SyncStorage>,
        identity: &UserKeypair,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let database = self.database.clone();
        let mut device_id = database
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        let identity_is_founder = self.verified_root_object().value.descriptor.founder_pubkey
            == crate::keys::public_key_hex(identity);
        if device_id.is_none() && !identity_is_founder {
            return Err(StoreInitializationError::ProtocolRoot(
                "opening a Store for a non-founder requires an installed local device".to_string(),
            ));
        }
        let founder_pubkey = self
            .verified_root_object()
            .value
            .descriptor
            .founder_pubkey
            .clone();
        self.load_and_install_owner_membership(&founder_pubkey)
            .await
            .map_err(|error| StoreInitializationError::MembershipAnchor(error.to_string()))?;

        if device_id.is_none() && identity_is_founder {
            self.install_existing_founder_device(identity)
                .await
                .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
            device_id = database
                .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                .await
                .map_err(|error| StoreInitializationError::ProtocolRoot(error.to_string()))?;
        }
        let device_id = device_id.ok_or_else(|| {
            StoreInitializationError::ProtocolRoot(
                "initialized Store has no local device registration id".to_string(),
            )
        })?;
        let store_root = self.root().clone();
        let protocol_root = self.verified_root_object().clone();
        let store = Store::new(
            database,
            Arc::clone(storage),
            identity.clone(),
            Some(device_id.clone()),
            store_root,
            protocol_root,
        )
        .map_err(StoreInitializationError::ProtocolRoot)?;
        Ok(InitializedStore { store, device_id })
    }

    async fn install_existing_founder_device(
        &self,
        signer: &UserKeypair,
    ) -> Result<(), super::registration::StoreRegistrationError> {
        use crate::protocol::store_commit::{
            ack_slot_prefix, DeviceStreamAnchor, StoreAck, StoreAckRef,
            StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef,
        };
        use crate::storage::ProtocolObjectDomain;

        let storage = self.history_verifier.storage();
        let root = self.history_verifier.root();
        let founder = self.history_verifier.load_founder_registration().await?;
        if founder.value.author_pubkey != crate::keys::public_key_hex(signer) {
            return Err(super::registration::StoreRegistrationError::Invalid(
                "Store founder registration belongs to another identity".to_string(),
            ));
        }
        if founder.value.provider
            != storage
                .provider_binding()
                .await
                .map_err(crate::storage::StoreObjectError::from)?
                .device
        {
            return Err(super::registration::StoreRegistrationError::Invalid(
                "Store founder registration belongs to another provider principal".to_string(),
            ));
        }
        founder.value.device_signer(signer).map_err(|error| {
            super::registration::StoreRegistrationError::Invalid(error.to_string())
        })?;

        let registration_context = crate::storage::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let registration_prefix =
            crate::protocol::store_commit::founder_registration_semantic_prefix(
                match founder.value.origin {
                    StoreDeviceRegistrationOrigin::Founder { creation_id } => creation_id,
                    _ => {
                        return Err(super::registration::StoreRegistrationError::Invalid(
                            "Store founder registration has a non-founder origin".to_string(),
                        ))
                    }
                },
            );
        let (registration_bytes, registration_prepared) = storage
            .read_prepared_protocol_slot(
                &registration_context,
                founder.object.slot(),
                &registration_prefix,
            )
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
        if registration_bytes != founder.bytes
            || registration_prepared.reference() != &founder.object
        {
            return Err(super::registration::StoreRegistrationError::Invalid(
                "prepared founder registration differs from its verified exact object".to_string(),
            ));
        }
        let registration_ref =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        let DeviceStreamAnchor::StoreAcknowledgements { first_slot } =
            &founder.value.acknowledgements
        else {
            return Err(super::registration::StoreRegistrationError::Invalid(
                "Store founder registration has no acknowledgement anchor".to_string(),
            ));
        };
        let ack_context = crate::storage::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let ack_prefix = ack_slot_prefix(&founder.value.device_id.to_string(), 1);
        let (ack_bytes, ack_prepared) = storage
            .read_prepared_protocol_slot(&ack_context, first_slot, &ack_prefix)
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
        let unverified_ack: StoreAck = serde_json::from_slice(&ack_bytes).map_err(|error| {
            super::registration::StoreRegistrationError::Invalid(error.to_string())
        })?;
        let ack_ref = StoreAckRef {
            registration: registration_ref.clone(),
            sequence: unverified_ack.sequence,
            ack_hash: unverified_ack.ack_hash(),
            object: ack_prepared.reference().clone(),
        };
        let ack =
            StoreAck::parse_at(&ack_bytes, root, &ack_ref, &founder.value).map_err(|error| {
                super::registration::StoreRegistrationError::Invalid(error.to_string())
            })?;
        if ack.registration != registration_ref {
            return Err(super::registration::StoreRegistrationError::Invalid(
                "Store founder acknowledgement names another registration".to_string(),
            ));
        }
        self.database
            .install_existing_local_founder_device(
                crate::database::ExactProtocolObject {
                    value: founder.value,
                    bytes: registration_bytes,
                    object: registration_prepared.reference().clone(),
                    prepared: registration_prepared,
                },
                ack_ref,
                crate::database::ExactProtocolObject {
                    value: ack,
                    bytes: ack_bytes,
                    object: ack_prepared.reference().clone(),
                    prepared: ack_prepared,
                },
            )
            .await
            .map_err(|error| {
                super::registration::StoreRegistrationError::Database(error.to_string())
            })
    }

    pub(super) async fn authorize_store(
        mut self,
        storage: &'storage Arc<dyn SyncStorage>,
        identity: &'storage UserKeypair,
        device_id: Option<&str>,
    ) -> Result<AuthorizedStore<'storage>, SyncCycleFailure> {
        let owner = self
            .database
            .validated_store_owner(self.root())
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("validate Store owner authority", error)
            })?;
        let membership = self
            .load_current_membership(&owner)
            .await
            .map_err(|error| SyncCycleFailure::operation("load membership chain", error))?;
        let local_device = match device_id {
            Some(device_id) => Some(
                LocalStoreDevice::load(&self.database, self.root(), device_id)
                    .await
                    .map_err(|error| {
                        SyncCycleFailure::operation("load local Store device authority", error)
                    })?,
            ),
            None => None,
        };
        Ok(AuthorizedStore::new(
            self,
            storage,
            identity,
            local_device,
            membership,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn authorize_writer(
        self,
        storage: &'storage Arc<dyn SyncStorage>,
        membership: crate::protocol::membership::MembershipChain,
        identity: &'storage UserKeypair,
        registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: crate::protocol::store_commit::StoreDeviceRegistration,
        device_signer: UserKeypair,
    ) -> super::writer::AuthorizedWriterOperation<'storage> {
        let database = self.database.clone();
        super::writer::authorize(
            database,
            self,
            storage,
            membership,
            identity,
            registration_ref,
            registration,
            device_signer,
        )
    }

    pub(crate) async fn pull(
        &mut self,
        store_dir: &crate::store_dir::StoreDir,
        membership: &crate::protocol::membership::MembershipChain,
        identity: Option<&UserKeypair>,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<pull::StorePullExecution, pull::StorePullError> {
        pull::execute(self, store_dir, membership, identity, routing_encryption).await
    }

    pub(super) async fn bind_pull_package_materializer(
        &self,
        store_dir: &crate::store_dir::StoreDir,
    ) -> Result<
        super::pull_package_materializer::PullPackageMaterializer<'storage>,
        crate::database::DbError,
    > {
        let schema = std::sync::Arc::new(self.database.table_schema_for_apply().await?);
        let blob_cache =
            crate::sync::store::blob::StoreBlobCache::new(self.database.clone(), store_dir.clone());
        Ok(
            super::pull_package_materializer::PullPackageMaterializer::new(
                self.database.clone(),
                self.blob_source.clone(),
                blob_cache,
                store_dir.clone(),
                schema,
            ),
        )
    }

    pub(super) fn circles(&mut self) -> super::circles::VerifiedCircleHistory<'_, 'storage> {
        super::circles::VerifiedCircleHistory::new(self)
    }

    pub(super) fn circle_operation_discarder(
        &mut self,
    ) -> super::circles::CircleOperationDiscarder<'_, 'storage> {
        let database = self.database.clone();
        let history = self.circles();
        super::circles::CircleOperationDiscarder::new(database, history)
    }

    pub(super) fn circle_activations(
        &mut self,
    ) -> super::circles::activation::CircleActivationVerifier<'_, 'storage> {
        super::circles::activation::CircleActivationVerifier::new(
            &self.database,
            &mut self.history_verifier,
        )
    }

    pub(super) fn circle_packages(
        &mut self,
    ) -> super::circles::packages::CirclePackageReader<'_, 'storage> {
        super::circles::packages::CirclePackageReader::new(
            &self.database,
            &mut self.history_verifier,
        )
    }

    pub(super) fn circle_acknowledgements(
        &mut self,
    ) -> super::circles::acknowledgements::CircleAcknowledgementReader<'_, 'storage> {
        super::circles::acknowledgements::CircleAcknowledgementReader::new(
            &self.database,
            &self.history_verifier,
        )
    }

    #[cfg(test)]
    pub(super) fn circle_snapshots(
        &mut self,
    ) -> super::circles::snapshots::CircleSnapshotReader<'_, 'storage> {
        super::circles::snapshots::CircleSnapshotReader::new(
            &self.database,
            &mut self.history_verifier,
        )
    }

    pub(super) fn device_join(
        &mut self,
    ) -> super::device_join::history::DeviceJoinHistory<'_, 'storage> {
        super::device_join::history::DeviceJoinHistory::new(
            self.database.clone(),
            &mut self.history_verifier,
        )
    }

    pub(super) fn device_exclusion(
        &mut self,
    ) -> super::device_exclusion::DeviceExclusionHistory<'_, 'storage> {
        super::device_exclusion::DeviceExclusionHistory::new(&mut self.history_verifier)
    }

    pub(super) fn reclaim(&mut self) -> ReclaimHistory<'_, 'storage> {
        ReclaimHistory::new(self.database.clone(), &mut self.history_verifier)
    }

    pub(super) fn restore_history(&self) -> RestoreHistory<'_, 'storage> {
        RestoreHistory::new(&self.history_verifier)
    }

    pub(super) fn owner_promotion(&mut self) -> OwnerPromotionHistory<'_, 'storage> {
        OwnerPromotionHistory::new(&mut self.history_verifier)
    }

    pub(super) fn bind_restore(
        self,
        storage: &'storage dyn SyncStorage,
        membership: crate::protocol::membership::MembershipChain,
        identity: UserKeypair,
        target_path: std::path::PathBuf,
    ) -> super::RestoringStore<'storage> {
        let database = self.database.clone();
        let root = self.root().clone();
        let protocol = self.verified_root_object().value.clone();
        super::RestoringStore::from_parts(
            self,
            database,
            storage,
            root,
            protocol,
            membership,
            identity,
            target_path,
        )
    }

    pub(super) fn from_pending_device_join(
        _authority: super::device_join::PendingDeviceJoinHistoryConstruction,
        database: StoreDatabase,
        history_verifier: MergeHistoryVerifier<'storage>,
        blob_source: crate::sync::store::blob::RemoteBlobSource<'storage>,
        keyrings: super::keyring::StoreKeyrings<'storage>,
    ) -> Self {
        Self::new(database, history_verifier, blob_source, keyrings)
    }

    pub(super) fn from_snapshot(
        _authority: super::writer::SnapshotHistoryConstruction,
        database: StoreDatabase,
        history_verifier: MergeHistoryVerifier<'storage>,
        blob_source: crate::sync::store::blob::RemoteBlobSource<'storage>,
        keyrings: super::keyring::StoreKeyrings<'storage>,
    ) -> Self {
        Self::new(database, history_verifier, blob_source, keyrings)
    }
}

#[derive(Clone, Copy)]
pub(super) struct HistoryConstructionAuthority(());

impl HistoryConstructionAuthority {
    pub(super) fn pending_device_join(
        _authority: super::device_join::PendingDeviceJoinHistoryConstruction,
    ) -> Self {
        Self(())
    }

    pub(super) fn snapshot(_authority: super::writer::SnapshotHistoryConstruction) -> Self {
        Self(())
    }
}

pub(super) struct FounderStoreInitialization<'operation, 'storage> {
    database: &'operation StoreDatabase,
    storage: &'storage dyn SyncStorage,
    founder_timestamp: &'operation str,
    identity: &'operation crate::keys::UserKeypair,
    graph: &'operation crate::database::DurableFounderGraph,
}

impl<'operation, 'storage> FounderStoreInitialization<'operation, 'storage> {
    pub(super) fn new(
        database: &'operation StoreDatabase,
        storage: &'storage dyn SyncStorage,
        founder_timestamp: &'operation str,
        identity: &'operation crate::keys::UserKeypair,
        graph: &'operation crate::database::DurableFounderGraph,
    ) -> Self {
        Self {
            database,
            storage,
            founder_timestamp,
            identity,
            graph,
        }
    }

    pub(super) async fn publish(
        self,
    ) -> Result<AuthorizedStoreHistory<'storage>, protocol_root::StoreProtocolRootError> {
        let root = StoreRootRef {
            store_root_id: self.graph.root.value.descriptor.store_root_id(),
            store_root_hash: self.graph.root.value.object_hash(),
            object: self.graph.root.object.clone(),
        };
        if self.graph.initial_ack.value.last_sync != self.founder_timestamp {
            return Err(protocol_root::StoreProtocolRootError::Database(
                "durable Store founder timestamp differs from this creation request".to_string(),
            ));
        }
        let protocol_root = StoreProtocolRoot::parse_expected(
            &self.graph.root.bytes,
            &root,
            self.database.sync_routing_hash(),
        )
        .map_err(|error| protocol_root::StoreProtocolRootError::Database(error.to_string()))?;
        if protocol_root.descriptor.founder_pubkey != crate::keys::public_key_hex(self.identity) {
            return Err(protocol_root::StoreProtocolRootError::Database(
                "durable Store founder differs from the creation signer".to_string(),
            ));
        }
        if protocol_root.descriptor.schema_version > self.database.schema_version() {
            return Err(protocol_root::StoreProtocolRootError::SchemaTooNew {
                root_schema: protocol_root.descriptor.schema_version,
                local: self.database.schema_version(),
            });
        }
        let registration_ref =
            crate::protocol::store_commit::StoreDeviceRegistrationRef::from_registration(
                &self.graph.registration.value,
                self.graph.registration.object.clone(),
            );
        self.storage
            .create_protocol_object(&self.graph.root.prepared)
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
        let opened_root = protocol_root::load_exact_store_protocol_root(
            self.storage,
            &root,
            self.database.sync_routing_hash(),
        )
        .await?;
        if opened_root.value != protocol_root {
            return Err(protocol_root::StoreProtocolRootError::Missing(
                root.store_root_hash,
            ));
        }
        let authority = HistoryConstructionAuthority(());
        let commit_verifier =
            StoreCommitVerifier::from_verified_root(authority, self.storage, &root, opened_root)
                .map_err(|error| {
                    protocol_root::StoreProtocolRootError::Database(error.to_string())
                })?;
        self.storage
            .create_protocol_object(&self.graph.registration.prepared)
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
        let registration = commit_verifier
            .load_registration(&registration_ref)
            .await?
            .value;
        if registration != self.graph.registration.value {
            return Err(protocol_root::StoreProtocolRootError::Database(
                "founder registration readback differs from durable bytes".to_string(),
            ));
        }
        self.storage
            .create_protocol_object(&self.graph.initial_ack.prepared)
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
        let initial_ack = commit_verifier
            .load_store_ack(&self.graph.initial_ack_ref, &registration)
            .await?
            .value;
        if initial_ack != self.graph.initial_ack.value {
            return Err(protocol_root::StoreProtocolRootError::Database(
                "founder initial acknowledgement readback differs from durable bytes".to_string(),
            ));
        }
        if !matches!(
            &self.graph.registration_state,
            crate::database::LocalDeviceRegistrationState::Activated { .. }
        ) {
            self.database
                .mark_local_store_device_registration_created(
                    self.graph.registration.clone(),
                    self.graph.initial_ack_ref.clone(),
                    self.graph.initial_ack.clone(),
                )
                .await
                .map_err(|error| {
                    protocol_root::StoreProtocolRootError::Database(error.to_string())
                })?;
        }
        let membership = &self.graph.membership;
        self.storage
            .create_protocol_object(&membership.entry.prepared)
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
        let loaded_entry = crate::storage::load_membership_entry_ref(
            self.storage,
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
        self.storage
            .create_protocol_object(&membership.head.prepared)
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
        let loaded_head = crate::storage::load_membership_head_ref(
            self.storage,
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
        self.database
            .complete_store_founder_graph(
                root.clone(),
                registration_ref,
                self.graph.initial_ack_ref.clone(),
                crate::database::FounderMembershipRefs {
                    entry: membership.entry_ref.clone(),
                    head: membership.head_ref.clone(),
                },
            )
            .await
            .map_err(|error| protocol_root::StoreProtocolRootError::Database(error.to_string()))?;
        let history_verifier =
            MergeHistoryVerifier::from_commit_verifier(authority, commit_verifier)
                .await
                .map_err(|error| {
                    protocol_root::StoreProtocolRootError::Database(error.to_string())
                })?;
        let blob_source = crate::sync::store::blob::RemoteBlobSource::authorized(
            self.database.clone(),
            self.storage,
            root.clone(),
        );
        let keyrings = super::keyring::StoreKeyrings::new(self.storage, root);
        Ok(AuthorizedStoreHistory::new(
            self.database.clone(),
            history_verifier,
            blob_source,
            keyrings,
        ))
    }
}

use crate::protocol::circle_control::StoreMembershipStateRef;
use crate::protocol::membership::{
    AuthorStreamId, MembershipChain, MembershipHeadRef, MembershipStatus,
};
use crate::protocol::store_commit::{
    CommitFrontier, OpenedRetainedMergeHistorySummary, ResolvedStoreDeviceState,
    StoreBatchCommitRef, StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreRootRef,
};

fn invitation_history_error(
    error: pull::StorePullError,
) -> crate::sync::store::membership::AnchoredChainError {
    match error {
        pull::StorePullError::Object(error) => {
            crate::sync::store::membership::AnchoredChainError::from_store_object(error)
        }
        pull::StorePullError::Storage(source) if source.is_transport() => {
            crate::sync::store::membership::AnchoredChainError::StorageUnavailable {
                operation: "authenticating membership Store history".to_string(),
                source,
            }
        }
        pull::StorePullError::Storage(error) => {
            crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
        }
        error => crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string()),
    }
}

pub(crate) struct InvitationHistory<'storage> {
    verifier: MergeHistoryVerifier<'storage>,
    identity: &'storage crate::keys::UserKeypair,
    keyrings: super::keyring::StoreKeyrings<'storage>,
}

impl<'storage> InvitationHistory<'storage> {
    fn new(
        verifier: MergeHistoryVerifier<'storage>,
        identity: &'storage crate::keys::UserKeypair,
        keyrings: super::keyring::StoreKeyrings<'storage>,
    ) -> Self {
        Self {
            verifier,
            identity,
            keyrings,
        }
    }

    pub(crate) async fn load_membership(
        &mut self,
        floor: &[MembershipHeadRef],
        founder: &str,
    ) -> Result<MembershipChain, crate::sync::store::membership::InviteError> {
        self.verifier
            .load_exact_anchored_membership(floor, Some(founder))
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(format!(
                    "membership chain: {error}"
                ))
            })
    }

    pub(crate) async fn open_keyring_containing(
        &self,
        membership: &MembershipChain,
        required: &crate::protocol::wrapped_store_key::WrappedStoreKeyRef,
    ) -> Result<crate::encryption::EncryptionService, crate::sync::store::membership::InviteError>
    {
        self.keyrings
            .open_containing(self.identity, membership, required)
            .await
    }
}

pub(crate) async fn open_invitation_history<'storage>(
    storage: &'storage dyn crate::storage::SyncStorage,
    identity: &'storage crate::keys::UserKeypair,
    root: &StoreRootRef,
) -> Result<InvitationHistory<'storage>, crate::sync::store::membership::InviteError> {
    let verifier = super::verified_history::open_merge_history_verifier(
        HistoryConstructionAuthority(()),
        storage,
        root,
    )
    .await
    .map_err(invitation_history_error)
    .map_err(|error| {
        crate::sync::store::membership::InviteError::Crypto(format!("membership chain: {error}"))
    })?;
    let keyrings = super::keyring::StoreKeyrings::new(storage, root.clone());
    Ok(InvitationHistory::new(verifier, identity, keyrings))
}

pub(super) async fn authorized_history_from_verified_root<'storage>(
    database: StoreDatabase,
    storage: &'storage dyn SyncStorage,
    root: &StoreRootRef,
    verified_root: crate::storage::VerifiedObject<StoreProtocolRoot>,
) -> Result<AuthorizedStoreHistory<'storage>, pull::StorePullError> {
    let authority = HistoryConstructionAuthority(());
    let commit_verifier =
        StoreCommitVerifier::from_verified_root(authority, storage, root, verified_root)
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
    let history_verifier =
        MergeHistoryVerifier::from_commit_verifier(authority, commit_verifier).await?;
    let blob_source = crate::sync::store::blob::RemoteBlobSource::authorized(
        database.clone(),
        storage,
        root.clone(),
    );
    let keyrings = super::keyring::StoreKeyrings::new(storage, root.clone());
    Ok(AuthorizedStoreHistory::new(
        database,
        history_verifier,
        blob_source,
        keyrings,
    ))
}

pub(super) struct MergeConflictResolutionAuthorization {
    pub(super) membership: MembershipChain,
    pub(super) device_state_ref: StoreDeviceStateRef,
    pub(super) device_state: ResolvedStoreDeviceState,
}

enum TerminalNonactivationCandidate {
    StoreWrite {
        write_id: crate::WriteId,
        verification: crate::database::TerminalCandidateCleanupVerification,
    },
    CircleOperation {
        operation_id: crate::protocol::circle::CircleOperationId,
        verification: crate::database::TerminalCandidateCleanupVerification,
    },
    MergeRetraction {
        reference: crate::protocol::store_commit::StoreBatchCommitRef,
        verification: crate::database::TerminalCandidateCleanupVerification,
    },
}

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(super) async fn stage_verified_blob_plaintext(
        &self,
        authority: &crate::blob::RowBlobAuthority,
        stored: &crate::blob::locator::StoredBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::storage::StagedBlobFile, crate::sync::BlobCacheError> {
        self.blob_source
            .stage_verified_plaintext(authority, stored, destination)
            .await
    }

    pub(super) async fn verify_blob_plaintext(
        &self,
        cache: &crate::sync::store::blob::StoreBlobCache,
        authority: &crate::blob::RowBlobAuthority,
        stored: &crate::blob::locator::StoredBlobRef,
        retain: bool,
    ) -> Result<(), crate::sync::store::blob::BlobDownloadFailureCause> {
        self.blob_source
            .verify_plaintext(cache, authority, stored, retain)
            .await
    }

    #[cfg(test)]
    pub(super) async fn blob_protection_for_test(
        &self,
        authority: &crate::blob::RowBlobAuthority,
        stored: &crate::blob::locator::StoredBlobRef,
    ) -> Result<crate::storage::BlobSpoolProtection, crate::sync::BlobCacheError> {
        self.blob_source
            .protection_for_test(authority, stored)
            .await
    }

    pub(super) fn root(&self) -> &StoreRootRef {
        self.history_verifier.root()
    }

    pub(super) fn verified_root_object(
        &self,
    ) -> &crate::storage::VerifiedObject<StoreProtocolRoot> {
        self.history_verifier.verified_root_object()
    }

    pub(super) async fn open_keyring(
        &self,
        identity: &UserKeypair,
        membership: &MembershipChain,
    ) -> Result<crate::encryption::EncryptionService, crate::sync::store::membership::InviteError>
    {
        self.keyrings.open(identity, membership).await
    }

    pub(super) async fn open_keyring_or(
        &self,
        identity: &UserKeypair,
        membership: &MembershipChain,
        initial: &crate::encryption::EncryptionService,
    ) -> Result<crate::encryption::EncryptionService, crate::sync::store::membership::InviteError>
    {
        self.keyrings.open_or(identity, membership, initial).await
    }

    pub(super) async fn prepare_wrapped_key(
        &self,
        recipient: &str,
        value: crate::protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<
        crate::protocol::wrapped_store_key::PreparedWrappedStoreKey,
        crate::storage::StorageError,
    > {
        self.keyrings.prepare(recipient, value).await
    }

    pub(super) async fn authenticate_commit_bytes(
        &mut self,
        reference: &StoreBatchCommitRef,
        bytes: &[u8],
    ) -> Result<
        crate::protocol::store_commit::VerifiedStoreBatchCommit,
        crate::storage::StoreObjectError,
    > {
        self.history_verifier
            .authenticate_bytes(reference, bytes)
            .await
    }

    pub(super) async fn authenticate_blocked_candidate(
        &mut self,
        candidate: &crate::database::BlockedMergeCandidate,
    ) -> Result<
        crate::protocol::store_commit::VerifiedStoreBatchCommit,
        crate::sync::store::StoreError,
    > {
        self.history_verifier
            .authenticate_blocked_candidate(candidate)
            .await
    }

    pub(super) async fn load_commit(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<crate::protocol::store_commit::VerifiedStoreBatchCommit, pull::StorePullError> {
        self.history_verifier.load_ref(reference).await
    }

    pub(super) async fn load_registration(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<
        crate::storage::VerifiedObject<crate::protocol::store_commit::StoreDeviceRegistration>,
        crate::storage::StoreObjectError,
    > {
        self.history_verifier.load_registration(reference).await
    }

    pub(super) async fn verify_membership_control(
        &mut self,
        verified_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<crate::sync::store::circle_controls::activation::VerifiedCircleActivations, String>
    {
        let root = self.history_verifier.root().clone();
        if verified_commit.store_root_hash() != root.store_root_hash {
            return Err(
                "authenticated Merge membership control belongs to another Store root".into(),
            );
        }
        let commit_ref = verified_commit.reference();
        let commit = verified_commit.value();
        self.history_verifier
            .verify_refs(pull::commit_predecessor_references(commit))
            .await
            .map_err(|error| error.to_string())?;
        let predecessor_state = self
            .history_verifier
            .verified_predecessor_state(commit)
            .map_err(|error| error.to_string())?;
        let verified_membership_activations = self
            .history_verifier
            .verified_membership_prefix(pull::commit_predecessor_references(commit))
            .map_err(|error| error.to_string())?;
        let pending_resolution = self
            .history_verifier
            .verify_resolution_activation_acceptance(commit)
            .await
            .map_err(|error| error.to_string())?;
        let predecessor_membership = load_merge_predecessor_membership_with_retained_history(
            &self.history_verifier,
            &commit.membership_state,
            &verified_membership_activations,
            pending_resolution.as_ref(),
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => error.to_string(),
            RegistrationLoadError::Invalid(error) => error,
        })?;
        verify_merge_membership_state_ref(
            &commit.membership_state,
            &predecessor_membership,
            &predecessor_state,
        )
        .map_err(|error| error.to_string())?;
        self.history_verifier
            .verify_membership_control_with_retained_history(
                commit_ref,
                commit,
                &predecessor_membership,
                &predecessor_state,
                pending_resolution.as_ref(),
            )
            .await
            .map(|(activations, _)| activations)
    }

    pub(super) async fn load_local_device_operations(
        &mut self,
        verified_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &MembershipChain,
        state_ref: &StoreDeviceStateRef,
        state: ResolvedStoreDeviceState,
    ) -> Result<crate::protocol::store_commit::VerifiedStoreDeviceOperations, pull::StorePullError>
    {
        self.history_verifier
            .load_local_device_operations(
                &self.database,
                verified_commit,
                membership,
                state_ref,
                state,
            )
            .await
    }

    pub(super) async fn retain_acknowledgement(
        &self,
        activating_commit: &StoreBatchCommitRef,
        activating_commit_value: &crate::protocol::store_commit::StoreBatchCommit,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        reference: crate::protocol::store_commit::StoreAckRef,
        value: crate::protocol::store_commit::StoreAck,
    ) -> Result<crate::protocol::store_commit::RetainedVerifiedActivatedAck, pull::StorePullError>
    {
        self.history_verifier
            .retain_acknowledgement(
                activating_commit,
                activating_commit_value,
                registration,
                reference,
                value,
            )
            .await
    }

    pub(super) async fn derive_local_post_device_state(
        &self,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
        predecessor_state: ResolvedStoreDeviceState,
        registrations: &[(
            crate::protocol::store_commit::StoreDeviceRegistration,
            crate::protocol::store_commit::StoreDeviceRegistrationActivation,
        )],
        device_operations: crate::protocol::store_commit::VerifiedStoreDeviceOperations,
    ) -> Result<ResolvedStoreDeviceState, pull::StorePullError> {
        self.history_verifier
            .derive_local_post_device_state(
                commit,
                predecessor_state,
                registrations,
                device_operations,
            )
            .await
    }

    #[cfg(test)]
    pub(super) async fn load_store_snapshot(
        &self,
        registration_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        (
            crate::protocol::store_commit::StoreSnapshotRef,
            crate::protocol::store_commit::SnapshotMeta,
        ),
        crate::storage::StoreObjectError,
    > {
        self.history_verifier
            .load_store_snapshot(registration_ref, registration, reference)
            .await
    }

    pub(super) async fn verify_snapshots_for_acknowledgement(
        &mut self,
        snapshots: &[crate::database::PublishedStoreSnapshot],
    ) -> Result<(), pull::StorePullError> {
        self.history_verifier
            .verify_snapshots_for_acknowledgement(snapshots)
            .await
    }

    pub(super) async fn select_acknowledgement_snapshot(
        &mut self,
        frontier: &CommitFrontier,
        device_state: &StoreDeviceStateRef,
    ) -> Result<Option<crate::protocol::store_commit::StoreSnapshotLocator>, writer::StoreAckError>
    {
        let registrations = self
            .database
            .activated_store_device_registration_records()
            .await?;
        let storage = self.history_verifier.storage();
        let root = self.history_verifier.root().clone();
        let mut candidates = Vec::new();
        for (registration_ref, registration) in registrations {
            for snapshot in snapshot::load_store_snapshot_stream(
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
            .map_err(|error| snapshot::SnapshotError::UnauthorizedAuthor(error.to_string()))?;
        Ok(
            snapshot::select_maximal_store_snapshot(candidates).map(|snapshot| {
                crate::protocol::store_commit::StoreSnapshotLocator {
                    author_registration: snapshot.meta.author_registration,
                    snapshot: snapshot.reference,
                }
            }),
        )
    }
}

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(super) async fn load_current_membership(
        &mut self,
        owner_pubkey: &str,
    ) -> Result<MembershipChain, crate::sync::store::membership::MembershipOpsError> {
        let _membership_load = self.database.membership_load_permit().await;
        let cursors = self
            .database
            .membership_head_cursors()
            .await
            .map_err(|error| {
                crate::sync::store::membership::MembershipOpsError::Database(error.to_string())
            })?;
        let chain = Box::pin(
            self.history_verifier
                .load_exact_anchored_membership(&cursors.head_refs, Some(owner_pubkey)),
        )
        .await?;
        self.database
            .persist_membership_head_cursors(chain.head_refs().to_vec())
            .await
            .map_err(|error| {
                crate::sync::store::membership::MembershipOpsError::Database(error.to_string())
            })?;
        Ok(chain)
    }

    pub(super) async fn load_and_install_owner_membership(
        &mut self,
        owner_pubkey: &str,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        let _membership_load = self.database.membership_load_permit().await;
        let cursors = self
            .database
            .membership_head_cursors()
            .await
            .map_err(|error| {
                crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
            })?;
        let chain = Box::pin(
            self.history_verifier
                .load_exact_anchored_membership(&cursors.head_refs, Some(owner_pubkey)),
        )
        .await?;
        let root = self.history_verifier.root().clone();
        let root_bytes = self.history_verifier.verified_root_object().bytes.clone();
        let protocol_root = self.history_verifier.verified_root().clone();
        let founder = chain.founder_coord().ok_or_else(|| {
            crate::sync::store::membership::AnchoredChainError::LoadFailed(
                "owner-anchored membership chain is empty".to_string(),
            )
        })?;
        let founder_head_ref = chain
            .head_ref_for_stream(
                &founder.author_pubkey,
                &founder.author_owner_grant,
                founder.stream_id,
            )
            .cloned()
            .ok_or_else(|| {
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "owner-anchored membership chain has no exact founder head".to_string(),
                )
            })?;
        let founder_head = self
            .history_verifier
            .load_exact_membership_head(&founder_head_ref)
            .await?;
        let founder_registration_ref = founder_head.body.author_registration.clone();
        let founder_registration = self
            .history_verifier
            .load_registration(&founder_registration_ref)
            .await
            .map_err(crate::sync::store::membership::AnchoredChainError::from_store_object)?;
        let founder_registration_bytes = founder_registration.bytes;
        let founder_registration = founder_registration.value;
        if founder_registration.author_pubkey != owner_pubkey
            || !matches!(
                founder_registration.origin,
                crate::protocol::store_commit::StoreDeviceRegistrationOrigin::Founder { .. }
            )
        {
            return Err(
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "founder head registration is not activated by the Store root".to_string(),
                ),
            );
        }
        if protocol_root.descriptor.founder_pubkey != owner_pubkey {
            return Err(
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "owner anchor differs from the Store root founder".to_string(),
                ),
            );
        }
        let founder_genesis = crate::protocol::store_commit::ResolvedStoreDeviceState::founder(
            &root,
            founder_registration_ref.clone(),
            &protocol_root.descriptor.founder_pubkey,
            protocol_root.descriptor.founder_grant.clone(),
            &protocol_root.descriptor.founder_recovery,
        )
        .map_err(|error| {
            crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
        })?;
        self.database
            .install_store_owner_anchor(
                root,
                root_bytes,
                founder_registration_ref,
                founder_registration,
                founder_registration_bytes,
                founder_genesis,
                owner_pubkey.to_string(),
                crate::database::InitialStoreMembershipAuthority {
                    head_refs: chain.head_refs().to_vec(),
                },
            )
            .await
            .map_err(|error| {
                crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
            })?;
        Ok(chain)
    }

    pub(super) async fn project_membership_to_verified_prefix(
        &self,
        candidate_heads: &[MembershipHeadRef],
        prefix: &VerifiedMergeMembershipPrefix,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        self.history_verifier
            .project_membership_to_verified_prefix(candidate_heads, prefix)
            .await
    }

    #[cfg(test)]
    pub(super) async fn load_exact_membership_head_for_test(
        &mut self,
        reference: &MembershipHeadRef,
    ) -> Result<
        crate::protocol::membership::AuthorHead,
        crate::sync::store::membership::AnchoredChainError,
    > {
        self.history_verifier
            .load_exact_membership_head(reference)
            .await
    }

    #[cfg(test)]
    pub(super) async fn load_membership_at_exact_heads_for_test(
        &mut self,
        heads: &[MembershipHeadRef],
        resolutions: &[crate::protocol::membership::StoreMembershipConflictResolutionRef],
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        self.history_verifier
            .load_membership_at_exact_heads(heads, resolutions)
            .await
    }

    #[cfg(test)]
    pub(super) async fn assert_deep_membership_projection_for_test(
        &mut self,
        heads: &[MembershipHeadRef],
    ) {
        self.history_verifier
            .assert_deep_membership_projection(heads)
            .await;
    }

    pub(super) fn pull_has_scoped_graph(&self) -> bool {
        self.database.gates().has_scoped_graph()
    }

    pub(super) fn pull_schema_version(&self) -> u32 {
        self.database.schema_version()
    }

    pub(super) fn pull_receive_wall_ms(&self) -> u64 {
        self.database.receive_wall_ms()
    }

    pub(super) async fn pull_materialized_frontier(
        &self,
    ) -> Result<std::collections::BTreeMap<String, StoreBatchCommitRef>, crate::database::DbError>
    {
        self.database.materialized_frontier().await
    }

    pub(super) async fn pull_device_state_for_cut(
        &self,
        cut: &crate::protocol::store_commit::StoreHistoryCut,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), crate::database::DbError> {
        self.database.store_device_state_for_history_cut(cut).await
    }

    pub(super) async fn pull_device_state_for_order(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), crate::database::DbError> {
        self.database.store_device_state_for_order(order).await
    }

    pub(super) async fn pull_exact_materialized_ref(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Option<StoreBatchCommitRef>, crate::database::DbError> {
        self.database
            .exact_materialized_ref(stream_id, sequence)
            .await
    }

    pub(super) async fn pull_snapshot_coverage(
        &self,
    ) -> Result<CommitFrontier, crate::database::DbError> {
        self.database.snapshot_coverage_frontier().await
    }

    pub(super) async fn pull_exclusion_freezes(
        &self,
    ) -> Result<Vec<crate::protocol::store_commit::StoreDeviceProposalAck>, crate::database::DbError>
    {
        self.database.store_device_exclusion_freezes().await
    }

    pub(super) async fn record_pull_circle_close_exclusions(
        &self,
        exclusions: Vec<crate::sync::LocalCircleExclusion>,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .record_circle_close_exclusions(exclusions)
            .await
    }

    #[cfg(test)]
    pub(super) async fn reach_pull_after_remote_commit_test_point(
        &self,
        device_id: String,
        seq: u64,
    ) {
        self.database
            .reach_test_point(crate::database::DatabaseTestPoint::PullAfterRemoteCommit {
                device_id,
                seq,
            })
            .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn commit_pull_materialization(
        &self,
        materialization: pull::PreparedMergeMaterialization,
        retractions: Vec<crate::protocol::remote_object::VerifiedCandidateNonactivation>,
        local_store_membership: pull::LocalStoreMembership,
        routing_key: Option<crate::protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<pull::ApplyOutcome, crate::database::DbError> {
        self.database
            .apply_received_merge_materialization(
                materialization,
                retractions,
                local_store_membership,
                routing_key,
                receiver_wall_ms,
            )
            .await
    }

    pub(super) async fn prepare_pull_retained_history(
        &mut self,
    ) -> Result<Vec<crate::database::OwnedVerifiedMergeMaterialization>, pull::StorePullError> {
        let retained_refs = self.database.retained_merge_materialization_refs().await?;
        self.history_verifier.verify_refs(retained_refs).await?;
        let retained_commit_proofs = self.history_verifier.retained_commit_proofs();
        let retained = self
            .database
            .retained_merge_replay_inputs_with_verified_commits(
                self.history_verifier.root().clone(),
                retained_commit_proofs,
            )
            .await?;
        self.resume_merge_retraction_cleanups().await?;
        Ok(retained)
    }

    pub(super) async fn load_active_pull_registrations(
        &self,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::StoreDeviceRegistrationRef,
            crate::protocol::store_commit::StoreDeviceRegistration,
        )>,
        pull::StorePullError,
    > {
        let durable = self
            .database
            .activated_store_device_registration_records()
            .await?;
        let mut verified = Vec::with_capacity(durable.len());
        for (reference, expected) in durable {
            let opened = self.history_verifier.load_registration(&reference).await?;
            if opened.value != expected {
                return Err(pull::StorePullError::Database(format!(
                    "activated Store registration {} differs from its exact remote bytes",
                    reference.device_id
                )));
            }
            if !matches!(
                opened.value.store_commits,
                crate::protocol::store_commit::DeviceStreamAnchor::StoreAnnouncements { .. }
            ) {
                return Err(pull::StorePullError::Database(format!(
                    "activated Store registration {} has no Merge announcement anchor",
                    reference.device_id
                )));
            }
            verified.push((reference, opened.value));
        }
        Ok(verified)
    }

    pub(super) async fn discover_pull_owner_recoveries(
        &self,
        membership: &MembershipChain,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::StoreDeviceRegistrationRef,
            crate::protocol::store_commit::StoreDeviceRegistration,
        )>,
        pull::StorePullError,
    > {
        self.history_verifier
            .discover_owner_recoveries(membership)
            .await
    }

    pub(super) async fn discover_pull_stream(
        &mut self,
        registration_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        inactive_accepted_cut: Option<&crate::protocol::store_commit::StoreHistoryCut>,
    ) -> Result<pull::MergeStreamDiscovery, pull::StorePullError> {
        pull::discover_merge_stream(
            &mut self.history_verifier,
            registration_ref,
            registration,
            inactive_accepted_cut,
        )
        .await
    }

    pub(super) async fn verify_pull_refs(
        &mut self,
        references: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<(), pull::StorePullError> {
        self.history_verifier.verify_refs(references).await
    }

    pub(super) fn verified_pull_commit(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<pull::VerifiedPullCandidate> {
        self.history_verifier.verified_pull_candidate(reference)
    }

    pub(super) fn verified_pull_membership_prefix(
        &self,
        predecessors: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<VerifiedMergeMembershipPrefix, pull::StorePullError> {
        self.history_verifier
            .verified_membership_prefix(predecessors)
    }

    pub(super) async fn load_pull_store_package(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<Option<crate::storage::VerifiedObject<Vec<u8>>>, crate::storage::StoreObjectError>
    {
        self.history_verifier.load_store_package(reference).await
    }

    pub(super) async fn load_pull_predecessor_membership(
        &mut self,
        state: &StoreMembershipStateRef,
    ) -> Result<MembershipChain, RegistrationLoadError> {
        load_merge_predecessor_membership_with_history(&mut self.history_verifier, state).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn pull_readiness(
        &mut self,
        coverage: &CommitFrontier,
        frontier: &std::collections::BTreeMap<String, StoreBatchCommitRef>,
        device_state: &ResolvedStoreDeviceState,
        exclusion_freezes: &[crate::protocol::store_commit::StoreDeviceProposalAck],
        commit_ref: &StoreBatchCommitRef,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
    ) -> Result<pull::Readiness, pull::StorePullError> {
        self.history_verifier
            .readiness(
                &self.database,
                coverage,
                frontier,
                device_state,
                exclusion_freezes,
                commit_ref,
                commit,
            )
            .await
    }

    pub(super) async fn verified_pull_membership_objects(
        &mut self,
        commit_ref: &StoreBatchCommitRef,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
    ) -> Result<Option<pull::VerifiedMergeMembershipClosure>, pull::StorePullError> {
        self.history_verifier
            .verified_membership_objects(commit_ref, commit)
            .await
    }

    pub(super) async fn verify_pull_owner_recovery_activation(
        &self,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
    ) -> Result<
        Option<(
            crate::protocol::membership::MembershipGrantId,
            crate::protocol::store_commit::OwnerRecoveryActivationId,
        )>,
        pull::StorePullError,
    > {
        self.history_verifier
            .verify_owner_recovery_activation(commit)
            .await
    }

    pub(super) async fn retain_pull_acknowledgement(
        &self,
        commit_ref: &StoreBatchCommitRef,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
        author: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<
        Option<crate::protocol::store_commit::RetainedVerifiedActivatedAck>,
        pull::StorePullError,
    > {
        let acknowledgement = self
            .history_verifier
            .validate_commit_acknowledgement(commit, author)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => pull::StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => pull::StorePullError::Database(error),
            })?;
        match acknowledgement {
            Some((reference, value)) => self
                .history_verifier
                .retain_acknowledgement(commit_ref, commit, author, reference, value)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    pub(super) fn remember_pull_commit(
        &mut self,
        commit: crate::protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<(), pull::StorePullError> {
        self.history_verifier
            .remember(commit)
            .map_err(|error| pull::StorePullError::Database(error.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn verified_pull_terminal_retractions(
        &mut self,
        activation_head: &crate::protocol::store_commit::StoreDeviceHead,
        activation_head_object: &crate::storage::ExactObjectRef,
        activation_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        activation_predecessor_state: &ResolvedStoreDeviceState,
        activation_predecessor_membership: &MembershipChain,
        device_operations: &crate::protocol::store_commit::VerifiedStoreDeviceOperations,
        loaded_predecessor_memberships: &pull::LoadedMergePredecessorMemberships,
    ) -> Result<
        Vec<crate::protocol::remote_object::VerifiedCandidateNonactivation>,
        pull::StorePullError,
    > {
        let root = self.history_verifier.root().clone();
        let retained = self
            .database
            .retained_merge_replay_inputs(root.clone())
            .await?;
        let mut verified_retained = BTreeMap::new();
        for materialization in &retained {
            let verified = self
                .history_verifier
                .authenticate_bytes(
                    materialization.commit_ref(),
                    &materialization.commit().to_bytes(),
                )
                .await?;
            if verified.value() != materialization.commit() {
                return Err(pull::StorePullError::Database(
                    "retained Merge materialization differs from its authenticated commit"
                        .to_string(),
                ));
            }
            verified_retained.insert(materialization.commit_ref().clone(), verified);
        }
        let activation_commit_ref = activation_commit.reference();
        let activation_commit_value = activation_commit.value();
        let activation_head_ref = crate::protocol::store_commit::StoreDeviceHeadRef {
            head_hash: activation_head.head_hash(),
            object: activation_head_object.clone(),
        };
        let current_membership_ref = &activation_commit_value.membership_state;
        let MembershipStatus::Resolved(current_resolved) =
            activation_predecessor_membership.status()
        else {
            return Err(pull::StorePullError::Database(
                "Merge terminal retraction witness membership is conflicted".to_string(),
            ));
        };
        let mut retractions = Vec::new();
        for materialization in &retained {
            let candidate = verified_retained
                .get(materialization.commit_ref())
                .expect("every retained Merge materialization was authenticated");
            let mut locator = self
                .database
                .author_exclusion_activation_for_candidate(
                    root.clone(),
                    materialization.commit_ref().clone(),
                    candidate.value().author_registration.clone(),
                )
                .await?;
            if locator.is_none() {
                let expected_stream =
                    crate::protocol::store_commit::StreamActivation::device_authorized_stream_id(
                        root.store_root_hash,
                        &candidate.value().author_registration,
                        crate::protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
                    );
                for (exclusion, accepted_cut) in device_operations.exclusions() {
                    if exclusion.proposal.target != candidate.value().author_registration {
                        continue;
                    }
                    let accepted_cut = &accepted_cut.0;
                    let beyond_cutoff =
                        accepted_cut.get(&expected_stream).is_none_or(|reference| {
                            materialization.commit_ref().coord.sequence()
                                > reference.coord.sequence()
                        });
                    if beyond_cutoff {
                        locator =
                            Some(crate::database::AuthorExclusionActivationLocator::verified(
                                exclusion.clone(),
                                accepted_cut.clone(),
                                activation_commit_ref.clone(),
                                activation_head_ref.clone(),
                            ));
                        break;
                    }
                }
            }
            let Some(locator) = locator else {
                let Some(authority) = candidate.value().membership_authority.as_ref() else {
                    continue;
                };
                let predecessor_membership =
                    loaded_predecessor_memberships.membership_for(materialization.commit_ref())?;
                let MembershipStatus::Resolved(predecessor_resolved) =
                    predecessor_membership.status()
                else {
                    return Err(pull::StorePullError::Database(
                        "retained candidate predecessor membership is conflicted".to_string(),
                    ));
                };
                let mut matching = predecessor_resolved
                    .active_grants()
                    .filter(|(_, record)| &record.creation_authority == authority);
                let Some((grant_id, _)) = matching.next() else {
                    return Err(pull::StorePullError::Database(
                        "retained candidate has no exact predecessor grant authority".to_string(),
                    ));
                };
                if matching.next().is_some() {
                    return Err(pull::StorePullError::Database(
                        "retained candidate authority identifies multiple predecessor grants"
                            .to_string(),
                    ));
                }
                if !matches!(
                    current_resolved.grants.get(grant_id),
                    Some(crate::protocol::causal_grants::GrantState::Tombstoned { .. })
                ) {
                    continue;
                }
                let nonactivation = self
                    .history_verifier
                    .verify_membership_grant_revocation_nonactivation(
                        grant_id,
                        current_membership_ref,
                        activation_commit_ref,
                        &activation_head_ref,
                        candidate,
                        materialization.activation_head(),
                        materialization.activation_head_object(),
                    )
                    .await?;
                retractions.push(nonactivation);
                continue;
            };
            let nonactivation = self
                .history_verifier
                .verify_author_exclusion_nonactivation(
                    &locator,
                    activation_head,
                    activation_head_object,
                    activation_commit,
                    activation_predecessor_state,
                    device_operations,
                    candidate,
                    materialization.activation_head(),
                    materialization.activation_head_object(),
                )
                .await?;
            retractions.push(nonactivation);
        }
        let mut verified_by_reference = retractions
            .into_iter()
            .map(|verified| {
                let reference = verified
                    .candidate_reference()
                    .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
                Ok((reference, verified))
            })
            .collect::<Result<BTreeMap<_, _>, pull::StorePullError>>()?;
        loop {
            let mut additions = Vec::new();
            for materialization in &retained {
                if verified_by_reference.contains_key(materialization.commit_ref()) {
                    continue;
                }
                let candidate = verified_retained
                    .get(materialization.commit_ref())
                    .expect("every retained Merge materialization was authenticated");
                let dependency = pull::commit_predecessor_references(candidate.value())
                    .into_iter()
                    .find_map(|reference| {
                        verified_by_reference
                            .get(&reference)
                            .map(|verified| (reference, verified))
                    });
                let Some((_dependency_reference, dependency)) = dependency else {
                    continue;
                };
                let verified = crate::protocol::remote_object::VerifiedCandidateNonactivation::dependency_retraction(
                    dependency,
                    crate::protocol::store_commit::StoreBatchCommitDeletionTarget {
                        coord: materialization.commit_ref().coord.clone(),
                        object: materialization.commit_ref().object.clone(),
                        canonical_signed_bytes: candidate.value().to_bytes(),
                    },
                    candidate.author(),
                    materialization.activation_head_object().clone(),
                )
                .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
                additions.push((materialization.commit_ref().clone(), verified));
            }
            if additions.is_empty() {
                break;
            }
            for (reference, verified) in additions {
                if verified_by_reference.insert(reference, verified).is_some() {
                    return Err(pull::StorePullError::Database(
                        "transitive Merge retraction constructed duplicate proof".to_string(),
                    ));
                }
            }
        }
        let removed = verified_by_reference
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        if retained.iter().any(|materialization| {
            !removed.contains(materialization.commit_ref())
                && materialization
                    .history_summary()
                    .causal_cut
                    .values()
                    .any(|reference| removed.contains(reference))
        }) {
            return Err(pull::StorePullError::Database(
                "surviving retained Merge summary contains a retracted dependency".to_string(),
            ));
        }
        Ok(verified_by_reference.into_values().collect())
    }

    pub(super) async fn cleanup_merge_candidate(
        &mut self,
        write_id: crate::WriteId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        let root = self.history_verifier.root().clone();
        for verification in self
            .database
            .merge_candidate_terminal_verifications(root, write_id.clone())
            .await?
        {
            self.apply_terminal_nonactivation(TerminalNonactivationCandidate::StoreWrite {
                write_id: write_id.clone(),
                verification,
            })
            .await?;
        }
        let targets = self
            .database
            .merge_candidate_cleanup_targets(write_id)
            .await?;
        for target in targets {
            self.history_verifier
                .storage()
                .delete_protocol_object(&target.object)
                .await
                .map_err(crate::storage::StoreObjectError::from)?;
            self.database
                .mark_candidate_cleanup_absent(target.object)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn cleanup_circle_operation_candidate(
        &mut self,
        operation_id: &crate::protocol::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        let root = self.history_verifier.root().clone();
        for verification in self
            .database
            .circle_operation_discard_terminal_verifications(root, operation_id)
            .await?
        {
            self.apply_terminal_nonactivation(TerminalNonactivationCandidate::CircleOperation {
                operation_id: operation_id.clone(),
                verification,
            })
            .await?;
        }
        let targets = self
            .database
            .circle_operation_discard_cleanup_targets(operation_id)
            .await?;
        for target in targets {
            self.history_verifier
                .storage()
                .delete_protocol_object(&target.object)
                .await
                .map_err(crate::storage::StoreObjectError::from)?;
            self.database
                .mark_candidate_cleanup_absent(target.object)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn resume_merge_retraction_cleanups(
        &mut self,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        for candidate in self.database.pending_merge_retraction_cleanups().await? {
            let root = self.history_verifier.root().clone();
            let verification = self
                .database
                .merge_retraction_cleanup_verification(root, candidate.clone())
                .await?;
            self.apply_terminal_nonactivation(TerminalNonactivationCandidate::MergeRetraction {
                reference: candidate.clone(),
                verification,
            })
            .await?;
            for target in self
                .database
                .merge_retraction_cleanup_targets(candidate.clone())
                .await?
            {
                self.history_verifier
                    .storage()
                    .delete_protocol_object(&target.object)
                    .await
                    .map_err(crate::storage::StoreObjectError::from)?;
                self.database
                    .mark_candidate_cleanup_absent(target.object)
                    .await?;
            }
            self.database
                .finish_merge_retraction_cleanup(candidate)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn materialize_device_join_activation(
        &mut self,
        reference: &StoreBatchCommitRef,
        expected_outcome: &crate::protocol::store_commit::DeviceJoinOutcomeRef,
        membership_state: &StoreMembershipStateRef,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        self.materialize_device_join_activation_inner(reference, expected_outcome, membership_state)
            .await
    }

    pub(super) async fn abandon_excluded_merge_candidate(
        &mut self,
        write_id: crate::WriteId,
    ) -> Result<Option<abandonment::MergeCandidateAbandonment>, StoreError> {
        let root = self.history_verifier.root().clone();
        let database = self.database.clone();
        let db = &database;
        match database.merge_abandonment_state(&write_id).await? {
            crate::database::MergeAbandonmentState::None => {
                if database.merge_candidate_cleanup_pending(&write_id).await? {
                    self.cleanup_merge_candidate(write_id.clone()).await?;
                    database
                        .finish_retracted_merge_candidate_cleanup(write_id)
                        .await?;
                    return Ok(Some(abandonment::MergeCandidateAbandonment::Abandoned));
                }
                if matches!(
                    db.write_status(&write_id).await?,
                    crate::WriteStatus::Resolved(_)
                ) {
                    return Ok(Some(abandonment::MergeCandidateAbandonment::NotRequired));
                }
                let Some(candidate) = database.blocked_merge_candidate(write_id.clone()).await?
                else {
                    return Ok(None);
                };
                let verified = self
                    .history_verifier
                    .authenticate_blocked_candidate(&candidate)
                    .await?;
                let Some(nonactivation) = self
                    .excluded_candidate_nonactivation(
                        &verified,
                        &candidate.head.value,
                        &candidate.head.object,
                    )
                    .await?
                else {
                    return Ok(None);
                };
                database
                    .begin_blocked_merge_candidate_nonactivation(
                        root.clone(),
                        write_id.clone(),
                        nonactivation,
                    )
                    .await?;
                self.cleanup_merge_candidate(write_id).await?;
                Ok(Some(abandonment::MergeCandidateAbandonment::Abandoned))
            }
            crate::database::MergeAbandonmentState::Prepared => {
                let candidates = database
                    .prepared_merge_abandonment_candidates(write_id.clone())
                    .await?
                    .ok_or_else(|| {
                        StoreError::InvalidOutbound(
                            "prepared Merge abandonment has no exact candidates".to_string(),
                        )
                    })?;
                let verified_candidate = self
                    .history_verifier
                    .authenticate_blocked_candidate(&candidates.candidate)
                    .await?;
                let candidate = self
                    .excluded_candidate_nonactivation(
                        &verified_candidate,
                        &candidates.candidate.head.value,
                        &candidates.candidate.head.object,
                    )
                    .await?;
                let verified_authority = self
                    .history_verifier
                    .authenticate_blocked_candidate(&candidates.authority)
                    .await?;
                let authority = self
                    .excluded_candidate_nonactivation(
                        &verified_authority,
                        &candidates.authority.head.value,
                        &candidates.authority.head.object,
                    )
                    .await?;
                match (candidate, authority) {
                    (Some(candidate), Some(authority)) => {
                        database
                            .begin_prepared_merge_abandonment_nonactivation(
                                root.clone(),
                                write_id.clone(),
                                candidate,
                                authority,
                            )
                            .await?;
                        self.cleanup_merge_candidate(write_id.clone()).await?;
                        database
                            .finish_author_excluded_merge_abandonment(write_id)
                            .await?;
                        Ok(Some(abandonment::MergeCandidateAbandonment::Abandoned))
                    }
                    (None, None) => Ok(None),
                    _ => Err(StoreError::InvalidOutbound(
                        "prepared Merge abandonment candidates disagree on author exclusion"
                            .to_string(),
                    )),
                }
            }
            crate::database::MergeAbandonmentState::Accepted
            | crate::database::MergeAbandonmentState::OtherWon => {
                if database.merge_candidate_cleanup_pending(&write_id).await? {
                    self.cleanup_merge_candidate(write_id.clone()).await?;
                }
                if matches!(
                    database.merge_abandonment_state(&write_id).await?,
                    crate::database::MergeAbandonmentState::OtherWon
                ) {
                    database.finish_lost_merge_abandonment(write_id).await?;
                }
                Ok(Some(abandonment::MergeCandidateAbandonment::Abandoned))
            }
            crate::database::MergeAbandonmentState::AuthorExcluded => {
                if database.merge_candidate_cleanup_pending(&write_id).await? {
                    self.cleanup_merge_candidate(write_id.clone()).await?;
                }
                database
                    .finish_author_excluded_merge_abandonment(write_id)
                    .await?;
                Ok(Some(abandonment::MergeCandidateAbandonment::Abandoned))
            }
            crate::database::MergeAbandonmentState::CandidateWon => Ok(None),
        }
    }
}

impl AuthorizedStoreHistory<'_> {
    async fn materialize_device_join_activation_inner(
        &mut self,
        reference: &StoreBatchCommitRef,
        expected_outcome: &crate::protocol::store_commit::DeviceJoinOutcomeRef,
        membership_state: &StoreMembershipStateRef,
    ) -> Result<(), pull::StorePullError> {
        let root = self.history_verifier.root().clone();
        let crate::protocol::store_commit::StoreCommitCoord {
            stream_id,
            sequence,
        } = reference.coord;
        let stream_id = stream_id.to_string();
        if let Some(materialized) = self
            .database
            .exact_materialized_ref(&stream_id, sequence)
            .await?
        {
            if materialized == *reference {
                return Ok(());
            }
            return Err(pull::StorePullError::Database(format!(
                "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
            )));
        }
        let verified_commit = self.history_verifier.load_ref(reference).await?;
        let commit = verified_commit.value().clone();
        let author = verified_commit.author().clone();
        pull::verify_device_join_activation_commit(&commit, expected_outcome)?;
        if &commit.membership_state != membership_state {
            return Err(pull::StorePullError::Database(
                "device join activation differs from its expected Merge membership state"
                    .to_string(),
            ));
        }
        let predecessor_cut = commit
            .order
            .predecessor_cut()
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        let frontier = predecessor_cut.0;
        let membership = self
            .history_verifier
            .verify_merge_history_authority(&frontier, &commit.membership_state)
            .await?
            .membership;
        let accepted_frontier = pull::commit_predecessor_references(&commit);
        let registrations = self
            .history_verifier
            .load_merge_commit_registrations(&commit, &author, &membership, &accepted_frontier)
            .await?;
        if !pull::membership_authorizes(Some(&membership), &commit, &author) {
            return Err(pull::StorePullError::Database(
                "device join activation author is not authorized by its exact predecessor membership"
                    .to_string(),
            ));
        }
        let head = self
            .history_verifier
            .load_activation_head(&verified_commit)
            .await?;
        let head_ref = crate::protocol::store_commit::StoreDeviceHeadRef {
            head_hash: head.value.head_hash(),
            object: head.object.clone(),
        };
        let (_, predecessor_state) = self
            .database
            .store_device_state_for_order(&commit.order)
            .await?;
        let (authorized_predecessor, recovery_author) =
            pull::predecessor_with_recovery_author(predecessor_state, &commit, &registrations)
                .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        let device_operations =
            crate::protocol::store_commit::VerifiedStoreDeviceOperations::without_exclusions(
                &commit,
            )
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        let state_after = device_operations
            .apply_to(authorized_predecessor, &commit.device_state)
            .and_then(|state| {
                pull::apply_verified_device_lifecycle(
                    state,
                    &commit,
                    &registrations,
                    recovery_author.as_ref(),
                    None,
                )
            })
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        let history = self
            .prepare_merge_history_successor(
                &verified_commit,
                &membership,
                recovery_author.as_ref(),
                state_after.clone(),
                MergeHistorySuccessorEvidence {
                    registrations: commit
                        .device_registrations()
                        .iter()
                        .zip(&registrations)
                        .map(|(activation, (value, _))| {
                            crate::protocol::store_commit::RetainedVerifiedRegistration {
                                reference: activation.registration.clone(),
                                value: value.clone(),
                            }
                        })
                        .collect(),
                    acknowledgement: None,
                    membership_proof: None,
                },
            )
            .await?;
        history
            .summary
            .open(&commit, reference, &head.value, &head_ref, &state_after)
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        self.database
            .materialize_device_join_activation(
                root,
                verified_commit,
                registrations,
                device_operations,
                head.value,
                head.object,
                history.summary,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn retained_device_state_for_order(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), pull::StorePullError> {
        let frontier = order
            .predecessor_cut()
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?
            .0;
        let checkpoints = self
            .retained_history_checkpoints(frontier.values().cloned().collect())
            .await?;
        retained_merge_device_state(&self.history_verifier, &frontier, &checkpoints).await
    }

    pub(super) async fn authorize_retained_conflict_resolution(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[MembershipHeadRef],
        author_registration: &StoreDeviceRegistrationRef,
        resolver_pubkey: &str,
    ) -> Result<MergeConflictResolutionAuthorization, pull::StorePullError> {
        let frontier = order
            .predecessor_cut()
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?
            .0;
        let checkpoints = self
            .retained_history_checkpoints(frontier.values().cloned().collect())
            .await?;
        let prefix = VerifiedMergeMembershipPrefix::from_retained(&checkpoints)?;
        let membership = self
            .project_membership_to_verified_prefix(candidate_membership_heads, &prefix)
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        validate_retained_membership_floors(&checkpoints, &membership)?;
        prefix
            .validate_complete_membership(&membership)
            .map_err(pull::StorePullError::Database)?;
        let (device_state_ref, device_state) =
            retained_merge_device_state(&self.history_verifier, &frontier, &checkpoints).await?;
        if !super::verified_history::registration::device_state_has_active_registration(
            &device_state,
            author_registration,
        ) {
            return Err(pull::StorePullError::Database(
                "Merge conflict-resolution author is inactive at its predecessor cut".to_string(),
            ));
        }
        self.history_verifier
            .verify_canonical_owner_registration(
                &device_state,
                resolver_pubkey,
                author_registration,
            )
            .await?;
        Ok(MergeConflictResolutionAuthorization {
            membership,
            device_state_ref,
            device_state,
        })
    }

    pub(super) async fn authorize_retained_outbound(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[MembershipHeadRef],
        author_registration: &StoreDeviceRegistrationRef,
    ) -> Result<MergeOutboundAuthorization, pull::StorePullError> {
        let frontier = order
            .predecessor_cut()
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?
            .0;
        let checkpoints = self
            .retained_history_checkpoints(frontier.values().cloned().collect())
            .await?;
        let prefix = VerifiedMergeMembershipPrefix::from_retained(&checkpoints)?;
        let membership = self
            .project_membership_to_verified_prefix(candidate_membership_heads, &prefix)
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        validate_retained_membership_floors(&checkpoints, &membership)?;
        prefix
            .validate_complete_membership(&membership)
            .map_err(pull::StorePullError::Database)?;
        let (device_state_ref, device_state) =
            retained_merge_device_state(&self.history_verifier, &frontier, &checkpoints).await?;
        if !super::verified_history::registration::device_state_has_active_registration(
            &device_state,
            author_registration,
        ) {
            return Err(pull::StorePullError::Database(
                "Merge outbound author is inactive at its exact predecessor cut".to_string(),
            ));
        }
        let MembershipStatus::Resolved(resolved) = membership.status() else {
            return Err(pull::StorePullError::Database(
                "Merge outbound predecessor membership is conflicted".to_string(),
            ));
        };
        let membership_state = StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            device_state.recovery.clone(),
            resolved.state_hash,
        )
        .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        Ok(MergeOutboundAuthorization {
            membership,
            membership_state,
            device_state_ref,
            device_state,
        })
    }

    pub(super) async fn prepare_merge_history_successor(
        &self,
        verified_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &crate::protocol::membership::MembershipChain,
        recovery_author: Option<&crate::protocol::store_commit::StoreDeviceRegistrationRef>,
        state_after: crate::protocol::store_commit::ResolvedStoreDeviceState,
        evidence: crate::sync::store::owner::verified_history::MergeHistorySuccessorEvidence,
    ) -> Result<
        crate::sync::store::owner::verified_history::PreparedMergeHistorySuccessor,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let root = self.history_verifier.root();
        if verified_commit.store_root_hash() != root.store_root_hash {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "authenticated Merge successor belongs to another Store root".to_string(),
            ));
        }
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let author = verified_commit.author();
        state_after.validate_canonical().map_err(|error| {
            crate::sync::store::owner::pull::StorePullError::Database(format!(
                "validate Merge successor post-state: {error}"
            ))
        })?;
        let predecessor_refs =
            crate::sync::store::owner::pull::commit_predecessor_references(commit);
        let predecessors = self
            .retained_history_checkpoints(predecessor_refs.clone())
            .await?;
        let (expected_predecessor_ref, predecessor_state) = self
            .database
            .store_device_state_for_order(&commit.order)
            .await
            .map_err(|error| {
                crate::sync::store::owner::pull::StorePullError::Database(error.to_string())
            })?;
        if commit.device_state != expected_predecessor_ref {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "Merge successor names another predecessor device state".to_string(),
            ));
        }
        if let Some(recovery_author) = recovery_author {
            let retained_recovery_registration =
                evidence.registrations.iter().any(|registration| {
                    registration.reference == *recovery_author
                        && matches!(
                            &registration.value.origin,
                            crate::protocol::store_commit::StoreDeviceRegistrationOrigin::Recovery {
                                ..
                            }
                        )
                });
            let recovery_activation =
                commit.device_registrations().iter().any(|activation| {
                    activation.registration == *recovery_author
                        && matches!(
                        &activation.authority,
                        crate::protocol::store_commit::StoreDeviceRegistrationActivationRef::Recovery {
                            ..
                        }
                    )
                });
            if recovery_author != &commit.author_registration
                || !retained_recovery_registration
                || !recovery_activation
            {
                return Err(crate::sync::store::owner::pull::StorePullError::Database(
                    "Merge successor recovery author lacks its exact retained activation"
                        .to_string(),
                ));
            }
        }
        if !super::verified_history::registration::device_state_has_active_registration(
            &predecessor_state,
            &commit.author_registration,
        ) && recovery_author != Some(&commit.author_registration)
        {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "Merge successor author is inactive at its exact predecessor cut".to_string(),
            ));
        }
        super::verified_history::verify_merge_membership_state_ref(
            &commit.membership_state,
            membership,
            &predecessor_state,
        )?;

        compose_merge_history_successor(
            root,
            commit,
            commit_ref,
            membership,
            author,
            state_after,
            predecessors,
            evidence,
        )
    }

    pub(super) async fn prepare_merge_snapshot_history_summary(
        &self,
        coverage: &crate::protocol::store_commit::CommitFrontier,
        membership: &crate::protocol::membership::MembershipChain,
        state: &crate::protocol::store_commit::ResolvedStoreDeviceState,
        author_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        author: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<
        crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let frontier = &coverage.0;
        let root = self.history_verifier.root();
        let predecessors = self
            .retained_history_checkpoints(frontier.values().cloned().collect())
            .await?;
        compose_merge_snapshot_history_summary(
            root,
            coverage,
            membership,
            state,
            author_ref,
            author,
            predecessors,
        )
    }

    pub(super) async fn observe_occupied_merge_head(
        &mut self,
        expected: &crate::protocol::store_commit::StoreDeviceHead,
        expected_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<abandonment::VerifiedMergeWinner, StoreError> {
        let store_root_hash = self.history_verifier.root().store_root_hash;
        let context = crate::storage::ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            crate::storage::ProtocolObjectDomain::StoreHead,
        );
        let (winner_bytes, winner_prepared) = self
            .history_verifier
            .storage()
            .read_prepared_protocol_slot(&context, slot, semantic_prefix)
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
        let unverified: crate::protocol::store_commit::StoreDeviceHead =
            serde_json::from_slice(&winner_bytes).map_err(|error| {
                StoreError::InvalidOutbound(format!("parse competing Merge head: {error}"))
            })?;
        if unverified.author_registration != expected.author_registration
            || unverified.commit.coord != expected.commit.coord
            || unverified.successor.activation != expected.successor.activation
            || unverified.successor.predecessor != expected.successor.predecessor
        {
            return Err(StoreError::InvalidOutbound(
                "competing Merge head does not occupy the prepared successor point".to_string(),
            ));
        }
        let registration = self
            .database
            .activated_store_device_registration(expected.author_registration.clone())
            .await?;
        if expected_commit.store_root_hash() != store_root_hash
            || expected_commit.reference() != &expected.commit
            || expected_commit.author() != &registration
        {
            return Err(StoreError::InvalidOutbound(
                "expected Merge head differs from its authenticated commit".to_string(),
            ));
        }
        crate::protocol::store_commit::StoreDeviceHead::parse_at(
            &expected.to_bytes(),
            store_root_hash,
            &registration,
            &expected.commit,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let winner_commit = self.history_verifier.load_ref(&unverified.commit).await?;
        if winner_commit.author() != &registration {
            return Err(StoreError::InvalidOutbound(
                "occupied Merge head commit has a different authenticated author".to_string(),
            ));
        }
        let winner = crate::protocol::store_commit::StoreDeviceHead::parse_at(
            &winner_bytes,
            store_root_hash,
            &registration,
            &unverified.commit,
        )
        .map_err(|error| {
            StoreError::InvalidOutbound(format!("verify occupied Merge head: {error}"))
        })?;
        Ok(abandonment::VerifiedMergeWinner::from_verified_parts(
            store_root_hash,
            slot.clone(),
            expected.clone(),
            expected_commit.clone(),
            winner,
            winner_prepared,
            winner_commit,
        ))
    }

    async fn retained_history_checkpoints(
        &self,
        references: Vec<StoreBatchCommitRef>,
    ) -> Result<Vec<OpenedRetainedMergeHistorySummary>, pull::StorePullError> {
        let root = self.history_verifier.root();
        let checkpoints = self
            .database
            .retained_merge_history_frontier(root.clone(), references)
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        if checkpoints
            .iter()
            .any(|checkpoint| checkpoint.summary.store_root_hash != root.store_root_hash)
        {
            return Err(pull::StorePullError::Database(
                "Merge operation is missing retained predecessor authority".to_string(),
            ));
        }
        Ok(checkpoints)
    }

    pub(super) async fn observe_excluded_candidate_head(
        &mut self,
        candidate: &crate::protocol::store_commit::StoreDeviceHead,
        candidate_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_object: &crate::storage::ExactObjectRef,
    ) -> Result<abandonment::ExcludedCandidateHeadObservation, StoreError> {
        let store_root_hash = self.history_verifier.root().store_root_hash;
        let context = crate::storage::ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            crate::storage::ProtocolObjectDomain::StoreHead,
        );
        let prefix = crate::protocol::store_commit::head_slot_prefix(
            &candidate.author_registration.device_id.to_string(),
            candidate.commit.coord.sequence(),
        );
        match self
            .history_verifier
            .storage()
            .read_protocol_slot(&context, candidate_object.slot(), &prefix)
            .await
        {
            Err(crate::storage::StorageError::NotFound(_)) => {
                Ok(abandonment::ExcludedCandidateHeadObservation::AuthorExclusion)
            }
            Ok((bytes, object)) if bytes == candidate.to_bytes() && object == *candidate_object => {
                Ok(abandonment::ExcludedCandidateHeadObservation::AuthorExclusion)
            }
            Ok(_) => self
                .observe_occupied_merge_head(
                    candidate,
                    candidate_commit,
                    candidate_object.slot(),
                    &prefix,
                )
                .await
                .map(abandonment::ExcludedCandidateHeadObservation::MergeWinner),
            Err(error) => Err(crate::storage::StoreObjectError::Storage(error).into()),
        }
    }

    pub(super) async fn discard_candidate_nonactivation(
        &mut self,
        candidate: &crate::database::BlockedMergeCandidate,
        revoked_grant: Option<&crate::protocol::membership::MembershipGrantId>,
    ) -> Result<Option<crate::protocol::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let verified_candidate = self
            .history_verifier
            .authenticate_blocked_candidate(candidate)
            .await?;
        if let abandonment::ExcludedCandidateHeadObservation::MergeWinner(observation) = self
            .observe_excluded_candidate_head(
                &candidate.head.value,
                &verified_candidate,
                &candidate.head.object,
            )
            .await?
        {
            let target = crate::protocol::store_commit::StoreBatchCommitDeletionTarget {
                coord: verified_candidate.reference().coord.clone(),
                object: verified_candidate.reference().object.clone(),
                canonical_signed_bytes: verified_candidate.value().to_bytes(),
            };
            return Ok(Some(
                observation
                    .verified_nonactivation(target, verified_candidate.author())
                    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
            ));
        }
        if let Some(nonactivation) = self
            .excluded_candidate_nonactivation(
                &verified_candidate,
                &candidate.head.value,
                &candidate.head.object,
            )
            .await?
        {
            return Ok(Some(nonactivation));
        }
        let Some(revoked_grant) = revoked_grant else {
            return Ok(None);
        };
        self.membership_revocation_candidate_nonactivation(
            revoked_grant,
            &verified_candidate,
            &candidate.head.value,
            &candidate.head.object,
        )
        .await
    }

    async fn membership_revocation_candidate_nonactivation(
        &mut self,
        revoked_grant: &crate::protocol::membership::MembershipGrantId,
        candidate: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_head: &crate::protocol::store_commit::StoreDeviceHead,
        candidate_head_object: &crate::storage::ExactObjectRef,
    ) -> Result<Option<crate::protocol::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let root = self.history_verifier.root().clone();
        let expected_stream =
            crate::protocol::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &candidate.value().author_registration,
                crate::protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
        let candidate_sequence = candidate.reference().coord.sequence();
        for witness in self
            .database
            .retained_merge_replay_inputs(root.clone())
            .await?
        {
            let predecessor_cut = witness
                .commit()
                .order
                .predecessor_cut()
                .map_err(|error| StoreError::Database(error.to_string()))?;
            if predecessor_cut
                .commits()
                .get(&expected_stream)
                .is_some_and(|covered| candidate_sequence <= covered.coord.sequence())
            {
                continue;
            }
            let membership =
                super::verified_history::load_merge_predecessor_membership_with_history(
                    &mut self.history_verifier,
                    &witness.commit().membership_state,
                )
                .await
                .map_err(|error| match error {
                    super::verified_history::registration::RegistrationLoadError::Object(error) => {
                        StoreError::Object(error)
                    }
                    super::verified_history::registration::RegistrationLoadError::Invalid(
                        error,
                    ) => StoreError::Database(error),
                })?;
            let crate::protocol::membership::MembershipStatus::Resolved(resolved) =
                membership.status()
            else {
                continue;
            };
            if !matches!(
                resolved.grants.get(revoked_grant),
                Some(crate::protocol::causal_grants::GrantState::Tombstoned { .. })
            ) {
                continue;
            }
            let activation_head = crate::protocol::store_commit::StoreDeviceHeadRef {
                head_hash: witness.activation_head().head_hash(),
                object: witness.activation_head_object().clone(),
            };
            return self
                .history_verifier
                .verify_membership_grant_revocation_nonactivation(
                    revoked_grant,
                    &witness.commit().membership_state,
                    witness.commit_ref(),
                    &activation_head,
                    candidate,
                    candidate_head,
                    candidate_head_object,
                )
                .await
                .map(Some)
                .map_err(StoreError::from);
        }
        Ok(None)
    }

    pub(super) async fn excluded_candidate_nonactivation(
        &mut self,
        candidate: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_head: &crate::protocol::store_commit::StoreDeviceHead,
        candidate_head_object: &crate::storage::ExactObjectRef,
    ) -> Result<Option<crate::protocol::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let candidate_ref = candidate.reference().clone();
        let root = self.history_verifier.root().clone();
        let Some(locator) = self
            .database
            .author_exclusion_activation_for_candidate(
                root,
                candidate_ref.clone(),
                candidate.value().author_registration.clone(),
            )
            .await?
        else {
            return Ok(None);
        };
        let candidate_target = crate::protocol::store_commit::StoreBatchCommitDeletionTarget {
            coord: candidate_ref.coord.clone(),
            object: candidate_ref.object.clone(),
            canonical_signed_bytes: candidate.value().to_bytes(),
        };
        let nonactivation = match self
            .observe_excluded_candidate_head(candidate_head, candidate, candidate_head_object)
            .await?
        {
            abandonment::ExcludedCandidateHeadObservation::AuthorExclusion => {
                self.verify_author_exclusion_nonactivation(
                    &locator,
                    candidate,
                    candidate_head,
                    candidate_head_object,
                )
                .await?
            }
            abandonment::ExcludedCandidateHeadObservation::MergeWinner(observation) => observation
                .verified_nonactivation(candidate_target, candidate.author())
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
        };
        Ok(Some(nonactivation))
    }

    pub(super) async fn verify_author_exclusion_nonactivation(
        &mut self,
        locator: &crate::database::AuthorExclusionActivationLocator,
        candidate: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_head: &crate::protocol::store_commit::StoreDeviceHead,
        candidate_head_object: &crate::storage::ExactObjectRef,
    ) -> Result<
        crate::protocol::remote_object::VerifiedCandidateNonactivation,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let retained = self
            .database
            .retained_merge_materialization(
                self.history_verifier.root().clone(),
                locator.activation_commit().clone(),
            )
            .await?;
        let (_, predecessor_state) = self
            .database
            .store_device_state_for_order(&retained.commit().order)
            .await?;
        let activation_commit = self
            .history_verifier
            .load_ref(retained.commit_ref())
            .await?;
        if activation_commit.value() != retained.commit() {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "retained exclusion activation differs from its authenticated commit".to_string(),
            ));
        }
        self.history_verifier
            .verify_author_exclusion_nonactivation(
                locator,
                retained.activation_head(),
                retained.activation_head_object(),
                &activation_commit,
                &predecessor_state,
                retained.device_operations(),
                candidate,
                candidate_head,
                candidate_head_object,
            )
            .await
    }
}

#[cfg(test)]
impl AuthorizedStoreHistory<'_> {
    pub(super) async fn verify_device_join_attempt_for_test(
        &mut self,
        reference: &crate::protocol::store_commit::DeviceJoinAttemptRef,
        owner: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), StoreError> {
        self.history_verifier
            .load_verified_device_join_attempt(reference, owner)
            .await?;
        Ok(())
    }

    pub(super) async fn exact_next_announcement_slot_for_test(
        &mut self,
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
        let previous = match previous {
            Some(reference) => Some(
                self.load_commit(reference)
                    .await
                    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
            ),
            None => None,
        };
        self.history_verifier
            .exact_next_announcement_slot(registration_ref, registration, previous.as_ref())
            .await
    }

    pub(super) async fn load_commit_ancestry_until_for_test(
        &mut self,
        start: crate::protocol::store_commit::StoreBatchCommitRef,
        coverage: &crate::protocol::store_commit::CommitFrontier,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::StoreBatchCommitRef,
            crate::protocol::store_commit::VerifiedStoreBatchCommit,
        )>,
        StoreError,
    > {
        let mut ancestry = Vec::new();
        let mut cursor = start;
        while !coverage.0.values().any(|covered| covered == &cursor) {
            let commit = self
                .load_commit(&cursor)
                .await
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
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

    pub(super) async fn open_circle_package_for_test(
        &self,
        access: &crate::sync::store::circle_controls::CirclePackageAccess,
        commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        reference: &crate::protocol::store_commit::CirclePackageRef,
    ) -> Result<Vec<u8>, StoreError> {
        let opened = access
            .open_package(
                self.history_verifier.storage(),
                commit,
                reference,
                commit.author(),
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(opened.object.value)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn pull_readiness_for_test(
        &mut self,
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
        self.history_verifier
            .readiness(
                &self.database,
                coverage,
                frontier,
                device_state,
                exclusion_freezes,
                commit_ref,
                commit,
            )
            .await
    }

    pub(super) async fn verified_merge_membership_prefix_for_test(
        &mut self,
        references: impl IntoIterator<Item = crate::protocol::store_commit::StoreBatchCommitRef>,
        predecessors: impl IntoIterator<Item = crate::protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<VerifiedMergeMembershipPrefix, pull::StorePullError> {
        self.history_verifier.verify_refs(references).await?;
        self.history_verifier
            .verified_membership_prefix(predecessors)
    }

    pub(super) async fn load_founder_registration_for_test(
        &mut self,
    ) -> Result<
        crate::storage::VerifiedObject<crate::protocol::store_commit::StoreDeviceRegistration>,
        StoreError,
    > {
        Ok(self.history_verifier.load_founder_registration().await?)
    }

    pub(super) async fn prepare_merge_history_successor_for_test(
        &mut self,
        verified_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &MembershipChain,
        recovery_author: Option<&crate::protocol::store_commit::StoreDeviceRegistrationRef>,
        evidence: MergeHistorySuccessorEvidence,
    ) -> Result<PreparedMergeHistorySuccessor, StoreError> {
        let (_, state_after) = self
            .database
            .store_device_state_for_order(&verified_commit.value().order)
            .await?;
        self.prepare_merge_history_successor(
            verified_commit,
            membership,
            recovery_author,
            state_after,
            evidence,
        )
        .await
        .map_err(StoreError::from)
    }

    pub(super) async fn prepare_device_join_bootstrap_for_test(
        &mut self,
        coverage: &crate::protocol::store_commit::StoreHistoryCut,
        attempt_activation: &crate::protocol::store_commit::StoreBatchCommitRef,
        membership_state: &crate::protocol::circle_control::StoreMembershipStateRef,
    ) -> Result<crate::sync::store::owner::pull::DeviceJoinBootstrapPlan, StoreError> {
        self.history_verifier
            .prepare_device_join_bootstrap(coverage, attempt_activation, membership_state)
            .await
            .map_err(StoreError::from)
    }

    pub(super) async fn load_store_package_for_test(
        &mut self,
        reference: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<Option<crate::storage::VerifiedObject<Vec<u8>>>, StoreError> {
        Ok(self.history_verifier.load_store_package(reference).await?)
    }

    pub(super) async fn load_store_ack_for_test(
        &mut self,
        reference: &crate::protocol::store_commit::StoreAckRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<crate::protocol::store_commit::StoreAck, StoreError> {
        Ok(self
            .history_verifier
            .load_store_ack(reference, registration)
            .await?
            .value)
    }

    pub(super) async fn load_head_for_test(
        &mut self,
        reference: &crate::protocol::store_commit::StoreDeviceHeadRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        commit: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<crate::protocol::store_commit::StoreDeviceHead, StoreError> {
        Ok(self
            .history_verifier
            .load_head(reference, registration, commit)
            .await?
            .value)
    }
}

fn validate_retained_membership_floors(
    checkpoints: &[OpenedRetainedMergeHistorySummary],
    membership: &MembershipChain,
) -> Result<(), pull::StorePullError> {
    if checkpoints.iter().any(|checkpoint| {
        !checkpoint
            .summary
            .membership_floor
            .is_included_in(membership)
    }) {
        return Err(pull::StorePullError::Database(
            "Merge membership omits retained effective predecessor authority".to_string(),
        ));
    }
    Ok(())
}

async fn retained_merge_device_state(
    history_verifier: &MergeHistoryVerifier<'_>,
    frontier: &BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
    checkpoints: &[OpenedRetainedMergeHistorySummary],
) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), pull::StorePullError> {
    let root = history_verifier.root();
    let root_value = history_verifier.verified_root();
    let state = if checkpoints.is_empty() {
        let founder = history_verifier.load_founder_registration().await?;
        let founder_ref =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        ResolvedStoreDeviceState::founder(
            root,
            founder_ref,
            &root_value.descriptor.founder_pubkey,
            root_value.descriptor.founder_grant.clone(),
            &root_value.descriptor.founder_recovery,
        )
        .map_err(|error| pull::StorePullError::Database(error.to_string()))?
    } else {
        ResolvedStoreDeviceState::merge(
            checkpoints
                .iter()
                .map(|checkpoint| checkpoint.post_state.clone()),
        )
        .map_err(|error| pull::StorePullError::Database(error.to_string()))?
    };
    let reference = StoreDeviceStateRef::from_resolved(CommitFrontier(frontier.clone()), &state)
        .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
    Ok((reference, state))
}

impl AuthorizedStoreHistory<'_> {
    async fn apply_terminal_nonactivation(
        &mut self,
        candidate: TerminalNonactivationCandidate,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        let verification = match &candidate {
            TerminalNonactivationCandidate::StoreWrite { verification, .. }
            | TerminalNonactivationCandidate::CircleOperation { verification, .. }
            | TerminalNonactivationCandidate::MergeRetraction { verification, .. } => verification,
        };
        let nonactivation = self.verify_terminal_nonactivation(verification).await?;
        let root = self.history_verifier.root().clone();
        match candidate {
            TerminalNonactivationCandidate::StoreWrite { write_id, .. } => {
                self.database
                    .reconcile_merge_candidate_terminal_head(root, write_id, nonactivation)
                    .await?;
            }
            TerminalNonactivationCandidate::CircleOperation { operation_id, .. } => {
                self.database
                    .reconcile_circle_operation_terminal_head(root, &operation_id, nonactivation)
                    .await?;
            }
            TerminalNonactivationCandidate::MergeRetraction { reference, .. } => {
                self.database
                    .confirm_merge_retraction_cleanup_nonactivation(root, reference, nonactivation)
                    .await?;
            }
        }
        Ok(())
    }

    async fn verify_terminal_nonactivation(
        &mut self,
        verification: &crate::database::TerminalCandidateCleanupVerification,
    ) -> Result<
        crate::protocol::remote_object::VerifiedCandidateNonactivation,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let reference = &verification.candidate.head.value.commit;
        let candidate = self
            .history_verifier
            .authenticate_bytes(reference, &verification.candidate.commit.bytes)
            .await?;
        if candidate.value() != verification.candidate.commit.value.value() {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "terminal cleanup candidate differs from its authenticated commit".to_string(),
            ));
        }
        let target = crate::protocol::store_commit::StoreBatchCommitDeletionTarget {
            coord: reference.coord.clone(),
            object: verification.candidate.commit.object.clone(),
            canonical_signed_bytes: verification.candidate.commit.bytes.clone(),
        };
        match &verification.authority {
            crate::database::TerminalCandidateAuthority::AuthorExclusion(locator) => {
                self.verify_author_exclusion_nonactivation(
                    locator,
                    &candidate,
                    &verification.candidate.head.value,
                    &verification.candidate.head.object,
                )
                .await
            }
            crate::database::TerminalCandidateAuthority::MembershipGrantRevocation {
                grant_id,
                membership,
                activation_commit,
                activation_head,
            } => {
                self.history_verifier
                    .verify_membership_grant_revocation_nonactivation(
                        grant_id,
                        membership,
                        activation_commit,
                        activation_head,
                        &candidate,
                        &verification.candidate.head.value,
                        &verification.candidate.head.object,
                    )
                    .await
            }
            crate::database::TerminalCandidateAuthority::DependencyRetraction(authority) => {
                crate::protocol::remote_object::VerifiedCandidateNonactivation::from_verified_dependency_retraction_authority(
                    authority.clone(),
                    target,
                    candidate.author(),
                    verification.candidate.head.object.clone(),
                )
                .map_err(|error| {
                    crate::sync::store::owner::pull::StorePullError::Database(error.to_string())
                })
            }
        }
    }
}
