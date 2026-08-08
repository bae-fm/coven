use super::*;
use crate::sync::store::pull::StorePullError;
use crate::sync::store::StoreRegistrationError;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::store_commit::{
    ack_slot_prefix, DeviceStreamAnchor, StoreAck, StoreAckExclusionState, StoreAckRef,
    SuccessorLink,
};
use coven_protocol::store_commit::{DeviceJoinOutcome, VerifiedStoreBatchCommit};

pub(crate) struct DeviceJoinHistory<'operation, 'storage> {
    database: StoreDatabase,
    storage: &'storage dyn coven_storage::SyncStorage,
    history: &'operation mut super::MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> DeviceJoinHistory<'operation, 'storage> {
    pub(crate) fn new(
        database: StoreDatabase,
        storage: &'storage dyn coven_storage::SyncStorage,
        history: &'operation mut super::MergeHistoryVerifier<'storage>,
    ) -> Self {
        Self {
            database,
            storage,
            history,
        }
    }

    fn root(&self) -> &crate::sync::store::protocol_root::VerifiedStoreRoot {
        self.history.verified_root()
    }

    pub(crate) async fn load_registration(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<coven_protocol::objects::VerifiedObject<StoreDeviceRegistration>, StoreObjectError>
    {
        self.history.load_registration(reference).await
    }

    pub(crate) async fn load_commit(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<VerifiedStoreBatchCommit, StorePullError> {
        self.history.load_ref(reference).await
    }

    pub(crate) async fn load_acknowledgement(
        &self,
        reference: &StoreAckRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<coven_protocol::objects::VerifiedObject<StoreAck>, StoreObjectError> {
        self.history.load_store_ack(reference, registration).await
    }

    pub(crate) async fn load_verified_attempt(
        &mut self,
        reference: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<coven_protocol::objects::VerifiedObject<DeviceJoinAttempt>, StorePullError> {
        self.history
            .load_verified_device_join_attempt(reference, owner)
            .await
    }

    pub(crate) async fn load_verified_attempt_and_owner(
        &mut self,
        reference: &DeviceJoinAttemptRef,
    ) -> Result<
        (
            coven_protocol::objects::VerifiedObject<DeviceJoinAttempt>,
            coven_protocol::objects::VerifiedObject<StoreDeviceRegistration>,
        ),
        StorePullError,
    > {
        self.history
            .load_verified_device_join_attempt_and_owner(reference)
            .await
    }

    pub(crate) async fn load_attempt_and_owner(
        &self,
        reference: &DeviceJoinAttemptRef,
    ) -> Result<
        (
            coven_protocol::objects::VerifiedObject<DeviceJoinAttempt>,
            coven_protocol::objects::VerifiedObject<StoreDeviceRegistration>,
        ),
        StoreObjectError,
    > {
        self.history
            .load_device_join_attempt_and_owner(reference)
            .await
    }

    pub(crate) async fn load_outcome(
        &self,
        reference: &DeviceJoinOutcomeRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<coven_protocol::objects::VerifiedObject<DeviceJoinOutcome>, StoreObjectError> {
        self.history
            .load_device_join_outcome(reference, owner)
            .await
    }

    pub(crate) async fn verify_accepted_provider_access_activation(
        &mut self,
        access: &coven_protocol::provider::ActivatedStoreMemberProviderAccessGrant,
        provider_admin: &coven_protocol::provider::ProviderAdminGrantRecord,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), StorePullError> {
        self.history
            .verify_accepted_provider_access_activation(access, provider_admin, administrator)
            .await
    }

    pub(crate) async fn history_cut_covers(
        &mut self,
        cut: &coven_protocol::store_commit::StoreHistoryCut,
        target: &StoreBatchCommitRef,
    ) -> Result<bool, StorePullError> {
        self.history.history_cut_covers(cut, target).await
    }

    pub(crate) async fn verify_attempt_and_prepare_bootstrap(
        &mut self,
        attempt: &DeviceJoinAttemptRef,
        attempt_owner: &StoreDeviceRegistration,
        attempt_activation: &StoreBatchCommitRef,
    ) -> Result<
        (
            coven_protocol::objects::VerifiedObject<DeviceJoinAttempt>,
            DeviceJoinBootstrapPlan,
        ),
        StorePullError,
    > {
        self.history
            .verify_attempt_and_prepare_device_join_bootstrap(
                attempt,
                attempt_owner,
                attempt_activation,
            )
            .await
    }

    pub(super) async fn verify_offer(
        &self,
        identity: &UserKeypair,
        offer: &DeviceJoinOffer,
    ) -> Result<(), DeviceJoinError> {
        verify_offer(self.storage, self.history, identity, offer).await
    }

    pub(super) async fn validate_store_owner(&self) -> Result<(), coven_database::DbError> {
        self.database
            .validated_store_owner(self.root().reference())
            .await
            .map(|_| ())
    }

    pub(super) fn sync_routing_hash(&self) -> ObjectHash {
        self.database.sync_routing_hash()
    }

    pub(super) async fn latest_local_registration(
        &self,
    ) -> Result<Option<coven_database::DurableDeviceRegistration>, coven_database::DbError> {
        self.database.latest_local_store_device_registration().await
    }

    pub(super) async fn completed_join(
        &self,
        attempt_id: DeviceJoinAttemptId,
    ) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinError> {
        self.database
            .load_device_join(attempt_id, DeviceJoinRole::Joiner)
            .await
            .map_err(DeviceJoinError::from)
    }

    pub(super) async fn complete_join(
        &self,
        pending: &super::joiner::PendingJoinJournal,
        current: &DeviceJoinJournalRecord,
        activated: &DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        pending
            .complete_on(&self.database, current, activated)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn bootstrap_pending_device(
        &self,
        identity: &UserKeypair,
        attempt_ref: DeviceJoinAttemptRef,
        verified_attempt: coven_protocol::objects::VerifiedObject<DeviceJoinAttempt>,
        bootstrap_plan: super::DeviceJoinBootstrapPlan,
        attempt_activation: StoreBatchCommitRef,
        owner: &StoreDeviceRegistration,
        published_at: &str,
    ) -> Result<DeviceReadinessProof, StoreRegistrationError> {
        if verified_attempt.semantic_hash != attempt_ref.attempt_hash
            || verified_attempt.object != attempt_ref.object
        {
            return Err(StoreRegistrationError::Invalid(
                "verified device join attempt differs from its exact reference".to_string(),
            ));
        }
        let database = &self.database;
        let storage = self.storage;
        let attempt = verified_attempt.value;
        let activation_stream = attempt_activation.coord.stream_id.to_string();
        let verified_activation = bootstrap_plan
            .verified_commit(&attempt_activation)
            .cloned()
            .ok_or_else(|| {
                StoreRegistrationError::Invalid(
                    "device join bootstrap omits its attempt activation".to_string(),
                )
            })?;
        Box::pin(
            database.install_device_join_bootstrap(attempt.store_root.clone(), bootstrap_plan),
        )
        .await
        .map_err(registration_database_error)?;
        if Box::pin(
            database
                .exact_materialized_ref(&activation_stream, attempt_activation.coord.sequence()),
        )
        .await
        .map_err(registration_database_error)?
        .as_ref()
            != Some(&attempt_activation)
        {
            return Err(StoreRegistrationError::ActivationRequired);
        }
        let activation_commit = verified_activation.value();
        if verified_activation.author() != owner
            || activation_commit.author_registration != attempt.owner_registration
            || !activation_commit
                .device_join_attempt_decisions()
                .iter()
                .any(|decision| {
                    matches!(
                        decision,
                        DeviceJoinAttemptDecisionRef::Attempt(reference)
                            if reference == &attempt_ref
                    )
                })
            || activation_commit
                .order
                .predecessor_cut()
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                != attempt.bootstrap_cut
            || activation_commit.membership_state != attempt.membership
        {
            return Err(StoreRegistrationError::Invalid(
                "device join attempt is not activated by the named exact Store commit".to_string(),
            ));
        }
        let provider = Box::pin(storage.provider_binding())
            .await
            .map_err(StoreObjectError::from)?;
        if provider.device != attempt.expected_registration.provider {
            return Err(StoreRegistrationError::Invalid(
                "joiner provider principal differs from the signed device join attempt".to_string(),
            ));
        }
        let expected_registration = attempt.expected_registration.clone();
        if expected_registration.author_pubkey != coven_keys::keys::public_key_hex(identity) {
            return Err(StoreRegistrationError::Invalid(
                "joiner identity differs from the signed device registration request".to_string(),
            ));
        }
        let existing = Box::pin(database.latest_local_store_device_registration())
            .await
            .map_err(registration_database_error)?;
        if let Some(existing) = existing.as_ref() {
            if existing.registration_bytes != expected_registration.to_bytes()
                || existing.prepared.reference().slot() != &attempt.registration_slot
                || existing.initial_ack.value.store_cut != attempt.bootstrap_cut
            {
                return Err(StoreRegistrationError::Invalid(
                    "local join journal owns different exact registration bytes".to_string(),
                ));
            }
        } else {
            let registration_prepared = prepare_registration_object(
                storage,
                &expected_registration,
                attempt.registration_slot.clone(),
            )?;
            let registration_ref = StoreDeviceRegistrationRef::from_registration(
                &expected_registration,
                registration_prepared.reference().clone(),
            );
            let DeviceStreamAnchor::StoreAcknowledgements { first_slot } =
                &expected_registration.acknowledgements
            else {
                return Err(StoreRegistrationError::Invalid(
                    "join registration has no acknowledgement anchor".to_string(),
                ));
            };
            let ack_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                attempt.store_root.store_root_hash,
                ProtocolObjectDomain::StoreAck,
            );
            let next_slot = Box::pin(storage.allocate_protocol_slot(
                &ack_context,
                &ack_slot_prefix(&expected_registration.device_id.to_string(), 2),
                ".json",
            ))
            .await
            .map_err(StoreObjectError::from)?;
            let device_signer = expected_registration
                .device_signer(identity)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let (device_state, _) =
                Box::pin(database.store_device_state_for_history_cut(&attempt.bootstrap_cut))
                    .await
                    .map_err(registration_database_error)?;
            let initial_ack = StoreAck::signed(
                attempt.store_root.store_root_hash,
                registration_ref.clone(),
                1,
                attempt.bootstrap_cut.clone(),
                device_state,
                None,
                StoreAckExclusionState {
                    proposal_freezes: Vec::new(),
                },
                published_at.to_string(),
                SuccessorLink {
                    activation: expected_registration
                        .store_acknowledgement_activation(&registration_ref)
                        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                        .activation_id(),
                    predecessor: None,
                    next_slot,
                },
                &device_signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let ack_prepared = storage
                .prepare_protocol_object(
                    &ack_context,
                    first_slot.clone(),
                    &ack_slot_prefix(&expected_registration.device_id.to_string(), 1),
                    initial_ack.to_bytes(),
                )
                .map_err(StoreObjectError::from)?;
            let initial_ack_ref = StoreAckRef {
                registration: registration_ref,
                sequence: 1,
                ack_hash: initial_ack.ack_hash(),
                object: ack_prepared.reference().clone(),
            };
            Box::pin(database.stage_local_store_device_registration(
                coven_database::ExactProtocolObject {
                    value: expected_registration.clone(),
                    bytes: expected_registration.to_bytes(),
                    prepared: registration_prepared,
                },
                initial_ack_ref,
                coven_database::ExactProtocolObject {
                    value: initial_ack.clone(),
                    bytes: initial_ack.to_bytes(),
                    prepared: ack_prepared,
                },
            ))
            .await
            .map_err(registration_database_error)?;
        }
        super::RegistrationOutbox::new(database.clone(), storage)
            .drain()
            .await?;
        let durable = Box::pin(database.latest_local_store_device_registration())
            .await
            .map_err(registration_database_error)?
            .ok_or(StoreRegistrationError::ActivationRequired)?;
        if !matches!(
            durable.state,
            coven_database::LocalDeviceRegistrationState::Created
                | coven_database::LocalDeviceRegistrationState::Activated { .. }
        ) {
            return Err(StoreRegistrationError::ActivationRequired);
        }
        let registration = StoreDeviceRegistration::parse_at(
            &durable.registration_bytes,
            &attempt.store_root,
            durable.device_id,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration,
            durable.prepared.reference().clone(),
        );
        let device_signer = registration
            .device_signer(identity)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        DeviceReadinessProof::signed(
            attempt_ref,
            registration_ref,
            durable.initial_ack_ref,
            attempt.bootstrap_cut.clone(),
            &registration,
            &device_signer,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))
    }

    pub(super) async fn create_cross_principal_response(
        &self,
        challenge: &CrossPrincipalProbeChallenge,
        context: &coven_protocol::provider::CrossPrincipalResponseContext,
        store: &StoreProviderBinding,
        administrator_signing_pubkey: &str,
        identity: &UserKeypair,
    ) -> Result<CrossPrincipalProbeResponse, coven_protocol::provider::ProviderProbeError> {
        self.storage
            .provider_probes()
            .create_cross_principal_response(
                challenge,
                context,
                store,
                administrator_signing_pubkey,
                identity,
            )
            .await
    }
}

fn registration_database_error(error: coven_database::DbError) -> StoreRegistrationError {
    StoreRegistrationError::Database(error.to_string())
}

/// An offer is this device's offer only when it names this member, the provider
/// principal this device is bound to, the provider the Store's own root
/// declares, and that exact root — and carries the owner's signature over all
/// of it. Both the pre-Store observation and the joining Store check it here.
pub(super) async fn verify_offer(
    storage: &dyn coven_storage::SyncStorage,
    history: &super::MergeHistoryVerifier<'_>,
    identity: &UserKeypair,
    offer: &DeviceJoinOffer,
) -> Result<(), DeviceJoinError> {
    let root = history.verified_root();
    if coven_keys::keys::public_key_hex(identity) != offer.member_pubkey
        || storage.provider_binding().await?.store != offer.provider
        || root.protocol().descriptor.provider != offer.provider
        || root.reference() != &offer.store_root
    {
        return Err(DeviceJoinError::OfferMismatch);
    }
    let owner = history
        .load_registration(&offer.owner_registration)
        .await?
        .value;
    offer.verify(&owner).map_err(DeviceJoinError::from)
}
