use super::*;
use crate::sync::store::pull::StorePullError;
use crate::sync::store::StoreRegistrationError;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::store_commit::VerifiedStoreBatchCommit;
use coven_protocol::store_commit::{
    ack_slot_prefix, DeviceStreamAnchor, StoreAck, StoreAckExclusionState, StoreAckRef,
    SuccessorLink,
};

pub(crate) struct DeviceJoinHistory<'operation, 'storage> {
    database: StoreDatabase,
    storage: &'storage dyn coven_storage::CloudSyncObjectStorage,
    history: &'operation mut super::MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> DeviceJoinHistory<'operation, 'storage> {
    pub(crate) fn new(
        database: StoreDatabase,
        storage: &'storage dyn coven_storage::CloudSyncObjectStorage,
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
    ) -> Result<StoreAck, StoreObjectError> {
        self.history.load_store_ack(reference, registration).await
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
        attempt_id: DeviceJoinAttemptId,
        attempt_activation: &StoreBatchCommitRef,
    ) -> Result<
        (
            coven_protocol::store_commit::StoreHistoryCut,
            DeviceJoinBootstrapPlan,
        ),
        StorePullError,
    > {
        // The joining device installs its Store snapshot image before it asks
        // for this plan, so the history it already holds is that image's
        // coverage. Reading it here is what stops the closure at the snapshot
        // rather than at genesis.
        let installed = self.database.snapshot_coverage_frontier().await?;
        self.history
            .verify_attempt_and_prepare_device_join_bootstrap(
                attempt_id,
                attempt_activation,
                &installed,
            )
            .await
    }

    pub(crate) async fn prepare_same_principal_installation(
        &mut self,
        attempt_activation: &StoreBatchCommitRef,
    ) -> Result<SamePrincipalStoreInstallation, DeviceJoinError> {
        let root = self.root().reference().clone();
        // The activation commit is the attempt: its predecessor cut is the
        // history the joining device installs from, and its membership state is
        // the authority that cut is read under.
        let activation = self.load_commit(attempt_activation).await?;
        let bootstrap_cut = activation.value().order.predecessor_cut()?;
        let membership_state = activation.value().membership_state.clone();
        let mut timings = coven_foundation::stage_timing::StageTimings::counting(
            "Same-provider join installation plan",
            self.storage.provider_requests(),
        );
        let snapshots = self.database.local_store_snapshots().await?;
        if snapshots.is_empty() {
            return Err(DeviceJoinError::Store(
                "same-provider device join requires a published Store snapshot".to_string(),
            ));
        }
        // The joining device installs this image and then materializes the
        // bootstrap plan on top of it, so the plan's cut has to reach at least
        // as far as the image does. A snapshot published past the attempt's cut
        // would hand the joiner rows its plan then disagrees with, which the
        // installation refuses — so it is never offered in the first place.
        let bootstrap_frontier = bootstrap_cut.frontier();
        let candidates = snapshots
            .into_iter()
            .filter(|snapshot| bootstrap_frontier.covers(&snapshot.meta.coverage))
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(DeviceJoinError::Store(
                "same-provider device join has no published Store snapshot within its bootstrap cut"
                    .to_string(),
            ));
        }
        let selected = timings
            .stage(
                "select the snapshot",
                self.history
                    .select_maximal_installable_store_snapshot(candidates),
            )
            .await
            .map_err(|error| StorePullError::context("verify same-provider join snapshot", error))?
            .ok_or_else(|| {
                DeviceJoinError::Store(
                    "same-provider device join has no installable Store snapshot".to_string(),
                )
            })?;
        let snapshot = selected.snapshot;
        let authority = selected.verified;
        // The joining device installs this snapshot's image before it applies
        // the plan, so the plan starts where that image ends. The candidate
        // filter above already established the bootstrap cut reaches past the
        // snapshot's coverage, so the trimmed closure still lands on the cut
        // the attempt's activation commit names.
        let plan = timings
            .stage(
                "walk the plan history",
                self.history.prepare_device_join_bootstrap(
                    &bootstrap_cut,
                    attempt_activation,
                    &membership_state,
                    &snapshot.meta.coverage,
                ),
            )
            .await
            .map_err(|error| {
                StorePullError::context("prepare same-provider join history", error)
            })?;
        let bootstrap = timings.mark("carry the plan closure", || plan.into_closure(&root))?;
        timings.report();
        Ok(SamePrincipalStoreInstallation {
            store_root: self.root().protocol().clone(),
            snapshot: snapshot.reference,
            metadata: snapshot.meta,
            authority: authority.into_authority(),
            bootstrap,
        })
    }

    pub(crate) async fn retain_same_principal_join_activation(
        &mut self,
        activation: &StoreBatchCommitRef,
    ) -> Result<(), DeviceJoinError> {
        let materialization = self
            .database
            .retained_merge_materialization(self.root().reference().clone(), activation.clone())
            .await?;
        self.history
            .retain_local_same_principal_join_activation(materialization)
            .await?;
        Ok(())
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

    pub(super) fn retire_join(
        &self,
        pending: &super::joiner::PendingJoinJournal,
        current: &DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        pending.retire(current)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn bootstrap_pending_device(
        &self,
        identity: &UserKeypair,
        attempt_id: DeviceJoinAttemptId,
        request: &DeviceRegistrationRequest,
        bootstrap: coven_database::ResolvedDeviceJoinBootstrap,
        attempt_activation: StoreBatchCommitRef,
        owner: &StoreDeviceRegistration,
        published_at: &str,
        timings: &mut coven_foundation::stage_timing::StageTimings,
    ) -> Result<DeviceReadinessProof, StoreRegistrationError> {
        bootstrap_pending_device_on(
            &self.database,
            self.storage,
            identity,
            attempt_id,
            request,
            bootstrap,
            attempt_activation,
            owner,
            published_at,
            timings,
        )
        .await
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn bootstrap_pending_device_on(
    database: &StoreDatabase,
    storage: &dyn coven_storage::CloudSyncObjectStorage,
    identity: &UserKeypair,
    attempt_id: DeviceJoinAttemptId,
    request: &DeviceRegistrationRequest,
    bootstrap: coven_database::ResolvedDeviceJoinBootstrap,
    attempt_activation: StoreBatchCommitRef,
    owner: &StoreDeviceRegistration,
    published_at: &str,
    timings: &mut coven_foundation::stage_timing::StageTimings,
) -> Result<DeviceReadinessProof, StoreRegistrationError> {
    // Everything this step needs about the attempt is in the request this
    // device signed and the commit that activated it. There is no separate
    // attempt file to agree with.
    let offer = &request.approval().request.offer;
    let expected_registration = request.expected_registration().clone();
    let registration_slot = request.registration_slot().clone();
    let store_root = offer.store_root.clone();
    let owner_registration = offer.owner_registration.clone();
    let activation_stream = attempt_activation.coord.stream_id.to_string();
    let verified_activation = bootstrap
        .plan
        .verified_commit(&attempt_activation)
        .cloned()
        .ok_or_else(|| {
            StoreRegistrationError::Invalid(
                "device join bootstrap omits its attempt activation".to_string(),
            )
        })?;
    let installed_registration_activation = bootstrap
        .plan
        .commits
        .iter()
        .find(|commit| commit.reference == attempt_activation)
        .and_then(|commit| {
            commit.registrations.iter().find(|registration| {
                registration.value().device_id == expected_registration.device_id
            })
        })
        .map(|registration| registration.activation().clone());
    timings
        .stage(
            "materialize the carried history",
            Box::pin(database.install_device_join_bootstrap(store_root.clone(), bootstrap)),
        )
        .await
        .map_err(registration_database_error)?;
    if Box::pin(
        database.exact_materialized_ref(&activation_stream, attempt_activation.coord.sequence()),
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
        || activation_commit.author_registration != owner_registration
        || !activation_commit
            .device_join_attempt_decisions()
            .iter()
            .any(|decision| {
                matches!(
                    decision,
                    DeviceJoinAttemptDecisionRef::Attempt(opened) if *opened == attempt_id
                )
            })
    {
        return Err(StoreRegistrationError::Invalid(
            "device join attempt is not activated by the named exact Store commit".to_string(),
        ));
    }
    // The commit that opened the attempt declared the history this device
    // installs from; the readiness proof below commits to the same cut.
    let bootstrap_cut = activation_commit
        .order
        .predecessor_cut()
        .map_err(StoreRegistrationError::from)?;
    let provider = timings
        .stage(
            "read the provider binding",
            Box::pin(storage.provider_binding()),
        )
        .await
        .map_err(StoreObjectError::from)?;
    if provider.device != expected_registration.provider {
        return Err(StoreRegistrationError::Invalid(
            "joiner provider principal differs from the signed device join attempt".to_string(),
        ));
    }
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
            || existing.prepared.reference().slot() != &registration_slot
            || existing.initial_ack.value.store_cut != bootstrap_cut
        {
            return Err(StoreRegistrationError::Invalid(
                "local join journal owns different exact registration bytes".to_string(),
            ));
        }
    } else {
        let registration_prepared = prepare_registration_object(
            storage,
            &expected_registration,
            registration_slot.clone(),
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
            store_root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let next_slot = timings
            .stage(
                "allocate the acknowledgement slot",
                Box::pin(storage.allocate_protocol_slot(
                    &ack_context,
                    &ack_slot_prefix(&expected_registration.device_id.to_string(), 2),
                    ".json",
                )),
            )
            .await
            .map_err(StoreObjectError::from)?;
        let device_signer = expected_registration
            .device_signer(identity)
            .map_err(StoreRegistrationError::from)?;
        let (device_state, _) = timings
            .stage(
                "resolve the device state",
                Box::pin(database.store_device_state_for_history_cut(&bootstrap_cut)),
            )
            .await
            .map_err(registration_database_error)?;
        let initial_ack = StoreAck::signed(
            store_root.store_root_hash,
            1,
            coven_protocol::store_commit::StoreAckAssertion {
                registration: registration_ref.clone(),
                store_cut: bootstrap_cut.clone(),
                device_state,
                snapshot: None,
                exclusions: StoreAckExclusionState {
                    proposal_freezes: Vec::new(),
                },
            },
            published_at.to_string(),
            SuccessorLink {
                activation: expected_registration
                    .store_acknowledgement_activation(&registration_ref)
                    .map_err(StoreRegistrationError::from)?
                    .activation_id(),
                predecessor: None,
                next_slot,
            },
            &device_signer,
        )
        .map_err(StoreRegistrationError::from)?;
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
        let exact_registration = coven_database::ExactProtocolObject {
            value: expected_registration.clone(),
            bytes: expected_registration.to_bytes(),
            prepared: registration_prepared,
        };
        let exact_ack = coven_database::ExactProtocolObject {
            value: initial_ack.clone(),
            bytes: initial_ack.to_bytes(),
            prepared: ack_prepared,
        };
        match installed_registration_activation {
            Some(authority) => {
                Box::pin(database.stage_activated_local_store_device_registration(
                    exact_registration,
                    initial_ack_ref,
                    exact_ack,
                    authority,
                ))
                .await
                .map_err(registration_database_error)?;
            }
            None => {
                Box::pin(database.stage_local_store_device_registration(
                    exact_registration,
                    initial_ack_ref,
                    exact_ack,
                ))
                .await
                .map_err(registration_database_error)?;
            }
        }
    }
    timings
        .stage(
            "publish the registration",
            super::RegistrationOutbox::new(database.clone(), storage).drain(),
        )
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
        &store_root,
        durable.device_id,
    )
    .map_err(StoreRegistrationError::from)?;
    let registration_ref = StoreDeviceRegistrationRef::from_registration(
        &registration,
        durable.prepared.reference().clone(),
    );
    let device_signer = registration
        .device_signer(identity)
        .map_err(StoreRegistrationError::from)?;
    DeviceReadinessProof::signed(
        attempt_id,
        registration_ref,
        durable.initial_ack_ref,
        bootstrap_cut.clone(),
        &registration,
        &device_signer,
    )
    .map_err(StoreRegistrationError::from)
}

fn registration_database_error(error: coven_database::DbError) -> StoreRegistrationError {
    StoreRegistrationError::from(error)
}

/// An offer is this device's offer only when it names this member, the provider
/// principal this device is bound to, the provider the Store's own root
/// declares, and that exact root — and carries the owner's signature over all
/// of it. Both the pre-Store observation and the joining Store check it here.
pub(super) async fn verify_offer(
    storage: &dyn coven_storage::CloudSyncObjectStorage,
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
