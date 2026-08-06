use super::journal::{database_error, provider_error};
use super::*;
use crate::protocol::store_commit::device_join_exchange::require_cancelled_outcome;

#[doc(hidden)]
pub(crate) struct PendingDeviceJoinAuthority<'storage> {
    observation: PendingDeviceJoinObservation<'storage>,
    offer: DeviceJoinOffer,
    identity: UserKeypair,
}

#[doc(hidden)]
pub(crate) struct PendingDeviceJoinObservation<'storage> {
    journal: PendingJoinJournal,
    storage: &'storage std::sync::Arc<dyn SyncStorage>,
    history_verifier: crate::sync::store::owner::verified_history::MergeHistoryVerifier<'storage>,
}

#[doc(hidden)]
pub(crate) struct PendingDeviceJoinClosure<'storage> {
    observation: PendingDeviceJoinObservation<'storage>,
    identity: UserKeypair,
}

#[doc(hidden)]
pub(crate) struct JoiningStore<'storage> {
    journal: PendingJoinJournal,
    history: super::AuthorizedStoreHistory<'storage>,
    membership: crate::protocol::membership::MembershipChain,
    identity: UserKeypair,
}

#[derive(Clone)]
pub(super) struct PendingJoinJournal {
    database: DeviceJoinJournalDatabase,
    attempt_id: DeviceJoinAttemptId,
}

impl PendingJoinJournal {
    fn new(database: &DeviceJoinJournalDatabase, attempt_id: DeviceJoinAttemptId) -> Self {
        Self {
            database: database.clone(),
            attempt_id,
        }
    }

    fn load(&self) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinError> {
        self.database.load(self.attempt_id, DeviceJoinRole::Joiner)
    }

    fn record(&self, progress: JoinerJoinProgress) -> DeviceJoinJournalRecord {
        DeviceJoinJournalRecord {
            attempt_id: self.attempt_id,
            progress: Box::new(progress.into()),
        }
    }

    /// Advance from `previous`, returning the record now durable so the next step
    /// advances from it.
    fn advance(
        &self,
        previous: &DeviceJoinJournalRecord,
        progress: JoinerJoinProgress,
    ) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
        let next = self.record(progress);
        self.advance_to(previous, &next)?;
        Ok(next)
    }

    fn advance_to(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: &DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        self.database.advance(previous, next.clone())
    }

    fn accept_offer(&self, offer: &DeviceJoinOffer) -> Result<(), DeviceJoinError> {
        if offer.attempt_id != self.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let initial = self.record(JoinerJoinProgress::OfferReceived(offer.clone()));
        match self.load()? {
            None => {
                if self.database.begin(initial.clone())? != initial {
                    return Err(DeviceJoinError::JournalConflict);
                }
            }
            Some(record) => match &*record.progress {
                DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(durable))
                    if durable != offer =>
                {
                    return Err(DeviceJoinError::JournalConflict);
                }
                DeviceJoinRoleProgress::Joiner(progress)
                    if progress_offer(progress).is_some_and(|durable| durable != offer) =>
                {
                    return Err(DeviceJoinError::JournalConflict);
                }
                _ => {}
            },
        }
        Ok(())
    }

    fn observe_activation_if_pending(
        &self,
        activation: &DeviceJoinActivation,
    ) -> Result<Option<DeviceJoinReadiness>, DeviceJoinError> {
        if activation.outcome.attempt().attempt_id != self.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        self.database
            .observe_joiner_activation_if_pending(activation)
    }

    fn advance_cleanup_from_replacement(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        self.database
            .advance_joiner_cleanup_from_replacement(previous, next)
    }

    pub(super) async fn complete_on(
        &self,
        database: &StoreDatabase,
        current: &DeviceJoinJournalRecord,
        activated: &DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        self.database
            .complete_into(database, current, activated)
            .await
    }

    fn record_readiness(
        &self,
        bootstrap: ProviderReadyDeviceBootstrap,
        readiness: DeviceJoinReadiness,
    ) -> Result<DeviceJoinReadiness, DeviceJoinError> {
        let offer = &bootstrap.bootstrap.request.approval.request.offer;
        if offer.attempt_id != self.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let current = self.load()?.ok_or(DeviceJoinError::JournalConflict)?;
        let prepared =
            match &*current.progress {
                DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(
                    request,
                )) if request == &*bootstrap.bootstrap.request => current,
                DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(existing))
                    if existing == &readiness =>
                {
                    return Ok(readiness)
                }
                _ => return Err(DeviceJoinError::JournalConflict),
            };
        self.advance(&prepared, JoinerJoinProgress::Ready(readiness.clone()))?;
        Ok(readiness)
    }
}

fn progress_offer(progress: &JoinerJoinProgress) -> Option<&DeviceJoinOffer> {
    match progress {
        JoinerJoinProgress::OfferReceived(offer) => Some(offer),
        JoinerJoinProgress::AccessRequested(request) => Some(&request.offer),
        JoinerJoinProgress::ApprovalReceived(approval) => Some(&approval.request.offer),
        JoinerJoinProgress::RegistrationPrepared(request) => Some(&request.approval.request.offer),
        _ => None,
    }
}

impl<'storage> JoiningStore<'storage> {
    pub(crate) async fn begin_from_restored_history(
        mut history: super::AuthorizedStoreHistory<'storage>,
        identity: UserKeypair,
        pending: &DeviceJoinJournalDatabase,
        offer: DeviceJoinOffer,
    ) -> Result<Self, DeviceJoinError> {
        history
            .device_join()
            .verify_offer(&identity, &offer)
            .await?;
        let journal = PendingJoinJournal::new(pending, offer.attempt_id);
        journal.accept_offer(&offer)?;
        let founder_pubkey = history
            .verified_root_object()
            .value
            .descriptor
            .founder_pubkey
            .clone();
        let membership = history
            .load_and_install_owner_membership(&founder_pubkey)
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        history
            .device_join()
            .validate_store_owner()
            .await
            .map_err(database_error)?;
        Ok(Self {
            journal,
            history,
            membership,
            identity,
        })
    }

    pub(crate) async fn materialize(
        &mut self,
        activation: DeviceJoinActivation,
    ) -> Result<JoinedStore, DeviceJoinError> {
        if !matches!(&activation.outcome, DeviceJoinOutcomeRef::Activated { .. }) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let attempt_ref = activation.outcome.attempt().clone();
        let root = self.history.root().clone();
        let (attempt, owner) = self
            .history
            .merge_history()
            .load_verified_device_join_attempt_and_owner(&attempt_ref)
            .await?;
        self.history
            .materialize_device_join_activation(
                &activation.outcome_activation,
                &activation.outcome,
                &attempt.value.membership,
            )
            .await?;
        let outcome = self
            .history
            .merge_history()
            .load_device_join_outcome(&activation.outcome, &owner.value)
            .await?
            .value;
        let crate::protocol::store_commit::DeviceJoinDisposition::Activated { readiness } =
            outcome.disposition.clone()
        else {
            return Err(DeviceJoinError::AttemptMismatch);
        };
        let local = self
            .history
            .device_join()
            .latest_local_registration()
            .await
            .map_err(database_error)?
            .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
        if !local.is_activated()
            || local.registration_hash != readiness.registration.registration_hash
            || local.device_id != readiness.registration.device_id
            || attempt.value.expected_registration.to_bytes() != local.registration_bytes
        {
            return Err(DeviceJoinError::ActivationNotMaterialized);
        }
        Ok(JoinedStore {
            store_root: root,
            registration: readiness.registration.clone(),
            activation,
        })
    }

    pub(crate) async fn pull_store_history(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<crate::sync::store::StorePullResult, DeviceJoinError> {
        let membership = self.membership.clone();
        let execution = self
            .history
            .pull(&membership, Some(&self.identity), routing_encryption)
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        self.membership = execution.membership;
        Ok(execution.result)
    }

    pub(crate) async fn bootstrap(
        &mut self,
        bootstrap: ProviderReadyDeviceBootstrap,
        published_at: &str,
    ) -> Result<DeviceJoinReadiness, DeviceJoinError> {
        let offer = &bootstrap.bootstrap.request.approval.request.offer;
        if &offer.store_root != self.history.root()
            || offer.member_pubkey != coven_keys::keys::public_key_hex(&self.identity)
            || self.history.device_join().sync_routing_hash()
                != self
                    .history
                    .verified_root_object()
                    .value
                    .descriptor
                    .sync_routing_hash
        {
            return Err(DeviceJoinError::OfferMismatch);
        }
        let attempt_owner = self
            .history
            .merge_history()
            .load_registration(&offer.owner_registration)
            .await?
            .value;
        let administrator = self
            .history
            .merge_history()
            .load_registration(&offer.provider_admin.administrator)
            .await?
            .value;
        let (verified_attempt, bootstrap_plan) = Box::pin(
            self.history
                .merge_history()
                .verify_attempt_and_prepare_device_join_bootstrap(
                    &bootstrap.bootstrap.publication_authorization.attempt,
                    &attempt_owner,
                    &bootstrap
                        .bootstrap
                        .publication_authorization
                        .attempt_activation,
                ),
        )
        .await?;
        if verified_attempt.value.expected_registration
            != bootstrap.bootstrap.request.expected_registration
        {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let proof = Box::pin(
            self.history.device_join().bootstrap_pending_device(
                &self.identity,
                bootstrap
                    .bootstrap
                    .publication_authorization
                    .attempt
                    .clone(),
                verified_attempt,
                bootstrap_plan,
                bootstrap
                    .bootstrap
                    .publication_authorization
                    .attempt_activation
                    .clone(),
                &attempt_owner,
                published_at,
            ),
        )
        .await?;
        let provider = match (
            &bootstrap.bootstrap.request.approval.admission,
            &bootstrap.bootstrap.request.response,
            &bootstrap.challenge_publication,
        ) {
            (
                DeviceProviderAdmissionChallenge::SamePrincipal,
                DeviceProviderResponseReservation::SamePrincipal,
                DeviceProviderChallengePublication::SamePrincipal,
            ) => DeviceProviderReadiness::SamePrincipal,
            (
                DeviceProviderAdmissionChallenge::CrossPrincipal(challenge),
                DeviceProviderResponseReservation::CrossPrincipal { response_slot },
                DeviceProviderChallengePublication::CrossPrincipal {
                    challenge: published,
                },
            ) if challenge == published => {
                let context = crate::protocol::provider::CrossPrincipalResponseContext {
                    challenge: bootstrap
                        .bootstrap
                        .request
                        .approval
                        .request
                        .cross_challenge_context(),
                    expected_registration_hash: bootstrap
                        .bootstrap
                        .request
                        .expected_registration
                        .registration_hash(),
                    response_slot: response_slot.clone(),
                };
                DeviceProviderReadiness::CrossPrincipal(
                    Box::pin(self.history.device_join().create_cross_principal_response(
                        challenge,
                        &context,
                        &offer.provider,
                        &administrator.device_signing_pubkey,
                        &self.identity,
                    ))
                    .await
                    .map_err(provider_error)?,
                )
            }
            _ => return Err(DeviceJoinError::AttemptMismatch),
        };
        let readiness = DeviceJoinReadiness { proof, provider };
        let journal = self.journal.clone();
        let bootstrap = Box::new(bootstrap);
        let readiness = Box::new(readiness);
        tokio::task::spawn_blocking(move || journal.record_readiness(*bootstrap, *readiness))
            .await
            .map_err(|error| {
                DeviceJoinError::Store(format!("joiner readiness journal task failed: {error}"))
            })?
    }

    pub(crate) async fn complete(
        &mut self,
        activation: DeviceJoinActivation,
    ) -> Result<JoinedStore, DeviceJoinError> {
        let attempt_id = activation.outcome.attempt().attempt_id;
        if let Some(record) = self
            .history
            .device_join()
            .completed_join(attempt_id)
            .await?
        {
            let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Activated(existing)) =
                &*record.progress
            else {
                return Err(DeviceJoinError::JournalConflict);
            };
            let joined = self.materialize(activation).await?;
            return (existing == &joined)
                .then_some(joined)
                .ok_or(DeviceJoinError::JournalConflict);
        }
        let current_readiness = self
            .journal
            .observe_activation_if_pending(&activation)?
            .ok_or(DeviceJoinError::JournalConflict)?;
        let joined = self.materialize(activation).await?;
        if current_readiness.proof.registration != joined.registration {
            return Err(DeviceJoinError::JournalConflict);
        }
        let current = self
            .journal
            .load()?
            .ok_or(DeviceJoinError::JournalConflict)?;
        let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ActivationObserved {
            readiness,
            activation: current_activation,
        }) = &*current.progress
        else {
            return Err(DeviceJoinError::JournalConflict);
        };
        if readiness != &current_readiness || current_activation != &joined.activation {
            return Err(DeviceJoinError::JournalConflict);
        }
        let activated_record = self
            .journal
            .record(JoinerJoinProgress::Activated(joined.clone()));
        self.history
            .device_join()
            .complete_join(&self.journal, &current, &activated_record)
            .await?;
        Ok(joined)
    }
}

impl<'storage> PendingDeviceJoinAuthority<'storage> {
    pub(crate) async fn open(
        observation: PendingDeviceJoinObservation<'storage>,
        identity: &UserKeypair,
        offer: DeviceJoinOffer,
    ) -> Result<Self, DeviceJoinError> {
        observation.verify_offer(identity, &offer).await?;
        observation.journal.accept_offer(&offer)?;
        Ok(Self {
            observation,
            offer,
            identity: identity.clone(),
        })
    }

    pub(crate) async fn prepare_provider_access_request(
        &self,
    ) -> Result<DeviceProviderAccessRequest, DeviceJoinError> {
        self.observation
            .prepare_provider_access_request(&self.offer, &self.identity)
            .await
    }

    pub(crate) async fn prepare_registration_request(
        &mut self,
        approval: DeviceProviderAdmissionApproval,
    ) -> Result<DeviceRegistrationRequest, DeviceJoinError> {
        self.observation
            .prepare_registration_request(&self.offer, &self.identity, approval)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn begin_joining_store(
        self,
        database: StoreDatabase,
        store_dir: &'storage coven_foundation::store_dir::StoreDir,
    ) -> Result<JoiningStore<'storage>, DeviceJoinError> {
        self.observation
            .into_joining_store(database, store_dir, self.identity)
            .await
    }
}

impl<'storage> PendingDeviceJoinObservation<'storage> {
    pub(crate) async fn open(
        pending: &DeviceJoinJournalDatabase,
        storage: &'storage std::sync::Arc<dyn SyncStorage>,
        root: &crate::protocol::store_commit::StoreRootRef,
        attempt_id: DeviceJoinAttemptId,
    ) -> Result<Self, crate::sync::store::StorePullError> {
        let history_verifier =
            crate::sync::store::HistoryConstructionAuthority::for_pending_device_join()
                .open_pinned(storage.as_ref(), root)
                .await?;
        Ok(Self::new(pending, storage, history_verifier, attempt_id))
    }

    pub(crate) fn new(
        pending: &DeviceJoinJournalDatabase,
        storage: &'storage std::sync::Arc<dyn SyncStorage>,
        history_verifier: crate::sync::store::owner::verified_history::MergeHistoryVerifier<
            'storage,
        >,
        attempt_id: DeviceJoinAttemptId,
    ) -> Self {
        Self {
            journal: PendingJoinJournal::new(pending, attempt_id),
            storage,
            history_verifier,
        }
    }

    pub(crate) async fn into_joining_store(
        self,
        database: StoreDatabase,
        store_dir: &'storage coven_foundation::store_dir::StoreDir,
        identity: UserKeypair,
    ) -> Result<JoiningStore<'storage>, DeviceJoinError> {
        let Self {
            journal,
            storage,
            history_verifier,
        } = self;
        let root = history_verifier.verified_root().clone();
        let blob_source = crate::sync::store::blob::RemoteBlobSource::authorized(
            database.clone(),
            storage.as_ref(),
            root.reference().clone(),
        );
        let keyrings = super::StoreKeyrings::new(storage.as_ref(), root.reference().clone());
        let blob_cache =
            crate::sync::store::blob::StoreBlobCache::new(database.clone(), store_dir.clone());
        let mut history = super::AuthorizedStoreHistory::from_pending_device_join(
            PendingDeviceJoinHistoryConstruction,
            database,
            storage,
            store_dir,
            blob_cache,
            history_verifier,
            blob_source,
            keyrings,
        );
        let founder_pubkey = root.protocol().descriptor.founder_pubkey.clone();
        let membership = history
            .load_and_install_owner_membership(&founder_pubkey)
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        history
            .device_join()
            .validate_store_owner()
            .await
            .map_err(database_error)?;
        Ok(JoiningStore {
            journal,
            history,
            membership,
            identity,
        })
    }

    async fn verify_offer(
        &self,
        identity: &UserKeypair,
        offer: &DeviceJoinOffer,
    ) -> Result<(), DeviceJoinError> {
        super::history::verify_offer(
            self.storage.as_ref(),
            &self.history_verifier,
            identity,
            offer,
        )
        .await
    }

    pub(crate) fn authorize_closure(
        self,
        identity: &UserKeypair,
    ) -> PendingDeviceJoinClosure<'storage> {
        PendingDeviceJoinClosure {
            observation: self,
            identity: identity.clone(),
        }
    }

    pub(crate) async fn observe_abandonment(
        &mut self,
        abandonment: DeviceJoinAbandonment,
    ) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
        if abandonment.abandonment.attempt_id != self.journal.attempt_id {
            return Err(DeviceJoinError::JournalConflict);
        }
        let current = self
            .journal
            .load()?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if crate::database::device_join_journal::joiner_abandonment_transition(
            &current,
            &abandonment,
        )?
        .is_none()
        {
            return Ok(abandonment);
        }
        let context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.history_verifier
                .verified_root()
                .reference()
                .store_root_hash,
            ProtocolObjectDomain::DeviceJoinAbandonment,
        );
        let prefix = crate::protocol::store_commit::device_join_abandonment_semantic_prefix(
            self.journal.attempt_id,
        );
        let bytes = self
            .storage
            .read_protocol_object(&context, &abandonment.abandonment.object, &prefix)
            .await?;
        let object: DeviceJoinAbandonmentObject = serde_json::from_slice(&bytes)?;
        let owner = self
            .history_verifier
            .load_registration(&object.owner_registration)
            .await?
            .value;
        abandonment.abandonment.verify(&object, &owner)?;
        let activation = self
            .history_verifier
            .load_ref(&abandonment.abandonment_activation)
            .await?;
        if activation.author() != &owner
            || !activation
                .value()
                .device_join_attempt_decisions()
                .iter()
                .any(|decision| {
                    matches!(
                        decision,
                        DeviceJoinAttemptDecisionRef::Abandoned(reference)
                            if reference == &abandonment.abandonment
                    )
                })
        {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let next = crate::database::device_join_journal::joiner_abandonment_transition(
            &current,
            &abandonment,
        )?
        .ok_or(DeviceJoinError::JournalConflict)?;
        self.journal.advance_to(&current, &next)?;
        Ok(abandonment)
    }

    async fn prepare_provider_access_request(
        &self,
        offer: &DeviceJoinOffer,
        identity: &UserKeypair,
    ) -> Result<DeviceProviderAccessRequest, DeviceJoinError> {
        let record = self
            .journal
            .load()?
            .ok_or(DeviceJoinError::JournalConflict)?;
        match &*record.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request))
                if request.offer.as_ref() == offer =>
            {
                Ok(request.clone())
            }
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(durable))
                if durable == offer =>
            {
                let binding = self.storage.provider_binding().await?;
                if binding.store != offer.provider {
                    return Err(DeviceJoinError::OfferMismatch);
                }
                let request =
                    DeviceProviderAccessRequest::signed(offer.clone(), binding.device, identity)?;
                self.journal.advance(
                    &record,
                    JoinerJoinProgress::AccessRequested(request.clone()),
                )?;
                Ok(request)
            }
            _ => Err(DeviceJoinError::JournalConflict),
        }
    }

    async fn prepare_registration_request(
        &mut self,
        offer: &DeviceJoinOffer,
        identity: &UserKeypair,
        approval: DeviceProviderAdmissionApproval,
    ) -> Result<DeviceRegistrationRequest, DeviceJoinError> {
        if approval.request.offer.as_ref() != offer {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        let attempt_id = approval.request.offer.attempt_id;
        if let Some(record) = self.journal.load()? {
            if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(
                request,
            )) = *record.progress
            {
                if *request.approval == approval {
                    return Ok(request);
                }
                return Err(DeviceJoinError::JournalConflict);
            }
        }
        let owner = self
            .history_verifier
            .load_registration(&approval.request.offer.owner_registration)
            .await?
            .value;
        let administrator = self
            .history_verifier
            .load_registration(&approval.request.offer.provider_admin.administrator)
            .await?
            .value;
        approval.verify(
            self.history_verifier.verified_root().object(),
            &owner,
            &administrator,
        )?;
        self.history_verifier
            .verify_accepted_provider_access_activation(
                &approval.access_grant,
                &approval.request.offer.provider_admin,
                &administrator,
            )
            .await?;
        let storage = self.storage;
        let live = storage.provider_binding().await?;
        if live.store != approval.request.offer.provider
            || live.device != approval.request.peer_provider
        {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        let current = self
            .journal
            .load()?
            .ok_or(DeviceJoinError::JournalConflict)?;
        let access_request = match &*current.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request))
                if request == &*approval.request =>
            {
                request.clone()
            }
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ApprovalReceived(existing))
                if existing == &approval =>
            {
                *approval.request.clone()
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        let approval_record = if matches!(
            *current.progress,
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ApprovalReceived(_))
        ) {
            current
        } else {
            self.journal.advance(
                &current,
                JoinerJoinProgress::ApprovalReceived(approval.clone()),
            )?
        };
        let origin = crate::protocol::store_commit::StoreDeviceRegistrationOrigin::Join {
            attempt_id,
            attempt_slot: approval.request.offer.attempt_slot.clone(),
            outcome_slot: approval.request.offer.outcome_slot.clone(),
        };
        let device_id = crate::protocol::store_commit::StoreDeviceId::derive(
            &approval.request.offer.store_root,
            &origin,
        );
        let registration_context =
            crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
                approval.request.offer.store_root.store_root_hash,
                ProtocolObjectDomain::StoreDeviceRegistration,
            );
        let registration_slot = storage
            .allocate_protocol_slot(
                &registration_context,
                &crate::protocol::store_commit::registration_semantic_prefix(
                    &device_id.to_string(),
                ),
                ".json",
            )
            .await?;
        let context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            approval.request.offer.store_root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let first_slot = storage
            .allocate_protocol_slot(
                &context,
                &crate::protocol::store_commit::head_slot_prefix(&device_id.to_string(), 1),
                ".json",
            )
            .await?;
        let store_commits =
            crate::protocol::store_commit::DeviceStreamAnchor::StoreAnnouncements { first_slot };
        let ack_context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            approval.request.offer.store_root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let first_ack = storage
            .allocate_protocol_slot(
                &ack_context,
                &crate::protocol::store_commit::ack_slot_prefix(&device_id.to_string(), 1),
                ".json",
            )
            .await?;
        let snapshot_context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            approval.request.offer.store_root.store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let first_snapshot = storage
            .allocate_protocol_slot(
                &snapshot_context,
                &crate::protocol::store_commit::snapshot_slot_prefix(&device_id.to_string(), 0),
                ".json",
            )
            .await?;
        let response = match &approval.admission {
            DeviceProviderAdmissionChallenge::SamePrincipal => {
                DeviceProviderResponseReservation::SamePrincipal
            }
            DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) => {
                let slot = storage
                    .provider_probes()
                    .reserve_cross_principal_response_slot(challenge.probe_id)
                    .await
                    .map_err(provider_error)?;
                DeviceProviderResponseReservation::CrossPrincipal {
                    response_slot: slot,
                }
            }
        };
        let registration = StoreDeviceRegistration::signed(
            approval.request.offer.store_root.clone(),
            origin,
            live.device,
            store_commits,
            crate::protocol::store_commit::DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: first_ack,
            },
            crate::protocol::store_commit::DeviceStreamAnchor::StoreSnapshots {
                first_slot: first_snapshot,
            },
            identity,
        )
        .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        prepare_registration_object(storage, &registration, registration_slot.clone())?;
        if access_request != *approval.request {
            return Err(DeviceJoinError::JournalConflict);
        }
        let request = DeviceRegistrationRequest::signed(
            approval,
            registration,
            registration_slot,
            response,
            identity,
        )?;
        self.journal.advance(
            &approval_record,
            JoinerJoinProgress::RegistrationPrepared(request.clone()),
        )?;
        Ok(request)
    }

    async fn close(
        &mut self,
        identity: &UserKeypair,
        cancellation: DeviceJoinCancellation,
    ) -> Result<JoinerJoinTerminal, DeviceJoinError> {
        require_cancelled_outcome(&cancellation.outcome)?;
        let attempt_ref = cancellation.outcome.attempt().clone();
        if attempt_ref.attempt_id != self.journal.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let current = self
            .journal
            .load()?
            .ok_or(DeviceJoinError::JournalConflict)?;
        match &*current.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Cancelled(closure)) => {
                return Ok(JoinerJoinTerminal::Cancelled(closure.clone()));
            }
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
                return Ok(JoinerJoinTerminal::WriteRevoked(revocation.clone()));
            }
            _ => {}
        }
        if !matches!(
            &*current.progress,
            DeviceJoinRoleProgress::Joiner(progress) if progress.holds_staged_work()
        ) {
            return Err(DeviceJoinError::JournalConflict);
        }
        let (attempt, owner) = self
            .history_verifier
            .load_device_join_attempt_and_owner(&attempt_ref)
            .await?;
        let outcome = self
            .history_verifier
            .load_device_join_outcome(&cancellation.outcome, &owner.value)
            .await?
            .value;
        if !matches!(
            outcome.disposition,
            crate::protocol::store_commit::DeviceJoinDisposition::Cancelled
        ) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let joining_device_signer = attempt
            .value
            .expected_registration
            .device_signer(identity)?;
        let (registration, initial_ack, response, prior_state_hash, intent) = match &*current
            .progress
        {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupIntent {
                cancellation: durable,
                registration,
                initial_ack,
                response,
                prior_state_hash,
            }) if durable == &cancellation => (
                registration.clone(),
                initial_ack.clone(),
                response.clone(),
                *prior_state_hash,
                current.clone(),
            ),
            _ => {
                let registration = self
                    .storage
                    .observe_exact_slot(&attempt.value.registration_slot)
                    .await
                    .map(SlotDisposition::from)
                    .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
                let initial_ack = self
                    .storage
                    .observe_exact_slot(
                        attempt
                            .value
                            .expected_registration
                            .acknowledgements
                            .first_slot(),
                    )
                    .await
                    .map(SlotDisposition::from)
                    .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
                let response = match &attempt.value.provider_response {
                    DeviceProviderResponseReservation::SamePrincipal => {
                        JoinerResponseDisposition::SamePrincipal
                    }
                    DeviceProviderResponseReservation::CrossPrincipal { response_slot } => {
                        JoinerResponseDisposition::Slot(
                            self.storage
                                .observe_exact_slot(response_slot)
                                .await
                                .map(SlotDisposition::from)
                                .map_err(|error| DeviceJoinError::Provider(error.to_string()))?,
                        )
                    }
                };
                let prior_state_hash = ObjectHash::digest(&serde_json::to_vec(&current.progress)?);
                let intent = self.journal.advance(
                    &current,
                    JoinerJoinProgress::CleanupIntent {
                        cancellation: cancellation.clone(),
                        registration: registration.clone(),
                        initial_ack: initial_ack.clone(),
                        response: response.clone(),
                        prior_state_hash,
                    },
                )?;
                (
                    registration,
                    initial_ack,
                    response,
                    prior_state_hash,
                    intent,
                )
            }
        };
        for slot in canonical_cleanup_slots(&attempt.value)? {
            self.storage
                .delete_exact_slot_and_verify_absent(&slot)
                .await
                .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
        }
        let closure = JoinerJoinClosure::signed(
            cancellation.outcome,
            attempt.value.expected_registration.clone(),
            registration,
            initial_ack,
            response,
            prior_state_hash,
            &joining_device_signer,
        )?;
        self.journal
            .advance(&intent, JoinerJoinProgress::Cancelled(closure.clone()))?;
        Ok(JoinerJoinTerminal::Cancelled(closure))
    }

    pub(crate) async fn accept_cleanup(
        &mut self,
        activation: DeviceJoinCleanupActivation,
    ) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
        let attempt_id = activation.receipt.attempt_id;
        if attempt_id != self.journal.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let current = self
            .journal
            .load()?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CancelledComplete(existing)) =
            &*current.progress
        {
            if existing == &activation {
                return Ok(existing.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(existing)) =
            &*current.progress
        {
            if existing == &activation {
                return Ok(existing.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        let local_terminal = match &*current.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Cancelled(closure)) => {
                Some(JoinerJoinTerminal::Cancelled(closure.clone()))
            }
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
                Some(JoinerJoinTerminal::WriteRevoked(revocation.clone()))
            }
            DeviceJoinRoleProgress::Joiner(progress) if progress.holds_staged_work() => None,
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        let evidence = self
            .history_verifier
            .load_device_join_cleanup_activation(&activation)
            .await?;
        let receipt_terminal = self
            .history_verifier
            .verify_device_join_cleanup_activation(evidence)
            .await?;
        match &local_terminal {
            Some(terminal) if terminal != &receipt_terminal => {
                return Err(DeviceJoinError::JournalConflict);
            }
            None if !matches!(
                &receipt_terminal,
                JoinerJoinTerminal::WriteRevoked(revocation)
                    if revocation.producer == DeviceJoinProducer::Joiner
            ) =>
            {
                return Err(DeviceJoinError::JournalConflict);
            }
            _ => {}
        }
        let activated = self
            .journal
            .record(JoinerJoinProgress::CleanupActivated(activation.clone()));
        if local_terminal.is_some() {
            self.journal.advance_to(&current, &activated)?;
        } else {
            self.advance_cleanup_from_replacement(&current, activated)?;
        }
        Ok(activation)
    }

    fn advance_cleanup_from_replacement(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        self.journal
            .advance_cleanup_from_replacement(previous, next)
    }
}

impl PendingDeviceJoinClosure<'_> {
    pub(crate) async fn close(
        &mut self,
        cancellation: DeviceJoinCancellation,
    ) -> Result<JoinerJoinTerminal, DeviceJoinError> {
        self.observation.close(&self.identity, cancellation).await
    }
}
