use super::cleanup::require_cancelled_outcome;
use super::journal::{attempt_key, database_error, provider_error, store_journal_key};
use super::*;

#[doc(hidden)]
pub struct PendingDeviceJoinAuthority<'storage> {
    observation: PendingDeviceJoinObservation<'storage>,
    offer: DeviceJoinOffer,
    identity: UserKeypair,
}

#[doc(hidden)]
pub struct PendingDeviceJoinObservation<'storage> {
    journal: PendingJoinJournal,
    history_verifier: crate::sync::store::owner::pull::MergeHistoryVerifier<'storage>,
}

#[doc(hidden)]
pub struct PendingDeviceJoinClosure<'storage> {
    observation: PendingDeviceJoinObservation<'storage>,
    identity: UserKeypair,
}

#[doc(hidden)]
pub struct JoiningStore<'storage> {
    journal: PendingJoinJournal,
    bootstrap: super::super::BootstrappedStore<'storage>,
}

#[derive(Clone)]
struct PendingJoinJournal {
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

    fn advance(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        self.database.advance(previous, next)
    }

    fn accept_offer(&self, offer: &DeviceJoinOffer) -> Result<(), DeviceJoinError> {
        if offer.attempt_id != self.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let initial = DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::OfferReceived(offer.clone()),
            )),
        };
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
}

fn progress_offer(progress: &JoinerJoinProgress) -> Option<&DeviceJoinOffer> {
    match progress {
        JoinerJoinProgress::OfferReceived(offer) => Some(offer),
        JoinerJoinProgress::AccessRequested(request) => Some(&request.offer),
        JoinerJoinProgress::ApprovalReceived(approval) => Some(&approval.request.offer),
        JoinerJoinProgress::RegistrationPrepared(request) => Some(&request.approval.request.offer),
        JoinerJoinProgress::ProviderReady(bootstrap)
        | JoinerJoinProgress::RegistrationCreateIntent(bootstrap) => {
            Some(&bootstrap.bootstrap.request.approval.request.offer)
        }
        _ => None,
    }
}

async fn verify_offer(
    history_verifier: &mut crate::sync::store::owner::pull::MergeHistoryVerifier<'_>,
    identity: &UserKeypair,
    offer: &DeviceJoinOffer,
) -> Result<(), DeviceJoinError> {
    if crate::keys::public_key_hex(identity) != offer.member_pubkey
        || history_verifier.storage().provider_binding().await?.store != offer.provider
        || history_verifier.verified_root().descriptor.provider != offer.provider
        || history_verifier.root() != &offer.store_root
    {
        return Err(DeviceJoinError::OfferMismatch);
    }
    let owner = history_verifier
        .load_registration(&offer.owner_registration)
        .await?
        .value;
    offer.verify(&owner)
}

impl JoiningStore<'_> {
    pub async fn materialize(
        &mut self,
        activation: DeviceJoinActivation,
    ) -> Result<JoinedStore, DeviceJoinError> {
        if !matches!(&activation.outcome, DeviceJoinOutcomeRef::Activated { .. }) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let attempt_ref = activation.outcome.attempt().clone();
        let root = self.bootstrap.history.history_verifier_mut().root().clone();
        let database = self.bootstrap.history.database().clone();
        let (attempt, owner) = self
            .bootstrap
            .history
            .history_verifier_mut()
            .load_verified_device_join_attempt_and_owner(&attempt_ref)
            .await?;
        self.bootstrap
            .history
            .materialize_device_join_activation(
                &activation.outcome_activation,
                &activation.outcome,
                &attempt.value.membership,
            )
            .await?;
        let outcome = self
            .bootstrap
            .history
            .history_verifier_mut()
            .load_device_join_outcome(&activation.outcome, &owner.value)
            .await?
            .value;
        let crate::sync::store_commit::DeviceJoinOutcomeBody::Activated { readiness } =
            outcome.body
        else {
            return Err(DeviceJoinError::AttemptMismatch);
        };
        let local = database
            .latest_local_store_device_registration()
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
}

impl<'storage> PendingDeviceJoinAuthority<'storage> {
    #[doc(hidden)]
    pub async fn open(
        pending: &DeviceJoinJournalDatabase,
        storage: &'storage dyn SyncStorage,
        identity: &UserKeypair,
        offer: DeviceJoinOffer,
    ) -> Result<Self, DeviceJoinError> {
        let mut observation = PendingDeviceJoinObservation::open(
            pending,
            storage,
            &offer.store_root,
            offer.attempt_id,
        )
        .await?;
        verify_offer(&mut observation.history_verifier, identity, &offer).await?;
        observation.journal.accept_offer(&offer)?;
        Ok(Self {
            observation,
            offer,
            identity: identity.clone(),
        })
    }
}

impl<'storage> JoiningStore<'storage> {
    pub async fn begin_from_pending(
        pending: PendingDeviceJoinAuthority<'storage>,
        database: StoreDatabase,
    ) -> Result<Self, DeviceJoinError> {
        Self::from_observation(pending.observation, database, pending.identity).await
    }

    pub async fn begin_from_restore(
        restoring: super::super::RestoringStore<'storage>,
        pending: &DeviceJoinJournalDatabase,
        offer: DeviceJoinOffer,
    ) -> Result<Self, DeviceJoinError> {
        let mut bootstrap = restoring.into_bootstrapped_store();
        verify_offer(
            &mut bootstrap.history.history_verifier,
            &bootstrap.identity,
            &offer,
        )
        .await?;
        let journal = PendingJoinJournal::new(pending, offer.attempt_id);
        journal.accept_offer(&offer)?;
        let founder_pubkey = bootstrap
            .history
            .history_verifier
            .verified_root()
            .descriptor
            .founder_pubkey
            .clone();
        bootstrap.membership = bootstrap
            .history
            .load_and_install_owner_membership(&founder_pubkey)
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        bootstrap
            .history
            .database
            .validated_store_owner(bootstrap.history.history_verifier.root())
            .await
            .map_err(database_error)?;
        Ok(Self { journal, bootstrap })
    }

    async fn from_observation(
        observation: PendingDeviceJoinObservation<'storage>,
        database: StoreDatabase,
        identity: UserKeypair,
    ) -> Result<Self, DeviceJoinError> {
        let mut history = super::super::AuthorizedStoreHistory {
            database,
            history_verifier: observation.history_verifier,
        };
        let founder_pubkey = history
            .history_verifier
            .verified_root()
            .descriptor
            .founder_pubkey
            .clone();
        let membership = history
            .load_and_install_owner_membership(&founder_pubkey)
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        history
            .database
            .validated_store_owner(history.history_verifier.root())
            .await
            .map_err(database_error)?;
        Ok(Self {
            journal: observation.journal,
            bootstrap: super::super::BootstrappedStore {
                history,
                membership,
                identity,
            },
        })
    }

    pub async fn resume(
        pending: &DeviceJoinJournalDatabase,
        database: StoreDatabase,
        storage: &'storage dyn SyncStorage,
        identity: &UserKeypair,
        store_root: &StoreRootRef,
        attempt_id: DeviceJoinAttemptId,
    ) -> Result<Self, DeviceJoinError> {
        let observation =
            PendingDeviceJoinObservation::open(pending, storage, store_root, attempt_id).await?;
        Self::from_observation(observation, database, identity.clone()).await
    }
}

impl<'storage> PendingDeviceJoinObservation<'storage> {
    pub async fn open(
        pending: &DeviceJoinJournalDatabase,
        storage: &'storage dyn SyncStorage,
        store_root: &StoreRootRef,
        attempt_id: DeviceJoinAttemptId,
    ) -> Result<Self, DeviceJoinError> {
        let verified_root =
            crate::sync::store::protocol_root::load_pinned_store_protocol_root(storage, store_root)
                .await
                .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        let commit_verifier = super::super::StoreCommitVerifier::from_verified_root(
            storage,
            store_root,
            verified_root,
        )?;
        let history_verifier =
            crate::sync::store::owner::pull::MergeHistoryVerifier::from_commit_verifier(
                commit_verifier,
            )
            .await?;
        Ok(Self {
            journal: PendingJoinJournal::new(pending, attempt_id),
            history_verifier,
        })
    }

    pub fn authorize_closure(self, identity: &UserKeypair) -> PendingDeviceJoinClosure<'storage> {
        PendingDeviceJoinClosure {
            observation: self,
            identity: identity.clone(),
        }
    }

    pub fn observe_activation_if_pending(
        &self,
        activation: &DeviceJoinActivation,
    ) -> Result<Option<DeviceJoinReadiness>, DeviceJoinError> {
        self.journal.observe_activation_if_pending(activation)
    }
}

impl JoiningStore<'_> {
    pub async fn pull_store_history(
        &mut self,
        store_dir: &crate::store_dir::StoreDir,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<crate::sync::store::StorePullResult, DeviceJoinError> {
        let tables = self
            .bootstrap
            .history
            .database()
            .sqlite()
            .synced_tables()
            .to_vec();
        let membership = self.bootstrap.membership.clone();
        let execution = self
            .bootstrap
            .history
            .pull(
                &tables,
                store_dir,
                &membership,
                Some(&self.bootstrap.identity),
                routing_encryption,
            )
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        self.bootstrap.membership = execution.membership;
        Ok(execution.result)
    }
}

impl PendingDeviceJoinObservation<'_> {
    pub async fn observe_abandonment(
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
        if current
            .joiner_abandonment_transition(&abandonment)?
            .is_none()
        {
            return Ok(abandonment);
        }
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            self.history_verifier.root().store_root_hash,
            ProtocolObjectDomain::DeviceJoinAbandonment,
        );
        let prefix = crate::sync::store_commit::device_join_abandonment_semantic_prefix(
            self.journal.attempt_id,
        );
        let bytes = self
            .history_verifier
            .storage()
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
        let next = current
            .joiner_abandonment_transition(&abandonment)?
            .ok_or(DeviceJoinError::JournalConflict)?;
        self.journal.advance(&current, next)?;
        Ok(abandonment)
    }
}

impl Store {
    #[doc(hidden)]
    pub async fn revoke_joining_device_writes(
        &self,
        cancellation: DeviceJoinCancellation,
        revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
    ) -> Result<JoinerJoinTerminal, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        let executor_grant = writer
            .protocol_root()
            .descriptor
            .founder_provider_admin
            .grant_id
            .clone();
        writer
            .provider_administrator_join()?
            .revoke_joiner_writes(cancellation, revocation_executor, executor_grant)
            .await
    }
}

impl PendingDeviceJoinAuthority<'_> {
    pub async fn prepare_provider_access_request(
        &self,
    ) -> Result<DeviceProviderAccessRequest, DeviceJoinError> {
        let record = self
            .observation
            .journal
            .load()?
            .ok_or(DeviceJoinError::JournalConflict)?;
        match &*record.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request))
                if *request.offer == self.offer =>
            {
                Ok(request.clone())
            }
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(durable))
                if durable == &self.offer =>
            {
                let binding = self
                    .observation
                    .history_verifier
                    .storage()
                    .provider_binding()
                    .await?;
                if binding.store != self.offer.provider {
                    return Err(DeviceJoinError::OfferMismatch);
                }
                let request = DeviceProviderAccessRequest::signed(
                    self.offer.clone(),
                    binding.device,
                    &self.identity,
                )?;
                self.observation.journal.advance(
                    &record,
                    DeviceJoinJournalRecord {
                        attempt_id: request.offer.attempt_id,
                        progress: Box::new(DeviceJoinRoleProgress::Joiner(
                            JoinerJoinProgress::AccessRequested(request.clone()),
                        )),
                    },
                )?;
                Ok(request)
            }
            _ => Err(DeviceJoinError::JournalConflict),
        }
    }
}

impl PendingDeviceJoinAuthority<'_> {
    pub async fn prepare_registration_request(
        &mut self,
        approval: DeviceProviderAdmissionApproval,
    ) -> Result<DeviceRegistrationRequest, DeviceJoinError> {
        if *approval.request.offer != self.offer {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        let attempt_id = approval.request.offer.attempt_id;
        if let Some(record) = self.observation.journal.load()? {
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
            .observation
            .history_verifier
            .load_registration(&approval.request.offer.owner_registration)
            .await?
            .value;
        let administrator = self
            .observation
            .history_verifier
            .load_registration(&approval.request.offer.provider_admin.administrator)
            .await?
            .value;
        approval.verify(
            self.observation.history_verifier.verified_root_object(),
            &owner,
            &administrator,
        )?;
        self.observation
            .history_verifier
            .verify_accepted_provider_access_activation(
                &approval.access_grant,
                &approval.request.offer.provider_admin,
                &administrator,
            )
            .await?;
        let storage = self.observation.history_verifier.storage();
        let live = storage.provider_binding().await?;
        if live.store != approval.request.offer.provider
            || live.device != approval.request.peer_provider
        {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        let current = self
            .observation
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
            let next = DeviceJoinJournalRecord {
                attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::ApprovalReceived(approval.clone()),
                )),
            };
            self.observation.journal.advance(&current, next.clone())?;
            next
        };
        let origin = crate::sync::store_commit::StoreDeviceRegistrationOrigin::Join {
            attempt_id,
            attempt_slot: approval.request.offer.attempt_slot.clone(),
            outcome_slot: approval.request.offer.outcome_slot.clone(),
        };
        let device_id = crate::sync::store_commit::StoreDeviceId::derive(
            &approval.request.offer.store_root,
            &origin,
        );
        let registration_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            approval.request.offer.store_root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let registration_slot = storage
            .allocate_protocol_slot(
                &registration_context,
                &crate::sync::store_commit::registration_semantic_prefix(&device_id.to_string()),
                ".json",
            )
            .await?;
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            approval.request.offer.store_root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let first_slot = storage
            .allocate_protocol_slot(
                &context,
                &crate::sync::store_commit::head_slot_prefix(&device_id.to_string(), 1),
                ".json",
            )
            .await?;
        let store_commits =
            crate::sync::store_commit::DeviceStreamAnchor::StoreAnnouncements { first_slot };
        let ack_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            approval.request.offer.store_root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let first_ack = storage
            .allocate_protocol_slot(
                &ack_context,
                &crate::sync::store_commit::ack_slot_prefix(&device_id.to_string(), 1),
                ".json",
            )
            .await?;
        let snapshot_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            approval.request.offer.store_root.store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let first_snapshot = storage
            .allocate_protocol_slot(
                &snapshot_context,
                &crate::sync::store_commit::snapshot_slot_prefix(&device_id.to_string(), 0),
                ".json",
            )
            .await?;
        let response = match &approval.admission {
            DeviceProviderAdmissionChallenge::SamePrincipal => {
                DeviceProviderResponseReservation::SamePrincipal
            }
            DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) => {
                let exact = storage.exact_slot_storage();
                let logical = crate::sync::provider::cross_peer_logical_key(challenge.probe_id);
                let slot = exact
                    .allocate_slot(&logical)
                    .await
                    .map_err(provider_error)?;
                if slot.logical_key() != logical {
                    return Err(DeviceJoinError::RegistrationRequestMismatch);
                }
                DeviceProviderResponseReservation::CrossPrincipal {
                    response_slot: slot,
                }
            }
        };
        let (registration, _) = crate::sync::store::prepare_registration_for_origin(
            storage,
            &self.identity,
            approval.request.offer.store_root.clone(),
            origin,
            registration_slot.clone(),
            live.device,
            store_commits,
            crate::sync::store_commit::DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: first_ack,
            },
            crate::sync::store_commit::DeviceStreamAnchor::StoreSnapshots {
                first_slot: first_snapshot,
            },
        )
        .await?;
        if access_request != *approval.request {
            return Err(DeviceJoinError::JournalConflict);
        }
        let request = DeviceRegistrationRequest::signed(
            approval,
            registration,
            registration_slot,
            response,
            &self.identity,
        )?;
        self.observation.journal.advance(
            &approval_record,
            DeviceJoinJournalRecord {
                attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::RegistrationPrepared(request.clone()),
                )),
            },
        )?;
        Ok(request)
    }
}

impl JoiningStore<'_> {
    pub async fn bootstrap(
        &mut self,
        bootstrap: ProviderReadyDeviceBootstrap,
        published_at: &str,
    ) -> Result<DeviceJoinReadiness, DeviceJoinError> {
        let database = self.bootstrap.history.database().clone();
        let offer = &bootstrap.bootstrap.request.approval.request.offer;
        if &offer.store_root != self.bootstrap.history.root()
            || offer.member_pubkey != crate::keys::public_key_hex(&self.bootstrap.identity)
            || database.sqlite().sync_routing_hash()
                != self
                    .bootstrap
                    .history
                    .verified_root_object()
                    .value
                    .descriptor
                    .sync_routing_hash
        {
            return Err(DeviceJoinError::OfferMismatch);
        }
        let attempt_owner = self
            .bootstrap
            .history
            .history_verifier_mut()
            .load_registration(&offer.owner_registration)
            .await?
            .value;
        let administrator = self
            .bootstrap
            .history
            .history_verifier_mut()
            .load_registration(&offer.provider_admin.administrator)
            .await?
            .value;
        let (verified_attempt, bootstrap_plan) = Box::pin(
            self.bootstrap
                .history
                .history_verifier_mut()
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
        let storage = self.bootstrap.history.storage();
        let proof = Box::pin(crate::sync::store::bootstrap_pending_device(
            &database,
            storage,
            &self.bootstrap.identity,
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
        ))
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
                let exact = storage.exact_slot_storage();
                let context = crate::sync::provider::CrossPrincipalResponseContext {
                    challenge: cross_challenge_context(
                        &bootstrap.bootstrap.request.approval.request,
                    ),
                    expected_registration_hash: bootstrap
                        .bootstrap
                        .request
                        .expected_registration
                        .registration_hash(),
                    response_slot: response_slot.clone(),
                };
                DeviceProviderReadiness::CrossPrincipal(
                    Box::pin(crate::sync::provider::create_cross_principal_response(
                        exact,
                        challenge,
                        &context,
                        &offer.provider,
                        &administrator.device_signing_pubkey,
                        &self.bootstrap.identity,
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
}

impl PendingJoinJournal {
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
        let provider_ready = DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::ProviderReady(bootstrap.clone()),
            )),
        };
        self.advance(&prepared, provider_ready.clone())?;
        let registration_intent = DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::RegistrationCreateIntent(bootstrap.clone()),
            )),
        };
        self.advance(&provider_ready, registration_intent.clone())?;
        let registration_created = DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::RegistrationCreated(readiness.proof.registration.clone()),
            )),
        };
        self.advance(&registration_intent, registration_created.clone())?;
        let ack_intent = DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::AckCreateIntent(readiness.proof.registration.clone()),
            )),
        };
        self.advance(&registration_created, ack_intent.clone())?;
        let ack_created = DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::AckCreated(readiness.proof.initial_ack.clone()),
            )),
        };
        self.advance(&ack_intent, ack_created.clone())?;
        let ready_record = DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(
                readiness.clone(),
            ))),
        };
        match readiness.provider {
            DeviceProviderReadiness::SamePrincipal => self.advance(&ack_created, ready_record)?,
            DeviceProviderReadiness::CrossPrincipal(_) => {
                let response_intent = DeviceJoinJournalRecord {
                    attempt_id: offer.attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Joiner(
                        JoinerJoinProgress::ResponseCreateIntent(readiness.clone()),
                    )),
                };
                self.advance(&ack_created, response_intent.clone())?;
                self.advance(&response_intent, ready_record)?;
            }
        }
        Ok(readiness)
    }
}

pub(super) fn cross_challenge_context(
    request: &DeviceProviderAccessRequest,
) -> crate::sync::provider::CrossPrincipalChallengeContext {
    crate::sync::provider::CrossPrincipalChallengeContext {
        root: request.offer.store_root.clone(),
        attempt_id: request.offer.attempt_id,
        access_request_hash: request.request_hash(),
        provider_admin_grant: request.offer.provider_admin.grant_id.clone(),
        owner_registration: request.offer.owner_registration.clone(),
        member_pubkey: request.offer.member_pubkey.clone(),
        administrator_binding: request.offer.provider_admin.provider.clone(),
        peer_binding: request.peer_provider.clone(),
    }
}

impl PendingDeviceJoinClosure<'_> {
    pub async fn close(
        &mut self,
        cancellation: DeviceJoinCancellation,
    ) -> Result<JoinerJoinTerminal, DeviceJoinError> {
        require_cancelled_outcome(&cancellation.outcome)?;
        let attempt_ref = cancellation.outcome.attempt().clone();
        if attempt_ref.attempt_id != self.observation.journal.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let current = self
            .observation
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
            DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::RegistrationPrepared(_)
                    | JoinerJoinProgress::ProviderReady(_)
                    | JoinerJoinProgress::RegistrationCreateIntent(_)
                    | JoinerJoinProgress::RegistrationCreated(_)
                    | JoinerJoinProgress::AckCreateIntent(_)
                    | JoinerJoinProgress::AckCreated(_)
                    | JoinerJoinProgress::ResponseCreateIntent(_)
                    | JoinerJoinProgress::Ready(_)
                    | JoinerJoinProgress::CleanupIntent { .. }
            )
        ) {
            return Err(DeviceJoinError::JournalConflict);
        }
        let (attempt, owner) = self
            .observation
            .history_verifier
            .load_device_join_attempt_and_owner(&attempt_ref)
            .await?;
        let outcome = self
            .observation
            .history_verifier
            .load_device_join_outcome(&cancellation.outcome, &owner.value)
            .await?
            .value;
        if !matches!(
            outcome.body,
            crate::sync::store_commit::DeviceJoinOutcomeBody::Cancelled
        ) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let joining_device_signer = attempt
            .value
            .expected_registration
            .device_signer(&self.identity)?;
        let peer_exact = self
            .observation
            .history_verifier
            .storage()
            .exact_slot_storage();
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
                let registration = peer_exact
                    .observe_at(&attempt.value.registration_slot)
                    .await
                    .map(SlotDisposition::from)
                    .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
                let initial_ack = peer_exact
                    .observe_at(
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
                            peer_exact
                                .observe_at(response_slot)
                                .await
                                .map(SlotDisposition::from)
                                .map_err(|error| DeviceJoinError::Provider(error.to_string()))?,
                        )
                    }
                };
                let prior_state_hash = ObjectHash::digest(&serde_json::to_vec(&current.progress)?);
                let intent = DeviceJoinJournalRecord {
                    attempt_id: attempt_ref.attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Joiner(
                        JoinerJoinProgress::CleanupIntent {
                            cancellation: cancellation.clone(),
                            registration: registration.clone(),
                            initial_ack: initial_ack.clone(),
                            response: response.clone(),
                            prior_state_hash,
                        },
                    )),
                };
                self.observation.journal.advance(&current, intent.clone())?;
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
            peer_exact
                .delete_and_verify_absent(&slot)
                .await
                .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
        }
        let closure = JoinerJoinClosure::signed(
            cancellation.outcome,
            attempt.value.expected_registration,
            registration,
            initial_ack,
            response,
            prior_state_hash,
            &joining_device_signer,
        )?;
        self.observation.journal.advance(
            &intent,
            DeviceJoinJournalRecord {
                attempt_id: attempt_ref.attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::Cancelled(closure.clone()),
                )),
            },
        )?;
        Ok(JoinerJoinTerminal::Cancelled(closure))
    }
}

impl PendingDeviceJoinObservation<'_> {
    pub async fn accept_cleanup(
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
            DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::RegistrationPrepared(_)
                | JoinerJoinProgress::ProviderReady(_)
                | JoinerJoinProgress::RegistrationCreateIntent(_)
                | JoinerJoinProgress::RegistrationCreated(_)
                | JoinerJoinProgress::AckCreateIntent(_)
                | JoinerJoinProgress::AckCreated(_)
                | JoinerJoinProgress::ResponseCreateIntent(_)
                | JoinerJoinProgress::Ready(_)
                | JoinerJoinProgress::CleanupIntent { .. },
            ) => None,
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        let evidence = crate::sync::store::owner::pull::load_device_join_cleanup_activation(
            &mut self.history_verifier,
            &activation,
        )
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
        let activated = DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::CleanupActivated(activation.clone()),
            )),
        };
        if local_terminal.is_some() {
            self.journal.advance(&current, activated)?;
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
        if previous.attempt_id != next.attempt_id
            || !matches!(
                &*previous.progress,
                DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::RegistrationPrepared(_)
                        | JoinerJoinProgress::ProviderReady(_)
                        | JoinerJoinProgress::RegistrationCreateIntent(_)
                        | JoinerJoinProgress::RegistrationCreated(_)
                        | JoinerJoinProgress::AckCreateIntent(_)
                        | JoinerJoinProgress::AckCreated(_)
                        | JoinerJoinProgress::ResponseCreateIntent(_)
                        | JoinerJoinProgress::Ready(_)
                        | JoinerJoinProgress::CleanupIntent { .. }
                )
            )
            || !matches!(
                &*next.progress,
                DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(_))
            )
        {
            return Err(DeviceJoinError::JournalConflict);
        }
        let previous_payload = serde_json::to_string(previous)?;
        let next_payload = serde_json::to_string(&next)?;
        let connection = Connection::open(self.journal.database.path())?;
        let changed = connection.execute(
            "UPDATE device_join_journals SET payload = ?1
             WHERE attempt_id = ?2 AND role = 'joiner' AND payload = ?3",
            (
                &next_payload,
                attempt_key(previous.attempt_id),
                &previous_payload,
            ),
        )?;
        if changed != 1 {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(())
    }
}

impl JoiningStore<'_> {
    pub async fn complete(
        &mut self,
        activation: DeviceJoinActivation,
    ) -> Result<JoinedStore, DeviceJoinError> {
        let database = self.bootstrap.history.database().clone();
        let db = database.sqlite();
        let attempt_id = activation.outcome.attempt().attempt_id;
        if let Some(record) = database
            .device_join_journal()
            .load(attempt_id, DeviceJoinRole::Joiner)
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
        let activated_record = DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::Activated(joined.clone()),
            )),
        };
        let store_key = store_journal_key(attempt_id, DeviceJoinRole::Joiner.as_str());
        let store_payload = serde_json::to_string(&activated_record)?;
        let pending_path = self.journal.database.path().to_string_lossy().into_owned();
        let pending_attempt = attempt_key(attempt_id);
        let expected_pending = serde_json::to_string(&current)?;
        db.call(move |connection| {
            connection
                .execute("ATTACH DATABASE ?1 AS pending_join_source", [&pending_path])
                .map_err(crate::database::DbError::from)?;
            let tx = connection
                .unchecked_transaction()
                .map_err(crate::database::DbError::from)?;
            let actual: String = tx
                .query_row(
                    "SELECT payload FROM pending_join_source.device_join_journals
                 WHERE attempt_id = ?1 AND role = 'joiner'",
                    [&pending_attempt],
                    |row| row.get(0),
                )
                .map_err(crate::database::DbError::from)?;
            if actual != expected_pending {
                return Err(crate::database::DbError::Message(
                    "pending join journal changed before activation".to_string(),
                ));
            }
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value WHERE value = excluded.value",
                (&store_key, &store_payload),
            )
            .map_err(crate::database::DbError::from)?;
            tx.execute(
                "DELETE FROM pending_join_source.device_join_journals
             WHERE attempt_id = ?1 AND role = 'joiner' AND payload = ?2",
                (&pending_attempt, &expected_pending),
            )
            .map_err(crate::database::DbError::from)?;
            tx.commit().map_err(crate::database::DbError::from)?;
            connection
                .execute_batch("DETACH DATABASE pending_join_source")
                .map_err(crate::database::DbError::from)
        })
        .await
        .map_err(database_error)?;
        Ok(joined)
    }
}
