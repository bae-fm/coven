use super::journal::database_error;
use super::*;
use coven_protocol::store_commit::device_join_exchange::require_cancelled_outcome;

#[doc(hidden)]
pub struct PendingDeviceJoinAuthority<'storage> {
    observation: PendingDeviceJoinObservation<'storage>,
    offer: DeviceJoinOffer,
    identity: UserKeypair,
}

#[doc(hidden)]
pub struct PendingDeviceJoinObservation<'storage> {
    journal: PendingJoinJournal,
    storage: &'storage std::sync::Arc<dyn CloudSyncObjectStorage>,
    history_verifier:
        crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier<'storage>,
}

#[doc(hidden)]
pub struct PendingDeviceJoinClosure<'storage> {
    observation: PendingDeviceJoinObservation<'storage>,
    identity: UserKeypair,
}

#[doc(hidden)]
pub struct PendingSamePrincipalDeviceJoinCompletion {
    database: StoreDatabase,
    journal: PendingJoinJournal,
    current: DeviceJoinJournalRecord,
    activated: DeviceJoinJournalRecord,
    joined: JoinedStore,
}

impl PendingSamePrincipalDeviceJoinCompletion {
    pub fn joined(&self) -> &JoinedStore {
        &self.joined
    }

    pub async fn complete(self) -> Result<JoinedStore, DeviceJoinError> {
        self.journal
            .complete_on(&self.database, &self.current, &self.activated)
            .await?;
        Ok(self.joined)
    }
}

#[doc(hidden)]
pub struct JoiningStore<'storage> {
    journal: PendingJoinJournal,
    history: super::AuthorizedStoreHistory<'storage>,
    membership: coven_protocol::membership::MembershipChain,
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

    fn record_registration_request(
        &self,
        offer: &DeviceJoinOffer,
        approval: DeviceProviderAdmissionApproval,
        request: DeviceRegistrationRequest,
    ) -> Result<DeviceRegistrationRequest, DeviceJoinError> {
        if approval.request.offer.as_ref() != offer || request.approval() != &approval {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        if let Some(record) = self.load()? {
            if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(
                durable,
            )) = *record.progress
            {
                if durable == request {
                    return Ok(durable);
                }
                return Err(DeviceJoinError::JournalConflict);
            }
        }
        let current = self.load()?.ok_or(DeviceJoinError::JournalConflict)?;
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
            self.advance(
                &current,
                JoinerJoinProgress::ApprovalReceived(approval.clone()),
            )?
        };
        if access_request != *approval.request {
            return Err(DeviceJoinError::JournalConflict);
        }
        self.advance(
            &approval_record,
            JoinerJoinProgress::RegistrationPrepared(request.clone()),
        )?;
        Ok(request)
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
        let offer = &bootstrap.bootstrap.request.approval().request.offer;
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
        JoinerJoinProgress::RegistrationPrepared(request) => {
            Some(&request.approval().request.offer)
        }
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
            .map_err(DeviceJoinError::from)?;
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

    pub async fn materialize(
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
            .device_join()
            .load_verified_attempt_and_owner(&attempt_ref)
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
            .device_join()
            .load_outcome(&activation.outcome, &owner.value)
            .await?
            .value;
        let coven_protocol::store_commit::DeviceJoinDisposition::Activated { registration } =
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
        if !local.is_activated() {
            return Err(DeviceJoinError::Store(format!(
                "joining device registration is not activated after materializing {:?}: {:?}",
                activation.outcome_activation.coord, local.state
            )));
        }
        if local.registration_hash != registration.registration_hash
            || local.device_id != registration.device_id
            || attempt.value.expected_registration.to_bytes() != local.registration_bytes
        {
            return Err(DeviceJoinError::Store(
                "joining device registration differs from the activated registration".to_string(),
            ));
        }
        Ok(JoinedStore {
            store_root: root,
            registration,
            activation,
        })
    }

    pub async fn pull_store_history(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<crate::sync::store::StorePullResult, DeviceJoinError> {
        let membership = self.membership.clone();
        let execution = self
            .history
            .pull(&membership, Some(&self.identity), routing_encryption)
            .await
            .map_err(DeviceJoinError::from)?;
        self.membership = execution.membership;
        Ok(execution.result)
    }

    /// Read and verify the row data the bootstrap plan's uncovered commits
    /// carry, so installing it materializes their rows instead of advancing
    /// the joining device's position past them.
    pub(crate) async fn resolve_bootstrap(
        &mut self,
        plan: coven_database::DeviceJoinBootstrapPlan,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<coven_database::ResolvedDeviceJoinBootstrap, DeviceJoinError> {
        let membership = self.membership.clone();
        Ok(Box::pin(self.history.resolve_device_join_bootstrap(
            plan,
            &membership,
            &self.identity,
            routing_encryption,
        ))
        .await?)
    }

    pub async fn bootstrap(
        &mut self,
        bootstrap: ProviderReadyDeviceBootstrap,
        published_at: &str,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<DeviceJoinReadiness, DeviceJoinError> {
        let mut timings =
            crate::sync::stage_timing::StageTimings::start("Device join history install");
        let outcome = Box::pin(self.bootstrap_staged(
            bootstrap,
            published_at,
            routing_encryption,
            &mut timings,
        ))
        .await;
        timings.report();
        outcome
    }

    async fn bootstrap_staged(
        &mut self,
        bootstrap: ProviderReadyDeviceBootstrap,
        published_at: &str,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        timings: &mut crate::sync::stage_timing::StageTimings,
    ) -> Result<DeviceJoinReadiness, DeviceJoinError> {
        let offer = &bootstrap.bootstrap.request.approval().request.offer;
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
        let attempt_owner = timings
            .stage(
                "read registrations",
                self.history
                    .device_join()
                    .load_registration(&offer.owner_registration),
            )
            .await?
            .value;
        let administrator = timings
            .stage(
                "read registrations",
                self.history
                    .device_join()
                    .load_registration(&offer.provider_admin.administrator),
            )
            .await?
            .value;
        let (verified_attempt, bootstrap_plan) = timings
            .stage(
                "verify history",
                Box::pin(
                    self.history
                        .device_join()
                        .verify_attempt_and_prepare_bootstrap(
                            &bootstrap.bootstrap.publication_authorization.attempt,
                            &attempt_owner,
                            &bootstrap
                                .bootstrap
                                .publication_authorization
                                .attempt_activation,
                        ),
                ),
            )
            .await?;
        if verified_attempt.value.expected_registration
            != *bootstrap.bootstrap.request.expected_registration()
        {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let resolved = timings
            .stage(
                "resolve row data",
                self.resolve_bootstrap(bootstrap_plan, routing_encryption),
            )
            .await?;
        let proof = timings
            .stage(
                "publish readiness",
                Box::pin(
                    self.history.device_join().bootstrap_pending_device(
                        &self.identity,
                        bootstrap
                            .bootstrap
                            .publication_authorization
                            .attempt
                            .clone(),
                        verified_attempt,
                        resolved,
                        bootstrap
                            .bootstrap
                            .publication_authorization
                            .attempt_activation
                            .clone(),
                        &attempt_owner,
                        published_at,
                    ),
                ),
            )
            .await?;
        let provider = match (
            &bootstrap.bootstrap.request.approval().admission,
            &bootstrap.bootstrap.request.response(),
            &bootstrap.challenge_publication,
        ) {
            (
                DeviceProviderAdmission::SamePrincipal,
                DeviceProviderResponseReservation::SamePrincipal,
                DeviceProviderChallengePublication::SamePrincipal,
            ) => DeviceProviderReadiness::SamePrincipal,
            (
                DeviceProviderAdmission::CrossPrincipal { challenge, .. },
                DeviceProviderResponseReservation::CrossPrincipal { response_slot },
                DeviceProviderChallengePublication::CrossPrincipal {
                    challenge: published,
                },
            ) if challenge == published => {
                let context = coven_protocol::provider::CrossPrincipalResponseContext {
                    challenge: bootstrap
                        .bootstrap
                        .request
                        .approval()
                        .request
                        .cross_challenge_context(),
                    expected_registration_hash: bootstrap
                        .bootstrap
                        .request
                        .expected_registration()
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
                    .map_err(DeviceJoinError::ProviderProbe)?,
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
            .map_err(DeviceJoinError::JoinTask)?
    }

    pub async fn complete(
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
    /// Record the same-provider approval without opening Store history. The
    /// joining device's first request already signed its complete registration;
    /// the snapshot bootstrap verifies the approval, attempt, and registration
    /// together before installing any trusted state.
    pub fn record_same_principal_registration_request(
        pending: &DeviceJoinJournalDatabase,
        offer: &DeviceJoinOffer,
        approval: DeviceProviderAdmissionApproval,
    ) -> Result<DeviceRegistrationRequest, DeviceJoinError> {
        if !matches!(approval.admission, DeviceProviderAdmission::SamePrincipal) {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        let request = DeviceRegistrationRequest::same_principal(approval.clone())?;
        PendingJoinJournal::new(pending, offer.attempt_id)
            .record_registration_request(offer, approval, request)
    }

    pub async fn prepare_same_principal_completion(
        pending: &DeviceJoinJournalDatabase,
        storage: &'storage std::sync::Arc<dyn CloudSyncObjectStorage>,
        store_dir: &'storage coven_foundation::store_dir::StoreDir,
        identity: &UserKeypair,
        join: SamePrincipalDeviceJoin,
        installed: crate::sync::store::InstalledDeviceJoinSnapshot,
        published_at: &str,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<PendingSamePrincipalDeviceJoinCompletion, DeviceJoinError> {
        join.verify_shape()?;
        let attempt_ref = join
            .bootstrap
            .bootstrap
            .publication_authorization
            .attempt
            .clone();
        if installed.root
            != join
                .bootstrap
                .bootstrap
                .request
                .approval()
                .request
                .offer
                .store_root
            || installed.attempt != join.installation.attempt
            || installed.outcome != join.installation.outcome
            || join.activation.outcome.attempt() != &attempt_ref
        {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let activation_commit = installed
            .bootstrap
            .verified_commit(&join.activation.outcome_activation)
            .ok_or(DeviceJoinError::AttemptMismatch)?;
        let activated_registration_matches = activation_commit
            .value()
            .device_registrations()
            .iter()
            .any(|reference| {
                reference.registration.device_id
                    == installed.attempt.expected_registration.device_id
                    && matches!(
                        &reference.authority,
                        coven_protocol::store_commit::StoreDeviceRegistrationActivationRef::Join {
                            attempt_id,
                            outcome,
                        } if *attempt_id == attempt_ref.attempt_id
                            && outcome == &join.activation.outcome
                    )
            });
        if activation_commit.value().device_join_attempt_decisions()
            != std::slice::from_ref(&DeviceJoinAttemptDecisionRef::Attempt(attempt_ref.clone()))
            || activation_commit.value().device_join_outcomes()
                != std::slice::from_ref(&join.activation.outcome)
            || !activated_registration_matches
        {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let owner = activation_commit.author().clone();
        let attempt_bytes = installed.attempt.to_bytes();
        let attempt = DeviceJoinAttempt::parse_at(&attempt_bytes, &attempt_ref, &owner)?;
        let verified_attempt = coven_protocol::objects::VerifiedObject {
            value: attempt.clone(),
            bytes: attempt_bytes,
            semantic_hash: attempt_ref.attempt_hash,
            object: attempt_ref.object.clone(),
        };
        installed
            .outcome
            .verify_at(&join.activation.outcome, &attempt, &owner)?;
        let approval = join.bootstrap.bootstrap.request.approval();
        let administrator = registration_in_bootstrap(
            &installed.bootstrap,
            &approval.request.offer.provider_admin.administrator,
        )
        .ok_or(DeviceJoinError::AttemptMismatch)?;
        approval.verify(&installed.verified_root, &owner, administrator)?;
        let database = installed.database;
        // The installed snapshot image covers the history behind it; every
        // commit between that snapshot and the bootstrap cut still carries its
        // rows in a package this device has to read before it installs.
        let mut joining = PendingDeviceJoinObservation::open(
            pending,
            storage,
            &installed.root,
            attempt_ref.attempt_id,
        )
        .await?
        .into_joining_store(database.clone(), store_dir, identity.clone())
        .await?;
        let resolved = joining
            .resolve_bootstrap(installed.bootstrap, routing_encryption)
            .await?;
        drop(joining);
        let proof = super::history::bootstrap_pending_device_on(
            &database,
            storage.as_ref(),
            identity,
            attempt_ref,
            verified_attempt,
            resolved,
            join.activation.outcome_activation.clone(),
            &owner,
            published_at,
        )
        .await?;
        let readiness = DeviceJoinReadiness {
            proof,
            provider: DeviceProviderReadiness::SamePrincipal,
        };
        let journal =
            PendingJoinJournal::new(pending, join.activation.outcome.attempt().attempt_id);
        let readiness = journal.record_readiness(join.bootstrap, readiness)?;
        let observed = journal
            .observe_activation_if_pending(&join.activation)?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if observed != readiness {
            return Err(DeviceJoinError::JournalConflict);
        }
        let joined = joined_store_from_materialized(
            &database,
            &attempt,
            &installed.outcome,
            join.activation,
        )
        .await?;
        if joined.registration != readiness.proof.registration {
            return Err(DeviceJoinError::JournalConflict);
        }
        let current = journal.load()?.ok_or(DeviceJoinError::JournalConflict)?;
        let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ActivationObserved {
            readiness: current_readiness,
            activation: current_activation,
        }) = &*current.progress
        else {
            return Err(DeviceJoinError::JournalConflict);
        };
        if current_readiness != &readiness || current_activation != &joined.activation {
            return Err(DeviceJoinError::JournalConflict);
        }
        let activated = journal.record(JoinerJoinProgress::Activated(joined.clone()));
        Ok(PendingSamePrincipalDeviceJoinCompletion {
            database,
            journal,
            current,
            activated,
            joined,
        })
    }

    pub async fn open(
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

    pub async fn prepare_provider_access_request(
        &self,
    ) -> Result<DeviceProviderAccessRequest, DeviceJoinError> {
        self.observation
            .prepare_provider_access_request(&self.offer, &self.identity)
            .await
    }

    pub async fn prepare_registration_request(
        &mut self,
        approval: DeviceProviderAdmissionApproval,
    ) -> Result<DeviceRegistrationRequest, DeviceJoinError> {
        self.observation
            .prepare_registration_request(&self.offer, &self.identity, approval)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
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

fn registration_in_bootstrap<'plan>(
    plan: &'plan DeviceJoinBootstrapPlan,
    reference: &StoreDeviceRegistrationRef,
) -> Option<&'plan StoreDeviceRegistration> {
    if &plan.founder_reference == reference {
        return Some(&plan.founder);
    }
    plan.commits.iter().find_map(|commit| {
        if &commit.commit.value().author_registration == reference {
            return Some(commit.commit.author());
        }
        commit
            .registrations
            .iter()
            .find(|registration| registration.reference() == reference)
            .map(|registration| registration.value())
    })
}

async fn joined_store_from_materialized(
    database: &StoreDatabase,
    attempt: &DeviceJoinAttempt,
    outcome: &coven_protocol::store_commit::DeviceJoinOutcome,
    activation: DeviceJoinActivation,
) -> Result<JoinedStore, DeviceJoinError> {
    if !matches!(&activation.outcome, DeviceJoinOutcomeRef::Activated { .. }) {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let coven_protocol::store_commit::DeviceJoinDisposition::Activated { registration } =
        outcome.disposition.clone()
    else {
        return Err(DeviceJoinError::AttemptMismatch);
    };
    if activation.outcome.attempt().attempt_id != attempt.attempt_id {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let local = database
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if !local.is_activated() {
        return Err(DeviceJoinError::Store(format!(
            "joining device registration is not activated after materializing {:?}: {:?}",
            activation.outcome_activation.coord, local.state
        )));
    }
    if local.registration_hash != registration.registration_hash
        || local.device_id != registration.device_id
        || attempt.expected_registration.to_bytes() != local.registration_bytes
    {
        return Err(DeviceJoinError::Store(
            "joining device registration differs from the activated registration".to_string(),
        ));
    }
    Ok(JoinedStore {
        store_root: attempt.store_root.clone(),
        registration,
        activation,
    })
}

impl<'storage> PendingDeviceJoinObservation<'storage> {
    pub async fn open(
        pending: &DeviceJoinJournalDatabase,
        storage: &'storage std::sync::Arc<dyn CloudSyncObjectStorage>,
        root: &coven_protocol::store_commit::StoreRootRef,
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
        storage: &'storage std::sync::Arc<dyn CloudSyncObjectStorage>,
        history_verifier: crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier<
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

    pub async fn into_joining_store(
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
            .map_err(DeviceJoinError::from)?;
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

    pub fn authorize_closure(self, identity: &UserKeypair) -> PendingDeviceJoinClosure<'storage> {
        PendingDeviceJoinClosure {
            observation: self,
            identity: identity.clone(),
        }
    }

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
        if coven_database::device_join_journal::joiner_abandonment_transition(
            &current,
            &abandonment,
        )?
        .is_none()
        {
            return Ok(abandonment);
        }
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.history_verifier
                .verified_root()
                .reference()
                .store_root_hash,
            ProtocolObjectDomain::DeviceJoinAbandonment,
        );
        let prefix = coven_protocol::store_commit::device_join_abandonment_semantic_prefix(
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
        let next = coven_database::device_join_journal::joiner_abandonment_transition(
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
                let origin = coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Join {
                    attempt_id: offer.attempt_id,
                    attempt_slot: offer.attempt_slot.clone(),
                    outcome_slot: offer.outcome_slot.clone(),
                };
                let device_id =
                    coven_protocol::store_commit::StoreDeviceId::derive(&offer.store_root, &origin);
                let registration_context =
                    coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                        offer.store_root.store_root_hash,
                        ProtocolObjectDomain::StoreDeviceRegistration,
                    );
                let registration_slot = self
                    .storage
                    .allocate_protocol_slot(
                        &registration_context,
                        &coven_protocol::store_commit::registration_semantic_prefix(
                            &device_id.to_string(),
                        ),
                        ".json",
                    )
                    .await?;
                let head_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                    offer.store_root.store_root_hash,
                    ProtocolObjectDomain::StoreHead,
                );
                let first_head = self
                    .storage
                    .allocate_protocol_slot(
                        &head_context,
                        &coven_protocol::store_commit::head_slot_prefix(&device_id.to_string(), 1),
                        ".json",
                    )
                    .await?;
                let ack_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                    offer.store_root.store_root_hash,
                    ProtocolObjectDomain::StoreAck,
                );
                let first_ack = self
                    .storage
                    .allocate_protocol_slot(
                        &ack_context,
                        &coven_protocol::store_commit::ack_slot_prefix(&device_id.to_string(), 1),
                        ".json",
                    )
                    .await?;
                let snapshot_context =
                    coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                        offer.store_root.store_root_hash,
                        ProtocolObjectDomain::StoreSnapshotMeta,
                    );
                let first_snapshot = self
                    .storage
                    .allocate_protocol_slot(
                        &snapshot_context,
                        &coven_protocol::store_commit::snapshot_slot_prefix(
                            &device_id.to_string(),
                            0,
                        ),
                        ".json",
                    )
                    .await?;
                let registration = StoreDeviceRegistration::signed(
                    offer.store_root.clone(),
                    origin,
                    binding.device.clone(),
                    coven_protocol::store_commit::DeviceStreamAnchor::StoreAnnouncements {
                        first_slot: first_head,
                    },
                    coven_protocol::store_commit::DeviceStreamAnchor::StoreAcknowledgements {
                        first_slot: first_ack,
                    },
                    coven_protocol::store_commit::DeviceStreamAnchor::StoreSnapshots {
                        first_slot: first_snapshot,
                    },
                    identity,
                )
                .map_err(DeviceJoinError::from)?;
                prepare_registration_object(
                    self.storage.as_ref(),
                    &registration,
                    registration_slot.clone(),
                )?;
                let request = DeviceProviderAccessRequest::signed(
                    offer.clone(),
                    binding.device,
                    registration,
                    registration_slot,
                    identity,
                )?;
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
        if let Some(access_grant) = approval.access_grant() {
            self.history_verifier
                .verify_accepted_provider_access_activation(
                    access_grant,
                    &approval.request.offer.provider_admin,
                    &administrator,
                )
                .await?;
        }
        let storage = self.storage;
        let live = storage.provider_binding().await?;
        if live.store != approval.request.offer.provider
            || live.device != approval.request.peer_provider
        {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        let request = match &approval.admission {
            DeviceProviderAdmission::SamePrincipal => {
                DeviceRegistrationRequest::same_principal(approval.clone())?
            }
            DeviceProviderAdmission::CrossPrincipal { challenge, .. } => {
                let slot = storage
                    .reserve_cross_principal_response_slot(challenge.probe_id)
                    .await
                    .map_err(DeviceJoinError::ProviderProbe)?;
                DeviceRegistrationRequest::cross_principal(approval.clone(), slot, identity)?
            }
        };
        self.journal
            .record_registration_request(offer, approval, request)
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
            coven_protocol::store_commit::DeviceJoinDisposition::Cancelled
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
                    .map_err(DeviceJoinError::ProviderStorage)?;
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
                    .map_err(DeviceJoinError::ProviderStorage)?;
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
                                .map_err(DeviceJoinError::ProviderStorage)?,
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
                .map_err(DeviceJoinError::ProviderStorage)?;
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
    pub async fn close(
        &mut self,
        cancellation: DeviceJoinCancellation,
    ) -> Result<JoinerJoinTerminal, DeviceJoinError> {
        self.observation.close(&self.identity, cancellation).await
    }
}
