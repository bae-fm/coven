use super::history::DeviceJoinHistory;
use super::*;
use coven_protocol::store_commit::DeviceJoinAbandonmentRef;

mod admission;
mod same_principal;

pub use admission::DeviceProviderAccessAdministrator;

/// One device admits a join: it answers the access request, prepares the
/// storage grant, signs the approval, registers the joining device and
/// activates it. Every step below runs against the same journal row, under the
/// provider-administrator grant this device itself holds.
pub(crate) struct AuthorizedJoin<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
    database: StoreDatabase,
    storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
    root: StoreRootRef,
    verified_root: coven_protocol::objects::VerifiedObject<StoreProtocolRoot>,
    membership: coven_protocol::membership::MembershipChain,
    local_writer: std::sync::Arc<crate::sync::store::commit_publication::LocalStoreWriter>,
}

impl<'operation, 'storage> AuthorizedJoin<'operation, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
        database: StoreDatabase,
        storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
        root: StoreRootRef,
        verified_root: coven_protocol::objects::VerifiedObject<StoreProtocolRoot>,
        membership: coven_protocol::membership::MembershipChain,
        local_writer: std::sync::Arc<crate::sync::store::commit_publication::LocalStoreWriter>,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            root,
            verified_root,
            membership,
            local_writer,
        }
    }

    /// Every provider-administrator grant this device itself holds.
    fn provider_admin_grants(
        &self,
    ) -> Result<
        std::collections::BTreeMap<ProviderAdminGrantId, ProviderAdminGrantRecord>,
        DeviceJoinError,
    > {
        let coven_protocol::membership::MembershipStatus::Resolved(resolved) =
            self.membership.status()
        else {
            return Err(DeviceJoinError::MembershipConflict);
        };
        Ok(self
            .local_writer
            .provider_administrator_grants(resolved.provider_admin.combined_state()))
    }

    fn join_history(&mut self) -> DeviceJoinHistory<'_, 'storage> {
        self.writer.join_history()
    }

    fn journal(&self, attempt_id: DeviceJoinAttemptId) -> StoreJoinJournal<OwnerJoinProgress> {
        StoreJoinJournal::new(&self.database, attempt_id)
    }

    fn verify_device_admission_approval(
        &self,
        approval: &DeviceProviderAdmissionApproval,
    ) -> Result<(), DeviceJoinError> {
        self.local_writer
            .verify_own_device_admission_approval(approval, &self.verified_root)
    }

    pub(crate) async fn begin(
        &self,
        member_pubkey: &str,
    ) -> Result<DeviceJoinOffer, DeviceJoinError> {
        self.require_eligible_member(member_pubkey)?;
        let mut existing = self
            .database
            .device_join_actions()
            .await?
            .into_iter()
            .filter_map(|action| match action {
                DeviceJoinAction::TransferOffer(offer) if offer.member_pubkey == member_pubkey => {
                    Some(offer)
                }
                _ => None,
            });
        if let Some(offer) = existing.next() {
            if existing.next().is_some() {
                return Err(DeviceJoinError::JournalConflict);
            }
            return Ok(offer);
        }
        let owner_pubkey = self.local_writer.author_pubkey();
        let owner_grant = self
            .membership
            .active_owner_grant(&owner_pubkey)
            .ok_or(DeviceJoinError::OwnerAuthorityRequired)?;
        // The device that offers the join is the device that will admit it, so
        // the offer names a provider-administrator grant this device holds. A
        // device holding none cannot grant the joiner storage access and so
        // cannot make the offer at all.
        let provider_admin = self
            .provider_admin_grants()?
            .into_values()
            .next()
            .ok_or(DeviceJoinError::ProviderAdministratorRequired)?;
        let root = self.root.clone();
        let binding = self.storage.provider_binding().await?;
        let attempt_id = self.database.new_device_join_attempt_id();
        let attempt_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let attempt_slot = self
            .storage
            .allocate_protocol_slot(
                &attempt_context,
                &coven_protocol::store_commit::device_join_attempt_semantic_prefix(attempt_id),
                ".json",
            )
            .await?;
        let outcome_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinOutcome,
        );
        let outcome_slot = self
            .storage
            .allocate_protocol_slot(
                &outcome_context,
                &coven_protocol::store_commit::device_join_outcome_semantic_prefix(attempt_id),
                ".json",
            )
            .await?;
        let offer = self.local_writer.sign_device_join_offer(
            attempt_id,
            member_pubkey.to_string(),
            root,
            binding.store,
            attempt_slot,
            outcome_slot,
            owner_grant,
            provider_admin,
        )?;
        self.database
            .begin_device_join(DeviceJoinJournalRecord::owner_offered(offer.clone()))
            .await?;
        Ok(offer)
    }

    fn require_eligible_member(&self, member_pubkey: &str) -> Result<(), DeviceJoinError> {
        if self
            .membership
            .current_members()
            .iter()
            .any(|(pubkey, role)| pubkey == member_pubkey && role.can_write())
        {
            Ok(())
        } else {
            Err(DeviceJoinError::MemberNotEligible)
        }
    }

    /// The record for a grant this device holds. A grant held by some other
    /// device is not something this device can admit under: one party answers
    /// the access request, prepares the grant and signs the approval.
    fn resolve_provider_admin(
        &self,
        grant_id: &ProviderAdminGrantId,
    ) -> Result<ProviderAdminGrantRecord, DeviceJoinError> {
        self.provider_admin_grants()?
            .remove(grant_id)
            .ok_or(DeviceJoinError::ProviderAdministratorRequired)
    }

    async fn validate_registration_request(
        &mut self,
        request: &DeviceRegistrationRequest,
    ) -> Result<DeviceJoinOffer, DeviceJoinError> {
        request.verify()?;
        let offer = request.approval().request.offer.as_ref().clone();
        if self.root != offer.store_root {
            return Err(DeviceJoinError::OfferMismatch);
        }
        if !self
            .local_writer
            .is_authored_by_registration(&offer.owner_registration)
        {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        let provider_admin = self.resolve_provider_admin(&offer.provider_admin.grant_id)?;
        if provider_admin != *offer.provider_admin {
            return Err(DeviceJoinError::ProviderAdministratorRequired);
        }
        let administrator = self
            .join_history()
            .load_registration(&provider_admin.administrator)
            .await?
            .value;
        self.verify_device_admission_approval(request.approval())?;
        if let Some(access_grant) = request.approval().access_grant() {
            self.join_history()
                .verify_accepted_provider_access_activation(
                    access_grant,
                    &provider_admin,
                    &administrator,
                )
                .await?;
        }
        if !self.local_writer.is_current_owner(&self.membership) {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        Ok(offer)
    }

    pub(super) async fn abandon(
        &mut self,
        offer: DeviceJoinOffer,
    ) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
        let journal = self.journal(offer.attempt_id);
        let current = journal.current().await?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(existing)) =
            &*current.progress
        {
            return Ok(existing.clone());
        }
        if !self
            .local_writer
            .is_authored_by_registration(&offer.owner_registration)
        {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        self.local_writer.verify_device_join_offer(&offer)?;
        if !self.local_writer.is_current_owner(&self.membership) {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        let abandonment_object = self.local_writer.sign_device_join_abandonment(&offer)?;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAbandonment,
        );
        let prefix =
            coven_protocol::store_commit::device_join_abandonment_semantic_prefix(offer.attempt_id);
        let prepared = self.storage.prepare_protocol_object(
            &context,
            offer.attempt_slot.clone(),
            &prefix,
            abandonment_object.to_bytes(),
        )?;
        let abandonment_ref = DeviceJoinAbandonmentRef {
            attempt_id: offer.attempt_id,
            abandonment_hash: abandonment_object.abandonment_hash(),
            object: prepared.reference().clone(),
        };
        let intent = journal.record(OwnerJoinProgress::AbandonmentCreateIntent {
            offer: offer.clone(),
            abandonment: abandonment_ref.clone(),
            prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
        });
        let durable_offer = match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(durable)) => Some(durable),
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AccessRequested(request)) => {
                Some(request.offer.as_ref())
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AccessGrantPrepared {
                request,
                ..
            }) => Some(request.offer.as_ref()),
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ApprovalPrepared(approval)) => {
                Some(approval.request.offer.as_ref())
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(request)) => {
                Some(request.approval().request.offer.as_ref())
            }
            _ => None,
        };
        match &*current.progress {
            _ if durable_offer == Some(&offer) => {
                journal.advance_to(&current, &intent).await?;
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AbandonmentCreateIntent {
                offer: durable_offer,
                abandonment,
                prepared: durable_prepared,
            }) if durable_offer == &offer
                && abandonment == &abandonment_ref
                && durable_prepared == &PreparedDeviceJoinObject::from_prepared(&prepared) => {}
            _ => return Err(DeviceJoinError::JournalConflict),
        }
        self.storage
            .create_verified_protocol_object(
                &context,
                &prepared,
                &prefix,
                &abandonment_object.to_bytes(),
            )
            .await
            .map_err(|error| {
                DeviceJoinError::prepared_object(error, DeviceJoinError::AttemptMismatch)
            })?;
        self.local_writer
            .verify_device_join_abandonment(&abandonment_ref, &abandonment_object)?;
        let plan = self.writer.prepare_plan().await?;
        let activation = self
            .writer
            .activate(
                plan,
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::Abandonment(
                    abandonment_ref.clone(),
                ),
            )
            .await?;
        let abandonment = DeviceJoinAbandonment {
            abandonment: abandonment_ref,
            abandonment_activation: activation,
        };
        journal
            .advance(&intent, OwnerJoinProgress::Abandoned(abandonment.clone()))
            .await?;
        Ok(abandonment)
    }

    pub(super) async fn accept_registration(
        &mut self,
        request: DeviceRegistrationRequest,
    ) -> Result<ProvisionalDeviceBootstrap, DeviceJoinError> {
        let access_grant = request
            .approval()
            .access_grant()
            .ok_or(DeviceJoinError::ApprovalMismatch)?
            .clone();
        let offer = self.validate_registration_request(&request).await?;
        let journal = self.journal(offer.attempt_id);
        let durable = journal.current().await?;
        match &*durable.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap)) => {
                if *bootstrap.request == request {
                    return Ok(bootstrap.clone());
                }
                return Err(DeviceJoinError::JournalConflict);
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ApprovalPrepared(approval))
                if approval == request.approval() => {}
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(durable))
                if durable == &request => {}
            _ => return Err(DeviceJoinError::JournalConflict),
        }
        let plan = self.writer.prepare_plan().await?;
        #[cfg(any(test, feature = "test-utils"))]
        self.database
            .reach_test_point(coven_database::DatabaseTestPoint::DeviceJoinAttemptPositionHeld)
            .await;
        let cut = plan.predecessor_cut()?;
        if !self
            .join_history()
            .history_cut_covers(&cut, &access_grant.activation)
            .await?
        {
            return Err(DeviceJoinError::ApprovalActivationMissing);
        }
        let requested = if matches!(
            &*durable.progress,
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(_))
        ) {
            durable
        } else {
            journal
                .advance(
                    &durable,
                    OwnerJoinProgress::RegistrationRequested(request.clone()),
                )
                .await?
        };
        let attempt = self.local_writer.sign_device_join_attempt(
            offer.store_root.clone(),
            offer.attempt_id,
            offer.attempt_slot.clone(),
            request.expected_registration().clone(),
            request.registration_slot().clone(),
            offer.outcome_slot.clone(),
            cut,
            plan.membership_state().clone(),
            offer.provider_admin.grant_id.clone(),
            request.approval().clone(),
            request.response(),
            offer.owner_grant.clone(),
        )?;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let prefix =
            coven_protocol::store_commit::device_join_attempt_semantic_prefix(offer.attempt_id);
        let prepared = self.storage.prepare_protocol_object(
            &context,
            offer.attempt_slot.clone(),
            &prefix,
            attempt.to_bytes(),
        )?;
        self.storage
            .create_verified_protocol_object(&context, &prepared, &prefix, &attempt.to_bytes())
            .await
            .map_err(|error| {
                DeviceJoinError::prepared_object(error, DeviceJoinError::AttemptMismatch)
            })?;
        let attempt_ref = DeviceJoinAttemptRef {
            attempt_id: offer.attempt_id,
            attempt_hash: attempt.attempt_hash(),
            object: prepared.reference().clone(),
        };
        let activation = self
            .writer
            .activate(
                plan,
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::Attempt(attempt_ref.clone()),
            )
            .await?;
        let bootstrap = ProvisionalDeviceBootstrap {
            request: Box::new(request),
            publication_authorization: DeviceJoinChallengePublicationAuthorization {
                attempt: attempt_ref,
                attempt_activation: activation,
            },
        };
        journal
            .advance(
                &requested,
                OwnerJoinProgress::AttemptActivated(bootstrap.clone()),
            )
            .await?;
        Ok(bootstrap)
    }

    pub(super) async fn finalize(
        &mut self,
        completion: DeviceProviderAdmissionCompletion,
    ) -> Result<DeviceJoinActivation, DeviceJoinError> {
        let attempt_ref = completion.attempt().clone();
        let attempt_id = attempt_ref.attempt_id;
        let journal = self.journal(attempt_id);
        let current = journal.current().await?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationPrepared {
            completion: durable_completion,
            activation,
            ..
        }) = &*current.progress
        {
            if durable_completion == &completion {
                return Ok(activation.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Completed(durable_completion))
                if durable_completion == &completion => {}
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationCreateIntent {
                completion: durable_completion,
                ..
            }) if durable_completion == &completion => {}
            _ => return Err(DeviceJoinError::JournalConflict),
        }
        let local_writer = std::sync::Arc::clone(&self.local_writer);
        let attempt = local_writer
            .load_verified_device_join_attempt(&mut self.writer.join_history(), &attempt_ref)
            .await?
            .value;
        let offer = &attempt.provider_approval.request.offer.clone();
        if self.root != offer.store_root {
            return Err(DeviceJoinError::OfferMismatch);
        }
        if !self
            .local_writer
            .is_authored_by_registration(&offer.owner_registration)
        {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        match (&attempt.provider_approval.admission, &completion) {
            (
                DeviceProviderAdmission::SamePrincipal,
                DeviceProviderAdmissionCompletion::SamePrincipal { bootstrap },
            ) if bootstrap.bootstrap.publication_authorization.attempt == attempt_ref => {}
            (
                DeviceProviderAdmission::CrossPrincipal { challenge, .. },
                DeviceProviderAdmissionCompletion::CrossPrincipal { readiness, receipt },
            ) => {
                let registration = self
                    .join_history()
                    .load_registration(&readiness.proof.registration)
                    .await?
                    .value;
                let ack = self
                    .join_history()
                    .load_acknowledgement(&readiness.proof.initial_ack, &registration)
                    .await?;
                readiness.proof.verify(
                    &attempt_ref,
                    &attempt,
                    &registration,
                    &readiness.proof.initial_ack,
                    &ack,
                )?;
                let provider_admin = self.resolve_provider_admin(&offer.provider_admin.grant_id)?;
                if &provider_admin != offer.provider_admin.as_ref() {
                    return Err(DeviceJoinError::ProviderAdministratorRequired);
                }
                let administrator = self
                    .join_history()
                    .load_registration(&provider_admin.administrator)
                    .await?
                    .value;
                let response_slot = match &attempt.provider_response {
                    DeviceProviderResponseReservation::CrossPrincipal { response_slot } => {
                        response_slot.clone()
                    }
                    DeviceProviderResponseReservation::SamePrincipal => {
                        return Err(DeviceJoinError::AttemptMismatch);
                    }
                };
                let context = coven_protocol::provider::CrossPrincipalResponseContext {
                    challenge: attempt.provider_approval.request.cross_challenge_context(),
                    expected_registration_hash: attempt.expected_registration.registration_hash(),
                    response_slot,
                };
                receipt
                    .verify(
                        &context,
                        &offer.provider,
                        &administrator.device_signing_pubkey,
                        &offer.member_pubkey,
                    )
                    .map_err(DeviceJoinError::ProviderProbe)?;
                if &receipt.transcript.challenge != challenge {
                    return Err(DeviceJoinError::AttemptMismatch);
                }
            }
            _ => return Err(DeviceJoinError::AttemptMismatch),
        }
        let registration_prepared = super::prepare_registration_object(
            self.storage.as_ref(),
            &attempt.expected_registration,
            attempt.registration_slot.clone(),
        )?;
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &attempt.expected_registration,
            registration_prepared.reference().clone(),
        );
        let registration_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let outcome = self.local_writer.sign_device_join_outcome(
            attempt_ref.clone(),
            coven_protocol::store_commit::DeviceJoinDisposition::Activated {
                registration: registration_ref.clone(),
            },
            offer.owner_grant.clone(),
        )?;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinOutcome,
        );
        let prefix = coven_protocol::store_commit::device_join_outcome_semantic_prefix(attempt_id);
        let outcome_hash = outcome.outcome_hash();
        let (prepared, outcome_ref, intent) = match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Completed(_)) => {
                let prepared = self.storage.prepare_protocol_object(
                    &context,
                    attempt.outcome_slot.clone(),
                    &prefix,
                    outcome.to_bytes(),
                )?;
                let outcome_ref = DeviceJoinOutcomeRef::Activated {
                    attempt: attempt_ref.clone(),
                    outcome_hash,
                    object: prepared.reference().clone(),
                };
                let intent = journal
                    .advance(
                        &current,
                        OwnerJoinProgress::ActivationCreateIntent {
                            completion: completion.clone(),
                            outcome: outcome_ref.clone(),
                            prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
                        },
                    )
                    .await?;
                (prepared, outcome_ref, intent)
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationCreateIntent {
                outcome: durable_outcome,
                prepared: durable_prepared,
                ..
            }) => {
                let expected = DeviceJoinOutcomeRef::Activated {
                    attempt: attempt_ref.clone(),
                    outcome_hash,
                    object: durable_prepared.object.clone(),
                };
                if durable_outcome != &expected {
                    return Err(DeviceJoinError::JournalConflict);
                }
                (durable_prepared.restore()?, expected, current.clone())
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        self.storage
            .create_verified_protocol_object(
                &registration_context,
                &registration_prepared,
                &coven_protocol::store_commit::registration_semantic_prefix(
                    &attempt.expected_registration.device_id.to_string(),
                ),
                &attempt.expected_registration.to_bytes(),
            )
            .await
            .map_err(DeviceJoinError::Storage)?;
        self.storage
            .create_verified_protocol_object(&context, &prepared, &prefix, &outcome.to_bytes())
            .await
            .map_err(|error| {
                DeviceJoinError::prepared_object(error, DeviceJoinError::AttemptMismatch)
            })?;
        let joined_registration = registration_ref.clone();
        let activated_registration =
            coven_protocol::store_commit::ActivatedStoreDeviceRegistration::verified(
                coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
                    registration_ref,
                    attempt.expected_registration.clone(),
                )?,
                coven_protocol::store_commit::StoreDeviceRegistrationActivation::Join {
                    attempt_id,
                    outcome: outcome_ref.clone(),
                },
            )?;
        let plan = self.writer.prepare_plan().await?;
        let activation_ref = self
            .writer
            .activate(
                plan,
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::Outcome {
                    outcome: outcome_ref.clone(),
                    registration: Some(Box::new(activated_registration)),
                },
            )
            .await?;
        let activation = DeviceJoinActivation {
            outcome: outcome_ref,
            outcome_activation: activation_ref,
        };
        journal
            .advance(
                &intent,
                OwnerJoinProgress::ActivationPrepared {
                    completion,
                    activation: activation.clone(),
                    registration: joined_registration,
                },
            )
            .await?;
        Ok(activation)
    }
}

impl Store {
    #[doc(hidden)]
    pub(crate) async fn abandon_device_join(
        &self,
        offer: DeviceJoinOffer,
    ) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer.join_operation().abandon(offer).await
    }

    #[doc(hidden)]
    pub(crate) async fn accept_device_registration_request(
        &self,
        request: DeviceRegistrationRequest,
    ) -> Result<ProvisionalDeviceBootstrap, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer.join_operation().accept_registration(request).await
    }

    /// Finish a same-provider join whose activation was already prepared but
    /// whose journal has not reached its completion.
    #[doc(hidden)]
    pub(crate) async fn resume_same_principal_device_join(
        &self,
        request: DeviceRegistrationRequest,
    ) -> Result<SamePrincipalDeviceJoin, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer
            .join_operation()
            .activate_same_principal_join(request)
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn finalize_device_join(
        &self,
        completion: DeviceProviderAdmissionCompletion,
    ) -> Result<DeviceJoinActivation, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(DeviceJoinError::from)?;
        writer.join_operation().finalize(completion).await
    }
}
