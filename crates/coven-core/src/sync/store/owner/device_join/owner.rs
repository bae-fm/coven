use super::joiner::cross_challenge_context;
use super::journal::{database_error, provider_error};
use super::*;

impl<'operation, 'storage> AuthorizedJoin<'operation, 'storage> {
    pub(crate) async fn begin(
        &self,
        member_pubkey: &str,
        provider_admin_grant: ProviderAdminGrantId,
    ) -> Result<DeviceJoinOffer, DeviceJoinError> {
        self.require_eligible_member(member_pubkey)?;
        let owner_pubkey = keys::public_key_hex(self.writer.identity());
        let owner_grant = self
            .writer
            .membership()
            .active_owner_grant(&owner_pubkey)
            .ok_or(DeviceJoinError::OwnerAuthorityRequired)?;
        let provider_admin = self.resolve_provider_admin(&provider_admin_grant)?;
        let (owner_registration, owner, owner_device_signer) = self.writer.registration();
        let root = self.writer.store_root().clone();
        let binding = self.writer.storage().provider_binding().await?;
        let journal = self.writer.database().device_join_journal();
        let attempt_id = journal.new_attempt_id();
        let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let attempt_slot = self
            .writer
            .storage()
            .allocate_protocol_slot(
                &attempt_context,
                &crate::sync::store_commit::device_join_attempt_semantic_prefix(attempt_id),
                ".json",
            )
            .await?;
        let outcome_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinOutcome,
        );
        let outcome_slot = self
            .writer
            .storage()
            .allocate_protocol_slot(
                &outcome_context,
                &crate::sync::store_commit::device_join_outcome_semantic_prefix(attempt_id),
                ".json",
            )
            .await?;
        let offer = DeviceJoinOffer::signed(
            attempt_id,
            member_pubkey.to_string(),
            root,
            binding.store,
            attempt_slot,
            outcome_slot,
            owner_registration.clone(),
            owner_grant,
            provider_admin,
            owner,
            owner_device_signer,
        )?;
        journal
            .begin(DeviceJoinJournalRecord::owner_offered(offer.clone()))
            .await?;
        Ok(offer)
    }

    fn require_eligible_member(&self, member_pubkey: &str) -> Result<(), DeviceJoinError> {
        if self
            .writer
            .membership()
            .current_members()
            .iter()
            .any(|(pubkey, role)| pubkey == member_pubkey && role.can_write())
        {
            Ok(())
        } else {
            Err(DeviceJoinError::MemberNotEligible)
        }
    }

    fn resolve_provider_admin(
        &self,
        grant_id: &ProviderAdminGrantId,
    ) -> Result<ProviderAdminGrantRecord, DeviceJoinError> {
        let crate::sync::membership::MembershipStatus::Resolved(resolved) =
            self.writer.membership().status()
        else {
            return Err(DeviceJoinError::MembershipConflict);
        };
        let state = resolved.provider_admin.combined_state();
        state
            .records()
            .get(grant_id)
            .filter(|record| state.authorizes(grant_id, &record.administrator))
            .cloned()
            .ok_or(DeviceJoinError::ProviderAdministratorRequired)
    }

    pub(super) async fn abandon(
        &mut self,
        offer: DeviceJoinOffer,
    ) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
        let database = self.writer.database().clone();
        let journal = database.device_join_journal();
        let current = journal
            .load(offer.attempt_id, DeviceJoinRole::Owner)
            .await?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(existing)) =
            &*current.progress
        {
            return Ok(existing.clone());
        }
        let (owner_registration, owner, owner_signer) = self.writer.registration();
        if owner_registration != &offer.owner_registration {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        offer.verify(owner)?;
        if !self
            .writer
            .membership()
            .is_owner_now(&keys::public_key_hex(self.writer.identity()))
        {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        let abandonment_object = DeviceJoinAbandonmentObject::signed(&offer, owner, owner_signer)?;
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAbandonment,
        );
        let prefix =
            crate::sync::store_commit::device_join_abandonment_semantic_prefix(offer.attempt_id);
        let prepared = self.writer.storage().prepare_protocol_object(
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
        let intent = DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::AbandonmentCreateIntent {
                    offer: offer.clone(),
                    abandonment: abandonment_ref.clone(),
                    prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
                },
            )),
        };
        match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(durable))
                if durable == &offer =>
            {
                journal.advance(&current, intent.clone()).await?;
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(request))
                if request.approval.request.offer.as_ref() == &offer =>
            {
                journal.advance(&current, intent.clone()).await?;
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
        self.writer
            .storage()
            .create_protocol_object(&prepared)
            .await?;
        let opened = self
            .writer
            .storage()
            .read_protocol_object(&context, prepared.reference(), &prefix)
            .await?;
        if opened != abandonment_object.to_bytes() {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        abandonment_ref.verify(&abandonment_object, owner)?;
        let plan = self.writer.prepare_plan().await?;
        let activation = self
            .writer
            .activate(
                plan,
                crate::sync::store::operations::StoreOperationBatch::Abandonment(
                    abandonment_ref.clone(),
                ),
            )
            .await?;
        let abandonment = DeviceJoinAbandonment {
            abandonment: abandonment_ref,
            abandonment_activation: activation,
        };
        journal
            .advance(
                &intent,
                DeviceJoinJournalRecord {
                    attempt_id: offer.attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Owner(
                        OwnerJoinProgress::Abandoned(abandonment.clone()),
                    )),
                },
            )
            .await?;
        Ok(abandonment)
    }

    pub(super) async fn accept_registration(
        &mut self,
        request: DeviceRegistrationRequest,
    ) -> Result<ProvisionalDeviceBootstrap, DeviceJoinError> {
        request.verify()?;
        let offer = &request.approval.request.offer;
        if self.writer.store_root() != &offer.store_root {
            return Err(DeviceJoinError::OfferMismatch);
        }
        let (owner_registration, owner, owner_signer) = {
            let (reference, registration, signer) = self.writer.registration();
            (reference.clone(), registration.clone(), signer.clone())
        };
        if owner_registration != offer.owner_registration {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        let provider_admin = self.resolve_provider_admin(&offer.provider_admin.grant_id)?;
        if &provider_admin != offer.provider_admin.as_ref() {
            return Err(DeviceJoinError::ProviderAdministratorRequired);
        }
        let administrator = self
            .writer
            .history_verifier_mut()
            .load_registration(&provider_admin.administrator)
            .await?
            .value;
        self.writer
            .verify_device_admission_approval(&request.approval, &owner, &administrator)?;
        self.writer
            .history_verifier_mut()
            .verify_accepted_provider_access_activation(
                &request.approval.access_grant,
                &provider_admin,
                &administrator,
            )
            .await?;
        if !self
            .writer
            .membership()
            .is_owner_now(&keys::public_key_hex(self.writer.identity()))
        {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        let database = self.writer.database().clone();
        let journal = database.device_join_journal();
        let offered = DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(
                *offer.clone(),
            ))),
        };
        let durable = journal.begin(offered.clone()).await?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap)) =
            *durable.progress
        {
            if *bootstrap.request == request {
                return Ok(bootstrap);
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        if durable != offered {
            return Err(DeviceJoinError::JournalConflict);
        }
        let plan = self.writer.prepare_plan().await?;
        #[cfg(any(test, feature = "test-utils"))]
        database
            .sqlite()
            .reach_test_point(crate::database::DatabaseTestPoint::DeviceJoinAttemptPositionHeld)
            .await;
        let cut = plan.predecessor_cut()?;
        if !self
            .writer
            .history_verifier_mut()
            .history_cut_covers(&cut, &request.approval.access_grant.activation)
            .await?
        {
            return Err(DeviceJoinError::ApprovalActivationMissing);
        }
        let requested = DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::RegistrationRequested(request.clone()),
            )),
        };
        journal.advance(&offered, requested.clone()).await?;
        let attempt = DeviceJoinAttempt::signed(
            offer.store_root.clone(),
            offer.attempt_id,
            offer.attempt_slot.clone(),
            request.expected_registration.clone(),
            request.registration_slot.clone(),
            offer.outcome_slot.clone(),
            cut,
            plan.membership_state().clone(),
            offer.provider_admin.grant_id.clone(),
            *request.approval.clone(),
            request.response.clone(),
            offer.owner_registration.clone(),
            offer.owner_grant.clone(),
            &owner,
            &owner_signer,
        )?;
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let prefix =
            crate::sync::store_commit::device_join_attempt_semantic_prefix(offer.attempt_id);
        let prepared = self.writer.storage().prepare_protocol_object(
            &context,
            offer.attempt_slot.clone(),
            &prefix,
            attempt.to_bytes(),
        )?;
        self.writer
            .storage()
            .create_protocol_object(&prepared)
            .await?;
        let opened = self
            .writer
            .storage()
            .read_protocol_object(&context, prepared.reference(), &prefix)
            .await?;
        if opened != attempt.to_bytes() {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let attempt_ref = DeviceJoinAttemptRef {
            attempt_id: offer.attempt_id,
            attempt_hash: attempt.attempt_hash(),
            object: prepared.reference().clone(),
        };
        let activation = self
            .writer
            .activate(
                plan,
                crate::sync::store::operations::StoreOperationBatch::Attempt(attempt_ref.clone()),
            )
            .await?;
        let attempt_id = offer.attempt_id;
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
                DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Owner(
                        OwnerJoinProgress::AttemptActivated(bootstrap.clone()),
                    )),
                },
            )
            .await?;
        Ok(bootstrap)
    }

    pub(super) async fn cancel(
        &mut self,
        attempt_ref: DeviceJoinAttemptRef,
    ) -> Result<DeviceJoinCancellation, DeviceJoinError> {
        let database = self.writer.database().clone();
        let journal = database.device_join_journal();
        let current = journal
            .load(attempt_ref.attempt_id, DeviceJoinRole::Owner)
            .await?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(existing)) =
            &*current.progress
        {
            if existing.outcome.attempt() == &attempt_ref {
                return Ok(existing.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        let expected_attempt = match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap)) => {
                &bootstrap.publication_authorization.attempt
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancellationCreateIntent {
                attempt,
                ..
            }) => attempt,
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        if expected_attempt != &attempt_ref {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let (owner_registration, owner, owner_signer) = {
            let (reference, registration, signer) = self.writer.registration();
            (reference.clone(), registration.clone(), signer.clone())
        };
        let attempt = self
            .writer
            .history_verifier_mut()
            .load_verified_device_join_attempt(&attempt_ref, &owner)
            .await?
            .value;
        if attempt.owner_registration != owner_registration
            || !self
                .writer
                .membership()
                .is_owner_now(&keys::public_key_hex(self.writer.identity()))
        {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        let outcome = crate::sync::store_commit::DeviceJoinOutcome::signed(
            attempt_ref.clone(),
            crate::sync::store_commit::DeviceJoinOutcomeBody::Cancelled,
            attempt.owner_registration.clone(),
            attempt.owner_grant.clone(),
            &owner,
            &owner_signer,
        )?;
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            self.writer.store_root().store_root_hash,
            ProtocolObjectDomain::DeviceJoinOutcome,
        );
        let prefix =
            crate::sync::store_commit::device_join_outcome_semantic_prefix(attempt_ref.attempt_id);
        let prepared = self.writer.storage().prepare_protocol_object(
            &context,
            attempt.outcome_slot.clone(),
            &prefix,
            outcome.to_bytes(),
        )?;
        let outcome_ref = DeviceJoinOutcomeRef::Cancelled {
            attempt: attempt_ref.clone(),
            outcome_hash: outcome.outcome_hash(),
            object: prepared.reference().clone(),
        };
        let intent = DeviceJoinJournalRecord {
            attempt_id: attempt_ref.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::CancellationCreateIntent {
                    attempt: attempt_ref.clone(),
                    cancellation: outcome_ref.clone(),
                    prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
                },
            )),
        };
        match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(_)) => {
                journal.advance(&current, intent.clone()).await?;
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancellationCreateIntent {
                attempt,
                cancellation,
                prepared: durable_prepared,
            }) if attempt == &attempt_ref
                && cancellation == &outcome_ref
                && durable_prepared == &PreparedDeviceJoinObject::from_prepared(&prepared) => {}
            _ => return Err(DeviceJoinError::JournalConflict),
        }
        self.writer
            .storage()
            .create_protocol_object(&prepared)
            .await?;
        let opened = self
            .writer
            .storage()
            .read_protocol_object(&context, prepared.reference(), &prefix)
            .await?;
        if opened != outcome.to_bytes() {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let verified_outcome = self
            .writer
            .history_verifier_mut()
            .load_device_join_outcome(&outcome_ref, &owner)
            .await?;
        if verified_outcome.value != outcome {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let plan = self.writer.prepare_plan().await?;
        let outcome_activation = self
            .writer
            .activate(
                plan,
                crate::sync::store::operations::StoreOperationBatch::Outcome {
                    outcome: outcome_ref.clone(),
                    registration: None,
                },
            )
            .await?;
        let cancellation = DeviceJoinCancellation {
            outcome: outcome_ref,
            outcome_activation,
        };
        journal
            .advance(
                &intent,
                DeviceJoinJournalRecord {
                    attempt_id: attempt_ref.attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Owner(
                        OwnerJoinProgress::Cancelled(cancellation.clone()),
                    )),
                },
            )
            .await?;
        Ok(cancellation)
    }

    pub(super) async fn finalize(
        &mut self,
        completion: DeviceProviderAdmissionCompletion,
    ) -> Result<DeviceJoinActivation, DeviceJoinError> {
        let database = self.writer.database().clone();
        let journal = database.device_join_journal();
        let attempt_ref = completion.readiness.proof.attempt.clone();
        let attempt_id = attempt_ref.attempt_id;
        let current = journal
            .load(attempt_id, DeviceJoinRole::Owner)
            .await?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationPrepared {
            completion: durable_completion,
            activation,
        }) = &*current.progress
        {
            if durable_completion == &completion {
                return Ok(activation.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        let provisional = match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap)) => {
                bootstrap.clone()
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationCreateIntent {
                bootstrap,
                completion: durable_completion,
                ..
            }) if durable_completion == &completion => bootstrap.clone(),
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        let offer = &provisional.request.approval.request.offer;
        if self.writer.store_root() != &offer.store_root {
            return Err(DeviceJoinError::OfferMismatch);
        }
        let (owner_registration, owner, owner_signer) = {
            let (reference, registration, signer) = self.writer.registration();
            (reference.clone(), registration.clone(), signer.clone())
        };
        if owner_registration != offer.owner_registration {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        let attempt = self
            .writer
            .history_verifier_mut()
            .load_verified_device_join_attempt(&attempt_ref, &owner)
            .await?
            .value;
        let registration = self
            .writer
            .history_verifier_mut()
            .load_registration(&completion.readiness.proof.registration)
            .await?
            .value;
        let ack = self
            .writer
            .history_verifier_mut()
            .load_store_ack(&completion.readiness.proof.initial_ack, &registration)
            .await?
            .value;
        completion.readiness.proof.verify(
            &attempt_ref,
            &attempt,
            &registration,
            &completion.readiness.proof.initial_ack,
            &ack,
        )?;
        match (&attempt.provider_approval.admission, &completion.admission) {
            (
                DeviceProviderAdmissionChallenge::SamePrincipal,
                DeviceProviderAdmission::SamePrincipal,
            ) => {}
            (
                DeviceProviderAdmissionChallenge::CrossPrincipal(challenge),
                DeviceProviderAdmission::CrossPrincipal(receipt),
            ) => {
                let provider_admin = self.resolve_provider_admin(&offer.provider_admin.grant_id)?;
                if &provider_admin != offer.provider_admin.as_ref() {
                    return Err(DeviceJoinError::ProviderAdministratorRequired);
                }
                let administrator = self
                    .writer
                    .history_verifier_mut()
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
                let context = crate::sync::provider::CrossPrincipalResponseContext {
                    challenge: cross_challenge_context(&attempt.provider_approval.request),
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
                    .map_err(provider_error)?;
                if &receipt.transcript.challenge != challenge {
                    return Err(DeviceJoinError::AttemptMismatch);
                }
            }
            _ => return Err(DeviceJoinError::AttemptMismatch),
        }
        let outcome = crate::sync::store_commit::DeviceJoinOutcome::signed(
            attempt_ref.clone(),
            crate::sync::store_commit::DeviceJoinOutcomeBody::Activated {
                readiness: completion.readiness.proof.clone(),
            },
            offer.owner_registration.clone(),
            offer.owner_grant.clone(),
            &owner,
            &owner_signer,
        )?;
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            offer.store_root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinOutcome,
        );
        let prefix = crate::sync::store_commit::device_join_outcome_semantic_prefix(attempt_id);
        let outcome_hash = outcome.outcome_hash();
        let (prepared, outcome_ref, intent) = match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(_)) => {
                let prepared = self.writer.storage().prepare_protocol_object(
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
                let intent = DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Owner(
                        OwnerJoinProgress::ActivationCreateIntent {
                            bootstrap: provisional.clone(),
                            completion: completion.clone(),
                            outcome: outcome_ref.clone(),
                            prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
                        },
                    )),
                };
                journal.advance(&current, intent.clone()).await?;
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
                let prepared = crate::sync::storage::PreparedExactObject::new(
                    durable_prepared.object.clone(),
                    durable_prepared.stored_bytes.clone(),
                )?;
                (prepared, expected, current.clone())
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        self.writer
            .storage()
            .create_protocol_object(&prepared)
            .await?;
        let opened = self
            .writer
            .storage()
            .read_protocol_object(&context, prepared.reference(), &prefix)
            .await?;
        if opened != outcome.to_bytes() {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let activated_registration =
            crate::sync::store::operations::DeviceJoinRegistrationActivation {
                reference: crate::sync::store_commit::ActivatedStoreDeviceRegistrationRef {
                    registration: completion.readiness.proof.registration.clone(),
                    authority:
                        crate::sync::store_commit::StoreDeviceRegistrationActivationRef::Join {
                            attempt_id,
                            outcome: outcome_ref.clone(),
                        },
                },
                registration: attempt.expected_registration.clone(),
                authority: crate::sync::store_commit::StoreDeviceRegistrationActivation::Join {
                    attempt_id,
                    outcome: outcome_ref.clone(),
                },
            };
        let plan = self.writer.prepare_plan().await?;
        let activation_ref = self
            .writer
            .activate(
                plan,
                crate::sync::store::operations::StoreOperationBatch::Outcome {
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
                DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Owner(
                        OwnerJoinProgress::ActivationPrepared {
                            completion,
                            activation: activation.clone(),
                        },
                    )),
                },
            )
            .await?;
        Ok(activation)
    }

    pub(super) async fn complete_cleanup(
        &mut self,
        activation: DeviceJoinCleanupActivation,
    ) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
        let journal = self.writer.database().device_join_journal();
        let attempt_id = activation.receipt.attempt_id;
        let current = journal
            .load(attempt_id, DeviceJoinRole::Owner)
            .await?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancelledComplete(existing)) =
            &*current.progress
        {
            if existing == &activation {
                return Ok(existing.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupActivated(durable)) =
            &*current.progress
        else {
            return Err(DeviceJoinError::JournalConflict);
        };
        if durable != &activation {
            return Err(DeviceJoinError::JournalConflict);
        }
        journal
            .advance(
                &current,
                DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Owner(
                        OwnerJoinProgress::CancelledComplete(activation.clone()),
                    )),
                },
            )
            .await?;
        Ok(activation)
    }

    pub(super) async fn prepare_cleanup(
        &mut self,
        cancellation: DeviceJoinCancellation,
        administrator_terminal: ProviderAdminJoinTerminal,
        joiner_terminal: JoinerJoinTerminal,
    ) -> Result<DeviceJoinCleanupReceipt, DeviceJoinError> {
        super::cleanup::require_cancelled_outcome(&cancellation.outcome)?;
        let attempt_ref = cancellation.outcome.attempt().clone();
        let database = self.writer.database().clone();
        let journal = database.device_join_journal();
        let current = journal
            .load(attempt_ref.attempt_id, DeviceJoinRole::Owner)
            .await?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceipt(existing)) =
            &*current.progress
        {
            return Ok(existing.clone());
        }
        let durable_cancellation = match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(durable)) => durable,
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceiptCreateIntent {
                cancellation: durable,
                ..
            }) => durable,
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        if durable_cancellation != &cancellation {
            return Err(DeviceJoinError::JournalConflict);
        }
        let (attempt, owner) = self
            .writer
            .history_verifier_mut()
            .load_device_join_attempt_and_owner(&attempt_ref)
            .await?;
        let outcome = self
            .writer
            .history_verifier_mut()
            .load_device_join_outcome(&cancellation.outcome, &owner.value)
            .await?
            .value;
        if !matches!(
            outcome.body,
            crate::sync::store_commit::DeviceJoinOutcomeBody::Cancelled
        ) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        super::cleanup::validate_terminals(
            &cancellation.outcome,
            &administrator_terminal,
            &joiner_terminal,
        )?;
        self.verify_cleanup_terminals(&administrator_terminal, &joiner_terminal)
            .await?;
        let (executor_ref, executor, executor_signer) = {
            let (reference, registration, signer) = self.writer.registration();
            (reference.clone(), registration.clone(), signer.clone())
        };
        if !self
            .writer
            .membership()
            .is_owner_now(&executor.author_pubkey)
        {
            return Err(DeviceJoinError::OwnerAuthorityRequired);
        }
        let executor_admin = self.resolve_provider_admin(
            &attempt
                .value
                .provider_approval
                .request
                .offer
                .provider_admin
                .grant_id,
        )?;
        if executor_admin != *attempt.value.provider_approval.request.offer.provider_admin
            || executor_admin.administrator != executor_ref
            || executor_admin.provider != executor.provider
        {
            return Err(DeviceJoinError::ProviderAdministratorRequired);
        }
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            self.writer.store_root().store_root_hash,
            ProtocolObjectDomain::DeviceJoinCleanupReceipt,
        );
        let prefix = crate::sync::store_commit::device_join_cleanup_receipt_semantic_prefix(
            attempt_ref.attempt_id,
        );
        let (receipt_object, receipt_ref, prepared, intent) = match &*current.progress {
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(_)) => {
                let plan = self.writer.prepare_plan().await?;
                let receipt_object = DeviceJoinCleanupReceiptObject::signed(
                    &attempt.value,
                    cancellation.outcome.clone(),
                    administrator_terminal.clone(),
                    joiner_terminal.clone(),
                    super::cleanup::canonical_cleanup_slots(&attempt.value)?,
                    plan.membership_state().clone(),
                    attempt
                        .value
                        .provider_approval
                        .request
                        .offer
                        .provider_admin
                        .grant_id
                        .clone(),
                    executor_ref,
                    &executor,
                    &executor_signer,
                )?;
                let slot = self
                    .writer
                    .storage()
                    .allocate_protocol_slot(&context, &prefix, ".json")
                    .await?;
                let prepared = self.writer.storage().prepare_protocol_object(
                    &context,
                    slot,
                    &prefix,
                    receipt_object.to_bytes(),
                )?;
                let receipt_ref = DeviceJoinCleanupReceiptRef {
                    attempt_id: attempt_ref.attempt_id,
                    receipt_hash: receipt_object.receipt_hash(),
                    object: prepared.reference().clone(),
                };
                let intent = DeviceJoinJournalRecord {
                    attempt_id: attempt_ref.attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Owner(
                        OwnerJoinProgress::CleanupReceiptCreateIntent {
                            cancellation: cancellation.clone(),
                            receipt: receipt_ref.clone(),
                            receipt_bytes: receipt_object.to_bytes(),
                            prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
                        },
                    )),
                };
                journal.advance(&current, intent.clone()).await?;
                (Box::new(receipt_object), receipt_ref, prepared, intent)
            }
            DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceiptCreateIntent {
                receipt,
                receipt_bytes,
                prepared,
                ..
            }) => {
                let receipt_object: DeviceJoinCleanupReceiptObject =
                    serde_json::from_slice(receipt_bytes)?;
                if receipt_object.to_bytes() != *receipt_bytes
                    || receipt_object.cancellation != cancellation.outcome
                    || receipt_object.administrator_terminal != administrator_terminal
                    || receipt_object.joiner_terminal != joiner_terminal
                    || receipt.attempt_id != attempt_ref.attempt_id
                    || receipt.receipt_hash != receipt_object.receipt_hash()
                    || receipt.object != prepared.object
                {
                    return Err(DeviceJoinError::JournalConflict);
                }
                (
                    Box::new(receipt_object),
                    receipt.clone(),
                    crate::sync::storage::PreparedExactObject::new(
                        prepared.object.clone(),
                        prepared.stored_bytes.clone(),
                    )?,
                    current.clone(),
                )
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        receipt_object.verify(&attempt.value, &executor)?;
        for slot in &receipt_object.deleted_slots {
            self.writer
                .storage()
                .exact_slot_storage()
                .delete_and_verify_absent(slot)
                .await
                .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
        }
        self.writer
            .storage()
            .create_protocol_object(&prepared)
            .await?;
        let opened = self
            .writer
            .storage()
            .read_protocol_object(&context, prepared.reference(), &prefix)
            .await?;
        if opened != receipt_object.to_bytes() {
            return Err(DeviceJoinError::CleanupMismatch);
        }
        receipt_ref.verify(&receipt_object, &executor)?;
        let receipt = DeviceJoinCleanupReceipt {
            receipt: receipt_ref,
        };
        journal
            .advance(
                &intent,
                DeviceJoinJournalRecord {
                    attempt_id: attempt_ref.attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Owner(
                        OwnerJoinProgress::CleanupReceipt(receipt.clone()),
                    )),
                },
            )
            .await?;
        Ok(receipt)
    }

    async fn verify_cleanup_terminals(
        &self,
        administrator: &ProviderAdminJoinTerminal,
        joiner: &JoinerJoinTerminal,
    ) -> Result<(), DeviceJoinError> {
        match administrator {
            ProviderAdminJoinTerminal::Completed(_) => {}
            ProviderAdminJoinTerminal::Cancelled(closure) => {
                let registration = self
                    .writer
                    .database()
                    .activated_store_device_registration(closure.administrator_registration.clone())
                    .await
                    .map_err(database_error)?;
                closure.verify(&registration)?;
            }
            ProviderAdminJoinTerminal::WriteRevoked(revocation) => {
                let registration = self
                    .writer
                    .database()
                    .activated_store_device_registration(revocation.executor.clone())
                    .await
                    .map_err(database_error)?;
                revocation.verify(&registration)?;
            }
        }
        match joiner {
            JoinerJoinTerminal::Ready(_) => {}
            JoinerJoinTerminal::Cancelled(closure) => closure.verify()?,
            JoinerJoinTerminal::WriteRevoked(revocation) => {
                let registration = self
                    .writer
                    .database()
                    .activated_store_device_registration(revocation.executor.clone())
                    .await
                    .map_err(database_error)?;
                revocation.verify(&registration)?;
            }
        }
        Ok(())
    }

    pub(super) async fn activate_cleanup(
        &mut self,
        receipt: DeviceJoinCleanupReceipt,
    ) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
        let attempt_id = receipt.receipt.attempt_id;
        let database = self.writer.database().clone();
        let journal = database.device_join_journal();
        let current = journal
            .load(attempt_id, DeviceJoinRole::Owner)
            .await?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupActivated(existing)) =
            &*current.progress
        {
            if existing.receipt == receipt.receipt {
                return Ok(existing.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceipt(durable)) =
            &*current.progress
        else {
            return Err(DeviceJoinError::JournalConflict);
        };
        if durable != &receipt {
            return Err(DeviceJoinError::JournalConflict);
        }
        let plan = self.writer.prepare_plan().await?;
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            self.writer.store_root().store_root_hash,
            ProtocolObjectDomain::DeviceJoinCleanupReceipt,
        );
        let prefix = crate::sync::store_commit::device_join_cleanup_receipt_semantic_prefix(
            receipt.receipt.attempt_id,
        );
        let bytes = self
            .writer
            .storage()
            .read_protocol_object(&context, &receipt.receipt.object, &prefix)
            .await?;
        let receipt_object: DeviceJoinCleanupReceiptObject = serde_json::from_slice(&bytes)?;
        let executor = database
            .activated_store_device_registration(receipt_object.executor.clone())
            .await
            .map_err(database_error)?;
        receipt.receipt.verify(&receipt_object, &executor)?;
        if receipt_object.store_root_hash != self.writer.store_root().store_root_hash
            || plan.membership_state() != &receipt_object.membership
        {
            return Err(DeviceJoinError::CleanupMismatch);
        }
        let activation_ref = self
            .writer
            .activate(
                plan,
                crate::sync::store::operations::StoreOperationBatch::CleanupReceipt(
                    receipt.receipt.clone(),
                ),
            )
            .await?;
        let activation = DeviceJoinCleanupActivation {
            receipt: receipt.receipt,
            activation: activation_ref,
        };
        journal
            .advance(
                &current,
                DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Owner(
                        OwnerJoinProgress::CleanupActivated(activation.clone()),
                    )),
                },
            )
            .await?;
        Ok(activation)
    }
}

impl Store {
    #[doc(hidden)]
    pub async fn abandon_device_join(
        &self,
        offer: DeviceJoinOffer,
    ) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer.join_operation().abandon(offer).await
    }

    #[doc(hidden)]
    pub async fn accept_device_registration_request(
        &self,
        request: DeviceRegistrationRequest,
    ) -> Result<ProvisionalDeviceBootstrap, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer.join_operation().accept_registration(request).await
    }

    #[doc(hidden)]
    pub async fn cancel_device_join(
        &self,
        attempt: DeviceJoinAttemptRef,
    ) -> Result<DeviceJoinCancellation, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer.join_operation().cancel(attempt).await
    }

    #[doc(hidden)]
    pub async fn finalize_device_join(
        &self,
        completion: DeviceProviderAdmissionCompletion,
    ) -> Result<DeviceJoinActivation, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer.join_operation().finalize(completion).await
    }

    #[doc(hidden)]
    pub async fn complete_owner_device_join_cleanup(
        &self,
        activation: DeviceJoinCleanupActivation,
    ) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer.join_operation().complete_cleanup(activation).await
    }
}
