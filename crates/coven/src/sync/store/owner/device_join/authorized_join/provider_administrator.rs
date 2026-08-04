use super::{provider_error, require_cancelled_outcome, *};

pub(crate) struct AuthorizedProviderAdministratorJoin<'operation, 'storage> {
    join: AuthorizedJoin<'operation, 'storage>,
    grants: std::collections::BTreeMap<ProviderAdminGrantId, ProviderAdminGrantRecord>,
}

impl<'operation, 'storage> AuthorizedProviderAdministratorJoin<'operation, 'storage> {
    pub(super) fn new(
        join: AuthorizedJoin<'operation, 'storage>,
        grants: std::collections::BTreeMap<ProviderAdminGrantId, ProviderAdminGrantRecord>,
    ) -> Self {
        Self { join, grants }
    }

    fn history(&mut self) -> history::DeviceJoinHistory<'_, 'storage> {
        self.join.history()
    }

    fn verify_device_admission_approval(
        &self,
        approval: &DeviceProviderAdmissionApproval,
        owner: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinError> {
        self.join
            .local_writer
            .verify_device_admission_approval_as_administrator(
                approval,
                &self.join.verified_root,
                owner,
            )
    }

    fn sign_device_admission_approval(
        &self,
        request: DeviceProviderAccessRequest,
        access_grant: ActivatedStoreMemberProviderAccessGrant,
        admission: DeviceProviderAdmissionChallenge,
    ) -> Result<DeviceProviderAdmissionApproval, DeviceJoinError> {
        self.join.local_writer.sign_device_admission_approval(
            request,
            access_grant,
            admission,
            &self.join.verified_root,
        )
    }

    async fn activate(
        &mut self,
        batch: crate::sync::store::operations::StoreOperationBatch,
    ) -> Result<StoreBatchCommitRef, crate::sync::store::StoreError> {
        let plan = self.join.writer.prepare_plan().await?;
        self.join.writer.activate(plan, batch).await
    }

    fn require_grant(
        &self,
        grant_id: &ProviderAdminGrantId,
    ) -> Result<&ProviderAdminGrantRecord, DeviceJoinError> {
        self.grants
            .get(grant_id)
            .ok_or(DeviceJoinError::ProviderAdministratorRequired)
    }

    async fn publish_cross_principal_challenge(
        &mut self,
        authorization: &DeviceJoinChallengePublicationAuthorization,
        challenge: &CrossPrincipalProbeChallenge,
        context: &crate::protocol::provider::CrossPrincipalChallengeContext,
        store: &StoreProviderBinding,
        attempt_owner: &StoreDeviceRegistration,
    ) -> Result<CrossPrincipalProbeChallenge, DeviceJoinError> {
        self.join
            .local_writer
            .verify_cross_principal_challenge(challenge, context, store)
            .map_err(provider_error)?;
        if authorization.attempt.attempt_id != context.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let attempt = self
            .history()
            .load_verified_attempt(&authorization.attempt, attempt_owner)
            .await?;
        if attempt.value.store_root != context.root
            || attempt.value.attempt_id != context.attempt_id
            || attempt.value.owner_registration != context.owner_registration
        {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let activation = self
            .history()
            .load_commit(&authorization.attempt_activation)
            .await?;
        if activation.author() != attempt_owner
            || !activation
                .device_join_attempt_decisions()
                .iter()
                .any(|decision| {
                    matches!(
                        decision,
                        DeviceJoinAttemptDecisionRef::Attempt(reference)
                            if reference == &authorization.attempt
                    )
                })
        {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        self.join
            .storage
            .settle_cross_principal_challenge(
                &self.join.database,
                authorization,
                challenge,
                context,
                store,
            )
            .await
            .map_err(provider_error)
    }

    pub(super) async fn authorize_access(
        &mut self,
        request: DeviceProviderAccessRequest,
        access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
    ) -> Result<DeviceProviderAdmissionApproval, DeviceJoinError> {
        let provider_admin = self
            .require_grant(&request.offer.provider_admin.grant_id)?
            .clone();
        if provider_admin != *request.offer.provider_admin {
            return Err(DeviceJoinError::OfferMismatch);
        }
        let owner = self
            .history()
            .load_registration(&request.offer.owner_registration)
            .await?
            .value;
        request.verify(&owner)?;
        if !self
            .join
            .local_writer
            .is_authored_by_registration(&provider_admin.administrator)
        {
            return Err(DeviceJoinError::ProviderAdministratorRequired);
        }
        let database = self.join.database.clone();
        let initial = DeviceJoinJournalRecord {
            attempt_id: request.offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::AccessRequested(request.clone()),
            )),
        };
        let durable = database.begin_device_join(initial.clone()).await?;
        let (grant, prepared, prepared_progress) = match &*durable.progress {
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::ApprovalPrepared(approval),
            ) => return Ok(approval.clone()),
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::AccessGrantPrepared {
                    request: durable_request,
                    grant,
                    prepared,
                },
            ) if durable_request == &request => (
                grant.clone(),
                crate::storage::PreparedExactObject::new(
                    prepared.object.clone(),
                    prepared.stored_bytes.clone(),
                )?,
                durable.clone(),
            ),
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::AccessRequested(durable_request),
            ) if durable_request == &request => {
                let locator = if provider_admin.provider == request.peer_provider {
                    provider_admin.access.clone()
                } else {
                    let administrator = access_administrator
                        .ok_or(DeviceJoinError::ProviderAdministratorRequired)?;
                    administrator
                        .grant_member_access(
                            &request.offer.member_pubkey,
                            self.join
                                .membership
                                .current_member_provider_email(&request.offer.member_pubkey),
                            &request.peer_provider,
                        )
                        .await?
                };
                let grant_id = ProviderAccessGrantId::from_random_bytes(
                    *ObjectHash::digest(database.new_store_write_id().as_str().as_bytes())
                        .as_bytes(),
                );
                let grant = self
                    .join
                    .local_writer
                    .sign_provider_access_grant(
                        grant_id,
                        request.offer.member_pubkey.clone(),
                        request.peer_provider.clone(),
                        locator,
                        provider_admin.grant_id.clone(),
                        provider_admin.administrator.clone(),
                        &request.offer.provider,
                    )
                    .map_err(provider_error)?;
                let context = crate::storage::ProtocolObjectContext::signed_plaintext(
                    request.offer.store_root.store_root_hash,
                    ProtocolObjectDomain::ProviderAccessGrant,
                );
                let prefix = crate::protocol::store_commit::provider_access_grant_semantic_prefix(
                    &grant.grant_id,
                );
                let slot = self
                    .join
                    .storage
                    .allocate_protocol_slot(&context, &prefix, ".json")
                    .await?;
                let prepared = self.join.storage.prepare_protocol_object(
                    &context,
                    slot,
                    &prefix,
                    grant.to_bytes(),
                )?;
                let prepared_progress = DeviceJoinJournalRecord {
                    attempt_id: request.offer.attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                        ProviderAdminJoinProgress::AccessGrantPrepared {
                            request: request.clone(),
                            grant: grant.clone(),
                            prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
                        },
                    )),
                };
                database
                    .advance_device_join(&initial, prepared_progress.clone())
                    .await?;
                (grant, prepared, prepared_progress)
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        let context = crate::storage::ProtocolObjectContext::signed_plaintext(
            request.offer.store_root.store_root_hash,
            ProtocolObjectDomain::ProviderAccessGrant,
        );
        let prefix =
            crate::protocol::store_commit::provider_access_grant_semantic_prefix(&grant.grant_id);
        self.join.storage.create_protocol_object(&prepared).await?;
        let opened = self
            .join
            .storage
            .read_protocol_object(&context, prepared.reference(), &prefix)
            .await?;
        if opened != grant.to_bytes() {
            return Err(DeviceJoinError::Provider(
                "provider access grant exact readback differs from its signed bytes".to_string(),
            ));
        }
        let grant_ref =
            StoreMemberProviderAccessGrantRef::from_grant(&grant, prepared.reference().clone());
        let activation = self
            .activate(
                crate::sync::store::operations::StoreOperationBatch::ProviderAccessGrant(
                    grant_ref.clone(),
                ),
            )
            .await?;
        let admission = if provider_admin.provider == request.peer_provider {
            DeviceProviderAdmissionChallenge::SamePrincipal
        } else {
            let challenge_context = request.cross_challenge_context();
            let probe_id = crate::protocol::provider::ProviderProbeId::from_bytes(
                *ObjectHash::digest(database.new_store_write_id().as_str().as_bytes()).as_bytes(),
            );
            DeviceProviderAdmissionChallenge::CrossPrincipal(
                self.join
                    .storage
                    .prepare_cross_principal_challenge(
                        &database,
                        probe_id,
                        &request.offer.provider,
                        &challenge_context,
                        self.join.local_writer.as_ref(),
                    )
                    .await
                    .map_err(provider_error)?,
            )
        };
        let approval = self.sign_device_admission_approval(
            request,
            ActivatedStoreMemberProviderAccessGrant {
                grant,
                grant_ref,
                activation,
            },
            admission,
        )?;
        database
            .advance_device_join(
                &prepared_progress,
                DeviceJoinJournalRecord {
                    attempt_id: approval.request.offer.attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                        ProviderAdminJoinProgress::ApprovalPrepared(approval.clone()),
                    )),
                },
            )
            .await?;
        Ok(approval)
    }

    pub(super) async fn publish_challenge(
        &mut self,
        bootstrap: ProvisionalDeviceBootstrap,
    ) -> Result<ProviderReadyDeviceBootstrap, DeviceJoinError> {
        let offer = &bootstrap.request.approval.request.offer;
        if self.require_grant(&offer.provider_admin.grant_id)? != offer.provider_admin.as_ref() {
            return Err(DeviceJoinError::OfferMismatch);
        }
        let owner = self
            .history()
            .load_registration(&offer.owner_registration)
            .await?
            .value;
        self.verify_device_admission_approval(&bootstrap.request.approval, &owner)?;
        let challenge_publication = match &bootstrap.request.approval.admission {
            DeviceProviderAdmissionChallenge::SamePrincipal => {
                DeviceProviderChallengePublication::SamePrincipal
            }
            DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) => {
                let context = bootstrap.request.approval.request.cross_challenge_context();
                let authorization = DeviceJoinChallengePublicationAuthorization {
                    attempt: bootstrap.publication_authorization.attempt.clone(),
                    attempt_activation: bootstrap
                        .publication_authorization
                        .attempt_activation
                        .clone(),
                };
                let published = self
                    .publish_cross_principal_challenge(
                        &authorization,
                        challenge,
                        &context,
                        &offer.provider,
                        &owner,
                    )
                    .await?;
                DeviceProviderChallengePublication::CrossPrincipal {
                    challenge: published,
                }
            }
        };
        let attempt_id = offer.attempt_id;
        let ready = ProviderReadyDeviceBootstrap {
            bootstrap: Box::new(bootstrap),
            challenge_publication,
        };
        if let Some(current) = self
            .join
            .database
            .load_device_join(attempt_id, DeviceJoinRole::ProviderAdministrator)
            .await?
        {
            match &*current.progress {
                DeviceJoinRoleProgress::ProviderAdministrator(
                    ProviderAdminJoinProgress::ProviderReady(existing),
                ) if existing == &ready => return Ok(ready),
                DeviceJoinRoleProgress::ProviderAdministrator(
                    ProviderAdminJoinProgress::ApprovalPrepared(approval),
                ) if approval == &*ready.bootstrap.request.approval => {
                    let observed = DeviceJoinJournalRecord {
                        attempt_id,
                        progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                            ProviderAdminJoinProgress::AttemptObserved(*ready.bootstrap.clone()),
                        )),
                    };
                    self.join
                        .database
                        .advance_device_join(&current, observed.clone())
                        .await?;
                    let intent = DeviceJoinJournalRecord {
                        attempt_id,
                        progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                            ProviderAdminJoinProgress::ChallengeCreateIntent(
                                *ready.bootstrap.clone(),
                            ),
                        )),
                    };
                    self.join
                        .database
                        .advance_device_join(&observed, intent.clone())
                        .await?;
                    self.join
                        .database
                        .advance_device_join(
                            &intent,
                            DeviceJoinJournalRecord {
                                attempt_id,
                                progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                                    ProviderAdminJoinProgress::ProviderReady(ready.clone()),
                                )),
                            },
                        )
                        .await?;
                }
                _ => return Err(DeviceJoinError::JournalConflict),
            }
        }
        Ok(ready)
    }

    pub(super) async fn complete_admission(
        &mut self,
        readiness: DeviceJoinReadiness,
    ) -> Result<DeviceProviderAdmissionCompletion, DeviceJoinError> {
        let attempt_id = readiness.proof.attempt.attempt_id;
        let database = self.join.database.clone();
        let current = database
            .load_device_join(attempt_id, DeviceJoinRole::ProviderAdministrator)
            .await?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if let DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::Completed(existing),
        ) = &*current.progress
        {
            if *existing.readiness == readiness {
                return Ok(existing.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        let bootstrap = match &*current.progress {
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::ProviderReady(bootstrap),
            ) => bootstrap.clone(),
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        if readiness.proof.attempt != bootstrap.bootstrap.publication_authorization.attempt {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let offer = &bootstrap.bootstrap.request.approval.request.offer;
        let provider_admin = self.require_grant(&offer.provider_admin.grant_id)?;
        if provider_admin != offer.provider_admin.as_ref() {
            return Err(DeviceJoinError::ProviderAdministratorRequired);
        }
        let admission = match (
            &bootstrap.bootstrap.request.approval.admission,
            &bootstrap.bootstrap.request.response,
            &readiness.provider,
        ) {
            (
                DeviceProviderAdmissionChallenge::SamePrincipal,
                DeviceProviderResponseReservation::SamePrincipal,
                DeviceProviderReadiness::SamePrincipal,
            ) => DeviceProviderAdmission::SamePrincipal,
            (
                DeviceProviderAdmissionChallenge::CrossPrincipal(challenge),
                DeviceProviderResponseReservation::CrossPrincipal { response_slot },
                DeviceProviderReadiness::CrossPrincipal(response),
            ) => {
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
                DeviceProviderAdmission::CrossPrincipal(
                    self.join
                        .storage
                        .complete_cross_principal_probe(
                            &database,
                            challenge,
                            response,
                            &context,
                            &offer.provider,
                            self.join.local_writer.as_ref(),
                            &offer.member_pubkey,
                        )
                        .await
                        .map_err(provider_error)?,
                )
            }
            _ => return Err(DeviceJoinError::AttemptMismatch),
        };
        let completion = DeviceProviderAdmissionCompletion {
            readiness: Box::new(readiness.clone()),
            admission,
        };
        let observed = DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::ResponseObserved(readiness),
            )),
        };
        database
            .advance_device_join(&current, observed.clone())
            .await?;
        database
            .advance_device_join(
                &observed,
                DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                        ProviderAdminJoinProgress::Completed(completion.clone()),
                    )),
                },
            )
            .await?;
        Ok(completion)
    }

    pub(super) async fn close(
        &mut self,
        cancellation: DeviceJoinCancellation,
    ) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
        require_cancelled_outcome(&cancellation.outcome)?;
        let attempt_ref = cancellation.outcome.attempt().clone();
        let database = self.join.database.clone();
        let current = database
            .load_device_join(
                attempt_ref.attempt_id,
                DeviceJoinRole::ProviderAdministrator,
            )
            .await?
            .ok_or(DeviceJoinError::JournalConflict)?;
        match &*current.progress {
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::Completed(completion),
            ) => return Ok(ProviderAdminJoinTerminal::Completed(completion.clone())),
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::Cancelled(closure),
            ) => return Ok(ProviderAdminJoinTerminal::Cancelled(closure.clone())),
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::WriteRevoked(revocation),
            ) => return Ok(ProviderAdminJoinTerminal::WriteRevoked(revocation.clone())),
            _ => {}
        }
        let (attempt, owner) = self.history().load_attempt_and_owner(&attempt_ref).await?;
        let outcome = self
            .history()
            .load_outcome(&cancellation.outcome, &owner.value)
            .await?
            .value;
        if !matches!(
            outcome.body,
            crate::protocol::store_commit::DeviceJoinOutcomeBody::Cancelled
        ) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let offer = &attempt.value.provider_approval.request.offer;
        let provider_admin = self.require_grant(&offer.provider_admin.grant_id)?;
        if provider_admin != offer.provider_admin.as_ref() {
            return Err(DeviceJoinError::ProviderAdministratorRequired);
        }
        let (challenge, prior_state_hash) = match &*current.progress {
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::CleanupIntent {
                    cancellation: durable,
                    challenge,
                    prior_state_hash,
                },
            ) if durable == &cancellation => (challenge.clone(), *prior_state_hash),
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::ApprovalPrepared(_)
                | ProviderAdminJoinProgress::AttemptObserved(_)
                | ProviderAdminJoinProgress::ChallengeCreateIntent(_)
                | ProviderAdminJoinProgress::ProviderReady(_)
                | ProviderAdminJoinProgress::ResponseObserved(_),
            ) => {
                let challenge = match &attempt.value.provider_approval.admission {
                    DeviceProviderAdmissionChallenge::SamePrincipal => {
                        ProviderChallengeDisposition::SamePrincipal
                    }
                    DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) => {
                        match self
                            .join
                            .storage
                            .observe_exact_slot(&challenge.administrator_object.slot)
                            .await
                        {
                            Ok(None) => ProviderChallengeDisposition::NeverCreated,
                            Ok(Some(object)) if object == challenge.administrator_object.object => {
                                ProviderChallengeDisposition::Created(object)
                            }
                            Ok(Some(_)) => return Err(DeviceJoinError::CleanupMismatch),
                            Err(error) => {
                                return Err(DeviceJoinError::Provider(error.to_string()));
                            }
                        }
                    }
                };
                let prior_state_hash = ObjectHash::digest(&serde_json::to_vec(&current.progress)?);
                let intent = DeviceJoinJournalRecord {
                    attempt_id: attempt_ref.attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                        ProviderAdminJoinProgress::CleanupIntent {
                            cancellation: cancellation.clone(),
                            challenge: challenge.clone(),
                            prior_state_hash,
                        },
                    )),
                };
                database.advance_device_join(&current, intent).await?;
                (challenge, prior_state_hash)
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        };
        if let DeviceProviderAdmissionChallenge::CrossPrincipal(probe) =
            &attempt.value.provider_approval.admission
        {
            self.join
                .storage
                .delete_exact_slot_and_verify_absent(&probe.administrator_object.slot)
                .await
                .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
        }
        let closure = self.join.local_writer.sign_provider_join_closure(
            cancellation.outcome,
            offer.provider_admin.administrator.clone(),
            challenge,
            prior_state_hash,
        )?;
        let intent = database
            .load_device_join(
                attempt_ref.attempt_id,
                DeviceJoinRole::ProviderAdministrator,
            )
            .await?
            .ok_or(DeviceJoinError::JournalConflict)?;
        database
            .advance_device_join(
                &intent,
                DeviceJoinJournalRecord {
                    attempt_id: attempt_ref.attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                        ProviderAdminJoinProgress::Cancelled(closure.clone()),
                    )),
                },
            )
            .await?;
        Ok(ProviderAdminJoinTerminal::Cancelled(closure))
    }

    async fn sign_write_revocation(
        &mut self,
        cancellation: DeviceJoinCancellation,
        producer: DeviceJoinProducer,
        revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
        executor_grant: &ProviderAdminGrantId,
    ) -> Result<DeviceJoinProducerWriteRevocation, DeviceJoinError> {
        require_cancelled_outcome(&cancellation.outcome)?;
        let attempt_ref = cancellation.outcome.attempt().clone();
        let (attempt, owner) = self.history().load_attempt_and_owner(&attempt_ref).await?;
        let outcome = self
            .history()
            .load_outcome(&cancellation.outcome, &owner.value)
            .await?
            .value;
        if !matches!(
            outcome.body,
            crate::protocol::store_commit::DeviceJoinOutcomeBody::Cancelled
        ) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let executor_admin = self.require_grant(executor_grant)?.clone();
        let (authority, protected_slots, locator) = match producer {
            DeviceJoinProducer::ProviderAdministrator => {
                let DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) =
                    &attempt.value.provider_approval.admission
                else {
                    return Err(DeviceJoinError::CleanupMismatch);
                };
                (
                    ProviderWriteAuthorityRef::ProviderAdministrator(
                        attempt
                            .value
                            .provider_approval
                            .request
                            .offer
                            .provider_admin
                            .grant_id
                            .clone(),
                    ),
                    vec![challenge.administrator_object.slot.clone()],
                    &attempt
                        .value
                        .provider_approval
                        .request
                        .offer
                        .provider_admin
                        .access,
                )
            }
            DeviceJoinProducer::Joiner => {
                let mut slots = vec![
                    attempt.value.registration_slot.clone(),
                    attempt
                        .value
                        .expected_registration
                        .acknowledgements
                        .first_slot()
                        .clone(),
                ];
                if let DeviceProviderResponseReservation::CrossPrincipal { response_slot } =
                    &attempt.value.provider_response
                {
                    slots.push(response_slot.clone());
                }
                (
                    ProviderWriteAuthorityRef::MemberAccess(
                        attempt
                            .value
                            .provider_approval
                            .access_grant
                            .grant_ref
                            .clone(),
                    ),
                    slots,
                    &attempt.value.provider_approval.access_grant.grant.locator,
                )
            }
        };
        let withdrawal = revocation_executor
            .revoke_write_authority(producer, &authority, locator, &protected_slots)
            .await?;
        withdrawal
            .verify_for_locator(locator)
            .map_err(|_| DeviceJoinError::CleanupMismatch)?;
        self.join.local_writer.sign_device_join_write_revocation(
            cancellation.outcome,
            producer,
            authority,
            protected_slots,
            withdrawal,
            executor_grant.clone(),
            executor_admin.administrator,
        )
    }

    pub(super) async fn revoke_writes(
        &mut self,
        cancellation: DeviceJoinCancellation,
        revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
    ) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
        let executor_grant = self
            .join
            .protocol_root
            .descriptor
            .founder_provider_admin
            .grant_id
            .clone();
        let attempt_id = cancellation.outcome.attempt().attempt_id;
        let database = self.join.database.clone();
        let current = database
            .load_device_join(attempt_id, DeviceJoinRole::ProviderAdministrator)
            .await?;
        if let Some(current) = &current {
            match &*current.progress {
                DeviceJoinRoleProgress::ProviderAdministrator(
                    ProviderAdminJoinProgress::Completed(completion),
                ) => return Ok(ProviderAdminJoinTerminal::Completed(completion.clone())),
                DeviceJoinRoleProgress::ProviderAdministrator(
                    ProviderAdminJoinProgress::Cancelled(closure),
                ) => return Ok(ProviderAdminJoinTerminal::Cancelled(closure.clone())),
                DeviceJoinRoleProgress::ProviderAdministrator(
                    ProviderAdminJoinProgress::WriteRevoked(revocation),
                ) => return Ok(ProviderAdminJoinTerminal::WriteRevoked(revocation.clone())),
                DeviceJoinRoleProgress::ProviderAdministrator(
                    ProviderAdminJoinProgress::ApprovalPrepared(_)
                    | ProviderAdminJoinProgress::AttemptObserved(_)
                    | ProviderAdminJoinProgress::ChallengeCreateIntent(_)
                    | ProviderAdminJoinProgress::ProviderReady(_)
                    | ProviderAdminJoinProgress::ResponseObserved(_)
                    | ProviderAdminJoinProgress::CleanupIntent { .. },
                ) => {}
                _ => return Err(DeviceJoinError::JournalConflict),
            }
        }
        let revocation = self
            .sign_write_revocation(
                cancellation,
                DeviceJoinProducer::ProviderAdministrator,
                revocation_executor,
                &executor_grant,
            )
            .await?;
        let terminal = DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::WriteRevoked(revocation.clone()),
            )),
        };
        if let Some(current) = current {
            database.advance_device_join(&current, terminal).await?;
        } else {
            database
                .begin_device_join_replacement_terminal(terminal)
                .await?;
        }
        Ok(ProviderAdminJoinTerminal::WriteRevoked(revocation))
    }

    async fn revoke_joiner_writes(
        &mut self,
        cancellation: DeviceJoinCancellation,
        revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
    ) -> Result<JoinerJoinTerminal, DeviceJoinError> {
        let executor_grant = self
            .join
            .protocol_root
            .descriptor
            .founder_provider_admin
            .grant_id
            .clone();
        let attempt_id = cancellation.outcome.attempt().attempt_id;
        let database = self.join.database.clone();
        if let Some(current) = database
            .load_device_join(attempt_id, DeviceJoinRole::Joiner)
            .await?
        {
            return match &*current.progress {
                DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
                    Ok(JoinerJoinTerminal::WriteRevoked(revocation.clone()))
                }
                _ => Err(DeviceJoinError::JournalConflict),
            };
        }
        let revocation = self
            .sign_write_revocation(
                cancellation,
                DeviceJoinProducer::Joiner,
                revocation_executor,
                &executor_grant,
            )
            .await?;
        database
            .begin_device_join_replacement_terminal(DeviceJoinJournalRecord {
                attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::WriteRevoked(revocation.clone()),
                )),
            })
            .await?;
        Ok(JoinerJoinTerminal::WriteRevoked(revocation))
    }
}

impl Store {
    #[doc(hidden)]
    pub(crate) async fn revoke_joining_device_writes(
        &self,
        cancellation: DeviceJoinCancellation,
        revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
    ) -> Result<JoinerJoinTerminal, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer
            .provider_administrator_join()?
            .revoke_joiner_writes(cancellation, revocation_executor)
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn authorize_device_provider_access(
        &self,
        request: DeviceProviderAccessRequest,
        access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
    ) -> Result<DeviceProviderAdmissionApproval, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer
            .provider_administrator_join()?
            .authorize_access(request, access_administrator)
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn publish_device_provider_challenge(
        &self,
        bootstrap: ProvisionalDeviceBootstrap,
    ) -> Result<ProviderReadyDeviceBootstrap, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer
            .provider_administrator_join()?
            .publish_challenge(bootstrap)
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn complete_device_provider_admission(
        &self,
        readiness: DeviceJoinReadiness,
    ) -> Result<DeviceProviderAdmissionCompletion, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer
            .provider_administrator_join()?
            .complete_admission(readiness)
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn close_device_provider_admission(
        &self,
        cancellation: DeviceJoinCancellation,
    ) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer
            .provider_administrator_join()?
            .close(cancellation)
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn revoke_device_provider_admission_writes(
        &self,
        cancellation: DeviceJoinCancellation,
        revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
    ) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer
            .provider_administrator_join()?
            .revoke_writes(cancellation, revocation_executor)
            .await
    }
}

#[async_trait::async_trait]
pub trait DeviceProviderAccessAdministrator: Send + Sync {
    async fn grant_member_access(
        &self,
        member_pubkey: &str,
        provider_account_email: Option<&str>,
        peer: &ProviderDeviceBinding,
    ) -> Result<crate::protocol::provider::ProviderAccessLocator, DeviceJoinError>;
}
