use super::authority::{authorize_store, resolved_provider_admin};
use super::cleanup::{
    ensure_exact_slot_absent, require_cancelled_outcome, sign_device_join_producer_write_revocation,
};
use super::joiner::cross_challenge_context;
use super::journal::{
    advance_store_journal, begin_store_journal, begin_store_replacement_terminal, database_error,
    load_store_journal, provider_error,
};
use super::*;

impl Store {
    #[doc(hidden)]
    pub async fn authorize_device_provider_access(
        &self,
        identity_signer: &UserKeypair,
        request: DeviceProviderAccessRequest,
        access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
    ) -> Result<DeviceProviderAdmissionApproval, DeviceJoinError> {
        let mut authorized = authorize_store(self).await?;
        let authority = authorized.operation_authority();
        let exact = self.storage().exact_slot_storage();
        authorize_device_provider_access(
            authority.database,
            authority.history_verifier,
            Some(exact),
            access_administrator,
            authority.membership,
            identity_signer,
            request,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn publish_device_provider_challenge(
        &self,
        bootstrap: ProvisionalDeviceBootstrap,
    ) -> Result<ProviderReadyDeviceBootstrap, DeviceJoinError> {
        let exact = self.storage().exact_slot_storage();
        let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(
            &**self.storage(),
            self.store_root(),
        )
        .await?;
        publish_device_provider_challenge_with_history(
            self.database(),
            &mut history_verifier,
            Some(exact),
            bootstrap,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn complete_device_provider_admission(
        &self,
        identity_signer: &UserKeypair,
        readiness: DeviceJoinReadiness,
    ) -> Result<DeviceProviderAdmissionCompletion, DeviceJoinError> {
        let exact = self.storage().exact_slot_storage();
        complete_device_provider_admission(self.database(), Some(exact), identity_signer, readiness)
            .await
    }

    #[doc(hidden)]
    pub async fn close_device_provider_admission(
        &self,
        identity_signer: &UserKeypair,
        cancellation: DeviceJoinCancellation,
    ) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
        let exact = self.storage().exact_slot_storage();
        let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(
            &**self.storage(),
            self.store_root(),
        )
        .await?;
        close_device_provider_admission_with_history(
            self.database(),
            &mut history_verifier,
            Some(exact),
            identity_signer,
            cancellation,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn revoke_device_provider_admission_writes(
        &self,
        identity_signer: &UserKeypair,
        cancellation: DeviceJoinCancellation,
        revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
        executor_grant: ProviderAdminGrantId,
    ) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
        let mut authorized = authorize_store(self).await?;
        let authority = authorized.operation_authority();
        revoke_device_provider_admission_writes(
            authority.database,
            authority.history_verifier,
            authority.membership,
            identity_signer,
            cancellation,
            revocation_executor,
            executor_grant,
        )
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
    ) -> Result<crate::sync::provider::ProviderAccessLocator, DeviceJoinError>;
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn publish_device_provider_challenge(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    administrator_exact: Option<&dyn ExactSlotStorage>,
    bootstrap: ProvisionalDeviceBootstrap,
) -> Result<ProviderReadyDeviceBootstrap, DeviceJoinError> {
    let root = database
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let mut history_verifier =
        crate::sync::store::pull::MergeHistoryVerifier::new(storage, &root).await?;
    publish_device_provider_challenge_with_history(
        database,
        &mut history_verifier,
        administrator_exact,
        bootstrap,
    )
    .await
}

pub(crate) async fn publish_device_provider_challenge_with_history(
    database: &StoreDatabase,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    administrator_exact: Option<&dyn ExactSlotStorage>,
    bootstrap: ProvisionalDeviceBootstrap,
) -> Result<ProviderReadyDeviceBootstrap, DeviceJoinError> {
    let db = database.sqlite();
    let offer = &bootstrap.request.approval.request.offer;
    let owner =
        Box::pin(database.activated_store_device_registration(offer.owner_registration.clone()))
            .await
            .map_err(database_error)?;
    let administrator = Box::pin(
        database.activated_store_device_registration(offer.provider_admin.administrator.clone()),
    )
    .await
    .map_err(database_error)?;
    let root_value = history_verifier.verified_root_object().clone();
    bootstrap
        .request
        .approval
        .verify(&root_value, &owner, &administrator)?;
    let challenge_publication = match &bootstrap.request.approval.admission {
        DeviceProviderAdmissionChallenge::SamePrincipal => {
            DeviceProviderChallengePublication::SamePrincipal
        }
        DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) => {
            let exact = administrator_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?;
            let context = cross_challenge_context(&bootstrap.request.approval.request);
            let authorization = DeviceJoinChallengePublicationAuthorization {
                attempt: bootstrap.publication_authorization.attempt.clone(),
                attempt_activation: bootstrap
                    .publication_authorization
                    .attempt_activation
                    .clone(),
            };
            let published = Box::pin(crate::sync::provider::publish_cross_principal_challenge(
                history_verifier,
                exact,
                database,
                &authorization,
                challenge,
                &context,
                &offer.provider,
                &owner,
                &administrator.device_signing_pubkey,
            ))
            .await
            .map_err(provider_error)?;
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
    if let Some(current) = Box::pin(load_store_journal(
        db,
        attempt_id,
        DeviceJoinRole::ProviderAdministrator,
    ))
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
                Box::pin(advance_store_journal(db, &current, observed.clone())).await?;
                let intent = DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                        ProviderAdminJoinProgress::ChallengeCreateIntent(*ready.bootstrap.clone()),
                    )),
                };
                Box::pin(advance_store_journal(db, &observed, intent.clone())).await?;
                Box::pin(advance_store_journal(
                    db,
                    &intent,
                    DeviceJoinJournalRecord {
                        attempt_id,
                        progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                            ProviderAdminJoinProgress::ProviderReady(ready.clone()),
                        )),
                    },
                ))
                .await?;
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        }
    }
    Ok(ready)
}

pub(crate) async fn complete_device_provider_admission(
    database: &StoreDatabase,
    administrator_exact: Option<&dyn ExactSlotStorage>,
    identity_signer: &UserKeypair,
    readiness: DeviceJoinReadiness,
) -> Result<DeviceProviderAdmissionCompletion, DeviceJoinError> {
    let db = database.sqlite();
    let attempt_id = readiness.proof.attempt.attempt_id;
    let current = load_store_journal(db, attempt_id, DeviceJoinRole::ProviderAdministrator)
        .await?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Completed(
        existing,
    )) = &*current.progress
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
    let administrator = database
        .activated_store_device_registration(offer.provider_admin.administrator.clone())
        .await
        .map_err(database_error)?;
    let administrator_signer = administrator.device_signer(identity_signer)?;
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
            let exact = administrator_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?;
            let context = crate::sync::provider::CrossPrincipalResponseContext {
                challenge: cross_challenge_context(&bootstrap.bootstrap.request.approval.request),
                expected_registration_hash: bootstrap
                    .bootstrap
                    .request
                    .expected_registration
                    .registration_hash(),
                response_slot: response_slot.clone(),
            };
            DeviceProviderAdmission::CrossPrincipal(
                crate::sync::provider::complete_cross_principal_probe(
                    exact,
                    db,
                    challenge,
                    response,
                    &context,
                    &offer.provider,
                    &administrator_signer,
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
    advance_store_journal(db, &current, observed.clone()).await?;
    advance_store_journal(
        db,
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

#[cfg(test)]
pub(crate) async fn close_device_provider_admission(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    administrator_exact: Option<&dyn ExactSlotStorage>,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
    let root = database
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let mut history_verifier =
        crate::sync::store::pull::MergeHistoryVerifier::new(storage, &root).await?;
    close_device_provider_admission_with_history(
        database,
        &mut history_verifier,
        administrator_exact,
        identity_signer,
        cancellation,
    )
    .await
}

pub(crate) async fn close_device_provider_admission_with_history(
    database: &StoreDatabase,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    administrator_exact: Option<&dyn ExactSlotStorage>,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
    let storage = history_verifier.storage();
    let db = database.sqlite();
    require_cancelled_outcome(&cancellation.outcome)?;
    let attempt_ref = cancellation.outcome.attempt().clone();
    let current = load_store_journal(
        db,
        attempt_ref.attempt_id,
        DeviceJoinRole::ProviderAdministrator,
    )
    .await?
    .ok_or(DeviceJoinError::JournalConflict)?;
    match &*current.progress {
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Completed(
            completion,
        )) => return Ok(ProviderAdminJoinTerminal::Completed(completion.clone())),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Cancelled(
            closure,
        )) => return Ok(ProviderAdminJoinTerminal::Cancelled(closure.clone())),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(
            revocation,
        )) => return Ok(ProviderAdminJoinTerminal::WriteRevoked(revocation.clone())),
        _ => {}
    }
    let root = history_verifier.root().clone();
    let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let attempt_prefix =
        crate::sync::store_commit::device_join_attempt_semantic_prefix(attempt_ref.attempt_id);
    let attempt_bytes = storage
        .read_protocol_object(&attempt_context, &attempt_ref.object, &attempt_prefix)
        .await?;
    let unverified_attempt: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)?;
    let owner = database
        .activated_store_device_registration(unverified_attempt.owner_registration.clone())
        .await
        .map_err(database_error)?;
    let attempt = crate::sync::store::pull::load_verified_device_join_attempt(
        history_verifier,
        &attempt_ref,
        &owner,
    )
    .await?
    .value;
    let outcome = crate::sync::store_objects::load_device_join_outcome_ref(
        storage,
        &root,
        &cancellation.outcome,
        &owner,
    )
    .await?
    .value;
    if !matches!(
        outcome.body,
        crate::sync::store_commit::DeviceJoinOutcomeBody::Cancelled
    ) {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let offer = &attempt.provider_approval.request.offer;
    let administrator = database
        .activated_store_device_registration(offer.provider_admin.administrator.clone())
        .await
        .map_err(database_error)?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if administrator.device_id.to_string() != local_device_id {
        return Err(DeviceJoinError::ProviderAdministratorRequired);
    }
    let administrator_signer = administrator.device_signer(identity_signer)?;
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
            let challenge = match &attempt.provider_approval.admission {
                DeviceProviderAdmissionChallenge::SamePrincipal => {
                    ProviderChallengeDisposition::SamePrincipal
                }
                DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) => {
                    let exact =
                        administrator_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?;
                    match exact.read_at(&challenge.administrator_object.slot).await {
                        Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => {
                            ProviderChallengeDisposition::NeverCreated
                        }
                        Ok(bytes) => {
                            challenge
                                .administrator_object
                                .object
                                .verify(&bytes)
                                .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
                            ProviderChallengeDisposition::Created(
                                challenge.administrator_object.object.clone(),
                            )
                        }
                        Err(error) => return Err(DeviceJoinError::Provider(error.to_string())),
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
            advance_store_journal(db, &current, intent).await?;
            (challenge, prior_state_hash)
        }
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    if let DeviceProviderAdmissionChallenge::CrossPrincipal(probe) =
        &attempt.provider_approval.admission
    {
        ensure_exact_slot_absent(
            administrator_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?,
            &probe.administrator_object.slot,
        )
        .await?;
    }
    let closure = ProviderAdminJoinClosure::signed(
        cancellation.outcome,
        offer.provider_admin.administrator.clone(),
        challenge,
        prior_state_hash,
        &administrator,
        &administrator_signer,
    )?;
    let intent = load_store_journal(
        db,
        attempt_ref.attempt_id,
        DeviceJoinRole::ProviderAdministrator,
    )
    .await?
    .ok_or(DeviceJoinError::JournalConflict)?;
    advance_store_journal(
        db,
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

pub(crate) async fn revoke_device_provider_admission_writes(
    database: &StoreDatabase,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
    revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
    executor_grant: ProviderAdminGrantId,
) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
    let db = database.sqlite();
    let attempt_id = cancellation.outcome.attempt().attempt_id;
    let current = load_store_journal(db, attempt_id, DeviceJoinRole::ProviderAdministrator).await?;
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
    let revocation = Box::pin(sign_device_join_producer_write_revocation(
        database,
        history_verifier,
        authorization,
        identity_signer,
        cancellation,
        DeviceJoinProducer::ProviderAdministrator,
        revocation_executor,
        executor_grant,
    ))
    .await?;
    let terminal = DeviceJoinJournalRecord {
        attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::WriteRevoked(revocation.clone()),
        )),
    };
    if let Some(current) = current {
        advance_store_journal(db, &current, terminal).await?;
    } else {
        begin_store_replacement_terminal(db, terminal).await?;
    }
    Ok(ProviderAdminJoinTerminal::WriteRevoked(revocation))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn authorize_device_provider_access(
    database: &StoreDatabase,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    administrator_exact: Option<&dyn ExactSlotStorage>,
    access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    request: DeviceProviderAccessRequest,
) -> Result<DeviceProviderAdmissionApproval, DeviceJoinError> {
    let storage = history_verifier.storage();
    let db = database.sqlite();
    let root_value = history_verifier.verified_root_object().clone();
    let owner = database
        .activated_store_device_registration(request.offer.owner_registration.clone())
        .await
        .map_err(database_error)?;
    request.verify(&owner)?;
    let provider_admin =
        resolved_provider_admin(authorization, &request.offer.provider_admin.grant_id)?;
    if provider_admin != *request.offer.provider_admin {
        return Err(DeviceJoinError::OfferMismatch);
    }
    let administrator = database
        .activated_store_device_registration(provider_admin.administrator.clone())
        .await
        .map_err(database_error)?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if administrator.device_id.to_string() != local_device_id {
        return Err(DeviceJoinError::ProviderAdministratorRequired);
    }
    let administrator_signer = administrator.device_signer(identity_signer)?;
    let initial = DeviceJoinJournalRecord {
        attempt_id: request.offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessRequested(request.clone()),
        )),
    };
    let durable = begin_store_journal(db, initial.clone()).await?;
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
            crate::sync::storage::PreparedExactObject::new(
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
                let administrator =
                    access_administrator.ok_or(DeviceJoinError::ProviderAdministratorRequired)?;
                administrator
                    .grant_member_access(
                        &request.offer.member_pubkey,
                        authorization.current_member_provider_email(&request.offer.member_pubkey),
                        &request.peer_provider,
                    )
                    .await?
            };
            let grant_id = ProviderAccessGrantId::from_random_bytes(
                *ObjectHash::digest(db.new_write_id().as_str().as_bytes()).as_bytes(),
            );
            let grant = StoreMemberProviderAccessGrant::signed(
                grant_id,
                request.offer.member_pubkey.clone(),
                request.peer_provider.clone(),
                locator,
                provider_admin.grant_id.clone(),
                provider_admin.administrator.clone(),
                &request.offer.provider,
                &administrator,
                &administrator_signer,
            )
            .map_err(provider_error)?;
            let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
                request.offer.store_root.store_root_hash,
                ProtocolObjectDomain::ProviderAccessGrant,
            );
            let prefix =
                crate::sync::store_commit::provider_access_grant_semantic_prefix(&grant.grant_id);
            let slot = storage
                .allocate_protocol_slot(&context, &prefix, ".json")
                .await?;
            let prepared =
                storage.prepare_protocol_object(&context, slot, &prefix, grant.to_bytes())?;
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
            advance_store_journal(db, &initial, prepared_progress.clone()).await?;
            (grant, prepared, prepared_progress)
        }
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        request.offer.store_root.store_root_hash,
        ProtocolObjectDomain::ProviderAccessGrant,
    );
    let prefix = crate::sync::store_commit::provider_access_grant_semantic_prefix(&grant.grant_id);
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != grant.to_bytes() {
        return Err(DeviceJoinError::Provider(
            "provider access grant exact readback differs from its signed bytes".to_string(),
        ));
    }
    let grant_ref =
        StoreMemberProviderAccessGrantRef::from_grant(&grant, prepared.reference().clone());
    let plan = crate::sync::store::operations::prepare_plan(
        database,
        history_verifier,
        authorization,
        &local_device_id,
        identity_signer,
    )
    .await?;
    let activation = crate::sync::store::operations::activate_store_operation_commit(
        database,
        history_verifier,
        plan,
        crate::sync::store::operations::StoreOperationBatch::ProviderAccessGrant(grant_ref.clone()),
    )
    .await?;
    let admission = if provider_admin.provider == request.peer_provider {
        DeviceProviderAdmissionChallenge::SamePrincipal
    } else {
        let exact = administrator_exact.ok_or(DeviceJoinError::ProviderAdministratorRequired)?;
        let challenge_context = crate::sync::provider::CrossPrincipalChallengeContext {
            root: request.offer.store_root.clone(),
            attempt_id: request.offer.attempt_id,
            access_request_hash: request.request_hash(),
            provider_admin_grant: provider_admin.grant_id.clone(),
            owner_registration: request.offer.owner_registration.clone(),
            member_pubkey: request.offer.member_pubkey.clone(),
            administrator_binding: provider_admin.provider.clone(),
            peer_binding: request.peer_provider.clone(),
        };
        let probe_id = crate::sync::provider::ProviderProbeId::from_bytes(
            *ObjectHash::digest(db.new_write_id().as_str().as_bytes()).as_bytes(),
        );
        DeviceProviderAdmissionChallenge::CrossPrincipal(
            crate::sync::provider::prepare_cross_principal_challenge(
                exact,
                database,
                probe_id,
                &request.offer.provider,
                &challenge_context,
                &administrator_signer,
            )
            .await
            .map_err(provider_error)?,
        )
    };
    let approval = DeviceProviderAdmissionApproval::signed(
        request,
        ActivatedStoreMemberProviderAccessGrant {
            grant,
            grant_ref,
            activation,
        },
        admission,
        &root_value,
        &administrator,
        &administrator_signer,
    )?;
    advance_store_journal(
        db,
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
