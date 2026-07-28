use super::authority::authorize_store;
use super::cleanup::{
    ensure_exact_slot_absent, observe_exact_slot, require_cancelled_outcome,
    sign_device_join_producer_write_revocation,
};
use super::journal::{
    attempt_key, begin_store_replacement_terminal, database_error, load_store_journal,
    provider_error, store_journal_key,
};
use super::*;

impl Store {
    #[doc(hidden)]
    pub async fn revoke_joining_device_writes(
        &self,
        identity_signer: &UserKeypair,
        cancellation: DeviceJoinCancellation,
        revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
        executor_grant: ProviderAdminGrantId,
    ) -> Result<JoinerJoinTerminal, DeviceJoinError> {
        let mut authorized = authorize_store(self).await?;
        let authority = authorized.operation_authority();
        revoke_joining_device_writes(
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

pub async fn prepare_device_provider_access_request(
    pending: &DeviceJoinJournalDatabase,
    provider_binding: crate::sync::storage::ResolvedProviderBinding,
    identity_signer: &UserKeypair,
    offer: DeviceJoinOffer,
) -> Result<DeviceProviderAccessRequest, DeviceJoinError> {
    if let Some(record) = pending.load(offer.attempt_id, DeviceJoinRole::Joiner)? {
        return match &*record.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request))
                if *request.offer == offer =>
            {
                Ok(request.clone())
            }
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(durable))
                if durable == &offer =>
            {
                prepare_new_access_request(
                    pending,
                    provider_binding,
                    identity_signer,
                    record.clone(),
                    durable.clone(),
                )
                .await
            }
            _ => Err(DeviceJoinError::JournalConflict),
        };
    }
    let initial = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::OfferReceived(offer.clone()),
        )),
    };
    let durable = pending.begin(initial.clone())?;
    if durable != initial {
        return Err(DeviceJoinError::JournalConflict);
    }
    prepare_new_access_request(pending, provider_binding, identity_signer, initial, offer).await
}

async fn prepare_new_access_request(
    pending: &DeviceJoinJournalDatabase,
    provider_binding: crate::sync::storage::ResolvedProviderBinding,
    identity_signer: &UserKeypair,
    initial: DeviceJoinJournalRecord,
    offer: DeviceJoinOffer,
) -> Result<DeviceProviderAccessRequest, DeviceJoinError> {
    if provider_binding.store != offer.provider {
        return Err(DeviceJoinError::OfferMismatch);
    }
    let request =
        DeviceProviderAccessRequest::signed(offer, provider_binding.device, identity_signer)?;
    pending.advance(
        &initial,
        DeviceJoinJournalRecord {
            attempt_id: request.offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::AccessRequested(request.clone()),
            )),
        },
    )?;
    Ok(request)
}

pub fn prepare_device_registration_request<'a>(
    pending: &'a DeviceJoinJournalDatabase,
    storage: &'a dyn SyncStorage,
    peer_exact: Option<&'a dyn ExactSlotStorage>,
    identity_signer: &'a UserKeypair,
    approval: DeviceProviderAdmissionApproval,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<DeviceRegistrationRequest, DeviceJoinError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let attempt_id = approval.request.offer.attempt_id;
        if let Some(record) = pending.load(attempt_id, DeviceJoinRole::Joiner)? {
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
        let verified_root = crate::sync::store_objects::load_store_protocol_root(
            storage,
            &approval.request.offer.store_root,
        )
        .await?;
        let commit_verifier = crate::sync::store::pull::StoreCommitVerifier::from_verified_root(
            storage,
            &approval.request.offer.store_root,
            verified_root,
        )?;
        let owner = crate::sync::store_objects::load_registration_ref_with_root(
            commit_verifier.storage(),
            commit_verifier.root(),
            commit_verifier.verified_root(),
            &approval.request.offer.owner_registration,
        )
        .await?
        .value;
        let administrator = crate::sync::store_objects::load_registration_ref_with_root(
            commit_verifier.storage(),
            commit_verifier.root(),
            commit_verifier.verified_root(),
            &approval.request.offer.provider_admin.administrator,
        )
        .await?
        .value;
        approval.verify(
            commit_verifier.verified_root_object(),
            &owner,
            &administrator,
        )?;
        let mut history_verifier =
            crate::sync::store::pull::MergeHistoryVerifier::from_commit_verifier(commit_verifier)
                .await?;
        crate::sync::store::pull::verify_accepted_provider_access_activation(
            &mut history_verifier,
            &approval.access_grant,
            &approval.request.offer.provider_admin,
            &administrator,
        )
        .await?;
        let live = storage.provider_binding().await?;
        if live.store != approval.request.offer.provider
            || live.device != approval.request.peer_provider
        {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        let current = pending
            .load(attempt_id, DeviceJoinRole::Joiner)?
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
            pending.advance(&current, next.clone())?;
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
                let exact = peer_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?;
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
            identity_signer,
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
            identity_signer,
        )?;
        pending.advance(
            &approval_record,
            DeviceJoinJournalRecord {
                attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::RegistrationPrepared(request.clone()),
                )),
            },
        )?;
        Ok(request)
    })
}

#[allow(clippy::too_many_arguments)]
pub fn bootstrap_joining_device<'a>(
    database: &'a StoreDatabase,
    pending: &'a DeviceJoinJournalDatabase,
    storage: &'a dyn SyncStorage,
    peer_exact: Option<&'a dyn ExactSlotStorage>,
    identity_signer: &'a UserKeypair,
    bootstrap: ProviderReadyDeviceBootstrap,
    published_at: &'a str,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<DeviceJoinReadiness, DeviceJoinError>> + Send + 'a>,
> {
    Box::pin(async move {
        let offer = &bootstrap.bootstrap.request.approval.request.offer;
        let mut history_verifier =
            crate::sync::store::pull::MergeHistoryVerifier::new(storage, &offer.store_root).await?;
        let attempt_owner = Box::pin(crate::sync::store_objects::load_registration_ref_with_root(
            history_verifier.storage(),
            history_verifier.root(),
            history_verifier.verified_root(),
            &offer.owner_registration,
        ))
        .await?
        .value;
        let administrator = Box::pin(crate::sync::store_objects::load_registration_ref_with_root(
            history_verifier.storage(),
            history_verifier.root(),
            history_verifier.verified_root(),
            &offer.provider_admin.administrator,
        ))
        .await?
        .value;
        let (verified_attempt, bootstrap_plan) = Box::pin(
            crate::sync::store::pull::verify_attempt_and_prepare_device_join_bootstrap(
                &mut history_verifier,
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
        let proof = Box::pin(crate::sync::store::bootstrap_pending_device(
            database,
            storage,
            identity_signer,
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
                let exact = peer_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?;
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
                        identity_signer,
                    ))
                    .await
                    .map_err(provider_error)?,
                )
            }
            _ => return Err(DeviceJoinError::AttemptMismatch),
        };
        let readiness = DeviceJoinReadiness { proof, provider };
        let pending = pending.clone();
        let bootstrap = Box::new(bootstrap);
        let readiness = Box::new(readiness);
        tokio::task::spawn_blocking(move || {
            record_joiner_readiness(&pending, *bootstrap, *readiness)
        })
        .await
        .map_err(|error| {
            DeviceJoinError::Store(format!("joiner readiness journal task failed: {error}"))
        })?
    })
}

fn record_joiner_readiness(
    pending: &DeviceJoinJournalDatabase,
    bootstrap: ProviderReadyDeviceBootstrap,
    readiness: DeviceJoinReadiness,
) -> Result<DeviceJoinReadiness, DeviceJoinError> {
    let offer = &bootstrap.bootstrap.request.approval.request.offer;
    let current = pending
        .load(offer.attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    let prepared = match &*current.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(request))
            if request == &*bootstrap.bootstrap.request =>
        {
            current
        }
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
    pending.advance(&prepared, provider_ready.clone())?;
    let registration_intent = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::RegistrationCreateIntent(bootstrap.clone()),
        )),
    };
    pending.advance(&provider_ready, registration_intent.clone())?;
    let registration_created = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::RegistrationCreated(readiness.proof.registration.clone()),
        )),
    };
    pending.advance(&registration_intent, registration_created.clone())?;
    let ack_intent = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::AckCreateIntent(readiness.proof.registration.clone()),
        )),
    };
    pending.advance(&registration_created, ack_intent.clone())?;
    let ack_created = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::AckCreated(readiness.proof.initial_ack.clone()),
        )),
    };
    pending.advance(&ack_intent, ack_created.clone())?;
    let ready_record = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(
            readiness.clone(),
        ))),
    };
    match readiness.provider {
        DeviceProviderReadiness::SamePrincipal => pending.advance(&ack_created, ready_record)?,
        DeviceProviderReadiness::CrossPrincipal(_) => {
            let response_intent = DeviceJoinJournalRecord {
                attempt_id: offer.attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::ResponseCreateIntent(readiness.clone()),
                )),
            };
            pending.advance(&ack_created, response_intent.clone())?;
            pending.advance(&response_intent, ready_record)?;
        }
    }
    Ok(readiness)
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

pub async fn close_joining_device(
    pending: &DeviceJoinJournalDatabase,
    storage: &dyn SyncStorage,
    peer_exact: &dyn ExactSlotStorage,
    root: &StoreRootRef,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
) -> Result<JoinerJoinTerminal, DeviceJoinError> {
    require_cancelled_outcome(&cancellation.outcome)?;
    let attempt_ref = cancellation.outcome.attempt().clone();
    let current = pending
        .load(attempt_ref.attempt_id, DeviceJoinRole::Joiner)?
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
    let allowed = matches!(
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
    );
    if !allowed {
        return Err(DeviceJoinError::JournalConflict);
    }
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
    let mut history_verifier =
        crate::sync::store::pull::MergeHistoryVerifier::new(storage, root).await?;
    let owner = crate::sync::store_objects::load_registration_ref_with_root(
        history_verifier.storage(),
        history_verifier.root(),
        history_verifier.verified_root(),
        &unverified_attempt.owner_registration,
    )
    .await?
    .value;
    let attempt = crate::sync::store::pull::load_verified_device_join_attempt(
        &mut history_verifier,
        &attempt_ref,
        &owner,
    )
    .await?
    .value;
    let outcome = crate::sync::store_objects::load_device_join_outcome_ref(
        storage,
        root,
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
    let joining_device_signer = attempt
        .expected_registration
        .device_signer(identity_signer)?;
    let (registration, initial_ack, response, prior_state_hash, intent) = match &*current.progress {
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
            let registration = observe_exact_slot(peer_exact, &attempt.registration_slot).await?;
            let initial_ack = observe_exact_slot(
                peer_exact,
                attempt.expected_registration.acknowledgements.first_slot(),
            )
            .await?;
            let response = match &attempt.provider_response {
                DeviceProviderResponseReservation::SamePrincipal => {
                    JoinerResponseDisposition::SamePrincipal
                }
                DeviceProviderResponseReservation::CrossPrincipal { response_slot } => {
                    JoinerResponseDisposition::Slot(
                        observe_exact_slot(peer_exact, response_slot).await?,
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
            pending.advance(&current, intent.clone())?;
            (
                registration,
                initial_ack,
                response,
                prior_state_hash,
                intent,
            )
        }
    };
    for slot in canonical_cleanup_slots(&attempt)? {
        ensure_exact_slot_absent(peer_exact, &slot).await?;
    }
    let closure = JoinerJoinClosure::signed(
        cancellation.outcome,
        attempt.expected_registration,
        registration,
        initial_ack,
        response,
        prior_state_hash,
        &joining_device_signer,
    )?;
    pending.advance(
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn revoke_joining_device_writes(
    database: &StoreDatabase,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
    revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
    executor_grant: ProviderAdminGrantId,
) -> Result<JoinerJoinTerminal, DeviceJoinError> {
    let db = database.sqlite();
    let attempt_id = cancellation.outcome.attempt().attempt_id;
    if let Some(current) = load_store_journal(db, attempt_id, DeviceJoinRole::Joiner).await? {
        return match &*current.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
                Ok(JoinerJoinTerminal::WriteRevoked(revocation.clone()))
            }
            _ => Err(DeviceJoinError::JournalConflict),
        };
    }
    let revocation = Box::pin(sign_device_join_producer_write_revocation(
        database,
        history_verifier,
        authorization,
        identity_signer,
        cancellation,
        DeviceJoinProducer::Joiner,
        revocation_executor,
        executor_grant,
    ))
    .await?;
    begin_store_replacement_terminal(
        db,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::WriteRevoked(revocation.clone()),
            )),
        },
    )
    .await?;
    Ok(JoinerJoinTerminal::WriteRevoked(revocation))
}

#[allow(clippy::too_many_arguments)]
pub async fn accept_joiner_device_join_cleanup(
    pending: &DeviceJoinJournalDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activation: DeviceJoinCleanupActivation,
) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
    let attempt_id = activation.receipt.attempt_id;
    let current = pending
        .load(attempt_id, DeviceJoinRole::Joiner)?
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
    let mut history_verifier =
        crate::sync::store::pull::MergeHistoryVerifier::new(storage, root).await?;
    let evidence = crate::sync::store::pull::load_device_join_cleanup_activation(
        &mut history_verifier,
        &activation,
    )
    .await?;
    let receipt_terminal = crate::sync::store::pull::verify_device_join_cleanup_activation(
        &mut history_verifier,
        evidence,
    )
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
        pending.advance(&current, activated)?;
    } else {
        advance_joiner_cleanup_from_replacement(pending, &current, activated)?;
    }
    Ok(activation)
}

fn advance_joiner_cleanup_from_replacement(
    pending: &DeviceJoinJournalDatabase,
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
    let connection = Connection::open(pending.path())?;
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

pub fn complete_joiner_device_join_cleanup(
    pending: &DeviceJoinJournalDatabase,
    activation: DeviceJoinCleanupActivation,
) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
    let attempt_id = activation.receipt.attempt_id;
    let current = pending
        .load(attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CancelledComplete(existing)) =
        &*current.progress
    {
        if existing == &activation {
            return Ok(existing.clone());
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(durable)) =
        &*current.progress
    else {
        return Err(DeviceJoinError::JournalConflict);
    };
    if durable != &activation {
        return Err(DeviceJoinError::JournalConflict);
    }
    pending.advance(
        &current,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::CancelledComplete(activation.clone()),
            )),
        },
    )?;
    Ok(activation)
}

pub fn complete_device_join<'a>(
    database: &'a StoreDatabase,
    pending: &'a DeviceJoinJournalDatabase,
    storage: &'a dyn SyncStorage,
    activation: DeviceJoinActivation,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<JoinedStore, DeviceJoinError>> + Send + 'a>,
> {
    Box::pin(async move {
        let db = database.sqlite();
        let attempt_id = activation.outcome.attempt().attempt_id;
        if let Some(record) = load_store_journal(db, attempt_id, DeviceJoinRole::Joiner).await? {
            let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Activated(existing)) =
                &*record.progress
            else {
                return Err(DeviceJoinError::JournalConflict);
            };
            let joined = Box::pin(materialize_joined_store_activation(
                database, storage, activation,
            ))
            .await?;
            return (existing == &joined)
                .then_some(joined)
                .ok_or(DeviceJoinError::JournalConflict);
        }
        let current_readiness = observe_device_join_activation(pending, &activation)?;
        let joined = Box::pin(materialize_joined_store_activation(
            database, storage, activation,
        ))
        .await?;
        if current_readiness.proof.registration != joined.registration {
            return Err(DeviceJoinError::JournalConflict);
        }
        let current = pending
            .load(attempt_id, DeviceJoinRole::Joiner)?
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
        let pending_path = pending.path().to_string_lossy().into_owned();
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
    })
}

pub fn observe_device_join_activation(
    pending: &DeviceJoinJournalDatabase,
    activation: &DeviceJoinActivation,
) -> Result<DeviceJoinReadiness, DeviceJoinError> {
    if !matches!(activation.outcome, DeviceJoinOutcomeRef::Activated { .. }) {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let attempt_id = activation.outcome.attempt().attempt_id;
    let current = pending
        .load(attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    match &*current.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
            let observed = DeviceJoinJournalRecord {
                attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::ActivationObserved {
                        readiness: readiness.clone(),
                        activation: activation.clone(),
                    },
                )),
            };
            pending.advance(&current, observed)?;
            Ok(readiness.clone())
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ActivationObserved {
            readiness,
            activation: existing,
        }) if existing == activation => Ok(readiness.clone()),
        _ => Err(DeviceJoinError::JournalConflict),
    }
}
