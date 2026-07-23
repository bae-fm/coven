use super::authority::{authorize_store, load_local_store_root, resolved_provider_admin};
use super::joiner::cross_challenge_context;
use super::journal::{
    advance_store_journal, begin_store_journal, database_error, load_store_journal, provider_error,
};
use super::*;

impl Store {
    #[doc(hidden)]
    pub async fn begin_device_join(
        &self,
        identity_signer: &UserKeypair,
        member_pubkey: &str,
        provider_admin_grant: ProviderAdminGrantId,
    ) -> Result<DeviceJoinOffer, DeviceJoinError> {
        let authorized = authorize_store(self).await?;
        begin_device_join(
            authorized.database(),
            authorized.storage(),
            authorized.membership(),
            identity_signer,
            member_pubkey,
            provider_admin_grant,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn abandon_device_join(
        &self,
        identity_signer: &UserKeypair,
        offer: DeviceJoinOffer,
    ) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
        let authorized = authorize_store(self).await?;
        abandon_device_join(
            authorized.database(),
            authorized.storage(),
            authorized.membership(),
            identity_signer,
            offer,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn accept_device_registration_request(
        &self,
        identity_signer: &UserKeypair,
        request: DeviceRegistrationRequest,
    ) -> Result<ProvisionalDeviceBootstrap, DeviceJoinError> {
        let authorized = authorize_store(self).await?;
        accept_device_registration_request(
            authorized.database(),
            authorized.storage(),
            authorized.membership(),
            identity_signer,
            request,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn cancel_device_join(
        &self,
        identity_signer: &UserKeypair,
        attempt: DeviceJoinAttemptRef,
    ) -> Result<DeviceJoinCancellation, DeviceJoinError> {
        let authorized = authorize_store(self).await?;
        cancel_device_join(
            authorized.database(),
            authorized.storage(),
            authorized.membership(),
            identity_signer,
            attempt,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn finalize_device_join(
        &self,
        identity_signer: &UserKeypair,
        completion: DeviceProviderAdmissionCompletion,
    ) -> Result<DeviceJoinActivation, DeviceJoinError> {
        let authorized = authorize_store(self).await?;
        finalize_device_join(
            authorized.database(),
            authorized.storage(),
            authorized.membership(),
            identity_signer,
            completion,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn complete_owner_device_join_cleanup(
        &self,
        activation: DeviceJoinCleanupActivation,
    ) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
        complete_owner_device_join_cleanup(
            self.database(),
            activation.receipt.attempt_id,
            activation,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn begin_device_join(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    member_pubkey: &str,
    provider_admin_grant: ProviderAdminGrantId,
) -> Result<DeviceJoinOffer, DeviceJoinError> {
    let db = database.sqlite();
    validate_member_for_join(member_pubkey, &authorization.current_members())?;
    let owner_pubkey = keys::public_key_hex(identity_signer);
    let owner_grant = authorization
        .active_owner_grant(&owner_pubkey)
        .ok_or(DeviceJoinError::OwnerAuthorityRequired)?;
    let provider_admin = resolved_provider_admin(authorization, &provider_admin_grant)?;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let (root, owner_registration, owner, owner_device_signer) =
        crate::sync::store::operations::load_local_store_authority(
            database,
            &device_id,
            identity_signer,
        )
        .await?;
    let binding = storage.provider_binding().await?;
    let attempt_id =
        DeviceJoinAttemptId::from_hash(ObjectHash::digest(db.new_write_id().as_str().as_bytes()));
    let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let attempt_slot = storage
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
    let outcome_slot = storage
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
        owner_registration,
        owner_grant,
        provider_admin,
        &owner,
        &owner_device_signer,
    )?;
    begin_store_journal(
        db,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(
                offer.clone(),
            ))),
        },
    )
    .await?;
    Ok(offer)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn abandon_device_join(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    offer: DeviceJoinOffer,
) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
    let db = database.sqlite();
    let current = load_store_journal(db, offer.attempt_id, DeviceJoinRole::Owner)
        .await?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(existing)) =
        &*current.progress
    {
        return Ok(existing.clone());
    }
    let owner = database
        .activated_store_device_registration(offer.owner_registration.clone())
        .await
        .map_err(database_error)?;
    offer.verify(&owner)?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if owner.device_id.to_string() != local_device_id
        || !authorization.is_owner_now(&keys::public_key_hex(identity_signer))
    {
        return Err(DeviceJoinError::OwnerAuthorityRequired);
    }
    let owner_signer = owner.device_signer(identity_signer)?;
    let abandonment_object = DeviceJoinAbandonmentObject::signed(&offer, &owner, &owner_signer)?;
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        offer.store_root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAbandonment,
    );
    let prefix =
        crate::sync::store_commit::device_join_abandonment_semantic_prefix(offer.attempt_id);
    let prepared = storage.prepare_protocol_object(
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
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(durable)) if durable == &offer => {
            advance_store_journal(db, &current, intent.clone()).await?;
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(request))
            if request.approval.request.offer.as_ref() == &offer =>
        {
            advance_store_journal(db, &current, intent.clone()).await?;
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
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != abandonment_object.to_bytes() {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    abandonment_ref.verify(&abandonment_object, &owner)?;
    let plan = crate::sync::store::operations::prepare_plan(
        database,
        storage,
        authorization,
        &local_device_id,
        identity_signer,
    )
    .await?;
    let activation = crate::sync::store::operations::activate_store_operation_commit(
        database,
        storage,
        plan,
        crate::sync::store::operations::StoreOperationBatch::Abandonment(abandonment_ref.clone()),
    )
    .await?;
    let abandonment = DeviceJoinAbandonment {
        abandonment: abandonment_ref,
        abandonment_activation: activation,
    };
    advance_store_journal(
        db,
        &intent,
        DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(
                abandonment.clone(),
            ))),
        },
    )
    .await?;
    Ok(abandonment)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn accept_device_registration_request(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    request: DeviceRegistrationRequest,
) -> Result<ProvisionalDeviceBootstrap, DeviceJoinError> {
    let db = database.sqlite();
    let root_value = load_local_store_root(database, storage).await?;
    request.verify()?;
    let offer = &request.approval.request.offer;
    let owner =
        Box::pin(database.activated_store_device_registration(offer.owner_registration.clone()))
            .await
            .map_err(database_error)?;
    let administrator = Box::pin(
        database.activated_store_device_registration(offer.provider_admin.administrator.clone()),
    )
    .await
    .map_err(database_error)?;
    request
        .approval
        .verify(&root_value, &owner, &administrator)?;
    crate::sync::store::verify_accepted_provider_access_activation(
        storage,
        &offer.store_root,
        &request.approval.access_grant,
        &offer.provider_admin,
        &administrator,
    )
    .await?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if owner.device_id.to_string() != local_device_id
        || !authorization.is_owner_now(&keys::public_key_hex(identity_signer))
    {
        return Err(DeviceJoinError::OwnerAuthorityRequired);
    }
    let owner_signer = owner.device_signer(identity_signer)?;
    let offered = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(
            *offer.clone(),
        ))),
    };
    let durable = begin_store_journal(db, offered.clone()).await?;
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
    let plan = crate::sync::store::operations::prepare_plan(
        database,
        storage,
        authorization,
        &local_device_id,
        identity_signer,
    )
    .await?;
    let cut = plan.predecessor_cut()?;
    if !crate::sync::store::pull::history_cut_covers(
        storage,
        &offer.store_root,
        &cut,
        &request.approval.access_grant.activation,
    )
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
    advance_store_journal(db, &offered, requested.clone()).await?;
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
    let prefix = crate::sync::store_commit::device_join_attempt_semantic_prefix(offer.attempt_id);
    let prepared = storage.prepare_protocol_object(
        &context,
        offer.attempt_slot.clone(),
        &prefix,
        attempt.to_bytes(),
    )?;
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
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
    let activation = crate::sync::store::operations::activate_store_operation_commit(
        database,
        storage,
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
    advance_store_journal(
        db,
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn cancel_device_join(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    attempt_ref: DeviceJoinAttemptRef,
) -> Result<DeviceJoinCancellation, DeviceJoinError> {
    let db = database.sqlite();
    let current = load_store_journal(db, attempt_ref.attempt_id, DeviceJoinRole::Owner)
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
    let root = database
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
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
    let attempt = Box::pin(crate::sync::store::load_verified_device_join_attempt_ref(
        storage,
        &root,
        &attempt_ref,
        &owner,
    ))
    .await?
    .value;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if owner.device_id.to_string() != local_device_id
        || !authorization.is_owner_now(&keys::public_key_hex(identity_signer))
    {
        return Err(DeviceJoinError::OwnerAuthorityRequired);
    }
    let owner_signer = owner.device_signer(identity_signer)?;
    let outcome = crate::sync::store_commit::DeviceJoinOutcome::signed(
        attempt_ref.clone(),
        crate::sync::store_commit::DeviceJoinOutcomeBody::Cancelled,
        attempt.owner_registration.clone(),
        attempt.owner_grant.clone(),
        &owner,
        &owner_signer,
    )?;
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinOutcome,
    );
    let prefix =
        crate::sync::store_commit::device_join_outcome_semantic_prefix(attempt_ref.attempt_id);
    let prepared = storage.prepare_protocol_object(
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
            advance_store_journal(db, &current, intent.clone()).await?;
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
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != outcome.to_bytes() {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let verified_outcome = crate::sync::store_objects::load_device_join_outcome_ref(
        storage,
        &root,
        &outcome_ref,
        &owner,
    )
    .await?;
    if verified_outcome.value != outcome {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let plan = crate::sync::store::operations::prepare_plan(
        database,
        storage,
        authorization,
        &local_device_id,
        identity_signer,
    )
    .await?;
    let outcome_activation = crate::sync::store::operations::activate_store_operation_commit(
        database,
        storage,
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
    advance_store_journal(
        db,
        &intent,
        DeviceJoinJournalRecord {
            attempt_id: attempt_ref.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(
                cancellation.clone(),
            ))),
        },
    )
    .await?;
    Ok(cancellation)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn complete_owner_device_join_cleanup(
    database: &StoreDatabase,
    attempt_id: DeviceJoinAttemptId,
    activation: DeviceJoinCleanupActivation,
) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
    let db = database.sqlite();
    let current = load_store_journal(db, attempt_id, DeviceJoinRole::Owner)
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
    advance_store_journal(
        db,
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

pub async fn observe_device_join_abandonment(
    pending: &DeviceJoinJournalDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    abandonment: DeviceJoinAbandonment,
) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
    let current = pending
        .load(abandonment.abandonment.attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Abandoned(existing)) =
        &*current.progress
    {
        if existing == &abandonment {
            return Ok(existing.clone());
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAbandonment,
    );
    let prefix = crate::sync::store_commit::device_join_abandonment_semantic_prefix(
        abandonment.abandonment.attempt_id,
    );
    let bytes = storage
        .read_protocol_object(&context, &abandonment.abandonment.object, &prefix)
        .await?;
    let object: DeviceJoinAbandonmentObject = serde_json::from_slice(&bytes)?;
    let owner = crate::sync::store_objects::load_registration_ref(
        storage,
        root,
        &object.owner_registration,
    )
    .await?
    .value;
    abandonment.abandonment.verify(&object, &owner)?;
    let (activation, author) = crate::sync::store::pull::load_commit_with_author(
        storage,
        root,
        &abandonment.abandonment_activation,
    )
    .await?;
    if author != owner
        || !activation
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
    match &*current.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(_))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ApprovalReceived(_)) => {}
        _ => return Err(DeviceJoinError::JournalConflict),
    }
    pending.advance(
        &current,
        DeviceJoinJournalRecord {
            attempt_id: abandonment.abandonment.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::Abandoned(abandonment.clone()),
            )),
        },
    )?;
    Ok(abandonment)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn finalize_device_join(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    completion: DeviceProviderAdmissionCompletion,
) -> Result<DeviceJoinActivation, DeviceJoinError> {
    let db = database.sqlite();
    let attempt_ref = completion.readiness.proof.attempt.clone();
    let attempt_id = attempt_ref.attempt_id;
    let current = load_store_journal(db, attempt_id, DeviceJoinRole::Owner)
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
    let owner = database
        .activated_store_device_registration(offer.owner_registration.clone())
        .await
        .map_err(database_error)?;
    let owner_signer = owner.device_signer(identity_signer)?;
    let attempt = crate::sync::store::load_verified_device_join_attempt_ref(
        storage,
        &offer.store_root,
        &attempt_ref,
        &owner,
    )
    .await?
    .value;
    let registration = crate::sync::store_objects::load_registration_ref(
        storage,
        &offer.store_root,
        &completion.readiness.proof.registration,
    )
    .await?
    .value;
    let ack = crate::sync::store_objects::load_store_ack_ref(
        storage,
        &offer.store_root,
        &completion.readiness.proof.initial_ack,
        &registration,
    )
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
            let administrator = database
                .activated_store_device_registration(offer.provider_admin.administrator.clone())
                .await
                .map_err(database_error)?;
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
            let prepared = storage.prepare_protocol_object(
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
            advance_store_journal(db, &current, intent.clone()).await?;
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
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != outcome.to_bytes() {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let activated_registration = crate::sync::store::operations::DeviceJoinRegistrationActivation {
        reference: crate::sync::store_commit::ActivatedStoreDeviceRegistrationRef {
            registration: completion.readiness.proof.registration.clone(),
            authority: crate::sync::store_commit::StoreDeviceRegistrationActivationRef::Join {
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
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let plan = crate::sync::store::operations::prepare_plan(
        database,
        storage,
        authorization,
        &local_device_id,
        identity_signer,
    )
    .await?;
    let activation_ref = crate::sync::store::operations::activate_store_operation_commit(
        database,
        storage,
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
    advance_store_journal(
        db,
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

pub fn materialize_joined_store_activation<'a>(
    database: &'a StoreDatabase,
    storage: &'a dyn SyncStorage,
    activation: DeviceJoinActivation,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<JoinedStore, DeviceJoinError>> + Send + 'a>,
> {
    Box::pin(async move {
        if !matches!(&activation.outcome, DeviceJoinOutcomeRef::Activated { .. }) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let root = database
            .local_store_root_ref()
            .await
            .map_err(database_error)?
            .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
        let attempt_ref = activation.outcome.attempt().clone();
        let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let attempt_bytes = storage
            .read_protocol_object(
                &attempt_context,
                &attempt_ref.object,
                &crate::sync::store_commit::device_join_attempt_semantic_prefix(
                    attempt_ref.attempt_id,
                ),
            )
            .await?;
        let unverified_attempt: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)?;
        let owner = database
            .activated_store_device_registration(unverified_attempt.owner_registration.clone())
            .await
            .map_err(database_error)?;
        let attempt = crate::sync::store::load_verified_device_join_attempt_ref(
            storage,
            &root,
            &attempt_ref,
            &owner,
        )
        .await?
        .value;
        Box::pin(crate::sync::store::materialize_device_join_activation(
            database,
            storage,
            &root,
            &activation.outcome_activation,
            &activation.outcome,
            &attempt.membership,
        ))
        .await?;
        let outcome = crate::sync::store_objects::load_device_join_outcome_ref(
            storage,
            &root,
            &activation.outcome,
            &owner,
        )
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
            || attempt.expected_registration.to_bytes() != local.registration_bytes
        {
            return Err(DeviceJoinError::ActivationNotMaterialized);
        }
        let joined = JoinedStore {
            store_root: root,
            registration: readiness.registration.clone(),
            activation,
        };
        Ok(joined)
    })
}
