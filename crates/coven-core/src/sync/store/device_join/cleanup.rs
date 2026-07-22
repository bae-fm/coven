use super::authority::resolved_provider_admin;
use super::journal::{
    advance_store_journal, database_error, load_store_journal, require_distinct_slots,
};
use super::*;

#[async_trait::async_trait]
pub trait DeviceJoinWriteRevocationExecutor: Send + Sync {
    /// Idempotently withdraws the exact provider authority, then verifies that
    /// the withdrawn authority cannot write any `protected_slots` before
    /// returning its provider-specific evidence.
    async fn revoke_write_authority(
        &self,
        producer: DeviceJoinProducer,
        authority: &ProviderWriteAuthorityRef,
        locator: &crate::sync::provider::ProviderAccessLocator,
        protected_slots: &[ObjectSlot],
    ) -> Result<ProviderAccessWithdrawal, DeviceJoinError>;
}

pub fn prepare_device_join_cleanup<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    executor_exact: &'a dyn ExactSlotStorage,
    authorization: &'a MembershipChain,
    identity_signer: &'a UserKeypair,
    cancellation: DeviceJoinCancellation,
    administrator_terminal: ProviderAdminJoinTerminal,
    joiner_terminal: JoinerJoinTerminal,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<DeviceJoinCleanupReceipt, DeviceJoinError>> + 'a>,
> {
    Box::pin(prepare_device_join_cleanup_inner(
        db,
        storage,
        executor_exact,
        authorization,
        identity_signer,
        Box::new(cancellation),
        Box::new(administrator_terminal),
        Box::new(joiner_terminal),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn prepare_device_join_cleanup_inner(
    db: &Database,
    storage: &dyn SyncStorage,
    executor_exact: &dyn ExactSlotStorage,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    cancellation: Box<DeviceJoinCancellation>,
    administrator_terminal: Box<ProviderAdminJoinTerminal>,
    joiner_terminal: Box<JoinerJoinTerminal>,
) -> Result<DeviceJoinCleanupReceipt, DeviceJoinError> {
    require_cancelled_outcome(&cancellation.outcome)?;
    let attempt_ref = cancellation.outcome.attempt().clone();
    let current = load_store_journal(db, attempt_ref.attempt_id, DeviceJoinRole::Owner)
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
    if durable_cancellation != cancellation.as_ref() {
        return Err(DeviceJoinError::JournalConflict);
    }
    let root = db
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
    let owner = db
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
    validate_terminals(
        &cancellation.outcome,
        administrator_terminal.as_ref(),
        joiner_terminal.as_ref(),
    )?;
    verify_cleanup_terminals(
        db,
        administrator_terminal.as_ref(),
        joiner_terminal.as_ref(),
    )
    .await?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let (local_root, executor_ref, executor, executor_signer) =
        crate::sync::store::operations::load_local_store_authority(
            db,
            &local_device_id,
            identity_signer,
        )
        .await?;
    if local_root != root || !authorization.is_owner_now(&executor.author_pubkey) {
        return Err(DeviceJoinError::OwnerAuthorityRequired);
    }
    let effective_executor = resolved_provider_admin(
        authorization,
        &attempt
            .provider_approval
            .request
            .offer
            .provider_admin
            .grant_id,
    )?;
    if effective_executor != *attempt.provider_approval.request.offer.provider_admin
        || effective_executor.administrator != executor_ref
        || effective_executor.provider != executor.provider
    {
        return Err(DeviceJoinError::ProviderAdministratorRequired);
    }
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinCleanupReceipt,
    );
    let prefix = crate::sync::store_commit::device_join_cleanup_receipt_semantic_prefix(
        attempt_ref.attempt_id,
    );
    let (receipt_object, receipt_ref, prepared, intent) = match &*current.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(_)) => {
            let plan = crate::sync::store::operations::prepare_plan(
                db,
                storage,
                authorization,
                &local_device_id,
                identity_signer,
            )
            .await?;
            let receipt_object = DeviceJoinCleanupReceiptObject::signed(
                &attempt,
                cancellation.outcome.clone(),
                administrator_terminal.as_ref().clone(),
                joiner_terminal.as_ref().clone(),
                canonical_cleanup_slots(&attempt)?,
                plan.membership_state().clone(),
                attempt
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
            let slot = storage
                .allocate_protocol_slot(&context, &prefix, ".json")
                .await?;
            let prepared = storage.prepare_protocol_object(
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
                        cancellation: cancellation.as_ref().clone(),
                        receipt: receipt_ref.clone(),
                        receipt_bytes: receipt_object.to_bytes(),
                        prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
                    },
                )),
            };
            advance_store_journal(db, &current, intent.clone()).await?;
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
                || &receipt_object.administrator_terminal != administrator_terminal.as_ref()
                || &receipt_object.joiner_terminal != joiner_terminal.as_ref()
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
    receipt_object.verify(&attempt, &executor)?;
    for slot in &receipt_object.deleted_slots {
        ensure_exact_slot_absent(executor_exact, slot).await?;
    }
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != receipt_object.to_bytes() {
        return Err(DeviceJoinError::CleanupMismatch);
    }
    receipt_ref.verify(&receipt_object, &executor)?;
    let receipt = DeviceJoinCleanupReceipt {
        receipt: receipt_ref,
    };
    advance_store_journal(
        db,
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

pub(super) async fn sign_device_join_producer_write_revocation(
    db: &Database,
    storage: &dyn SyncStorage,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
    producer: DeviceJoinProducer,
    revocation_executor: &dyn DeviceJoinWriteRevocationExecutor,
    executor_grant: ProviderAdminGrantId,
) -> Result<DeviceJoinProducerWriteRevocation, DeviceJoinError> {
    require_cancelled_outcome(&cancellation.outcome)?;
    let attempt_ref = cancellation.outcome.attempt().clone();
    let root = db
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
    let owner = db
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
    let executor_admin = resolved_provider_admin(authorization, &executor_grant)?;
    let executor = db
        .activated_store_device_registration(executor_admin.administrator.clone())
        .await
        .map_err(database_error)?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if executor.device_id.to_string() != local_device_id {
        return Err(DeviceJoinError::ProviderAdministratorRequired);
    }
    let executor_signer = executor.device_signer(identity_signer)?;
    let (authority, protected_slots, locator) = match producer {
        DeviceJoinProducer::ProviderAdministrator => {
            let DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) =
                &attempt.provider_approval.admission
            else {
                return Err(DeviceJoinError::CleanupMismatch);
            };
            (
                ProviderWriteAuthorityRef::ProviderAdministrator(
                    attempt
                        .provider_approval
                        .request
                        .offer
                        .provider_admin
                        .grant_id
                        .clone(),
                ),
                vec![challenge.administrator_object.slot.clone()],
                &attempt
                    .provider_approval
                    .request
                    .offer
                    .provider_admin
                    .access,
            )
        }
        DeviceJoinProducer::Joiner => {
            let mut slots = vec![
                attempt.registration_slot.clone(),
                attempt
                    .expected_registration
                    .acknowledgements
                    .first_slot()
                    .clone(),
            ];
            if let DeviceProviderResponseReservation::CrossPrincipal { response_slot } =
                &attempt.provider_response
            {
                slots.push(response_slot.clone());
            }
            (
                ProviderWriteAuthorityRef::MemberAccess(
                    attempt.provider_approval.access_grant.grant_ref.clone(),
                ),
                slots,
                &attempt.provider_approval.access_grant.grant.locator,
            )
        }
    };
    let withdrawal = revocation_executor
        .revoke_write_authority(producer, &authority, locator, &protected_slots)
        .await?;
    withdrawal
        .verify_for_locator(locator)
        .map_err(|_| DeviceJoinError::CleanupMismatch)?;
    DeviceJoinProducerWriteRevocation::signed(
        cancellation.outcome,
        producer,
        authority,
        protected_slots,
        withdrawal,
        executor_grant,
        executor_admin.administrator,
        &executor,
        &executor_signer,
    )
}

#[allow(clippy::too_many_arguments)]
pub async fn activate_device_join_cleanup(
    db: &Database,
    storage: &dyn SyncStorage,
    authorization: &MembershipChain,
    identity_signer: &UserKeypair,
    attempt_id: DeviceJoinAttemptId,
    receipt: DeviceJoinCleanupReceipt,
) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
    let current = load_store_journal(db, attempt_id, DeviceJoinRole::Owner)
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
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let plan = crate::sync::store::operations::prepare_plan(
        db,
        storage,
        authorization,
        &local_device_id,
        identity_signer,
    )
    .await?;
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinCleanupReceipt,
    );
    let prefix = crate::sync::store_commit::device_join_cleanup_receipt_semantic_prefix(
        receipt.receipt.attempt_id,
    );
    let bytes = storage
        .read_protocol_object(&context, &receipt.receipt.object, &prefix)
        .await?;
    let receipt_object: DeviceJoinCleanupReceiptObject = serde_json::from_slice(&bytes)?;
    let executor = db
        .activated_store_device_registration(receipt_object.executor.clone())
        .await
        .map_err(database_error)?;
    receipt.receipt.verify(&receipt_object, &executor)?;
    if receipt_object.store_root_hash != root.store_root_hash
        || plan.membership_state() != &receipt_object.membership
    {
        return Err(DeviceJoinError::CleanupMismatch);
    }
    let activation_ref = crate::sync::store::operations::activate_store_operation_commit(
        db,
        storage,
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
    advance_store_journal(
        db,
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

pub fn validate_member_for_join(
    member_pubkey: &str,
    members: &[(String, MemberRole)],
) -> Result<(), DeviceJoinError> {
    if members
        .iter()
        .any(|(pubkey, role)| pubkey == member_pubkey && role.can_write())
    {
        Ok(())
    } else {
        Err(DeviceJoinError::MemberNotEligible)
    }
}

pub fn canonical_cleanup_slots(
    attempt: &DeviceJoinAttempt,
) -> Result<Vec<ObjectSlot>, DeviceJoinError> {
    let mut slots = vec![
        attempt.registration_slot.clone(),
        attempt
            .expected_registration
            .acknowledgements
            .first_slot()
            .clone(),
    ];
    match (
        &attempt.provider_approval.admission,
        &attempt.provider_response,
    ) {
        (
            DeviceProviderAdmissionChallenge::SamePrincipal,
            DeviceProviderResponseReservation::SamePrincipal,
        ) => {}
        (
            DeviceProviderAdmissionChallenge::CrossPrincipal(challenge),
            DeviceProviderResponseReservation::CrossPrincipal { response_slot },
        ) => {
            slots.push(challenge.administrator_object.slot.clone());
            slots.push(response_slot.clone());
        }
        _ => return Err(DeviceJoinError::AttemptMismatch),
    }
    slots.sort();
    require_distinct_slots(&slots)?;
    Ok(slots)
}

pub(super) fn require_cancelled_outcome(
    outcome: &DeviceJoinOutcomeRef,
) -> Result<(), DeviceJoinError> {
    if matches!(outcome, DeviceJoinOutcomeRef::Cancelled { .. }) {
        Ok(())
    } else {
        Err(DeviceJoinError::AttemptMismatch)
    }
}

pub(super) fn validate_terminals(
    cancellation: &DeviceJoinOutcomeRef,
    administrator: &ProviderAdminJoinTerminal,
    joiner: &JoinerJoinTerminal,
) -> Result<(), DeviceJoinError> {
    let administrator_cancellation = match administrator {
        ProviderAdminJoinTerminal::Completed(completion) => {
            if completion.readiness.proof.attempt != *cancellation.attempt() {
                return Err(DeviceJoinError::AttemptMismatch);
            }
            None
        }
        ProviderAdminJoinTerminal::Cancelled(closure) => Some(&closure.cancellation),
        ProviderAdminJoinTerminal::WriteRevoked(revocation) => Some(&revocation.cancellation),
    };
    let joiner_cancellation = match joiner {
        JoinerJoinTerminal::Ready(readiness) => {
            if readiness.proof.attempt != *cancellation.attempt() {
                return Err(DeviceJoinError::AttemptMismatch);
            }
            None
        }
        JoinerJoinTerminal::Cancelled(closure) => Some(&closure.cancellation),
        JoinerJoinTerminal::WriteRevoked(revocation) => Some(&revocation.cancellation),
    };
    if administrator_cancellation.is_some_and(|value| value != cancellation)
        || joiner_cancellation.is_some_and(|value| value != cancellation)
    {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    Ok(())
}

async fn verify_cleanup_terminals(
    db: &Database,
    administrator: &ProviderAdminJoinTerminal,
    joiner: &JoinerJoinTerminal,
) -> Result<(), DeviceJoinError> {
    match administrator {
        ProviderAdminJoinTerminal::Completed(_) => {}
        ProviderAdminJoinTerminal::Cancelled(closure) => {
            let registration = db
                .activated_store_device_registration(closure.administrator_registration.clone())
                .await
                .map_err(database_error)?;
            closure.verify(&registration)?;
        }
        ProviderAdminJoinTerminal::WriteRevoked(revocation) => {
            let registration = db
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
            let registration = db
                .activated_store_device_registration(revocation.executor.clone())
                .await
                .map_err(database_error)?;
            revocation.verify(&registration)?;
        }
    }
    Ok(())
}

pub(super) async fn ensure_exact_slot_absent(
    storage: &dyn ExactSlotStorage,
    slot: &ObjectSlot,
) -> Result<(), DeviceJoinError> {
    match storage.read_at(slot).await {
        Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => Ok(()),
        Ok(_) => {
            storage
                .delete_at(slot)
                .await
                .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
            match storage.read_at(slot).await {
                Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => Ok(()),
                Ok(_) => Err(DeviceJoinError::CleanupMismatch),
                Err(error) => Err(DeviceJoinError::Provider(error.to_string())),
            }
        }
        Err(error) => Err(DeviceJoinError::Provider(error.to_string())),
    }
}

pub(super) async fn observe_exact_slot(
    storage: &dyn ExactSlotStorage,
    slot: &ObjectSlot,
) -> Result<SlotDisposition, DeviceJoinError> {
    match storage.read_at(slot).await {
        Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => {
            Ok(SlotDisposition::NeverCreated)
        }
        Ok(bytes) => Ok(SlotDisposition::Created(ExactObjectRef::new(
            slot.clone(),
            bytes.len() as u64,
            ObjectHash::digest(&bytes),
        ))),
        Err(error) => Err(DeviceJoinError::Provider(error.to_string())),
    }
}
