use super::super::*;
use super::invitation::publish_serial_membership_wraps;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SerialRemovalPlan {
    prepared: crate::sync::store_outbound::PreparedStoreOperationCommit,
    revokee_pubkey: String,
    revokee_email: Option<String>,
    wraps: Vec<crate::sync::wrapped_store_key::PreparedWrappedStoreKey>,
    keyring_payload: Vec<u8>,
    generation: u64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum SerialRemovalProgress {
    Pending,
    AccessRevoked,
    CandidateNonactivating,
    Activated { candidate: StoreBatchCommitRef },
}

struct ActivatedSerialRemoval {
    encryption: EncryptionService,
    intent_hash: crate::sync::store_commit::ObjectHash,
    generation: u64,
}

enum SerialRemovalResult {
    Activated(ActivatedSerialRemoval),
    Terminal(String),
}

fn serial_removal_receipt_is_current(
    plan: &SerialRemovalPlan,
    authorization: &crate::sync::membership::SerialAuthorizationState,
) -> Result<bool, MembershipOpsError> {
    plan.prepared.validate_closed_shape().map_err(|error| {
        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
            "terminal Serial removal candidate is invalid: {error}"
        )))
    })?;
    let Some(StoreControl::SerialMembershipAndKeyRotation {
        entry, generation, ..
    }) = plan.prepared.commit.control()
    else {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial removal does not carry a membership key rotation".to_string(),
            ),
        ));
    };
    let crate::sync::membership::SerialMembershipChange::RemoveMember { user_pubkey, .. } =
        &entry.change
    else {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial removal carries a membership grant".to_string(),
            ),
        ));
    };
    if user_pubkey != &plan.revokee_pubkey || generation != &plan.generation {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial removal plan differs from its membership rotation".to_string(),
            ),
        ));
    }
    Ok(authorization.key_generation == plan.generation
        && authorization
            .membership
            .active_grant_ids(&plan.revokee_pubkey)
            .is_empty()
        && authorization
            .active_wrapped_keys_for(&plan.revokee_pubkey)
            .is_empty())
}

async fn restore_serial_removal_provider_access(
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    plan: &SerialRemovalPlan,
) -> Result<(), MembershipOpsError> {
    let outcome = cloud_home
        .set_access(CloudAccessState::Present {
            member_pubkey: plan.revokee_pubkey.clone(),
            provider_account_email: plan.revokee_email.clone(),
        })
        .await
        .map_err(InviteError::from)?;
    if !matches!(outcome, CloudAccessOutcome::Present(_)) {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "provider returned absent while rolling back a Serial removal".to_string(),
            ),
        ));
    }
    Ok(())
}

async fn finish_nonactivating_serial_removal(
    db: &Database,
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    plan: &SerialRemovalPlan,
    intent_hash: crate::sync::store_commit::ObjectHash,
    pending_rotation: &PendingRotation,
) -> Result<(), MembershipOpsError> {
    restore_serial_removal_provider_access(cloud_home, plan).await?;
    crate::sync::store_objects::delete_exact_object(storage, &plan.prepared.reference.object)
        .await?;
    db.mark_candidate_cleanup_absent(plan.prepared.reference.object.clone())
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    db.complete_nonactivating_membership_candidate_mutation(
        intent_hash,
        plan.prepared.reference.clone(),
        vec![plan.prepared.reference.object.clone()],
        plan.wraps
            .iter()
            .map(|wrap| wrap.reference.object.clone())
            .collect(),
        Some(plan.generation),
    )
    .await
    .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    pending_rotation
        .remove_candidate(plan.generation, intent_hash)
        .map_err(|error| MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error)))
}

#[allow(clippy::too_many_arguments)]
async fn remove_serial_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    store_id: &str,
    current_encryption: &EncryptionService,
    new_key: [u8; 32],
    pending_rotation: &PendingRotation,
    db: &Database,
) -> Result<SerialRemovalResult, MembershipOpsError> {
    let root_ref = required_store_root_ref(db).await?;
    let _mutation = db.lock_membership_mutation().await;
    if let Some(receipt) = db
        .terminal_serial_removal_mutation()
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
    {
        let plan: SerialRemovalPlan =
            serde_json::from_slice(&receipt.plan_bytes).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "parse terminal Serial removal plan: {error}"
                )))
            })?;
        if plan.revokee_pubkey == public_key_hex {
            let authorization = crate::sync::store_outbound::current_serial_authorization(
                db,
                storage,
                coordination,
            )
            .await?;
            if serial_removal_receipt_is_current(&plan, &authorization)? {
                let fingerprint =
                    serde_json::from_slice(&receipt.result_bytes).map_err(|error| {
                        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                            "parse terminal Serial removal result: {error}"
                        )))
                    })?;
                return Ok(SerialRemovalResult::Terminal(fingerprint));
            }
        }
    }
    let (plan, mut progress, intent_hash) = match db
        .outbound_membership_mutation()
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
    {
        Some(row) => {
            let plan: SerialRemovalPlan =
                serde_json::from_slice(&row.plan_bytes).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "parse Serial removal plan: {error}"
                    )))
                })?;
            let progress: SerialRemovalProgress = serde_json::from_slice(&row.progress_bytes)
                .map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "parse Serial removal progress: {error}"
                    )))
                })?;
            if plan.revokee_pubkey != public_key_hex {
                return Err(MembershipOpsError::Invite(InviteError::PendingMutation(
                    "the pending Serial removal names another member".to_string(),
                )));
            }
            (plan, progress, row.intent_hash)
        }
        None => {
            let authorization = crate::sync::store_outbound::current_serial_authorization(
                db,
                storage,
                coordination,
            )
            .await?;
            let authority_refs =
                authorization.active_wrapped_keys_for(&crate::keys::public_key_hex(user_keypair));
            let current_keyring = crate::sync::invite::load_authorized_owner_keyring(
                storage,
                root_ref.store_root_hash,
                user_keypair,
                store_id,
                &authority_refs,
                current_encryption,
            )
            .await?;
            if current_keyring.current_generation() != authorization.key_generation {
                return Err(MembershipOpsError::Invite(
                    InviteError::InvalidDurableMutation(format!(
                        "authorized key generation {} differs from committed Serial generation {}",
                        current_keyring.current_generation(),
                        authorization.key_generation
                    )),
                ));
            }
            let revokee_email = authorization
                .membership
                .current_member_provider_email(public_key_hex)
                .map(str::to_string);
            let entry = authorization
                .membership
                .signed_remove_member(
                    user_keypair,
                    public_key_hex.to_string(),
                    hlc.now().to_string(),
                )
                .map_err(|error| match error {
                    crate::sync::membership::SerialMembershipError::NotAMember(pubkey) => {
                        MembershipOpsError::Invite(InviteError::NotAMember(pubkey))
                    }
                    crate::sync::membership::SerialMembershipError::LastOwner => {
                        MembershipOpsError::Invite(InviteError::LastOwner)
                    }
                    error => MembershipOpsError::Invite(InviteError::InvalidDurableMutation(
                        error.to_string(),
                    )),
                })?;
            let generation = authorization.key_generation.checked_add(1).ok_or_else(|| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(
                    "Serial key generation overflow".to_string(),
                ))
            })?;
            let new_keyring = current_keyring
                .with_appended_generation(generation, new_key)
                .map_err(|error| {
                    MembershipOpsError::Invite(InviteError::Crypto(format!(
                        "append Serial key generation: {error}"
                    )))
                })?;
            let membership_after = authorization.membership.apply(&entry).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error.to_string()))
            })?;
            let mut wraps = Vec::new();
            for (recipient, _) in membership_after.current_members() {
                wraps.push(
                    crate::sync::wrapped_store_key::prepare_wrapped_store_key(
                        storage,
                        root_ref.store_root_hash,
                        &recipient,
                        crate::sync::invite::signed_serial_wrapped_key(
                            store_id,
                            &recipient,
                            &new_keyring,
                            user_keypair,
                        )?,
                    )
                    .await?,
                );
            }
            wraps.sort_by(|left, right| left.reference.cmp(&right.reference));
            let wrapped_keys = wraps.iter().map(|wrap| wrap.reference.clone()).collect();
            let operation = crate::sync::store_outbound::prepare_store_operation_commit(
                db,
                storage,
                crate::sync::store_outbound::StoreOperationPreparation::Serial { coordination },
                device_id,
                user_keypair,
            )
            .await?;
            let prepared = crate::sync::store_outbound::prepare_store_operation_candidate(
                db,
                storage,
                operation,
                crate::sync::store_outbound::StoreOperationBatch::Control(
                    StoreControl::SerialMembershipAndKeyRotation {
                        entry,
                        generation,
                        wrapped_keys,
                    },
                ),
            )
            .await?;
            let keyring_payload = new_keyring.to_keyring_payload().map_err(|error| {
                MembershipOpsError::Invite(InviteError::Crypto(format!(
                    "serialize Serial rotated keyring: {error}"
                )))
            })?;
            let plan = SerialRemovalPlan {
                prepared,
                revokee_pubkey: public_key_hex.to_string(),
                revokee_email,
                wraps,
                keyring_payload,
                generation,
            };
            let plan_bytes = serde_json::to_vec(&plan).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize Serial removal plan: {error}"
                )))
            })?;
            let progress = SerialRemovalProgress::Pending;
            let progress_bytes = serde_json::to_vec(&progress).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize Serial removal progress: {error}"
                )))
            })?;
            let remote_objects = plan
                .prepared
                .membership_control_remote_objects(&plan.wraps)?;
            let intent_hash = db
                .stage_serial_removal_candidate_mutation(
                    plan_bytes,
                    progress_bytes,
                    remote_objects,
                    generation,
                )
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
            (plan, progress, intent_hash)
        }
    };
    match &progress {
        SerialRemovalProgress::Activated { .. } => {
            pending_rotation.mark_committed_mutation(plan.generation, intent_hash)
        }
        _ => pending_rotation.mark_candidate(plan.generation, intent_hash),
    }
    .map_err(|error| MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error)))?;
    if matches!(&progress, SerialRemovalProgress::CandidateNonactivating) {
        finish_nonactivating_serial_removal(
            db,
            storage,
            cloud_home,
            &plan,
            intent_hash,
            pending_rotation,
        )
        .await?;
        return Err(
            crate::sync::store_outbound::StoreOutboundError::InvalidOutbound(
                "Serial removal candidate did not activate".to_string(),
            )
            .into(),
        );
    }
    if let SerialRemovalProgress::Activated { candidate } = &progress {
        if candidate != &plan.prepared.reference {
            return Err(MembershipOpsError::Database(
                "activated Serial removal names another candidate".to_string(),
            ));
        }
        let encryption =
            EncryptionService::from_keyring_payload(plan.keyring_payload).map_err(|error| {
                MembershipOpsError::Invite(InviteError::Crypto(format!(
                    "parse Serial rotated keyring: {error}"
                )))
            })?;
        return Ok(SerialRemovalResult::Activated(ActivatedSerialRemoval {
            encryption,
            intent_hash,
            generation: plan.generation,
        }));
    }
    let outcome = cloud_home
        .set_access(CloudAccessState::Absent {
            member_pubkey: plan.revokee_pubkey.clone(),
            provider_account_email: plan.revokee_email.clone(),
        })
        .await
        .map_err(InviteError::from)?;
    if !matches!(outcome, CloudAccessOutcome::Absent(_)) {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "provider returned present for a Serial removal".to_string(),
            ),
        ));
    }
    if matches!(&progress, SerialRemovalProgress::Pending) {
        progress = SerialRemovalProgress::AccessRevoked;
        db.update_membership_mutation_progress(
            intent_hash,
            serde_json::to_vec(&progress).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize revoked Serial removal access: {error}"
                )))
            })?,
        )
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    }
    publish_serial_membership_wraps(db, storage, &root_ref, &plan.prepared, &plan.wraps).await?;
    let mut plan = plan;
    let mut intent_hash = intent_hash;
    loop {
        let activated_progress = SerialRemovalProgress::Activated {
            candidate: plan.prepared.reference.clone(),
        };
        let remote_objects = plan
            .prepared
            .membership_control_remote_objects(&plan.wraps)?;
        match crate::sync::store_outbound::publish_prepared_serial_membership_operation(
            db,
            storage,
            coordination,
            Box::new(plan.prepared.clone()),
            crate::sync::store_outbound::StoreMembershipJournalCompletion::RotationMutation {
                intent_hash,
                progress_bytes: serde_json::to_vec(&activated_progress).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize activated Serial removal: {error}"
                    )))
                })?,
                generation: plan.generation,
                remote_objects,
            },
        )
        .await?
        {
            crate::sync::store_outbound::StoreOperationPublicationOutcome::Activated(_) => break,
            crate::sync::store_outbound::StoreOperationPublicationOutcome::RepreparedCandidate(
                candidate,
            ) => {
                plan.prepared = *candidate;
                let plan_bytes = serde_json::to_vec(&plan).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize reprepared Serial removal: {error}"
                    )))
                })?;
                let previous_intent_hash = intent_hash;
                let replacement_hash = db
                    .replace_serial_removal_candidate(
                        previous_intent_hash,
                        plan.generation,
                        plan_bytes,
                    )
                    .await
                    .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
                pending_rotation
                    .replace_candidate_mutation(
                        plan.generation,
                        previous_intent_hash,
                        replacement_hash,
                    )
                    .map_err(|error| {
                        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error))
                    })?;
                intent_hash = replacement_hash;
            }
            crate::sync::store_outbound::StoreOperationPublicationOutcome::NonactivatedCandidate {
                candidate,
                nonactivation,
            } => {
                if *candidate != plan.prepared {
                    return Err(MembershipOpsError::Database(
                        "nonactivation returned a different Serial removal candidate".to_string(),
                    ));
                }
                progress = SerialRemovalProgress::CandidateNonactivating;
                let progress_bytes = serde_json::to_vec(&progress).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize nonactivating Serial removal: {error}"
                    )))
                })?;
                db.begin_membership_candidate_nonactivation(
                    intent_hash,
                    plan.prepared.reference.clone(),
                    vec![plan.prepared.reference.object.clone()],
                    plan.wraps
                        .iter()
                        .map(|wrap| wrap.reference.object.clone())
                        .collect(),
                    progress_bytes,
                    *nonactivation,
                )
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
                finish_nonactivating_serial_removal(
                    db,
                    storage,
                    cloud_home,
                    &plan,
                    intent_hash,
                    pending_rotation,
                )
                .await?;
                return Err(crate::sync::store_outbound::StoreOutboundError::InvalidOutbound(
                    "Serial removal candidate did not activate".to_string(),
                )
                .into());
            }
            crate::sync::store_outbound::StoreOperationPublicationOutcome::Nonactivated(reference) => {
                return Err(
                    crate::sync::store_outbound::StoreOutboundError::InvalidOutbound(format!(
                        "Serial removal candidate {} did not activate",
                        reference.commit_hash
                    ))
                    .into(),
                );
            }
            crate::sync::store_outbound::StoreOperationPublicationOutcome::Reprepared => {
                return Err(MembershipOpsError::Database(
                    "Serial removal returned acknowledgement-only reprepare state".to_string(),
                ));
            }
        }
    }
    pending_rotation
        .mark_committed_mutation(plan.generation, intent_hash)
        .map_err(|error| MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error)))?;
    let encryption =
        EncryptionService::from_keyring_payload(plan.keyring_payload).map_err(|error| {
            MembershipOpsError::Invite(InviteError::Crypto(format!(
                "parse Serial rotated keyring: {error}"
            )))
        })?;
    Ok(SerialRemovalResult::Activated(ActivatedSerialRemoval {
        encryption,
        intent_hash,
        generation: plan.generation,
    }))
}

/// Remove a member from the shared store and adopt the rotated key locally.
///
/// Downloads the membership chain, creates a signed Remove entry, rotates the
/// encryption key on the cloud (re-wrapping it for every remaining member), then
/// swaps this device's live cipher and persists the rotated key to its keyring.
/// Returns the rotated key's fingerprint for the host to record in its own config.
///
/// The cloud rotation commits before the local adoption. When adoption fails, the
/// removal is already durable, so the failure surfaces as
/// [`MembershipOpsError::RotationCommittedAdoptionFailed`] — distinct from a
/// rotation that never committed. The durable operation remains resumable by the
/// same call, while its rotation gate prevents every cloud seal until adoption
/// and exact journal completion both succeed.
pub(crate) async fn remove_serial_member_and_adopt(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    current_encryption: &EncryptionService,
    custody: &dyn MasterKeyCustody,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    db: &Database,
) -> Result<String, MembershipOpsError> {
    let root_ref = required_store_root_ref(db).await?;
    let protocol_store_id = root_ref.store_root_id.to_string();
    let removal = Box::pin(remove_serial_member(
        storage,
        cloud_home,
        coordination,
        device_id,
        user_keypair,
        hlc,
        public_key_hex,
        &protocol_store_id,
        current_encryption,
        crate::encryption::generate_random_key(),
        pending_rotation,
        db,
    ))
    .await?;
    let removal = match removal {
        SerialRemovalResult::Activated(removal) => removal,
        SerialRemovalResult::Terminal(fingerprint) => return Ok(fingerprint),
    };
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::SerialRemovalBeforeAdoption)
        .await;
    let fingerprint = apply_key_rotation(removal.encryption, custody, cipher)
        .map_err(|source| MembershipOpsError::RotationCommittedAdoptionFailed { source })?;
    let result_bytes = serde_json::to_vec(&fingerprint).map_err(|error| {
        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
            "serialize terminal Serial removal result: {error}"
        )))
    })?;
    let gate = db
        .complete_serial_removal_mutation(removal.intent_hash, removal.generation, result_bytes)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    pending_rotation
        .install_durable_gate(gate)
        .map_err(|error| MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error)))?;
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::SerialMembershipTerminalized)
        .await;
    Ok(fingerprint)
}
