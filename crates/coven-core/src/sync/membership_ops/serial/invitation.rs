use super::super::*;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SerialInvitePlan {
    prepared: crate::sync::store_outbound::PreparedStoreOperationCommit,
    invitee_pubkey: String,
    invitee_email: Option<String>,
    role: MemberRole,
    desired_access: CloudAccessState,
    invitee_was_member: bool,
    wrapped_key: crate::sync::wrapped_store_key::PreparedWrappedStoreKey,
    store_id: String,
    store_name: String,
    store_root: StoreRootRef,
    owner_pubkey: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum SerialInviteProgress {
    Pending,
    AccessGranted {
        join_info: crate::storage::cloud::CloudHomeJoinInfo,
    },
    CandidateNonactivating {
        join_info: crate::storage::cloud::CloudHomeJoinInfo,
    },
    Activated {
        join_info: crate::storage::cloud::CloudHomeJoinInfo,
        candidate: StoreBatchCommitRef,
    },
}

fn serial_invite_result(
    plan: &SerialInvitePlan,
    join_info: crate::storage::cloud::CloudHomeJoinInfo,
) -> Result<crate::join_code::InviteCode, MembershipOpsError> {
    if plan.store_root.store_root_hash != plan.prepared.commit.store_root_hash {
        return Err(MembershipOpsError::Database(
            "Serial invite commit names a different Store root".to_string(),
        ));
    }
    Ok(crate::join_code::InviteCode {
        v: crate::join_code::INVITE_CODE_VERSION,
        store_id: plan.store_id.clone(),
        store_name: plan.store_name.clone(),
        join_info,
        owner_pubkey: plan.owner_pubkey.clone(),
        wrapped_key: plan.wrapped_key.reference.clone(),
        store_root: plan.store_root.clone(),
        membership_floor: crate::join_code::MembershipFloor::Serial(Some(
            plan.prepared.reference.clone(),
        )),
    })
}

fn serial_invite_receipt_is_current(
    plan: &SerialInvitePlan,
    authorization: &crate::sync::membership::SerialAuthorizationState,
) -> Result<bool, MembershipOpsError> {
    plan.prepared.validate_closed_shape().map_err(|error| {
        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
            "terminal Serial invitation candidate is invalid: {error}"
        )))
    })?;
    let Some(StoreControl::SerialMembership { entry }) = plan.prepared.commit.control() else {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial invitation does not carry a Serial membership grant".to_string(),
            ),
        ));
    };
    let crate::sync::membership::SerialMembershipChange::SetMember {
        user_pubkey,
        provider_account_email,
        role,
        grant_id,
        wrapped_key,
        ..
    } = &entry.change
    else {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial invitation carries a removal".to_string(),
            ),
        ));
    };
    let role_matches = matches!(
        (&plan.role, role),
        (
            MemberRole::Member,
            crate::sync::membership::StoreMembershipRoleGrant::Member
        ) | (
            MemberRole::Follower,
            crate::sync::membership::StoreMembershipRoleGrant::Follower
        )
    );
    if user_pubkey != &plan.invitee_pubkey
        || provider_account_email != &plan.invitee_email
        || !role_matches
        || wrapped_key != &plan.wrapped_key.reference
    {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial invitation plan differs from its membership grant".to_string(),
            ),
        ));
    }
    let expected_grants = BTreeSet::from([grant_id.clone()]);
    let expected_wraps = vec![wrapped_key.clone()];
    Ok(authorization.key_generation == wrapped_key.generation
        && authorization
            .membership
            .active_grant_ids(&plan.invitee_pubkey)
            == expected_grants
        && authorization.active_wrapped_keys_for(&plan.invitee_pubkey) == expected_wraps
        && authorization
            .membership
            .current_members()
            .iter()
            .any(|(pubkey, role)| pubkey == &plan.invitee_pubkey && role == &plan.role)
        && authorization
            .membership
            .current_member_provider_email(&plan.invitee_pubkey)
            == plan.invitee_email.as_deref())
}

async fn restore_serial_invite_provider_access(
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    plan: &SerialInvitePlan,
) -> Result<(), MembershipOpsError> {
    if !plan.invitee_was_member {
        let outcome = cloud_home
            .set_access(CloudAccessState::Absent {
                member_pubkey: plan.invitee_pubkey.clone(),
                provider_account_email: plan.invitee_email.clone(),
            })
            .await
            .map_err(InviteError::from)?;
        if !matches!(outcome, CloudAccessOutcome::Absent(_)) {
            return Err(MembershipOpsError::Invite(
                InviteError::InvalidDurableMutation(
                    "provider returned present while rolling back a Serial invitation".to_string(),
                ),
            ));
        }
    }
    Ok(())
}

async fn finish_nonactivating_serial_invite(
    db: &Database,
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    plan: &SerialInvitePlan,
    intent_hash: crate::sync::store_commit::ObjectHash,
) -> Result<(), MembershipOpsError> {
    restore_serial_invite_provider_access(cloud_home, plan).await?;
    crate::sync::store_objects::delete_exact_object(storage, &plan.prepared.reference.object)
        .await?;
    db.mark_candidate_cleanup_absent(plan.prepared.reference.object.clone())
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    db.complete_nonactivating_membership_candidate_mutation(
        intent_hash,
        plan.prepared.reference.clone(),
        vec![plan.prepared.reference.object.clone()],
        vec![plan.wrapped_key.reference.object.clone()],
        None,
    )
    .await
    .map_err(|error| MembershipOpsError::Database(error.to_string()))
}

pub(crate) async fn publish_serial_membership_wraps(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    candidate: &crate::sync::store_outbound::PreparedStoreOperationCommit,
    wraps: &[crate::sync::wrapped_store_key::PreparedWrappedStoreKey],
) -> Result<(), MembershipOpsError> {
    let remote_objects = candidate.membership_control_remote_objects(wraps)?;
    for prepared in wraps {
        prepared.validate()?;
        storage.create_protocol_object(&prepared.object).await?;
        crate::sync::wrapped_store_key::load_wrapped_store_key(
            storage,
            root.store_root_hash,
            &prepared.reference,
        )
        .await?;
        let expected = remote_objects
            .iter()
            .find(|remote| remote.object() == &prepared.reference.object)
            .cloned()
            .ok_or_else(|| {
                MembershipOpsError::Database(
                    "Serial membership wrap is absent from its durable ownership graph".to_string(),
                )
            })?;
        db.mark_reusable_retained_authority_uploaded(expected)
            .await
            .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    }
    crate::sync::store_pull::validate_serial_control_wrapped_keys(
        storage,
        root,
        candidate.commit.control(),
    )
    .await
    .map_err(|error| MembershipOpsError::Database(error.to_string()))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn invite_serial_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    store_id: &str,
    store_name: &str,
    db: &Database,
) -> Result<crate::join_code::InviteCode, MembershipOpsError> {
    validate_invitation(user_keypair, public_key_hex, &role)?;
    let root_ref = required_store_root_ref(db).await?;
    let protocol_store_id = root_ref.store_root_id.to_string();
    let _mutation = db.lock_membership_mutation().await;
    if let Some(receipt) = db
        .terminal_serial_invite_mutation()
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
    {
        let plan: SerialInvitePlan =
            serde_json::from_slice(&receipt.plan_bytes).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "parse terminal Serial invitation plan: {error}"
                )))
            })?;
        if plan.invitee_pubkey == public_key_hex
            && plan.invitee_email.as_deref() == invitee_email
            && plan.role == role
            && plan.store_id == store_id
            && plan.store_name == store_name
        {
            let authorization =
                crate::sync::store_engine::serial::publication::current_serial_authorization(
                    db,
                    storage,
                    coordination,
                )
                .await?;
            if serial_invite_receipt_is_current(&plan, &authorization)? {
                return serde_json::from_slice(&receipt.result_bytes).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "parse terminal Serial invitation result: {error}"
                    )))
                });
            }
        }
    }
    let (plan, mut progress, intent_hash) = match db
        .outbound_membership_mutation()
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
    {
        Some(row) => {
            let plan: SerialInvitePlan =
                serde_json::from_slice(&row.plan_bytes).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "parse Serial invitation plan: {error}"
                    )))
                })?;
            let progress = serde_json::from_slice(&row.progress_bytes).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "parse Serial invitation progress: {error}"
                )))
            })?;
            if plan.invitee_pubkey != public_key_hex
                || plan.invitee_email.as_deref() != invitee_email
                || plan.role != role
            {
                return Err(MembershipOpsError::Invite(InviteError::PendingMutation(
                    "the pending Serial invitation has different immutable inputs".to_string(),
                )));
            }
            (plan, progress, row.intent_hash)
        }
        None => {
            let root = crate::sync::store_objects::load_store_protocol_root(storage, &root_ref)
                .await?
                .value;
            let authorization =
                crate::sync::store_engine::serial::publication::current_serial_authorization(
                    db,
                    storage,
                    coordination,
                )
                .await?;
            let invitee_was_member = authorization
                .membership
                .current_members()
                .iter()
                .any(|(pubkey, _)| pubkey == public_key_hex);
            let authority_refs =
                authorization.active_wrapped_keys_for(&crate::keys::public_key_hex(user_keypair));
            let authorized_keyring = crate::sync::invite::load_authorized_owner_keyring(
                storage,
                root_ref.store_root_hash,
                user_keypair,
                &protocol_store_id,
                &authority_refs,
                encryption,
            )
            .await?;
            if authorized_keyring.current_generation() != authorization.key_generation {
                return Err(MembershipOpsError::Invite(
                    InviteError::InvalidDurableMutation(format!(
                        "authorized key generation {} differs from committed Serial generation {}",
                        authorized_keyring.current_generation(),
                        authorization.key_generation
                    )),
                ));
            }
            let wrapped_key = crate::sync::wrapped_store_key::prepare_wrapped_store_key(
                storage,
                root_ref.store_root_hash,
                public_key_hex,
                crate::sync::invite::signed_serial_wrapped_key(
                    &protocol_store_id,
                    public_key_hex,
                    &authorized_keyring,
                    user_keypair,
                )?,
            )
            .await?;
            let entry = authorization
                .membership
                .signed_set_member_with_wrapped_key(
                    user_keypair,
                    public_key_hex.to_string(),
                    invitee_email.map(str::to_string),
                    role.clone(),
                    wrapped_key.reference.clone(),
                    hlc.now().to_string(),
                )
                .map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(
                        error.to_string(),
                    ))
                })?;
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
                    StoreControl::SerialMembership { entry },
                ),
            )
            .await?;
            let plan = SerialInvitePlan {
                prepared,
                invitee_pubkey: public_key_hex.to_string(),
                invitee_email: invitee_email.map(str::to_string),
                role,
                desired_access: CloudAccessState::Present {
                    member_pubkey: public_key_hex.to_string(),
                    provider_account_email: invitee_email.map(str::to_string),
                },
                invitee_was_member,
                wrapped_key,
                store_id: store_id.to_string(),
                store_name: store_name.to_string(),
                store_root: root_ref.clone(),
                owner_pubkey: root.descriptor.founder_pubkey,
            };
            let plan_bytes = serde_json::to_vec(&plan).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize Serial invitation plan: {error}"
                )))
            })?;
            let progress = SerialInviteProgress::Pending;
            let progress_bytes = serde_json::to_vec(&progress).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize Serial invitation progress: {error}"
                )))
            })?;
            let remote_objects = plan
                .prepared
                .membership_control_remote_objects(std::slice::from_ref(&plan.wrapped_key))?;
            let intent_hash = db
                .stage_serial_invite_candidate_mutation(plan_bytes, progress_bytes, remote_objects)
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
            (plan, progress, intent_hash)
        }
    };
    if matches!(
        &progress,
        SerialInviteProgress::CandidateNonactivating { .. }
    ) {
        finish_nonactivating_serial_invite(db, storage, cloud_home, &plan, intent_hash).await?;
        return Err(
            crate::sync::store_outbound::StoreOutboundError::InvalidOutbound(
                "Serial invitation candidate did not activate".to_string(),
            )
            .into(),
        );
    }
    let outcome = cloud_home
        .set_access(plan.desired_access.clone())
        .await
        .map_err(InviteError::from)?;
    let CloudAccessOutcome::Present(observed_join_info) = outcome else {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "provider returned absent for a Serial invitation".to_string(),
            ),
        ));
    };
    let join_info =
        match &progress {
            SerialInviteProgress::Pending => {
                progress = SerialInviteProgress::AccessGranted {
                    join_info: observed_join_info.clone(),
                };
                let progress_bytes = serde_json::to_vec(&progress).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize Serial invitation progress: {error}"
                    )))
                })?;
                db.update_membership_mutation_progress(intent_hash, progress_bytes)
                    .await
                    .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
                observed_join_info
            }
            SerialInviteProgress::AccessGranted { join_info } => {
                if *join_info != observed_join_info {
                    return Err(MembershipOpsError::Invite(InviteError::InvalidDurableMutation(
                    "provider returned different join information for persisted Serial access"
                        .to_string(),
                )));
                }
                join_info.clone()
            }
            SerialInviteProgress::CandidateNonactivating { .. } => {
                unreachable!("nonactivating invitation returned before access grant")
            }
            SerialInviteProgress::Activated { join_info, .. } => join_info.clone(),
        };
    if let SerialInviteProgress::Activated {
        join_info,
        candidate,
    } = &progress
    {
        if candidate != &plan.prepared.reference {
            return Err(MembershipOpsError::Database(
                "activated Serial invitation names another candidate".to_string(),
            ));
        }
        let result = serial_invite_result(&plan, join_info.clone())?;
        let result_bytes = serde_json::to_vec(&result).map_err(|error| {
            MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                "serialize terminal Serial invitation result: {error}"
            )))
        })?;
        db.complete_serial_invite_mutation(intent_hash, result_bytes)
            .await
            .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
        #[cfg(any(test, feature = "test-utils"))]
        db.reach_test_point(crate::database::DatabaseTestPoint::SerialMembershipTerminalized)
            .await;
        return Ok(result);
    }
    publish_serial_membership_wraps(
        db,
        storage,
        &root_ref,
        &plan.prepared,
        std::slice::from_ref(&plan.wrapped_key),
    )
    .await?;
    let mut plan = plan;
    let mut intent_hash = intent_hash;
    loop {
        let activated_progress = SerialInviteProgress::Activated {
            join_info: join_info.clone(),
            candidate: plan.prepared.reference.clone(),
        };
        let remote_objects = plan
            .prepared
            .membership_control_remote_objects(std::slice::from_ref(&plan.wrapped_key))?;
        match crate::sync::store_outbound::publish_prepared_serial_membership_operation(
            db,
            storage,
            coordination,
            Box::new(plan.prepared.clone()),
            crate::sync::store_outbound::StoreMembershipJournalCompletion::Mutation {
                intent_hash,
                progress_bytes: serde_json::to_vec(&activated_progress).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize activated Serial invitation: {error}"
                    )))
                })?,
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
                        "serialize reprepared Serial invitation: {error}"
                    )))
                })?;
                intent_hash = db
                    .replace_membership_candidate(intent_hash, plan_bytes)
                    .await
                    .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
            }
            crate::sync::store_outbound::StoreOperationPublicationOutcome::NonactivatedCandidate {
                candidate,
                nonactivation,
            } => {
                if *candidate != plan.prepared {
                    return Err(MembershipOpsError::Database(
                        "nonactivation returned a different Serial invitation candidate"
                            .to_string(),
                    ));
                }
                progress = SerialInviteProgress::CandidateNonactivating {
                    join_info: join_info.clone(),
                };
                let progress_bytes = serde_json::to_vec(&progress).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize nonactivating Serial invitation: {error}"
                    )))
                })?;
                db.begin_membership_candidate_nonactivation(
                    intent_hash,
                    plan.prepared.reference.clone(),
                    vec![plan.prepared.reference.object.clone()],
                    vec![plan.wrapped_key.reference.object.clone()],
                    progress_bytes,
                    *nonactivation,
                )
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
                finish_nonactivating_serial_invite(db, storage, cloud_home, &plan, intent_hash)
                    .await?;
                return Err(crate::sync::store_outbound::StoreOutboundError::InvalidOutbound(
                    "Serial invitation candidate did not activate".to_string(),
                )
                .into());
            }
            crate::sync::store_outbound::StoreOperationPublicationOutcome::Nonactivated(reference) => {
                return Err(
                    crate::sync::store_outbound::StoreOutboundError::InvalidOutbound(format!(
                        "Serial invitation candidate {} did not activate",
                        reference.commit_hash
                    ))
                    .into(),
                );
            }
            crate::sync::store_outbound::StoreOperationPublicationOutcome::Reprepared => {
                return Err(MembershipOpsError::Database(
                    "Serial invitation returned acknowledgement-only reprepare state".to_string(),
                ));
            }
        }
    }
    let result = serial_invite_result(&plan, join_info)?;
    let result_bytes = serde_json::to_vec(&result).map_err(|error| {
        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
            "serialize terminal Serial invitation result: {error}"
        )))
    })?;
    db.complete_serial_invite_mutation(intent_hash, result_bytes)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::SerialMembershipTerminalized)
        .await;
    Ok(result)
}
