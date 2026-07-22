use super::*;

pub(crate) struct VerifiedStoreHistoryAuthority;

pub(crate) fn verify_store_history_state<'a>(
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    root: &'a StoreRootRef,
    cut: &'a StoreHistoryCut,
    membership_ref: &'a StoreMembershipStateRef,
) -> StorePullFuture<'a, VerifiedStoreHistoryAuthority> {
    Box::pin(verify_store_history_state_impl(
        storage,
        serial_coordination,
        root,
        cut,
        membership_ref,
    ))
}

async fn verify_store_history_state_impl(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    cut: &StoreHistoryCut,
    membership_ref: &StoreMembershipStateRef,
) -> Result<VerifiedStoreHistoryAuthority, StorePullError> {
    match (cut, membership_ref) {
        (
            StoreHistoryCut::MergeConcurrent(frontier),
            StoreMembershipStateRef::MergeConcurrent(_),
        ) => {
            if serial_coordination.is_some() {
                return Err(StorePullError::Database(
                    "Merge history verification received Serial coordination".to_string(),
                ));
            }
            let history = Box::pin(verify_merge_history_refs(
                storage,
                root,
                frontier.values().cloned().collect::<Vec<_>>(),
            ))
            .await?;
            let device_state = if frontier.is_empty() {
                history.genesis.clone()
            } else {
                ResolvedStoreDeviceState::merge(
                    frontier
                        .values()
                        .map(|reference| {
                            history
                                .commits
                                .get(reference)
                                .map(|commit| commit.state_after.clone())
                                .ok_or_else(|| {
                                    StorePullError::Database(
                                        "Merge history frontier is absent from its verified graph"
                                            .to_string(),
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?
            };
            let verified_membership_activations =
                verified_merge_membership_prefix(&history.commits, frontier.values().cloned())?;
            let membership = Box::pin(load_merge_predecessor_membership_with_verified_activations(
                storage,
                root,
                membership_ref,
                &verified_membership_activations,
                None,
            ))
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
            verified_membership_activations
                .validate_complete_membership(&membership)
                .map_err(StorePullError::Database)?;
            verify_merge_membership_state_ref(membership_ref, &membership, &device_state)?;
            Ok(VerifiedStoreHistoryAuthority)
        }
        (StoreHistoryCut::Serial(position), StoreMembershipStateRef::Serial(_)) => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Serial history verification requires coordination capability".to_string(),
                )
            })?;
            let verified_head = read_serial_head(storage, coordination, root).await?;
            let accepted = load_authorized_serial_chain(storage, root, &verified_head.head).await?;
            let (_, genesis_authorization, genesis_state) =
                Box::pin(load_authorized_serial_prefix(storage, root, None)).await?;
            let founder = load_founder_registration(storage, root).await?;
            let founder_ref = StoreDeviceRegistrationRef::from_registration(
                &founder.value,
                founder.object.clone(),
            );
            let expected_genesis = super::store_commit::StoreSerialPredecessor::Genesis {
                root: root.clone(),
                founder_registration: founder_ref,
            };
            let accepted_prefix = match position {
                super::store_commit::StoreSerialPredecessor::Genesis { .. }
                    if position == &expected_genesis =>
                {
                    &accepted[..0]
                }
                super::store_commit::StoreSerialPredecessor::Genesis { .. } => {
                    return Err(StorePullError::Serial(
                        "Serial history cut names another genesis authority".to_string(),
                    ));
                }
                super::store_commit::StoreSerialPredecessor::Commit(reference) => {
                    let index = accepted
                        .iter()
                        .position(|candidate| &candidate.commit_ref == reference)
                        .ok_or_else(|| {
                            StorePullError::Serial(
                                "Serial history cut is absent from the signed coordinated chain"
                                    .to_string(),
                            )
                        })?;
                    &accepted[..=index]
                }
            };
            let (authorization, device_state) = accepted_prefix.last().map_or_else(
                || (genesis_authorization, genesis_state),
                |accepted| {
                    (
                        accepted.authorization_after.clone(),
                        accepted.device_state_after.clone(),
                    )
                },
            );
            let expected_membership = StoreMembershipStateRef::serial(
                position.clone(),
                device_state.recovery.clone(),
                &authorization,
            )
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
            if &expected_membership != membership_ref {
                return Err(StorePullError::Serial(
                    "Serial history membership reference differs from its accepted state"
                        .to_string(),
                ));
            }
            Ok(VerifiedStoreHistoryAuthority)
        }
        _ => Err(StorePullError::Database(
            "Store history cut and membership state use different policies".to_string(),
        )),
    }
}

async fn verify_merge_owner_promotion_acceptance(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<(), StorePullError> {
    let super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent {
        commit: activation_commit,
        ..
    } = &acceptance.activation
    else {
        return Err(StorePullError::Database(
            "Merge Owner promotion carries Serial activation".to_string(),
        ));
    };
    let history = verify_merge_history_refs(storage, root, [activation_commit.clone()]).await?;
    verify_merge_owner_promotion_acceptance_with_history(
        storage,
        root,
        acceptance,
        &history.commits,
    )
    .await
}

pub(crate) async fn verify_merge_owner_promotion_acceptance_with_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
    verified_commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
) -> Result<(), StorePullError> {
    let request = &acceptance.request;
    let super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent {
        commit: activation_commit,
        head: activation_head,
    } = &acceptance.activation
    else {
        return Err(StorePullError::Database(
            "Merge Owner promotion carries Serial activation".to_string(),
        ));
    };
    let promoter = load_registration_ref(storage, root, &request.promoter_registration).await?;
    let candidate = load_registration_ref(storage, root, &request.member_registration).await?;
    request
        .verify(root, &promoter.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    acceptance
        .verify(&candidate.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;

    let head_prefix =
        super::store_commit::semantic_prefix_from_exact_object(&activation_head.object, ".json")
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_bytes = storage
        .read_protocol_object(&context, &activation_head.object, &head_prefix)
        .await?;
    activation_head.object.verify(&head_bytes)?;
    let head: StoreDeviceHead = serde_json::from_slice(&head_bytes).map_err(|error| {
        StorePullError::Database(format!("Owner-promotion activation head: {error}"))
    })?;
    let opened = super::store_objects::load_head_ref(
        storage,
        root.store_root_hash,
        activation_head,
        &promoter.value,
        activation_commit,
    )
    .await?;
    let (_, exact_head) = super::store_outbound::exact_next_announcement_slot(
        storage,
        root,
        &request.promoter_registration,
        &promoter.value,
        Some(activation_commit),
    )
    .await
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if opened.value != head
        || head.head_hash() != activation_head.head_hash
        || head.commit != *activation_commit
        || exact_head.as_ref() != Some(activation_head)
    {
        return Err(StorePullError::Database(
            "Owner-promotion request is not activated by its exact Merge head".to_string(),
        ));
    }
    let verified = verified_commits.get(activation_commit).ok_or_else(|| {
        StorePullError::Database(
            "Owner-promotion request activation is absent from its verified history".to_string(),
        )
    })?;
    if verified.commit.owner_promotion_request() != Some(request)
        || verified.commit.membership_state != request.predecessor_membership
        || verified.commit.device_state != request.predecessor_devices
        || verified.commit.author_registration != request.promoter_registration
    {
        return Err(StorePullError::Database(
            "Owner-promotion request commit differs from its signed predecessor authority"
                .to_string(),
        ));
    }
    let verified_membership_activations = verified_merge_membership_prefix(
        verified_commits,
        commit_predecessor_references(&verified.commit),
    )?;
    let membership = load_merge_predecessor_membership_with_verified_activations(
        storage,
        root,
        &request.predecessor_membership,
        &verified_membership_activations,
        None,
    )
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })?;
    verify_merge_membership_state_ref(
        &request.predecessor_membership,
        &membership,
        &verified.predecessor_state,
    )?;
    if !device_state_has_active_registration(
        &verified.predecessor_state,
        &request.promoter_registration,
    ) || !device_state_has_active_registration(
        &verified.predecessor_state,
        &request.member_registration,
    ) {
        return Err(StorePullError::Database(
            "Owner-promotion request registrations are not active at its exact predecessor"
                .to_string(),
        ));
    }
    if membership
        .active_owner_grant(&promoter.value.author_pubkey)
        .as_ref()
        != Some(&request.promoter_owner_grant)
        || membership.active_grant_ids(&request.member_pubkey)
            != BTreeSet::from([request.member_grant.clone()])
        || membership
            .active_grant(&request.member_grant)
            .is_none_or(|record| {
                record.member_pubkey != request.member_pubkey
                    || record.role != super::membership::StoreMembershipRoleGrant::Member
            })
        || candidate.value.author_pubkey != request.member_pubkey
    {
        return Err(StorePullError::Database(
            "Owner-promotion request does not name the exact active Owner and Member grants"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) enum VerifiedOwnerPromotionAcceptance {
    MergeConcurrent,
    Serial(crate::sync::store_engine::serial::publication::SerialAuthorizationSnapshot),
}

pub(crate) async fn verify_owner_promotion_acceptance(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<VerifiedOwnerPromotionAcceptance, StorePullError> {
    match &acceptance.activation {
        super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent { .. } => {
            verify_merge_owner_promotion_acceptance(storage, root, acceptance).await?;
            Ok(VerifiedOwnerPromotionAcceptance::MergeConcurrent)
        }
        super::store_commit::OwnerPromotionRequestActivation::Serial { .. } => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Serial Owner-promotion verification requires coordination".to_string(),
                )
            })?;
            let request = &acceptance.request;
            let promoter =
                load_registration_ref(storage, root, &request.promoter_registration).await?;
            let candidate =
                load_registration_ref(storage, root, &request.member_registration).await?;
            request
                .verify(root, &promoter.value)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
            acceptance
                .verify(&candidate.value)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
            let verified_head = read_serial_head(storage, coordination, root).await?;
            let accepted = load_authorized_serial_chain(storage, root, &verified_head.head).await?;
            let mut matches = accepted
                .iter()
                .filter(|candidate| candidate.commit.owner_promotion_request() == Some(request));
            let Some(activated) = matches.next() else {
                return Err(StorePullError::Serial(
                    "Owner-promotion request has no accepted Serial activation".to_string(),
                ));
            };
            if matches.next().is_some() {
                return Err(StorePullError::Serial(
                    "Owner-promotion request has more than one Serial activation".to_string(),
                ));
            }
            let discovered = super::store_commit::OwnerPromotionRequestActivation::Serial {
                commit: activated.commit_ref.clone(),
            };
            if discovered != acceptance.activation {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion acceptance names another activation".to_string(),
                ));
            }
            let commit = &activated.commit;
            if commit.owner_promotion_request() != Some(request)
                || commit.membership_state != request.predecessor_membership
                || commit.device_state != request.predecessor_devices
                || commit.author_registration != request.promoter_registration
            {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion request commit differs from its signed authority"
                        .to_string(),
                ));
            }
            if !device_state_has_active_registration(
                &activated.device_state_before,
                &request.promoter_registration,
            ) || !device_state_has_active_registration(
                &activated.device_state_before,
                &request.member_registration,
            ) {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion registrations are not active at its predecessor"
                        .to_string(),
                ));
            }
            if activated
                .authorization_before
                .membership
                .active_owner_grant(&promoter.value.author_pubkey)
                .as_ref()
                != Some(&request.promoter_owner_grant)
                || activated
                    .authorization_before
                    .membership
                    .active_grant_ids(&request.member_pubkey)
                    != BTreeSet::from([request.member_grant.clone()])
                || !activated
                    .authorization_before
                    .membership
                    .is_member_grant(&request.member_pubkey, &request.member_grant)
                || candidate.value.author_pubkey != request.member_pubkey
            {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion request does not name the active Owner and Member"
                        .to_string(),
                ));
            }
            let authorization = accepted
                .last()
                .ok_or_else(|| {
                    StorePullError::Serial(
                        "Serial Owner-promotion activation has no accepted commit".to_string(),
                    )
                })?
                .authorization_after
                .clone();
            let base = match &verified_head.head.state {
                StoreSerialHeadState::Genesis { .. } => None,
                StoreSerialHeadState::Commit { commit, .. } => Some(commit.clone()),
            };
            Ok(VerifiedOwnerPromotionAcceptance::Serial(
                crate::sync::store_engine::serial::publication::SerialAuthorizationSnapshot {
                    base,
                    base_head: verified_head.object,
                    authorization,
                },
            ))
        }
    }
}
