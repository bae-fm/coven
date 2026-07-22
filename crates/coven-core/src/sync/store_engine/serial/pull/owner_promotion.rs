use super::*;

pub(in crate::sync::store_engine) async fn find_request_activation(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    request: &super::store_commit::OwnerPromotionRequest,
) -> Result<super::store_commit::OwnerPromotionRequestActivation, StorePullError> {
    let promoter = load_registration_ref(storage, root, &request.promoter_registration).await?;
    request
        .verify(root, &promoter.value)
        .map_err(|error| StorePullError::Serial(error.to_string()))?;
    if !matches!(
        request.finalization,
        super::store_commit::OwnerPromotionFinalization::Serial
    ) {
        return Err(StorePullError::Serial(
            "Serial Owner-promotion discovery received Merge finalization".to_string(),
        ));
    }
    let head = read_serial_head(storage, coordination, root).await?;
    let accepted = load_authorized_serial_chain(storage, root, &head.head).await?;
    let mut matches = accepted
        .into_iter()
        .filter(|candidate| candidate.commit.owner_promotion_request() == Some(request));
    let Some(accepted) = matches.next() else {
        return Err(StorePullError::Serial(
            "Owner-promotion request has no accepted Serial activation".to_string(),
        ));
    };
    if matches.next().is_some() {
        return Err(StorePullError::Serial(
            "Owner-promotion request has more than one Serial activation".to_string(),
        ));
    }
    Ok(
        super::store_commit::OwnerPromotionRequestActivation::Serial {
            commit: accepted.commit_ref,
        },
    )
}

pub(in crate::sync::store_engine) async fn verify_acceptance(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<
    crate::sync::store_engine::serial::publication::SerialAuthorizationSnapshot,
    StorePullError,
> {
    let request = &acceptance.request;
    if !matches!(
        acceptance.activation,
        super::store_commit::OwnerPromotionRequestActivation::Serial { .. }
    ) {
        return Err(StorePullError::Serial(
            "Serial Owner promotion carries Merge activation".to_string(),
        ));
    }
    let promoter = load_registration_ref(storage, root, &request.promoter_registration).await?;
    let candidate = load_registration_ref(storage, root, &request.member_registration).await?;
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
            "Serial Owner-promotion request commit differs from its signed authority".to_string(),
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
            "Serial Owner-promotion registrations are not active at its predecessor".to_string(),
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
            "Serial Owner-promotion request does not name the active Owner and Member".to_string(),
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
    Ok(
        crate::sync::store_engine::serial::publication::SerialAuthorizationSnapshot {
            base,
            base_head: verified_head.object,
            authorization,
        },
    )
}
