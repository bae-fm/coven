use super::join_validation::*;
use super::*;

pub(crate) enum RegistrationLoadError {
    Object(StoreObjectError),
    Invalid(String),
}

pub(crate) struct VerifiedCommitJoinOutcome {
    pub(crate) attempt: DeviceJoinAttempt,
    pub(crate) owner: StoreDeviceRegistration,
    pub(crate) outcome: super::store_commit::DeviceJoinOutcome,
}

pub(crate) fn registration_attempt_error(error: StorePullError) -> RegistrationLoadError {
    match error {
        StorePullError::Object(error) => RegistrationLoadError::Object(error),
        StorePullError::Storage(error) => {
            RegistrationLoadError::Object(StoreObjectError::Storage(error))
        }
        error => RegistrationLoadError::Invalid(error.to_string()),
    }
}

pub(crate) struct LoadedDeviceJoinCleanupActivation {
    pub(crate) verified_commit: VerifiedStoreBatchCommit,
    pub(crate) receipts: Vec<LoadedCommitJoinCleanupReceipt>,
}

/// Bind a row blob to the package that published it. The blob is never named in a
/// commit body — only inside the package's bindings — so what a commit establishes
/// is that the named package was activated by a commit in this device's
/// predecessor history. The blob's own reference is self-binding: its object key is
/// derived from its locator, which names the audience and uploading device, and
/// the audience must be the one the package addresses. Reading the bindings
/// themselves requires the package's audience key, which a Store member outside a
/// Circle does not hold; the Owner re-reads them before authorizing any delete.
fn validate_package_bound_reclaim_target(
    target: &super::store_reclaim::ReclaimTarget,
    activation: &super::store_reclaim::PackageBlobBindingActivation<'_>,
    accepted: VerifiedMergePredecessorHistory<'_>,
) -> Result<(), RegistrationLoadError> {
    let super::store_reclaim::ReclaimTarget::AudienceBlob(blob) = target else {
        return Err(RegistrationLoadError::Invalid(
            "reclaim target is not published by a package binding".to_string(),
        ));
    };
    let expected = activation.activation.clone();
    let activating = accepted
        .find(|candidate, _| candidate == &expected)
        .map_err(registration_attempt_error)?
        .ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "reclaim evidence blob activation is absent from predecessor history".to_string(),
            )
        })?;
    let names_package = match activation.package {
        super::store_reclaim::AudienceBlobBindingPackage::Store(package) => {
            activating.verified.value().store_package() == Some(package)
        }
        super::store_reclaim::AudienceBlobBindingPackage::Circle(package) => activating
            .verified
            .value()
            .circle_packages()
            .contains(package),
    };
    if !names_package {
        return Err(RegistrationLoadError::Invalid(
            "reclaim evidence blob package differs from its exact activation".to_string(),
        ));
    }
    if blob.blob.locator().audience() != activation.package.remote_audience() {
        return Err(RegistrationLoadError::Invalid(
            "reclaim evidence blob names a package for another audience".to_string(),
        ));
    }
    Ok(())
}

/// Bind a reclaim target to the retained Store commit that published it: the
/// commit must sit in this device's predecessor history and its body must name the
/// exact object the evidence authorizes deleting.
fn validate_commit_activated_reclaim_target(
    target: &super::store_reclaim::ReclaimTarget,
    activating_commit: &StoreBatchCommitRef,
    accepted: VerifiedMergePredecessorHistory<'_>,
) -> Result<(), RegistrationLoadError> {
    let expected = activating_commit.clone();
    let activation = accepted
        .find(|candidate, _| candidate == &expected)
        .map_err(registration_attempt_error)?
        .ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "reclaim evidence package activation is absent from predecessor history"
                    .to_string(),
            )
        })?;
    let names_target = match target {
        super::store_reclaim::ReclaimTarget::StorePackage(store) => {
            activation.verified.value().store_package() == Some(&store.package)
        }
        super::store_reclaim::ReclaimTarget::CirclePackage(circle) => activation
            .verified
            .value()
            .circle_packages()
            .contains(&circle.package),
        super::store_reclaim::ReclaimTarget::CircleBootstrapImage(bootstrap) => activation
            .verified
            .value()
            .circle_controls()
            .iter()
            .flat_map(|control| control.objects.access.iter())
            .any(|access| access.bootstrap.as_ref() == Some(&bootstrap.coverage.bootstrap.image)),
        super::store_reclaim::ReclaimTarget::CircleSnapshotImage(_)
        | super::store_reclaim::ReclaimTarget::AudienceBlob(_) => {
            return Err(RegistrationLoadError::Invalid(
                "reclaim target claims a Store commit activation it is not published by"
                    .to_string(),
            ));
        }
    };
    if !names_target {
        return Err(RegistrationLoadError::Invalid(
            "reclaim evidence target differs from its exact package activation".to_string(),
        ));
    }
    Ok(())
}

/// Bind a reclaim target to the Circle snapshot generation that published it.
/// The generation's metadata rides no Store commit and is sealed under the Circle
/// epoch key, so a Store member outside the Circle cannot read it. What every
/// member can check is the addressing the stream derives from public identity:
/// both the metadata slot and the image key are computed from the Circle, the
/// author's device, and the generation, so evidence naming another Circle, another
/// device's stream, or another generation cannot describe this object. A member
/// inside the Circle re-walks the stream itself before authorizing any delete.
fn validate_circle_snapshot_activated_reclaim_target(
    target: &super::store_reclaim::ReclaimTarget,
    activation: &super::store_reclaim::CircleSnapshotStreamActivation<'_>,
) -> Result<(), RegistrationLoadError> {
    let super::store_reclaim::ReclaimTarget::CircleSnapshotImage(snapshot_image) = target else {
        return Err(RegistrationLoadError::Invalid(
            "reclaim target is not published by a Circle snapshot stream".to_string(),
        ));
    };
    let device_id = activation.author_registration.device_id.to_string();
    let expected_metadata = format!(
        "{}.json",
        super::store_commit::circle_snapshot_slot_prefix(
            activation.circle_id,
            &device_id,
            activation.snapshot.generation,
        )
    );
    let expected_image = format!(
        "{}.db",
        super::store_commit::circle_snapshot_image_semantic_prefix(
            activation.circle_id,
            &device_id,
            snapshot_image.image.image_hash,
        )
    );
    if activation.snapshot.object.slot().logical_key() != expected_metadata
        || snapshot_image.image.object.slot().logical_key() != expected_image
    {
        return Err(RegistrationLoadError::Invalid(
            "reclaim evidence target differs from its exact Circle snapshot generation".to_string(),
        ));
    }
    Ok(())
}

async fn validate_commit_reclaim_receipt(
    history_verifier: &MergeHistoryVerifier<'_>,
    commit: &StoreBatchCommit,
    reference: &super::store_reclaim::ReclaimReceiptRef,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&MembershipChain>,
    accepted: VerifiedMergePredecessorHistory<'_>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "reclaim receipt activation has no exact predecessor provider authority".to_string(),
        )
    })?;
    let (receipt_executor, provider_admin_state, provider_admin_grant, authorization, executor) = {
        let opened = Box::pin(history_verifier.load_reclaim_receipt(reference))
            .await
            .map_err(RegistrationLoadError::Object)?;
        (
            opened.receipt.value.executor.clone(),
            opened.receipt.value.provider_admin_state.clone(),
            opened.receipt.value.provider_admin_grant.clone(),
            opened.receipt.value.authorization.clone(),
            opened.executor,
        )
    };
    if receipt_executor != commit.author_registration
        || executor != *activating_author
        || provider_admin_state != commit.membership_state
        || !predecessor_verifies_provider_administrator_grant(
            predecessor,
            &provider_admin_grant,
            &receipt_executor,
        )
    {
        return Err(RegistrationLoadError::Invalid(
            "reclaim receipt signer is not the effective provider administrator at its exact predecessor"
                .to_string(),
        ));
    }
    if accepted
        .find(|_, candidate| candidate.reclaim_authorization() == Some(&authorization))
        .map_err(registration_attempt_error)?
        .is_none()
    {
        return Err(RegistrationLoadError::Invalid(
            "reclaim receipt authorization is absent from predecessor history".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn load_commit_registrations(
    history_verifier: &MergeHistoryVerifier<'_>,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&MembershipChain>,
    join_evidence: &VerifiedCommitJoinEvidence,
    accepted: VerifiedMergePredecessorHistory<'_>,
) -> Result<Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>, RegistrationLoadError>
{
    if join_evidence.commit != *commit {
        return Err(RegistrationLoadError::Invalid(
            "verified device-join evidence belongs to another Store commit".to_string(),
        ));
    }
    if commit.acknowledgement().is_some() {
        history_verifier
            .validate_commit_acknowledgement(commit, activating_author)
            .await?;
    }
    if let Some(reference) = commit.reclaim_authorization() {
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "reclaim authorization activation has no exact predecessor owner authority"
                    .to_string(),
            )
        })?;
        let opened = history_verifier
            .load_reclaim_authorization(reference)
            .await
            .map_err(RegistrationLoadError::Object)?;
        let evidence = &opened.evidence.value;
        let authorization = &opened.authorization.value;
        let owner_authorized = authorization.authority.membership == commit.membership_state
            && predecessor_verifies_owner(
                predecessor,
                &authorization.authority.membership,
                &evidence.author_pubkey,
                &authorization.authority.owner_grant,
            );
        if evidence.author_pubkey != activating_author.author_pubkey || !owner_authorized {
            return Err(RegistrationLoadError::Invalid(
                "reclaim authorization signer is not an active Owner at its exact predecessor"
                    .to_string(),
            ));
        }
        // Each kind of activating authority is re-read differently, so the binding
        // between the evidence and the object it authorizes deleting dispatches on
        // which authority published the target.
        let target = evidence.claim.target();
        match target.activation() {
            super::store_reclaim::ReclaimActivation::Commit(activating_commit) => {
                validate_commit_activated_reclaim_target(&target, activating_commit, accepted)
            }
            super::store_reclaim::ReclaimActivation::CircleSnapshotMetadata(activation) => {
                validate_circle_snapshot_activated_reclaim_target(&target, &activation)
            }
            super::store_reclaim::ReclaimActivation::PackageBlobBinding(activation) => {
                validate_package_bound_reclaim_target(&target, &activation, accepted)
            }
        }?;
    }
    if let Some(reference) = commit.reclaim_receipt() {
        Box::pin(validate_commit_reclaim_receipt(
            history_verifier,
            commit,
            reference,
            activating_author,
            predecessor,
            accepted,
        ))
        .await?;
    }
    let has_join_attempt = commit
        .device_join_attempt_decisions()
        .iter()
        .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(_)));
    if has_join_attempt {
        validate_commit_join_attempts(commit, activating_author, predecessor, join_evidence)?;
    }
    let verified_join_outcomes = if commit.device_join_outcomes().is_empty() {
        BTreeMap::new()
    } else {
        Box::pin(history_verifier.validate_commit_join_outcomes(
            commit,
            activating_author,
            predecessor,
            join_evidence,
            accepted,
        ))
        .await?
    };
    let has_join_abandonment = commit
        .device_join_attempt_decisions()
        .iter()
        .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Abandoned(_)));
    if has_join_abandonment {
        history_verifier
            .validate_commit_join_abandonments(commit, activating_author, predecessor)
            .await?;
    }
    if !commit.device_join_cleanup_receipts().is_empty() {
        validate_commit_join_cleanup_receipts(
            activating_author,
            predecessor,
            join_evidence,
            accepted,
        )?;
    }
    let mut registrations = Vec::with_capacity(commit.device_registrations().len());
    for activated in commit.device_registrations() {
        let registration = Box::pin(history_verifier.load_registration(&activated.registration))
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "registration activation has no exact predecessor membership authority".to_string(),
            )
        })?;
        let authority = Box::pin(history_verifier.registration_activation(
            activated,
            &registration,
            activating_author,
            predecessor,
            &verified_join_outcomes,
        ))
        .await?;
        registrations.push((registration, authority));
    }
    Ok(registrations)
}

pub(crate) fn device_state_has_active_registration(
    state: &ResolvedStoreDeviceState,
    registration: &StoreDeviceRegistrationRef,
) -> bool {
    state
        .devices
        .get(&registration.device_id)
        .is_some_and(|record| {
            record.registration == *registration
                && matches!(record.status, StoreDeviceStatus::Active)
        })
}

pub(crate) fn device_state_has_pending_proposal(
    state: &ResolvedStoreDeviceState,
    proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
) -> bool {
    state
        .devices
        .get(&proposal.target.device_id)
        .and_then(|record| record.proposals.get(&proposal.proposal_id))
        .is_some_and(|state| {
            matches!(state, StoreDeviceProposalState::Pending { proposal: pending } if pending == proposal)
        })
}
