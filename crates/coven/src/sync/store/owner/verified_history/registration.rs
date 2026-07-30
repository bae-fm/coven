use super::join_validation::*;
use super::*;
use std::future::Future;
use std::pin::Pin;

pub(crate) enum RegistrationLoadError {
    Object(StoreObjectError),
    Invalid(String),
}

pub(crate) type RegistrationLoadFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, RegistrationLoadError>> + Send + 'a>>;

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

fn predecessor_provider_admin_state(
    predecessor: &MembershipChain,
) -> Option<&super::provider::ProviderAdminState> {
    let crate::protocol::membership::MembershipStatus::Resolved(resolved) = predecessor.status()
    else {
        return None;
    };
    Some(resolved.provider_admin.combined_state())
}

pub(crate) fn predecessor_verifies_owner(
    predecessor: &MembershipChain,
    membership: &StoreMembershipStateRef,
    owner_pubkey: &str,
    owner_grant: &crate::protocol::membership::MembershipGrantId,
) -> bool {
    let MembershipStatus::Resolved(resolved) = predecessor.status() else {
        return false;
    };
    StoreMembershipStateRef::from_parts(
        predecessor.head_refs().to_vec(),
        predecessor.resolution_refs().to_vec(),
        membership.recovery().to_vec(),
        resolved.state_hash,
    )
    .is_ok_and(|expected| membership == &expected)
        && predecessor.active_owner_grant(owner_pubkey).as_ref() == Some(owner_grant)
}

pub(crate) fn predecessor_verifies_provider_administrator(
    predecessor: &MembershipChain,
    grant_id: &super::provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
    expected: &super::provider::ProviderAdminGrantRecord,
) -> bool {
    let Some(state) = predecessor_provider_admin_state(predecessor) else {
        return false;
    };
    state.authorizes(grant_id, executor)
        && state
            .records()
            .get(grant_id)
            .is_some_and(|record| record == expected)
}

fn predecessor_verifies_provider_administrator_grant(
    predecessor: &MembershipChain,
    grant_id: &super::provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
) -> bool {
    predecessor_provider_admin_state(predecessor)
        .is_some_and(|state| state.authorizes(grant_id, executor))
}

pub(crate) struct LoadedDeviceJoinCleanupActivation {
    pub(crate) verified_commit: VerifiedStoreBatchCommit,
    pub(crate) receipts: Vec<LoadedCommitJoinCleanupReceipt>,
}

pub(crate) async fn load_device_join_cleanup_activation(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    activation: &super::device_join::DeviceJoinCleanupActivation,
) -> Result<LoadedDeviceJoinCleanupActivation, StorePullError> {
    let verified_commit = history_verifier.load_ref(&activation.activation).await?;
    if verified_commit.value().device_join_cleanup_receipts()
        != std::slice::from_ref(&activation.receipt)
    {
        return Err(StorePullError::Database(
            "device join cleanup activation does not contain its exact sole receipt".to_string(),
        ));
    }
    let receipts = load_commit_join_cleanup_receipts(
        history_verifier,
        verified_commit.value(),
        verified_commit.author(),
    )
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })?;
    Ok(LoadedDeviceJoinCleanupActivation {
        verified_commit,
        receipts,
    })
}

pub(crate) async fn validate_commit_acknowledgement(
    history_verifier: &MergeHistoryVerifier<'_>,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
) -> Result<
    Option<(
        super::store_commit::StoreAckRef,
        super::store_commit::StoreAck,
    )>,
    RegistrationLoadError,
> {
    let Some(reference) = commit.acknowledgement() else {
        return Ok(None);
    };
    let ack = Box::pin(history_verifier.load_store_ack(reference, activating_author))
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
    let predecessor_cut = commit
        .order
        .predecessor_cut()
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    if ack.registration != commit.author_registration
        || ack.store_cut != predecessor_cut
        || ack.device_state != commit.device_state
    {
        return Err(RegistrationLoadError::Invalid(
            "Store acknowledgement differs from its activating commit predecessor".to_string(),
        ));
    }
    if let Some(snapshot) = &ack.snapshot {
        let snapshot_author = history_verifier
            .load_registration(&snapshot.author_registration)
            .await
            .map_err(RegistrationLoadError::Object)?;
        let (_, metadata) = Box::pin(history_verifier.load_store_snapshot(
            &snapshot.author_registration,
            &snapshot_author.value,
            &snapshot.snapshot,
        ))
        .await
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
        if !ack.store_cut.frontier().covers(&metadata.coverage) {
            return Err(RegistrationLoadError::Invalid(
                "Store acknowledgement does not cover its exact snapshot".to_string(),
            ));
        }
    }
    Ok(Some((reference.clone(), ack)))
}

pub(crate) async fn load_acknowledgement_proof_chain(
    history_verifier: &MergeHistoryVerifier<'_>,
    latest_ref: super::store_commit::StoreAckRef,
    latest: super::store_commit::StoreAck,
    registration: &StoreDeviceRegistration,
) -> Result<
    BTreeMap<
        u64,
        (
            super::store_commit::StoreAckRef,
            super::store_commit::StoreAck,
        ),
    >,
    RegistrationLoadError,
> {
    let mut chain = BTreeMap::new();
    let mut current_ref = latest_ref;
    let mut current = latest;
    loop {
        if chain
            .insert(current_ref.sequence, (current_ref.clone(), current.clone()))
            .is_some()
        {
            return Err(RegistrationLoadError::Invalid(
                "Store acknowledgement proof chain repeats a sequence".to_string(),
            ));
        }
        let Some((predecessor_ref, predecessor)) = history_verifier
            .load_store_ack_predecessor(&current_ref, &current, registration)
            .await
            .map_err(RegistrationLoadError::Object)?
        else {
            break;
        };
        current_ref = predecessor_ref;
        current = predecessor.value;
    }
    if chain.first_key_value().map(|(sequence, _)| *sequence) != Some(1)
        || chain.last_key_value().map(|(sequence, _)| *sequence) != Some(chain.len() as u64)
    {
        return Err(RegistrationLoadError::Invalid(
            "Store acknowledgement proof chain is not contiguous from sequence one".to_string(),
        ));
    }
    Ok(chain)
}

async fn validate_commit_reclaim_authorization(
    history_verifier: &MergeHistoryVerifier<'_>,
    commit: &StoreBatchCommit,
    reference: &super::store_reclaim::ReclaimAuthorizationRef,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&MembershipChain>,
    accepted: VerifiedMergePredecessorHistory<'_>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "reclaim authorization activation has no exact predecessor owner authority".to_string(),
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
    }
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
    let storage = history_verifier.storage();
    let root = history_verifier.root();
    if join_evidence.commit != *commit {
        return Err(RegistrationLoadError::Invalid(
            "verified device-join evidence belongs to another Store commit".to_string(),
        ));
    }
    if commit.acknowledgement().is_some() {
        Box::pin(validate_commit_acknowledgement(
            history_verifier,
            commit,
            activating_author,
        ))
        .await?;
    }
    if let Some(reference) = commit.reclaim_authorization() {
        Box::pin(validate_commit_reclaim_authorization(
            history_verifier,
            commit,
            reference,
            activating_author,
            predecessor,
            accepted,
        ))
        .await?;
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
        Box::pin(validate_commit_join_outcomes(
            history_verifier,
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
        Box::pin(validate_commit_join_abandonments(
            storage,
            root,
            commit,
            activating_author,
            predecessor,
        ))
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
        let authority = Box::pin(registration_activation(
            history_verifier,
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

pub(crate) async fn verify_canonical_owner_registration(
    commit_verifier: &StoreCommitVerifier<'_>,
    state: &ResolvedStoreDeviceState,
    owner_pubkey: &str,
    selected: &StoreDeviceRegistrationRef,
) -> Result<(), StorePullError> {
    let active = load_active_history_registrations(commit_verifier, state).await?;
    let canonical = active
        .values()
        .filter(|(_, registration)| registration.author_pubkey == owner_pubkey)
        .map(|(reference, _)| reference)
        .min();
    if canonical != Some(selected) {
        return Err(StorePullError::Database(
            "conflict-resolution acceptance does not use the canonical active Owner registration"
                .to_string(),
        ));
    }
    Ok(())
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
