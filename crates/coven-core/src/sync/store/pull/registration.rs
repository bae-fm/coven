use super::*;

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

pub(crate) struct RegistrationPredecessorAuthority<'a>(pub(crate) &'a MembershipChain);

impl RegistrationPredecessorAuthority<'_> {
    fn provider_admin_state(&self) -> Option<&super::provider::ProviderAdminState> {
        {
            let chain = self.0;
            let super::membership::MembershipStatus::Resolved(resolved) = chain.status() else {
                return None;
            };
            Some(resolved.provider_admin.combined_state())
        }
    }

    pub(crate) fn verifies_owner(
        &self,
        membership: &StoreMembershipStateRef,
        owner_pubkey: &str,
        owner_grant: &super::membership::MembershipGrantId,
    ) -> bool {
        {
            let chain = self.0;
            let MembershipStatus::Resolved(resolved) = chain.status() else {
                return false;
            };
            StoreMembershipStateRef::from_parts(
                chain.head_refs().to_vec(),
                chain.resolution_refs().to_vec(),
                membership.recovery().to_vec(),
                resolved.state_hash,
            )
            .is_ok_and(|expected| membership == &expected)
                && chain.active_owner_grant(owner_pubkey).as_ref() == Some(owner_grant)
        }
    }

    pub(crate) fn verifies_active_owner(&self, owner_pubkey: &str) -> bool {
        self.0.is_owner_now(owner_pubkey)
    }

    pub(crate) fn verifies_provider_administrator(
        &self,
        grant_id: &super::provider::ProviderAdminGrantId,
        executor: &StoreDeviceRegistrationRef,
        expected: &super::provider::ProviderAdminGrantRecord,
    ) -> bool {
        let Some(state) = self.provider_admin_state() else {
            return false;
        };
        state.authorizes(grant_id, executor)
            && state
                .records()
                .get(grant_id)
                .is_some_and(|record| record == expected)
    }

    fn verifies_provider_administrator_grant(
        &self,
        grant_id: &super::provider::ProviderAdminGrantId,
        executor: &StoreDeviceRegistrationRef,
    ) -> bool {
        self.provider_admin_state()
            .is_some_and(|state| state.authorizes(grant_id, executor))
    }
}

pub(crate) struct LoadedDeviceJoinCleanupActivation {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) author: StoreDeviceRegistration,
    pub(crate) receipts: Vec<LoadedCommitJoinCleanupReceipt>,
}

pub(crate) fn load_device_join_cleanup_activation<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    activation: &'a super::device_join::DeviceJoinCleanupActivation,
) -> StorePullFuture<'a, LoadedDeviceJoinCleanupActivation> {
    Box::pin(async move {
        let root_value = load_store_protocol_root(storage, root).await?.value;
        let (commit, author) =
            load_commit_with_author_at_root(storage, root, &root_value, &activation.activation)
                .await?;
        if commit.device_join_cleanup_receipts() != std::slice::from_ref(&activation.receipt) {
            return Err(StorePullError::Database(
                "device join cleanup activation does not contain its exact sole receipt"
                    .to_string(),
            ));
        }
        let receipts =
            load_commit_join_cleanup_receipts(storage, root, &root_value, &commit, &author)
                .await
                .map_err(|error| match error {
                    RegistrationLoadError::Object(error) => StorePullError::Object(error),
                    RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
                })?;
        Ok(LoadedDeviceJoinCleanupActivation {
            commit,
            author,
            receipts,
        })
    })
}

pub(crate) async fn validate_commit_acknowledgement(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
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
    let ack = Box::pin(load_store_ack_ref(
        storage,
        root,
        reference,
        activating_author,
    ))
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
        let snapshot_author = load_registration_ref(storage, root, &snapshot.author_registration)
            .await
            .map_err(RegistrationLoadError::Object)?;
        let (_, metadata) = Box::pin(crate::sync::store::snapshot::load_store_snapshot_ref(
            storage,
            root,
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
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
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
        let Some((predecessor_ref, predecessor)) =
            load_store_ack_predecessor(storage, root, &current_ref, &current, registration)
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

pub(crate) async fn retain_activated_acknowledgement(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activating_commit: &StoreBatchCommitRef,
    activating_commit_value: &StoreBatchCommit,
    registration: &StoreDeviceRegistration,
    reference: super::store_commit::StoreAckRef,
    value: super::store_commit::StoreAck,
) -> Result<super::store_commit::RetainedVerifiedActivatedAck, StorePullError> {
    if activating_commit_value.acknowledgement() != Some(&reference)
        || activating_commit_value.author_registration != reference.registration
        || value.registration != reference.registration
    {
        return Err(StorePullError::Database(
            "Store acknowledgement differs from its activating commit".to_string(),
        ));
    }
    activating_commit
        .verify_commit(activating_commit_value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let chain = load_acknowledgement_proof_chain(storage, root, reference, value, registration)
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
    Ok(super::store_commit::RetainedVerifiedActivatedAck {
        chain,
        activating_commit: activating_commit.clone(),
        activating_commit_value: activating_commit_value.clone(),
    })
}

async fn validate_commit_reclaim_authorization(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    reference: &super::store_reclaim::ReclaimAuthorizationRef,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "reclaim authorization activation has no exact predecessor owner authority".to_string(),
        )
    })?;
    let opened = load_reclaim_authorization_ref(storage, root, reference)
        .await
        .map_err(RegistrationLoadError::Object)?;
    let evidence = &opened.evidence.value;
    let authorization = &opened.authorization.value;
    let owner_authorized = authorization.authority.membership == commit.membership_state
        && predecessor.verifies_owner(
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
    let target = evidence.claim.target();
    let target_activation = target.activation().clone();
    let activation = Box::pin(predecessor_commit_matching_at_root(
        storage,
        root,
        root_value,
        &commit.order,
        Box::new(move |candidate, _| candidate == &target_activation),
    ))
    .await?
    .ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "reclaim evidence package activation is absent from predecessor history".to_string(),
        )
    })?;
    let names_target = match &target {
        super::store_reclaim::ReclaimTarget::StorePackage(store) => {
            activation.1.store_package() == Some(&store.package)
        }
        super::store_reclaim::ReclaimTarget::CirclePackage(circle) => {
            activation.1.circle_packages().contains(&circle.package)
        }
        super::store_reclaim::ReclaimTarget::CircleBootstrapImage(bootstrap) => activation
            .1
            .circle_controls()
            .iter()
            .flat_map(|control| control.objects.access.iter())
            .any(|access| access.bootstrap.as_ref() == Some(&bootstrap.coverage.bootstrap.image)),
    };
    if !names_target {
        return Err(RegistrationLoadError::Invalid(
            "reclaim evidence target differs from its exact package activation".to_string(),
        ));
    }
    Ok(())
}

async fn validate_commit_reclaim_receipt(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    reference: &super::store_reclaim::ReclaimReceiptRef,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "reclaim receipt activation has no exact predecessor provider authority".to_string(),
        )
    })?;
    let (receipt_executor, provider_admin_state, provider_admin_grant, authorization, executor) = {
        let opened = Box::pin(load_reclaim_receipt_ref(storage, root, reference))
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
        || !predecessor
            .verifies_provider_administrator_grant(&provider_admin_grant, &receipt_executor)
    {
        return Err(RegistrationLoadError::Invalid(
            "reclaim receipt signer is not the effective provider administrator at its exact predecessor"
                .to_string(),
        ));
    }
    if Box::pin(predecessor_commit_matching_at_root(
        storage,
        root,
        root_value,
        &commit.order,
        Box::new(|_, candidate| candidate.reclaim_authorization() == Some(&authorization)),
    ))
    .await?
    .is_none()
    {
        return Err(RegistrationLoadError::Invalid(
            "reclaim receipt authorization is absent from predecessor history".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn load_commit_registrations_with_root(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    join_evidence: &VerifiedCommitJoinEvidence,
) -> Result<Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>, RegistrationLoadError>
{
    if join_evidence.commit != *commit {
        return Err(RegistrationLoadError::Invalid(
            "verified device-join evidence belongs to another Store commit".to_string(),
        ));
    }
    if commit.acknowledgement().is_some() {
        Box::pin(validate_commit_acknowledgement(
            storage,
            root,
            commit,
            activating_author,
        ))
        .await?;
    }
    if let Some(reference) = commit.reclaim_authorization() {
        Box::pin(validate_commit_reclaim_authorization(
            storage,
            root,
            root_value,
            commit,
            reference,
            activating_author,
            predecessor,
        ))
        .await?;
    }
    if let Some(reference) = commit.reclaim_receipt() {
        Box::pin(validate_commit_reclaim_receipt(
            storage,
            root,
            root_value,
            commit,
            reference,
            activating_author,
            predecessor,
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
            storage,
            root,
            root_value,
            commit,
            activating_author,
            predecessor,
            join_evidence,
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
        validate_commit_join_cleanup_receipts(activating_author, predecessor, join_evidence)?;
    }
    let mut registrations = Vec::with_capacity(commit.device_registrations().len());
    for activated in commit.device_registrations() {
        let registration = Box::pin(load_registration_ref_with_root(
            storage,
            root,
            root_value,
            &activated.registration,
        ))
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "registration activation has no exact predecessor membership authority".to_string(),
            )
        })?;
        let authority = Box::pin(registration_activation(
            storage,
            root,
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
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &ResolvedStoreDeviceState,
    owner_pubkey: &str,
    selected: &StoreDeviceRegistrationRef,
) -> Result<(), StorePullError> {
    let active = load_active_history_registrations(storage, root, state).await?;
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

pub(super) fn device_state_has_pending_proposal(
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
