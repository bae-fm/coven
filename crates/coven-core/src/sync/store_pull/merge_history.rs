use super::*;

pub(super) struct VerifiedMergeHistoryCommit {
    pub(super) commit: StoreBatchCommit,
    pub(super) predecessor_membership: MembershipChain,
    pub(super) predecessor_state: ResolvedStoreDeviceState,
    pub(super) state_after: ResolvedStoreDeviceState,
    pub(super) operations: VerifiedStoreDeviceOperations,
    pub(super) acknowledgement: Option<(
        super::store_commit::StoreAckRef,
        super::store_commit::StoreAck,
    )>,
    pub(super) membership_control: Option<VerifiedMergeMembershipControl>,
    pub(super) history: OpenedRetainedMergeHistorySummary,
}

impl VerifiedAcceptedPredecessor<'_> {
    pub(super) fn serial_history_commit(
        &self,
        target: &StoreBatchCommitRef,
    ) -> Result<Option<&AuthorizedSerialCommit>, StorePullError> {
        let Self::SerialHistory { commits } = self else {
            return Ok(None);
        };
        commits
            .iter()
            .find(|accepted| &accepted.commit_ref == target)
            .map(Some)
            .ok_or_else(|| {
                StorePullError::Serial(
                    "provider-access activation is outside the accepted Serial predecessor history"
                        .to_string(),
                )
            })
    }

    pub(super) fn merge_history_commit(
        &self,
        target: &StoreBatchCommitRef,
    ) -> Result<Option<&VerifiedMergeHistoryCommit>, StorePullError> {
        let Self::MergeHistory { commits, frontier } = self else {
            return Ok(None);
        };
        let mut pending = frontier.clone();
        let mut visited = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            let commit = commits.get(&reference).ok_or_else(|| {
                StorePullError::Database(
                    "accepted Merge predecessor graph is missing an exact commit".to_string(),
                )
            })?;
            if &reference == target {
                return Ok(Some(commit));
            }
            pending.extend(commit_predecessor_references(&commit.commit));
        }
        Err(StorePullError::Database(
            "provider-access activation is outside the accepted Merge predecessor graph"
                .to_string(),
        ))
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct VerifiedMergeMembershipHeadActivation {
    commit: StoreBatchCommitRef,
    transition: super::membership::MergeMembershipHeadTransition,
}

impl VerifiedMergeMembershipHeadActivation {
    pub(crate) fn verifies(
        &self,
        reference: &super::membership::MembershipHeadRef,
        head: &super::membership::AuthorHead,
        commit: &StoreBatchCommitRef,
    ) -> bool {
        &self.commit == commit && self.transition.matches_head(head, reference)
    }
}

pub(super) struct VerifiedMergeMembershipControl {
    pub(super) activations: VerifiedCircleActivations,
    head_activation: VerifiedMergeMembershipHeadActivation,
    conflict_resolution: Option<VerifiedMergeConflictResolutionActivation>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct VerifiedMergeConflictResolutionActivation {
    reference: super::membership::StoreMembershipConflictResolutionRef,
}

impl VerifiedMergeConflictResolutionActivation {
    pub(crate) fn reference(&self) -> &super::membership::StoreMembershipConflictResolutionRef {
        &self.reference
    }

    pub(crate) fn verifies(
        &self,
        reference: &super::membership::StoreMembershipConflictResolutionRef,
    ) -> bool {
        &self.reference == reference
    }
}

#[derive(Clone, Default)]
pub(crate) struct VerifiedMergeMembershipPrefix {
    commits: BTreeSet<StoreBatchCommitRef>,
    predecessor_memberships: Vec<MembershipChain>,
    head_activations: BTreeMap<StoreBatchCommitRef, VerifiedMergeMembershipHeadActivation>,
    conflict_resolutions: BTreeMap<
        super::membership::StoreMembershipConflictResolutionRef,
        VerifiedMergeConflictResolutionActivation,
    >,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VerifiedMergePrefixHeadStatus {
    Included,
    OutsidePrefix,
}

impl VerifiedMergeMembershipPrefix {
    pub(super) fn from_retained(
        checkpoints: &[OpenedRetainedMergeHistorySummary],
    ) -> Result<Self, StorePullError> {
        let mut prefix = Self::default();
        for checkpoint in checkpoints {
            for reference in checkpoint.summary.causal_cut.values() {
                prefix.commits.insert(reference.clone());
            }
            for proof in checkpoint.summary.membership_proofs.values() {
                let Some(super::store_commit::StoreControl::MergeMembership { transition }) =
                    proof.commit_value.control()
                else {
                    return Err(StorePullError::Database(
                        "retained Merge membership proof has no membership control".to_string(),
                    ));
                };
                let activation = VerifiedMergeMembershipHeadActivation {
                    commit: proof.commit.clone(),
                    transition: transition.clone(),
                };
                match prefix.head_activations.entry(proof.commit.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(activation);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if entry.get() == &activation => {}
                    std::collections::btree_map::Entry::Occupied(_) => {
                        return Err(StorePullError::Database(
                            "retained checkpoints disagree on a membership activation".to_string(),
                        ));
                    }
                }
                if let Some(reference) = &proof.resolution {
                    let activation = VerifiedMergeConflictResolutionActivation {
                        reference: reference.clone(),
                    };
                    match prefix.conflict_resolutions.entry(reference.clone()) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(activation);
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if entry.get() == &activation => {}
                        std::collections::btree_map::Entry::Occupied(_) => {
                            return Err(StorePullError::Database(
                                "retained checkpoints disagree on a conflict resolution"
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        }
        Ok(prefix)
    }

    pub(crate) fn head_activation(
        &self,
        commit: &StoreBatchCommitRef,
    ) -> Option<&VerifiedMergeMembershipHeadActivation> {
        self.head_activations.get(commit)
    }

    pub(crate) fn verifies_conflict_resolution(
        &self,
        reference: &super::membership::StoreMembershipConflictResolutionRef,
    ) -> bool {
        self.conflict_resolutions
            .get(reference)
            .is_some_and(|proof| proof.verifies(reference))
    }

    pub(crate) fn classify_head(
        &self,
        reference: &super::membership::MembershipHeadRef,
        head: &super::membership::AuthorHead,
        commit: &StoreBatchCommitRef,
    ) -> Result<VerifiedMergePrefixHeadStatus, String> {
        if !self.commits.contains(commit) {
            return Ok(VerifiedMergePrefixHeadStatus::OutsidePrefix);
        }
        let proof = self.head_activations.get(commit).ok_or_else(|| {
            "in-prefix membership activation is absent from its verified Store control".to_string()
        })?;
        if !proof.verifies(reference, head, commit) {
            return Err(
                "membership head differs from its in-prefix verified Store control".to_string(),
            );
        }
        Ok(VerifiedMergePrefixHeadStatus::Included)
    }

    pub(crate) fn validate_complete_membership(
        &self,
        membership: &MembershipChain,
    ) -> Result<(), String> {
        if self
            .predecessor_memberships
            .iter()
            .any(|predecessor| !membership.causally_includes(predecessor))
        {
            return Err(
                "membership state regresses below an exact Store predecessor membership"
                    .to_string(),
            );
        }
        if self
            .head_activations
            .values()
            .any(|proof| !membership.contains_coord(&proof.transition.body.entry.coord))
        {
            return Err("membership state omits an accepted Store membership control".to_string());
        }
        if self.conflict_resolutions.keys().any(|reference| {
            membership
                .resolution_refs()
                .binary_search(reference)
                .is_err()
        }) {
            return Err("membership state omits an accepted Store conflict resolution".to_string());
        }
        Ok(())
    }
}

pub(super) fn verified_merge_membership_prefix(
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<VerifiedMergeMembershipPrefix, StorePullError> {
    let closure = verified_merge_commit_closure(commits, tips)?;
    let mut prefix = VerifiedMergeMembershipPrefix {
        commits: closure.clone(),
        ..VerifiedMergeMembershipPrefix::default()
    };
    for reference in closure {
        let verified = &commits[&reference];
        prefix
            .predecessor_memberships
            .push(verified.predecessor_membership.clone());
        if let Some(control) = &verified.membership_control {
            prefix
                .head_activations
                .insert(reference, control.head_activation.clone());
            if let Some(resolution) = &control.conflict_resolution {
                prefix
                    .conflict_resolutions
                    .insert(resolution.reference.clone(), resolution.clone());
            }
        }
    }
    Ok(prefix)
}

fn verified_merge_commit_closure(
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<BTreeSet<StoreBatchCommitRef>, StorePullError> {
    let mut pending = tips.into_iter().collect::<Vec<_>>();
    let mut closure = BTreeSet::new();
    while let Some(reference) = pending.pop() {
        if !closure.insert(reference.clone()) {
            continue;
        }
        let verified = commits.get(&reference).ok_or_else(|| {
            StorePullError::Database(
                "verified Merge predecessor closure is absent from its history".to_string(),
            )
        })?;
        pending.extend(commit_predecessor_references(&verified.commit));
    }
    Ok(closure)
}

fn merge_device_state_from_verified_history(
    reference: &StoreDeviceStateRef,
    genesis: &ResolvedStoreDeviceState,
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    allowed_tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let StoreDeviceStateRef::MergeConcurrent { frontier, .. } = reference else {
        return Err(StorePullError::Database(
            "Merge authority carries Serial device state".to_string(),
        ));
    };
    let CommitFrontier::MergeConcurrent(frontier) = frontier else {
        return Err(StorePullError::Database(
            "Merge device state carries Serial frontier".to_string(),
        ));
    };
    let allowed = verified_merge_commit_closure(commits, allowed_tips)?;
    if frontier
        .values()
        .any(|reference| !allowed.contains(reference))
    {
        return Err(StorePullError::Database(
            "Merge device state names a commit outside its causal predecessor history".to_string(),
        ));
    }
    let state = if frontier.is_empty() {
        genesis.clone()
    } else {
        ResolvedStoreDeviceState::merge(
            frontier
                .values()
                .map(|reference| {
                    commits
                        .get(reference)
                        .map(|verified| verified.state_after.clone())
                        .ok_or_else(|| {
                            StorePullError::Database(
                                "Merge device-state frontier is absent from its verified history"
                                    .to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    };
    let expected = StoreDeviceStateRef::merge_concurrent(
        CommitFrontier::MergeConcurrent(frontier.clone()),
        &state,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if &expected != reference {
        return Err(StorePullError::Database(
            "Merge device-state reference differs from its verified history".to_string(),
        ));
    }
    Ok(state)
}

async fn verify_merge_owner_conflict_acceptance_with_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerConflictResolutionAcceptance,
    resolver_pubkey: &str,
    genesis: &ResolvedStoreDeviceState,
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    allowed_tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<(), StorePullError> {
    let registration = load_registration_ref(storage, root, &acceptance.owner_registration).await?;
    acceptance
        .verify(&registration.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let state = merge_device_state_from_verified_history(
        &acceptance.device_state,
        genesis,
        commits,
        allowed_tips,
    )?;
    if !device_state_has_active_registration(&state, &acceptance.owner_registration) {
        return Err(StorePullError::Database(
            "conflict-resolution Owner registration is not active at its exact device state"
                .to_string(),
        ));
    }
    verify_canonical_owner_registration(
        storage,
        root,
        &state,
        resolver_pubkey,
        &acceptance.owner_registration,
    )
    .await?;
    Ok(())
}

pub(super) async fn verify_merge_resolution_activation_acceptance_with_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    genesis: &ResolvedStoreDeviceState,
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
) -> Result<Option<VerifiedMergeConflictResolutionActivation>, StorePullError> {
    let Some(super::store_commit::StoreControl::MergeMembership { transition }) = commit.control()
    else {
        return Ok(None);
    };
    let entry = super::store_objects::load_membership_entry_ref(
        storage,
        root.store_root_hash,
        &transition.body.entry,
    )
    .await?;
    let super::membership::MembershipChange::ResolutionActivation { resolution } =
        &entry.value.change
    else {
        return Ok(None);
    };
    if entry.value.coord() != transition.body.entry.coord {
        return Err(StorePullError::Database(
            "Merge resolution activation differs from its exact transition".to_string(),
        ));
    }
    let value = super::store_objects::load_membership_resolution_ref(
        storage,
        root.store_root_hash,
        resolution,
    )
    .await?;
    let registration = load_registration_ref(storage, root, &commit.author_registration).await?;
    let acceptance = &value.value.replacement_acceptance;
    let mut expected_activations = vec![
        super::store_commit::StreamActivation::grant_authorized(
            root.store_root_hash,
            acceptance.owner_registration.clone(),
            value.value.replacement_grant.clone(),
            acceptance.membership.clone(),
        ),
        super::store_commit::StreamActivation::grant_authorized(
            root.store_root_hash,
            acceptance.owner_registration.clone(),
            value.value.replacement_grant.clone(),
            acceptance.recovery.clone(),
        ),
    ];
    expected_activations.sort();
    if acceptance.owner_registration != commit.author_registration
        || registration.value.author_pubkey != value.value.resolver_pubkey
        || entry.value.author_pubkey != value.value.resolver_pubkey
        || transition.body.author_registration != commit.author_registration
        || commit.stream_activations() != expected_activations
    {
        return Err(StorePullError::Database(
            "Merge resolution activation differs from its accepted Owner authority".to_string(),
        ));
    }
    verify_merge_owner_conflict_acceptance_with_history(
        storage,
        root,
        acceptance,
        &value.value.resolver_pubkey,
        genesis,
        commits,
        commit_predecessor_references(commit),
    )
    .await?;
    Ok(Some(VerifiedMergeConflictResolutionActivation {
        reference: resolution.clone(),
    }))
}

pub(super) struct VerifiedMergeHistory {
    pub(super) genesis: ResolvedStoreDeviceState,
    pub(super) commits: BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
}

pub(crate) struct MergeOutboundAuthorization {
    pub(crate) membership: MembershipChain,
    pub(crate) membership_state: StoreMembershipStateRef,
    pub(crate) device_state_ref: StoreDeviceStateRef,
    pub(crate) device_state: ResolvedStoreDeviceState,
}

pub(crate) struct PreparedMergeHistorySuccessor {
    pub(crate) summary: RetainedVerifiedMergeHistorySummary,
    pub(crate) head_slot: crate::storage::cloud::ObjectSlot,
    pub(crate) predecessor_head: Option<super::store_commit::StoreDeviceHeadRef>,
}

pub(crate) struct MergeHistorySuccessorEvidence {
    pub(crate) registrations: Vec<RetainedVerifiedRegistration>,
    pub(crate) acknowledgement: Option<super::store_commit::RetainedVerifiedActivatedAck>,
    pub(crate) membership_proof: Option<super::store_commit::RetainedMergeMembershipProof>,
}

impl MergeHistorySuccessorEvidence {
    pub(crate) fn none() -> Self {
        Self {
            registrations: Vec::new(),
            acknowledgement: None,
            membership_proof: None,
        }
    }
}

fn insert_exact<K, V>(
    target: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    conflict: &str,
) -> Result<(), StorePullError>
where
    K: Ord,
    V: PartialEq,
{
    match target.entry(key) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &value => Ok(()),
        std::collections::btree_map::Entry::Occupied(_) => {
            Err(StorePullError::Database(conflict.to_string()))
        }
    }
}

pub(super) fn insert_latest_acknowledgement(
    target: &mut BTreeMap<
        super::store_commit::StoreDeviceId,
        super::store_commit::RetainedVerifiedActivatedAck,
    >,
    device_id: super::store_commit::StoreDeviceId,
    value: super::store_commit::RetainedVerifiedActivatedAck,
) -> Result<(), StorePullError> {
    match target.entry(device_id) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &value => Ok(()),
        std::collections::btree_map::Entry::Occupied(mut entry)
            if value.exactly_extends(entry.get()) =>
        {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry)
            if entry.get().exactly_extends(&value) =>
        {
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(_) => Err(StorePullError::Database(
            "Merge predecessor checkpoints contain forked acknowledgement proof chains".to_string(),
        )),
    }
}

fn insert_latest_announcement(
    target: &mut BTreeMap<
        super::membership::AuthorStreamId,
        super::store_commit::RetainedAcceptedStoreAnnouncement,
    >,
    stream_id: super::membership::AuthorStreamId,
    value: super::store_commit::RetainedAcceptedStoreAnnouncement,
) -> Result<(), StorePullError> {
    match target.entry(stream_id) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &value => Ok(()),
        std::collections::btree_map::Entry::Occupied(mut entry)
            if entry.get().value.commit.coord.sequence() < value.value.commit.coord.sequence() =>
        {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry)
            if entry.get().value.commit.coord.sequence() > value.value.commit.coord.sequence() =>
        {
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(_) => Err(StorePullError::Database(
            "Merge predecessor checkpoints contain conflicting announcement heads at one sequence"
                .to_string(),
        )),
    }
}

fn insert_membership_proof(
    target: &mut BTreeMap<StoreBatchCommitRef, super::store_commit::RetainedMergeMembershipProof>,
    reference: StoreBatchCommitRef,
    value: super::store_commit::RetainedMergeMembershipProof,
) -> Result<(), StorePullError> {
    if target
        .keys()
        .any(|existing| existing.coord == reference.coord && existing != &reference)
    {
        return Err(StorePullError::Database(
            "Merge predecessor checkpoints contain conflicting membership proofs at one Store coordinate"
                .to_string(),
        ));
    }
    insert_exact(
        target,
        reference,
        value,
        "Merge predecessor checkpoints disagree on a membership proof",
    )
}

pub(crate) async fn prepare_merge_history_successor(
    db: &Database,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    commit_ref: &StoreBatchCommitRef,
    membership: &MembershipChain,
    author: &StoreDeviceRegistration,
    recovery_author: Option<&StoreDeviceRegistrationRef>,
    state_after: ResolvedStoreDeviceState,
    evidence: MergeHistorySuccessorEvidence,
) -> Result<PreparedMergeHistorySuccessor, StorePullError> {
    commit_ref
        .verify_commit(commit)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    state_after.validate_canonical().map_err(|error| {
        StorePullError::Database(format!("validate Merge successor post-state: {error}"))
    })?;
    let predecessor_refs = commit_predecessor_references(commit);
    let predecessors = db
        .retained_merge_history_frontier(predecessor_refs.clone())
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if predecessors.len() != predecessor_refs.len() {
        return Err(StorePullError::Database(
            "Merge successor is missing a retained direct predecessor".to_string(),
        ));
    }
    let (expected_predecessor_ref, predecessor_state) = db
        .store_device_state_for_order(&commit.order)
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if commit.device_state != expected_predecessor_ref {
        return Err(StorePullError::Database(
            "Merge successor names another predecessor device state".to_string(),
        ));
    }
    if let Some(recovery_author) = recovery_author {
        let retained_recovery_registration = evidence.registrations.iter().any(|registration| {
            registration.reference == *recovery_author
                && matches!(
                    &registration.value.origin,
                    super::store_commit::StoreDeviceRegistrationOrigin::Recovery { .. }
                )
        });
        let recovery_activation = commit.device_registrations().iter().any(|activation| {
            activation.registration == *recovery_author
                && matches!(
                    &activation.authority,
                    super::store_commit::StoreDeviceRegistrationActivationRef::Recovery { .. }
                )
        });
        if recovery_author != &commit.author_registration
            || !retained_recovery_registration
            || !recovery_activation
        {
            return Err(StorePullError::Database(
                "Merge successor recovery author lacks its exact retained activation".to_string(),
            ));
        }
    }
    if !device_state_has_active_registration(&predecessor_state, &commit.author_registration)
        && recovery_author != Some(&commit.author_registration)
    {
        return Err(StorePullError::Database(
            "Merge successor author is inactive at its exact predecessor cut".to_string(),
        ));
    }
    verify_merge_membership_state_ref(&commit.membership_state, membership, &predecessor_state)?;

    compose_merge_history_successor(
        root,
        commit,
        commit_ref,
        membership,
        author,
        state_after,
        predecessors,
        evidence,
    )
}

pub(super) struct MergedRetainedMergeHistory {
    causal_cut: BTreeMap<StoreCommitCoord, StoreBatchCommitRef>,
    registrations: BTreeMap<super::store_commit::StoreDeviceId, RetainedVerifiedRegistration>,
    acknowledgements: BTreeMap<
        super::store_commit::StoreDeviceId,
        super::store_commit::RetainedVerifiedActivatedAck,
    >,
    membership_proofs:
        BTreeMap<StoreBatchCommitRef, super::store_commit::RetainedMergeMembershipProof>,
    announcement_frontier: BTreeMap<
        super::membership::AuthorStreamId,
        super::store_commit::RetainedAcceptedStoreAnnouncement,
    >,
}

pub(super) fn merge_retained_merge_history(
    root: &StoreRootRef,
    membership: &MembershipChain,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
) -> Result<MergedRetainedMergeHistory, StorePullError> {
    let mut causal_cut = BTreeMap::new();
    let mut registrations = BTreeMap::new();
    let mut acknowledgements = BTreeMap::new();
    let mut membership_proofs = BTreeMap::new();
    let mut announcement_frontier = BTreeMap::new();
    for predecessor in predecessors {
        let predecessor_cut = predecessor.summary.causal_cut.clone();
        if predecessor.summary.store_root_hash != root.store_root_hash
            || predecessor.summary.policy != crate::WritePolicy::MergeConcurrent
        {
            return Err(StorePullError::Database(
                "Merge predecessor checkpoint belongs to another Store or policy".to_string(),
            ));
        }
        if predecessor
            .summary
            .membership_floor
            .effective_coordinates
            .iter()
            .any(|coordinate| !membership.effectively_contains_coord(coordinate))
            || predecessor
                .summary
                .membership_floor
                .resolutions
                .iter()
                .any(|reference| {
                    membership
                        .resolution_refs()
                        .binary_search(reference)
                        .is_err()
                })
        {
            return Err(StorePullError::Database(
                "Merge successor membership omits its retained causal floor".to_string(),
            ));
        }
        for (key, value) in predecessor.summary.causal_cut {
            insert_exact(
                &mut causal_cut,
                key,
                value,
                "Merge predecessor checkpoints disagree on a Store coordinate",
            )?;
        }
        for (key, value) in predecessor.summary.registrations {
            insert_exact(
                &mut registrations,
                key,
                value,
                "Merge predecessor checkpoints disagree on a device registration",
            )?;
        }
        for (key, value) in predecessor.summary.acknowledgements {
            insert_latest_acknowledgement(&mut acknowledgements, key, value)?;
        }
        for (key, mut value) in predecessor.summary.membership_proofs {
            if predecessor_cut.get(&value.commit.coord) == Some(&value.commit)
                && value.announcement.is_none()
            {
                let StoreCommitCoord::MergeConcurrent { stream_id, .. } = value.commit.coord else {
                    return Err(StorePullError::Database(
                        "Merge membership proof contains a Serial commit".to_string(),
                    ));
                };
                value.announcement = predecessor
                    .announcement_frontier
                    .get(&stream_id)
                    .filter(|announcement| announcement.value.commit == value.commit)
                    .cloned();
            }
            insert_membership_proof(&mut membership_proofs, key, value)?;
        }
        for (key, value) in predecessor.announcement_frontier {
            insert_latest_announcement(&mut announcement_frontier, key, value)?;
        }
    }
    Ok(MergedRetainedMergeHistory {
        causal_cut,
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    })
}

fn compose_merge_history_successor(
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    commit_ref: &StoreBatchCommitRef,
    membership: &MembershipChain,
    author: &StoreDeviceRegistration,
    state_after: ResolvedStoreDeviceState,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
    evidence: MergeHistorySuccessorEvidence,
) -> Result<PreparedMergeHistorySuccessor, StorePullError> {
    let MergedRetainedMergeHistory {
        mut causal_cut,
        mut registrations,
        mut acknowledgements,
        mut membership_proofs,
        announcement_frontier,
    } = merge_retained_merge_history(root, membership, predecessors)?;
    let mut membership_floor =
        super::store_commit::MembershipCausalFloor::from_membership(membership);
    insert_exact(
        &mut causal_cut,
        commit_ref.coord.clone(),
        commit_ref.clone(),
        "Merge successor conflicts at its Store coordinate",
    )?;
    for registration in evidence.registrations {
        if !commit
            .device_registrations()
            .iter()
            .any(|activation| activation.registration == registration.reference)
        {
            return Err(StorePullError::Database(
                "Merge history registration is absent from its activating commit".to_string(),
            ));
        }
        insert_exact(
            &mut registrations,
            registration.reference.device_id,
            registration,
            "Merge successor registration conflicts with retained authority",
        )?;
    }
    if let Some(retained) = evidence.acknowledgement {
        let (reference, _) = retained.latest().ok_or_else(|| {
            StorePullError::Database(
                "Merge history acknowledgement proof chain is empty".to_string(),
            )
        })?;
        if commit.acknowledgement() != Some(reference)
            || retained.activating_commit != *commit_ref
            || retained.activating_commit_value != *commit
        {
            return Err(StorePullError::Database(
                "Merge history acknowledgement differs from its activating commit".to_string(),
            ));
        }
        insert_latest_acknowledgement(
            &mut acknowledgements,
            reference.registration.device_id,
            retained,
        )?;
    }
    if let Some(proof) = evidence.membership_proof {
        if proof.commit != *commit_ref {
            return Err(StorePullError::Database(
                "Merge membership proof names another activating commit".to_string(),
            ));
        }
        membership_floor
            .advance(
                proof.entry.coord.clone(),
                &proof.head_value.body.resolutions,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        insert_membership_proof(&mut membership_proofs, commit_ref.clone(), proof)?;
    }
    let author_ref = commit.author_registration.clone();
    author_ref
        .verify_registration(author)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    insert_exact(
        &mut registrations,
        author_ref.device_id,
        RetainedVerifiedRegistration {
            reference: author_ref.clone(),
            value: author.clone(),
        },
        "Merge successor author registration conflicts with retained authority",
    )?;
    let mut post_frontier = BTreeMap::new();
    for reference in causal_cut.values() {
        let StoreCommitCoord::MergeConcurrent {
            stream_id,
            sequence,
        } = reference.coord
        else {
            return Err(StorePullError::Database(
                "Merge causal cut contains a Serial coordinate".to_string(),
            ));
        };
        match post_frontier.entry(stream_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(reference.clone());
            }
            std::collections::btree_map::Entry::Occupied(mut entry)
                if sequence > entry.get().coord.sequence() =>
            {
                entry.insert(reference.clone());
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    let summary = RetainedVerifiedMergeHistorySummary {
        version: super::store_commit::STORE_PROTOCOL_VERSION,
        store_root_hash: root.store_root_hash,
        policy: crate::WritePolicy::MergeConcurrent,
        causal_cut,
        post_state: StoreDeviceStateRef::merge_concurrent(
            CommitFrontier::MergeConcurrent(post_frontier),
            &state_after,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?,
        membership_floor,
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    };
    summary
        .validate_shape()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let StoreCommitCoord::MergeConcurrent {
        stream_id,
        sequence,
    } = commit_ref.coord
    else {
        return Err(StorePullError::Database(
            "Merge successor carries a Serial coordinate".to_string(),
        ));
    };
    let predecessor_head = summary
        .announcement_frontier
        .get(&stream_id)
        .map(|accepted| accepted.reference.clone());
    let head_slot = match summary.announcement_frontier.get(&stream_id) {
        Some(accepted) => accepted.value.successor.next_slot.clone(),
        None => match &author.store_commits {
            StoreCommitAnchor::MergeConcurrent {
                announcements: DeviceStreamAnchor::StoreAnnouncements { first_slot },
            } if sequence == 1 => first_slot.clone(),
            StoreCommitAnchor::MergeConcurrent { .. } | StoreCommitAnchor::Serial => {
                return Err(StorePullError::Database(
                    "Merge successor has no exact retained announcement predecessor".to_string(),
                ));
            }
        },
    };
    Ok(PreparedMergeHistorySuccessor {
        summary,
        head_slot,
        predecessor_head,
    })
}

pub(crate) async fn prepare_merge_snapshot_history_summary(
    db: &Database,
    root: &StoreRootRef,
    coverage: &CommitFrontier,
    membership: &MembershipChain,
    state: &ResolvedStoreDeviceState,
    author_ref: &super::store_commit::StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    let CommitFrontier::MergeConcurrent(frontier) = coverage else {
        return Err(StorePullError::Database(
            "Merge snapshot history received Serial coverage".to_string(),
        ));
    };
    let predecessors = db
        .retained_merge_history_frontier(frontier.values().cloned().collect())
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if predecessors.len() != frontier.len() {
        return Err(StorePullError::Database(
            "Merge snapshot is missing a retained checkpoint at its coverage frontier".to_string(),
        ));
    }
    compose_merge_snapshot_history_summary(
        root,
        coverage,
        membership,
        state,
        author_ref,
        author,
        predecessors,
    )
}

pub(super) fn compose_merge_snapshot_history_summary(
    root: &StoreRootRef,
    coverage: &CommitFrontier,
    membership: &MembershipChain,
    state: &ResolvedStoreDeviceState,
    author_ref: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    let CommitFrontier::MergeConcurrent(frontier) = coverage else {
        return Err(StorePullError::Database(
            "Merge snapshot history received Serial coverage".to_string(),
        ));
    };
    let MergedRetainedMergeHistory {
        causal_cut,
        mut registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    } = merge_retained_merge_history(root, membership, predecessors)?;
    author_ref
        .verify_registration(author)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    insert_exact(
        &mut registrations,
        author_ref.device_id,
        RetainedVerifiedRegistration {
            reference: author_ref.clone(),
            value: author.clone(),
        },
        "Merge snapshot author registration conflicts with retained authority",
    )?;
    let summary = RetainedVerifiedMergeHistorySummary {
        version: super::store_commit::STORE_PROTOCOL_VERSION,
        store_root_hash: root.store_root_hash,
        policy: crate::WritePolicy::MergeConcurrent,
        causal_cut,
        post_state: StoreDeviceStateRef::merge_concurrent(coverage.clone(), state)
            .map_err(|error| StorePullError::Database(error.to_string()))?,
        membership_floor: super::store_commit::MembershipCausalFloor::from_membership(membership),
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    };
    summary
        .validate_snapshot_baseline()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if summary
        .frontier()
        .map_err(|error| StorePullError::Database(error.to_string()))?
        != *frontier
    {
        return Err(StorePullError::Database(
            "Merge snapshot history does not exactly cover its signed frontier".to_string(),
        ));
    }
    Ok(summary)
}

pub(crate) fn prepare_merge_abandonment_history_summary(
    candidate_summary: &RetainedVerifiedMergeHistorySummary,
    candidate: &StoreBatchCommitRef,
    candidate_value: &StoreBatchCommit,
    abandonment: &StoreBatchCommitRef,
    abandonment_value: &StoreBatchCommit,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    candidate_summary
        .validate_shape()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    candidate
        .verify_commit(candidate_value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    abandonment
        .verify_commit(abandonment_value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if candidate.coord != abandonment.coord
        || candidate_value.order != abandonment_value.order
        || candidate_value.membership_state != abandonment_value.membership_state
        || candidate_value.device_state != abandonment_value.device_state
        || candidate_summary.causal_cut.get(&candidate.coord) != Some(candidate)
        || candidate_summary.membership_proofs.contains_key(candidate)
    {
        return Err(StorePullError::Database(
            "Merge abandonment differs from its retained candidate history".to_string(),
        ));
    }
    let mut summary = candidate_summary.clone();
    summary
        .causal_cut
        .insert(abandonment.coord.clone(), abandonment.clone());
    let frontier = CommitFrontier::MergeConcurrent(
        summary
            .frontier()
            .map_err(|error| StorePullError::Database(error.to_string()))?,
    );
    let StoreDeviceStateRef::MergeConcurrent {
        frontier: post_state_frontier,
        ..
    } = &mut summary.post_state
    else {
        return Err(StorePullError::Database(
            "Merge abandonment retained a Serial post-state".to_string(),
        ));
    };
    *post_state_frontier = frontier;
    summary
        .validate_shape()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    Ok(summary)
}

pub(super) fn verify_merge_history_refs<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> StorePullFuture<'a, VerifiedMergeHistory> {
    let pending = tips.into_iter().collect::<Vec<_>>();
    Box::pin(verify_merge_history_refs_impl(storage, root, pending))
}

async fn verify_merge_history_refs_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    mut pending: Vec<StoreBatchCommitRef>,
) -> Result<VerifiedMergeHistory, StorePullError> {
    let verified_root = Box::pin(load_store_protocol_root(storage, root))
        .await?
        .value;
    if verified_root.descriptor.write_policy != crate::WritePolicy::MergeConcurrent {
        return Err(StorePullError::Database(
            "Merge history belongs to a non-Merge Store".to_string(),
        ));
    }
    let founder = Box::pin(load_founder_registration_with_root(
        storage,
        root,
        &verified_root,
    ))
    .await?;
    let founder_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let genesis = ResolvedStoreDeviceState::founder(
        root,
        founder_ref.clone(),
        &verified_root.descriptor.founder_pubkey,
        verified_root.descriptor.founder_grant.clone(),
        &verified_root.descriptor.founder_recovery,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;

    let mut loaded =
        BTreeMap::<StoreBatchCommitRef, (StoreBatchCommit, StoreDeviceRegistration)>::new();
    while let Some(reference) = pending.pop() {
        if loaded.contains_key(&reference) {
            continue;
        }
        if !matches!(reference.coord, StoreCommitCoord::MergeConcurrent { .. }) {
            return Err(StorePullError::Database(
                "Merge history contains a Serial commit".to_string(),
            ));
        }
        let (commit, author) = Box::pin(load_commit_with_author_at_root(
            storage,
            root,
            &verified_root,
            &reference,
        ))
        .await?;
        pending.extend(commit_predecessor_references(&commit));
        loaded.insert(reference, (commit, author));
    }

    let mut states = BTreeMap::<StoreBatchCommitRef, ResolvedStoreDeviceState>::new();
    let mut verified = BTreeMap::new();
    while !loaded.is_empty() {
        let next = loaded.iter().find_map(|(reference, (commit, _))| {
            commit_predecessor_references(commit)
                .iter()
                .all(|dependency| states.contains_key(dependency))
                .then(|| reference.clone())
        });
        let Some(reference) = next else {
            return Err(StorePullError::Database(
                "Merge history is cyclic or has an unresolved predecessor".to_string(),
            ));
        };
        let (commit, author) = loaded.remove(&reference).ok_or_else(|| {
            StorePullError::Database(
                "selected exclusion-history commit disappeared before verification".to_string(),
            )
        })?;
        let (_, accepted_head) = Box::pin(
            super::store_outbound::exact_next_announcement_slot_for_verified_commit(
                storage,
                root,
                &commit.author_registration,
                &author,
                &reference,
                &commit,
            ),
        )
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        let activation_head_ref = accepted_head.ok_or_else(|| {
            StorePullError::Database(
                "Merge history commit has no accepted announcement head".to_string(),
            )
        })?;
        let predecessor_state = verified_merge_predecessor_state(&genesis, &states, &commit)?;
        let verified_membership_prefix =
            verified_merge_membership_prefix(&verified, commit_predecessor_references(&commit))?;
        let pending_resolution =
            Box::pin(verify_merge_resolution_activation_acceptance_with_history(
                storage, root, &commit, &genesis, &verified,
            ))
            .await?;
        let membership = Box::pin(load_merge_predecessor_membership_with_verified_activations(
            storage,
            root,
            &commit.membership_state,
            &verified_membership_prefix,
            pending_resolution.as_ref(),
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        verified_membership_prefix
            .validate_complete_membership(&membership)
            .map_err(StorePullError::Database)?;
        verify_merge_membership_state_ref(
            &commit.membership_state,
            &membership,
            &predecessor_state,
        )?;
        if !membership_authorizes(Some(&membership), &commit, &author) {
            return Err(StorePullError::Database(
                "Merge history commit lacks exact membership authority".to_string(),
            ));
        }
        let authority = RegistrationPredecessorAuthority::MergeConcurrent(&membership);
        let accepted_predecessor = VerifiedAcceptedPredecessor::MergeHistory {
            commits: &verified,
            frontier: commit_predecessor_references(&commit),
        };
        let registrations = Box::pin(load_commit_registrations_with_root(
            storage,
            root,
            &verified_root,
            &commit,
            &author,
            Some(&authority),
            Some(&accepted_predecessor),
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let (authorized_predecessor, recovery_author) =
            predecessor_with_recovery_author(predecessor_state.clone(), &commit, &registrations)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        if !device_state_has_active_registration(
            &authorized_predecessor,
            &commit.author_registration,
        ) {
            return Err(StorePullError::Database(
                "author exclusion history commit author is inactive at its predecessor".to_string(),
            ));
        }
        let resolver = DeviceStateResolver::Loaded {
            genesis: &genesis,
            states: &states,
        };
        let operations = Box::pin(load_commit_device_operations(
            Some(&resolver),
            storage,
            root,
            &commit,
            &authorized_predecessor,
            Some(&authority),
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let acknowledgement = Box::pin(validate_commit_acknowledgement(
            storage, root, &commit, &author,
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let membership_control =
            if let Some(super::store_commit::StoreControl::MergeMembership { transition }) =
                commit.control()
            {
                let (activations, conflict_resolution) =
                    Box::pin(verify_merge_membership_control_with_history(
                        storage,
                        root,
                        &reference,
                        &commit,
                        &membership,
                        &predecessor_state,
                        &verified,
                        pending_resolution.as_ref(),
                    ))
                    .await
                    .map_err(StorePullError::Database)?;
                Some(VerifiedMergeMembershipControl {
                    activations,
                    head_activation: VerifiedMergeMembershipHeadActivation {
                        commit: reference.clone(),
                        transition: transition.clone(),
                    },
                    conflict_resolution,
                })
            } else {
                None
            };
        let owner_recovery = Box::pin(verify_commit_owner_recovery_activation(
            storage, root, &commit, None,
        ))
        .await?;
        let state = operations
            .apply_to(authorized_predecessor, &commit.device_state)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let state = apply_verified_device_lifecycle(
            state,
            &commit,
            &registrations,
            recovery_author.as_ref(),
            owner_recovery,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        let predecessor_histories = commit_predecessor_references(&commit)
            .iter()
            .map(|predecessor| {
                verified
                    .get(predecessor)
                    .map(|verified: &VerifiedMergeHistoryCommit| verified.history.clone())
                    .ok_or_else(|| {
                        StorePullError::Database(
                            "Merge history summary has an unresolved predecessor".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let membership_closure = Box::pin(verified_merge_membership_objects(
            storage, root, &reference, &commit,
        ))
        .await?;
        let retained_registrations = commit
            .device_registrations()
            .iter()
            .zip(&registrations)
            .map(|(activation, (value, _))| RetainedVerifiedRegistration {
                reference: activation.registration.clone(),
                value: value.clone(),
            })
            .collect();
        let retained_acknowledgement = match acknowledgement.clone() {
            Some((acknowledgement_ref, acknowledgement_value)) => Some(
                retain_activated_acknowledgement(
                    storage,
                    root,
                    &reference,
                    &commit,
                    &author,
                    acknowledgement_ref,
                    acknowledgement_value,
                )
                .await?,
            ),
            None => None,
        };
        let successor = compose_merge_history_successor(
            root,
            &commit,
            &reference,
            &membership,
            &author,
            state.clone(),
            predecessor_histories,
            MergeHistorySuccessorEvidence {
                registrations: retained_registrations,
                acknowledgement: retained_acknowledgement,
                membership_proof: membership_closure.map(|closure| closure.proof),
            },
        )?;
        let activation_head = Box::pin(super::store_objects::load_head_ref(
            storage,
            root.store_root_hash,
            &activation_head_ref,
            &author,
            &reference,
        ))
        .await?;
        let history = successor
            .summary
            .open(
                &commit,
                &reference,
                &activation_head.value,
                &activation_head_ref,
                &state,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        states.insert(reference.clone(), state.clone());
        verified.insert(
            reference,
            VerifiedMergeHistoryCommit {
                commit,
                predecessor_membership: membership,
                predecessor_state,
                state_after: state,
                operations,
                acknowledgement,
                membership_control,
                history,
            },
        );
    }
    Ok(VerifiedMergeHistory {
        genesis,
        commits: verified,
    })
}
