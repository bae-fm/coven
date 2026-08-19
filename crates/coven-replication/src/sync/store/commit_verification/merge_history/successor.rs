use super::*;

pub struct PreparedMergeHistorySuccessor {
    pub(crate) history_evidence: store_commit::RetainedMergeCommitEvidence,
    pub(crate) head_slot: coven_protocol::objects::ObjectSlot,
    pub(crate) predecessor_head: Option<store_commit::StoreDeviceHeadRef>,
}

pub struct MergeHistorySuccessorEvidence {
    pub(crate) registrations: Vec<ReferencedStoreDeviceRegistration>,
    pub(crate) acknowledgement: Option<store_commit::RetainedVerifiedActivatedAck>,
    pub(crate) membership_proof: Option<store_commit::RetainedMergeMembershipProof>,
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
            Err(StorePullError::InvalidState(conflict.to_string()))
        }
    }
}

/// Merge one predecessor summary's acknowledgement chain for a device into the
/// chain being composed. Two chains for one device must agree — one extending
/// the other is a longer view of the same history; anything else is a fork.
pub(crate) fn insert_latest_acknowledgement(
    target: &mut BTreeMap<store_commit::StoreDeviceId, store_commit::RetainedAcknowledgementChain>,
    device_id: store_commit::StoreDeviceId,
    value: store_commit::RetainedAcknowledgementChain,
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
        std::collections::btree_map::Entry::Occupied(_) => Err(StorePullError::InvalidState(
            "Merge predecessor checkpoints contain forked acknowledgement proof chains".to_string(),
        )),
    }
}

/// Fold the one acknowledgement a retained commit activated into the chain being
/// composed for its device.
///
/// The rows in a cut carry the acknowledgements made within it, which is enough
/// to identify each device's latest — but not enough to reach sequence one when
/// the cut starts above it. A summary states contiguity, so the caller completes
/// each chain by walking it from the latest entry this fold found.
pub(crate) fn extend_acknowledgement_chain(
    target: &mut BTreeMap<store_commit::StoreDeviceId, store_commit::RetainedAcknowledgementChain>,
    device_id: store_commit::StoreDeviceId,
    activated: &store_commit::RetainedVerifiedActivatedAck,
    activating_commit_value: &store_commit::StoreBatchCommit,
) -> Result<(), StorePullError> {
    let extended = match target.entry(device_id) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(store_commit::RetainedAcknowledgementChain::activated(
                activated,
                activating_commit_value,
            ));
            true
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            entry.get_mut().extend(activated, activating_commit_value)
        }
    };
    if extended {
        Ok(())
    } else {
        Err(StorePullError::InvalidState(
            "retained acknowledgements fork at one sequence".to_string(),
        ))
    }
}

fn insert_latest_announcement(
    target: &mut BTreeMap<
        protocol_membership::AuthorStreamId,
        store_commit::RetainedAcceptedStoreAnnouncement,
    >,
    stream_id: protocol_membership::AuthorStreamId,
    value: store_commit::RetainedAcceptedStoreAnnouncement,
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
        std::collections::btree_map::Entry::Occupied(_) => Err(StorePullError::InvalidState(
            "Merge predecessor checkpoints contain conflicting announcement heads at one sequence"
                .to_string(),
        )),
    }
}

pub(crate) struct MergedRetainedMergeHistory {
    causal_cut: BTreeMap<StoreCommitCoord, StoreBatchCommitRef>,
    registrations: BTreeMap<store_commit::StoreDeviceId, ReferencedStoreDeviceRegistration>,
    acknowledgements:
        BTreeMap<store_commit::StoreDeviceId, store_commit::RetainedAcknowledgementChain>,
    membership_proofs: BTreeMap<StoreBatchCommitRef, store_commit::RetainedMergeMembershipProof>,
    announcement_frontier: BTreeMap<
        protocol_membership::AuthorStreamId,
        store_commit::RetainedAcceptedStoreAnnouncement,
    >,
}

impl MergedRetainedMergeHistory {
    fn insert_membership_proof(
        &mut self,
        reference: StoreBatchCommitRef,
        value: store_commit::RetainedMergeMembershipProof,
    ) -> Result<(), StorePullError> {
        if self
            .membership_proofs
            .keys()
            .any(|existing| existing.coord == reference.coord && existing != &reference)
        {
            return Err(StorePullError::InvalidState(
                "Merge predecessor checkpoints contain conflicting membership proofs at one Store coordinate"
                    .to_string(),
            ));
        }
        insert_exact(
            &mut self.membership_proofs,
            reference,
            value,
            "Merge predecessor checkpoints disagree on a membership proof",
        )
    }
}

pub(crate) fn merge_retained_merge_history(
    root: &StoreRootRef,
    membership: &MembershipChain,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
) -> Result<MergedRetainedMergeHistory, StorePullError> {
    let mut merged = MergedRetainedMergeHistory {
        causal_cut: BTreeMap::new(),
        registrations: BTreeMap::new(),
        acknowledgements: BTreeMap::new(),
        membership_proofs: BTreeMap::new(),
        announcement_frontier: BTreeMap::new(),
    };
    for predecessor in predecessors {
        let predecessor_cut = predecessor.summary.causal_cut.clone();
        if predecessor.summary.store_root_hash != root.store_root_hash {
            return Err(StorePullError::InvalidState(
                "Merge predecessor checkpoint belongs to another Store".to_string(),
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
            return Err(StorePullError::InvalidState(
                "Merge successor membership omits its retained causal floor".to_string(),
            ));
        }
        for (key, value) in predecessor.summary.causal_cut {
            insert_exact(
                &mut merged.causal_cut,
                key,
                value,
                "Merge predecessor checkpoints disagree on a Store coordinate",
            )?;
        }
        for (key, value) in predecessor.summary.registrations {
            insert_exact(
                &mut merged.registrations,
                key,
                value,
                "Merge predecessor checkpoints disagree on a device registration",
            )?;
        }
        for (key, value) in predecessor.summary.acknowledgements {
            insert_latest_acknowledgement(&mut merged.acknowledgements, key, value)?;
        }
        for (key, mut value) in predecessor.summary.membership_proofs {
            if predecessor_cut.get(&value.commit.coord) == Some(&value.commit)
                && value.announcement.is_none()
            {
                let stream_id = value.commit.coord.stream_id;
                value.announcement = predecessor
                    .announcement_frontier
                    .get(&stream_id)
                    .filter(|announcement| announcement.value.commit == value.commit)
                    .cloned();
            }
            merged.insert_membership_proof(key, value)?;
        }
        for (key, value) in predecessor.announcement_frontier {
            insert_latest_announcement(&mut merged.announcement_frontier, key, value)?;
        }
    }
    Ok(merged)
}

pub(crate) fn compose_merge_snapshot_history_summary(
    root: &StoreRootRef,
    coverage: &CommitFrontier,
    membership: &MembershipChain,
    state: &ResolvedStoreDeviceState,
    author_ref: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    predecessors: Vec<coven_database::RetainedMergeHistoryCheckpoint>,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    let frontier = &coverage.0;
    let snapshot_predecessors = predecessors
        .iter()
        .filter_map(|checkpoint| match checkpoint {
            coven_database::RetainedMergeHistoryCheckpoint::Snapshot(checkpoint) => {
                Some(checkpoint.clone())
            }
            coven_database::RetainedMergeHistoryCheckpoint::Commit(_) => None,
        })
        .collect();
    let mut merged = merge_retained_merge_history(root, membership, snapshot_predecessors)?;
    for checkpoint in predecessors {
        let coven_database::RetainedMergeHistoryCheckpoint::Commit(materialization) = checkpoint
        else {
            continue;
        };
        insert_snapshot_commit(
            &mut merged,
            root,
            materialization.commit_ref(),
            materialization.commit(),
            materialization.verified_commit().author(),
            materialization.registrations(),
            materialization.history_evidence(),
            materialization.activation_head(),
            materialization.activation_head_object(),
        )?;
    }
    let MergedRetainedMergeHistory {
        causal_cut,
        mut registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    } = merged;
    author_ref
        .verify_registration(author)
        .map_err(StorePullError::Protocol)?;
    insert_exact(
        &mut registrations,
        author_ref.device_id,
        ReferencedStoreDeviceRegistration::verified(author_ref.clone(), author.clone())
            .map_err(StorePullError::Protocol)?,
        "Merge snapshot author registration conflicts with retained authority",
    )?;
    let summary = RetainedVerifiedMergeHistorySummary {
        version: store_commit::STORE_PROTOCOL_VERSION,
        store_root_hash: root.store_root_hash,
        causal_cut,
        post_state: StoreDeviceStateRef::from_resolved(coverage.clone(), state)
            .map_err(StorePullError::Protocol)?,
        membership_floor: store_commit::MembershipCausalFloor::from_membership(membership),
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    };
    // Assembled, not yet valid — see
    // `validate_composed_snapshot_history_summary`, which the caller runs once
    // each device's acknowledgement chain has been walked back to sequence one.
    let _ = frontier;
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn insert_snapshot_commit(
    merged: &mut MergedRetainedMergeHistory,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    registrations: &[ActivatedStoreDeviceRegistration],
    evidence: &store_commit::RetainedMergeCommitEvidence,
    activation_head: &StoreDeviceHead,
    activation_head_object: &ExactObjectRef,
) -> Result<(), StorePullError> {
    if commit.store_root_hash != root.store_root_hash {
        return Err(StorePullError::InvalidState(
            "retained Merge commit belongs to another Store".to_string(),
        ));
    }
    insert_exact(
        &mut merged.causal_cut,
        commit_ref.coord.clone(),
        commit_ref.clone(),
        "retained Merge commits disagree on a Store coordinate",
    )?;
    for registration in registrations {
        insert_exact(
            &mut merged.registrations,
            registration.reference().device_id,
            registration.registration().clone(),
            "retained Merge commits disagree on a device registration",
        )?;
    }
    let author = ReferencedStoreDeviceRegistration::verified(
        commit.author_registration.clone(),
        author.clone(),
    )
    .map_err(StorePullError::Protocol)?;
    insert_exact(
        &mut merged.registrations,
        author.reference().device_id,
        author,
        "retained Merge commit author conflicts with retained authority",
    )?;
    if let Some(acknowledgement) = &evidence.acknowledgement {
        let device_id = acknowledgement.acknowledgement().0.registration.device_id;
        extend_acknowledgement_chain(
            &mut merged.acknowledgements,
            device_id,
            acknowledgement,
            commit,
        )?;
    }
    let announcement = store_commit::RetainedAcceptedStoreAnnouncement {
        reference: store_commit::StoreDeviceHeadRef {
            head_hash: activation_head.head_hash(),
            object: activation_head_object.clone(),
        },
        value: activation_head.clone(),
    };
    if let Some(proof) = &evidence.membership_proof {
        let mut proof = proof.clone();
        proof.announcement = Some(announcement.clone());
        merged.insert_membership_proof(commit_ref.clone(), *proof)?;
    }
    insert_latest_announcement(
        &mut merged.announcement_frontier,
        commit_ref.coord.stream_id,
        announcement,
    )
}

pub(crate) fn compose_verified_merge_snapshot_history_summary<'a>(
    root: &StoreRootRef,
    coverage: &CommitFrontier,
    membership: &MembershipChain,
    state: &ResolvedStoreDeviceState,
    author_ref: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    commits: impl IntoIterator<Item = &'a VerifiedMergeHistoryCommit>,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    let mut merged = merge_retained_merge_history(root, membership, Vec::new())?;
    for verified in commits {
        insert_snapshot_commit(
            &mut merged,
            root,
            verified.verified.reference(),
            verified.verified.value(),
            verified.verified.author(),
            &verified.registrations,
            &verified.history_evidence,
            &verified.activation_head,
            &verified.activation_head_object,
        )?;
    }
    let MergedRetainedMergeHistory {
        causal_cut,
        mut registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    } = merged;
    author_ref
        .verify_registration(author)
        .map_err(StorePullError::Protocol)?;
    insert_exact(
        &mut registrations,
        author_ref.device_id,
        ReferencedStoreDeviceRegistration::verified(author_ref.clone(), author.clone())
            .map_err(StorePullError::Protocol)?,
        "Merge snapshot author registration conflicts with retained authority",
    )?;
    let summary = RetainedVerifiedMergeHistorySummary {
        version: store_commit::STORE_PROTOCOL_VERSION,
        store_root_hash: root.store_root_hash,
        causal_cut,
        post_state: StoreDeviceStateRef::from_resolved(coverage.clone(), state)
            .map_err(StorePullError::Protocol)?,
        membership_floor: store_commit::MembershipCausalFloor::from_membership(membership),
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    };
    // Assembled, not yet valid: each device's acknowledgement chain still has to
    // be completed back to sequence one, which needs a walker this function does
    // not have. `validate_composed_snapshot_history_summary` is the other half
    // and runs once the caller has completed them.
    Ok(summary)
}

/// Check a composed snapshot summary once its acknowledgement chains are whole.
pub(crate) fn validate_composed_snapshot_history_summary(
    summary: &RetainedVerifiedMergeHistorySummary,
    coverage: &CommitFrontier,
) -> Result<(), StorePullError> {
    summary
        .validate_snapshot_baseline()
        .map_err(StorePullError::Protocol)?;
    if summary.frontier().map_err(StorePullError::Protocol)? != coverage.0 {
        return Err(StorePullError::InvalidState(
            "Merge snapshot history does not exactly cover its signed frontier".to_string(),
        ));
    }
    Ok(())
}
