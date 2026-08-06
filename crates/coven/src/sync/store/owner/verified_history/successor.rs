use super::*;

pub(crate) struct PreparedMergeHistorySuccessor {
    pub(crate) summary: RetainedVerifiedMergeHistorySummary,
    pub(crate) head_slot: crate::protocol::objects::ObjectSlot,
    pub(crate) predecessor_head: Option<store_commit::StoreDeviceHeadRef>,
}

pub(crate) struct MergeHistorySuccessorEvidence {
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

pub(crate) fn insert_latest_acknowledgement(
    target: &mut BTreeMap<store_commit::StoreDeviceId, store_commit::RetainedVerifiedActivatedAck>,
    device_id: store_commit::StoreDeviceId,
    value: store_commit::RetainedVerifiedActivatedAck,
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
        BTreeMap<store_commit::StoreDeviceId, store_commit::RetainedVerifiedActivatedAck>,
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

pub(crate) fn compose_merge_history_successor(
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    commit_ref: &StoreBatchCommitRef,
    membership: &MembershipChain,
    author: &StoreDeviceRegistration,
    state_after: ResolvedStoreDeviceState,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
    evidence: MergeHistorySuccessorEvidence,
) -> Result<PreparedMergeHistorySuccessor, StorePullError> {
    let mut merged = merge_retained_merge_history(root, membership, predecessors)?;
    let mut membership_floor = store_commit::MembershipCausalFloor::from_membership(membership);
    insert_exact(
        &mut merged.causal_cut,
        commit_ref.coord.clone(),
        commit_ref.clone(),
        "Merge successor conflicts at its Store coordinate",
    )?;
    for registration in evidence.registrations {
        if !commit
            .device_registrations()
            .iter()
            .any(|activation| &activation.registration == registration.reference())
        {
            return Err(StorePullError::InvalidState(
                "Merge history registration is absent from its activating commit".to_string(),
            ));
        }
        insert_exact(
            &mut merged.registrations,
            registration.reference().device_id,
            registration,
            "Merge successor registration conflicts with retained authority",
        )?;
    }
    if let Some(retained) = evidence.acknowledgement {
        let (reference, _) = retained.latest().ok_or_else(|| {
            StorePullError::InvalidState(
                "Merge history acknowledgement proof chain is empty".to_string(),
            )
        })?;
        if commit.acknowledgement() != Some(reference)
            || retained.activating_commit != *commit_ref
            || retained.activating_commit_value != *commit
        {
            return Err(StorePullError::InvalidState(
                "Merge history acknowledgement differs from its activating commit".to_string(),
            ));
        }
        insert_latest_acknowledgement(
            &mut merged.acknowledgements,
            reference.registration.device_id,
            retained,
        )?;
    }
    if let Some(proof) = evidence.membership_proof {
        if proof.commit != *commit_ref {
            return Err(StorePullError::InvalidState(
                "Merge membership proof names another activating commit".to_string(),
            ));
        }
        membership_floor
            .advance(
                proof.entry.coord.clone(),
                &proof.head_value.body.resolutions,
            )
            .map_err(StorePullError::Protocol)?;
        merged.insert_membership_proof(commit_ref.clone(), proof)?;
    }
    let author_ref = commit.author_registration.clone();
    author_ref
        .verify_registration(author)
        .map_err(StorePullError::Protocol)?;
    insert_exact(
        &mut merged.registrations,
        author_ref.device_id,
        ReferencedStoreDeviceRegistration::verified(author_ref.clone(), author.clone())
            .map_err(StorePullError::Protocol)?,
        "Merge successor author registration conflicts with retained authority",
    )?;
    let mut post_frontier = BTreeMap::new();
    for reference in merged.causal_cut.values() {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = reference.coord;
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
    let MergedRetainedMergeHistory {
        causal_cut,
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    } = merged;
    let summary = RetainedVerifiedMergeHistorySummary {
        version: store_commit::STORE_PROTOCOL_VERSION,
        store_root_hash: root.store_root_hash,
        causal_cut,
        post_state: StoreDeviceStateRef::from_resolved(CommitFrontier(post_frontier), &state_after)
            .map_err(StorePullError::Protocol)?,
        membership_floor,
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    };
    summary.validate_shape().map_err(StorePullError::Protocol)?;
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = commit_ref.coord;
    let predecessor_head = summary
        .announcement_frontier
        .get(&stream_id)
        .map(|accepted| accepted.reference.clone());
    let head_slot = match summary.announcement_frontier.get(&stream_id) {
        Some(accepted) => accepted.value.successor.next_slot.clone(),
        None => match &author.store_commits {
            DeviceStreamAnchor::StoreAnnouncements { first_slot } if sequence == 1 => {
                first_slot.clone()
            }
            _ => {
                return Err(StorePullError::InvalidState(
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

pub(crate) fn compose_merge_snapshot_history_summary(
    root: &StoreRootRef,
    coverage: &CommitFrontier,
    membership: &MembershipChain,
    state: &ResolvedStoreDeviceState,
    author_ref: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    let frontier = &coverage.0;
    let MergedRetainedMergeHistory {
        causal_cut,
        mut registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    } = merge_retained_merge_history(root, membership, predecessors)?;
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
    summary
        .validate_snapshot_baseline()
        .map_err(StorePullError::Protocol)?;
    if summary.frontier().map_err(StorePullError::Protocol)? != *frontier {
        return Err(StorePullError::InvalidState(
            "Merge snapshot history does not exactly cover its signed frontier".to_string(),
        ));
    }
    Ok(summary)
}

pub(crate) fn prepare_merge_abandonment_history_summary(
    candidate_summary: &RetainedVerifiedMergeHistorySummary,
    candidate: &VerifiedStoreBatchCommit,
    abandonment: &VerifiedStoreBatchCommit,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    candidate_summary
        .validate_shape()
        .map_err(StorePullError::Protocol)?;
    let candidate_value = candidate.value();
    let candidate = candidate.reference();
    let abandonment_value = abandonment.value();
    let abandonment = abandonment.reference();
    if candidate_summary.store_root_hash != candidate_value.store_root_hash
        || candidate_summary.store_root_hash != abandonment_value.store_root_hash
    {
        return Err(StorePullError::InvalidState(
            "Merge abandonment history belongs to another Store root".to_string(),
        ));
    }
    if candidate.coord != abandonment.coord
        || candidate_value.order != abandonment_value.order
        || candidate_value.membership_state != abandonment_value.membership_state
        || candidate_value.device_state != abandonment_value.device_state
        || candidate_summary.causal_cut.get(&candidate.coord) != Some(candidate)
        || candidate_summary.membership_proofs.contains_key(candidate)
    {
        return Err(StorePullError::InvalidState(
            "Merge abandonment differs from its retained candidate history".to_string(),
        ));
    }
    let mut summary = candidate_summary.clone();
    summary
        .causal_cut
        .insert(abandonment.coord.clone(), abandonment.clone());
    let frontier = CommitFrontier(summary.frontier().map_err(StorePullError::Protocol)?);
    summary.post_state = candidate_summary
        .post_state
        .with_frontier(frontier)
        .map_err(StorePullError::Protocol)?;
    summary.validate_shape().map_err(StorePullError::Protocol)?;
    Ok(summary)
}
