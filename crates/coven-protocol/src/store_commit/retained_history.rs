use super::validation::require_version;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipCausalFloor {
    pub effective_coordinates: Vec<MembershipCoord>,
    pub resolutions: Vec<StoreMembershipConflictResolutionRef>,
}

impl MembershipCausalFloor {
    pub fn from_membership(membership: &crate::membership::MembershipChain) -> Self {
        Self {
            effective_coordinates: membership.effective_frontier(),
            resolutions: membership.resolution_refs().to_vec(),
        }
    }

    pub fn advance(
        &mut self,
        coordinate: crate::membership::MembershipCoord,
        resolutions: &[StoreMembershipConflictResolutionRef],
    ) -> Result<(), StoreProtocolError> {
        let stream = coordinate.stream_key();
        self.effective_coordinates
            .retain(|current| current.stream_key() != stream);
        self.effective_coordinates.push(coordinate);
        self.effective_coordinates.sort();
        self.resolutions.extend_from_slice(resolutions);
        self.resolutions.sort();
        self.resolutions.dedup();
        self.validate()
    }

    pub fn is_included_in(&self, membership: &crate::membership::MembershipChain) -> bool {
        self.effective_coordinates
            .iter()
            .all(|coordinate| membership.effectively_contains_coord(coordinate))
            && self.resolutions.iter().all(|reference| {
                membership
                    .resolution_refs()
                    .binary_search(reference)
                    .is_ok()
            })
    }

    fn validate(&self) -> Result<(), StoreProtocolError> {
        if self
            .effective_coordinates
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
            || self.resolutions.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(StoreProtocolError::Malformed(
                "Merge history membership floor is not canonical".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedVerifiedActivatedAck {
    #[serde(with = "ordered_map_entries")]
    pub chain: BTreeMap<u64, (StoreAckRef, StoreAck)>,
    pub activating_commit: StoreBatchCommitRef,
    pub activating_commit_value: StoreBatchCommit,
}

impl RetainedVerifiedActivatedAck {
    pub fn latest(&self) -> Option<&(StoreAckRef, StoreAck)> {
        self.chain
            .last_key_value()
            .map(|(_, acknowledgement)| acknowledgement)
    }

    pub fn exactly_extends(&self, predecessor: &Self) -> bool {
        self.chain.len() > predecessor.chain.len()
            && predecessor.chain.iter().all(|(sequence, acknowledgement)| {
                self.chain.get(sequence) == Some(acknowledgement)
            })
    }

    pub fn validate_chain(
        &self,
        root: &StoreRootRef,
        registration: &ReferencedStoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        if self.chain.is_empty() {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let mut predecessor: Option<&StoreAckRef> = None;
        for (expected_sequence, (sequence, (reference, value))) in (1_u64..).zip(self.chain.iter())
        {
            if *sequence != expected_sequence
                || reference.sequence != expected_sequence
                || value.sequence != expected_sequence
                || reference.registration != *registration.reference()
                || value.registration != *registration.reference()
                || value.successor.predecessor.as_ref()
                    != predecessor.map(|reference| &reference.object)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            reference
                .object
                .verify(&value.to_bytes())
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            StoreAck::parse_at(&value.to_bytes(), root, reference, registration.value())?;
            predecessor = Some(reference);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedAcceptedStoreAnnouncement {
    pub reference: StoreDeviceHeadRef,
    pub value: StoreDeviceHead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMergeMembershipProof {
    pub commit: StoreBatchCommitRef,
    pub commit_value: StoreBatchCommit,
    pub announcement: Option<RetainedAcceptedStoreAnnouncement>,
    pub entry: MembershipEntryRef,
    pub entry_value: MembershipEntry,
    pub head: MembershipHeadRef,
    pub head_value: AuthorHead,
    pub resolution: Option<StoreMembershipConflictResolutionRef>,
    pub resolution_value: Option<StoreMembershipConflictResolution>,
}

/// The proof values introduced by one verified Merge commit and retained with
/// that commit after its remote authority objects can be reclaimed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMergeCommitEvidence {
    pub acknowledgement: Option<Box<RetainedVerifiedActivatedAck>>,
    pub membership_proof: Option<Box<RetainedMergeMembershipProof>>,
}

impl RetainedMergeCommitEvidence {
    pub fn none() -> Self {
        Self {
            acknowledgement: None,
            membership_proof: None,
        }
    }

    pub fn validate_for(
        &self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
    ) -> Result<(), StoreProtocolError> {
        commit_ref.verify_commit(commit)?;
        if commit.acknowledgement().is_some() != self.acknowledgement.is_some()
            || commit.control().is_some() != self.membership_proof.is_some()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        if let Some(acknowledgement) = &self.acknowledgement {
            let (reference, _) = acknowledgement
                .latest()
                .ok_or(StoreProtocolError::DeviceStateMismatch)?;
            acknowledgement
                .activating_commit
                .verify_commit(&acknowledgement.activating_commit_value)?;
            if acknowledgement.activating_commit != *commit_ref
                || acknowledgement.activating_commit_value != *commit
                || commit.acknowledgement() != Some(reference)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
        if let Some(proof) = &self.membership_proof {
            if proof.commit != *commit_ref || proof.commit_value != *commit {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            let control = commit
                .control()
                .ok_or(StoreProtocolError::DeviceStateMismatch)?;
            if control.transition.body.entry != proof.entry
                || proof.entry.coord != proof.entry_value.coord()
                || !crate::membership::verify_membership_entry(&proof.entry_value)
                || !control
                    .transition
                    .matches_head(&proof.head_value, &proof.head)
                || !matches!(
                    &proof.head_value.activation,
                    crate::membership::MembershipHeadActivation::StoreCommit { commit }
                        if commit == commit_ref
                )
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            proof
                .entry
                .object
                .verify(
                    &serde_json::to_vec(&proof.entry_value)
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?,
                )
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            proof
                .head
                .object
                .verify(
                    &serde_json::to_vec(&proof.head_value)
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?,
                )
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            match (
                &proof.entry_value.change,
                &proof.resolution,
                &proof.resolution_value,
            ) {
                (
                    crate::membership::MembershipChange::ResolutionActivation { resolution },
                    Some(reference),
                    Some(value),
                ) if resolution == reference
                    && value.store_root_hash == commit.store_root_hash
                    && value.resolution_ref(reference.object.clone()) == *reference
                    && value.verify_signature() =>
                {
                    reference
                        .object
                        .verify(
                            &serde_json::to_vec(value).map_err(|error| {
                                StoreProtocolError::Malformed(error.to_string())
                            })?,
                        )
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                }
                (crate::membership::MembershipChange::ResolutionActivation { .. }, _, _)
                | (_, Some(_), _)
                | (_, _, Some(_)) => return Err(StoreProtocolError::DeviceStateMismatch),
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedVerifiedMergeHistorySummary {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    #[serde(with = "ordered_map_entries")]
    pub causal_cut: BTreeMap<StoreCommitCoord, StoreBatchCommitRef>,
    pub post_state: StoreDeviceStateRef,
    pub membership_floor: MembershipCausalFloor,
    #[serde(with = "ordered_map_entries")]
    pub registrations: BTreeMap<StoreDeviceId, ReferencedStoreDeviceRegistration>,
    #[serde(with = "ordered_map_entries")]
    pub acknowledgements: BTreeMap<StoreDeviceId, RetainedVerifiedActivatedAck>,
    #[serde(with = "ordered_map_entries")]
    pub membership_proofs: BTreeMap<StoreBatchCommitRef, RetainedMergeMembershipProof>,
    #[serde(with = "ordered_map_entries")]
    pub announcement_frontier: BTreeMap<AuthorStreamId, RetainedAcceptedStoreAnnouncement>,
}

#[derive(Debug, Clone)]
pub struct OpenedRetainedMergeHistorySummary {
    pub summary: RetainedVerifiedMergeHistorySummary,
    pub announcement_frontier: BTreeMap<AuthorStreamId, RetainedAcceptedStoreAnnouncement>,
    pub post_state: ResolvedStoreDeviceState,
}

impl RetainedVerifiedMergeHistorySummary {
    pub fn frontier(
        &self,
    ) -> Result<BTreeMap<AuthorStreamId, StoreBatchCommitRef>, StoreProtocolError> {
        let mut frontier = BTreeMap::new();
        for reference in self.causal_cut.values() {
            let stream_id = reference.coord.stream_id;
            let sequence = reference.coord.sequence;
            match frontier.entry(stream_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(reference.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if sequence > entry.get().coord.sequence() {
                        entry.insert(reference.clone());
                    }
                }
            }
        }
        Ok(frontier)
    }

    pub fn validate_shape(&self) -> Result<(), StoreProtocolError> {
        require_version(self.version)?;
        self.membership_floor.validate()?;
        for (coord, reference) in &self.causal_cut {
            if coord != &reference.coord {
                return Err(StoreProtocolError::Malformed(
                    "Merge history causal cut contains a mismatched coordinate".to_string(),
                ));
            }
        }
        let expected_frontier = CommitFrontier(self.frontier()?);
        if self.post_state.frontier() != &expected_frontier {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        for (device_id, registration) in &self.registrations {
            if device_id != &registration.reference().device_id
                || registration.value().store_root.store_root_hash != self.store_root_hash
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            registration
                .reference()
                .verify_registration(registration.value())?;
            registration
                .reference()
                .object
                .verify(&registration.value().to_bytes())
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            StoreDeviceRegistration::parse_at(
                &registration.value().to_bytes(),
                &registration.value().store_root,
                *device_id,
            )?;
        }
        for (device_id, acknowledgement) in &self.acknowledgements {
            let registration = self
                .registrations
                .get(device_id)
                .ok_or(StoreProtocolError::DeviceStateMismatch)?;
            acknowledgement.validate_chain(&registration.value().store_root, registration)?;
            let (acknowledgement_ref, acknowledgement_value) = acknowledgement
                .latest()
                .ok_or(StoreProtocolError::DeviceStateMismatch)?;
            acknowledgement
                .activating_commit
                .verify_commit(&acknowledgement.activating_commit_value)?;
            if device_id != &acknowledgement_ref.registration.device_id
                || acknowledgement.activating_commit_value.acknowledgement()
                    != Some(acknowledgement_ref)
                || acknowledgement.activating_commit_value.author_registration
                    != *registration.reference()
                || self
                    .causal_cut
                    .get(&acknowledgement.activating_commit.coord)
                    != Some(&acknowledgement.activating_commit)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            let predecessor_cut = acknowledgement
                .activating_commit_value
                .order
                .predecessor_cut()?;
            if acknowledgement_value.store_cut != predecessor_cut
                || acknowledgement_value.device_state
                    != acknowledgement.activating_commit_value.device_state
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
        for (reference, proof) in &self.membership_proofs {
            if reference != &proof.commit
                || self.causal_cut.get(&proof.commit.coord) != Some(&proof.commit)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            proof.commit.verify_commit(&proof.commit_value)?;
            let Some(control) = proof.commit_value.control() else {
                return Err(StoreProtocolError::DeviceStateMismatch);
            };
            let transition = &control.transition;
            if transition.body.entry != proof.entry
                || proof.entry.coord != proof.entry_value.coord()
                || !crate::membership::verify_membership_entry(&proof.entry_value)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            proof
                .entry
                .object
                .verify(
                    &serde_json::to_vec(&proof.entry_value)
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?,
                )
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            let head_author = self
                .registrations
                .get(&proof.head_value.body.author_registration.device_id)
                .ok_or(StoreProtocolError::DeviceStateMismatch)?;
            if !transition.matches_head(&proof.head_value, &proof.head)
                || !proof.head_value.verify(head_author.value())
                || !matches!(
                    &proof.head_value.activation,
                    crate::membership::MembershipHeadActivation::StoreCommit { commit }
                        if commit == &proof.commit
                )
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            proof
                .head
                .object
                .verify(
                    &serde_json::to_vec(&proof.head_value)
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?,
                )
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            match (
                &proof.entry_value.change,
                &proof.resolution,
                &proof.resolution_value,
            ) {
                (
                    crate::membership::MembershipChange::ResolutionActivation { resolution },
                    Some(reference),
                    Some(value),
                ) if resolution == reference
                    && value.store_root_hash == self.store_root_hash
                    && value.resolution_ref(reference.object.clone()) == *reference
                    && value.verify_signature() =>
                {
                    reference
                        .object
                        .verify(
                            &serde_json::to_vec(value).map_err(|error| {
                                StoreProtocolError::Malformed(error.to_string())
                            })?,
                        )
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                }
                (crate::membership::MembershipChange::ResolutionActivation { .. }, _, _)
                | (_, Some(_), _)
                | (_, _, Some(_)) => return Err(StoreProtocolError::DeviceStateMismatch),
                _ => {}
            }
            if let Some(announcement) = &proof.announcement {
                self.validate_announcement(announcement)?;
                if announcement.value.commit != proof.commit {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
            }
        }
        for (stream_id, announcement) in &self.announcement_frontier {
            self.validate_announcement(announcement)?;
            if announcement.value.commit.coord.stream_id != *stream_id
                || self.causal_cut.get(&announcement.value.commit.coord)
                    != Some(&announcement.value.commit)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
        Ok(())
    }

    pub fn validate_snapshot_baseline(&self) -> Result<(), StoreProtocolError> {
        self.validate_shape()?;
        let frontier = self.frontier()?;
        if self.announcement_frontier.len() != frontier.len()
            || frontier.iter().any(|(stream_id, commit)| {
                self.announcement_frontier
                    .get(stream_id)
                    .is_none_or(|announcement| announcement.value.commit != *commit)
            })
            || self
                .membership_proofs
                .values()
                .any(|proof| proof.announcement.is_none())
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(())
    }

    fn validate_announcement(
        &self,
        announcement: &RetainedAcceptedStoreAnnouncement,
    ) -> Result<(), StoreProtocolError> {
        let registration = self
            .registrations
            .get(&announcement.value.author_registration.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if announcement.value.store_root_hash != self.store_root_hash
            || announcement.value.author_registration != *registration.reference()
            || announcement.reference.head_hash != announcement.value.head_hash()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        announcement
            .reference
            .object
            .verify(&announcement.value.to_bytes())
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        StoreDeviceHead::parse_at(
            &announcement.value.to_bytes(),
            self.store_root_hash,
            registration.value(),
            &announcement.value.commit,
        )?;
        Ok(())
    }
}
