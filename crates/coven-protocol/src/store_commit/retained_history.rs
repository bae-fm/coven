use super::validation::require_version;
use super::*;

#[path = "pending_device_join.rs"]
mod pending_device_join;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipCausalFloor {
    pub effective_coordinates: Vec<MembershipCoord>,
}

impl MembershipCausalFloor {
    pub fn from_membership(membership: &crate::membership::MembershipChain) -> Self {
        Self {
            effective_coordinates: membership.effective_frontier(),
        }
    }

    pub fn advance(
        &mut self,
        coordinate: crate::membership::MembershipCoord,
    ) -> Result<(), StoreProtocolError> {
        let stream = coordinate.stream_key();
        self.effective_coordinates
            .retain(|current| current.stream_key() != stream);
        self.effective_coordinates.push(coordinate);
        self.effective_coordinates.sort();
        self.validate()
    }

    pub fn is_included_in(&self, membership: &crate::membership::MembershipChain) -> bool {
        self.effective_coordinates
            .iter()
            .all(|coordinate| membership.effectively_contains_coord(coordinate))
    }

    fn validate(&self) -> Result<(), StoreProtocolError> {
        if self
            .effective_coordinates
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(StoreProtocolError::Malformed(
                "Merge history membership floor is not canonical".to_string(),
            ));
        }
        Ok(())
    }
}

/// The acknowledgement one commit activated, together with any uploaded
/// predecessors whose activation candidates this operation retired. Previously
/// activated acknowledgements remain owned by their own retained commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedVerifiedActivatedAck {
    pub acknowledgement: (StoreAckRef, StoreAck),
    pub activating_commit: StoreBatchCommitRef,
    pub predecessors: Vec<(StoreAckRef, StoreAck)>,
}

/// A device's acknowledgement chain, contiguous from sequence one, carried by a
/// snapshot's portable summary.
///
/// This is the one place the whole chain belongs. A device restoring from a
/// snapshot has no retained rows to walk, so the summary has to state the
/// contiguity itself; it is folded once per snapshot from the rows
/// the snapshot covers, rather than rebuilt into every row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedAcknowledgementChain {
    #[serde(with = "ordered_map_entries")]
    pub chain: BTreeMap<u64, (StoreAckRef, StoreAck)>,
    pub activating_commit: StoreBatchCommitRef,
    pub activating_commit_value: StoreBatchCommit,
}

/// Everything a device needs to install one snapshot as its starting state and
/// verify what arrives after it: the Store root and founder it belongs to, the
/// signed metadata, the cut it covers, and the device state and registrations
/// active at that cut.
///
/// Every field is re-derived from the signed `metadata` by
/// [`validate`](Self::validate), so an installing device trusts the owner's
/// signature over the snapshot and nothing local. What is deliberately absent
/// is any claim about the *other* devices having caught up: that is
/// a separate access concern. A device installing
/// a baseline verifies each later commit against the registrations and device
/// state carried here, exactly as a device that never installed a snapshot
/// verifies them against its own history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedReplaySnapshotAuthority {
    pub store_root: StoreRootRef,
    pub founder_registration: StoreDeviceRegistrationRef,
    pub snapshot: StoreSnapshotRef,
    pub metadata: SnapshotMeta,
    #[serde(with = "ordered_map_entries")]
    pub active_registrations: BTreeMap<StoreDeviceId, ReferencedStoreDeviceRegistration>,
}

impl RetainedReplaySnapshotAuthority {
    pub fn validate(&self) -> Result<(), StoreProtocolError> {
        self.metadata.state.devices.validate_canonical()?;
        let author = self
            .active_registrations
            .get(&self.metadata.author_registration.device_id)
            .filter(|registration| registration.reference() == &self.metadata.author_registration)
            .ok_or_else(|| {
                StoreProtocolError::Malformed(
                    "retained snapshot author is absent from its active registrations".to_string(),
                )
            })?;
        self.metadata.verify_at(
            self.store_root.store_root_hash,
            &self.snapshot,
            author.value(),
        )?;
        let expected_active = self
            .metadata
            .state
            .devices
            .devices
            .iter()
            .filter_map(|(device_id, record)| {
                matches!(record.status, StoreDeviceStatus::Active)
                    .then_some((*device_id, &record.registration))
            })
            .collect::<BTreeMap<_, _>>();
        if expected_active.len() != self.active_registrations.len()
            || expected_active.iter().any(|(device_id, reference)| {
                self.active_registrations
                    .get(device_id)
                    .is_none_or(|registration| registration.reference() != *reference)
            })
        {
            return Err(StoreProtocolError::Malformed(
                "retained snapshot replay authority does not exactly cover active devices"
                    .to_string(),
            ));
        }
        for (device_id, registration) in &self.active_registrations {
            let bytes = registration.value().to_bytes();
            registration.reference().object.verify(&bytes)?;
            let parsed = StoreDeviceRegistration::parse_at(&bytes, &self.store_root, *device_id)?;
            if &parsed != registration.value() {
                return Err(StoreProtocolError::Malformed(
                    "retained snapshot registration is not canonical".to_string(),
                ));
            }
            registration
                .reference()
                .verify_registration(registration.value())?;
        }
        Ok(())
    }
}

impl RetainedVerifiedActivatedAck {
    pub fn acknowledgement(&self) -> &(StoreAckRef, StoreAck) {
        &self.acknowledgement
    }

    pub fn proof_objects(&self) -> impl Iterator<Item = &(StoreAckRef, StoreAck)> {
        self.predecessors
            .iter()
            .chain(std::iter::once(&self.acknowledgement))
    }

    pub fn validate_predecessors(&self) -> Result<(), StoreProtocolError> {
        let mut successor = &self.acknowledgement;
        for predecessor in self.predecessors.iter().rev() {
            let (reference, value) = predecessor;
            reference.object.verify(&value.to_bytes())?;
            if reference.registration != self.acknowledgement.0.registration
                || reference.registration != value.registration
                || reference.sequence != value.sequence
                || reference.ack_hash != value.ack_hash()
                || reference.sequence.checked_add(1) != Some(successor.0.sequence)
                || successor.1.successor.predecessor.as_ref() != Some(&reference.object)
                || successor.0.object.slot() != &value.successor.next_slot
                || value.successor.activation != successor.1.successor.activation
                || value.store_root_hash != successor.1.store_root_hash
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            successor = predecessor;
        }
        Ok(())
    }
}

impl RetainedAcknowledgementChain {
    /// Start a chain from the one acknowledgement a commit activated. Contiguity
    /// is not claimed yet: [`extend`](Self::extend) adds the rest, and
    /// [`validate_chain`](Self::validate_chain) is what asserts the result runs
    /// from sequence one.
    pub fn activated(
        activated: &RetainedVerifiedActivatedAck,
        activating_commit_value: &StoreBatchCommit,
    ) -> Self {
        Self {
            chain: activated
                .proof_objects()
                .map(|proof| (proof.0.sequence, proof.clone()))
                .collect(),
            activating_commit: activated.activating_commit.clone(),
            activating_commit_value: activating_commit_value.clone(),
        }
    }

    /// Fold one more retained acknowledgement in. A sequence already present
    /// must carry the same acknowledgement — two different ones at one sequence
    /// is a forked chain, not a longer one. The activating commit tracks the
    /// highest sequence, which is the one the summary reports.
    pub fn extend(
        &mut self,
        activated: &RetainedVerifiedActivatedAck,
        activating_commit_value: &StoreBatchCommit,
    ) -> bool {
        let (reference, _) = &activated.acknowledgement;
        for proof in activated.proof_objects() {
            match self.chain.get(&proof.0.sequence) {
                Some(existing) if existing == proof => {}
                Some(_) => return false,
                None => {
                    self.chain.insert(proof.0.sequence, proof.clone());
                }
            }
        }
        if self
            .latest()
            .is_some_and(|(latest, _)| latest.sequence == reference.sequence)
        {
            self.activating_commit = activated.activating_commit.clone();
            self.activating_commit_value = activating_commit_value.clone();
        }
        true
    }

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
            reference.object.verify(&value.to_bytes())?;
            StoreAck::parse_at(&value.to_bytes(), root, reference, registration.value())?;
            predecessor = Some(reference);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMergeMembershipProof {
    pub commit: StoreBatchCommitRef,
    pub commit_value: StoreBatchCommit,
    pub entry: MembershipEntryRef,
    pub entry_value: MembershipEntry,
    pub head: MembershipHeadRef,
    pub head_value: AuthorHead,
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
            acknowledgement.validate_predecessors()?;
            let (reference, _) = acknowledgement.acknowledgement();
            if acknowledgement.activating_commit != *commit_ref
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
                    crate::membership::MembershipHeadActivation::StoreCommit { commit, .. }
                        if commit == commit_ref
                )
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            proof
                .entry
                .object
                .verify(&serde_json::to_vec(&proof.entry_value)?)?;
            proof
                .head
                .object
                .verify(&serde_json::to_vec(&proof.head_value)?)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedVerifiedMergeHistorySummary {
    pub reclaim: RetainedReclaimState,
    pub version: u32,
    pub store_root_hash: ObjectHash,
    #[serde(with = "ordered_map_entries")]
    pub causal_cut: BTreeMap<StoreCommitCoord, StoreBatchCommitRef>,
    /// Latest meaningful publication in each author stream at this snapshot's
    /// frontier. Canonical composition derives these exact references from
    /// verified commits; they classify acknowledgement-only advances without
    /// retaining those commits or authenticating arbitrary historical cuts.
    #[serde(with = "ordered_map_entries")]
    pub last_non_acknowledgement_commits: BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
    pub post_state: StoreDeviceStateRef,
    pub membership_floor: MembershipCausalFloor,
    #[serde(with = "ordered_map_entries")]
    pub registrations: BTreeMap<StoreDeviceId, ReferencedStoreDeviceRegistration>,
    #[serde(with = "ordered_map_entries")]
    pub acknowledgements: BTreeMap<StoreDeviceId, RetainedAcknowledgementChain>,
    #[serde(with = "ordered_map_entries")]
    pub membership_proofs: BTreeMap<StoreBatchCommitRef, RetainedMergeMembershipProof>,
    #[serde(with = "ordered_map_entries")]
    pub pending_owner_promotions: BTreeMap<OwnerPromotionId, RetainedOwnerPromotionRequest>,
    #[serde(with = "ordered_map_entries")]
    pub pending_device_joins:
        BTreeMap<StoreBatchCommitRef, device_join_exchange::DeviceJoinBootstrapClosure>,
}

#[derive(Debug, Clone)]
pub struct OpenedRetainedMergeHistorySummary {
    pub summary: RetainedVerifiedMergeHistorySummary,
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
        self.reclaim.validate()?;
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
        for (stream, reference) in &self.last_non_acknowledgement_commits {
            reference.coord.validate()?;
            if stream != &reference.coord.stream_id
                || !self.post_state.frontier().covers_commit(reference)
                || self
                    .causal_cut
                    .get(&reference.coord)
                    .is_some_and(|covered| covered != reference)
            {
                return Err(StoreProtocolError::Malformed(
                    "Merge acknowledgement summary differs from its exact author frontier".into(),
                ));
            }
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
                .verify(&registration.value().to_bytes())?;
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
        for (id, proof) in &self.pending_owner_promotions {
            proof.validate_shape()?;
            let request = proof.request()?;
            let author = self
                .registrations
                .get(&request.promoter_registration.device_id)
                .filter(|author| author.reference() == &request.promoter_registration)
                .ok_or(StoreProtocolError::OwnerPromotionMismatch)?;
            if id != &request.promotion_id
                || request.store_root_hash != self.store_root_hash
                || !self
                    .post_state
                    .frontier()
                    .covers_commit(&proof.publication.value.commit)
            {
                return Err(StoreProtocolError::OwnerPromotionMismatch);
            }
            proof
                .publication
                .value
                .verify_for(&proof.commit, author.value())?;
        }
        for (activation, closure) in &self.pending_device_joins {
            closure.accepted_commit(activation)?;
            let opening = closure.verified_commit(activation)?;
            if closure.publication.current.store_root_hash != self.store_root_hash
                || !self.post_state.frontier().covers_commit(activation)
                || !opening
                    .device_join_attempt_decisions()
                    .iter()
                    .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(_)))
                || closure.publication.current.latest_snapshot().is_none()
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
                .verify(&serde_json::to_vec(&proof.entry_value)?)?;
            let head_author = self
                .registrations
                .get(&proof.head_value.body.author_registration.device_id)
                .ok_or(StoreProtocolError::DeviceStateMismatch)?;
            if !transition.matches_head(&proof.head_value, &proof.head)
                || !proof.head_value.verify(head_author.value())
                || !matches!(
                    &proof.head_value.activation,
                    crate::membership::MembershipHeadActivation::StoreCommit { commit, .. }
                        if commit == &proof.commit
                )
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            proof
                .head
                .object
                .verify(&serde_json::to_vec(&proof.head_value)?)?;
        }
        Ok(())
    }

    pub fn validate_snapshot_baseline(&self) -> Result<(), StoreProtocolError> {
        self.validate_shape()
    }
}
