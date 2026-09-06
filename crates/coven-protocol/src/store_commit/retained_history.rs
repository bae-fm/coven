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

/// The acknowledgement one commit activated, retained beside that commit.
///
/// One acknowledgement, not the chain behind it. A retained row describes its
/// own commit, and an acknowledgement's predecessors are described by the rows
/// that retained *them* — each acknowledgement names its predecessor's object,
/// so contiguity follows from the rows in the same way a commit's ancestry
/// follows from the commits, without every row carrying a copy of everything
/// before it.
///
/// Storing the chain here instead made a retained row grow with the history in
/// front of it: on a two-device store where nearly every commit acknowledges,
/// the row at sequence N held N acknowledgements, so the table grew with the
/// square of the history. A field store reached 223 MB over 385 rows, and both
/// applying a commit and reading the retained rows back paid for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedVerifiedActivatedAck {
    pub acknowledgement: (StoreAckRef, StoreAck),
    pub activating_commit: StoreBatchCommitRef,
}

/// A device's acknowledgement chain, contiguous from sequence one, carried by a
/// snapshot's portable summary.
///
/// This is the one place the whole chain belongs. A device restoring from a
/// snapshot has no retained rows to walk, so the summary has to state the
/// contiguity itself; it is folded once per snapshot generation from the rows
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
/// [`AcknowledgedStoreSnapshot`], and only reclaim needs it. A device installing
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
    pub snapshot_cut: StoreHistoryCut,
    pub accepted_cut: StoreHistoryCut,
    pub device_state: ResolvedStoreDeviceState,
    #[serde(with = "ordered_map_entries")]
    pub active_registrations: BTreeMap<StoreDeviceId, ReferencedStoreDeviceRegistration>,
}

/// One snapshot every device active at its cut has acknowledged.
///
/// This is the unanimity proof, and it answers only one question: may history
/// behind this snapshot be deleted? It may, because every device that could
/// still need that history has said in a signed acknowledgement — activated by
/// a commit in the verified closure — that it holds this snapshot.
///
/// Installing a snapshot asks a different question and does not need this. A
/// device joining or restoring wants a signed, owner-authored, history-
/// consistent image; whether some other device has caught up has no bearing on
/// that, and a device that is behind converges through an ordinary pull no
/// matter which image the joiner installed. Requiring unanimity there made a
/// store with one joined-and-idle device fall back to its generation-zero
/// image forever.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcknowledgedStoreSnapshot {
    pub authority: RetainedReplaySnapshotAuthority,
    /// One chain per device that had to acknowledge, which is a subset of the
    /// devices active at the coverage: those still active now. Which subset is
    /// a question about the current device state, so it is decided by the
    /// builder against verified history and recorded here — `validate` can
    /// check that these devices were active at the coverage and that each chain
    /// proves what it claims, but not that the set is the right one to have
    /// asked. See the reclaim module for why the set is what it is.
    #[serde(with = "ordered_map_entries")]
    pub acknowledgements: BTreeMap<StoreDeviceId, RetainedAcknowledgementChain>,
}

/// The evidence required to retire local replay inputs behind one snapshot.
///
/// Cloud reclaim asks whether the devices active at the snapshot have made the
/// exact snapshot promise. Local retirement asks a stronger and different
/// question: whether every writer active now has crossed that cut, including a
/// writer activated after the snapshot. Keeping the proofs separate prevents
/// the local ordering rule from changing which cloud objects may be reclaimed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayBaselineRetirementProof {
    pub authority: RetainedReplaySnapshotAuthority,
    pub current_cut: StoreHistoryCut,
    pub current_state: StoreDeviceStateRef,
    pub current_device_state: ResolvedStoreDeviceState,
    pub current_membership: StoreMembershipStateRef,
    pub membership_witness: ReplayRetirementMembershipWitness,
    #[serde(with = "ordered_map_entries")]
    pub current_registrations: BTreeMap<StoreDeviceId, ReferencedStoreDeviceRegistration>,
    #[serde(with = "ordered_map_entries")]
    pub acknowledgements: BTreeMap<StoreDeviceId, RetainedAcknowledgementChain>,
}

/// Accepted Store history that names the exact membership used to decide which
/// writers must acknowledge a replay cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplayRetirementMembershipWitness {
    Snapshot,
    StoreCommit(StoreBatchCommitRef),
}

impl RetainedReplaySnapshotAuthority {
    pub fn validate(&self) -> Result<(), StoreProtocolError> {
        let metadata_bytes = self.metadata.to_bytes();
        let author = self
            .active_registrations
            .get(&self.metadata.author_registration.device_id)
            .filter(|registration| registration.reference() == &self.metadata.author_registration)
            .ok_or_else(|| {
                StoreProtocolError::Malformed(
                    "retained snapshot author is absent from its active registrations".to_string(),
                )
            })?;
        let parsed = SnapshotMeta::parse_at(
            &metadata_bytes,
            self.store_root.store_root_hash,
            &self.snapshot,
            author.value(),
        )?;
        if self.metadata.store_root_hash != self.store_root.store_root_hash
            || self.metadata.generation != self.snapshot.generation
            || self.metadata.snapshot_hash() != self.snapshot.snapshot_hash
            || self.snapshot.object.verify(&metadata_bytes).is_err()
            || self.snapshot_cut.frontier() != self.metadata.coverage
            || !self
                .accepted_cut
                .frontier()
                .covers(&self.snapshot_cut.frontier())
            || parsed != self.metadata
            || self.device_state.state_hash != self.metadata.state.devices.state_hash()
            || self.device_state.recovery != self.metadata.state.devices.recovery()
        {
            return Err(StoreProtocolError::Malformed(
                "retained snapshot replay authority differs from its signed snapshot state"
                    .to_string(),
            ));
        }
        let expected_active = self
            .device_state
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

impl AcknowledgedStoreSnapshot {
    /// The latest acknowledgement each active device signed for this snapshot,
    /// in a stable order. This is the evidence a reclaim claim carries: the
    /// devices are named by what they signed, not by the chains behind it.
    pub fn acknowledgement_refs(&self) -> Result<Vec<StoreAckRef>, StoreProtocolError> {
        let mut references = self
            .acknowledgements
            .values()
            .map(|acknowledgement| {
                acknowledgement
                    .latest()
                    .map(|(reference, _)| reference.clone())
                    .ok_or_else(|| {
                        StoreProtocolError::Malformed(
                            "acknowledged snapshot proof chain is empty".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        references.sort();
        Ok(references)
    }

    /// The installable authority, plus proof that every device active at its cut
    /// acknowledged this exact snapshot. Reclaim deletes history behind a
    /// snapshot only against this.
    pub fn validate(&self) -> Result<(), StoreProtocolError> {
        self.authority.validate()?;
        if self.acknowledgements.is_empty() {
            return Err(StoreProtocolError::Malformed(
                "acknowledged snapshot has no acknowledgements".to_string(),
            ));
        }
        for (device_id, acknowledgement) in &self.acknowledgements {
            let registration = self
                .authority
                .active_registrations
                .get(device_id)
                .ok_or_else(|| {
                    StoreProtocolError::Malformed(
                        "acknowledged snapshot names a device that was not active at its coverage"
                            .to_string(),
                    )
                })?;
            let acknowledgement_value = validate_acknowledgement_activation(
                &self.authority.store_root,
                &self.authority.accepted_cut,
                registration,
                acknowledgement,
            )?;
            if !acknowledgement_value
                .snapshot
                .as_ref()
                .is_some_and(|acknowledged| {
                    acknowledged.author_registration == self.authority.metadata.author_registration
                        && acknowledged.snapshot == self.authority.snapshot
                })
                || acknowledgement_value.device_state != self.authority.metadata.state.devices
                || !acknowledgement_value
                    .store_cut
                    .frontier()
                    .covers(&self.authority.metadata.coverage)
            {
                return Err(StoreProtocolError::Malformed(
                    "retained snapshot acknowledgement differs from its activated commit"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl ReplayBaselineRetirementProof {
    pub fn validate(
        &self,
        membership: &crate::membership::MembershipChain,
    ) -> Result<BTreeSet<StoreDeviceId>, StoreProtocolError> {
        self.authority.validate()?;
        let crate::membership::MembershipStatus::Resolved(resolved_membership) =
            membership.status()
        else {
            return Err(StoreProtocolError::Malformed(
                "replay baseline retirement membership is conflicted".to_string(),
            ));
        };
        let expected_membership = StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            self.current_device_state.recovery.clone(),
            resolved_membership.state_hash,
        )?;
        let required_writer_ids = replay_retirement_writer_ids(
            self.authority.store_root.store_root_hash,
            &self.current_device_state,
            &self.current_registrations,
            membership,
        )?;
        let membership_is_witnessed = match &self.membership_witness {
            ReplayRetirementMembershipWitness::Snapshot => {
                self.current_membership == self.authority.metadata.state.membership
            }
            ReplayRetirementMembershipWitness::StoreCommit(reference) => {
                self.current_cut.frontier().covers_commit(reference)
            }
        };
        if required_writer_ids.is_empty()
            || self.current_membership != expected_membership
            || !membership_is_witnessed
            || self.acknowledgements.len() != required_writer_ids.len()
            || !self
                .current_cut
                .frontier()
                .covers(&self.authority.accepted_cut.frontier())
            || StoreDeviceStateRef::from_resolved(
                self.current_cut.frontier(),
                &self.current_device_state,
            )? != self.current_state
        {
            return Err(StoreProtocolError::Malformed(
                "replay baseline retirement has inconsistent current authority".to_string(),
            ));
        }
        for device_id in &required_writer_ids {
            let registration = self
                .current_registrations
                .get(device_id)
                .expect("current writer derivation validates registration coverage");
            let acknowledgement = self.acknowledgements.get(device_id).ok_or_else(|| {
                StoreProtocolError::Malformed(
                    "replay baseline retirement omits a required writer".to_string(),
                )
            })?;
            let acknowledgement_value = validate_acknowledgement_activation(
                &self.authority.store_root,
                &self.current_cut,
                registration,
                acknowledgement,
            )?;
            if !acknowledgement_value
                .store_cut
                .frontier()
                .covers(&self.authority.metadata.coverage)
            {
                return Err(StoreProtocolError::Malformed(
                    "replay baseline retirement acknowledgement does not cross its cut".to_string(),
                ));
            }
        }
        Ok(required_writer_ids)
    }
}

pub fn replay_retirement_writer_ids(
    store_root_hash: ObjectHash,
    current_device_state: &ResolvedStoreDeviceState,
    current_registrations: &BTreeMap<StoreDeviceId, ReferencedStoreDeviceRegistration>,
    membership: &crate::membership::MembershipChain,
) -> Result<BTreeSet<StoreDeviceId>, StoreProtocolError> {
    current_device_state.validate_canonical()?;
    if current_registrations.len() != current_device_state.devices.len() {
        return Err(StoreProtocolError::Malformed(
            "replay baseline retirement registrations do not exactly cover current devices"
                .to_string(),
        ));
    }
    let mut writers = BTreeSet::new();
    for (device_id, record) in &current_device_state.devices {
        let registration = current_registrations
            .get(device_id)
            .filter(|registration| registration.reference() == &record.registration)
            .ok_or_else(|| {
                StoreProtocolError::Malformed(
                    "replay baseline retirement registration differs from current device state"
                        .to_string(),
                )
            })?;
        let bytes = registration.value().to_bytes();
        registration.reference().object.verify(&bytes)?;
        let parsed = StoreDeviceRegistration::parse_at(
            &bytes,
            &registration.value().store_root,
            *device_id,
        )?;
        if parsed != *registration.value()
            || registration.value().store_root.store_root_hash != store_root_hash
        {
            return Err(StoreProtocolError::Malformed(
                "replay baseline retirement registration is not canonical".to_string(),
            ));
        }
        if matches!(record.status, StoreDeviceStatus::Active)
            && membership.is_member_now(&registration.value().author_pubkey)
        {
            writers.insert(*device_id);
        }
    }
    Ok(writers)
}

fn validate_acknowledgement_activation<'a>(
    root: &StoreRootRef,
    cut: &StoreHistoryCut,
    registration: &ReferencedStoreDeviceRegistration,
    acknowledgement: &'a RetainedAcknowledgementChain,
) -> Result<&'a StoreAck, StoreProtocolError> {
    acknowledgement.validate_chain(root, registration)?;
    let (acknowledgement_ref, acknowledgement_value) =
        acknowledgement.latest().ok_or_else(|| {
            StoreProtocolError::Malformed(
                "retained snapshot acknowledgement proof chain is empty".to_string(),
            )
        })?;
    let commit_bytes = acknowledgement.activating_commit_value.to_bytes();
    acknowledgement
        .activating_commit
        .object
        .verify(&commit_bytes)?;
    let parsed_commit = VerifiedStoreBatchCommit::parse(
        &commit_bytes,
        root.store_root_hash,
        &acknowledgement.activating_commit,
        registration.value(),
    )?;
    if parsed_commit.value() != &acknowledgement.activating_commit_value
        || parsed_commit.commit_hash() != acknowledgement.activating_commit.commit_hash
        || parsed_commit.acknowledgement() != Some(acknowledgement_ref)
        || !history_cut_covers_commit(cut, &acknowledgement.activating_commit)
    {
        return Err(StoreProtocolError::Malformed(
            "retained snapshot acknowledgement differs from its activated commit".to_string(),
        ));
    }
    Ok(acknowledgement_value)
}

fn history_cut_covers_commit(cut: &StoreHistoryCut, reference: &StoreBatchCommitRef) -> bool {
    let covered = CommitFrontier(BTreeMap::from([(
        reference.coord.stream_id,
        reference.clone(),
    )]));
    cut.frontier().covers(&covered)
}

impl RetainedVerifiedActivatedAck {
    pub fn acknowledgement(&self) -> &(StoreAckRef, StoreAck) {
        &self.acknowledgement
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
        let (reference, value) = activated.acknowledgement.clone();
        Self {
            chain: BTreeMap::from([(reference.sequence, (reference, value))]),
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
        let (reference, value) = &activated.acknowledgement;
        match self.chain.get(&reference.sequence) {
            Some(existing) if existing == &activated.acknowledgement => {}
            Some(_) => return false,
            None => {
                self.chain
                    .insert(reference.sequence, (reference.clone(), value.clone()));
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
                    crate::membership::MembershipHeadActivation::StoreCommit { commit }
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
                    reference.object.verify(&serde_json::to_vec(value)?)?;
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
    pub acknowledgements: BTreeMap<StoreDeviceId, RetainedAcknowledgementChain>,
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
                    crate::membership::MembershipHeadActivation::StoreCommit { commit }
                        if commit == &proof.commit
                )
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            proof
                .head
                .object
                .verify(&serde_json::to_vec(&proof.head_value)?)?;
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
                    reference.object.verify(&serde_json::to_vec(value)?)?;
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
            .verify(&announcement.value.to_bytes())?;
        StoreDeviceHead::parse_at(
            &announcement.value.to_bytes(),
            self.store_root_hash,
            registration.value(),
            &announcement.value.commit,
        )?;
        Ok(())
    }
}
