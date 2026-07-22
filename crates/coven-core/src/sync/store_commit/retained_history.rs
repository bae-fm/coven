use super::validation::require_version;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipCausalFloor {
    pub effective_coordinates: Vec<MembershipCoord>,
    pub resolutions: Vec<StoreMembershipConflictResolutionRef>,
}

impl MembershipCausalFloor {
    pub fn from_membership(membership: &crate::sync::membership::MembershipChain) -> Self {
        Self {
            effective_coordinates: membership.effective_frontier(),
            resolutions: membership.resolution_refs().to_vec(),
        }
    }

    pub(crate) fn advance(
        &mut self,
        coordinate: crate::sync::membership::MembershipCoord,
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
pub struct RetainedVerifiedRegistration {
    pub reference: StoreDeviceRegistrationRef,
    pub value: StoreDeviceRegistration,
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

    pub(crate) fn validate_chain(
        &self,
        root: &StoreRootRef,
        registration: &RetainedVerifiedRegistration,
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
                || reference.registration != registration.reference
                || value.registration != registration.reference
                || value.successor.predecessor.as_ref()
                    != predecessor.map(|reference| &reference.object)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            reference
                .object
                .verify(&value.to_bytes())
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            StoreAck::parse_at(&value.to_bytes(), root, reference, &registration.value)?;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedVerifiedMergeHistorySummary {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub policy: WritePolicy,
    #[serde(with = "ordered_map_entries")]
    pub causal_cut: BTreeMap<StoreCommitCoord, StoreBatchCommitRef>,
    pub post_state: StoreDeviceStateRef,
    pub membership_floor: MembershipCausalFloor,
    #[serde(with = "ordered_map_entries")]
    pub registrations: BTreeMap<StoreDeviceId, RetainedVerifiedRegistration>,
    #[serde(with = "ordered_map_entries")]
    pub acknowledgements: BTreeMap<StoreDeviceId, RetainedVerifiedActivatedAck>,
    #[serde(with = "ordered_map_entries")]
    pub membership_proofs: BTreeMap<StoreBatchCommitRef, RetainedMergeMembershipProof>,
    #[serde(with = "ordered_map_entries")]
    pub announcement_frontier: BTreeMap<AuthorStreamId, RetainedAcceptedStoreAnnouncement>,
}

#[derive(Debug, Clone)]
pub(crate) struct OpenedRetainedMergeHistorySummary {
    pub(crate) summary: RetainedVerifiedMergeHistorySummary,
    pub(crate) announcement_frontier: BTreeMap<AuthorStreamId, RetainedAcceptedStoreAnnouncement>,
    pub(crate) post_state: ResolvedStoreDeviceState,
}

impl RetainedVerifiedMergeHistorySummary {
    pub fn digest(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(MERGE_HISTORY_SUMMARY_DOMAIN, self))
    }

    pub fn frontier(
        &self,
    ) -> Result<BTreeMap<AuthorStreamId, StoreBatchCommitRef>, StoreProtocolError> {
        let mut frontier = BTreeMap::new();
        for reference in self.causal_cut.values() {
            let StoreCommitCoord::MergeConcurrent {
                stream_id,
                sequence,
            } = reference.coord
            else {
                return Err(StoreProtocolError::WritePolicyMismatch {
                    expected: WritePolicy::MergeConcurrent,
                    actual: WritePolicy::Serial,
                });
            };
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
        if self.policy != WritePolicy::MergeConcurrent
            || self.post_state.write_policy() != WritePolicy::MergeConcurrent
        {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: self.policy,
            });
        }
        self.membership_floor.validate()?;
        for (coord, reference) in &self.causal_cut {
            if coord != &reference.coord
                || !matches!(coord, StoreCommitCoord::MergeConcurrent { .. })
            {
                return Err(StoreProtocolError::Malformed(
                    "Merge history causal cut contains a mismatched coordinate".to_string(),
                ));
            }
        }
        let expected_frontier = CommitFrontier::MergeConcurrent(self.frontier()?);
        let StoreDeviceStateRef::MergeConcurrent { frontier, .. } = &self.post_state else {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            });
        };
        if frontier != &expected_frontier {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        for (device_id, registration) in &self.registrations {
            if device_id != &registration.reference.device_id
                || registration.value.store_root.store_root_hash != self.store_root_hash
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            registration
                .reference
                .verify_registration(&registration.value)?;
            registration
                .reference
                .object
                .verify(&registration.value.to_bytes())
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            StoreDeviceRegistration::parse_at(
                &registration.value.to_bytes(),
                &registration.value.store_root,
                *device_id,
            )?;
        }
        for (device_id, acknowledgement) in &self.acknowledgements {
            let registration = self
                .registrations
                .get(device_id)
                .ok_or(StoreProtocolError::DeviceStateMismatch)?;
            acknowledgement.validate_chain(&registration.value.store_root, registration)?;
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
                    != registration.reference
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
            let Some(StoreControl::MergeMembership { transition }) = proof.commit_value.control()
            else {
                return Err(StoreProtocolError::DeviceStateMismatch);
            };
            if transition.body.entry != proof.entry
                || proof.entry.coord != proof.entry_value.coord()
                || !crate::sync::membership::verify_membership_entry(&proof.entry_value)
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
                || !proof.head_value.verify(&head_author.value)
                || !matches!(
                    &proof.head_value.activation,
                    crate::sync::membership::MembershipHeadActivation::StoreCommit { commit }
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
                    crate::sync::membership::MembershipChange::ResolutionActivation { resolution },
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
                (crate::sync::membership::MembershipChange::ResolutionActivation { .. }, _, _)
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
            if !matches!(
                announcement.value.commit.coord,
                StoreCommitCoord::MergeConcurrent {
                    stream_id: announcement_stream,
                    ..
                } if announcement_stream == *stream_id
            ) || self.causal_cut.get(&announcement.value.commit.coord)
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
            || announcement.value.author_registration != registration.reference
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
            &registration.value,
            &announcement.value.commit,
        )?;
        Ok(())
    }

    pub(crate) fn open(
        &self,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        head: &StoreDeviceHead,
        head_ref: &StoreDeviceHeadRef,
        state: &ResolvedStoreDeviceState,
    ) -> Result<OpenedRetainedMergeHistorySummary, StoreProtocolError> {
        self.validate_shape()?;
        state.validate_canonical()?;
        commit_ref.verify_commit(commit)?;
        if self.store_root_hash != commit.store_root_hash
            || self.digest() != head.history_summary
            || head.commit != *commit_ref
            || head.head_hash() != head_ref.head_hash
            || !self.causal_cut.contains_key(&commit_ref.coord)
            || self.causal_cut.get(&commit_ref.coord) != Some(commit_ref)
            || self.post_state.state_hash() != state.state_hash
            || self.post_state.recovery() != state.recovery
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        head_ref
            .object
            .verify(&head.to_bytes())
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        let registration = self
            .registrations
            .get(&commit.author_registration.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if registration.reference != commit.author_registration
            || registration.value.store_root.store_root_hash != self.store_root_hash
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        registration
            .reference
            .verify_registration(&registration.value)?;
        StoreDeviceHead::parse_at(
            &head.to_bytes(),
            self.store_root_hash,
            &registration.value,
            commit_ref,
        )?;
        let StoreCommitCoord::MergeConcurrent {
            stream_id,
            sequence,
        } = commit_ref.coord
        else {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            });
        };
        let frontier = self.frontier()?;
        for (accepted_stream, accepted_commit) in &frontier {
            if *accepted_stream == stream_id {
                if accepted_commit != commit_ref {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
                continue;
            }
            if self
                .announcement_frontier
                .get(accepted_stream)
                .map(|announcement| &announcement.value.commit)
                != Some(accepted_commit)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
        let current_membership_floor_matches = match commit.control() {
            Some(StoreControl::MergeMembership { .. }) => {
                self.membership_proofs.get(commit_ref).is_some_and(|proof| {
                    self.membership_floor
                        .effective_coordinates
                        .contains(&proof.entry.coord)
                        && proof.head_value.body.resolutions.iter().all(|resolution| {
                            self.membership_floor
                                .resolutions
                                .binary_search(resolution)
                                .is_ok()
                        })
                })
            }
            _ => true,
        };
        if !current_membership_floor_matches
            || self
                .membership_proofs
                .iter()
                .any(|(reference, proof)| proof.announcement.is_none() && reference != commit_ref)
            || matches!(commit.control(), Some(StoreControl::MergeMembership { .. }))
                != self.membership_proofs.contains_key(commit_ref)
            || commit.acknowledgement().is_some_and(|reference| {
                self.acknowledgements
                    .get(&reference.registration.device_id)
                    .is_none_or(|acknowledgement| {
                        acknowledgement
                            .latest()
                            .is_none_or(|(retained, _)| retained != reference)
                            || acknowledgement.activating_commit != *commit_ref
                    })
            })
            || commit.device_registrations().iter().any(|activation| {
                self.registrations
                    .get(&activation.registration.device_id)
                    .is_none_or(|registration| registration.reference != activation.registration)
            })
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let predecessor = self.announcement_frontier.get(&stream_id);
        let first_slot = match &registration.value.store_commits {
            StoreCommitAnchor::MergeConcurrent {
                announcements: DeviceStreamAnchor::StoreAnnouncements { first_slot },
            } => first_slot,
            StoreCommitAnchor::MergeConcurrent { .. } | StoreCommitAnchor::Serial => {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        };
        if predecessor.is_none() && (sequence != 1 || head_ref.object.slot() != first_slot)
            || predecessor
                .as_ref()
                .map(|accepted| accepted.value.commit.coord.sequence())
                .is_some_and(|previous| previous.checked_add(1) != Some(sequence))
            || head.successor.predecessor
                != predecessor.map(|accepted| accepted.reference.object.clone())
            || predecessor.is_some_and(|accepted| {
                accepted.value.successor.next_slot != *head_ref.object.slot()
            })
        {
            return Err(StoreProtocolError::Malformed(
                "Merge history head does not exactly extend its retained announcement frontier"
                    .to_string(),
            ));
        }
        let mut announcement_frontier = self.announcement_frontier.clone();
        announcement_frontier.insert(
            stream_id,
            RetainedAcceptedStoreAnnouncement {
                reference: head_ref.clone(),
                value: head.clone(),
            },
        );
        Ok(OpenedRetainedMergeHistorySummary {
            summary: self.clone(),
            announcement_frontier,
            post_state: state.clone(),
        })
    }
}
