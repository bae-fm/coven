use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceProposalState {
    Pending {
        proposal: StoreDeviceExclusionProposalRef,
    },
    Cancelled {
        outcome: StoreDeviceExclusionCancellationRef,
    },
    Superseded {
        proposal: StoreDeviceExclusionProposalRef,
        terminals: Vec<StoreDeviceExclusionRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedStoreDeviceState {
    pub devices: BTreeMap<StoreDeviceId, StoreDeviceRecord>,
    pub recovery: Vec<OwnerRecoveryCursor>,
    pub state_hash: ObjectHash,
}

impl ResolvedStoreDeviceState {
    pub fn validate_canonical(&self) -> Result<(), StoreProtocolError> {
        let canonical = Self::from_parts(self.devices.clone(), self.recovery.clone())?;
        if canonical != *self {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(())
    }

    pub fn founder(
        root: &StoreRootRef,
        founder_registration: StoreDeviceRegistrationRef,
        founder_pubkey: &str,
        founder_grant: MembershipGrantId,
        founder_recovery: &GrantStreamAnchor,
    ) -> Result<Self, StoreProtocolError> {
        let cursor = OwnerRecoveryCursor {
            owner_grant: founder_grant.clone(),
            position: OwnerRecoveryPosition::BeforeFirst {
                activation: OwnerRecoveryActivationId::derive(
                    root,
                    founder_pubkey,
                    &founder_grant,
                    founder_recovery,
                )?,
            },
        };
        let devices = BTreeMap::from([(
            founder_registration.device_id,
            StoreDeviceRecord {
                registration: founder_registration,
                proposals: BTreeMap::new(),
                status: StoreDeviceStatus::Active,
            },
        )]);
        Self::from_parts(devices, vec![cursor])
    }

    pub fn activate_registration(
        &self,
        registration: StoreDeviceRegistrationRef,
        recovery: Option<OwnerRecoveryCursor>,
    ) -> Result<Self, StoreProtocolError> {
        if self.devices.contains_key(&registration.device_id) {
            return Err(StoreProtocolError::DuplicateDeviceRegistration {
                device_id: registration.device_id.to_string(),
            });
        }
        let mut devices = self.devices.clone();
        devices.insert(
            registration.device_id,
            StoreDeviceRecord {
                registration,
                proposals: BTreeMap::new(),
                status: StoreDeviceStatus::Active,
            },
        );
        let mut cursors = self.recovery.clone();
        if let Some(cursor) = recovery {
            if let Some(existing) = cursors
                .iter_mut()
                .find(|existing| existing.owner_grant == cursor.owner_grant)
            {
                *existing = cursor;
            } else {
                cursors.push(cursor);
            }
        }
        Self::from_parts(devices, cursors)
    }

    pub fn activate_owner_recovery(
        &self,
        owner_grant: MembershipGrantId,
        activation: OwnerRecoveryActivationId,
    ) -> Result<Self, StoreProtocolError> {
        if self
            .recovery
            .iter()
            .any(|cursor| cursor.owner_grant == owner_grant)
        {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        let mut recovery = self.recovery.clone();
        recovery.push(OwnerRecoveryCursor {
            owner_grant,
            position: OwnerRecoveryPosition::BeforeFirst { activation },
        });
        Self::from_parts(self.devices.clone(), recovery)
    }

    pub fn preactivate_recovery_author(
        mut self,
        commit: &StoreBatchCommit,
        registrations: &[ActivatedStoreDeviceRegistration],
    ) -> Result<(Self, Option<StoreDeviceRegistrationRef>), StoreProtocolError> {
        if commit.device_registrations().len() != registrations.len() {
            return Err(StoreProtocolError::Malformed(
                "verified registrations do not cover every activation".to_string(),
            ));
        }
        for (activated, registration) in commit.device_registrations().iter().zip(registrations) {
            registration.verify_reference(activated)?;
            if activated.registration == commit.author_registration {
                if let Some(cursor) = registration.recovery_cursor()? {
                    self =
                        self.activate_registration(activated.registration.clone(), Some(cursor))?;
                    return Ok((self, Some(activated.registration.clone())));
                }
            }
        }
        Ok((self, None))
    }

    pub fn apply_verified_lifecycle(
        mut self,
        commit: &StoreBatchCommit,
        registrations: &[ActivatedStoreDeviceRegistration],
        preactivated: Option<&StoreDeviceRegistrationRef>,
        owner_recovery: Option<(MembershipGrantId, OwnerRecoveryActivationId)>,
    ) -> Result<Self, StoreProtocolError> {
        if commit.device_registrations().len() != registrations.len() {
            return Err(StoreProtocolError::Malformed(
                "verified registrations do not cover every activation".to_string(),
            ));
        }
        for (activated, registration) in commit.device_registrations().iter().zip(registrations) {
            registration.verify_reference(activated)?;
            if preactivated != Some(&activated.registration) {
                self = self.activate_registration(
                    activated.registration.clone(),
                    registration.recovery_cursor()?,
                )?;
            }
        }
        if let Some((grant_id, activation)) = owner_recovery {
            self = self.activate_owner_recovery(grant_id, activation)?;
        }
        Ok(self)
    }

    pub fn propose_exclusion(
        &self,
        reference: StoreDeviceExclusionProposalRef,
        proposal: &StoreDeviceExclusionProposal,
        predecessor_ref: &StoreDeviceStateRef,
    ) -> Result<Self, StoreProtocolError> {
        reference.verify_proposal(proposal)?;
        if &proposal.frozen_device_state != predecessor_ref
            || predecessor_ref.state_hash() != self.state_hash
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let mut devices = self.devices.clone();
        let record = devices
            .get_mut(&reference.target.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if record.registration != reference.target
            || !matches!(record.status, StoreDeviceStatus::Active)
            || record.proposals.contains_key(&reference.proposal_id)
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        record.proposals.insert(
            reference.proposal_id,
            StoreDeviceProposalState::Pending {
                proposal: reference,
            },
        );
        Self::from_parts(devices, self.recovery.clone())
    }

    pub fn cancel_exclusion(
        &self,
        cancellation: StoreDeviceExclusionCancellationRef,
    ) -> Result<Self, StoreProtocolError> {
        let mut devices = self.devices.clone();
        let record = devices
            .get_mut(&cancellation.proposal.target.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        let state = record
            .proposals
            .get_mut(&cancellation.proposal.proposal_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if !matches!(state, StoreDeviceProposalState::Pending { proposal } if proposal == &cancellation.proposal)
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        *state = StoreDeviceProposalState::Cancelled {
            outcome: cancellation,
        };
        Self::from_parts(devices, self.recovery.clone())
    }

    pub fn exclude(
        &self,
        exclusion: StoreDeviceExclusionRef,
        accepted_cut: StoreHistoryCut,
    ) -> Result<Self, StoreProtocolError> {
        validate_store_history_cut(&accepted_cut)?;
        let mut devices = self.devices.clone();
        let record = devices
            .get_mut(&exclusion.proposal.target.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if record.registration != exclusion.proposal.target
            || !matches!(record.status, StoreDeviceStatus::Active)
            || !matches!(
                record.proposals.get(&exclusion.proposal.proposal_id),
                Some(StoreDeviceProposalState::Pending { proposal }) if proposal == &exclusion.proposal
            )
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let terminals = vec![exclusion];
        supersede_pending_proposals(&mut record.proposals, &terminals);
        record.status = StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        };
        Self::from_parts(devices, self.recovery.clone())
    }

    pub fn merge(states: impl IntoIterator<Item = Self>) -> Result<Self, StoreProtocolError> {
        let mut devices = BTreeMap::new();
        let mut recovery = BTreeMap::<MembershipGrantId, OwnerRecoveryPosition>::new();
        for state in states {
            for (device_id, record) in state.devices {
                match devices.entry(device_id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(record);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        if entry.get().registration != record.registration {
                            return Err(StoreProtocolError::DeviceStateMismatch);
                        }
                        let merged_status =
                            merge_device_status(entry.get().status.clone(), record.status)?;
                        let mut merged_proposals = merge_device_proposals(
                            entry.get().proposals.clone(),
                            record.proposals,
                        )?;
                        if let StoreDeviceStatus::Inactive { terminals, .. } = &merged_status {
                            supersede_pending_proposals(&mut merged_proposals, terminals);
                        }
                        entry.get_mut().status = merged_status;
                        entry.get_mut().proposals = merged_proposals;
                    }
                }
            }
            for cursor in state.recovery {
                match recovery.entry(cursor.owner_grant) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(cursor.position);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        // Stream heads on either side of a recovery commit
                        // name different positions on the grant's one chain;
                        // the merged state stands at the furthest.
                        let merged = entry.get().merge(&cursor.position)?;
                        entry.insert(merged);
                    }
                }
            }
        }
        Self::from_parts(
            devices,
            recovery
                .into_iter()
                .map(|(owner_grant, position)| OwnerRecoveryCursor {
                    owner_grant,
                    position,
                })
                .collect(),
        )
    }

    fn from_parts(
        devices: BTreeMap<StoreDeviceId, StoreDeviceRecord>,
        mut recovery: Vec<OwnerRecoveryCursor>,
    ) -> Result<Self, StoreProtocolError> {
        recovery.sort();
        validate_recovery_cursors(&recovery)?;
        validate_store_device_records(&devices)?;
        let state_hash = ObjectHash::digest(&domain_json(
            b"coven.store-device-state.v1\0",
            &(&devices, &recovery),
        ));
        Ok(Self {
            devices,
            recovery,
            state_hash,
        })
    }
}

fn supersede_pending_proposals(
    proposals: &mut BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
    terminals: &[StoreDeviceExclusionRef],
) {
    for state in proposals.values_mut() {
        if let StoreDeviceProposalState::Pending { proposal } = state {
            *state = StoreDeviceProposalState::Superseded {
                proposal: proposal.clone(),
                terminals: terminals.to_vec(),
            };
        }
    }
}

fn merge_device_proposals(
    mut left: BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
    right: BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
) -> Result<BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>, StoreProtocolError>
{
    for (proposal_id, right_state) in right {
        match left.entry(proposal_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(right_state);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let merged = merge_device_proposal_state(entry.get().clone(), right_state)?;
                entry.insert(merged);
            }
        }
    }
    Ok(left)
}

fn merge_device_proposal_state(
    left: StoreDeviceProposalState,
    right: StoreDeviceProposalState,
) -> Result<StoreDeviceProposalState, StoreProtocolError> {
    let left_proposal = match &left {
        StoreDeviceProposalState::Pending { proposal }
        | StoreDeviceProposalState::Superseded { proposal, .. } => proposal,
        StoreDeviceProposalState::Cancelled { outcome } => &outcome.proposal,
    };
    let right_proposal = match &right {
        StoreDeviceProposalState::Pending { proposal }
        | StoreDeviceProposalState::Superseded { proposal, .. } => proposal,
        StoreDeviceProposalState::Cancelled { outcome } => &outcome.proposal,
    };
    if left_proposal != right_proposal {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    match (left, right) {
        (
            StoreDeviceProposalState::Pending { proposal },
            StoreDeviceProposalState::Pending { .. },
        ) => Ok(StoreDeviceProposalState::Pending { proposal }),
        (
            StoreDeviceProposalState::Cancelled { outcome },
            StoreDeviceProposalState::Cancelled { outcome: other },
        ) => {
            if outcome != other {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            Ok(StoreDeviceProposalState::Cancelled { outcome })
        }
        (StoreDeviceProposalState::Cancelled { outcome }, _)
        | (_, StoreDeviceProposalState::Cancelled { outcome }) => {
            Ok(StoreDeviceProposalState::Cancelled { outcome })
        }
        (
            StoreDeviceProposalState::Superseded {
                proposal,
                terminals: left,
            },
            StoreDeviceProposalState::Superseded {
                terminals: right, ..
            },
        ) => Ok(StoreDeviceProposalState::Superseded {
            proposal,
            terminals: merge_terminal_refs(left, right)?,
        }),
        (
            StoreDeviceProposalState::Superseded {
                proposal,
                terminals,
            },
            _,
        )
        | (
            _,
            StoreDeviceProposalState::Superseded {
                proposal,
                terminals,
            },
        ) => Ok(StoreDeviceProposalState::Superseded {
            proposal,
            terminals,
        }),
    }
}

pub(crate) fn merge_device_status(
    left: StoreDeviceStatus,
    right: StoreDeviceStatus,
) -> Result<StoreDeviceStatus, StoreProtocolError> {
    match (left, right) {
        (StoreDeviceStatus::Active, StoreDeviceStatus::Active) => Ok(StoreDeviceStatus::Active),
        (
            StoreDeviceStatus::Inactive {
                terminals,
                accepted_cut,
            },
            StoreDeviceStatus::Active,
        )
        | (
            StoreDeviceStatus::Active,
            StoreDeviceStatus::Inactive {
                terminals,
                accepted_cut,
            },
        ) => Ok(StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        }),
        (
            StoreDeviceStatus::Inactive {
                terminals: left_terminals,
                accepted_cut: left_cut,
            },
            StoreDeviceStatus::Inactive {
                terminals: right_terminals,
                accepted_cut: right_cut,
            },
        ) => Ok(StoreDeviceStatus::Inactive {
            terminals: merge_terminal_refs(left_terminals, right_terminals)?,
            accepted_cut: intersect_terminal_history_cuts(left_cut, right_cut)?,
        }),
    }
}

fn merge_terminal_refs(
    left: Vec<StoreDeviceExclusionRef>,
    right: Vec<StoreDeviceExclusionRef>,
) -> Result<Vec<StoreDeviceExclusionRef>, StoreProtocolError> {
    let terminals = left
        .into_iter()
        .chain(right)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    validate_terminal_refs(&terminals)?;
    Ok(terminals)
}

pub(crate) fn merge_history_cuts(
    left: StoreHistoryCut,
    right: StoreHistoryCut,
) -> Result<StoreHistoryCut, StoreProtocolError> {
    {
        let StoreHistoryCut(mut left) = left;
        let StoreHistoryCut(right) = right;
        for (stream, reference) in right {
            match left.entry(stream) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(reference);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let current = entry.get();
                    if reference.coord.sequence() > current.coord.sequence() {
                        entry.insert(reference);
                    } else if reference.coord.sequence() == current.coord.sequence()
                        && reference != *current
                    {
                        return Err(StoreProtocolError::DeviceStateMismatch);
                    }
                }
            }
        }
        Ok(StoreHistoryCut(left))
    }
}

fn intersect_terminal_history_cuts(
    left: StoreHistoryCut,
    right: StoreHistoryCut,
) -> Result<StoreHistoryCut, StoreProtocolError> {
    {
        let StoreHistoryCut(left) = left;
        let StoreHistoryCut(right) = right;
        let mut intersection = BTreeMap::new();
        for (stream, left_reference) in left {
            let Some(right_reference) = right.get(&stream) else {
                continue;
            };
            let left_sequence = left_reference.coord.sequence();
            let right_sequence = right_reference.coord.sequence();
            let reference = if left_sequence < right_sequence {
                left_reference
            } else if right_sequence < left_sequence {
                right_reference.clone()
            } else if left_reference == *right_reference {
                left_reference
            } else {
                return Err(StoreProtocolError::DeviceStateMismatch);
            };
            intersection.insert(stream, reference);
        }
        Ok(StoreHistoryCut(intersection))
    }
}

fn validate_store_device_records(
    devices: &BTreeMap<StoreDeviceId, StoreDeviceRecord>,
) -> Result<(), StoreProtocolError> {
    for (device_id, record) in devices {
        if record.registration.device_id != *device_id {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        for (proposal_id, state) in &record.proposals {
            let proposal = match state {
                StoreDeviceProposalState::Pending { proposal }
                | StoreDeviceProposalState::Superseded { proposal, .. } => proposal,
                StoreDeviceProposalState::Cancelled { outcome } => &outcome.proposal,
            };
            if proposal.proposal_id != *proposal_id {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            if proposal.target != record.registration {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            if let StoreDeviceProposalState::Superseded { terminals, .. } = state {
                validate_terminal_refs(terminals)?;
            }
        }
        if let StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        } = &record.status
        {
            validate_terminal_refs(terminals)?;
            validate_store_history_cut(accepted_cut)?;
            if record
                .proposals
                .values()
                .any(|state| matches!(state, StoreDeviceProposalState::Pending { .. }))
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
    }
    Ok(())
}

fn validate_terminal_refs(terminals: &[StoreDeviceExclusionRef]) -> Result<(), StoreProtocolError> {
    if terminals.is_empty() || terminals.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    Ok(())
}

pub(crate) fn canonical_recovery_cursors(
    mut recovery: Vec<OwnerRecoveryCursor>,
) -> Result<Vec<OwnerRecoveryCursor>, StoreProtocolError> {
    recovery.sort();
    validate_recovery_cursors(&recovery)?;
    Ok(recovery)
}

pub(crate) fn validate_recovery_cursors(
    recovery: &[OwnerRecoveryCursor],
) -> Result<(), StoreProtocolError> {
    if recovery.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::OwnerRecoveryMismatch);
    }
    Ok(())
}
