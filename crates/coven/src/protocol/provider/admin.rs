use super::probe::*;
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCapabilityProof {
    pub exact_slots: ExactSlotProbeReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FounderProviderAdminGrant {
    pub grant_id: ProviderAdminGrantId,
    pub provider: ProviderDeviceBinding,
    pub access: ProviderAccessLocator,
    pub capability: ProviderCapabilityProof,
}

impl FounderProviderAdminGrant {
    #[cfg(test)]
    pub(crate) fn from_test_label(label: &str) -> Self {
        let probe_id =
            ProviderProbeId::from_bytes(*ObjectHash::digest(label.as_bytes()).as_bytes());
        let slot = ObjectSlot::logical(format!("store-v1/test/{label}/provider-probe/exact"))
            .expect("valid exact-probe test slot");
        let first = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateFirst);
        let second = probe_payload(&probe_id, ProbePayloadLabel::ExactCreateSecond);
        let accepted =
            ExactObjectRef::new(slot.clone(), first.len() as u64, ObjectHash::digest(&first));
        let lost_slot = ObjectSlot::logical(format!(
            "store-v1/test/{label}/provider-probe/lost-response"
        ))
        .expect("valid lost-response test slot");
        let lost_payload = probe_payload(&probe_id, ProbePayloadLabel::LostResponse);
        let lost_ref = ExactObjectRef::new(
            lost_slot.clone(),
            lost_payload.len() as u64,
            ObjectHash::digest(&lost_payload),
        );
        let device = ProviderDeviceBinding {
            principal: crate::protocol::objects::ProviderPrincipalId::CustomS3Credential {
                access_key_id_hash: ObjectHash::digest(format!("{label} access key").as_bytes()),
            },
        };
        let store = StoreProviderBinding::S3 {
            endpoint: crate::protocol::objects::S3EndpointBinding::Custom {
                origin: "https://test.invalid".to_string(),
            },
            region: "test-region".to_string(),
            bucket: format!("{label}-bucket"),
            key_prefix: None,
        };
        let transcript = ExactSlotProbeTranscript {
            probe_id,
            logical_key: slot.logical_key().to_string(),
            slot,
            contenders: [
                ProbeCreateAttempt {
                    payload_hash: ObjectHash::digest(&first),
                    outcome: ProbeCreateOutcome::Created,
                },
                ProbeCreateAttempt {
                    payload_hash: ObjectHash::digest(&second),
                    outcome: ProbeCreateOutcome::RejectedOccupied,
                },
            ],
            accepted: accepted.clone(),
            full_read_hash: accepted.stored_hash(),
            range: ProbeRangeReceipt {
                start: PROBE_RANGE_START,
                end: PROBE_RANGE_END,
                bytes_hash: ObjectHash::digest(
                    &first[PROBE_RANGE_START as usize..PROBE_RANGE_END as usize],
                ),
            },
            lost_response: LostResponseProbeReceipt {
                logical_key: lost_slot.logical_key().to_string(),
                slot: lost_slot,
                payload_hash: ObjectHash::digest(&lost_payload),
                settled: lost_ref,
                readback_hash: ObjectHash::digest(&lost_payload),
            },
        };
        Self {
            grant_id: ProviderAdminGrantId(ObjectHash::digest(
                format!("{label} provider admin grant").as_bytes(),
            )),
            provider: device.clone(),
            access: ProviderAccessLocator::S3SharedCredentialGeneration {
                generation: 1,
                access_key_id_hash: ObjectHash::digest(format!("{label} access key").as_bytes()),
            },
            capability: ProviderCapabilityProof {
                exact_slots: ExactSlotProbeReceipt::from_transcript(transcript, &store, &device),
            },
        }
    }
}

impl ProviderCapabilityProof {
    pub fn verify(
        &self,
        store: &StoreProviderBinding,
        device: &ProviderDeviceBinding,
    ) -> Result<(), ProviderProbeError> {
        self.exact_slots.verify(store, device)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminGrantRecord {
    pub grant_id: ProviderAdminGrantId,
    pub administrator: StoreDeviceRegistrationRef,
    pub provider: ProviderDeviceBinding,
    pub access: ProviderAccessLocator,
    pub capability: ProviderCapabilityProof,
    pub created_at: ProviderAdminGrantOrigin,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAdminGrantOrigin {
    Founder { root: StoreRootRef },
    Membership { coord: MembershipCoord },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminMembershipChange {
    pub change: ProviderAdminChange,
    #[serde(with = "ordered_owner_barriers")]
    pub owner_barriers: BTreeMap<MembershipGrantId, OwnerStreamBarrier>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAdminChange {
    Set {
        administrator: StoreDeviceRegistrationRef,
        provider: ProviderDeviceBinding,
        access: ProviderAccessLocator,
        capability: ProviderCapabilityProof,
        grant_id: ProviderAdminGrantId,
        replaces: BTreeSet<ProviderAdminGrantId>,
    },
    Remove {
        removes: BTreeSet<ProviderAdminGrantId>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminState {
    records: BTreeMap<ProviderAdminGrantId, ProviderAdminGrantRecord>,
    tombstones: BTreeSet<ProviderAdminGrantId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminBranch {
    pub heads: Vec<MembershipCoord>,
    pub state: ProviderAdminState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminConflict {
    pub raw_heads: Vec<MembershipCoord>,
    pub cyclic_sources: Vec<MembershipCoord>,
    pub involved_grants: BTreeSet<ProviderAdminGrantId>,
    pub maximal_valid_branches: Vec<ProviderAdminBranch>,
    pub combined: ProviderAdminState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAdminResolution {
    Resolved(ProviderAdminState),
    RevocationConflict(ProviderAdminConflict),
}

impl ProviderAdminResolution {
    pub fn combined_state(&self) -> &ProviderAdminState {
        match self {
            Self::Resolved(state) => state,
            Self::RevocationConflict(conflict) => &conflict.combined,
        }
    }

    pub fn state_hash(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(b"coven.provider-admin-resolution.v1\0", self))
    }
}

impl ProviderAdminState {
    pub fn founder(grant: ProviderAdminGrantRecord) -> Self {
        let grant_id = grant.grant_id.clone();
        Self {
            records: BTreeMap::from([(grant_id.clone(), grant)]),
            tombstones: BTreeSet::new(),
        }
    }

    pub fn founder_from_root(
        root: StoreRootRef,
        administrator: StoreDeviceRegistrationRef,
        grant: &FounderProviderAdminGrant,
    ) -> Self {
        Self::founder(ProviderAdminGrantRecord {
            grant_id: grant.grant_id.clone(),
            administrator,
            provider: grant.provider.clone(),
            access: grant.access.clone(),
            capability: grant.capability.clone(),
            created_at: ProviderAdminGrantOrigin::Founder { root },
        })
    }

    pub fn authorizes(
        &self,
        grant_id: &ProviderAdminGrantId,
        administrator: &StoreDeviceRegistrationRef,
    ) -> bool {
        !self.tombstones.contains(grant_id)
            && self
                .records
                .get(grant_id)
                .is_some_and(|record| &record.administrator == administrator)
    }

    pub fn records(&self) -> &BTreeMap<ProviderAdminGrantId, ProviderAdminGrantRecord> {
        &self.records
    }

    pub fn active(&self) -> BTreeSet<ProviderAdminGrantId> {
        self.records
            .keys()
            .filter(|grant_id| !self.tombstones.contains(*grant_id))
            .cloned()
            .collect()
    }

    pub fn tombstones(&self) -> &BTreeSet<ProviderAdminGrantId> {
        &self.tombstones
    }

    pub fn apply(
        &mut self,
        change: ProviderAdminChange,
        origin: ProviderAdminGrantOrigin,
    ) -> Result<(), ProviderAdminReducerError> {
        let mut next = self.clone();
        next.apply_unchecked(change, origin)?;
        if next.active().is_empty() {
            return Err(ProviderAdminReducerError::NoEffectiveAdministrator);
        }
        *self = next;
        Ok(())
    }

    fn apply_unchecked(
        &mut self,
        change: ProviderAdminChange,
        origin: ProviderAdminGrantOrigin,
    ) -> Result<(), ProviderAdminReducerError> {
        match change {
            ProviderAdminChange::Set {
                administrator,
                provider,
                access,
                capability,
                grant_id,
                replaces,
            } => {
                let record = ProviderAdminGrantRecord {
                    grant_id: grant_id.clone(),
                    administrator,
                    provider,
                    access,
                    capability,
                    created_at: origin,
                };
                if let Some(existing) = self.records.get(&grant_id) {
                    if existing != &record {
                        return Err(ProviderAdminReducerError::GrantIdReuse);
                    }
                    if !replaces.iter().all(|id| self.tombstones.contains(id)) {
                        return Err(ProviderAdminReducerError::UnknownReplacement);
                    }
                    return Ok(());
                }
                if !replaces
                    .iter()
                    .all(|id| self.records.contains_key(id) && !self.tombstones.contains(id))
                {
                    return Err(ProviderAdminReducerError::UnknownReplacement);
                }
                for replaced in replaces {
                    self.tombstones.insert(replaced);
                }
                self.records.insert(grant_id, record);
            }
            ProviderAdminChange::Remove { removes } => {
                if removes.is_empty()
                    || !removes
                        .iter()
                        .all(|id| self.records.contains_key(id) || self.tombstones.contains(id))
                {
                    return Err(ProviderAdminReducerError::UnknownRemoval);
                }
                for removed in removes {
                    self.tombstones.insert(removed);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_membership_change(
        &mut self,
        change: ProviderAdminMembershipChange,
        origin: ProviderAdminGrantOrigin,
    ) -> Result<(), ProviderAdminReducerError> {
        if !matches!(origin, ProviderAdminGrantOrigin::Membership { .. }) {
            return Err(ProviderAdminReducerError::PolicyOriginMismatch);
        }
        self.apply(change.change, origin)
    }

    pub fn state_hash(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(
            b"coven.provider-admin-state.v1\0",
            &(self.records(), self.tombstones()),
        ))
    }

    pub fn merge(
        states: impl IntoIterator<Item = Self>,
    ) -> Result<Self, ProviderAdminReducerError> {
        let mut records = BTreeMap::new();
        let mut tombstones = BTreeSet::new();
        for state in states {
            for (grant_id, record) in state.records {
                if records
                    .insert(grant_id.clone(), record.clone())
                    .is_some_and(|current| current != record)
                {
                    return Err(ProviderAdminReducerError::GrantIdReuse);
                }
            }
            tombstones.extend(state.tombstones);
        }
        Ok(Self {
            records,
            tombstones,
        })
    }

    pub(crate) fn reduce_merge(
        genesis: &Self,
        entries: &[MembershipEntry],
        included: &BTreeSet<MembershipCoord>,
    ) -> Result<ProviderAdminResolution, ProviderAdminReducerError> {
        let by_coord = entries
            .iter()
            .filter(|entry| included.contains(&entry.coord()))
            .map(|entry| (entry.coord(), entry))
            .collect::<BTreeMap<_, _>>();
        let mut states = BTreeMap::<MembershipCoord, Self>::new();
        let mut pending = by_coord.keys().cloned().collect::<BTreeSet<_>>();
        while !pending.is_empty() {
            let ready = pending.iter().find(|coord| {
                let entry = by_coord[*coord];
                let predecessor = (entry.seq > 1)
                    .then(|| stream_predecessor(&by_coord, entry))
                    .flatten();
                (entry.seq == 1 || predecessor.is_some_and(|value| states.contains_key(value)))
                    && entry
                        .dependencies
                        .iter()
                        .filter(|dependency| included.contains(*dependency))
                        .all(|dependency| states.contains_key(dependency))
            });
            let Some(coord) = ready.cloned() else {
                if pending.iter().any(|coord| {
                    let entry = by_coord[coord];
                    entry.seq > 1 && stream_predecessor(&by_coord, entry).is_none()
                }) {
                    return Err(ProviderAdminReducerError::MissingPredecessor);
                }
                return Err(ProviderAdminReducerError::CausalCycle);
            };
            let entry = by_coord[&coord];
            let mut causal_states = entry
                .dependencies
                .iter()
                .filter_map(|dependency| states.get(dependency).cloned())
                .collect::<Vec<_>>();
            if entry.seq > 1 {
                if let Some(predecessor) = stream_predecessor(&by_coord, entry) {
                    if !entry.dependencies.contains(predecessor) {
                        causal_states.push(states[predecessor].clone());
                    }
                }
            }
            let mut state = if causal_states.is_empty() {
                genesis.clone()
            } else {
                Self::merge(causal_states)?
            };
            if let Some(change) = entry.provider_admin.clone() {
                state.apply_membership_change(
                    change,
                    ProviderAdminGrantOrigin::Membership {
                        coord: coord.clone(),
                    },
                )?;
            }
            states.insert(coord.clone(), state);
            pending.remove(&coord);
        }
        let raw_heads = by_coord
            .keys()
            .filter(|coord| {
                !by_coord.values().any(|entry| {
                    entry.dependencies.contains(*coord)
                        || (entry.seq == coord.seq + 1
                            && entry.author_pubkey == coord.author_pubkey
                            && entry.author_owner_grant == coord.author_owner_grant
                            && entry.stream_id == coord.stream_id
                            && entry.previous_hash == Some(coord.entry_hash))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let combined =
            Self::merge(std::iter::once(genesis.clone()).chain(states.values().cloned()))?;
        if !combined.active().is_empty() {
            return Ok(ProviderAdminResolution::Resolved(combined));
        }
        if raw_heads.len() > 12 {
            return Err(ProviderAdminReducerError::ConflictTooWide(raw_heads.len()));
        }
        let head_states = raw_heads
            .iter()
            .map(|head| (head.clone(), states[head].clone()))
            .collect::<Vec<_>>();
        let mut valid = Vec::<ProviderAdminBranch>::new();
        for mask in 1usize..(1usize << head_states.len()) {
            let heads = head_states
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1usize << index) != 0)
                .map(|(_, (head, _))| head.clone())
                .collect::<Vec<_>>();
            let state = Self::merge(
                head_states
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| mask & (1usize << index) != 0)
                    .map(|(_, (_, state))| state.clone()),
            )?;
            if !state.active().is_empty() {
                valid.push(ProviderAdminBranch { heads, state });
            }
        }
        let valid_head_sets = valid
            .iter()
            .map(|branch| branch.heads.iter().cloned().collect::<BTreeSet<_>>())
            .collect::<Vec<_>>();
        let maximal_valid_branches = valid
            .into_iter()
            .enumerate()
            .filter(|(index, _)| {
                !valid_head_sets.iter().enumerate().any(|(other, heads)| {
                    other != *index && valid_head_sets[*index].is_subset(heads)
                })
            })
            .map(|(_, branch)| branch)
            .collect();
        let mut cyclic_sources = Vec::new();
        let mut involved_grants = BTreeSet::new();
        for (coord, entry) in &by_coord {
            if let Some(ProviderAdminMembershipChange {
                change: ProviderAdminChange::Remove { removes },
                ..
            }) = &entry.provider_admin
            {
                cyclic_sources.push(coord.clone());
                involved_grants.extend(removes.iter().cloned());
            }
        }
        cyclic_sources.sort();
        Ok(ProviderAdminResolution::RevocationConflict(
            ProviderAdminConflict {
                raw_heads,
                cyclic_sources,
                involved_grants,
                maximal_valid_branches,
                combined,
            },
        ))
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProviderAdminReducerError {
    #[error("provider administrator grant id was reused with different facts")]
    GrantIdReuse,
    #[error("provider administrator replacement names an inactive grant")]
    UnknownReplacement,
    #[error("provider administrator removal names an inactive grant")]
    UnknownRemoval,
    #[error("provider administrator change leaves no effective administrator")]
    NoEffectiveAdministrator,
    #[error("provider administrator change policy does not match its derived origin")]
    PolicyOriginMismatch,
    #[error("provider administrator causal history is missing an exact stream predecessor")]
    MissingPredecessor,
    #[error("provider administrator causal history contains a cycle")]
    CausalCycle,
    #[error("provider administrator revocation conflict has {0} heads, exceeding 12")]
    ConflictTooWide(usize),
}

/// The in-stream predecessor of `entry` among the included coordinates: the
/// same author stream at the previous sequence, carrying the hash the entry
/// links back to.
fn stream_predecessor<'coords>(
    by_coord: &'coords BTreeMap<MembershipCoord, &MembershipEntry>,
    entry: &MembershipEntry,
) -> Option<&'coords MembershipCoord> {
    by_coord.keys().find(|candidate| {
        candidate.author_pubkey == entry.author_pubkey
            && candidate.author_owner_grant == entry.author_owner_grant
            && candidate.stream_id == entry.stream_id
            && candidate.seq + 1 == entry.seq
            && Some(candidate.entry_hash) == entry.previous_hash
    })
}
