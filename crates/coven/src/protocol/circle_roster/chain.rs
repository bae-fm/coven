use super::reduction::*;
use super::*;

fn causal_owner_barriers(
    owner_barriers: &BTreeMap<MembershipGrantId, CircleOwnerGrantBarrier>,
) -> BTreeMap<MembershipGrantId, OwnerGrantBarrier<CircleRosterCoord>> {
    owner_barriers
        .iter()
        .map(|(grant, barrier)| {
            (
                grant.clone(),
                OwnerGrantBarrier::from_observed(barrier.observed_streams.iter().cloned()),
            )
        })
        .collect()
}

#[derive(Debug, Clone)]
pub(crate) struct CircleRosterChain {
    pub(super) entries: Vec<CircleRosterEntry>,
    pub(super) reduced: Option<causal_grants::ReducedGrants<CircleRosterCoord, CircleRole>>,
    pub(super) status: CircleRosterStatus,
    pub(super) head_refs: Vec<CircleRosterHeadRef>,
    pub(super) resolution_checkpoint: Option<CircleRosterResolutionCheckpoint>,
}

#[derive(Debug, Clone)]
pub(super) struct CircleRosterResolutionCheckpoint {
    raw_heads: Vec<CircleRosterCoord>,
    effective_frontier: Vec<CircleRosterCoord>,
    grants: BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    included: BTreeSet<CircleRosterCoord>,
    resolutions: Vec<CircleRosterConflictResolutionRef>,
}

impl CircleRosterChain {
    pub(crate) fn from_entries(entries: Vec<CircleRosterEntry>) -> Result<Self, CircleRosterError> {
        Self::from_entries_and_head_refs(entries, Vec::new())
    }

    pub(crate) fn from_entries_with_heads(
        entries: Vec<CircleRosterEntry>,
        heads: Vec<ExactCircleRosterHead>,
    ) -> Result<Self, CircleRosterError> {
        let head_refs = Self::validate_exact_heads(&entries, &heads)?;
        Self::from_entries_and_head_refs(entries, head_refs)
    }

    pub(crate) fn with_exact_successor(
        &self,
        entry: CircleRosterEntry,
        head: ExactCircleRosterHead,
    ) -> Result<Self, CircleRosterError> {
        if head.head().entry_coord() != entry.coord() {
            return Err(CircleRosterError::MissingConflictHeads);
        }
        let stream = entry.coord().stream_key();
        let mut entries = self.entries.clone();
        entries.push(entry);
        let mut head_refs = self.head_refs.clone();
        head_refs.retain(|reference| reference.coord.stream_key() != stream);
        head_refs.push(head.reference().clone());
        head_refs.sort_by_key(|reference| reference.coord.stream_key());
        Self::from_entries_head_refs_and_checkpoint(
            entries,
            head_refs,
            self.resolution_checkpoint.clone(),
        )
    }

    pub(crate) fn resolved_with_successor(
        &self,
        entry: CircleRosterEntry,
    ) -> Result<ResolvedCircleRoster, CircleRosterError> {
        let mut entries = self.entries.clone();
        entries.push(entry);
        Self::from_entries_head_refs_and_checkpoint(
            entries,
            self.head_refs.clone(),
            self.resolution_checkpoint.clone(),
        )?
        .try_resolved()
    }

    pub(crate) fn validate_exact_heads(
        entries: &[CircleRosterEntry],
        heads: &[ExactCircleRosterHead],
    ) -> Result<Vec<CircleRosterHeadRef>, CircleRosterError> {
        let founder = entries.first().ok_or(CircleRosterError::Empty)?;
        if heads.iter().any(|bound| {
            let head = bound.head();
            let reference = bound.reference();
            head.store_root_hash != founder.store_root_hash
                || head.circle_id != founder.circle_id
                || head.entry_coord() != reference.coord
                || entries
                    .iter()
                    .find(|entry| entry.coord() == reference.coord)
                    .is_none_or(|entry| head.resolutions != entry.resolution_dependencies)
        }) {
            return Err(CircleRosterError::MissingConflictHeads);
        }
        Ok(heads.iter().map(|head| head.reference().clone()).collect())
    }

    fn from_entries_and_head_refs(
        entries: Vec<CircleRosterEntry>,
        head_refs: Vec<CircleRosterHeadRef>,
    ) -> Result<Self, CircleRosterError> {
        Self::from_entries_head_refs_and_checkpoint(entries, head_refs, None)
    }

    fn from_entries_head_refs_and_checkpoint(
        entries: Vec<CircleRosterEntry>,
        head_refs: Vec<CircleRosterHeadRef>,
        resolution_checkpoint: Option<CircleRosterResolutionCheckpoint>,
    ) -> Result<Self, CircleRosterError> {
        let founder = entries.first().ok_or(CircleRosterError::Empty)?;
        let expected_store = founder.store_root_hash;
        let expected_circle = founder.circle_id;
        for (index, entry) in entries.iter().enumerate() {
            if !entry.verify() {
                return Err(CircleRosterError::InvalidEntry(index));
            }
            if entry.store_root_hash != expected_store || entry.circle_id != expected_circle {
                return Err(CircleRosterError::ContextMismatch { index });
            }
            if matches!(
                entry.change,
                CircleRosterChange::ResolutionActivation { .. }
            ) && resolution_checkpoint.as_ref().is_none_or(|checkpoint| {
                let already_checkpointed = checkpoint.included.contains(&entry.coord())
                    || checkpoint.raw_heads.contains(&entry.coord());
                !already_checkpointed
                    && (entry.dependencies != checkpoint.effective_frontier
                        || entry.resolution_dependencies != checkpoint.resolutions)
            }) {
                return Err(CircleRosterError::InvalidEntry(index));
            }
        }
        let checkpoint_heads = resolution_checkpoint
            .as_ref()
            .map_or_else(Vec::new, |checkpoint| checkpoint.raw_heads.clone());
        let normalized = causal_grants::entries_beyond_checkpoint(&entries, &checkpoint_heads)
            .map(|entry| CausalEntry {
                coord: entry.coord(),
                previous_hash: entry.previous_hash,
                dependencies: entry
                    .dependencies
                    .iter()
                    .cloned()
                    .map(|coord| (coord.stream_key(), coord))
                    .collect(),
                change: match &entry.change {
                    CircleRosterChange::Founder {
                        member_pubkey,
                        grant_id,
                    } => CausalChange::Founder {
                        member_pubkey: member_pubkey.clone(),
                        grant_id: grant_id.clone(),
                        assignment: CircleRole::Owner,
                    },
                    CircleRosterChange::SetMember {
                        member_pubkey,
                        role,
                        grant_id,
                        replaces,
                        owner_barriers,
                    } => CausalChange::SetMember {
                        member_pubkey: member_pubkey.clone(),
                        assignment: *role,
                        grant_id: grant_id.clone(),
                        replaces: replaces.clone(),
                        owner_barriers: causal_owner_barriers(owner_barriers),
                    },
                    CircleRosterChange::RemoveMember {
                        member_pubkey,
                        removes,
                        owner_barriers,
                    } => CausalChange::RemoveMember {
                        member_pubkey: member_pubkey.clone(),
                        removes: removes.clone(),
                        owner_barriers: causal_owner_barriers(owner_barriers),
                    },
                    CircleRosterChange::ResolutionActivation { .. } => {
                        CausalChange::ResolutionActivation
                    }
                },
            })
            .collect::<Vec<_>>();
        let reduction = match &resolution_checkpoint {
            Some(checkpoint) => {
                let seeds = causal_grants::checkpoint_seed_grants(&checkpoint.grants, |record| {
                    causal_grants::CausalSeedGrant {
                        member_pubkey: record.member_pubkey.clone(),
                        assignment: record.role,
                    }
                });
                causal_grants::reduce_from_checkpoint(
                    &normalized,
                    &checkpoint.raw_heads,
                    &checkpoint.effective_frontier,
                    &seeds,
                    &checkpoint.included,
                )?
            }
            None => causal_grants::reduce(&normalized)?,
        };
        let founder_entry = entries
            .iter()
            .find(|entry| matches!(entry.change, CircleRosterChange::Founder { .. }))
            .expect("shared reducer requires one founder");
        if founder_entry.circle_id
            != CircleId::founder(
                founder_entry.store_root_hash,
                &founder_entry.author_pubkey,
                &founder_entry.author_owner_grant,
            )
        {
            return Err(CircleRosterError::InvalidFounderIdentity);
        }
        let (reduced, status) = match reduction {
            CausalGrantStatus::Resolved(reduced) => {
                let resolved = resolved_circle_roster(
                    &reduced,
                    resolution_checkpoint
                        .as_ref()
                        .map(|checkpoint| &checkpoint.grants),
                )?;
                (Some(reduced), CircleRosterStatus::Resolved(resolved))
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::ConcurrentMemberAssignments {
                raw_heads,
                effective_frontier,
                member_pubkey,
                conflicting_grants,
                uncontested_grants,
                reduced,
            }) => {
                let heads = exact_circle_head_refs(&head_refs, &raw_heads)?;
                let conflict_hash = circle_assignment_conflict_hash(
                    expected_store,
                    expected_circle,
                    &heads,
                    &member_pubkey,
                    &conflicting_grants,
                );
                (
                    Some(reduced),
                    CircleRosterStatus::Conflict(
                        CircleRosterConflict::ConcurrentMemberAssignments {
                            conflict_hash,
                            heads,
                            effective_frontier,
                            member_pubkey,
                            conflicting_grants: map_circle_grants(
                                conflicting_grants,
                                resolution_checkpoint
                                    .as_ref()
                                    .map(|checkpoint| &checkpoint.grants),
                            )?,
                            uncontested_grants: map_circle_grants(
                                uncontested_grants,
                                resolution_checkpoint
                                    .as_ref()
                                    .map(|checkpoint| &checkpoint.grants),
                            )?,
                        },
                    ),
                )
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::RevocationCycle {
                raw_heads,
                cyclic_sources,
                involved_owner_grants,
                maximal_valid_branches,
            }) => {
                let heads = exact_circle_head_refs(&head_refs, &raw_heads)?;
                let branches = maximal_valid_branches
                    .into_iter()
                    .map(|branch| -> Result<CircleRosterBranch, CircleRosterError> {
                        let resolved = resolved_circle_roster(
                            &branch.reduced,
                            resolution_checkpoint
                                .as_ref()
                                .map(|checkpoint| &checkpoint.grants),
                        )?;
                        Ok(CircleRosterBranch {
                            heads: exact_circle_head_refs(&head_refs, &branch.raw_heads)?,
                            effective_frontier: branch.effective_frontier,
                            grants: resolved.grants,
                            state_hash: resolved.state_hash,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let conflict_hash = circle_revocation_conflict_hash(
                    expected_store,
                    expected_circle,
                    &heads,
                    &cyclic_sources,
                    &involved_owner_grants,
                );
                (
                    None,
                    CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                        conflict_hash,
                        heads,
                        cyclic_sources,
                        involved_owner_grants,
                        maximal_valid_branches: branches,
                    }),
                )
            }
        };
        Ok(Self {
            entries,
            reduced,
            status,
            head_refs,
            resolution_checkpoint,
        })
    }

    pub(crate) fn entries(&self) -> &[CircleRosterEntry] {
        &self.entries
    }

    pub(crate) fn status(&self) -> &CircleRosterStatus {
        &self.status
    }

    pub(crate) fn resolution_refs(&self) -> &[CircleRosterConflictResolutionRef] {
        self.resolution_checkpoint
            .as_ref()
            .map_or(&[], |checkpoint| checkpoint.resolutions.as_slice())
    }

    pub(crate) fn resolution_checkpoint_covers(&self, coord: &CircleRosterCoord) -> bool {
        self.resolution_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| {
                checkpoint.included.contains(coord) || checkpoint.raw_heads.contains(coord)
            })
    }

    pub(crate) fn replay_resolved_history_to_heads(
        &self,
        entries: Vec<CircleRosterEntry>,
        heads: Vec<CircleRosterHeadRef>,
    ) -> Result<Self, CircleRosterError> {
        let checkpoint = self
            .resolution_checkpoint
            .clone()
            .ok_or(CircleRosterError::InvalidConflictResolution)?;
        if heads.iter().any(|head| {
            entries
                .iter()
                .find(|entry| entry.coord() == head.coord)
                .is_none()
        }) {
            return Err(CircleRosterError::MissingConflictHeads);
        }
        Self::from_entries_head_refs_and_checkpoint(entries, heads, Some(checkpoint))
    }

    pub(crate) fn replay_merged_resolved_histories_to_heads(
        chains: &[&CircleRosterChain],
        entries: Vec<CircleRosterEntry>,
        heads: Vec<CircleRosterHeadRef>,
    ) -> Result<Self, CircleRosterError> {
        let mut raw_by_stream = BTreeMap::new();
        let mut effective_by_stream = BTreeMap::new();
        let mut grants = BTreeMap::new();
        let mut included = BTreeSet::new();
        let mut resolutions = BTreeSet::new();
        for chain in chains {
            let checkpoint = chain
                .resolution_checkpoint
                .as_ref()
                .ok_or(CircleRosterError::InvalidConflictResolution)?;
            if !causal_grants::merge_checkpoint_frontier(&mut raw_by_stream, &checkpoint.raw_heads)
                || !causal_grants::merge_checkpoint_frontier(
                    &mut effective_by_stream,
                    &checkpoint.effective_frontier,
                )
                || !causal_grants::merge_checkpoint_evidence(
                    &mut grants,
                    &mut included,
                    &checkpoint.grants,
                    &checkpoint.included,
                )
            {
                return Err(CircleRosterError::InvalidConflictResolution);
            }
            resolutions.extend(checkpoint.resolutions.iter().cloned());
        }
        let checkpoint = CircleRosterResolutionCheckpoint {
            raw_heads: raw_by_stream.into_values().collect(),
            effective_frontier: effective_by_stream.into_values().collect(),
            grants,
            included,
            resolutions: resolutions.into_iter().collect(),
        };
        let base = chains
            .first()
            .ok_or(CircleRosterError::InvalidConflictResolution)?;
        let mut merged = (*base).clone();
        merged.resolution_checkpoint = Some(checkpoint);
        merged.replay_resolved_history_to_heads(entries, heads)
    }

    pub(crate) fn checkpoint_current_resolved_state(&mut self) -> Result<(), CircleRosterError> {
        self.try_resolved()?;
        let resolutions = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(Vec::new, |checkpoint| checkpoint.resolutions.clone());
        let checkpoint_grants = self
            .resolution_checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.grants);
        let reduced = self
            .reduced
            .as_ref()
            .ok_or(CircleRosterError::InvalidConflictResolution)?;
        let grants = reduced
            .grants
            .iter()
            .map(|(grant, state)| -> Result<_, CircleRosterError> {
                Ok((
                    grant.clone(),
                    map_circle_grant_state(grant, state, checkpoint_grants)?,
                ))
            })
            .collect::<Result<_, _>>()?;
        self.resolution_checkpoint = Some(CircleRosterResolutionCheckpoint {
            raw_heads: self.author_heads(),
            effective_frontier: self.effective_frontier(),
            grants,
            included: reduced.included.clone(),
            resolutions,
        });
        Ok(())
    }

    pub(crate) fn resolved(&self) -> ResolvedCircleRoster {
        self.try_resolved()
            .expect("caller must inspect Circle roster status before consuming resolved state")
    }

    pub(crate) fn try_resolved(&self) -> Result<ResolvedCircleRoster, CircleRosterError> {
        match &self.status {
            CircleRosterStatus::Resolved(resolved) => Ok(resolved.clone()),
            CircleRosterStatus::Conflict(_) => Err(CircleRosterError::Conflict),
        }
    }

    pub(crate) fn resolved_with(
        &self,
        resolutions: &[CircleRosterConflictResolution],
    ) -> Result<ResolvedCircleRoster, CircleRosterError> {
        match &self.status {
            CircleRosterStatus::Resolved(resolved) if resolutions.is_empty() => {
                Ok(resolved.clone())
            }
            CircleRosterStatus::Conflict(conflict) => resolve_circle_roster_conflict(
                self.entries[0].store_root_hash,
                self.entries[0].circle_id,
                conflict,
                resolutions,
            ),
            CircleRosterStatus::Resolved(_) => Err(CircleRosterError::InvalidConflictResolution),
        }
    }

    pub(crate) fn apply_resolutions(
        &mut self,
        resolutions: &[CircleRosterConflictResolution],
    ) -> Result<(), CircleRosterError> {
        let (raw_heads, effective_frontier) = match self.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                heads,
                maximal_valid_branches,
                ..
            }) => {
                let selected = resolutions
                    .iter()
                    .map(|resolution| {
                        maximal_valid_branches
                            .iter()
                            .find(|branch| branch.heads == resolution.resolver_branch_heads)
                            .map(|branch| branch.effective_frontier.as_slice())
                            .ok_or(CircleRosterError::InvalidConflictResolution)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (
                    heads
                        .iter()
                        .map(|reference| reference.coord.clone())
                        .collect(),
                    causal_grants::common_frontier(&selected),
                )
            }
            _ => return Err(CircleRosterError::InvalidConflictResolution),
        };
        let resolved = self.resolved_with(resolutions)?;
        let grants = resolved.grants.clone();
        let included = causal_grants::history_closure(&self.entries, &effective_frontier);
        let mut resolution_refs = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(Vec::new, |checkpoint| checkpoint.resolutions.clone());
        resolution_refs.extend(
            resolutions
                .iter()
                .map(CircleRosterConflictResolution::resolution_ref),
        );
        resolution_refs.sort();
        resolution_refs.dedup();
        let checkpoint = CircleRosterResolutionCheckpoint {
            raw_heads,
            effective_frontier: effective_frontier.clone(),
            grants: grants.clone(),
            included: included.clone(),
            resolutions: resolution_refs,
        };
        self.reduced = Some(causal_grants::ReducedGrants {
            grants: grants
                .iter()
                .map(|(grant, state)| {
                    let concrete = state.record();
                    let record = causal_grants::GrantRecord {
                        member_pubkey: concrete.member_pubkey.clone(),
                        assignment: concrete.role,
                        creation: causal_grants::CausalGrantCreation::Checkpoint,
                    };
                    (
                        grant.clone(),
                        match state {
                            GrantState::Active { .. } => GrantState::Active { record },
                            GrantState::Tombstoned { .. } => GrantState::Tombstoned {
                                record,
                                retirements: GrantRetirements::new(
                                    causal_grants::CausalGrantRetirement::Checkpoint,
                                ),
                            },
                        },
                    )
                })
                .collect(),
            included: included.clone(),
        });
        self.status = CircleRosterStatus::Resolved(resolved);
        self.resolution_checkpoint = Some(checkpoint);
        Ok(())
    }

    pub(crate) fn author_heads(&self) -> Vec<CircleRosterCoord> {
        causal_grants::stream_frontier(self.entries.iter().map(CircleRosterEntry::coord))
    }

    pub(crate) fn effective_frontier(&self) -> Vec<CircleRosterCoord> {
        let Some(reduced) = &self.reduced else {
            return Vec::new();
        };
        causal_grants::stream_frontier(
            self.entries
                .iter()
                .map(CircleRosterEntry::coord)
                .filter(|coord| reduced.includes_coord(coord)),
        )
    }

    fn active_grants(&self, member_pubkey: &str) -> BTreeSet<MembershipGrantId> {
        let reduced = self
            .reduced
            .as_ref()
            .expect("resolved roster has reduced grants");
        reduced
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                state
                    .active()
                    .is_some_and(|record| record.member_pubkey == member_pubkey)
                    .then_some(grant.clone())
            })
            .collect()
    }

    fn active_owner_grant(&self, member_pubkey: &str) -> Option<MembershipGrantId> {
        self.active_grants(member_pubkey).into_iter().find(|grant| {
            self.reduced
                .as_ref()
                .expect("resolved roster has reduced grants")
                .active_grant(grant)
                .is_some_and(|record| record.assignment == CircleRole::Owner)
        })
    }

    pub(crate) fn reusable_author_streams(
        &self,
        author_pubkey: &str,
        device_id: &str,
        grant: &MembershipGrantId,
    ) -> BTreeSet<AuthorStreamId> {
        self.effective_frontier()
            .into_iter()
            .filter(|effective_tip| {
                effective_tip.author_pubkey == author_pubkey
                    && effective_tip.device_id == device_id
                    && effective_tip.author_owner_grant == *grant
                    && self
                        .entries
                        .iter()
                        .map(CircleRosterEntry::coord)
                        .filter(|coord| coord.stream_key() == effective_tip.stream_key())
                        .max_by_key(|coord| coord.seq)
                        .as_ref()
                        == Some(effective_tip)
            })
            .map(|coord| coord.stream_id)
            .collect()
    }

    fn owner_barriers(
        &self,
        grants: &BTreeSet<MembershipGrantId>,
        dependencies: &[CircleRosterCoord],
    ) -> BTreeMap<MembershipGrantId, CircleOwnerGrantBarrier> {
        grants
            .iter()
            .filter(|grant| {
                self.reduced
                    .as_ref()
                    .expect("resolved roster has reduced grants")
                    .active_grant(grant)
                    .is_some_and(|record| record.assignment == CircleRole::Owner)
            })
            .map(|grant| {
                let observed_streams = dependencies
                    .iter()
                    .filter(|coord| coord.author_owner_grant == *grant)
                    .cloned()
                    .collect();
                (grant.clone(), CircleOwnerGrantBarrier { observed_streams })
            })
            .collect()
    }

    pub(super) fn next_position(
        &self,
        stream: &CircleAuthorStreamKey,
    ) -> Result<(u64, Option<ObjectHash>), CircleRosterError> {
        let raw_tip = self
            .entries
            .iter()
            .map(CircleRosterEntry::coord)
            .filter(|coord| coord.stream_key() == *stream)
            .max_by_key(|coord| coord.seq);
        let effective_tip = self
            .effective_frontier()
            .into_iter()
            .find(|coord| coord.stream_key() == *stream);
        if raw_tip.is_some()
            && !self
                .reusable_author_streams(
                    &stream.author_pubkey,
                    &stream.device_id,
                    &stream.author_owner_grant,
                )
                .contains(&stream.stream_id)
        {
            return Err(CircleRosterError::PrunedAuthorStream);
        }
        match effective_tip {
            Some(tip) => Ok((
                tip.seq
                    .checked_add(1)
                    .ok_or(CircleRosterError::SequenceExhausted { current: tip.seq })?,
                Some(tip.entry_hash),
            )),
            None => Ok((1, None)),
        }
    }

    pub(crate) fn signed_set_member(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        member_pubkey: String,
        role: CircleRole,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
        self.signed_change(device_id, stream_id, member_pubkey, Some(role), signer)
    }

    pub(crate) fn signed_remove_member(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        member_pubkey: String,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
        if self.active_grants(&member_pubkey).is_empty() {
            return Err(CircleRosterError::NotAMember(member_pubkey));
        }
        self.signed_change(device_id, stream_id, member_pubkey, None, signer)
    }

    fn signed_change(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        member_pubkey: String,
        role: Option<CircleRole>,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
        if matches!(self.status, CircleRosterStatus::Conflict(_)) {
            return Err(CircleRosterError::Conflict);
        }
        let author_pubkey = keys::public_key_hex(signer);
        let author_owner_grant = self
            .active_owner_grant(&author_pubkey)
            .ok_or_else(|| CircleRosterError::SignerIsNotOwner(author_pubkey.clone()))?;
        let stream = CircleAuthorStreamKey {
            author_pubkey: author_pubkey.clone(),
            device_id: device_id.to_string(),
            stream_id,
            author_owner_grant: author_owner_grant.clone(),
        };
        let (seq, previous_hash) = self.next_position(&stream)?;
        let dependencies = self.effective_frontier();
        let replaced = self.active_grants(&member_pubkey);
        let owner_barriers = self.owner_barriers(&replaced, &dependencies);
        let change = match role {
            Some(role) => CircleRosterChange::SetMember {
                member_pubkey: member_pubkey.clone(),
                role,
                grant_id: MembershipGrantId(ObjectHash::digest(
                    format!(
                        "coven.circle-roster-grant.v1\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
                        self.entries[0].circle_id,
                        author_pubkey,
                        device_id,
                        stream_id,
                        author_owner_grant,
                        seq,
                        member_pubkey
                    )
                    .as_bytes(),
                )),
                replaces: replaced,
                owner_barriers,
            },
            None => CircleRosterChange::RemoveMember {
                member_pubkey,
                removes: replaced,
                owner_barriers,
            },
        };
        let mut entry = CircleRosterEntry {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: self.entries[0].store_root_hash,
            circle_id: self.entries[0].circle_id,
            author_pubkey,
            device_id: device_id.to_string(),
            stream_id,
            author_owner_grant,
            seq,
            previous_hash,
            dependencies,
            resolution_dependencies: self.resolution_refs().to_vec(),
            change,
            signature: String::new(),
        };
        entry.signature = keys::sign_hex(signer, &entry.canonical_bytes()).1;
        let mut candidate_history = self.entries.clone();
        candidate_history.push(entry.clone());
        Self::from_entries_head_refs_and_checkpoint(
            candidate_history,
            self.head_refs.clone(),
            self.resolution_checkpoint.clone(),
        )?;
        Ok(entry)
    }

    #[cfg(test)]
    pub(crate) fn signed_cycle_resolution(
        &self,
        resolver_branch_heads: Vec<CircleRosterHeadRef>,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<CircleRosterConflictResolution, CircleRosterError> {
        let CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
            conflict_hash,
            heads,
            involved_owner_grants,
            maximal_valid_branches,
            ..
        }) = self.status()
        else {
            return Err(CircleRosterError::Conflict);
        };
        let resolver_pubkey = keys::public_key_hex(signer);
        let branch = maximal_valid_branches
            .iter()
            .find(|branch| branch.heads == resolver_branch_heads)
            .ok_or(CircleRosterError::InvalidConflictResolution)?;
        if !causal_grants::active_grants(&branch.grants).any(|(_, record)| {
            record.member_pubkey == resolver_pubkey && record.role == CircleRole::Owner
        }) {
            return Err(CircleRosterError::SignerIsNotOwner(resolver_pubkey));
        }
        let replacement_grant = derive_circle_resolution_grant(conflict_hash, &resolver_pubkey);
        let mut retired_owner_grants = involved_owner_grants.clone();
        retired_owner_grants.extend(causal_grants::active_grants(&branch.grants).filter_map(
            |(grant, record)| {
                (record.member_pubkey == resolver_pubkey && record.role == CircleRole::Owner)
                    .then_some(grant.clone())
            },
        ));
        let mut resolution = CircleRosterConflictResolution {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: self.entries[0].store_root_hash,
            circle_id: self.entries[0].circle_id,
            conflict_hash: *conflict_hash,
            conflicting_heads: heads.clone(),
            retired_owner_grants,
            resolver_pubkey,
            resolver_branch_heads,
            replacement_grant,
            signature: String::new(),
        };
        resolution.signature = keys::sign_hex(signer, &resolution.canonical_bytes()).1;
        Ok(resolution)
    }
}
