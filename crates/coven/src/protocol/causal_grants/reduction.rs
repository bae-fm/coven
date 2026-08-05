use super::fixed_sets::*;
use super::*;

#[derive(Debug, Clone)]
struct CausalState<C: CausalCoordinate, A: CausalAssignment> {
    grants: BTreeMap<MembershipGrantId, GrantState<GrantRecord<C, A>, CausalGrantRetirement<C>>>,
}

impl<C: CausalCoordinate, A: CausalAssignment> Default for CausalState<C, A> {
    fn default() -> Self {
        Self {
            grants: BTreeMap::new(),
        }
    }
}

impl<C: CausalCoordinate, A: CausalAssignment> CausalState<C, A> {
    fn merge(&mut self, other: &Self, index: usize) -> Result<(), CausalGrantError<C>> {
        for (grant, state) in &other.grants {
            match self.grants.entry(grant.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(state.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if !entry.get_mut().merge(state) {
                        return Err(CausalGrantError::DuplicateGrant {
                            index,
                            grant: grant.clone(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn active_owner(&self, grant: &MembershipGrantId, pubkey: &str) -> bool {
        self.grants
            .get(grant)
            .and_then(GrantState::active)
            .is_some_and(|record| record.member_pubkey == pubkey && record.assignment.is_owner())
    }

    fn apply(
        &mut self,
        coord: &C,
        change: &CausalChange<C, A>,
        index: usize,
        validate_exact_grants: bool,
    ) -> Result<(), CausalGrantError<C>> {
        match change {
            CausalChange::Founder {
                member_pubkey,
                grant_id,
                assignment,
            } => self.insert(index, coord, member_pubkey, grant_id, assignment.clone())?,
            CausalChange::SetMember {
                member_pubkey,
                assignment,
                grant_id,
                replaces,
                owner_barriers,
            } => {
                self.remove(
                    index,
                    coord,
                    member_pubkey,
                    replaces,
                    owner_barriers,
                    false,
                    validate_exact_grants,
                )?;
                self.insert(index, coord, member_pubkey, grant_id, assignment.clone())?;
            }
            CausalChange::RemoveMember {
                member_pubkey,
                removes,
                owner_barriers,
            } => self.remove(
                index,
                coord,
                member_pubkey,
                removes,
                owner_barriers,
                true,
                validate_exact_grants,
            )?,
            CausalChange::Control | CausalChange::ResolutionActivation => {}
        }
        Ok(())
    }

    fn insert(
        &mut self,
        index: usize,
        coord: &C,
        member_pubkey: &str,
        grant: &MembershipGrantId,
        assignment: A,
    ) -> Result<(), CausalGrantError<C>> {
        if self.grants.contains_key(grant) {
            return Err(CausalGrantError::DuplicateGrant {
                index,
                grant: grant.clone(),
            });
        }
        self.grants.insert(
            grant.clone(),
            GrantState::Active {
                record: GrantRecord {
                    member_pubkey: member_pubkey.to_string(),
                    assignment,
                    creation: CausalGrantCreation::Entry(coord.clone()),
                },
            },
        );
        Ok(())
    }

    fn remove(
        &mut self,
        index: usize,
        coord: &C,
        member_pubkey: &str,
        grants: &BTreeSet<MembershipGrantId>,
        owner_barriers: &BTreeMap<MembershipGrantId, OwnerGrantBarrier<C>>,
        require_nonempty: bool,
        validate_exact_grants: bool,
    ) -> Result<(), CausalGrantError<C>> {
        if require_nonempty && grants.is_empty() {
            return Err(CausalGrantError::EmptyRemoval { index });
        }
        let active = self
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                state
                    .active()
                    .is_some_and(|record| record.member_pubkey == member_pubkey)
                    .then_some(grant.clone())
            })
            .collect::<BTreeSet<_>>();
        if validate_exact_grants && active != *grants {
            return Err(CausalGrantError::GrantSetMismatch {
                index,
                member_pubkey: member_pubkey.to_string(),
            });
        }
        for grant in grants {
            let state =
                self.grants
                    .get_mut(grant)
                    .ok_or_else(|| CausalGrantError::GrantOwnerMismatch {
                        index,
                        grant: grant.clone(),
                    })?;
            if state.record().member_pubkey != member_pubkey {
                return Err(CausalGrantError::GrantOwnerMismatch {
                    index,
                    grant: grant.clone(),
                });
            }
            let retirement = CausalGrantRetirement::Entry {
                coord: coord.clone(),
                owner_barrier: owner_barriers.get(grant).cloned(),
            };
            match state {
                GrantState::Active { record } => {
                    *state = GrantState::Tombstoned {
                        record: record.clone(),
                        retirements: GrantRetirements::new(retirement),
                    };
                }
                GrantState::Tombstoned { retirements, .. } => {
                    retirements.insert(retirement);
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn reduce<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
) -> Result<CausalGrantStatus<C, A>, CausalGrantError<C>> {
    reduce_internal(entries, &[], &[], &BTreeMap::new(), &BTreeSet::new(), true)
}

pub(crate) fn reduce_from_checkpoint<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    raw_checkpoint_heads: &[C],
    effective_checkpoint_frontier: &[C],
    seed_grants: &BTreeMap<MembershipGrantId, GrantState<CausalSeedGrant<A>, ()>>,
    seed_included: &BTreeSet<C>,
) -> Result<CausalGrantStatus<C, A>, CausalGrantError<C>> {
    reduce_internal(
        entries,
        raw_checkpoint_heads,
        effective_checkpoint_frontier,
        seed_grants,
        seed_included,
        false,
    )
}

fn reduce_internal<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    raw_checkpoint_heads: &[C],
    effective_checkpoint_frontier: &[C],
    seed_grants: &BTreeMap<MembershipGrantId, GrantState<CausalSeedGrant<A>, ()>>,
    seed_included: &BTreeSet<C>,
    require_founder: bool,
) -> Result<CausalGrantStatus<C, A>, CausalGrantError<C>> {
    if entries.is_empty() && raw_checkpoint_heads.is_empty() {
        return Err(CausalGrantError::Empty);
    }
    let checkpoint_by_stream = raw_checkpoint_heads
        .iter()
        .map(|coord| (coord.stream_key(), coord.clone()))
        .collect::<BTreeMap<_, _>>();
    if checkpoint_by_stream.len() != raw_checkpoint_heads.len() {
        return Err(CausalGrantError::ConflictingSequence {
            stream: raw_checkpoint_heads[0].stream_key(),
            seq: raw_checkpoint_heads[0].seq(),
        });
    }
    let checkpoint_set = seed_included.iter().cloned().collect::<BTreeSet<_>>();
    let mut index_by_coord = BTreeMap::new();
    let mut streams = BTreeMap::<C::StreamKey, BTreeMap<u64, usize>>::new();
    for (index, entry) in entries.iter().enumerate() {
        if index_by_coord.insert(entry.coord.clone(), index).is_some() {
            return Err(CausalGrantError::ConflictingSequence {
                stream: entry.coord.stream_key(),
                seq: entry.coord.seq(),
            });
        }
        if streams
            .entry(entry.coord.stream_key())
            .or_default()
            .insert(entry.coord.seq(), index)
            .is_some()
        {
            return Err(CausalGrantError::ConflictingSequence {
                stream: entry.coord.stream_key(),
                seq: entry.coord.seq(),
            });
        }
        if entry
            .dependencies
            .iter()
            .any(|(stream, coord)| stream != &coord.stream_key())
        {
            return Err(CausalGrantError::DependencyStreamMismatch { index });
        }
    }

    for (stream, positions) in &streams {
        let max_seq = *positions.keys().next_back().expect("stream is non-empty");
        let checkpoint = checkpoint_by_stream.get(stream);
        let first_seq = checkpoint.map_or(1, |coord| coord.seq() + 1);
        if positions.keys().next().copied() != Some(first_seq) {
            return Err(CausalGrantError::MissingSequence {
                stream: stream.clone(),
                seq: first_seq,
            });
        }
        let mut previous_hash = checkpoint.map(|coord| coord.entry_hash());
        for seq in first_seq..=max_seq {
            let Some(index) = positions.get(&seq).copied() else {
                return Err(CausalGrantError::MissingSequence {
                    stream: stream.clone(),
                    seq,
                });
            };
            let entry = &entries[index];
            if entry.previous_hash != previous_hash {
                return Err(CausalGrantError::BrokenStreamLink {
                    index,
                    expected: previous_hash,
                    actual: entry.previous_hash,
                });
            }
            let own_dependency_is_exact = if seq == 1 {
                !entry.dependencies.contains_key(stream)
            } else {
                let predecessor = if seq == first_seq {
                    checkpoint.expect("a suffix above sequence one has a checkpoint predecessor")
                } else {
                    &entries[positions[&(seq - 1)]].coord
                };
                entry.dependencies.get(stream) == Some(predecessor)
            };
            if !own_dependency_is_exact {
                return Err(CausalGrantError::MissingOwnDependency { index });
            }
            previous_hash = Some(entry.coord.entry_hash());
        }
    }

    for (index, entry) in entries.iter().enumerate() {
        for dependency in entry.dependencies.values() {
            if !index_by_coord.contains_key(dependency) && !checkpoint_set.contains(dependency) {
                return Err(CausalGrantError::MissingDependency {
                    index,
                    dependency: dependency.clone(),
                });
            }
        }
    }

    let founders = entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            matches!(entry.change, CausalChange::Founder { .. }).then_some(index)
        })
        .collect::<Vec<_>>();
    if (require_founder && founders.len() != 1) || (!require_founder && !founders.is_empty()) {
        return Err(CausalGrantError::InvalidFounder);
    }
    let founder_index = founders.first().copied();
    if let Some(founder_index) = founder_index {
        let founder = &entries[founder_index];
        let CausalChange::Founder {
            member_pubkey,
            grant_id,
            assignment,
        } = &founder.change
        else {
            unreachable!()
        };
        if founder.coord.seq() != 1
            || founder.coord.author_pubkey() != member_pubkey
            || founder.coord.author_owner_grant() != grant_id
            || founder.previous_hash.is_some()
            || !founder.dependencies.is_empty()
            || !assignment.is_owner()
        {
            return Err(CausalGrantError::InvalidFounder);
        }
    }

    let mut remaining = (0..entries.len()).collect::<BTreeSet<_>>();
    let mut states = BTreeMap::<C, CausalState<C, A>>::new();
    let seed_state = CausalState {
        grants: map_checkpoint_grants(
            seed_grants,
            |record| GrantRecord {
                member_pubkey: record.member_pubkey.clone(),
                assignment: record.assignment.clone(),
                creation: CausalGrantCreation::Checkpoint,
            },
            || CausalGrantRetirement::Checkpoint,
        ),
    };
    for head in effective_checkpoint_frontier {
        states.insert(head.clone(), seed_state.clone());
    }
    let mut causal_order = Vec::with_capacity(entries.len());
    while !remaining.is_empty() {
        let ready = remaining
            .iter()
            .copied()
            .filter(|index| {
                entries[*index]
                    .dependencies
                    .values()
                    .all(|dependency| states.contains_key(dependency))
            })
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err(CausalGrantError::DependencyCycle);
        }
        for index in ready {
            remaining.remove(&index);
            causal_order.push(index);
            let entry = &entries[index];
            let mut state = CausalState::default();
            for dependency in entry.dependencies.values() {
                state.merge(&states[dependency], index)?;
            }
            if Some(index) != founder_index
                && !state.active_owner(
                    entry.coord.author_owner_grant(),
                    entry.coord.author_pubkey(),
                )
            {
                return Err(CausalGrantError::AuthorGrantInactive {
                    index,
                    grant: entry.coord.author_owner_grant().clone(),
                });
            }
            validate_barriers(index, entry, &state)?;
            state.apply(&entry.coord, &entry.change, index, true)?;
            states.insert(entry.coord.clone(), state);
        }
    }

    let mut raw = CausalState::default();
    for state in states.values() {
        raw.merge(state, 0)?;
    }
    let mut all_sources = BTreeSet::new();
    for (index, entry) in entries.iter().enumerate() {
        if let Some((removed, _)) = entry.change.removed() {
            for grant in removed {
                if raw
                    .grants
                    .get(grant)
                    .is_some_and(|state| state.record().assignment.is_owner())
                {
                    all_sources.insert(index);
                }
            }
        }
    }

    let mut sources = all_sources.clone();
    let mut source_sets = Vec::new();
    let mut source_set_positions = BTreeMap::new();
    let included = loop {
        if let Some(cycle_start) = source_set_positions.get(&sources).copied() {
            let cyclic_source_indices = source_sets[cycle_start..]
                .iter()
                .flat_map(BTreeSet::iter)
                .copied()
                .chain(sources.iter().copied())
                .collect::<BTreeSet<_>>();
            return Ok(CausalGrantStatus::Conflict(revocation_cycle_conflict(
                entries,
                &index_by_coord,
                &raw,
                &all_sources,
                &cyclic_source_indices,
                &causal_order,
                raw_checkpoint_heads,
                effective_checkpoint_frontier,
                seed_included,
                &seed_state,
            )?));
        }
        source_set_positions.insert(sources.clone(), source_sets.len());
        source_sets.push(sources.clone());
        let cap_sources = cap_sources(entries, &raw, &sources);
        let included = included_entries(entries, &index_by_coord, &cap_sources, &checkpoint_set);
        let next_sources = all_sources
            .intersection(&included)
            .copied()
            .collect::<BTreeSet<_>>();
        if next_sources == sources {
            break included;
        }
        sources = next_sources;
    };

    let effective = effective_state(entries, &causal_order, &included, &seed_state)?;

    let mut active_by_member = BTreeMap::<String, Vec<MembershipGrantId>>::new();
    for (grant, state) in &effective.grants {
        let Some(record) = state.active() else {
            continue;
        };
        active_by_member
            .entry(record.member_pubkey.clone())
            .or_default()
            .push(grant.clone());
    }
    if let Some((member_pubkey, grants)) = active_by_member
        .into_iter()
        .find(|(_, grants)| grants.len() > 1)
    {
        let conflicting_grants = grants
            .iter()
            .map(|grant| {
                (
                    grant.clone(),
                    effective.grants[grant]
                        .active()
                        .expect("conflicting grant is active")
                        .clone(),
                )
            })
            .collect();
        let uncontested_grants = effective
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                (!grants.contains(grant))
                    .then(|| state.active().map(|record| (grant.clone(), record.clone())))
                    .flatten()
            })
            .collect();
        let seed_included = seed_included.iter().cloned().collect::<Vec<_>>();
        let reduced = reduced_from_state(entries, effective, included, &seed_included);
        return Ok(CausalGrantStatus::Conflict(
            CausalGrantConflict::ConcurrentMemberAssignments {
                raw_heads: frontier_with_checkpoint(
                    entries,
                    0..entries.len(),
                    raw_checkpoint_heads,
                ),
                effective_frontier: frontier_with_checkpoint(
                    entries,
                    reduced
                        .included
                        .iter()
                        .filter_map(|coord| index_by_coord.get(coord).copied()),
                    effective_checkpoint_frontier,
                ),
                member_pubkey,
                conflicting_grants,
                uncontested_grants,
                reduced,
            },
        ));
    }
    require_owner(&effective)?;

    let seed_included = seed_included.iter().cloned().collect::<Vec<_>>();
    Ok(CausalGrantStatus::Resolved(reduced_from_state(
        entries,
        effective,
        included,
        &seed_included,
    )))
}

fn effective_state<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    causal_order: &[usize],
    included: &BTreeSet<usize>,
    seed: &CausalState<C, A>,
) -> Result<CausalState<C, A>, CausalGrantError<C>> {
    let mut effective = seed.clone();
    for index in causal_order {
        if included.contains(index) {
            effective.apply(
                &entries[*index].coord,
                &entries[*index].change,
                *index,
                false,
            )?;
        }
    }
    Ok(effective)
}

fn reduced_from_state<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    state: CausalState<C, A>,
    included: BTreeSet<usize>,
    checkpoint_heads: &[C],
) -> ReducedGrants<C, A> {
    ReducedGrants {
        grants: state.grants,
        included: checkpoint_heads
            .iter()
            .cloned()
            .chain(
                included
                    .into_iter()
                    .map(|index| entries[index].coord.clone()),
            )
            .collect(),
    }
}

fn frontier_with_checkpoint<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    indices: impl IntoIterator<Item = usize>,
    checkpoint_heads: &[C],
) -> Vec<C> {
    stream_frontier(
        checkpoint_heads.iter().cloned().chain(
            indices
                .into_iter()
                .map(|index| entries[index].coord.clone()),
        ),
    )
}

fn revocation_cycle_conflict<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    index_by_coord: &BTreeMap<C, usize>,
    raw: &CausalState<C, A>,
    all_sources: &BTreeSet<usize>,
    cyclic_source_indices: &BTreeSet<usize>,
    causal_order: &[usize],
    raw_checkpoint_heads: &[C],
    effective_checkpoint_frontier: &[C],
    seed_included: &BTreeSet<C>,
    seed: &CausalState<C, A>,
) -> Result<CausalGrantConflict<C, A>, CausalGrantError<C>> {
    if cyclic_source_indices.len() > MAX_CYCLIC_REVOCATION_SOURCES {
        return Err(CausalGrantError::RevocationCycleTooWide {
            sources: cyclic_source_indices.len(),
            maximum: MAX_CYCLIC_REVOCATION_SOURCES,
        });
    }
    let checkpoint_set = seed_included.clone();
    let mandatory_sources = all_sources
        .difference(cyclic_source_indices)
        .copied()
        .collect::<BTreeSet<_>>();
    let attacks = all_sources
        .iter()
        .map(|source| {
            let selected = BTreeSet::from([*source]);
            let included = included_entries(
                entries,
                index_by_coord,
                &cap_sources(entries, raw, &selected),
                &checkpoint_set,
            );
            (
                *source,
                all_sources.difference(&included).copied().collect(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let (fixed_source_sets, _) =
        fixed_sets_from_attack_graph(&attacks, &mandatory_sources, cyclic_source_indices.len())
            .map_err(|sources| CausalGrantError::RevocationCycleTooWide {
                sources,
                maximum: MAX_CYCLIC_REVOCATION_SOURCES,
            })?;
    let mut branches = Vec::new();
    let raw_heads = frontier_with_checkpoint(entries, 0..entries.len(), raw_checkpoint_heads);
    for sources in fixed_source_sets {
        let included = included_entries(
            entries,
            index_by_coord,
            &cap_sources(entries, raw, &sources),
            &checkpoint_set,
        );
        let next_sources = all_sources
            .intersection(&included)
            .copied()
            .collect::<BTreeSet<_>>();
        if next_sources != sources {
            continue;
        }
        let effective = effective_state(entries, causal_order, &included, seed)?;
        if !has_owner(&effective) || has_concurrent_assignments(&effective) {
            continue;
        }
        let reduced = reduced_from_state(
            entries,
            effective,
            included.clone(),
            &seed_included.iter().cloned().collect::<Vec<_>>(),
        );
        branches.push(CausalGrantBranch {
            raw_heads: raw_heads
                .iter()
                .filter(|coord| {
                    seed_included.contains(*coord)
                        || index_by_coord
                            .get(*coord)
                            .is_some_and(|index| included.contains(index))
                })
                .cloned()
                .collect(),
            effective_frontier: frontier_with_checkpoint(
                entries,
                included.iter().copied(),
                effective_checkpoint_frontier,
            ),
            reduced,
        });
    }
    let branch_inclusions = branches
        .iter()
        .map(|branch| branch.reduced.included.clone())
        .collect::<Vec<_>>();
    branches.retain(|branch| {
        !branch_inclusions.iter().any(|other| {
            branch.reduced.included != *other && branch.reduced.included.is_subset(other)
        })
    });
    branches.sort_by(|left, right| left.effective_frontier.cmp(&right.effective_frontier));

    let involved_owner_grants = cyclic_source_indices
        .iter()
        .flat_map(|index| {
            entries[*index]
                .change
                .removed()
                .into_iter()
                .flat_map(|(removed, _)| removed.iter())
        })
        .filter(|grant| {
            raw.grants
                .get(*grant)
                .is_some_and(|state| state.record().assignment.is_owner())
        })
        .cloned()
        .collect();
    let mut cyclic_sources = cyclic_source_indices
        .iter()
        .map(|index| entries[*index].coord.clone())
        .collect::<Vec<_>>();
    cyclic_sources.sort();
    Ok(CausalGrantConflict::RevocationCycle {
        raw_heads,
        cyclic_sources,
        involved_owner_grants,
        maximal_valid_branches: branches,
    })
}

fn has_concurrent_assignments<C: CausalCoordinate, A: CausalAssignment>(
    state: &CausalState<C, A>,
) -> bool {
    super::has_concurrent_assignments(&state.grants, |record| &record.member_pubkey)
}

fn has_owner<C: CausalCoordinate, A: CausalAssignment>(state: &CausalState<C, A>) -> bool {
    super::has_active_owner(&state.grants, |record| record.assignment.is_owner())
}

fn cap_sources<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    raw: &CausalState<C, A>,
    source_indices: &BTreeSet<usize>,
) -> BTreeMap<MembershipGrantId, Vec<(usize, OwnerGrantBarrier<C>)>> {
    let mut sources = BTreeMap::<MembershipGrantId, Vec<(usize, OwnerGrantBarrier<C>)>>::new();
    for index in source_indices {
        let entry = &entries[*index];
        let Some((removed, barriers)) = entry.change.removed() else {
            continue;
        };
        for grant in removed {
            if raw
                .grants
                .get(grant)
                .is_some_and(|state| state.record().assignment.is_owner())
            {
                sources.entry(grant.clone()).or_default().push((
                    *index,
                    barriers
                        .get(grant)
                        .expect("Owner barriers were validated")
                        .clone(),
                ));
            }
        }
    }
    sources
}

fn included_entries<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    index_by_coord: &BTreeMap<C, usize>,
    cap_sources: &BTreeMap<MembershipGrantId, Vec<(usize, OwnerGrantBarrier<C>)>>,
    checkpoint: &BTreeSet<C>,
) -> BTreeSet<usize> {
    let mut included = (0..entries.len())
        .filter(|index| {
            let entry = &entries[*index];
            !cap_sources
                .get(entry.coord.author_owner_grant())
                .is_some_and(|sources| {
                    sources.iter().any(|(source_index, barrier)| {
                        let own_self_revocation = *source_index == *index
                            && entry.change.removes_grant(entry.coord.author_owner_grant());
                        !own_self_revocation && !barrier.includes(&entry.coord)
                    })
                })
        })
        .collect::<BTreeSet<_>>();
    loop {
        let descendants = included
            .iter()
            .copied()
            .filter(|index| {
                entries[*index].dependencies.values().any(|dependency| {
                    !checkpoint.contains(dependency)
                        && !index_by_coord
                            .get(dependency)
                            .is_some_and(|index| included.contains(index))
                })
            })
            .collect::<Vec<_>>();
        if descendants.is_empty() {
            return included;
        }
        for index in descendants {
            included.remove(&index);
        }
    }
}

fn validate_barriers<C: CausalCoordinate, A: CausalAssignment>(
    index: usize,
    entry: &CausalEntry<C, A>,
    state: &CausalState<C, A>,
) -> Result<(), CausalGrantError<C>> {
    let Some((removed, barriers)) = entry.change.removed() else {
        return Ok(());
    };
    for grant in barriers.keys() {
        if !removed.contains(grant)
            || !state
                .grants
                .get(grant)
                .and_then(GrantState::active)
                .is_some_and(|record| record.assignment.is_owner())
        {
            return Err(CausalGrantError::InvalidOwnerRevocationBarrier {
                index,
                grant: grant.clone(),
            });
        }
    }
    for grant in removed {
        if !state
            .grants
            .get(grant)
            .and_then(GrantState::active)
            .is_some_and(|record| record.assignment.is_owner())
        {
            continue;
        }
        let Some(barrier) = barriers.get(grant) else {
            return Err(CausalGrantError::MissingOwnerRevocationBarrier {
                index,
                grant: grant.clone(),
            });
        };
        let observed = entry
            .dependencies
            .iter()
            .filter(|(_, coord)| coord.author_owner_grant() == grant)
            .map(|(stream, coord)| (stream.clone(), coord.clone()))
            .collect::<BTreeMap<_, _>>();
        if barrier.observed_streams != observed {
            return Err(CausalGrantError::InvalidOwnerRevocationBarrier {
                index,
                grant: grant.clone(),
            });
        }
    }
    Ok(())
}

fn require_owner<C: CausalCoordinate, A: CausalAssignment>(
    state: &CausalState<C, A>,
) -> Result<(), CausalGrantError<C>> {
    if has_owner(state) {
        Ok(())
    } else {
        Err(CausalGrantError::NoActiveOwner)
    }
}
