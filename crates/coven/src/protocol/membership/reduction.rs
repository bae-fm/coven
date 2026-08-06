use super::*;

pub(super) fn validate_membership_retirement_barriers(
    entries: &[MembershipEntry],
    checkpoint: Option<&MembershipResolutionCheckpoint>,
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        if checkpoint.is_some_and(|checkpoint| {
            checkpoint.raw_heads.iter().any(|head| {
                head.stream_key() == entry.coord().stream_key() && entry.seq <= head.seq
            })
        }) {
            continue;
        }
        let (retired, barriers) = match &entry.change {
            MembershipChange::SetMember {
                replaces,
                retirement_barriers,
                ..
            } => (replaces, retirement_barriers),
            MembershipChange::RemoveMember {
                removes,
                retirement_barriers,
                ..
            } => (removes, retirement_barriers),
            MembershipChange::Founder { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => continue,
        };
        if retired != &barriers.keys().cloned().collect::<BTreeSet<_>>() {
            let barrier_grants = barriers.keys().cloned().collect::<BTreeSet<_>>();
            let grant = retired
                .symmetric_difference(&barrier_grants)
                .next()
                .cloned()
                .expect("unequal retirement and barrier grant sets have a difference");
            return Err(MembershipError::InvalidOwnerRevocationBarrier { index, grant });
        }
        let included = causal_grants::history_closure(entries, &entry.dependencies);
        let causal_past = entries
            .iter()
            .filter(|candidate| included.contains(&candidate.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let reduced = match checkpoint {
            Some(checkpoint) => reduce_store_membership_from_checkpoint(&causal_past, checkpoint)?,
            None => reduce_store_membership(&causal_past)?,
        };
        let CausalGrantStatus::Resolved(reduced) = reduced else {
            return Err(MembershipError::Conflict);
        };
        for (grant, barrier) in barriers {
            let Some(record) = reduced.grants.get(grant).and_then(GrantState::active) else {
                return Err(MembershipError::InvalidOwnerRevocationBarrier {
                    index,
                    grant: grant.clone(),
                });
            };
            let expected_streams = entry
                .dependencies
                .iter()
                .filter(|coord| coord.author_owner_grant == *grant)
                .cloned()
                .collect::<Vec<_>>();
            let shape_matches = matches!(
                (record.assignment.is_owner(), barrier),
                (true, MergeMembershipGrantRetirementBarrier::Owner { .. })
                    | (
                        false,
                        MergeMembershipGrantRetirementBarrier::NonOwner { .. }
                    )
            );
            if !shape_matches || barrier.author_streams().observed_streams != expected_streams {
                return Err(MembershipError::InvalidOwnerRevocationBarrier {
                    index,
                    grant: grant.clone(),
                });
            }
        }
    }
    Ok(())
}

pub(super) fn validate_membership_wrapped_keys(
    entries: &[MembershipEntry],
    checkpoint: Option<&MembershipResolutionCheckpoint>,
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        let included = causal_grants::history_closure(entries, &entry.dependencies);
        let causal_generation = membership_causal_generation(entries, &entry.dependencies);
        let references = match &entry.change {
            MembershipChange::SetMember {
                user_pubkey,
                wrapped_key,
                ..
            } => {
                if wrapped_key.owner_pubkey != entry.author_pubkey
                    || wrapped_key.recipient_pubkey != *user_pubkey
                    || wrapped_key.generation != causal_generation
                    || wrapped_key.validate_identity().is_err()
                {
                    return Err(MembershipError::InvalidWrappedKeys(index));
                }
                continue;
            }
            MembershipChange::RemoveMember {
                user_pubkey,
                wrapped_keys,
                ..
            } => (user_pubkey, wrapped_keys),
            MembershipChange::Founder { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => continue,
        };
        let (removed_pubkey, wrapped_keys) = references;
        let rotation_generation = wrapped_keys.first().map(|reference| reference.generation);
        if causal_generation.checked_add(1) != rotation_generation
            || !wrapped_keys.windows(2).all(|pair| pair[0] < pair[1])
            || wrapped_keys.iter().any(|reference| {
                reference.owner_pubkey != entry.author_pubkey
                    || reference.recipient_pubkey == *removed_pubkey
                    || Some(reference.generation) != rotation_generation
                    || reference.validate_identity().is_err()
            })
        {
            return Err(MembershipError::InvalidWrappedKeys(index));
        }
        let causal_past = entries
            .iter()
            .filter(|candidate| included.contains(&candidate.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let precedes_checkpoint = checkpoint.is_some_and(|checkpoint| {
            checkpoint.raw_heads.iter().any(|head| {
                head.stream_key() == entry.coord().stream_key() && entry.seq <= head.seq
            })
        });
        let reduced = match (checkpoint, precedes_checkpoint) {
            (Some(checkpoint), false) => {
                reduce_store_membership_from_checkpoint(&causal_past, checkpoint)?
            }
            (None, _) | (Some(_), true) => reduce_store_membership(&causal_past)?,
        };
        let CausalGrantStatus::Resolved(reduced) = reduced else {
            return Err(MembershipError::InvalidWrappedKeys(index));
        };
        let expected_recipients = reduced
            .grants
            .values()
            .filter_map(GrantState::active)
            .filter(|record| record.member_pubkey != *removed_pubkey)
            .map(|record| record.member_pubkey.clone())
            .collect::<BTreeSet<_>>();
        let actual_recipients = wrapped_keys
            .iter()
            .map(|reference| reference.recipient_pubkey.clone())
            .collect::<BTreeSet<_>>();
        if expected_recipients != actual_recipients || actual_recipients.len() != wrapped_keys.len()
        {
            return Err(MembershipError::InvalidWrappedKeys(index));
        }
    }
    Ok(())
}

pub(super) fn membership_causal_generation(
    entries: &[MembershipEntry],
    dependencies: &[MembershipCoord],
) -> u64 {
    let included = causal_grants::history_closure(entries, dependencies);
    entries
        .iter()
        .filter(|candidate| included.contains(&candidate.coord()))
        .flat_map(|candidate| match &candidate.change {
            MembershipChange::SetMember { wrapped_key, .. } => std::slice::from_ref(wrapped_key),
            MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys.as_slice(),
            MembershipChange::Founder { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => &[],
        })
        .map(|reference| reference.generation)
        .max()
        .unwrap_or(coven_keys::encryption::INITIAL_KEY_GENERATION)
}

pub(super) fn reduce_store_membership(
    entries: &[MembershipEntry],
) -> Result<CausalGrantStatus<MembershipCoord, StoreAssignment>, MembershipError> {
    let normalized = normalize_store_membership(entries);
    causal_grants::reduce(&normalized).map_err(map_store_causal_error)
}

pub(super) fn reduce_store_membership_from_checkpoint(
    entries: &[MembershipEntry],
    checkpoint: &MembershipResolutionCheckpoint,
) -> Result<CausalGrantStatus<MembershipCoord, StoreAssignment>, MembershipError> {
    let suffix = causal_grants::entries_beyond_checkpoint(entries, &checkpoint.raw_heads)
        .cloned()
        .collect::<Vec<_>>();
    let normalized = normalize_store_membership(&suffix);
    let seeds = causal_grants::map_checkpoint_grants(
        &checkpoint.grants,
        |record| causal_grants::CausalSeedGrant {
            member_pubkey: record.member_pubkey.clone(),
            assignment: StoreAssignment {
                role: record.role.clone(),
                provider_account_email: record.provider_account_email.clone(),
            },
        },
        || (),
    );
    causal_grants::reduce_from_checkpoint(
        &normalized,
        &checkpoint.raw_heads,
        &checkpoint.effective_frontier,
        &seeds,
        &checkpoint.included,
    )
    .map_err(map_store_causal_error)
}

pub(super) fn validate_provider_admin_controls(
    entries: &[MembershipEntry],
    checkpoint: Option<&MembershipResolutionCheckpoint>,
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        let Some(crate::protocol::provider::ProviderAdminMembershipChange {
            owner_barriers, ..
        }) = &entry.provider_admin
        else {
            continue;
        };
        let included = causal_grants::history_closure(entries, &entry.dependencies);
        let causal_past = entries
            .iter()
            .filter(|candidate| included.contains(&candidate.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let reduced = match checkpoint {
            Some(checkpoint) => reduce_store_membership_from_checkpoint(&causal_past, checkpoint)?,
            None => reduce_store_membership(&causal_past)?,
        };
        let CausalGrantStatus::Resolved(reduced) = reduced else {
            return Err(MembershipError::InvalidProviderAdminChange(index));
        };
        let expected = reduced
            .grants
            .iter()
            .filter(|(_, state)| {
                state
                    .active()
                    .is_some_and(|record| record.assignment.is_owner())
            })
            .map(|(grant_id, _)| {
                let observed_streams = entry
                    .dependencies
                    .iter()
                    .filter(|coord| coord.author_owner_grant == *grant_id)
                    .cloned()
                    .collect();
                (grant_id.clone(), OwnerStreamBarrier { observed_streams })
            })
            .collect::<BTreeMap<_, _>>();
        if *owner_barriers != expected {
            return Err(MembershipError::InvalidProviderAdminChange(index));
        }
    }
    Ok(())
}

pub(super) fn normalize_store_membership(
    entries: &[MembershipEntry],
) -> Vec<CausalEntry<MembershipCoord, StoreAssignment>> {
    entries
        .iter()
        .map(|entry| {
            let dependencies = entry
                .dependencies
                .iter()
                .cloned()
                .map(|coord| (coord.stream_key(), coord))
                .collect();
            let change = match &entry.change {
                MembershipChange::Founder {
                    creation_id,
                    owner_pubkey,
                    owner_grant_id,
                    ..
                } => CausalChange::Founder {
                    member_pubkey: owner_pubkey.clone(),
                    grant_id: owner_grant_id.clone(),
                    assignment: StoreAssignment {
                        role: StoreMembershipRoleGrant::Owner {
                            recovery: OwnerRecoveryAnchorRef::Founder {
                                creation_id: *creation_id,
                            },
                        },
                        provider_account_email: None,
                    },
                },
                MembershipChange::SetMember {
                    user_pubkey,
                    provider_account_email,
                    role,
                    grant_id,
                    membership: _,
                    replaces,
                    retirement_barriers,
                    ..
                } => CausalChange::SetMember {
                    member_pubkey: user_pubkey.clone(),
                    assignment: StoreAssignment {
                        role: role.clone(),
                        provider_account_email: provider_account_email.clone(),
                    },
                    grant_id: grant_id.clone(),
                    replaces: replaces.clone(),
                    owner_barriers: retirement_barriers
                        .iter()
                        .filter_map(|(grant, barrier)| {
                            barrier
                                .owner_stream_barrier()
                                .map(|barrier| (grant.clone(), barrier))
                        })
                        .collect(),
                },
                MembershipChange::RemoveMember {
                    user_pubkey,
                    removes,
                    retirement_barriers,
                    ..
                } => CausalChange::RemoveMember {
                    member_pubkey: user_pubkey.clone(),
                    removes: removes.clone(),
                    owner_barriers: retirement_barriers
                        .iter()
                        .filter_map(|(grant, barrier)| {
                            barrier
                                .owner_stream_barrier()
                                .map(|barrier| (grant.clone(), barrier))
                        })
                        .collect(),
                },
                MembershipChange::ProviderAdmin => CausalChange::Control,
                MembershipChange::ResolutionActivation { .. } => CausalChange::ResolutionActivation,
            };
            CausalEntry {
                coord: entry.coord(),
                previous_hash: entry.previous_hash,
                dependencies,
                change,
            }
        })
        .collect()
}

pub(super) fn map_store_grants(
    grants: BTreeMap<
        MembershipGrantId,
        causal_grants::GrantRecord<MembershipCoord, StoreAssignment>,
    >,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    >,
) -> Result<BTreeMap<MembershipGrantId, MembershipGrantRecord>, MembershipError> {
    grants
        .into_iter()
        .map(|(grant, record)| -> Result<_, MembershipError> {
            let creation_authority =
                membership_creation_authority(&grant, record.creation, checkpoint)?;
            Ok((
                grant,
                MembershipGrantRecord {
                    member_pubkey: record.member_pubkey,
                    role: record.assignment.role,
                    provider_account_email: record.assignment.provider_account_email,
                    creation_authority,
                },
            ))
        })
        .collect()
}

pub(super) fn resolved_store_membership(
    reduced: &causal_grants::ReducedGrants<MembershipCoord, StoreAssignment>,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    >,
    provider_admin: crate::protocol::provider::ProviderAdminResolution,
    entries: &[MembershipEntry],
) -> Result<ResolvedStoreMembership, MembershipError> {
    let grants = reduced
        .grants
        .iter()
        .map(|(grant, state)| -> Result<_, MembershipError> {
            Ok((
                grant.clone(),
                map_store_grant_state(grant, state, checkpoint, entries)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let state_hash = store_membership_state_hash(&grants, &provider_admin);
    Ok(ResolvedStoreMembership {
        grants,
        provider_admin,
        state_hash,
    })
}

pub(super) fn map_store_grant_state(
    grant: &MembershipGrantId,
    state: &GrantState<
        causal_grants::GrantRecord<MembershipCoord, StoreAssignment>,
        causal_grants::CausalGrantRetirement<MembershipCoord>,
    >,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    >,
    entries: &[MembershipEntry],
) -> Result<GrantState<MembershipGrantRecord, MembershipGrantRetirement>, MembershipError> {
    let causal_record = state.record();
    let record = MembershipGrantRecord {
        member_pubkey: causal_record.member_pubkey.clone(),
        role: causal_record.assignment.role.clone(),
        provider_account_email: causal_record.assignment.provider_account_email.clone(),
        creation_authority: membership_creation_authority(
            grant,
            causal_record.creation.clone(),
            checkpoint,
        )?,
    };
    causal_grants::try_map_grant_state(
        state,
        record,
        checkpoint
            .and_then(|grants| grants.get(grant))
            .and_then(GrantState::retirements),
        || MembershipError::MissingCheckpointRetirementEvidence {
            grant: grant.clone(),
        },
        |coord, _owner_barrier| {
            Ok(MembershipGrantRetirement::Entry {
                authority: coord.clone(),
                barrier: membership_retirement_barrier(entries, coord, grant).ok_or_else(|| {
                    MembershipError::MissingRetirementBarrier {
                        grant: grant.clone(),
                        authority: Box::new(coord.clone()),
                    }
                })?,
            })
        },
    )
}

pub(super) fn membership_retirement_barrier(
    entries: &[MembershipEntry],
    authority: &MembershipCoord,
    grant: &MembershipGrantId,
) -> Option<MergeMembershipGrantRetirementBarrier> {
    let entry = entries.iter().find(|entry| entry.coord() == *authority)?;
    let barriers = match &entry.change {
        MembershipChange::SetMember {
            retirement_barriers,
            ..
        }
        | MembershipChange::RemoveMember {
            retirement_barriers,
            ..
        } => retirement_barriers,
        MembershipChange::Founder { .. }
        | MembershipChange::ProviderAdmin
        | MembershipChange::ResolutionActivation { .. } => return None,
    };
    barriers.get(grant).cloned()
}

pub(super) fn membership_creation_authority(
    grant: &MembershipGrantId,
    creation: causal_grants::CausalGrantCreation<MembershipCoord>,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    >,
) -> Result<MembershipGrantCreationAuthority, MembershipError> {
    match creation {
        causal_grants::CausalGrantCreation::Entry(coord) => {
            Ok(MembershipGrantCreationAuthority::Entry(coord))
        }
        causal_grants::CausalGrantCreation::Checkpoint => checkpoint
            .and_then(|grants| grants.get(grant))
            .ok_or_else(|| MembershipError::MissingCheckpointGrant {
                grant: grant.clone(),
            })
            .map(|state| state.record().creation_authority.clone()),
    }
}

pub(super) fn store_membership_state_hash(
    grants: &BTreeMap<
        MembershipGrantId,
        GrantState<MembershipGrantRecord, MembershipGrantRetirement>,
    >,
    provider_admin: &crate::protocol::provider::ProviderAdminResolution,
) -> ObjectHash {
    #[derive(Serialize)]
    struct State<'a> {
        domain: &'static str,
        grants: &'a BTreeMap<
            MembershipGrantId,
            GrantState<MembershipGrantRecord, MembershipGrantRetirement>,
        >,
        provider_admin: &'a crate::protocol::provider::ProviderAdminResolution,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&State {
            domain: "coven.store-membership-state.v2",
            grants,
            provider_admin,
        })
        .expect("Store membership state serialization cannot fail"),
    )
}

pub(super) fn membership_assignment_conflict_hash(
    heads: &[MembershipHeadRef],
    member_pubkey: &str,
    conflicting_grants: &BTreeMap<
        MembershipGrantId,
        causal_grants::GrantRecord<MembershipCoord, StoreAssignment>,
    >,
) -> ObjectHash {
    #[derive(Serialize)]
    struct Conflict<'a> {
        domain: &'static str,
        heads: &'a [MembershipHeadRef],
        member_pubkey: &'a str,
        conflicting_grant_ids: Vec<&'a MembershipGrantId>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&Conflict {
            domain: "coven.store-membership-assignment-conflict.v1",
            heads,
            member_pubkey,
            conflicting_grant_ids: conflicting_grants.keys().collect(),
        })
        .expect("Store membership conflict serialization cannot fail"),
    )
}

pub(super) fn membership_revocation_conflict_hash(
    heads: &[MembershipHeadRef],
    cyclic_sources: &[MembershipCoord],
    involved_owner_grants: &BTreeSet<MembershipGrantId>,
) -> ObjectHash {
    #[derive(Serialize)]
    struct Conflict<'a> {
        domain: &'static str,
        heads: &'a [MembershipHeadRef],
        cyclic_sources: &'a [MembershipCoord],
        involved_owner_grants: &'a BTreeSet<MembershipGrantId>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&Conflict {
            domain: "coven.store-membership-revocation-conflict.v1",
            heads,
            cyclic_sources,
            involved_owner_grants,
        })
        .expect("Store membership revocation conflict serialization cannot fail"),
    )
}

pub(super) fn shared_store_barrier(
    barrier: &StoreGrantStreamBarrier,
) -> OwnerGrantBarrier<MembershipCoord> {
    let observed_streams = barrier
        .observed_streams
        .iter()
        .cloned()
        .map(|coord| (coord.stream_key(), coord))
        .collect();
    OwnerGrantBarrier { observed_streams }
}

pub(super) fn map_store_causal_error(error: CausalGrantError<MembershipCoord>) -> MembershipError {
    match error {
        CausalGrantError::Empty => MembershipError::EmptyChain,
        CausalGrantError::ConflictingSequence { stream, seq } => {
            MembershipError::ConflictingSequence {
                author: stream.author_pubkey,
                grant: stream.author_owner_grant,
                seq,
            }
        }
        CausalGrantError::MissingSequence { stream, seq } => MembershipError::MissingSequence {
            author: stream.author_pubkey,
            grant: stream.author_owner_grant,
            seq,
        },
        CausalGrantError::BrokenStreamLink {
            index,
            expected,
            actual,
        } => MembershipError::BrokenStreamLink {
            index,
            expected,
            actual,
        },
        CausalGrantError::MissingOwnDependency { index } => {
            MembershipError::MissingOwnDependency { index }
        }
        CausalGrantError::DependencyStreamMismatch { .. } => {
            unreachable!("Store dependencies are normalized from their signed coordinates")
        }
        CausalGrantError::MissingDependency { index, dependency } => {
            MembershipError::MissingDependency {
                index,
                dependency: Box::new(dependency),
            }
        }
        CausalGrantError::DependencyCycle => MembershipError::DependencyCycle,
        CausalGrantError::InvalidFounder => MembershipError::InvalidFounder,
        CausalGrantError::AuthorGrantInactive { index, grant } => {
            MembershipError::AuthorGrantInactive { index, grant }
        }
        CausalGrantError::DuplicateGrant { index, grant } => {
            MembershipError::DuplicateGrant { index, grant }
        }
        CausalGrantError::GrantOwnerMismatch { index, grant } => {
            MembershipError::GrantOwnerMismatch { index, grant }
        }
        CausalGrantError::GrantSetMismatch {
            index,
            member_pubkey,
        } => MembershipError::GrantSetMismatch {
            index,
            pubkey: member_pubkey,
        },
        CausalGrantError::EmptyRemoval { index } => MembershipError::EmptyRemoval { index },
        CausalGrantError::MissingOwnerRevocationBarrier { index, grant } => {
            MembershipError::MissingOwnerRevocationBarrier { index, grant }
        }
        CausalGrantError::InvalidOwnerRevocationBarrier { index, grant } => {
            MembershipError::InvalidOwnerRevocationBarrier { index, grant }
        }
        CausalGrantError::NoActiveOwner => MembershipError::NoActiveOwner,
        CausalGrantError::RevocationCycleTooWide { sources, maximum } => {
            MembershipError::RevocationCycleTooWide { sources, maximum }
        }
    }
}
