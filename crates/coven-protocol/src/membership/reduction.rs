use super::*;

pub(super) fn validate_membership_retirement_barriers(
    entries: &[MembershipEntry],
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        let (retired, barriers) = match &entry.change {
            StoreAuthorityChange::SetMember {
                replaces,
                retirement_barriers,
                ..
            } => (replaces, retirement_barriers),
            StoreAuthorityChange::RemoveMember {
                removes,
                retirement_barriers,
                ..
            } => (removes, retirement_barriers),
            StoreAuthorityChange::Founder { .. }
            | StoreAuthorityChange::DeviceRegistrationActivation { .. }
            | StoreAuthorityChange::DeviceExclusionProposal { .. }
            | StoreAuthorityChange::DeviceExclusionOutcome { .. }
            | StoreAuthorityChange::ProviderAdmin => continue,
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
        let reduced = reduce_store_membership(&causal_past)?;
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

pub(super) fn validate_membership_sealed_keys(
    entries: &[MembershipEntry],
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        let (removed_pubkey, rotation_generation, sealed_keys) = match &entry.change {
            StoreAuthorityChange::SetMember { sealed_key, .. } => {
                if sealed_key.validate().is_err() {
                    return Err(MembershipError::InvalidSealedKeys(index));
                }
                continue;
            }
            StoreAuthorityChange::RemoveMember {
                user_pubkey,
                rotation_generation,
                sealed_keys,
                ..
            } => (user_pubkey, *rotation_generation, sealed_keys),
            StoreAuthorityChange::Founder { .. }
            | StoreAuthorityChange::DeviceRegistrationActivation { .. }
            | StoreAuthorityChange::DeviceExclusionProposal { .. }
            | StoreAuthorityChange::DeviceExclusionOutcome { .. }
            | StoreAuthorityChange::ProviderAdmin => continue,
        };
        let causal_generation = membership_causal_generation(entries, &entry.dependencies);
        if causal_generation.checked_add(1) != Some(rotation_generation)
            || sealed_keys.contains_key(removed_pubkey)
            || sealed_keys.values().any(|key| key.validate().is_err())
        {
            return Err(MembershipError::InvalidSealedKeys(index));
        }
        let included = causal_grants::history_closure(entries, &entry.dependencies);
        let causal_past = entries
            .iter()
            .filter(|candidate| included.contains(&candidate.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let reduced = reduce_store_membership(&causal_past)?;
        let expected_recipients = reduced
            .grants
            .values()
            .filter_map(GrantState::active)
            .filter(|record| record.member_pubkey != *removed_pubkey)
            .map(|record| record.member_pubkey.clone())
            .collect::<BTreeSet<_>>();
        if expected_recipients != sealed_keys.keys().cloned().collect::<BTreeSet<_>>() {
            return Err(MembershipError::InvalidSealedKeys(index));
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
        .filter_map(|candidate| match &candidate.change {
            StoreAuthorityChange::RemoveMember {
                rotation_generation,
                ..
            } => Some(*rotation_generation),
            StoreAuthorityChange::Founder { .. }
            | StoreAuthorityChange::SetMember { .. }
            | StoreAuthorityChange::DeviceRegistrationActivation { .. }
            | StoreAuthorityChange::DeviceExclusionProposal { .. }
            | StoreAuthorityChange::DeviceExclusionOutcome { .. }
            | StoreAuthorityChange::ProviderAdmin => None,
        })
        .max()
        .unwrap_or(coven_keys::encryption::INITIAL_KEY_GENERATION)
}

pub(super) fn reduce_store_membership(
    entries: &[MembershipEntry],
) -> Result<causal_grants::ReducedGrants<MembershipCoord, StoreAssignment>, MembershipError> {
    let normalized = normalize_store_membership(entries);
    match causal_grants::reduce(&normalized).map_err(map_store_causal_error)? {
        CausalGrantStatus::Resolved(reduced) => Ok(reduced),
        CausalGrantStatus::Conflict(_) => Err(MembershipError::Conflict),
    }
}

pub(super) fn validate_provider_admin_controls(
    entries: &[MembershipEntry],
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        let Some(crate::provider::ProviderAdminMembershipChange { owner_barriers, .. }) =
            &entry.provider_admin
        else {
            continue;
        };
        let included = causal_grants::history_closure(entries, &entry.dependencies);
        let causal_past = entries
            .iter()
            .filter(|candidate| included.contains(&candidate.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let reduced = reduce_store_membership(&causal_past)?;
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
                StoreAuthorityChange::Founder {
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
                StoreAuthorityChange::SetMember {
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
                StoreAuthorityChange::RemoveMember {
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
                StoreAuthorityChange::ProviderAdmin
                | StoreAuthorityChange::DeviceRegistrationActivation { .. }
                | StoreAuthorityChange::DeviceExclusionProposal { .. }
                | StoreAuthorityChange::DeviceExclusionOutcome { .. } => CausalChange::Control,
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

pub(super) fn resolved_store_membership(
    reduced: &causal_grants::ReducedGrants<MembershipCoord, StoreAssignment>,
    provider_admin: crate::provider::ProviderAdminResolution,
    entries: &[MembershipEntry],
) -> Result<ResolvedStoreMembership, MembershipError> {
    let grants = reduced
        .grants
        .iter()
        .map(|(grant, state)| -> Result<_, MembershipError> {
            Ok((grant.clone(), map_store_grant_state(grant, state, entries)?))
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
    entries: &[MembershipEntry],
) -> Result<GrantState<MembershipGrantRecord, MembershipGrantRetirement>, MembershipError> {
    let causal_record = state.record();
    let record = MembershipGrantRecord {
        member_pubkey: causal_record.member_pubkey.clone(),
        role: causal_record.assignment.role.clone(),
        provider_account_email: causal_record.assignment.provider_account_email.clone(),
        creation_authority: causal_record.creation.clone(),
    };
    causal_grants::try_map_grant_state(state, record, |coord, _owner_barrier| {
        Ok(MembershipGrantRetirement {
            authority: coord.clone(),
            barrier: membership_retirement_barrier(entries, coord, grant).ok_or_else(|| {
                MembershipError::MissingRetirementBarrier {
                    grant: grant.clone(),
                    authority: Box::new(coord.clone()),
                }
            })?,
        })
    })
}

pub(super) fn membership_retirement_barrier(
    entries: &[MembershipEntry],
    authority: &MembershipCoord,
    grant: &MembershipGrantId,
) -> Option<MergeMembershipGrantRetirementBarrier> {
    let entry = entries.iter().find(|entry| entry.coord() == *authority)?;
    let barriers = match &entry.change {
        StoreAuthorityChange::SetMember {
            retirement_barriers,
            ..
        }
        | StoreAuthorityChange::RemoveMember {
            retirement_barriers,
            ..
        } => retirement_barriers,
        StoreAuthorityChange::Founder { .. }
        | StoreAuthorityChange::DeviceRegistrationActivation { .. }
        | StoreAuthorityChange::DeviceExclusionProposal { .. }
        | StoreAuthorityChange::DeviceExclusionOutcome { .. }
        | StoreAuthorityChange::ProviderAdmin => return None,
    };
    barriers.get(grant).cloned()
}

pub(super) fn store_membership_state_hash(
    grants: &BTreeMap<
        MembershipGrantId,
        GrantState<MembershipGrantRecord, MembershipGrantRetirement>,
    >,
    provider_admin: &crate::provider::ProviderAdminResolution,
) -> ObjectHash {
    #[derive(Serialize)]
    struct State<'a> {
        domain: &'static str,
        grants: &'a BTreeMap<
            MembershipGrantId,
            GrantState<MembershipGrantRecord, MembershipGrantRetirement>,
        >,
        provider_admin: &'a crate::provider::ProviderAdminResolution,
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
    }
}
