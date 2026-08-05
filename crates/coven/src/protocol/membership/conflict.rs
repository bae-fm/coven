use super::*;

#[derive(Clone, PartialEq, Eq)]
pub struct MembershipConflictChoice {
    pub id: String,
    pub members: Vec<MemberInfo>,
    conflict_hash: ObjectHash,
    selection: MembershipConflictSelection,
}

impl std::fmt::Debug for MembershipConflictChoice {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MembershipConflictChoice")
            .field("id", &self.id)
            .field("members", &self.members)
            .finish()
    }
}

impl MembershipConflictChoice {
    pub(crate) fn new(
        id: String,
        members: Vec<MemberInfo>,
        conflict_hash: ObjectHash,
        selection: MembershipConflictSelection,
    ) -> Self {
        Self {
            id,
            members,
            conflict_hash,
            selection,
        }
    }

    pub(crate) fn conflict_hash(&self) -> ObjectHash {
        self.conflict_hash
    }

    pub(crate) fn selection(&self) -> &MembershipConflictSelection {
        &self.selection
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipConflictInfo {
    ConcurrentMemberAssignments {
        id: String,
        member_pubkey: String,
        choices: Vec<MembershipConflictChoice>,
    },
    RevocationCycle {
        id: String,
        choices: Vec<MembershipConflictChoice>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MembershipConflict {
    ConcurrentMemberAssignments {
        conflict_hash: ObjectHash,
        heads: Vec<MembershipHeadRef>,
        effective_frontier: Vec<MembershipCoord>,
        member_pubkey: String,
        conflicting_grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
        uncontested_grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
        grants: BTreeMap<
            MembershipGrantId,
            GrantState<MembershipGrantRecord, MembershipGrantRetirement>,
        >,
        provider_admin: crate::protocol::provider::ProviderAdminResolution,
    },
    RevocationCycle {
        conflict_hash: ObjectHash,
        heads: Vec<MembershipHeadRef>,
        cyclic_sources: Vec<MembershipCoord>,
        involved_owner_grants: BTreeSet<MembershipGrantId>,
        maximal_valid_branches: Vec<StoreMembershipBranch>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MembershipStatus {
    Resolved(ResolvedStoreMembership),
    Conflict(MembershipConflict),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMembershipConflictResolutionRef {
    pub conflict_hash: ObjectHash,
    pub resolver_pubkey: String,
    pub resolution_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum MembershipConflictSelection {
    MemberAssignment { grant: MembershipGrantId },
    RevocationBranch { heads: Vec<MembershipHeadRef> },
}

/// The wire body of one membership conflict resolution. Every field here is
/// signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreMembershipConflictResolutionBody {
    pub store_root_hash: ObjectHash,
    pub conflict_hash: ObjectHash,
    pub conflicting_heads: Vec<MembershipHeadRef>,
    pub retired_owner_grants: BTreeSet<MembershipGrantId>,
    pub retirement_barriers: BTreeMap<MembershipGrantId, MergeMembershipGrantRetirementBarrier>,
    pub resolver_pubkey: String,
    pub selection: MembershipConflictSelection,
    pub replacement_grant: MembershipGrantId,
    pub replacement_membership: GrantStreamAnchor,
    pub replacement_acceptance: OwnerConflictResolutionAcceptance,
}

impl SignedBody for StoreMembershipConflictResolutionBody {
    const DOMAIN: &'static [u8] = MEMBERSHIP_RESOLUTION_DOMAIN;
}

pub(crate) type StoreMembershipConflictResolution = Signed<StoreMembershipConflictResolutionBody>;

impl StoreMembershipConflictResolution {
    pub(crate) fn resolution_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub(crate) fn resolution_ref(
        &self,
        object: ExactObjectRef,
    ) -> StoreMembershipConflictResolutionRef {
        StoreMembershipConflictResolutionRef {
            conflict_hash: self.conflict_hash,
            resolver_pubkey: self.resolver_pubkey.clone(),
            resolution_hash: self.resolution_hash(),
            object,
        }
    }

    pub(crate) fn verify_signature(&self) -> bool {
        self.replacement_grant
            == derive_store_resolution_grant(&self.conflict_hash, &self.resolver_pubkey)
            && self.replacement_acceptance.store_root_hash == self.store_root_hash
            && self.replacement_acceptance.owner_grant == self.replacement_grant
            && self.replacement_acceptance.membership == self.replacement_membership
            && self.verify_by(&self.resolver_pubkey).is_ok()
    }

    pub(crate) fn verify_against(
        &self,
        store_root_hash: ObjectHash,
        conflict: &MembershipConflict,
    ) -> bool {
        let (conflict_hash, heads, expected_retired, known_grants, resolver_is_owner) =
            match (conflict, &self.selection) {
                (
                    MembershipConflict::ConcurrentMemberAssignments {
                        conflict_hash,
                        heads,
                        conflicting_grants,
                        uncontested_grants,
                        grants,
                        ..
                    },
                    MembershipConflictSelection::MemberAssignment { grant },
                ) => (
                    conflict_hash,
                    heads,
                    uncontested_grants
                        .iter()
                        .filter_map(|(grant, record)| {
                            (record.member_pubkey == self.resolver_pubkey && record.role.is_owner())
                                .then_some(grant.clone())
                        })
                        .collect(),
                    grants.keys().cloned().collect::<BTreeSet<_>>(),
                    conflicting_grants.contains_key(grant)
                        && uncontested_grants.values().any(|record| {
                            record.member_pubkey == self.resolver_pubkey && record.role.is_owner()
                        }),
                ),
                (
                    MembershipConflict::RevocationCycle {
                        conflict_hash,
                        heads,
                        involved_owner_grants,
                        maximal_valid_branches,
                        ..
                    },
                    MembershipConflictSelection::RevocationBranch {
                        heads: selected_heads,
                    },
                ) => {
                    let Some(branch) = maximal_valid_branches
                        .iter()
                        .find(|branch| branch.heads == *selected_heads)
                    else {
                        return false;
                    };
                    let mut retired = involved_owner_grants.clone();
                    retired.extend(branch.active_grants().filter_map(|(grant, record)| {
                        (record.member_pubkey == self.resolver_pubkey && record.role.is_owner())
                            .then_some(grant.clone())
                    }));
                    (
                        conflict_hash,
                        heads,
                        retired,
                        maximal_valid_branches
                            .iter()
                            .flat_map(|branch| branch.grants.keys().cloned())
                            .collect(),
                        branch.active_grants().any(|(_, record)| {
                            record.member_pubkey == self.resolver_pubkey && record.role.is_owner()
                        }),
                    )
                }
                _ => return false,
            };
        self.store_root_hash == store_root_hash
            && self.conflict_hash == *conflict_hash
            && self.conflicting_heads == *heads
            && self.retired_owner_grants == expected_retired
            && self.retirement_barriers.len() == known_grants.len()
            && self
                .retirement_barriers
                .keys()
                .all(|grant| known_grants.contains(grant))
            && self.replacement_grant
                == derive_store_resolution_grant(conflict_hash, &self.resolver_pubkey)
            && resolver_is_owner
            && self.verify_signature()
    }
}

pub(crate) fn derive_store_resolution_grant(
    conflict_hash: &ObjectHash,
    resolver_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!("coven.store-membership-resolution-grant.v1\0{conflict_hash}\0{resolver_pubkey}")
            .as_bytes(),
    ))
}

pub(super) fn conflict_retirement_barriers(
    records: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
    effective_frontier: Vec<MembershipCoord>,
    device_state: &StoreDeviceStateRef,
) -> Result<BTreeMap<MembershipGrantId, MergeMembershipGrantRetirementBarrier>, MembershipError> {
    let recovery = device_state.recovery();
    records
        .into_iter()
        .map(|(grant, record)| {
            let mut observed_streams = effective_frontier
                .iter()
                .filter(|coord| coord.author_owner_grant == grant)
                .cloned()
                .collect::<Vec<_>>();
            observed_streams.sort_by_key(MembershipCoord::stream_key);
            observed_streams.dedup_by_key(|coord| coord.stream_key());
            let author_streams = StoreGrantStreamBarrier { observed_streams };
            let barrier = if record.role.is_owner() {
                let cursor = recovery
                    .iter()
                    .find(|cursor| cursor.owner_grant == grant)
                    .cloned()
                    .ok_or(MembershipError::MissingOwnerRecoveryState)?;
                MergeMembershipGrantRetirementBarrier::Owner {
                    barrier: MergeStoreOwnerGrantBarrier {
                        author_streams,
                        recovery: cursor,
                    },
                }
            } else {
                MergeMembershipGrantRetirementBarrier::NonOwner { author_streams }
            };
            Ok((grant, barrier))
        })
        .collect()
}

pub(crate) fn resolve_store_membership_conflict(
    store_root_hash: ObjectHash,
    conflict: &MembershipConflict,
    resolutions: &[(
        StoreMembershipConflictResolutionRef,
        StoreMembershipConflictResolution,
    )],
) -> Result<ResolvedStoreMembership, MembershipError> {
    if resolutions.is_empty() {
        return Err(MembershipError::InvalidConflictResolution);
    }
    let mut by_resolver = BTreeMap::new();
    let mut retired_owner_grants = BTreeSet::new();
    for (_, resolution) in resolutions {
        if !resolution.verify_against(store_root_hash, conflict) {
            return Err(MembershipError::InvalidConflictResolution);
        }
        if let Some(existing) = by_resolver.insert(
            resolution.resolver_pubkey.clone(),
            resolution.resolution_hash(),
        ) {
            if existing != resolution.resolution_hash() {
                return Err(MembershipError::InvalidConflictResolution);
            }
            continue;
        }
        retired_owner_grants.extend(resolution.retired_owner_grants.iter().cloned());
    }
    let (mut grants, known_records, provider_admin) = match conflict {
        MembershipConflict::ConcurrentMemberAssignments {
            conflicting_grants,
            grants,
            provider_admin,
            ..
        } => {
            let selected = resolutions
                .iter()
                .filter_map(|(_, resolution)| match &resolution.selection {
                    MembershipConflictSelection::MemberAssignment { grant } => Some(grant.clone()),
                    MembershipConflictSelection::RevocationBranch { .. } => None,
                })
                .collect::<BTreeSet<_>>();
            let retained = (selected.len() == 1)
                .then(|| selected.first().cloned())
                .flatten();
            let mut resolved = grants.clone();
            for (grant, record) in conflicting_grants {
                if retained.as_ref() == Some(grant) {
                    continue;
                }
                let retirements = assignment_conflict_retirements(resolutions, grant)?;
                resolved.insert(
                    grant.clone(),
                    GrantState::Tombstoned {
                        record: record.clone(),
                        retirements,
                    },
                );
            }
            (
                resolved,
                grants
                    .iter()
                    .map(|(grant, state)| (grant.clone(), state.record().clone()))
                    .collect::<BTreeMap<_, _>>(),
                provider_admin.clone(),
            )
        }
        MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } => {
            let mut selected_branches = Vec::new();
            for (_, resolution) in resolutions {
                let MembershipConflictSelection::RevocationBranch {
                    heads: selected_heads,
                } = &resolution.selection
                else {
                    return Err(MembershipError::InvalidConflictResolution);
                };
                let branch = maximal_valid_branches
                    .iter()
                    .find(|branch| branch.heads == *selected_heads)
                    .ok_or(MembershipError::InvalidConflictResolution)?;
                if !selected_branches
                    .iter()
                    .any(|selected: &&StoreMembershipBranch| selected.heads == branch.heads)
                {
                    selected_branches.push(branch);
                }
            }
            let resolved = causal_grants::resolve_conflict_grants(
                maximal_valid_branches.iter().map(|branch| &branch.grants),
                selected_branches
                    .iter()
                    .copied()
                    .map(|branch| &branch.grants),
                &retired_owner_grants,
                |grant| conflict_resolution_retirements(resolutions, grant),
                || MembershipError::InvalidConflictResolution,
            )?;
            let known_records = maximal_valid_branches
                .iter()
                .flat_map(|branch| branch.grants.iter())
                .map(|(grant, state)| (grant.clone(), state.record().clone()))
                .collect::<BTreeMap<_, _>>();
            let provider_admin = crate::protocol::provider::ProviderAdminResolution::Resolved(
                crate::protocol::provider::ProviderAdminState::merge(
                    selected_branches
                        .iter()
                        .map(|branch| branch.provider_admin.combined_state().clone()),
                )?,
            );
            (resolved, known_records, provider_admin)
        }
    };
    for (reference, resolution) in resolutions {
        for retired in &resolution.retired_owner_grants {
            let record = known_records
                .get(retired)
                .ok_or(MembershipError::InvalidConflictResolution)?
                .clone();
            let barrier = resolution
                .retirement_barriers
                .get(retired)
                .cloned()
                .ok_or(MembershipError::InvalidConflictResolution)?;
            let retirements =
                GrantRetirements::new(MembershipGrantRetirement::ConflictResolution {
                    authority: reference.clone(),
                    barrier,
                });
            causal_grants::tombstone_conflict_grant(&mut grants, retired, &record, &retirements)
                .map_err(|()| MembershipError::InvalidConflictResolution)?;
        }
    }
    for (reference, resolution) in resolutions {
        let record = MembershipGrantRecord {
            member_pubkey: resolution.resolver_pubkey.clone(),
            role: StoreMembershipRoleGrant::Owner {
                recovery: OwnerRecoveryAnchorRef::ConflictResolution {
                    acceptance: Box::new(resolution.replacement_acceptance.clone()),
                },
            },
            provider_account_email: None,
            creation_authority: MembershipGrantCreationAuthority::ConflictResolution(
                reference.clone(),
            ),
        };
        if grants
            .insert(
                resolution.replacement_grant.clone(),
                GrantState::Active {
                    record: record.clone(),
                },
            )
            .is_some_and(|current| current.active() != Some(&record))
        {
            return Err(MembershipError::InvalidConflictResolution);
        }
    }
    if !causal_grants::has_active_owner(&grants, |record| record.role.is_owner())
        || causal_grants::has_concurrent_assignments(&grants, |record| &record.member_pubkey)
    {
        return Err(MembershipError::InvalidConflictResolution);
    }
    Ok(ResolvedStoreMembership {
        state_hash: store_membership_state_hash(&grants, &provider_admin),
        grants,
        provider_admin,
    })
}

pub(super) fn conflict_resolution_retirements<'resolution>(
    resolutions: impl IntoIterator<
        Item = &'resolution (
            StoreMembershipConflictResolutionRef,
            StoreMembershipConflictResolution,
        ),
    >,
    grant: &MembershipGrantId,
) -> Result<GrantRetirements<MembershipGrantRetirement>, MembershipError> {
    let mut retirements = resolutions.into_iter().map(|(reference, resolution)| {
        resolution
            .retirement_barriers
            .get(grant)
            .cloned()
            .map(|barrier| MembershipGrantRetirement::ConflictResolution {
                authority: reference.clone(),
                barrier,
            })
            .ok_or(MembershipError::InvalidConflictResolution)
    });
    let first = retirements
        .next()
        .ok_or(MembershipError::InvalidConflictResolution)??;
    let mut result = GrantRetirements::new(first);
    for retirement in retirements {
        result.insert(retirement?);
    }
    Ok(result)
}

/// The conflict-resolution retirements for `grant`, excluding the resolution
/// whose member-assignment selection kept that grant.
pub(super) fn assignment_conflict_retirements(
    resolutions: &[(
        StoreMembershipConflictResolutionRef,
        StoreMembershipConflictResolution,
    )],
    grant: &MembershipGrantId,
) -> Result<GrantRetirements<MembershipGrantRetirement>, MembershipError> {
    conflict_resolution_retirements(
        resolutions.iter().filter(|(_, resolution)| {
            !matches!(
                &resolution.selection,
                MembershipConflictSelection::MemberAssignment { grant: selected }
                    if selected == grant
            )
        }),
        grant,
    )
}
