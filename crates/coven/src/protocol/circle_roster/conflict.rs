use super::reduction::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolvedCircleRoster {
    pub grants: BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleRosterBranch {
    pub heads: Vec<CircleRosterHeadRef>,
    pub effective_frontier: Vec<CircleRosterCoord>,
    pub grants: BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleRosterConflict {
    ConcurrentMemberAssignments {
        conflict_hash: ObjectHash,
        heads: Vec<CircleRosterHeadRef>,
        effective_frontier: Vec<CircleRosterCoord>,
        member_pubkey: String,
        conflicting_grants: BTreeMap<MembershipGrantId, CircleGrantRecord>,
        uncontested_grants: BTreeMap<MembershipGrantId, CircleGrantRecord>,
    },
    RevocationCycle {
        conflict_hash: ObjectHash,
        heads: Vec<CircleRosterHeadRef>,
        cyclic_sources: Vec<CircleRosterCoord>,
        involved_owner_grants: BTreeSet<MembershipGrantId>,
        maximal_valid_branches: Vec<CircleRosterBranch>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleRosterStatus {
    Resolved(ResolvedCircleRoster),
    Conflict(CircleRosterConflict),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterConflictResolutionRef {
    pub conflict_hash: ObjectHash,
    pub resolver_pubkey: String,
    pub resolution_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleRosterConflictResolution {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub conflict_hash: ObjectHash,
    pub conflicting_heads: Vec<CircleRosterHeadRef>,
    pub retired_owner_grants: BTreeSet<MembershipGrantId>,
    pub resolver_pubkey: String,
    pub resolver_branch_heads: Vec<CircleRosterHeadRef>,
    pub replacement_grant: MembershipGrantId,
    pub signature: String,
}

impl CircleRosterConflictResolution {
    pub(super) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            conflict_hash: ObjectHash,
            conflicting_heads: &'a [CircleRosterHeadRef],
            retired_owner_grants: &'a BTreeSet<MembershipGrantId>,
            resolver_pubkey: &'a str,
            resolver_branch_heads: &'a [CircleRosterHeadRef],
            replacement_grant: &'a MembershipGrantId,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.circle-roster-conflict-resolution.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            conflict_hash: self.conflict_hash,
            conflicting_heads: &self.conflicting_heads,
            retired_owner_grants: &self.retired_owner_grants,
            resolver_pubkey: &self.resolver_pubkey,
            resolver_branch_heads: &self.resolver_branch_heads,
            replacement_grant: &self.replacement_grant,
        })
        .expect("Circle roster resolution serialization cannot fail")
    }

    pub(crate) fn resolution_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("Circle roster resolution serialization cannot fail"),
        )
    }

    pub(crate) fn resolution_ref(&self) -> CircleRosterConflictResolutionRef {
        CircleRosterConflictResolutionRef {
            conflict_hash: self.conflict_hash,
            resolver_pubkey: self.resolver_pubkey.clone(),
            resolution_hash: self.resolution_hash(),
        }
    }

    pub(crate) fn verify_signature(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.replacement_grant
                == derive_circle_resolution_grant(&self.conflict_hash, &self.resolver_pubkey)
            && keys::verify_signature_hex(
                &self.resolver_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub(crate) fn verify_against(
        &self,
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        conflict: &CircleRosterConflict,
    ) -> bool {
        let CircleRosterConflict::RevocationCycle {
            conflict_hash,
            heads,
            involved_owner_grants,
            maximal_valid_branches,
            ..
        } = conflict
        else {
            return false;
        };
        let Some(branch) = maximal_valid_branches
            .iter()
            .find(|branch| branch.heads == self.resolver_branch_heads)
        else {
            return false;
        };
        let mut expected_retired = involved_owner_grants.clone();
        expected_retired.extend(active_circle_grants(&branch.grants).filter_map(
            |(grant, record)| {
                (record.member_pubkey == self.resolver_pubkey && record.role == CircleRole::Owner)
                    .then_some(grant.clone())
            },
        ));
        self.version == STORE_PROTOCOL_VERSION
            && self.store_root_hash == store_root_hash
            && self.circle_id == circle_id
            && self.conflict_hash == *conflict_hash
            && self.conflicting_heads == *heads
            && self.retired_owner_grants == expected_retired
            && self.replacement_grant
                == derive_circle_resolution_grant(conflict_hash, &self.resolver_pubkey)
            && active_circle_grants(&branch.grants).any(|(_, record)| {
                record.member_pubkey == self.resolver_pubkey && record.role == CircleRole::Owner
            })
            && self.verify_signature()
    }
}

pub(crate) fn derive_circle_resolution_grant(
    conflict_hash: &ObjectHash,
    resolver_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!("coven.circle-roster-resolution-grant.v1\0{conflict_hash}\0{resolver_pubkey}")
            .as_bytes(),
    ))
}

pub(crate) fn resolve_circle_roster_conflict(
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    conflict: &CircleRosterConflict,
    resolutions: &[CircleRosterConflictResolution],
) -> Result<ResolvedCircleRoster, CircleRosterError> {
    let CircleRosterConflict::RevocationCycle {
        maximal_valid_branches,
        ..
    } = conflict
    else {
        return Err(CircleRosterError::InvalidConflictResolution);
    };
    if resolutions.is_empty() {
        return Err(CircleRosterError::InvalidConflictResolution);
    }
    let mut by_resolver = BTreeMap::new();
    let mut selected_branches = Vec::new();
    let mut retired_owner_grants = BTreeSet::new();
    for resolution in resolutions {
        if !resolution.verify_against(store_root_hash, circle_id, conflict) {
            return Err(CircleRosterError::InvalidConflictResolution);
        }
        let resolution_hash = resolution.resolution_hash();
        if let Some(existing) =
            by_resolver.insert(resolution.resolver_pubkey.clone(), resolution_hash)
        {
            if existing != resolution_hash {
                return Err(CircleRosterError::InvalidConflictResolution);
            }
            continue;
        }
        let branch = maximal_valid_branches
            .iter()
            .find(|branch| branch.heads == resolution.resolver_branch_heads)
            .ok_or(CircleRosterError::InvalidConflictResolution)?;
        if !selected_branches
            .iter()
            .any(|selected: &&CircleRosterBranch| selected.heads == branch.heads)
        {
            selected_branches.push(branch);
        }
        retired_owner_grants.extend(resolution.retired_owner_grants.iter().cloned());
    }
    let (first_branch, other_branches) = selected_branches
        .split_first()
        .ok_or(CircleRosterError::InvalidConflictResolution)?;
    let mut grants = active_circle_grants(&first_branch.grants)
        .filter(|(grant, _)| !retired_owner_grants.contains(*grant))
        .filter(|(grant, record)| {
            other_branches.iter().all(|branch| {
                branch.grants.get(*grant).and_then(GrantState::active) == Some(*record)
            })
        })
        .map(|(grant, record)| {
            (
                grant.clone(),
                GrantState::Active {
                    record: record.clone(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for branch in maximal_valid_branches {
        for (grant, state) in &branch.grants {
            if state.retirements().is_some()
                && causal_grants::merge_conflict_grant_state(&mut grants, grant.clone(), state)
                    .is_err()
            {
                return Err(CircleRosterError::InvalidConflictResolution);
            }
        }
    }
    let mut resolution_retirements = resolutions
        .iter()
        .map(|resolution| CircleGrantRetirement::ConflictResolution(resolution.resolution_ref()));
    let mut resolution_retirements = GrantRetirements::new(
        resolution_retirements
            .next()
            .expect("validated conflict has a resolution"),
    );
    resolution_retirements.extend(
        resolutions.iter().skip(1).map(|resolution| {
            CircleGrantRetirement::ConflictResolution(resolution.resolution_ref())
        }),
    );
    for branch in maximal_valid_branches {
        for (grant, record) in branch.active_grants() {
            if grants.get(grant).and_then(GrantState::active).is_some() {
                continue;
            }
            match grants.entry(grant.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(GrantState::Tombstoned {
                        record: record.clone(),
                        retirements: resolution_retirements.clone(),
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if entry.get().record() != record {
                        return Err(CircleRosterError::InvalidConflictResolution);
                    }
                    let GrantState::Tombstoned { retirements, .. } = entry.get_mut() else {
                        unreachable!("active conflict grant was handled above")
                    };
                    retirements.extend(resolution_retirements.iter().cloned());
                }
            }
        }
    }
    for resolution in resolutions {
        let reference = resolution.resolution_ref();
        for retired in &resolution.retired_owner_grants {
            let record = selected_branches
                .iter()
                .find_map(|branch| branch.grants.get(retired).map(GrantState::record))
                .ok_or(CircleRosterError::InvalidConflictResolution)?
                .clone();
            let retirement = CircleGrantRetirement::ConflictResolution(reference.clone());
            match grants.entry(retired.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(GrantState::Tombstoned {
                        record,
                        retirements: GrantRetirements::new(retirement),
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if entry.get().record() != &record {
                        return Err(CircleRosterError::InvalidConflictResolution);
                    }
                    let mut retirements = match entry.get().retirements() {
                        Some(retirements) => retirements.clone(),
                        None => GrantRetirements::new(retirement.clone()),
                    };
                    retirements.insert(retirement);
                    *entry.get_mut() = GrantState::Tombstoned {
                        record,
                        retirements,
                    };
                }
            }
        }
    }
    for resolution in resolutions {
        let record = CircleGrantRecord {
            member_pubkey: resolution.resolver_pubkey.clone(),
            role: CircleRole::Owner,
            creation_authority: CircleGrantCreationAuthority::ConflictResolution(
                resolution.resolution_ref(),
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
            return Err(CircleRosterError::InvalidConflictResolution);
        }
    }
    if !roster_grants_are_valid(&grants) {
        return Err(CircleRosterError::InvalidConflictResolution);
    }
    Ok(ResolvedCircleRoster {
        state_hash: circle_roster_state_hash(&grants),
        grants,
    })
}

impl ResolvedCircleRoster {
    pub(crate) fn state_hash(&self) -> ObjectHash {
        self.state_hash
    }

    pub(crate) fn members(&self) -> BTreeMap<String, CircleRole> {
        roster_members(&self.grants)
    }

    pub(crate) fn authorizes_owner_grant(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
        created_at: &CircleRosterCoord,
    ) -> bool {
        self.authorizes_owner_grant_id(author_pubkey, grant_id)
            && self
                .grants
                .get(grant_id)
                .and_then(GrantState::active)
                .is_some_and(|record| {
                    record.creation_authority
                        == CircleGrantCreationAuthority::Entry(created_at.clone())
                })
    }

    pub(crate) fn authorizes_resolution_grant(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
        resolution: &CircleRosterConflictResolutionRef,
    ) -> bool {
        self.authorizes_owner_grant_id(author_pubkey, grant_id)
            && self
                .grants
                .get(grant_id)
                .and_then(GrantState::active)
                .is_some_and(|record| {
                    record.creation_authority
                        == CircleGrantCreationAuthority::ConflictResolution(resolution.clone())
                })
    }

    pub(crate) fn authorizes_owner_grant_id(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
    ) -> bool {
        roster_authorizes_owner_grant(&self.grants, author_pubkey, grant_id)
    }

    pub(crate) fn verify(&self) -> bool {
        self.state_hash == circle_roster_state_hash(&self.grants)
            && roster_grants_are_valid(&self.grants)
    }

    pub(crate) fn active_grants(
        &self,
    ) -> impl Iterator<Item = (&MembershipGrantId, &CircleGrantRecord)> {
        active_circle_grants(&self.grants)
    }
}

impl CircleRosterBranch {
    pub(crate) fn active_grants(
        &self,
    ) -> impl Iterator<Item = (&MembershipGrantId, &CircleGrantRecord)> {
        active_circle_grants(&self.grants)
    }
}

pub(crate) type CircleMaterializedRoster = ResolvedCircleRoster;
