use super::reduction::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedCircleRoster {
    pub grants: BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterBranch {
    pub heads: Vec<CircleRosterHeadRef>,
    pub effective_frontier: Vec<CircleRosterCoord>,
    pub grants: BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleRosterConflict {
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
pub enum CircleRosterStatus {
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

/// The wire body of one Circle roster conflict resolution. Every field here is
/// signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterConflictResolutionBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub conflict_hash: ObjectHash,
    pub conflicting_heads: Vec<CircleRosterHeadRef>,
    pub retired_owner_grants: BTreeSet<MembershipGrantId>,
    pub resolver_pubkey: String,
    pub resolver_branch_heads: Vec<CircleRosterHeadRef>,
    pub replacement_grant: MembershipGrantId,
}

impl SignedBody for CircleRosterConflictResolutionBody {
    const DOMAIN: &'static [u8] = ROSTER_RESOLUTION_DOMAIN;
}

pub type CircleRosterConflictResolution = Signed<CircleRosterConflictResolutionBody>;

impl CircleRosterConflictResolution {
    pub fn resolution_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn resolution_ref(&self) -> CircleRosterConflictResolutionRef {
        CircleRosterConflictResolutionRef {
            conflict_hash: self.conflict_hash,
            resolver_pubkey: self.resolver_pubkey.clone(),
            resolution_hash: self.resolution_hash(),
        }
    }

    pub fn verify_signature(&self) -> bool {
        self.replacement_grant
            == derive_circle_resolution_grant(&self.conflict_hash, &self.resolver_pubkey)
            && self.verify_by(&self.resolver_pubkey).is_ok()
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
        expected_retired.extend(causal_grants::active_grants(&branch.grants).filter_map(
            |(grant, record)| {
                (record.member_pubkey == self.resolver_pubkey && record.role == CircleRole::Owner)
                    .then_some(grant.clone())
            },
        ));
        self.store_root_hash == store_root_hash
            && self.circle_id == circle_id
            && self.conflict_hash == *conflict_hash
            && self.conflicting_heads == *heads
            && self.retired_owner_grants == expected_retired
            && self.replacement_grant
                == derive_circle_resolution_grant(conflict_hash, &self.resolver_pubkey)
            && causal_grants::active_grants(&branch.grants).any(|(_, record)| {
                record.member_pubkey == self.resolver_pubkey && record.role == CircleRole::Owner
            })
            && self.verify_signature()
    }
}

pub fn derive_circle_resolution_grant(
    conflict_hash: &ObjectHash,
    resolver_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!("coven.circle-roster-resolution-grant.v1\0{conflict_hash}\0{resolver_pubkey}")
            .as_bytes(),
    ))
}

pub fn resolve_circle_roster_conflict(
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
    let mut grants = causal_grants::resolve_conflict_grants(
        maximal_valid_branches.iter().map(|branch| &branch.grants),
        selected_branches
            .iter()
            .copied()
            .map(|branch| &branch.grants),
        &retired_owner_grants,
        |_| Ok(resolution_retirements.clone()),
        || CircleRosterError::InvalidConflictResolution,
    )?;
    for resolution in resolutions {
        let retirements = GrantRetirements::new(CircleGrantRetirement::ConflictResolution(
            resolution.resolution_ref(),
        ));
        for retired in &resolution.retired_owner_grants {
            let record = selected_branches
                .iter()
                .find_map(|branch| branch.grants.get(retired).map(GrantState::record))
                .ok_or(CircleRosterError::InvalidConflictResolution)?
                .clone();
            causal_grants::tombstone_conflict_grant(&mut grants, retired, &record, &retirements)
                .map_err(|()| CircleRosterError::InvalidConflictResolution)?;
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
    pub fn state_hash(&self) -> ObjectHash {
        self.state_hash
    }

    pub fn members(&self) -> BTreeMap<String, CircleRole> {
        roster_members(&self.grants)
    }

    pub fn authorizes_owner_grant(
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

    pub fn authorizes_resolution_grant(
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

    pub fn authorizes_owner_grant_id(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
    ) -> bool {
        roster_authorizes_owner_grant(&self.grants, author_pubkey, grant_id)
    }

    pub fn verify(&self) -> bool {
        self.state_hash == circle_roster_state_hash(&self.grants)
            && roster_grants_are_valid(&self.grants)
    }

    pub fn active_grants(&self) -> impl Iterator<Item = (&MembershipGrantId, &CircleGrantRecord)> {
        causal_grants::active_grants(&self.grants)
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl CircleRosterBranch {
    pub fn active_grants(&self) -> impl Iterator<Item = (&MembershipGrantId, &CircleGrantRecord)> {
        causal_grants::active_grants(&self.grants)
    }
}

pub type CircleMaterializedRoster = ResolvedCircleRoster;
