use super::reduction::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedCircleRoster {
    pub grants: BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleRosterConflict {
    ConcurrentMemberAssignments {
        heads: Vec<CircleRosterHeadRef>,
        effective_frontier: Vec<CircleRosterCoord>,
        member_pubkey: String,
        conflicting_grants: BTreeMap<MembershipGrantId, CircleGrantRecord>,
        uncontested_grants: BTreeMap<MembershipGrantId, CircleGrantRecord>,
    },
    RevocationCycle {
        heads: Vec<CircleRosterHeadRef>,
        cyclic_sources: Vec<CircleRosterCoord>,
        involved_owner_grants: BTreeSet<MembershipGrantId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleRosterStatus {
    Resolved(ResolvedCircleRoster),
    Conflict(CircleRosterConflict),
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
                .is_some_and(|record| record.creation_authority == *created_at)
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

pub type CircleMaterializedRoster = ResolvedCircleRoster;
