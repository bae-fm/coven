use super::*;

pub(super) fn roster_members(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
) -> BTreeMap<String, CircleRole> {
    causal_grants::active_grants(grants)
        .map(|(_, record)| record)
        .map(|record| (record.member_pubkey.clone(), record.role))
        .collect()
}

pub(super) fn roster_authorizes_owner_grant(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    author_pubkey: &str,
    grant_id: &MembershipGrantId,
) -> bool {
    grants
        .get(grant_id)
        .and_then(GrantState::active)
        .is_some_and(|record| {
            record.member_pubkey == author_pubkey && record.role == CircleRole::Owner
        })
}

pub(super) fn roster_grants_are_valid(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
) -> bool {
    causal_grants::has_active_owner(grants, |record| record.role == CircleRole::Owner)
        && !causal_grants::has_concurrent_assignments(grants, |record| &record.member_pubkey)
}

pub(super) fn circle_roster_state_hash(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
) -> ObjectHash {
    #[derive(Serialize)]
    struct State<'a> {
        domain: &'static str,
        grants:
            &'a BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&State {
            domain: "coven.circle-roster-state.v2",
            grants,
        })
        .expect("circle roster state serialization cannot fail"),
    )
}

pub(super) fn exact_circle_head_refs(
    head_refs: &[CircleRosterHeadRef],
    coords: &[CircleRosterCoord],
) -> Result<Vec<CircleRosterHeadRef>, CircleRosterError> {
    crate::causal_grants::exact_head_refs(head_refs, coords, |reference| &reference.coord)
        .ok_or(CircleRosterError::HeadEntryMismatch)
}

pub(super) fn map_circle_grants(
    grants: BTreeMap<MembershipGrantId, causal_grants::GrantRecord<CircleRosterCoord, CircleRole>>,
) -> BTreeMap<MembershipGrantId, CircleGrantRecord> {
    grants
        .into_iter()
        .map(|(grant, record)| {
            (
                grant,
                CircleGrantRecord {
                    member_pubkey: record.member_pubkey,
                    role: record.assignment,
                    creation_authority: record.creation,
                },
            )
        })
        .collect()
}

pub(super) fn resolved_circle_roster(
    reduced: &causal_grants::ReducedGrants<CircleRosterCoord, CircleRole>,
) -> ResolvedCircleRoster {
    let grants = reduced
        .grants
        .iter()
        .map(|(grant, state)| (grant.clone(), map_circle_grant_state(state)))
        .collect::<BTreeMap<_, _>>();
    ResolvedCircleRoster {
        state_hash: circle_roster_state_hash(&grants),
        grants,
    }
}

pub(super) fn map_circle_grant_state(
    state: &GrantState<
        causal_grants::GrantRecord<CircleRosterCoord, CircleRole>,
        causal_grants::CausalGrantRetirement<CircleRosterCoord>,
    >,
) -> GrantState<CircleGrantRecord, CircleGrantRetirement> {
    let causal_record = state.record();
    let record = CircleGrantRecord {
        member_pubkey: causal_record.member_pubkey.clone(),
        role: causal_record.assignment,
        creation_authority: causal_record.creation.clone(),
    };
    causal_grants::try_map_grant_state(state, record, |coord, owner_barrier| {
        Ok(CircleGrantRetirement {
            authority: coord.clone(),
            owner_barrier: owner_barrier.map(|barrier| CircleOwnerGrantBarrier {
                observed_streams: barrier.observed_streams.values().cloned().collect(),
            }),
        })
    })
    .unwrap_or_else(|never: std::convert::Infallible| match never {})
}
