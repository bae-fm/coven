use super::*;

pub(super) fn active_circle_grants(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
) -> impl Iterator<Item = (&MembershipGrantId, &CircleGrantRecord)> {
    grants
        .iter()
        .filter_map(|(grant, state)| state.active().map(|record| (grant, record)))
}

pub(super) fn roster_members(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
) -> BTreeMap<String, CircleRole> {
    active_circle_grants(grants)
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
    active_circle_grants(grants)
        .map(|(_, record)| record)
        .any(|record| record.role == CircleRole::Owner)
        && roster_members(grants).len() == active_circle_grants(grants).count()
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
    let expected = coords.iter().cloned().collect::<BTreeSet<_>>();
    let mut references = head_refs
        .iter()
        .filter(|reference| expected.contains(&reference.coord))
        .cloned()
        .collect::<Vec<_>>();
    let actual = references
        .iter()
        .map(|reference| reference.coord.clone())
        .collect::<BTreeSet<_>>();
    if expected != actual || references.len() != expected.len() {
        return Err(CircleRosterError::MissingConflictHeads);
    }
    references.sort();
    Ok(references)
}

pub(super) fn map_circle_grants(
    grants: BTreeMap<MembershipGrantId, causal_grants::GrantRecord<CircleRosterCoord, CircleRole>>,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    >,
) -> Result<BTreeMap<MembershipGrantId, CircleGrantRecord>, CircleRosterError> {
    grants
        .into_iter()
        .map(|(grant, record)| -> Result<_, CircleRosterError> {
            let creation_authority =
                circle_creation_authority(&grant, record.creation, checkpoint)?;
            Ok((
                grant,
                CircleGrantRecord {
                    member_pubkey: record.member_pubkey,
                    role: record.assignment,
                    creation_authority,
                },
            ))
        })
        .collect()
}

pub(super) fn resolved_circle_roster(
    reduced: &causal_grants::ReducedGrants<CircleRosterCoord, CircleRole>,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    >,
) -> Result<ResolvedCircleRoster, CircleRosterError> {
    let grants = reduced
        .grants
        .iter()
        .map(|(grant, state)| -> Result<_, CircleRosterError> {
            Ok((
                grant.clone(),
                map_circle_grant_state(grant, state, checkpoint)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Ok(ResolvedCircleRoster {
        state_hash: circle_roster_state_hash(&grants),
        grants,
    })
}

pub(super) fn map_circle_grant_state(
    grant: &MembershipGrantId,
    state: &GrantState<
        causal_grants::GrantRecord<CircleRosterCoord, CircleRole>,
        causal_grants::CausalGrantRetirement<CircleRosterCoord>,
    >,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    >,
) -> Result<GrantState<CircleGrantRecord, CircleGrantRetirement>, CircleRosterError> {
    let causal_record = state.record();
    let record = CircleGrantRecord {
        member_pubkey: causal_record.member_pubkey.clone(),
        role: causal_record.assignment,
        creation_authority: circle_creation_authority(
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
        || CircleRosterError::MissingCheckpointRetirementEvidence {
            grant: grant.clone(),
        },
        |coord, owner_barrier| {
            Ok(CircleGrantRetirement::Entry {
                authority: coord.clone(),
                owner_barrier: owner_barrier.map(|barrier| CircleOwnerGrantBarrier {
                    observed_streams: barrier.observed_streams.values().cloned().collect(),
                }),
            })
        },
    )
}

pub(super) fn circle_creation_authority(
    grant: &MembershipGrantId,
    creation: causal_grants::CausalGrantCreation<CircleRosterCoord>,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    >,
) -> Result<CircleGrantCreationAuthority, CircleRosterError> {
    match creation {
        causal_grants::CausalGrantCreation::Entry(coord) => {
            Ok(CircleGrantCreationAuthority::Entry(coord))
        }
        causal_grants::CausalGrantCreation::Checkpoint => checkpoint
            .and_then(|grants| grants.get(grant))
            .ok_or_else(|| CircleRosterError::MissingCheckpointGrant {
                grant: grant.clone(),
            })
            .map(|state| state.record().creation_authority.clone()),
    }
}

pub(super) fn circle_assignment_conflict_hash(
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    heads: &[CircleRosterHeadRef],
    member_pubkey: &str,
    conflicting_grants: &BTreeMap<
        MembershipGrantId,
        causal_grants::GrantRecord<CircleRosterCoord, CircleRole>,
    >,
) -> ObjectHash {
    #[derive(Serialize)]
    struct Conflict<'a> {
        domain: &'static str,
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        heads: &'a [CircleRosterHeadRef],
        member_pubkey: &'a str,
        conflicting_grant_ids: Vec<&'a MembershipGrantId>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&Conflict {
            domain: "coven.circle-roster-assignment-conflict.v1",
            store_root_hash,
            circle_id,
            heads,
            member_pubkey,
            conflicting_grant_ids: conflicting_grants.keys().collect(),
        })
        .expect("Circle roster conflict serialization cannot fail"),
    )
}

pub(super) fn circle_revocation_conflict_hash(
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    heads: &[CircleRosterHeadRef],
    cyclic_sources: &[CircleRosterCoord],
    involved_owner_grants: &BTreeSet<MembershipGrantId>,
) -> ObjectHash {
    #[derive(Serialize)]
    struct Conflict<'a> {
        domain: &'static str,
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        heads: &'a [CircleRosterHeadRef],
        cyclic_sources: &'a [CircleRosterCoord],
        involved_owner_grants: &'a BTreeSet<MembershipGrantId>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&Conflict {
            domain: "coven.circle-roster-revocation-conflict.v1",
            store_root_hash,
            circle_id,
            heads,
            cyclic_sources,
            involved_owner_grants,
        })
        .expect("Circle roster revocation conflict serialization cannot fail"),
    )
}
