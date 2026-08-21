use crate::*;
use coven_protocol::store_commit::{
    CommitFrontier, ResolvedStoreDeviceState, StoreBatchCommitRef, StoreDeviceExclusionProposalId,
    StoreDeviceProposalAck, StoreDeviceProposalState, StoreDeviceStateRef, StoreHistoryCut,
    VerifiedStoreDeviceOperations,
};
use rusqlite::Connection;
use std::collections::BTreeMap;

pub(crate) fn load_store_device_genesis_state_on(
    conn: &Connection,
) -> Result<ResolvedStoreDeviceState, DbError> {
    let raw = crate::required_protocol_state_on(conn, STORE_DEVICE_GENESIS_STATE_KEY)?;
    serde_json::from_str(&raw)
        .map_err(|error| DbError::context("parse Store device genesis state", error))
}

pub(crate) fn load_store_device_snapshot_on(
    conn: &Connection,
    reference: &StoreBatchCommitRef,
) -> Result<ResolvedStoreDeviceState, DbError> {
    let exact = serde_json::to_string(reference)
        .map_err(|error| DbError::context("serialize Store commit ref", error))?;
    // An absent row is not a database fault: it means this device's local Store
    // history does not cover `reference`, so no state was ever recorded for it.
    // The caller resolved a history cut it has not materialized — name that
    // rather than letting an anonymous "no rows" carry it up.
    let raw: String = conn
        .query_row(
            "SELECT state FROM store_device_state_snapshots WHERE commit_ref = ?1",
            [exact],
            |row| row.get(0),
        )
        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => DbError::Message(format!(
                "local Store history does not cover {}/{}, so it holds no device state for it",
                reference.coord.stream_id,
                reference.coord.sequence()
            )),
            other => DbError::from(other),
        })?;
    let state: ResolvedStoreDeviceState = serde_json::from_str(&raw)
        .map_err(|error| DbError::context("parse Store device state snapshot", error))?;
    state.validate_canonical().map_err(DbError::from)?;
    Ok(state)
}

/// Every device state this database holds at a position `coverage` covers.
///
/// These are the positions an installed replay baseline supersedes: the
/// baseline restates the rows they produced, so nothing re-derives them, but a
/// commit standing above the coverage still names one of them as its
/// predecessor and has to be checked against the state that stood there.
/// `retain_snapshot_device_states` is what keeps exactly this set alive across
/// a baseline install or advance.
pub(crate) fn load_covered_store_device_snapshots_on(
    conn: &Connection,
    coverage: &CommitFrontier,
) -> Result<BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>, DbError> {
    let rows = crate::query_mapped_rows(
        conn,
        "SELECT commit_ref, state FROM store_device_state_snapshots ORDER BY commit_ref",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    let mut covered = BTreeMap::new();
    for (encoded_ref, encoded_state) in rows {
        let reference: StoreBatchCommitRef = serde_json::from_str(&encoded_ref)
            .map_err(|error| DbError::context("covered device-state commit ref", error))?;
        if !coverage.covers_commit(&reference) {
            continue;
        }
        let state: ResolvedStoreDeviceState = serde_json::from_str(&encoded_state)
            .map_err(|error| DbError::context("covered device state", error))?;
        state.validate_canonical().map_err(DbError::from)?;
        covered.insert(reference, state);
    }
    Ok(covered)
}

pub(crate) fn store_device_state_for_history_cut_on(
    conn: &Connection,
    cut: &coven_protocol::store_commit::StoreHistoryCut,
) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), DbError> {
    let genesis = load_store_device_genesis_state_on(conn)?;
    let frontier = &cut.0;
    let state = if frontier.is_empty() {
        genesis
    } else {
        ResolvedStoreDeviceState::merge(
            frontier
                .values()
                .map(|reference| load_store_device_snapshot_on(conn, reference))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(DbError::from)?
    };
    let reference = StoreDeviceStateRef::from_resolved(CommitFrontier(frontier.clone()), &state)
        .map_err(DbError::from)?;
    Ok((reference, state))
}

pub(crate) fn load_declared_store_device_state_on(
    conn: &Connection,
    reference: &StoreDeviceStateRef,
) -> Result<ResolvedStoreDeviceState, DbError> {
    let frontier = reference.frontier();
    let state = if frontier.0.is_empty() {
        load_store_device_genesis_state_on(conn)?
    } else {
        ResolvedStoreDeviceState::merge(
            frontier
                .0
                .values()
                .map(|commit| load_store_device_snapshot_on(conn, commit))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(DbError::from)?
    };
    if state.state_hash != reference.state_hash() || state.recovery != reference.recovery() {
        return Err(DbError::Message(
            "declared Store device state differs from its exact predecessor snapshots".into(),
        ));
    }
    Ok(state)
}

pub(crate) fn load_store_device_exclusion_freezes_on(
    conn: &Connection,
    root: &coven_protocol::store_commit::StoreRootRef,
) -> Result<BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalAck>, DbError> {
    let mut statement = conn
        .prepare(
            "SELECT proposal_id, proposal_ref, target_cut \
             FROM store_device_exclusion_freezes ORDER BY proposal_id",
        )
        .map_err(DbError::from)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(DbError::from)?;
    let mut existing = BTreeMap::new();
    for row in rows {
        let (proposal_id, proposal_ref, target_cut) = row.map_err(DbError::from)?;
        let proposal: coven_protocol::store_commit::StoreDeviceExclusionProposalRef =
            serde_json::from_str(&proposal_ref)
                .map_err(|error| DbError::context("stored device exclusion proposal ref", error))?;
        proposal.validate_path().map_err(DbError::from)?;
        if proposal.proposal_id.to_string() != proposal_id {
            return Err(DbError::Message(
                "stored device exclusion freeze uses another proposal id".to_string(),
            ));
        }
        let target_cut: StoreHistoryCut = serde_json::from_str(&target_cut)
            .map_err(|error| DbError::context("stored device exclusion target cut", error))?;
        let target_frontier = &target_cut.0;
        let target_stream =
            coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &proposal.target,
                coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
        if target_frontier.len() > 1
            || target_frontier
                .keys()
                .any(|stream| stream != &target_stream)
        {
            return Err(DbError::Message(
                "stored device exclusion freeze contains a non-target stream".to_string(),
            ));
        }
        existing.insert(
            proposal.proposal_id,
            StoreDeviceProposalAck {
                proposal,
                target_cut,
            },
        );
    }
    Ok(existing)
}

pub(crate) fn apply_store_device_exclusion_freezes_on(
    conn: &Connection,
    root: &coven_protocol::store_commit::StoreRootRef,
    state: &ResolvedStoreDeviceState,
    operations: &VerifiedStoreDeviceOperations,
) -> Result<(), DbError> {
    let existing = load_store_device_exclusion_freezes_on(conn, root)?;

    let mut desired = Vec::with_capacity(existing.len() + operations.proposals().len());
    for freeze in existing.into_values() {
        let proposal_state = state
            .devices
            .get(&freeze.proposal.target.device_id)
            .and_then(|record| record.proposals.get(&freeze.proposal.proposal_id));
        match proposal_state {
            Some(StoreDeviceProposalState::Pending { proposal })
                if proposal == &freeze.proposal =>
            {
                desired.push(freeze);
            }
            Some(StoreDeviceProposalState::Cancelled { outcome })
                if outcome.proposal == freeze.proposal => {}
            Some(StoreDeviceProposalState::Superseded { proposal, .. })
                if proposal == &freeze.proposal => {}
            _ => {
                return Err(DbError::Message(
                    "stored device exclusion freeze differs from materialized device state"
                        .to_string(),
                ));
            }
        }
    }

    if let Some(local_registration) = local_activated_registration_ref_on(conn)? {
        for (reference, proposal) in operations.proposals() {
            if reference.target == local_registration {
                continue;
            }
            let frozen = load_declared_store_device_state_on(conn, &proposal.frozen_device_state)?;
            let local_was_active = frozen
                .devices
                .get(&local_registration.device_id)
                .is_some_and(|record| {
                    record.registration == local_registration
                        && matches!(
                            record.status,
                            coven_protocol::store_commit::StoreDeviceStatus::Active
                        )
                });
            if !local_was_active {
                continue;
            }
            if desired
                .iter()
                .any(|freeze| freeze.proposal.proposal_id == reference.proposal_id)
            {
                return Err(DbError::Message(
                    "new device exclusion proposal duplicates a stored freeze".to_string(),
                ));
            }
            let target_stream =
                coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
                    root.store_root_hash,
                    &reference.target,
                    coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
                );
            let target = crate::store::materialized_commit_index::latest_position_for_device_on(
                conn,
                &target_stream.to_string(),
            )?;
            desired.push(StoreDeviceProposalAck {
                proposal: reference.clone(),
                target_cut: StoreHistoryCut(match target {
                    Some(reference) => BTreeMap::from([(target_stream, reference)]),
                    None => BTreeMap::new(),
                }),
            });
        }
    }
    desired.sort_by_key(|freeze| freeze.proposal.proposal_id);
    replace_store_device_exclusion_freezes_on(conn, &desired)
}

pub(crate) fn replace_store_device_exclusion_freezes_on(
    conn: &Connection,
    desired: &[StoreDeviceProposalAck],
) -> Result<(), DbError> {
    conn.execute("DELETE FROM store_device_exclusion_freezes", [])
        .map_err(DbError::from)?;
    for freeze in desired {
        conn.execute(
            "INSERT INTO store_device_exclusion_freezes \
             (proposal_id, proposal_ref, target_cut) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                freeze.proposal.proposal_id.to_string(),
                serde_json::to_string(&freeze.proposal).map_err(|error| {
                    DbError::context("serialize device exclusion proposal ref", error)
                })?,
                serde_json::to_string(&freeze.target_cut).map_err(|error| {
                    DbError::context("serialize device exclusion target cut", error)
                })?,
            ],
        )
        .map_err(DbError::from)?;
    }
    Ok(())
}
