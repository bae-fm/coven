use crate::*;
use coven_protocol::store_commit::{
    CommitFrontier, ResolvedStoreDeviceState, StoreBatchCommitRef, StoreDeviceStateRef,
};
use rusqlite::{Connection, Transaction};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Record the resulting state of an accepted commit. Its body and exact
/// reference belong to the same materialization transaction.
pub(crate) fn record_store_device_snapshot_on(
    transaction: &Transaction<'_>,
    reference: &StoreBatchCommitRef,
    state: &ResolvedStoreDeviceState,
) -> Result<(), DbError> {
    state.validate_canonical().map_err(DbError::from)?;
    let hash = state.state_hash.to_string();
    let encoded = serde_json::to_string(state)
        .map_err(|error| DbError::context("serialize Store device state", error))?;
    let inserted = transaction
        .execute(
            "INSERT INTO store_device_states (state_hash, state) VALUES (?1, ?2)
             ON CONFLICT(state_hash) DO NOTHING",
            (&hash, &encoded),
        )
        .map_err(DbError::from)?;
    if inserted == 0 {
        let existing = load_store_device_state_on(transaction, state.state_hash)?;
        if &existing != state {
            return Err(DbError::Message(format!(
                "stored Store device state disagrees with accepted state {hash}"
            )));
        }
    }
    let reference = serde_json::to_string(reference)
        .map_err(|error| DbError::context("serialize Store commit ref", error))?;
    transaction
        .execute(
            "INSERT INTO store_device_state_snapshots (commit_ref, state_hash) VALUES (?1, ?2)",
            (&reference, &hash),
        )
        .map_err(DbError::from)?;
    Ok(())
}

/// Remove bodies only after their last exact reference has been removed in
/// the same snapshot projection or installation transaction.
pub(crate) fn prune_unreferenced_store_device_states_on(
    transaction: &Transaction<'_>,
) -> Result<(), DbError> {
    transaction
        .execute(
            "DELETE FROM store_device_states
             WHERE NOT EXISTS (
                 SELECT 1 FROM store_device_state_snapshots AS snapshot
                 WHERE snapshot.state_hash = store_device_states.state_hash
             )",
            [],
        )
        .map_err(DbError::from)?;
    Ok(())
}

fn load_store_device_state_on(
    conn: &Connection,
    hash: coven_protocol::store_commit::ObjectHash,
) -> Result<ResolvedStoreDeviceState, DbError> {
    let raw: String = conn
        .query_row(
            "SELECT state FROM store_device_states WHERE state_hash = ?1",
            [hash.to_string()],
            |row| row.get(0),
        )
        .map_err(|error| DbError::context(format!("load Store device state body {hash}"), error))?;
    let state: ResolvedStoreDeviceState = serde_json::from_str(&raw)
        .map_err(|error| DbError::context("parse Store device state body", error))?;
    state.validate_canonical().map_err(DbError::from)?;
    if state.state_hash != hash {
        return Err(DbError::Message(format!(
            "Store device state body differs from its stored hash {hash}"
        )));
    }
    Ok(state)
}

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
    // An absent row means this exact state is not retained. Snapshot coverage
    // does not establish an arbitrary historical commit's state.
    let hash: String = conn
        .query_row(
            "SELECT state_hash FROM store_device_state_snapshots WHERE commit_ref = ?1",
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
    let hash = hash
        .parse()
        .map_err(|error| DbError::context("parse Store device state hash", error))?;
    load_store_device_state_on(conn, hash)
}

/// Every device state this database holds at a position `coverage` covers.
///
/// Snapshot retention limits these to its exact tips and the predecessors of
/// retained materializations. Each distinct body is read and validated once;
/// references to the same state share that body in memory.
pub(crate) fn load_covered_store_device_snapshots_on(
    conn: &Connection,
    coverage: &CommitFrontier,
) -> Result<BTreeMap<StoreBatchCommitRef, Arc<ResolvedStoreDeviceState>>, DbError> {
    let rows = crate::query_mapped_rows(
        conn,
        "SELECT commit_ref, state_hash FROM store_device_state_snapshots ORDER BY commit_ref",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    let mut covered = BTreeMap::new();
    let mut states = BTreeMap::new();
    for (encoded_ref, encoded_hash) in rows {
        let reference: StoreBatchCommitRef = serde_json::from_str(&encoded_ref)
            .map_err(|error| DbError::context("covered device-state commit ref", error))?;
        if !coverage.covers_commit(&reference) {
            continue;
        }
        let hash = encoded_hash
            .parse()
            .map_err(|error| DbError::context("covered device state hash", error))?;
        let state = match states.entry(hash) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(Arc::new(load_store_device_state_on(conn, hash)?))
            }
        };
        covered.insert(reference, Arc::clone(state));
    }
    Ok(covered)
}

pub(crate) fn store_device_state_for_history_cut_on(
    conn: &Connection,
    cut: &coven_protocol::store_commit::StoreHistoryCut,
) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), DbError> {
    let frontier = &cut.0;
    let state = if frontier.is_empty() {
        load_store_device_genesis_state_on(conn)?
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

#[cfg(test)]
#[path = "store_device_state_tests.rs"]
mod tests;
