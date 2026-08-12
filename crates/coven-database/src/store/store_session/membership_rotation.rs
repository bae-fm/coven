use super::{StoreDatabase, StoreSession};
use crate::DbError;
use coven_protocol::objects::RotationGate;
use coven_protocol::store_commit::ObjectHash;

impl StoreSession<'_> {
    fn load_rotation_gate(&mut self) -> Result<Option<RotationGate>, DbError> {
        load_rotation_gate_on(self.conn).map(|gate| gate.map(|(_, gate)| gate))
    }

    fn record_peer_rotation(&mut self, generation: u64) -> Result<RotationGate, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let existing = load_rotation_gate_on(&tx)?;
        let next = RotationGate::merge_peer_commit(
            existing.as_ref().map(|(_, gate)| gate.clone()),
            generation,
        )
        .map_err(DbError::from)?;
        replace_rotation_gate_on(
            &tx,
            existing.as_ref(),
            Some(next.clone()),
            "peer rotation recording",
        )?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn complete_peer_rotation_adoption(
        &mut self,
        adopted_generation: u64,
    ) -> Result<Option<RotationGate>, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let existing = load_rotation_gate_on(&tx)?.ok_or_else(|| {
            DbError::Message("rotation gate is absent during peer rotation adoption".to_string())
        })?;
        let next = existing
            .1
            .clone()
            .complete_peer_adoption(adopted_generation)
            .map_err(DbError::from)?;
        replace_rotation_gate_on(&tx, Some(&existing), next.clone(), "peer rotation adoption")?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn complete_local_rotation_adoption(
        &mut self,
        intent_hash: ObjectHash,
        generation: u64,
    ) -> Result<Option<RotationGate>, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let existing = load_rotation_gate_on(&tx)?.ok_or_else(|| {
            DbError::Message("rotation gate is absent during local rotation adoption".to_string())
        })?;
        let next = existing
            .1
            .clone()
            .complete_local_adoption(generation, intent_hash)
            .map_err(DbError::from)?;
        if tx
            .execute(
                "DELETE FROM outbound_membership_mutation \
                 WHERE singleton = 1 AND intent_hash = ?1",
                [intent_hash.to_string()],
            )
            .map_err(DbError::from)?
            != 1
        {
            return Err(DbError::Message(
                "membership mutation changed during local rotation adoption".to_string(),
            ));
        }
        replace_rotation_gate_on(
            &tx,
            Some(&existing),
            next.clone(),
            "local rotation adoption",
        )?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }
}

impl StoreDatabase {
    pub async fn load_rotation_gate(&self) -> Result<Option<RotationGate>, DbError> {
        self.call_store(|session| session.load_rotation_gate())
            .await
    }

    pub async fn record_peer_rotation(&self, generation: u64) -> Result<RotationGate, DbError> {
        self.call_store(move |session| session.record_peer_rotation(generation))
            .await
    }

    pub async fn complete_peer_rotation_adoption(
        &self,
        adopted_generation: u64,
    ) -> Result<Option<RotationGate>, DbError> {
        self.call_store(move |session| session.complete_peer_rotation_adoption(adopted_generation))
            .await
    }

    pub async fn complete_local_rotation_adoption(
        &self,
        intent_hash: ObjectHash,
        generation: u64,
    ) -> Result<Option<RotationGate>, DbError> {
        self.call_store(move |session| {
            session.complete_local_rotation_adoption(intent_hash, generation)
        })
        .await
    }
}

pub(super) fn stage_pending_rotation_on(
    tx: &rusqlite::Transaction<'_>,
    generation: Option<u64>,
    mutation: ObjectHash,
) -> Result<(), DbError> {
    let Some(generation) = generation else {
        return Ok(());
    };
    let existing = load_rotation_gate_on(tx)?;
    let gate = RotationGate::with_candidate(
        existing.as_ref().map(|(_, gate)| gate.clone()),
        generation,
        mutation,
    )
    .map_err(DbError::from)?;
    replace_rotation_gate_on(tx, existing.as_ref(), Some(gate), "candidate staging")
}

pub(super) fn replace_rotation_candidate_mutation_on(
    tx: &rusqlite::Transaction<'_>,
    previous: ObjectHash,
    replacement: ObjectHash,
    generation: u64,
) -> Result<(), DbError> {
    let existing = load_rotation_gate_on(tx)?.ok_or_else(|| {
        DbError::Message("rotation gate is absent during candidate replacement".to_string())
    })?;
    let next = existing
        .1
        .clone()
        .replace_candidate_mutation(generation, previous, replacement)
        .map_err(DbError::from)?;
    replace_rotation_gate_on(tx, Some(&existing), Some(next), "candidate replacement")
}

pub(super) fn remove_rotation_candidate_on(
    tx: &rusqlite::Transaction<'_>,
    intent_hash: ObjectHash,
    generation: u64,
) -> Result<(), DbError> {
    let existing = load_rotation_gate_on(tx)?.ok_or_else(|| {
        DbError::Message("rotation gate is absent during candidate loss".to_string())
    })?;
    let next = existing
        .1
        .clone()
        .remove_candidate(generation, intent_hash)
        .map_err(DbError::from)?;
    replace_rotation_gate_on(tx, Some(&existing), next, "candidate loss")
}

pub(super) fn commit_rotation_candidate_on(
    tx: &rusqlite::Transaction<'_>,
    intent_hash: ObjectHash,
    generation: u64,
) -> Result<(), DbError> {
    let existing = load_rotation_gate_on(tx)?.ok_or_else(|| {
        DbError::Message("rotation gate is absent during candidate activation".to_string())
    })?;
    let gate = RotationGate::commit_candidate(Some(existing.1.clone()), generation, intent_hash)
        .map_err(DbError::from)?;
    replace_rotation_gate_on(tx, Some(&existing), Some(gate), "membership activation")
}

fn load_rotation_gate_on(
    connection: &rusqlite::Connection,
) -> Result<Option<(String, RotationGate)>, DbError> {
    let key = coven_protocol::objects::ROTATION_GATE_STATE_KEY;
    crate::get_protocol_state_on(connection, key)?
        .map(|encoded| {
            let gate = serde_json::from_str::<RotationGate>(&encoded)
                .map_err(|error| DbError::context("parse rotation gate", error))?;
            Ok((encoded, gate))
        })
        .transpose()
}

fn replace_rotation_gate_on(
    tx: &rusqlite::Transaction<'_>,
    expected: Option<&(String, RotationGate)>,
    next: Option<RotationGate>,
    operation: &'static str,
) -> Result<(), DbError> {
    let key = coven_protocol::objects::ROTATION_GATE_STATE_KEY;
    let changed = match (expected, next) {
        (Some((expected, _)), Some(next)) => {
            let encoded = serde_json::to_string(&next).map_err(|error| {
                DbError::context(format!("serialize rotation gate during {operation}"), error)
            })?;
            tx.execute(
                "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                (&encoded, key, expected),
            )
            .map_err(DbError::from)?
        }
        (Some((expected, _)), None) => tx
            .execute(
                "DELETE FROM protocol_state WHERE key = ?1 AND value = ?2",
                (key, expected),
            )
            .map_err(DbError::from)?,
        (None, Some(next)) => {
            let encoded = serde_json::to_string(&next).map_err(|error| {
                DbError::context(format!("serialize rotation gate during {operation}"), error)
            })?;
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
                (key, &encoded),
            )
            .map_err(DbError::from)?
        }
        (None, None) => return Ok(()),
    };
    if changed != 1 {
        return Err(DbError::Message(format!(
            "rotation gate changed during {operation}"
        )));
    }
    Ok(())
}
