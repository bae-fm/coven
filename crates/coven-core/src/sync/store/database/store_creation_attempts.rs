use crate::database::*;

use super::*;

pub(super) fn consume_store_creation_probes_on(
    conn: &rusqlite::Connection,
    graph: &DurableFounderGraph,
) -> Result<(), DbError> {
    use crate::sync::provider::{ExactProbeProgress, ProviderProbeJournalRecord};
    use crate::sync::store::protocol_root::{
        StoreCreationAttempt, STORE_CREATION_ATTEMPT_STATE_KEY,
    };

    let attempt_json =
        crate::database::required_protocol_state_on(conn, STORE_CREATION_ATTEMPT_STATE_KEY)?;
    let attempt: StoreCreationAttempt = serde_json::from_str(&attempt_json)
        .map_err(|error| DbError::Message(format!("parse Store creation attempt: {error}")))?;
    let StoreCreationAttempt::FounderGraphReserved(graph_reservation) = attempt else {
        return Err(DbError::Message(
            "Store creation attempt has not reserved the complete founder graph".to_string(),
        ));
    };
    let reservation = &graph_reservation.descriptor;
    let descriptor = &graph.root.value.descriptor;
    let founder = &reservation.membership.founder;
    let authority = &founder.root.authority;
    if authority.creation_id != descriptor.creation_id
        || authority.founder_grant != descriptor.founder_grant
        || authority.provider_admin_grant != descriptor.founder_provider_admin.grant_id
        || authority.binding.store != descriptor.provider
        || authority.binding.device != descriptor.founder_provider_admin.provider
        || authority.founder_pubkey != descriptor.founder_pubkey
        || authority.schema_version != descriptor.schema_version
        || authority.sync_routing_hash != descriptor.sync_routing_hash
        || founder.root.root_slot != descriptor.root_slot
        || founder.registration_slot != descriptor.founder_registration
        || &reservation.recovery_slot != descriptor.founder_recovery.first_slot()
        || descriptor.founder_membership.first_slot() != &reservation.membership.first_slot
    {
        return Err(DbError::Message(
            "signed Store descriptor differs from its durable creation attempt".to_string(),
        ));
    }
    if graph.registration.value.store_commits != graph_reservation.store_commits
        || graph.registration.value.acknowledgements != graph_reservation.acknowledgements
        || graph.registration.value.snapshots != graph_reservation.snapshots
        || graph.initial_ack.value.last_sync != authority.founder_timestamp
        || graph.initial_ack.value.successor.next_slot != graph_reservation.next_ack_slot
        || graph.membership.entry.value.created_at != authority.founder_timestamp
        || graph.membership.head.value.body.successor.next_slot
            != graph_reservation.membership.next_head_slot
    {
        return Err(DbError::Message(
            "signed founder graph differs from its durable slot reservation".to_string(),
        ));
    }

    let load_probe = |probe_id: crate::sync::provider::ProviderProbeId| {
        let key = format!("provider_probe/{}", hex::encode(probe_id.as_bytes()));
        let value = crate::database::required_protocol_state_on(conn, &key)?;
        let record = serde_json::from_str(&value)
            .map_err(|error| DbError::Message(format!("parse provider probe journal: {error}")))?;
        Ok::<_, DbError>((key, record))
    };
    let (exact_key, exact) = load_probe(authority.probes.exact_slots())?;
    let ProviderProbeJournalRecord::Exact(exact) = exact else {
        return Err(DbError::Message(
            "Store creation exact probe id names another probe kind".to_string(),
        ));
    };
    let ExactProbeProgress::ReceiptReady { receipt } = exact.progress else {
        return Err(DbError::Message(
            "Store creation exact probe has no terminal receipt".to_string(),
        ));
    };
    if receipt != descriptor.founder_provider_admin.capability.exact_slots {
        return Err(DbError::Message(
            "signed Store descriptor differs from its terminal exact probe".to_string(),
        ));
    }
    let mut consumed_keys = vec![exact_key];
    consumed_keys.push(STORE_CREATION_ATTEMPT_STATE_KEY.to_string());
    for key in consumed_keys {
        let deleted = crate::database::delete_protocol_state_on(conn, &key)?;
        if deleted != 1 {
            return Err(DbError::Message(
                "Store creation journal disappeared during typed consumption".to_string(),
            ));
        }
    }
    Ok(())
}

impl StoreDatabase {
    pub(in crate::sync::store) async fn begin_store_creation_attempt(
        &self,
        initialized: crate::sync::store::protocol_root::StoreCreationAttempt,
    ) -> Result<crate::sync::store::protocol_root::StoreCreationAttempt, DbError> {
        let value = serde_json::to_string(&initialized).map_err(|error| {
            DbError::Message(format!("serialize Store creation attempt: {error}"))
        })?;
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                tx.execute(
                    "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (
                        crate::sync::store::protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY,
                        &value,
                    ),
                )
                .map_err(DbError::from)?;
                let actual = crate::database::required_protocol_state_on(
                    &tx,
                    crate::sync::store::protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY,
                )?;
                tx.commit().map_err(DbError::from)?;
                serde_json::from_str(&actual).map_err(|error| {
                    DbError::Message(format!("parse Store creation attempt: {error}"))
                })
            })
            .await
    }

    pub(in crate::sync::store) async fn load_store_creation_attempt(
        &self,
    ) -> Result<Option<crate::sync::store::protocol_root::StoreCreationAttempt>, DbError> {
        self.sqlite()
            .call(move |conn| {
                let value = crate::database::get_protocol_state_on(
                    conn,
                    crate::sync::store::protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY,
                )?;
                value
                    .map(|value| {
                        serde_json::from_str(&value).map_err(|error| {
                            DbError::Message(format!("parse Store creation attempt: {error}"))
                        })
                    })
                    .transpose()
            })
            .await
    }

    pub(in crate::sync::store) async fn advance_store_creation_attempt(
        &self,
        previous: crate::sync::store::protocol_root::StoreCreationAttempt,
        next: crate::sync::store::protocol_root::StoreCreationAttempt,
    ) -> Result<(), DbError> {
        let previous = serde_json::to_string(&previous).map_err(|error| {
            DbError::Message(format!("serialize Store creation predecessor: {error}"))
        })?;
        let next = serde_json::to_string(&next).map_err(|error| {
            DbError::Message(format!("serialize Store creation successor: {error}"))
        })?;
        self.sqlite()
            .call(move |conn| {
                let changed = conn
                    .execute(
                        "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                        (
                            &next,
                            crate::sync::store::protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY,
                            &previous,
                        ),
                    )
                    .map_err(DbError::from)?;
                if changed != 1 {
                    return Err(DbError::Message(
                        "Store creation attempt advance lost its exact predecessor".to_string(),
                    ));
                }
                Ok(())
            })
            .await
    }
}
