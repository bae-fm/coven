use crate::database::*;

pub(super) fn consume_store_creation_probes_on(
    conn: &rusqlite::Connection,
    graph: &DurableFounderGraph,
) -> Result<(), DbError> {
    use crate::protocol::provider::{ExactProbeProgress, ProviderProbeJournalRecord};
    use crate::sync::{StoreCreationAttempt, STORE_CREATION_ATTEMPT_STATE_KEY};

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

    let load_probe = |probe_id: crate::protocol::provider::ProviderProbeId| {
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
