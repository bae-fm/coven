use crate::*;
use coven_protocol::objects::PreparedExactObject;
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, StoreAck, StoreAckRef, StoreBatchCommit,
};
use rusqlite::{Connection, OptionalExtension};

pub(crate) fn record_activated_store_device_registrations_on(
    conn: &Connection,
    commit: &StoreBatchCommit,
    registrations: &[ActivatedStoreDeviceRegistration],
) -> Result<(), DbError> {
    if registrations.len() != commit.device_registrations().len() {
        return Err(DbError::Message(
            "Store device registration activation count differs from the signed commit".to_string(),
        ));
    }
    for signed in commit.device_registrations() {
        let activated = registrations
            .iter()
            .find(|registration| registration.value().device_id == signed.registration.device_id)
            .ok_or_else(|| {
                DbError::Message(format!(
                    "Store commit is missing registration bytes for {}",
                    signed.registration.device_id
                ))
            })?;
        let registration = activated.value();
        let authority = activated.activation();
        signed
            .registration
            .verify_registration(registration)
            .map_err(DbError::from)?;
        if registration.store_root.store_root_hash != commit.store_root_hash {
            return Err(DbError::Message(format!(
                "Store registration {} belongs to a different Store",
                registration.device_id
            )));
        }
        let expected_authority = match (&registration.origin, &signed.authority) {
            (
                coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Join {
                    attempt_id: origin_attempt,
                    outcome_slot,
                    ..
                },
                coven_protocol::store_commit::StoreDeviceRegistrationActivationRef::Join {
                    attempt_id,
                    outcome,
                },
            ) if origin_attempt == attempt_id && outcome_slot == outcome.slot() => {
                coven_protocol::store_commit::StoreDeviceRegistrationActivation::Join {
                    attempt_id: *attempt_id,
                    outcome: outcome.clone(),
                }
            }
            (
                coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Recovery {
                    recovery_id: origin_recovery,
                    recovery_slot,
                    ..
                },
                coven_protocol::store_commit::StoreDeviceRegistrationActivationRef::Recovery {
                    recovery_id,
                    node,
                },
            ) if origin_recovery == recovery_id && recovery_slot == node.slot() => {
                coven_protocol::store_commit::StoreDeviceRegistrationActivation::Recovery {
                    recovery_id: *recovery_id,
                    node: node.clone(),
                }
            }
            _ => {
                return Err(DbError::Message(format!(
                    "Store registration {} origin differs from its signed activation authority",
                    registration.device_id
                )))
            }
        };
        if authority != &expected_authority {
            return Err(DbError::Message(format!(
                "verified Store registration {} authority differs from the signed commit",
                registration.device_id
            )));
        }
        let registration_bytes = registration.to_bytes();
        let registration_object = serde_json::to_string(&signed.registration)
            .map_err(|error| DbError::context("serialize Store registration exact ref", error))?;
        let activation_authority = serde_json::to_string(authority)
            .map_err(|error| DbError::context("serialize Store registration authority", error))?;
        let inserted = conn
            .execute(
                "INSERT INTO store_device_registration_activations
                     (device_id, registration_hash, author_pubkey, device_signing_pubkey,
                      registration_bytes, registration_object, activation_authority)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(device_id) DO NOTHING",
                rusqlite::params![
                    registration.device_id.to_string(),
                    signed.registration.registration_hash.to_string(),
                    registration.author_pubkey,
                    registration.device_signing_pubkey,
                    registration_bytes,
                    registration_object,
                    activation_authority,
                ],
            )
            .map_err(DbError::from)?;
        if inserted == 0 {
            let existing: (String, Vec<u8>, String, String) = conn
                .query_row(
                    "SELECT registration_hash, registration_bytes, registration_object,
                                activation_authority
                         FROM store_device_registration_activations WHERE device_id = ?1",
                    [registration.device_id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(DbError::from)?;
            if existing
                != (
                    signed.registration.registration_hash.to_string(),
                    registration.to_bytes(),
                    serde_json::to_string(&signed.registration).map_err(|error| {
                        DbError::context("serialize Store registration exact ref", error)
                    })?,
                    serde_json::to_string(authority).map_err(|error| {
                        DbError::context("serialize Store registration authority", error)
                    })?,
                )
            {
                return Err(DbError::Message(format!(
                    "Store device {} already has a different one-shot registration",
                    registration.device_id
                )));
            }
        }
        let local: Option<LocalDeviceRegistrationJournalRow> = conn
            .query_row(
                "SELECT device_id, registration_hash, registration_bytes, prepared_object, \
                            initial_ack_ref, initial_ack_bytes, initial_ack_prepared, state \
                     FROM local_store_device_registration WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?;
        let Some((
            local_device,
            local_hash,
            local_bytes,
            local_prepared,
            local_ack_ref,
            local_ack_bytes,
            local_ack_prepared,
            local_state,
        )) = local
        else {
            continue;
        };
        if local_device != registration.device_id.to_string() {
            continue;
        }
        let local_prepared: PreparedExactObject = serde_json::from_str(&local_prepared)
            .map_err(|error| DbError::context("local registration object", error))?;
        let local_ack_ref: StoreAckRef = serde_json::from_str(&local_ack_ref)
            .map_err(|error| DbError::context("local initial ack ref", error))?;
        let local_ack_prepared: PreparedExactObject = serde_json::from_str(&local_ack_prepared)
            .map_err(|error| DbError::context("local initial ack object", error))?;
        if local_hash != signed.registration.registration_hash.to_string()
            || local_bytes != registration.to_bytes()
            || local_prepared.reference() != &signed.registration.object
            || local_ack_prepared.reference() != &local_ack_ref.object
        {
            return Err(DbError::Message(
                "activating commit differs from the complete local registration ref".to_string(),
            ));
        }
        let ack = StoreAck::parse_at(
            &local_ack_bytes,
            &registration.store_root,
            &local_ack_ref,
            registration,
        )
        .map_err(|error| DbError::context("local initial ack", error))?;
        if ack.sequence != 1
            || ack.successor.predecessor.is_some()
            || local_ack_prepared
                .reference()
                .verify(local_ack_prepared.stored_bytes())
                .is_err()
        {
            return Err(DbError::Message(
                "local registration journal does not carry an initial acknowledgement".to_string(),
            ));
        }
        let state: LocalDeviceRegistrationState = serde_json::from_str(&local_state)
            .map_err(|error| DbError::context("local registration journal", error))?;
        let activated_state = LocalDeviceRegistrationState::Activated {
            authority: authority.clone(),
        };
        match state {
            LocalDeviceRegistrationState::Prepared => {
                return Err(DbError::Message(
                    "Store commit cannot activate a registration before exact creation".to_string(),
                ));
            }
            LocalDeviceRegistrationState::Created => {
                let updated = conn
                    .execute(
                        "UPDATE local_store_device_registration SET state = ?1 \
                             WHERE singleton = 1 AND device_id = ?2 AND registration_hash = ?3 \
                               AND initial_ack_ref = ?4 AND state = ?5",
                        rusqlite::params![
                            serde_json::to_string(&activated_state).map_err(|error| {
                                DbError::context("serialize activated journal", error)
                            })?,
                            local_device,
                            local_hash,
                            serde_json::to_string(&local_ack_ref).map_err(|error| {
                                DbError::context("serialize local initial ack", error)
                            })?,
                            serde_json::to_string(&LocalDeviceRegistrationState::Created).map_err(
                                |error| DbError::context("serialize created journal", error)
                            )?,
                        ],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "local registration journal changed during activation".to_string(),
                    ));
                }
                conn.execute(
                    "INSERT INTO published_store_acks (singleton, ack_ref, successor_slot) \
                         VALUES (1, ?1, ?2)",
                    rusqlite::params![
                        serde_json::to_string(&local_ack_ref).map_err(|error| {
                            DbError::context("serialize activated initial ack", error)
                        })?,
                        serde_json::to_string(&ack.successor.next_slot).map_err(|error| {
                            DbError::context("serialize activated ack successor", error)
                        })?,
                    ],
                )
                .map_err(DbError::from)?;
                crate::set_protocol_state_on(
                    conn,
                    LOCAL_DEVICE_ID_STATE_KEY,
                    &registration.device_id.to_string(),
                )?;
            }
            LocalDeviceRegistrationState::Activated {
                authority: existing,
            } if existing == *authority => {
                let stored_ack: (String, String) = conn
                    .query_row(
                        "SELECT ack_ref, successor_slot FROM published_store_acks \
                             WHERE singleton = 1",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let local_device_id =
                    crate::get_protocol_state_on(conn, LOCAL_DEVICE_ID_STATE_KEY)?;
                if stored_ack.0
                    != serde_json::to_string(&local_ack_ref).map_err(|error| {
                        DbError::context("serialize replayed initial ack", error)
                    })?
                    || stored_ack.1
                        != serde_json::to_string(&ack.successor.next_slot).map_err(|error| {
                            DbError::context("serialize replayed ack successor", error)
                        })?
                    || local_device_id.as_deref() != Some(local_device.as_str())
                {
                    return Err(DbError::Message(
                        "activated local journal differs from its exact initial ack".to_string(),
                    ));
                }
            }
            LocalDeviceRegistrationState::Activated { .. } => {
                return Err(DbError::Message(
                    "local registration already has another exact activation authority".to_string(),
                ));
            }
        }
    }
    Ok(())
}
