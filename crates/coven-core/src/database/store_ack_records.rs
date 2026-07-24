use crate::database::local_store_identity::local_store_authority_on;
use crate::sync::store_commit::{CircleAck, CircleAckRef};

use super::*;

pub(crate) fn verify_next_local_store_ack_on(
    conn: &Connection,
    bytes: &[u8],
    prepared: &PreparedExactObject,
) -> Result<(StoreAckRef, StoreAck), DbError> {
    let (root, registration_ref, registration) = local_store_authority_on(conn)?;
    let unverified: StoreAck = serde_json::from_slice(bytes)
        .map_err(|error| DbError::Message(format!("parse Store acknowledgement: {error}")))?;
    if unverified.registration != registration_ref {
        return Err(DbError::Message(
            "Store acknowledgement author differs from local activation".to_string(),
        ));
    }
    let reference = StoreAckRef {
        registration: registration_ref.clone(),
        sequence: unverified.sequence,
        ack_hash: unverified.ack_hash(),
        object: prepared.reference().clone(),
    };
    let ack = StoreAck::parse_at(bytes, &root, &reference, &registration)
        .map_err(|error| DbError::Message(format!("verify Store acknowledgement: {error}")))?;
    let previous = load_published_store_ack_on(conn)?;
    let (expected_sequence, expected_predecessor, expected_slot) = match &previous {
        Some(previous) => (
            previous.reference.sequence.checked_add(1).ok_or_else(|| {
                DbError::Message("Store acknowledgement sequence overflow".to_string())
            })?,
            Some(previous.reference.object.clone()),
            previous.successor_slot.clone(),
        ),
        None => (1, None, store_ack_first_slot(&registration)?.clone()),
    };
    if ack.sequence != expected_sequence
        || ack.successor.predecessor != expected_predecessor
        || prepared.reference().slot() != &expected_slot
    {
        return Err(DbError::Message(
            "Store acknowledgement does not extend the exact local stream".to_string(),
        ));
    }
    let next_sequence = ack
        .sequence
        .checked_add(1)
        .ok_or_else(|| DbError::Message("Store acknowledgement sequence overflow".to_string()))?;
    if ack.successor.activation
        != registration
            .store_acknowledgement_activation(&registration_ref)
            .map_err(|error| DbError::Message(error.to_string()))?
            .activation_id()
        || ack.successor.next_slot.logical_key()
            != format!(
                "{}.json",
                ack_slot_prefix(&registration.device_id.to_string(), next_sequence)
            )
    {
        return Err(DbError::Message(
            "Store acknowledgement successor is outside its activated exact stream".to_string(),
        ));
    }
    Ok((reference, ack))
}

pub(super) fn store_ack_first_slot(
    registration: &StoreDeviceRegistration,
) -> Result<&crate::storage::cloud::ObjectSlot, DbError> {
    match &registration.acknowledgements {
        crate::sync::store_commit::DeviceStreamAnchor::StoreAcknowledgements { first_slot } => {
            Ok(first_slot)
        }
        _ => Err(DbError::Message(
            "local Store registration has no acknowledgement stream anchor".to_string(),
        )),
    }
}

pub(crate) fn store_snapshot_first_slot(
    registration: &StoreDeviceRegistration,
) -> Result<&crate::storage::cloud::ObjectSlot, DbError> {
    match &registration.snapshots {
        crate::sync::store_commit::DeviceStreamAnchor::StoreSnapshots { first_slot } => {
            Ok(first_slot)
        }
        _ => Err(DbError::Message(
            "local Store registration has no snapshot stream anchor".to_string(),
        )),
    }
}

pub(crate) fn load_published_store_ack_on(
    conn: &Connection,
) -> Result<Option<PublishedStoreAck>, DbError> {
    conn.query_row(
        "SELECT ack_ref, successor_slot FROM published_store_acks WHERE singleton = 1",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )
    .optional()
    .map_err(DbError::from)?
    .map(|(reference, successor_slot)| {
        let reference: StoreAckRef = serde_json::from_str(&reference).map_err(|error| {
            DbError::Message(format!("published Store acknowledgement ref: {error}"))
        })?;
        if reference.sequence == 0 {
            return Err(DbError::Message(
                "published Store acknowledgement uses sequence zero".to_string(),
            ));
        }
        Ok(PublishedStoreAck {
            reference,
            successor_slot: serde_json::from_str(&successor_slot).map_err(|error| {
                DbError::Message(format!(
                    "published Store acknowledgement successor slot: {error}"
                ))
            })?,
        })
    })
    .transpose()
}

fn record_published_store_ack_on(
    conn: &Connection,
    reference: &StoreAckRef,
    successor_slot: &crate::storage::cloud::ObjectSlot,
) -> Result<(), DbError> {
    let successor_slot = serde_json::to_string(successor_slot).map_err(|error| {
        DbError::Message(format!(
            "serialize Store acknowledgement successor slot: {error}"
        ))
    })?;
    conn.execute(
        "INSERT INTO published_store_acks (singleton, ack_ref, successor_slot) \
         VALUES (1, ?1, ?2) \
         ON CONFLICT(singleton) DO UPDATE SET \
           ack_ref = excluded.ack_ref, successor_slot = excluded.successor_slot",
        (
            serde_json::to_string(reference).map_err(|error| {
                DbError::Message(format!(
                    "serialize published Store acknowledgement ref: {error}"
                ))
            })?,
            successor_slot,
        ),
    )
    .map(|_| ())
    .map_err(DbError::from)
}

pub(crate) fn finish_outbound_store_ack_on(
    conn: &Connection,
    reference: &StoreAckRef,
    successor_slot: &crate::storage::cloud::ObjectSlot,
) -> Result<(), DbError> {
    let removed = conn
        .execute(
            "DELETE FROM outbound_store_acks WHERE singleton = 1 AND ack_ref = ?1",
            [serde_json::to_string(reference).map_err(|error| {
                DbError::Message(format!("serialize Store acknowledgement ref: {error}"))
            })?],
        )
        .map_err(DbError::from)?;
    if removed != 1 {
        return Err(DbError::Message(
            "outbound Store acknowledgement disappeared".to_string(),
        ));
    }
    record_published_store_ack_on(conn, reference, successor_slot)
}

pub(crate) fn record_activated_store_ack_on(
    conn: &Connection,
    commit: &StoreBatchCommit,
    commit_ref: &StoreBatchCommitRef,
) -> Result<(), DbError> {
    let Some(reference) = commit.acknowledgement() else {
        return Ok(());
    };
    if reference.registration != commit.author_registration {
        return Err(DbError::Message(
            "activated Store acknowledgement names another registration".to_string(),
        ));
    }
    let device_id = reference.registration.device_id.to_string();
    let current = conn
        .query_row(
            "SELECT ack_ref FROM activated_store_acks WHERE device_id = ?1",
            [&device_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .map(|raw| {
            serde_json::from_str::<StoreAckRef>(&raw).map_err(|error| {
                DbError::Message(format!("activated Store acknowledgement ref: {error}"))
            })
        })
        .transpose()?;
    if current.as_ref().is_some_and(|current| {
        current.registration != reference.registration || current.sequence >= reference.sequence
    }) {
        return Err(DbError::Message(
            "Store acknowledgement activation does not advance the exact registration stream"
                .to_string(),
        ));
    }
    conn.execute(
        "INSERT INTO activated_store_acks (device_id, ack_ref, activating_commit) \
         VALUES (?1, ?2, ?3) \
         ON CONFLICT(device_id) DO UPDATE SET \
           ack_ref = excluded.ack_ref, activating_commit = excluded.activating_commit",
        rusqlite::params![
            device_id,
            serde_json::to_string(reference).map_err(|error| DbError::Message(format!(
                "serialize activated Store acknowledgement ref: {error}"
            )))?,
            serde_json::to_string(commit_ref).map_err(|error| DbError::Message(format!(
                "serialize acknowledgement activating commit ref: {error}"
            )))?,
        ],
    )
    .map(|_| ())
    .map_err(DbError::from)
}

pub(crate) fn finish_outbound_circle_acks_on(
    conn: &Connection,
    circle_acknowledgements: &[crate::sync::store::CircleAckActivation],
) -> Result<(), DbError> {
    for circle in circle_acknowledgements {
        let circle_id = circle.reference.circle_id.to_string();
        let removed = conn
            .execute(
                "DELETE FROM outbound_circle_acks WHERE circle_id = ?1",
                [&circle_id],
            )
            .map_err(DbError::from)?;
        if removed != 1 {
            return Err(DbError::Message(
                "outbound Circle acknowledgement disappeared during completion".to_string(),
            ));
        }
        let successor_slot =
            serde_json::to_string(&circle.ack.value.successor.next_slot).map_err(|error| {
                DbError::Message(format!(
                    "serialize Circle acknowledgement successor slot: {error}"
                ))
            })?;
        let store_cut = serde_json::to_string(&circle.ack.value.store_cut).map_err(|error| {
            DbError::Message(format!("serialize Circle acknowledgement cut: {error}"))
        })?;
        let control_coord = serde_json::to_string(&circle.ack.value.control).map_err(|error| {
            DbError::Message(format!("serialize Circle acknowledgement control: {error}"))
        })?;
        conn.execute(
            "INSERT INTO published_circle_acks
               (circle_id, ack_ref, successor_slot, store_cut, control_coord)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(circle_id) DO UPDATE SET
               ack_ref = excluded.ack_ref, successor_slot = excluded.successor_slot,
               store_cut = excluded.store_cut, control_coord = excluded.control_coord",
            rusqlite::params![
                circle_id,
                serde_json::to_string(&circle.reference).map_err(|error| DbError::Message(
                    format!("serialize published Circle acknowledgement ref: {error}")
                ))?,
                successor_slot,
                store_cut,
                control_coord,
            ],
        )
        .map_err(DbError::from)?;
    }
    Ok(())
}

pub(crate) fn record_activated_circle_acks_on(
    conn: &Connection,
    commit: &StoreBatchCommit,
    commit_ref: &StoreBatchCommitRef,
) -> Result<(), DbError> {
    for reference in commit.circle_acknowledgements() {
        if reference.registration != commit.author_registration {
            return Err(DbError::Message(
                "activated Circle acknowledgement names another registration".to_string(),
            ));
        }
        let circle_id = reference.circle_id.to_string();
        let device_id = reference.registration.device_id.to_string();
        let current: Option<CircleAckRef> = conn
            .query_row(
                "SELECT ack_ref FROM activated_circle_acks
                 WHERE circle_id = ?1 AND device_id = ?2",
                rusqlite::params![circle_id, device_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|raw| {
                serde_json::from_str::<CircleAckRef>(&raw).map_err(|error| {
                    DbError::Message(format!("activated Circle acknowledgement ref: {error}"))
                })
            })
            .transpose()?;
        if current.as_ref().is_some_and(|current| {
            current.registration != reference.registration
                || current.circle_id != reference.circle_id
                || current.sequence >= reference.sequence
        }) {
            return Err(DbError::Message(
                "Circle acknowledgement activation does not advance the exact stream".to_string(),
            ));
        }
        conn.execute(
            "INSERT INTO activated_circle_acks (circle_id, device_id, ack_ref, activating_commit)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(circle_id, device_id) DO UPDATE SET
               ack_ref = excluded.ack_ref, activating_commit = excluded.activating_commit",
            rusqlite::params![
                circle_id,
                device_id,
                serde_json::to_string(reference).map_err(|error| DbError::Message(format!(
                    "serialize activated Circle acknowledgement ref: {error}"
                )))?,
                serde_json::to_string(commit_ref).map_err(|error| DbError::Message(format!(
                    "serialize Circle acknowledgement activating commit: {error}"
                )))?,
            ],
        )
        .map(|_| ())
        .map_err(DbError::from)?;
    }
    Ok(())
}

pub(crate) fn load_outbound_circle_acks_on(
    conn: &Connection,
) -> Result<Vec<crate::sync::store::CircleAckActivation>, DbError> {
    let (root, _, registration) = local_store_authority_on(conn)?;
    let mut statement = conn
        .prepare(
            "SELECT ack_ref, ack_bytes, prepared_object FROM outbound_circle_acks
             ORDER BY circle_id",
        )
        .map_err(DbError::from)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(DbError::from)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(DbError::from)?;
    drop(statement);
    let mut activations = Vec::with_capacity(rows.len());
    for (reference, bytes, prepared) in rows {
        let reference: crate::sync::store_commit::CircleAckRef = serde_json::from_str(&reference)
            .map_err(|error| {
            DbError::Message(format!("outbound Circle acknowledgement ref: {error}"))
        })?;
        let prepared: PreparedExactObject = serde_json::from_str(&prepared).map_err(|error| {
            DbError::Message(format!("outbound prepared Circle acknowledgement: {error}"))
        })?;
        if prepared.reference() != &reference.object {
            return Err(DbError::Message(
                "outbound Circle acknowledgement ref differs from its prepared object".to_string(),
            ));
        }
        let value =
            CircleAck::parse_at(&bytes, &root, &reference, &registration).map_err(|error| {
                DbError::Message(format!("outbound Circle acknowledgement: {error}"))
            })?;
        activations.push(crate::sync::store::CircleAckActivation {
            reference,
            ack: ExactProtocolObject {
                value,
                bytes,
                object: prepared.reference().clone(),
                prepared,
            },
        });
    }
    Ok(activations)
}

pub(crate) fn load_outbound_store_ack_on(
    conn: &Connection,
) -> Result<Option<OutboundStoreAck>, DbError> {
    conn.query_row(
        "SELECT ack_ref, ack_bytes, prepared_object, activation \
         FROM outbound_store_acks WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        },
    )
    .optional()
    .map_err(DbError::from)?
    .map(|(reference, bytes, prepared, activation)| {
        let reference: StoreAckRef = serde_json::from_str(&reference).map_err(|error| {
            DbError::Message(format!("outbound Store acknowledgement ref: {error}"))
        })?;
        let prepared: PreparedExactObject = serde_json::from_str(&prepared).map_err(|error| {
            DbError::Message(format!("outbound prepared Store acknowledgement: {error}"))
        })?;
        let activation: OutboundStoreAckActivation =
            serde_json::from_str(&activation).map_err(|error| {
                DbError::Message(format!(
                    "outbound Store acknowledgement activation: {error}"
                ))
            })?;
        if prepared.reference() != &reference.object {
            return Err(DbError::Message(
                "outbound Store acknowledgement ref differs from its prepared object".to_string(),
            ));
        }
        let (root, author_ref, author) = local_store_authority_on(conn)?;
        let value = StoreAck::parse_at(&bytes, &root, &reference, &author).map_err(|error| {
            DbError::Message(format!("outbound Store acknowledgement: {error}"))
        })?;
        if value.registration != author_ref {
            return Err(DbError::Message(
                "outbound Store acknowledgement author differs from local activation".to_string(),
            ));
        }
        Ok(OutboundStoreAck {
            reference,
            ack: ExactProtocolObject {
                value,
                bytes,
                object: prepared.reference().clone(),
                prepared,
            },
            circle_acknowledgements: load_outbound_circle_acks_on(conn)?,
            activation,
        })
    })
    .transpose()
}
