use crate::query_mapped_rows;
use coven_protocol::store_commit::CircleAck;

use super::*;

pub(crate) fn verify_next_local_store_ack_on(
    conn: &Connection,
    authority: &coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
    bytes: &[u8],
    prepared: &PreparedExactObject,
) -> Result<(StoreAckRef, StoreAck), DbError> {
    let registration_ref = authority.reference();
    let registration = authority.value();
    let root = &registration.store_root;
    let unverified: StoreAck = serde_json::from_slice(bytes)
        .map_err(|error| DbError::context("parse Store acknowledgement", error))?;
    if &unverified.registration != registration_ref {
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
    let ack = StoreAck::parse_at(bytes, root, &reference, registration)
        .map_err(|error| DbError::context("verify Store acknowledgement", error))?;
    let previous = load_published_store_ack_on(conn)?;
    let (expected_sequence, expected_predecessor, expected_slot) = match &previous {
        Some(previous) => (
            previous.reference.sequence.checked_add(1).ok_or_else(|| {
                DbError::Message("Store acknowledgement sequence overflow".to_string())
            })?,
            Some(previous.reference.object.clone()),
            previous.successor_slot.clone(),
        ),
        None => (1, None, store_ack_first_slot(registration)?.clone()),
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
            .store_acknowledgement_activation(registration_ref)
            .map_err(DbError::from)?
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

pub(crate) fn store_ack_first_slot(
    registration: &StoreDeviceRegistration,
) -> Result<&coven_protocol::objects::ObjectSlot, DbError> {
    match &registration.acknowledgements {
        coven_protocol::store_commit::DeviceStreamAnchor::StoreAcknowledgements { first_slot } => {
            Ok(first_slot)
        }
        _ => Err(DbError::Message(
            "local Store registration has no acknowledgement stream anchor".to_string(),
        )),
    }
}

pub fn store_snapshot_first_slot(
    registration: &StoreDeviceRegistration,
) -> Result<&coven_protocol::objects::ObjectSlot, DbError> {
    match &registration.snapshots {
        coven_protocol::store_commit::DeviceStreamAnchor::StoreSnapshots { first_slot } => {
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
        let reference: StoreAckRef = serde_json::from_str(&reference)
            .map_err(|error| DbError::context("published Store acknowledgement ref", error))?;
        if reference.sequence == 0 {
            return Err(DbError::Message(
                "published Store acknowledgement uses sequence zero".to_string(),
            ));
        }
        Ok(PublishedStoreAck {
            reference,
            successor_slot: serde_json::from_str(&successor_slot).map_err(|error| {
                DbError::context("published Store acknowledgement successor slot", error)
            })?,
        })
    })
    .transpose()
}

pub(crate) fn finish_outbound_store_ack_on(
    conn: &Connection,
    reference: &StoreAckRef,
    successor_slot: &coven_protocol::objects::ObjectSlot,
) -> Result<(), DbError> {
    let removed = conn
        .execute(
            "DELETE FROM outbound_store_acks WHERE singleton = 1 AND ack_ref = ?1",
            [serde_json::to_string(reference)
                .map_err(|error| DbError::context("serialize Store acknowledgement ref", error))?],
        )
        .map_err(DbError::from)?;
    if removed != 1 {
        return Err(DbError::Message(
            "outbound Store acknowledgement disappeared".to_string(),
        ));
    }
    let successor_slot = serde_json::to_string(successor_slot).map_err(|error| {
        DbError::context("serialize Store acknowledgement successor slot", error)
    })?;
    conn.execute(
        "INSERT INTO published_store_acks (singleton, ack_ref, successor_slot) \
         VALUES (1, ?1, ?2) \
         ON CONFLICT(singleton) DO UPDATE SET \
           ack_ref = excluded.ack_ref, successor_slot = excluded.successor_slot",
        (
            serde_json::to_string(reference).map_err(|error| {
                DbError::context("serialize published Store acknowledgement ref", error)
            })?,
            successor_slot,
        ),
    )
    .map(|_| ())
    .map_err(DbError::from)
}

pub(crate) fn load_outbound_circle_acks_on(
    conn: &Connection,
    authority: &coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
) -> Result<Vec<coven_protocol::prepared_commit::CircleAckActivation>, DbError> {
    let registration = authority.value();
    let root = &registration.store_root;
    let rows = query_mapped_rows(
        conn,
        "SELECT ack_ref, ack_bytes, prepared_object FROM outbound_circle_acks
             ORDER BY circle_id",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )?;
    let mut activations = Vec::with_capacity(rows.len());
    for (reference, bytes, prepared) in rows {
        let reference: coven_protocol::store_commit::CircleAckRef =
            serde_json::from_str(&reference)
                .map_err(|error| DbError::context("outbound Circle acknowledgement ref", error))?;
        let prepared: PreparedExactObject = serde_json::from_str(&prepared)
            .map_err(|error| DbError::context("outbound prepared Circle acknowledgement", error))?;
        if prepared.reference() != &reference.object {
            return Err(DbError::Message(
                "outbound Circle acknowledgement ref differs from its prepared object".to_string(),
            ));
        }
        let value = CircleAck::parse_at(&bytes, root, &reference, registration)
            .map_err(|error| DbError::context("outbound Circle acknowledgement", error))?;
        activations.push(coven_protocol::prepared_commit::CircleAckActivation {
            reference,
            ack: ExactProtocolObject {
                value,
                bytes,
                prepared,
            },
        });
    }
    Ok(activations)
}

/// Load the singleton outbound Store acknowledgement and require it to match
/// `expected`; `mismatch` is the error when another acknowledgement is queued.
pub(crate) fn load_expected_outbound_store_ack_on(
    conn: &Connection,
    authority: &coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
    expected: &coven_protocol::store_commit::StoreAckRef,
    mismatch: &str,
) -> Result<OutboundStoreAck, DbError> {
    let outbound = load_outbound_store_ack_on(conn, authority)?
        .ok_or_else(|| DbError::Message("outbound Store acknowledgement is absent".to_string()))?;
    if &outbound.reference != expected {
        return Err(DbError::Message(mismatch.to_string()));
    }
    Ok(outbound)
}

/// Write the singleton outbound acknowledgement's activation column, keyed by
/// the acknowledgement reference it must still carry; `missing` is the error
/// when that row is gone.
pub(crate) fn set_outbound_store_ack_activation_on(
    conn: &Connection,
    expected: &coven_protocol::store_commit::StoreAckRef,
    activation: &crate::OutboundStoreAckActivation,
    missing: &str,
) -> Result<(), DbError> {
    let activation = serde_json::to_string(activation).map_err(|error| {
        DbError::context("serialize Merge Store acknowledgement activation", error)
    })?;
    let updated = conn
        .execute(
            "UPDATE outbound_store_acks SET activation = ?2 \
             WHERE singleton = 1 AND ack_ref = ?1",
            rusqlite::params![
                serde_json::to_string(expected).map_err(|error| DbError::context(
                    "serialize Store acknowledgement ref",
                    error
                ))?,
                activation,
            ],
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(missing.to_string()));
    }
    Ok(())
}

pub(crate) fn load_outbound_store_ack_on(
    conn: &Connection,
    authority: &coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
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
        let reference: StoreAckRef = serde_json::from_str(&reference)
            .map_err(|error| DbError::context("outbound Store acknowledgement ref", error))?;
        let prepared: PreparedExactObject = serde_json::from_str(&prepared)
            .map_err(|error| DbError::context("outbound prepared Store acknowledgement", error))?;
        let activation: OutboundStoreAckActivation =
            serde_json::from_str(&activation).map_err(|error| {
                DbError::context("outbound Store acknowledgement activation", error)
            })?;
        if prepared.reference() != &reference.object {
            return Err(DbError::Message(
                "outbound Store acknowledgement ref differs from its prepared object".to_string(),
            ));
        }
        let author_ref = authority.reference();
        let author = authority.value();
        let value = StoreAck::parse_at(&bytes, &author.store_root, &reference, author)
            .map_err(|error| DbError::context("outbound Store acknowledgement", error))?;
        if &value.registration != author_ref {
            return Err(DbError::Message(
                "outbound Store acknowledgement author differs from local activation".to_string(),
            ));
        }
        Ok(OutboundStoreAck {
            reference,
            ack: ExactProtocolObject {
                value,
                bytes,
                prepared,
            },
            circle_acknowledgements: load_outbound_circle_acks_on(conn, authority)?,
            activation,
        })
    })
    .transpose()
}
