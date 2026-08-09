use crate::*;
use coven_protocol::store_commit::{StoreAck, StoreAckRef, StoreDeviceRegistrationRef};
use rusqlite::OptionalExtension;

use super::*;
use crate::store_ack_records::{load_expected_outbound_store_ack_on, load_outbound_store_ack_on};

impl StoreSession<'_> {
    fn latest_local_store_ack(&self) -> Result<Option<PublishedStoreAck>, DbError> {
        load_published_store_ack_on(self.records.conn)
    }

    fn activated_store_ack(
        &self,
        registration: &StoreDeviceRegistrationRef,
    ) -> Result<Option<StoreAckRef>, DbError> {
        self.records
            .conn
            .query_row(
                "SELECT ack_ref FROM activated_store_acks WHERE device_id = ?1",
                [registration.device_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|raw| {
                let reference: StoreAckRef = serde_json::from_str(&raw).map_err(|error| {
                    DbError::context("activated Store acknowledgement ref", error)
                })?;
                if &reference.registration != registration {
                    return Err(DbError::Message(
                        "activated Store acknowledgement names another registration".to_string(),
                    ));
                }
                Ok(reference)
            })
            .transpose()
    }

    fn stage_store_ack(
        &mut self,
        ack: StoreAck,
        prepared: PreparedExactObject,
    ) -> Result<StoreAckRef, DbError> {
        let authority = self.local_store_authority()?;
        let bytes = ack.to_bytes();
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let (reference, verified) =
            verify_next_local_store_ack_on(&tx, &authority, &bytes, &prepared)?;
        if verified != ack {
            return Err(DbError::Message(
                "staged Store acknowledgement changed during exact verification".to_string(),
            ));
        }
        let ack_ref = serde_json::to_string(&reference).map_err(|error| {
            DbError::context("serialize exact Store acknowledgement ref", error)
        })?;
        let prepared = serde_json::to_string(&prepared)
            .map_err(|error| DbError::context("serialize prepared Store acknowledgement", error))?;
        let activation = serde_json::to_string(&OutboundStoreAckActivation::AwaitingCandidate)
            .map_err(|error| {
                DbError::context("serialize Store acknowledgement activation state", error)
            })?;
        tx.execute(
            "INSERT INTO outbound_store_acks
             (singleton, ack_ref, ack_bytes, prepared_object, activation)
             VALUES (1, ?1, ?2, ?3, ?4)",
            rusqlite::params![ack_ref, bytes, prepared, activation],
        )
        .map_err(DbError::from)?;
        tx.commit().map_err(DbError::from)?;
        Ok(reference)
    }

    fn adopt_outbound_store_ack_slot_winner(
        &mut self,
        expected: &StoreAckRef,
        winner_bytes: Vec<u8>,
        winner_prepared: PreparedExactObject,
    ) -> Result<(), DbError> {
        let authority = self.local_store_authority()?;
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let outbound = load_expected_outbound_store_ack_on(
            &tx,
            &authority,
            expected,
            "acknowledgement slot winner names another queued object",
        )?;
        let OutboundStoreAckActivation::Prepared(candidate) = &outbound.activation else {
            return Err(DbError::Message(
                "acknowledgement slot collision has no prepared activation candidate".to_string(),
            ));
        };
        if candidate.commit.acknowledgement() != Some(expected) {
            return Err(DbError::Message(
                "prepared activation candidate names another acknowledgement".to_string(),
            ));
        }
        if winner_prepared.reference().slot() != expected.object.slot()
            || winner_prepared.reference() == &expected.object
        {
            return Err(DbError::Message(
                "acknowledgement slot winner is not a distinct object at the occupied slot"
                    .to_string(),
            ));
        }
        let (winner_reference, _) =
            verify_next_local_store_ack_on(&tx, &authority, &winner_bytes, &winner_prepared)?;
        let expected_records = candidate
            .acknowledgement_remote_objects(&outbound.ack)
            .map_err(|error| DbError::Message(error.to_string()))?;
        for expected_record in &expected_records {
            let object_id = expected_record.object_id();
            let stored = load_remote_object_on(&tx, object_id)?;
            if stored != **expected_record {
                return Err(DbError::Message(
                    "losing acknowledgement candidate is no longer wholly unuploaded".to_string(),
                ));
            }
        }
        for expected_record in expected_records {
            if !crate::remote_object_records::delete_remote_object_on(
                &tx,
                expected_record.object_id(),
            )? {
                return Err(DbError::Message(
                    "losing acknowledgement candidate object disappeared".to_string(),
                ));
            }
        }
        let activation = serde_json::to_string(&OutboundStoreAckActivation::AwaitingCandidate)
            .map_err(|error| {
                DbError::context("serialize adopted Store acknowledgement activation", error)
            })?;
        let winner_ref = serde_json::to_string(&winner_reference).map_err(|error| {
            DbError::context("serialize adopted Store acknowledgement ref", error)
        })?;
        let winner_prepared = serde_json::to_string(&winner_prepared).map_err(|error| {
            DbError::context("serialize adopted prepared Store acknowledgement", error)
        })?;
        let updated = tx
            .execute(
                "UPDATE outbound_store_acks
                 SET ack_ref = ?2, ack_bytes = ?3, prepared_object = ?4, activation = ?5
                 WHERE singleton = 1 AND ack_ref = ?1",
                rusqlite::params![
                    serde_json::to_string(expected).map_err(|error| DbError::Message(format!(
                        "serialize losing Store acknowledgement ref: {error}"
                    )))?,
                    winner_ref,
                    winner_bytes,
                    winner_prepared,
                    activation,
                ],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(
                "outbound Store acknowledgement changed during winner adoption".to_string(),
            ));
        }
        tx.commit().map_err(DbError::from)
    }

    fn oldest_outbound_store_ack(&mut self) -> Result<Option<OutboundStoreAck>, DbError> {
        let authority = self.local_store_authority()?;
        load_outbound_store_ack_on(self.records.conn, &authority)
    }

    fn complete_outbound_store_ack(&mut self, accepted: &StoreAckRef) -> Result<(), DbError> {
        let authority = self.local_store_authority()?;
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let outbound = load_expected_outbound_store_ack_on(
            &tx,
            &authority,
            accepted,
            "accepted Store acknowledgement differs from the prepared exact object",
        )?;
        finish_outbound_store_ack_on(&tx, accepted, &outbound.ack.value.successor.next_slot)?;
        for circle in &outbound.circle_acknowledgements {
            let circle_id = circle.reference.circle_id.to_string();
            let removed = tx
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
            let successor_slot = serde_json::to_string(&circle.ack.value.successor.next_slot)
                .map_err(|error| {
                    DbError::context("serialize Circle acknowledgement successor slot", error)
                })?;
            let store_cut = serde_json::to_string(&circle.ack.value.store_cut)
                .map_err(|error| DbError::context("serialize Circle acknowledgement cut", error))?;
            let control_coord =
                serde_json::to_string(&circle.ack.value.control).map_err(|error| {
                    DbError::context("serialize Circle acknowledgement control", error)
                })?;
            tx.execute(
                "INSERT INTO published_circle_acks
                   (circle_id, ack_ref, successor_slot, store_cut, control_coord)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(circle_id) DO UPDATE SET
                   ack_ref = excluded.ack_ref, successor_slot = excluded.successor_slot,
                   store_cut = excluded.store_cut, control_coord = excluded.control_coord",
                rusqlite::params![
                    circle_id,
                    serde_json::to_string(&circle.reference).map_err(|error| {
                        DbError::context("serialize published Circle acknowledgement ref", error)
                    })?,
                    successor_slot,
                    store_cut,
                    control_coord,
                ],
            )
            .map_err(DbError::from)?;
        }
        tx.commit().map_err(DbError::from)
    }
}

impl StoreDatabase {
    pub async fn latest_local_store_ack(&self) -> Result<Option<PublishedStoreAck>, DbError> {
        self.connection
            .call_store(|session| session.latest_local_store_ack())
            .await
    }

    pub async fn activated_store_ack(
        &self,
        registration: &StoreDeviceRegistrationRef,
    ) -> Result<Option<StoreAckRef>, DbError> {
        let registration = registration.clone();
        self.connection
            .call_store(move |session| session.activated_store_ack(&registration))
            .await
    }

    pub async fn stage_store_ack(
        &self,
        ack: StoreAck,
        prepared: PreparedExactObject,
    ) -> Result<StoreAckRef, DbError> {
        self.connection
            .call_store(move |session| session.stage_store_ack(ack, prepared))
            .await
    }

    pub async fn adopt_outbound_store_ack_slot_winner(
        &self,
        expected: StoreAckRef,
        winner_bytes: Vec<u8>,
        winner_prepared: PreparedExactObject,
    ) -> Result<(), DbError> {
        self.connection
            .call_store(move |session| {
                session.adopt_outbound_store_ack_slot_winner(
                    &expected,
                    winner_bytes,
                    winner_prepared,
                )
            })
            .await
    }

    pub async fn oldest_outbound_store_ack(&self) -> Result<Option<OutboundStoreAck>, DbError> {
        self.connection
            .call_store(|session| session.oldest_outbound_store_ack())
            .await
    }

    pub async fn complete_outbound_store_ack(&self, accepted: StoreAckRef) -> Result<(), DbError> {
        self.connection
            .call_store(move |session| session.complete_outbound_store_ack(&accepted))
            .await
    }
}
