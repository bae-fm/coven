use crate::*;
use coven_protocol::store_commit::{
    SnapshotMeta, StoreAck, StoreAckRef, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StoreSnapshotRef,
};

use super::*;

impl StoreDatabase {
    pub async fn latest_local_store_device_registration(
        &self,
    ) -> Result<Option<DurableDeviceRegistration>, DbError> {
        self.read_local_store_device_registration(
            "SELECT device_id, registration_hash, registration_bytes, prepared_object, \
                    initial_ack_ref, initial_ack_bytes, initial_ack_prepared, state \
             FROM local_store_device_registration WHERE singleton = 1",
        )
        .await
    }

    pub async fn export_activated_device_continuation(
        &self,
        identity_signer: &coven_keys::keys::UserKeypair,
    ) -> Result<coven_protocol::recovery::ActivatedContinuation, DbError> {
        let durable = self
            .latest_local_store_device_registration()
            .await?
            .ok_or_else(|| DbError::Message("local Store device registration is absent".into()))?;
        let LocalDeviceRegistrationState::Activated { authority } = durable.state else {
            return Err(DbError::Message(
                "local Store device registration is not activated".into(),
            ));
        };
        let root = self
            .local_store_root_ref()
            .await?
            .ok_or_else(|| DbError::Message("local Store root hash is absent".into()))?;
        let registration = StoreDeviceRegistration::parse_at(
            &durable.registration_bytes,
            &root,
            durable.device_id,
        )
        .map_err(|error| DbError::context("local Store registration", error))?;
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration,
            durable.prepared.reference().clone(),
        );
        if registration_ref.registration_hash != durable.registration_hash {
            return Err(DbError::Message(
                "local Store registration hash differs from its exact object".into(),
            ));
        }
        let device_signer = registration
            .device_signer(identity_signer)
            .map_err(|error| DbError::context("local device signer", error))?;
        let latest_ack = self
            .latest_local_store_ack()
            .await?
            .ok_or_else(|| DbError::Message("local Store acknowledgement is absent".into()))?;
        let announcement_stream_id =
            coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &registration_ref,
                coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
        Ok(coven_protocol::recovery::ActivatedContinuation {
            identity_signing_secret: hex::encode(identity_signer.to_keypair_bytes()),
            device_signing_secret: hex::encode(device_signer.to_keypair_bytes()),
            registration: registration_ref,
            registration_bytes: durable.registration_bytes,
            registration_prepared: durable.prepared,
            initial_ack: durable.initial_ack_ref,
            initial_ack_bytes: durable.initial_ack.bytes,
            initial_ack_prepared: durable.initial_ack.prepared,
            activation: authority,
            latest_ack: latest_ack.reference,
            latest_snapshot: self
                .latest_local_store_snapshot()
                .await?
                .map(|snapshot| snapshot.reference),
            latest_position: self
                .latest_local_store_position(announcement_stream_id)
                .await?,
        })
    }

    pub async fn install_activated_device_continuation(
        &self,
        continuation: coven_protocol::recovery::ActivatedContinuation,
        identity_signer: &coven_keys::keys::UserKeypair,
        device_signer: &coven_keys::keys::UserKeypair,
        ack_chain: Vec<(StoreAckRef, StoreAck)>,
        latest_snapshot: Option<(StoreSnapshotRef, SnapshotMeta)>,
    ) -> Result<(), DbError> {
        let root = self
            .local_store_root_ref()
            .await?
            .ok_or_else(|| DbError::Message("local Store root hash is absent".into()))?;
        let registration = StoreDeviceRegistration::parse_at(
            &continuation.registration_bytes,
            &root,
            continuation.registration.device_id,
        )
        .map_err(|error| DbError::context("continued Store registration", error))?;
        continuation
            .registration
            .verify_registration(&registration)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let derived_device = registration
            .device_signer(identity_signer)
            .map_err(|error| DbError::context("continued device signer", error))?;
        if derived_device.to_keypair_bytes() != device_signer.to_keypair_bytes()
            || continuation.registration_prepared.reference() != &continuation.registration.object
            || continuation.initial_ack_prepared.reference() != &continuation.initial_ack.object
        {
            return Err(DbError::Message(
                "continued device keys or exact registration objects differ".into(),
            ));
        }
        let initial_ack = StoreAck::parse_at(
            &continuation.initial_ack_bytes,
            &root,
            &continuation.initial_ack,
            &registration,
        )
        .map_err(|error| DbError::context("continued initial ack", error))?;
        let Some((latest_ack_ref, latest_ack)) = ack_chain.first() else {
            return Err(DbError::Message(
                "continued acknowledgement chain is empty".into(),
            ));
        };
        if initial_ack.sequence != 1
            || initial_ack.successor.predecessor.is_some()
            || latest_ack.registration != continuation.registration
            || latest_ack_ref != &continuation.latest_ack
            || ack_chain.last().map(|(reference, _)| reference) != Some(&continuation.initial_ack)
            || ack_chain.windows(2).any(|pair| {
                pair[0].1.successor.predecessor.as_ref() != Some(&pair[1].0.object)
                    || pair[0].0.sequence != pair[0].1.sequence
                    || pair[1].0.sequence != pair[1].1.sequence
                    || pair[0].0.registration != pair[0].1.registration
                    || pair[1].0.registration != pair[1].1.registration
            })
        {
            return Err(DbError::Message(
                "continued acknowledgement chain differs from its exact authority".into(),
            ));
        }
        let latest_successor_slot = latest_ack.successor.next_slot.clone();
        match (&continuation.latest_snapshot, &latest_snapshot) {
            (None, None) => {}
            (Some(expected), Some((reference, meta))) if expected == reference => {
                let verified = SnapshotMeta::parse_stream_entry_at(
                    &meta.to_bytes(),
                    &root,
                    &continuation.registration,
                    &registration,
                    reference,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                if verified != *meta {
                    return Err(DbError::Message(
                        "continued snapshot changed during exact verification".into(),
                    ));
                }
            }
            _ => {
                return Err(DbError::Message(
                    "continued snapshot stream differs from its exact authority".into(),
                ));
            }
        }

        self.connection
            .call_store(move |session| {
                session.install_activated_device_continuation(
                    continuation,
                    registration,
                    ack_chain,
                    latest_snapshot,
                    latest_successor_slot,
                )
            })
            .await
    }
}

impl StoreSession<'_> {
    fn install_activated_device_continuation(
        &mut self,
        continuation: coven_protocol::recovery::ActivatedContinuation,
        registration: StoreDeviceRegistration,
        ack_chain: Vec<(StoreAckRef, StoreAck)>,
        latest_snapshot: Option<(StoreSnapshotRef, SnapshotMeta)>,
        latest_successor_slot: coven_protocol::objects::ObjectSlot,
    ) -> Result<(), DbError> {
        let activated = self.activated_registration(&continuation.registration)?;
        if activated.value() != &registration {
            return Err(DbError::Message(
                "continued registration differs from activated Store state".into(),
            ));
        }
        let conn = self.records.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let stored_authority: String = tx
            .query_row(
                "SELECT activation_authority FROM store_device_registration_activations \
                     WHERE device_id = ?1 AND registration_hash = ?2",
                (
                    continuation.registration.device_id.to_string(),
                    continuation.registration.registration_hash.to_string(),
                ),
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let stored_authority: coven_protocol::store_commit::StoreDeviceRegistrationActivation =
            serde_json::from_str(&stored_authority)
                .map_err(|error| DbError::context("continued activation authority", error))?;
        if stored_authority != continuation.activation {
            return Err(DbError::Message(
                "continued registration has another activation authority".into(),
            ));
        }
        if let Some(position) = &continuation.latest_position {
            let stream_id = position.coord.stream_id.to_string();
            let restored_position = StoreDatabase::latest_position_for_device_on(&tx, &stream_id)?;
            if restored_position.as_ref() != Some(position) {
                return Err(DbError::Message(
                    "continued device position is absent from restored history".into(),
                ));
            }
        }
        let existing_local: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM local_store_device_registration",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let existing_ack: i64 = tx
            .query_row("SELECT COUNT(*) FROM published_store_acks", [], |row| {
                row.get(0)
            })
            .map_err(DbError::from)?;
        let existing_snapshot = load_published_store_snapshot_on(&tx, &activated)?;
        let existing_device = crate::get_protocol_state_on(&tx, LOCAL_DEVICE_ID_STATE_KEY)?;
        let state = serde_json::to_string(&LocalDeviceRegistrationState::Activated {
            authority: continuation.activation.clone(),
        })
        .map_err(|error| DbError::context("continued activation", error))?;
        let expected_local = (
            continuation.registration.device_id.to_string(),
            continuation.registration.registration_hash.to_string(),
            continuation.registration_bytes.clone(),
            serde_json::to_string(&continuation.registration_prepared)
                .map_err(|error| DbError::context("continued registration object", error))?,
            serde_json::to_string(&continuation.initial_ack)
                .map_err(|error| DbError::context("continued initial ack ref", error))?,
            continuation.initial_ack_bytes.clone(),
            serde_json::to_string(&continuation.initial_ack_prepared)
                .map_err(|error| DbError::context("continued initial ack object", error))?,
            state,
        );
        match existing_local {
            0 => {
                tx.execute(
                    "INSERT INTO local_store_device_registration \
                         (singleton, device_id, registration_hash, registration_bytes, \
                          prepared_object, initial_ack_ref, initial_ack_bytes, \
                          initial_ack_prepared, state) \
                         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        expected_local.0,
                        expected_local.1,
                        expected_local.2,
                        expected_local.3,
                        expected_local.4,
                        expected_local.5,
                        expected_local.6,
                        expected_local.7,
                    ],
                )
                .map_err(DbError::from)?;
            }
            1 => {
                let actual = tx
                    .query_row(
                        "SELECT device_id, registration_hash, registration_bytes, \
                             prepared_object, initial_ack_ref, initial_ack_bytes, \
                             initial_ack_prepared, state FROM local_store_device_registration \
                             WHERE singleton = 1",
                        [],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, Vec<u8>>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                                row.get::<_, Vec<u8>>(5)?,
                                row.get::<_, String>(6)?,
                                row.get::<_, String>(7)?,
                            ))
                        },
                    )
                    .map_err(DbError::from)?;
                if actual != expected_local {
                    return Err(DbError::Message(
                        "restored local device state differs from continuation".into(),
                    ));
                }
            }
            _ => {
                return Err(DbError::Message(
                    "restored database carries multiple local device journals".into(),
                ));
            }
        }
        match existing_ack {
            0 => {}
            1 => {
                let (stored_ref, stored_successor): (String, String) = tx
                    .query_row(
                        "SELECT ack_ref, successor_slot FROM published_store_acks \
                             WHERE singleton = 1",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let stored_ref: StoreAckRef = serde_json::from_str(&stored_ref)
                    .map_err(|error| DbError::context("restored acknowledgement", error))?;
                let Some((_, stored_ack)) = ack_chain
                    .iter()
                    .find(|(reference, _)| reference == &stored_ref)
                else {
                    return Err(DbError::Message(
                        "restored acknowledgement is outside the continuation chain".into(),
                    ));
                };
                if stored_successor
                    != serde_json::to_string(&stored_ack.successor.next_slot)
                        .map_err(|error| DbError::context("restored ack successor", error))?
                {
                    return Err(DbError::Message(
                        "restored acknowledgement successor differs from its signature".into(),
                    ));
                }
            }
            _ => {
                return Err(DbError::Message(
                    "restored database carries multiple local acknowledgements".into(),
                ));
            }
        }
        match (existing_snapshot, latest_snapshot.as_ref()) {
            (None, None) => {}
            (None, Some((reference, meta))) => {
                let generation = i64::try_from(reference.generation).map_err(|_| {
                    DbError::Message(
                        "continued Store snapshot generation exceeds SQLite INTEGER".to_string(),
                    )
                })?;
                tx.execute(
                    "INSERT INTO published_store_snapshot \
                         (generation, snapshot_ref, successor_slot, meta_bytes) \
                         VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        generation,
                        serde_json::to_string(reference).map_err(|error| {
                            DbError::context("serialize continued Store snapshot ref", error)
                        })?,
                        serde_json::to_string(&meta.successor.next_slot).map_err(|error| {
                            DbError::context("serialize continued Store snapshot successor", error)
                        })?,
                        meta.to_bytes(),
                    ],
                )
                .map_err(DbError::from)?;
            }
            (Some(actual), Some((reference, meta))) => {
                if actual.reference != *reference
                    || actual.successor_slot != meta.successor.next_slot
                    || actual.meta != *meta
                {
                    return Err(DbError::Message(
                        "restored local snapshot stream differs from continuation".into(),
                    ));
                }
            }
            (Some(_), None) => {
                return Err(DbError::Message(
                    "restored database carries a snapshot outside the continuation".into(),
                ));
            }
        }
        let latest_ref = serde_json::to_string(&continuation.latest_ack)
            .map_err(|error| DbError::context("continued latest ack", error))?;
        let latest_successor = serde_json::to_string(&latest_successor_slot)
            .map_err(|error| DbError::context("continued ack successor", error))?;
        if existing_ack == 0 {
            tx.execute(
                "INSERT INTO published_store_acks (singleton, ack_ref, successor_slot) \
                     VALUES (1, ?1, ?2)",
                (&latest_ref, &latest_successor),
            )
            .map_err(DbError::from)?;
        } else {
            tx.execute(
                "UPDATE published_store_acks SET ack_ref = ?1, successor_slot = ?2 \
                     WHERE singleton = 1",
                (&latest_ref, &latest_successor),
            )
            .map_err(DbError::from)?;
        }
        match existing_device {
            Some(existing) if existing == continuation.registration.device_id.to_string() => {}
            Some(_) => {
                return Err(DbError::Message(
                    "restored local device id differs from continuation".into(),
                ));
            }
            None => {
                tx.execute(
                    "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (
                        LOCAL_DEVICE_ID_STATE_KEY,
                        continuation.registration.device_id.to_string(),
                    ),
                )
                .map_err(DbError::from)?;
            }
        }
        tx.commit().map_err(DbError::from)
    }
}
