use crate::database::blob_records::load_activated_registration_on;

use super::*;

impl Database {
    pub(crate) async fn stage_local_store_device_registration(
        &self,
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack_ref: StoreAckRef,
        initial_ack: ExactProtocolObject<StoreAck>,
    ) -> Result<(), DbError> {
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration.value,
            registration.object.clone(),
        );
        if registration.value.to_bytes() != registration.bytes
            || registration.object != *registration.prepared.reference()
            || initial_ack.value.to_bytes() != initial_ack.bytes
            || initial_ack.object != *initial_ack.prepared.reference()
            || initial_ack_ref.object != initial_ack.object
            || initial_ack_ref.ack_hash != initial_ack.value.ack_hash()
            || initial_ack_ref.registration != registration_ref
            || initial_ack_ref.sequence != initial_ack.value.sequence
            || initial_ack.value.registration != registration_ref
        {
            return Err(DbError::Message(
                "local registration staging graph contains mismatched exact objects".to_string(),
            ));
        }
        self.call(move |conn| {
            let root = required_store_root_authority_on(conn)?;
            if registration.value.store_root != root {
                return Err(DbError::Message(
                    "local registration staging graph belongs to another Store root".to_string(),
                ));
            }
            let prepared = serde_json::to_string(&registration.prepared).map_err(|error| {
                DbError::Message(format!("serialize prepared local registration: {error}"))
            })?;
            let ack_ref = serde_json::to_string(&initial_ack_ref).map_err(|error| {
                DbError::Message(format!("serialize local initial ack ref: {error}"))
            })?;
            let ack_prepared = serde_json::to_string(&initial_ack.prepared).map_err(|error| {
                DbError::Message(format!("serialize prepared local initial ack: {error}"))
            })?;
            let existing: Option<PreparedLocalDeviceRegistrationRow> = conn
                .query_row(
                    "SELECT device_id, registration_hash, registration_bytes, prepared_object, \
                            initial_ack_ref, initial_ack_bytes, initial_ack_prepared \
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
                        ))
                    },
                )
                .optional()
                .map_err(DbError::from)?;
            let expected = (
                registration_ref.device_id.to_string(),
                registration_ref.registration_hash.to_string(),
                registration.bytes.clone(),
                prepared.clone(),
                ack_ref.clone(),
                initial_ack.bytes.clone(),
                ack_prepared.clone(),
            );
            match existing {
                Some(existing) if existing == expected => Ok(()),
                Some(_) => Err(DbError::Message(
                    "local registration journal already owns different exact objects".to_string(),
                )),
                None => conn
                    .execute(
                        "INSERT INTO local_store_device_registration \
                         (singleton, device_id, registration_hash, registration_bytes, \
                          prepared_object, initial_ack_ref, initial_ack_bytes, \
                          initial_ack_prepared, state) \
                         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        rusqlite::params![
                            expected.0,
                            expected.1,
                            expected.2,
                            expected.3,
                            expected.4,
                            expected.5,
                            expected.6,
                            serde_json::to_string(&LocalDeviceRegistrationState::Prepared)
                                .map_err(|error| DbError::Message(format!(
                                    "serialize local registration state: {error}"
                                )))?,
                        ],
                    )
                    .map(|_| ())
                    .map_err(DbError::from),
            }
        })
        .await
    }

    pub(crate) async fn install_existing_local_founder_device(
        &self,
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack_ref: StoreAckRef,
        initial_ack: ExactProtocolObject<StoreAck>,
    ) -> Result<(), DbError> {
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration.value,
            registration.object.clone(),
        );
        if registration.value.to_bytes() != registration.bytes
            || registration.object != *registration.prepared.reference()
            || initial_ack.value.to_bytes() != initial_ack.bytes
            || initial_ack.object != *initial_ack.prepared.reference()
            || initial_ack_ref.object != initial_ack.object
            || initial_ack_ref.ack_hash != initial_ack.value.ack_hash()
            || initial_ack_ref.registration != registration_ref
            || initial_ack_ref.sequence != 1
            || initial_ack.value.sequence != 1
            || initial_ack.value.successor.predecessor.is_some()
            || initial_ack.value.registration != registration_ref
        {
            return Err(DbError::Message(
                "existing founder device graph contains mismatched exact objects".to_string(),
            ));
        }
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let root = required_store_root_authority_on(&tx)?;
            let crate::sync::store_commit::StoreDeviceRegistrationOrigin::Founder { .. } =
                &registration.value.origin
            else {
                return Err(DbError::Message(
                    "existing local founder device has a non-founder origin".to_string(),
                ));
            };
            if registration.value.store_root != root {
                return Err(DbError::Message(
                    "existing local founder device belongs to another Store root".to_string(),
                ));
            }
            let activated = load_activated_registration_on(&tx, &root, &registration_ref)?;
            if activated != registration.value {
                return Err(DbError::Message(
                    "existing local founder device differs from its installed activation"
                        .to_string(),
                ));
            }
            let authority = crate::sync::store_commit::StoreDeviceRegistrationActivation::Founder {
                root: root.clone(),
            };
            let expected = (
                registration_ref.device_id.to_string(),
                registration_ref.registration_hash.to_string(),
                registration.bytes.clone(),
                serde_json::to_string(&registration.prepared).map_err(|error| {
                    DbError::Message(format!("serialize existing founder registration: {error}"))
                })?,
                serde_json::to_string(&initial_ack_ref).map_err(|error| {
                    DbError::Message(format!("serialize existing founder ack ref: {error}"))
                })?,
                initial_ack.bytes.clone(),
                serde_json::to_string(&initial_ack.prepared).map_err(|error| {
                    DbError::Message(format!("serialize existing founder ack object: {error}"))
                })?,
                serde_json::to_string(&LocalDeviceRegistrationState::Activated { authority })
                    .map_err(|error| {
                        DbError::Message(format!(
                            "serialize existing founder registration state: {error}"
                        ))
                    })?,
            );
            tx.execute(
                "INSERT INTO local_store_device_registration
                 (singleton, device_id, registration_hash, registration_bytes,
                  prepared_object, initial_ack_ref, initial_ack_bytes,
                  initial_ack_prepared, state)
                 VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(singleton) DO NOTHING",
                rusqlite::params![
                    &expected.0,
                    &expected.1,
                    &expected.2,
                    &expected.3,
                    &expected.4,
                    &expected.5,
                    &expected.6,
                    &expected.7,
                ],
            )
            .map_err(DbError::from)?;
            let stored: LocalDeviceRegistrationJournalRow = tx
                .query_row(
                    "SELECT device_id, registration_hash, registration_bytes, prepared_object,
                            initial_ack_ref, initial_ack_bytes, initial_ack_prepared, state
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
                .map_err(DbError::from)?;
            if stored != expected {
                return Err(DbError::Message(
                    "existing local founder journal owns different exact objects".to_string(),
                ));
            }
            let ack_ref = serde_json::to_string(&initial_ack_ref).map_err(|error| {
                DbError::Message(format!("serialize existing founder ack ref: {error}"))
            })?;
            let successor =
                serde_json::to_string(&initial_ack.value.successor.next_slot).map_err(|error| {
                    DbError::Message(format!("serialize existing founder ack successor: {error}"))
                })?;
            tx.execute(
                "INSERT INTO published_store_acks (singleton, ack_ref, successor_slot)
                 VALUES (1, ?1, ?2) ON CONFLICT(singleton) DO NOTHING",
                (&ack_ref, &successor),
            )
            .map_err(DbError::from)?;
            let stored_ack: (String, String) = tx
                .query_row(
                    "SELECT ack_ref, successor_slot FROM published_store_acks WHERE singleton = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            if stored_ack != (ack_ref, successor) {
                return Err(DbError::Message(
                    "existing local founder acknowledgement differs from exact cloud state"
                        .to_string(),
                ));
            }
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO NOTHING",
                (LOCAL_DEVICE_ID_STATE_KEY, &expected.0),
            )
            .map_err(DbError::from)?;
            let stored_device_id: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if stored_device_id != expected.0 {
                return Err(DbError::Message(
                    "existing local founder device id conflicts with installed state".to_string(),
                ));
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn stage_owner_recovery_registration(
        &self,
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack_ref: StoreAckRef,
        initial_ack: ExactProtocolObject<StoreAck>,
        activation: crate::sync::store_commit::StoreDeviceRegistrationActivation,
    ) -> Result<bool, DbError> {
        let (
            crate::sync::store_commit::StoreDeviceRegistrationOrigin::Recovery {
                recovery_id: origin_recovery_id,
                recovery_slot,
                owner_grant,
                ..
            },
            crate::sync::store_commit::StoreDeviceRegistrationActivation::Recovery {
                recovery_id: activation_recovery_id,
                node,
            },
        ) = (&registration.value.origin, &activation)
        else {
            return Err(DbError::Message(
                "Owner recovery journal requires one Recovery registration authority".into(),
            ));
        };
        if origin_recovery_id != activation_recovery_id
            || node.object.slot() != recovery_slot
            || node.owner_grant != *owner_grant
        {
            return Err(DbError::Message(
                "Owner recovery registration differs from its activation authority".into(),
            ));
        }
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration.value,
            registration.object.clone(),
        );
        if registration.value.to_bytes() != registration.bytes
            || registration.object != *registration.prepared.reference()
            || initial_ack.value.to_bytes() != initial_ack.bytes
            || initial_ack.object != *initial_ack.prepared.reference()
            || initial_ack_ref.object != initial_ack.object
            || initial_ack_ref.ack_hash != initial_ack.value.ack_hash()
            || initial_ack_ref.registration != registration_ref
            || initial_ack_ref.sequence != 1
            || initial_ack.value.successor.predecessor.is_some()
            || initial_ack.value.registration != registration_ref
        {
            return Err(DbError::Message(
                "Owner recovery registration graph contains mismatched exact objects".into(),
            ));
        }
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let root = required_store_root_authority_on(&tx)?;
            if registration.value.store_root != root {
                return Err(DbError::Message(
                    "Owner recovery registration belongs to another Store root".into(),
                ));
            }
            let exact_registration_ref =
                serde_json::to_string(&registration_ref).map_err(|error| {
                    DbError::Message(format!("Owner recovery registration ref: {error}"))
                })?;
            let exact_activation = serde_json::to_string(&activation).map_err(|error| {
                DbError::Message(format!("Owner recovery activation authority: {error}"))
            })?;
            let activated = tx
                .query_row(
                    "SELECT registration_hash, registration_bytes, registration_object, \
                            activation_authority \
                     FROM store_device_registration_activations WHERE device_id = ?1",
                    [registration_ref.device_id.to_string()],
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
                .map_err(DbError::from)?;
            let activated = match activated {
                None => false,
                Some(existing)
                    if existing
                        == (
                            registration_ref.registration_hash.to_string(),
                            registration.bytes.clone(),
                            exact_registration_ref,
                            exact_activation,
                        ) =>
                {
                    true
                }
                Some(_) => {
                    return Err(DbError::Message(
                        "Owner recovery device already has different exact activation authority"
                            .into(),
                    ));
                }
            };
            tx.execute("DELETE FROM local_store_device_registration", [])
                .map_err(DbError::from)?;
            tx.execute("DELETE FROM published_store_acks", [])
                .map_err(DbError::from)?;
            tx.execute(
                "DELETE FROM protocol_state WHERE key = ?1",
                [LOCAL_DEVICE_ID_STATE_KEY],
            )
            .map_err(DbError::from)?;
            tx.execute(
                "INSERT INTO local_store_device_registration \
                 (singleton, device_id, registration_hash, registration_bytes, \
                  prepared_object, initial_ack_ref, initial_ack_bytes, \
                  initial_ack_prepared, state) \
                 VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    registration_ref.device_id.to_string(),
                    registration_ref.registration_hash.to_string(),
                    registration.bytes,
                    serde_json::to_string(&registration.prepared).map_err(|error| {
                        DbError::Message(format!("Owner recovery registration object: {error}"))
                    })?,
                    serde_json::to_string(&initial_ack_ref).map_err(|error| {
                        DbError::Message(format!("Owner recovery initial ack ref: {error}"))
                    })?,
                    initial_ack.bytes,
                    serde_json::to_string(&initial_ack.prepared).map_err(|error| {
                        DbError::Message(format!("Owner recovery initial ack object: {error}"))
                    })?,
                    serde_json::to_string(&if activated {
                        LocalDeviceRegistrationState::Activated {
                            authority: activation,
                        }
                    } else {
                        LocalDeviceRegistrationState::Prepared
                    })
                    .map_err(|error| DbError::Message(format!(
                        "Owner recovery journal state: {error}"
                    )))?,
                ],
            )
            .map_err(DbError::from)?;
            if activated {
                tx.execute(
                    "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (
                        LOCAL_DEVICE_ID_STATE_KEY,
                        registration_ref.device_id.to_string(),
                    ),
                )
                .map_err(DbError::from)?;
                tx.execute(
                    "INSERT INTO published_store_acks (singleton, ack_ref, successor_slot) \
                     VALUES (1, ?1, ?2)",
                    rusqlite::params![
                        serde_json::to_string(&initial_ack_ref).map_err(|error| {
                            DbError::Message(format!(
                                "Owner recovery published initial ack ref: {error}"
                            ))
                        })?,
                        serde_json::to_string(&initial_ack.value.successor.next_slot).map_err(
                            |error| DbError::Message(format!(
                                "Owner recovery initial ack successor: {error}"
                            ))
                        )?,
                    ],
                )
                .map_err(DbError::from)?;
            }
            tx.commit().map_err(DbError::from)?;
            Ok(activated)
        })
        .await
    }

    pub(crate) async fn oldest_unpublished_store_device_registration(
        &self,
    ) -> Result<Option<DurableDeviceRegistration>, DbError> {
        self.read_local_store_device_registration(
            "SELECT device_id, registration_hash, registration_bytes, prepared_object, \
                    initial_ack_ref, initial_ack_bytes, initial_ack_prepared, state \
             FROM local_store_device_registration WHERE singleton = 1 AND state = '\"prepared\"'",
        )
        .await
    }

    pub(crate) async fn read_local_store_device_registration(
        &self,
        sql: &'static str,
    ) -> Result<Option<DurableDeviceRegistration>, DbError> {
        self.call(move |conn| {
            conn.query_row(sql, [], |row| {
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
            })
            .optional()
            .map_err(DbError::from)?
            .map(
                |(device_id, hash, bytes, prepared, ack_ref, ack_bytes, ack_prepared, state)| {
                    let device_id = device_id.parse().map_err(|error| {
                        DbError::Message(format!("local Store device id: {error}"))
                    })?;
                    let prepared: PreparedExactObject =
                        serde_json::from_str(&prepared).map_err(|error| {
                            DbError::Message(format!(
                                "local Store device registration prepared object: {error}"
                            ))
                        })?;
                    let registration = StoreDeviceRegistration::parse_at(
                        &bytes,
                        &required_store_root_authority_on(conn)?,
                        device_id,
                    )
                    .map_err(|error| {
                        DbError::Message(format!("local Store device registration: {error}"))
                    })?;
                    let initial_ack_ref: StoreAckRef =
                        serde_json::from_str(&ack_ref).map_err(|error| {
                            DbError::Message(format!(
                                "local Store initial acknowledgement ref: {error}"
                            ))
                        })?;
                    let initial_ack_value = StoreAck::parse_at(
                        &ack_bytes,
                        &registration.store_root,
                        &initial_ack_ref,
                        &registration,
                    )
                    .map_err(|error| {
                        DbError::Message(format!("local Store initial acknowledgement: {error}"))
                    })?;
                    let initial_ack_prepared: PreparedExactObject =
                        serde_json::from_str(&ack_prepared).map_err(|error| {
                            DbError::Message(format!("local Store initial ack object: {error}"))
                        })?;
                    Ok(DurableDeviceRegistration {
                        device_id,
                        registration_hash: hash.parse().map_err(|error| {
                            DbError::Message(format!(
                                "local Store device registration hash: {error}"
                            ))
                        })?,
                        registration_bytes: bytes,
                        prepared,
                        initial_ack_ref,
                        initial_ack: ExactProtocolObject {
                            value: initial_ack_value,
                            bytes: ack_bytes,
                            object: initial_ack_prepared.reference().clone(),
                            prepared: initial_ack_prepared,
                        },
                        state: serde_json::from_str(&state).map_err(|error| {
                            DbError::Message(format!(
                                "local Store registration journal state: {error}"
                            ))
                        })?,
                    })
                },
            )
            .transpose()
        })
        .await
    }

    pub(crate) async fn mark_local_store_device_registration_created(
        &self,
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack: StoreAckRef,
        initial_ack_object: ExactProtocolObject<StoreAck>,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let registration_ref = StoreDeviceRegistrationRef::from_registration(
                &registration.value,
                registration.object.clone(),
            );
            if registration_ref.object != *registration.prepared.reference()
                || initial_ack.object != *initial_ack_object.prepared.reference()
            {
                return Err(DbError::Message(
                    "created registration refs differ from their prepared objects".to_string(),
                ));
            }
            let durable: (Vec<u8>, String, String, Vec<u8>, String, String) = tx
                .query_row(
                    "SELECT registration_bytes, prepared_object, initial_ack_ref, \
                            initial_ack_bytes, initial_ack_prepared, state \
                     FROM local_store_device_registration \
                     WHERE singleton = 1 AND device_id = ?1 AND registration_hash = ?2",
                    (
                        registration_ref.device_id.to_string(),
                        registration_ref.registration_hash.to_string(),
                    ),
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .map_err(DbError::from)?;
            let stored_registration_prepared: PreparedExactObject =
                serde_json::from_str(&durable.1).map_err(|error| {
                    DbError::Message(format!("stored registration object: {error}"))
                })?;
            let stored_ack_ref: StoreAckRef = serde_json::from_str(&durable.2)
                .map_err(|error| DbError::Message(format!("stored initial ack ref: {error}")))?;
            let stored_ack_prepared: PreparedExactObject = serde_json::from_str(&durable.4)
                .map_err(|error| DbError::Message(format!("stored initial ack object: {error}")))?;
            if registration_ref.object != *stored_registration_prepared.reference()
                || registration.prepared.reference() != stored_registration_prepared.reference()
                || registration.prepared.stored_bytes()
                    != stored_registration_prepared.stored_bytes()
                || registration.bytes != durable.0
                || initial_ack != stored_ack_ref
                || initial_ack_object.prepared.reference() != stored_ack_prepared.reference()
                || initial_ack_object.prepared.stored_bytes() != stored_ack_prepared.stored_bytes()
                || initial_ack_object.bytes != durable.3
            {
                return Err(DbError::Message(
                    "created registration differs from its complete durable exact objects"
                        .to_string(),
                ));
            }
            let prepared = serde_json::to_string(&LocalDeviceRegistrationState::Prepared).map_err(
                |error| DbError::Message(format!("serialize prepared journal: {error}")),
            )?;
            let created = serde_json::to_string(&LocalDeviceRegistrationState::Created)
                .map_err(|error| DbError::Message(format!("serialize created journal: {error}")))?;
            let updated = tx
                .execute(
                    "UPDATE local_store_device_registration SET state = ?1 \
                     WHERE singleton = 1 AND device_id = ?2 AND registration_hash = ?3 \
                       AND initial_ack_ref = ?4 AND state = ?5",
                    rusqlite::params![
                        created,
                        registration_ref.device_id.to_string(),
                        registration_ref.registration_hash.to_string(),
                        serde_json::to_string(&initial_ack).map_err(|error| {
                            DbError::Message(format!("serialize initial ack ref: {error}"))
                        })?,
                        prepared,
                    ],
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                let already: Option<String> = tx
                    .query_row(
                        "SELECT state FROM local_store_device_registration \
                         WHERE singleton = 1 AND device_id = ?1 AND registration_hash = ?2",
                        (
                            registration_ref.device_id.to_string(),
                            registration_ref.registration_hash.to_string(),
                        ),
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(DbError::from)?;
                if already.as_deref() != Some(created.as_str()) {
                    return Err(DbError::Message(
                        "local registration journal is absent or differs from the created object"
                            .to_string(),
                    ));
                }
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }
}
