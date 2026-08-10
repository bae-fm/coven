use crate::*;
use coven_protocol::store_commit::{StoreAck, StoreDeviceRegistration, StoreDeviceRegistrationRef};
use rusqlite::OptionalExtension;

use super::*;

/// One local device registration together with the first acknowledgement that
/// anchors its stream, checked against each other before any of it reaches a
/// column. Every way a registration enters the journal — a joining device's
/// staged registration, an existing founder's installation, an Owner recovery —
/// builds one of these first, so the journal cannot hold a graph whose
/// references disagree with the objects beside them.
pub(crate) struct LocalRegistrationRecord {
    registration: ExactProtocolObject<StoreDeviceRegistration>,
    initial_ack_ref: StoreAckRef,
    initial_ack: ExactProtocolObject<StoreAck>,
    reference: StoreDeviceRegistrationRef,
}

impl LocalRegistrationRecord {
    /// Each reference must name the exact object beside it, and both the
    /// registration and its acknowledgement must serialize back to the bytes
    /// they carry. `subject` names the graph in any refusal.
    pub(crate) fn checked(
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack_ref: StoreAckRef,
        initial_ack: ExactProtocolObject<StoreAck>,
        subject: &str,
    ) -> Result<Self, DbError> {
        let reference = StoreDeviceRegistrationRef::from_registration(
            &registration.value,
            registration.prepared.reference().clone(),
        );
        if registration.value.to_bytes() != registration.bytes
            || initial_ack.value.to_bytes() != initial_ack.bytes
            || &initial_ack_ref.object != initial_ack.prepared.reference()
            || initial_ack_ref.ack_hash != initial_ack.value.ack_hash()
            || initial_ack_ref.registration != reference
            || initial_ack_ref.sequence != initial_ack.value.sequence
            || initial_ack.value.registration != reference
        {
            return Err(DbError::Message(format!(
                "{subject} contains mismatched exact objects"
            )));
        }
        Ok(Self {
            registration,
            initial_ack_ref,
            initial_ack,
            reference,
        })
    }

    /// The same graph, for a device whose acknowledgement stream begins here:
    /// the acknowledgement is the first one and has no predecessor.
    pub(crate) fn checked_at_stream_start(
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack_ref: StoreAckRef,
        initial_ack: ExactProtocolObject<StoreAck>,
        subject: &str,
    ) -> Result<Self, DbError> {
        let record = Self::checked(registration, initial_ack_ref, initial_ack, subject)?;
        if record.initial_ack_ref.sequence != 1
            || record.initial_ack.value.successor.predecessor.is_some()
        {
            return Err(DbError::Message(format!(
                "{subject} does not start its acknowledgement stream"
            )));
        }
        Ok(record)
    }

    pub(crate) fn reference(&self) -> &StoreDeviceRegistrationRef {
        &self.reference
    }

    pub(crate) fn registration(&self) -> &StoreDeviceRegistration {
        &self.registration.value
    }

    pub(crate) fn device_id(&self) -> String {
        self.reference.device_id.to_string()
    }

    /// The Store root this database is installed under, refusing a graph signed
    /// against a different one.
    pub(crate) fn require_installed_store_root(
        &self,
        root: &coven_protocol::store_commit::StoreRootRef,
        subject: &str,
    ) -> Result<(), DbError> {
        if &self.registration.value.store_root != root {
            return Err(DbError::Message(format!(
                "{subject} belongs to another Store root"
            )));
        }
        Ok(())
    }

    /// The journal's seven object columns, in table order.
    pub(crate) fn columns(
        &self,
        subject: &str,
    ) -> Result<PreparedLocalDeviceRegistrationRow, DbError> {
        Ok((
            self.device_id(),
            self.reference.registration_hash.to_string(),
            self.registration.bytes.clone(),
            encode(&self.registration.prepared, subject, "registration object")?,
            encode(&self.initial_ack_ref, subject, "acknowledgement ref")?,
            self.initial_ack.bytes.clone(),
            encode(
                &self.initial_ack.prepared,
                subject,
                "acknowledgement object",
            )?,
        ))
    }

    /// The published-acknowledgement columns that name this device's first
    /// acknowledgement and the slot its successor will occupy.
    pub(crate) fn published_ack_columns(&self, subject: &str) -> Result<(String, String), DbError> {
        Ok((
            encode(&self.initial_ack_ref, subject, "acknowledgement ref")?,
            encode(
                &self.initial_ack.value.successor.next_slot,
                subject,
                "acknowledgement successor",
            )?,
        ))
    }
}

fn encode<T: serde::Serialize>(value: &T, subject: &str, what: &str) -> Result<String, DbError> {
    serde_json::to_string(value)
        .map_err(|error| DbError::context(format!("serialize {subject} {what}"), error))
}

impl StoreSession<'_> {
    fn stage_local_store_device_registration(
        &mut self,
        record: LocalRegistrationRecord,
        subject: &str,
    ) -> Result<(), DbError> {
        let records = self.records;
        let root = self
            .verified_store_authority
            .required_root_authority_on(records)?;
        record.require_installed_store_root(&root, subject)?;
        let expected = record.columns(subject)?;
        let existing: Option<PreparedLocalDeviceRegistrationRow> = records
            .conn
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
        match existing {
            Some(existing) if existing == expected => Ok(()),
            Some(_) => Err(DbError::Message(
                "local registration journal already owns different exact objects".to_string(),
            )),
            None => records
                .conn
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
                        serde_json::to_string(&LocalDeviceRegistrationState::Prepared).map_err(
                            |error| DbError::context("serialize local registration state", error)
                        )?,
                    ],
                )
                .map(|_| ())
                .map_err(DbError::from),
        }
    }

    fn install_existing_local_founder_device(
        &mut self,
        record: LocalRegistrationRecord,
        subject: &str,
    ) -> Result<(), DbError> {
        let records = self.records;
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let transaction_records = crate::payload_spool::StoreRecords::new(&tx, records.store_dir);
        let root = self
            .verified_store_authority
            .required_root_authority_on(transaction_records)?;
        record.require_installed_store_root(&root, subject)?;
        let coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Founder { .. } =
            &record.registration().origin
        else {
            return Err(DbError::Message(
                "existing local founder device has a non-founder origin".to_string(),
            ));
        };
        let activated = self.verified_store_authority.activated_registration_on(
            transaction_records,
            &root,
            record.reference(),
        )?;
        if activated != *record.registration() {
            return Err(DbError::Message(
                "existing local founder device differs from its installed activation".to_string(),
            ));
        }
        let authority = coven_protocol::store_commit::StoreDeviceRegistrationActivation::Founder {
            root: root.clone(),
        };
        let objects = record.columns(subject)?;
        let expected = (
            objects.0,
            objects.1,
            objects.2,
            objects.3,
            objects.4,
            objects.5,
            objects.6,
            encode(
                &LocalDeviceRegistrationState::Activated { authority },
                subject,
                "registration state",
            )?,
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
        let published_ack = record.published_ack_columns(subject)?;
        tx.execute(
            "INSERT INTO published_store_acks (singleton, ack_ref, successor_slot)
                 VALUES (1, ?1, ?2) ON CONFLICT(singleton) DO NOTHING",
            (&published_ack.0, &published_ack.1),
        )
        .map_err(DbError::from)?;
        let stored_ack: (String, String) = tx
            .query_row(
                "SELECT ack_ref, successor_slot FROM published_store_acks WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        if stored_ack != published_ack {
            return Err(DbError::Message(
                "existing local founder acknowledgement differs from exact cloud state".to_string(),
            ));
        }
        tx.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO NOTHING",
            (LOCAL_DEVICE_ID_STATE_KEY, &expected.0),
        )
        .map_err(DbError::from)?;
        let stored_device_id = crate::required_protocol_state_on(&tx, LOCAL_DEVICE_ID_STATE_KEY)?;
        if stored_device_id != expected.0 {
            return Err(DbError::Message(
                "existing local founder device id conflicts with installed state".to_string(),
            ));
        }
        tx.commit().map_err(DbError::from)
    }

    fn stage_owner_recovery_registration(
        &mut self,
        record: LocalRegistrationRecord,
        activation: coven_protocol::store_commit::StoreDeviceRegistrationActivation,
        subject: &str,
    ) -> Result<bool, DbError> {
        let records = self.records;
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let transaction_records = crate::payload_spool::StoreRecords::new(&tx, records.store_dir);
        let root = self
            .verified_store_authority
            .required_root_authority_on(transaction_records)?;
        record.require_installed_store_root(&root, subject)?;
        let objects = record.columns(subject)?;
        let exact_registration_ref = encode(record.reference(), subject, "registration ref")?;
        let exact_activation = encode(&activation, subject, "activation authority")?;
        let installed = tx
            .query_row(
                "SELECT registration_hash, registration_bytes, registration_object, \
                    activation_authority \
                 FROM store_device_registration_activations WHERE device_id = ?1",
                [record.device_id()],
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
        let activated = match installed {
            None => false,
            Some(existing)
                if existing
                    == (
                        objects.1.clone(),
                        objects.2.clone(),
                        exact_registration_ref,
                        exact_activation,
                    ) =>
            {
                true
            }
            Some(_) => {
                return Err(DbError::Message(
                    "Owner recovery device already has different exact activation authority".into(),
                ));
            }
        };
        tx.execute("DELETE FROM local_store_device_registration", [])
            .map_err(DbError::from)?;
        tx.execute("DELETE FROM published_store_acks", [])
            .map_err(DbError::from)?;
        crate::delete_protocol_state_on(&tx, LOCAL_DEVICE_ID_STATE_KEY)?;
        let state = encode(
            &if activated {
                LocalDeviceRegistrationState::Activated {
                    authority: activation,
                }
            } else {
                LocalDeviceRegistrationState::Prepared
            },
            subject,
            "journal state",
        )?;
        tx.execute(
            "INSERT INTO local_store_device_registration \
                 (singleton, device_id, registration_hash, registration_bytes, \
                  prepared_object, initial_ack_ref, initial_ack_bytes, \
                  initial_ack_prepared, state) \
                 VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                objects.0, objects.1, objects.2, objects.3, objects.4, objects.5, objects.6, state,
            ],
        )
        .map_err(DbError::from)?;
        if activated {
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
                (LOCAL_DEVICE_ID_STATE_KEY, &objects.0),
            )
            .map_err(DbError::from)?;
            let published_ack = record.published_ack_columns(subject)?;
            tx.execute(
                "INSERT INTO published_store_acks (singleton, ack_ref, successor_slot) \
                     VALUES (1, ?1, ?2)",
                (&published_ack.0, &published_ack.1),
            )
            .map_err(DbError::from)?;
        }
        tx.commit().map_err(DbError::from)?;
        Ok(activated)
    }

    fn read_local_store_device_registration(
        &mut self,
        sql: &'static str,
    ) -> Result<Option<DurableDeviceRegistration>, DbError> {
        let records = self.records;
        records
            .conn
            .query_row(sql, [], |row| {
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
                    let device_id = device_id
                        .parse()
                        .map_err(|error| DbError::context("local Store device id", error))?;
                    let prepared: PreparedExactObject =
                        serde_json::from_str(&prepared).map_err(|error| {
                            DbError::context(
                                "local Store device registration prepared object",
                                error,
                            )
                        })?;
                    let registration = StoreDeviceRegistration::parse_at(
                        &bytes,
                        &self
                            .verified_store_authority
                            .required_root_authority_on(records)?,
                        device_id,
                    )
                    .map_err(|error| DbError::context("local Store device registration", error))?;
                    let initial_ack_ref: StoreAckRef =
                        serde_json::from_str(&ack_ref).map_err(|error| {
                            DbError::context("local Store initial acknowledgement ref", error)
                        })?;
                    let initial_ack_value = StoreAck::parse_at(
                        &ack_bytes,
                        &registration.store_root,
                        &initial_ack_ref,
                        &registration,
                    )
                    .map_err(|error| {
                        DbError::context("local Store initial acknowledgement", error)
                    })?;
                    let initial_ack_prepared: PreparedExactObject =
                        serde_json::from_str(&ack_prepared).map_err(|error| {
                            DbError::context("local Store initial ack object", error)
                        })?;
                    Ok(DurableDeviceRegistration {
                        device_id,
                        registration_hash: hash.parse().map_err(|error| {
                            DbError::context("local Store device registration hash", error)
                        })?,
                        registration_bytes: bytes,
                        prepared,
                        initial_ack_ref,
                        initial_ack: ExactProtocolObject {
                            value: initial_ack_value,
                            bytes: ack_bytes,
                            prepared: initial_ack_prepared,
                        },
                        state: serde_json::from_str(&state).map_err(|error| {
                            DbError::context("local Store registration journal state", error)
                        })?,
                    })
                },
            )
            .transpose()
    }

    fn mark_local_store_device_registration_created(
        &mut self,
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack: StoreAckRef,
        initial_ack_object: ExactProtocolObject<StoreAck>,
    ) -> Result<(), DbError> {
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration.value,
            registration.prepared.reference().clone(),
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
        let stored_registration_prepared: PreparedExactObject = serde_json::from_str(&durable.1)
            .map_err(|error| DbError::context("stored registration object", error))?;
        let stored_ack_ref: StoreAckRef = serde_json::from_str(&durable.2)
            .map_err(|error| DbError::context("stored initial ack ref", error))?;
        let stored_ack_prepared: PreparedExactObject = serde_json::from_str(&durable.4)
            .map_err(|error| DbError::context("stored initial ack object", error))?;
        if registration_ref.object != *stored_registration_prepared.reference()
            || registration.prepared.reference() != stored_registration_prepared.reference()
            || registration.prepared.stored_bytes() != stored_registration_prepared.stored_bytes()
            || registration.bytes != durable.0
            || initial_ack != stored_ack_ref
            || initial_ack_object.prepared.reference() != stored_ack_prepared.reference()
            || initial_ack_object.prepared.stored_bytes() != stored_ack_prepared.stored_bytes()
            || initial_ack_object.bytes != durable.3
        {
            return Err(DbError::Message(
                "created registration differs from its complete durable exact objects".to_string(),
            ));
        }
        let prepared = serde_json::to_string(&LocalDeviceRegistrationState::Prepared)
            .map_err(|error| DbError::context("serialize prepared journal", error))?;
        let created = serde_json::to_string(&LocalDeviceRegistrationState::Created)
            .map_err(|error| DbError::context("serialize created journal", error))?;
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
                        DbError::context("serialize initial ack ref", error)
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
    }
}

impl StoreDatabase {
    pub async fn stage_local_store_device_registration(
        &self,
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack_ref: StoreAckRef,
        initial_ack: ExactProtocolObject<StoreAck>,
    ) -> Result<(), DbError> {
        const SUBJECT: &str = "local registration staging graph";
        let record =
            LocalRegistrationRecord::checked(registration, initial_ack_ref, initial_ack, SUBJECT)?;
        self.connection
            .call_store(move |session| {
                session.stage_local_store_device_registration(record, SUBJECT)
            })
            .await
    }

    pub async fn install_existing_local_founder_device(
        &self,
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack_ref: StoreAckRef,
        initial_ack: ExactProtocolObject<StoreAck>,
    ) -> Result<(), DbError> {
        const SUBJECT: &str = "existing founder device graph";
        let record = LocalRegistrationRecord::checked_at_stream_start(
            registration,
            initial_ack_ref,
            initial_ack,
            SUBJECT,
        )?;
        self.connection
            .call_store(move |session| {
                session.install_existing_local_founder_device(record, SUBJECT)
            })
            .await
    }

    pub async fn stage_owner_recovery_registration(
        &self,
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack_ref: StoreAckRef,
        initial_ack: ExactProtocolObject<StoreAck>,
        activation: coven_protocol::store_commit::StoreDeviceRegistrationActivation,
    ) -> Result<bool, DbError> {
        let (
            coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Recovery {
                recovery_id: origin_recovery_id,
                recovery_slot,
                owner_grant,
                ..
            },
            coven_protocol::store_commit::StoreDeviceRegistrationActivation::Recovery {
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
        const SUBJECT: &str = "Owner recovery registration graph";
        let record = LocalRegistrationRecord::checked_at_stream_start(
            registration,
            initial_ack_ref,
            initial_ack,
            SUBJECT,
        )?;
        self.connection
            .call_store(move |session| {
                session.stage_owner_recovery_registration(record, activation, SUBJECT)
            })
            .await
    }

    pub async fn oldest_unpublished_store_device_registration(
        &self,
    ) -> Result<Option<DurableDeviceRegistration>, DbError> {
        self.read_local_store_device_registration(
            "SELECT device_id, registration_hash, registration_bytes, prepared_object, \
                    initial_ack_ref, initial_ack_bytes, initial_ack_prepared, state \
             FROM local_store_device_registration WHERE singleton = 1 AND state = '\"prepared\"'",
        )
        .await
    }

    pub async fn read_local_store_device_registration(
        &self,
        sql: &'static str,
    ) -> Result<Option<DurableDeviceRegistration>, DbError> {
        self.connection
            .call_store(move |session| session.read_local_store_device_registration(sql))
            .await
    }

    pub async fn mark_local_store_device_registration_created(
        &self,
        registration: ExactProtocolObject<StoreDeviceRegistration>,
        initial_ack: StoreAckRef,
        initial_ack_object: ExactProtocolObject<StoreAck>,
    ) -> Result<(), DbError> {
        self.connection
            .call_store(move |session| {
                session.mark_local_store_device_registration_created(
                    registration,
                    initial_ack,
                    initial_ack_object,
                )
            })
            .await
    }
}
