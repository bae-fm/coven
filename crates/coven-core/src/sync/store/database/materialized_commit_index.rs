use crate::database::*;
use crate::sync::storage::PreparedExactObject;
use crate::sync::store_commit::{
    CommitFrontier, ResolvedStoreDeviceState, StoreAck, StoreAckRef, StoreBatchCommit,
    StoreBatchCommitRef, StoreDeviceProposalAck, StoreDeviceRegistration,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreHistoryCut, VerifiedStoreBatchCommit,
};
use rusqlite::{Connection, OptionalExtension};
use std::collections::BTreeMap;

use super::store_device_state::{
    load_declared_store_device_state_on, load_store_device_exclusion_freezes_on,
    store_device_state_for_history_cut_on,
};
use super::*;

impl StoreDatabase {
    pub(crate) async fn materialized_frontier(
        &self,
    ) -> Result<BTreeMap<String, StoreBatchCommitRef>, DbError> {
        self.database
            .call(|conn| Self::materialized_frontier_on(conn, None))
            .await
    }

    pub(crate) async fn retained_merge_replay_inputs(
        &self,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let cache = self.retained_merge_materialization_cache();
        self.database
            .call(move |conn| {
                let mut cache = cache.lock().map_err(|_| {
                    DbError::Message(
                        "retained Merge materialization cache lock is poisoned".to_string(),
                    )
                })?;
                Self::cached_retained_merge_replay_inputs_on(conn, &mut cache)
            })
            .await
    }

    pub(crate) async fn retained_merge_materialization_refs(
        &self,
    ) -> Result<Vec<StoreBatchCommitRef>, DbError> {
        self.database
            .call(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT device_id, seq, commit_ref
                         FROM retained_merge_materializations
                         ORDER BY device_id, seq",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .map_err(DbError::from)?;
                rows.map(|row| {
                    let (stream_id, sequence, encoded_ref) = row.map_err(DbError::from)?;
                    let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
                    Self::parse_stored_commit_ref(&stream_id, sequence, &encoded_ref)
                })
                .collect()
            })
            .await
    }

    pub(crate) async fn retained_merge_replay_inputs_with_verified_commits(
        &self,
        verified: BTreeMap<StoreBatchCommitRef, VerifiedStoreBatchCommit>,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let cache = self.retained_merge_materialization_cache();
        self.database
            .call(move |conn| {
                let mut cache = cache.lock().map_err(|_| {
                    DbError::Message(
                        "retained Merge materialization cache lock is poisoned".to_string(),
                    )
                })?;
                Self::cached_retained_merge_replay_inputs_with_verified_commits_on(
                    conn, &mut cache, &verified,
                )
            })
            .await
    }

    pub(crate) async fn retained_merge_materialization(
        &self,
        reference: StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let cache = self.retained_merge_materialization_cache();
        self.database
            .call(move |conn| {
                let mut cache = cache.lock().map_err(|_| {
                    DbError::Message(
                        "retained Merge materialization cache lock is poisoned".to_string(),
                    )
                })?;
                Self::cached_retained_merge_replay_inputs_on(conn, &mut cache)?
                    .into_iter()
                    .find(|materialization| materialization.commit_ref() == &reference)
                    .ok_or_else(|| {
                        DbError::Message(
                            "retained Merge materialization is absent at its exact coordinate"
                                .to_string(),
                        )
                    })
            })
            .await
    }

    pub(crate) async fn retained_merge_history_frontier(
        &self,
        references: Vec<StoreBatchCommitRef>,
    ) -> Result<Vec<crate::sync::store_commit::OpenedRetainedMergeHistorySummary>, DbError> {
        let cache = self.retained_merge_materialization_cache();
        self.database
            .call(move |conn| {
                let retained = {
                    let mut cache = cache.lock().map_err(|_| {
                        DbError::Message(
                            "retained Merge materialization cache lock is poisoned".to_string(),
                        )
                    })?;
                    Self::cached_retained_merge_replay_inputs_on(conn, &mut cache)?
                };
                references
                    .iter()
                    .map(|reference| {
                        match retained
                            .iter()
                            .find(|materialization| materialization.commit_ref() == reference)
                        {
                            Some(materialization) => {
                                Self::open_retained_merge_history_checkpoint_on(
                                    conn,
                                    reference,
                                    materialization,
                                )
                            }
                            None => {
                                Self::load_retained_merge_history_checkpoint_on(conn, reference)
                            }
                        }
                    })
                    .collect()
            })
            .await
    }

    pub(crate) async fn exact_materialized_ref(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        let stream_id = stream_id.to_string();
        self.database
            .call(move |conn| Self::materialized_commit_ref_on(conn, &stream_id, sequence))
            .await
    }

    pub(crate) async fn snapshot_coverage_frontier(&self) -> Result<CommitFrontier, DbError> {
        self.database
            .call(move |conn| {
                let mut stmt = conn
                    .prepare("SELECT device_id, seq, commit_ref FROM snapshot_coverage")
                    .map_err(DbError::from)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .map_err(DbError::from)?;
                let mut frontier = BTreeMap::new();
                for row in rows {
                    let (device_id, seq, reference) = row.map_err(DbError::from)?;
                    let seq = Database::sequence_from_sqlite(&device_id, seq)?;
                    let reference = Self::parse_stored_commit_ref(&device_id, seq, &reference)?;
                    frontier.insert(device_id.clone(), reference);
                }
                CommitFrontier::from_refs(frontier).map_err(|error| {
                    DbError::Message(format!("snapshot coverage frontier: {error}"))
                })
            })
            .await
    }

    pub(crate) fn materialized_frontier_on(
        conn: &Connection,
        exclude_device: Option<&str>,
    ) -> Result<BTreeMap<String, StoreBatchCommitRef>, DbError> {
        let mut frontier = BTreeMap::new();
        let mut stmt = conn
            .prepare(
                "SELECT m.device_id, m.seq, m.commit_ref,
                        m.retained_commit_ref, m.retained_input_hash \
                 FROM materialized_commits m \
                 JOIN (SELECT device_id, MAX(seq) AS seq FROM materialized_commits \
                       GROUP BY device_id) latest \
                   ON latest.device_id = m.device_id AND latest.seq = m.seq",
            )
            .map_err(DbError::from)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(stmt);
        for row in rows {
            let (device_id, seq, reference, retained_commit_ref, retained_input_hash) = row;
            if exclude_device == Some(device_id.as_str()) {
                continue;
            }
            let seq = Database::sequence_from_sqlite(&device_id, seq)?;
            frontier.insert(
                device_id.clone(),
                Self::parse_materialized_commit_row_on(
                    &device_id,
                    seq,
                    &reference,
                    retained_commit_ref.as_deref(),
                    retained_input_hash.as_deref(),
                )?,
            );
        }

        let mut coverage = conn
            .prepare("SELECT device_id, seq, commit_ref FROM snapshot_coverage")
            .map_err(DbError::from)?;
        let rows = coverage
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(DbError::from)?;
        for row in rows {
            let (device_id, seq, reference) = row.map_err(DbError::from)?;
            if exclude_device == Some(device_id.as_str()) {
                continue;
            }
            let seq = Database::sequence_from_sqlite(&device_id, seq)?;
            let reference = Self::parse_stored_commit_ref(&device_id, seq, &reference)?;
            if frontier
                .get(&device_id)
                .is_none_or(|current| current.coord.sequence() < reference.coord.sequence())
            {
                frontier.insert(device_id, reference);
            }
        }
        Ok(frontier)
    }

    pub(crate) fn parse_stored_commit_ref(
        stream_id: &str,
        sequence: u64,
        encoded: &str,
    ) -> Result<StoreBatchCommitRef, DbError> {
        let reference: StoreBatchCommitRef = serde_json::from_str(encoded)
            .map_err(|error| DbError::Message(format!("stored exact Store commit ref: {error}")))?;
        let coordinate_matches = reference.coord.stream_id.to_string() == stream_id
            && reference.coord.sequence == sequence;
        if !coordinate_matches {
            return Err(DbError::Message(format!(
                "stored exact Store commit ref differs from {stream_id}/{sequence}"
            )));
        }
        Ok(reference)
    }

    pub(super) fn parse_materialized_commit_row_on(
        stream_id: &str,
        sequence: u64,
        encoded: &str,
        retained_commit_ref: Option<&str>,
        retained_input_hash: Option<&str>,
    ) -> Result<StoreBatchCommitRef, DbError> {
        let reference = Self::parse_stored_commit_ref(stream_id, sequence, encoded)?;
        if retained_commit_ref != Some(encoded) {
            return Err(DbError::Message(format!(
                "materialized coordinate {stream_id}/{sequence} does not bind its exact retained commit"
            )));
        }
        let input_hash = retained_input_hash.ok_or_else(|| {
            DbError::Message(format!(
                "materialized coordinate {stream_id}/{sequence} has no retained input hash"
            ))
        })?;
        input_hash.parse::<crate::sync::store_commit::ObjectHash>().map_err(
            |error| {
                DbError::Message(format!(
                    "materialized coordinate {stream_id}/{sequence} retained input hash is invalid: {error}"
                ))
            },
        )?;
        Ok(reference)
    }

    pub(crate) fn materialized_commit_ref_on(
        conn: &Connection,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        let seq = Database::sequence_to_sqlite(stream_id, sequence)?;
        conn.query_row(
            "SELECT commit_ref, retained_commit_ref, retained_input_hash
             FROM materialized_commits WHERE device_id = ?1 AND seq = ?2",
            (stream_id, seq),
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(DbError::from)?
        .map(|(encoded, retained_commit_ref, retained_input_hash)| {
            Self::parse_materialized_commit_row_on(
                stream_id,
                sequence,
                &encoded,
                retained_commit_ref.as_deref(),
                retained_input_hash.as_deref(),
            )
        })
        .transpose()
    }

    pub(crate) fn latest_position_for_device_on(
        conn: &Connection,
        device_id: &str,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        let materialized = conn
            .query_row(
                "SELECT seq, commit_ref, retained_commit_ref, retained_input_hash
                 FROM materialized_commits
                 WHERE device_id = ?1 ORDER BY seq DESC LIMIT 1",
                [device_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?;
        let coverage = conn
            .query_row(
                "SELECT seq, commit_ref FROM snapshot_coverage WHERE device_id = ?1",
                [device_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        let mut references = Vec::new();
        if let Some((seq, reference, retained_commit_ref, retained_input_hash)) = materialized {
            let seq = Database::sequence_from_sqlite(device_id, seq)?;
            references.push(Self::parse_materialized_commit_row_on(
                device_id,
                seq,
                &reference,
                retained_commit_ref.as_deref(),
                retained_input_hash.as_deref(),
            )?);
        }
        if let Some((seq, reference)) = coverage {
            let seq = Database::sequence_from_sqlite(device_id, seq)?;
            references.push(Self::parse_stored_commit_ref(device_id, seq, &reference)?);
        }
        if references.len() == 2
            && references[0].coord.sequence() == references[1].coord.sequence()
            && references[0] != references[1]
        {
            return Err(DbError::Message(format!(
                "materialized ledger and snapshot coverage fork {device_id:?} at sequence {}",
                references[0].coord.sequence()
            )));
        }
        Ok(references
            .into_iter()
            .max_by_key(|reference| reference.coord.sequence()))
    }

    pub(crate) fn record_activated_store_device_registrations_on(
        conn: &Connection,
        commit: &StoreBatchCommit,
        registrations: &[(
            StoreDeviceRegistration,
            crate::sync::store_commit::StoreDeviceRegistrationActivation,
        )],
    ) -> Result<(), DbError> {
        if registrations.len() != commit.device_registrations().len() {
            return Err(DbError::Message(
                "Store device registration activation count differs from the signed commit"
                    .to_string(),
            ));
        }
        for signed in commit.device_registrations() {
            let (registration, authority) = registrations
                .iter()
                .find(|(registration, _)| registration.device_id == signed.registration.device_id)
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "Store commit is missing registration bytes for {}",
                        signed.registration.device_id
                    ))
                })?;
            signed
                .registration
                .verify_registration(registration)
                .map_err(|error| DbError::Message(error.to_string()))?;
            if registration.store_root.store_root_hash != commit.store_root_hash {
                return Err(DbError::Message(format!(
                    "Store registration {} belongs to a different Store",
                    registration.device_id
                )));
            }
            let expected_authority = match (&registration.origin, &signed.authority) {
                (
                    crate::sync::store_commit::StoreDeviceRegistrationOrigin::Join {
                        attempt_id: origin_attempt,
                        outcome_slot,
                        ..
                    },
                    crate::sync::store_commit::StoreDeviceRegistrationActivationRef::Join {
                        attempt_id,
                        outcome,
                    },
                ) if origin_attempt == attempt_id && outcome_slot == outcome.slot() => {
                    crate::sync::store_commit::StoreDeviceRegistrationActivation::Join {
                        attempt_id: *attempt_id,
                        outcome: outcome.clone(),
                    }
                }
                (
                    crate::sync::store_commit::StoreDeviceRegistrationOrigin::Recovery {
                        recovery_id: origin_recovery,
                        recovery_slot,
                        ..
                    },
                    crate::sync::store_commit::StoreDeviceRegistrationActivationRef::Recovery {
                        recovery_id,
                        node,
                    },
                ) if origin_recovery == recovery_id && recovery_slot == node.slot() => {
                    crate::sync::store_commit::StoreDeviceRegistrationActivation::Recovery {
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
            let registration_object =
                serde_json::to_string(&signed.registration).map_err(|error| {
                    DbError::Message(format!("serialize Store registration exact ref: {error}"))
                })?;
            let activation_authority = serde_json::to_string(authority).map_err(|error| {
                DbError::Message(format!("serialize Store registration authority: {error}"))
            })?;
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
                            DbError::Message(format!(
                                "serialize Store registration exact ref: {error}"
                            ))
                        })?,
                        serde_json::to_string(authority).map_err(|error| {
                            DbError::Message(format!(
                                "serialize Store registration authority: {error}"
                            ))
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
                .map_err(|error| DbError::Message(format!("local registration object: {error}")))?;
            let local_ack_ref: StoreAckRef = serde_json::from_str(&local_ack_ref)
                .map_err(|error| DbError::Message(format!("local initial ack ref: {error}")))?;
            let local_ack_prepared: PreparedExactObject = serde_json::from_str(&local_ack_prepared)
                .map_err(|error| DbError::Message(format!("local initial ack object: {error}")))?;
            if local_hash != signed.registration.registration_hash.to_string()
                || local_bytes != registration.to_bytes()
                || local_prepared.reference() != &signed.registration.object
                || local_ack_prepared.reference() != &local_ack_ref.object
            {
                return Err(DbError::Message(
                    "activating commit differs from the complete local registration ref"
                        .to_string(),
                ));
            }
            let ack = StoreAck::parse_at(
                &local_ack_bytes,
                &registration.store_root,
                &local_ack_ref,
                registration,
            )
            .map_err(|error| DbError::Message(format!("local initial ack: {error}")))?;
            if ack.sequence != 1
                || ack.successor.predecessor.is_some()
                || local_ack_prepared
                    .reference()
                    .verify(local_ack_prepared.stored_bytes())
                    .is_err()
            {
                return Err(DbError::Message(
                    "local registration journal does not carry an initial acknowledgement"
                        .to_string(),
                ));
            }
            let state: LocalDeviceRegistrationState =
                serde_json::from_str(&local_state).map_err(|error| {
                    DbError::Message(format!("local registration journal: {error}"))
                })?;
            let activated_state = LocalDeviceRegistrationState::Activated {
                authority: authority.clone(),
            };
            match state {
                LocalDeviceRegistrationState::Prepared => {
                    return Err(DbError::Message(
                        "Store commit cannot activate a registration before exact creation"
                            .to_string(),
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
                                    DbError::Message(format!(
                                        "serialize activated journal: {error}"
                                    ))
                                })?,
                                local_device,
                                local_hash,
                                serde_json::to_string(&local_ack_ref).map_err(|error| {
                                    DbError::Message(format!(
                                        "serialize local initial ack: {error}"
                                    ))
                                })?,
                                serde_json::to_string(&LocalDeviceRegistrationState::Created)
                                    .map_err(|error| DbError::Message(format!(
                                        "serialize created journal: {error}"
                                    )))?,
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
                                DbError::Message(format!(
                                    "serialize activated initial ack: {error}"
                                ))
                            })?,
                            serde_json::to_string(&ack.successor.next_slot).map_err(|error| {
                                DbError::Message(format!(
                                    "serialize activated ack successor: {error}"
                                ))
                            })?,
                        ],
                    )
                    .map_err(DbError::from)?;
                    crate::database::set_protocol_state_on(
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
                        crate::database::get_protocol_state_on(conn, LOCAL_DEVICE_ID_STATE_KEY)?;
                    if stored_ack.0
                        != serde_json::to_string(&local_ack_ref).map_err(|error| {
                            DbError::Message(format!("serialize replayed initial ack: {error}"))
                        })?
                        || stored_ack.1
                            != serde_json::to_string(&ack.successor.next_slot).map_err(|error| {
                                DbError::Message(format!(
                                    "serialize replayed ack successor: {error}"
                                ))
                            })?
                        || local_device_id.as_deref() != Some(local_device.as_str())
                    {
                        return Err(DbError::Message(
                            "activated local journal differs from its exact initial ack"
                                .to_string(),
                        ));
                    }
                }
                LocalDeviceRegistrationState::Activated { .. } => {
                    return Err(DbError::Message(
                        "local registration already has another exact activation authority"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn activated_store_device_registrations(
        &self,
    ) -> Result<Vec<StoreDeviceRegistration>, DbError> {
        Ok(self
            .activated_store_device_registration_records()
            .await?
            .into_iter()
            .map(|(_, registration)| registration)
            .collect())
    }

    pub(crate) async fn store_device_state_for_order(
        &self,
        order: &crate::sync::store_commit::StoreCommitOrder,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), DbError> {
        let cut = order
            .predecessor_cut()
            .map_err(|error| DbError::Message(error.to_string()))?;
        self.database
            .call(move |conn| store_device_state_for_history_cut_on(conn, &cut))
            .await
    }

    pub(crate) async fn store_device_state_for_history_cut(
        &self,
        cut: &StoreHistoryCut,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), DbError> {
        let cut = cut.clone();
        self.database
            .call(move |conn| store_device_state_for_history_cut_on(conn, &cut))
            .await
    }

    pub(crate) async fn resolved_store_device_state(
        &self,
        reference: &StoreDeviceStateRef,
    ) -> Result<ResolvedStoreDeviceState, DbError> {
        let reference = reference.clone();
        self.database
            .call(move |conn| load_declared_store_device_state_on(conn, &reference))
            .await
    }

    pub(crate) async fn store_device_exclusion_freezes(
        &self,
    ) -> Result<Vec<StoreDeviceProposalAck>, DbError> {
        let root = self.local_store_root_ref().await?.ok_or_else(|| {
            DbError::Message("Store root is absent while loading exclusion freezes".to_string())
        })?;
        self.database
            .call(move |conn| {
                Ok(load_store_device_exclusion_freezes_on(conn, &root)?
                    .into_values()
                    .collect())
            })
            .await
    }

    pub(crate) async fn activated_store_device_registration_records(
        &self,
    ) -> Result<Vec<(StoreDeviceRegistrationRef, StoreDeviceRegistration)>, DbError> {
        let root = self.local_store_root_ref().await?.ok_or_else(|| {
            DbError::Message("Store root is absent while loading activated devices".to_string())
        })?;
        self.database.call(move |conn| {
            let mut statement = conn
                .prepare(
                    "SELECT device_id, registration_hash, registration_bytes,
                            registration_object
                     FROM store_device_registration_activations ORDER BY device_id",
                )
                .map_err(DbError::from)?;
                let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(DbError::from)?;
            rows
                .map(|row| {
                    let (device_id, registration_hash, bytes, object) =
                        row.map_err(DbError::from)?;
                    let device_id = device_id.parse().map_err(|error| {
                        DbError::Message(format!("activated Store device id: {error}"))
                    })?;
                    let registration_hash = registration_hash.parse().map_err(|error| {
                        DbError::Message(format!(
                            "activated Store device registration hash: {error}"
                        ))
                    })?;
                    let reference: StoreDeviceRegistrationRef =
                        serde_json::from_str(&object).map_err(|error| {
                        DbError::Message(format!(
                            "activated Store device exact reference: {error}"
                        ))
                    })?;
                    if reference.device_id != device_id
                        || reference.registration_hash != registration_hash
                    {
                        return Err(DbError::Message(
                            "activated Store registration columns differ from its exact reference"
                                .to_string(),
                        ));
                    }
                    let registration = StoreDeviceRegistration::parse_at(&bytes, &root, device_id)
                        .map_err(|error| {
                            DbError::Message(format!(
                                "activated Store device registration {device_id}: {error}"
                            ))
                        })?;
                    reference.verify_registration(&registration).map_err(|error| {
                        DbError::Message(format!(
                            "activated Store device registration {device_id} exact reference: {error}"
                        ))
                    })?;
                    Ok((reference, registration))
                })
                .collect::<Result<Vec<_>, DbError>>()
        })
        .await
    }

    pub(crate) async fn activated_store_device_registration(
        &self,
        reference: StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceRegistration, DbError> {
        let root = self.local_store_root_ref().await?.ok_or_else(|| {
            DbError::Message("Store root is absent while loading an activated device".to_string())
        })?;
        self.database
            .call(move |conn| load_activated_registration_on(conn, &root, &reference))
            .await
    }

    /// The exact registration this device is activated under, or `None` before
    /// it has one. This is the identity a signed artifact names when it names a
    /// device, so it is what a role check compares against.
    pub(crate) async fn local_activated_registration_ref(
        &self,
    ) -> Result<Option<StoreDeviceRegistrationRef>, DbError> {
        self.database
            .call(local_activated_registration_ref_on)
            .await
    }

    pub(crate) async fn local_blob_write_authority(
        &self,
    ) -> Result<(StoreDeviceRegistrationRef, StoreDeviceRegistration), DbError> {
        self.database
            .call(|conn| {
                local_store_authority_on(conn)
                    .map(|(_, reference, registration)| (reference, registration))
            })
            .await
    }

    pub(crate) async fn activated_store_device_registration_with_authority(
        &self,
        reference: StoreDeviceRegistrationRef,
    ) -> Result<
        (
            StoreDeviceRegistration,
            crate::sync::store_commit::StoreDeviceRegistrationActivation,
        ),
        DbError,
    > {
        let root = self.local_store_root_ref().await?.ok_or_else(|| {
            DbError::Message("Store root is absent while loading an activated device".to_string())
        })?;
        self.database
            .call(move |conn| {
                let registration = load_activated_registration_on(conn, &root, &reference)?;
                let authority: String = conn
                    .query_row(
                        "SELECT activation_authority FROM store_device_registration_activations \
                     WHERE device_id = ?1 AND registration_hash = ?2",
                        (
                            reference.device_id.to_string(),
                            reference.registration_hash.to_string(),
                        ),
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                let authority = serde_json::from_str(&authority).map_err(|error| {
                    DbError::Message(format!("activated Store registration authority: {error}"))
                })?;
                Ok((registration, authority))
            })
            .await
    }

    pub(crate) async fn activated_store_device_registration_for_device(
        &self,
        device_id: crate::sync::store_commit::StoreDeviceId,
    ) -> Result<
        Option<(
            StoreDeviceRegistrationRef,
            StoreDeviceRegistration,
            crate::sync::store_commit::StoreDeviceRegistrationActivation,
        )>,
        DbError,
    > {
        let root = self.local_store_root_ref().await?.ok_or_else(|| {
            DbError::Message("Store root is absent while loading an activated device".to_string())
        })?;
        self.database
            .call(move |conn| {
                let stored: Option<(String, String)> = conn
                    .query_row(
                        "SELECT registration_object, activation_authority \
                     FROM store_device_registration_activations WHERE device_id = ?1",
                        [device_id.to_string()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .map_err(DbError::from)?;
                let Some((reference, authority)) = stored else {
                    return Ok(None);
                };
                let reference: StoreDeviceRegistrationRef = serde_json::from_str(&reference)
                    .map_err(|error| {
                        DbError::Message(format!("activated Store registration ref: {error}"))
                    })?;
                if reference.device_id != device_id {
                    return Err(DbError::Message(
                        "activated Store registration row names another device".to_string(),
                    ));
                }
                let registration = load_activated_registration_on(conn, &root, &reference)?;
                let authority = serde_json::from_str(&authority).map_err(|error| {
                    DbError::Message(format!("activated Store registration authority: {error}"))
                })?;
                Ok(Some((reference, registration, authority)))
            })
            .await
    }
}
