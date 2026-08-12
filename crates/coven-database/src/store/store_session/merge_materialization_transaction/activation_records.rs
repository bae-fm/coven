use super::*;

impl<'transaction, 'connection> MergeMaterializationTransaction<'transaction, 'connection> {
    pub(crate) fn record_activated_store_ack(
        &self,
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
        let conn = self.store.transaction;
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
                serde_json::from_str::<StoreAckRef>(&raw)
                    .map_err(|error| DbError::context("activated Store acknowledgement ref", error))
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
                serde_json::to_string(reference).map_err(|error| DbError::context(
                    "serialize activated Store acknowledgement ref",
                    error
                ))?,
                serde_json::to_string(commit_ref).map_err(|error| DbError::context(
                    "serialize acknowledgement activating commit ref",
                    error
                ))?,
            ],
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    pub(crate) fn record_activated_circle_acks(
        &self,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let conn = self.store.transaction;
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
                        DbError::context("activated Circle acknowledgement ref", error)
                    })
                })
                .transpose()?;
            if current.as_ref().is_some_and(|current| {
                current.registration != reference.registration
                    || current.circle_id != reference.circle_id
                    || current.sequence >= reference.sequence
            }) {
                return Err(DbError::Message(
                    "Circle acknowledgement activation does not advance the exact stream"
                        .to_string(),
                ));
            }
            conn.execute(
                "INSERT INTO activated_circle_acks
                   (circle_id, device_id, ack_ref, activating_commit)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(circle_id, device_id) DO UPDATE SET
                   ack_ref = excluded.ack_ref, activating_commit = excluded.activating_commit",
                rusqlite::params![
                    circle_id,
                    device_id,
                    serde_json::to_string(reference).map_err(|error| DbError::context(
                        "serialize activated Circle acknowledgement ref",
                        error
                    ))?,
                    serde_json::to_string(commit_ref).map_err(|error| DbError::context(
                        "serialize Circle acknowledgement activating commit",
                        error
                    ))?,
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    pub(super) fn record_author_exclusion_activations(
        &self,
        materialization: &VerifiedMergeMaterialization<'_>,
    ) -> Result<(), DbError> {
        let conn = self.store.transaction;
        let commit_ref = materialization.commit_ref();
        let activation_head = materialization.activation_head();
        let activation_head = coven_protocol::store_commit::StoreDeviceHeadRef {
            head_hash: activation_head.head_hash(),
            object: materialization.activation_head_object().clone(),
        };
        let activation_commit = serde_json::to_string(commit_ref).map_err(|error| {
            DbError::context("serialize author exclusion activation commit", error)
        })?;
        for (exclusion, accepted_cut) in materialization.device_operations().exclusions() {
            let StoreHistoryCut(accepted_cut) = accepted_cut;
            let exclusion_json = serde_json::to_string(exclusion)
                .map_err(|error| DbError::context("serialize author exclusion reference", error))?;
            let accepted_cut_json = serde_json::to_string(accepted_cut).map_err(|error| {
                DbError::context("serialize author exclusion accepted cut", error)
            })?;
            let activation_head_json =
                serde_json::to_string(&activation_head).map_err(|error| {
                    DbError::context("serialize author exclusion activation head", error)
                })?;
            let inserted = conn
                .execute(
                    "INSERT INTO store_author_exclusion_activations (
                         exclusion_ref, accepted_cut, activation_commit, activation_head
                     ) VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(exclusion_ref) DO NOTHING",
                    (
                        &exclusion_json,
                        &accepted_cut_json,
                        &activation_commit,
                        &activation_head_json,
                    ),
                )
                .map_err(DbError::from)?;
            if inserted == 0 {
                let stored: (String, String, String) = conn
                    .query_row(
                        "SELECT accepted_cut, activation_commit, activation_head
                         FROM store_author_exclusion_activations
                         WHERE exclusion_ref = ?1",
                        [&exclusion_json],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(DbError::from)?;
                if stored
                    != (
                        accepted_cut_json,
                        activation_commit.clone(),
                        activation_head_json,
                    )
                {
                    return Err(DbError::Message(
                        "author exclusion already names different activation evidence".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn record_verified_circle_activations(
        &self,
        verified_commit: &VerifiedStoreBatchCommit,
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<(), DbError> {
        let conn = self.store.transaction;
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        if activations.len() != commit.circle_controls().len() {
            return Err(DbError::Message(
                "verified circle activations do not cover every control reference".to_string(),
            ));
        }
        let stream_id = commit_ref.coord.stream_id.to_string();
        let seq = Database::sequence_to_sqlite(&stream_id, commit_ref.coord.sequence())?;
        for activation in activations {
            if !commit.circle_controls().contains(&activation.reference)
                || activation.reference.circle_id() != activation.circle_id
                || activation.reference.control() != &activation.control.coord
                || !activation.control.verify()
            {
                return Err(DbError::Message(
                    "verified circle activation differs from Store control reference".to_string(),
                ));
            }
            let circle_id = activation.circle_id.to_string();
            if let Some(access) = &activation.local_access {
                let leaf = &access.leaf.value;
                if activation.control.value.author_pubkey != leaf.owner_pubkey {
                    return Err(DbError::Message(format!(
                        "circle {circle_id} local access signer differs from its control author"
                    )));
                }
                match (&leaf.disposition, &access.active) {
                    (coven_protocol::circle::CircleAccessDisposition::Active { .. }, Some(_))
                    | (coven_protocol::circle::CircleAccessDisposition::Inactive, None) => {}
                    _ => {
                        return Err(DbError::Message(format!(
                            "circle {circle_id} access state differs from its disposition"
                        )));
                    }
                }
            }
            let mut statement = conn
                .prepare(
                    "SELECT control_bytes FROM circle_control_activations
                     WHERE circle_id = ?1",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([&circle_id], |row| row.get::<_, Vec<u8>>(0))
                .map_err(DbError::from)?;
            let mut existing_controls = Vec::new();
            for bytes in rows {
                let bytes = bytes.map_err(DbError::from)?;
                let control: coven_protocol::circle::CircleControl = serde_json::from_slice(&bytes)
                    .map_err(|error| DbError::context("parse activated circle control", error))?;
                existing_controls.push(control);
            }
            drop(statement);
            if activation.control.value.is_founder() {
                if !existing_controls.is_empty() {
                    return Err(DbError::Message(format!(
                        "circle {circle_id} already has a founder control"
                    )));
                }
            } else {
                let covered = existing_controls
                    .iter()
                    .filter(|control| activation.control.value.causally_covers(control))
                    .collect::<Vec<_>>();
                let order = &activation.control.value.value.order;
                let expected_covered =
                    order.dependencies.len() + usize::from(order.previous_control_hash.is_some());
                if covered.len() != expected_covered
                    || covered.iter().any(|control| {
                        control
                            .owners()
                            .binary_search(&activation.control.value.author_pubkey)
                            .is_err()
                    })
                {
                    return Err(DbError::Message(format!(
                        "circle {circle_id} control does not cover every authorized predecessor"
                    )));
                }
            }
            let next_state = coven_protocol::circle_activation::CircleCurrentState::from_verified(
                commit.candidate_family(),
                activation,
            )
            .map_err(DbError::from)?;
            let current_state = crate::store::circle_operations::circle_current_state_on(
                conn,
                activation.circle_id,
            )?;
            let current_state = match current_state {
                Some(current) => current.advance(next_state).map_err(DbError::from)?,
                None if activation.control.value.is_founder() => next_state,
                None => {
                    return Err(DbError::Message(format!(
                        "circle {} current state is absent for a successor control",
                        activation.circle_id
                    )));
                }
            };
            let current_state_payload = serde_json::to_vec(&current_state)
                .map_err(|error| DbError::context("serialize Circle current state", error))?;
            let control_coord = serde_json::to_string(&activation.control.coord)
                .map_err(|error| DbError::context("serialize circle control coordinate", error))?;
            conn.execute(
                "INSERT INTO circle_control_activations
                 (circle_id, control_coord, stream_id, seq, commit_hash, control_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    &circle_id,
                    &control_coord,
                    stream_id,
                    seq,
                    commit.commit_hash().to_string(),
                    &activation.control.bytes,
                ],
            )
            .map_err(DbError::from)?;
            if let Some(access) = &activation.local_access {
                let disposition = match access.leaf.value.disposition {
                    coven_protocol::circle::CircleAccessDisposition::Active { .. } => "active",
                    coven_protocol::circle::CircleAccessDisposition::Inactive => "inactive",
                };
                conn.execute(
                    "INSERT INTO circle_access_cache
                     (circle_id, control_coord, owner_pubkey, disposition)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        &circle_id,
                        &control_coord,
                        &access.leaf.value.owner_pubkey,
                        disposition,
                    ],
                )
                .map_err(DbError::from)?;
            }
            conn.execute(
                "INSERT INTO circle_current_state (circle_id, state) VALUES (?1, ?2)
                 ON CONFLICT(circle_id) DO UPDATE SET state = excluded.state",
                rusqlite::params![&circle_id, current_state_payload],
            )
            .map_err(DbError::from)?;
            if self.circle_current_state_is_deleted(activation.circle_id)? {
                conn.execute(
                    "DELETE FROM circle_access_cache WHERE circle_id = ?1",
                    [&circle_id],
                )
                .map_err(DbError::from)?;
            }
        }
        Ok(())
    }
}
