use crate::database::blob_records::load_activated_registration_on;
use crate::database::remote_object_records::candidate_graph_exact_objects;
use crate::database::remote_object_records::mark_remote_object_uploaded_on;
use crate::database::remote_object_records::persist_exact_remote_object_on;

use super::*;

impl Database {
    pub(crate) async fn insert_circle_operation(
        &self,
        journal: crate::sync::circle_ops::CircleOperationJournal,
    ) -> Result<(), DbError> {
        let remotes = journal
            .closed_remote_objects()
            .map_err(|error| DbError::Message(error.to_string()))?;
        let row = PreparedCircleOperationRow::from_journal(journal)?;
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            for remote in &remotes {
                persist_exact_remote_object_on(&tx, remote, "Circle candidate graph")?;
            }
            tx.execute(
                "INSERT INTO circle_operations (operation_id, circle_id, payload)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![row.operation_id, row.circle_id, row.payload],
            )
            .map_err(DbError::from)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn circle_operation(
        &self,
        operation_id: &crate::sync::circle::CircleOperationId,
    ) -> Result<Option<crate::sync::circle_ops::CircleOperationJournal>, DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.call(move |conn| load_circle_operation_on(conn, &operation_id))
            .await
    }

    pub(crate) async fn oldest_pending_circle_operation(
        &self,
    ) -> Result<Option<crate::sync::circle_ops::CircleOperationJournal>, DbError> {
        self.call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT operation_id, circle_id, payload
                     FROM circle_operations
                     ORDER BY rowid",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .map_err(DbError::from)?;
            for row in rows {
                let (operation_id, circle_id, payload) = row.map_err(DbError::from)?;
                let journal = parse_circle_operation_row(&operation_id, &circle_id, &payload)?;
                if matches!(
                    journal.state(),
                    crate::sync::circle::CircleOperationState::Pending
                ) {
                    return Ok(Some(journal));
                }
            }
            Ok(None)
        })
        .await
    }

    pub async fn get_circle_operations(
        &self,
    ) -> Result<Vec<crate::sync::circle::CircleOperationInfo>, DbError> {
        self.call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT operation_id, circle_id, payload
                     FROM circle_operations
                     ORDER BY rowid",
                )
                .map_err(DbError::from)?;
            let operations = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .map_err(DbError::from)?
                .map(|row| {
                    let (operation_id, circle_id, payload) = row.map_err(DbError::from)?;
                    let journal = parse_circle_operation_row(&operation_id, &circle_id, &payload)?;
                    Ok(crate::sync::circle::CircleOperationInfo {
                        operation_id: journal.operation_id.clone(),
                        circle_id: journal.circle_id(),
                        kind: journal.kind(),
                        state: journal.state(),
                    })
                })
                .collect();
            operations
        })
        .await
    }

    pub async fn get_circles(
        &self,
        identity_pubkey: &str,
    ) -> Result<Vec<crate::sync::circle::CircleInfo>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.call(move |conn| {
            let mut statement = conn
                .prepare(
                    "SELECT circle_id, state
                     FROM circle_current_state
                     ORDER BY circle_id",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(DbError::from)?;
            let mut circles = Vec::new();
            for row in rows {
                let (stored_circle_id, state) = row.map_err(DbError::from)?;
                let state = Self::parse_circle_current_state(&stored_circle_id, &state)?;
                if let Some((_current, access, roster, metadata)) = state.active() {
                    let circle_id = state.circle_id();
                    if access.recipient_pubkey != identity_pubkey {
                        return Err(DbError::Message(format!(
                            "active circle {circle_id} belongs to another local identity"
                        )));
                    }
                    let role =
                        roster
                            .members()
                            .get(&identity_pubkey)
                            .copied()
                            .ok_or_else(|| {
                                DbError::Message(format!(
                                    "activated circle {circle_id} excludes the local identity"
                                ))
                            })?;
                    circles.push(crate::sync::circle::CircleInfo {
                        id: circle_id,
                        name: metadata.name.clone(),
                        role,
                    });
                }
            }
            Ok(circles)
        })
        .await
    }

    pub async fn get_circle_members(
        &self,
        circle_id: crate::sync::circle::CircleId,
        identity_pubkey: &str,
        store_members: std::collections::BTreeSet<String>,
    ) -> Result<Vec<crate::sync::circle::CircleMemberInfo>, DbError> {
        let identity_pubkey = identity_pubkey.to_string();
        self.call(move |conn| {
            let state = Self::circle_current_state_on(conn, circle_id)?.ok_or_else(|| {
                DbError::Message(format!("Circle {circle_id} has no current state"))
            })?;
            let Some((_current, access, roster, _metadata)) = state.active() else {
                return Err(DbError::Message(format!(
                    "Circle {circle_id} has no active local state"
                )));
            };
            if access.recipient_pubkey != identity_pubkey {
                return Err(DbError::Message(format!(
                    "active Circle {circle_id} belongs to another local identity"
                )));
            }
            Ok(roster
                .members()
                .into_iter()
                .filter(|(pubkey, _)| store_members.contains(pubkey))
                .map(|(pubkey, role)| crate::sync::circle::CircleMemberInfo {
                    is_self: pubkey == identity_pubkey,
                    pubkey,
                    role,
                })
                .collect())
        })
        .await
    }

    pub(crate) async fn circle_authoring_context(
        &self,
        circle_id: crate::sync::circle::CircleId,
        identity_pubkey: &str,
    ) -> Result<
        (
            crate::sync::circle_activation::CircleAuthoringState,
            StoreBatchCommitRef,
        ),
        DbError,
    > {
        let identity_pubkey = identity_pubkey.to_string();
        self.call(move |conn| {
            let state = Self::circle_current_state_on(conn, circle_id)?.ok_or_else(|| {
                DbError::Message(format!("Circle {circle_id} has no current state"))
            })?;
            let authoring = state.authoring_state().ok_or_else(|| {
                DbError::Message(format!("Circle {circle_id} has no active authoring state"))
            })?;
            if authoring.access.recipient_pubkey != identity_pubkey {
                return Err(DbError::Message(format!(
                    "active Circle {circle_id} belongs to another local identity"
                )));
            }
            let control_coord = serde_json::to_string(&authoring.control.coord).map_err(|error| {
                DbError::Message(format!("serialize current Circle control coordinate: {error}"))
            })?;
            let commit_hash = conn
                .query_row(
                    "SELECT commit_hash FROM circle_control_activations
                     WHERE circle_id = ?1 AND control_coord = ?2",
                    rusqlite::params![circle_id.to_string(), control_coord],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)?;
            let mut statement = conn
                .prepare(
                    "SELECT device_id, seq, commit_ref,
                            retained_commit_ref, retained_input_hash
                     FROM materialized_commits",
                )
                .map_err(DbError::from)?;
            let rows = statement
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
            drop(statement);
            let mut activated_commit = None;
            for row in rows {
                let (
                    stream_id,
                    sequence,
                    encoded,
                    retained_commit_ref,
                    retained_input_hash,
                ) = row;
                let sequence = Self::sequence_from_sqlite(&stream_id, sequence)?;
                let reference = Self::parse_materialized_commit_row_on(
                    conn,
                    &stream_id,
                    sequence,
                    &encoded,
                    retained_commit_ref.as_deref(),
                    retained_input_hash.as_deref(),
                )?;
                if reference.commit_hash.to_string() != commit_hash {
                    continue;
                }
                if activated_commit.replace(reference).is_some() {
                    return Err(DbError::Message(format!(
                        "Circle {circle_id} activation commit is duplicated in the materialized ledger"
                    )));
                }
            }
            let activated_commit = activated_commit.ok_or_else(|| {
                DbError::Message(format!(
                    "Circle {circle_id} activation commit {commit_hash} is absent from the materialized ledger"
                ))
            })?;
            Ok((authoring, activated_commit))
        })
        .await
    }

    pub(crate) async fn circle_publication_context(
        &self,
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
    ) -> Result<(EncryptionService, crate::KeyFingerprint), DbError> {
        self.circle_access_context(circle_id, expected_control)
            .await?
            .ok_or_else(|| {
                DbError::Message(format!("Circle {circle_id} has no active publication key"))
            })
    }

    pub(crate) async fn circle_access_context(
        &self,
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
    ) -> Result<Option<(EncryptionService, crate::KeyFingerprint)>, DbError> {
        self.call(move |conn| {
            let Some(state) = Self::circle_current_state_on(conn, circle_id)? else {
                return Ok(None);
            };
            let Some((current, access, _roster, _metadata)) = state.active() else {
                return Ok(None);
            };
            if current.coordinate() != &expected_control {
                return Ok(None);
            }
            let crate::sync::circle::CircleAccessDisposition::Active {
                keyring,
                key_fingerprint,
                ..
            } = &access.disposition
            else {
                return Err(DbError::Message(format!(
                    "active Circle {circle_id} has inactive access"
                )));
            };
            let encryption = EncryptionService::from(
                crate::encryption::MasterKeyring::from_serialized(keyring).map_err(|error| {
                    DbError::Message(format!("parse Circle publication keyring: {error}"))
                })?,
            );
            if encryption.seal_key_fingerprint() != *key_fingerprint {
                return Err(DbError::Message(format!(
                    "Circle {circle_id} publication key fingerprint is invalid"
                )));
            }
            Ok(Some((encryption, *key_fingerprint)))
        })
        .await
    }

    pub(crate) async fn circle_authorizes_writer(
        &self,
        circle_id: crate::sync::circle::CircleId,
        expected_control: crate::sync::circle::CircleControlCoord,
        author_pubkey: String,
    ) -> Result<bool, DbError> {
        self.call(move |conn| {
            let Some(state) = Self::circle_current_state_on(conn, circle_id)? else {
                return Ok(false);
            };
            let Some((current, _access, roster, _metadata)) = state.active() else {
                return Ok(false);
            };
            if current.coordinate() != &expected_control {
                return Ok(false);
            }
            Ok(roster.members().contains_key(&author_pubkey))
        })
        .await
    }

    fn circle_current_state_on(
        conn: &Connection,
        circle_id: crate::sync::circle::CircleId,
    ) -> Result<Option<crate::sync::circle_activation::CircleCurrentState>, DbError> {
        let stored = conn
            .query_row(
                "SELECT circle_id, state FROM circle_current_state WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        stored
            .map(|(stored_circle_id, state)| {
                Self::parse_circle_current_state(&stored_circle_id, &state)
            })
            .transpose()
    }

    fn parse_circle_current_state(
        stored_circle_id: &str,
        payload: &[u8],
    ) -> Result<crate::sync::circle_activation::CircleCurrentState, DbError> {
        let circle_id: crate::sync::circle::CircleId = stored_circle_id
            .parse()
            .map_err(|error| DbError::Message(format!("parse current Circle id: {error}")))?;
        let state: crate::sync::circle_activation::CircleCurrentState =
            serde_json::from_slice(payload).map_err(|error| {
                DbError::Message(format!("parse Circle current state: {error}"))
            })?;
        if !state.verify() || state.circle_id() != circle_id {
            return Err(DbError::Message(format!(
                "Circle {circle_id} has invalid current state"
            )));
        }
        Ok(state)
    }

    pub(super) fn reduce_circle_current_state_on(
        conn: &Connection,
        candidate_family: crate::sync::store_commit::CandidateFamilyId,
        activation: &crate::sync::circle_ops::VerifiedCircleReference,
    ) -> Result<Vec<u8>, DbError> {
        let next_state = crate::sync::circle_activation::CircleCurrentState::from_verified(
            candidate_family,
            activation,
        )
        .map_err(DbError::Message)?;
        let current_state = Self::circle_current_state_on(conn, activation.circle_id)?;
        let current_state = match current_state {
            Some(current) => current.advance(next_state).map_err(DbError::Message)?,
            None if activation.control.value.is_founder() => next_state,
            None => {
                return Err(DbError::Message(format!(
                    "circle {} current state is absent for a successor control",
                    activation.circle_id
                )))
            }
        };
        serde_json::to_vec(&current_state)
            .map_err(|error| DbError::Message(format!("serialize Circle current state: {error}")))
    }

    pub(crate) async fn update_circle_operation(
        &self,
        journal: crate::sync::circle_ops::CircleOperationJournal,
    ) -> Result<(), DbError> {
        let uploaded_ids = journal
            .operation()
            .uploaded
            .iter()
            .filter_map(|step| journal.operation().prepared_objects.get(step))
            .map(|prepared| remote_object_id(prepared.reference()))
            .collect::<BTreeSet<_>>();
        let uploaded = journal
            .closed_remote_objects()
            .map_err(|error| DbError::Message(error.to_string()))?
            .into_iter()
            .filter(|remote| uploaded_ids.contains(&remote.object_id()))
            .collect::<Vec<_>>();
        let row = PreparedCircleOperationRow::from_journal(journal)?;
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            load_circle_operation_on(&tx, &row.operation_id)?.ok_or_else(|| {
                DbError::Message(format!(
                    "circle operation {} disappeared during publication",
                    row.operation_id
                ))
            })?;
            let updated = tx
                .execute(
                    "UPDATE circle_operations SET payload = ?3
                     WHERE operation_id = ?1 AND circle_id = ?2",
                    rusqlite::params![row.operation_id, row.circle_id, row.payload],
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(
                    "circle operation disappeared during publication".to_string(),
                ));
            }
            for remote in uploaded {
                mark_remote_object_uploaded_on(&tx, remote)?;
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn block_circle_operation(
        &self,
        operation_id: &crate::sync::circle::CircleOperationId,
        reason: String,
    ) -> Result<(), DbError> {
        let mut journal = self.circle_operation(operation_id).await?.ok_or_else(|| {
            DbError::Message(format!("circle operation {operation_id} is absent"))
        })?;
        journal
            .block(reason)
            .map_err(|error| DbError::Message(error.to_string()))?;
        self.update_circle_operation(journal).await
    }

    pub(crate) async fn activate_circle_operation(
        &self,
        journal: crate::sync::circle_ops::CircleOperationJournal,
        verified: crate::sync::circle_ops::VerifiedCircleActivations,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            journal
                .validate_identity()
                .map_err(|error| DbError::Message(error.to_string()))?;
            let durable = load_circle_operation_on(&tx, journal.operation_id.as_str())?
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "circle operation {} disappeared during activation",
                        journal.operation_id
                    ))
                })?;
            if durable != journal {
                return Err(DbError::Message(format!(
                    "circle operation {} changed before activation",
                    journal.operation_id
                )));
            }
            if !matches!(
                journal.state(),
                crate::sync::circle::CircleOperationState::Pending
            ) {
                return Err(DbError::Message(
                    "blocked circle operation cannot activate".to_string(),
                ));
            }
            let operation = journal.operation();
            let creation = &operation.creation;
            let resolved_roster = creation.resolved_roster();
            if !creation.control.verify()
                || !creation.metadata.verify()
                || !resolved_roster.verify()
            {
                return Err(DbError::Message(
                    "circle operation contains invalid signed objects".to_string(),
                ));
            }
            let unverified_commit: StoreBatchCommit =
                serde_json::from_slice(&operation.commit_bytes).map_err(|error| {
                    DbError::Message(format!("parse circle Store commit: {error}"))
                })?;
            let root = required_store_root_authority_on(&tx)?;
            let author =
                load_activated_registration_on(&tx, &root, &unverified_commit.author_registration)?;
            let [activation] = verified.circles() else {
                return Err(DbError::Message(
                    "local Circle publication must carry one common-verifier result".to_string(),
                ));
            };
            let verify_commit = || {
                let commit = StoreBatchCommit::parse_at(
                    &operation.commit_bytes,
                    root.store_root_hash,
                    &operation.commit_ref.coord,
                    &author,
                )
                .map_err(|error| {
                    DbError::Message(format!("verify circle Store commit: {error}"))
                })?;
                operation
                    .commit_ref
                    .verify_commit(&commit)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                if operation.commit_ref.object.slot().logical_key()
                    != commit_semantic_prefix(
                        commit.candidate_family(),
                        &operation.commit_ref.coord.stream_id.to_string(),
                        commit.seq(),
                        commit.commit_hash(),
                    ) + ".json"
                {
                    return Err(DbError::Message(
                        "circle commit exact object occupies a different semantic slot".to_string(),
                    ));
                }
                let [control_ref] = commit.circle_controls() else {
                    return Err(DbError::Message(
                        "circle creation Store commit is not an exact control-only batch"
                            .to_string(),
                    ));
                };
                let expected_ref = creation.control_ref(
                    control_ref.objects().clone(),
                    Some(control_ref.head_object().clone()),
                );
                if control_ref != &expected_ref
                    || !commit.operations().is_some_and(
                        crate::sync::store_commit::StoreCommitOperations::is_circle_control_activation_only,
                    )
                {
                    return Err(DbError::Message(
                        "circle creation Store commit is not an exact control-only batch"
                            .to_string(),
                    ));
                }
                if activation.reference != *control_ref
                    || activation.circle_id != creation.circle_id
                    || activation.control != creation.control
                    || verified.stream_activations().activating_commit() != &operation.commit_ref
                    || verified.stream_activations().as_slice() != commit.stream_activations()
                {
                    return Err(DbError::Message(
                        "common-verifier Circle result differs from the durable signed operation"
                            .to_string(),
                    ));
                }
                Ok(commit)
            };
            let (commit, activation, head_object_id) = {
                    let head = &operation.policy.head;
                    let history_summary = &operation.policy.history_summary;
                    let commit = verify_commit()?;
                    let parsed = StoreDeviceHead::parse_at(
                        &head.to_bytes(),
                        commit.store_root_hash,
                        &author,
                        &operation.commit_ref,
                    )
                    .map_err(|error| {
                        DbError::Message(format!("verify circle activation head: {error}"))
                    })?;
                    if parsed.commit != operation.commit_ref {
                        return Err(DbError::Message(
                            "circle activation head names a different commit".to_string(),
                        ));
                    }
                    let device_operations =
                        VerifiedStoreDeviceOperations::without_exclusions(&commit)
                            .map_err(|error| DbError::Message(error.to_string()))?;
                    let prepared_head = operation.prepared_objects.get("store-head").ok_or_else(|| {
                        DbError::Message(
                            "Merge Circle operation lacks its prepared Store head".to_string(),
                        )
                    })?;
                    let materialization = VerifiedMergeMaterialization::verify(
                        &root,
                        &commit,
                        &operation.commit_ref,
                        &[],
                        &device_operations,
                        &verified,
                        head,
                        prepared_head.reference(),
                        history_summary,
                        None,
                        &[],
                        None,
                    )?;
                    Self::record_verified_merge_materialization_on(&tx, materialization)?;
                    (
                        commit,
                        activation.clone(),
                        Some(remote_object_id(prepared_head.reference())),
                    )
            };
            let mut object_ids = candidate_graph_exact_objects(&commit)?
                .iter()
                .map(remote_object_id)
                .collect::<Vec<_>>();
            object_ids.push(remote_object_id(&operation.commit_ref.object));
            if let Some(head_object_id) = head_object_id {
                object_ids.push(head_object_id);
            }
            Self::activate_store_operation_remote_objects_on(
                &tx,
                &operation.commit_ref,
                &object_ids,
            )?;
            Self::record_verified_circle_activations_on(
                &tx,
                &commit,
                &operation.commit_ref,
                &[activation],
            )?;
            let deleted = tx
                .execute(
                    "DELETE FROM circle_operations WHERE operation_id = ?1 AND circle_id = ?2",
                    rusqlite::params![
                        journal.operation_id.as_str(),
                        creation.circle_id.to_string()
                    ],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "circle operation disappeared during activation".to_string(),
                ));
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }
}
