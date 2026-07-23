use rusqlite::Connection;

use super::{StoreDatabase, StoreDatabaseTransaction};
use crate::database::{
    candidate_graph_exact_objects, load_activated_registration_on, load_circle_operation_on,
    mark_remote_object_uploaded_on, persist_prepared_remote_object_on,
    required_store_root_authority_on, DbError, PreparedCircleOperationRow,
    VerifiedMergeMaterialization,
};
use crate::sync::circle::{CircleOperationId, CircleOperationState};
use crate::sync::remote_object::remote_object_id;
use crate::sync::store::circle_controls::{CircleOperationJournal, VerifiedCircleActivations};
use crate::sync::store_commit::{
    commit_semantic_prefix, StoreBatchCommit, StoreDeviceHead, VerifiedStoreDeviceOperations,
};

impl StoreDatabase {
    pub(in crate::sync::store) async fn insert_circle_operation(
        &self,
        journal: CircleOperationJournal,
    ) -> Result<(), DbError> {
        let remotes = journal
            .closed_remote_objects()
            .map_err(|error| DbError::Message(error.to_string()))?;
        let owner = journal.operation().commit_ref.clone();
        let row = PreparedCircleOperationRow::from_journal(journal)?;
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                for remote in &remotes {
                    persist_prepared_remote_object_on(
                        &tx,
                        remote,
                        &owner,
                        "Circle candidate graph",
                    )?;
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

    pub(in crate::sync::store) async fn circle_operation(
        &self,
        operation_id: &CircleOperationId,
    ) -> Result<Option<CircleOperationJournal>, DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.database
            .call(move |conn| load_circle_operation_on(conn, &operation_id))
            .await
    }

    pub(in crate::sync::store) async fn oldest_pending_circle_operation(
        &self,
    ) -> Result<Option<CircleOperationJournal>, DbError> {
        self.database
            .call(|conn| {
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
                    let journal = crate::database::parse_circle_operation_row(
                        &operation_id,
                        &circle_id,
                        &payload,
                    )?;
                    if journal.is_publishable() {
                        return Ok(Some(journal));
                    }
                }
                Ok(None)
            })
            .await
    }

    pub(in crate::sync::store) async fn waiting_circle_operations(
        &self,
    ) -> Result<Vec<CircleOperationJournal>, DbError> {
        self.database
            .call(|conn| {
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
                let mut waiting = Vec::new();
                for row in rows {
                    let (operation_id, circle_id, payload) = row.map_err(DbError::from)?;
                    let journal = crate::database::parse_circle_operation_row(
                        &operation_id,
                        &circle_id,
                        &payload,
                    )?;
                    if matches!(
                        journal.state(),
                        CircleOperationState::WaitingForCloseResponses
                    ) {
                        waiting.push(journal);
                    }
                }
                Ok(waiting)
            })
            .await
    }

    pub(in crate::sync::store) async fn update_circle_operation(
        &self,
        journal: CircleOperationJournal,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                update_circle_operation_on(&tx, journal)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(in crate::sync::store) async fn begin_circle_operation_finalization(
        &self,
        journal: CircleOperationJournal,
    ) -> Result<(), DbError> {
        if !matches!(journal.state(), CircleOperationState::Finalizing) {
            return Err(DbError::Message(
                "Circle finalization journal is not in finalizing state".to_string(),
            ));
        }
        let remotes = journal
            .closed_remote_objects()
            .map_err(|error| DbError::Message(error.to_string()))?;
        let owner = journal.operation().commit_ref.clone();
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let durable = load_circle_operation_on(&tx, journal.operation_id.as_str())?
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "Circle operation {} disappeared before finalization",
                            journal.operation_id
                        ))
                    })?;
                if !matches!(
                    durable.state(),
                    CircleOperationState::WaitingForCloseResponses
                ) || durable.circle_id != journal.circle_id
                    || durable.intent != journal.intent
                {
                    return Err(DbError::Message(format!(
                        "Circle operation {} changed before finalization",
                        journal.operation_id
                    )));
                }
                for remote in &remotes {
                    persist_prepared_remote_object_on(
                        &tx,
                        remote,
                        &owner,
                        "Circle close-finalization candidate graph",
                    )?;
                }
                persist_circle_operation_row_on(&tx, journal)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(in crate::sync::store) async fn block_circle_operation(
        &self,
        operation_id: &CircleOperationId,
        reason: String,
    ) -> Result<(), DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let mut journal =
                    load_circle_operation_on(&tx, &operation_id)?.ok_or_else(|| {
                        DbError::Message(format!("circle operation {operation_id} is absent"))
                    })?;
                journal
                    .block(reason)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                update_circle_operation_on(&tx, journal)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(in crate::sync::store) async fn activate_circle_operation(
        &self,
        journal: CircleOperationJournal,
        verified: VerifiedCircleActivations,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
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
                if !journal.is_publishable() {
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
                let author = load_activated_registration_on(
                    &tx,
                    &root,
                    &unverified_commit.author_registration,
                )?;
                let [activation] = verified.circles() else {
                    return Err(DbError::Message(
                        "local Circle publication must carry one common-verifier result"
                            .to_string(),
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
                            "circle commit exact object occupies a different semantic slot"
                                .to_string(),
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
                        || verified.stream_activations().activating_commit()
                            != &operation.commit_ref
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
                    let device_operations = VerifiedStoreDeviceOperations::without_exclusions(
                        &commit,
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?;
                    let prepared_head = operation.prepared_objects.get("store-head").ok_or_else(
                        || {
                            DbError::Message(
                                "Merge Circle operation lacks its prepared Store head".to_string(),
                            )
                        },
                    )?;
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
                    StoreDatabaseTransaction::new(&tx)
                        .record_verified_merge_materialization(materialization)?;
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
                for access in &creation.access {
                    if let crate::sync::circle::CircleAccessDisposition::Active {
                        bootstrap: Some(bootstrap),
                        ..
                    } = &access.leaf.value.disposition
                    {
                        object_ids.extend(
                            bootstrap
                                .blobs
                                .iter()
                                .map(|blob| remote_object_id(blob.object())),
                        );
                    }
                }
                object_ids.push(remote_object_id(&operation.commit_ref.object));
                if let Some(head_object_id) = head_object_id {
                    object_ids.push(head_object_id);
                }
                let store_transaction = StoreDatabaseTransaction::new(&tx);
                store_transaction.activate_store_operation_remote_objects(
                    &operation.commit_ref,
                    &object_ids,
                )?;
                store_transaction.record_verified_circle_activations(
                    &commit,
                    &operation.commit_ref,
                    &[activation],
                )?;
                if !journal.is_finalizing()
                    && matches!(
                    journal.intent,
                    crate::sync::store::circle_controls::CircleOperationIntent::RemoveMember { .. }
                )
                {
                    let mut waiting = journal;
                    waiting
                        .wait_for_close_responses()
                        .map_err(|error| DbError::Message(error.to_string()))?;
                    persist_circle_operation_row_on(&tx, waiting)?;
                } else {
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
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }
}

fn update_circle_operation_on(
    conn: &Connection,
    journal: CircleOperationJournal,
) -> Result<(), DbError> {
    let uploaded_ids = journal
        .uploaded_object_ids()
        .map_err(|error| DbError::Message(error.to_string()))?;
    let uploaded = journal
        .closed_remote_objects()
        .map_err(|error| DbError::Message(error.to_string()))?
        .into_iter()
        .filter(|remote| uploaded_ids.contains(&remote.object_id()))
        .collect::<Vec<_>>();
    persist_circle_operation_row_on(conn, journal)?;
    for remote in uploaded {
        mark_remote_object_uploaded_on(conn, remote)?;
    }
    Ok(())
}

fn persist_circle_operation_row_on(
    conn: &Connection,
    journal: CircleOperationJournal,
) -> Result<(), DbError> {
    let row = PreparedCircleOperationRow::from_journal(journal)?;
    load_circle_operation_on(conn, &row.operation_id)?.ok_or_else(|| {
        DbError::Message(format!(
            "circle operation {} disappeared during publication",
            row.operation_id
        ))
    })?;
    let updated = conn
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
    Ok(())
}
