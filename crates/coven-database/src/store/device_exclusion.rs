use std::future::Future;
use std::pin::Pin;

use rusqlite::{Connection, OptionalExtension};

use super::*;
use crate::store::StoreSession;
use crate::{mark_remote_object_uploaded_on, update_remote_object_on};
use coven_protocol::device_exclusion_journal::{
    DurableStoreDeviceExclusionObject, DurableStoreDeviceExclusionOperation,
    StoreDeviceExclusionCompletion, StoreDeviceExclusionJournalError,
};
use coven_protocol::remote_object::{
    remote_object_id, ClosedRemoteObject, RemoteObjectRecord, RetainedAuthorityObjectState,
};
use coven_protocol::store_commit::ObjectHash;

pub(crate) fn store_device_exclusion_journal_error(
    error: StoreDeviceExclusionJournalError,
) -> DbError {
    DbError::Message(error.to_string())
}

pub(crate) fn parse_store_device_exclusion_operation(
    operation_id: ObjectHash,
    raw: &str,
) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
    let operation: DurableStoreDeviceExclusionOperation =
        serde_json::from_str(raw).map_err(|error| {
            DbError::context(
                format!(
                    "Store-device exclusion operation {operation_id} has invalid durable state"
                ),
                error,
            )
        })?;
    operation
        .validate()
        .map_err(store_device_exclusion_journal_error)?;
    if operation.operation_id() != operation_id {
        return Err(DbError::Message(format!(
            "Store-device exclusion operation key {operation_id} differs from its signed object {}",
            operation.operation_id()
        )));
    }
    Ok(operation)
}

pub(crate) fn load_store_device_exclusion_on(
    conn: &Connection,
    operation_id: ObjectHash,
) -> Result<Option<DurableStoreDeviceExclusionOperation>, DbError> {
    conn.query_row(
        "SELECT state FROM outbound_store_device_exclusion WHERE operation_id = ?1",
        [operation_id.to_string()],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(DbError::from)?
    .map(|raw| parse_store_device_exclusion_operation(operation_id, &raw))
    .transpose()
}

pub(crate) fn load_active_store_device_exclusion_on(
    conn: &Connection,
) -> Result<Option<DurableStoreDeviceExclusionOperation>, DbError> {
    conn.query_row(
        "SELECT operation_id, state FROM outbound_store_device_exclusion WHERE active_key = 1",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )
    .optional()
    .map_err(DbError::from)?
    .map(|(raw_id, raw)| {
        let operation_id = raw_id
            .parse::<ObjectHash>()
            .map_err(|error| DbError::context("Store-device exclusion operation id", error))?;
        let operation = parse_store_device_exclusion_operation(operation_id, &raw)?;
        if operation.is_completed() {
            return Err(DbError::Message(
                "completed Store-device exclusion remains active".to_string(),
            ));
        }
        Ok(operation)
    })
    .transpose()
}

pub(crate) fn insert_store_device_exclusion_on(
    conn: &Connection,
    operation: &DurableStoreDeviceExclusionOperation,
    active: bool,
) -> Result<(), DbError> {
    operation
        .validate()
        .map_err(store_device_exclusion_journal_error)?;
    if active == operation.is_completed() {
        return Err(DbError::Message(
            "Store-device exclusion active marker differs from its closed state".to_string(),
        ));
    }
    let encoded = serde_json::to_string(operation)
        .map_err(|error| DbError::context("serialize Store-device exclusion operation", error))?;
    conn.execute(
        "INSERT INTO outbound_store_device_exclusion (operation_id, active_key, state)
         VALUES (?1, ?2, ?3)",
        rusqlite::params![
            operation.operation_id().to_string(),
            active.then_some(1_i64),
            encoded,
        ],
    )
    .map(|_| ())
    .map_err(DbError::from)
}

pub(crate) fn require_store_device_exclusion_transition_on(
    conn: &Connection,
    expected: &DurableStoreDeviceExclusionOperation,
    next: &DurableStoreDeviceExclusionOperation,
) -> Result<(), DbError> {
    if !expected.allows_transition_to(next) {
        return Err(DbError::Message(
            "invalid Store-device exclusion journal transition".to_string(),
        ));
    }
    let expected_state = serde_json::to_string(expected)
        .map_err(|error| DbError::context("serialize expected Store-device exclusion", error))?;
    let current = conn
        .query_row(
            "SELECT state FROM outbound_store_device_exclusion WHERE operation_id = ?1",
            [expected.operation_id().to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .ok_or_else(|| {
            DbError::Message("Store-device exclusion journal disappeared".to_string())
        })?;
    if current != expected_state {
        return Err(DbError::Message(
            "Store-device exclusion journal changed during transition".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn update_store_device_exclusion_on(
    conn: &Connection,
    expected: &DurableStoreDeviceExclusionOperation,
    next: &DurableStoreDeviceExclusionOperation,
    active: bool,
) -> Result<(), DbError> {
    require_store_device_exclusion_transition_on(conn, expected, next)?;
    if active == next.is_completed() {
        return Err(DbError::Message(
            "Store-device exclusion active marker differs from its next state".to_string(),
        ));
    }
    let expected_state = serde_json::to_string(expected)
        .map_err(|error| DbError::context("serialize expected Store-device exclusion", error))?;
    let next_state = serde_json::to_string(next)
        .map_err(|error| DbError::context("serialize next Store-device exclusion", error))?;
    let updated = conn
        .execute(
            "UPDATE outbound_store_device_exclusion
             SET active_key = ?3, state = ?4
             WHERE operation_id = ?1 AND state = ?2",
            rusqlite::params![
                expected.operation_id().to_string(),
                expected_state,
                active.then_some(1_i64),
                next_state,
            ],
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(
            "Store-device exclusion journal disappeared during transition".to_string(),
        ));
    }
    Ok(())
}

impl StoreSession<'_> {
    fn begin_outbound_store_device_exclusion(
        &mut self,
        operation: DurableStoreDeviceExclusionOperation,
        remotes: Vec<ClosedRemoteObject>,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        if let Some(active) = load_active_store_device_exclusion_on(&tx)? {
            if active.operation_id() != operation.operation_id() {
                return Err(DbError::Message(format!(
                    "Store-device exclusion operation {} remains active",
                    active.operation_id()
                )));
            }
            return Ok(active);
        }
        let operation_id = operation.operation_id();
        if let Some(existing) = load_store_device_exclusion_on(&tx, operation_id)? {
            if existing != operation || !existing.is_completed() {
                return Err(DbError::Message(format!(
                    "Store-device exclusion operation {operation_id} already has different durable state"
                )));
            }
            return Ok(existing);
        }
        for remote in &remotes {
            persist_exact_remote_object_on(
                &tx,
                self.store_dir,
                remote,
                "Store-device exclusion candidate object",
            )?;
        }
        insert_store_device_exclusion_on(&tx, &operation, true)?;
        tx.commit().map_err(DbError::from)?;
        Ok(operation)
    }

    fn active_outbound_store_device_exclusion(
        &mut self,
    ) -> Result<Option<DurableStoreDeviceExclusionOperation>, DbError> {
        load_active_store_device_exclusion_on(self.conn)
    }

    fn replace_outbound_store_device_exclusion_candidate(
        &mut self,
        expected: DurableStoreDeviceExclusionOperation,
        next: DurableStoreDeviceExclusionOperation,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
        let next_candidate = next.candidate().expect("validated candidate state");
        match (candidate.head_ref(), next_candidate.head_ref()) {
            (current, replacement) if current != replacement => {
                let (winner, prepared) = next_candidate.publication();
                replace_prepared_merge_head_remote_on(
                    &tx,
                    self.store_dir,
                    &current.object,
                    winner,
                    prepared,
                    &candidate.reference,
                )?;
            }
            _ => {}
        }
        update_store_device_exclusion_on(&tx, &expected, &next, true)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn complete_outbound_store_device_exclusion_activation(
        &mut self,
        expected: Box<DurableStoreDeviceExclusionOperation>,
        next: Box<DurableStoreDeviceExclusionOperation>,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        require_store_device_exclusion_transition_on(&tx, expected.as_ref(), next.as_ref())?;
        let candidate = expected
            .candidate()
            .expect("candidate-prepared exclusion has a candidate");
        let stream = candidate.reference.coord.stream_id.to_string();
        if crate::store::materialized_commit_index::materialized_commit_ref_on(
            &tx,
            &stream,
            candidate.reference.coord.sequence(),
        )? != Some(candidate.reference.clone())
        {
            return Err(DbError::Message(
                "Store-device exclusion completion is not materialized at its exact position"
                    .to_string(),
            ));
        }
        update_store_device_exclusion_on(&tx, expected.as_ref(), next.as_ref(), false)?;
        tx.commit().map_err(DbError::from)?;
        Ok(*next)
    }

    fn complete_outbound_store_device_exclusion_slot_loss(
        &mut self,
        expected: DurableStoreDeviceExclusionOperation,
        next: DurableStoreDeviceExclusionOperation,
        remotes: Vec<ClosedRemoteObject>,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
        for remote in &remotes {
            let object_id = remote.object_id();
            let current = load_remote_object_on(&tx, object_id)?;
            let unuploaded = matches!(
                &current,
                RemoteObjectRecord::CandidateCommit(record)
                    if matches!(record.state, coven_protocol::remote_object::CandidateCommitState::Prepared)
            ) || matches!(
                &current,
                RemoteObjectRecord::RetainedAuthority(record)
                    if matches!(
                        record.state,
                        coven_protocol::remote_object::RetainedAuthorityObjectState::Prepared { .. }
                    )
            );
            if current != **remote || !unuploaded {
                return Err(DbError::Message(format!(
                    "outcome-slot loss cannot discard uploaded exclusion object {object_id}"
                )));
            }
            if !crate::remote_object_records::delete_remote_object_on(&tx, object_id)? {
                return Err(DbError::Message(format!(
                    "unuploaded exclusion object {object_id} disappeared during slot resolution"
                )));
            }
        }
        update_store_device_exclusion_on(&tx, &expected, &next, false)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn begin_outbound_store_device_exclusion_nonactivation(
        &mut self,
        expected: DurableStoreDeviceExclusionOperation,
        next: DurableStoreDeviceExclusionOperation,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        nonactivation: coven_protocol::remote_object::CandidateNonactivation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
        let authority_id = remote_object_id(expected.object().object());
        if begin_remote_candidate_nonactivation_on(&tx, authority_id, nonactivation.clone())?
            .is_some()
        {
            return Err(DbError::Message(
                "uploaded exclusion authority became a deletion target".to_string(),
            ));
        }
        let head = candidate.head_ref();
        if begin_remote_candidate_nonactivation_on(
            &tx,
            remote_object_id(&head.object),
            nonactivation.clone(),
        )?
        .is_some()
        {
            return Err(DbError::Message(
                "Store-device exclusion activation head became a deletion target".to_string(),
            ));
        }
        if begin_remote_candidate_nonactivation_on(
            &tx,
            remote_object_id(&candidate.reference.object),
            nonactivation,
        )?
        .is_none()
        {
            return Err(DbError::Message(
                "losing Store-device exclusion commit has no deletion target".to_string(),
            ));
        }
        update_store_device_exclusion_on(&tx, &expected, &next, true)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn begin_outbound_store_device_exclusion_replacement(
        &mut self,
        expected: DurableStoreDeviceExclusionOperation,
        next: DurableStoreDeviceExclusionOperation,
        replacement_candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        losing_candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        authority_id: ObjectHash,
        replacement_remotes: Vec<ClosedRemoteObject>,
        nonactivation: coven_protocol::remote_object::CandidateNonactivation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
        for remote in replacement_remotes
            .iter()
            .filter(|remote| remote.object_id() != authority_id)
        {
            persist_exact_remote_object_on(
                &tx,
                self.store_dir,
                remote,
                "replacement Store-device exclusion candidate object",
            )?;
        }
        let mut authority = load_remote_object_on(&tx, authority_id)?;
        authority
            .add_retained_authority_candidate(replacement_candidate.reference.clone())
            .map_err(|error| {
                DbError::context("attach replacement exclusion candidate authority", error)
            })?;
        update_remote_object_on(&tx, authority_id, &authority)?;
        if begin_remote_candidate_nonactivation_on(&tx, authority_id, nonactivation.clone())?
            .is_some()
        {
            return Err(DbError::Message(
                "reusable exclusion outcome became a deletion target".to_string(),
            ));
        }
        let head = losing_candidate.head_ref();
        if begin_remote_candidate_nonactivation_on(
            &tx,
            remote_object_id(&head.object),
            nonactivation.clone(),
        )?
        .is_some()
        {
            return Err(DbError::Message(
                "losing exclusion activation head became a deletion target".to_string(),
            ));
        }
        if begin_remote_candidate_nonactivation_on(
            &tx,
            remote_object_id(&losing_candidate.reference.object),
            nonactivation,
        )?
        .is_none()
        {
            return Err(DbError::Message(
                "losing exclusion candidate has no exact deletion target".to_string(),
            ));
        }
        update_store_device_exclusion_on(&tx, &expected, &next, true)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn nonactivating_store_device_exclusion_cleanup_targets(
        &mut self,
        expected: &DurableStoreDeviceExclusionOperation,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        let conn = self.conn;
        let current =
            load_store_device_exclusion_on(conn, expected.operation_id())?.ok_or_else(|| {
                DbError::Message("Store-device exclusion journal is absent".to_string())
            })?;
        if current != *expected {
            return Err(DbError::Message(
                "Store-device exclusion is not awaiting candidate cleanup".to_string(),
            ));
        }
        let candidate = match &current {
            DurableStoreDeviceExclusionOperation::CandidateNonactivating { candidate, .. } => {
                candidate
            }
            DurableStoreDeviceExclusionOperation::ReplacingCandidate { losing, .. } => {
                &losing.candidate
            }
            _ => {
                return Err(DbError::Message(
                    "Store-device exclusion is not awaiting candidate cleanup".to_string(),
                ));
            }
        };
        super::candidate_records::candidate_cleanup_targets_on(
            conn,
            &candidate.reference,
            std::slice::from_ref(&candidate.reference.object),
        )
    }

    fn complete_store_device_exclusion_replacement_cleanup(
        &mut self,
        expected: DurableStoreDeviceExclusionOperation,
        next: DurableStoreDeviceExclusionOperation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let DurableStoreDeviceExclusionOperation::ReplacingCandidate { object, losing, .. } =
            &expected
        else {
            unreachable!("validated replacement state")
        };
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
        let commit_id = remote_object_id(&losing.candidate.reference.object);
        let head = losing.candidate.head_ref();
        let head_id = remote_object_id(&head.object);
        super::candidate_records::require_candidate_cleanup_complete_on(
            &tx,
            &losing.candidate.reference,
            &[
                losing.candidate.reference.object.clone(),
                object.object().clone(),
                head.object.clone(),
            ],
            "replaced exclusion cleanup is incomplete",
        )?;
        let commit = load_remote_object_on(&tx, commit_id)?;
        if commit
            .candidate_nonactivation_proof(&losing.candidate.reference)
            .map_err(|error| DbError::Message(error.to_string()))?
            != Some(&losing.proof)
        {
            return Err(DbError::Message(
                "replaced exclusion commit lacks complete nonactivation evidence".to_string(),
            ));
        }
        let authority = load_remote_object_on(&tx, remote_object_id(object.object()))?;
        if authority
            .candidate_nonactivation_proof(&losing.candidate.reference)
            .map_err(|error| DbError::Message(error.to_string()))?
            != Some(&losing.proof)
        {
            return Err(DbError::Message(
                "reusable exclusion outcome lacks its former candidate proof".to_string(),
            ));
        }
        let mut removable = vec![commit_id];
        let remote = load_remote_object_on(&tx, head_id)?;
        if remote
            .candidate_nonactivation_proof(&losing.candidate.reference)
            .map_err(|error| DbError::Message(error.to_string()))?
            != Some(&losing.proof)
        {
            return Err(DbError::Message(
                "replaced exclusion head lacks complete nonactivation evidence".to_string(),
            ));
        }
        removable.push(head_id);
        super::candidate_records::delete_remote_objects_on(&tx, removable, "replaced exclusion")?;
        update_store_device_exclusion_on(&tx, &expected, &next, true)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn complete_nonactivating_store_device_exclusion(
        &mut self,
        expected: DurableStoreDeviceExclusionOperation,
        next: DurableStoreDeviceExclusionOperation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let DurableStoreDeviceExclusionOperation::CandidateNonactivating {
            object,
            candidate,
            proof,
        } = &expected
        else {
            unreachable!("validated nonactivating state")
        };
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
        let commit_id = remote_object_id(&candidate.reference.object);
        let head = candidate.head_ref();
        let head_id = remote_object_id(&head.object);
        super::candidate_records::require_candidate_cleanup_complete_on(
            &tx,
            &candidate.reference,
            &[candidate.reference.object.clone(), head.object.clone()],
            "nonactivating exclusion cleanup is incomplete",
        )?;
        let commit = load_remote_object_on(&tx, commit_id)?;
        if commit
            .candidate_nonactivation_proof(&candidate.reference)
            .map_err(|error| DbError::Message(error.to_string()))?
            != Some(proof)
        {
            return Err(DbError::Message(
                "losing exclusion commit lacks complete exact nonactivation evidence".to_string(),
            ));
        }
        let inert = load_protocol_inert_object_on(&tx, remote_object_id(object.object()))?;
        if inert
            .candidate_nonactivation_proof(&candidate.reference)
            .map_err(|error| DbError::Message(error.to_string()))?
            != Some(proof)
        {
            return Err(DbError::Message(
                "protocol-inert exclusion object lacks its candidate proof".to_string(),
            ));
        }
        let mut removable = vec![commit_id];
        let head_remote = load_remote_object_on(&tx, head_id)?;
        if head_remote
            .candidate_nonactivation_proof(&candidate.reference)
            .map_err(|error| DbError::Message(error.to_string()))?
            != Some(proof)
        {
            return Err(DbError::Message(
                "losing exclusion head lacks complete nonactivation evidence".to_string(),
            ));
        }
        removable.push(head_id);
        super::candidate_records::delete_remote_objects_on(
            &tx,
            removable,
            "nonactivating exclusion",
        )?;
        update_store_device_exclusion_on(&tx, &expected, &next, false)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn mark_store_device_exclusion_authority_uploaded(
        &mut self,
        expected: ClosedRemoteObject,
        candidate: StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let conn = self.conn;
        let object_id = expected.object_id();
        let current = load_remote_object_on(conn, object_id)?;
        let (
            RemoteObjectRecord::RetainedAuthority(expected_record),
            RemoteObjectRecord::RetainedAuthority(current_record),
        ) = (expected.record(), &current)
        else {
            return Err(DbError::Message(
                "Store-device exclusion authority is not retained authority".to_string(),
            ));
        };
        if expected_record.identity != current_record.identity
            || expected_record.payloads != current_record.payloads
        {
            return Err(DbError::Message(
                "Store-device exclusion authority changed before upload completion".to_string(),
            ));
        }
        match &current_record.state {
            RetainedAuthorityObjectState::Prepared { ownership }
                if ownership.pending.contains(&candidate) =>
            {
                mark_remote_object_uploaded_on(conn, current)?;
            }
            RetainedAuthorityObjectState::UploadedVerified { ownership }
                if ownership.pending.contains(&candidate) => {}
            _ => {
                return Err(DbError::Message(
                    "Store-device exclusion authority does not belong to its current candidate"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn outbound_store_device_exclusion_operations(
        &mut self,
    ) -> Result<Vec<DurableStoreDeviceExclusionOperation>, DbError> {
        let conn = self.conn;
        let mut statement = conn
            .prepare(
                "SELECT operation_id, state
                 FROM outbound_store_device_exclusion
                 ORDER BY operation_id",
            )
            .map_err(DbError::from)?;
        let operations = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(DbError::from)?
            .map(|row| {
                let (raw_id, raw) = row.map_err(DbError::from)?;
                let operation_id = raw_id.parse::<ObjectHash>().map_err(|error| {
                    DbError::context("Store-device exclusion operation id", error)
                })?;
                parse_store_device_exclusion_operation(operation_id, &raw)
            })
            .collect();
        operations
    }
}

impl StoreDatabase {
    pub async fn begin_outbound_store_device_exclusion(
        &self,
        operation: DurableStoreDeviceExclusionOperation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        operation
            .validate()
            .map_err(store_device_exclusion_journal_error)?;
        if !matches!(
            operation,
            DurableStoreDeviceExclusionOperation::CandidatePrepared { .. }
        ) {
            return Err(DbError::Message(
                "a new Store-device exclusion journal must own its exact activation candidate"
                    .to_string(),
            ));
        }
        let remotes = operation
            .remote_objects()
            .map_err(store_device_exclusion_journal_error)?;
        Box::pin(self.connection.call_store(move |session| {
            session.begin_outbound_store_device_exclusion(operation, remotes)
        }))
        .await
    }

    pub async fn active_outbound_store_device_exclusion(
        &self,
    ) -> Result<Option<DurableStoreDeviceExclusionOperation>, DbError> {
        Box::pin(
            self.connection
                .call_store(|session| session.active_outbound_store_device_exclusion()),
        )
        .await
    }

    pub async fn replace_outbound_store_device_exclusion_candidate(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
        replacement: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let DurableStoreDeviceExclusionOperation::CandidatePrepared { object, candidate } =
            expected.clone()
        else {
            return Err(DbError::Message(
                "Store-device exclusion has no replaceable activation candidate".to_string(),
            ));
        };
        let next = DurableStoreDeviceExclusionOperation::CandidatePrepared {
            object,
            candidate: replacement,
        };
        next.validate()
            .map_err(store_device_exclusion_journal_error)?;
        if !expected.allows_transition_to(&next) {
            return Err(DbError::Message(
                "replacement Store-device exclusion candidate changes its signed commit"
                    .to_string(),
            ));
        }
        Box::pin(self.connection.call_store(move |session| {
            session.replace_outbound_store_device_exclusion_candidate(expected, next, candidate)
        }))
        .await
    }

    pub fn complete_outbound_store_device_exclusion_activation<'a>(
        &'a self,
        expected: DurableStoreDeviceExclusionOperation,
    ) -> Pin<
        Box<dyn Future<Output = Result<DurableStoreDeviceExclusionOperation, DbError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let next = match &expected {
                DurableStoreDeviceExclusionOperation::CandidatePrepared { object, candidate } => {
                    DurableStoreDeviceExclusionOperation::Completed(
                        StoreDeviceExclusionCompletion::Activated {
                            object: object.clone(),
                            candidate: candidate.clone(),
                        },
                    )
                }
                _ => {
                    return Err(DbError::Message(
                        "Store-device exclusion has no activated candidate".to_string(),
                    ));
                }
            };
            let expected = Box::new(expected);
            let next = Box::new(next);
            Box::pin(self.connection.call_store(move |session| {
                session.complete_outbound_store_device_exclusion_activation(expected, next)
            }))
            .await
        })
    }

    pub async fn complete_outbound_store_device_exclusion_slot_loss(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
        winner: DurableStoreDeviceExclusionObject,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let next = DurableStoreDeviceExclusionOperation::Completed(
            StoreDeviceExclusionCompletion::OutcomeSlotOccupied {
                intended: expected.object().clone(),
                winner,
            },
        );
        next.validate()
            .map_err(store_device_exclusion_journal_error)?;
        let remotes = expected
            .remote_objects()
            .map_err(store_device_exclusion_journal_error)?;
        Box::pin(self.connection.call_store(move |session| {
            session.complete_outbound_store_device_exclusion_slot_loss(expected, next, remotes)
        }))
        .await
    }

    pub async fn begin_outbound_store_device_exclusion_nonactivation(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let candidate = expected.candidate().cloned().ok_or_else(|| {
            DbError::Message("Store-device exclusion has no losing candidate".to_string())
        })?;
        if nonactivation
            .candidate_reference()
            .map_err(|error| DbError::Message(error.to_string()))?
            != candidate.reference
        {
            return Err(DbError::Message(
                "verified nonactivation names another Store-device exclusion candidate".to_string(),
            ));
        }
        let nonactivation = nonactivation.into_durable();
        let (next, nonactivation) = expected
            .begin_nonactivation(nonactivation)
            .map_err(store_device_exclusion_journal_error)?;
        if nonactivation.candidate().canonical_signed_bytes != candidate.commit.to_bytes() {
            return Err(DbError::Message(
                "verified nonactivation bytes differ from the Store-device exclusion candidate"
                    .to_string(),
            ));
        }
        Box::pin(self.connection.call_store(move |session| {
            session.begin_outbound_store_device_exclusion_nonactivation(
                expected,
                next,
                candidate,
                nonactivation,
            )
        }))
        .await
    }

    pub async fn begin_outbound_store_device_exclusion_replacement(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
        replacement: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let expected_candidate = expected.candidate().cloned().ok_or_else(|| {
            DbError::Message("Store-device exclusion has no losing candidate".to_string())
        })?;
        if nonactivation
            .candidate_reference()
            .map_err(|error| DbError::Message(error.to_string()))?
            != expected_candidate.reference
        {
            return Err(DbError::Message(
                "verified nonactivation names another Store-device exclusion candidate".to_string(),
            ));
        }
        let nonactivation = nonactivation.into_durable();
        let (next, nonactivation) = expected
            .begin_replacement(replacement, nonactivation)
            .map_err(store_device_exclusion_journal_error)?;
        if nonactivation.candidate().canonical_signed_bytes != expected_candidate.commit.to_bytes()
        {
            return Err(DbError::Message(
                "verified nonactivation bytes differ from the Store-device exclusion candidate"
                    .to_string(),
            ));
        }
        let DurableStoreDeviceExclusionOperation::ReplacingCandidate {
            candidate, losing, ..
        } = &next
        else {
            unreachable!("begin_replacement returns replacement state")
        };
        let replacement_candidate = candidate.clone();
        let losing_candidate = losing.candidate.clone();
        let authority_id = remote_object_id(expected.object().object());
        let replacement_remotes = DurableStoreDeviceExclusionOperation::CandidatePrepared {
            object: expected.object().clone(),
            candidate: replacement_candidate.clone(),
        }
        .remote_objects()
        .map_err(store_device_exclusion_journal_error)?;
        Box::pin(self.connection.call_store(move |session| {
            session.begin_outbound_store_device_exclusion_replacement(
                expected,
                next,
                replacement_candidate,
                losing_candidate,
                authority_id,
                replacement_remotes,
                nonactivation,
            )
        }))
        .await
    }

    pub async fn nonactivating_store_device_exclusion_cleanup_targets(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        Box::pin(self.connection.call_store(move |session| {
            session.nonactivating_store_device_exclusion_cleanup_targets(&expected)
        }))
        .await
    }

    pub async fn complete_store_device_exclusion_replacement_cleanup(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let DurableStoreDeviceExclusionOperation::ReplacingCandidate {
            object,
            candidate,
            losing: _,
        } = expected.clone()
        else {
            return Err(DbError::Message(
                "Store-device exclusion has no replacement cleanup".to_string(),
            ));
        };
        let next = DurableStoreDeviceExclusionOperation::CandidatePrepared {
            object: object.clone(),
            candidate,
        };
        next.validate()
            .map_err(store_device_exclusion_journal_error)?;
        Box::pin(self.connection.call_store(move |session| {
            session.complete_store_device_exclusion_replacement_cleanup(expected, next)
        }))
        .await
    }

    pub async fn complete_nonactivating_store_device_exclusion(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let DurableStoreDeviceExclusionOperation::CandidateNonactivating {
            object,
            candidate,
            proof,
        } = expected.clone()
        else {
            return Err(DbError::Message(
                "Store-device exclusion is not nonactivating".to_string(),
            ));
        };
        let next = DurableStoreDeviceExclusionOperation::Completed(
            StoreDeviceExclusionCompletion::CandidateNonactivated {
                object: object.clone(),
                candidate: candidate.clone(),
                proof: proof.clone(),
            },
        );
        next.validate()
            .map_err(store_device_exclusion_journal_error)?;
        Box::pin(self.connection.call_store(move |session| {
            session.complete_nonactivating_store_device_exclusion(expected, next)
        }))
        .await
    }

    pub async fn mark_store_device_exclusion_authority_uploaded(
        &self,
        operation: DurableStoreDeviceExclusionOperation,
    ) -> Result<(), DbError> {
        let expected = operation
            .authority_remote_object()
            .map_err(store_device_exclusion_journal_error)?;
        let candidate = operation
            .candidate()
            .ok_or_else(|| {
                DbError::Message(
                    "Store-device exclusion authority has no current candidate".to_string(),
                )
            })?
            .reference
            .clone();
        self.connection
            .call_store(move |session| {
                session.mark_store_device_exclusion_authority_uploaded(expected, candidate)
            })
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn outbound_store_device_exclusion_operations(
        &self,
    ) -> Result<Vec<DurableStoreDeviceExclusionOperation>, DbError> {
        Box::pin(
            self.connection
                .call_store(|session| session.outbound_store_device_exclusion_operations()),
        )
        .await
    }
}
