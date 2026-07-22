use crate::database::remote_object_records::begin_remote_candidate_nonactivation_on;
use crate::database::remote_object_records::load_protocol_inert_object_on;
use crate::database::remote_object_records::load_remote_object_on;
use crate::database::remote_object_records::persist_exact_remote_object_on;
use crate::database::remote_object_records::replace_prepared_merge_head_remote_on;
use crate::database::remote_object_records::update_remote_object_on;
use crate::database::store_device_exclusion_records::insert_store_device_exclusion_on;
use crate::database::store_device_exclusion_records::load_active_store_device_exclusion_on;
use crate::database::store_device_exclusion_records::load_store_device_exclusion_on;
use crate::database::store_device_exclusion_records::parse_store_device_exclusion_operation;
use crate::database::store_device_exclusion_records::require_store_device_exclusion_transition_on;
use crate::database::store_device_exclusion_records::store_device_exclusion_journal_error;
use crate::database::store_device_exclusion_records::update_store_device_exclusion_on;

use super::*;

impl Database {
    pub(crate) async fn begin_outbound_store_device_exclusion(
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
        Box::pin(self.call(move |conn| {
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
                    remote,
                    "Store-device exclusion candidate object",
                )?;
            }
            insert_store_device_exclusion_on(&tx, &operation, true)?;
            tx.commit().map_err(DbError::from)?;
            Ok(operation)
        }))
        .await
    }

    pub(crate) async fn active_outbound_store_device_exclusion(
        &self,
    ) -> Result<Option<DurableStoreDeviceExclusionOperation>, DbError> {
        Box::pin(self.call(load_active_store_device_exclusion_on)).await
    }

    pub(crate) async fn outbound_store_device_exclusion_operations(
        &self,
    ) -> Result<Vec<DurableStoreDeviceExclusionOperation>, DbError> {
        Box::pin(self.call(|conn| {
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
                        DbError::Message(format!("Store-device exclusion operation id: {error}"))
                    })?;
                    parse_store_device_exclusion_operation(operation_id, &raw)
                })
                .collect();
            operations
        }))
        .await
    }

    pub(crate) async fn replace_outbound_store_device_exclusion_candidate(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
        replacement: crate::sync::store_engine::engine::operations::PreparedStoreOperationCommit,
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
        Box::pin(self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
            let next_candidate = next.candidate().expect("validated candidate state");
            match (candidate.head_ref(), next_candidate.head_ref()) {
                (current, replacement) if current != replacement => {
                    let (winner, prepared) = next_candidate.publication();
                    replace_prepared_merge_head_remote_on(
                        &tx,
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
        }))
        .await
    }

    pub(crate) fn complete_outbound_store_device_exclusion_activation<'a>(
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
            Box::pin(self.call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                require_store_device_exclusion_transition_on(
                    &tx,
                    expected.as_ref(),
                    next.as_ref(),
                )?;
                let candidate = expected
                    .candidate()
                    .expect("candidate-prepared exclusion has a candidate");
                let stream = candidate.reference.coord.stream_id.to_string();
                if Self::materialized_commit_ref_on(
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
            }))
            .await
        })
    }

    pub(crate) async fn complete_outbound_store_device_exclusion_slot_loss(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
        winner: crate::sync::store_device_exclusion::DurableStoreDeviceExclusionObject,
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
        Box::pin(self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
            for remote in &remotes {
                let object_id = remote.object_id();
                let current = load_remote_object_on(&tx, object_id)?;
                let unuploaded = matches!(
                    &current,
                    RemoteObjectRecord::CandidateCommit(record)
                        if matches!(record.state, crate::sync::remote_object::CandidateCommitState::Prepared)
                ) || matches!(
                    &current,
                    RemoteObjectRecord::RetainedAuthority(record)
                        if matches!(
                            record.state,
                            crate::sync::remote_object::RetainedAuthorityObjectState::Prepared { .. }
                        )
                );
                if current != *remote || !unuploaded {
                    return Err(DbError::Message(format!(
                        "outcome-slot loss cannot discard uploaded exclusion object {object_id}"
                    )));
                }
                if tx
                    .execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [object_id.to_string()],
                    )
                    .map_err(DbError::from)?
                    != 1
                {
                    return Err(DbError::Message(format!(
                        "unuploaded exclusion object {object_id} disappeared during slot resolution"
                    )));
                }
            }
            update_store_device_exclusion_on(&tx, &expected, &next, false)?;
            tx.commit().map_err(DbError::from)?;
            Ok(next)
        }))
        .await
    }

    pub(crate) async fn begin_outbound_store_device_exclusion_nonactivation(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
        nonactivation: crate::sync::remote_object::VerifiedCandidateNonactivation,
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
        Box::pin(self.call(move |conn| {
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
        }))
        .await
    }

    pub(crate) async fn begin_outbound_store_device_exclusion_replacement(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
        replacement: crate::sync::store_engine::engine::operations::PreparedStoreOperationCommit,
        nonactivation: crate::sync::remote_object::VerifiedCandidateNonactivation,
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
        Box::pin(self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
            for remote in replacement_remotes
                .iter()
                .filter(|remote| remote.object_id() != authority_id)
            {
                persist_exact_remote_object_on(
                    &tx,
                    remote,
                    "replacement Store-device exclusion candidate object",
                )?;
            }
            let mut authority = load_remote_object_on(&tx, authority_id)?;
            authority
                .add_retained_authority_candidate(replacement_candidate.reference.clone())
                .map_err(|error| {
                    DbError::Message(format!(
                        "attach replacement exclusion candidate authority: {error}"
                    ))
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
        }))
        .await
    }

    pub(crate) async fn nonactivating_store_device_exclusion_cleanup_targets(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        Box::pin(self.call(move |conn| {
            let current = load_store_device_exclusion_on(conn, expected.operation_id())?
                .ok_or_else(|| {
                    DbError::Message("Store-device exclusion journal is absent".to_string())
                })?;
            if current != expected {
                return Err(DbError::Message(
                    "Store-device exclusion is not awaiting candidate cleanup".to_string(),
                ));
            }
            let candidate = match &current {
                DurableStoreDeviceExclusionOperation::CandidateNonactivating {
                    candidate, ..
                } => candidate,
                DurableStoreDeviceExclusionOperation::ReplacingCandidate { losing, .. } => {
                    &losing.candidate
                }
                _ => {
                    return Err(DbError::Message(
                        "Store-device exclusion is not awaiting candidate cleanup".to_string(),
                    ));
                }
            };
            let commit =
                load_remote_object_on(conn, remote_object_id(&candidate.reference.object))?;
            if let Some(object) = commit.cleanup_target() {
                Ok(vec![CandidateCleanupObject {
                    object: object.clone(),
                }])
            } else if commit
                .candidate_cleanup_complete(&candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
            {
                Ok(Vec::new())
            } else {
                Err(DbError::Message(
                    "losing Store-device exclusion commit is not awaiting deletion".to_string(),
                ))
            }
        }))
        .await
    }

    pub(crate) async fn complete_store_device_exclusion_replacement_cleanup(
        &self,
        expected: DurableStoreDeviceExclusionOperation,
    ) -> Result<DurableStoreDeviceExclusionOperation, DbError> {
        let DurableStoreDeviceExclusionOperation::ReplacingCandidate {
            object,
            candidate,
            losing,
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
        Box::pin(self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
            let commit_id = remote_object_id(&losing.candidate.reference.object);
            let commit = load_remote_object_on(&tx, commit_id)?;
            if !commit
                .candidate_cleanup_complete(&losing.candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
                || commit
                    .candidate_nonactivation_proof(&losing.candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != Some(&losing.proof)
            {
                return Err(DbError::Message(
                    "replaced exclusion commit lacks complete nonactivation evidence".to_string(),
                ));
            }
            let authority = load_remote_object_on(&tx, remote_object_id(object.object()))?;
            if !authority
                .candidate_cleanup_complete(&losing.candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
                || authority
                    .candidate_nonactivation_proof(&losing.candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != Some(&losing.proof)
            {
                return Err(DbError::Message(
                    "reusable exclusion outcome lacks its former candidate proof".to_string(),
                ));
            }
            let mut removable = vec![commit_id];
            let head = losing.candidate.head_ref();
            let head_id = remote_object_id(&head.object);
            let remote = load_remote_object_on(&tx, head_id)?;
            if !remote
                .candidate_cleanup_complete(&losing.candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
                || remote
                    .candidate_nonactivation_proof(&losing.candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != Some(&losing.proof)
            {
                return Err(DbError::Message(
                    "replaced exclusion head lacks complete nonactivation evidence".to_string(),
                ));
            }
            removable.push(head_id);
            for object_id in removable {
                if tx
                    .execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [object_id.to_string()],
                    )
                    .map_err(DbError::from)?
                    != 1
                {
                    return Err(DbError::Message(format!(
                        "replaced exclusion object {object_id} disappeared during cleanup"
                    )));
                }
            }
            update_store_device_exclusion_on(&tx, &expected, &next, true)?;
            tx.commit().map_err(DbError::from)?;
            Ok(next)
        }))
        .await
    }

    pub(crate) async fn complete_nonactivating_store_device_exclusion(
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
        Box::pin(self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            require_store_device_exclusion_transition_on(&tx, &expected, &next)?;
            let commit_id = remote_object_id(&candidate.reference.object);
            let commit = load_remote_object_on(&tx, commit_id)?;
            if !commit
                .candidate_cleanup_complete(&candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
                || commit
                    .candidate_nonactivation_proof(&candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != Some(&proof)
            {
                return Err(DbError::Message(
                    "losing exclusion commit lacks complete exact nonactivation evidence"
                        .to_string(),
                ));
            }
            let inert = load_protocol_inert_object_on(&tx, remote_object_id(object.object()))?;
            if inert
                .candidate_nonactivation_proof(&candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
                != Some(&proof)
            {
                return Err(DbError::Message(
                    "protocol-inert exclusion object lacks its candidate proof".to_string(),
                ));
            }
            let mut removable = vec![commit_id];
            let head = candidate.head_ref();
            let head_id = remote_object_id(&head.object);
            let head_remote = load_remote_object_on(&tx, head_id)?;
            if !head_remote
                .candidate_cleanup_complete(&candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
                || head_remote
                    .candidate_nonactivation_proof(&candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != Some(&proof)
            {
                return Err(DbError::Message(
                    "losing exclusion head lacks complete nonactivation evidence".to_string(),
                ));
            }
            removable.push(head_id);
            for object_id in removable {
                if tx
                    .execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [object_id.to_string()],
                    )
                    .map_err(DbError::from)?
                    != 1
                {
                    return Err(DbError::Message(format!(
                        "nonactivating exclusion object {object_id} disappeared during completion"
                    )));
                }
            }
            update_store_device_exclusion_on(&tx, &expected, &next, false)?;
            tx.commit().map_err(DbError::from)?;
            Ok(next)
        }))
        .await
    }
}
