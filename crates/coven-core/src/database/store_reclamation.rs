use crate::database::remote_object_records::begin_remote_candidate_nonactivation_on;
use crate::database::remote_object_records::load_remote_object_on;
use crate::database::remote_object_records::persist_exact_remote_object_on;
use crate::database::remote_object_records::record_reclaimed_store_package_on;
use crate::database::remote_object_records::replace_prepared_merge_head_remote_on;
use crate::database::remote_object_records::update_remote_object_on;
use crate::database::store_reclaim_records::insert_store_reclaim_operation_on;
use crate::database::store_reclaim_records::load_store_reclaim_operation_on;
use crate::database::store_reclaim_records::parse_store_reclaim_operation;
use crate::database::store_reclaim_records::store_reclaim_journal_error;
use crate::database::store_reclaim_records::update_store_reclaim_operation_on;

use super::*;

impl Database {
    pub(crate) async fn begin_store_reclaim_operation(
        &self,
        operation: DurableStoreReclaimOperation,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        operation.validate().map_err(store_reclaim_journal_error)?;
        let DurableStoreReclaimOperation::AuthorizationCandidate { .. } = &operation else {
            return Err(DbError::Message(
                "a new Store reclaim operation must own an activation candidate".to_string(),
            ));
        };
        let remotes = match &operation {
            DurableStoreReclaimOperation::AuthorizationCandidate { object, candidate } => object
                .remote_objects(candidate)
                .map_err(store_reclaim_journal_error)?,
            _ => unreachable!("matched reclaim candidate"),
        };
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let operation_id = operation.operation_id();
            if let Some(existing) = load_store_reclaim_operation_on(&tx, operation_id)? {
                if existing != operation {
                    return Err(DbError::Message(format!(
                        "Store reclaim operation {operation_id} already has different durable state"
                    )));
                }
                return Ok(existing);
            }
            for remote in &remotes {
                persist_exact_remote_object_on(&tx, remote, "Store reclaim candidate object")?;
            }
            insert_store_reclaim_operation_on(&tx, &operation)?;
            tx.commit().map_err(DbError::from)?;
            Ok(operation)
        })
        .await
    }

    pub(crate) async fn store_package_is_retained_for_replay(
        &self,
        target: StorePackageRef,
        activation: StoreBatchCommitRef,
    ) -> Result<bool, DbError> {
        self.call(move |conn| {
            let object_id = remote_object_id(&target.object);
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                    [object_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if !exists {
                return Ok(false);
            }
            let remote = load_remote_object_on(conn, object_id)?;
            let retained = remote
                .store_package_is_retained_for_replay(&target, &activation)
                .map_err(|error| {
                    DbError::Message(format!(
                        "validate Store package {object_id} replay ownership: {error}"
                    ))
                })?;
            if !retained {
                return Ok(false);
            }
            for owner in remote.retained_replay_owners() {
                let RetainedReplayOwner::Commit { commit, input_hash } = owner;
                let StoreCommitCoord::MergeConcurrent {
                    stream_id,
                    sequence,
                } = &commit.coord
                else {
                    return Err(DbError::Message(format!(
                        "Store package {object_id} has a Serial retained-replay owner"
                    )));
                };
                Self::load_retained_merge_materialization_on(
                    conn,
                    &stream_id.to_string(),
                    *sequence,
                    commit,
                    &input_hash.to_string(),
                )?;
            }
            Ok(true)
        })
        .await
    }

    pub(crate) async fn store_reclaim_operations(
        &self,
    ) -> Result<Vec<DurableStoreReclaimOperation>, DbError> {
        self.call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT authorization_hash, state FROM store_reclaim_operations
                     ORDER BY authorization_hash",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            rows.into_iter()
                .map(|(raw_id, raw)| {
                    let id = raw_id.parse().map_err(|error| {
                        DbError::Message(format!("Store reclaim operation id: {error}"))
                    })?;
                    parse_store_reclaim_operation(id, &raw)
                })
                .collect()
        })
        .await
    }

    pub(crate) async fn begin_store_reclaim_receipt(
        &self,
        expected: DurableStoreReclaimOperation,
        object: crate::sync::store_reclaim_journal::DurableStoreReclaimObject,
        candidate: crate::sync::store_outbound::PreparedStoreOperationCommit,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let DurableStoreReclaimOperation::AbsentVerified {
            authorization,
            authorization_activation,
            ..
        } = &expected
        else {
            return Err(DbError::Message(
                "only an authorized reclaim can prepare a receipt".to_string(),
            ));
        };
        let next = DurableStoreReclaimOperation::ReceiptCandidate {
            authorization: authorization.clone(),
            authorization_activation: authorization_activation.clone(),
            object: Box::new(object),
            candidate: Box::new(candidate),
        };
        next.validate().map_err(store_reclaim_journal_error)?;
        let remotes = match &next {
            DurableStoreReclaimOperation::ReceiptCandidate {
                object, candidate, ..
            } => object
                .remote_objects(candidate)
                .map_err(store_reclaim_journal_error)?,
            _ => unreachable!("constructed receipt candidate"),
        };
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
                .ok_or_else(|| {
                    DbError::Message("Store reclaim operation disappeared".to_string())
                })?;
            if current != expected {
                return Err(DbError::Message(
                    "Store reclaim operation changed before receipt preparation".to_string(),
                ));
            }
            for remote in &remotes {
                persist_exact_remote_object_on(&tx, remote, "Store reclaim receipt candidate")?;
            }
            update_store_reclaim_operation_on(&tx, &expected, &next)?;
            tx.commit().map_err(DbError::from)?;
            Ok(next)
        })
        .await
    }

    pub(crate) async fn mark_store_reclaim_target_absent(
        &self,
        expected: DurableStoreReclaimOperation,
        target: crate::sync::store_commit::StorePackageRef,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let DurableStoreReclaimOperation::Authorized {
            authorization,
            activation,
        } = &expected
        else {
            return Err(DbError::Message(
                "only an authorized reclaim can record target absence".to_string(),
            ));
        };
        if &target != authorization.target() {
            return Err(DbError::Message(
                "verified reclaim target differs from its signed exact reference".to_string(),
            ));
        }
        let next = DurableStoreReclaimOperation::AbsentVerified {
            authorization: authorization.clone(),
            authorization_activation: activation.clone(),
            target,
        };
        let reclaimed =
            ReclaimedStorePackage::absent_verified(authorization.clone(), activation.clone())
                .map_err(store_reclaim_journal_error)?;
        next.validate().map_err(store_reclaim_journal_error)?;
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
                .ok_or_else(|| {
                    DbError::Message("Store reclaim operation disappeared".to_string())
                })?;
            if current != expected {
                return Err(DbError::Message(
                    "Store reclaim operation changed before absence recording".to_string(),
                ));
            }
            record_reclaimed_store_package_on(&tx, &reclaimed)?;
            update_store_reclaim_operation_on(&tx, &expected, &next)?;
            tx.commit().map_err(DbError::from)?;
            Ok(next)
        })
        .await
    }

    pub(crate) async fn replace_store_reclaim_candidate(
        &self,
        expected: DurableStoreReclaimOperation,
        replacement: crate::sync::store_outbound::PreparedStoreOperationCommit,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let current_candidate = expected.candidate().cloned().ok_or_else(|| {
            DbError::Message("Store reclaim state has no replaceable candidate".to_string())
        })?;
        if current_candidate.reference != replacement.reference
            || current_candidate.commit != replacement.commit
        {
            return Err(DbError::Message(
                "Store reclaim candidate replacement changes its signed commit".to_string(),
            ));
        }
        let next = match &expected {
            DurableStoreReclaimOperation::AuthorizationCandidate { object, .. } => {
                DurableStoreReclaimOperation::AuthorizationCandidate {
                    object: object.clone(),
                    candidate: Box::new(replacement),
                }
            }
            DurableStoreReclaimOperation::ReceiptCandidate {
                authorization,
                authorization_activation,
                object,
                ..
            } => DurableStoreReclaimOperation::ReceiptCandidate {
                authorization: authorization.clone(),
                authorization_activation: authorization_activation.clone(),
                object: object.clone(),
                candidate: Box::new(replacement),
            },
            _ => {
                return Err(DbError::Message(
                    "Store reclaim state has no replaceable candidate".to_string(),
                ));
            }
        };
        next.validate().map_err(store_reclaim_journal_error)?;
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
                .ok_or_else(|| {
                    DbError::Message("Store reclaim operation disappeared".to_string())
                })?;
            if current != expected {
                return Err(DbError::Message(
                    "Store reclaim operation changed before candidate replacement".to_string(),
                ));
            }
            let next_candidate = next.candidate().expect("constructed candidate state");
            match (
                current_candidate.merge_head_ref(),
                next_candidate.merge_head_ref(),
            ) {
                (Some(current), Some(replacement)) if current != replacement => {
                    let (winner, prepared) =
                        next_candidate.merge_publication().ok_or_else(|| {
                            DbError::Message(
                                "replacement Merge reclaim candidate has no signed head"
                                    .to_string(),
                            )
                        })?;
                    replace_prepared_merge_head_remote_on(
                        &tx,
                        &current.object,
                        winner,
                        prepared,
                        &current_candidate.reference,
                    )?;
                }
                (None, None) | (Some(_), Some(_)) => {}
                _ => {
                    return Err(DbError::Message(
                        "replacement reclaim candidate changes Store write policy".to_string(),
                    ));
                }
            }
            update_store_reclaim_operation_on(&tx, &expected, &next)?;
            tx.commit().map_err(DbError::from)?;
            Ok(next)
        })
        .await
    }

    pub(crate) async fn begin_store_reclaim_candidate_replacement(
        &self,
        expected: DurableStoreReclaimOperation,
        replacement: crate::sync::store_outbound::PreparedStoreOperationCommit,
        nonactivation: crate::sync::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let object = expected.object().cloned().ok_or_else(|| {
            DbError::Message("Store reclaim operation has no replaceable object".to_string())
        })?;
        let losing_candidate = expected.candidate().cloned().ok_or_else(|| {
            DbError::Message("Store reclaim operation has no losing candidate".to_string())
        })?;
        if nonactivation
            .candidate_reference()
            .map_err(|error| DbError::Message(error.to_string()))?
            != losing_candidate.reference
        {
            return Err(DbError::Message(
                "verified nonactivation names another Store reclaim candidate".to_string(),
            ));
        }
        let nonactivation = nonactivation.into_durable();
        let proof = nonactivation.proof().clone();
        let loss = crate::sync::store_reclaim_journal::StoreReclaimCandidateLoss {
            candidate: Box::new(losing_candidate.clone()),
            proof: proof.clone(),
        };
        let next = match &expected {
            DurableStoreReclaimOperation::AuthorizationCandidate { .. } => {
                DurableStoreReclaimOperation::AuthorizationReplacing {
                    object: Box::new(object.clone()),
                    candidate: Box::new(replacement.clone()),
                    losing: Box::new(loss),
                }
            }
            DurableStoreReclaimOperation::ReceiptCandidate {
                authorization,
                authorization_activation,
                ..
            } => DurableStoreReclaimOperation::ReceiptReplacing {
                authorization: authorization.clone(),
                authorization_activation: authorization_activation.clone(),
                object: Box::new(object.clone()),
                candidate: Box::new(replacement.clone()),
                losing: Box::new(loss),
            },
            _ => {
                return Err(DbError::Message(
                    "Store reclaim operation is not awaiting candidate publication".to_string(),
                ));
            }
        };
        next.validate().map_err(store_reclaim_journal_error)?;
        if nonactivation.candidate().canonical_signed_bytes != losing_candidate.commit.to_bytes() {
            return Err(DbError::Message(
                "verified nonactivation bytes differ from the Store reclaim candidate".to_string(),
            ));
        }
        let replacement_remotes = object
            .remote_objects(&replacement)
            .map_err(store_reclaim_journal_error)?;
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
                .ok_or_else(|| DbError::Message("Store reclaim operation disappeared".to_string()))?;
            if current != expected {
                return Err(DbError::Message(
                    "Store reclaim operation changed before candidate replacement".to_string(),
                ));
            }
            let authority_ids = replacement_remotes
                .iter()
                .filter(|remote| matches!(
                    remote,
                    RemoteObjectRecord::RetainedAuthority(record)
                        if matches!(
                            record.identity.domain,
                            crate::sync::remote_object::RetainedAuthorityObjectDomain::ReclaimEvidence { .. }
                                | crate::sync::remote_object::RetainedAuthorityObjectDomain::ReclaimAuthorization { .. }
                                | crate::sync::remote_object::RetainedAuthorityObjectDomain::ReclaimReceipt { .. }
                        )
                ))
                .map(RemoteObjectRecord::object_id)
                .collect::<BTreeSet<_>>();
            for remote in replacement_remotes
                .iter()
                .filter(|remote| !authority_ids.contains(&remote.object_id()))
            {
                persist_exact_remote_object_on(
                    &tx,
                    remote,
                    "replacement Store reclaim candidate object",
                )?;
            }
            for authority_id in authority_ids {
                let mut authority = load_remote_object_on(&tx, authority_id)?;
                authority
                    .add_retained_authority_candidate(replacement.reference.clone())
                    .map_err(|error| {
                        DbError::Message(format!(
                            "attach replacement reclaim authority candidate: {error}"
                        ))
                    })?;
                update_remote_object_on(&tx, authority_id, &authority)?;
                if begin_remote_candidate_nonactivation_on(
                    &tx,
                    authority_id,
                    nonactivation.clone(),
                )?
                .is_some()
                {
                    return Err(DbError::Message(
                        "reusable reclaim authority became a deletion target".to_string(),
                    ));
                }
            }
            if let Some(head) = losing_candidate.merge_head_ref() {
                if begin_remote_candidate_nonactivation_on(
                    &tx,
                    remote_object_id(&head.object),
                    nonactivation.clone(),
                )?
                .is_some()
                {
                    return Err(DbError::Message(
                        "losing reclaim activation head became a deletion target".to_string(),
                    ));
                }
            }
            if begin_remote_candidate_nonactivation_on(
                &tx,
                remote_object_id(&losing_candidate.reference.object),
                nonactivation,
            )?
            .is_none()
            {
                return Err(DbError::Message(
                    "losing reclaim commit has no exact deletion target".to_string(),
                ));
            }
            update_store_reclaim_operation_on(&tx, &expected, &next)?;
            tx.commit().map_err(DbError::from)?;
            Ok(next)
        })
        .await
    }

    pub(crate) async fn store_reclaim_replacement_cleanup_targets(
        &self,
        expected: DurableStoreReclaimOperation,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        self.call(move |conn| {
            let current = load_store_reclaim_operation_on(conn, expected.operation_id())?
                .ok_or_else(|| {
                    DbError::Message("Store reclaim operation disappeared".to_string())
                })?;
            if current != expected {
                return Err(DbError::Message(
                    "Store reclaim operation changed before cleanup".to_string(),
                ));
            }
            let losing = current.losing_candidate().ok_or_else(|| {
                DbError::Message("Store reclaim operation has no losing candidate".to_string())
            })?;
            let commit =
                load_remote_object_on(conn, remote_object_id(&losing.candidate.reference.object))?;
            if let Some(object) = commit.cleanup_target() {
                Ok(vec![CandidateCleanupObject {
                    object: object.clone(),
                }])
            } else if commit
                .candidate_cleanup_complete(&losing.candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
            {
                Ok(Vec::new())
            } else {
                Err(DbError::Message(
                    "losing reclaim commit is not awaiting exact cleanup".to_string(),
                ))
            }
        })
        .await
    }

    pub(crate) async fn complete_store_reclaim_candidate_replacement(
        &self,
        expected: DurableStoreReclaimOperation,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let losing = expected.losing_candidate().cloned().ok_or_else(|| {
            DbError::Message("Store reclaim operation has no replacement cleanup".to_string())
        })?;
        let next = match &expected {
            DurableStoreReclaimOperation::AuthorizationReplacing {
                object, candidate, ..
            } => DurableStoreReclaimOperation::AuthorizationCandidate {
                object: object.clone(),
                candidate: candidate.clone(),
            },
            DurableStoreReclaimOperation::ReceiptReplacing {
                authorization,
                authorization_activation,
                object,
                candidate,
                ..
            } => DurableStoreReclaimOperation::ReceiptCandidate {
                authorization: authorization.clone(),
                authorization_activation: authorization_activation.clone(),
                object: object.clone(),
                candidate: candidate.clone(),
            },
            _ => {
                return Err(DbError::Message(
                    "Store reclaim operation has no replacement cleanup".to_string(),
                ));
            }
        };
        next.validate().map_err(store_reclaim_journal_error)?;
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
                .ok_or_else(|| {
                    DbError::Message("Store reclaim operation disappeared".to_string())
                })?;
            if current != expected {
                return Err(DbError::Message(
                    "Store reclaim operation changed before replacement completion".to_string(),
                ));
            }
            let object_id = remote_object_id(&losing.candidate.reference.object);
            let commit = load_remote_object_on(&tx, object_id)?;
            if !commit
                .candidate_cleanup_complete(&losing.candidate.reference)
                .map_err(|error| DbError::Message(error.to_string()))?
            {
                return Err(DbError::Message(
                    "losing reclaim commit cleanup is incomplete".to_string(),
                ));
            }
            if tx
                .execute(
                    "DELETE FROM remote_objects WHERE object_id = ?1",
                    [object_id.to_string()],
                )
                .map_err(DbError::from)?
                != 1
            {
                return Err(DbError::Message(
                    "losing reclaim commit disappeared during completion".to_string(),
                ));
            }
            update_store_reclaim_operation_on(&tx, &expected, &next)?;
            tx.commit().map_err(DbError::from)?;
            Ok(next)
        })
        .await
    }
}
