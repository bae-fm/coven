//! Journal-sourced durable state for discarding a Circle operation whose
//! candidate has a verified permanent-nonactivation proof. The object-graph
//! machine is shared verbatim with Merge candidate abandonment
//! ([`candidate_records`]); only the durable home differs — a Merge candidate
//! lives in `store_writes.prepared`, a Circle operation in the
//! `circle_operations` journal — so these methods build the same
//! [`PreparedMergeCandidate`] from the journal payload and clear the journal row
//! (instead of the write row) when cleanup completes.

use super::candidate_records::{
    begin_blocked_merge_candidate_nonactivation_on, blocked_merge_candidate_from_prepared,
    blocked_merge_candidate_nonactivation, merge_candidate_cleanup_targets_on,
    parse_prepared_merge_candidate_parts_on, terminal_candidate_verification_on,
    validate_terminal_candidate_authority_on, CandidateCleanupObject, PreparedMergeCandidate,
};
use super::{StoreDatabase, StoreSession};
use crate::{
    candidate_graph_exact_objects, finish_remote_candidate_nonactivation_on,
    load_protocol_inert_object_on, load_remote_object_on, TerminalCandidateCleanupVerification,
};
use coven_protocol::circle::{CircleAccessDisposition, CircleOperationId};
use coven_protocol::circle_journal::{CircleOperationJournal, PreparedCircleOperation};
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::remote_object::remote_object_id;
use rusqlite::Connection;

/// The candidate a Circle operation would activate, plus the shared bootstrap
/// blob objects it owns. The blobs are ownership-tracked (`SharedLiveSet`), so
/// they ride the same nonactivation pass Merge indexed blobs take: the candidate
/// is removed from their ownership, and any that end up sole-owned retire by the
/// shared path rather than as candidate-exclusive deletions.
struct CircleOperationCandidate {
    candidate: PreparedMergeCandidate,
    bootstrap_blobs: Vec<ExactObjectRef>,
}

pub struct CircleOperationDiscardCandidate {
    pub candidate: crate::BlockedMergeCandidate,
    pub revoked_grant: Option<coven_protocol::membership::MembershipGrantId>,
}

fn circle_operation_candidate_on(
    records: crate::store::StoreRecords<'_>,
    authority: &mut super::VerifiedStoreAuthority,
    operation: &PreparedCircleOperation,
) -> Result<CircleOperationCandidate, crate::DbError> {
    let commit_object = operation
        .prepared_objects
        .get("store-commit")
        .ok_or_else(|| {
            crate::DbError::Message("Circle operation lacks its prepared Store commit".to_string())
        })?;
    let head_object = operation
        .prepared_objects
        .get("store-head")
        .ok_or_else(|| {
            crate::DbError::Message("Circle operation lacks its prepared Store head".to_string())
        })?;
    let candidate = parse_prepared_merge_candidate_parts_on(
        records,
        authority,
        &operation.commit_bytes,
        commit_object,
        &operation.policy.head.to_bytes(),
        head_object,
    )?;
    if candidate.reference != operation.commit_ref {
        return Err(crate::DbError::Message(
            "Circle operation candidate differs from its durable commit reference".to_string(),
        ));
    }
    let mut bootstrap_blobs = Vec::new();
    for access in &operation.creation.access {
        if let CircleAccessDisposition::Active {
            bootstrap: Some(bootstrap),
            ..
        } = &access.leaf.value.disposition
        {
            for blob in &bootstrap.blobs {
                let stored = blob.stored().ok_or_else(|| {
                    crate::DbError::Message(
                        "Circle bootstrap row blob has no exact stored locator".to_string(),
                    )
                })?;
                bootstrap_blobs.push(stored.object().clone());
            }
        }
    }
    Ok(CircleOperationCandidate {
        candidate,
        bootstrap_blobs,
    })
}

impl StoreSession<'_> {
    fn circle_operation_discard_candidate(
        &mut self,
        operation_id: String,
    ) -> Result<CircleOperationDiscardCandidate, crate::DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let journal = load_discardable_operation_on(records.conn, &operation_id)?;
        let candidate = circle_operation_candidate_on(
            records,
            self.verified_store_authority,
            journal.operation(),
        )?;
        let revoked_grant = match journal.state() {
            coven_protocol::circle::CircleOperationState::Blocked {
                block: coven_protocol::circle::CircleOperationBlock::AuthorityLost { grant_id },
            } => Some(grant_id),
            coven_protocol::circle::CircleOperationState::Blocked {
                block: coven_protocol::circle::CircleOperationBlock::PositionLost { .. },
            }
            | coven_protocol::circle::CircleOperationState::Pending
            | coven_protocol::circle::CircleOperationState::WaitingForCloseResponses
            | coven_protocol::circle::CircleOperationState::Finalizing
            | coven_protocol::circle::CircleOperationState::Discarding => None,
        };
        Ok(CircleOperationDiscardCandidate {
            candidate: blocked_merge_candidate_from_prepared(candidate.candidate),
            revoked_grant,
        })
    }

    fn begin_circle_operation_discard(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        operation_id: String,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), crate::DbError> {
        let nonactivation = blocked_merge_candidate_nonactivation(nonactivation)?;
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(crate::DbError::from)?;
        let mut journal = load_discardable_operation_on(&tx, &operation_id)?;
        let CircleOperationCandidate {
            candidate,
            bootstrap_blobs,
        } = circle_operation_candidate_on(
            crate::store::StoreRecords::new(&tx, records.store_dir),
            self.verified_store_authority,
            journal.operation(),
        )?;
        begin_blocked_merge_candidate_nonactivation_on(
            crate::store::StoreRecordTransaction::new(&tx, records.store_dir),
            self.verified_store_authority,
            &root,
            &candidate.commit.write_id,
            &candidate,
            &nonactivation,
            false,
            &bootstrap_blobs,
        )?;
        journal
            .begin_discard()
            .map_err(|error| crate::DbError::Message(error.to_string()))?;
        super::circle_controls::update_circle_operation_phase_on(&tx, &journal)?;
        tx.commit().map_err(crate::DbError::from)
    }

    fn circle_operation_discard_terminal_verifications(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        operation_id: String,
    ) -> Result<Vec<TerminalCandidateCleanupVerification>, crate::DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let journal = load_discarding_operation_on(records.conn, &operation_id)?;
        let candidate = circle_operation_candidate_on(
            records,
            self.verified_store_authority,
            journal.operation(),
        )?;
        Ok(terminal_candidate_verification_on(
            records,
            self.verified_store_authority,
            &root,
            candidate.candidate,
        )?
        .into_iter()
        .collect())
    }

    fn reconcile_circle_operation_terminal_head(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        operation_id: String,
        durable: coven_protocol::remote_object::CandidateNonactivation,
        head_nonactivation: coven_protocol::remote_object::VerifiedCandidateHeadNonactivation,
    ) -> Result<(), crate::DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(crate::DbError::from)?;
        let journal = load_discarding_operation_on(&tx, &operation_id)?;
        let candidate = circle_operation_candidate_on(
            crate::store::StoreRecords::new(&tx, records.store_dir),
            self.verified_store_authority,
            journal.operation(),
        )?
        .candidate;
        let reference = durable
            .reference()
            .map_err(|error| crate::DbError::Message(error.to_string()))?;
        if reference != candidate.reference {
            return Err(crate::DbError::Message(
                "fresh excluded-author head evidence names another candidate".to_string(),
            ));
        }
        validate_terminal_candidate_authority_on(
            crate::store::StoreRecords::new(&tx, records.store_dir),
            self.verified_store_authority,
            &root,
            &candidate,
            &durable,
        )?;
        let object_id = remote_object_id(&candidate.head_object);
        let remote_exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(crate::DbError::from)?;
        if !remote_exists {
            let inert = load_protocol_inert_object_on(&tx, object_id)?;
            if inert
                .candidate_nonactivation_proof(&candidate.reference)
                .map_err(|error| crate::DbError::Message(error.to_string()))?
                != Some(durable.proof())
            {
                return Err(crate::DbError::Message(
                    "protocol-inert candidate head carries another proof".to_string(),
                ));
            }
            return tx.commit().map_err(crate::DbError::from);
        }
        let mut remote = load_remote_object_on(&tx, object_id)?;
        let inert = remote
            .begin_candidate_nonactivation_with_verified_head_nonactivation(
                durable,
                &head_nonactivation,
            )
            .map_err(|error| {
                crate::DbError::context(
                    format!("reconcile excluded-author head {object_id}"),
                    error,
                )
            })?;
        finish_remote_candidate_nonactivation_on(&tx, object_id, remote, inert)?;
        tx.commit().map_err(crate::DbError::from)
    }

    fn circle_operation_discard_cleanup_targets(
        &mut self,
        operation_id: String,
    ) -> Result<Vec<CandidateCleanupObject>, crate::DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let journal = load_discarding_operation_on(records.conn, &operation_id)?;
        let CircleOperationCandidate {
            candidate,
            bootstrap_blobs,
        } = circle_operation_candidate_on(
            records,
            self.verified_store_authority,
            journal.operation(),
        )?;
        merge_candidate_cleanup_targets_on(
            records.conn,
            &candidate.commit.write_id,
            &candidate,
            false,
            &bootstrap_blobs,
        )
    }

    fn discarding_circle_operations(&mut self) -> Result<Vec<CircleOperationId>, crate::DbError> {
        let conn = self.conn;
        crate::circle_operation_ids_in_phase_on(conn, |progress| {
            matches!(
                progress,
                coven_protocol::circle_journal::CircleOperationProgress::Discarding
            )
        })?
        .into_iter()
        .map(|operation_id| Ok(load_discarding_operation_on(conn, &operation_id)?.operation_id))
        .collect()
    }

    fn finish_circle_operation_discard(
        &mut self,
        operation_id: String,
    ) -> Result<(), crate::DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(crate::DbError::from)?;
        let journal = load_discarding_operation_on(&tx, &operation_id)?;
        let CircleOperationCandidate {
            candidate,
            bootstrap_blobs,
        } = circle_operation_candidate_on(
            crate::store::StoreRecords::new(&tx, records.store_dir),
            self.verified_store_authority,
            journal.operation(),
        )?;
        if !merge_candidate_cleanup_targets_on(
            &tx,
            &candidate.commit.write_id,
            &candidate,
            false,
            &bootstrap_blobs,
        )?
        .is_empty()
        {
            return Err(crate::DbError::Message(
                "Circle operation discard still has remote cleanup targets".to_string(),
            ));
        }
        let mut object_ids = candidate_graph_exact_objects(&candidate.commit)?
            .iter()
            .map(remote_object_id)
            .collect::<std::collections::BTreeSet<_>>();
        object_ids.insert(remote_object_id(&candidate.reference.object));
        for object_id in object_ids {
            let remote = load_remote_object_on(&tx, object_id)?;
            if !remote
                .candidate_cleanup_complete(&candidate.reference)
                .map_err(|error| {
                    crate::DbError::context(
                        format!("finish Circle operation discard for {object_id}"),
                        error,
                    )
                })?
            {
                return Err(crate::DbError::Message(format!(
                    "Circle discard object {object_id} is not terminal"
                )));
            }
            if matches!(
                remote,
                coven_protocol::remote_object::RemoteObjectRecord::CandidateCommit(
                    coven_protocol::remote_object::CandidateCommitRecord {
                        state:
                            coven_protocol::remote_object::CandidateCommitState::AbsentVerified { .. },
                        ..
                    }
                ) | coven_protocol::remote_object::RemoteObjectRecord::CandidateExclusive(
                    coven_protocol::remote_object::CandidateObjectRecord {
                        state:
                            coven_protocol::remote_object::CandidateObjectState::AbsentVerified { .. },
                        ..
                    }
                )
            ) && !crate::remote_object_records::delete_remote_object_on(&tx, object_id)?
            {
                return Err(crate::DbError::Message(format!(
                    "Circle discard object {object_id} disappeared during finalization"
                )));
            }
        }
        super::circle_controls::release_operation_payloads_on(&tx, &journal.operation_id)?;
        let deleted = tx
            .execute(
                "DELETE FROM circle_operations WHERE operation_id = ?1",
                [operation_id.as_str()],
            )
            .map_err(crate::DbError::from)?;
        if deleted != 1 {
            return Err(crate::DbError::Message(
                "discarded Circle operation disappeared during finalization".to_string(),
            ));
        }
        tx.commit().map_err(crate::DbError::from)
    }
}

impl StoreDatabase {
    /// The candidate a Circle operation would activate, plus the exact grant
    /// named by its durable authority-loss block when one exists.
    pub async fn circle_operation_discard_candidate(
        &self,
        operation_id: &CircleOperationId,
    ) -> Result<CircleOperationDiscardCandidate, crate::DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call_store(move |session| session.circle_operation_discard_candidate(operation_id))
            .await
    }

    /// Record verified nonactivation and move the journal into discarding in one
    /// transaction.
    pub async fn begin_circle_operation_discard(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        operation_id: &CircleOperationId,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), crate::DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call_store(move |session| {
                session.begin_circle_operation_discard(root, operation_id, nonactivation)
            })
            .await
    }

    /// Return the terminal authorities that require fresh head evidence.
    pub async fn circle_operation_discard_terminal_verifications(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        operation_id: &CircleOperationId,
    ) -> Result<Vec<TerminalCandidateCleanupVerification>, crate::DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call_store(move |session| {
                session.circle_operation_discard_terminal_verifications(root, operation_id)
            })
            .await
    }

    /// Reconcile an activation head against fresh excluded-author evidence.
    pub async fn reconcile_circle_operation_terminal_head(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        operation_id: &CircleOperationId,
        verified: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), crate::DbError> {
        if !matches!(
            verified.proof(),
            coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                | coven_protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
        ) {
            return Err(crate::DbError::Message(
                "terminal head reconciliation received another proof family".to_string(),
            ));
        }
        let (durable, head_nonactivation) = verified
            .into_terminal_head_nonactivation()
            .map_err(|error| crate::DbError::Message(error.to_string()))?;
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call_store(move |session| {
                session.reconcile_circle_operation_terminal_head(
                    root,
                    operation_id,
                    durable,
                    head_nonactivation,
                )
            })
            .await
    }

    /// Return candidate-exclusive cloud objects still awaiting cleanup.
    pub async fn circle_operation_discard_cleanup_targets(
        &self,
        operation_id: &CircleOperationId,
    ) -> Result<Vec<CandidateCleanupObject>, crate::DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call_store(move |session| {
                session.circle_operation_discard_cleanup_targets(operation_id)
            })
            .await
    }

    /// Return every Circle operation durably in the discarding state.
    pub async fn discarding_circle_operations(
        &self,
    ) -> Result<Vec<CircleOperationId>, crate::DbError> {
        self.connection
            .call_store(|session| session.discarding_circle_operations())
            .await
    }

    /// Assert terminal cleanup, remove terminal candidate rows, and clear the
    /// journal in one transaction.
    pub async fn finish_circle_operation_discard(
        &self,
        operation_id: &CircleOperationId,
    ) -> Result<(), crate::DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call_store(move |session| session.finish_circle_operation_discard(operation_id))
            .await
    }
}

/// A Circle operation whose candidate may still activate — a ready, blocked, or
/// finalization candidate. Refuses a row that already entered discard or waits on
/// close responses (its candidate already won its slot).
fn load_discardable_operation_on(
    conn: &Connection,
    operation_id: &str,
) -> Result<CircleOperationJournal, crate::DbError> {
    let journal = crate::load_circle_operation_on(conn, operation_id)?.ok_or_else(|| {
        crate::DbError::Message(format!("circle operation {operation_id} is absent"))
    })?;
    if journal.is_discarding() {
        return Err(crate::DbError::Message(format!(
            "circle operation {operation_id} is already discarding"
        )));
    }
    Ok(journal)
}

fn load_discarding_operation_on(
    conn: &Connection,
    operation_id: &str,
) -> Result<CircleOperationJournal, crate::DbError> {
    let journal = crate::load_circle_operation_on(conn, operation_id)?.ok_or_else(|| {
        crate::DbError::Message(format!("circle operation {operation_id} is absent"))
    })?;
    if !journal.is_discarding() {
        return Err(crate::DbError::Message(format!(
            "circle operation {operation_id} is not discarding"
        )));
    }
    Ok(journal)
}
