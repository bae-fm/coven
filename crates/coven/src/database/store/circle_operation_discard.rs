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
use super::StoreDatabase;
use crate::database::{
    candidate_graph_exact_objects, finish_remote_candidate_nonactivation_on,
    load_protocol_inert_object_on, load_remote_object_on, DurablePreparedProtocolObject,
    TerminalCandidateCleanupVerification,
};
use crate::protocol::circle::{CircleAccessDisposition, CircleOperationId};
use crate::protocol::circle_journal::{CircleOperationJournal, PreparedCircleOperation};
use crate::protocol::objects::ExactObjectRef;
use crate::protocol::remote_object::remote_object_id;
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

pub(crate) struct CircleOperationDiscardCandidate {
    pub(crate) candidate: crate::database::BlockedMergeCandidate,
    pub(crate) revoked_grant: Option<crate::protocol::membership::MembershipGrantId>,
}

fn circle_operation_candidate_on(
    conn: &Connection,
    operation: &PreparedCircleOperation,
) -> Result<CircleOperationCandidate, crate::DbError> {
    let commit = DurablePreparedProtocolObject::new(
        operation.commit_bytes.clone(),
        operation
            .prepared_objects
            .get("store-commit")
            .ok_or_else(|| {
                crate::DbError::Message(
                    "Circle operation lacks its prepared Store commit".to_string(),
                )
            })?
            .clone(),
    );
    let head = DurablePreparedProtocolObject::new(
        operation.policy.head.to_bytes(),
        operation
            .prepared_objects
            .get("store-head")
            .ok_or_else(|| {
                crate::DbError::Message(
                    "Circle operation lacks its prepared Store head".to_string(),
                )
            })?
            .clone(),
    );
    let candidate = parse_prepared_merge_candidate_parts_on(conn, &commit, &head)?;
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

impl StoreDatabase {
    /// The candidate a Circle operation would activate, plus the exact grant
    /// named by its durable authority-loss block when one exists.
    ///
    /// The candidate and block are read from one journal row in one database
    /// call, so discard never combines proof inputs from different operation
    /// states.
    pub(crate) async fn circle_operation_discard_candidate(
        &self,
        operation_id: &CircleOperationId,
    ) -> Result<CircleOperationDiscardCandidate, crate::DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call(move |conn| {
                let journal = load_discardable_operation_on(conn, &operation_id)?;
                let candidate = circle_operation_candidate_on(conn, journal.operation())?;
                let revoked_grant = match journal.state() {
                    crate::protocol::circle::CircleOperationState::Blocked {
                        block:
                            crate::protocol::circle::CircleOperationBlock::AuthorityLost { grant_id },
                    } => Some(grant_id),
                    // A lost position needs no grant: its proof is the direct
                    // observation of the winner that holds the head slot.
                    crate::protocol::circle::CircleOperationState::Blocked {
                        block: crate::protocol::circle::CircleOperationBlock::PositionLost { .. },
                    }
                    | crate::protocol::circle::CircleOperationState::Pending
                    | crate::protocol::circle::CircleOperationState::WaitingForCloseResponses
                    | crate::protocol::circle::CircleOperationState::Finalizing
                    | crate::protocol::circle::CircleOperationState::Discarding => None,
                };
                Ok(CircleOperationDiscardCandidate {
                    candidate: blocked_merge_candidate_from_prepared(candidate.candidate),
                    revoked_grant,
                })
            })
            .await
    }

    /// Record the verified nonactivation across the candidate's exact object
    /// graph — bootstrap blobs included — and move the journal row into the
    /// `Discarding` state in one transaction. Restart resumes cleanup from there.
    pub(crate) async fn begin_circle_operation_discard(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        operation_id: &CircleOperationId,
        nonactivation: crate::protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), crate::DbError> {
        let nonactivation = blocked_merge_candidate_nonactivation(nonactivation)?;
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(crate::DbError::from)?;
                let mut journal = load_discardable_operation_on(&tx, &operation_id)?;
                let CircleOperationCandidate {
                    candidate,
                    bootstrap_blobs,
                } = circle_operation_candidate_on(&tx, journal.operation())?;
                begin_blocked_merge_candidate_nonactivation_on(
                    &tx,
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
                super::circle_controls::persist_circle_operation_row_on(&tx, journal)?;
                tx.commit().map_err(crate::DbError::from)
            })
            .await
    }

    /// The terminal cleanup authorities the candidate carries — non-empty only
    /// for author-exclusion or membership-revocation proofs, whose head absence
    /// is reconciled against fresh evidence. A Merge-winner proof cleans by
    /// occupation and yields no terminal verification.
    pub(crate) async fn circle_operation_discard_terminal_verifications(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        operation_id: &CircleOperationId,
    ) -> Result<Vec<TerminalCandidateCleanupVerification>, crate::DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call(move |conn| {
                let journal = load_discarding_operation_on(conn, &operation_id)?;
                let candidate = circle_operation_candidate_on(conn, journal.operation())?;
                Ok(
                    terminal_candidate_verification_on(conn, &root, candidate.candidate)?
                        .into_iter()
                        .collect(),
                )
            })
            .await
    }

    /// Reconcile the candidate's activation head against fresh excluded-author
    /// evidence, mirroring the Merge terminal-head reconciliation but sourcing
    /// the candidate from the journal.
    pub(crate) async fn reconcile_circle_operation_terminal_head(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        operation_id: &CircleOperationId,
        verified: crate::protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), crate::DbError> {
        if !matches!(
            verified.proof(),
            crate::protocol::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                | crate::protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
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
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(crate::DbError::from)?;
                let journal = load_discarding_operation_on(&tx, &operation_id)?;
                let candidate = circle_operation_candidate_on(&tx, journal.operation())?.candidate;
                let reference = durable
                    .reference()
                    .map_err(|error| crate::DbError::Message(error.to_string()))?;
                if reference != candidate.reference {
                    return Err(crate::DbError::Message(
                        "fresh excluded-author head evidence names another candidate".to_string(),
                    ));
                }
                validate_terminal_candidate_authority_on(&tx, &root, &candidate, &durable)?;
                let object_id = remote_object_id(candidate.head_prepared.reference());
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
            })
            .await
    }

    /// The candidate-exclusive objects still present in cloud storage, ordered by
    /// the signed manifest. Shared bootstrap blobs never appear here — they are
    /// asserted cleanup-complete but retire through the shared path.
    pub(crate) async fn circle_operation_discard_cleanup_targets(
        &self,
        operation_id: &CircleOperationId,
    ) -> Result<Vec<CandidateCleanupObject>, crate::DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call(move |conn| {
                let journal = load_discarding_operation_on(conn, &operation_id)?;
                let CircleOperationCandidate {
                    candidate,
                    bootstrap_blobs,
                } = circle_operation_candidate_on(conn, journal.operation())?;
                merge_candidate_cleanup_targets_on(
                    conn,
                    &candidate.commit.write_id,
                    &candidate,
                    false,
                    &bootstrap_blobs,
                )
            })
            .await
    }

    /// Every Circle operation durably in the `Discarding` state, oldest first, so
    /// a restart resumes their interrupted cleanup.
    pub(crate) async fn discarding_circle_operations(
        &self,
    ) -> Result<Vec<CircleOperationId>, crate::DbError> {
        self.connection
            .call(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT operation_id, circle_id, payload
                         FROM circle_operations
                         ORDER BY rowid",
                    )
                    .map_err(crate::DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    })
                    .map_err(crate::DbError::from)?;
                let mut discarding = Vec::new();
                for row in rows {
                    let (operation_id, circle_id, payload) = row.map_err(crate::DbError::from)?;
                    let journal = crate::database::parse_circle_operation_row(
                        &operation_id,
                        &circle_id,
                        &payload,
                    )?;
                    if journal.is_discarding() {
                        discarding.push(journal.operation_id);
                    }
                }
                Ok(discarding)
            })
            .await
    }

    /// Complete the discard: assert every candidate object is terminal, delete
    /// the absence-verified candidate-exclusive remote-object rows, and clear the
    /// journal row — all in one transaction so no half-cleared row survives.
    pub(crate) async fn finish_circle_operation_discard(
        &self,
        operation_id: &CircleOperationId,
    ) -> Result<(), crate::DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(crate::DbError::from)?;
                let journal = load_discarding_operation_on(&tx, &operation_id)?;
                let CircleOperationCandidate {
                    candidate,
                    bootstrap_blobs,
                } = circle_operation_candidate_on(&tx, journal.operation())?;
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
                            crate::DbError::context(format!("finish Circle operation discard for {object_id}"), error)
                        })?
                    {
                        return Err(crate::DbError::Message(format!(
                            "Circle discard object {object_id} is not terminal"
                        )));
                    }
                    if matches!(
                        remote,
                        crate::protocol::remote_object::RemoteObjectRecord::CandidateCommit(
                            crate::protocol::remote_object::CandidateCommitRecord {
                                state:
                                    crate::protocol::remote_object::CandidateCommitState::AbsentVerified {
                                        ..
                                    },
                                ..
                            }
                        ) | crate::protocol::remote_object::RemoteObjectRecord::CandidateExclusive(
                            crate::protocol::remote_object::CandidateObjectRecord {
                                state:
                                    crate::protocol::remote_object::CandidateObjectState::AbsentVerified {
                                        ..
                                    },
                                ..
                            }
                        )
                    ) {
                        let removed = tx
                            .execute(
                                "DELETE FROM remote_objects WHERE object_id = ?1",
                                [object_id.to_string()],
                            )
                            .map_err(crate::DbError::from)?;
                        if removed != 1 {
                            return Err(crate::DbError::Message(format!(
                                "Circle discard object {object_id} disappeared during finalization"
                            )));
                        }
                    }
                }
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
            })
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
    let journal =
        crate::database::load_circle_operation_on(conn, operation_id)?.ok_or_else(|| {
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
    let journal =
        crate::database::load_circle_operation_on(conn, operation_id)?.ok_or_else(|| {
            crate::DbError::Message(format!("circle operation {operation_id} is absent"))
        })?;
    if !journal.is_discarding() {
        return Err(crate::DbError::Message(format!(
            "circle operation {operation_id} is not discarding"
        )));
    }
    Ok(journal)
}
