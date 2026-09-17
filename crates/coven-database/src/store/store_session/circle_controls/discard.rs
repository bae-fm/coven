use super::*;

/// What establishes that a discarded Circle candidate can never activate.
///
/// A candidate whose commit never reached the provider is settled by the
/// device's own durable record of its uploads; one whose coordinate is held by
/// a different accepted commit is settled by that commit; and one whose author
/// lost write authority is settled by the retirement the accepted membership
/// carries.
pub enum CircleDiscardGround {
    Unpublished,
    PositionTaken {
        publication: StorePublicationRef,
        coverage: coven_protocol::store_commit::CommitFrontier,
        accepted: coven_protocol::store_commit::StoreBatchCommitRef,
    },
    AuthorityRetirement {
        membership: MembershipChain,
        publication: StorePublicationRef,
    },
}

impl CircleDiscardGround {
    fn into_proof(self) -> coven_protocol::remote_object::CandidateNonactivationProof {
        use coven_protocol::remote_object::CandidateNonactivationProof as Proof;
        match self {
            Self::Unpublished => Proof::Unpublished,
            Self::PositionTaken {
                publication,
                coverage,
                accepted,
            } => Proof::PositionTaken {
                publication,
                coverage,
                accepted,
            },
            Self::AuthorityRetirement { .. } => {
                unreachable!("an authority retirement builds its proof from accepted membership")
            }
        }
    }
}

use crate::store::store_session::{active_store_publication, candidate_records, StoreTransaction};
use coven_protocol::membership::MembershipChain;
use coven_protocol::store_commit::StorePublicationRef;

impl StoreSession<'_> {
    fn begin_circle_operation_discard(
        &mut self,
        expected: CircleOperationJournal,
        ground: CircleDiscardGround,
    ) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction()?;
        let current = load_circle_operation_on(&tx, expected.operation_id.as_str())?
            .ok_or_else(|| DbError::Message("discarded Circle operation is absent".into()))?;
        if current != expected {
            return Err(DbError::Message(
                "Circle discard differs from its exact candidate".into(),
            ));
        }
        let nonactivation = match ground {
            CircleDiscardGround::AuthorityRetirement {
                membership,
                publication,
            } => StoreTransaction::new(&tx, self.store_dir).candidate_grant_nonactivation(
                self.verified_store_authority,
                &membership,
                expected.operation().commit_ref(),
                expected.operation().commit(),
                &publication,
            )?,
            ground => coven_protocol::remote_object::CandidateNonactivation::from_durable_parts(
                expected.operation().commit_ref(),
                expected.operation().commit(),
                ground.into_proof(),
            )
            .map_err(DbError::from)?,
        };
        circle_discard_reservation_on(&tx, &expected)?;
        circle_bootstrap_blob_releases_on(&tx, &expected)?;
        let mut discarding = expected;
        discarding.begin_discard()?;
        candidate_records::begin_candidate_nonactivation_targets_on(
            &tx,
            discarding.operation().commit_ref(),
            &discarding
                .candidate_owned_objects()?
                .into_iter()
                .collect::<Vec<_>>(),
            &nonactivation,
        )?;
        update_circle_operation_phase_on(&tx, &discarding)?;
        tx.commit()?;
        Ok(())
    }

    /// The commit this device has accepted at one author-stream coordinate, if
    /// any. A coordinate carries one accepted commit.
    fn accepted_commit_at(
        &self,
        coord: &coven_protocol::store_commit::StoreCommitCoord,
    ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, DbError> {
        crate::store::materialized_commit_index::materialized_commit_ref_on(
            self.conn,
            &coord.stream_id.to_string(),
            coord.sequence(),
        )
    }

    fn circle_operation_discard_targets(
        &self,
        expected: &CircleOperationJournal,
    ) -> Result<Vec<crate::CandidateCleanupObject>, DbError> {
        if !expected.is_discarding()
            || load_circle_operation_on(self.conn, expected.operation_id.as_str())?.as_ref()
                != Some(expected)
        {
            return Err(DbError::Message(
                "Circle discard journal changed before cleanup".into(),
            ));
        }
        let active = circle_discard_reservation_on(self.conn, expected)?;
        let mut targets = candidate_records::candidate_cleanup_targets_on(
            self.conn,
            expected.operation().commit_ref(),
            &expected
                .candidate_owned_objects()?
                .into_iter()
                .collect::<Vec<_>>(),
        )?;
        // The prepared publication entry is the journal's, whether or not the
        // reservation that would have published it is still held.
        targets.push(crate::CandidateCleanupObject {
            object: expected
                .operation()
                .store_commit
                .publication
                .entry_object
                .clone(),
        });
        if let Some(previous) = active
            .as_ref()
            .and_then(ActiveStorePublication::superseded_entry)
        {
            targets.push(crate::CandidateCleanupObject {
                object: previous.object.clone(),
            });
        }
        targets.sort_by(|a, b| a.object.cmp(&b.object));
        targets.dedup_by(|a, b| a.object == b.object);
        Ok(targets)
    }

    fn complete_circle_operation_discard(
        &self,
        expected: CircleOperationJournal,
    ) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction()?;
        self.circle_operation_discard_targets(&expected)?;
        let objects = expected
            .candidate_owned_objects()?
            .into_iter()
            .collect::<Vec<_>>();
        let targets = candidate_records::candidate_cleanup_targets_on(
            &tx,
            expected.operation().commit_ref(),
            &objects,
        )?;
        let mut removed = Vec::new();
        for target in targets {
            let id = remote_object_id(&target.object);
            let mut remote = load_remote_object_on(&tx, id)?;
            remote.mark_absent_verified()?;
            update_remote_object_on(&tx, id, &remote)?;
            removed.push(id);
        }
        candidate_records::require_candidate_cleanup_complete_on(
            &tx,
            expected.operation().commit_ref(),
            &objects,
            "Circle candidate cleanup is incomplete",
        )?;
        candidate_records::delete_remote_objects_on(&tx, removed, "discarded Circle candidate")?;
        for (id, record) in circle_bootstrap_blob_releases_on(&tx, &expected)? {
            update_remote_object_on(&tx, id, &record)?;
        }
        release_operation_payloads_on(&tx, &expected.operation_id)?;
        if tx.execute(
            "DELETE FROM circle_operations WHERE operation_id = ?1",
            [expected.operation_id.as_str()],
        )? != 1
        {
            return Err(DbError::Message(
                "Circle discard journal disappeared".into(),
            ));
        }
        if let Some(active) = circle_discard_reservation_on(&tx, &expected)? {
            let mut completed = active.clone();
            if completed.superseded_entry().is_some() {
                completed.complete_superseded_entry_cleanup()?;
                active_store_publication::update_active_store_publication_on(
                    &tx, &active, &completed,
                )?;
            }
            active_store_publication::clear_active_store_publication_on(&tx, &completed)?;
        }
        tx.commit()?;
        Ok(())
    }
}

impl StoreDatabase {
    /// The commit accepted at one author-stream coordinate, if this device has
    /// installed one.
    pub async fn accepted_commit_at(
        &self,
        coord: coven_protocol::store_commit::StoreCommitCoord,
    ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, DbError> {
        self.call_store(move |session| session.accepted_commit_at(&coord))
            .await
    }

    pub async fn begin_circle_operation_discard(
        &self,
        expected: CircleOperationJournal,
        ground: CircleDiscardGround,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.begin_circle_operation_discard(expected, ground))
            .await
    }

    pub async fn circle_operation_discard_targets(
        &self,
        expected: CircleOperationJournal,
    ) -> Result<Vec<crate::CandidateCleanupObject>, DbError> {
        self.call_store(move |session| session.circle_operation_discard_targets(&expected))
            .await
    }

    pub async fn complete_circle_operation_discard(
        &self,
        expected: CircleOperationJournal,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.complete_circle_operation_discard(expected))
            .await
    }
}

/// The publication reservation this discarded operation still holds, if any.
///
/// A refused operation released its reservation when the refusal was recorded,
/// so a discard of one finds none. The objects it would have named are still
/// named by the durable journal, which is where the cleanup reads them.
fn circle_discard_reservation_on(
    conn: &Connection,
    journal: &CircleOperationJournal,
) -> Result<Option<ActiveStorePublication>, DbError> {
    let Some(active) = active_store_publication::load_active_store_publication_on(conn)? else {
        return Ok(None);
    };
    let commit = journal.operation().commit();
    if active.owner() != &ActiveStorePublicationOwner::CircleOperation(journal.operation_id.clone())
        || active.commit_reservation()
            != Some((
                &commit.write_id,
                &commit.author_registration,
                &journal.operation().commit_ref().coord,
            ))
        || active.attempt()?.entry.payload
            != coven_protocol::store_commit::StorePublicationPayload::Commit(
                journal.operation().commit_ref().clone(),
            )
        || !active.retired_candidates().is_empty()
    {
        return Err(DbError::Message(
            "Circle discard differs from its reserved candidate".into(),
        ));
    }
    Ok(Some(active))
}

fn circle_bootstrap_blob_releases_on(
    conn: &Connection,
    journal: &CircleOperationJournal,
) -> Result<
    Vec<(
        coven_protocol::store_commit::ObjectHash,
        coven_protocol::remote_object::RemoteObjectRecord,
    )>,
    DbError,
> {
    use coven_protocol::remote_object::{
        OwnedObjectState, PendingCandidateRelease, RemoteObjectRecord,
    };

    journal.operation().bootstrap_blobs()?.into_iter().map(|(id, blob)| {
        let record = load_remote_object_on(conn, id)?;
        if record.object() != blob.object() {
            return Err(DbError::Message("Circle bootstrap blob differs from its retained exact object".into()));
        }
        let PendingCandidateRelease::Retained(record) = record.release_pending_candidate(journal.operation().commit_ref())? else {
            return Err(DbError::Message("Circle bootstrap blob lost its accepted ownership before discard".into()));
        };
        if !matches!(&record, RemoteObjectRecord::SharedLiveSet(shared)
            if matches!(&shared.state, OwnedObjectState::UploadedVerified { ownership } if !ownership.activated.is_empty())) {
            return Err(DbError::Message("Circle bootstrap blob has no accepted owner after discard".into()));
        }
        Ok((id, record))
    }).collect()
}
