use super::*;
use crate::store::store_session::{active_store_publication, candidate_records, StoreTransaction};
use coven_protocol::membership::MembershipChain;
use coven_protocol::store_commit::StorePublicationRef;

impl StoreSession<'_> {
    fn begin_circle_operation_discard(
        &mut self,
        expected: CircleOperationJournal,
        membership: MembershipChain,
        publication: StorePublicationRef,
    ) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction()?;
        let current = load_circle_operation_on(&tx, expected.operation_id.as_str())?
            .ok_or_else(|| DbError::Message("discarded Circle operation is absent".into()))?;
        if current != expected {
            return Err(DbError::Message(
                "Circle discard differs from its exact candidate".into(),
            ));
        }
        let nonactivation = StoreTransaction::new(&tx, self.store_dir)
            .candidate_grant_nonactivation(
                self.verified_store_authority,
                &membership,
                expected.operation().commit_ref(),
                expected.operation().commit(),
                &publication,
            )?;
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
        targets.push(crate::CandidateCleanupObject {
            object: active.attempt()?.reference()?.object,
        });
        if let Some(previous) = active.superseded_entry() {
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
        let active = circle_discard_reservation_on(&tx, &expected)?;
        let mut completed = active.clone();
        if completed.superseded_entry().is_some() {
            completed.complete_superseded_entry_cleanup()?;
            active_store_publication::update_active_store_publication_on(&tx, &active, &completed)?;
        }
        active_store_publication::clear_active_store_publication_on(&tx, &completed)?;
        tx.commit()?;
        Ok(())
    }
}

impl StoreDatabase {
    pub async fn begin_circle_operation_discard(
        &self,
        expected: CircleOperationJournal,
        membership: MembershipChain,
        publication: StorePublicationRef,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.begin_circle_operation_discard(expected, membership, publication)
        })
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

fn circle_discard_reservation_on(
    conn: &Connection,
    journal: &CircleOperationJournal,
) -> Result<ActiveStorePublication, DbError> {
    let active =
        active_store_publication::load_active_store_publication_on(conn)?.ok_or_else(|| {
            DbError::Message("Circle discard lost its publication reservation".into())
        })?;
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
    Ok(active)
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
