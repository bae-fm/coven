use super::*;
use crate::store::store_session::{active_store_publication, candidate_records, StoreTransaction};
use coven_protocol::membership::MembershipChain;
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::owner_promotion_journal::{
    OwnerPromotionFinalizationReceipt, OwnerPromotionJournal, OwnerPromotionJournalState,
    OwnerPromotionStaleEvidence,
};
use coven_protocol::remote_object::{remote_object_id, CandidateNonactivation};
use coven_protocol::store_commit::{
    OwnerPromotionStaleReason, StoreBatchCommitRef, StorePublicationRef,
};

fn require_journal_id_on(
    conn: &rusqlite::Connection,
    expected: &OwnerPromotionJournal,
) -> Result<String, DbError> {
    expected.validate_id(expected.promotion_id)?;
    let encoded = serde_json::to_string(expected)
        .map_err(|error| DbError::context("serialize promotion retirement", error))?;
    let key = format!("owner_promotion/{}", expected.promotion_id);
    if crate::required_protocol_state_on(conn, &key)? != encoded {
        return Err(DbError::Message(
            "promotion retirement lost its exact journal".into(),
        ));
    }
    Ok(encoded)
}

fn require_target_on(
    conn: &rusqlite::Connection,
    expected: &OwnerPromotionJournal,
    encoded: &str,
) -> Result<(), DbError> {
    if crate::required_protocol_state_on(conn, &expected.target_state_key()?)? != encoded {
        return Err(DbError::Message(
            "promotion retirement lost its current target attempt".into(),
        ));
    }
    Ok(())
}

fn retirement_parts(
    journal: &OwnerPromotionJournal,
) -> Result<(&CandidateNonactivation, Vec<ExactObjectRef>), DbError> {
    match &journal.state {
        OwnerPromotionJournalState::Nonactivated { nonactivation, .. } => Ok((
            nonactivation,
            vec![nonactivation.candidate().object.clone()],
        )),
        OwnerPromotionJournalState::Stale { evidence, .. } => match evidence.as_ref() {
            OwnerPromotionStaleEvidence::Candidate {
                nonactivation,
                receipt,
            } => Ok((
                nonactivation,
                receipt.publication.candidate_object_refs(
                    &receipt.candidate.commit,
                    &receipt.candidate.reference,
                )?,
            )),
            OwnerPromotionStaleEvidence::BeforePublication => Err(DbError::Message(
                "unprepared promotion has no candidate cleanup".into(),
            )),
        },
        _ => Err(DbError::Message(
            "promotion has no durable nonactivation decision".into(),
        )),
    }
}

fn require_reservation(
    active: &ActiveStorePublication,
    promotion: &OwnerPromotionJournal,
    candidate: &StoreBatchCommitRef,
) -> Result<(), DbError> {
    if active.owner() != &ActiveStorePublicationOwner::OwnerPromotion(promotion.promotion_id)
        || active.attempt()?.entry.payload
            != coven_protocol::store_commit::StorePublicationPayload::Commit(candidate.clone())
        || !active.retired_candidates().is_empty()
    {
        return Err(DbError::Message(
            "promotion retirement differs from its reserved candidate".into(),
        ));
    }
    Ok(())
}

fn pending_retirement_on(
    conn: &rusqlite::Connection,
    journal: &OwnerPromotionJournal,
) -> Result<Option<ActiveStorePublication>, DbError> {
    let encoded = require_journal_id_on(conn, journal)?;
    let (proof, _) = retirement_parts(journal)?;
    let Some(active) = active_store_publication::load_active_store_publication_on(conn)? else {
        return Ok(None);
    };
    if active.owner() != &ActiveStorePublicationOwner::OwnerPromotion(journal.promotion_id) {
        // A completed terminal journal survives subsequent independent operations.
        return Ok(None);
    }
    require_target_on(conn, journal, &encoded)?;
    require_reservation(&active, journal, &proof.reference()?)?;
    Ok(Some(active))
}

impl StoreSession<'_> {
    fn retire_owner_promotion_candidate_authority(
        &mut self,
        expected: OwnerPromotionJournal,
        membership: MembershipChain,
        publication: StorePublicationRef,
    ) -> Result<OwnerPromotionJournal, DbError> {
        let candidate = match &expected.state {
            OwnerPromotionJournalState::RequestPrepared { candidate, .. }
            | OwnerPromotionJournalState::MergeHeadPrepared { candidate, .. } => candidate,
            _ => {
                return Err(DbError::Message(
                    "promotion retirement requires an unaccepted prepared candidate".into(),
                ))
            }
        };
        let tx = self.conn.unchecked_transaction()?;
        let encoded = require_journal_id_on(&tx, &expected)?;
        require_target_on(&tx, &expected, &encoded)?;
        let active = active_store_publication::load_active_store_publication_on(&tx)?
            .ok_or_else(|| DbError::Message("promotion retirement lost its reservation".into()))?;
        require_reservation(&active, &expected, &candidate.reference)?;
        if active.commit_reservation()
            != Some((
                &candidate.commit.write_id,
                &candidate.commit.author_registration,
                &candidate.reference.coord,
            ))
        {
            return Err(DbError::Message(
                "promotion retirement differs from its logical write".into(),
            ));
        }
        crate::remote_object_records::validate_remote_object_on(
            &tx,
            remote_object_id(&candidate.reference.object),
            &candidate.reference.object,
            &candidate.commit.to_bytes(),
        )?;
        let nonactivation = StoreTransaction::new(&tx, self.store_dir)
            .candidate_grant_nonactivation(
                self.verified_store_authority,
                &membership,
                &candidate.reference,
                &candidate.commit,
                &publication,
            )?;
        let state = match &expected.state {
            OwnerPromotionJournalState::RequestPrepared { request, .. } => {
                OwnerPromotionJournalState::Nonactivated {
                    request: request.clone(),
                    nonactivation,
                }
            }
            OwnerPromotionJournalState::MergeHeadPrepared {
                acceptance,
                publication,
                candidate,
                ..
            } => OwnerPromotionJournalState::Stale {
                acceptance: acceptance.clone(),
                reason: OwnerPromotionStaleReason::MergeActivationRejected,
                evidence: Box::new(OwnerPromotionStaleEvidence::Candidate {
                    nonactivation,
                    receipt: Box::new(OwnerPromotionFinalizationReceipt {
                        candidate: candidate.clone(),
                        publication: publication.clone(),
                    }),
                }),
            },
            _ => unreachable!("prepared promotion was checked"),
        };
        let next = OwnerPromotionJournal {
            promotion_id: expected.promotion_id,
            target: expected.target.clone(),
            state,
        };
        let (previous, _) = expected.into_predecessor()?;
        let transition = previous.transition_to(&next)?;
        let (proof, objects) = retirement_parts(&next)?;
        candidate_records::begin_candidate_nonactivation_targets_on(
            &tx,
            &proof.reference()?,
            &objects,
            proof,
        )?;
        let (journal_key, target_key, before, after, _) = transition.into_values();
        replace_owner_promotion_journal_on(&tx, &journal_key, &target_key, &before, &after)?;
        tx.commit()?;
        Ok(next)
    }

    fn owner_promotion_retirement_targets(
        &self,
        expected: &OwnerPromotionJournal,
    ) -> Result<Vec<crate::CandidateCleanupObject>, DbError> {
        let Some(active) = pending_retirement_on(self.conn, expected)? else {
            return Ok(Vec::new());
        };
        let (proof, objects) = retirement_parts(expected)?;
        let mut targets = candidate_records::candidate_cleanup_targets_on(
            self.conn,
            &proof.reference()?,
            &objects,
        )?;
        targets.push(crate::CandidateCleanupObject {
            object: active.attempt()?.entry_object.clone(),
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

    fn complete_owner_promotion_retirement(
        &self,
        expected: OwnerPromotionJournal,
    ) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction()?;
        let Some(active) = pending_retirement_on(&tx, &expected)? else {
            return Ok(());
        };
        let (proof, objects) = retirement_parts(&expected)?;
        let candidate = proof.reference()?;
        let targets = candidate_records::candidate_cleanup_targets_on(&tx, &candidate, &objects)?;
        let mut removed = Vec::new();
        for target in targets {
            let id = remote_object_id(&target.object);
            let mut remote = crate::load_remote_object_on(&tx, id)?;
            remote.mark_absent_verified()?;
            crate::update_remote_object_on(&tx, id, &remote)?;
            removed.push(id);
        }
        candidate_records::require_candidate_cleanup_complete_on(
            &tx,
            &candidate,
            &objects,
            "promotion cleanup is incomplete",
        )?;
        candidate_records::delete_remote_objects_on(&tx, removed, "retired promotion")?;
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
    pub async fn retire_owner_promotion_candidate_authority(
        &self,
        expected: OwnerPromotionJournal,
        membership: MembershipChain,
        publication: StorePublicationRef,
    ) -> Result<OwnerPromotionJournal, DbError> {
        self.call_store(move |session| {
            session.retire_owner_promotion_candidate_authority(expected, membership, publication)
        })
        .await
    }

    pub async fn owner_promotion_retirement_targets(
        &self,
        expected: OwnerPromotionJournal,
    ) -> Result<(OwnerPromotionJournal, Vec<crate::CandidateCleanupObject>), DbError> {
        self.call_store(move |session| {
            let targets = session.owner_promotion_retirement_targets(&expected)?;
            Ok((expected, targets))
        })
        .await
    }

    pub async fn complete_owner_promotion_retirement(
        &self,
        expected: OwnerPromotionJournal,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.complete_owner_promotion_retirement(expected))
            .await
    }
}
