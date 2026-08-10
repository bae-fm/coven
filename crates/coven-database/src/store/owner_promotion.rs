use std::collections::BTreeSet;

use crate::{persist_exact_remote_object_on, DbError};

use super::{StoreDatabase, StoreSession};

impl StoreSession<'_> {
    fn begin_owner_promotion_journal(
        &self,
        journal_key: &str,
        target_key: &str,
        value: &str,
    ) -> Result<coven_protocol::owner_promotion_journal::OwnerPromotionJournal, DbError> {
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        tx.execute(
            "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
            (journal_key, value),
        )
        .map_err(DbError::from)?;
        tx.execute(
            "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
            (target_key, value),
        )
        .map_err(DbError::from)?;
        let by_id = crate::required_protocol_state_on(&tx, journal_key)?;
        let by_target = crate::required_protocol_state_on(&tx, target_key)?;
        if by_id != by_target {
            return Err(DbError::Message(
                "Owner-promotion id and target journals disagree".to_string(),
            ));
        }
        tx.commit().map_err(DbError::from)?;
        serde_json::from_str(&by_id)
            .map_err(|error| DbError::context("parse begun Owner-promotion journal", error))
    }

    fn begin_owner_promotion_acceptance_journal(
        &self,
        journal_key: &str,
        value: &str,
    ) -> Result<coven_protocol::owner_promotion_journal::OwnerPromotionJournal, DbError> {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                (journal_key, value),
            )
            .map_err(DbError::from)?;
        let actual = crate::required_protocol_state_on(self.conn, journal_key)?;
        if actual != value {
            return Err(DbError::Message(
                "Owner-promotion id is already bound to different candidate acceptance".to_string(),
            ));
        }
        serde_json::from_str(&actual).map_err(|error| {
            DbError::context("parse begun Owner-promotion candidate acceptance", error)
        })
    }

    fn advance_owner_promotion_journal(
        &self,
        transition: coven_protocol::owner_promotion_journal::OwnerPromotionJournalTransition,
    ) -> Result<(), DbError> {
        let (journal_key, target_key, previous_value, next_value, remote_objects) =
            transition.into_values();
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        crate::store::StoreRecordTransaction::new(&tx, self.store_dir)
            .advance_owner_promotion_journal(
                journal_key,
                target_key,
                previous_value,
                next_value,
                remote_objects,
            )?;
        tx.commit().map_err(DbError::from)
    }

    fn end_nonactivated_owner_promotion_candidate(
        &self,
        transition: coven_protocol::owner_promotion_journal::OwnerPromotionJournalTransition,
        candidate: coven_protocol::store_commit::StoreBatchCommitRef,
        objects: Vec<coven_protocol::objects::ExactObjectRef>,
        nonactivation: coven_protocol::remote_object::CandidateNonactivation,
    ) -> Result<Vec<super::candidate_records::CandidateCleanupObject>, DbError> {
        let (journal_key, target_key, previous_value, next_value, remote_objects) =
            transition.into_values();
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let cleanup = super::candidate_records::begin_candidate_nonactivation_targets_on(
            &tx,
            &candidate,
            &objects,
            &nonactivation,
        )?;
        crate::store::StoreRecordTransaction::new(&tx, self.store_dir)
            .advance_owner_promotion_journal(
                journal_key,
                target_key,
                previous_value,
                next_value,
                remote_objects,
            )?;
        tx.commit().map_err(DbError::from)?;
        Ok(cleanup)
    }

    fn owner_promotion_candidate_cleanup_targets(
        &self,
        candidate: &coven_protocol::store_commit::StoreBatchCommitRef,
        objects: &[coven_protocol::objects::ExactObjectRef],
    ) -> Result<Vec<super::candidate_records::CandidateCleanupObject>, DbError> {
        super::candidate_records::candidate_cleanup_targets_on(self.conn, candidate, objects)
    }

    fn replace_failed_owner_promotion_journal(
        &self,
        replacement: coven_protocol::owner_promotion_journal::OwnerPromotionJournal,
        target_key: String,
        replacement_key: String,
        previous_value: String,
        replacement_value: String,
    ) -> Result<coven_protocol::owner_promotion_journal::OwnerPromotionJournal, DbError> {
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let inserted = tx
            .execute(
                "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                (&replacement_key, &replacement_value),
            )
            .map_err(DbError::from)?;
        if inserted != 1 {
            return Err(DbError::Message(
                "fresh Owner-promotion retry identity is already present".to_string(),
            ));
        }
        let replaced = tx
            .execute(
                "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                (&replacement_value, &target_key, &previous_value),
            )
            .map_err(DbError::from)?;
        if replaced != 1 {
            return Err(DbError::Message(
                "Owner-promotion retry lost its exact failed target attempt".to_string(),
            ));
        }
        tx.commit().map_err(DbError::from)?;
        Ok(replacement)
    }
}

impl StoreDatabase {
    pub async fn load_owner_promotion_journal(
        &self,
        promotion_id: coven_protocol::store_commit::OwnerPromotionId,
    ) -> Result<Option<coven_protocol::owner_promotion_journal::OwnerPromotionJournal>, DbError>
    {
        let key = format!("owner_promotion/{promotion_id}");
        self.connection
            .call_store(move |session| {
                session
                    .protocol_state(&key)?
                    .map(|value| {
                        let journal: coven_protocol::owner_promotion_journal::OwnerPromotionJournal =
                            serde_json::from_str(&value).map_err(|error| {
                                DbError::context("parse Owner-promotion journal", error)
                            })?;
                        journal
                            .validate_id(promotion_id)
                            .map_err(|error| DbError::Message(error.to_string()))?;
                        Ok(journal)
                    })
                    .transpose()
            })
            .await
    }

    pub async fn load_owner_promotion_target(
        &self,
        key: String,
    ) -> Result<Option<coven_protocol::owner_promotion_journal::OwnerPromotionJournal>, DbError>
    {
        self.connection
            .call_store(move |session| {
                let value = session.protocol_state(&key)?;
                let Some(value) = value else {
                    return Ok(None);
                };
                let journal: coven_protocol::owner_promotion_journal::OwnerPromotionJournal =
                    serde_json::from_str(&value).map_err(|error| {
                        DbError::context("parse Owner-promotion target journal", error)
                    })?;
                journal
                    .validate_target_key(&key)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let journal_key = format!("owner_promotion/{}", journal.promotion_id());
                let by_id = session.protocol_state(&journal_key)?;
                if by_id.as_deref() != Some(value.as_str()) {
                    return Err(DbError::Message(
                        "Owner-promotion target and id journals disagree".to_string(),
                    ));
                }
                Ok(Some(journal))
            })
            .await
    }

    pub async fn begin_owner_promotion_journal(
        &self,
        target_key: String,
        journal: coven_protocol::owner_promotion_journal::OwnerPromotionJournal,
    ) -> Result<coven_protocol::owner_promotion_journal::OwnerPromotionJournal, DbError> {
        journal
            .validate_begin()
            .map_err(|error| DbError::Message(error.to_string()))?;
        if journal
            .target_state_key()
            .map_err(|error| DbError::Message(error.to_string()))?
            != target_key
        {
            return Err(DbError::Message(
                "Owner-promotion target index differs from its journal target".to_string(),
            ));
        }
        let journal_key = format!("owner_promotion/{}", journal.promotion_id());
        let value = serde_json::to_string(&journal)
            .map_err(|error| DbError::context("serialize Owner-promotion journal", error))?;
        self.connection
            .call_store(move |session| {
                session.begin_owner_promotion_journal(&journal_key, &target_key, &value)
            })
            .await
    }

    pub async fn begin_owner_promotion_acceptance_journal(
        &self,
        journal: coven_protocol::owner_promotion_journal::OwnerPromotionJournal,
    ) -> Result<coven_protocol::owner_promotion_journal::OwnerPromotionJournal, DbError> {
        journal
            .validate_acceptance_begin()
            .map_err(|error| DbError::Message(error.to_string()))?;
        let journal_key = format!("owner_promotion/{}", journal.promotion_id());
        let value = serde_json::to_string(&journal).map_err(|error| {
            DbError::context("serialize Owner-promotion candidate acceptance", error)
        })?;
        self.connection
            .call_store(move |session| {
                session.begin_owner_promotion_acceptance_journal(&journal_key, &value)
            })
            .await
    }

    pub async fn advance_owner_promotion_journal(
        &self,
        transition: coven_protocol::owner_promotion_journal::OwnerPromotionJournalTransition,
    ) -> Result<(), DbError> {
        self.connection
            .call_store(move |session| session.advance_owner_promotion_journal(transition))
            .await
    }

    /// End a promotion whose Store candidate lost its stream position: record the
    /// nonactivation against every object that candidate published and advance the
    /// journal onto its stale successor in one transaction, returning the objects
    /// to delete. The candidate publishes its membership entry and head before the
    /// Store head that decides the position, so those sit in create-once slots the
    /// promoter's next attempt composes into; leaving them there would refuse every
    /// later membership publication on that stream.
    pub async fn end_nonactivated_owner_promotion_candidate(
        &self,
        transition: coven_protocol::owner_promotion_journal::OwnerPromotionJournalTransition,
        candidate: coven_protocol::store_commit::StoreBatchCommitRef,
        objects: Vec<coven_protocol::objects::ExactObjectRef>,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<Vec<super::candidate_records::CandidateCleanupObject>, DbError> {
        if nonactivation
            .candidate_reference()
            .map_err(|error| DbError::Message(error.to_string()))?
            != candidate
        {
            return Err(DbError::Message(
                "verified nonactivation names another Owner-promotion candidate".to_string(),
            ));
        }
        let nonactivation = nonactivation.into_durable();
        self.connection
            .call_store(move |session| {
                session.end_nonactivated_owner_promotion_candidate(
                    transition,
                    candidate,
                    objects,
                    nonactivation,
                )
            })
            .await
    }

    /// The published objects of a promotion candidate that already lost, still
    /// awaiting deletion. An interrupted cleanup resumes through this: the stale
    /// journal names the candidate, and each object's durable state says whether it
    /// is still there.
    pub async fn owner_promotion_candidate_cleanup_targets(
        &self,
        candidate: coven_protocol::store_commit::StoreBatchCommitRef,
        objects: Vec<coven_protocol::objects::ExactObjectRef>,
    ) -> Result<Vec<super::candidate_records::CandidateCleanupObject>, DbError> {
        self.connection
            .call_store(move |session| {
                session.owner_promotion_candidate_cleanup_targets(&candidate, &objects)
            })
            .await
    }

    pub async fn replace_failed_owner_promotion_journal(
        &self,
        previous: coven_protocol::owner_promotion_journal::OwnerPromotionJournal,
        replacement: coven_protocol::owner_promotion_journal::OwnerPromotionJournal,
    ) -> Result<coven_protocol::owner_promotion_journal::OwnerPromotionJournal, DbError> {
        previous
            .validate_failed_attempt_replacement(&replacement)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let target_key = previous
            .target_state_key()
            .map_err(|error| DbError::Message(error.to_string()))?;
        if replacement
            .target_state_key()
            .map_err(|error| DbError::Message(error.to_string()))?
            != target_key
        {
            return Err(DbError::Message(
                "Owner-promotion retry target differs from its failed attempt".to_string(),
            ));
        }
        let replacement_key = format!("owner_promotion/{}", replacement.promotion_id());
        let previous_value = serde_json::to_string(&previous)
            .map_err(|error| DbError::context("serialize failed Owner-promotion journal", error))?;
        let replacement_value = serde_json::to_string(&replacement).map_err(|error| {
            DbError::context("serialize replacement Owner-promotion journal", error)
        })?;
        self.connection
            .call_store(move |session| {
                session.replace_failed_owner_promotion_journal(
                    replacement,
                    target_key,
                    replacement_key,
                    previous_value,
                    replacement_value,
                )
            })
            .await
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn advance_owner_promotion_journal_on(
    tx: &rusqlite::Transaction<'_>,
    store_dir: &coven_foundation::store_dir::StoreDir,
    journal_key: String,
    target_key: String,
    previous_value: String,
    next_value: String,
    remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
) -> Result<(), DbError> {
    let mut object_ids = BTreeSet::new();
    for remote in &remote_objects {
        if !object_ids.insert(remote.object_id()) {
            return Err(DbError::Message(
                "Owner-promotion journal repeats a remote object".to_string(),
            ));
        }
        persist_exact_remote_object_on(tx, store_dir, remote, "Owner-promotion candidate object")?;
    }
    let by_id = tx
        .execute(
            "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
            (&next_value, &journal_key, &previous_value),
        )
        .map_err(DbError::from)?;
    let by_target = tx
        .execute(
            "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
            (&next_value, &target_key, &previous_value),
        )
        .map_err(DbError::from)?;
    if by_id != 1 || by_target != 1 {
        return Err(DbError::Message(
            "Owner-promotion journal advance lost its exact predecessor".to_string(),
        ));
    }
    Ok(())
}
