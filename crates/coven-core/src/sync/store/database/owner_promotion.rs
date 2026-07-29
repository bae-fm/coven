use std::collections::BTreeSet;

use crate::database::{persist_exact_remote_object_on, DbError};
use crate::sync::remote_object::RemoteObjectRecord;

use super::StoreDatabase;

impl StoreDatabase {
    pub(crate) async fn load_owner_promotion_journal(
        &self,
        promotion_id: crate::sync::store_commit::OwnerPromotionId,
    ) -> Result<Option<crate::sync::store::owner::owner_promotion::OwnerPromotionJournal>, DbError>
    {
        let key = format!("owner_promotion/{promotion_id}");
        self.database
            .call(move |conn| {
                crate::database::get_protocol_state_on(conn, &key)?
                    .map(|value| {
                        let journal: crate::sync::store::owner::owner_promotion::OwnerPromotionJournal =
                            serde_json::from_str(&value).map_err(|error| {
                                DbError::Message(format!("parse Owner-promotion journal: {error}"))
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

    pub(crate) async fn load_owner_promotion_target(
        &self,
        key: String,
    ) -> Result<Option<crate::sync::store::owner::owner_promotion::OwnerPromotionJournal>, DbError>
    {
        self.database
            .call(move |conn| {
                let value = crate::database::get_protocol_state_on(conn, &key)?;
                let Some(value) = value else {
                    return Ok(None);
                };
                let journal: crate::sync::store::owner::owner_promotion::OwnerPromotionJournal =
                    serde_json::from_str(&value).map_err(|error| {
                        DbError::Message(format!("parse Owner-promotion target journal: {error}"))
                    })?;
                journal
                    .validate_target_key(&key)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let journal_key = format!("owner_promotion/{}", journal.promotion_id());
                let by_id = crate::database::get_protocol_state_on(conn, &journal_key)?;
                if by_id.as_deref() != Some(value.as_str()) {
                    return Err(DbError::Message(
                        "Owner-promotion target and id journals disagree".to_string(),
                    ));
                }
                Ok(Some(journal))
            })
            .await
    }

    pub(crate) async fn begin_owner_promotion_journal(
        &self,
        target_key: String,
        journal: crate::sync::store::owner::owner_promotion::OwnerPromotionJournal,
    ) -> Result<crate::sync::store::owner::owner_promotion::OwnerPromotionJournal, DbError> {
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
        let value = serde_json::to_string(&journal).map_err(|error| {
            DbError::Message(format!("serialize Owner-promotion journal: {error}"))
        })?;
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                tx.execute(
                    "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (&journal_key, &value),
                )
                .map_err(DbError::from)?;
                tx.execute(
                    "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (&target_key, &value),
                )
                .map_err(DbError::from)?;
                let by_id = crate::database::required_protocol_state_on(&tx, &journal_key)?;
                let by_target = crate::database::required_protocol_state_on(&tx, &target_key)?;
                if by_id != by_target {
                    return Err(DbError::Message(
                        "Owner-promotion id and target journals disagree".to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)?;
                serde_json::from_str(&by_id).map_err(|error| {
                    DbError::Message(format!("parse begun Owner-promotion journal: {error}"))
                })
            })
            .await
    }

    pub(crate) async fn begin_owner_promotion_acceptance_journal(
        &self,
        journal: crate::sync::store::owner::owner_promotion::OwnerPromotionJournal,
    ) -> Result<crate::sync::store::owner::owner_promotion::OwnerPromotionJournal, DbError> {
        journal
            .validate_acceptance_begin()
            .map_err(|error| DbError::Message(error.to_string()))?;
        let journal_key = format!("owner_promotion/{}", journal.promotion_id());
        let value = serde_json::to_string(&journal).map_err(|error| {
            DbError::Message(format!(
                "serialize Owner-promotion candidate acceptance: {error}"
            ))
        })?;
        self.database
            .call(move |conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (&journal_key, &value),
                )
                .map_err(DbError::from)?;
                let actual = crate::database::required_protocol_state_on(conn, &journal_key)?;
                if actual != value {
                    return Err(DbError::Message(
                        "Owner-promotion id is already bound to different candidate acceptance"
                            .to_string(),
                    ));
                }
                serde_json::from_str(&actual).map_err(|error| {
                    DbError::Message(format!(
                        "parse begun Owner-promotion candidate acceptance: {error}"
                    ))
                })
            })
            .await
    }

    pub(crate) async fn advance_owner_promotion_journal(
        &self,
        transition: crate::sync::store::owner::owner_promotion::OwnerPromotionJournalTransition,
    ) -> Result<(), DbError> {
        let (journal_key, target_key, previous_value, next_value, remote_objects) =
            transition.into_values();
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                Self::advance_owner_promotion_journal_on(
                    &tx,
                    journal_key,
                    target_key,
                    previous_value,
                    next_value,
                    remote_objects,
                )?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    /// End a promotion whose Store candidate lost its stream position: record the
    /// nonactivation against every object that candidate published and advance the
    /// journal onto its stale successor in one transaction, returning the objects
    /// to delete. The candidate publishes its membership entry and head before the
    /// Store head that decides the position, so those sit in create-once slots the
    /// promoter's next attempt composes into; leaving them there would refuse every
    /// later membership publication on that stream.
    pub(crate) async fn end_nonactivated_owner_promotion_candidate(
        &self,
        transition: crate::sync::store::owner::owner_promotion::OwnerPromotionJournalTransition,
        candidate: crate::sync::store_commit::StoreBatchCommitRef,
        objects: Vec<crate::sync::storage::ExactObjectRef>,
        nonactivation: crate::sync::remote_object::VerifiedCandidateNonactivation,
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
        let (journal_key, target_key, previous_value, next_value, remote_objects) =
            transition.into_values();
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let cleanup = super::candidate_records::begin_candidate_nonactivation_targets_on(
                    &tx,
                    &candidate,
                    &objects,
                    &nonactivation,
                )?;
                Self::advance_owner_promotion_journal_on(
                    &tx,
                    journal_key,
                    target_key,
                    previous_value,
                    next_value,
                    remote_objects,
                )?;
                tx.commit().map_err(DbError::from)?;
                Ok(cleanup)
            })
            .await
    }

    /// The published objects of a promotion candidate that already lost, still
    /// awaiting deletion. An interrupted cleanup resumes through this: the stale
    /// journal names the candidate, and each object's durable state says whether it
    /// is still there.
    pub(crate) async fn owner_promotion_candidate_cleanup_targets(
        &self,
        candidate: crate::sync::store_commit::StoreBatchCommitRef,
        objects: Vec<crate::sync::storage::ExactObjectRef>,
    ) -> Result<Vec<super::candidate_records::CandidateCleanupObject>, DbError> {
        self.database
            .call(move |conn| {
                super::candidate_records::candidate_cleanup_targets_on(conn, &candidate, &objects)
            })
            .await
    }

    pub(crate) fn advance_owner_promotion_journal_on(
        tx: &rusqlite::Transaction<'_>,
        journal_key: String,
        target_key: String,
        previous_value: String,
        next_value: String,
        remote_objects: Vec<RemoteObjectRecord>,
    ) -> Result<(), DbError> {
        let mut object_ids = BTreeSet::new();
        for remote in &remote_objects {
            if !object_ids.insert(remote.object_id()) {
                return Err(DbError::Message(
                    "Owner-promotion journal repeats a remote object".to_string(),
                ));
            }
            persist_exact_remote_object_on(tx, remote, "Owner-promotion candidate object")?;
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

    pub(crate) async fn replace_failed_owner_promotion_journal(
        &self,
        previous: crate::sync::store::owner::owner_promotion::OwnerPromotionJournal,
        replacement: crate::sync::store::owner::owner_promotion::OwnerPromotionJournal,
    ) -> Result<crate::sync::store::owner::owner_promotion::OwnerPromotionJournal, DbError> {
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
        let previous_value = serde_json::to_string(&previous).map_err(|error| {
            DbError::Message(format!("serialize failed Owner-promotion journal: {error}"))
        })?;
        let replacement_value = serde_json::to_string(&replacement).map_err(|error| {
            DbError::Message(format!(
                "serialize replacement Owner-promotion journal: {error}"
            ))
        })?;
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
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
            })
            .await
    }
}
