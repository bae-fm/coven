use crate::database::remote_object_records::persist_exact_remote_object_on;

use super::*;

impl Database {
    pub(crate) async fn load_owner_promotion_journal(
        &self,
        promotion_id: crate::sync::store_commit::OwnerPromotionId,
    ) -> Result<Option<crate::sync::owner_promotion::OwnerPromotionJournal>, DbError> {
        let key = format!("owner_promotion/{promotion_id}");
        self.call(move |conn| {
            conn.query_row(
                "SELECT value FROM protocol_state WHERE key = ?1",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|value| {
                let journal: crate::sync::owner_promotion::OwnerPromotionJournal =
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
    ) -> Result<Option<crate::sync::owner_promotion::OwnerPromotionJournal>, DbError> {
        self.call(move |conn| {
            let value = conn
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [&key],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(DbError::from)?;
            let Some(value) = value else {
                return Ok(None);
            };
            let journal: crate::sync::owner_promotion::OwnerPromotionJournal =
                serde_json::from_str(&value).map_err(|error| {
                    DbError::Message(format!("parse Owner-promotion target journal: {error}"))
                })?;
            journal
                .validate_target_key(&key)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let journal_key = format!("owner_promotion/{}", journal.promotion_id());
            let by_id = conn
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [journal_key],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(DbError::from)?;
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
        journal: crate::sync::owner_promotion::OwnerPromotionJournal,
    ) -> Result<crate::sync::owner_promotion::OwnerPromotionJournal, DbError> {
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
        self.call(move |conn| {
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
            let by_id: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [&journal_key],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let by_target: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [&target_key],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
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
        journal: crate::sync::owner_promotion::OwnerPromotionJournal,
    ) -> Result<crate::sync::owner_promotion::OwnerPromotionJournal, DbError> {
        journal
            .validate_acceptance_begin()
            .map_err(|error| DbError::Message(error.to_string()))?;
        let journal_key = format!("owner_promotion/{}", journal.promotion_id());
        let value = serde_json::to_string(&journal).map_err(|error| {
            DbError::Message(format!(
                "serialize Owner-promotion candidate acceptance: {error}"
            ))
        })?;
        self.call(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                (&journal_key, &value),
            )
            .map_err(DbError::from)?;
            let actual: String = conn
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [&journal_key],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
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
        transition: crate::sync::owner_promotion::OwnerPromotionJournalTransition,
    ) -> Result<(), DbError> {
        let (journal_key, target_key, previous_value, next_value, remote_objects) =
            transition.into_values();
        self.call(move |conn| {
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
        previous: crate::sync::owner_promotion::OwnerPromotionJournal,
        replacement: crate::sync::owner_promotion::OwnerPromotionJournal,
    ) -> Result<crate::sync::owner_promotion::OwnerPromotionJournal, DbError> {
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
        self.call(move |conn| {
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
