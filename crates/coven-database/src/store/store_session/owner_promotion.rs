mod retirement;

use std::collections::BTreeSet;

use crate::{
    persist_exact_remote_object_on, ActiveStorePublication, ActiveStorePublicationOwner, DbError,
};

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
        accepted_request: Option<crate::AcceptedStoreCommitEvidence>,
    ) -> Result<(), DbError> {
        let (journal_key, target_key, previous_value, next_value, remote_objects) =
            transition.into_values();
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        advance_owner_promotion_journal_on(
            &tx,
            self.store_dir,
            journal_key,
            target_key,
            previous_value,
            next_value,
            remote_objects,
            accepted_request.as_ref(),
        )?;
        tx.commit().map_err(DbError::from)
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
        let previous: coven_protocol::owner_promotion_journal::OwnerPromotionJournal =
            serde_json::from_str(&previous_value)
                .map_err(|error| DbError::context("parse failed promotion", error))?;
        if super::active_store_publication::load_active_store_publication_on(&tx)?.is_some_and(
            |active| {
                active.owner()
                    == &ActiveStorePublicationOwner::OwnerPromotion(previous.promotion_id)
            },
        ) {
            return Err(DbError::Message(
                "failed promotion still owns candidate cleanup".into(),
            ));
        }
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
        self.call_store(move |session| {
            session
                .protocol_state(&key)?
                .map(|value| {
                    let journal: coven_protocol::owner_promotion_journal::OwnerPromotionJournal =
                        serde_json::from_str(&value).map_err(|error| {
                            DbError::context("parse Owner-promotion journal", error)
                        })?;
                    journal.validate_id(promotion_id).map_err(DbError::from)?;
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
        self.call_store(move |session| {
            let value = session.protocol_state(&key)?;
            let Some(value) = value else {
                return Ok(None);
            };
            let journal: coven_protocol::owner_promotion_journal::OwnerPromotionJournal =
                serde_json::from_str(&value).map_err(|error| {
                    DbError::context("parse Owner-promotion target journal", error)
                })?;
            journal.validate_target_key(&key).map_err(DbError::from)?;
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
        journal.validate_begin().map_err(DbError::from)?;
        if journal.target_state_key().map_err(DbError::from)? != target_key {
            return Err(DbError::Message(
                "Owner-promotion target index differs from its journal target".to_string(),
            ));
        }
        let journal_key = format!("owner_promotion/{}", journal.promotion_id());
        let value = serde_json::to_string(&journal)
            .map_err(|error| DbError::context("serialize Owner-promotion journal", error))?;
        self.call_store(move |session| {
            session.begin_owner_promotion_journal(&journal_key, &target_key, &value)
        })
        .await
    }

    pub async fn begin_owner_promotion_acceptance_journal(
        &self,
        journal: coven_protocol::owner_promotion_journal::OwnerPromotionJournal,
    ) -> Result<coven_protocol::owner_promotion_journal::OwnerPromotionJournal, DbError> {
        journal.validate_acceptance_begin().map_err(DbError::from)?;
        let journal_key = format!("owner_promotion/{}", journal.promotion_id());
        let value = serde_json::to_string(&journal).map_err(|error| {
            DbError::context("serialize Owner-promotion candidate acceptance", error)
        })?;
        self.call_store(move |session| {
            session.begin_owner_promotion_acceptance_journal(&journal_key, &value)
        })
        .await
    }

    pub async fn advance_owner_promotion_journal(
        &self,
        transition: coven_protocol::owner_promotion_journal::OwnerPromotionJournalTransition,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.advance_owner_promotion_journal(transition, None))
            .await
    }

    /// Record the winning request publication through the exact accepted
    /// capability returned by its publisher, including a replaced envelope.
    pub async fn advance_accepted_owner_promotion_request(
        &self,
        transition: coven_protocol::owner_promotion_journal::OwnerPromotionJournalTransition,
        acceptance: crate::AcceptedStoreCommitEvidence,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.advance_owner_promotion_journal(transition, Some(acceptance))
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
            .map_err(DbError::from)?;
        let target_key = previous.target_state_key().map_err(DbError::from)?;
        if replacement.target_state_key().map_err(DbError::from)? != target_key {
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
        self.call_store(move |session| {
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
    accepted_request: Option<&crate::AcceptedStoreCommitEvidence>,
) -> Result<(), DbError> {
    use coven_protocol::owner_promotion_journal::{
        OwnerPromotionJournal, OwnerPromotionJournalState,
    };

    let previous: OwnerPromotionJournal = serde_json::from_str(&previous_value)
        .map_err(|error| DbError::context("parse prior Owner-promotion journal", error))?;
    let next: OwnerPromotionJournal = serde_json::from_str(&next_value)
        .map_err(|error| DbError::context("parse successor Owner-promotion journal", error))?;
    if previous.promotion_id() != next.promotion_id() {
        return Err(DbError::Message(
            "Owner-promotion journal advance changes promotion identity".to_string(),
        ));
    }
    if matches!(&next.state, OwnerPromotionJournalState::Nonactivated { .. })
        || matches!(&next.state, OwnerPromotionJournalState::Stale { evidence, .. }
            if matches!(evidence.as_ref(), coven_protocol::owner_promotion_journal::OwnerPromotionStaleEvidence::Candidate { .. }))
    {
        return Err(DbError::Message(
            "promotion nonactivation requires accepted authority and owned cleanup".into(),
        ));
    }
    match (&previous.state, &next.state, accepted_request) {
        (
            OwnerPromotionJournalState::RequestPrepared { candidate, .. },
            OwnerPromotionJournalState::RequestAccepted { publication, .. },
            Some(accepted),
        ) => {
            let exact = accepted.exact_publication().ok_or_else(|| {
                DbError::Message(
                    "Owner-promotion request lacks its exact accepted publication receipt".into(),
                )
            })?;
            if accepted.commit_ref() != &candidate.reference
                || accepted.commit_ref() != &publication.value.commit
                || exact.reference() != &publication.value.publication
            {
                return Err(DbError::Message(
                    "Owner-promotion request differs from its accepted publication evidence".into(),
                ));
            }
        }
        (
            OwnerPromotionJournalState::RequestPrepared { .. },
            OwnerPromotionJournalState::RequestAccepted { .. },
            None,
        ) => {
            return Err(DbError::Message(
                "Owner-promotion request advancement requires accepted publication evidence".into(),
            ));
        }
        (_, _, Some(_)) => {
            return Err(DbError::Message(
                "accepted request evidence cannot authorize another promotion transition".into(),
            ));
        }
        (_, _, None) => {}
    }
    let previous_candidate = match &previous.state {
        OwnerPromotionJournalState::RequestPrepared { candidate, .. }
        | OwnerPromotionJournalState::RequestAccepted { candidate, .. }
        | OwnerPromotionJournalState::MergeHeadPrepared { candidate, .. } => {
            Some(candidate.as_ref())
        }
        _ => None,
    };
    let next_candidate = match &next.state {
        OwnerPromotionJournalState::RequestPrepared { candidate, .. }
        | OwnerPromotionJournalState::RequestAccepted { candidate, .. }
        | OwnerPromotionJournalState::MergeHeadPrepared { candidate, .. } => {
            Some(candidate.as_ref())
        }
        _ => None,
    };
    let owner = ActiveStorePublicationOwner::OwnerPromotion(next.promotion_id());
    if let Some(candidate) = next_candidate {
        let active = ActiveStorePublication::for_commit(owner.clone(), candidate)?;
        match super::active_store_publication::load_active_store_publication_on(tx)? {
            Some(existing)
                if previous_candidate.is_some() && existing.same_commit_reservation(&active) => {}
            Some(existing) => {
                return Err(DbError::Message(format!(
                    "Owner-promotion publication is occupied by {:?}",
                    existing.owner()
                )));
            }
            None if previous_candidate.is_none() => {
                let claim = super::active_store_publication::claim_active_store_publication_on(
                    tx, &active,
                )?;
                if claim != super::active_store_publication::ActiveStorePublicationClaim::Acquired {
                    return Err(DbError::Message(
                        "Owner-promotion publication changed during journal advance".to_string(),
                    ));
                }
            }
            None => {
                return Err(DbError::Message(
                    "prepared Owner-promotion journal lost its active publication".to_string(),
                ));
            }
        }
    }
    let mut object_ids = BTreeSet::new();
    for remote in &remote_objects {
        if !object_ids.insert(remote.object_id()) {
            return Err(DbError::Message(
                "Owner-promotion journal repeats a remote object".to_string(),
            ));
        }
        persist_exact_remote_object_on(tx, store_dir, remote, "Owner-promotion candidate object")?;
    }
    if let (
        OwnerPromotionJournalState::RequestAccepted {
            candidate,
            publication,
            ..
        },
        OwnerPromotionJournalState::AwaitingAcceptance { .. },
    ) = (&previous.state, &next.state)
    {
        let object_id = coven_protocol::remote_object::remote_object_id(&publication.object);
        let remote = crate::load_remote_object_on(tx, object_id)?;
        let expected = coven_protocol::remote_object::RemoteObjectRecord::prepared_owner_promotion_request_publication(
            publication,
            &candidate.commit,
        )?;
        if remote.object() != expected.object() || !remote.records_verified_upload() {
            return Err(DbError::Message(
                "Owner-promotion request result has not completed its exact upload".into(),
            ));
        }
        let remote = remote.into_activated(&candidate.reference)?;
        crate::update_remote_object_on(tx, object_id, &remote)?;
    }
    replace_owner_promotion_journal_on(
        tx,
        &journal_key,
        &target_key,
        &previous_value,
        &next_value,
    )?;
    if let Some(candidate) = previous_candidate.filter(|_| next_candidate.is_none()) {
        super::active_store_publication::clear_active_store_commit_for_owner_on(
            tx,
            &owner,
            &candidate.reference,
        )?;
    }
    Ok(())
}

fn replace_owner_promotion_journal_on(
    tx: &rusqlite::Transaction<'_>,
    journal_key: &str,
    target_key: &str,
    previous_value: &str,
    next_value: &str,
) -> Result<(), DbError> {
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
