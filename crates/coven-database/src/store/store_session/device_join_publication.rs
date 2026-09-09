use super::{StoreDatabase, StoreSession};
use crate::{
    persist_exact_remote_object_on, ActiveStorePublication, ActiveStorePublicationOwner, DbError,
};
use coven_protocol::store_commit::device_join_journal::{
    DeviceJoinJournalRecord, DeviceJoinRole, DeviceJoinRoleProgress, OwnerJoinProgress,
    PreparedOwnerJoinPublication,
};

impl StoreSession<'_> {
    fn prepare_owner_device_join_publication(
        &mut self,
        previous: DeviceJoinJournalRecord,
        prepared: PreparedOwnerJoinPublication,
    ) -> Result<DeviceJoinJournalRecord, DbError> {
        if previous.progress.role() != DeviceJoinRole::Owner {
            return Err(DbError::Message(
                "device join publication belongs to a non-Owner journal".to_string(),
            ));
        }
        prepared
            .validate_for(previous.attempt_id)
            .map_err(|error| DbError::context("prepared owner device join publication", error))?;
        let next = DeviceJoinJournalRecord {
            attempt_id: previous.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::StorePublicationPrepared(prepared.clone()),
            )),
        };
        crate::store::device_join_journal::validate_successor(&previous, &next).map_err(
            |error| DbError::Message(format!("prepare owner device join publication: {error}")),
        )?;
        let active = ActiveStorePublication::for_commit(
            ActiveStorePublicationOwner::DeviceJoin(previous.attempt_id),
            &prepared.candidate,
        )?;
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let key = previous.store_key();
        let expected = serde_json::to_string(&previous)
            .map_err(|error| DbError::context("serialize device join predecessor", error))?;
        let actual = crate::required_protocol_state_on(&transaction, &key)?;
        if actual != expected {
            return Err(DbError::Message(
                "device join journal changed before publication preparation".to_string(),
            ));
        }
        match super::active_store_publication::claim_active_store_publication_on(
            &transaction,
            &active,
        )? {
            super::active_store_publication::ActiveStorePublicationClaim::Acquired => {}
            super::active_store_publication::ActiveStorePublicationClaim::AlreadyOwned => {
                return Err(DbError::Message(
                    "device join publication was active before its journal was prepared"
                        .to_string(),
                ));
            }
            super::active_store_publication::ActiveStorePublicationClaim::Occupied(owner) => {
                return Err(DbError::Message(format!(
                    "another local Store operation owns publication: {owner:?}"
                )));
            }
        }
        for remote in prepared
            .remote_objects(previous.attempt_id)
            .map_err(|error| DbError::context("prepare device join remote graph", error))?
        {
            persist_exact_remote_object_on(
                &transaction,
                self.store_dir,
                &remote,
                "device join candidate object",
            )?;
        }
        let next_value = serde_json::to_string(&next)
            .map_err(|error| DbError::context("serialize prepared device join", error))?;
        if transaction
            .execute(
                "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                (&next_value, &key, &expected),
            )
            .map_err(DbError::from)?
            != 1
        {
            return Err(DbError::Message(
                "device join journal changed during publication preparation".to_string(),
            ));
        }
        transaction.commit().map_err(DbError::from)?;
        Ok(next)
    }
}

pub(crate) fn complete_owner_device_join_publication_on(
    transaction: &rusqlite::Transaction<'_>,
    candidate: &coven_protocol::store_commit::StoreBatchCommitRef,
    completion: Option<&coven_protocol::membership_mutation::StoreMembershipJournalCompletion>,
) -> Result<(), DbError> {
    let active = super::active_store_publication::load_active_store_publication_on(transaction)?
        .ok_or_else(|| {
            DbError::Message(format!(
                "accepted device join candidate {candidate:?} has no active publication"
            ))
        })?;
    let ActiveStorePublicationOwner::DeviceJoin(attempt_id) = active.owner() else {
        return Ok(());
    };
    if !matches!(
        &active.attempt()?.entry.payload,
        coven_protocol::store_commit::StorePublicationPayload::Commit(reference)
            if reference == candidate
    ) {
        return Err(DbError::Message(
            "accepted device join candidate differs from its active publication".to_string(),
        ));
    }
    let key = DeviceJoinJournalRecord::store_key_for(*attempt_id, DeviceJoinRole::Owner);
    let current_value = crate::required_protocol_state_on(transaction, &key)?;
    let current: DeviceJoinJournalRecord = serde_json::from_str(&current_value)
        .map_err(|error| DbError::context("prepared device join journal", error))?;
    let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::StorePublicationPrepared(prepared)) =
        &*current.progress
    else {
        return Err(DbError::Message(
            "active device join publication has no prepared owner journal".to_string(),
        ));
    };
    if prepared.candidate.reference != *candidate
        || active.commit_reservation()
            != Some((
                &prepared.candidate.commit.write_id,
                &prepared.candidate.commit.author_registration,
                &candidate.coord,
            ))
    {
        return Err(DbError::Message(
            "active device join publication differs from its prepared journal".to_string(),
        ));
    }
    let winning = active.attempt()?;
    if matches!(
        prepared.operation,
        coven_protocol::store_commit::device_join_journal::OwnerJoinPublication::SamePrincipalActivation { .. }
    ) {
        let Some(coven_protocol::membership_mutation::StoreMembershipJournalCompletion::DeviceJoin {
            remote_objects,
        }) = completion else {
            return Err(DbError::Message(
                "same-principal handoff has no finalized registration authority".into(),
            ));
        };
        let proof = prepared.candidate.history_evidence.membership_proof.as_ref()
            .ok_or_else(|| DbError::Message("same-principal handoff has no exact authority head".into()))?;
        let results = remote_objects.iter().filter_map(|record| {
            let coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(value) = record else {
                return None;
            };
            match &value.identity.domain {
                coven_protocol::remote_object::RetainedAuthorityObjectDomain::MembershipHeadAcceptance { head, publication }
                    if head == &proof.head => Some(publication),
                _ => None,
            }
        }).collect::<Vec<_>>();
        if results.as_slice() != [&winning.reference()?] {
            return Err(DbError::Message(
                "same-principal handoff differs from its exact winning publication".into(),
            ));
        }
    }
    let accepted = DeviceJoinJournalRecord {
        attempt_id: *attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Owner(
            prepared
                .accepted_progress(*attempt_id, winning.replacement.clone())
                .map_err(|error| DbError::context("accepted device join progress", error))?,
        )),
    };
    crate::store::device_join_journal::validate_successor(&current, &accepted).map_err(
        |error| DbError::Message(format!("complete owner device join publication: {error}")),
    )?;
    let accepted_value = serde_json::to_string(&accepted)
        .map_err(|error| DbError::context("serialize accepted device join", error))?;
    if transaction
        .execute(
            "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
            (&accepted_value, &key, &current_value),
        )
        .map_err(DbError::from)?
        != 1
    {
        return Err(DbError::Message(
            "device join journal changed during publication completion".to_string(),
        ));
    }
    super::active_store_publication::clear_active_store_publication_on(transaction, &active)
}

impl StoreDatabase {
    pub async fn prepare_owner_device_join_publication(
        &self,
        previous: DeviceJoinJournalRecord,
        prepared: PreparedOwnerJoinPublication,
    ) -> Result<DeviceJoinJournalRecord, crate::store::device_join_journal::DeviceJoinJournalError>
    {
        self.call_store(move |session| {
            session.prepare_owner_device_join_publication(previous, prepared)
        })
        .await
        .map_err(crate::store::device_join_journal::DeviceJoinJournalError::Database)
    }
}
