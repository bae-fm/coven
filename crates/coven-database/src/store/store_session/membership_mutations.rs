use super::*;
use crate::store::StoreSession;
use crate::*;
use coven_protocol::store_commit::ObjectHash;
use rusqlite::OptionalExtension;
use std::collections::BTreeSet;

impl StoreSession<'_> {
    fn outbound_membership_mutation(
        &mut self,
    ) -> Result<Option<DurableMembershipMutation>, DbError> {
        let conn = self.conn;
        conn.query_row(
            "SELECT intent_hash, plan_bytes, progress_bytes \
             FROM outbound_membership_mutation WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(DbError::from)?
        .map(|(hash, plan_bytes, progress_bytes)| {
            let intent_hash: ObjectHash = hash
                .parse()
                .map_err(|error| DbError::context("membership intent hash", error))?;
            if ObjectHash::digest(&plan_bytes) != intent_hash {
                return Err(DbError::Message(
                    "membership intent hash differs from its exact plan bytes".to_string(),
                ));
            }
            Ok(DurableMembershipMutation {
                intent_hash,
                plan_bytes,
                progress_bytes,
            })
        })
        .transpose()
    }

    fn select_causal_author_stream(
        &mut self,
        key: &str,
        reusable: &std::collections::BTreeSet<coven_protocol::membership::AuthorStreamId>,
        candidate: coven_protocol::membership::AuthorStreamId,
    ) -> Result<coven_protocol::membership::AuthorStreamId, DbError> {
        let conn = self.conn;
        let existing = crate::get_protocol_state_on(conn, key)?
            .map(|value| value.parse().map_err(DbError::from))
            .transpose()?;
        if let Some(existing) = existing {
            if reusable.contains(&existing) {
                return Ok(existing);
            }
        }
        let selected = reusable.iter().next_back().copied().unwrap_or(candidate);
        crate::set_protocol_state_on(conn, key, &selected.to_string())?;
        Ok(selected)
    }

    fn stage_membership_candidate_mutation(
        &mut self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<ObjectHash, DbError> {
        let publication = validate_membership_candidate_objects(&candidate, &remote_objects)?;
        let pending_rotation_generation = membership_rotation_generation(&publication.entry)?;
        let active_publication = ActiveStorePublication::for_commit(
            ActiveStorePublicationOwner::MembershipMutation,
            &candidate,
        )?;
        let conn = self.conn;
        let intent_hash = ObjectHash::digest(&plan_bytes);
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let existing = tx
            .query_row(
                "SELECT intent_hash, plan_bytes FROM outbound_membership_mutation \
                 WHERE singleton = 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        if let Some((existing_hash, existing_plan)) = existing {
            if existing_hash != intent_hash.to_string() || existing_plan != plan_bytes {
                return Err(DbError::Message(
                    "a different membership mutation is already pending".to_string(),
                ));
            }
            for remote in &remote_objects {
                let stored = load_remote_object_on(&tx, remote.object_id())?;
                if stored != **remote {
                    return Err(DbError::Message(
                        "persisted membership ownership differs from its durable plan".to_string(),
                    ));
                }
            }
            if !super::active_store_publication::load_active_store_publication_on(&tx)?
                .is_some_and(|existing| existing.same_commit_reservation(&active_publication))
            {
                return Err(DbError::Message(
                    "membership candidate differs from its active Store publication".to_string(),
                ));
            }
            super::membership_rotation::stage_pending_rotation_on(
                &tx,
                pending_rotation_generation,
                intent_hash,
            )?;
            tx.commit().map_err(DbError::from)?;
            return Ok(intent_hash);
        }
        match super::active_store_publication::claim_active_store_publication_on(
            &tx,
            &active_publication,
        )? {
            super::active_store_publication::ActiveStorePublicationClaim::Acquired => {}
            super::active_store_publication::ActiveStorePublicationClaim::AlreadyOwned => {
                return Err(DbError::Message(
                    "membership mutation owns publication before its journal".to_string(),
                ));
            }
            super::active_store_publication::ActiveStorePublicationClaim::Occupied(owner) => {
                return Err(DbError::Message(format!(
                    "another local Store operation owns publication: {owner:?}"
                )));
            }
        }
        for remote in &remote_objects {
            persist_exact_remote_object_on(
                &tx,
                self.store_dir,
                remote,
                "membership candidate object",
            )?;
        }
        tx.execute(
            "INSERT INTO outbound_membership_mutation \
             (singleton, intent_hash, plan_bytes, progress_bytes) \
             VALUES (1, ?1, ?2, ?3)",
            rusqlite::params![intent_hash.to_string(), plan_bytes, progress_bytes],
        )
        .map_err(DbError::from)?;
        super::membership_rotation::stage_pending_rotation_on(
            &tx,
            pending_rotation_generation,
            intent_hash,
        )?;
        tx.commit().map_err(DbError::from)?;
        Ok(intent_hash)
    }

    fn update_membership_mutation_progress(
        &mut self,
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        let conn = self.conn;
        let updated = conn
            .execute(
                "UPDATE outbound_membership_mutation SET progress_bytes = ?1 \
                 WHERE singleton = 1 AND intent_hash = ?2",
                rusqlite::params![progress_bytes, intent_hash.to_string()],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(
                "membership mutation ownership row is absent or changed".to_string(),
            ));
        }
        Ok(())
    }

    fn stage_membership_candidate_abandonment(
        &mut self,
        intent_hash: ObjectHash,
        expected: ActiveStorePublication,
        original: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        abandonment: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<ActiveStorePublication, DbError> {
        original.validate_closed_shape()?;
        abandonment.validate_closed_shape()?;
        let original_target = coven_protocol::store_commit::StoreBatchCommitDeletionTarget {
            coord: original.reference.coord.clone(),
            object: original.reference.object.clone(),
            canonical_signed_bytes: original.commit.to_bytes(),
        };
        if expected.owner() != &ActiveStorePublicationOwner::MembershipMutation
            || expected.commit_reservation()
                != Some((
                    &original.commit.write_id,
                    &original.commit.author_registration,
                    &original.reference.coord,
                ))
            || abandonment.commit.write_id != original.commit.write_id
            || abandonment.commit.author_registration != original.commit.author_registration
            || abandonment.reference.coord != original.reference.coord
            || abandonment.commit.order.predecessor() != original.commit.order.predecessor()
            || abandonment.commit.abandoned_candidates()
                != [coven_protocol::store_commit::CandidateCleanupManifest {
                    candidate: original_target,
                }]
            || expected.attempt()?.entry.payload
                != coven_protocol::store_commit::StorePublicationPayload::Commit(
                    original.reference.clone(),
                )
            || !expected.retired_candidates().is_empty()
        {
            return Err(DbError::Message(
                "membership abandonment differs from its exact reserved candidate".into(),
            ));
        }
        original.prepared_membership_publication()?;
        let mut replacement = expected.begin_membership_abandonment(abandonment.clone())?;
        replacement.retain_superseded_entry(expected.attempt()?.reference()?)?;
        let bytes = abandonment.commit.to_bytes();
        let remote =
            RemoteObjectRecord::candidate_commit(abandonment.reference.clone(), &bytes, &bytes)?;
        let tx = self.conn.unchecked_transaction()?;
        require_membership_mutation_on(&tx, intent_hash)?;
        let installed = super::observed_store_publication::load_store_current_publication_on(&tx)?;
        if installed.record() != &abandonment.publication.previous
            || installed.observed_version() != Some(&abandonment.publication.previous_version)
        {
            return Err(DbError::Message(
                "membership abandonment does not extend the installed boundary".into(),
            ));
        }
        let entries = super::observed_store_publication::load_store_publication_entries_on(&tx)?;
        if entries.iter().any(|entry| {
            matches!(&entry.value.payload,
                coven_protocol::store_commit::StorePublicationPayload::Commit(candidate)
                    if candidate.coord == original.reference.coord)
        }) {
            return Err(DbError::Message(
                "accepted membership author position cannot be abandoned".into(),
            ));
        }
        if super::materialized_commit_index::latest_position_for_device_on(
            &tx,
            &original.reference.coord.stream_id.to_string(),
        )?
        .is_some_and(|tip| tip.coord.sequence >= original.reference.coord.sequence)
        {
            return Err(DbError::Message(
                "membership abandonment cannot consume a covered author position".into(),
            ));
        }
        let previous_attempt = expected.attempt()?.reference()?;
        let competing = entries.iter().any(|entry| {
            entry.value.position == previous_attempt.position
                && entry.prepared.reference() != &previous_attempt.object
        });
        let retired = installed
            .record()
            .latest_snapshot()
            .is_some_and(|snapshot| snapshot.publication.position > previous_attempt.position);
        if !competing && !retired {
            return Err(DbError::Message(
                "membership abandonment has no accepted supersession of its previous attempt"
                    .into(),
            ));
        }
        crate::remote_object_records::validate_remote_object_on(
            &tx,
            remote_object_id(&original.reference.object),
            &original.reference.object,
            &original.commit.to_bytes(),
        )?;
        persist_exact_remote_object_on(&tx, self.store_dir, &remote, "membership abandonment")?;
        super::active_store_publication::update_active_store_publication_on(
            &tx,
            &expected,
            &replacement,
        )?;
        tx.commit()?;
        Ok(replacement)
    }

    fn replace_membership_candidate_mutation(
        &mut self,
        intent_hash: ObjectHash,
        expected: ActiveStorePublication,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    ) -> Result<ObjectHash, DbError> {
        if expected.owner() != &ActiveStorePublicationOwner::MembershipMutation
            || !expected.is_awaiting_preparation()
            || expected.commit_reservation()
                != Some((
                    &candidate.commit.write_id,
                    &candidate.commit.author_registration,
                    &candidate.reference.coord,
                ))
        {
            return Err(DbError::Message(
                "replacement membership candidate lacks its exact completed reservation".into(),
            ));
        }
        let publication = validate_membership_candidate_objects(&candidate, &remote_objects)?;
        let replacement_hash = ObjectHash::digest(&plan_bytes);
        let tx = self.conn.unchecked_transaction()?;
        require_membership_mutation_on(&tx, intent_hash)?;
        let retired = require_retired_membership_candidate_on(&tx, &expected)?;
        let RetiredStoreCandidateInputs::Membership(original) = &retired.inputs else {
            unreachable!("retired membership candidate is validated")
        };
        use coven_protocol::membership::StoreAuthorityChange;
        let same_request = match (&original.entry.change, &publication.entry.change) {
            (
                StoreAuthorityChange::RemoveMember {
                    user_pubkey: before,
                    ..
                },
                StoreAuthorityChange::RemoveMember {
                    user_pubkey: after, ..
                },
            ) => before == after,
            (
                StoreAuthorityChange::SetMember {
                    user_pubkey: before,
                    provider_account_email: old_email,
                    role: old_role,
                    ..
                },
                StoreAuthorityChange::SetMember {
                    user_pubkey: after,
                    provider_account_email: new_email,
                    role: new_role,
                    ..
                },
            ) => before == after && old_email == new_email && old_role == new_role,
            (
                StoreAuthorityChange::ResolutionActivation { resolution: before },
                StoreAuthorityChange::ResolutionActivation { resolution: after },
            ) => before == after,
            _ => false,
        };
        if !same_request {
            return Err(DbError::Message(
                "replacement changes the retained membership request".into(),
            ));
        }
        let previous_rotation_generation = membership_rotation_generation(&original.entry)?;
        let pending_rotation_generation = membership_rotation_generation(&publication.entry)?;
        let replacement = consume_retired_membership_candidate_on(&tx, &expected)?
            .replace_attempt(candidate.publication.clone())?;
        let installed = super::observed_store_publication::load_store_current_publication_on(&tx)?;
        if installed.record() != &candidate.publication.previous
            || installed.observed_version() != Some(&candidate.publication.previous_version)
        {
            return Err(DbError::Message(
                "replacement membership candidate does not extend the installed boundary".into(),
            ));
        }
        for remote in &remote_objects {
            persist_exact_remote_object_on(
                &tx,
                self.store_dir,
                remote,
                "replacement membership candidate object",
            )?;
        }
        if tx.execute(
            "UPDATE outbound_membership_mutation SET intent_hash = ?1, plan_bytes = ?2, progress_bytes = ?3 \
             WHERE singleton = 1 AND intent_hash = ?4",
            rusqlite::params![
                replacement_hash.to_string(),
                plan_bytes,
                progress_bytes,
                intent_hash.to_string()
            ],
        )? != 1
        {
            return Err(DbError::Message(
                "membership mutation changed during replacement".into(),
            ));
        }
        if let Some(generation) = previous_rotation_generation {
            super::membership_rotation::remove_rotation_candidate_on(&tx, intent_hash, generation)?;
        }
        super::membership_rotation::stage_pending_rotation_on(
            &tx,
            pending_rotation_generation,
            replacement_hash,
        )?;
        super::active_store_publication::update_active_store_publication_on(
            &tx,
            &expected,
            &replacement,
        )?;
        tx.commit()?;
        Ok(replacement_hash)
    }

    fn complete_membership_mutation(&mut self, intent_hash: ObjectHash) -> Result<(), DbError> {
        let conn = self.conn;
        let deleted = conn
            .execute(
                "DELETE FROM outbound_membership_mutation \
                 WHERE singleton = 1 AND intent_hash = ?1",
                [intent_hash.to_string()],
            )
            .map_err(DbError::from)?;
        if deleted != 1 {
            return Err(DbError::Message(
                "membership mutation ownership row is absent or changed".to_string(),
            ));
        }
        Ok(())
    }
}

impl StoreDatabase {
    pub async fn stage_membership_candidate_abandonment(
        &self,
        intent_hash: ObjectHash,
        expected: ActiveStorePublication,
        original: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        abandonment: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<ActiveStorePublication, DbError> {
        self.call_store(move |session| {
            session.stage_membership_candidate_abandonment(
                intent_hash,
                expected,
                original,
                abandonment,
            )
        })
        .await
    }

    pub async fn replace_membership_candidate_mutation(
        &self,
        intent_hash: ObjectHash,
        expected: ActiveStorePublication,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    ) -> Result<ObjectHash, DbError> {
        self.call_store(move |session| {
            session.replace_membership_candidate_mutation(
                intent_hash,
                expected,
                candidate,
                plan_bytes,
                progress_bytes,
                remote_objects,
            )
        })
        .await
    }

    pub async fn outbound_membership_mutation(
        &self,
    ) -> Result<Option<DurableMembershipMutation>, DbError> {
        self.call_store(|session| session.outbound_membership_mutation())
            .await
    }

    pub async fn select_membership_author_stream(
        &self,
        author_pubkey: &str,
        author_owner_grant: &coven_protocol::membership::MembershipGrantId,
        reusable: std::collections::BTreeSet<coven_protocol::membership::AuthorStreamId>,
    ) -> Result<coven_protocol::membership::AuthorStreamId, DbError> {
        self.select_causal_author_stream(
            format!("membership_author_stream/{author_pubkey}/{author_owner_grant}"),
            reusable,
        )
        .await
    }

    pub async fn select_causal_author_stream(
        &self,
        key: String,
        reusable: std::collections::BTreeSet<coven_protocol::membership::AuthorStreamId>,
    ) -> Result<coven_protocol::membership::AuthorStreamId, DbError> {
        let candidate = coven_protocol::membership::AuthorStreamId::from_digest(
            ObjectHash::digest(self.new_store_write_id().as_str().as_bytes()),
        );
        self.call_store(move |session| {
            session.select_causal_author_stream(&key, &reusable, candidate)
        })
        .await
    }

    pub async fn stage_membership_candidate_mutation(
        &self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<ObjectHash, DbError> {
        self.call_store(move |session| {
            session.stage_membership_candidate_mutation(
                plan_bytes,
                progress_bytes,
                remote_objects,
                candidate,
            )
        })
        .await
    }

    pub async fn update_membership_mutation_progress(
        &self,
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.update_membership_mutation_progress(intent_hash, progress_bytes)
        })
        .await
    }

    pub async fn complete_membership_mutation(
        &self,
        intent_hash: ObjectHash,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.complete_membership_mutation(intent_hash))
            .await
    }
}

pub(super) fn require_membership_mutation_on(
    conn: &rusqlite::Connection,
    intent_hash: ObjectHash,
) -> Result<(), DbError> {
    let stored: Vec<u8> = conn.query_row(
        "SELECT plan_bytes FROM outbound_membership_mutation WHERE singleton = 1 AND intent_hash = ?1",
        [intent_hash.to_string()],
        |row| row.get(0),
    )?;
    if ObjectHash::digest(&stored) != intent_hash {
        return Err(DbError::Message(
            "membership mutation plan differs from its owner".into(),
        ));
    }
    Ok(())
}

fn validate_membership_candidate_objects(
    candidate: &coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    remote_objects: &[coven_protocol::remote_object::ClosedRemoteObject],
) -> Result<coven_protocol::membership_mutation::PreparedMembershipPublication, DbError> {
    candidate.validate_closed_shape()?;
    let publication = candidate.prepared_membership_publication()?;
    let objects = publication.candidate_object_refs(&candidate.commit, &candidate.reference)?;
    let supplied = remote_objects
        .iter()
        .map(|remote| remote.object().clone())
        .collect::<BTreeSet<_>>();
    if supplied.len() != remote_objects.len()
        || supplied != objects.into_iter().collect::<BTreeSet<_>>()
    {
        return Err(DbError::Message(
            "membership ownership differs from its exact candidate".into(),
        ));
    }
    let owns_candidate = |ownership: &coven_protocol::remote_object::PendingCandidateOwnership| {
        ownership.pending.len() == 1
            && ownership.pending.contains(&candidate.reference)
            && ownership.nonactivated.is_empty()
    };
    for remote in remote_objects {
        use coven_protocol::remote_object::{
            CandidateCommitState, CandidateObjectState, RetainedAuthorityObjectState,
        };
        let prepared_for_candidate = match remote.record() {
            RemoteObjectRecord::CandidateCommit(record) => {
                record.identity == candidate.reference
                    && matches!(record.state, CandidateCommitState::Prepared)
            }
            RemoteObjectRecord::CandidateExclusive(record) => matches!(
                &record.state,
                CandidateObjectState::Prepared { ownership } if owns_candidate(ownership)
            ),
            RemoteObjectRecord::RetainedAuthority(record) => matches!(
                &record.state,
                RetainedAuthorityObjectState::Prepared { ownership } if owns_candidate(ownership)
            ),
            RemoteObjectRecord::SharedLiveSet(_) => false,
        };
        if !prepared_for_candidate {
            return Err(DbError::Message(
                "membership object is not prepared for its exact candidate".into(),
            ));
        }
    }
    Ok(publication)
}

pub(super) fn membership_rotation_generation(
    entry: &coven_protocol::membership::MembershipEntry,
) -> Result<Option<u64>, DbError> {
    match &entry.change {
        coven_protocol::membership::StoreAuthorityChange::RemoveMember { wrapped_keys, .. } => {
            let generation = wrapped_keys
                .first()
                .ok_or_else(|| {
                    DbError::Message(
                        "retained member removal has no replacement key generation".into(),
                    )
                })?
                .generation;
            Ok(Some(generation))
        }
        _ => Ok(None),
    }
}

pub(super) fn require_retired_membership_candidate_on<'a>(
    conn: &rusqlite::Connection,
    expected: &'a ActiveStorePublication,
) -> Result<&'a RetiredStoreCandidate, DbError> {
    let [retired] = expected.retired_candidates() else {
        return Err(DbError::Message(
            "membership continuation has no exact original candidate".into(),
        ));
    };
    if expected.owner() != &ActiveStorePublicationOwner::MembershipMutation
        || !expected.is_awaiting_preparation()
        || !matches!(retired.inputs, RetiredStoreCandidateInputs::Membership(_))
        || super::active_store_publication::load_active_store_publication_on(conn)?.as_ref()
            != Some(expected)
    {
        return Err(DbError::Message(
            "membership continuation differs from its durable owner".into(),
        ));
    }
    super::candidate_records::require_candidate_cleanup_complete_on(
        conn,
        &retired.candidate()?,
        &retired.objects()?,
        "membership candidate cleanup is incomplete",
    )?;
    Ok(retired)
}

pub(super) fn consume_retired_membership_candidate_on(
    tx: &rusqlite::Transaction<'_>,
    expected: &ActiveStorePublication,
) -> Result<ActiveStorePublication, DbError> {
    let retired = require_retired_membership_candidate_on(tx, expected)?;
    super::candidate_records::delete_remote_objects_on(
        tx,
        retired.objects()?.iter().map(remote_object_id),
        "retired membership candidate",
    )?;
    let mut completed = expected.clone();
    completed.complete_retired_candidate_cleanup()?;
    Ok(completed)
}

#[cfg(test)]
#[path = "membership_mutations_tests.rs"]
mod tests;
