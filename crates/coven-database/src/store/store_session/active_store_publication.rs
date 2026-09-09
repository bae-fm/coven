use super::{StoreDatabase, StoreSession};
use crate::{ActiveStorePublication, ActiveStorePublicationOwner, DbError};
use coven_protocol::store_commit::{StorePublicationPayload, VerifiedStoreBatchCommit};
use rusqlite::OptionalExtension;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActiveStorePublicationClaim {
    Acquired,
    AlreadyOwned,
    Occupied(ActiveStorePublicationOwner),
}

fn encode(value: &ActiveStorePublication) -> Result<String, DbError> {
    serde_json::to_string(value)
        .map_err(|error| DbError::context("serialize active Store publication", error))
}

fn decode(raw: &str) -> Result<ActiveStorePublication, DbError> {
    serde_json::from_str(raw)
        .map_err(|error| DbError::context("parse active Store publication", error))
}

pub(crate) fn load_active_store_publication_on(
    conn: &rusqlite::Connection,
) -> Result<Option<ActiveStorePublication>, DbError> {
    conn.query_row(
        "SELECT state FROM active_store_publication WHERE singleton = 1",
        [],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(DbError::from)?
    .map(|raw| decode(&raw))
    .transpose()
}

pub(crate) fn claim_active_store_publication_on(
    conn: &rusqlite::Connection,
    publication: &ActiveStorePublication,
) -> Result<ActiveStorePublicationClaim, DbError> {
    match load_active_store_publication_on(conn)? {
        Some(active) if active == *publication => Ok(ActiveStorePublicationClaim::AlreadyOwned),
        Some(active) => Ok(ActiveStorePublicationClaim::Occupied(
            active.owner().clone(),
        )),
        None => {
            conn.execute(
                "INSERT INTO active_store_publication (singleton, state) VALUES (1, ?1)",
                [encode(publication)?],
            )
            .map_err(DbError::from)?;
            Ok(ActiveStorePublicationClaim::Acquired)
        }
    }
}

pub(crate) fn clear_active_store_publication_on(
    conn: &rusqlite::Connection,
    expected: &ActiveStorePublication,
) -> Result<(), DbError> {
    if expected.superseded_entry().is_some()
        || !expected.retired_candidates().is_empty()
        || !expected.retired_snapshot_objects().is_empty()
    {
        return Err(DbError::Message(
            "Store publication still owns superseded entry cleanup".to_string(),
        ));
    }
    let deleted = conn
        .execute(
            "DELETE FROM active_store_publication WHERE singleton = 1 AND state = ?1",
            [encode(expected)?],
        )
        .map_err(DbError::from)?;
    if deleted != 1 {
        return Err(DbError::Message(
            "active Store publication changed before completion".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn clear_active_store_commit_for_owner_on(
    conn: &rusqlite::Connection,
    owner: &ActiveStorePublicationOwner,
    candidate: &coven_protocol::store_commit::StoreBatchCommitRef,
) -> Result<(), DbError> {
    let active = load_active_store_publication_on(conn)?.ok_or_else(|| {
        DbError::Message(format!(
            "accepted Store candidate {candidate:?} has no active publication"
        ))
    })?;
    if active.owner() != owner
        || !matches!(
            &active.attempt()?.entry.payload,
            coven_protocol::store_commit::StorePublicationPayload::Commit(reference)
                if reference == candidate
        )
    {
        return Err(DbError::Message(format!(
            "accepted Store candidate {candidate:?} differs from active publication {:?}",
            active.owner()
        )));
    }
    clear_active_store_publication_on(conn, &active)
}

pub(super) fn update_active_store_publication_on(
    conn: &rusqlite::Connection,
    expected: &ActiveStorePublication,
    replacement: &ActiveStorePublication,
) -> Result<(), DbError> {
    let updated = conn
        .execute(
            "UPDATE active_store_publication SET state = ?1 \
             WHERE singleton = 1 AND state = ?2",
            [encode(replacement)?, encode(expected)?],
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(
            "active Store publication changed before attempt replacement".to_string(),
        ));
    }
    Ok(())
}

impl StoreSession<'_> {
    fn active_store_publication(&self) -> Result<Option<ActiveStorePublication>, DbError> {
        load_active_store_publication_on(self.conn)
    }

    fn replace_active_store_commit_publication(
        &self,
        commit: VerifiedStoreBatchCommit,
        expected: ActiveStorePublication,
        mut replacement: ActiveStorePublication,
    ) -> Result<(), DbError> {
        if expected.replace_attempt(replacement.attempt()?.clone())? != replacement {
            return Err(DbError::Message(
                "replacement changes the active Store publication owner or reservation".to_string(),
            ));
        }
        if expected.commit_reservation()
            != Some((
                &commit.write_id,
                &commit.author_registration,
                &commit.reference().coord,
            ))
            || expected.attempt()?.entry.payload
                != StorePublicationPayload::Commit(commit.reference().clone())
            || expected.attempt()?.entry.payload != replacement.attempt()?.entry.payload
        {
            return Err(DbError::Message(
                "Store candidate replacement must include its owning journal".to_string(),
            ));
        }
        replacement.attempt()?.verify_commit(&commit)?;
        replacement.attempt()?.prepared_entry()?;
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let installed =
            super::observed_store_publication::load_store_current_publication_on(&transaction)?;
        if installed.record() != &replacement.attempt()?.previous
            || installed.observed_version() != Some(&replacement.attempt()?.previous_version)
        {
            return Err(DbError::Message(
                "replacement Store publication does not extend the installed boundary".to_string(),
            ));
        }
        let superseded = expected.attempt()?.reference()?;
        let entries = self.store_publication_entries()?;
        let winner = entries
            .iter()
            .find(|entry| entry.value.position == superseded.position)
            .ok_or_else(|| {
                DbError::Message(
                    "Store publication replacement lacks its settled exact position".to_string(),
                )
            })?;
        if winner.prepared.reference() == &superseded.object {
            return Err(DbError::Message(
                "accepted Store publication cannot be replaced".to_string(),
            ));
        }
        if let coven_protocol::store_commit::StorePublicationPayload::Commit(candidate) =
            &expected.attempt()?.entry.payload
        {
            if entries.iter().any(|entry| {
                matches!(
                    &entry.value.payload,
                    coven_protocol::store_commit::StorePublicationPayload::Commit(accepted)
                        if accepted.coord == candidate.coord
                )
            }) {
                return Err(DbError::Message(
                    "accepted Store operation cannot receive another publication attempt"
                        .to_string(),
                ));
            }
        }
        replacement.retain_superseded_entry(superseded)?;
        update_active_store_publication_on(&transaction, &expected, &replacement)?;
        transaction.commit().map_err(DbError::from)
    }

    fn begin_retired_store_write_discard(
        &self,
        expected: ActiveStorePublication,
    ) -> Result<ActiveStorePublication, DbError> {
        let tx = self.conn.unchecked_transaction()?;
        if load_active_store_publication_on(&tx)?.as_ref() != Some(&expected) {
            return Err(DbError::Message(
                "discarding Store publication changed".to_string(),
            ));
        }
        let ActiveStorePublicationOwner::StoreWrite(write_id) = expected.owner() else {
            return Err(DbError::Message(
                "only a Store write can discard its captured rows".to_string(),
            ));
        };
        let status: String = tx.query_row(
            "SELECT status FROM store_writes WHERE write_id = ?1",
            [write_id.as_str()],
            |row| row.get(0),
        )?;
        let status: coven_protocol::write::WriteStatus = serde_json::from_str(&status)
            .map_err(|error| DbError::context("write status before discard", error))?;
        if !matches!(status, coven_protocol::write::WriteStatus::Blocked(_)) {
            return Err(DbError::Message(
                "retired write must be blocked before discard".to_string(),
            ));
        }
        if expected.is_discarding() {
            return Ok(expected);
        }
        let replacement = expected.begin_discard()?;
        for retired in expected.retired_candidates() {
            super::candidate_records::begin_candidate_nonactivation_targets_on(
                &tx,
                &retired.candidate()?,
                &retired.objects()?,
                &retired.nonactivation,
            )?;
        }
        update_active_store_publication_on(&tx, &expected, &replacement)?;
        tx.commit()?;
        Ok(replacement)
    }

    fn retired_store_write_cleanup(
        &self,
        expected: &ActiveStorePublication,
    ) -> Result<Vec<super::candidate_records::CandidateCleanupObject>, DbError> {
        if load_active_store_publication_on(self.conn)?.as_ref() != Some(expected)
            || (expected.is_awaiting_preparation()
                && expected.owner() != &ActiveStorePublicationOwner::MembershipMutation)
        {
            return Err(DbError::Message(
                "retired candidate cleanup requires its prepared replacement owner".to_string(),
            ));
        }
        let mut targets = std::collections::BTreeMap::new();
        for retired in expected.retired_candidates() {
            for target in super::candidate_records::candidate_cleanup_targets_on(
                self.conn,
                &retired.candidate()?,
                &retired.objects()?,
            )? {
                targets.insert(target.object.clone(), target);
            }
        }
        Ok(targets.into_values().collect())
    }

    fn complete_retired_store_write_cleanup(
        &self,
        expected: ActiveStorePublication,
    ) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction()?;
        if load_active_store_publication_on(&tx)?.as_ref() != Some(&expected)
            || (expected.is_awaiting_preparation()
                && expected.owner() != &ActiveStorePublicationOwner::MembershipMutation)
        {
            return Err(DbError::Message(
                "retired candidate owner changed before cleanup completion".to_string(),
            ));
        }
        let targets = self.retired_store_write_cleanup(&expected)?;
        let mut removed = Vec::new();
        for target in targets {
            let id = coven_protocol::remote_object::remote_object_id(&target.object);
            let mut remote = crate::load_remote_object_on(&tx, id)?;
            remote.mark_absent_verified()?;
            crate::update_remote_object_on(&tx, id, &remote)?;
            removed.push(id);
        }
        for retired in expected.retired_candidates() {
            super::candidate_records::require_candidate_cleanup_complete_on(
                &tx,
                &retired.candidate()?,
                &retired.objects()?,
                "retired candidate cleanup is incomplete",
            )?;
        }
        if expected.owner() == &ActiveStorePublicationOwner::MembershipMutation {
            // The request's signed original authority remains bound to this
            // reservation until replacement or verified satisfaction consumes it.
            tx.commit()?;
            return Ok(());
        }
        for retired in expected.retired_candidates() {
            for blob in retired.blobs() {
                let remote = crate::load_remote_object_on(&tx, blob.remote_object_id())?;
                let verified = crate::PreparedAudienceBlob::from_remote(
                    blob.audience().clone(),
                    &blob.blob().locator().locator_hash().to_string(),
                    remote,
                    blob.spool_path().map(std::path::Path::to_path_buf),
                )?;
                if &verified != blob {
                    return Err(DbError::Message(
                        "retired blob source differs from its exact owner".to_string(),
                    ));
                }
                let Some(path) = blob.spool_path() else {
                    continue;
                };
                if path
                    != self
                        .store_dir
                        .outbound_blob_spool_path(blob.blob().locator().locator_hash())
                {
                    return Err(DbError::Message(
                        "retired blob spool differs from its locator".to_string(),
                    ));
                }
                let ignored_write = if expected.is_discarding() {
                    match expected.owner() {
                        ActiveStorePublicationOwner::StoreWrite(write_id) => Some(write_id),
                        _ => {
                            return Err(DbError::Message(
                                "discarded spool has no Store-write owner".to_string(),
                            ));
                        }
                    }
                } else {
                    None
                };
                if retained_blob_spool_has_claim_on(&tx, path, ignored_write)? {
                    continue;
                }
                match std::fs::remove_file(path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(coven_foundation::atomic_file::FileError::Path {
                            operation: "remove retired Store write blob spool",
                            path: path.to_path_buf(),
                            source,
                        }
                        .into());
                    }
                }
                coven_foundation::atomic_file::sync_parent_dir_blocking(path)?;
            }
        }
        super::candidate_records::delete_remote_objects_on(
            &tx,
            removed,
            "retired Store write candidate",
        )?;
        let mut completed = expected.clone();
        completed.complete_retired_candidate_cleanup()?;
        update_active_store_publication_on(&tx, &expected, &completed)?;
        tx.commit()?;
        Ok(())
    }

    fn complete_superseded_publication_cleanup(
        &self,
        expected: ActiveStorePublication,
    ) -> Result<(), DbError> {
        let mut completed = expected.clone();
        completed.complete_superseded_entry_cleanup()?;
        update_active_store_publication_on(self.conn, &expected, &completed)
    }
}

impl StoreDatabase {
    pub async fn begin_retired_store_write_discard(
        &self,
        expected: ActiveStorePublication,
    ) -> Result<ActiveStorePublication, DbError> {
        self.call_store(move |session| session.begin_retired_store_write_discard(expected))
            .await
    }

    pub async fn active_store_publication(
        &self,
    ) -> Result<Option<ActiveStorePublication>, DbError> {
        self.call_store(|session| session.active_store_publication())
            .await
    }

    pub async fn replace_active_store_commit_publication(
        &self,
        commit: VerifiedStoreBatchCommit,
        expected: ActiveStorePublication,
        replacement: ActiveStorePublication,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.replace_active_store_commit_publication(commit, expected, replacement)
        })
        .await
    }

    pub async fn retired_store_write_cleanup(
        &self,
        expected: ActiveStorePublication,
    ) -> Result<Vec<super::candidate_records::CandidateCleanupObject>, DbError> {
        self.call_store(move |session| session.retired_store_write_cleanup(&expected))
            .await
    }

    pub async fn complete_retired_store_write_cleanup(
        &self,
        expected: ActiveStorePublication,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.complete_retired_store_write_cleanup(expected))
            .await
    }

    pub async fn complete_superseded_publication_cleanup(
        &self,
        expected: ActiveStorePublication,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.complete_superseded_publication_cleanup(expected))
            .await
    }
}

/// Read the existing owners while the database job keeps new claims from racing
/// the unlink. The caller also holds the upload and snapshot publication permits
/// across this job, covering files prepared before those owners persist them.
pub(super) fn retained_blob_spool_has_claim_on(
    conn: &rusqlite::Connection,
    path: &std::path::Path,
    discarded_write: Option<&coven_protocol::write::WriteId>,
) -> Result<bool, DbError> {
    let encoded = path
        .to_str()
        .ok_or_else(|| DbError::Message("blob spool path is not UTF-8".to_string()))?;
    if conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM store_write_blobs WHERE spool_path = ?1)",
        [encoded],
        |row| row.get::<_, bool>(0),
    )? {
        return Ok(true);
    }
    let discarded_ordinal = discarded_write
        .map(|write_id| {
            conn.query_row(
                "SELECT ordinal FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get::<_, i64>(0),
            )
        })
        .transpose()?;
    for (ordinal, original, rebased) in crate::query_mapped_rows(
        conn,
        "SELECT ordinal, blob_facts, rebased FROM store_writes
         WHERE blob_facts IS NOT NULL
           AND json_extract(status, '$.published') IS NULL
           AND json_extract(status, '$.resolved') IS NULL",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        },
    )? {
        if discarded_ordinal.is_some_and(|discarded| ordinal >= discarded) {
            continue;
        }
        // Preparation and subsequent rebase both consume the effective facts.
        // Original capture remains immutable, but cannot keep a replaced source
        // alive after the current input owns a verified remote object instead.
        let facts = match rebased {
            Some(encoded) => {
                let rebased: crate::write_models::RebasedStoreWrite =
                    serde_json::from_str(&encoded)
                        .map_err(|error| DbError::context("rebased blob spool owner", error))?;
                rebased.blob_facts
            }
            None => serde_json::from_str::<crate::StoreWriteBlobFacts>(&original)
                .map_err(|error| DbError::context("captured blob spool owner", error))?,
        };
        for fact in &facts.blobs {
            if matches!(&fact.audience_move,
                Some(crate::StoreWriteBlobMoveDestination::Remote { spool_path, .. }) if spool_path == path)
            {
                return Ok(true);
            }
        }
    }
    for encoded in crate::query_mapped_rows(
        conn,
        "SELECT upload_state FROM cloud_outbox WHERE operation = 'upload'",
        [],
        |row| row.get::<_, String>(0),
    )? {
        let state: super::blob_outbox::OutboxUploadState = serde_json::from_str(&encoded)
            .map_err(|error| DbError::context("outbox blob spool owner", error))?;
        match state {
            super::blob_outbox::OutboxUploadState::Pending => {}
            super::blob_outbox::OutboxUploadState::Prepared { spool_path, .. }
            | super::blob_outbox::OutboxUploadState::Created { spool_path, .. }
                if spool_path == path =>
            {
                return Ok(true);
            }
            super::blob_outbox::OutboxUploadState::Prepared { .. }
            | super::blob_outbox::OutboxUploadState::Created { .. } => {}
        }
    }
    Ok(false)
}
