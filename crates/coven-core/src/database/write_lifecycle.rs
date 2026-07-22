use crate::database::remote_object_records::load_remote_object_on;

use super::*;

impl Database {
    pub(super) fn notify_write_status_in(
        statuses: &Arc<std::sync::Mutex<HashMap<WriteId, tokio::sync::watch::Sender<WriteStatus>>>>,
        write_id: &WriteId,
        status: WriteStatus,
    ) {
        let senders = statuses.lock().expect("write status mutex poisoned");
        if let Some(sender) = senders.get(write_id) {
            sender.send_replace(status);
        }
    }

    pub(crate) fn notify_write_status(&self, write_id: WriteId, status: WriteStatus) {
        Self::notify_write_status_in(&self.state.write_statuses, &write_id, status);
    }

    pub(crate) fn set_write_status_on(
        conn: &Connection,
        write_id: &WriteId,
        status: &WriteStatus,
    ) -> Result<(), DbError> {
        let status = serde_json::to_string(status)
            .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
        let updated = conn
            .execute(
                "UPDATE store_writes SET status = ?2 WHERE write_id = ?1",
                rusqlite::params![write_id.as_str(), status],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(format!("write {write_id} does not exist")));
        }
        Ok(())
    }

    pub(crate) fn resolve_unpublished_writes_on(
        tx: &rusqlite::Transaction<'_>,
        write_ids: &[WriteId],
        resolution: &WriteResolution,
    ) -> Result<(), DbError> {
        let status = WriteStatus::Resolved(resolution.clone());
        for write_id in write_ids {
            let raw_prepared: Option<String> = tx
                .query_row(
                    "SELECT prepared FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let mut removable = Vec::new();
            let mut candidate = None;
            if let Some(raw_prepared) = raw_prepared.as_deref() {
                let prepared: PreparedStoreWriteState = serde_json::from_str(raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("resolved prepared write: {error}"))
                    })?;
                let merge = parse_prepared_merge_candidate_on(tx, &prepared)?;
                removable.push(remote_object_id(&merge.reference.object));
                match load_merge_candidate_head_cleanup_on(
                    tx,
                    merge.head_prepared.reference(),
                    &merge.reference,
                )? {
                    MergeCandidateHeadCleanup::Remote { .. } => {
                        removable.push(remote_object_id(merge.head_prepared.reference()))
                    }
                    MergeCandidateHeadCleanup::ProtocolInert => {}
                }
                removable.extend(
                    candidate_graph_exact_objects(&merge.commit)?
                        .iter()
                        .map(remote_object_id),
                );
                candidate = Some(merge.reference);
            }
            let mut statement = tx
                .prepare("SELECT remote_object_id FROM store_write_blobs WHERE write_id = ?1")
                .map_err(DbError::from)?;
            let indexed = statement
                .query_map([write_id.as_str()], |row| row.get::<_, String>(0))
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            drop(statement);
            for encoded in indexed {
                removable.push(encoded.parse().map_err(|error| {
                    DbError::Message(format!("resolved remote object id: {error}"))
                })?);
            }
            if let Some(candidate) = &candidate {
                for object_id in &removable {
                    let remote = load_remote_object_on(tx, *object_id)?;
                    if !remote
                        .candidate_cleanup_complete(candidate)
                        .map_err(|error| {
                            DbError::Message(format!(
                                "validate candidate cleanup for {object_id}: {error}"
                            ))
                        })?
                    {
                        return Err(DbError::Message(format!(
                            "candidate cleanup for remote object {object_id} is incomplete"
                        )));
                    }
                }
            }
            tx.execute(
                "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            tx.execute(
                "DELETE FROM store_write_packages WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            tx.execute(
                "DELETE FROM store_write_blobs WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            for object_id in removable {
                let remote = load_remote_object_on(tx, object_id)?;
                let absent = matches!(
                    remote,
                    RemoteObjectRecord::CandidateCommit(
                        crate::sync::remote_object::CandidateCommitRecord {
                            state:
                                crate::sync::remote_object::CandidateCommitState::AbsentVerified { .. },
                            ..
                        }
                    ) | RemoteObjectRecord::CandidateExclusive(
                        crate::sync::remote_object::CandidateObjectRecord {
                            state:
                                crate::sync::remote_object::CandidateObjectState::AbsentVerified { .. },
                            ..
                        }
                    ) | RemoteObjectRecord::RetainedAuthority(
                        crate::sync::remote_object::RetainedAuthorityRecord {
                            state:
                                crate::sync::remote_object::RetainedAuthorityObjectState::UncreatedVerified { .. },
                            ..
                        }
                    )
                );
                if absent {
                    tx.execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [object_id.to_string()],
                    )
                    .map_err(DbError::from)?;
                }
            }
            tx.execute(
                "UPDATE store_writes SET prepared = NULL WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            Self::set_write_status_on(tx, write_id, &status)?;
        }
        Ok(())
    }

    pub(crate) async fn set_write_status(
        &self,
        write_id: &WriteId,
        status: WriteStatus,
    ) -> Result<(), DbError> {
        let stored_id = write_id.clone();
        let stored_status = status.clone();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            Self::set_write_status_on(conn, &stored_id, &stored_status)?;
            Self::notify_write_status_in(&statuses, &stored_id, stored_status);
            Ok(())
        })
        .await
    }

    pub(crate) async fn block_write_if_unresolved(
        &self,
        write_id: &WriteId,
        block: crate::WriteBlock,
    ) -> Result<bool, DbError> {
        let write_id = write_id.clone();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let raw: String = conn
                .query_row(
                    "SELECT status FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let current: WriteStatus = serde_json::from_str(&raw).map_err(|error| {
                DbError::Message(format!("write {write_id} status before blocking: {error}"))
            })?;
            match current {
                WriteStatus::Resolved(_) => Ok(false),
                WriteStatus::Pending | WriteStatus::Publishing | WriteStatus::Blocked(_) => {
                    let blocked = WriteStatus::Blocked(block);
                    Self::set_write_status_on(conn, &write_id, &blocked)?;
                    Self::notify_write_status_in(&statuses, &write_id, blocked);
                    Ok(true)
                }
                state @ (WriteStatus::LocalOnly | WriteStatus::Published(_)) => {
                    Err(DbError::Message(format!(
                        "write {write_id} cannot become blocked from {state:?}"
                    )))
                }
            }
        })
        .await
    }

    pub async fn write_status(&self, write_id: &WriteId) -> Result<WriteStatus, DbError> {
        let write_id = write_id.clone();
        self.call(move |conn| {
            let raw: String = conn
                .query_row(
                    "SELECT status FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            serde_json::from_str(&raw)
                .map_err(|error| DbError::Message(format!("write {write_id} status: {error}")))
        })
        .await
    }

    pub async fn pending_writes(&self) -> Result<Vec<PendingWrite>, DbError> {
        self.call(move |conn| {
            let mut statement = conn
                .prepare(
                    "SELECT write_id, status, affected_rows FROM store_writes
                     WHERE status IN ('\"pending\"', '\"publishing\"')
                        OR json_extract(status, '$.blocked') IS NOT NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(DbError::from)?;
            rows.map(|row| {
                let (write_id, status, affected_rows) = row.map_err(DbError::from)?;
                Ok(PendingWrite {
                    write_id: WriteId::from_generated(write_id),
                    status: serde_json::from_str(&status).map_err(|error| {
                        DbError::Message(format!("pending write status: {error}"))
                    })?,
                    affected_rows: serde_json::from_str(&affected_rows).map_err(|error| {
                        DbError::Message(format!("pending affected rows: {error}"))
                    })?,
                })
            })
            .collect()
        })
        .await
    }

    /// Writes whose semantic publication fault requires an explicit host action.
    pub async fn blocked_writes(&self) -> Result<Vec<PendingWrite>, DbError> {
        Ok(self
            .pending_writes()
            .await?
            .into_iter()
            .filter(|write| matches!(write.status, WriteStatus::Blocked(_)))
            .collect())
    }

    /// Return one blocked write to publication. The next preparation attempt
    /// revalidates every captured fact; another semantic failure records a fresh
    /// `Blocked` status.
    pub async fn retry_blocked_write(&self, write_id: &WriteId) -> Result<Vec<WriteId>, DbError> {
        let write_id = write_id.clone();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let (raw_status, prepared): (String, Option<String>) = tx
                .query_row(
                    "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let status: WriteStatus = serde_json::from_str(&raw_status)
                .map_err(|error| DbError::Message(format!("blocked write {write_id} status: {error}")))?;
            if !matches!(status, WriteStatus::Blocked(_)) {
                return Err(DbError::Message(format!("write {write_id} is not blocked")));
            }
            let mut retried = Vec::new();
            if let Some(raw_prepared) = prepared.as_deref() {
                let prepared: PreparedStoreWriteState = serde_json::from_str(raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "blocked write {write_id} preparation: {error}"
                        ))
                    })?;
                let candidate = parse_prepared_merge_candidate_on(&tx, &prepared)?.reference;
                let remote = load_remote_object_on(&tx, remote_object_id(&candidate.object))?;
                if matches!(
                    remote,
                    RemoteObjectRecord::CandidateCommit(
                        crate::sync::remote_object::CandidateCommitRecord {
                            state:
                                crate::sync::remote_object::CandidateCommitState::CleanupPending {
                                    proof: crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. }
                                }
                                | crate::sync::remote_object::CandidateCommitState::AbsentVerified {
                                    proof: crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. }
                                },
                            ..
                        }
                    )
                ) {
                    return Err(DbError::Message(format!(
                        "Merge write {write_id} has an irreversible winner and cannot be retried"
                    )));
                }
            }
            let next = if prepared.is_some() {
                WriteStatus::Publishing
            } else {
                WriteStatus::Pending
            };
            let next_json = serde_json::to_string(&next).map_err(|error| {
                DbError::Message(format!("serialize retry status: {error}"))
            })?;
            let updated = tx
                .execute(
                    "UPDATE store_writes SET status = ?2
                     WHERE write_id = ?1 AND json_extract(status, '$.blocked') IS NOT NULL",
                    rusqlite::params![write_id.as_str(), next_json],
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(format!(
                    "blocked write {write_id} changed during retry"
                )));
            }
            retried.push((write_id, next));
            tx.commit().map_err(DbError::from)?;
            let retried_ids = retried
                .iter()
                .map(|(write_id, _)| write_id.clone())
                .collect();
            for (write_id, status) in retried {
                Self::notify_write_status_in(&statuses, &write_id, status);
            }
            Ok(retried_ids)
        })
        .await
    }

    /// Atomically reverse a blocked write and every later unpublished shared
    /// write whose working-row state depends on it.
    pub async fn discard_blocked_write(&self, write_id: &WriteId) -> Result<Vec<WriteId>, DbError> {
        let write_id = write_id.clone();
        let synced_tables = self.synced_tables().to_vec();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let (raw_status, target_ordinal): (String, i64) = tx
                .query_row(
                    "SELECT status, ordinal FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let target_status: WriteStatus = serde_json::from_str(&raw_status)
                .map_err(|error| DbError::Message(format!("blocked write {write_id} status: {error}")))?;
            if !matches!(target_status, WriteStatus::Blocked(_)) {
                return Err(DbError::Message(format!("write {write_id} is not blocked")));
            }

            let mut statement = tx
                .prepare(
                    "SELECT write_id, status, inverse_changeset FROM store_writes
                     WHERE ordinal >= ?1
                       AND status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([target_ordinal], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .map_err(DbError::from)?;
            let mut discarded = Vec::new();
            for row in rows {
                let (stored_id, raw_status, inverse) = row.map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status)
                    .map_err(|error| DbError::Message(format!("discard write status: {error}")))?;
                if !matches!(status, WriteStatus::Pending | WriteStatus::Blocked(_)) {
                    return Err(DbError::Message(format!(
                        "write {stored_id} after blocked write {write_id} has non-discardable status {status:?}"
                    )));
                }
                discarded.push((WriteId::from_generated(stored_id), inverse));
            }
            drop(statement);
            if discarded.first().map(|(stored_id, _)| stored_id) != Some(&write_id) {
                return Err(DbError::Message(format!(
                    "blocked write {write_id} is absent from its unpublished suffix"
                )));
            }
            let schema = Arc::new(crate::sync::conflict::TableSchema::from_db(
                &tx,
                &synced_tables,
            )?);
            for (_, inverse) in discarded.iter().rev() {
                let inverse = crate::sync::apply::ValidatedChangeset::new(
                    inverse,
                    schema.clone(),
                )
                .map_err(|error| DbError::Message(format!("invalid blocked-write inverse: {error}")))?;
                crate::sync::apply::apply_changeset_strict_on(&tx, inverse)
                    .map_err(|error| DbError::Message(format!("reverse blocked-write suffix: {error}")))?;
            }
            let discarded_ids: Vec<_> = discarded
                .into_iter()
                .map(|(write_id, _)| write_id)
                .collect();
            let resolution = WriteResolution::Discarded;
            Self::resolve_unpublished_writes_on(&tx, &discarded_ids, &resolution)?;
            tx.commit().map_err(DbError::from)?;
            let status = WriteStatus::Resolved(resolution);
            for discarded_id in &discarded_ids {
                Self::notify_write_status_in(&statuses, discarded_id, status.clone());
            }
            Ok(discarded_ids)
        })
        .await
    }

    pub async fn subscribe_write_status(
        &self,
        write_id: &WriteId,
    ) -> Result<tokio::sync::watch::Receiver<WriteStatus>, DbError> {
        let write_id = write_id.clone();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let raw: String = conn
                .query_row(
                    "SELECT status FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let current: WriteStatus = serde_json::from_str(&raw)
                .map_err(|error| DbError::Message(format!("write {write_id} status: {error}")))?;
            let mut senders = statuses.lock().expect("write status mutex poisoned");
            let sender = senders
                .entry(write_id)
                .or_insert_with(|| tokio::sync::watch::channel(current.clone()).0);
            sender.send_replace(current);
            Ok(sender.subscribe())
        })
        .await
    }
}
