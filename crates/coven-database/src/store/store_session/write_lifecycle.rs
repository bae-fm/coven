use crate::*;
use coven_protocol::write::{PendingWrite, WriteId, WriteResolution, WriteStatus};
use std::sync::Arc;

use super::publication_state::PreparedStoreWriteState;
use super::*;

#[derive(Debug, PartialEq, Eq)]
pub enum BlockedWriteDiscard {
    Discarded(Vec<coven_protocol::write::WriteId>),
    RemoteResolutionRequired,
}

impl StoreSession<'_> {
    fn pending_writes(&self) -> Result<Vec<PendingWrite>, DbError> {
        let mut statement = self
            .conn
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
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(DbError::from)?;
        rows.map(|row| {
            let (write_id, status, affected_rows) = row.map_err(DbError::from)?;
            // Only a folded write has no affected rows, and the fold stops at
            // the first write that is not settled — which every write selected
            // here is not.
            let affected_rows = affected_rows.ok_or_else(|| {
                DbError::Message(format!("unpublished write {write_id} has been folded"))
            })?;
            Ok(PendingWrite {
                write_id: WriteId::from_generated(write_id),
                status: serde_json::from_str(&status)
                    .map_err(|error| DbError::context("pending write status", error))?,
                affected_rows: serde_json::from_str(&affected_rows)
                    .map_err(|error| DbError::context("pending affected rows", error))?,
            })
        })
        .collect()
    }

    /// Where each published write landed, in publication order.
    ///
    /// The device's own record of its writes, which survives a replay-baseline
    /// advance — the per-position `materialized_commits` rows do not, because
    /// the advance retires the retained rows they name.
    #[cfg(any(test, feature = "test-utils"))]
    fn published_write_commits(
        &self,
    ) -> Result<Vec<coven_protocol::store_commit::StoreBatchCommitRef>, DbError> {
        let rows = crate::query_mapped_rows(
            self.conn,
            "SELECT status FROM store_writes ORDER BY ordinal",
            [],
            |row| row.get::<_, String>(0),
        )?;
        let mut commits = Vec::new();
        for raw in rows {
            let status: WriteStatus = serde_json::from_str(&raw)
                .map_err(|error| DbError::context("published write status", error))?;
            if let WriteStatus::Published(position) = status {
                commits.push(position.commit().clone());
            }
        }
        Ok(commits)
    }

    fn set_write_status(&self, write_id: &WriteId, status: &WriteStatus) -> Result<(), DbError> {
        Database::set_write_status_on(self.conn, write_id, status)
    }

    fn block_write_if_unresolved(
        &self,
        write_id: &WriteId,
        block: coven_protocol::write::WriteBlock,
    ) -> Result<Option<WriteStatus>, DbError> {
        let raw: String = self
            .conn
            .query_row(
                "SELECT status FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let current: WriteStatus = serde_json::from_str(&raw).map_err(|error| {
            DbError::context(format!("write {write_id} status before blocking"), error)
        })?;
        match current {
            WriteStatus::Resolved(_) => Ok(None),
            WriteStatus::Pending | WriteStatus::Publishing | WriteStatus::Blocked(_) => {
                let blocked = WriteStatus::Blocked(block);
                Database::set_write_status_on(self.conn, write_id, &blocked)?;
                Ok(Some(blocked))
            }
            state @ (WriteStatus::LocalOnly | WriteStatus::Published(_)) => Err(DbError::Message(
                format!("write {write_id} cannot become blocked from {state:?}"),
            )),
        }
    }

    fn retry_blocked_write(
        &mut self,
        write_id: WriteId,
    ) -> Result<Vec<(WriteId, WriteStatus)>, DbError> {
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let (raw_status, prepared): (String, Option<String>) = tx
            .query_row(
                "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context(format!("blocked write {write_id} status"), error))?;
        if !matches!(status, WriteStatus::Blocked(_)) {
            return Err(DbError::Message(format!("write {write_id} is not blocked")));
        }
        if let Some(raw_prepared) = prepared.as_deref() {
            let prepared: PreparedStoreWriteState =
                serde_json::from_str(raw_prepared).map_err(|error| {
                    DbError::context(format!("blocked write {write_id} preparation"), error)
                })?;
            let candidate = crate::store::store_session::StoreTransaction::new(&tx, self.store_dir)
                .prepared_merge_candidate(self.verified_store_authority, &prepared)?
                .reference;
            let remote = load_remote_object_on(&tx, remote_object_id(&candidate.object))?;
            if matches!(
                remote,
                RemoteObjectRecord::CandidateCommit(
                    coven_protocol::remote_object::CandidateCommitRecord {
                        state:
                            coven_protocol::remote_object::CandidateCommitState::CleanupPending {
                                proof: coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner { .. }
                            }
                            | coven_protocol::remote_object::CandidateCommitState::AbsentVerified {
                                proof: coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner { .. }
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
        let next_json = serde_json::to_string(&next)
            .map_err(|error| DbError::context("serialize retry status", error))?;
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
        let retried = vec![(write_id, next)];
        tx.commit().map_err(DbError::from)?;
        Ok(retried)
    }

    fn discard_blocked_write(&mut self, write_id: WriteId) -> Result<BlockedWriteDiscard, DbError> {
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let (raw_status, target_ordinal): (String, i64) = tx
            .query_row(
                "SELECT status, ordinal FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let target_status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context(format!("blocked write {write_id} status"), error))?;
        if !matches!(target_status, WriteStatus::Blocked(_)) {
            return Err(DbError::Message(format!("write {write_id} is not blocked")));
        }

        let mut statement = tx
            .prepare(
                "SELECT write_id, status, changeset_hash FROM store_writes
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
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(DbError::from)?;
        let mut discarded = Vec::new();
        for row in rows {
            let (stored_id, raw_status, changeset_hash) = row.map_err(DbError::from)?;
            // The changeset is what reversing an unpublished write needs, and
            // only a folded write is without one. The fold stops at the first
            // unsettled write, so no write in this suffix can have been folded.
            let changeset_hash = changeset_hash.ok_or_else(|| {
                DbError::Message(format!("unpublished write {stored_id} has been folded"))
            })?;
            let status: WriteStatus = serde_json::from_str(&raw_status)
                .map_err(|error| DbError::context("discard write status", error))?;
            if !matches!(status, WriteStatus::Pending | WriteStatus::Blocked(_)) {
                return Err(DbError::Message(format!(
                    "write {stored_id} after blocked write {write_id} has non-discardable status {status:?}"
                )));
            }
            discarded.push((
                WriteId::from_generated(stored_id),
                changeset_hash.parse::<coven_protocol::store_commit::ObjectHash>()?,
            ));
        }
        drop(statement);
        if discarded.first().map(|(stored_id, _)| stored_id) != Some(&write_id) {
            return Err(DbError::Message(format!(
                "blocked write {write_id} is absent from its unpublished suffix"
            )));
        }
        for (discarded_id, _) in &discarded {
            if !crate::store::store_session::StoreTransaction::new(&tx, self.store_dir)
                .unpublished_write_cleanup_is_complete(
                    self.verified_store_authority,
                    discarded_id,
                )?
            {
                return Ok(BlockedWriteDiscard::RemoteResolutionRequired);
            }
        }
        let schema = Arc::new(crate::TableSchema::for_apply(
            &tx,
            self.synced_tables,
            self.gates,
        )?);
        let store_transaction =
            crate::store::store_session::StoreTransaction::new(&tx, self.store_dir);
        for (_, changeset_hash) in discarded.iter().rev() {
            let changeset = store_transaction.payload(*changeset_hash)?;
            let inverse = StoreDatabase::invert_changeset(&changeset)?;
            let inverse = crate::ValidatedChangeset::new(inverse, schema.clone())
                .map_err(|error| DbError::context("invalid blocked-write inverse", error))?;
            MergeMaterializationTransaction::from_store(
                crate::store::store_session::StoreTransaction::new(&tx, self.store_dir),
            )
            .apply_changeset_strict(inverse)
            .map_err(|error| DbError::context("reverse blocked-write suffix", error))?;
        }
        let discarded_ids: Vec<_> = discarded
            .into_iter()
            .map(|(write_id, _)| write_id)
            .collect();
        let resolution = WriteResolution::Discarded;
        crate::store::store_session::StoreTransaction::new(&tx, self.store_dir)
            .resolve_unpublished_writes(
                self.verified_store_authority,
                &discarded_ids,
                &resolution,
            )?;
        tx.commit().map_err(DbError::from)?;
        Ok(BlockedWriteDiscard::Discarded(discarded_ids))
    }
}

impl StoreDatabase {
    #[doc(hidden)]
    pub async fn pending_writes(&self) -> Result<Vec<PendingWrite>, DbError> {
        self.call_store(|session| session.pending_writes()).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn published_write_commits(
        &self,
    ) -> Result<Vec<coven_protocol::store_commit::StoreBatchCommitRef>, DbError> {
        self.call_store(|session| session.published_write_commits())
            .await
    }

    #[doc(hidden)]
    pub async fn blocked_writes(&self) -> Result<Vec<PendingWrite>, DbError> {
        Ok(self
            .pending_writes()
            .await?
            .into_iter()
            .filter(|write| matches!(write.status, WriteStatus::Blocked(_)))
            .collect())
    }

    #[doc(hidden)]
    pub async fn subscribe_write_status(
        &self,
        write_id: &WriteId,
    ) -> Result<tokio::sync::watch::Receiver<WriteStatus>, DbError> {
        let write_id = write_id.clone();
        let current = self
            .call_store({
                let write_id = write_id.clone();
                move |session| session.write_status(&write_id)
            })
            .await?;
        Ok(self.subscribe_store_write_status(write_id, current))
    }

    pub async fn set_write_status(
        &self,
        write_id: &WriteId,
        status: WriteStatus,
    ) -> Result<(), DbError> {
        let stored_id = write_id.clone();
        let stored_status = status.clone();
        self.call_store(move |session| session.set_write_status(&stored_id, &stored_status))
            .await?;
        self.notify_write_status(write_id.clone(), status);
        Ok(())
    }

    pub async fn block_write_if_unresolved(
        &self,
        write_id: &WriteId,
        block: coven_protocol::write::WriteBlock,
    ) -> Result<bool, DbError> {
        let write_id = write_id.clone();
        let notified_write_id = write_id.clone();
        let outcome = self
            .call_store(move |session| session.block_write_if_unresolved(&write_id, block))
            .await?;
        if let Some(status) = outcome {
            self.notify_write_status(notified_write_id, status);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Return one blocked write to publication. The next preparation attempt
    /// revalidates every captured fact; another semantic failure records a fresh
    /// `Blocked` status.
    #[doc(hidden)]
    pub async fn retry_blocked_write(&self, write_id: &WriteId) -> Result<Vec<WriteId>, DbError> {
        let write_id = write_id.clone();
        let retried = self
            .call_store(move |session| session.retry_blocked_write(write_id))
            .await?;
        let retried_ids = retried
            .iter()
            .map(|(write_id, _)| write_id.clone())
            .collect();
        for (write_id, status) in retried {
            self.notify_write_status(write_id, status);
        }
        Ok(retried_ids)
    }

    /// Atomically reverse a blocked write and every later unpublished shared
    /// write whose working-row state depends on it.
    #[doc(hidden)]
    pub async fn discard_blocked_write(
        &self,
        write_id: &WriteId,
    ) -> Result<BlockedWriteDiscard, DbError> {
        let write_id = write_id.clone();
        let discarded_ids = self
            .call_store(move |session| session.discard_blocked_write(write_id))
            .await?;
        if let BlockedWriteDiscard::Discarded(discarded_ids) = &discarded_ids {
            let status = WriteStatus::Resolved(WriteResolution::Discarded);
            for discarded_id in discarded_ids {
                self.notify_write_status(discarded_id.clone(), status.clone());
            }
        }
        Ok(discarded_ids)
    }
}
