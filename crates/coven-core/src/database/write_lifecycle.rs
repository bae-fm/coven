use crate::database::store_device_state::store_serial_predecessor_on;

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

    pub(super) fn set_write_status_on(
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
                WriteStatus::Pending
                | WriteStatus::Publishing
                | WriteStatus::Blocked(_)
                | WriteStatus::Conflict(_) => {
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
                        OR json_extract(status, '$.conflict') IS NOT NULL
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

    /// Return one blocked write, or its whole Serial branch, to production
    /// publication. The next preparation attempt revalidates every captured fact;
    /// another semantic failure records a fresh `Blocked` status.
    pub async fn retry_blocked_write(&self, write_id: &WriteId) -> Result<Vec<WriteId>, DbError> {
        let write_id = write_id.clone();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let (raw_status, raw_base, prepared): (String, String, Option<String>) = tx
                .query_row(
                    "SELECT status, base, prepared FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(DbError::from)?;
            let status: WriteStatus = serde_json::from_str(&raw_status)
                .map_err(|error| DbError::Message(format!("blocked write {write_id} status: {error}")))?;
            if !matches!(status, WriteStatus::Blocked(_)) {
                return Err(DbError::Message(format!("write {write_id} is not blocked")));
            }
            let base: StoreWriteBase = serde_json::from_str(&raw_base)
                .map_err(|error| DbError::Message(format!("blocked write {write_id} base: {error}")))?;

            let mut retried = Vec::new();
            match base {
                StoreWriteBase::MergeConcurrent { .. } => {
                    if let Some(raw_prepared) = prepared.as_deref() {
                        let prepared: PreparedStoreWriteState = serde_json::from_str(raw_prepared)
                            .map_err(|error| {
                                DbError::Message(format!("blocked write {write_id} preparation: {error}"))
                            })?;
                        if !matches!(prepared, PreparedStoreWriteState::MergeConcurrent { .. }) {
                            return Err(DbError::Message(format!(
                                "blocked MergeConcurrent write {write_id} has Serial preparation"
                            )));
                        }
                        let candidate = parse_prepared_merge_candidate_on(&tx, &prepared)?
                            .expect("matched Merge preparation")
                            .reference;
                        let remote =
                            load_remote_object_on(&tx, remote_object_id(&candidate.object))?;
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
                    let next_json = serde_json::to_string(&next)
                        .map_err(|error| DbError::Message(format!("serialize retry status: {error}")))?;
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
                }
                StoreWriteBase::Serial {
                    branch_id,
                    base: branch_base,
                } => {
                    if prepared.is_some() {
                        return Err(DbError::Message(format!(
                            "blocked Serial branch {} retains publication preparation",
                            branch_id.first_write_id()
                        )));
                    }
                    let expected_base = StoreWriteBase::Serial {
                        branch_id: branch_id.clone(),
                        base: branch_base,
                    };
                    let mut statement = tx
                        .prepare(
                            "SELECT write_id, status, base, prepared FROM store_writes
                             WHERE base = ?1
                               AND status != '\"local_only\"'
                               AND json_extract(status, '$.published') IS NULL
                               AND json_extract(status, '$.resolved') IS NULL
                             ORDER BY ordinal",
                        )
                        .map_err(DbError::from)?;
                    let rows = statement
                        .query_map([&raw_base], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, Option<String>>(3)?,
                            ))
                        })
                        .map_err(DbError::from)?;
                    let mut branch_write_ids = Vec::new();
                    for row in rows {
                        let (stored_id, raw_status, raw_base, prepared) =
                            row.map_err(DbError::from)?;
                        let stored_base: StoreWriteBase = serde_json::from_str(&raw_base)
                            .map_err(|error| DbError::Message(format!("Serial retry base: {error}")))?;
                        if stored_base != expected_base {
                            return Err(DbError::Message(
                                "Serial database contains more than one unresolved branch"
                                    .to_string(),
                            ));
                        }
                        let stored_status: WriteStatus = serde_json::from_str(&raw_status)
                            .map_err(|error| DbError::Message(format!("Serial retry status: {error}")))?;
                        if !matches!(stored_status, WriteStatus::Pending | WriteStatus::Blocked(_))
                            || prepared.is_some()
                        {
                            return Err(DbError::Message(format!(
                                "Serial branch write {stored_id} is not pending or blocked without preparation"
                            )));
                        }
                        if matches!(stored_status, WriteStatus::Blocked(_)) {
                            branch_write_ids.push(WriteId::from_generated(stored_id));
                        }
                    }
                    drop(statement);
                    let pending = serde_json::to_string(&WriteStatus::Pending)
                        .map_err(|error| DbError::Message(format!("serialize retry status: {error}")))?;
                    for branch_write_id in branch_write_ids {
                        let updated = tx
                            .execute(
                                "UPDATE store_writes SET status = ?2
                                 WHERE write_id = ?1
                                   AND json_extract(status, '$.blocked') IS NOT NULL
                                   AND prepared IS NULL",
                                rusqlite::params![branch_write_id.as_str(), &pending],
                            )
                            .map_err(DbError::from)?;
                        if updated != 1 {
                            return Err(DbError::Message(format!(
                                "blocked Serial write {branch_write_id} changed during retry"
                            )));
                        }
                        retried.push((branch_write_id, WriteStatus::Pending));
                    }
                }
            }
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

    pub async fn pending_branches(&self) -> Result<Option<PendingBranch>, DbError> {
        self.call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT write_id, status, affected_rows, base FROM store_writes
                     WHERE status IN ('\"pending\"', '\"publishing\"')
                        OR json_extract(status, '$.blocked') IS NOT NULL
                        OR json_extract(status, '$.conflict') IS NOT NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(DbError::from)?;
            let mut records = Vec::new();
            for row in rows {
                let (write_id, status, affected_rows, base) = row.map_err(DbError::from)?;
                records.push((
                    PendingWrite {
                        write_id: WriteId::from_generated(write_id),
                        status: serde_json::from_str(&status).map_err(|error| {
                            DbError::Message(format!("pending write status: {error}"))
                        })?,
                        affected_rows: serde_json::from_str(&affected_rows).map_err(|error| {
                            DbError::Message(format!("pending affected rows: {error}"))
                        })?,
                    },
                    serde_json::from_str::<StoreWriteBase>(&base).map_err(|error| {
                        DbError::Message(format!("pending write base: {error}"))
                    })?,
                ));
            }
            drop(statement);

            let mut conflict = None;
            let mut conflict_exact_base = None;
            for (write, base) in &records {
                let WriteStatus::Conflict(candidate) = &write.status else {
                    continue;
                };
                let StoreWriteBase::Serial {
                    branch_id,
                    base: stored_base,
                } = base
                else {
                    return Err(DbError::Message(
                        "MergeConcurrent base carries a Serial conflict".to_string(),
                    ));
                };
                let exact_base = store_serial_predecessor_on(conn, stored_base.as_ref())?;
                if branch_id != &candidate.branch_id || exact_base != candidate.base {
                    return Err(DbError::Message(
                        "Serial conflict status differs from its durable branch base".to_string(),
                    ));
                }
                match &conflict {
                    None => {
                        conflict = Some(candidate.clone());
                        conflict_exact_base = Some(stored_base.clone());
                    }
                    Some(existing) if existing == candidate => {}
                    Some(_) => {
                        return Err(DbError::Message(
                            "Serial database contains more than one conflict branch".to_string(),
                        ))
                    }
                }
            }
            let Some(conflict) = conflict else {
                return Ok(None);
            };
            let conflict = *conflict;
            let expected_base = StoreWriteBase::Serial {
                branch_id: conflict.branch_id.clone(),
                base: conflict_exact_base.expect("conflict row carries exact base"),
            };
            let mut writes = Vec::new();
            for (write, base) in records {
                if matches!(base, StoreWriteBase::MergeConcurrent { .. }) {
                    continue;
                }
                if base != expected_base {
                    return Err(DbError::Message(
                        "Serial database contains more than one unresolved branch".to_string(),
                    ));
                }
                if !matches!(
                    &write.status,
                    WriteStatus::Conflict(_) | WriteStatus::Pending
                ) {
                    return Err(DbError::Message(format!(
                        "conflicted Serial branch write {} has non-resolvable status {:?}",
                        write.write_id, write.status
                    )));
                }
                writes.push(write);
            }
            Ok(Some(PendingBranch {
                branch_id: conflict.branch_id,
                base: conflict.base,
                current: conflict.current,
                writes,
            }))
        })
        .await
    }

    pub async fn serial_branch_discard_state(
        &self,
        branch_id: &PendingBranchId,
    ) -> Result<SerialBranchDiscardState, DbError> {
        let branch_id = branch_id.clone();
        self.call(move |conn| {
            let abandonment: Option<String> = conn
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [SERIAL_CANDIDATE_ABANDONMENT_STATE_KEY],
                    |row| row.get(0),
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some(abandonment) = abandonment {
                let abandonment: DurableSerialCandidateAbandonment =
                    serde_json::from_str(&abandonment).map_err(|error| {
                        DbError::Message(format!("Serial candidate abandonment: {error}"))
                    })?;
                if abandonment.branch_id != branch_id {
                    return Err(DbError::Message(
                        "another Serial branch owns candidate abandonment".to_string(),
                    ));
                }
                return Ok(SerialBranchDiscardState::Abandonment);
            }
            let mut statement = conn
                .prepare(
                    "SELECT base, status, prepared FROM store_writes
                     WHERE status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                       AND json_type(base, '$.serial') IS NOT NULL
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
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            drop(statement);
            if rows.is_empty() {
                return Err(DbError::Message("Serial branch is absent".to_string()));
            }
            let mut base = None;
            let mut saw_local = false;
            let mut saw_prepared = false;
            let mut saw_conflict = false;
            for (raw_base, raw_status, raw_prepared) in rows {
                let StoreWriteBase::Serial {
                    branch_id: stored_branch_id,
                    base: stored_base,
                } = serde_json::from_str(&raw_base).map_err(|error| {
                    DbError::Message(format!("Serial discard branch base: {error}"))
                })?
                else {
                    unreachable!("selected Serial base")
                };
                if stored_branch_id != branch_id {
                    return Err(DbError::Message(
                        "Serial database contains another unresolved branch".to_string(),
                    ));
                }
                if base.as_ref().is_some_and(|base| base != &stored_base) {
                    return Err(DbError::Message(
                        "Serial discard branch has inconsistent bases".to_string(),
                    ));
                }
                base.get_or_insert(stored_base);
                let status: WriteStatus = serde_json::from_str(&raw_status)
                    .map_err(|error| DbError::Message(format!("Serial discard status: {error}")))?;
                match status {
                    WriteStatus::Pending | WriteStatus::Blocked(_) if raw_prepared.is_none() => {
                        saw_local = true;
                    }
                    WriteStatus::Publishing if raw_prepared.is_some() => saw_prepared = true,
                    WriteStatus::Conflict(conflict) if conflict.branch_id == branch_id => {
                        saw_conflict = true;
                    }
                    other => {
                        return Err(DbError::Message(format!(
                            "Serial branch has non-discardable state {other:?}"
                        )));
                    }
                }
            }
            match (saw_local, saw_prepared, saw_conflict) {
                (true, false, false) => Ok(SerialBranchDiscardState::Local),
                (false, true, false) => Ok(SerialBranchDiscardState::Abandonment),
                (false, false, true) => {
                    base.expect("nonempty branch");
                    Ok(SerialBranchDiscardState::Conflict)
                }
                _ => Err(DbError::Message(
                    "Serial branch mixes incompatible discard states".to_string(),
                )),
            }
        })
        .await
    }

    pub async fn discard_local_serial_branch(
        &self,
        branch_id: PendingBranchId,
    ) -> Result<Vec<WriteId>, DbError> {
        let synced_tables = self.synced_tables().to_vec();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let mut statement = tx
                .prepare(
                    "SELECT write_id, base, status, prepared, inverse_changeset
                     FROM store_writes
                     WHERE status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                       AND json_type(base, '$.serial') IS NOT NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                    ))
                })
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            drop(statement);
            if rows.is_empty() {
                return Err(DbError::Message("Serial branch is absent".to_string()));
            }
            let mut branch = Vec::new();
            for (write_id, raw_base, raw_status, prepared, inverse) in rows {
                let StoreWriteBase::Serial {
                    branch_id: stored_branch_id,
                    ..
                } = serde_json::from_str(&raw_base).map_err(|error| {
                    DbError::Message(format!("local Serial discard base: {error}"))
                })?
                else {
                    unreachable!("selected Serial base")
                };
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("local Serial discard status: {error}"))
                })?;
                if stored_branch_id != branch_id
                    || !matches!(status, WriteStatus::Pending | WriteStatus::Blocked(_))
                    || prepared.is_some()
                {
                    return Err(DbError::Message(
                        "Serial branch is not locally discardable".to_string(),
                    ));
                }
                branch.push((WriteId::from_generated(write_id), inverse));
            }
            let schema = Arc::new(crate::sync::conflict::TableSchema::from_db(
                &tx,
                &synced_tables,
            )?);
            for (_, inverse) in branch.iter().rev() {
                let inverse = crate::sync::apply::ValidatedChangeset::new(inverse, schema.clone())
                    .map_err(|error| {
                        DbError::Message(format!("invalid Serial inverse: {error}"))
                    })?;
                crate::sync::apply::apply_changeset_strict_on(&tx, inverse)
                    .map_err(|error| DbError::Message(format!("reverse Serial branch: {error}")))?;
            }
            let write_ids = branch
                .into_iter()
                .map(|(write_id, _)| write_id)
                .collect::<Vec<_>>();
            let resolution = WriteResolution::Discarded;
            Self::resolve_unpublished_writes_on(&tx, &write_ids, &resolution)?;
            tx.commit().map_err(DbError::from)?;
            let status = WriteStatus::Resolved(resolution);
            for write_id in &write_ids {
                Self::notify_write_status_in(&statuses, write_id, status.clone());
            }
            Ok(write_ids)
        })
        .await
    }

    pub async fn conflicted_serial_branch_base(
        &self,
        branch_id: &PendingBranchId,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        let branch = self.unresolved_serial_branch().await?.ok_or_else(|| {
            DbError::Message(format!(
                "Serial branch {} does not exist",
                branch_id.first_write_id()
            ))
        })?;
        if &branch.branch_id != branch_id || !branch.conflicted {
            return Err(DbError::Message(format!(
                "Serial branch {} is not conflicted",
                branch_id.first_write_id()
            )));
        }
        Ok(branch.base)
    }

    pub(crate) async fn unresolved_serial_branch(
        &self,
    ) -> Result<Option<UnresolvedSerialBranch>, DbError> {
        self.call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT base, status FROM store_writes
                     WHERE status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                       AND json_type(base, '$.serial') IS NOT NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?;
            let mut branch = None;
            for row in rows {
                let (raw_base, raw_status) = row.map_err(DbError::from)?;
                let StoreWriteBase::Serial { branch_id, base } = serde_json::from_str(&raw_base)
                    .map_err(|error| {
                        DbError::Message(format!("unresolved Serial base: {error}"))
                    })?
                else {
                    return Err(DbError::Message(
                        "MergeConcurrent base reached a Serial branch query".to_string(),
                    ));
                };
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("unresolved Serial status: {error}"))
                })?;
                match &mut branch {
                    None => {
                        branch = Some(UnresolvedSerialBranch {
                            branch_id,
                            base,
                            conflicted: matches!(status, WriteStatus::Conflict(_)),
                        });
                    }
                    Some(existing) if existing.branch_id == branch_id && existing.base == base => {
                        existing.conflicted |= matches!(status, WriteStatus::Conflict(_));
                    }
                    Some(_) => {
                        return Err(DbError::Message(
                            "Serial database contains more than one unresolved branch".to_string(),
                        ));
                    }
                }
            }
            Ok(branch)
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
