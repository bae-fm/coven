//! Operation transactions, borrowing the database owner's connections.

use super::Database;
use crate::{DbError, NewOperation, OperationId, OperationRecord, OperationUpdate};

impl Database {
    /// Install a ready batch and its replay, data effects, drop reports and operation
    /// progress together. Reject a result prepared against another committed log.
    /// The retained decoded value changes on the database thread after commit,
    /// including when the awaiting caller is cancelled or subsequent cleanup fails.
    pub async fn apply_store_log_batch<P: crate::PendingValue>(
        &self,
        checked: crate::StoreLog,
        entries: Vec<crate::ReplayEntry>,
        replay: crate::StoreLogReplay,
        operations: Vec<crate::NewOperation>,
        updates: Vec<crate::OperationUpdate>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_store_log(|current| {
                if *current != checked {
                    return Err(DbError::StoreLogEntriesChanged);
                }
                let mut next = checked;
                for entry in &entries {
                    match next
                        .entries
                        .iter()
                        .find(|old| old.entry.position == entry.entry.position)
                    {
                        Some(old) if old != entry => {
                            return Err(DbError::StoreLogEntryChanged(entry.entry.position))
                        }
                        Some(_) => (),
                        None => next.entries.push(entry.clone()),
                    }
                }
                next.entries.sort_by_key(|entry| entry.entry.timestamp);
                next.replay = replay;
                if !next
                    .entries
                    .iter()
                    .map(|entry| entry.entry.position)
                    .collect::<std::collections::BTreeSet<_>>()
                    .iter()
                    .eq(next.replay.entries.keys())
                {
                    return Err(DbError::StoreLogEntriesChanged);
                }
                let changes_projection = next.boundaries().ne(current.boundaries())
                    || next.replay.state.circles != current.replay.state.circles
                    || next.replay.state.circle_deletions != current.replay.state.circle_deletions;
                let reader = changes_projection.then(|| inner.access.readers.acquire_reader());
                inner.with_files(Vec::new(), |writer, files| {
                    let apply = |before: &crate::sqlite::DatabaseConnection| {
                        crate::store_log::apply::<P>(
                            writer,
                            before,
                            &inner.access.write_schema,
                            &entries,
                            &next.replay,
                            files,
                            &operations,
                            &updates,
                        )
                    };
                    if let Some(reader) = reader {
                        reader.with_reader(|reader| {
                            reader.read_transaction(|| {
                                inner.access.write_schema.prepare(reader)?;
                                apply(reader)
                            })
                        })?;
                    } else {
                        apply(writer)?;
                    }
                    *current = next;
                    #[cfg(any(test, feature = "test-utils"))]
                    writer.record_work(|work| work.store_log_commits += 1);
                    Ok(())
                })
            })
        })
        .await
    }

    /// Prepare a store-log entry and advance its operation in one transaction.
    /// The sealing closure receives only values and must also encode the entry’s
    /// identity and author view into the operation data it returns.
    pub async fn prepare_operation_entry<F, E>(
        &self,
        author: coven_crypto::MemberId,
        change: coven_format::store_log::StoreChange,
        seal: F,
    ) -> Result<crate::EntryId, E>
    where
        F: FnOnce(
                &crate::StoreLog,
                &coven_format::store_log::StoreLogEntry,
                crate::KeyUploadBatch,
            ) -> Result<(crate::StoreLogSealing, OperationUpdate), E>
            + Send
            + 'static,
        E: From<DbError> + Send + 'static,
    {
        self.call(move |inner| {
            inner.with_store_log(|log| {
                inner.with_writer(|writer| {
                    crate::store_log_upload::prepare(
                        writer,
                        log,
                        inner.access.directory.id(),
                        inner.access.device,
                        inner.clock.now(),
                        author,
                        change,
                        move |log, entry, copies| {
                            seal(log, entry, copies).map(|(sealed, update)| (sealed, Some(update)))
                        },
                    )
                })
            })
        })
        .await
    }

    /// Read every unfinished operation, including permanently failed ones.
    pub async fn operations(&self) -> Result<Vec<OperationRecord>, DbError> {
        self.call(move |inner| inner.with_writer(crate::operation::read))
            .await
    }

    /// Read one committed journal row without loading unrelated operation data.
    /// Absence means the operation has finished or never existed.
    pub async fn operation(&self, id: OperationId) -> Result<Option<OperationRecord>, DbError> {
        self.call(move |inner| inner.with_writer(|writer| crate::operation::find(writer, id)))
            .await
    }

    /// Read the earliest unfinished row of a sync-owned kind, including failures.
    pub async fn first_operation(&self, kind: &str) -> Result<Option<OperationRecord>, DbError> {
        let kind = kind.to_owned();
        self.call(move |inner| inner.with_writer(|writer| crate::operation::first(writer, &kind)))
            .await
    }

    /// Commit the operation before its first step.
    pub async fn start_operation(&self, operation: NewOperation) -> Result<OperationId, DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                writer.transaction(|db| crate::operation::insert(db, &operation))
            })
        })
        .await
    }

    /// Commit a local step's state after an idempotent storage effect.
    pub async fn advance_operation(&self, update: OperationUpdate) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                writer.transaction(|db| crate::operation::advance(db, &update))
            })
        })
        .await
    }

    /// Read the pending condition associated with a journal row.
    pub async fn operation_pending<P: crate::PendingValue>(
        &self,
        id: OperationId,
    ) -> Result<Option<P>, DbError> {
        self.call(move |inner| inner.with_writer(|db| crate::pending::operation(db, id)))
            .await
    }

    /// Clear a condition for an explicit retry; completed steps stay recorded.
    pub async fn retry_operation(&self, id: OperationId) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                writer.transaction(|db| crate::pending::clear_operation(db, id))
            })
        })
        .await
    }

    /// Commit completed work and its first remaining condition in one transaction.
    pub async fn advance_operation_pending<P: crate::PendingValue>(
        &self,
        update: OperationUpdate,
        pending: P,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                if pending.operation() != Some(update.id)
                    || pending.reported_by() != inner.access.device
                {
                    return Err(DbError::DamagedDatabase);
                }
                writer.transaction(|db| {
                    crate::operation::advance(db, &update)?;
                    crate::pending::save(db, &pending)
                })
            })
        })
        .await
    }

    /// Retire a finished operation. The caller must publish every reserved entry
    /// before discarding; removing this row cannot retire the publication queue.
    pub async fn finish_operation(&self, id: OperationId) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                writer.transaction(|db| {
                    crate::pending::clear_operation(db, id)?;
                    db.internal_execute("DELETE FROM _coven_operations WHERE id=?1", [id.0])?;
                    Ok(())
                })
            })
        })
        .await
    }

    /// Persist public S3 key ids with their operation step. Previously confirmed
    /// ids remain confirmed even when another entry or invitation names them.
    /// A retained-grant condition commits with the same completed step.
    pub async fn record_access_key_deletions<P: crate::PendingValue>(
        &self,
        update: OperationUpdate,
        keys: Vec<crate::AccessKeyToDelete>,
        pending: Option<P>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                writer.transaction(|db| {
                    for key in keys {
                        db.internal_execute("INSERT INTO _coven_access_keys_to_delete(access_key_id,member) VALUES(?1,?2) ON CONFLICT(access_key_id) DO UPDATE SET member=excluded.member WHERE excluded.member IS NOT NULL", (key.access_key_id, key.member.map(|m| m.to_bytes().to_vec())))?;
                    }
                    crate::operation::advance(db, &update)?;
                    if let Some(pending) = pending {
                        if pending.operation() != Some(update.id)
                            || pending.reported_by() != inner.access.device {
                            return Err(DbError::DamagedDatabase);
                        }
                        crate::pending::save(db, &pending)?;
                    }
                    Ok(())
                })
            })
        })
        .await
    }

    /// The durable provider-console actions still awaiting confirmation.
    pub async fn access_keys_to_delete(&self) -> Result<Vec<crate::AccessKeyToDelete>, DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                writer.query(
                    "SELECT access_key_id,member FROM _coven_access_keys_to_delete WHERE confirmed=0 ORDER BY access_key_id",
                    [],
                    |r| Ok(crate::AccessKeyToDelete {
                        access_key_id: r.get(0)?,
                        member: r.get::<_, Option<[u8; 32]>>(1)?.map(coven_crypto::MemberId::from_bytes).transpose().map_err(|e| crate::write_encoding::decode_failure(1, rusqlite::types::Type::Blob, e))?,
                    }),
                )
            })
        })
        .await
    }

    /// Remember an S3 key deletion, including confirmation before its notice.
    /// Repeating confirmation or replaying an access entry cannot revive it.
    pub async fn confirm_access_key_deleted(&self, key: String) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute(
                        "INSERT INTO _coven_access_keys_to_delete(access_key_id,confirmed) VALUES(?1,1) ON CONFLICT(access_key_id) DO UPDATE SET confirmed=1",
                        [key],
                    )?;
                    Ok(())
                })
            })
        })
        .await
    }
}
