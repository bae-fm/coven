//! Operation transactions, borrowing the database owner's connections.

use super::Database;
use crate::{DbError, NewOperation, OperationId, OperationRecord, OperationUpdate};

impl Database {
    /// Install a ready batch and its replay, data effects, drop reports, operation
    /// progress and S3 deletion obligations together. Reject a result prepared against
    /// another committed log, and a kept creation that disagrees with the directory
    /// settings (§9).
    /// The retained decoded value changes on the database thread after commit,
    /// including when the awaiting caller is cancelled or subsequent cleanup fails.
    pub async fn apply_store_log_batch<P: crate::PendingValue>(
        &self,
        checked: crate::StoreLog,
        entries: Vec<crate::ReplayEntry>,
        replay: crate::StoreLogReplay,
        operations: Vec<crate::NewOperation>,
        updates: Vec<crate::OperationUpdate>,
        access_keys: Vec<crate::AccessKeyDeletion>,
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
                // Identity is read back from the kept creation, so a replay naming
                // another one would not survive reopening.
                let created = next.created_store();
                if next.replay.state.store != created {
                    return Err(DbError::StoreLogEntriesChanged);
                }
                if let Some(created) = &created {
                    crate::store_log::check_creation(
                        created.id,
                        &created.name,
                        inner.access.directory.id(),
                        &inner.name,
                    )?;
                }
                let circles = next
                    .replay
                    .state
                    .devices
                    .get(&inner.access.device)
                    .map(|device| crate::entry_effects::CircleReads {
                        returning: current.returning_circles(&next.replay.state, &device.member),
                        readable: next.replay.state.readable_circles(&device.member),
                    })
                    .unwrap_or_default();
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
                            &circles,
                            files,
                            &operations,
                            &updates,
                            &access_keys,
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
                inner.access.with_writer(|writer| {
                    crate::store_log_upload::prepare(
                        writer,
                        log,
                        inner.access.directory.id(),
                        &inner.name,
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
        self.call(move |inner| inner.access.with_writer(crate::operation::read))
            .await
    }

    /// Read one committed journal row without loading unrelated operation data.
    /// Absence means the operation has finished or never existed.
    pub async fn operation(&self, id: OperationId) -> Result<Option<OperationRecord>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| crate::operation::find(writer, id))
        })
        .await
    }

    /// Read the earliest unfinished row of a sync-owned kind, including failures.
    pub async fn first_operation(&self, kind: &str) -> Result<Option<OperationRecord>, DbError> {
        let kind = kind.to_owned();
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| crate::operation::first(writer, &kind))
        })
        .await
    }

    /// Commit the operation before its first step.
    pub async fn start_operation(&self, operation: NewOperation) -> Result<OperationId, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| crate::operation::insert(db, &operation))
            })
        })
        .await
    }

    /// Commit a local step's state after an idempotent storage effect.
    pub async fn advance_operation(&self, update: OperationUpdate) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
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
        self.call(move |inner| {
            inner
                .access
                .with_writer(|db| crate::pending::operation(db, id))
        })
        .await
    }

    /// Clear a condition for an explicit retry; completed steps stay recorded.
    pub async fn retry_operation(&self, id: OperationId) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
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
            inner.access.with_writer(|writer| {
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
            inner
                .access
                .with_writer(|writer| writer.transaction(|db| finish(db, id).map(drop)))
        })
        .await
    }

    /// Retire an operation together with the work its last step leaves: one
    /// new journal row per follow-up, each committed with the pending condition
    /// built from its new id. Nothing commits unless all of it does.
    pub async fn finish_operation_with_work<P, F>(
        &self,
        id: OperationId,
        follow_ups: Vec<(NewOperation, F)>,
    ) -> Result<(), DbError>
    where
        P: crate::PendingValue,
        F: FnOnce(OperationId) -> P + Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    for (operation, pending) in follow_ups {
                        let started = crate::operation::insert(db, &operation)?;
                        let pending = pending(started);
                        if pending.operation() != Some(started)
                            || pending.reported_by() != inner.access.device
                        {
                            return Err(DbError::DamagedDatabase);
                        }
                        crate::pending::save(db, &pending)?;
                    }
                    // Follow-ups belong to this step; they never outlive a lost row.
                    if finish(db, id)? != 1 {
                        return Err(DbError::OperationChanged(id));
                    }
                    Ok(())
                })
            })
        })
        .await
    }

    /// Commit an operation's step together with new S3 deletion obligations,
    /// each with its own operation and pending notice. Keys already open or
    /// confirmed deleted keep their state (§13).
    pub async fn record_access_key_deletions<P: crate::PendingValue>(
        &self,
        update: Option<OperationUpdate>,
        deletions: Vec<crate::AccessKeyDeletion>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    if let Some(update) = &update {
                        crate::operation::advance(db, update)?;
                    }
                    crate::operation::record_access_keys::<P>(db, inner.access.device, &deletions)
                })
            })
        })
        .await
    }

    /// Retire an operation together with the S3 deletion obligations for keys
    /// it made but never recorded, so no key outlives both its journal row and
    /// a notice. Nothing commits unless all of it does (§13).
    pub async fn finish_operation_with_access_keys<P: crate::PendingValue>(
        &self,
        id: OperationId,
        deletions: Vec<crate::AccessKeyDeletion>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    crate::operation::record_access_keys::<P>(db, inner.access.device, &deletions)?;
                    if finish(db, id)? != 1 {
                        return Err(DbError::OperationChanged(id));
                    }
                    Ok(())
                })
            })
        })
        .await
    }

    /// Clear an S3 key's deletion obligation, after an explicit confirmation or
    /// a verified provider deletion, and retain the confirmation by key id.
    /// Confirmation before any notice prevents that notice (§13).
    pub async fn clear_access_key(&self, key: String) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| crate::operation::clear_access_key(db, &key))
            })
        })
        .await
    }
}

/// Delete the row and its condition, returning how many rows were deleted.
fn finish(db: &crate::sqlite::DatabaseConnection, id: OperationId) -> Result<usize, DbError> {
    crate::pending::clear_operation(db, id)?;
    db.internal_execute("DELETE FROM _coven_operations WHERE id=?1", [id.0])
}
