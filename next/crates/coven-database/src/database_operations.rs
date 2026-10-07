//! Operation transactions, borrowing the database owner's connections.

use super::{finish_blocking, Database};
use crate::{
    DbError, NewOperation, OperationCommit, OperationId, OperationRecord, OperationUpdate,
};

impl Database {
    /// Apply an entry and record work caused by its kept effects atomically.
    /// The sync owner supplies the operation data; the database does not interpret it.
    pub async fn apply_store_log_operations(
        &self,
        entry: crate::ReplayEntry,
        replay: crate::StoreLogReplay,
        operations: Vec<crate::NewOperation>,
        updates: Vec<crate::OperationUpdate>,
    ) -> Result<(), DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                let files = crate::file_write::FileWrite::new(
                    &writer,
                    &inner.directory,
                    &inner.write_schema,
                    inner.device,
                    &inner.staging,
                    Vec::new(),
                );
                files.finish(crate::store_log::apply(
                    &writer,
                    &inner.write_schema,
                    entry,
                    replay,
                    &files,
                    &operations,
                    &updates,
                ))
            })
            .await,
        )
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
            ) -> Result<(crate::SealedStoreLog, OperationUpdate), E>
            + Send
            + 'static,
        E: From<DbError> + Send + 'static,
    {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                crate::store_log_upload::prepare(
                    &writer,
                    inner.device,
                    inner.clock.now(),
                    author,
                    change,
                    move |log, entry| {
                        seal(log, entry).map(|(sealed, update)| (sealed, Some(update)))
                    },
                )
            })
            .await,
        )
    }

    /// Commit circle row deletions, their queued write and their operation together.
    /// The callback records the write dependency when starting or restarting the operation.
    pub async fn delete_circle_rows<F>(
        &self,
        circle: coven_foundation::id_source::CircleId,
        build: F,
    ) -> Result<OperationId, DbError>
    where
        F: FnOnce(Option<coven_merge::WriteId>) -> Result<OperationCommit, DbError>
            + Send
            + 'static,
    {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                let files = crate::file_write::FileWrite::new(
                    &writer,
                    &inner.directory,
                    &inner.write_schema,
                    inner.device,
                    &inner.staging,
                    Vec::new(),
                );
                files.finish(writer.transaction(|db| {
                    let write = crate::circle_deletion::write(
                        db,
                        &inner.write_schema,
                        inner.device,
                        inner.clock.now(),
                        circle,
                        &files,
                    )?;
                    match build(write)? {
                        OperationCommit::Start(new) => {
                            let id = crate::operation::insert(db, &new)?;
                            crate::operation::advance(
                                db,
                                &OperationUpdate {
                                    id,
                                    previous: 0,
                                    last_step: 1,
                                    data: new.data,
                                },
                            )?;
                            Ok(id)
                        }
                        OperationCommit::Advance(update) => {
                            crate::operation::advance(db, &update)?;
                            Ok(update.id)
                        }
                    }
                }))
            })
            .await,
        )
    }

    /// Whether a circle deletion’s write has left the device-log upload queue.
    /// Device-log transport retires that row only after publishing its fixed bytes.
    pub async fn write_is_uploaded(&self, write: coven_merge::WriteId) -> Result<bool, DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                writer.query_row(
                    "SELECT NOT EXISTS(SELECT 1 FROM coven_uploads WHERE device=?1 AND number=?2)",
                    (
                        write.device.0.to_be_bytes().as_slice(),
                        write.number.to_be_bytes().as_slice(),
                    ),
                    |r| r.get(0),
                )
            })
            .await,
        )
    }

    /// Read every unfinished operation, including permanently failed ones.
    pub async fn operations(&self) -> Result<Vec<OperationRecord>, DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                crate::operation::read(&writer)
            })
            .await,
        )
    }

    /// Commit the operation before its first step.
    pub async fn start_operation(&self, operation: NewOperation) -> Result<OperationId, DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                writer.transaction(|db| crate::operation::insert(db, &operation))
            })
            .await,
        )
    }

    /// Commit a local step's state after an idempotent storage effect.
    pub async fn advance_operation(&self, update: OperationUpdate) -> Result<(), DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                writer.transaction(|db| crate::operation::advance(db, &update))
            })
            .await,
        )
    }

    /// Record a permanent failure, or clear it for an explicit retry.
    pub async fn operation_failure(
        &self,
        id: OperationId,
        failure: Option<String>,
    ) -> Result<(), DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                writer.transaction(|db| {
                    if db.internal_execute(
                        "UPDATE coven_operations SET failure=?1 WHERE id=?2",
                        (failure, id.0),
                    )? != 1
                    {
                        return Err(DbError::OperationChanged(id));
                    }
                    Ok(())
                })
            })
            .await,
        )
    }

    /// Retire a finished operation. The caller must publish every reserved entry
    /// before discarding; removing this row cannot retire the publication queue.
    pub async fn finish_operation(&self, id: OperationId) -> Result<(), DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                writer.transaction(|db| {
                    db.internal_execute("DELETE FROM coven_operations WHERE id=?1", [id.0])?;
                    Ok(())
                })
            })
            .await,
        )
    }

    /// Persist public S3 key ids with their operation step. Previously confirmed
    /// ids remain confirmed even when another entry or invitation names them.
    pub async fn record_access_key_deletions(
        &self,
        update: OperationUpdate,
        keys: Vec<crate::AccessKeyToDelete>,
    ) -> Result<(), DbError> {
        let owner = self.clone();
        finish_blocking(tokio::task::spawn_blocking(move || {
            let slot = owner.inner.read().expect("database lock poisoned");
            let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
            let writer = inner.writer.lock().expect("writer lock poisoned");
            writer.transaction(|db| {
                for key in keys {
                    db.internal_execute("INSERT INTO coven_access_keys_to_delete(access_key_id,member) VALUES(?1,?2) ON CONFLICT(access_key_id) DO UPDATE SET member=excluded.member WHERE excluded.member IS NOT NULL", (key.access_key_id, key.member.map(|m| m.to_bytes().to_vec())))?;
                }
                crate::operation::advance(db, &update)
            })
        }).await)
    }

    /// The durable provider-console actions still awaiting confirmation.
    pub async fn access_keys_to_delete(&self) -> Result<Vec<crate::AccessKeyToDelete>, DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                writer.query(
                    "SELECT access_key_id,member FROM coven_access_keys_to_delete WHERE confirmed=0 ORDER BY access_key_id",
                    [],
                    |r| Ok(crate::AccessKeyToDelete {
                        access_key_id: r.get(0)?,
                        member: r.get::<_, Option<[u8; 32]>>(1)?.map(coven_crypto::MemberId::from_bytes).transpose().map_err(|e| rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Blob, Box::new(e)))?,
                    }),
                )
            })
            .await,
        )
    }

    /// Remember an S3 key deletion, including confirmation before its notice.
    /// Repeating confirmation or replaying an access entry cannot revive it.
    pub async fn confirm_access_key_deleted(&self, key: String) -> Result<(), DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                writer.transaction(|db| {
                    db.internal_execute(
                        "INSERT INTO coven_access_keys_to_delete(access_key_id,confirmed) VALUES(?1,1) ON CONFLICT(access_key_id) DO UPDATE SET confirmed=1",
                        [key],
                    )?;
                    Ok(())
                })
            })
            .await,
        )
    }
}
