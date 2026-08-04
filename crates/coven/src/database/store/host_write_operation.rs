use crate::blob::local_cleanup::LocalBlobCleanupIntent;
use crate::blob::BlobRef;
use crate::database::DbError;
use crate::encryption::EncryptionService;
use crate::store_dir::{PathTokenError, StoreDir};
use crate::WriteReceipt;

use super::host_sql_transaction::HostSqlTransaction;
use super::{SqlContext, StoreDatabase};

pub struct WriteBatch {
    new_blobs: Vec<NewBlob>,
    deleted_blobs: Vec<BlobRef>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            new_blobs: Vec::new(),
            deleted_blobs: Vec::new(),
        }
    }

    pub fn put_blob(
        &mut self,
        namespace: impl Into<String>,
        id: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) {
        self.new_blobs.push(NewBlob {
            namespace: namespace.into(),
            id: id.into(),
            bytes: bytes.into(),
        });
    }

    pub fn delete_blob(&mut self, blob: BlobRef) {
        self.deleted_blobs.push(blob);
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

struct NewBlob {
    namespace: String,
    id: String,
    bytes: Vec<u8>,
}

struct StagedBlob {
    namespace: String,
    id: String,
    staged: Option<crate::local_file::AtomicStagedFile>,
    published: Option<crate::local_file::PublishedAtomicFile>,
}

struct StagedBlobBatch {
    blobs: Vec<StagedBlob>,
}

impl StagedBlob {
    async fn stage<E>(store_dir: &StoreDir, blob: NewBlob) -> Result<Self, HostWriteError<E>> {
        let destination = store_dir.local_blob_path(&blob.namespace, &blob.id)?;
        let staged = crate::local_file::AtomicStagedFile::create(&destination)
            .await
            .map_err(HostWriteError::Blob)?;
        let mut staged_blob = Self {
            namespace: blob.namespace,
            id: blob.id,
            staged: Some(staged),
            published: None,
        };
        if let Err(operation) = staged_blob.staged_mut().write_bytes(&blob.bytes).await {
            return match staged_blob.discard().await {
                Ok(()) => Err(HostWriteError::Blob(operation)),
                Err(cleanup) => Err(HostWriteError::BlobCleanupFailed {
                    operation: Box::new(HostWriteError::Blob(operation)),
                    cleanup,
                }),
            };
        }
        Ok(staged_blob)
    }

    fn staged_mut(&mut self) -> &mut crate::local_file::AtomicStagedFile {
        self.staged.as_mut().expect("blob is staged")
    }

    fn publish(&mut self) -> Result<(), String> {
        let staged = self.staged.take().expect("blob is staged");
        self.published = Some(staged.publish_for_transaction()?);
        Ok(())
    }

    async fn discard(mut self) -> Result<(), String> {
        match self.staged.take() {
            Some(staged) => staged.discard().await,
            None => Ok(()),
        }
    }

    fn rollback(mut self) -> Result<(), String> {
        let mut failures = Vec::new();
        if let Some(published) = self.published.take() {
            if let Err(error) = published.rollback() {
                failures.push(error);
            }
        }
        if let Some(staged) = self.staged.take() {
            if let Err(error) = staged.discard_blocking() {
                failures.push(error);
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    fn commit(mut self) {
        assert!(self.staged.is_none(), "committed blob remains staged");
        assert!(self.published.take().is_some(), "blob was not published");
    }
}

impl StagedBlobBatch {
    async fn stage<E>(
        store_dir: &StoreDir,
        blobs: Vec<NewBlob>,
    ) -> Result<Self, HostWriteError<E>> {
        let mut staged = Vec::new();
        for blob in blobs {
            match StagedBlob::stage(store_dir, blob).await {
                Ok(blob) => staged.push(blob),
                Err(error) => {
                    return Err(Self { blobs: staged }
                        .discard_after_stage_failure(error)
                        .await);
                }
            }
        }
        Ok(Self { blobs: staged })
    }

    async fn discard_after_stage_failure<E>(
        self,
        operation: HostWriteError<E>,
    ) -> HostWriteError<E> {
        let mut failures = Vec::new();
        for blob in self.blobs {
            let identity = format!("{}/{}", blob.namespace, blob.id);
            if let Err(error) = blob.discard().await {
                failures.push(format!("{identity}: {error}"));
            }
        }
        if failures.is_empty() {
            operation
        } else {
            HostWriteError::BlobCleanupFailed {
                operation: Box::new(operation),
                cleanup: failures.join("; "),
            }
        }
    }

    fn publish<E>(
        &mut self,
        mut validate: impl FnMut(&str, &str) -> Result<(), HostWriteError<E>>,
    ) -> Result<(), HostWriteError<E>> {
        for blob in &mut self.blobs {
            validate(&blob.namespace, &blob.id)?;
            blob.publish().map_err(HostWriteError::Blob)?;
        }
        Ok(())
    }

    fn commit(self) {
        for blob in self.blobs {
            blob.commit();
        }
    }

    fn rollback<E>(self, write: HostWriteError<E>) -> HostWriteError<E> {
        let mut failures = Vec::new();
        for blob in self.blobs.into_iter().rev() {
            let identity = format!("{}/{}", blob.namespace, blob.id);
            if let Err(error) = blob.rollback() {
                failures.push(format!("{identity}: {error}"));
            }
        }
        if failures.is_empty() {
            write
        } else {
            HostWriteError::WriteRollbackFailed {
                write: Box::new(write),
                rollback: failures.join("; "),
            }
        }
    }
}

type HostSql<R, E> = Box<
    dyn for<'context, 'connection> FnOnce(SqlContext<'context, 'connection>) -> Result<R, E> + Send,
>;

pub(crate) struct HostWriteOperation<R, E> {
    batch: WriteBatch,
    sql: HostSql<R, E>,
}

impl<R, E> HostWriteOperation<R, E> {
    pub(crate) fn new(
        batch: WriteBatch,
        sql: impl for<'context, 'connection> FnOnce(SqlContext<'context, 'connection>) -> Result<R, E>
            + Send
            + 'static,
    ) -> Self {
        Self {
            batch,
            sql: Box::new(sql),
        }
    }
}

#[derive(Debug)]
pub(crate) enum HostWriteError<E> {
    Host(E),
    Database(DbError),
    Blob(String),
    UnsafeBlobPath(PathTokenError),
    WriteClosurePanicked,
    WriteRollbackFailed {
        write: Box<Self>,
        rollback: String,
    },
    BlobCleanupFailed {
        operation: Box<Self>,
        cleanup: String,
    },
    BlobStillReferenced {
        namespace: String,
        id: String,
    },
    BlobAlreadyReferenced {
        namespace: String,
        id: String,
    },
    BlobOwnedByPendingWrite {
        namespace: String,
        id: String,
    },
    Io(std::io::Error),
}

impl<E> From<DbError> for HostWriteError<E> {
    fn from(value: DbError) -> Self {
        Self::Database(value)
    }
}

impl<E> From<PathTokenError> for HostWriteError<E> {
    fn from(value: PathTokenError) -> Self {
        Self::UnsafeBlobPath(value)
    }
}

impl<E> From<std::io::Error> for HostWriteError<E> {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Clone)]
pub(crate) struct StoreRowWrites {
    database: StoreDatabase,
    store_dir: StoreDir,
}

impl StoreRowWrites {
    pub(crate) fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
        Self {
            database,
            store_dir,
        }
    }

    pub(crate) fn requires_routing_encryption(&self) -> bool {
        self.database.has_scoped_graph()
    }

    pub(crate) async fn pending_writes(&self) -> Result<Vec<crate::PendingWrite>, DbError> {
        self.database.pending_writes().await
    }

    pub(crate) async fn blocked_writes(&self) -> Result<Vec<crate::PendingWrite>, DbError> {
        self.database.blocked_writes().await
    }

    pub(crate) async fn retry_blocked_write(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, DbError> {
        self.database.retry_blocked_write(write_id).await
    }

    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<super::BlockedWriteDiscard, DbError> {
        self.database.discard_blocked_write(write_id).await
    }

    pub(crate) async fn write_status(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<crate::WriteStatus, DbError> {
        self.database.write_status(write_id).await
    }

    pub(crate) async fn subscribe_write_status(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<tokio::sync::watch::Receiver<crate::WriteStatus>, DbError> {
        self.database.subscribe_write_status(write_id).await
    }

    pub(crate) async fn execute<R, E>(
        &self,
        operation: HostWriteOperation<R, E>,
        routing_encryption: Option<EncryptionService>,
        blob_staging: Option<crate::sync::HostWriteBlobStaging>,
    ) -> Result<WriteReceipt<R>, HostWriteError<E>>
    where
        R: Send + 'static,
        E: Send + 'static,
    {
        let database = &self.database;
        let HostWriteOperation { batch, sql } = operation;
        let staged = StagedBlobBatch::stage(&self.store_dir, batch.new_blobs).await?;
        let tables = database.synced_tables.to_vec();
        let gates = database.gates.clone();
        let blob_decls = database.blob_decls.clone();
        let write_id = database.new_store_write_id();
        let deleted = batch.deleted_blobs;
        let cleanup_store_dir = self.store_dir.clone();
        let stamper = crate::sync::hlc::UpdatedAtStamper::new(database.hlc.clone());

        let outcome = database
            .connection
            .call(move |connection| {
                let mut staged = staged;
                let result = super::host_write_capture::CapturedStoreWriteTransaction::begin_host(
                    connection,
                    &tables,
                    &gates,
                    &blob_decls,
                    routing_encryption.as_ref(),
                    blob_staging.as_ref(),
                    write_id,
                )
                .map_err(HostWriteError::from)
                .and_then(|transaction| {
                    transaction.execute(|transaction| -> Result<R, HostWriteError<E>> {
                        let cleanup_intents = deleted
                            .iter()
                            .map(|blob| {
                                blob_decls
                                    .row_for_blob_in_namespace(
                                        transaction,
                                        &blob.namespace,
                                        &blob.id,
                                    )
                                    .map_err(|error| HostWriteError::Blob(error.to_string()))
                                    .map(|row| match row {
                                        Some((table, row_id)) => LocalBlobCleanupIntent::for_row(
                                            &blob.namespace,
                                            &blob.id,
                                            table,
                                            row_id,
                                        ),
                                        None => {
                                            LocalBlobCleanupIntent::local(&blob.namespace, &blob.id)
                                        }
                                    })
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        staged.publish(|namespace, id| {
                            match blob_decls.row_for_blob_in_namespace(transaction, namespace, id) {
                                Ok(Some(_)) => {
                                    return Err(HostWriteError::BlobAlreadyReferenced {
                                        namespace: namespace.to_string(),
                                        id: id.to_string(),
                                    });
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    return Err(HostWriteError::Blob(error.to_string()));
                                }
                            }
                            let leased = transaction
                                .query_row(
                                    "SELECT EXISTS(\
                                         SELECT 1 FROM store_write_blob_leases \
                                         WHERE namespace = ?1 AND blob_id = ?2\
                                     )",
                                    (namespace, id),
                                    |row| row.get::<_, bool>(0),
                                )
                                .map_err(DbError::from)?;
                            if leased {
                                return Err(HostWriteError::BlobOwnedByPendingWrite {
                                    namespace: namespace.to_string(),
                                    id: id.to_string(),
                                });
                            }
                            Ok(())
                        })?;

                        let host_sql = HostSqlTransaction::begin(transaction)?;
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            host_sql.run(|transaction| {
                                sql(SqlContext::new(transaction, stamper, &tables, &gates))
                            })
                        })) {
                            Ok(Ok(value)) => {
                                for (blob, intent) in deleted.iter().zip(&cleanup_intents) {
                                    let _ = cleanup_store_dir
                                        .local_blob_path(&blob.namespace, &blob.id)?;
                                    if blob_decls
                                        .blob_id_is_referenced(
                                            transaction,
                                            &blob.namespace,
                                            &blob.id,
                                        )
                                        .map_err(|error| DbError::Message(error.to_string()))?
                                    {
                                        return Err(HostWriteError::BlobStillReferenced {
                                            namespace: blob.namespace.clone(),
                                            id: blob.id.clone(),
                                        });
                                    }
                                    super::local_blob_cleanup::record_obsolete_copy_intents_on(
                                        transaction,
                                        &blob_decls,
                                        intent,
                                    )?;
                                }
                                Ok(value)
                            }
                            Ok(Err(error)) => Err(HostWriteError::Host(error)),
                            Err(_) => Err(HostWriteError::WriteClosurePanicked),
                        }
                    })
                });

                match result {
                    Ok(receipt) => {
                        staged.commit();
                        Ok(Ok(receipt))
                    }
                    Err(error) => Ok(Err(staged.rollback(error))),
                }
            })
            .await;

        let receipt = match outcome {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(HostWriteError::Database(error)),
        };

        if let Err(error) =
            super::local_blob_cleanup::LocalBlobCleanup::new(database, &self.store_dir)
                .drain()
                .await
        {
            tracing::warn!(
                error = %error,
                "failed to drain local blob cleanup intents after write commit"
            );
        }
        Ok(receipt)
    }

    #[cfg(test)]
    pub(crate) async fn write_changeset_for_test(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<Vec<u8>, DbError> {
        self.database.write_changeset_for_test(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn write_blob_lease_count_for_test(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<i64, DbError> {
        self.database
            .write_blob_lease_count_for_test(write_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn cleanup_intent_count_for_test(
        &self,
        namespace: &str,
        blob_id: &str,
    ) -> Result<i64, DbError> {
        self.database
            .cleanup_intent_count_for_test(namespace, blob_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn coven_table_exists_for_test(
        &self,
        table: crate::database::DatabaseTestTable,
    ) -> Result<bool, DbError> {
        self.database.coven_table_exists_for_test(table).await
    }

    #[cfg(test)]
    pub(crate) async fn install_store_write_failure_trigger_for_test(&self) -> Result<(), DbError> {
        self.database
            .install_store_write_failure_trigger_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn remove_store_write_failure_trigger_for_test(&self) -> Result<(), DbError> {
        self.database
            .remove_store_write_failure_trigger_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn write_blob_facts_for_test(
        &self,
        write_id: crate::WriteId,
    ) -> Result<String, DbError> {
        self.database.write_blob_facts_for_test(write_id).await
    }
}
