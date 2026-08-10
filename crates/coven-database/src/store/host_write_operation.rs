use crate::local_blob_cleanup_intents::LocalBlobCleanupIntent;
use crate::DbError;
use coven_foundation::store_dir::{PathTokenError, StoreDir};
use coven_keys::encryption::EncryptionService;
use coven_protocol::blob::BlobRef;
use coven_protocol::write::WriteReceipt;

use super::host_sql_transaction::HostSqlTransaction;
use super::{SqlContext, StoreDatabase, StoreSession};

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

/// A local blob file that failed to unwind after a host write failed. Names the
/// blob so a host learns which files are left behind, not just that some were.
#[derive(Debug)]
pub struct BlobFileFailure {
    pub namespace: String,
    pub id: String,
    pub reason: String,
}

impl std::fmt::Display for BlobFileFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}: {}", self.namespace, self.id, self.reason)
    }
}

/// Every blob that failed to unwind, in the order they were attempted.
#[derive(Debug)]
pub struct BlobFileFailures(pub Vec<BlobFileFailure>);

impl std::fmt::Display for BlobFileFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, failure) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{failure}")?;
        }
        Ok(())
    }
}

pub(crate) struct NewBlob {
    namespace: String,
    id: String,
    bytes: Vec<u8>,
}

struct StagedBlob {
    namespace: String,
    id: String,
    staged: Option<coven_foundation::local_file::AtomicStagedFile>,
    published: Option<coven_foundation::local_file::PublishedAtomicFile>,
}

pub(crate) struct StagedBlobBatch {
    blobs: Vec<StagedBlob>,
}

impl StagedBlob {
    async fn stage<E>(store_dir: &StoreDir, blob: NewBlob) -> Result<Self, HostWriteError<E>> {
        let destination = store_dir.local_blob_path(&blob.namespace, &blob.id)?;
        let staged = store_dir
            .stage_atomic_file(&destination)
            .await
            .map_err(HostWriteError::Blob)?;
        let mut staged_blob = Self {
            namespace: blob.namespace,
            id: blob.id,
            staged: Some(staged),
            published: None,
        };
        if let Err(operation) = staged_blob.staged_mut().write_bytes(&blob.bytes).await {
            let namespace = staged_blob.namespace.clone();
            let id = staged_blob.id.clone();
            return match staged_blob.discard().await {
                Ok(()) => Err(HostWriteError::Blob(operation)),
                Err(reason) => Err(HostWriteError::BlobCleanupFailed {
                    operation: Box::new(HostWriteError::Blob(operation)),
                    cleanup: BlobFileFailures(vec![BlobFileFailure {
                        namespace,
                        id,
                        reason,
                    }]),
                }),
            };
        }
        Ok(staged_blob)
    }

    fn staged_mut(&mut self) -> &mut coven_foundation::local_file::AtomicStagedFile {
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
    pub(crate) async fn stage<E>(
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
            let (namespace, id) = (blob.namespace.clone(), blob.id.clone());
            if let Err(reason) = blob.discard().await {
                failures.push(BlobFileFailure {
                    namespace,
                    id,
                    reason,
                });
            }
        }
        if failures.is_empty() {
            operation
        } else {
            HostWriteError::BlobCleanupFailed {
                operation: Box::new(operation),
                cleanup: BlobFileFailures(failures),
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
            let (namespace, id) = (blob.namespace.clone(), blob.id.clone());
            if let Err(reason) = blob.rollback() {
                failures.push(BlobFileFailure {
                    namespace,
                    id,
                    reason,
                });
            }
        }
        if failures.is_empty() {
            write
        } else {
            HostWriteError::WriteRollbackFailed {
                write: Box::new(write),
                rollback: BlobFileFailures(failures),
            }
        }
    }
}

type HostSql<R, E> = Box<
    dyn for<'context, 'connection> FnOnce(SqlContext<'context, 'connection>) -> Result<R, E> + Send,
>;

pub struct HostWriteOperation<R, E> {
    batch: WriteBatch,
    sql: HostSql<R, E>,
}

impl<R, E> HostWriteOperation<R, E> {
    pub fn new(
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
pub enum HostWriteError<E> {
    Host(E),
    Database(DbError),
    Blob(String),
    UnsafeBlobPath(PathTokenError),
    WriteClosurePanicked,
    WriteRollbackFailed {
        write: Box<Self>,
        rollback: BlobFileFailures,
    },
    BlobCleanupFailed {
        operation: Box<Self>,
        cleanup: BlobFileFailures,
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
pub struct StoreRowWrites {
    database: StoreDatabase,
}

impl StoreSession<'_> {
    fn execute_host_write<R, E>(
        &mut self,
        staged: StagedBlobBatch,
        deleted: Vec<BlobRef>,
        sql: HostSql<R, E>,
        routing_encryption: Option<EncryptionService>,
        blob_staging: Option<Box<dyn crate::AudienceBlobMoveStaging>>,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<Result<WriteReceipt<R>, HostWriteError<E>>, DbError> {
        let verified_authority = &mut *self.verified_store_authority;
        let mut staged = staged;
        let stamper = coven_protocol::hlc::UpdatedAtStamper::new(self.hlc.clone());
        let result = super::host_write_capture::CapturedStoreWriteTransaction::begin_host(
            self.records.conn,
            self.records.store_dir,
            self.synced_tables,
            self.gates,
            self.blob_decls,
            routing_encryption.as_ref(),
            blob_staging.as_deref(),
            verified_authority,
            write_id,
        )
        .map_err(HostWriteError::from)
        .and_then(|transaction| {
            transaction.execute(|transaction| -> Result<R, HostWriteError<E>> {
                let cleanup_intents = deleted
                    .iter()
                    .map(|blob| {
                        self.blob_decls
                            .row_for_blob_in_namespace(transaction, &blob.namespace, &blob.id)
                            .map_err(|error| HostWriteError::Blob(error.to_string()))
                            .map(|row| match row {
                                Some((table, row_id)) => LocalBlobCleanupIntent::for_row(
                                    &blob.namespace,
                                    &blob.id,
                                    table,
                                    row_id,
                                ),
                                None => LocalBlobCleanupIntent::local(&blob.namespace, &blob.id),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                staged.publish(|namespace, id| {
                    match self
                        .blob_decls
                        .row_for_blob_in_namespace(transaction, namespace, id)
                    {
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
                        sql(SqlContext::new(
                            transaction,
                            stamper,
                            self.synced_tables,
                            self.gates,
                        ))
                    })
                })) {
                    Ok(Ok(value)) => {
                        for (blob, intent) in deleted.iter().zip(&cleanup_intents) {
                            let _ = self
                                .records
                                .store_dir
                                .local_blob_path(&blob.namespace, &blob.id)?;
                            if self
                                .blob_decls
                                .blob_id_is_referenced(transaction, &blob.namespace, &blob.id)
                                .map_err(|error| DbError::Message(error.to_string()))?
                            {
                                return Err(HostWriteError::BlobStillReferenced {
                                    namespace: blob.namespace.clone(),
                                    id: blob.id.clone(),
                                });
                            }
                            super::local_blob_cleanup::record_obsolete_copy_intents_on(
                                transaction,
                                self.blob_decls,
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
    }
}

impl StoreRowWrites {
    pub fn new(database: StoreDatabase) -> Self {
        Self { database }
    }

    pub fn requires_routing_encryption(&self) -> bool {
        self.database.has_scoped_graph()
    }

    pub async fn pending_writes(
        &self,
    ) -> Result<Vec<coven_protocol::write::PendingWrite>, DbError> {
        self.database.pending_writes().await
    }

    pub async fn blocked_writes(
        &self,
    ) -> Result<Vec<coven_protocol::write::PendingWrite>, DbError> {
        self.database.blocked_writes().await
    }

    pub async fn retry_blocked_write(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<Vec<coven_protocol::write::WriteId>, DbError> {
        self.database.retry_blocked_write(write_id).await
    }

    pub async fn discard_blocked_write(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<super::BlockedWriteDiscard, DbError> {
        self.database.discard_blocked_write(write_id).await
    }

    pub async fn write_status(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<coven_protocol::write::WriteStatus, DbError> {
        self.database.write_status(write_id).await
    }

    pub async fn subscribe_write_status(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<tokio::sync::watch::Receiver<coven_protocol::write::WriteStatus>, DbError> {
        self.database.subscribe_write_status(write_id).await
    }

    pub async fn execute<R, E>(
        &self,
        operation: HostWriteOperation<R, E>,
        routing_encryption: Option<EncryptionService>,
        blob_staging: Option<Box<dyn crate::AudienceBlobMoveStaging>>,
    ) -> Result<WriteReceipt<R>, HostWriteError<E>>
    where
        R: Send + 'static,
        E: Send + 'static,
    {
        let database = &self.database;
        let HostWriteOperation { batch, sql } = operation;
        let staged = database.stage_host_write_blobs(batch.new_blobs).await?;
        let write_id = database.new_store_write_id();
        let deleted = batch.deleted_blobs;

        let outcome = database
            .call_store(move |session| {
                session.execute_host_write(
                    staged,
                    deleted,
                    sql,
                    routing_encryption,
                    blob_staging,
                    write_id,
                )
            })
            .await;

        let receipt = match outcome {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(HostWriteError::Database(error)),
        };

        if let Err(error) = super::local_blob_cleanup::LocalBlobCleanup::new(database)
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

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn store_write_partition_for_test(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<Vec<u8>, DbError> {
        self.database.store_write_partition_for_test(write_id).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn write_blob_lease_count_for_test(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<i64, DbError> {
        self.database
            .write_blob_lease_count_for_test(write_id)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn cleanup_intent_count_for_test(
        &self,
        namespace: &str,
        blob_id: &str,
    ) -> Result<i64, DbError> {
        self.database
            .cleanup_intent_count_for_test(namespace, blob_id)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn coven_table_exists_for_test(
        &self,
        table: crate::DatabaseTestTable,
    ) -> Result<bool, DbError> {
        self.database.coven_table_exists_for_test(table).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn install_store_write_failure_trigger_for_test(&self) -> Result<(), DbError> {
        self.database
            .install_store_write_failure_trigger_for_test()
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn remove_store_write_failure_trigger_for_test(&self) -> Result<(), DbError> {
        self.database
            .remove_store_write_failure_trigger_for_test()
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn write_blob_facts_for_test(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<String, DbError> {
        self.database.write_blob_facts_for_test(write_id).await
    }
}
