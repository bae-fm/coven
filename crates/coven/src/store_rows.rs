use crate::{SqlContext, SqlReadContext, WriteBatch, WriteReceipt};
use coven_database::{HostWriteError, HostWriteOperation, StoreRowWrites};

use crate::store_security::StoreSecurity;
use crate::store_sync::StoreSync;
use crate::{CovenError, CovenResult};
use coven_database::StoreDatabase;

/// Run a host read on `database`'s retained read connection.
///
/// The connection's own failure to carry the read out and the read's own
/// failure are separate results; flattening them is the only thing this adds,
/// and it is the same for every reader. Both the full handle's rows owner and
/// the read-only handle call it, so a read means one thing across both.
pub(crate) async fn read_rows<F, R>(database: &StoreDatabase, read: F) -> CovenResult<R>
where
    F: for<'connection> FnOnce(SqlReadContext<'connection>) -> CovenResult<R> + Send + 'static,
    R: Send + 'static,
{
    database.read(read).await.map_err(CovenError::from)?
}

#[derive(Clone)]
pub(crate) struct StoreRows {
    writes: StoreRowWrites,
    read_database: StoreDatabase,
    security: StoreSecurity,
    sync: StoreSync,
}

impl StoreRows {
    pub(crate) fn new(
        writes: StoreRowWrites,
        read_database: StoreDatabase,
        security: StoreSecurity,
        sync: StoreSync,
    ) -> Self {
        Self {
            writes,
            read_database,
            security,
            sync,
        }
    }

    pub(crate) async fn sql<F, R>(&self, sql: F) -> CovenResult<WriteReceipt<R>>
    where
        F: for<'context, 'connection> FnOnce(SqlContext<'context, 'connection>) -> CovenResult<R>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.execute(HostWriteOperation::new(WriteBatch::new(), sql))
            .await
    }

    pub(crate) async fn read<F, R>(&self, read: F) -> CovenResult<R>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        read_rows(&self.read_database, read).await
    }

    pub(crate) async fn write<F, S, R>(&self, build: F, sql: S) -> CovenResult<WriteReceipt<R>>
    where
        F: FnOnce(&mut WriteBatch) -> CovenResult<()> + Send + 'static,
        S: for<'context, 'connection> FnOnce(SqlContext<'context, 'connection>) -> CovenResult<R>
            + Send
            + 'static,
        R: Send + 'static,
    {
        let mut batch = WriteBatch::new();
        build(&mut batch)?;
        self.execute(HostWriteOperation::new(batch, sql)).await
    }

    async fn execute<R>(
        &self,
        operation: HostWriteOperation<R, CovenError>,
    ) -> CovenResult<WriteReceipt<R>>
    where
        R: Send + 'static,
    {
        let routing_encryption = self.routing_encryption()?;
        let blob_staging = self.host_write_blob_staging();
        self.writes
            .execute(operation, routing_encryption, blob_staging)
            .await
            .map_err(map_host_write_error)
    }

    fn routing_encryption(
        &self,
    ) -> Result<
        Option<coven_keys::encryption::EncryptionService>,
        coven_keys::keys::RoutingEncryptionError,
    > {
        self.security
            .routing_encryption(self.writes.requires_routing_encryption())
    }

    fn host_write_blob_staging(&self) -> Option<Box<dyn coven_database::AudienceBlobMoveStaging>> {
        self.sync
            .host_write_blob_staging()
            .map(|staging| Box::new(staging) as Box<dyn coven_database::AudienceBlobMoveStaging>)
    }

    pub(crate) async fn pending_writes(
        &self,
    ) -> Result<Vec<crate::PendingWrite>, coven_database::DbError> {
        self.writes.pending_writes().await
    }

    pub(crate) async fn blocked_writes(
        &self,
    ) -> Result<Vec<crate::PendingWrite>, coven_database::DbError> {
        self.writes.blocked_writes().await
    }

    pub(crate) async fn retry_blocked_write(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, crate::CovenError> {
        let retried = self
            .writes
            .retry_blocked_write(write_id)
            .await
            .map_err(crate::CovenError::from)?;
        self.sync.trigger();
        Ok(retried)
    }

    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, crate::CovenError> {
        let outcome = self
            .writes
            .discard_blocked_write(write_id)
            .await
            .map_err(crate::CovenError::from)?;
        if let coven_database::BlockedWriteDiscard::Discarded(discarded) = outcome {
            return Ok(discarded);
        }
        self.sync
            .discard_blocked_write(write_id.clone())
            .await
            .map_err(|error| crate::CovenError::CandidateResolution(error.to_string()))
    }

    pub(crate) async fn write_status(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<crate::WriteStatus, coven_database::DbError> {
        self.writes.write_status(write_id).await
    }

    pub(crate) async fn subscribe_write_status(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<tokio::sync::watch::Receiver<crate::WriteStatus>, coven_database::DbError> {
        self.writes.subscribe_write_status(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn write_changeset_for_test(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<Vec<u8>, coven_database::DbError> {
        self.writes.write_changeset_for_test(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn write_blob_lease_count_for_test(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<i64, coven_database::DbError> {
        self.writes.write_blob_lease_count_for_test(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn cleanup_intent_count_for_test(
        &self,
        namespace: &str,
        blob_id: &str,
    ) -> Result<i64, coven_database::DbError> {
        self.writes
            .cleanup_intent_count_for_test(namespace, blob_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn coven_table_exists_for_test(
        &self,
        table: coven_database::DatabaseTestTable,
    ) -> Result<bool, coven_database::DbError> {
        self.writes.coven_table_exists_for_test(table).await
    }

    #[cfg(test)]
    pub(crate) async fn install_store_write_failure_trigger_for_test(
        &self,
    ) -> Result<(), coven_database::DbError> {
        self.writes
            .install_store_write_failure_trigger_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn remove_store_write_failure_trigger_for_test(
        &self,
    ) -> Result<(), coven_database::DbError> {
        self.writes
            .remove_store_write_failure_trigger_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn write_blob_facts_for_test(
        &self,
        write_id: crate::WriteId,
    ) -> Result<String, coven_database::DbError> {
        self.writes.write_blob_facts_for_test(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn execute_sql_with_blob_staging_for_test(
        &self,
        blob_staging: Option<Box<dyn coven_database::AudienceBlobMoveStaging>>,
        sql: String,
    ) -> CovenResult<WriteReceipt<()>> {
        let operation = HostWriteOperation::new(WriteBatch::new(), move |context| {
            context.execute_batch(&sql)?;
            Ok(())
        });
        self.writes
            .execute(operation, self.routing_encryption()?, blob_staging)
            .await
            .map_err(map_host_write_error)
    }
}

fn map_host_write_error(error: HostWriteError<CovenError>) -> CovenError {
    match error {
        HostWriteError::Host(error) => error,
        HostWriteError::Database(error) => CovenError::Database(error),
        HostWriteError::Blob(error) => CovenError::Blob(error),
        HostWriteError::UnsafeBlobPath(error) => CovenError::UnsafeBlobPath(error),
        HostWriteError::WriteClosurePanicked => CovenError::WriteClosurePanicked,
        HostWriteError::WriteRollbackFailed { write, rollback } => {
            CovenError::WriteRollbackFailed {
                write: Box::new(map_host_write_error(*write)),
                rollback,
            }
        }
        HostWriteError::BlobCleanupFailed { operation, cleanup } => CovenError::BlobCleanupFailed {
            operation: Box::new(map_host_write_error(*operation)),
            cleanup,
        },
        HostWriteError::BlobStillReferenced { namespace, id } => {
            CovenError::BlobStillReferenced { namespace, id }
        }
        HostWriteError::BlobAlreadyReferenced { namespace, id } => {
            CovenError::BlobAlreadyReferenced { namespace, id }
        }
        HostWriteError::BlobOwnedByPendingWrite { namespace, id } => {
            CovenError::BlobOwnedByPendingWrite { namespace, id }
        }
        HostWriteError::Io(error) => CovenError::Io(error),
    }
}
