use std::path::{Path, PathBuf};

use crate::blob::local_cleanup::LocalBlobCleanupIntent;
use crate::blob::BlobRef;
use crate::database::DbError;
use crate::encryption::EncryptionService;
use crate::store_dir::{PathTokenError, StoreDir};
use crate::sync::hlc::UpdatedAtStamper;
use crate::WriteReceipt;

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

#[derive(Clone)]
struct StagedBlob {
    namespace: String,
    id: String,
    staged: PathBuf,
    final_path: PathBuf,
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
    MalformedPath(String),
    WriteClosurePanicked,
    WriteRollbackFailed { write: Box<Self>, rollback: String },
    BlobStillReferenced { namespace: String, id: String },
    BlobAlreadyReferenced { namespace: String, id: String },
    BlobOwnedByPendingWrite { namespace: String, id: String },
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

impl StoreDatabase {
    pub(crate) async fn execute_host_write<R, E>(
        &self,
        operation: HostWriteOperation<R, E>,
        store_dir: StoreDir,
        stamper: UpdatedAtStamper,
        routing_encryption: Option<EncryptionService>,
        blob_staging: Option<crate::sync::HostWriteBlobStaging>,
    ) -> Result<WriteReceipt<R>, HostWriteError<E>>
    where
        R: Send + 'static,
        E: Send + 'static,
    {
        let HostWriteOperation { batch, sql } = operation;
        let staged = stage_blobs(&store_dir, batch.new_blobs).await?;
        let staged_paths = staged
            .iter()
            .map(|blob| blob.staged.clone())
            .collect::<Vec<_>>();
        let tables = self.synced_tables.to_vec();
        let gates = self.gates.clone();
        let blob_decls = self.blob_decls.clone();
        let write_id = self.new_store_write_id();
        let deleted = batch.deleted_blobs;
        let cleanup_store_dir = store_dir.clone();

        let outcome = self
            .connection
            .call(move |connection| {
                let mut moved = Vec::new();
                let result = StoreDatabase::run_store_write_transaction_on(
                    connection,
                    &tables,
                    &gates,
                    &blob_decls,
                    routing_encryption.as_ref(),
                    blob_staging.as_ref(),
                    write_id,
                    |transaction| -> Result<R, HostWriteError<E>> {
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

                        for blob in &staged {
                            match blob_decls.row_for_blob_in_namespace(
                                transaction,
                                &blob.namespace,
                                &blob.id,
                            ) {
                                Ok(Some(_)) => {
                                    return Err(HostWriteError::BlobAlreadyReferenced {
                                        namespace: blob.namespace.clone(),
                                        id: blob.id.clone(),
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
                                    (&blob.namespace, &blob.id),
                                    |row| row.get::<_, bool>(0),
                                )
                                .map_err(DbError::from)?;
                            if leased {
                                return Err(HostWriteError::BlobOwnedByPendingWrite {
                                    namespace: blob.namespace.clone(),
                                    id: blob.id.clone(),
                                });
                            }
                            if let Some(parent) = blob.final_path.parent() {
                                std::fs::create_dir_all(parent).map_err(|error| {
                                    HostWriteError::Blob(format!(
                                        "create local blob parent {}: {error}",
                                        parent.display()
                                    ))
                                })?;
                            }
                            std::fs::rename(&blob.staged, &blob.final_path).map_err(|error| {
                                HostWriteError::Blob(format!(
                                    "install staged blob {} -> {}: {error}",
                                    blob.staged.display(),
                                    blob.final_path.display()
                                ))
                            })?;
                            moved.push(blob.clone());
                            sync_parent_dir(&blob.final_path).map_err(|error| {
                                HostWriteError::Blob(format!(
                                    "sync local blob parent after installing {}: {error}",
                                    blob.final_path.display()
                                ))
                            })?;
                        }

                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            sql(SqlContext::new(transaction, stamper, &tables, &gates))
                        })) {
                            Ok(Ok(value)) => {
                                for (blob, intent) in deleted.iter().zip(&cleanup_intents) {
                                    let _ = cleanup_store_dir
                                        .local_blob_path(&blob.namespace, &blob.id)?;
                                    if crate::blob::local_cleanup::logical_blob_is_referenced_on(
                                        transaction,
                                        &blob_decls,
                                        &blob.namespace,
                                        &blob.id,
                                    )? {
                                        return Err(HostWriteError::BlobStillReferenced {
                                            namespace: blob.namespace.clone(),
                                            id: blob.id.clone(),
                                        });
                                    }
                                    crate::blob::local_cleanup::record_obsolete_copy_intents_on(
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
                    },
                );

                match result {
                    Ok(receipt) => Ok(Ok(receipt)),
                    Err(error) => {
                        let mut rollback_failures = Vec::new();
                        for blob in moved.iter().rev() {
                            match std::fs::remove_file(&blob.final_path) {
                                Ok(()) => {}
                                Err(rollback)
                                    if rollback.kind() == std::io::ErrorKind::NotFound => {}
                                Err(rollback) => rollback_failures.push(format!(
                                    "{}/{} at {}: {rollback}",
                                    blob.namespace,
                                    blob.id,
                                    blob.final_path.display()
                                )),
                            }
                        }
                        if rollback_failures.is_empty() {
                            Ok(Err(error))
                        } else {
                            Ok(Err(HostWriteError::WriteRollbackFailed {
                                write: Box::new(error),
                                rollback: rollback_failures.join("; "),
                            }))
                        }
                    }
                }
            })
            .await;

        let receipt = match outcome {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(error)) => {
                remove_staged_paths(&staged_paths).await;
                return Err(error);
            }
            Err(error) => {
                remove_staged_paths(&staged_paths).await;
                return Err(HostWriteError::Database(error));
            }
        };

        if let Err(error) = self.drain_local_blob_cleanup(&store_dir).await {
            tracing::warn!(
                error = %error,
                "failed to drain local blob cleanup intents after write commit"
            );
        }
        Ok(receipt)
    }
}

async fn stage_blobs<E>(
    store_dir: &StoreDir,
    blobs: Vec<NewBlob>,
) -> Result<Vec<StagedBlob>, HostWriteError<E>> {
    let mut staged = Vec::new();
    for blob in blobs {
        let final_path = store_dir.local_blob_path(&blob.namespace, &blob.id)?;
        let staged_path = crate::local_blob::staged_blob_path(&final_path)
            .map_err(HostWriteError::MalformedPath)?;
        if let Err(error) = crate::local_blob::write_atomic(&staged_path, &blob.bytes).await {
            remove_staged_files(&staged).await;
            return Err(HostWriteError::Blob(error));
        }
        staged.push(StagedBlob {
            namespace: blob.namespace,
            id: blob.id,
            staged: staged_path,
            final_path,
        });
    }
    Ok(staged)
}

async fn remove_staged_files(staged: &[StagedBlob]) {
    let paths = staged
        .iter()
        .map(|blob| blob.staged.clone())
        .collect::<Vec<_>>();
    remove_staged_paths(&paths).await;
}

async fn remove_staged_paths(paths: &[PathBuf]) {
    for path in paths {
        if let Err(error) = crate::local_blob::remove_file(path).await {
            tracing::warn!(
                path = %path.display(),
                error = %error,
                "failed to remove staged local blob"
            );
        }
    }
}

fn sync_parent_dir(path: &Path) -> Result<(), std::io::Error> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("path has no parent directory: {}", path.display()),
        )
    })?;
    std::fs::File::open(parent)?.sync_all()
}
