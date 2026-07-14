use crate::blob::{BlobRef, Provenance};
use crate::database::Database;

use super::storage::{StorageError, SyncStorage};

#[derive(Debug, thiserror::Error)]
pub(crate) enum PublishBlobError {
    #[error("user-provided blob {namespace}/{id} still has a local external ref")]
    LocalUserProvided { namespace: String, id: String },
    #[error("user-provided blob {namespace}/{id} is absent from remote storage")]
    MissingRemote { namespace: String, id: String },
    #[error("failed to read external ref for blob {namespace}/{id}: {source}")]
    ExternalLookup {
        namespace: String,
        id: String,
        source: crate::database::DbError,
    },
    #[error("failed to check remote blob {namespace}/{id}: {source}")]
    RemoteCheck {
        namespace: String,
        id: String,
        source: StorageError,
    },
}

pub(crate) async fn ensure_publishable_blobs(
    db: &Database,
    storage: &dyn SyncStorage,
    blobs: &[BlobRef],
) -> Result<(), PublishBlobError> {
    for blob in blobs {
        if blob.provenance == Provenance::HostProvided {
            continue;
        }

        if db
            .external_blob(&blob.id)
            .await
            .map_err(|source| PublishBlobError::ExternalLookup {
                namespace: blob.namespace.clone(),
                id: blob.id.clone(),
                source,
            })?
            .is_some()
        {
            return Err(PublishBlobError::LocalUserProvided {
                namespace: blob.namespace.clone(),
                id: blob.id.clone(),
            });
        }

        if !storage
            .blob_exists(&blob.namespace, &blob.id, blob.cloud_path.as_deref())
            .await
            .map_err(|source| PublishBlobError::RemoteCheck {
                namespace: blob.namespace.clone(),
                id: blob.id.clone(),
                source,
            })?
        {
            return Err(PublishBlobError::MissingRemote {
                namespace: blob.namespace.clone(),
                id: blob.id.clone(),
            });
        }
    }

    Ok(())
}
