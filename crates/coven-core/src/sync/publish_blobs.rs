use crate::blob::{BlobRef, Provenance};
use crate::changeset::{ChangeOp, RowChange};
use crate::database::Database;

use super::envelope;
use super::storage::{StorageError, SyncStorage};

#[derive(Debug, thiserror::Error)]
pub(crate) enum PublishBlobError {
    #[error("failed to unpack changeset envelope: {0}")]
    PackedChangeset(envelope::UnpackError),
    #[error("failed to scan changeset blobs: {0}")]
    ChangesetScan(String),
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

pub(crate) fn publish_blobs_from_changes(
    blob_decls: &crate::blob::decl::BlobDecls,
    changes: &[RowChange],
) -> Vec<BlobRef> {
    changes
        .iter()
        .filter(|change| matches!(change.op, ChangeOp::Insert | ChangeOp::Update))
        .filter_map(|change| blob_decls.ref_from_change(change))
        .collect()
}

pub(crate) async fn ensure_publishable_changeset_blobs(
    db: &Database,
    storage: &dyn SyncStorage,
    packed: &[u8],
) -> Result<(), PublishBlobError> {
    let (_envelope, changeset) =
        envelope::unpack(packed).map_err(PublishBlobError::PackedChangeset)?;
    let changes = crate::changeset::walk(&changeset).map_err(PublishBlobError::ChangesetScan)?;
    let blobs = publish_blobs_from_changes(&db.blob_decls(), &changes);
    ensure_publishable_blobs(db, storage, &blobs).await
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
