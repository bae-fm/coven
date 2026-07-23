//! Store authority resolution for blob reads and local materialization.

use futures_util::stream::TryStreamExt;

use crate::blob::cache::{BlobCacheError, RemoteBlobAccess};
use crate::blob::{RowBlobAuthority, RowBlobRef};
use crate::encryption::KeyFingerprint;
use crate::store_dir::StoreDir;
use crate::sync::circle::CircleId;
use crate::sync::storage::{BlobSpoolProtection, StorageError, SyncStorage};

use super::StoreDatabase;

pub(crate) async fn opening_protection(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    authority: &RowBlobAuthority,
    stored: &crate::blob::locator::StoredBlobRef,
) -> Result<BlobSpoolProtection, BlobCacheError> {
    match authority {
        RowBlobAuthority::Local | RowBlobAuthority::PendingRemote(_) => {
            Err(BlobCacheError::LocalityUnresolved {
                id: stored.locator().blob_id().to_string(),
            })
        }
        RowBlobAuthority::Remote(crate::sync::audience_package::PackageAudience::Store) => storage
            .store_blob_protection()
            .map_err(BlobCacheError::Storage),
        RowBlobAuthority::Remote(crate::sync::audience_package::PackageAudience::Circle {
            circle_id,
            control,
            key_fingerprint,
        }) => {
            validate_circle_blob_locator(*circle_id, *key_fingerprint, stored)?;
            let encryption = database
                .circle_blob_opening_key(*circle_id, control.clone(), *key_fingerprint)
                .await
                .map_err(BlobCacheError::Metadata)?;
            Ok(BlobSpoolProtection::Opaque(encryption))
        }
    }
}

fn validate_circle_blob_locator(
    circle_id: CircleId,
    key_fingerprint: KeyFingerprint,
    stored: &crate::blob::locator::StoredBlobRef,
) -> Result<(), BlobCacheError> {
    if stored.locator().audience() != crate::blob::locator::RemoteAudience::Circle(circle_id)
        || stored.locator().key_fingerprint() != Some(key_fingerprint)
    {
        return Err(BlobCacheError::Storage(StorageError::InvalidContent(
            format!(
                "Circle {circle_id} blob locator audience or key differs from its exact activated authority"
            ),
        )));
    }
    Ok(())
}

async fn remote_access<'a>(
    database: &StoreDatabase,
    storage: Option<&'a dyn SyncStorage>,
    reference: &RowBlobRef,
) -> Result<Option<RemoteBlobAccess<'a>>, BlobCacheError> {
    let Some(storage) = storage else {
        return Ok(None);
    };
    if !matches!(reference.authority(), RowBlobAuthority::Remote(_)) {
        return Ok(None);
    }
    let stored = reference
        .stored()
        .ok_or_else(|| BlobCacheError::LocalityUnresolved {
            id: reference.blob().id.clone(),
        })?;
    let protection = opening_protection(database, storage, reference.authority(), stored).await?;
    Ok(Some(RemoteBlobAccess::new(storage, protection)))
}

#[doc(hidden)]
pub async fn read_blob(
    database: &StoreDatabase,
    store_dir: &StoreDir,
    storage: Option<&dyn SyncStorage>,
    reference: &RowBlobRef,
) -> Result<Vec<u8>, BlobCacheError> {
    let remote = remote_access(database, storage, reference).await?;
    crate::blob::cache::read_blob(database.sqlite(), store_dir, remote, reference).await
}

#[doc(hidden)]
pub async fn open_blob_stream(
    database: &StoreDatabase,
    store_dir: &StoreDir,
    storage: Option<&dyn SyncStorage>,
    reference: &RowBlobRef,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, BlobCacheError> {
    let remote = remote_access(database, storage, reference).await?;
    crate::blob::cache::open_blob_stream(
        database.sqlite(),
        store_dir,
        remote,
        reference,
        offset,
        len,
    )
    .await
}

#[doc(hidden)]
pub async fn materialize_row_blob(
    database: &StoreDatabase,
    store_dir: &StoreDir,
    storage: Option<&dyn SyncStorage>,
    reference: &RowBlobRef,
) -> Result<(), BlobCacheError> {
    let remote = remote_access(database, storage, reference).await?;
    crate::blob::cache::materialize_row_blob(database.sqlite(), store_dir, remote, reference).await
}

pub(crate) async fn stage_remote_blob_plaintext(
    database: &StoreDatabase,
    store_dir: &StoreDir,
    storage: Option<&dyn SyncStorage>,
    reference: &RowBlobRef,
    destination: &std::path::Path,
) -> Result<crate::local_blob::AtomicStagedFile, BlobCacheError> {
    let remote = remote_access(database, storage, reference).await?;
    crate::blob::cache::stage_remote_blob_plaintext(
        database.sqlite(),
        store_dir,
        remote,
        reference,
        destination,
    )
    .await
}

#[doc(hidden)]
pub async fn pin(
    database: &StoreDatabase,
    store_dir: &StoreDir,
    storage: Option<&dyn SyncStorage>,
    blobs: &[RowBlobRef],
) -> Result<(), BlobCacheError> {
    let limit = database.sqlite().transfer_limits().downloads.get();
    futures_util::stream::iter(blobs.iter().map(Ok::<&RowBlobRef, BlobCacheError>))
        .try_for_each_concurrent(limit, |reference| async move {
            let remote = remote_access(database, storage, reference).await?;
            crate::blob::cache::pin_one(database.sqlite(), store_dir, remote, reference).await
        })
        .await
}
