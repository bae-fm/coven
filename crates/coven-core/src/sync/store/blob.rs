//! Store authority resolution for blob reads and local materialization.

use futures_util::stream::TryStreamExt;

use crate::blob::cache::{BlobCacheError, RemoteBlobAccess};
use crate::blob::{RowBlobAuthority, RowBlobRef};
use crate::encryption::KeyFingerprint;
use crate::store_dir::StoreDir;
use crate::sync::circle::CircleId;
use crate::sync::storage::{BlobSpoolProtection, StorageError, SyncStorage};
use crate::sync::store_commit::StoreRootRef;

use super::StoreDatabase;

enum BlobOpeningAuthority<'a> {
    Store,
    Circle {
        circle_id: CircleId,
        control: &'a crate::sync::circle::CircleControlCoord,
        key_fingerprint: KeyFingerprint,
    },
}

#[doc(hidden)]
pub struct StoreBlobOpening<'a> {
    database: StoreDatabase,
    storage: &'a dyn SyncStorage,
    root: StoreRootRef,
}

impl<'a> StoreBlobOpening<'a> {
    #[doc(hidden)]
    pub async fn open(
        database: &'a StoreDatabase,
        storage: &'a dyn SyncStorage,
    ) -> Result<Self, BlobCacheError> {
        let root = database
            .local_store_root_ref()
            .await
            .map_err(BlobCacheError::Metadata)?
            .ok_or(BlobCacheError::Metadata(
                crate::database::DbError::StoreRootHashMissing,
            ))?;
        Ok(Self {
            database: database.clone(),
            storage,
            root,
        })
    }

    pub(super) fn for_store(store: &super::owner::AuthorizedStore<'a>) -> Self {
        Self {
            database: store.database().clone(),
            storage: store.storage(),
            root: store.store_root().clone(),
        }
    }

    #[doc(hidden)]
    pub async fn protection(
        &self,
        authority: &RowBlobAuthority,
        stored: &crate::blob::locator::StoredBlobRef,
    ) -> Result<BlobSpoolProtection, BlobCacheError> {
        opening_protection(&self.database, self.storage, &self.root, authority, stored).await
    }
}

fn blob_opening_authority<'a>(
    authority: &'a RowBlobAuthority,
    stored: &crate::blob::locator::StoredBlobRef,
) -> Result<BlobOpeningAuthority<'a>, BlobCacheError> {
    match authority {
        RowBlobAuthority::Local | RowBlobAuthority::PendingRemote(_) => {
            Err(BlobCacheError::LocalityUnresolved {
                id: stored.locator().blob_id().to_string(),
            })
        }
        RowBlobAuthority::Remote(crate::sync::audience_package::PackageAudience::Store) => {
            Ok(BlobOpeningAuthority::Store)
        }
        RowBlobAuthority::Remote(crate::sync::audience_package::PackageAudience::Circle {
            circle_id,
            control,
            key_fingerprint,
        }) => {
            if stored.locator().audience()
                != crate::blob::locator::RemoteAudience::Circle(*circle_id)
                || stored.locator().key_fingerprint() != Some(*key_fingerprint)
            {
                return Err(BlobCacheError::Storage(StorageError::InvalidContent(
                    format!(
                        "Circle {circle_id} blob locator audience or key differs from its exact activated authority"
                    ),
                )));
            }
            Ok(BlobOpeningAuthority::Circle {
                circle_id: *circle_id,
                control,
                key_fingerprint: *key_fingerprint,
            })
        }
    }
}

pub(crate) fn opening_protection_on(
    conn: &rusqlite::Connection,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    authority: &RowBlobAuthority,
    stored: &crate::blob::locator::StoredBlobRef,
) -> Result<BlobSpoolProtection, BlobCacheError> {
    match blob_opening_authority(authority, stored)? {
        BlobOpeningAuthority::Store => storage
            .store_blob_protection()
            .map_err(BlobCacheError::Storage),
        BlobOpeningAuthority::Circle {
            circle_id,
            control,
            key_fingerprint,
        } => {
            let encryption = StoreDatabase::circle_blob_opening_key_on(
                conn,
                root,
                circle_id,
                control,
                key_fingerprint,
            )
            .map_err(BlobCacheError::Metadata)?;
            Ok(BlobSpoolProtection::Opaque(encryption))
        }
    }
}

#[doc(hidden)]
pub struct StoreBlobAccess<'a> {
    database: StoreDatabase,
    store_dir: StoreDir,
    storage: Option<&'a dyn SyncStorage>,
    root: Option<StoreRootRef>,
}

impl<'a> StoreBlobAccess<'a> {
    #[doc(hidden)]
    pub async fn open(
        database: &'a StoreDatabase,
        store_dir: &'a StoreDir,
        storage: Option<&'a dyn SyncStorage>,
    ) -> Result<Self, BlobCacheError> {
        let root = database
            .local_store_root_ref()
            .await
            .map_err(BlobCacheError::Metadata)?;
        Ok(Self {
            database: database.clone(),
            store_dir: store_dir.clone(),
            storage,
            root,
        })
    }

    async fn remote_access(
        &self,
        reference: &RowBlobRef,
    ) -> Result<Option<RemoteBlobAccess<'a>>, BlobCacheError> {
        let Some(storage) = self.storage else {
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
        let root = self.root.as_ref().ok_or(BlobCacheError::Metadata(
            crate::database::DbError::StoreRootHashMissing,
        ))?;
        let protection =
            opening_protection(&self.database, storage, root, reference.authority(), stored)
                .await?;
        Ok(Some(RemoteBlobAccess::new(storage, protection)))
    }

    #[doc(hidden)]
    pub async fn read(&self, reference: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        let remote = self.remote_access(reference).await?;
        crate::blob::cache::read_blob(self.database.sqlite(), &self.store_dir, remote, reference)
            .await
    }

    #[doc(hidden)]
    pub async fn open_stream(
        &self,
        reference: &RowBlobRef,
    ) -> Result<crate::blob::cache::BlobStream, BlobCacheError> {
        let remote = self.remote_access(reference).await?;
        crate::blob::cache::open_blob_stream(
            self.database.sqlite(),
            &self.store_dir,
            remote,
            reference,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn materialize(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        let remote = self.remote_access(reference).await?;
        crate::blob::cache::materialize_row_blob(
            self.database.sqlite(),
            &self.store_dir,
            remote,
            reference,
        )
        .await
    }

    pub(crate) async fn stage_remote_plaintext(
        &self,
        reference: &RowBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, BlobCacheError> {
        let remote = self.remote_access(reference).await?;
        crate::blob::cache::stage_remote_blob_plaintext(
            self.database.sqlite(),
            &self.store_dir,
            remote,
            reference,
            destination,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        let limit = self.database.sqlite().transfer_limits().downloads.get();
        futures_util::stream::iter(blobs.iter().map(Ok::<&RowBlobRef, BlobCacheError>))
            .try_for_each_concurrent(limit, |reference| async move {
                let remote = self.remote_access(reference).await?;
                crate::blob::cache::pin_one(
                    self.database.sqlite(),
                    &self.store_dir,
                    remote,
                    reference,
                )
                .await
            })
            .await
    }
}

pub(super) async fn opening_protection(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    authority: &RowBlobAuthority,
    stored: &crate::blob::locator::StoredBlobRef,
) -> Result<BlobSpoolProtection, BlobCacheError> {
    match blob_opening_authority(authority, stored)? {
        BlobOpeningAuthority::Store => storage
            .store_blob_protection()
            .map_err(BlobCacheError::Storage),
        BlobOpeningAuthority::Circle {
            circle_id,
            control,
            key_fingerprint,
        } => {
            let encryption = database
                .circle_blob_opening_key(root.clone(), circle_id, control.clone(), key_fingerprint)
                .await
                .map_err(BlobCacheError::Metadata)?;
            Ok(BlobSpoolProtection::Opaque(encryption))
        }
    }
}
