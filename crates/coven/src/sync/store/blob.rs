//! Store authority resolution for blob reads and local materialization.

use futures_util::stream::TryStreamExt;

use crate::blob::cache::{BlobCacheError, RemoteBlobAccess as ExactRemoteBlobAccess};
use crate::blob::{RowBlobAuthority, RowBlobRef};
use crate::encryption::KeyFingerprint;
use crate::protocol::circle::CircleId;
use crate::protocol::store_commit::StoreRootRef;
use crate::storage::{BlobSpoolProtection, StorageError, SyncStorage};
use crate::store_dir::StoreDir;

use crate::database::StoreDatabase;

/// Why materializing one exact remote blob failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BlobDownloadFailureCause {
    Invalid(String),
    Local(String),
    Metadata(String),
    Storage(StorageError),
}

impl std::fmt::Display for BlobDownloadFailureCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => write!(formatter, "invalid blob: {reason}"),
            Self::Local(reason) => write!(formatter, "local cache: {reason}"),
            Self::Metadata(reason) => write!(formatter, "blob metadata: {reason}"),
            Self::Storage(error) => write!(formatter, "provider: {error}"),
        }
    }
}

enum BlobOpeningAuthority<'a> {
    Store,
    Circle {
        circle_id: CircleId,
        control: &'a crate::protocol::circle::CircleControlCoord,
        key_fingerprint: KeyFingerprint,
    },
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
        RowBlobAuthority::Remote(crate::protocol::audience_package::PackageAudience::Store) => {
            Ok(BlobOpeningAuthority::Store)
        }
        RowBlobAuthority::Remote(crate::protocol::audience_package::PackageAudience::Circle {
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

#[derive(Clone)]
pub(crate) struct LocalStoreBlobAccess {
    database: StoreDatabase,
    store_dir: StoreDir,
}

impl LocalStoreBlobAccess {
    pub(crate) fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
        Self {
            database,
            store_dir,
        }
    }

    pub(crate) async fn read(&self, reference: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        crate::blob::cache::read_blob(&self.database, &self.store_dir, None, reference).await
    }

    pub(crate) async fn open_stream(
        &self,
        reference: &RowBlobRef,
    ) -> Result<crate::blob::cache::BlobStream, BlobCacheError> {
        crate::blob::cache::open_blob_stream(&self.database, &self.store_dir, None, reference).await
    }

    pub(crate) async fn materialize(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        crate::blob::cache::materialize_row_blob(&self.database, &self.store_dir, None, reference)
            .await
    }
}

#[derive(Clone)]
enum RemoteBlobStorage<'storage> {
    Borrowed(&'storage dyn SyncStorage),
    Shared(std::sync::Arc<dyn SyncStorage>),
}

impl RemoteBlobStorage<'_> {
    fn as_ref(&self) -> &dyn SyncStorage {
        match self {
            Self::Borrowed(storage) => *storage,
            Self::Shared(storage) => storage.as_ref(),
        }
    }
}

#[derive(Clone)]
enum RemoteBlobRoot {
    Current,
    Exact(StoreRootRef),
}

#[derive(Clone)]
pub(super) struct RemoteBlobSource<'storage> {
    database: StoreDatabase,
    storage: RemoteBlobStorage<'storage>,
    root: RemoteBlobRoot,
}

impl RemoteBlobSource<'static> {
    fn current(database: StoreDatabase, storage: std::sync::Arc<dyn SyncStorage>) -> Self {
        Self {
            database,
            storage: RemoteBlobStorage::Shared(storage),
            root: RemoteBlobRoot::Current,
        }
    }
}

impl<'storage> RemoteBlobSource<'storage> {
    pub(super) fn authorized(
        database: StoreDatabase,
        storage: &'storage dyn SyncStorage,
        root: StoreRootRef,
    ) -> Self {
        Self {
            database,
            storage: RemoteBlobStorage::Borrowed(storage),
            root: RemoteBlobRoot::Exact(root),
        }
    }

    async fn exact_root(&self) -> Result<StoreRootRef, BlobCacheError> {
        match &self.root {
            RemoteBlobRoot::Exact(root) => Ok(root.clone()),
            RemoteBlobRoot::Current => self
                .database
                .local_store_root_ref()
                .await
                .map_err(BlobCacheError::Metadata)?
                .ok_or(BlobCacheError::Metadata(
                    crate::database::DbError::StoreRootHashMissing,
                )),
        }
    }

    async fn protection(
        &self,
        authority: &RowBlobAuthority,
        stored: &crate::blob::locator::StoredBlobRef,
    ) -> Result<BlobSpoolProtection, BlobCacheError> {
        match blob_opening_authority(authority, stored)? {
            BlobOpeningAuthority::Store => self
                .storage
                .as_ref()
                .store_blob_protection()
                .map_err(BlobCacheError::Storage),
            BlobOpeningAuthority::Circle {
                circle_id,
                control,
                key_fingerprint,
            } => {
                let encryption = self
                    .database
                    .circle_blob_opening_key(
                        self.exact_root().await?,
                        circle_id,
                        control.clone(),
                        key_fingerprint,
                    )
                    .await
                    .map_err(BlobCacheError::Metadata)?;
                Ok(BlobSpoolProtection::Opaque(encryption))
            }
        }
    }

    pub(super) fn store_protection(&self) -> Result<BlobSpoolProtection, StorageError> {
        self.storage.as_ref().store_blob_protection()
    }

    pub(super) async fn stage_verified_plaintext(
        &self,
        authority: &RowBlobAuthority,
        stored: &crate::blob::locator::StoredBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, BlobCacheError> {
        let protection = self.protection(authority, stored).await?;
        self.storage
            .as_ref()
            .stage_verified_blob_plaintext(stored, protection, destination)
            .await
            .map_err(BlobCacheError::Storage)
    }

    pub(super) async fn verify_plaintext(
        &self,
        store_dir: &StoreDir,
        authority: &RowBlobAuthority,
        stored: &crate::blob::locator::StoredBlobRef,
        retain: bool,
    ) -> Result<(), BlobDownloadFailureCause> {
        let protection = self
            .protection(authority, stored)
            .await
            .map_err(|error| BlobDownloadFailureCause::Metadata(error.to_string()))?;
        self.verify_plaintext_with_protection(store_dir, stored, protection, retain)
            .await
    }

    pub(super) async fn verify_plaintext_with_protection(
        &self,
        store_dir: &StoreDir,
        stored: &crate::blob::locator::StoredBlobRef,
        protection: BlobSpoolProtection,
        retain: bool,
    ) -> Result<(), BlobDownloadFailureCause> {
        let namespace = stored.locator().namespace();
        let id = stored.locator().blob_id();
        let locator_hash = stored.locator().locator_hash();
        crate::store_dir::validate_path_token(namespace)
            .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
        crate::store_dir::validate_path_token(id)
            .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
        let cache = store_dir
            .cache_blob_path(namespace, locator_hash)
            .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
        let pinned = store_dir
            .pinned_blob_path(namespace, locator_hash)
            .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
        let staged = self
            .storage
            .as_ref()
            .stage_verified_blob_plaintext(stored, protection, &cache)
            .await
            .map_err(BlobDownloadFailureCause::Storage)?;
        if !retain {
            return Ok(());
        }
        if cached_exact_in_either_folder(
            &cache,
            &pinned,
            stored.locator().plaintext_size(),
            stored.locator().plaintext_hash(),
        )
        .await
        .map_err(BlobDownloadFailureCause::Local)?
        {
            return Ok(());
        }
        match staged.commit_new().await {
            Ok(()) => {}
            Err(crate::local_blob::CommitNewFileError::DestinationExists(_)) => {
                if !cached_exact_in_either_folder(
                    &cache,
                    &pinned,
                    stored.locator().plaintext_size(),
                    stored.locator().plaintext_hash(),
                )
                .await
                .map_err(BlobDownloadFailureCause::Local)?
                {
                    return Err(BlobDownloadFailureCause::Local(
                        "occupied exact blob cache path differs from its locator".to_string(),
                    ));
                }
            }
            Err(error) => return Err(BlobDownloadFailureCause::Local(error.to_string())),
        }
        StoreBlobCache::new(self.database.clone(), store_dir.clone())
            .enforce_budget(namespace, Some(&cache))
            .await
            .map_err(|error| BlobDownloadFailureCause::Local(error.to_string()))
    }

    #[cfg(test)]
    pub(super) async fn protection_for_test(
        &self,
        authority: &RowBlobAuthority,
        stored: &crate::blob::locator::StoredBlobRef,
    ) -> Result<BlobSpoolProtection, BlobCacheError> {
        self.protection(authority, stored).await
    }

    async fn access(
        &self,
        reference: &RowBlobRef,
    ) -> Result<ExactRemoteBlobAccess<'_>, BlobCacheError> {
        let stored = reference
            .stored()
            .ok_or_else(|| BlobCacheError::LocalityUnresolved {
                id: reference.blob().id.clone(),
            })?;
        let protection = self.protection(reference.authority(), stored).await?;
        Ok(ExactRemoteBlobAccess::new(
            self.storage.as_ref(),
            protection,
        ))
    }
}

async fn cached_exact_in_either_folder(
    cache: &std::path::Path,
    pinned: &std::path::Path,
    expected_size: u64,
    expected_hash: crate::protocol::store_commit::ObjectHash,
) -> Result<bool, String> {
    for path in [cache, pinned] {
        if crate::local_blob::exists(path).await? {
            let (size, hash) = crate::local_blob::exact_file_facts(path).await?;
            if size == expected_size && hash == expected_hash {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) struct RemoteStoreBlobAccess {
    local: LocalStoreBlobAccess,
    remote: RemoteBlobSource<'static>,
    store_dir: StoreDir,
}

impl RemoteStoreBlobAccess {
    pub(crate) fn new(
        local: LocalStoreBlobAccess,
        storage: std::sync::Arc<dyn SyncStorage>,
    ) -> Self {
        Self {
            remote: RemoteBlobSource::current(local.database.clone(), storage),
            store_dir: local.store_dir.clone(),
            local,
        }
    }

    pub(crate) async fn read(&self, reference: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        if !matches!(reference.authority(), RowBlobAuthority::Remote(_)) {
            return self.local.read(reference).await;
        }
        let remote = self.remote.access(reference).await?;
        crate::blob::cache::read_blob(
            &self.remote.database,
            &self.store_dir,
            Some(remote),
            reference,
        )
        .await
    }

    pub(crate) async fn open_stream(
        &self,
        reference: &RowBlobRef,
    ) -> Result<crate::blob::cache::BlobStream, BlobCacheError> {
        if !matches!(reference.authority(), RowBlobAuthority::Remote(_)) {
            return self.local.open_stream(reference).await;
        }
        let remote = self.remote.access(reference).await?;
        crate::blob::cache::open_blob_stream(
            &self.remote.database,
            &self.store_dir,
            Some(remote),
            reference,
        )
        .await
    }

    pub(crate) async fn materialize(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        if !matches!(reference.authority(), RowBlobAuthority::Remote(_)) {
            return self.local.materialize(reference).await;
        }
        let remote = self.remote.access(reference).await?;
        crate::blob::cache::materialize_row_blob(
            &self.remote.database,
            &self.store_dir,
            Some(remote),
            reference,
        )
        .await
    }

    pub(crate) async fn stage_verified_local_copy(
        &self,
        reference: &RowBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, BlobCacheError> {
        let remote = self.remote.access(reference).await?;
        crate::blob::cache::stage_remote_blob_plaintext(
            &self.remote.database,
            &self.store_dir,
            Some(remote),
            reference,
            destination,
        )
        .await
    }
}

pub(crate) enum StoreBlobAccess {
    Local(LocalStoreBlobAccess),
    Remote(RemoteStoreBlobAccess),
}

impl StoreBlobAccess {
    pub(crate) fn new(
        local: LocalStoreBlobAccess,
        storage: Option<std::sync::Arc<dyn SyncStorage>>,
    ) -> Self {
        match storage {
            Some(storage) => Self::Remote(RemoteStoreBlobAccess::new(local, storage)),
            None => Self::Local(local),
        }
    }

    pub(crate) async fn read(&self, reference: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        match self {
            Self::Local(access) => access.read(reference).await,
            Self::Remote(access) => access.read(reference).await,
        }
    }

    pub(crate) async fn open_stream(
        &self,
        reference: &RowBlobRef,
    ) -> Result<crate::blob::cache::BlobStream, BlobCacheError> {
        match self {
            Self::Local(access) => access.open_stream(reference).await,
            Self::Remote(access) => access.open_stream(reference).await,
        }
    }

    pub(crate) async fn materialize(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        match self {
            Self::Local(access) => access.materialize(reference).await,
            Self::Remote(access) => access.materialize(reference).await,
        }
    }

    async fn remote_access(
        &self,
        reference: &RowBlobRef,
    ) -> Result<Option<ExactRemoteBlobAccess<'_>>, BlobCacheError> {
        match self {
            Self::Local(_) => Ok(None),
            Self::Remote(access)
                if matches!(reference.authority(), RowBlobAuthority::Remote(_)) =>
            {
                Ok(Some(access.remote.access(reference).await?))
            }
            Self::Remote(_) => Ok(None),
        }
    }
}

#[derive(Clone)]
pub(crate) struct StoreBlobCache {
    database: StoreDatabase,
    store_dir: StoreDir,
}

impl StoreBlobCache {
    pub(crate) fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
        Self {
            database,
            store_dir,
        }
    }

    pub(crate) async fn pin(
        &self,
        access: &StoreBlobAccess,
        blobs: &[RowBlobRef],
    ) -> Result<(), BlobCacheError> {
        let limit = self.database.transfer_limits().downloads.get();
        futures_util::stream::iter(blobs.iter().map(Ok::<&RowBlobRef, BlobCacheError>))
            .try_for_each_concurrent(limit, |reference| async move {
                crate::blob::cache::pin_one(
                    &self.database,
                    &self.store_dir,
                    || access.remote_access(reference),
                    reference,
                )
                .await
            })
            .await
    }

    pub(crate) async fn unpin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        crate::blob::cache::unpin(&self.database, &self.store_dir, blobs).await
    }

    pub(crate) async fn all_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        for blob in blobs {
            if !crate::blob::cache::is_pinned(&self.database, &self.store_dir, blob).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) async fn evict(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        crate::blob::cache::drop_cached_blob(&self.database, &self.store_dir, blob).await
    }

    pub(crate) async fn enforce_budget(
        &self,
        namespace: &str,
        protect: Option<&std::path::Path>,
    ) -> Result<(), BlobCacheError> {
        crate::blob::cache::evict_to_budget(&self.database, &self.store_dir, namespace, protect)
            .await
    }
}
