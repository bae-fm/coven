//! Store authority resolution for blob reads and local materialization.

use futures_util::stream::TryStreamExt;

use crate::protocol::blob::{RowBlobAuthority, RowBlobRef};
use crate::protocol::objects::{BlobSpoolProtection, StorageError};
use crate::protocol::store_commit::StoreRootRef;
use crate::storage::SyncStorage;
use crate::store_dir::StoreDir;

use crate::database::StoreDatabase;

mod cache;
#[cfg(test)]
mod tests;

pub use cache::{BlobCacheError, BlobStream};
use cache::{BlobStreamSource, RemoteBlobAccess as ExactRemoteBlobAccess};

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

fn blob_opening_authority<'a>(
    authority: &'a RowBlobAuthority,
    stored: &crate::protocol::blob::locator::StoredBlobRef,
) -> Result<crate::protocol::blob::BlobOpeningAuthority<'a>, BlobCacheError> {
    authority
        .opening_authority(stored)
        .map_err(|error| match error {
            crate::protocol::blob::BlobOpeningAuthorityError::LocalityUnresolved { id } => {
                BlobCacheError::LocalityUnresolved { id }
            }
            error @ crate::protocol::blob::BlobOpeningAuthorityError::CircleAuthorityMismatch {
                ..
            } => BlobCacheError::Storage(StorageError::InvalidContent(error.to_string())),
        })
}

enum BlobSource {
    Cache,
    External,
    LocalStore,
}

fn blob_source(reference: &RowBlobRef) -> Result<BlobSource, BlobCacheError> {
    match reference.authority() {
        RowBlobAuthority::Remote(_) => Ok(BlobSource::Cache),
        RowBlobAuthority::Local | RowBlobAuthority::PendingRemote(_) => {
            Ok(match reference.blob().provenance {
                crate::protocol::blob::Provenance::UserProvided => BlobSource::External,
                crate::protocol::blob::Provenance::HostProvided => BlobSource::LocalStore,
            })
        }
    }
}

fn remote_stored_ref(
    reference: &RowBlobRef,
) -> Result<&crate::protocol::blob::locator::StoredBlobRef, BlobCacheError> {
    reference
        .stored()
        .ok_or_else(|| BlobCacheError::LocalityUnresolved {
            id: reference.blob().id.clone(),
        })
}

async fn exact_file_facts(
    path: &std::path::Path,
) -> Result<(Vec<u8>, u64, crate::protocol::store_commit::ObjectHash), String> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| format!("read blob file {}: {error}", path.display()))?;
    let size = bytes.len() as u64;
    let hash = crate::protocol::store_commit::ObjectHash::digest(&bytes);
    Ok((bytes, size, hash))
}

fn verify_file_identity(
    path: &std::path::Path,
    reference: &RowBlobRef,
    actual_size: u64,
    actual_hash: crate::protocol::store_commit::ObjectHash,
) -> Result<(), BlobCacheError> {
    if actual_size != reference.plaintext_size() || actual_hash != reference.plaintext_hash() {
        return Err(BlobCacheError::LocalIntegrity {
            path: path.to_path_buf(),
            expected_size: reference.plaintext_size(),
            actual_size,
            expected_hash: reference.plaintext_hash(),
            actual_hash,
        });
    }
    Ok(())
}

async fn read_exact_file(
    path: &std::path::Path,
    reference: &RowBlobRef,
) -> Result<Vec<u8>, BlobCacheError> {
    let (bytes, size, hash) = exact_file_facts(path).await.map_err(BlobCacheError::Io)?;
    verify_file_identity(path, reference, size, hash)?;
    Ok(bytes)
}

async fn verify_exact_file(
    path: &std::path::Path,
    reference: &RowBlobRef,
) -> Result<(), BlobCacheError> {
    let (_, size, hash) = exact_file_facts(path).await.map_err(BlobCacheError::Io)?;
    verify_file_identity(path, reference, size, hash)
}

fn verify_external_file_facts(
    reference: &RowBlobRef,
    external: &crate::database::ExternalBlob,
    size: u64,
    hash: crate::protocol::store_commit::ObjectHash,
) -> Result<(), BlobCacheError> {
    if size != external.size || size != reference.plaintext_size() {
        return Err(BlobCacheError::ExternalSizeMismatch {
            id: reference.blob().id.clone(),
            path: external.path.clone(),
        });
    }
    verify_file_identity(&external.path, reference, size, hash)
}

async fn open_local_file(path: &std::path::Path) -> Result<BlobStreamSource, BlobCacheError> {
    crate::local_file::OpenFile::open(path)
        .await
        .map(BlobStreamSource::Local)
        .map_err(BlobCacheError::Io)
}

async fn open_external_file(
    reference: &RowBlobRef,
    external: crate::database::ExternalBlob,
) -> Result<BlobStreamSource, BlobCacheError> {
    let file = crate::local_file::OpenFile::open(&external.path)
        .await
        .map_err(|source| BlobCacheError::ExternalMissing {
            id: reference.blob().id.clone(),
            path: external.path.clone(),
            source,
        })?;
    if file.size() != external.size || file.size() != reference.plaintext_size() {
        return Err(BlobCacheError::ExternalSizeMismatch {
            id: reference.blob().id.clone(),
            path: external.path,
        });
    }
    Ok(BlobStreamSource::Local(file))
}

/// The four blob operations that mean the same thing whether or not a cloud
/// home is attached: serve a blob's plaintext, prove it durable on this device,
/// open it for ranged reading, and keep a set offline.
///
/// A store with no cloud home answers them from the local store and cache
/// alone; a cloud-backed one falls back to fetching. Because the answer is the
/// same question either way, a resolved connection dispatches to whichever
/// access it holds rather than being re-matched at each operation. Operations
/// that only a cloud-backed store can carry out are not here — those stay on
/// [`RemoteStoreBlobAccess`], where a store without a home has no way to reach
/// them by accident.
#[async_trait::async_trait]
pub(crate) trait BlobAccess: Send + Sync {
    async fn read(&self, reference: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError>;
    async fn materialize(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError>;
    async fn open_stream(&self, reference: &RowBlobRef) -> Result<BlobStream, BlobCacheError>;
    async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError>;
}

#[async_trait::async_trait]
impl BlobAccess for LocalStoreBlobAccess {
    async fn read(&self, reference: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        LocalStoreBlobAccess::read(self, reference).await
    }

    async fn materialize(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        LocalStoreBlobAccess::materialize(self, reference).await
    }

    async fn open_stream(&self, reference: &RowBlobRef) -> Result<BlobStream, BlobCacheError> {
        LocalStoreBlobAccess::open_stream(self, reference).await
    }

    async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        LocalStoreBlobAccess::pin(self, blobs).await
    }
}

#[async_trait::async_trait]
impl BlobAccess for RemoteStoreBlobAccess {
    async fn read(&self, reference: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        RemoteStoreBlobAccess::read(self, reference).await
    }

    async fn materialize(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        RemoteStoreBlobAccess::materialize(self, reference).await
    }

    async fn open_stream(&self, reference: &RowBlobRef) -> Result<BlobStream, BlobCacheError> {
        RemoteStoreBlobAccess::open_stream(self, reference).await
    }

    async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        RemoteStoreBlobAccess::pin(self, blobs).await
    }
}

#[derive(Clone)]
pub(crate) struct LocalStoreBlobAccess {
    database: StoreDatabase,
    store_dir: StoreDir,
    cache: StoreBlobCache,
}

impl LocalStoreBlobAccess {
    pub(crate) fn new(database: StoreDatabase, store_dir: StoreDir, cache: StoreBlobCache) -> Self {
        Self {
            database,
            store_dir,
            cache,
        }
    }

    pub(crate) async fn drain_published_blob_drop_intents(
        &self,
        max_seq: u64,
    ) -> Result<(), String> {
        let intents = self
            .database
            .published_blob_drop_intents(max_seq)
            .await
            .map_err(|error| format!("Failed to load published blob drop intents: {error}"))?;
        for intent in intents {
            let deferred = &intent.drop;
            let asset_upload_error = |error: String| {
                crate::sync::store::StorePreparationError::AssetUpload(error).to_string()
            };
            let local = self
                .store_dir
                .local_blob_path_if_present(&deferred.namespace, &deferred.id, deferred.size)
                .await
                .map_err(|error| asset_upload_error(error.to_string()))?;
            let remove_local = match (deferred.disposition, local) {
                (crate::protocol::blob::DeferredLocalBlobDisposition::Pin, Some(source)) => {
                    self.store_dir
                        .populate_pinned_blob_from_file(
                            &deferred.namespace,
                            deferred.locator_hash,
                            deferred.size,
                            deferred.plaintext_hash,
                            &source,
                        )
                        .await
                        .map_err(|error| asset_upload_error(error.to_string()))?;
                    true
                }
                (crate::protocol::blob::DeferredLocalBlobDisposition::Cache, Some(source)) => {
                    self.cache
                        .populate_from_file(
                            &deferred.namespace,
                            deferred.locator_hash,
                            deferred.size,
                            deferred.plaintext_hash,
                            &source,
                        )
                        .await
                        .map_err(|error| asset_upload_error(error.to_string()))?;
                    true
                }
                (crate::protocol::blob::DeferredLocalBlobDisposition::Drop, _) => true,
                (
                    crate::protocol::blob::DeferredLocalBlobDisposition::Pin
                    | crate::protocol::blob::DeferredLocalBlobDisposition::Cache,
                    None,
                ) => {
                    let exact = match deferred.disposition {
                        crate::protocol::blob::DeferredLocalBlobDisposition::Pin => {
                            self.store_dir
                                .pinned_blob_is_exact(
                                    &deferred.namespace,
                                    deferred.locator_hash,
                                    deferred.size,
                                    deferred.plaintext_hash,
                                )
                                .await
                        }
                        crate::protocol::blob::DeferredLocalBlobDisposition::Cache => {
                            self.store_dir
                                .cached_blob_is_exact(
                                    &deferred.namespace,
                                    deferred.locator_hash,
                                    deferred.size,
                                    deferred.plaintext_hash,
                                )
                                .await
                        }
                        crate::protocol::blob::DeferredLocalBlobDisposition::Drop => unreachable!(),
                    }
                    .map_err(|error| asset_upload_error(error.to_string()))?;
                    if !exact {
                        return Err(asset_upload_error(format!(
                            "published blob {}/{} is missing from both the local store and its {:?} destination",
                            deferred.namespace, deferred.id, deferred.disposition,
                        )));
                    }
                    false
                }
            };
            if remove_local {
                self.store_dir
                    .remove_local_blob(&deferred.namespace, &deferred.id)
                    .await
                    .map_err(|error| asset_upload_error(error.to_string()))?;
            }
            self.database
                .clear_published_blob_drop_intent(&intent)
                .await
                .map_err(|error| format!("Failed to clear published blob drop intent: {error}"))?;
        }
        Ok(())
    }

    pub(crate) async fn read(&self, reference: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        self.database.validate_row_blob_ref(reference).await?;
        let blob = reference.blob();
        let bytes = match blob_source(reference)? {
            BlobSource::Cache => self
                .cache
                .read_exact(reference)
                .await?
                .ok_or(BlobCacheError::NoCloudHome),
            BlobSource::External => {
                let external = self
                    .database
                    .external_blob_for_row(reference)
                    .await?
                    .ok_or_else(|| BlobCacheError::NoExternalRef {
                        id: blob.id.clone(),
                    })?;
                let (bytes, size, hash) =
                    exact_file_facts(&external.path).await.map_err(|source| {
                        BlobCacheError::ExternalMissing {
                            id: reference.blob().id.clone(),
                            path: external.path.clone(),
                            source,
                        }
                    })?;
                verify_external_file_facts(reference, &external, size, hash)?;
                Ok(bytes)
            }
            BlobSource::LocalStore => {
                let path = self
                    .store_dir
                    .require_local_blob_path(&blob.namespace, &blob.id)
                    .await?;
                read_exact_file(&path, reference).await
            }
        }?;
        self.database.validate_row_blob_ref(reference).await?;
        Ok(bytes)
    }

    pub(crate) async fn open_stream(
        &self,
        reference: &RowBlobRef,
    ) -> Result<BlobStream, BlobCacheError> {
        self.database.validate_row_blob_ref(reference).await?;
        let blob = reference.blob();
        let source = match blob_source(reference)? {
            BlobSource::Cache => self
                .cache
                .open_exact(reference)
                .await?
                .ok_or(BlobCacheError::NoCloudHome),
            BlobSource::External => {
                let external = self
                    .database
                    .external_blob_for_row(reference)
                    .await?
                    .ok_or_else(|| BlobCacheError::NoExternalRef {
                        id: blob.id.clone(),
                    })?;
                open_external_file(reference, external).await
            }
            BlobSource::LocalStore => {
                let path = self
                    .store_dir
                    .require_local_blob_path(&blob.namespace, &blob.id)
                    .await?;
                open_local_file(&path).await
            }
        }?;
        self.database.validate_row_blob_ref(reference).await?;
        Ok(BlobStream::from_source(blob.clone(), source))
    }

    pub(crate) async fn materialize(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.database.validate_row_blob_ref(reference).await?;
        match blob_source(reference)? {
            BlobSource::Cache => {
                if self.cache.has_exact(reference).await? {
                    self.database.validate_row_blob_ref(reference).await?;
                    return Ok(());
                }
                Err(BlobCacheError::NoCloudHome)
            }
            BlobSource::External => {
                let external = self
                    .database
                    .external_blob_for_row(reference)
                    .await?
                    .ok_or_else(|| BlobCacheError::NoExternalRef {
                        id: reference.blob().id.clone(),
                    })?;
                let (_, size, hash) = exact_file_facts(&external.path).await.map_err(|source| {
                    BlobCacheError::ExternalMissing {
                        id: reference.blob().id.clone(),
                        path: external.path.clone(),
                        source,
                    }
                })?;
                verify_external_file_facts(reference, &external, size, hash)?;
                self.database.validate_row_blob_ref(reference).await?;
                Ok(())
            }
            BlobSource::LocalStore => {
                let path = self
                    .store_dir
                    .require_local_blob_path(&reference.blob().namespace, &reference.blob().id)
                    .await?;
                verify_exact_file(&path, reference).await?;
                self.database.validate_row_blob_ref(reference).await?;
                Ok(())
            }
        }
    }

    pub(crate) async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.cache.pin(None, blobs).await
    }

    pub(crate) async fn unpin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.cache.unpin(blobs).await
    }

    pub(crate) async fn all_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        self.cache.all_pinned(blobs).await
    }

    pub(crate) async fn evict(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.cache.evict(blob).await
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
struct RemoteBlobSourceInner<'storage> {
    database: StoreDatabase,
    storage: RemoteBlobStorage<'storage>,
    root: RemoteBlobRoot,
}

#[derive(Clone)]
pub(crate) struct CurrentRemoteBlobSource {
    inner: RemoteBlobSourceInner<'static>,
}

impl CurrentRemoteBlobSource {
    pub(crate) fn current(
        database: StoreDatabase,
        storage: std::sync::Arc<dyn SyncStorage>,
    ) -> Self {
        Self {
            inner: RemoteBlobSourceInner {
                database,
                storage: RemoteBlobStorage::Shared(storage),
                root: RemoteBlobRoot::Current,
            },
        }
    }

    async fn validate(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.inner.validate(reference).await
    }

    async fn access(
        &self,
        reference: &RowBlobRef,
    ) -> Result<ExactRemoteBlobAccess<'_>, BlobCacheError> {
        self.inner.access(reference).await
    }

    async fn stage_verified_plaintext(
        &self,
        authority: &RowBlobAuthority,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, BlobCacheError> {
        self.inner
            .stage_verified_plaintext(authority, stored, destination)
            .await
    }
}

#[derive(Clone)]
pub(crate) struct RemoteBlobSource<'storage> {
    inner: RemoteBlobSourceInner<'storage>,
}

impl<'storage> RemoteBlobSource<'storage> {
    pub(super) fn authorized(
        database: StoreDatabase,
        storage: &'storage dyn SyncStorage,
        root: StoreRootRef,
    ) -> Self {
        Self {
            inner: RemoteBlobSourceInner {
                database,
                storage: RemoteBlobStorage::Borrowed(storage),
                root: RemoteBlobRoot::Exact(root),
            },
        }
    }

    pub(super) fn store_protection(&self) -> Result<BlobSpoolProtection, StorageError> {
        self.inner.store_protection()
    }

    pub(super) async fn stage_verified_plaintext(
        &self,
        authority: &RowBlobAuthority,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, BlobCacheError> {
        self.inner
            .stage_verified_plaintext(authority, stored, destination)
            .await
    }

    pub(super) async fn verify_plaintext(
        &self,
        cache: &StoreBlobCache,
        authority: &RowBlobAuthority,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        retain: bool,
    ) -> Result<(), BlobDownloadFailureCause> {
        self.inner
            .verify_plaintext(cache, authority, stored, retain)
            .await
    }

    pub(super) async fn verify_plaintext_with_protection(
        &self,
        cache_owner: &StoreBlobCache,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        protection: BlobSpoolProtection,
        retain: bool,
    ) -> Result<(), BlobDownloadFailureCause> {
        self.inner
            .verify_plaintext_with_protection(cache_owner, stored, protection, retain)
            .await
    }

    #[cfg(test)]
    pub(super) async fn protection_for_test(
        &self,
        authority: &RowBlobAuthority,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<BlobSpoolProtection, BlobCacheError> {
        self.inner.protection(authority, stored).await
    }
}

impl RemoteBlobSourceInner<'_> {
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

    async fn validate(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.database
            .validate_row_blob_ref(reference)
            .await
            .map_err(Into::into)
    }

    async fn protection(
        &self,
        authority: &RowBlobAuthority,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<BlobSpoolProtection, BlobCacheError> {
        match blob_opening_authority(authority, stored)? {
            crate::protocol::blob::BlobOpeningAuthority::Store => self
                .storage
                .as_ref()
                .store_blob_protection()
                .map_err(BlobCacheError::Storage),
            crate::protocol::blob::BlobOpeningAuthority::Circle {
                circle_id,
                control,
                key_fingerprint,
            } => self
                .database
                .circle_blob_opening_protection(
                    self.exact_root().await?,
                    circle_id,
                    control.clone(),
                    key_fingerprint,
                )
                .await
                .map_err(BlobCacheError::Metadata),
        }
    }

    pub(super) fn store_protection(&self) -> Result<BlobSpoolProtection, StorageError> {
        self.storage.as_ref().store_blob_protection()
    }

    pub(super) async fn stage_verified_plaintext(
        &self,
        authority: &RowBlobAuthority,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, BlobCacheError> {
        let protection = self.protection(authority, stored).await?;
        self.storage
            .as_ref()
            .stage_verified_blob_plaintext(stored, protection, destination)
            .await
            .map_err(BlobCacheError::Storage)
    }

    pub(super) async fn verify_plaintext(
        &self,
        cache: &StoreBlobCache,
        authority: &RowBlobAuthority,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        retain: bool,
    ) -> Result<(), BlobDownloadFailureCause> {
        let protection = self
            .protection(authority, stored)
            .await
            .map_err(|error| BlobDownloadFailureCause::Metadata(error.to_string()))?;
        self.verify_plaintext_with_protection(cache, stored, protection, retain)
            .await
    }

    pub(super) async fn verify_plaintext_with_protection(
        &self,
        cache_owner: &StoreBlobCache,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        protection: BlobSpoolProtection,
        retain: bool,
    ) -> Result<(), BlobDownloadFailureCause> {
        let remote = ExactRemoteBlobAccess::new(self.storage.as_ref(), protection);
        cache_owner
            .verify_remote_plaintext(&remote, stored, retain)
            .await
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

#[derive(Clone)]
pub(crate) struct RemoteStoreBlobAccess {
    local: LocalStoreBlobAccess,
    remote: CurrentRemoteBlobSource,
}

impl RemoteStoreBlobAccess {
    pub(crate) fn new(local: LocalStoreBlobAccess, remote: CurrentRemoteBlobSource) -> Self {
        Self { remote, local }
    }

    pub(crate) async fn read(&self, reference: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        if !matches!(reference.authority(), RowBlobAuthority::Remote(_)) {
            return self.local.read(reference).await;
        }
        self.remote.validate(reference).await?;
        let bytes = match self.local.cache.read_exact(reference).await? {
            Some(bytes) => bytes,
            None => {
                let stored = remote_stored_ref(reference)?;
                let (_, destination) = self.local.store_dir.remote_blob_paths(
                    stored.locator().namespace(),
                    stored.locator().locator_hash(),
                )?;
                let remote = self.remote.access(reference).await?;
                let staged = remote
                    .stage_verified_plaintext(stored, &destination)
                    .await?;
                let bytes = staged.read_bytes().await.map_err(BlobCacheError::Io)?;
                self.remote.validate(reference).await?;
                self.local
                    .cache
                    .publish_materialization(staged, reference)
                    .await?;
                self.local
                    .cache
                    .enforce_budget(&reference.blob().namespace, Some(&destination))
                    .await?;
                bytes
            }
        };
        self.remote.validate(reference).await?;
        Ok(bytes)
    }

    pub(crate) async fn open_stream(
        &self,
        reference: &RowBlobRef,
    ) -> Result<BlobStream, BlobCacheError> {
        if !matches!(reference.authority(), RowBlobAuthority::Remote(_)) {
            return self.local.open_stream(reference).await;
        }
        self.remote.validate(reference).await?;
        let source = if let Some(source) = self.local.cache.open_exact(reference).await? {
            source
        } else {
            let stored = remote_stored_ref(reference)?;
            let remote = self.remote.access(reference).await?;
            if stored.locator().is_sealed() {
                BlobStreamSource::Remote(remote.open_range_reader(stored).await?)
            } else {
                self.materialize_and_open_remote(remote, reference).await?
            }
        };
        self.remote.validate(reference).await?;
        Ok(BlobStream::from_source(reference.blob().clone(), source))
    }

    pub(crate) async fn materialize(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        if !matches!(reference.authority(), RowBlobAuthority::Remote(_)) {
            return self.local.materialize(reference).await;
        }
        self.remote.validate(reference).await?;
        if self.local.cache.has_exact(reference).await? {
            self.remote.validate(reference).await?;
            return Ok(());
        }
        let stored = remote_stored_ref(reference)?;
        let (_, destination) = self.local.store_dir.remote_blob_paths(
            stored.locator().namespace(),
            stored.locator().locator_hash(),
        )?;
        let staged = self
            .stage_verified_local_copy(reference, &destination)
            .await?;
        verify_exact_file(staged.path(), reference).await?;
        self.remote.validate(reference).await?;
        self.local
            .cache
            .publish_materialization(staged, reference)
            .await
    }

    pub(crate) async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.local.cache.pin(Some(&self.remote), blobs).await
    }

    pub(crate) async fn stage_verified_local_copy(
        &self,
        reference: &RowBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, BlobCacheError> {
        self.remote.validate(reference).await?;
        if let Some(hit) = self.local.cache.cached_path(reference, false).await? {
            let staged = self
                .local
                .cache
                .stage_exact_copy(hit.path(), destination, reference)
                .await?;
            self.remote.validate(reference).await?;
            return Ok(staged);
        }
        let stored = remote_stored_ref(reference)?;
        let staged = self
            .remote
            .stage_verified_plaintext(reference.authority(), stored, destination)
            .await?;
        self.remote.validate(reference).await?;
        Ok(staged)
    }

    async fn materialize_and_open_remote(
        &self,
        remote: ExactRemoteBlobAccess<'_>,
        reference: &RowBlobRef,
    ) -> Result<BlobStreamSource, BlobCacheError> {
        let stored = remote_stored_ref(reference)?;
        let (_, destination) = self.local.store_dir.remote_blob_paths(
            stored.locator().namespace(),
            stored.locator().locator_hash(),
        )?;
        let staged = remote
            .stage_verified_plaintext(stored, &destination)
            .await?;
        verify_exact_file(staged.path(), reference).await?;
        let source = open_local_file(staged.path()).await?;
        self.remote.validate(reference).await?;
        self.local
            .cache
            .publish_materialization(staged, reference)
            .await?;
        self.local
            .cache
            .enforce_budget(&reference.blob().namespace, Some(&destination))
            .await?;
        Ok(source)
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

    async fn cached_path(
        &self,
        reference: &RowBlobRef,
        verify: bool,
    ) -> Result<Option<CachedStoreBlobPath>, BlobCacheError> {
        let stored = remote_stored_ref(reference)?;
        let (pinned, cached) = self.store_dir.remote_blob_paths(
            stored.locator().namespace(),
            stored.locator().locator_hash(),
        )?;
        for candidate in [
            CachedStoreBlobPath::Pinned(pinned),
            CachedStoreBlobPath::Evictable(cached),
        ] {
            match tokio::fs::metadata(candidate.path()).await {
                Ok(metadata) if metadata.is_file() => {
                    if verify {
                        verify_exact_file(candidate.path(), reference).await?;
                    }
                    return Ok(Some(candidate));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(BlobCacheError::Io(format!(
                        "inspect cached blob {}: {error}",
                        candidate.path().display()
                    )))
                }
            }
        }
        Ok(None)
    }

    async fn read_exact(&self, reference: &RowBlobRef) -> Result<Option<Vec<u8>>, BlobCacheError> {
        let Some(path) = self.cached_path(reference, false).await? else {
            return Ok(None);
        };
        read_exact_file(path.path(), reference).await.map(Some)
    }

    async fn open_exact(
        &self,
        reference: &RowBlobRef,
    ) -> Result<Option<BlobStreamSource>, BlobCacheError> {
        let Some(path) = self.cached_path(reference, false).await? else {
            return Ok(None);
        };
        open_local_file(path.path()).await.map(Some)
    }

    async fn has_exact(&self, reference: &RowBlobRef) -> Result<bool, BlobCacheError> {
        Ok(self.cached_path(reference, true).await?.is_some())
    }

    async fn publish_materialization(
        &self,
        staged: crate::local_file::AtomicStagedFile,
        reference: &RowBlobRef,
    ) -> Result<(), BlobCacheError> {
        match staged.commit_new().await {
            Ok(()) => Ok(()),
            Err(crate::local_file::CommitNewFileError::DestinationExists(path)) => {
                verify_exact_file(&path, reference).await
            }
            Err(error) => Err(BlobCacheError::Io(error.to_string())),
        }
    }

    async fn verify_remote_plaintext(
        &self,
        remote: &ExactRemoteBlobAccess<'_>,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        retain: bool,
    ) -> Result<(), BlobDownloadFailureCause> {
        let locator = stored.locator();
        crate::store_dir::validate_path_token(locator.namespace())
            .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
        crate::store_dir::validate_path_token(locator.blob_id())
            .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
        let destination = self
            .store_dir
            .cache_blob_path(locator.namespace(), locator.locator_hash())
            .map_err(|error| BlobDownloadFailureCause::Invalid(error.to_string()))?;
        let staged = remote
            .stage_verified_plaintext(stored, &destination)
            .await
            .map_err(|error| match error {
                BlobCacheError::Storage(error) => BlobDownloadFailureCause::Storage(error),
                other => BlobDownloadFailureCause::Local(other.to_string()),
            })?;
        if !retain {
            return Ok(());
        }
        if self
            .store_dir
            .remote_blob_is_exact(
                locator.namespace(),
                locator.locator_hash(),
                locator.plaintext_size(),
                locator.plaintext_hash(),
            )
            .await
            .map_err(|error| BlobDownloadFailureCause::Local(error.to_string()))?
        {
            return Ok(());
        }
        match staged.commit_new().await {
            Ok(()) => {}
            Err(crate::local_file::CommitNewFileError::DestinationExists(_)) => {
                if !self
                    .store_dir
                    .remote_blob_is_exact(
                        locator.namespace(),
                        locator.locator_hash(),
                        locator.plaintext_size(),
                        locator.plaintext_hash(),
                    )
                    .await
                    .map_err(|error| BlobDownloadFailureCause::Local(error.to_string()))?
                {
                    return Err(BlobDownloadFailureCause::Local(
                        "occupied exact blob cache path differs from its locator".to_string(),
                    ));
                }
            }
            Err(error) => return Err(BlobDownloadFailureCause::Local(error.to_string())),
        }
        self.enforce_budget(locator.namespace(), Some(&destination))
            .await
            .map_err(|error| BlobDownloadFailureCause::Local(error.to_string()))
    }

    async fn stage_exact_copy(
        &self,
        source: &std::path::Path,
        destination: &std::path::Path,
        reference: &RowBlobRef,
    ) -> Result<crate::local_file::AtomicStagedFile, BlobCacheError> {
        let staged = crate::local_file::AtomicStagedFile::create(destination)
            .await
            .map_err(BlobCacheError::Io)?;
        let (staged, size, digest) = staged.copy_from(source).await.map_err(BlobCacheError::Io)?;
        verify_file_identity(
            source,
            reference,
            size,
            crate::protocol::store_commit::ObjectHash::from_digest(digest),
        )?;
        Ok(staged)
    }

    async fn move_exact(
        &self,
        source: &std::path::Path,
        destination: &std::path::Path,
        reference: &RowBlobRef,
    ) -> Result<(), BlobCacheError> {
        let staged = self
            .stage_exact_copy(source, destination, reference)
            .await?;
        self.database.validate_row_blob_ref(reference).await?;
        self.publish_materialization(staged, reference).await?;
        match tokio::fs::remove_file(source).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(BlobCacheError::Io(format!(
                    "remove cached blob {}: {error}",
                    source.display()
                )))
            }
        }
        crate::atomic_file::sync_parent_dir(source)
            .await
            .map_err(BlobCacheError::Io)
    }

    pub(crate) async fn pin(
        &self,
        remote: Option<&CurrentRemoteBlobSource>,
        blobs: &[RowBlobRef],
    ) -> Result<(), BlobCacheError> {
        let limit = self.database.transfer_limits().downloads.get();
        futures_util::stream::iter(blobs.iter().map(Ok::<&RowBlobRef, BlobCacheError>))
            .try_for_each_concurrent(limit, |reference| async move {
                self.pin_one(remote, reference).await
            })
            .await
    }

    async fn pin_one(
        &self,
        remote: Option<&CurrentRemoteBlobSource>,
        reference: &RowBlobRef,
    ) -> Result<(), BlobCacheError> {
        self.database.validate_row_blob_ref(reference).await?;
        let stored = remote_stored_ref(reference)?;
        let locator = stored.locator();
        let (pinned, _) = self.store_dir.remote_blob_paths(
            stored.locator().namespace(),
            stored.locator().locator_hash(),
        )?;
        if self
            .store_dir
            .pinned_blob_is_exact(
                locator.namespace(),
                locator.locator_hash(),
                reference.plaintext_size(),
                reference.plaintext_hash(),
            )
            .await?
        {
            return Ok(());
        }
        match self.cached_path(reference, true).await? {
            Some(CachedStoreBlobPath::Pinned(_)) => return Ok(()),
            Some(CachedStoreBlobPath::Evictable(path)) => {
                return self.move_exact(&path, &pinned, reference).await;
            }
            None => {}
        }
        let remote = remote
            .ok_or(BlobCacheError::NoCloudHome)?
            .access(reference)
            .await?;
        let staged = remote.stage_verified_plaintext(stored, &pinned).await?;
        verify_exact_file(staged.path(), reference).await?;
        self.database.validate_row_blob_ref(reference).await?;
        self.publish_materialization(staged, reference).await
    }

    pub(crate) async fn populate_from_file(
        &self,
        namespace: &str,
        locator_hash: crate::protocol::store_commit::ObjectHash,
        plaintext_size: u64,
        plaintext_hash: crate::protocol::store_commit::ObjectHash,
        source: &std::path::Path,
    ) -> Result<(), BlobCacheError> {
        let destination = self
            .store_dir
            .populate_cached_blob_from_file(
                namespace,
                locator_hash,
                plaintext_size,
                plaintext_hash,
                source,
            )
            .await?;
        self.enforce_budget(namespace, Some(&destination)).await
    }

    pub(crate) async fn unpin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        for reference in blobs {
            self.database.validate_row_blob_ref(reference).await?;
            let stored = remote_stored_ref(reference)?;
            let locator = stored.locator();
            let (pinned, cached) = self.store_dir.remote_blob_paths(
                stored.locator().namespace(),
                stored.locator().locator_hash(),
            )?;
            if self
                .store_dir
                .pinned_blob_is_exact(
                    locator.namespace(),
                    locator.locator_hash(),
                    reference.plaintext_size(),
                    reference.plaintext_hash(),
                )
                .await?
            {
                self.move_exact(&pinned, &cached, reference).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn all_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        for blob in blobs {
            self.database.validate_row_blob_ref(blob).await?;
            let stored = blob
                .stored()
                .ok_or_else(|| BlobCacheError::LocalityUnresolved {
                    id: blob.blob().id.clone(),
                })?;
            let locator = stored.locator();
            if !self
                .store_dir
                .pinned_blob_is_exact(
                    locator.namespace(),
                    locator.locator_hash(),
                    blob.plaintext_size(),
                    blob.plaintext_hash(),
                )
                .await?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) async fn evict(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.database.validate_row_blob_ref(blob).await?;
        let stored = blob
            .stored()
            .ok_or_else(|| BlobCacheError::LocalityUnresolved {
                id: blob.blob().id.clone(),
            })?;
        let locator = stored.locator();
        self.store_dir
            .remove_cached_locator(locator.namespace(), locator.locator_hash())
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn enforce_budget(
        &self,
        namespace: &str,
        protect: Option<&std::path::Path>,
    ) -> Result<(), BlobCacheError> {
        let budget = match self.database.get_cache_budget(namespace).await? {
            Some(budget) => budget,
            None => return Ok(()),
        };
        let mut files = self.store_dir.cached_blob_files(namespace).await?;
        let mut total = files.iter().map(|file| file.size()).sum::<u64>();
        if let Some(protect) = protect {
            files.retain(|file| file.path() != protect);
        }
        if total <= budget {
            return Ok(());
        }
        files.sort_by_key(|file| file.recency());
        for file in files {
            if total <= budget {
                break;
            }
            let size = file.size();
            let subtract = |total: u64| {
                total.checked_sub(size).unwrap_or_else(|| {
                    panic!(
                        "evict accounting underflow at {}: size {size} > running total {total}",
                        file.path().display()
                    )
                })
            };
            if self.store_dir.remove_cached_blob_file(&file).await? {
                total = subtract(total);
            } else {
                tracing::debug!(path = %file.path().display(), "cached blob already absent during eviction");
                total = subtract(total);
            }
        }
        if total > budget {
            tracing::warn!(
                overage = total - budget,
                total,
                budget,
                "protected cached blob exceeds its namespace budget"
            );
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn populate_bytes_for_test(
        &self,
        namespace: &str,
        locator_hash: crate::protocol::store_commit::ObjectHash,
        bytes: &[u8],
    ) -> Result<(), BlobCacheError> {
        let destination = self.store_dir.cache_blob_path(namespace, locator_hash)?;
        let mut staged = crate::local_file::AtomicStagedFile::create(&destination)
            .await
            .map_err(BlobCacheError::Io)?;
        staged
            .write_bytes(bytes)
            .await
            .map_err(BlobCacheError::Io)?;
        staged.commit().await.map_err(BlobCacheError::Io)?;
        self.enforce_budget(namespace, Some(&destination)).await
    }

    #[cfg(test)]
    pub(crate) async fn populate_bytes_with_mtime_for_test(
        &self,
        namespace: &str,
        id: &str,
        bytes: &[u8],
        mtime_secs: u64,
    ) -> Result<(), BlobCacheError> {
        let locator_hash = crate::protocol::store_commit::ObjectHash::digest(id.as_bytes());
        self.populate_bytes_for_test(namespace, locator_hash, bytes)
            .await?;
        let path = self.store_dir.cache_blob_path(namespace, locator_hash)?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|error| {
                BlobCacheError::Io(format!(
                    "open cached blob {} to set modification time: {error}",
                    path.display()
                ))
            })?;
        file.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(mtime_secs))
            .map_err(|error| {
                BlobCacheError::Io(format!(
                    "set cached blob modification time {}: {error}",
                    path.display()
                ))
            })
    }

    #[cfg(test)]
    pub(crate) async fn clear_for_test(&self) -> Result<(), BlobCacheError> {
        let cache_dir = self.store_dir.cache_dir();
        match tokio::fs::remove_dir_all(&cache_dir).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(BlobCacheError::Io(format!(
                "remove cache directory {}: {error}",
                cache_dir.display()
            ))),
        }
    }
}

enum CachedStoreBlobPath {
    Pinned(std::path::PathBuf),
    Evictable(std::path::PathBuf),
}

impl CachedStoreBlobPath {
    fn path(&self) -> &std::path::Path {
        match self {
            Self::Pinned(path) | Self::Evictable(path) => path,
        }
    }
}
