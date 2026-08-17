//! Store authority resolution for blob reads and local materialization.

use futures_util::stream::TryStreamExt;

use coven_foundation::atomic_file::FileError;
use coven_foundation::store_dir::StoreDir;
use coven_protocol::blob::{RowBlobAuthority, RowBlobRef};
use coven_protocol::objects::{BlobSpoolProtection, StorageError};
use coven_protocol::store_commit::StoreRootRef;
use coven_storage::CloudSyncObjectStorage;

use coven_database::StoreDatabase;

mod cache;
pub(crate) mod eager_cache;
#[cfg(test)]
mod tests;

pub use cache::{BlobCacheError, BlobStream};
use cache::{BlobStreamSource, RemoteBlobAccess as ExactRemoteBlobAccess};

#[derive(Debug, thiserror::Error)]
pub(crate) enum PublishedBlobDropError {
    #[error("published blob drop database state: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("published local blob: {0}")]
    LocalStore(#[from] coven_foundation::store_dir::LocalBlobStoreError),
    #[error("published pinned blob: {0}")]
    Pinned(#[source] coven_foundation::store_dir::StoreBlobFileError),
    #[error("published cached blob: {0}")]
    Cache(#[from] BlobCacheError),
    #[error("published local blob removal: {0}")]
    Remove(#[from] coven_foundation::store_dir::LocalBlobRemovalError),
    #[error(
        "published blob {namespace}/{id} is missing from both the local store and its {disposition:?} destination"
    )]
    MissingDestination {
        namespace: String,
        id: String,
        disposition: coven_protocol::blob::DeferredLocalBlobDisposition,
    },
}

/// Why materializing one exact remote blob failed.
#[derive(Debug, thiserror::Error)]
pub enum BlobDownloadFailureCause {
    #[error("invalid blob path: {0}")]
    Invalid(#[source] coven_foundation::store_dir::PathTokenError),
    #[error("local cache file: {0}")]
    File(#[source] FileError),
    #[error("publish local cache file: {0}")]
    Commit(#[source] coven_foundation::local_file::CommitNewFileError),
    #[error("local cache: {0}")]
    Cache(#[source] BlobCacheError),
    #[error("occupied exact blob cache path differs from its locator")]
    OccupiedPathMismatch,
    #[error("provider: {0}")]
    Storage(#[source] StorageError),
}

fn blob_opening_authority<'a>(
    authority: &'a RowBlobAuthority,
    stored: &coven_protocol::blob::locator::StoredBlobRef,
) -> Result<coven_protocol::blob::BlobOpeningAuthority<'a>, BlobCacheError> {
    authority
        .opening_authority(stored)
        .map_err(|error| match error {
            coven_protocol::blob::BlobOpeningAuthorityError::LocalityUnresolved { id } => {
                BlobCacheError::LocalityUnresolved { id }
            }
            error @ coven_protocol::blob::BlobOpeningAuthorityError::CircleAuthorityMismatch {
                ..
            } => BlobCacheError::OpeningAuthority(error),
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
                coven_protocol::blob::Provenance::UserProvided => BlobSource::External,
                coven_protocol::blob::Provenance::HostProvided => BlobSource::LocalStore,
            })
        }
    }
}

fn remote_stored_ref(
    reference: &RowBlobRef,
) -> Result<&coven_protocol::blob::locator::StoredBlobRef, BlobCacheError> {
    reference
        .stored()
        .ok_or_else(|| BlobCacheError::LocalityUnresolved {
            id: reference.blob().id.clone(),
        })
}

async fn exact_file_facts(
    path: &std::path::Path,
) -> Result<(Vec<u8>, u64, coven_protocol::store_commit::ObjectHash), FileError> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|source| FileError::at("read blob file", path, source))?;
    let size = bytes.len() as u64;
    let hash = coven_protocol::store_commit::ObjectHash::digest(&bytes);
    Ok((bytes, size, hash))
}

fn verify_file_identity(
    path: &std::path::Path,
    reference: &RowBlobRef,
    actual_size: u64,
    actual_hash: coven_protocol::store_commit::ObjectHash,
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
    let (bytes, size, hash) = exact_file_facts(path).await.map_err(BlobCacheError::File)?;
    verify_file_identity(path, reference, size, hash)?;
    Ok(bytes)
}

async fn verify_exact_file(
    path: &std::path::Path,
    reference: &RowBlobRef,
) -> Result<(), BlobCacheError> {
    let (_, size, hash) = exact_file_facts(path).await.map_err(BlobCacheError::File)?;
    verify_file_identity(path, reference, size, hash)
}

fn verify_external_file_facts(
    reference: &RowBlobRef,
    external: &coven_database::ExternalBlob,
    size: u64,
    hash: coven_protocol::store_commit::ObjectHash,
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
    coven_foundation::local_file::OpenFile::open(path)
        .await
        .map(BlobStreamSource::Local)
        .map_err(BlobCacheError::File)
}

async fn open_external_file(
    reference: &RowBlobRef,
    external: coven_database::ExternalBlob,
) -> Result<BlobStreamSource, BlobCacheError> {
    let file = coven_foundation::local_file::OpenFile::open(&external.path)
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
pub trait BlobAccess: Send + Sync {
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
pub struct LocalStoreBlobAccess {
    database: StoreDatabase,
    store_dir: StoreDir,
    cache: StoreBlobCache,
}

impl LocalStoreBlobAccess {
    pub fn new(database: StoreDatabase, store_dir: StoreDir, cache: StoreBlobCache) -> Self {
        Self {
            database,
            store_dir,
            cache,
        }
    }

    pub(crate) async fn drain_published_blob_drop_intents(
        &self,
        max_seq: u64,
    ) -> Result<(), PublishedBlobDropError> {
        let intents = self.database.published_blob_drop_intents(max_seq).await?;
        for intent in intents {
            let deferred = &intent.drop;
            let local = self
                .store_dir
                .local_blob_path_if_present(&deferred.namespace, &deferred.id, deferred.size)
                .await?;
            let remove_local = match (deferred.disposition, local) {
                (coven_protocol::blob::DeferredLocalBlobDisposition::Pin, Some(source)) => {
                    self.store_dir
                        .populate_pinned_blob_from_file(
                            &deferred.namespace,
                            deferred.locator_hash,
                            deferred.size,
                            deferred.plaintext_hash,
                            &source,
                        )
                        .await
                        .map_err(PublishedBlobDropError::Pinned)?;
                    true
                }
                (coven_protocol::blob::DeferredLocalBlobDisposition::Cache, Some(source)) => {
                    self.cache
                        .populate_from_file(
                            &deferred.namespace,
                            deferred.locator_hash,
                            deferred.size,
                            deferred.plaintext_hash,
                            &source,
                        )
                        .await?;
                    true
                }
                (coven_protocol::blob::DeferredLocalBlobDisposition::Drop, _) => true,
                (
                    coven_protocol::blob::DeferredLocalBlobDisposition::Pin
                    | coven_protocol::blob::DeferredLocalBlobDisposition::Cache,
                    None,
                ) => {
                    let exact = match deferred.disposition {
                        coven_protocol::blob::DeferredLocalBlobDisposition::Pin => {
                            self.store_dir
                                .pinned_blob_is_exact(
                                    &deferred.namespace,
                                    deferred.locator_hash,
                                    deferred.size,
                                    deferred.plaintext_hash,
                                )
                                .await
                        }
                        coven_protocol::blob::DeferredLocalBlobDisposition::Cache => {
                            self.store_dir
                                .cached_blob_is_exact(
                                    &deferred.namespace,
                                    deferred.locator_hash,
                                    deferred.size,
                                    deferred.plaintext_hash,
                                )
                                .await
                        }
                        coven_protocol::blob::DeferredLocalBlobDisposition::Drop => unreachable!(),
                    }
                    .map_err(|error| match deferred.disposition {
                        coven_protocol::blob::DeferredLocalBlobDisposition::Pin => {
                            PublishedBlobDropError::Pinned(error)
                        }
                        coven_protocol::blob::DeferredLocalBlobDisposition::Cache => {
                            PublishedBlobDropError::Cache(error.into())
                        }
                        coven_protocol::blob::DeferredLocalBlobDisposition::Drop => unreachable!(),
                    })?;
                    if !exact {
                        return Err(PublishedBlobDropError::MissingDestination {
                            namespace: deferred.namespace.clone(),
                            id: deferred.id.clone(),
                            disposition: deferred.disposition,
                        });
                    }
                    false
                }
            };
            if remove_local {
                self.store_dir
                    .remove_local_blob(&deferred.namespace, &deferred.id)
                    .await?;
            }
            self.database
                .clear_published_blob_drop_intent(&intent)
                .await?;
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

    pub async fn unpin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.cache.unpin(blobs).await
    }

    pub async fn all_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        self.cache.all_pinned(blobs).await
    }

    pub async fn evict(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.cache.evict(blob).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn uses_store_dir_for_test(&self, expected: &StoreDir) -> bool {
        &self.store_dir == expected
    }
}

#[derive(Clone)]
enum RemoteBlobStorage<'storage> {
    Borrowed(&'storage dyn CloudSyncObjectStorage),
    Shared(std::sync::Arc<dyn CloudSyncObjectStorage>),
}

impl RemoteBlobStorage<'_> {
    fn store_access(&self) -> ExactRemoteBlobAccess<'_> {
        match self {
            Self::Borrowed(storage) => ExactRemoteBlobAccess::store(*storage),
            Self::Shared(storage) => ExactRemoteBlobAccess::store(storage.as_ref()),
        }
    }

    fn circle_access(&self, protection: BlobSpoolProtection) -> ExactRemoteBlobAccess<'_> {
        match self {
            Self::Borrowed(storage) => ExactRemoteBlobAccess::circle(*storage, protection),
            Self::Shared(storage) => ExactRemoteBlobAccess::circle(storage.as_ref(), protection),
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
pub struct CurrentRemoteBlobSource {
    inner: RemoteBlobSourceInner<'static>,
}

impl CurrentRemoteBlobSource {
    pub fn current(
        database: StoreDatabase,
        storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
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
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        stage: coven_foundation::local_file::AtomicStagedFile,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, BlobCacheError> {
        self.inner
            .stage_verified_plaintext(authority, stored, stage, progress)
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
        storage: &'storage dyn CloudSyncObjectStorage,
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

    pub(super) async fn stage_verified_plaintext(
        &self,
        authority: &RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        stage: coven_foundation::local_file::AtomicStagedFile,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, BlobCacheError> {
        self.inner
            .stage_verified_plaintext(authority, stored, stage, progress)
            .await
    }

    pub(super) async fn verify_plaintext(
        &self,
        cache: &StoreBlobCache,
        authority: &RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        retain: bool,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<(), BlobDownloadFailureCause> {
        self.inner
            .verify_plaintext(cache, authority, stored, retain, progress)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) async fn key_fingerprint_for_test(
        &self,
        authority: &RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<Option<coven_keys::encryption::KeyFingerprint>, BlobCacheError> {
        self.inner
            .resolved_access(authority, stored)
            .await?
            .key_fingerprint()
            .map_err(BlobCacheError::Storage)
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
                    coven_database::DbError::StoreRootHashMissing,
                )),
        }
    }

    async fn validate(&self, reference: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.database
            .validate_row_blob_ref(reference)
            .await
            .map_err(Into::into)
    }

    async fn resolved_access(
        &self,
        authority: &RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<ExactRemoteBlobAccess<'_>, BlobCacheError> {
        match blob_opening_authority(authority, stored)? {
            coven_protocol::blob::BlobOpeningAuthority::Store => Ok(self.storage.store_access()),
            coven_protocol::blob::BlobOpeningAuthority::Circle {
                circle_id,
                control,
                key_fingerprint,
            } => {
                let protection = self
                    .database
                    .circle_blob_opening_protection(
                        self.exact_root().await?,
                        circle_id,
                        control.clone(),
                        key_fingerprint,
                    )
                    .await
                    .map_err(BlobCacheError::Metadata)?;
                Ok(self.storage.circle_access(protection))
            }
        }
    }

    pub(super) async fn stage_verified_plaintext(
        &self,
        authority: &RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        stage: coven_foundation::local_file::AtomicStagedFile,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, BlobCacheError> {
        self.resolved_access(authority, stored)
            .await?
            .stage_verified_plaintext(stored, stage, progress)
            .await
    }

    pub(super) async fn verify_plaintext(
        &self,
        cache: &StoreBlobCache,
        authority: &RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        retain: bool,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<(), BlobDownloadFailureCause> {
        let remote = self
            .resolved_access(authority, stored)
            .await
            .map_err(BlobDownloadFailureCause::Cache)?;
        cache
            .verify_remote_plaintext(&remote, stored, retain, progress)
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
        self.resolved_access(reference.authority(), stored).await
    }
}

/// A connected sync session uses this exact remote source for verified local
/// copies, so a locality transition cannot resolve against a different cloud.
#[async_trait::async_trait]
impl crate::blob::transition::VerifiedLocalCopyStaging for RemoteStoreBlobAccess {
    async fn stage_verified_local_copy(
        &self,
        reference: &RowBlobRef,
        destination: &std::path::Path,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, BlobCacheError> {
        RemoteStoreBlobAccess::stage_verified_local_copy(self, reference, destination).await
    }
}

#[derive(Clone)]
pub struct RemoteStoreBlobAccess {
    local: LocalStoreBlobAccess,
    remote: CurrentRemoteBlobSource,
}

impl RemoteStoreBlobAccess {
    pub fn new(local: LocalStoreBlobAccess, remote: CurrentRemoteBlobSource) -> Self {
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
                let stage = self
                    .local
                    .store_dir
                    .stage_atomic_file(&destination)
                    .await
                    .map_err(BlobCacheError::File)?;
                let staged = remote
                    .stage_verified_plaintext(
                        stored,
                        stage,
                        coven_storage::cloud::no_download_progress(),
                    )
                    .await?;
                let bytes = staged.read_bytes().await.map_err(BlobCacheError::File)?;
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
        self.materialize_with_progress(reference, coven_storage::cloud::no_download_progress())
            .await
    }

    pub(crate) async fn is_materialized(
        &self,
        reference: &RowBlobRef,
    ) -> Result<bool, BlobCacheError> {
        if !matches!(reference.authority(), RowBlobAuthority::Remote(_)) {
            return Ok(true);
        }
        self.remote.validate(reference).await?;
        let materialized = self.local.cache.has_exact(reference).await?;
        self.remote.validate(reference).await?;
        Ok(materialized)
    }

    pub(crate) async fn materialize_with_progress(
        &self,
        reference: &RowBlobRef,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<(), BlobCacheError> {
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
            .stage_verified_local_copy_with_progress(reference, &destination, progress)
            .await?;
        verify_exact_file(staged.path(), reference).await?;
        self.remote.validate(reference).await?;
        self.local
            .cache
            .publish_materialization(staged, reference)
            .await?;
        self.local
            .cache
            .enforce_budget(&reference.blob().namespace, Some(&destination))
            .await
    }

    pub(crate) async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.local.cache.pin(Some(&self.remote), blobs).await
    }

    pub async fn stage_verified_local_copy(
        &self,
        reference: &RowBlobRef,
        destination: &std::path::Path,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, BlobCacheError> {
        self.stage_verified_local_copy_with_progress(
            reference,
            destination,
            coven_storage::cloud::no_download_progress(),
        )
        .await
    }

    async fn stage_verified_local_copy_with_progress(
        &self,
        reference: &RowBlobRef,
        destination: &std::path::Path,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, BlobCacheError> {
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
        let stage = self
            .local
            .store_dir
            .stage_atomic_file(destination)
            .await
            .map_err(BlobCacheError::File)?;
        let staged = self
            .remote
            .stage_verified_plaintext(reference.authority(), stored, stage, progress)
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
        let stage = self
            .local
            .store_dir
            .stage_atomic_file(&destination)
            .await
            .map_err(BlobCacheError::File)?;
        let staged = remote
            .stage_verified_plaintext(stored, stage, coven_storage::cloud::no_download_progress())
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
pub struct StoreBlobCache {
    database: StoreDatabase,
    store_dir: StoreDir,
}

impl StoreBlobCache {
    pub fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
        Self {
            database,
            store_dir,
        }
    }

    /// Delete the local-store blobs whose last row reference is gone, then
    /// report whether any intent survived the pass and must be retried.
    pub(crate) async fn drain_local_cleanup(&self) -> Result<bool, coven_database::DbError> {
        coven_database::LocalBlobCleanup::new(&self.database)
            .drain()
            .await
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
                    return Err(BlobCacheError::File(FileError::at(
                        "inspect cached blob",
                        candidate.path(),
                        error,
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
        staged: coven_foundation::local_file::AtomicStagedFile,
        reference: &RowBlobRef,
    ) -> Result<(), BlobCacheError> {
        match staged.commit_new().await {
            Ok(()) => Ok(()),
            Err(coven_foundation::local_file::CommitNewFileError::DestinationExists(path)) => {
                verify_exact_file(&path, reference).await
            }
            Err(error) => Err(BlobCacheError::Commit(error)),
        }
    }

    async fn verify_remote_plaintext(
        &self,
        remote: &ExactRemoteBlobAccess<'_>,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        retain: bool,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<(), BlobDownloadFailureCause> {
        let locator = stored.locator();
        coven_foundation::store_dir::validate_path_token(locator.namespace())
            .map_err(BlobDownloadFailureCause::Invalid)?;
        coven_foundation::store_dir::validate_path_token(locator.blob_id())
            .map_err(BlobDownloadFailureCause::Invalid)?;
        let destination = self
            .store_dir
            .cache_blob_path(locator.namespace(), locator.locator_hash())
            .map_err(BlobDownloadFailureCause::Invalid)?;
        let stage = self
            .store_dir
            .stage_atomic_file(&destination)
            .await
            .map_err(BlobDownloadFailureCause::File)?;
        let staged = remote
            .stage_verified_plaintext(stored, stage, progress)
            .await
            .map_err(|error| match error {
                BlobCacheError::Storage(error) => BlobDownloadFailureCause::Storage(error),
                other => BlobDownloadFailureCause::Cache(other),
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
            .map_err(|error| BlobDownloadFailureCause::Cache(error.into()))?
        {
            return Ok(());
        }
        match staged.commit_new().await {
            Ok(()) => {}
            Err(coven_foundation::local_file::CommitNewFileError::DestinationExists(_)) => {
                if !self
                    .store_dir
                    .remote_blob_is_exact(
                        locator.namespace(),
                        locator.locator_hash(),
                        locator.plaintext_size(),
                        locator.plaintext_hash(),
                    )
                    .await
                    .map_err(|error| BlobDownloadFailureCause::Cache(error.into()))?
                {
                    return Err(BlobDownloadFailureCause::OccupiedPathMismatch);
                }
            }
            Err(error) => return Err(BlobDownloadFailureCause::Commit(error)),
        }
        self.enforce_budget(locator.namespace(), Some(&destination))
            .await
            .map_err(BlobDownloadFailureCause::Cache)
    }

    async fn stage_exact_copy(
        &self,
        source: &std::path::Path,
        destination: &std::path::Path,
        reference: &RowBlobRef,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, BlobCacheError> {
        let staged = self
            .store_dir
            .stage_atomic_file(destination)
            .await
            .map_err(BlobCacheError::File)?;
        let (staged, size, digest) = staged
            .copy_from(source)
            .await
            .map_err(BlobCacheError::File)?;
        verify_file_identity(
            source,
            reference,
            size,
            coven_protocol::store_commit::ObjectHash::from_digest(digest),
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
                return Err(BlobCacheError::File(FileError::at(
                    "remove cached blob",
                    source,
                    error,
                )))
            }
        }
        self.store_dir
            .sync_parent_dir(source)
            .await
            .map_err(BlobCacheError::File)
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
        let stage = self
            .store_dir
            .stage_atomic_file(&pinned)
            .await
            .map_err(BlobCacheError::File)?;
        let staged = remote
            .stage_verified_plaintext(stored, stage, coven_storage::cloud::no_download_progress())
            .await?;
        verify_exact_file(staged.path(), reference).await?;
        self.database.validate_row_blob_ref(reference).await?;
        self.publish_materialization(staged, reference).await
    }

    pub(crate) async fn populate_from_file(
        &self,
        namespace: &str,
        locator_hash: coven_protocol::store_commit::ObjectHash,
        plaintext_size: u64,
        plaintext_hash: coven_protocol::store_commit::ObjectHash,
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
        locator_hash: coven_protocol::store_commit::ObjectHash,
        bytes: &[u8],
    ) -> Result<(), BlobCacheError> {
        let destination = self.store_dir.cache_blob_path(namespace, locator_hash)?;
        let mut staged = self
            .store_dir
            .stage_atomic_file(&destination)
            .await
            .map_err(BlobCacheError::File)?;
        staged
            .write_bytes(bytes)
            .await
            .map_err(BlobCacheError::File)?;
        staged.commit().await.map_err(BlobCacheError::File)?;
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
        let locator_hash = coven_protocol::store_commit::ObjectHash::digest(id.as_bytes());
        self.populate_bytes_for_test(namespace, locator_hash, bytes)
            .await?;
        let path = self.store_dir.cache_blob_path(namespace, locator_hash)?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|source| {
                BlobCacheError::File(FileError::at(
                    "open cached blob to set modification time",
                    &path,
                    source,
                ))
            })?;
        file.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(mtime_secs))
            .map_err(|source| {
                BlobCacheError::File(FileError::at(
                    "set cached blob modification time",
                    &path,
                    source,
                ))
            })
    }

    #[cfg(test)]
    pub(crate) async fn clear_for_test(&self) -> Result<(), BlobCacheError> {
        let cache_dir = self.store_dir.cache_dir();
        match tokio::fs::remove_dir_all(&cache_dir).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(BlobCacheError::File(FileError::at(
                "remove cache directory",
                cache_dir,
                source,
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
