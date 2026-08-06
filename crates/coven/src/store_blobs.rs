use std::sync::Arc;

use crate::config::Config;
use crate::database::StoreDatabase;
use crate::protocol::blob::RowBlobRef;
use crate::storage::cloud::setup::StorageSetupError;
use crate::store_cloud_storage::StoreCloudStorage;
use crate::store_sync::ConfigProvider;
use crate::sync::store::blob::{
    BlobAccess, CurrentRemoteBlobSource, LocalStoreBlobAccess, RemoteStoreBlobAccess,
};
use crate::sync::{BlobCacheError, BlobStream};

#[derive(Clone)]
pub(crate) struct StoreBlobAccess {
    database: StoreDatabase,
    local: LocalStoreBlobAccess,
    config_provider: ConfigProvider,
    cloud_storage: StoreCloudStorage,
    resolved: Arc<std::sync::RwLock<ResolvedBlobState>>,
    resolution: Arc<tokio::sync::Mutex<()>>,
}

impl StoreBlobAccess {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        database: StoreDatabase,
        config_provider: ConfigProvider,
        cloud_storage: StoreCloudStorage,
        local: LocalStoreBlobAccess,
    ) -> Self {
        Self {
            database,
            local,
            config_provider,
            cloud_storage,
            resolved: Arc::new(std::sync::RwLock::new(ResolvedBlobState::new())),
            resolution: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    async fn resolve(&self) -> Result<ResolvedBlobAccess, StorageSetupError> {
        let (config_provider, cloud_storage) = (&self.config_provider, &self.cloud_storage);
        loop {
            let config = config_provider();
            if let Some(access) = self.cached_access(&config) {
                return Ok(access);
            }

            let _resolution = self.resolution.lock().await;
            let config = config_provider();
            if let Some(access) = self.cached_access(&config) {
                return Ok(access);
            }
            let generation = self
                .resolved
                .read()
                .expect("read Store blob access")
                .generation;
            let access = if config.cloud_home.provider.is_none() {
                ResolvedBlobAccess::Local(self.local.clone())
            } else {
                let storage = cloud_storage.open(&config, None, None).await?;
                let storage: Arc<dyn crate::storage::SyncStorage> = Arc::new(storage);
                ResolvedBlobAccess::Remote(RemoteStoreBlobAccess::new(
                    self.local.clone(),
                    CurrentRemoteBlobSource::current(self.database.clone(), storage),
                ))
            };
            let mut state = self.resolved.write().expect("write Store blob access");
            if state.generation != generation {
                if let Some(current) = &state.connection {
                    return Ok(current.access.clone());
                }
                continue;
            }
            state.connection = Some(ResolvedBlobConnection {
                config: Some(config),
                access: access.clone(),
            });
            return Ok(access);
        }
    }

    fn cached_access(&self, config: &Config) -> Option<ResolvedBlobAccess> {
        self.resolved
            .read()
            .expect("read Store blob access")
            .connection
            .as_ref()
            .filter(|resolved| resolved.config.as_ref() == Some(config))
            .map(|resolved| resolved.access.clone())
    }

    pub(crate) fn install_connected(&self, storage: Arc<dyn crate::storage::SyncStorage>) {
        let access = ResolvedBlobAccess::Remote(RemoteStoreBlobAccess::new(
            self.local.clone(),
            CurrentRemoteBlobSource::current(self.database.clone(), storage),
        ));
        let mut state = self.resolved.write().expect("write Store blob access");
        state.generation = state
            .generation
            .checked_add(1)
            .expect("Store blob connection generation overflow");
        state.connection = Some(ResolvedBlobConnection {
            config: Some((self.config_provider)()),
            access,
        });
    }

    pub(crate) fn clear_connection(&self) {
        let mut state = self.resolved.write().expect("write Store blob access");
        state.generation = state
            .generation
            .checked_add(1)
            .expect("Store blob connection generation overflow");
        state.connection = None;
    }

    pub(crate) async fn read(&self, blob: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        self.resolve().await?.access().read(blob).await
    }

    pub(crate) async fn materialize(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.resolve().await?.access().materialize(blob).await
    }

    pub(crate) async fn open_stream(
        &self,
        blob: &RowBlobRef,
    ) -> Result<BlobStream, BlobCacheError> {
        self.resolve().await?.access().open_stream(blob).await
    }

    pub(crate) async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.resolve().await?.access().pin(blobs).await
    }

    pub(crate) async fn all_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        self.local.all_pinned(blobs).await
    }

    pub(crate) async fn stage_verified_local_copy(
        &self,
        reference: &RowBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, BlobCacheError> {
        match self.resolve().await? {
            ResolvedBlobAccess::Remote(access) => {
                access
                    .stage_verified_local_copy(reference, destination)
                    .await
            }
            ResolvedBlobAccess::Local(_) => Err(BlobCacheError::NoCloudHome),
        }
    }
}

struct ResolvedBlobConnection {
    config: Option<Config>,
    access: ResolvedBlobAccess,
}

struct ResolvedBlobState {
    generation: u64,
    connection: Option<ResolvedBlobConnection>,
}

impl ResolvedBlobState {
    fn new() -> Self {
        Self {
            generation: 0,
            connection: None,
        }
    }
}

#[derive(Clone)]
enum ResolvedBlobAccess {
    Local(LocalStoreBlobAccess),
    Remote(RemoteStoreBlobAccess),
}

impl ResolvedBlobAccess {
    /// Whichever access this connection resolved to, as the operations both
    /// answer the same way. Naming the variant is only needed for what a
    /// cloud-backed store alone can do — see
    /// [`StoreBlobAccess::stage_verified_local_copy`].
    fn access(&self) -> &dyn BlobAccess {
        match self {
            Self::Local(access) => access,
            Self::Remote(access) => access,
        }
    }
}

/// A blob's on-device life: reading it wherever its locality puts it, keeping
/// it offline, and the durable queue and cache bookkeeping the database holds.
///
/// None of that involves sync. A blob read resolves against the blob access the
/// connection installs, not the connection; only a *transition* between
/// localities needs a sync loop, and those live on [`StoreSync`] where their
/// caller reaches them directly.
#[derive(Clone)]
pub(crate) struct StoreBlobs {
    database: StoreDatabase,
    blobs: StoreBlobAccess,
    local: LocalStoreBlobAccess,
}

impl StoreBlobs {
    pub(crate) fn new(
        database: StoreDatabase,
        blobs: StoreBlobAccess,
        local: LocalStoreBlobAccess,
    ) -> Self {
        Self {
            database,
            blobs,
            local,
        }
    }

    pub(crate) async fn read(&self, blob: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        self.blobs.read(blob).await
    }

    pub(crate) async fn materialize(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.blobs.materialize(blob).await
    }

    pub(crate) async fn open_stream(
        &self,
        blob: &RowBlobRef,
    ) -> Result<BlobStream, BlobCacheError> {
        self.blobs.open_stream(blob).await
    }

    pub(crate) async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.blobs.pin(blobs).await
    }

    pub(crate) async fn unpin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.local.unpin(blobs).await
    }

    pub(crate) async fn all_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        self.blobs.all_pinned(blobs).await
    }

    pub(crate) async fn evict(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.local.evict(blob).await
    }

    pub(crate) async fn row_blob_ref(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<RowBlobRef, crate::database::DbError> {
        self.database.row_blob_ref(table, row_id).await
    }

    pub(crate) async fn queued_uploads(
        &self,
    ) -> Result<Vec<crate::QueuedUpload>, crate::database::DbError> {
        self.database.queued_uploads().await
    }

    pub(crate) async fn queued_uploads_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<crate::QueuedUpload>, crate::database::DbError> {
        self.database
            .queued_uploads_for_root(root_table, root_id)
            .await
    }

    pub(crate) async fn external_blob(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<Option<crate::ExternalBlob>, crate::database::DbError> {
        self.database.external_blob(table, row_id).await
    }

    pub(crate) async fn queued_deletes(
        &self,
    ) -> Result<Vec<crate::QueuedDelete>, crate::database::DbError> {
        self.database.queued_deletes().await
    }

    pub(crate) async fn make_remote_progress(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<crate::MakeRemoteProgress>, crate::database::DbError> {
        self.database
            .make_remote_progress(root_table, root_id)
            .await
    }

    pub(crate) async fn cache_budget(
        &self,
        namespace: &str,
    ) -> Result<Option<u64>, crate::database::DbError> {
        self.database.get_cache_budget(namespace).await
    }

    pub(crate) async fn set_cache_budget(
        &self,
        namespace: &str,
        max_bytes: u64,
    ) -> Result<(), crate::database::DbError> {
        self.database.set_cache_budget(namespace, max_bytes).await
    }
}

#[async_trait::async_trait]
impl crate::blob::transition::VerifiedLocalCopyStaging for StoreBlobAccess {
    async fn stage_verified_local_copy(
        &self,
        reference: &RowBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, BlobCacheError> {
        StoreBlobAccess::stage_verified_local_copy(self, reference, destination).await
    }
}
