use std::sync::Arc;

use crate::config::Config;
use crate::database::StoreDatabase;
use crate::protocol::blob::RowBlobRef;
use crate::storage::cloud::setup::StorageSetupError;
use crate::store_cloud_storage::StoreCloudStorage;
use crate::store_sync::{ConfigProvider, StoreSync};
use crate::sync::store::blob::{
    CurrentRemoteBlobSource, LocalStoreBlobAccess, RemoteStoreBlobAccess,
};
use crate::sync::{BlobCacheError, BlobStream};

#[derive(Clone)]
pub(crate) struct StoreBlobAccess {
    database: StoreDatabase,
    local: LocalStoreBlobAccess,
    resolver: BlobAccessResolver,
    resolved: Arc<std::sync::RwLock<ResolvedBlobState>>,
    resolution: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone)]
enum BlobAccessResolver {
    Configured {
        config_provider: ConfigProvider,
        cloud_storage: StoreCloudStorage,
    },
    #[cfg(test)]
    Exact,
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
            resolver: BlobAccessResolver::Configured {
                config_provider,
                cloud_storage,
            },
            resolved: Arc::new(std::sync::RwLock::new(ResolvedBlobState::new())),
            resolution: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    #[cfg(test)]
    pub(crate) fn connected_for_test(
        database: StoreDatabase,
        local: LocalStoreBlobAccess,
        storage: Arc<dyn crate::storage::SyncStorage>,
    ) -> Self {
        let access = ResolvedBlobAccess::Remote(RemoteStoreBlobAccess::new(
            local.clone(),
            CurrentRemoteBlobSource::current(database.clone(), storage),
        ));
        Self {
            database,
            local,
            resolver: BlobAccessResolver::Exact,
            resolved: Arc::new(std::sync::RwLock::new(ResolvedBlobState {
                generation: 0,
                connection: Some(ResolvedBlobConnection {
                    config: None,
                    access,
                }),
            })),
            resolution: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    async fn resolve(&self) -> Result<ResolvedBlobAccess, StorageSetupError> {
        let (config_provider, cloud_storage) = match &self.resolver {
            BlobAccessResolver::Configured {
                config_provider,
                cloud_storage,
            } => (config_provider, cloud_storage),
            #[cfg(test)]
            BlobAccessResolver::Exact => {
                return Ok(self
                    .resolved
                    .read()
                    .expect("read exact Store blob access")
                    .connection
                    .as_ref()
                    .expect("exact Store blob access retains its connection")
                    .access
                    .clone());
            }
        };
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
            config: match &self.resolver {
                BlobAccessResolver::Configured {
                    config_provider, ..
                } => Some(config_provider()),
                #[cfg(test)]
                BlobAccessResolver::Exact => None,
            },
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
        self.resolve().await?.read(blob).await
    }

    pub(crate) async fn materialize(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.resolve().await?.materialize(blob).await
    }

    pub(crate) async fn open_stream(
        &self,
        blob: &RowBlobRef,
    ) -> Result<BlobStream, BlobCacheError> {
        self.resolve().await?.open_stream(blob).await
    }

    pub(crate) async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.resolve().await?.pin(blobs).await
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
    async fn read(&self, blob: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        match self {
            Self::Local(access) => access.read(blob).await,
            Self::Remote(access) => access.read(blob).await,
        }
    }

    async fn materialize(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        match self {
            Self::Local(access) => access.materialize(blob).await,
            Self::Remote(access) => access.materialize(blob).await,
        }
    }

    async fn open_stream(&self, blob: &RowBlobRef) -> Result<BlobStream, BlobCacheError> {
        match self {
            Self::Local(access) => access.open_stream(blob).await,
            Self::Remote(access) => access.open_stream(blob).await,
        }
    }

    async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        match self {
            Self::Local(access) => access.pin(blobs).await,
            Self::Remote(access) => access.pin(blobs).await,
        }
    }
}

#[derive(Clone)]
pub(crate) struct StoreBlobs {
    database: StoreDatabase,
    sync: StoreSync,
}

impl StoreBlobs {
    pub(crate) fn new(database: StoreDatabase, sync: StoreSync) -> Self {
        Self { database, sync }
    }

    pub(crate) async fn read(&self, blob: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        self.sync.read_blob(blob).await
    }

    pub(crate) async fn materialize(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.sync.materialize_blob(blob).await
    }

    pub(crate) async fn open_stream(
        &self,
        blob: &RowBlobRef,
    ) -> Result<BlobStream, BlobCacheError> {
        self.sync.open_blob_stream(blob).await
    }

    pub(crate) async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.sync.pin_blobs(blobs).await
    }

    pub(crate) async fn unpin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.sync.unpin_blobs(blobs).await
    }

    pub(crate) async fn all_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        self.sync.all_blobs_pinned(blobs).await
    }

    pub(crate) async fn evict(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.sync.evict_blob(blob).await
    }

    pub(crate) async fn row_blob_ref(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<RowBlobRef, crate::database::DbError> {
        self.database.row_blob_ref(table, row_id).await
    }

    pub(crate) fn cloud_key(
        &self,
        blob: &crate::protocol::blob::BlobRef,
    ) -> Result<String, crate::protocol::objects::StorageError> {
        self.sync.blob_cloud_key(blob)
    }

    pub(crate) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        self.sync.make_remote(root_table, root_id, pin).await
    }

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        self.sync.cancel_make_remote(root_table, root_id).await
    }

    pub(crate) async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &std::collections::HashMap<String, std::path::PathBuf>,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), crate::blob::transition::MakeLocalError> {
        self.sync
            .make_local(root_table, root_id, dest, cancel)
            .await
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

    pub(crate) async fn drain_uploads(
        &self,
    ) -> Result<crate::protocol::blob::DrainOutcome, crate::store_sync::SyncError> {
        self.sync.drain_uploads().await
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

#[derive(Clone)]
pub(crate) struct ReadStoreBlobs {
    database: StoreDatabase,
    storage: StoreBlobAccess,
}

impl ReadStoreBlobs {
    pub(crate) fn new(database: StoreDatabase, storage: StoreBlobAccess) -> Self {
        Self { database, storage }
    }

    pub(crate) async fn row_blob_ref(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<RowBlobRef, crate::database::DbError> {
        self.database.row_blob_ref(table, row_id).await
    }

    pub(crate) async fn read(&self, blob: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        self.storage.read(blob).await
    }

    pub(crate) async fn open_stream(
        &self,
        blob: &RowBlobRef,
    ) -> Result<BlobStream, BlobCacheError> {
        self.storage.open_stream(blob).await
    }

    pub(crate) async fn all_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        self.storage.all_pinned(blobs).await
    }
}
