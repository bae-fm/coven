use std::sync::Arc;

use crate::blob::RowBlobRef;
use crate::clock::ClockRef;
use crate::config::Config;
use crate::database::StoreDatabase;
use crate::storage::cloud::setup::StorageSetupError;
use crate::storage::BlobChunking;
use crate::store_security::StoreSecurity;
use crate::store_sync::{ConfigProvider, StoreSync};
use crate::sync::store::blob::{LocalStoreBlobAccess, RemoteBlobSource, RemoteStoreBlobAccess};
use crate::sync::{BlobCacheError, BlobStream};

#[derive(Clone)]
pub(crate) struct ReadOnlyBlobStorage {
    database: StoreDatabase,
    config_provider: ConfigProvider,
    security: StoreSecurity,
    clock: ClockRef,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    blob_chunking: BlobChunking,
    local: LocalStoreBlobAccess,
}

impl ReadOnlyBlobStorage {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        database: StoreDatabase,
        config_provider: ConfigProvider,
        security: StoreSecurity,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        blob_chunking: BlobChunking,
        local: LocalStoreBlobAccess,
    ) -> Self {
        Self {
            database,
            config_provider,
            security,
            clock,
            cloudkit_ops,
            blob_chunking,
            local,
        }
    }

    fn config(&self) -> Config {
        (self.config_provider)()
    }

    async fn resolve(&self) -> Result<ResolvedBlobAccess, StorageSetupError> {
        let config = self.config();
        if config.cloud_home.provider.is_none() {
            return Ok(ResolvedBlobAccess::Local(self.local.clone()));
        }
        let storage = self
            .security
            .create_sync_storage(
                &config,
                None,
                self.clock.clone(),
                self.cloudkit_ops.clone(),
                self.blob_chunking,
            )
            .await?;
        let storage: Arc<dyn crate::storage::SyncStorage> = Arc::new(storage);
        Ok(ResolvedBlobAccess::Remote(RemoteStoreBlobAccess::new(
            self.local.clone(),
            RemoteBlobSource::current(self.database.clone(), storage),
        )))
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
}

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
        blob: &crate::blob::BlobRef,
    ) -> Result<String, crate::storage::StorageError> {
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
    ) -> Result<crate::blob::upload::DrainOutcome, crate::store_sync::SyncError> {
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
    storage: ReadOnlyBlobStorage,
}

impl ReadStoreBlobs {
    pub(crate) fn new(database: StoreDatabase, storage: ReadOnlyBlobStorage) -> Self {
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
