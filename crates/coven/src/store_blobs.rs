use std::sync::Arc;

use crate::blob::cache::{BlobCacheError, BlobStream};
use crate::blob::RowBlobRef;
use crate::clock::ClockRef;
use crate::config::Config;
use crate::database::StoreDatabase;
use crate::storage::cloud::setup::StorageSetupError;
use crate::storage::BlobChunking;
use crate::store_dir::StoreDir;
use crate::store_security::StoreSecurity;
use crate::store_sync::{ConfigProvider, StoreSync};
use crate::sync::store::blob::{LocalStoreBlobAccess, StoreBlobAccess, StoreBlobCache};

#[derive(Clone)]
struct ReadOnlyBlobStorage {
    config_provider: ConfigProvider,
    security: StoreSecurity,
    clock: ClockRef,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    blob_chunking: BlobChunking,
}

impl ReadOnlyBlobStorage {
    fn config(&self) -> Config {
        (self.config_provider)()
    }

    async fn access(
        &self,
        local: LocalStoreBlobAccess,
    ) -> Result<StoreBlobAccess, StorageSetupError> {
        let config = self.config();
        if config.cloud_home.provider.is_none() {
            return Ok(StoreBlobAccess::new(local, None));
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
        Ok(StoreBlobAccess::new(local, Some(Arc::new(storage))))
    }
}

trait BlobAccessSource: Clone {
    async fn access(&self, local: LocalStoreBlobAccess) -> Result<StoreBlobAccess, BlobCacheError>;
}

#[derive(Clone)]
struct ConnectedBlobStorage {
    sync: StoreSync,
}

impl BlobAccessSource for ConnectedBlobStorage {
    async fn access(&self, local: LocalStoreBlobAccess) -> Result<StoreBlobAccess, BlobCacheError> {
        self.sync.blob_access(local).await
    }
}

impl BlobAccessSource for ReadOnlyBlobStorage {
    async fn access(&self, local: LocalStoreBlobAccess) -> Result<StoreBlobAccess, BlobCacheError> {
        ReadOnlyBlobStorage::access(self, local)
            .await
            .map_err(Into::into)
    }
}

#[derive(Clone)]
struct StoreBlobReads<Storage> {
    access: LocalStoreBlobAccess,
    cache: StoreBlobCache,
    storage: Storage,
}

impl<Storage> StoreBlobReads<Storage> {
    fn new(database: StoreDatabase, store_dir: StoreDir, storage: Storage) -> Self {
        Self {
            access: LocalStoreBlobAccess::new(database.clone(), store_dir.clone()),
            cache: StoreBlobCache::new(database, store_dir),
            storage,
        }
    }
}

impl<Storage: BlobAccessSource> StoreBlobReads<Storage> {
    async fn access(&self) -> Result<StoreBlobAccess, BlobCacheError> {
        self.storage.access(self.access.clone()).await
    }

    pub(crate) async fn read(&self, blob: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        self.access().await?.read(blob).await
    }

    pub(crate) async fn materialize(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.access().await?.materialize(blob).await
    }

    pub(crate) async fn open_stream(
        &self,
        blob: &RowBlobRef,
    ) -> Result<BlobStream, BlobCacheError> {
        self.access().await?.open_stream(blob).await
    }

    pub(crate) async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        let access = self.access().await?;
        self.cache.pin(&access, blobs).await
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
pub(crate) struct StoreBlobs {
    database: StoreDatabase,
    reads: StoreBlobReads<ConnectedBlobStorage>,
    sync: StoreSync,
}

impl StoreBlobs {
    pub(crate) fn new(database: StoreDatabase, store_dir: StoreDir, sync: StoreSync) -> Self {
        Self {
            database: database.clone(),
            reads: StoreBlobReads::new(
                database,
                store_dir,
                ConnectedBlobStorage { sync: sync.clone() },
            ),
            sync,
        }
    }

    pub(crate) async fn read(&self, blob: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        self.reads.read(blob).await
    }

    pub(crate) async fn materialize(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.reads.materialize(blob).await
    }

    pub(crate) async fn open_stream(
        &self,
        blob: &RowBlobRef,
    ) -> Result<BlobStream, BlobCacheError> {
        self.reads.open_stream(blob).await
    }

    pub(crate) async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.reads.pin(blobs).await
    }

    pub(crate) async fn unpin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.reads.unpin(blobs).await
    }

    pub(crate) async fn all_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        self.reads.all_pinned(blobs).await
    }

    pub(crate) async fn evict(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.reads.evict(blob).await
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
    reads: StoreBlobReads<ReadOnlyBlobStorage>,
}

impl ReadStoreBlobs {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        database: StoreDatabase,
        store_dir: StoreDir,
        config_provider: ConfigProvider,
        security: StoreSecurity,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        blob_chunking: BlobChunking,
    ) -> Self {
        Self {
            database: database.clone(),
            reads: StoreBlobReads::new(
                database,
                store_dir,
                ReadOnlyBlobStorage {
                    config_provider,
                    security,
                    clock,
                    cloudkit_ops,
                    blob_chunking,
                },
            ),
        }
    }

    pub(crate) async fn row_blob_ref(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<RowBlobRef, crate::database::DbError> {
        self.database.row_blob_ref(table, row_id).await
    }

    pub(crate) async fn read(&self, blob: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        self.reads.read(blob).await
    }

    pub(crate) async fn open_stream(
        &self,
        blob: &RowBlobRef,
    ) -> Result<BlobStream, BlobCacheError> {
        self.reads.open_stream(blob).await
    }

    pub(crate) async fn all_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        self.reads.all_pinned(blobs).await
    }
}
