use std::sync::Arc;

use crate::blob::transition::{
    BlobTransitionJournal, ConnectedBlobTransitions, LocalBlobTransitions,
};
use crate::database::StoreDatabase;
use crate::storage::SyncStorage;
use crate::store_dir::StoreDir;
use crate::sync::store::blob::{LocalStoreBlobAccess, StoreBlobAccess, StoreBlobCache};

#[derive(Clone)]
pub(crate) struct TestOwnerGraph {
    database: StoreDatabase,
    cache: StoreBlobCache,
    local_access: LocalStoreBlobAccess,
    local_transitions: LocalBlobTransitions,
}

fn blob_owners(
    database: StoreDatabase,
    store_dir: StoreDir,
) -> (StoreBlobCache, LocalStoreBlobAccess) {
    let cache = StoreBlobCache::new(database.clone(), store_dir.clone());
    let local_access = LocalStoreBlobAccess::new(database, store_dir, cache.clone());
    (cache, local_access)
}

pub(crate) fn local_blob_access(
    database: StoreDatabase,
    store_dir: StoreDir,
) -> LocalStoreBlobAccess {
    blob_owners(database, store_dir).1
}

impl TestOwnerGraph {
    pub(crate) fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
        let (cache, local_access) = blob_owners(database.clone(), store_dir.clone());
        let local_transitions = LocalBlobTransitions::new(
            BlobTransitionJournal::new(database.clone()),
            store_dir.clone(),
        );
        Self {
            database,
            cache,
            local_access,
            local_transitions,
        }
    }

    pub(crate) fn local_access(&self) -> LocalStoreBlobAccess {
        self.local_access.clone()
    }

    pub(crate) fn local_transitions(&self) -> LocalBlobTransitions {
        self.local_transitions.clone()
    }

    pub(crate) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        self.local_transitions
            .make_remote(root_table, root_id, pin)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn make_local(
        &self,
        storage: Arc<dyn SyncStorage>,
        routing_encryption: Option<crate::encryption::EncryptionService>,
        observer: Option<Arc<dyn crate::blob::BlobTransitionObserver>>,
        root_table: &str,
        root_id: &str,
        dest: &std::collections::HashMap<String, std::path::PathBuf>,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), crate::blob::transition::MakeLocalError> {
        self.connected_blob_transitions(storage, routing_encryption, observer)
            .make_local(root_table, root_id, dest, cancel)
            .await
    }

    pub(crate) fn blob_access(&self, storage: Option<Arc<dyn SyncStorage>>) -> StoreBlobAccess {
        match storage {
            Some(storage) => StoreBlobAccess::remote(self.local_access.connect(storage)),
            None => StoreBlobAccess::local(self.local_access.clone()),
        }
    }

    pub(crate) async fn read_blob(
        &self,
        storage: Option<Arc<dyn SyncStorage>>,
        reference: &crate::blob::RowBlobRef,
    ) -> Result<Vec<u8>, crate::sync::BlobCacheError> {
        self.blob_access(storage).read(reference).await
    }

    pub(crate) async fn open_blob_stream(
        &self,
        storage: Option<Arc<dyn SyncStorage>>,
        reference: &crate::blob::RowBlobRef,
    ) -> Result<crate::sync::BlobStream, crate::sync::BlobCacheError> {
        self.blob_access(storage).open_stream(reference).await
    }

    pub(crate) async fn read_blob_range(
        &self,
        storage: Option<Arc<dyn SyncStorage>>,
        reference: &crate::blob::RowBlobRef,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, crate::sync::BlobCacheError> {
        self.open_blob_stream(storage, reference)
            .await?
            .read_at(offset, len)
            .await
    }

    pub(crate) async fn materialize_blob(
        &self,
        storage: Option<Arc<dyn SyncStorage>>,
        reference: &crate::blob::RowBlobRef,
    ) -> Result<(), crate::sync::BlobCacheError> {
        self.blob_access(storage).materialize(reference).await
    }

    pub(crate) async fn pin_blobs(
        &self,
        storage: Option<Arc<dyn SyncStorage>>,
        references: &[crate::blob::RowBlobRef],
    ) -> Result<(), crate::sync::BlobCacheError> {
        let access = self.blob_access(storage);
        self.cache.pin(&access, references).await
    }

    pub(crate) fn connected_blob_transitions(
        &self,
        storage: Arc<dyn SyncStorage>,
        routing_encryption: Option<crate::encryption::EncryptionService>,
        observer: Option<Arc<dyn crate::blob::BlobTransitionObserver>>,
    ) -> ConnectedBlobTransitions {
        ConnectedBlobTransitions::new(
            self.local_transitions.clone(),
            self.local_access.connect(storage),
            routing_encryption,
            observer,
        )
    }

    pub(crate) async fn prepare_sync(
        &self,
        storage: impl Into<std::sync::Arc<crate::storage::CloudSyncStorage>>,
    ) -> Result<crate::sync::cycle::SyncComponents, String> {
        let expected_store_root = self
            .database
            .local_store_root_ref()
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "cycle fixture database has no exact Store root".to_string())?;
        let components = Box::pin(crate::sync::cycle::PreparedSyncComponents::prepare(
            self.database.clone(),
            self.local_access.clone(),
            storage,
            crate::sync::cycle::StoreInitialization::OpenStore {
                expected_store_root,
            },
            None,
        ))
        .await
        .map_err(|error| error.to_string())?;
        Box::pin(components.initialize())
            .await
            .map_err(|error| error.to_string())
    }
}
