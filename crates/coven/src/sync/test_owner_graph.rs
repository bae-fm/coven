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
            cache,
            local_access,
            local_transitions,
        }
    }

    pub(crate) fn cache(&self) -> StoreBlobCache {
        self.cache.clone()
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
}
