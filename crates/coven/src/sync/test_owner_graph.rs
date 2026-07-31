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

impl TestOwnerGraph {
    pub(crate) fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
        let cache = StoreBlobCache::new(database.clone(), store_dir.clone());
        let local_access =
            LocalStoreBlobAccess::new(database.clone(), store_dir.clone(), cache.clone());
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

    pub(crate) fn blob_access(&self, storage: Option<Arc<dyn SyncStorage>>) -> StoreBlobAccess {
        match storage {
            Some(storage) => StoreBlobAccess::remote(self.local_access.connect(storage)),
            None => StoreBlobAccess::local(self.local_access.clone()),
        }
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
