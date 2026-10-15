use super::*;
use crate::store_log_sync::tests::{device, member, storage, store};
use coven_storage::{
    test_utils::MemoryStorage, AccessGrant, ByteRange, ObjectPrefix, RetainedAccess, StorageConfig,
    StorageError, StoredObject, UploadSession,
};
use tokio::sync::watch;

fn access() -> coven_format::MemberAccess {
    coven_format::MemberAccess::S3AccessKey {
        access_key_id: "fixture-access-key".into(),
        principal: coven_format::S3Principal::Console,
    }
}

/// A never-connected store's original installation (device 1) and its
/// storage-less reset copy (device 2) set up one location in turn. Without
/// reading the store folder both created it (storage
/// `StoreCreation.blind_setup_two_creations`); the second now finds the
/// first's creation marker and refuses before reserving a creation of its own
/// (`marked_setups_create_once`).
#[tokio::test]
async fn a_second_installation_never_creates_a_store_already_created_here() {
    let storage = storage();
    let mut original = device(storage.clone(), 1, member(1), store(1)).await;
    let mut copy = device(storage.clone(), 2, member(1), store(1)).await;
    original
        .sync
        .setup_storage(
            storage.clone(),
            access(),
            "phone".into(),
            Box::new(|| Ok(Box::new(|| Ok(())))),
        )
        .await
        .unwrap();
    let refused = copy
        .sync
        .setup_storage(
            storage.clone(),
            access(),
            "restored".into(),
            Box::new(|| panic!("a refused setup commits no connection state")),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&refused, SyncError::Storage(error)
            if error.failure() == coven_storage::StorageFailure::StoreExists),
        "{refused:?}"
    );
    let entries: Vec<_> = storage
        .list(&ObjectPrefix::store_logs(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path.store_log_position().unwrap().0)
        .collect();
    assert_eq!(entries, [coven_foundation::id_source::DeviceId(1)]);
    let local = copy.db.local_store_log().await.unwrap();
    assert!(local.upload.is_none() && local.log.entries.is_empty());
}

/// Holds every store-log and key-copy create until `claims` creation-marker
/// claims have been attempted, so concurrent setups both claim before either
/// writes: the interleaving a listing could not order
/// (`StoreCreation.concurrent_setups_two_creations`).
struct ClaimsFirst {
    storage: Arc<MemoryStorage>,
    claims: watch::Sender<usize>,
    wanted: usize,
}

#[async_trait::async_trait]
impl Storage for ClaimsFirst {
    fn config(&self) -> StorageConfig {
        self.storage.config()
    }
    fn single_request_limit(&self) -> u64 {
        self.storage.single_request_limit()
    }
    async fn create(
        &self,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<Option<StoredObject>, StorageError> {
        if *path == ObjectPath::created(path.store()) {
            let result = self.storage.create(path, bytes).await;
            self.claims.send_modify(|claims| *claims += 1);
            return result;
        }
        let mut claims = self.claims.subscribe();
        claims
            .wait_for(|claims| *claims >= self.wanted)
            .await
            .expect("claim counter kept");
        self.storage.create(path, bytes).await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.storage.replace(path, bytes).await
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        self.storage.status(path).await
    }
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<coven_storage::ObjectStream<'_>, StorageError> {
        self.storage.stream_object(path, range).await
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        self.storage.list(prefix).await
    }
    async fn list_writers(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<Vec<coven_foundation::id_source::DeviceId>, StorageError> {
        self.storage.list_writers(store).await
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.storage.delete(path).await
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.storage.grant_access(account).await
    }
    async fn revoke_access(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        self.storage.revoke_access(account).await
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.storage.begin_upload(path, total).await
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.storage.resume_upload(session).await
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.storage.upload_part(session, bytes).await
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.storage.finish_upload(session).await
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.storage.abort_upload(session).await
    }
}

/// T163's race: two installations of one never-connected store both reach
/// storage before either writes. Each claims the creation marker first; exactly
/// one setup creates the store, and the other refuses having written nothing
/// (`StoreCreation.concurrent_marked_setups_one_creation`).
#[tokio::test]
async fn concurrent_setups_of_one_store_create_it_once() {
    let memory = storage();
    let held: Arc<dyn Storage> = Arc::new(ClaimsFirst {
        storage: memory.clone(),
        claims: watch::channel(0).0,
        wanted: 2,
    });
    let mut original = device(memory.clone(), 1, member(1), store(1)).await;
    let mut copy = device(memory.clone(), 2, member(1), store(1)).await;
    let (first, second) = tokio::join!(
        original.sync.setup_storage(
            held.clone(),
            access(),
            "phone".into(),
            Box::new(|| Ok(Box::new(|| Ok(())))),
        ),
        copy.sync.setup_storage(
            held.clone(),
            access(),
            "restored".into(),
            Box::new(|| Ok(Box::new(|| Ok(())))),
        ),
    );
    let (winner, loser, refused) = match (first, second) {
        (Ok(()), Err(refused)) => (1, 2, refused),
        (Err(refused), Ok(())) => (2, 1, refused),
        outcome => panic!("exactly one setup creates the store: {outcome:?}"),
    };
    assert!(
        matches!(&refused, SyncError::Storage(error)
            if error.failure() == coven_storage::StorageFailure::StoreExists),
        "{refused:?}"
    );
    let marker = coven_format::creation_marker::CreationMarker::decode(
        &memory.read(&ObjectPath::created(store(1))).await.unwrap(),
    )
    .unwrap();
    assert_eq!(marker.device, coven_foundation::id_source::DeviceId(winner));
    // The loser's only request was its refused claim: no entry and no key copy.
    for object in memory.list(&ObjectPrefix::all(store(1))).await.unwrap() {
        assert_ne!(
            object.path.device(),
            Some(coven_foundation::id_source::DeviceId(loser)),
            "{}",
            object.path.as_str()
        );
    }
    let entries: Vec<_> = memory
        .list(&ObjectPrefix::store_logs(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path.store_log_position().unwrap().0)
        .collect();
    assert_eq!(entries, [coven_foundation::id_source::DeviceId(winner)]);
}

/// A setup whose claim's reply was lost finds its own marker on retry and
/// goes on to create the store (`StoreCreation.own_marker_passes`).
#[tokio::test]
async fn a_retried_setup_passes_its_own_creation_marker() {
    let storage = storage();
    storage
        .create(
            &ObjectPath::created(store(1)),
            &coven_format::creation_marker::CreationMarker {
                store: store(1),
                device: coven_foundation::id_source::DeviceId(1),
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
    let mut original = device(storage.clone(), 1, member(1), store(1)).await;
    original
        .sync
        .setup_storage(
            storage.clone(),
            access(),
            "phone".into(),
            Box::new(|| Ok(Box::new(|| Ok(())))),
        )
        .await
        .unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::store_logs(store(1)))
            .await
            .unwrap()
            .len(),
        1
    );
}
