use std::sync::Arc;

use crate::blob::transition::{ConnectedBlobTransitions, LocalBlobTransitions};
use crate::database::StoreDatabase;
use crate::storage::SyncStorage;
use crate::store_dir::StoreDir;
use crate::sync::store::blob::RemoteBlobSource;
use crate::sync::store::blob::{LocalStoreBlobAccess, RemoteStoreBlobAccess, StoreBlobCache};

#[derive(Clone)]
pub(crate) struct TestOwnerGraph {
    database: StoreDatabase,
    store_dir: StoreDir,
    local_access: LocalStoreBlobAccess,
    local_transitions: LocalBlobTransitions,
}

fn blob_owner(database: StoreDatabase, store_dir: StoreDir) -> LocalStoreBlobAccess {
    let cache = StoreBlobCache::new(database.clone(), store_dir.clone());
    LocalStoreBlobAccess::new(database, store_dir, cache)
}

pub(crate) fn local_blob_access(
    database: StoreDatabase,
    store_dir: StoreDir,
) -> LocalStoreBlobAccess {
    blob_owner(database, store_dir)
}

impl TestOwnerGraph {
    pub(crate) fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
        let local_access = blob_owner(database.clone(), store_dir.clone());
        let local_transitions = LocalBlobTransitions::new(database.clone(), store_dir.clone());
        Self {
            database,
            store_dir,
            local_access,
            local_transitions,
        }
    }

    /// Insert a Local release: a gated-off note plus a blob-bearing photo with an
    /// external source file registered for it. Returns the external source path.
    pub(crate) async fn seed_local_release(
        &self,
        user_dir: &std::path::Path,
        note_id: &str,
        photo_id: &str,
        cloud_path: &str,
        bytes: &[u8],
    ) -> std::path::PathBuf {
        self.database
            .seed_local_release_rows_for_test(note_id, photo_id, cloud_path, bytes)
            .await;
        std::fs::create_dir_all(user_dir).expect("create external blob fixture directory");
        let source = user_dir.join(format!("{photo_id}.jpg"));
        std::fs::write(&source, bytes).expect("write external blob fixture");
        self.database
            .register_external_blob_for_test("note_photos", photo_id, &source)
            .await;
        source
    }

    /// Insert a Remote release: a gated-on note plus a photo whose blob is already
    /// in cloud storage at the readable path the plaintext scheme derives.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn seed_remote_release(
        &self,
        store: &crate::sync::test_helpers::TestStore,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        note_id: &str,
        photo_id: &str,
        cloud_path: &str,
        bytes: &[u8],
    ) {
        self.database
            .seed_local_release_rows_for_test(note_id, photo_id, cloud_path, bytes)
            .await;
        StoreDir::store_local_blob(&self.store_dir, "fixture_sources", photo_id, bytes)
            .await
            .expect("write exact remote fixture source");
        let source = self
            .store_dir
            .local_blob_path("fixture_sources", photo_id)
            .expect("build exact remote fixture source path");
        self.database
            .register_external_blob_for_test("note_photos", photo_id, &source)
            .await;
        store
            .open_into_store_database(&self.database)
            .await
            .expect("open exact test Store");
        self.make_remote("notes", note_id, false)
            .await
            .expect("queue exact remote fixture upload");
        let outcome = store
            .drain_uploads(
                &self.database,
                &self.store_dir,
                &crate::clock::SystemClock,
                routing_encryption,
                None,
            )
            .await
            .expect("create exact remote fixture blob");
        assert_eq!(outcome.uploaded(), 1);
        assert!(outcome.yielded_for_publish());
        assert!(
            store
                .publish_pending_store_database(&self.database, &self.store_dir)
                .await
                .expect("publish exact remote fixture"),
            "remote fixture publishes its Store write",
        );
    }

    pub(crate) async fn stage_pending_upload_for_test(
        &self,
        source_dir: &std::path::Path,
        blob_id: &str,
        bytes: &[u8],
        created_at: &str,
    ) {
        let source = source_dir.join(blob_id);
        crate::storage::StagedBlobFile::write_for_test(&source, bytes)
            .await
            .expect("write upload source");
        let reference = self
            .database
            .row_blob_ref("note_photos", blob_id)
            .await
            .expect("load exact Local row blob reference");
        self.database
            .enqueue_blob_upload_for_test(
                "notes",
                &format!("note-{blob_id}"),
                &reference,
                &source,
                created_at,
            )
            .await
            .expect("enqueue exact Local row upload");
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

    fn remote_blob_access(&self, storage: Arc<dyn SyncStorage>) -> RemoteStoreBlobAccess {
        RemoteStoreBlobAccess::new(
            self.local_access.clone(),
            RemoteBlobSource::current(self.database.clone(), storage),
        )
    }

    pub(crate) async fn read_blob(
        &self,
        storage: Option<Arc<dyn SyncStorage>>,
        reference: &crate::blob::RowBlobRef,
    ) -> Result<Vec<u8>, crate::sync::BlobCacheError> {
        match storage {
            Some(storage) => self.remote_blob_access(storage).read(reference).await,
            None => self.local_access.read(reference).await,
        }
    }

    pub(crate) async fn open_blob_stream(
        &self,
        storage: Option<Arc<dyn SyncStorage>>,
        reference: &crate::blob::RowBlobRef,
    ) -> Result<crate::sync::BlobStream, crate::sync::BlobCacheError> {
        match storage {
            Some(storage) => {
                self.remote_blob_access(storage)
                    .open_stream(reference)
                    .await
            }
            None => self.local_access.open_stream(reference).await,
        }
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
        match storage {
            Some(storage) => {
                self.remote_blob_access(storage)
                    .materialize(reference)
                    .await
            }
            None => self.local_access.materialize(reference).await,
        }
    }

    pub(crate) async fn pin_blobs(
        &self,
        storage: Option<Arc<dyn SyncStorage>>,
        references: &[crate::blob::RowBlobRef],
    ) -> Result<(), crate::sync::BlobCacheError> {
        match storage {
            Some(storage) => self.remote_blob_access(storage).pin(references).await,
            None => self.local_access.pin(references).await,
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
            crate::sync::store::blob::RemoteStoreBlobAccess::new(
                self.local_access.clone(),
                RemoteBlobSource::current(self.database.clone(), storage),
            ),
            routing_encryption,
            observer,
        )
    }

    pub(crate) async fn prepare_sync(
        &self,
        storage: impl Into<std::sync::Arc<crate::storage::CloudSyncStorage>>,
        identity: crate::keys::UserKeypair,
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
            identity,
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
