use std::sync::Arc;

use crate::blob::transition::{ConnectedBlobTransitions, LocalBlobTransitions};
use crate::sync::store::blob::{
    CurrentRemoteBlobSource, LocalStoreBlobAccess, RemoteStoreBlobAccess, StoreBlobCache,
};
use coven_database::StoreDatabase;
use coven_foundation::store_dir::StoreDir;
use coven_storage::CloudSyncObjectStorage;

#[derive(Clone)]
pub struct TestOwnerGraph {
    database: StoreDatabase,
    store_dir: StoreDir,
    local_access: LocalStoreBlobAccess,
    local_transitions: LocalBlobTransitions,
}

fn blob_owner(database: StoreDatabase, store_dir: StoreDir) -> LocalStoreBlobAccess {
    let cache = StoreBlobCache::new(database.clone(), store_dir.clone());
    LocalStoreBlobAccess::new(database, store_dir, cache)
}

impl TestOwnerGraph {
    pub fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
        database.assert_owns_payload_directory_for_test(&store_dir);
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
    pub async fn seed_local_release(
        &self,
        user_dir: &std::path::Path,
        note_id: &str,
        photo_id: &str,
        cloud_path: &str,
        bytes: &[u8],
    ) -> std::path::PathBuf {
        self.database
            .seed_local_release_rows_for_test(None, note_id, photo_id, cloud_path, bytes)
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
    pub async fn seed_remote_release(
        &self,
        store: &crate::sync::test_helpers::TestStore,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        note_id: &str,
        photo_id: &str,
        cloud_path: &str,
        bytes: &[u8],
    ) {
        self.database
            .seed_local_release_rows_for_test(
                routing_encryption.cloned(),
                note_id,
                photo_id,
                cloud_path,
                bytes,
            )
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
            .open_into_store_database(&self.database, self.store_dir.clone())
            .await
            .expect("open exact test Store");
        self.make_remote("notes", note_id, "Notes Root", false)
            .await
            .expect("queue exact remote fixture upload");
        let outcome = store
            .drain_uploads(
                &self.database,
                &self.store_dir,
                &coven_foundation::clock::SystemClock,
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

    pub async fn stage_pending_upload_for_test(
        &self,
        source_dir: &std::path::Path,
        blob_id: &str,
        bytes: &[u8],
        created_at: &str,
    ) {
        let source = source_dir.join(blob_id);
        coven_foundation::local_file::AtomicStagedFile::write_for_test(&source, bytes)
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

    pub async fn drain_published_blob_drop_intents(
        &self,
        through_sequence: u64,
    ) -> Result<(), crate::sync::test_helpers::TestError> {
        Ok(self
            .local_access
            .drain_published_blob_drop_intents(through_sequence)
            .await?)
    }

    pub async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        root_label: &str,
        pin: bool,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        let refs = self
            .database
            .row_blob_refs_for_root(root_table, root_id)
            .await?;
        self.local_transitions
            .make_remote(root_table, root_id, root_label, pin, refs)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn make_local(
        &self,
        storage: Arc<dyn CloudSyncObjectStorage>,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
        observer: Option<Arc<dyn coven_protocol::blob::BlobTransitionObserver>>,
        root_table: &str,
        root_id: &str,
        dest: &std::collections::HashMap<String, std::path::PathBuf>,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), crate::blob::transition::MakeLocalError> {
        self.connected_blob_transitions(storage, routing_encryption, observer)
            .make_local(root_table, root_id, dest, cancel)
            .await
    }

    fn remote_blob_access(
        &self,
        storage: Arc<dyn CloudSyncObjectStorage>,
    ) -> RemoteStoreBlobAccess {
        RemoteStoreBlobAccess::new(
            self.local_access.clone(),
            CurrentRemoteBlobSource::current(self.database.clone(), storage),
        )
    }

    /// Run the eager cache fill the sync loop runs behind its cycles, which is
    /// what makes an eager blob local now that a pull downloads nothing.
    pub async fn fill_eager_cache(
        &self,
        storage: Arc<dyn CloudSyncObjectStorage>,
    ) -> Result<(), std::sync::Arc<crate::sync::EagerCacheFillError>> {
        let (_cancel, cancel_rx) = tokio::sync::watch::channel(false);
        let (status, _status_rx) =
            tokio::sync::watch::channel(crate::sync::EagerCacheFillStatus::Scanning);
        crate::sync::store::blob::eager_cache::run(
            &self.database,
            &self.remote_blob_access(storage),
            cancel_rx,
            &status,
        )
        .await
    }

    pub async fn read_blob(
        &self,
        storage: Option<Arc<dyn CloudSyncObjectStorage>>,
        reference: &coven_protocol::blob::RowBlobRef,
    ) -> Result<Vec<u8>, crate::sync::BlobCacheError> {
        match storage {
            Some(storage) => self.remote_blob_access(storage).read(reference).await,
            None => self.local_access.read(reference).await,
        }
    }

    pub async fn open_blob_stream(
        &self,
        storage: Option<Arc<dyn CloudSyncObjectStorage>>,
        reference: &coven_protocol::blob::RowBlobRef,
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

    pub async fn read_blob_range(
        &self,
        storage: Option<Arc<dyn CloudSyncObjectStorage>>,
        reference: &coven_protocol::blob::RowBlobRef,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, crate::sync::BlobCacheError> {
        self.open_blob_stream(storage, reference)
            .await?
            .read_at(offset, len)
            .await
    }

    pub async fn materialize_blob(
        &self,
        storage: Option<Arc<dyn CloudSyncObjectStorage>>,
        reference: &coven_protocol::blob::RowBlobRef,
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

    pub async fn pin_blobs(
        &self,
        storage: Option<Arc<dyn CloudSyncObjectStorage>>,
        references: &[coven_protocol::blob::RowBlobRef],
    ) -> Result<(), crate::sync::BlobCacheError> {
        match storage {
            Some(storage) => self.remote_blob_access(storage).pin(references).await,
            None => self.local_access.pin(references).await,
        }
    }

    fn connected_blob_transitions(
        &self,
        storage: Arc<dyn CloudSyncObjectStorage>,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
        observer: Option<Arc<dyn coven_protocol::blob::BlobTransitionObserver>>,
    ) -> ConnectedBlobTransitions {
        ConnectedBlobTransitions::new(
            self.local_transitions.clone(),
            Arc::new(crate::sync::store::blob::RemoteStoreBlobAccess::new(
                self.local_access.clone(),
                crate::sync::store::blob::CurrentRemoteBlobSource::current(
                    self.database.clone(),
                    storage,
                ),
            )),
            routing_encryption,
            observer,
        )
    }

    pub async fn run_sync_cycle(
        &self,
        storage: impl Into<std::sync::Arc<coven_storage::CloudSyncConnection>>,
        identity: coven_keys::keys::UserKeypair,
    ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::test_helpers::TestError> {
        let expected_store_root = self.database.local_store_root_ref().await?.ok_or_else(|| {
            crate::sync::test_helpers::TestError::invariant(
                "cycle fixture database has no exact Store root",
            )
        })?;
        let components = Box::pin(crate::sync::cycle::PreparedSyncComponents::prepare(
            self.database.clone(),
            self.store_dir.clone(),
            storage,
            identity,
            crate::sync::cycle::StoreInitialization::OpenStore {
                expected_store_root,
            },
            None,
            std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default()),
        ))
        .await?;
        let components = Box::pin(components.initialize(None)).await?;
        Ok(components
            .run_cycle(&coven_foundation::clock::SystemClock, None)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "payload directory does not belong to this database")]
    fn owner_graph_rejects_a_database_payload_directory_mismatch() {
        let database_store_dir = crate::sync::test_helpers::test_store_dir();
        let database = crate::sync::test_helpers::open_test_db(database_store_dir);
        let unrelated_store_dir = crate::sync::test_helpers::test_store_dir();

        TestOwnerGraph::new(StoreDatabase::new(&database), unrelated_store_dir);
    }
}
