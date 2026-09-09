use super::*;
use coven_database::Database;

pub(super) struct PreparedWriteFixture {
    home: InMemoryCloudHome,
    db: Database,
    database: StoreDatabase,
    device: crate::sync::test_helpers::TestDevice,
    device_id: String,
    write_id: coven_protocol::write::WriteId,
    commit_ref: StoreBatchCommitRef,
    package_object: coven_protocol::objects::ExactObjectRef,
    publication_object: coven_protocol::objects::ExactObjectRef,
}

impl PreparedWriteFixture {
    pub(super) fn device_id(&self) -> String {
        self.device_id.clone()
    }

    pub(super) fn write_id(&self) -> coven_protocol::write::WriteId {
        self.write_id.clone()
    }

    pub(super) fn commit_ref(&self) -> StoreBatchCommitRef {
        self.commit_ref.clone()
    }

    pub(super) fn package_object(&self) -> coven_protocol::objects::ExactObjectRef {
        self.package_object.clone()
    }

    pub(super) fn publication_object(&self) -> coven_protocol::objects::ExactObjectRef {
        self.publication_object.clone()
    }

    pub(super) fn fail_exact_create_before_call(&self, call: usize) {
        self.home.fail_exact_create_before_call(call);
    }

    pub(super) fn contains_exact_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
    ) -> bool {
        self.home.contains_exact_object(object)
    }

    pub(super) async fn active_publication(
        &self,
    ) -> Option<coven_database::ActiveStorePublication> {
        self.database
            .active_store_publication()
            .await
            .expect("read active publication attempt")
    }

    pub(super) async fn accepted_publication(
        &self,
    ) -> coven_protocol::objects::ExactProtocolObject<
        coven_protocol::store_commit::StorePublicationEntry,
    > {
        self.database
            .store_publication_entries()
            .await
            .expect("read accepted publication entries")
            .into_iter()
            .find(|entry| entry.prepared.reference() == &self.publication_object)
            .expect("published entry has accepted ownership")
    }

    pub(super) async fn write_status(&self) -> coven_protocol::write::WriteStatus {
        self.database
            .write_status(&self.write_id)
            .await
            .expect("read prepared write status")
    }

    pub(super) async fn prepared_write(&self) -> coven_database::PreparedStoreWriteCommit {
        self.database
            .oldest_prepared_store_write()
            .await
            .expect("load prepared Merge write")
            .expect("prepared Merge write exists")
    }

    pub(super) async fn prepared_write_exists(&self) -> bool {
        self.database
            .oldest_prepared_store_write()
            .await
            .expect("inspect prepared Merge write")
            .is_some()
    }

    pub(super) async fn exact_materialized_ref(
        &self,
    ) -> Option<coven_protocol::store_commit::StoreBatchCommitRef> {
        self.database
            .exact_materialized_ref(&commit_stream(&self.commit_ref), 1)
            .await
            .expect("read exact materialized position")
    }

    pub(super) async fn retained_canonical_input(&self) -> Vec<u8> {
        let stream_id = commit_stream(&self.commit_ref);
        self.db
            .retained_canonical_input_for_test(stream_id, 1)
            .await
            .expect("load retained local package application")
    }

    pub(super) async fn corrupt_retained_input(&self) {
        let stream_id = commit_stream(&self.commit_ref);
        self.db
            .corrupt_retained_materialization_input_for_test(stream_id, 1)
            .await
            .expect("corrupt retained local materialization");
    }

    pub(super) async fn retained_merge_replay_inputs(
        &self,
    ) -> Result<Vec<coven_database::OwnedVerifiedMergeMaterialization>, coven_database::DbError>
    {
        self.database
            .retained_merge_replay_inputs(self.device.store_root().clone())
            .await
    }

    pub(super) async fn drain_store_writes(&self) -> Result<u64, StoreError> {
        self.device.drain_store_writes().await
    }

    pub(super) async fn stored_remote_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
    ) -> coven_protocol::remote_object::RemoteObjectRecord {
        self.db
            .remote_object_for_test(object.clone())
            .await
            .expect("load stored remote object")
    }

    pub(super) async fn remote_object_exists(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
    ) -> bool {
        self.db
            .remote_object_exists_for_test(object.clone())
            .await
            .expect("check stored remote object")
    }

    pub(super) async fn prepare() -> Self {
        tokio::spawn(async {
            let home = InMemoryCloudHome::new();
            let keypair = UserKeypair::generate();
            let storage = Arc::new(CloudSyncConnection::new(
                Arc::new(home.clone()),
                CloudCipher::Plaintext,
                BlobPathScheme::Plain,
                "outbound-crash-test",
                keypair.clone(),
            ));
            let db_store_dir = crate::sync::test_helpers::test_store_dir();
            let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
            let device = crate::sync::test_helpers::TestDevice::create(
                &db,
                db_store_dir,
                storage,
                "outbound-crash-test",
                keypair,
            )
            .await
            .expect("create outbound crash test Store");
            let device_id = device.device_id().clone();
            let database = coven_database::StoreDatabase::new(&db);
            db.execute_test_host_write(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'outbound', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
            )
            .await;
            assert!(device
                .prepare_pending_store_write()
                .await
                .expect("prepare outbound write"));
            let batch = database
                .oldest_prepared_store_write()
                .await
                .expect("read prepared write")
                .expect("prepared write exists");
            let commit_ref = batch.commit.value.reference().clone();
            let package_object = batch
                .commit
                .value
                .store_package()
                .as_ref()
                .expect("Store package")
                .object
                .clone();
            Self {
                home,
                db,
                database,
                device,
                device_id,
                write_id: batch.commit.value.write_id.clone(),
                commit_ref,
                package_object,
                publication_object: batch.publication.entry_object.clone(),
            }
        })
        .await
        .expect("prepared Store write fixture task")
    }
}

pub(super) fn commit_stream(reference: &StoreBatchCommitRef) -> String {
    reference.coord.stream_id.to_string()
}
