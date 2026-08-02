use super::*;
use crate::protocol::store_commit::StorePackageInput;
use crate::protocol::store_commit::SuccessorLink;
use crate::protocol::store_commit::{commit_semantic_prefix, package_semantic_prefix};

pub(super) struct PreparedWriteFixture {
    pub(super) home: InMemoryCloudHome,
    pub(super) storage: Arc<CloudSyncStorage>,
    pub(super) db: Database,
    pub(super) database: StoreDatabase,
    pub(super) device: crate::sync::test_helpers::TestDevice,
    pub(super) keypair: UserKeypair,
    pub(super) root: StoreRootRef,
    pub(super) device_id: String,
    pub(super) write_id: crate::WriteId,
    pub(super) commit_ref: StoreBatchCommitRef,
    pub(super) package_object: crate::storage::ExactObjectRef,
    pub(super) head_object: crate::storage::ExactObjectRef,
}

impl PreparedWriteFixture {
    pub(super) async fn drain_store_writes(&self) -> Result<u64, StoreError> {
        self.device.drain_store_writes().await
    }

    pub(super) async fn abandon_merge_candidate(
        &self,
    ) -> Result<MergeCandidateAbandonment, StoreError> {
        self.device
            .abandon_merge_candidate(self.write_id.clone())
            .await
    }

    pub(super) async fn prepare_merge_candidate_abandonment(&self) -> Result<bool, StoreError> {
        self.device
            .prepare_merge_candidate_abandonment(self.write_id.clone())
            .await
    }

    pub(super) async fn publish_prepared_remote_objects(&self) -> Result<(), StoreError> {
        self.device
            .authorize_writer()
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
            .publish_prepared_remote_objects(&self.write_id)
            .await
    }

    pub(super) async fn prepare() -> Self {
        tokio::spawn(async {
            let home = InMemoryCloudHome::new();
            let keypair = UserKeypair::generate();
            let storage = Arc::new(
                CloudSyncStorage::new(
                    Arc::new(home.clone()),
                    CloudCipher::Plaintext,
                    BlobPathScheme::Plain,
                    "outbound-crash-test",
                    keypair.clone(),
                )
                .expect("in-memory home supports immutable copies"),
            );
            let db = open_test_db();
            let device = crate::sync::test_helpers::TestDevice::create(
                &db,
                storage.clone(),
                "outbound-crash-test",
                keypair.clone(),
            )
            .await
            .expect("create outbound crash test Store");
            let root = device.store_root().clone();
            let device_id = device.device_id.clone();
            let database = crate::database::StoreDatabase::new(&db);
            db.execute_test_host_write(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'outbound', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
            )
            .await;
            let (_temp, store_dir) = temp_store_dir();
            assert!(device
                .prepare_pending_store_write(&store_dir)
                .await
                .expect("prepare outbound write"));
            let batch = database
                .oldest_prepared_store_write()
                .await
                .expect("read prepared write")
                .expect("prepared write exists");
            let commit_ref = batch.head.value.commit.clone();
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
                storage,
                db,
                database,
                device,
                keypair,
                root,
                device_id,
                write_id: batch.commit.value.write_id.clone(),
                commit_ref,
                package_object,
                head_object: batch.head.object.clone(),
            }
        })
        .await
        .expect("prepared Store write fixture task")
    }

    pub(super) async fn publish_competing_merge_head(&self) -> StoreDeviceHeadRef {
        let batch = self
            .database
            .clone()
            .oldest_prepared_store_write()
            .await
            .expect("load prepared Merge write")
            .expect("prepared Merge write exists");
        let candidate = &batch.commit.value;
        let registration = self
            .database
            .clone()
            .activated_store_device_registration(candidate.author_registration.clone())
            .await
            .expect("load Merge author registration");
        let signer = registration
            .device_signer(&self.keypair)
            .expect("derive Merge device signer");
        let stream_id = batch.head.value.commit.coord.stream_id;
        let package = crate::protocol::audience_package::AudiencePackage::store(
            self.root.store_root_hash,
            candidate.candidate_family(),
            candidate.write_id.clone(),
            batch.head.value.commit.coord.clone(),
            self.db.schema_version(),
            b"competing valid package".to_vec(),
            Vec::new(),
        )
        .expect("construct competing package");
        let package_bytes = package.to_bytes();
        let package_prefix = package_semantic_prefix(
            candidate.candidate_family(),
            &stream_id.to_string(),
            candidate.seq(),
            ObjectHash::digest(&package_bytes),
        );
        let package_context = ProtocolObjectContext::store_encrypted(
            self.root.store_root_hash,
            ProtocolObjectDomain::StorePackage,
        );
        let package_slot = self
            .storage
            .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
            .await
            .expect("reserve competing package slot");
        let package_prepared = self
            .storage
            .prepare_protocol_object(
                &package_context,
                package_slot,
                &package_prefix,
                package_bytes.clone(),
            )
            .expect("prepare competing package");
        self.storage
            .create_protocol_object(&package_prepared)
            .await
            .expect("publish competing package");
        let membership = self
            .device
            .membership_for_test()
            .await
            .expect("load competing commit membership");
        let predecessor = membership
            .write_grant_authority(&registration.author_pubkey)
            .expect("Merge test author has an active write grant");
        let winner = StoreBatchCommit::signed(
            self.root.store_root_hash,
            candidate.write_id.clone(),
            batch.head.value.commit.coord.clone(),
            candidate.author_registration.clone(),
            &registration,
            candidate.order.clone(),
            candidate.membership_state.clone(),
            candidate.device_state.clone(),
            StoreOperationMembershipAuthority { predecessor },
            StorePackageInput {
                candidate_family: candidate.candidate_family(),
                schema_version: self.db.schema_version(),
                bytes: &package_bytes,
                object: package_prepared.reference().clone(),
            },
            &signer,
        )
        .expect("sign competing commit");
        let commit_prefix = commit_semantic_prefix(
            winner.candidate_family(),
            &stream_id.to_string(),
            winner.seq(),
            winner.commit_hash(),
        );
        let commit_context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let commit_slot = self
            .storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .expect("reserve competing commit slot");
        let commit_prepared = self
            .storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                winner.to_bytes(),
            )
            .expect("prepare competing commit");
        self.storage
            .create_protocol_object(&commit_prepared)
            .await
            .expect("publish competing commit");
        let winner_ref = StoreBatchCommitRef::from_commit(
            &winner,
            batch.head.value.commit.coord.clone(),
            commit_prepared.reference().clone(),
        )
        .expect("reference competing commit");
        assert_ne!(winner_ref, batch.head.value.commit);
        let winner_head = StoreDeviceHead::signed(
            self.root.store_root_hash,
            candidate.author_registration.clone(),
            winner_ref,
            batch.head.value.history_summary,
            batch.head.value.successor.clone(),
            &signer,
        )
        .expect("sign competing head");
        let head_context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let head_prefix = head_slot_prefix(&self.device_id, candidate.seq());
        let head_prepared = self
            .storage
            .prepare_protocol_object(
                &head_context,
                self.head_object.slot().clone(),
                &head_prefix,
                winner_head.to_bytes(),
            )
            .expect("prepare competing head");
        self.storage
            .create_protocol_object(&head_prepared)
            .await
            .expect("publish competing head");
        StoreDeviceHeadRef {
            head_hash: winner_head.head_hash(),
            object: head_prepared.reference().clone(),
        }
    }

    pub(super) async fn publish_alternate_head_for_prepared_commit(&self) -> StoreDeviceHeadRef {
        let batch = self
            .database
            .clone()
            .oldest_prepared_store_write()
            .await
            .expect("load prepared Merge write")
            .expect("prepared Merge write exists");
        let registration = self
            .database
            .clone()
            .activated_store_device_registration(batch.head.value.author_registration.clone())
            .await
            .expect("load Merge author registration");
        let signer = registration
            .device_signer(&self.keypair)
            .expect("derive Merge device signer");
        let head_context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let next_prefix = head_slot_prefix(&self.device_id, batch.commit.value.seq() + 1);
        let alternate_next = crate::storage::cloud::ObjectSlot::opaque(
            format!("{next_prefix}.json"),
            "alternate-successor".to_string(),
        )
        .expect("reserve alternate successor slot");
        let alternate = StoreDeviceHead::signed(
            self.root.store_root_hash,
            batch.head.value.author_registration.clone(),
            batch.head.value.commit.clone(),
            batch.head.value.history_summary,
            SuccessorLink {
                activation: batch.head.value.successor.activation,
                predecessor: batch.head.value.successor.predecessor.clone(),
                next_slot: alternate_next,
            },
            &signer,
        )
        .expect("sign alternate activating head");
        let head_prefix = head_slot_prefix(&self.device_id, batch.commit.value.seq());
        let prepared = self
            .storage
            .prepare_protocol_object(
                &head_context,
                self.head_object.slot().clone(),
                &head_prefix,
                alternate.to_bytes(),
            )
            .expect("prepare alternate activating head");
        self.storage
            .create_protocol_object(&prepared)
            .await
            .expect("publish alternate activating head");
        StoreDeviceHeadRef {
            head_hash: alternate.head_hash(),
            object: prepared.reference().clone(),
        }
    }

    pub(super) async fn stored_remote_object(
        &self,
        object: &crate::storage::ExactObjectRef,
    ) -> crate::protocol::remote_object::RemoteObjectRecord {
        self.db
            .remote_object_for_test(object.clone())
            .await
            .expect("load stored remote object")
    }

    pub(super) async fn remote_object_exists(
        &self,
        object: &crate::storage::ExactObjectRef,
    ) -> bool {
        self.db
            .remote_object_exists_for_test(object.clone())
            .await
            .expect("check stored remote object")
    }
}

pub(super) fn commit_stream(reference: &StoreBatchCommitRef) -> String {
    reference.coord.stream_id.to_string()
}
