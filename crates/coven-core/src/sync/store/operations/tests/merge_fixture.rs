use super::*;
use crate::sync::store_commit::StorePackageInput;
use crate::sync::store_commit::SuccessorLink;

pub(super) struct PreparedWriteFixture {
    pub(super) home: InMemoryCloudHome,
    pub(super) storage: CloudSyncStorage,
    pub(super) db: Database,
    pub(super) database: StoreDatabase,
    pub(super) keypair: UserKeypair,
    pub(super) root: StoreRootRef,
    pub(super) device_id: String,
    pub(super) write_id: crate::WriteId,
    pub(super) commit_ref: StoreBatchCommitRef,
    pub(super) package_object: crate::sync::storage::ExactObjectRef,
    pub(super) head_object: crate::sync::storage::ExactObjectRef,
}

pub(super) async fn prepared_write_fixture() -> PreparedWriteFixture {
    tokio::spawn(async {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "outbound-crash-test",
            keypair.clone(),
        )
        .expect("in-memory home supports immutable copies");
        let db = open_test_db();
        let (root, device_id) =
            initialize_exact_store(&db, &storage, "outbound-crash-test", &keypair).await;
        let database = StoreDatabase::new(&db);
        let membership = crate::sync::store::membership::load_and_persist_owner_anchor(
            &storage,
            &root,
            &crate::keys::public_key_hex(&keypair),
            &crate::sync::store::database::StoreDatabase::new(&db),
        )
        .await
        .expect("load exact founder membership");
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'outbound', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_merge_store_write(
            &db,
            &storage,
            &device_id,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            &membership,
        )
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
        PreparedWriteFixture {
            home,
            storage,
            db,
            database,
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

pub(super) async fn publish_competing_merge_head(
    fixture: &PreparedWriteFixture,
) -> StoreDeviceHeadRef {
    let batch = crate::sync::store::database::StoreDatabase::new(&fixture.db)
        .oldest_prepared_store_write()
        .await
        .expect("load prepared Merge write")
        .expect("prepared Merge write exists");
    let candidate = &batch.commit.value;
    let registration = crate::sync::store::database::StoreDatabase::new(&fixture.db)
        .activated_store_device_registration(candidate.author_registration.clone())
        .await
        .expect("load Merge author registration");
    let signer = registration
        .device_signer(&fixture.keypair)
        .expect("derive Merge device signer");
    let stream_id = batch.head.value.commit.coord.stream_id;
    let package = crate::sync::audience_package::AudiencePackage::store(
        fixture.root.store_root_hash,
        candidate.candidate_family(),
        candidate.write_id.clone(),
        batch.head.value.commit.coord.clone(),
        fixture.db.schema_version(),
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
        fixture.root.store_root_hash,
        ProtocolObjectDomain::StorePackage,
    );
    let package_slot = fixture
        .storage
        .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
        .await
        .expect("reserve competing package slot");
    let package_prepared = fixture
        .storage
        .prepare_protocol_object(
            &package_context,
            package_slot,
            &package_prefix,
            package_bytes.clone(),
        )
        .expect("prepare competing package");
    fixture
        .storage
        .create_protocol_object(&package_prepared)
        .await
        .expect("publish competing package");
    let membership = crate::sync::store::pull::load_cycle_membership(
        &fixture.storage,
        &crate::sync::store::database::StoreDatabase::new(&fixture.db),
    )
    .await
    .expect("load competing commit membership");
    let predecessor = membership
        .write_grant_authority(&registration.author_pubkey)
        .expect("Merge test author has an active write grant");
    let winner = StoreBatchCommit::signed(
        fixture.root.store_root_hash,
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
            schema_version: fixture.db.schema_version(),
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
        fixture.root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let commit_slot = fixture
        .storage
        .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
        .await
        .expect("reserve competing commit slot");
    let commit_prepared = fixture
        .storage
        .prepare_protocol_object(
            &commit_context,
            commit_slot,
            &commit_prefix,
            winner.to_bytes(),
        )
        .expect("prepare competing commit");
    fixture
        .storage
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
        fixture.root.store_root_hash,
        candidate.author_registration.clone(),
        winner_ref,
        batch.head.value.history_summary,
        batch.head.value.successor.clone(),
        &signer,
    )
    .expect("sign competing head");
    let head_context = ProtocolObjectContext::signed_plaintext(
        fixture.root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_prefix = head_slot_prefix(&fixture.device_id, candidate.seq());
    let head_prepared = fixture
        .storage
        .prepare_protocol_object(
            &head_context,
            fixture.head_object.slot().clone(),
            &head_prefix,
            winner_head.to_bytes(),
        )
        .expect("prepare competing head");
    fixture
        .storage
        .create_protocol_object(&head_prepared)
        .await
        .expect("publish competing head");
    StoreDeviceHeadRef {
        head_hash: winner_head.head_hash(),
        object: head_prepared.reference().clone(),
    }
}

pub(super) async fn publish_alternate_head_for_prepared_commit(
    fixture: &PreparedWriteFixture,
) -> StoreDeviceHeadRef {
    let batch = crate::sync::store::database::StoreDatabase::new(&fixture.db)
        .oldest_prepared_store_write()
        .await
        .expect("load prepared Merge write")
        .expect("prepared Merge write exists");
    let registration = crate::sync::store::database::StoreDatabase::new(&fixture.db)
        .activated_store_device_registration(batch.head.value.author_registration.clone())
        .await
        .expect("load Merge author registration");
    let signer = registration
        .device_signer(&fixture.keypair)
        .expect("derive Merge device signer");
    let head_context = ProtocolObjectContext::signed_plaintext(
        fixture.root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let next_prefix = head_slot_prefix(&fixture.device_id, batch.commit.value.seq() + 1);
    let alternate_next = crate::storage::cloud::ObjectSlot::opaque(
        format!("{next_prefix}.json"),
        "alternate-successor".to_string(),
    )
    .expect("reserve alternate successor slot");
    let alternate = StoreDeviceHead::signed(
        fixture.root.store_root_hash,
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
    let head_prefix = head_slot_prefix(&fixture.device_id, batch.commit.value.seq());
    let prepared = fixture
        .storage
        .prepare_protocol_object(
            &head_context,
            fixture.head_object.slot().clone(),
            &head_prefix,
            alternate.to_bytes(),
        )
        .expect("prepare alternate activating head");
    fixture
        .storage
        .create_protocol_object(&prepared)
        .await
        .expect("publish alternate activating head");
    StoreDeviceHeadRef {
        head_hash: alternate.head_hash(),
        object: prepared.reference().clone(),
    }
}

pub(super) fn exact_object_exists(
    home: &InMemoryCloudHome,
    object: &crate::sync::storage::ExactObjectRef,
) -> bool {
    home.get(object.slot().logical_key()).is_some()
}

pub(super) fn commit_stream(reference: &StoreBatchCommitRef) -> String {
    reference.coord.stream_id.to_string()
}

pub(super) async fn stored_remote_object(
    db: &Database,
    object: &crate::sync::storage::ExactObjectRef,
) -> crate::sync::remote_object::RemoteObjectRecord {
    let object_id = crate::sync::remote_object::remote_object_id(object);
    db.call(move |conn| {
        let encoded: String = conn
            .query_row(
                "SELECT state FROM remote_objects WHERE object_id = ?1",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(crate::DbError::from)?;
        serde_json::from_str(&encoded).map_err(|error| crate::DbError::Message(error.to_string()))
    })
    .await
    .expect("load stored remote object")
}

pub(super) async fn remote_object_exists(
    db: &Database,
    object: &crate::sync::storage::ExactObjectRef,
) -> bool {
    let object_id = crate::sync::remote_object::remote_object_id(object);
    db.call(move |conn| {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
            [object_id.to_string()],
            |row| row.get(0),
        )
        .map_err(crate::DbError::from)
    })
    .await
    .expect("check stored remote object")
}
