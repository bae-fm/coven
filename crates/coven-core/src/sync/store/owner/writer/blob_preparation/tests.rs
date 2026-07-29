use super::*;

fn exact_partition_blob(
    physical_id: &str,
    uploaded_verified: bool,
    spool_path: Option<&str>,
) -> PreparedPartitionBlob {
    let uploader_bytes = b"outbound exact-ref test uploader";
    let uploader = crate::sync::store_commit::StoreDeviceRegistrationRef {
        device_id: "ab"
            .repeat(32)
            .parse()
            .expect("valid exact-ref test device id"),
        registration_hash: ObjectHash::digest(uploader_bytes),
        object: crate::sync::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(
                "store-v1/test/exact-ref-uploader.json".to_string(),
            )
            .expect("valid exact-ref uploader slot"),
            uploader_bytes.len() as u64,
            ObjectHash::digest(uploader_bytes),
        ),
    };
    let locator = crate::blob::locator::BlobLocator::browsable(
        "images",
        "shared-blob",
        uploader,
        "photos/shared.bin",
        12,
        ObjectHash::digest(b"shared bytes"),
    )
    .expect("valid exact-ref test locator");
    let stored_bytes = b"stored exact-ref bytes";
    let object = crate::sync::storage::ExactObjectRef::new(
        crate::storage::cloud::ObjectSlot::opaque(locator.semantic_key(), physical_id.to_string())
            .expect("valid exact-ref physical slot"),
        stored_bytes.len() as u64,
        ObjectHash::digest(stored_bytes),
    );
    PreparedPartitionBlob {
        audience: crate::blob::locator::RemoteAudience::Store,
        stored: crate::blob::locator::StoredBlobRef::new(locator, object)
            .expect("valid exact stored blob"),
        spool_path: spool_path.map(std::path::PathBuf::from),
        uploaded_verified,
    }
}

fn exact_blob_owner() -> StoreBatchCommitRef {
    let stream_id = crate::sync::membership::AuthorStreamId::from_digest(ObjectHash::digest(
        b"exact-ref owner stream",
    ));
    StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id,
            sequence: 1,
        },
        commit_hash: ObjectHash::digest(b"exact-ref owner"),
        object: crate::sync::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(
                "store-v1/test/exact-ref-owner.json".to_string(),
            )
            .expect("valid exact-ref owner slot"),
            1,
            ObjectHash::digest(b"x"),
        ),
    }
}

#[test]
fn blob_closure_deduplicates_only_identical_exact_refs_and_merges_state() {
    let owner = exact_blob_owner();
    let close = |reversed: bool| {
        let prepared = exact_partition_blob("physical-a", false, Some("/spool/shared"));
        let uploaded = exact_partition_blob("physical-a", true, None);
        let distinct = exact_partition_blob("physical-b", true, None);
        let blobs = if reversed {
            vec![distinct, uploaded, prepared]
        } else {
            vec![prepared, uploaded, distinct]
        };
        close_prepared_blobs(blobs, &owner).expect("close exact prepared blobs")
    };

    let forward = close(false);
    let reversed = close(true);
    assert_eq!(forward, reversed);
    assert_eq!(forward.0.len(), 2);
    assert_eq!(forward.1.len(), 2);
    let first_object = exact_partition_blob("physical-a", true, None)
        .stored
        .object()
        .clone();
    let first_id = crate::sync::remote_object::remote_object_id(&first_object);
    let first_remote = forward
        .0
        .iter()
        .find(|remote| remote.object_id() == first_id)
        .expect("identical exact ref remains indexed");
    assert!(matches!(
        first_remote,
        crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                record.state,
                crate::sync::remote_object::OwnedObjectState::UploadedVerified { .. }
            )
    ));
    let first_index = forward
        .1
        .iter()
        .find(|blob| blob.remote_object_id() == first_id)
        .expect("identical exact ref retains its index");
    assert_eq!(
        first_index.spool_path(),
        Some(std::path::Path::new("/spool/shared"))
    );
    let conflict = close_prepared_blobs(
        vec![
            exact_partition_blob("physical-a", false, Some("/spool/first")),
            exact_partition_blob("physical-a", false, Some("/spool/second")),
        ],
        &owner,
    )
    .expect_err("one exact prepared object cannot own two spools");
    assert!(conflict.to_string().contains("conflicting spool paths"));
}

#[tokio::test]
async fn failed_partition_preparation_cleans_up_only_its_own_exact_spool() {
    let database = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &database,
        "shared-spool-test",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("create exact test Store");
    let device = store
        .founder_device()
        .await
        .expect("bind exact blob preparation Store");
    let writer = device
        .authorize_writer()
        .await
        .expect("authorize exact blob preparation writer");
    let (uploader, registration, _) = store
        .founder_device_authority()
        .await
        .expect("load exact blob write authority");
    let authority = BlobWriteAuthority::new(&uploader, &registration)
        .expect("validate exact blob write authority");
    let protection = store
        .storage
        .store_blob_protection()
        .expect("load Store blob protection");
    let (temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let source = temp.path().join("source");
    let plaintext = b"spool owned by another pending write";
    crate::local_blob::write_atomic(&source, plaintext)
        .await
        .expect("write blob plaintext");
    let fact = StoreWriteBlobFact {
        table: String::new(),
        row_id: "photo-1".to_string(),
        row_stamp: "0000000001000-0000-A".to_string(),
        column: "id".to_string(),
        blob: crate::blob::BlobRef {
            namespace: "photos".to_string(),
            id: "photo-1".to_string(),
            scope: crate::blob::BlobScope::Master,
            cloud_path: None,
            provenance: crate::blob::Provenance::UserProvided,
            fill: crate::blob::CacheFill::CacheLazy,
        },
        plaintext_size: plaintext.len() as u64,
        plaintext_hash: ObjectHash::digest(plaintext),
        external_path: Some(source),
        previous: None,
        audience_move: None,
    };
    let audience = crate::blob::locator::RemoteAudience::Store;
    let locator = prepare_partition_blob_locator(&fact, audience.clone(), &protection, &authority)
        .expect("prepare exact blob locator");
    let spool = store_dir.outbound_blob_spool_path(locator.locator_hash());
    assert_eq!(
        store
            .storage
            .seal_blob_to_spool(
                &locator,
                &authority,
                protection.clone(),
                fact.external_path.as_ref().expect("external source"),
                &spool,
            )
            .await
            .expect("seed exact spool"),
        crate::sync::storage::BlobSpoolWrite::Created
    );
    let expected_spool = tokio::fs::read(&spool)
        .await
        .expect("read seeded exact spool");

    let error = match writer
        .prepare_partition_blob(&fact, audience, protection, &authority, &store_dir)
        .await
    {
        Ok(_) => panic!("invalid binding must fail after reusing the exact spool"),
        Err(error) => error,
    };

    assert_eq!(
        tokio::fs::read(&spool)
            .await
            .expect("shared exact spool remains"),
        expected_spool
    );
    assert!(error.to_string().contains("table"));

    assert!(crate::local_blob::remove_file(&spool)
        .await
        .expect("remove shared exact spool"));
    crate::local_blob::sync_parent_dir(&spool)
        .await
        .expect("sync removed shared exact spool");

    let error = match writer
        .prepare_partition_blob(
            &fact,
            crate::blob::locator::RemoteAudience::Store,
            store
                .storage
                .store_blob_protection()
                .expect("reload Store blob protection"),
            &authority,
            &store_dir,
        )
        .await
    {
        Ok(_) => panic!("invalid binding must fail after creating an exact spool"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("table"));
    assert!(
        !spool.exists(),
        "failed preparation removes the exact spool it created"
    );
}
