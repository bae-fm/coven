use super::*;

#[test]
fn store_sequence_exhaustion_fails_instead_of_reusing_the_last_sequence() {
    assert!(matches!(
        successor_store_sequence(u64::MAX),
        Err(StoreOutboundError::SequenceExhausted { current: u64::MAX })
    ));
}

fn exact_partition_blob(
    physical_id: &str,
    uploaded_verified: bool,
    spool_path: Option<&str>,
) -> PreparedPartitionBlob {
    let uploader_bytes = b"outbound exact-ref test uploader";
    let uploader = StoreDeviceRegistrationRef {
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
    let stream_id = super::super::super::membership::AuthorStreamId::from_digest(
        ObjectHash::digest(b"exact-ref owner stream"),
    );
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
    let first_id = super::super::super::remote_object::remote_object_id(&first_object);
    let first_remote = forward
        .0
        .iter()
        .find(|remote| remote.object_id() == first_id)
        .expect("identical exact ref remains indexed");
    assert!(matches!(
        first_remote,
        super::super::super::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                record.state,
                super::super::super::remote_object::OwnedObjectState::UploadedVerified { .. }
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

pub(super) async fn initialize_exact_store(
    db: &Database,
    storage: &CloudSyncStorage,
    store_id: &str,
    keypair: &UserKeypair,
) -> (StoreRootRef, String) {
    let root = create_exact_test_store(db, storage, store_id, keypair)
        .await
        .expect("create exact test Store");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local device id")
        .expect("exact Store has an activated local device");
    (root, device_id)
}

pub(super) async fn local_device_id(db: &Database) -> String {
    db.get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local device id")
        .expect("local device id exists")
}

pub(super) async fn remove_exact_store_root(db: &Database) {
    db.call(|connection| {
        connection
            .execute("DELETE FROM store_protocol_root_authority", [])
            .map(|_| ())
            .map_err(crate::database::DbError::from)
    })
    .await
    .expect("remove exact Store root authority");
}

pub(super) async fn reinstall_exact_store_root(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
) {
    let verified = super::super::super::store_objects::load_store_protocol_root(storage, root)
        .await
        .expect("load exact Store root authority");
    db.install_store_root_authority(root.clone(), verified.bytes)
        .await
        .expect("reinstall exact Store root authority");
}
