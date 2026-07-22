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
