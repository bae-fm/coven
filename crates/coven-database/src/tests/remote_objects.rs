use crate::remote_object_records::merge_prepared_remote_object;

use crate::*;
use coven_protocol::blob::BLOB_TOMBSTONE_GRACE;
use coven_protocol::store_commit::commit_semantic_prefix;

use super::fixtures::*;

#[tokio::test]
async fn prepared_audience_objects_reload_the_same_verified_bytes_and_spool() {
    let store_dir = coven_foundation::store_dir::StoreDir::new_ephemeral(
        std::env::temp_dir().join(format!("coven-remote-objects-{}", uuid::Uuid::new_v4())),
    );
    let db = Database::open_with_hlc_in_store_dir_for_test(
        Path::new(":memory:"),
        store_dir.clone(),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        std::sync::Arc::new(
            coven_protocol::hlc::Hlc::try_new(
                "prepared-audience-objects".to_string(),
                std::sync::Arc::new(coven_foundation::clock::SystemClock),
            )
            .expect("create test register clock"),
        ),
        &[],
    )
    .expect("open database");
    let empty_changeset_hash = StoreDatabase::new(&db)
        .install_payload_for_test(Vec::new())
        .await
        .expect("install empty captured changeset");
    let write_id = WriteId::from_generated("write-1".to_string());
    StoreDatabase::new(&db)
        .seed_prepared_audience_write_for_test(write_id.clone(), empty_changeset_hash)
        .await
        .expect("seed write");

    let binding = exact_blob_binding("photo", "0000000001000-0000-a", b"photo bytes");
    let second_object = ExactObjectRef::new(
        coven_protocol::objects::ObjectSlot::opaque(
            binding.blob().locator().semantic_key(),
            "database-test-second-physical-object".to_string(),
        )
        .expect("second blob slot"),
        binding.blob().object().stored_size(),
        binding.blob().object().stored_hash(),
    );
    let second_blob = StoredBlobRef::new(binding.blob().locator().clone(), second_object)
        .expect("second exact object for the same locator");
    let second_binding = RowBlobLocatorBinding::new(
        "photos",
        "photo-copy",
        "0000000001000-0000-a",
        "id",
        second_blob.clone(),
    )
    .expect("second row binding");
    let package = AudiencePackage::store(
        ObjectHash::digest(b"root"),
        test_candidate_family(),
        write_id.clone(),
        test_commit_coord(),
        1,
        b"changeset".to_vec(),
        vec![binding.clone(), second_binding],
    )
    .expect("build package");
    let semantic = package.to_bytes();
    let stored_package = b"stored package representation".to_vec();
    let StoreCommitCoord { stream_id, .. } = test_commit_coord();
    let package_slot = coven_protocol::objects::ObjectSlot::logical(format!(
        "{}.pkg",
        coven_protocol::store_commit::package_semantic_prefix(
            test_candidate_family(),
            &stream_id.to_string(),
            1,
            ObjectHash::digest(&semantic),
        )
    ))
    .expect("package slot");
    let package_object = ExactObjectRef::new(
        package_slot,
        stored_package.len() as u64,
        ObjectHash::digest(&stored_package),
    );
    let owner_commit_hash = ObjectHash::digest(b"owner commit semantic bytes");
    let owner_object = ExactObjectRef::new(
        coven_protocol::objects::ObjectSlot::logical(format!(
            "{}.json",
            commit_semantic_prefix(
                test_candidate_family(),
                &stream_id.to_string(),
                1,
                owner_commit_hash,
            )
        ))
        .expect("owner commit slot"),
        1,
        ObjectHash::digest(b"owner commit"),
    );
    let owner = StoreBatchCommitRef {
        coord: test_commit_coord(),
        commit_hash: owner_commit_hash,
        object: owner_object,
    };
    let package_remote = coven_protocol::remote_object::RemoteObjectRecord::CandidateExclusive(
        coven_protocol::remote_object::CandidateObjectRecord {
            identity: coven_protocol::remote_object::CandidateExclusiveTarget {
                family: package.candidate_family(),
                domain:
                    coven_protocol::remote_object::CandidateExclusiveObjectDomain::StorePackage {
                        reference: coven_protocol::store_commit::StorePackageRef {
                            candidate_family: package.candidate_family(),
                            content_hash: ObjectHash::digest(&semantic),
                            schema_version: package.schema_version(),
                            changeset_size: semantic.len() as u64,
                            object: package_object.clone(),
                        },
                    },
                semantic_hash: ObjectHash::digest(&semantic),
                object: package_object.clone(),
            },
            payloads: coven_protocol::remote_object::RemoteObjectPayloads::SpooledInline,
            state: coven_protocol::remote_object::CandidateObjectState::Prepared {
                ownership: coven_protocol::remote_object::PendingCandidateOwnership {
                    pending: std::collections::BTreeSet::from([owner.clone()]),
                    nonactivated: Vec::new(),
                },
            },
        },
    );
    let mut activated_package = package_remote.clone();
    activated_package
        .mark_uploaded_verified()
        .expect("mark first package owner uploaded");
    let activated_package = activated_package
        .into_activated(&owner)
        .expect("activate first package owner");
    let mut sibling = owner.clone();
    sibling.commit_hash = ObjectHash::digest(b"sibling owner commit");
    sibling.object = ExactObjectRef::new(
        coven_protocol::objects::ObjectSlot::logical(format!(
            "{}.json",
            commit_semantic_prefix(
                test_candidate_family(),
                &stream_id.to_string(),
                1,
                sibling.commit_hash,
            )
        ))
        .expect("sibling commit slot"),
        1,
        ObjectHash::digest(b"sibling owner stored commit"),
    );
    let mut sibling_proposal = package_remote.clone();
    let coven_protocol::remote_object::RemoteObjectRecord::CandidateExclusive(record) =
        &mut sibling_proposal
    else {
        unreachable!("constructed candidate-exclusive package")
    };
    let coven_protocol::remote_object::CandidateObjectState::Prepared { ownership } =
        &mut record.state
    else {
        unreachable!("constructed prepared package")
    };
    ownership.pending = BTreeSet::from([sibling.clone()]);
    let sibling_owned =
        merge_prepared_remote_object(activated_package, &sibling_proposal, &sibling)
            .expect("merge sibling package ownership");
    assert!(matches!(
        sibling_owned,
        coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                &record.state,
                coven_protocol::remote_object::OwnedObjectState::UploadedVerified { ownership }
                    if ownership.pending == BTreeSet::from([sibling.clone()])
                        && ownership.activated == BTreeSet::from([
                            coven_protocol::remote_object::SharedObjectOwner::StoreCommit(
                                owner.clone()
                            )
                        ])
            )
    ));
    let package_remote_id = package_remote.object_id();
    let semantic_for_spool = semantic.clone();
    let prepared_package = PreparedAudiencePackage::new(
        package_remote_id,
        semantic,
        stored_package.clone(),
        package_object.clone(),
    )
    .expect("prepare package");

    let directory = tempfile::tempdir().expect("temp dir");
    let spool = directory.path().join("blob.spool");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(
        &spool,
        b"stored representation",
    )
    .await
    .expect("write spool");
    let blob_remote = coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(
        coven_protocol::remote_object::SharedObjectRecord {
            identity: coven_protocol::remote_object::SharedLiveSetObjectRef {
                domain: coven_protocol::remote_object::SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&binding.blob().locator().to_bytes()),
                object: binding.blob().object().clone(),
            },
            payloads: coven_protocol::remote_object::RemoteObjectPayloads::RowBlob {
                locator_bytes: binding.blob().locator().to_bytes(),
            },
            state: coven_protocol::remote_object::OwnedObjectState::Prepared {
                ownership: coven_protocol::remote_object::PendingCandidateOwnership {
                    pending: std::collections::BTreeSet::from([owner.clone()]),
                    nonactivated: Vec::new(),
                },
            },
        },
    );
    let blob_remote_id = blob_remote.object_id();
    let prepared_blob = PreparedAudienceBlob::from_remote(
        RemoteAudience::Store,
        &binding.blob().locator().locator_hash().to_string(),
        blob_remote.clone(),
        Some(spool.clone()),
    )
    .expect("prepare blob");
    let second_blob_remote = coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(
        coven_protocol::remote_object::SharedObjectRecord {
            identity: coven_protocol::remote_object::SharedLiveSetObjectRef {
                domain: coven_protocol::remote_object::SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&second_blob.locator().to_bytes()),
                object: second_blob.object().clone(),
            },
            payloads: coven_protocol::remote_object::RemoteObjectPayloads::RowBlob {
                locator_bytes: second_blob.locator().to_bytes(),
            },
            state: coven_protocol::remote_object::OwnedObjectState::Prepared {
                ownership: coven_protocol::remote_object::PendingCandidateOwnership {
                    pending: std::collections::BTreeSet::from([owner]),
                    nonactivated: Vec::new(),
                },
            },
        },
    );
    let second_blob_remote_id = second_blob_remote.object_id();
    let second_prepared_blob = PreparedAudienceBlob::from_remote(
        RemoteAudience::Store,
        &second_blob.locator().locator_hash().to_string(),
        second_blob_remote.clone(),
        Some(spool.clone()),
    )
    .expect("prepare second blob");
    // The row names its payloads; the package reload reads them back out of the
    // spool, so they have to be installed the way a real persist installs them.
    for bytes in [&semantic_for_spool, &stored_package] {
        StoreDatabase::new(&db)
            .install_payload_for_test(bytes.to_vec())
            .await
            .expect("install package payload");
    }
    StoreDatabase::new(&db)
        .persist_prepared_audience_objects_for_test(
            write_id.clone(),
            vec![package_remote, blob_remote, second_blob_remote],
            vec![prepared_package],
            vec![prepared_blob, second_prepared_blob],
        )
        .await
        .expect("persist prepared objects");

    let reloaded = crate::StoreDatabase::new(&db)
        .prepared_audience_objects(&write_id)
        .await
        .expect("reload prepared objects");
    assert_eq!(reloaded.packages.len(), 1);
    assert_eq!(reloaded.packages[0].package(), &package);
    assert_eq!(reloaded.packages[0].remote_object_id(), package_remote_id);
    assert_eq!(reloaded.blobs.len(), 2);
    let reloaded_ids = reloaded
        .blobs
        .iter()
        .map(PreparedAudienceBlob::remote_object_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        reloaded_ids,
        std::collections::BTreeSet::from([blob_remote_id, second_blob_remote_id])
    );
    assert!(reloaded
        .blobs
        .iter()
        .all(|blob| blob.spool_path() == Some(spool.as_path())));
    assert_eq!(
        reloaded
            .blobs
            .iter()
            .map(|blob| blob.blob().locator().locator_hash())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        1,
        "same-locator exact objects survive persistence independently",
    );
}
