use crate::database::remote_object_records::merge_prepared_remote_object;

use super::super::*;
use crate::blob::BLOB_TOMBSTONE_GRACE;

use super::fixtures::*;

#[tokio::test]
async fn prepared_audience_objects_reload_the_same_verified_bytes_and_spool() {
    let (db, _stamper) = Database::open(
        Path::new(":memory:"),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "prepared-audience-objects".to_string(),
        &[],
    )
    .expect("open database");
    let write_id = WriteId::from_generated("write-1".to_string());
    let stored_write_id = write_id.clone();
    db.call(move |conn| {
        let base = serde_json::to_string(&StoreWriteBase {
            dependencies: BTreeMap::new(),
        })
        .expect("serialize base");
        conn.execute(
            "INSERT INTO store_writes
             (write_id, status, affected_rows, changeset, inverse_changeset, base, blob_facts)
             VALUES (?1, '\"pending\"', '[]', X'', X'', ?2, '{\"blobs\":[]}')",
            rusqlite::params![stored_write_id.as_str(), base],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .expect("seed write");

    let binding = exact_blob_binding("photo", "0000000001000-0000-a", b"photo bytes");
    let second_object = ExactObjectRef::new(
        crate::storage::cloud::ObjectSlot::opaque(
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
    let package_slot = crate::storage::cloud::ObjectSlot::logical(format!(
        "{}.pkg",
        crate::sync::store_commit::package_semantic_prefix(
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
        crate::storage::cloud::ObjectSlot::logical(format!(
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
    let package_remote = crate::sync::remote_object::RemoteObjectRecord::CandidateExclusive(
        crate::sync::remote_object::CandidateObjectRecord {
            identity: crate::sync::remote_object::CandidateExclusiveTarget {
                family: package.candidate_family(),
                domain: crate::sync::remote_object::CandidateExclusiveObjectDomain::StorePackage {
                    reference: crate::sync::store_commit::StorePackageRef {
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
            bytes: crate::sync::remote_object::RemoteObjectBytes::inline(
                semantic.clone(),
                stored_package.clone(),
                package_object.clone(),
            )
            .expect("package remote bytes"),
            state: crate::sync::remote_object::CandidateObjectState::Prepared {
                ownership: crate::sync::remote_object::PendingCandidateOwnership {
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
        crate::storage::cloud::ObjectSlot::logical(format!(
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
    let crate::sync::remote_object::RemoteObjectRecord::CandidateExclusive(record) =
        &mut sibling_proposal
    else {
        unreachable!("constructed candidate-exclusive package")
    };
    let crate::sync::remote_object::CandidateObjectState::Prepared { ownership } =
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
        crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                &record.state,
                crate::sync::remote_object::OwnedObjectState::UploadedVerified { ownership }
                    if ownership.pending == BTreeSet::from([sibling.clone()])
                        && ownership.activated == BTreeSet::from([
                            crate::sync::remote_object::SharedObjectOwner::StoreCommit(
                                owner.clone()
                            )
                        ])
            )
    ));
    let package_remote_id = package_remote.object_id();
    let prepared_package = PreparedAudiencePackage::new(
        package_remote_id,
        semantic,
        stored_package.clone(),
        package_object.clone(),
    )
    .expect("prepare package");

    let directory = tempfile::tempdir().expect("temp dir");
    let spool = directory.path().join("blob.spool");
    crate::local_blob::write_atomic_durable(&spool, b"stored representation")
        .await
        .expect("write spool");
    let blob_remote = crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(
        crate::sync::remote_object::SharedObjectRecord {
            identity: crate::sync::remote_object::SharedLiveSetObjectRef {
                domain: crate::sync::remote_object::SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&binding.blob().locator().to_bytes()),
                object: binding.blob().object().clone(),
            },
            bytes: crate::sync::remote_object::RemoteObjectBytes::blob(
                binding.blob().locator().to_bytes(),
                binding.blob().object().clone(),
            )
            .expect("blob remote bytes"),
            state: crate::sync::remote_object::OwnedObjectState::Prepared {
                ownership: crate::sync::remote_object::PendingCandidateOwnership {
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
    let second_blob_remote = crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(
        crate::sync::remote_object::SharedObjectRecord {
            identity: crate::sync::remote_object::SharedLiveSetObjectRef {
                domain: crate::sync::remote_object::SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&second_blob.locator().to_bytes()),
                object: second_blob.object().clone(),
            },
            bytes: crate::sync::remote_object::RemoteObjectBytes::blob(
                second_blob.locator().to_bytes(),
                second_blob.object().clone(),
            )
            .expect("second blob remote bytes"),
            state: crate::sync::remote_object::OwnedObjectState::Prepared {
                ownership: crate::sync::remote_object::PendingCandidateOwnership {
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
    let persisted_write_id = write_id.clone();
    let package_state = serde_json::to_string(&package_remote).expect("package remote state");
    let blob_state = serde_json::to_string(&blob_remote).expect("blob remote state");
    let second_blob_state =
        serde_json::to_string(&second_blob_remote).expect("second blob remote state");
    db.call(move |conn| {
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
            rusqlite::params![package_remote_id.to_string(), package_state],
        )
        .map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
            rusqlite::params![blob_remote_id.to_string(), blob_state],
        )
        .map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
            rusqlite::params![second_blob_remote_id.to_string(), second_blob_state],
        )
        .map_err(DbError::from)?;
        Database::persist_prepared_audience_objects_on(
            &tx,
            &persisted_write_id,
            &[prepared_package],
            &[prepared_blob, second_prepared_blob],
        )?;
        tx.commit().map_err(DbError::from)
    })
    .await
    .expect("persist prepared objects");

    let reloaded = db
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
