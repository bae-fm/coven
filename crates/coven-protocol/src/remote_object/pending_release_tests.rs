use super::*;

fn candidate_commit(candidate: &StoreBatchCommitRef) -> RemoteObjectRecord {
    RemoteObjectRecord::CandidateCommit(CandidateCommitRecord {
        identity: candidate.clone(),
        semantic_hash: ObjectHash::digest(b"candidate canonical bytes"),
        payloads: RemoteObjectPayloads::SpooledInline,
        state: CandidateCommitState::Prepared,
    })
}

fn candidate_package(candidate: &StoreBatchCommitRef) -> RemoteObjectRecord {
    let (reference, package) = test_store_package(candidate);
    let record = RemoteObjectRecord::CandidateExclusive(CandidateObjectRecord {
        identity: CandidateExclusiveTarget {
            family: reference.candidate_family,
            semantic_hash: reference.content_hash,
            object: reference.object.clone(),
            domain: CandidateExclusiveObjectDomain::StorePackage { reference },
        },
        payloads: RemoteObjectPayloads::SpooledInline,
        state: CandidateObjectState::Prepared {
            ownership: PendingCandidateOwnership {
                pending: BTreeSet::from([candidate.clone()]),
                nonactivated: Vec::new(),
            },
        },
    });
    record
        .validate_payload(&package.to_bytes())
        .expect("valid candidate package");
    record
}

#[test]
fn releasing_sole_candidate_returns_exact_commit_package_and_blob_targets() {
    let candidate = test_commit_ref("covered-row-write", 1);
    let blob = test_stored_blob("covered-row-write");
    for uploaded in [false, true] {
        for mut record in [candidate_commit(&candidate), candidate_package(&candidate)] {
            if uploaded {
                record.mark_uploaded_verified().expect("record upload");
            }
            let expected = record.object().clone();
            assert_eq!(
                record
                    .release_pending_candidate(&candidate)
                    .expect("release candidate"),
                PendingCandidateRelease::DeleteProtocol(expected)
            );
        }
        let record = RemoteObjectRecord::candidate_owned_blob(&blob, candidate.clone(), uploaded)
            .expect("prepare blob")
            .into_record();
        assert_eq!(
            record
                .release_pending_candidate(&candidate)
                .expect("release blob"),
            PendingCandidateRelease::DeleteBlob(blob.clone())
        );
    }
}

#[test]
fn releasing_candidate_preserves_every_other_shared_owner() {
    let candidate = test_commit_ref("covered-owner", 1);
    let pending = test_commit_ref("pending-owner", 1);
    let activated = test_commit_ref("activated-owner", 2);
    let blob = test_stored_blob("multiply-owned");
    let mut original = RemoteObjectRecord::candidate_owned_blob(&blob, candidate.clone(), true)
        .expect("prepare uploaded blob")
        .into_record();
    let RemoteObjectRecord::SharedLiveSet(record) = &mut original else {
        unreachable!()
    };
    let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
        unreachable!()
    };
    ownership.pending.insert(pending.clone());
    ownership.activated = BTreeSet::from([
        SharedObjectOwner::StoreCommit(activated),
        SharedObjectOwner::Snapshot(SnapshotObjectOwner::Store {
            metadata_slot: ObjectSlot::logical("store-v1/snapshots/live.json".to_owned())
                .expect("snapshot slot"),
        }),
        SharedObjectOwner::Snapshot(SnapshotObjectOwner::Circle {
            activation: StreamActivationId::from_digest(ObjectHash::digest(b"Circle activation")),
            generation: 2,
        }),
    ]);
    original.validate().expect("valid shared ownership");
    let mut expected = original.clone();
    let RemoteObjectRecord::SharedLiveSet(record) = &mut expected else {
        unreachable!()
    };
    let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
        unreachable!()
    };
    ownership.pending = BTreeSet::from([pending]);
    assert_eq!(
        original
            .clone()
            .release_pending_candidate(&candidate)
            .expect("release one owner"),
        PendingCandidateRelease::Retained(expected)
    );
    let RemoteObjectRecord::SharedLiveSet(record) = original else {
        unreachable!()
    };
    let OwnedObjectState::UploadedVerified { ownership } = record.state else {
        unreachable!()
    };
    assert!(
        ownership.pending.contains(&candidate),
        "calculating cleanup must leave original claims intact"
    );
    assert!(ownership.nonactivated.is_empty());
}

#[test]
fn releasing_candidate_preserves_another_pending_package_owner() {
    let candidate = test_commit_ref("covered-package-owner", 1);
    let remaining = test_commit_ref("remaining-package-owner", 1);
    for uploaded in [false, true] {
        let mut original = candidate_package(&candidate);
        let RemoteObjectRecord::CandidateExclusive(record) = &mut original else {
            unreachable!()
        };
        let CandidateObjectState::Prepared { ownership } = &mut record.state else {
            unreachable!()
        };
        ownership.pending.insert(remaining.clone());
        if uploaded {
            original
                .mark_uploaded_verified()
                .expect("record package upload");
        }
        let PendingCandidateRelease::Retained(released) = original
            .release_pending_candidate(&candidate)
            .expect("release shared candidate")
        else {
            panic!("another pending owner protects the package");
        };
        released.validate().expect("valid retained package");
        let RemoteObjectRecord::CandidateExclusive(record) = released else {
            unreachable!()
        };
        let (CandidateObjectState::Prepared { ownership }
        | CandidateObjectState::UploadedVerified { ownership }) = record.state
        else {
            unreachable!()
        };
        assert_eq!(ownership.pending, BTreeSet::from([remaining.clone()]));
        assert!(ownership.nonactivated.is_empty());
    }
}

#[test]
fn releasing_already_activated_candidate_keeps_its_exact_activation() {
    let candidate = test_commit_ref("already-accepted-row-write", 1);
    let blob = test_stored_blob("accepted-blob");
    let mut commit = candidate_commit(&candidate);
    commit
        .mark_uploaded_verified()
        .expect("record commit upload");
    let mut package = candidate_package(&candidate);
    package
        .mark_uploaded_verified()
        .expect("record package upload");
    for record in [
        commit.into_activated(&candidate).expect("activate commit"),
        package
            .into_activated(&candidate)
            .expect("activate package"),
        RemoteObjectRecord::activated_blob(&blob, candidate.clone())
            .expect("activate blob")
            .into_record(),
    ] {
        assert_eq!(
            record
                .clone()
                .release_pending_candidate(&candidate)
                .expect("preserve activation"),
            PendingCandidateRelease::Retained(record)
        );
    }
}

#[test]
fn candidate_release_rejects_other_candidates_and_membership_authority() {
    let candidate = test_commit_ref("covered-release", 1);
    let wrong = test_commit_ref("unrelated-release", 1);
    let blob = test_stored_blob("unrelated-blob");
    for record in [
        candidate_commit(&candidate),
        candidate_package(&candidate),
        RemoteObjectRecord::candidate_owned_blob(&blob, candidate.clone(), true)
            .expect("prepare blob")
            .into_record(),
    ] {
        assert!(matches!(
            record.release_pending_candidate(&wrong),
            Err(RemoteObjectRecordError::CandidateOwnerMismatch)
        ));
    }
    let (reference, canonical) = test_membership_entry();
    let authority = test_membership_entry_record(reference, canonical, candidate.clone())
        .expect("prepare membership authority");
    assert!(matches!(
        authority.release_pending_candidate(&candidate),
        Err(RemoteObjectRecordError::DomainMismatch)
    ));
}

#[test]
fn candidate_release_rejects_snapshot_artifacts_and_invalid_ownership() {
    let candidate = test_commit_ref("invalid-release", 1);
    let blob = test_stored_blob("invalid-release-blob");
    let mut invalid = RemoteObjectRecord::candidate_owned_blob(&blob, candidate.clone(), true)
        .expect("prepare blob")
        .into_record();
    let RemoteObjectRecord::SharedLiveSet(record) = &mut invalid else {
        unreachable!()
    };
    let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
        unreachable!()
    };
    ownership.pending.clear();
    assert!(matches!(
        invalid.release_pending_candidate(&candidate),
        Err(RemoteObjectRecordError::EmptyOwnership)
    ));

    let image = store_commit::SnapshotImageRef {
        image_hash: ObjectHash::digest(b"snapshot plaintext"),
        object: ExactObjectRef::new(
            ObjectSlot::logical("store-v1/snapshots/image.db".to_owned()).expect("image slot"),
            16,
            ObjectHash::digest(b"snapshot encoded"),
        ),
    };
    let record = RemoteObjectRecord::snapshot_activated_image(
        &image,
        SnapshotObjectOwner::Store {
            metadata_slot: ObjectSlot::logical("store-v1/snapshots/image.json".to_owned())
                .expect("metadata slot"),
        },
    )
    .expect("prepare snapshot ownership")
    .into_record();
    assert!(matches!(
        record.release_pending_candidate(&candidate),
        Err(RemoteObjectRecordError::DomainMismatch)
    ));
}
