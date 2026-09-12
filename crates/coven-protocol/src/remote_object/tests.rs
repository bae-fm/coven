use super::*;
use crate::blob::locator::{BlobLocator, RemoteAudience};
use crate::blob::BlobScope;
use crate::objects::ObjectSlot;
use crate::write::WriteId;
use crate::{audience_package, membership, store_commit};
use coven_keys::encryption::KeyFingerprint;

#[path = "pending_release_tests.rs"]
mod pending_release_tests;

fn test_commit_ref(label: &str, sequence: u64) -> StoreBatchCommitRef {
    let commit_hash = ObjectHash::digest(format!("{label} semantic commit").as_bytes());
    let stored = format!("{label} stored commit");
    let stream_id = membership::AuthorStreamId::from_digest(ObjectHash::digest(
        format!("{label} author stream").as_bytes(),
    ));
    StoreBatchCommitRef {
        coord: store_commit::StoreCommitCoord {
            stream_id,
            sequence,
        },
        commit_hash,
        object: ExactObjectRef::new(
            ObjectSlot::logical(format!("store-v1/commits/{label}.json"))
                .expect("valid test commit slot"),
            stored.len() as u64,
            ObjectHash::digest(stored.as_bytes()),
        ),
    }
}

fn test_store_package(
    owner: &StoreBatchCommitRef,
) -> (
    store_commit::StorePackageRef,
    audience_package::AudiencePackage,
) {
    let family = CandidateFamilyId::from_hash(ObjectHash::digest(b"test package family"));
    let package = audience_package::AudiencePackage::store(
        ObjectHash::digest(b"test Store root"),
        family,
        WriteId::from_generated("test-package-write".to_string()),
        owner.coord.clone(),
        1,
        b"changeset".to_vec(),
        Vec::new(),
    )
    .expect("valid test package");
    let semantic = package.to_bytes();
    let stored = b"encrypted test package";
    let reference = store_commit::StorePackageRef {
        candidate_family: family,
        content_hash: ObjectHash::digest(&semantic),
        schema_version: package.schema_version(),
        changeset_size: semantic.len() as u64,
        object: ExactObjectRef::new(
            ObjectSlot::logical("store-v1/packages/test.pkg".to_string())
                .expect("valid test package slot"),
            stored.len() as u64,
            ObjectHash::digest(stored),
        ),
    };
    (reference, package)
}

fn test_stored_blob(label: &str) -> crate::blob::locator::StoredBlobRef {
    let uploader_bytes = b"uploader registration";
    let uploader = store_commit::StoreDeviceRegistrationRef {
        device_id: "11".repeat(32).parse().expect("valid test device id"),
        registration_hash: ObjectHash::digest(b"uploader registration semantic bytes"),
        object: ExactObjectRef::new(
            ObjectSlot::logical("store-v1/registrations/uploader.json".to_string())
                .expect("valid uploader registration slot"),
            uploader_bytes.len() as u64,
            ObjectHash::digest(uploader_bytes),
        ),
    };
    let locator = BlobLocator::opaque(
        "covers",
        label,
        uploader,
        RemoteAudience::Store,
        BlobScope::Master,
        KeyFingerprint::from_bytes([4; 32]),
        7,
        ObjectHash::digest(label.as_bytes()),
    )
    .expect("valid locator");
    let stored = format!("stored {label}");
    let semantic_key = locator.semantic_key();
    crate::blob::locator::StoredBlobRef::new(
        locator,
        ExactObjectRef::new(
            ObjectSlot::opaque(semantic_key, format!("physical-{label}")).expect("valid blob slot"),
            stored.len() as u64,
            ObjectHash::digest(stored.as_bytes()),
        ),
    )
    .expect("valid stored blob")
}

fn test_membership_entry() -> (membership::MembershipEntryRef, Vec<u8>) {
    let owner = coven_keys::keys::UserKeypair::generate();
    let entry = crate::circle_test_fixtures::test_founder_entry(
        "remote-object membership",
        &owner,
        store_commit::GrantStreamAnchor::StoreMembership {
            first_slot: ObjectSlot::logical("store-v1/test/remote-object/membership/1.json".into())
                .expect("valid membership slot"),
        },
    );
    let canonical = serde_json::to_vec(&entry).expect("serialize membership entry");
    let coord = entry.coord();
    let object = ExactObjectRef::new(
        ObjectSlot::logical(format!(
            "{}.json",
            store_commit::membership_entry_semantic_prefix(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                coord.seq,
                coord.entry_hash,
            )
        ))
        .expect("valid membership entry slot"),
        canonical.len() as u64,
        ObjectHash::digest(&canonical),
    );
    (membership::MembershipEntryRef { coord, object }, canonical)
}

fn test_membership_entry_record(
    reference: membership::MembershipEntryRef,
    canonical: Vec<u8>,
    candidate: StoreBatchCommitRef,
) -> Result<RemoteObjectRecord, RemoteObjectRecordError> {
    let object = reference.object.clone();
    RemoteObjectRecord::candidate_activated_retained_authority(
        RetainedAuthorityObjectDomain::MergeMembershipEntry { reference },
        ObjectHash::digest(&canonical),
        object,
        &canonical,
        &canonical,
        candidate,
    )
    .map(ClosedRemoteObject::into_record)
}

fn activate_test_retained_authority(
    mut record: RemoteObjectRecord,
    owner: &StoreBatchCommitRef,
) -> RemoteObjectRecord {
    record
        .mark_uploaded_verified()
        .expect("mark retained authority uploaded");
    record
        .into_activated(owner)
        .expect("activate retained authority")
}

#[test]
fn pulled_retained_authority_merges_an_exact_additional_commit_owner() {
    let (reference, canonical) = test_membership_entry();
    let first = test_commit_ref("first-membership-owner", 1);
    let second = test_commit_ref("second-membership-owner", 1);
    let mut existing = activate_test_retained_authority(
        test_membership_entry_record(reference.clone(), canonical.clone(), first.clone())
            .expect("prepare first retained authority"),
        &first,
    );
    let expected = activate_test_retained_authority(
        test_membership_entry_record(reference, canonical, second.clone())
            .expect("prepare second retained authority"),
        &second,
    );

    existing
        .merge_retained_authority_activation(&expected, &second)
        .expect("merge pulled retained authority activation");

    let RemoteObjectRecord::RetainedAuthority(record) = existing else {
        panic!("merged membership entry changed domain")
    };
    let RetainedAuthorityObjectState::UploadedVerified { ownership } = record.state else {
        panic!("merged membership entry lost uploaded state")
    };
    assert_eq!(ownership.activated, BTreeSet::from([first, second]));
    assert!(ownership.pending.is_empty());
    assert!(ownership.nonactivated.is_empty());
}

/// An uploaded membership-entry authority whose one pending candidate signed
/// its own abandonment, with the nonactivation that abandonment proves.
fn test_abandoned_membership_authority_candidate() -> (
    membership::MembershipEntryRef,
    Vec<u8>,
    RemoteObjectRecord,
    CandidateNonactivation,
) {
    let owner = coven_keys::keys::UserKeypair::generate();
    let root_hash = ObjectHash::digest(b"retained membership nonactivation");
    let author = crate::circle_test_fixtures::merge_device_authority(
        &owner,
        root_hash,
        "retained-membership-nonactivation",
    );
    let (membership_state, membership_authority) =
        crate::circle_test_fixtures::merge_membership_ref(
            &owner,
            &[],
            "retained-membership-nonactivation",
        );
    let device_state = store_commit::StoreDeviceStateRef::from_resolved(
        store_commit::CommitFrontier(BTreeMap::new()),
        &store_commit::ResolvedStoreDeviceState::merge([]).expect("initial device state"),
    )
    .expect("initial device state reference");
    let coord = store_commit::StoreCommitCoord {
        stream_id: author.stream_id(),
        sequence: 1,
    };
    let order = store_commit::StoreCommitOrder {
        seq: coord.sequence,
        predecessor: None,
        dependencies: BTreeMap::new(),
    };
    let write_id = WriteId::from_generated("retained-membership-candidate".into());
    let family = CandidateFamilyId::derive(root_hash, author.reference(), &write_id, &order);
    let package = audience_package::AudiencePackage::store(
        root_hash,
        family,
        write_id.clone(),
        coord.clone(),
        1,
        b"candidate changeset".to_vec(),
        Vec::new(),
    )
    .expect("candidate package")
    .to_bytes();
    let package_object = crate::circle_test_fixtures::exact_logical_object(
        format!(
            "{}.pkg",
            store_commit::package_semantic_prefix(
                family,
                &coord.stream_id.to_string(),
                coord.sequence,
                ObjectHash::digest(&package),
            )
        ),
        &package,
    );
    let candidate = author
        .sign_operations(
            root_hash,
            write_id,
            coord.clone(),
            order.clone(),
            membership_state.clone(),
            device_state.clone(),
            membership_authority,
            store_commit::StoreCommitOperationsInput {
                store_package: Some(store_commit::StorePackageInput {
                    candidate_family: family,
                    schema_version: 1,
                    bytes: &package,
                    object: package_object,
                }),
                ..store_commit::StoreCommitOperationsInput::empty()
            },
        )
        .expect("sign the candidate");
    let target = |commit: &store_commit::StoreBatchCommit| {
        let bytes = commit.to_bytes();
        store_commit::StoreBatchCommitDeletionTarget {
            coord: coord.clone(),
            object: crate::circle_test_fixtures::exact_logical_object(
                format!(
                    "{}.json",
                    store_commit::commit_semantic_prefix(
                        commit.candidate_family(),
                        &coord.stream_id.to_string(),
                        coord.sequence,
                        commit.commit_hash(),
                    )
                ),
                &bytes,
            ),
            canonical_signed_bytes: bytes,
        }
    };
    let candidate_target = target(&candidate);
    let candidate_ref = StoreBatchCommitRef::from_commit(
        &candidate,
        coord.clone(),
        candidate_target.object.clone(),
    )
    .expect("exact candidate reference");
    let signer = author.registration().device_signer(&owner).unwrap();
    let abandonment = store_commit::StoreBatchCommit::signed_with_candidate_abandonment(
        root_hash,
        WriteId::from_generated("retained-membership-abandonment".into()),
        coord.clone(),
        author.reference().clone(),
        author.registration(),
        order,
        store_commit::StorePublicationBase::Genesis,
        membership_state,
        device_state,
        vec![store_commit::CandidateCleanupManifest {
            candidate: candidate_target,
        }],
        &signer,
    )
    .expect("sign abandonment of the exact candidate");
    abandonment
        .verify_at(root_hash, &coord, author.registration())
        .expect("verify the signed abandonment");
    let nonactivation = CandidateNonactivation::from_durable_parts(
        &candidate_ref,
        &candidate,
        CandidateNonactivationProof::AcceptedAbandonment {
            abandonment: target(&abandonment),
        },
    )
    .expect("bind nonactivation to the signed candidate and abandonment");

    let (reference, canonical) = test_membership_entry();
    let mut record =
        test_membership_entry_record(reference.clone(), canonical.clone(), candidate_ref)
            .expect("prepare membership authority");
    record.mark_uploaded_verified().expect("record upload");
    (reference, canonical, record, nonactivation)
}

#[test]
fn nonactivation_preserves_another_pending_membership_authority_owner() {
    let (reference, canonical, mut record, nonactivation) =
        test_abandoned_membership_authority_candidate();
    let remaining = test_commit_ref("remaining-membership-owner", 1);
    record
        .add_retained_authority_candidate(remaining.clone())
        .expect("retain another pending candidate");

    record
        .begin_candidate_nonactivation(nonactivation.clone())
        .expect("nonactivate only the abandoned candidate");

    record
        .validate_payload(&canonical)
        .expect("retained authority still names the exact membership entry");
    assert_eq!(record.object(), &reference.object);
    assert!(record.cleanup_target().is_none());
    let RemoteObjectRecord::RetainedAuthority(retained) = &record else {
        panic!("membership authority must remain retained");
    };
    let RetainedAuthorityObjectState::UploadedVerified { ownership } = &retained.state else {
        panic!("remaining owner must preserve uploaded authority");
    };
    assert_eq!(ownership.pending, BTreeSet::from([remaining]));
    assert!(ownership.activated.is_empty());
    assert_eq!(ownership.nonactivated, vec![nonactivation.clone()]);
    let expected = record.clone();
    record
        .begin_candidate_nonactivation(nonactivation)
        .expect("repeat exact nonactivation");
    assert_eq!(record, expected);
}

#[test]
fn nonactivating_the_last_pending_owner_of_unactivated_authority_is_refused() {
    let (_reference, _canonical, mut record, nonactivation) =
        test_abandoned_membership_authority_candidate();
    let expected = record.clone();

    let error = record
        .begin_candidate_nonactivation(nonactivation)
        .expect_err("an authority cannot give up its only owner");

    assert!(matches!(error, RemoteObjectRecordError::EmptyOwnership));
    assert_eq!(record, expected);
}

#[test]
fn external_package_keeps_exact_ciphertext_identity_and_idempotent_replay_owner() {
    let commit = test_commit_ref("external-package", 1);
    let (reference, package) = test_store_package(&commit);
    let domain = SharedLiveSetObjectDomain::StorePackage {
        reference: reference.clone(),
    };
    let mut record =
        RemoteObjectRecord::activated_external_package(domain.clone(), &package, commit.clone())
            .expect("activate external package")
            .into_record();
    let replay = RetainedReplayOwner::Commit {
        commit: commit.clone(),
        input_hash: ObjectHash::digest(b"retained input"),
    };

    record
        .merge_retained_replay_owner(replay.clone())
        .expect("pin external package");
    record
        .merge_retained_replay_owner(replay.clone())
        .expect("repeat exact pin");

    assert_eq!(record.payloads(), &RemoteObjectPayloads::SpooledExternal);
    assert_eq!(record.object(), &reference.object);
    assert_eq!(
        record.retained_replay_owners().collect::<Vec<_>>(),
        vec![&replay]
    );
    assert!(record
        .validate_reclaimable_store_package(&reference, &commit)
        .is_err());

    let mut wrong_plaintext = record.clone();
    let RemoteObjectRecord::SharedLiveSet(inner) = &mut wrong_plaintext else {
        unreachable!("constructed shared package")
    };
    inner.identity.semantic_hash = ObjectHash::digest(b"another package");
    assert!(wrong_plaintext
        .validate_payload(&package.to_bytes())
        .is_err());

    let mut wrong_reference = record;
    let RemoteObjectRecord::SharedLiveSet(inner) = &mut wrong_reference else {
        unreachable!("constructed shared package")
    };
    inner.identity.domain = domain;
    inner.identity.object = test_commit_ref("wrong-package", 2).object;
    assert!(wrong_reference
        .validate_payload(&package.to_bytes())
        .is_err());
}

#[test]
fn shared_blob_retains_each_commit_owner_independently() {
    let blob = test_stored_blob("shared-blob");
    let first = test_commit_ref("first-blob-owner", 1);
    let second = test_commit_ref("second-blob-owner", 2);
    let first_replay = RetainedReplayOwner::Commit {
        commit: first.clone(),
        input_hash: ObjectHash::digest(b"first retained input"),
    };
    let second_replay = RetainedReplayOwner::Commit {
        commit: second.clone(),
        input_hash: ObjectHash::digest(b"second retained input"),
    };
    let mut record = RemoteObjectRecord::activated_blob(&blob, first.clone())
        .expect("activate shared blob")
        .into_record();
    record
        .merge_blob_activation(&blob, &second)
        .expect("activate second blob owner");
    record
        .merge_retained_replay_owner(first_replay.clone())
        .expect("pin first retained input");
    record
        .merge_retained_replay_owner(second_replay.clone())
        .expect("pin second retained input");

    assert_eq!(
        record
            .retained_replay_owners()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([first_replay, second_replay])
    );
    assert!(record.validate().is_ok());
}

#[test]
fn stored_blob_record_rejects_object_outside_locator_semantic_slot() {
    let uploader_bytes = b"uploader registration";
    let uploader = store_commit::StoreDeviceRegistrationRef {
        device_id: "11".repeat(32).parse().expect("valid test device id"),
        registration_hash: ObjectHash::digest(b"uploader registration semantic bytes"),
        object: ExactObjectRef::new(
            ObjectSlot::logical("store-v1/registrations/uploader.json".to_string())
                .expect("valid uploader registration slot"),
            uploader_bytes.len() as u64,
            ObjectHash::digest(uploader_bytes),
        ),
    };
    let locator = BlobLocator::opaque(
        "covers",
        "cover-a",
        uploader,
        RemoteAudience::Store,
        BlobScope::Master,
        KeyFingerprint::from_bytes([4; 32]),
        7,
        ObjectHash::digest(b"cover-a"),
    )
    .expect("valid locator");
    let canonical_semantic_bytes = locator.to_bytes();
    let stored_bytes = b"stored cover".to_vec();
    let object = ExactObjectRef::new(
        ObjectSlot::logical("covers/opaque/wrong-slot".to_string()).expect("valid slot"),
        stored_bytes.len() as u64,
        ObjectHash::digest(&stored_bytes),
    );
    let stream_id = membership::AuthorStreamId::from_digest(ObjectHash::digest(
        b"remote object test author stream",
    ));
    let owner = StoreBatchCommitRef {
        coord: store_commit::StoreCommitCoord {
            stream_id,
            sequence: 1,
        },
        commit_hash: ObjectHash::digest(b"commit semantic bytes"),
        object: ExactObjectRef::new(
            ObjectSlot::logical(format!(
                "{}.json",
                store_commit::commit_semantic_prefix(
                    store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(
                        b"remote object test candidate family"
                    ),),
                    &stream_id.to_string(),
                    1,
                    ObjectHash::digest(b"commit semantic bytes"),
                )
            ))
            .expect("valid slot"),
            1,
            ObjectHash::digest(b"commit"),
        ),
    };
    let record = RemoteObjectRecord::SharedLiveSet(SharedObjectRecord {
        identity: SharedLiveSetObjectRef {
            domain: SharedLiveSetObjectDomain::StoredBlob,
            semantic_hash: ObjectHash::digest(&canonical_semantic_bytes),
            object: object.clone(),
        },
        payloads: RemoteObjectPayloads::RowBlob {
            locator_bytes: canonical_semantic_bytes,
        },
        state: OwnedObjectState::UploadedVerified {
            ownership: SharedObjectOwnership {
                pending: BTreeSet::new(),
                activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner)]),
                nonactivated: Vec::new(),
            },
        },
    });

    assert!(matches!(
        record.validate(),
        Err(RemoteObjectRecordError::BlobLocator(_))
    ));
}

#[test]
fn membership_entry_is_candidate_activated_retained_authority() {
    let (reference, canonical) = test_membership_entry();
    let candidate = test_commit_ref("membership-entry-owner", 1);

    let record = test_membership_entry_record(reference, canonical, candidate.clone())
        .expect("close membership-entry ownership");
    let encoded = serde_json::to_vec(&record).expect("serialize retained membership authority");
    let record: RemoteObjectRecord =
        serde_json::from_slice(&encoded).expect("deserialize retained membership authority");

    assert!(record.validate().is_ok());
    assert!(matches!(
        record,
        RemoteObjectRecord::RetainedAuthority(RetainedAuthorityRecord {
            identity: RetainedAuthorityObjectRef {
                domain: RetainedAuthorityObjectDomain::MergeMembershipEntry { .. },
                ..
            },
            state: RetainedAuthorityObjectState::Prepared { ownership },
            ..
        }) if ownership.pending == BTreeSet::from([candidate])
    ));
}

#[test]
fn membership_entry_authority_rejects_a_different_semantic_reference() {
    let (reference, canonical) = test_membership_entry();
    let candidate = test_commit_ref("membership-entry-mismatch", 1);
    let mut record = test_membership_entry_record(reference, canonical.clone(), candidate)
        .expect("close membership-entry ownership");
    let RemoteObjectRecord::RetainedAuthority(inner) = &mut record else {
        panic!("membership entry must use retained authority ownership")
    };
    let RetainedAuthorityObjectDomain::MergeMembershipEntry { reference } =
        &mut inner.identity.domain
    else {
        panic!("membership entry must retain its exact domain")
    };
    reference.coord.entry_hash = ObjectHash::digest(b"another membership entry");

    assert!(matches!(
        record.validate_payload(&canonical),
        Err(RemoteObjectRecordError::StoredReferenceMismatch)
    ));
}

#[test]
fn deserialized_acknowledgement_rejects_unsupported_cleanup_state() {
    let identity = coven_keys::keys::UserKeypair::generate();
    let root_hash = ObjectHash::digest(b"remote-object acknowledgement Store root");
    let author = crate::circle_test_fixtures::merge_device_authority(
        &identity,
        root_hash,
        "remote-object acknowledgement",
    );
    let device_state = store_commit::StoreDeviceStateRef::from_resolved(
        store_commit::CommitFrontier(BTreeMap::new()),
        &store_commit::ResolvedStoreDeviceState {
            devices: BTreeMap::new(),
            recovery: Vec::new(),
            state_hash: ObjectHash::digest(b"acknowledgement device state"),
        },
    )
    .expect("construct acknowledgement device state");
    let candidate = test_commit_ref("invalid-ack-cleanup-state", 1);
    let acknowledgement = store_commit::StoreAck::unsigned_for_test(store_commit::StoreAckBody {
        store_root_hash: root_hash,
        registration: author.reference().clone(),
        sequence: 1,
        store_cut: store_commit::StoreHistoryCut(BTreeMap::new()),
        device_state,
        last_sync: "2026-01-01T00:00:00Z".into(),
        successor: store_commit::SuccessorLink {
            activation: store_commit::StreamActivation::device_authorized(
                root_hash,
                author.reference().clone(),
                store_commit::DeviceStreamAnchor::StoreAcknowledgements {
                    first_slot: ObjectSlot::logical("store-v1/acks/invalid-cleanup.json".into())
                        .expect("valid first ack slot"),
                },
            )
            .activation_id(),
            predecessor: None,
            next_slot: ObjectSlot::logical("store-v1/acks/invalid-cleanup-successor.json".into())
                .expect("valid successor slot"),
        },
    });
    let bytes = acknowledgement.to_bytes();
    let object = ExactObjectRef::new(
        ObjectSlot::logical("store-v1/acks/invalid-cleanup.json".into()).expect("valid ack slot"),
        bytes.len() as u64,
        ObjectHash::digest(&bytes),
    );
    let record = RemoteObjectRecord::candidate_activated_store_acknowledgement(
        store_commit::StoreAckRef {
            registration: acknowledgement.registration.clone(),
            sequence: acknowledgement.sequence,
            ack_hash: acknowledgement.ack_hash(),
            object,
        },
        &bytes,
        &bytes,
        candidate,
    )
    .expect("prepare retained Store acknowledgement")
    .into_record();
    record.validate().expect("valid retained acknowledgement");
    let mut encoded = serde_json::to_value(&record).expect("serialize retained state");
    encoded["retained_authority"]["state"] = serde_json::json!({
        "cleanup_pending": { "former_candidates": [] }
    });
    assert!(serde_json::from_value::<RemoteObjectRecord>(encoded).is_err());
}

/// A record's payload variant says where its bytes are, and only a stored blob's
/// row travels — inside published snapshot and bootstrap images, where the
/// receiving device has the row and none of this device's spool. Every other
/// domain names its payloads in the spool. A record that claims the wrong one is
/// refused, so the carry set is structural rather than a convention each new
/// constructor has to honour.
#[test]
fn a_record_whose_payloads_contradict_its_domain_is_refused() {
    let commit = test_commit_ref("payload-placement", 1);
    let blob = test_stored_blob("payload-placement-blob");
    let carried = RemoteObjectRecord::activated_blob(&blob, commit.clone())
        .expect("activate stored blob")
        .into_record();
    let locator_bytes = carried
        .payloads()
        .carried_locator_bytes()
        .expect("a stored blob carries its locator")
        .to_vec();

    for spooled in [
        RemoteObjectPayloads::SpooledInline,
        RemoteObjectPayloads::SpooledExternal,
    ] {
        let mut relocated = carried.clone();
        let RemoteObjectRecord::SharedLiveSet(inner) = &mut relocated else {
            unreachable!("constructed stored blob")
        };
        inner.payloads = spooled;
        assert!(
            matches!(
                relocated.validate(),
                Err(RemoteObjectRecordError::PayloadPlacement)
            ),
            "a stored blob whose row travels must carry its locator, not spool it"
        );
    }

    let (reference, package) = test_store_package(&commit);
    let mut spooling = RemoteObjectRecord::activated_external_package(
        SharedLiveSetObjectDomain::StorePackage { reference },
        &package,
        commit,
    )
    .expect("activate external package")
    .into_record();
    let RemoteObjectRecord::SharedLiveSet(inner) = &mut spooling else {
        unreachable!("constructed shared package")
    };
    inner.payloads = RemoteObjectPayloads::RowBlob { locator_bytes };
    assert!(
        matches!(
            spooling.validate(),
            Err(RemoteObjectRecordError::PayloadPlacement)
        ),
        "only a stored blob may carry a locator in its row"
    );
}
