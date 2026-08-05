use super::nonactivation::*;
use super::*;
use crate::protocol::blob::locator::{BlobLocator, RemoteAudience};
use crate::protocol::objects::ObjectSlot;
use crate::protocol::{audience_package, membership, store_commit};
use crate::{BlobScope, KeyFingerprint, WriteId};

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

fn test_stored_blob(label: &str) -> crate::protocol::blob::locator::StoredBlobRef {
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
    crate::protocol::blob::locator::StoredBlobRef::new(
        locator,
        ExactObjectRef::new(
            ObjectSlot::opaque(semantic_key, format!("physical-{label}")).expect("valid blob slot"),
            stored.len() as u64,
            ObjectHash::digest(stored.as_bytes()),
        ),
    )
    .expect("valid stored blob")
}

fn test_membership_resolution() -> (membership::StoreMembershipConflictResolutionRef, Vec<u8>) {
    let conflict_hash = ObjectHash::digest(b"remote-object membership conflict");
    let resolver_pubkey = "22".repeat(crate::keys::SIGN_PUBLICKEYBYTES);
    let replacement_grant =
        membership::derive_store_resolution_grant(&conflict_hash, &resolver_pubkey);
    let registration_bytes = b"resolution registration";
    let registration = store_commit::StoreDeviceRegistrationRef {
        device_id: "33".repeat(32).parse().expect("valid resolution device id"),
        registration_hash: ObjectHash::digest(registration_bytes),
        object: ExactObjectRef::new(
            ObjectSlot::logical("store-v1/devices/resolution-registration.json".to_string())
                .expect("valid resolution registration slot"),
            registration_bytes.len() as u64,
            ObjectHash::digest(registration_bytes),
        ),
    };
    let membership = store_commit::GrantStreamAnchor::StoreMembership {
        first_slot: ObjectSlot::logical(
            "store-v1/membership/heads/resolver/replacement/stream/1.json".to_string(),
        )
        .expect("valid resolution membership slot"),
    };
    let recovery = store_commit::GrantStreamAnchor::OwnerRecovery {
        first_slot: ObjectSlot::logical(
            "store-v1/recovery/resolver/replacement/1.json".to_string(),
        )
        .expect("valid resolution recovery slot"),
    };
    let resolution = membership::StoreMembershipConflictResolution::unsigned_for_test(
        membership::StoreMembershipConflictResolutionBody {
            store_root_hash: ObjectHash::digest(b"remote-object resolution Store root"),
            conflict_hash,
            conflicting_heads: Vec::new(),
            retired_owner_grants: BTreeSet::new(),
            retirement_barriers: BTreeMap::new(),
            resolver_pubkey: resolver_pubkey.clone(),
            selection: membership::MembershipConflictSelection::RevocationBranch {
                heads: Vec::new(),
            },
            replacement_grant: replacement_grant.clone(),
            replacement_membership: membership.clone(),
            replacement_acceptance:
                store_commit::OwnerConflictResolutionAcceptance::unsigned_for_test(
                    store_commit::OwnerConflictResolutionAcceptanceBody {
                        store_root_hash: ObjectHash::digest(b"remote-object resolution Store root"),
                        owner_grant: replacement_grant,
                        owner_registration: registration,
                        provider: crate::protocol::objects::ProviderDeviceBinding {
                            principal:
                                crate::protocol::objects::ProviderPrincipalId::CustomS3Credential {
                                    access_key_id_hash: ObjectHash::digest(
                                        b"resolution provider credential",
                                    ),
                                },
                        },
                        membership,
                        recovery,
                        device_state: store_commit::StoreDeviceStateRef::from_resolved(
                            store_commit::CommitFrontier(BTreeMap::new()),
                            &store_commit::ResolvedStoreDeviceState {
                                devices: BTreeMap::new(),
                                recovery: Vec::new(),
                                state_hash: ObjectHash::digest(b"resolution device state"),
                            },
                        )
                        .expect("construct resolution device state"),
                    },
                ),
        },
    );
    let canonical = serde_json::to_vec(&resolution).expect("serialize membership resolution");
    let resolution_hash = resolution.resolution_hash();
    let object = ExactObjectRef::new(
        ObjectSlot::logical(format!(
            "{}.json",
            store_commit::membership_resolution_semantic_prefix(
                conflict_hash,
                &resolver_pubkey,
                resolution_hash,
            )
        ))
        .expect("valid membership resolution slot"),
        canonical.len() as u64,
        ObjectHash::digest(&canonical),
    );
    (resolution.resolution_ref(object), canonical)
}

fn test_membership_resolution_record(
    reference: membership::StoreMembershipConflictResolutionRef,
    canonical: Vec<u8>,
    candidate: StoreBatchCommitRef,
) -> Result<RemoteObjectRecord, RemoteObjectRecordError> {
    let object = reference.object.clone();
    RemoteObjectRecord::candidate_activated_retained_authority(
        RetainedAuthorityObjectDomain::StoreMembershipResolution { reference },
        ObjectHash::digest(&canonical),
        object,
        canonical.clone(),
        canonical,
        candidate,
    )
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
    let (reference, canonical) = test_membership_resolution();
    let first = test_commit_ref("first-resolution-owner", 1);
    let second = test_commit_ref("second-resolution-owner", 1);
    let mut existing = activate_test_retained_authority(
        test_membership_resolution_record(reference.clone(), canonical.clone(), first.clone())
            .expect("prepare first retained authority"),
        &first,
    );
    let expected = activate_test_retained_authority(
        test_membership_resolution_record(reference, canonical, second.clone())
            .expect("prepare second retained authority"),
        &second,
    );

    existing
        .merge_retained_authority_activation(&expected, &second)
        .expect("merge pulled retained authority activation");

    let RemoteObjectRecord::RetainedAuthority(record) = existing else {
        panic!("merged membership resolution changed domain")
    };
    let RetainedAuthorityObjectState::UploadedVerified { ownership } = record.state else {
        panic!("merged membership resolution lost uploaded state")
    };
    assert_eq!(ownership.activated, BTreeSet::from([first, second]));
    assert!(ownership.pending.is_empty());
    assert!(ownership.nonactivated.is_empty());
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
            .expect("activate external package");
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

    assert!(matches!(
        record.bytes().stored(),
        RemoteStoredRepresentation::ExternalExact { object }
            if object == &reference.object
    ));
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
    inner.bytes.canonical_semantic_bytes.push(b' ');
    assert!(wrong_plaintext.validate().is_err());

    let mut wrong_reference = record;
    let RemoteObjectRecord::SharedLiveSet(inner) = &mut wrong_reference else {
        unreachable!("constructed shared package")
    };
    inner.identity.domain = domain;
    inner.identity.object = test_commit_ref("wrong-package", 2).object;
    assert!(wrong_reference.validate().is_err());
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
    let mut record =
        RemoteObjectRecord::activated_blob(&blob, first.clone()).expect("activate shared blob");
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
        bytes: RemoteObjectBytes::inline(canonical_semantic_bytes, stored_bytes, object)
            .expect("valid stored bytes"),
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
        Err(RemoteObjectRecordError::InvalidDomain(_))
    ));
}

#[test]
fn membership_resolution_is_candidate_activated_retained_authority() {
    let (reference, canonical) = test_membership_resolution();
    let candidate = test_commit_ref("membership-resolution-owner", 1);

    let record = test_membership_resolution_record(reference, canonical, candidate.clone())
        .expect("close membership-resolution ownership");
    let encoded = serde_json::to_vec(&record).expect("serialize retained resolution authority");
    let record: RemoteObjectRecord =
        serde_json::from_slice(&encoded).expect("deserialize retained resolution authority");

    assert!(record.validate().is_ok());
    assert!(matches!(
        record,
        RemoteObjectRecord::RetainedAuthority(RetainedAuthorityRecord {
            identity: RetainedAuthorityObjectRef {
                domain: RetainedAuthorityObjectDomain::StoreMembershipResolution { .. },
                ..
            },
            state: RetainedAuthorityObjectState::Prepared { ownership },
            ..
        }) if ownership.pending == BTreeSet::from([candidate])
    ));
}

#[test]
fn membership_resolution_authority_rejects_a_different_semantic_reference() {
    let (reference, canonical) = test_membership_resolution();
    let candidate = test_commit_ref("membership-resolution-mismatch", 1);
    let mut record = test_membership_resolution_record(reference, canonical, candidate)
        .expect("close membership-resolution ownership");
    let RemoteObjectRecord::RetainedAuthority(inner) = &mut record else {
        panic!("membership resolution must use retained authority ownership")
    };
    let RetainedAuthorityObjectDomain::StoreMembershipResolution { reference } =
        &mut inner.identity.domain
    else {
        panic!("membership resolution must retain its exact domain")
    };
    reference.conflict_hash = ObjectHash::digest(b"another membership conflict");

    assert!(matches!(
        record.validate(),
        Err(RemoteObjectRecordError::StoredReferenceMismatch)
    ));
}

#[test]
fn membership_resolution_authority_rejects_a_relocated_object() {
    let (mut reference, canonical) = test_membership_resolution();
    let stored_hash = reference.object.stored_hash();
    reference.object = ExactObjectRef::new(
        ObjectSlot::logical("store-v1/membership/resolutions/relocated.json".to_string())
            .expect("valid relocated resolution slot"),
        canonical.len() as u64,
        stored_hash,
    );
    let candidate = test_commit_ref("membership-resolution-relocation", 1);

    let error = test_membership_resolution_record(reference, canonical, candidate)
        .expect_err("relocated resolution must not enter retained authority");

    assert!(matches!(
        error,
        RemoteObjectRecordError::StoredReferenceMismatch
    ));
}

#[test]
fn sole_losing_membership_resolution_becomes_exact_cleanable() {
    let (reference, _) = test_membership_resolution();
    let disposition = uploaded_retained_nonactivation_disposition(
        &RetainedAuthorityObjectDomain::StoreMembershipResolution { reference },
        CandidateOwnership {
            pending: BTreeSet::new(),
            activated: BTreeSet::new(),
            nonactivated: Vec::new(),
        },
    );

    assert!(matches!(
        disposition,
        UploadedRetainedNonactivation::Cleanup(_)
    ));
}

#[test]
fn shared_membership_resolution_retains_its_remaining_candidate_owner() {
    let (reference, _) = test_membership_resolution();
    let remaining = test_commit_ref("shared-resolution-owner", 2);
    let disposition = uploaded_retained_nonactivation_disposition(
        &RetainedAuthorityObjectDomain::StoreMembershipResolution { reference },
        CandidateOwnership {
            pending: BTreeSet::from([remaining.clone()]),
            activated: BTreeSet::new(),
            nonactivated: Vec::new(),
        },
    );

    assert!(matches!(
        disposition,
        UploadedRetainedNonactivation::Retain(CandidateOwnership { pending, .. })
            if pending == BTreeSet::from([remaining])
    ));
}

#[test]
fn deserialized_device_head_rejects_resolution_cleanup_state() {
    let (_, resolution_bytes) = test_membership_resolution();
    let resolution: membership::StoreMembershipConflictResolution =
        serde_json::from_slice(&resolution_bytes).expect("parse resolution fixture");
    let candidate = test_commit_ref("invalid-head-cleanup-state", 1);
    let head =
        store_commit::StoreDeviceHead::unsigned_for_test(store_commit::StoreDeviceHeadBody {
            store_root_hash: resolution.store_root_hash,
            author_registration: resolution.replacement_acceptance.owner_registration.clone(),
            commit: candidate.clone(),
            history_summary: store_commit::ObjectHash::digest(&resolution_bytes),
            successor: store_commit::SuccessorLink {
                activation: store_commit::StreamActivation::grant_authorized(
                    resolution.store_root_hash,
                    resolution.replacement_acceptance.owner_registration.clone(),
                    resolution.replacement_grant.clone(),
                    resolution.replacement_membership.clone(),
                )
                .activation_id(),
                predecessor: None,
                next_slot: ObjectSlot::logical(
                    "store-v1/heads/invalid-cleanup-successor.json".to_string(),
                )
                .expect("valid successor slot"),
            },
        });
    let bytes = head.to_bytes();
    let object = ExactObjectRef::new(
        ObjectSlot::logical("store-v1/heads/invalid-cleanup.json".to_string())
            .expect("valid head slot"),
        bytes.len() as u64,
        ObjectHash::digest(&bytes),
    );
    let mut record = RemoteObjectRecord::candidate_activated_store_head(
        store_commit::StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object,
        },
        bytes.clone(),
        bytes,
        candidate,
    )
    .expect("prepare retained Store head");
    let RemoteObjectRecord::RetainedAuthority(retained) = &mut record else {
        panic!("Store head must use retained authority")
    };
    retained.state = RetainedAuthorityObjectState::CleanupPending {
        former_candidates: Vec::new(),
    };
    let encoded = serde_json::to_vec(&record).expect("serialize invalid retained state");
    let decoded: RemoteObjectRecord =
        serde_json::from_slice(&encoded).expect("deserialize invalid retained state");

    assert!(matches!(
        decoded.validate(),
        Err(RemoteObjectRecordError::DomainMismatch)
    ));
}
