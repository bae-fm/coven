use super::batch_commit::{candidate_manifest, validate_stream_activations};
use super::operation_refs::validate_device_join_attempt_decision_refs;
use super::*;
use crate::sync::membership::founder_entry;

fn routing_hash() -> ObjectHash {
    ObjectHash::digest(b"test-sync-schema")
}

struct Fixture {
    signer: UserKeypair,
    root: StoreProtocolRoot,
    root_ref: StoreRootRef,
    registration: StoreDeviceRegistration,
    registration_ref: StoreDeviceRegistrationRef,
    commit: StoreBatchCommit,
    commit_ref: StoreBatchCommitRef,
    package: Vec<u8>,
}

fn slot(key: String) -> ObjectSlot {
    ObjectSlot::logical(key).expect("valid test object slot")
}

fn exact(key: String, bytes: &[u8]) -> ExactObjectRef {
    ExactObjectRef::new(slot(key), bytes.len() as u64, ObjectHash::digest(bytes))
}

fn test_circle_control_coord(fixture: &Fixture, control_hash: ObjectHash) -> CircleControlCoord {
    CircleControlCoord {
        device_id: fixture.registration.device_id.to_string(),
        stream_id: fixture.commit_ref.coord.stream_id,
        author_pubkey: keys::public_key_hex(&fixture.signer),
        author_owner_grant: fixture.root.descriptor.founder_grant.clone(),
        seq: 1,
        control_hash,
    }
}

fn circle_activation(
    fixture: &Fixture,
    circle_id: CircleId,
    grant_id: MembershipGrantId,
    anchor: fn(CircleId, ObjectSlot) -> GrantStreamAnchor,
    first_slot: ObjectSlot,
) -> StreamActivation {
    StreamActivation::grant_authorized(
        fixture.root_ref.store_root_hash,
        fixture.registration_ref.clone(),
        grant_id,
        anchor(circle_id, first_slot),
    )
}

fn joined_registration(
    fixture: &Fixture,
    identity: &UserKeypair,
    label: &str,
) -> (StoreDeviceRegistration, StoreDeviceRegistrationRef) {
    let registration = StoreDeviceRegistration::signed(
        fixture.root_ref.clone(),
        StoreDeviceRegistrationOrigin::Join {
            attempt_id: DeviceJoinAttemptId::from_hash(ObjectHash::digest(label.as_bytes())),
            attempt_slot: slot(format!("store-v1/tests/{label}/join-attempt.json")),
            outcome_slot: slot(format!("store-v1/tests/{label}/join-outcome.json")),
        },
        crate::sync::storage::ProviderDeviceBinding {
            principal: crate::sync::storage::ProviderPrincipalId::CustomS3Credential {
                access_key_id_hash: ObjectHash::digest(label.as_bytes()),
            },
        },
        DeviceStreamAnchor::StoreAnnouncements {
            first_slot: slot(format!("store-v1/tests/{label}/announcements/1.json")),
        },
        DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: slot(format!("store-v1/tests/{label}/acks/1.json")),
        },
        DeviceStreamAnchor::StoreSnapshots {
            first_slot: slot(format!("store-v1/tests/{label}/snapshots/1.json")),
        },
        identity,
    )
    .expect("sign joined registration");
    let bytes = registration.to_bytes();
    let reference = StoreDeviceRegistrationRef::from_registration(
        &registration,
        exact(
            format!(
                "{}.json",
                registration_semantic_prefix(&registration.device_id.to_string())
            ),
            &bytes,
        ),
    );
    (registration, reference)
}

#[test]
fn owner_promotion_request_and_acceptance_bind_both_exact_devices() {
    let fixture = fixture();
    let candidate_identity = UserKeypair::generate();
    let candidate_pubkey = keys::public_key_hex(&candidate_identity);
    let (candidate, candidate_ref) =
        joined_registration(&fixture, &candidate_identity, "promotion-candidate");
    let request = OwnerPromotionRequest::signed(
        OwnerPromotionId::from_generated("promotion-1".to_string()),
        &fixture.root_ref,
        fixture.registration_ref.clone(),
        &fixture.registration,
        fixture.root.descriptor.founder_grant.clone(),
        candidate_pubkey,
        MembershipGrantId(ObjectHash::digest(b"candidate Member grant")),
        candidate_ref,
        fixture.commit.membership_state.clone(),
        fixture.commit.device_state.clone(),
        OwnerPromotionFinalization {
            author_stream: fixture.commit_ref.coord.stream_id,
            seq: 2,
            previous_hash: Some(fixture.commit.commit_hash()),
        },
        &fixture.signer,
    )
    .expect("sign promotion request");
    request
        .verify(&fixture.root_ref, &fixture.registration)
        .expect("verify promotion request");
    let acceptance = OwnerPromotionAcceptance::signed(
        request.clone(),
        OwnerPromotionRequestActivation {
            commit: fixture.commit_ref.clone(),
            head: StoreDeviceHeadRef {
                head_hash: ObjectHash::digest(b"promotion activation head"),
                object: exact(
                    "store-v1/tests/promotion-activation-head.json".to_string(),
                    b"promotion activation head",
                ),
            },
        },
        OwnerPromotionAnchors {
            membership: GrantStreamAnchor::StoreMembership {
                first_slot: slot(format!(
                    "{}.json",
                    membership_head_slot_prefix(
                        &request.member_pubkey,
                        &request.intended_owner_grant,
                        StreamActivation::grant_authorized_stream_id(
                            request.store_root_hash,
                            &request.member_registration,
                            &request.intended_owner_grant,
                            StreamAnchorDomain::StoreMembership,
                        ),
                        1,
                    )
                )),
            },
            recovery: GrantStreamAnchor::OwnerRecovery {
                first_slot: slot(format!(
                    "{}.json",
                    owner_recovery_semantic_prefix(
                        &request.member_pubkey,
                        request.intended_owner_grant.clone(),
                        1,
                    )
                )),
            },
        },
        &candidate,
        &candidate_identity,
    )
    .expect("sign promotion acceptance");
    acceptance
        .verify(&candidate)
        .expect("verify promotion acceptance");
    assert!(OwnerPromotionAcceptance::signed(
        request.clone(),
        OwnerPromotionRequestActivation {
            commit: fixture.commit_ref.clone(),
            head: StoreDeviceHeadRef {
                head_hash: ObjectHash::digest(b"promotion activation head"),
                object: exact(
                    "store-v1/tests/promotion-activation-head.json".to_string(),
                    b"promotion activation head",
                ),
            },
        },
        OwnerPromotionAnchors {
            membership: GrantStreamAnchor::StoreMembership {
                first_slot: slot(format!(
                    "{}.json",
                    membership_head_slot_prefix(
                        &request.member_pubkey,
                        &request.intended_owner_grant,
                        StreamActivation::grant_authorized_stream_id(
                            request.store_root_hash,
                            &request.member_registration,
                            &request.intended_owner_grant,
                            StreamAnchorDomain::StoreMembership,
                        ),
                        1,
                    )
                )),
            },
            recovery: GrantStreamAnchor::OwnerRecovery {
                first_slot: slot(
                    "store-v1/recovery/another-owner/another-grant/1.json".to_string(),
                ),
            },
        },
        &candidate,
        &candidate_identity,
    )
    .is_err());

    let mut substituted = request;
    substituted.member_grant = MembershipGrantId(ObjectHash::digest(b"other Member grant"));
    assert!(matches!(
        substituted.verify(&fixture.root_ref, &fixture.registration),
        Err(StoreProtocolError::InvalidSignature)
    ));
}

#[test]
fn stream_activation_descriptor_and_locator_derivations_are_identical() {
    let fixture = fixture();
    let circle_id = CircleId::from_bytes([4; 16]);
    let other_circle = CircleId::from_bytes([5; 16]);
    let grant = MembershipGrantId(ObjectHash::digest(b"Circle activation grant"));
    let other_grant = MembershipGrantId(ObjectHash::digest(b"other Circle activation grant"));
    let first_slot = slot("store-v1/circles/stream/first.json".to_string());
    let activation = circle_activation(
        &fixture,
        circle_id,
        grant.clone(),
        |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
            circle_id,
            first_slot,
        },
        first_slot.clone(),
    );
    let locator = StreamActivation::grant_authorized_stream_id(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        &grant,
        StreamAnchorDomain::CircleRoster { circle_id },
    );
    assert_eq!(activation.author_stream_id(), locator);
    let locator_text = locator.to_string();
    assert_eq!(locator_text.len(), 64);
    assert!(locator_text
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));

    let other_slot = circle_activation(
        &fixture,
        circle_id,
        grant.clone(),
        |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
            circle_id,
            first_slot,
        },
        slot("store-v1/circles/stream/other-first.json".to_string()),
    );
    assert_eq!(activation.author_stream_id(), other_slot.author_stream_id());
    assert_ne!(activation.activation_id(), other_slot.activation_id());

    let other_domain = circle_activation(
        &fixture,
        circle_id,
        grant.clone(),
        |circle_id, first_slot| GrantStreamAnchor::CircleMetadata {
            circle_id,
            first_slot,
        },
        first_slot.clone(),
    );
    let other_circle = circle_activation(
        &fixture,
        other_circle,
        grant,
        |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
            circle_id,
            first_slot,
        },
        first_slot.clone(),
    );
    let other_grant = circle_activation(
        &fixture,
        circle_id,
        other_grant,
        |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
            circle_id,
            first_slot,
        },
        first_slot,
    );
    assert_ne!(
        activation.author_stream_id(),
        other_domain.author_stream_id()
    );
    assert_ne!(
        activation.author_stream_id(),
        other_circle.author_stream_id()
    );
    assert_ne!(
        activation.author_stream_id(),
        other_grant.author_stream_id()
    );
}

#[test]
fn commit_stream_activation_validation_rejects_wrong_authority_order_and_identity_collisions() {
    let (fixture, other_fixture) = (fixture(), fixture());
    let circle_id = CircleId::from_bytes([6; 16]);
    let grant = MembershipGrantId(ObjectHash::digest(b"validation Circle grant"));
    let control = circle_activation(
        &fixture,
        circle_id,
        grant.clone(),
        |circle_id, first_slot| GrantStreamAnchor::CircleControl {
            circle_id,
            first_slot,
        },
        slot("store-v1/circles/validation/control.json".to_string()),
    );
    let mut wrong_root = control.clone();
    let StreamActivation::GrantAuthorized {
        store_root_hash, ..
    } = &mut wrong_root
    else {
        unreachable!()
    };
    *store_root_hash = ObjectHash::digest(b"wrong Store root");
    assert!(validate_stream_activations(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        None,
        &[wrong_root],
    )
    .is_err());

    let mut wrong_registration = control.clone();
    let StreamActivation::GrantAuthorized {
        author_registration,
        ..
    } = &mut wrong_registration
    else {
        unreachable!()
    };
    *author_registration = other_fixture.registration_ref;
    assert!(validate_stream_activations(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        None,
        &[wrong_registration],
    )
    .is_err());

    let non_circle = StreamActivation::grant_authorized(
        fixture.root_ref.store_root_hash,
        fixture.registration_ref.clone(),
        grant.clone(),
        GrantStreamAnchor::StoreMembership {
            first_slot: slot("store-v1/membership/non-circle.json".to_string()),
        },
    );
    assert!(validate_stream_activations(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        None,
        &[non_circle],
    )
    .is_err());

    let roster = circle_activation(
        &fixture,
        circle_id,
        grant.clone(),
        |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
            circle_id,
            first_slot,
        },
        slot("store-v1/circles/validation/roster.json".to_string()),
    );
    let mut unsorted = vec![control.clone(), roster.clone()];
    unsorted.sort();
    unsorted.reverse();
    assert!(validate_stream_activations(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        None,
        &unsorted,
    )
    .is_err());
    assert!(validate_stream_activations(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        None,
        &[control.clone(), control.clone()],
    )
    .is_err());

    let same_stream = circle_activation(
        &fixture,
        circle_id,
        grant.clone(),
        |circle_id, first_slot| GrantStreamAnchor::CircleControl {
            circle_id,
            first_slot,
        },
        slot("store-v1/circles/validation/control-other.json".to_string()),
    );
    let mut duplicate_stream = vec![control.clone(), same_stream];
    duplicate_stream.sort();
    assert!(validate_stream_activations(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        None,
        &duplicate_stream,
    )
    .is_err());

    let shared_slot = slot("store-v1/circles/validation/shared.json".to_string());
    let mut duplicate_slot = vec![
        circle_activation(
            &fixture,
            circle_id,
            grant.clone(),
            |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
                circle_id,
                first_slot,
            },
            shared_slot.clone(),
        ),
        circle_activation(
            &fixture,
            circle_id,
            grant,
            |circle_id, first_slot| GrantStreamAnchor::CircleMetadata {
                circle_id,
                first_slot,
            },
            shared_slot,
        ),
    ];
    duplicate_slot.sort();
    assert!(validate_stream_activations(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        None,
        &duplicate_slot,
    )
    .is_err());
}

fn fixture() -> Fixture {
    let signer = UserKeypair::generate();
    let founder_grant =
        crate::sync::test_helpers::test_membership_grant_id("store-a founder grant");
    let provider_admin = crate::sync::test_helpers::test_founder_provider_admin("store-a");
    let founder_recovery = GrantStreamAnchor::OwnerRecovery {
        first_slot: slot("store-v1/recovery/founder/1.json".to_string()),
    };
    let store_protocol_root = StoreProtocolRoot::signed(
        StoreCreationDescriptor {
            version: STORE_PROTOCOL_VERSION,
            creation_id: StoreCreationId::from_nonce("store-a"),
            provider: crate::sync::storage::StoreProviderBinding::S3 {
                endpoint: crate::sync::storage::S3EndpointBinding::Custom {
                    origin: "https://test.invalid".to_string(),
                },
                region: "test-region".to_string(),
                bucket: "store-a-bucket".to_string(),
                key_prefix: None,
            },
            schema_version: 3,
            sync_routing_hash: routing_hash(),
            founder_pubkey: keys::public_key_hex(&signer),
            founder_grant: founder_grant.clone(),
            root_slot: slot(format!("{}.json", store_protocol_root_logical_key())),
            founder_registration: slot("store-v1/device-registrations/founder.json".to_string()),
            founder_provider_admin: provider_admin.clone(),
            founder_membership: GrantStreamAnchor::StoreMembership {
                first_slot: slot("store-v1/membership/founder/1.json".to_string()),
            },
            founder_recovery: founder_recovery.clone(),
        },
        &signer,
    )
    .expect("sign Store protocol root");
    let store_root_id = store_protocol_root.descriptor.store_root_id();
    let founder = founder_entry(
        &store_root_id.to_string(),
        &signer,
        founder_grant,
        "0000000001000-0000-device-a",
        GrantStreamAnchor::StoreMembership {
            first_slot: slot("store-v1/membership/founder/1.json".to_string()),
        },
        provider_admin,
    );
    let root_bytes = store_protocol_root.to_bytes();
    let root_ref = StoreRootRef {
        store_root_id,
        store_root_hash: store_protocol_root.object_hash(),
        object: exact(
            format!("{}.json", store_protocol_root_logical_key()),
            &root_bytes,
        ),
    };
    let registration = StoreDeviceRegistration::signed(
        root_ref.clone(),
        StoreDeviceRegistrationOrigin::Founder {
            creation_id: StoreCreationId::from_nonce("store-a"),
        },
        crate::sync::storage::ProviderDeviceBinding {
            principal: crate::sync::storage::ProviderPrincipalId::CustomS3Credential {
                access_key_id_hash: ObjectHash::digest(b"test access key"),
            },
        },
        DeviceStreamAnchor::StoreAnnouncements {
            first_slot: slot("store-v1/announcements/founder/1.json".to_string()),
        },
        DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: slot("store-v1/acks/founder/1.json".to_string()),
        },
        DeviceStreamAnchor::StoreSnapshots {
            first_slot: slot("store-v1/snapshots/founder/1.json".to_string()),
        },
        &signer,
    )
    .expect("sign founder registration");
    let registration_bytes = registration.to_bytes();
    let registration_ref = StoreDeviceRegistrationRef::from_registration(
        &registration,
        exact(
            format!(
                "{}.json",
                registration_semantic_prefix(&registration.device_id.to_string())
            ),
            &registration_bytes,
        ),
    );
    let resolved_devices = ResolvedStoreDeviceState::founder(
        &root_ref,
        registration_ref.clone(),
        &store_protocol_root.descriptor.founder_pubkey,
        founder.author_owner_grant.clone(),
        &founder_recovery,
    )
    .expect("founder device state");
    let device_state =
        StoreDeviceStateRef::from_resolved(CommitFrontier(BTreeMap::new()), &resolved_devices)
            .expect("founder device state ref");
    let membership = crate::sync::membership::MembershipChain::from_entries(vec![founder.clone()])
        .expect("founder membership");
    let crate::sync::membership::MembershipStatus::Resolved(resolved_membership) =
        membership.status()
    else {
        panic!("founder membership resolves")
    };
    let membership_head = crate::sync::membership::MembershipHeadRef {
        coord: founder.coord(),
        head_hash: ObjectHash::digest(b"founder membership head"),
        object: exact(
            "store-v1/membership/founder/head.json".to_string(),
            b"founder membership head",
        ),
    };
    let membership_state = StoreMembershipStateRef::from_parts(
        vec![membership_head],
        Vec::new(),
        resolved_devices.recovery.clone(),
        resolved_membership.state_hash,
    )
    .expect("founder membership ref");
    let package = b"package".to_vec();
    let write_id = WriteId::from_generated("canonical-write".to_string());
    let order = StoreCommitOrder {
        seq: 1,
        predecessor: None,
        dependencies: BTreeMap::new(),
    };
    let stream_id = registration
        .store_announcement_activation(&registration_ref)
        .expect("derive founder Store announcement activation")
        .author_stream_id();
    let candidate_family = CandidateFamilyId::derive(
        root_ref.store_root_hash,
        &registration_ref,
        &write_id,
        &order,
    );
    let package_object = exact(
        format!(
            "{}.pkg",
            package_semantic_prefix(
                candidate_family,
                &stream_id.to_string(),
                1,
                ObjectHash::digest(&package),
            )
        ),
        &package,
    );
    let device_signer = registration.device_signer(&signer).unwrap();
    let commit = StoreBatchCommit::signed(
        root_ref.store_root_hash,
        write_id,
        StoreCommitCoord {
            stream_id,
            sequence: 1,
        },
        registration_ref.clone(),
        &registration,
        order,
        membership_state,
        device_state,
        StoreOperationMembershipAuthority {
            predecessor: crate::sync::membership::MembershipGrantCreationAuthority::Entry(
                founder.coord(),
            ),
        },
        StorePackageInput {
            candidate_family,
            schema_version: 3,
            bytes: &package,
            object: package_object,
        },
        &device_signer,
    )
    .expect("sign commit");
    let commit_bytes = commit.to_bytes();
    let commit_ref = StoreBatchCommitRef::from_commit(
        &commit,
        StoreCommitCoord {
            stream_id,
            sequence: 1,
        },
        exact(
            format!(
                "{}.json",
                commit_semantic_prefix(
                    commit.candidate_family(),
                    &stream_id.to_string(),
                    1,
                    commit.commit_hash(),
                )
            ),
            &commit_bytes,
        ),
    )
    .expect("exact commit ref");
    Fixture {
        signer,
        root: store_protocol_root,
        root_ref,
        registration,
        registration_ref,
        commit,
        commit_ref,
        package,
    }
}

#[test]
fn object_hash_is_strict_lowercase_hex() {
    let hash = ObjectHash::digest(b"fixture");
    assert_eq!(hash.to_string().parse::<ObjectHash>().unwrap(), hash);
    assert!(hash
        .to_string()
        .to_uppercase()
        .parse::<ObjectHash>()
        .is_err());
    assert!("0".repeat(63).parse::<ObjectHash>().is_err());
    assert!(format!("{}g", "0".repeat(63))
        .parse::<ObjectHash>()
        .is_err());
}

#[test]
fn canonical_commit_round_trip_and_literal_bytes() {
    let fixture = fixture();
    let bytes = fixture.commit.to_bytes();
    let parsed = StoreBatchCommit::parse_at(
        &bytes,
        fixture.root_ref.store_root_hash,
        &fixture.commit_ref.coord,
        &fixture.registration,
    )
    .expect("parse commit");
    parsed
        .verify_store_package(&fixture.package)
        .expect("verify package");
    assert_eq!(parsed, fixture.commit);
    assert!(fixture
        .commit
        .canonical_signed_bytes()
        .starts_with(COMMIT_DOMAIN));
}

#[test]
fn commit_rejects_package_signature_and_coordinate_tamper() {
    let fixture = fixture();
    let mut tampered = fixture.commit.clone();
    tampered.signature.push('0');
    assert!(matches!(
        tampered.verify_at(
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        ),
        Err(StoreProtocolError::InvalidSignature)
    ));

    let mut tampered = fixture.commit.clone();
    let StoreCommitBody::Operations(operations) = &mut tampered.body else {
        panic!("fixture commit carries operations")
    };
    operations
        .store_package
        .as_mut()
        .expect("fixture has Store package")
        .content_hash = ObjectHash::digest(b"different");
    assert!(matches!(
        tampered.verify_at(
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        ),
        Err(StoreProtocolError::RelocatedPackage { .. })
    ));

    assert!(matches!(
        fixture.commit.verify_at(
            fixture.root_ref.store_root_hash,
            &StoreCommitCoord {
                stream_id: fixture.commit_ref.coord.stream_id,
                sequence: 2,
            },
            &fixture.registration,
        ),
        Err(StoreProtocolError::RelocatedSlot { .. })
    ));
    assert!(matches!(
        fixture.commit.verify_store_package(b"different"),
        Err(StoreProtocolError::PackageLengthMismatch { .. })
            | Err(StoreProtocolError::PackageHashMismatch { .. })
    ));
    fixture
        .commit
        .verify_store_package(&fixture.package)
        .unwrap();
}

#[test]
fn unknown_fields_and_versions_are_rejected() {
    let fixture = fixture();
    let mut value = serde_json::to_value(&fixture.commit).unwrap();
    value["unknown"] = serde_json::json!(true);
    assert!(StoreBatchCommit::parse_at(
        &serde_json::to_vec(&value).unwrap(),
        fixture.root_ref.store_root_hash,
        &fixture.commit_ref.coord,
        &fixture.registration,
    )
    .is_err());

    let mut value = serde_json::to_value(&fixture.commit).unwrap();
    value["version"] = serde_json::json!(2);
    assert!(matches!(
        StoreBatchCommit::parse_at(
            &serde_json::to_vec(&value).unwrap(),
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        ),
        Err(StoreProtocolError::UnsupportedVersion(2))
    ));
}

#[test]
fn readiness_rejects_a_bootstrap_cut_other_than_the_signed_attempt_cut() {
    let fixture = fixture();
    let joiner = UserKeypair::generate();
    let attempt_id = DeviceJoinAttemptId::from_hash(ObjectHash::digest(b"join attempt"));
    let attempt_slot = slot("store-v1/device-join-attempts/test.json".to_string());
    let outcome_slot = slot("store-v1/device-join-outcomes/test.json".to_string());
    let registration_slot = slot("store-v1/device-registrations/joiner.json".to_string());
    let provider_admin = crate::sync::provider::ProviderAdminState::founder_from_root(
        fixture.root_ref.clone(),
        fixture.registration_ref.clone(),
        &fixture.root.descriptor.founder_provider_admin,
    )
    .records()
    .values()
    .next()
    .expect("founder provider administrator exists")
    .clone();
    let registration = StoreDeviceRegistration::signed(
        fixture.root_ref.clone(),
        StoreDeviceRegistrationOrigin::Join {
            attempt_id,
            attempt_slot: attempt_slot.clone(),
            outcome_slot: outcome_slot.clone(),
        },
        provider_admin.provider.clone(),
        DeviceStreamAnchor::StoreAnnouncements {
            first_slot: slot("store-v1/heads/joiner/1.json".to_string()),
        },
        DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: slot("store-v1/acks/joiner/1.json".to_string()),
        },
        DeviceStreamAnchor::StoreSnapshots {
            first_slot: slot("store-v1/snapshots/joiner/1.json".to_string()),
        },
        &joiner,
    )
    .unwrap();
    let registration_ref = StoreDeviceRegistrationRef::from_registration(
        &registration,
        ExactObjectRef::new(
            registration_slot.clone(),
            registration.to_bytes().len() as u64,
            ObjectHash::digest(&registration.to_bytes()),
        ),
    );
    let membership = StoreMembershipStateRef::from_parts(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        ObjectHash::digest(b"membership"),
    )
    .unwrap();
    let attempt_cut = StoreHistoryCut(BTreeMap::new());
    let owner_device_signer = fixture.registration.device_signer(&fixture.signer).unwrap();
    let offer = crate::sync::device_join::DeviceJoinOffer::signed(
        attempt_id,
        keys::public_key_hex(&joiner),
        fixture.root_ref.clone(),
        fixture.root.descriptor.provider.clone(),
        attempt_slot.clone(),
        outcome_slot.clone(),
        fixture.registration_ref.clone(),
        fixture.root.descriptor.founder_grant.clone(),
        provider_admin.clone(),
        &fixture.registration,
        &owner_device_signer,
    )
    .unwrap();
    let access_request = crate::sync::device_join::DeviceProviderAccessRequest::signed(
        offer,
        provider_admin.provider.clone(),
        &joiner,
    )
    .unwrap();
    let access_grant_id = crate::sync::provider::ProviderAccessGrantId::from_random_bytes(
        *ObjectHash::digest(b"join provider access grant").as_bytes(),
    );
    let access_grant = crate::sync::provider::StoreMemberProviderAccessGrant::signed(
        access_grant_id,
        keys::public_key_hex(&joiner),
        provider_admin.provider.clone(),
        provider_admin.access.clone(),
        provider_admin.grant_id.clone(),
        fixture.registration_ref.clone(),
        &fixture.root.descriptor.provider,
        &fixture.registration,
        &owner_device_signer,
    )
    .unwrap();
    let access_grant_ref = crate::sync::provider::StoreMemberProviderAccessGrantRef::from_grant(
        &access_grant,
        exact(
            provider_access_grant_semantic_prefix(&access_grant.grant_id) + ".json",
            &access_grant.to_bytes(),
        ),
    );
    let verified_root = crate::sync::store_objects::VerifiedObject {
        value: fixture.root.clone(),
        bytes: fixture.root.to_bytes(),
        semantic_hash: fixture.root_ref.store_root_hash,
        object: fixture.root_ref.object.clone(),
    };
    let approval = crate::sync::device_join::DeviceProviderAdmissionApproval::signed(
        access_request,
        crate::sync::provider::ActivatedStoreMemberProviderAccessGrant {
            grant: access_grant,
            grant_ref: access_grant_ref,
            activation: fixture.commit_ref.clone(),
        },
        crate::sync::device_join::DeviceProviderAdmissionChallenge::SamePrincipal,
        &verified_root,
        &fixture.registration,
        &owner_device_signer,
    )
    .unwrap();
    let attempt = DeviceJoinAttempt::signed(
        fixture.root_ref.clone(),
        attempt_id,
        attempt_slot.clone(),
        registration.clone(),
        registration_slot,
        outcome_slot,
        attempt_cut,
        membership,
        provider_admin.grant_id,
        approval,
        crate::sync::device_join::DeviceProviderResponseReservation::SamePrincipal,
        fixture.registration_ref.clone(),
        fixture.root.descriptor.founder_grant.clone(),
        &fixture.registration,
        &owner_device_signer,
    )
    .unwrap();
    let attempt_ref = DeviceJoinAttemptRef {
        attempt_id,
        attempt_hash: attempt.attempt_hash(),
        object: ExactObjectRef::new(
            attempt_slot,
            attempt.to_bytes().len() as u64,
            ObjectHash::digest(&attempt.to_bytes()),
        ),
    };
    let stream_id = AuthorStreamId::from_digest(ObjectHash::digest(b"other stream"));
    let other_commit_hash = ObjectHash::digest(b"other commit");
    let other_commit = StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id,
            sequence: 1,
        },
        commit_hash: other_commit_hash,
        object: exact(
            format!(
                "{}.json",
                commit_semantic_prefix(
                    CandidateFamilyId::from_hash(ObjectHash::digest(
                        b"other commit candidate family",
                    )),
                    &stream_id.to_string(),
                    1,
                    other_commit_hash,
                )
            ),
            b"other commit",
        ),
    };
    let other_frontier = BTreeMap::from([(stream_id, other_commit)]);
    let other_cut = StoreHistoryCut(other_frontier.clone());
    let mut other_resolved_devices = ResolvedStoreDeviceState::founder(
        &fixture.root_ref,
        registration_ref.clone(),
        &fixture.root.descriptor.founder_pubkey,
        fixture.root.descriptor.founder_grant.clone(),
        &fixture.root.descriptor.founder_recovery,
    )
    .expect("derive other device state");
    other_resolved_devices.state_hash = ObjectHash::digest(b"other device state");
    let other_device_state =
        StoreDeviceStateRef::from_resolved(CommitFrontier(other_frontier), &other_resolved_devices)
            .expect("bind other device state frontier");
    let device_signer = registration.device_signer(&joiner).unwrap();
    let ack = StoreAck::signed(
        fixture.root_ref.store_root_hash,
        registration_ref.clone(),
        1,
        other_cut.clone(),
        other_device_state,
        None,
        StoreAckExclusionState {
            proposal_freezes: Vec::new(),
        },
        "2026-07-16T00:00:00Z".to_string(),
        SuccessorLink {
            activation: registration
                .store_acknowledgement_activation(&registration_ref)
                .expect("derive exact Store acknowledgement activation")
                .activation_id(),
            predecessor: None,
            next_slot: slot("store-v1/acks/joiner/2.json".to_string()),
        },
        &device_signer,
    )
    .unwrap();
    let ack_ref = StoreAckRef {
        registration: registration_ref.clone(),
        sequence: 1,
        ack_hash: ack.ack_hash(),
        object: exact("store-v1/acks/joiner/1.json".to_string(), &ack.to_bytes()),
    };
    let proof = DeviceReadinessProof::signed(
        attempt_ref.clone(),
        registration_ref,
        ack_ref.clone(),
        other_cut,
        &registration,
        &device_signer,
    )
    .unwrap();

    assert!(matches!(
        proof.verify(&attempt_ref, &attempt, &registration, &ack_ref, &ack),
        Err(StoreProtocolError::DeviceReadinessMismatch)
    ));
}

fn signed_test_ack(fixture: &Fixture, last_sync: &str) -> StoreAck {
    StoreAck::signed(
        fixture.root_ref.store_root_hash,
        fixture.registration_ref.clone(),
        1,
        StoreHistoryCut(BTreeMap::new()),
        fixture.commit.device_state.clone(),
        None,
        StoreAckExclusionState {
            proposal_freezes: Vec::new(),
        },
        last_sync.to_string(),
        SuccessorLink {
            activation: fixture
                .registration
                .store_acknowledgement_activation(&fixture.registration_ref)
                .expect("derive acknowledgement activation")
                .activation_id(),
            predecessor: None,
            next_slot: slot("store-v1/acks/founder/2.json".to_string()),
        },
        &fixture
            .registration
            .device_signer(&fixture.signer)
            .expect("derive device signer"),
    )
    .expect("sign Store acknowledgement")
}

#[test]
fn store_ack_semantic_hash_is_distinct_from_its_stored_json_hash() {
    let ack = signed_test_ack(&fixture(), "2026-07-16T00:00:00Z");
    let bytes = ack.to_bytes();
    let semantic_hash = StoreAck::semantic_hash_from_bytes(&bytes).unwrap();

    assert_eq!(semantic_hash, ack.ack_hash());
    assert_ne!(semantic_hash, ObjectHash::digest(&bytes));
}

#[test]
fn store_ack_wire_shape_binds_activation_state_without_a_parallel_predecessor_ref() {
    let ack = signed_test_ack(&fixture(), "2026-07-18T00:00:00Z");
    let value = serde_json::to_value(ack).unwrap();

    assert!(value.get("registration").is_some());
    assert!(value.get("sequence").is_some());
    assert!(value.get("device_state").is_some());
    assert!(value.get("snapshot").is_some());
    assert!(value.get("exclusions").is_some());
    assert!(value.get("author_registration").is_none());
    assert!(value.get("revision").is_none());
    assert!(value.get("predecessor").is_none());
}

#[test]
fn store_protocol_root_authenticates_the_creation_descriptor() {
    let fixture = fixture();
    let bytes = fixture.root.to_bytes();
    let parsed = StoreProtocolRoot::parse_expected(&bytes, &fixture.root_ref, routing_hash())
        .expect("parse exact Store protocol root");
    assert_eq!(parsed, fixture.root);
}

#[test]
fn store_protocol_root_signs_the_sync_routing_contract() {
    let fixture = fixture();
    let value = serde_json::to_value(fixture.root).expect("serialize Store root");

    let descriptor = value
        .get("descriptor")
        .expect("Store root carries its creation descriptor");
    assert!(
        descriptor.get("sync_routing_hash").is_some(),
        "the signed Store root must bind the sync-routing contract"
    );
}

#[test]
fn operations_commit_uses_the_closed_body_and_signed_manifest_shape() {
    let fixture = fixture();
    let value = serde_json::to_value(&fixture.commit).expect("serialize Store commit");

    assert!(value.get("package").is_none());
    assert!(value.get("store_package").is_none());
    assert!(value.get("device_registrations").is_none());
    assert!(value.get("circle_controls").is_none());
    assert!(value.get("circle_packages").is_none());
    let operations = value
        .get("body")
        .and_then(|body| body.get("operations"))
        .expect("Store commit carries one closed operations body");
    assert!(operations.get("store_package").is_some());
    assert_eq!(
        operations.get("device_registrations"),
        Some(&serde_json::json!([]))
    );
    assert_eq!(
        operations.get("device_join_attempt_decisions"),
        Some(&serde_json::json!([]))
    );
    assert_eq!(
        operations.get("circle_controls"),
        Some(&serde_json::json!([]))
    );
    assert_eq!(
        operations.get("circle_packages"),
        Some(&serde_json::json!([]))
    );
    assert_eq!(
        value
            .get("candidate_objects")
            .and_then(|manifest| manifest.get("family")),
        Some(&serde_json::to_value(fixture.commit.candidate_family()).unwrap())
    );
}

#[test]
fn one_join_attempt_cannot_be_activated_and_abandoned_in_the_same_commit() {
    let attempt_id = DeviceJoinAttemptId::from_hash(ObjectHash::digest(b"join attempt"));
    let attempt = DeviceJoinAttemptRef {
        attempt_id,
        attempt_hash: ObjectHash::digest(b"attempt body"),
        object: exact(
            "store-v1/device-join-attempts/attempt.json".to_string(),
            b"attempt body",
        ),
    };
    let abandonment = super::super::device_join::DeviceJoinAbandonmentRef {
        attempt_id,
        abandonment_hash: ObjectHash::digest(b"abandonment body"),
        object: exact(
            "store-v1/device-join-abandonments/attempt.json".to_string(),
            b"abandonment body",
        ),
    };

    assert_eq!(
        validate_device_join_attempt_decision_refs(&[
            DeviceJoinAttemptDecisionRef::Attempt(attempt),
            DeviceJoinAttemptDecisionRef::Abandoned(abandonment),
        ]),
        Err(StoreProtocolError::JoinAttemptMismatch)
    );
}

fn resign_commit(commit: &mut StoreBatchCommit, fixture: &Fixture) {
    let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
    commit.signature = keys::sign_hex(&signer, &commit.canonical_signed_bytes()).1;
}

fn candidate_cleanup_manifest(fixture: &Fixture, label: &str) -> CandidateCleanupManifest {
    let package = label.as_bytes();
    let write_id = WriteId::from_generated(format!("{label}-write"));
    let order = fixture.commit.order.clone();
    let sequence = order.seq();
    let family = CandidateFamilyId::derive(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        &write_id,
        &order,
    );
    let package_object = exact(
        format!(
            "{}.pkg",
            package_semantic_prefix(
                family,
                &fixture.commit_ref.coord.stream_id.to_string(),
                sequence,
                ObjectHash::digest(package),
            )
        ),
        package,
    );
    let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
    let commit = StoreBatchCommit::signed(
        fixture.root_ref.store_root_hash,
        write_id,
        fixture.commit_ref.coord.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
        order,
        fixture.commit.membership_state.clone(),
        fixture.commit.device_state.clone(),
        fixture
            .commit
            .operations_membership_authority()
            .expect("fixture carries membership authority"),
        StorePackageInput {
            candidate_family: family,
            schema_version: 3,
            bytes: package,
            object: package_object,
        },
        &signer,
    )
    .expect("sign candidate commit");
    let bytes = commit.to_bytes();
    CandidateCleanupManifest {
        candidate: StoreBatchCommitDeletionTarget {
            coord: fixture.commit_ref.coord.clone(),
            object: exact(
                format!(
                    "{}.json",
                    commit_semantic_prefix(
                        commit.candidate_family(),
                        &fixture.commit_ref.coord.stream_id.to_string(),
                        commit.seq(),
                        commit.commit_hash(),
                    )
                ),
                &bytes,
            ),
            canonical_signed_bytes: bytes,
        },
    }
}

fn sign_candidate_abandonment(
    fixture: &Fixture,
    manifests: Vec<CandidateCleanupManifest>,
) -> Result<StoreBatchCommit, StoreProtocolError> {
    let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
    StoreBatchCommit::signed_with_candidate_abandonment(
        fixture.root_ref.store_root_hash,
        WriteId::from_generated("abandon-candidates".to_string()),
        fixture.commit_ref.coord.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
        fixture.commit.order.clone(),
        fixture.commit.membership_state.clone(),
        fixture.commit.device_state.clone(),
        manifests,
        &signer,
    )
}

#[test]
fn candidate_abandonment_is_signed_canonical_cleanup_authority() {
    let fixture = fixture();
    let first = candidate_cleanup_manifest(&fixture, "first candidate");
    let second = candidate_cleanup_manifest(&fixture, "second candidate");
    let commit = sign_candidate_abandonment(&fixture, vec![second.clone(), first.clone()])
        .expect("sign candidate abandonment");

    assert!(commit.candidate_objects.objects.is_empty());
    assert_eq!(
        commit.abandoned_candidates(),
        [first, second]
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    );
    commit
        .verify_at(
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        )
        .expect("verify candidate abandonment");
}

#[test]
fn candidate_abandonment_rejects_duplicate_and_inexact_targets() {
    let fixture = fixture();
    let manifest = candidate_cleanup_manifest(&fixture, "candidate");
    assert!(matches!(
        sign_candidate_abandonment(&fixture, vec![manifest.clone(), manifest.clone()]),
        Err(StoreProtocolError::Malformed(reason))
            if reason.contains("strictly sorted and unique")
    ));

    let mut inexact = manifest;
    inexact.candidate.object = exact(
        inexact.candidate.object.slot().logical_key().to_string(),
        b"different stored bytes",
    );
    assert!(matches!(
        sign_candidate_abandonment(&fixture, vec![inexact]),
        Err(StoreProtocolError::Malformed(reason))
            if reason.contains("does not match stored size/hash")
    ));
}

#[test]
fn candidate_abandonment_rejects_noncanonical_or_unsigned_candidate_bytes() {
    let fixture = fixture();
    let manifest = candidate_cleanup_manifest(&fixture, "candidate");
    let candidate: StoreBatchCommit =
        serde_json::from_slice(&manifest.candidate.canonical_signed_bytes).unwrap();

    let mut noncanonical = manifest.clone();
    noncanonical.candidate.canonical_signed_bytes =
        serde_json::to_vec_pretty(&candidate).expect("serialize noncanonical candidate");
    noncanonical.candidate.object = exact(
        noncanonical
            .candidate
            .object
            .slot()
            .logical_key()
            .to_string(),
        &noncanonical.candidate.canonical_signed_bytes,
    );
    assert!(matches!(
        sign_candidate_abandonment(&fixture, vec![noncanonical]),
        Err(StoreProtocolError::Malformed(reason))
            if reason.contains("not canonical")
    ));

    let mut unsigned_candidate = candidate;
    unsigned_candidate.signature.push('0');
    let mut unsigned = manifest;
    unsigned.candidate.canonical_signed_bytes = unsigned_candidate.to_bytes();
    unsigned.candidate.object = exact(
        unsigned.candidate.object.slot().logical_key().to_string(),
        &unsigned.candidate.canonical_signed_bytes,
    );
    assert!(matches!(
        sign_candidate_abandonment(&fixture, vec![unsigned]),
        Err(StoreProtocolError::InvalidSignature)
    ));
}

#[test]
fn candidate_abandonment_rejects_retained_authority_target() {
    let fixture = fixture();
    let inner = sign_candidate_abandonment(
        &fixture,
        vec![candidate_cleanup_manifest(&fixture, "candidate")],
    )
    .expect("sign inner candidate abandonment");
    let bytes = inner.to_bytes();
    let retained = CandidateCleanupManifest {
        candidate: StoreBatchCommitDeletionTarget {
            coord: fixture.commit_ref.coord.clone(),
            object: exact(
                format!(
                    "{}.json",
                    commit_semantic_prefix(
                        inner.candidate_family(),
                        &fixture.commit_ref.coord.stream_id.to_string(),
                        inner.seq(),
                        inner.commit_hash(),
                    )
                ),
                &bytes,
            ),
            canonical_signed_bytes: bytes,
        },
    };

    assert!(matches!(
        sign_candidate_abandonment(&fixture, vec![retained]),
        Err(StoreProtocolError::Malformed(reason))
            if reason.contains("retained authority")
    ));
}

#[test]
fn parsed_candidate_abandonment_rejects_noncanonical_manifest_order() {
    let fixture = fixture();
    let first = candidate_cleanup_manifest(&fixture, "first candidate");
    let second = candidate_cleanup_manifest(&fixture, "second candidate");
    let mut commit = sign_candidate_abandonment(&fixture, vec![first, second])
        .expect("sign candidate abandonment");
    let StoreCommitBody::AbandonCandidates { manifests } = &mut commit.body else {
        panic!("commit carries candidate abandonment")
    };
    manifests.reverse();
    resign_commit(&mut commit, &fixture);

    assert!(matches!(
        commit.verify_at(
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        ),
        Err(StoreProtocolError::Malformed(reason))
            if reason.contains("strictly sorted and unique")
    ));
}

#[test]
fn commit_rejects_manifest_omission_invention_and_family_substitution() {
    let fixture = fixture();

    let mut omitted = fixture.commit.clone();
    omitted.candidate_objects.objects.clear();
    resign_commit(&mut omitted, &fixture);
    assert!(matches!(
        omitted.verify_at(
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        ),
        Err(StoreProtocolError::Malformed(reason))
            if reason.contains("manifest differs")
    ));

    let mut invented = fixture.commit.clone();
    invented
        .candidate_objects
        .objects
        .push(invented.candidate_objects.objects[0].clone());
    resign_commit(&mut invented, &fixture);
    assert!(matches!(
        invented.verify_at(
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        ),
        Err(StoreProtocolError::Malformed(reason))
            if reason.contains("manifest differs")
    ));

    let mut substituted = fixture.commit.clone();
    substituted.candidate_objects.family =
        CandidateFamilyId::from_hash(ObjectHash::digest(b"substituted candidate family"));
    resign_commit(&mut substituted, &fixture);
    assert!(matches!(
        substituted.verify_at(
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        ),
        Err(StoreProtocolError::Malformed(reason))
            if reason.contains("manifest differs")
    ));
}

fn closed_store_package_fixture(
    fixture: &Fixture,
) -> (StoreBatchCommit, StoreBatchCommitRef, Vec<u8>, Vec<u8>) {
    let write_id = WriteId::from_generated("closed-package-graph".to_string());
    let order = fixture.commit.order.clone();
    let sequence = order.seq();
    let family = CandidateFamilyId::derive(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        &write_id,
        &order,
    );
    let package = super::super::audience_package::AudiencePackage::store(
        fixture.root_ref.store_root_hash,
        family,
        write_id.clone(),
        fixture.commit_ref.coord.clone(),
        3,
        b"closed graph changeset".to_vec(),
        Vec::new(),
    )
    .unwrap();
    let semantic = package.to_bytes();
    let stored = b"encrypted closed graph package".to_vec();
    let package_object = exact(
        format!(
            "{}.pkg",
            package_semantic_prefix(
                family,
                &fixture.commit_ref.coord.stream_id.to_string(),
                sequence,
                ObjectHash::digest(&semantic),
            )
        ),
        &stored,
    );
    let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
    let commit = StoreBatchCommit::signed(
        fixture.root_ref.store_root_hash,
        write_id,
        fixture.commit_ref.coord.clone(),
        fixture.registration_ref.clone(),
        &fixture.registration,
        order,
        fixture.commit.membership_state.clone(),
        fixture.commit.device_state.clone(),
        fixture
            .commit
            .operations_membership_authority()
            .expect("fixture carries membership authority"),
        StorePackageInput {
            candidate_family: family,
            schema_version: 3,
            bytes: &semantic,
            object: package_object,
        },
        &signer,
    )
    .unwrap();
    let commit_bytes = commit.to_bytes();
    let reference = StoreBatchCommitRef::from_commit(
        &commit,
        fixture.commit_ref.coord.clone(),
        exact(
            format!(
                "{}.json",
                commit_semantic_prefix(
                    family,
                    &fixture.commit_ref.coord.stream_id.to_string(),
                    sequence,
                    commit.commit_hash(),
                )
            ),
            &commit_bytes,
        ),
    )
    .unwrap();
    (commit, reference, semantic, stored)
}

#[test]
fn closed_candidate_graph_rejects_omitted_invented_and_substituted_package_material() {
    let fixture = fixture();
    let (commit, owner, semantic, stored) = closed_store_package_fixture(&fixture);
    let package = commit.store_package().cloned().unwrap();
    let graph = super::super::remote_object::CandidateObjectGraph::from_commit(&commit).unwrap();
    assert!(matches!(
        graph.clone().close(&commit, &owner, Vec::new()),
        Err(super::super::remote_object::RemoteObjectRecordError::CandidateObjectMissing)
    ));
    let exact_material = super::super::remote_object::CandidateObjectMaterial {
        object: package.object.clone(),
        canonical_semantic_bytes: semantic.clone(),
        stored_bytes: stored.clone(),
    };
    let invented_material = super::super::remote_object::CandidateObjectMaterial {
        object: exact("store-v1/candidates/invented.pkg".to_string(), b"invented"),
        canonical_semantic_bytes: b"invented".to_vec(),
        stored_bytes: b"invented".to_vec(),
    };
    assert!(matches!(
        graph.clone().close(
            &commit,
            &owner,
            vec![exact_material.clone(), invented_material]
        ),
        Err(super::super::remote_object::RemoteObjectRecordError::CandidateObjectInvented)
    ));
    let mut records = graph.close(&commit, &owner, vec![exact_material]).unwrap();
    let super::super::remote_object::RemoteObjectRecord::CandidateExclusive(record) =
        &mut records[0]
    else {
        panic!("package graph must close as candidate-exclusive")
    };
    record.identity.domain =
        super::super::remote_object::CandidateExclusiveObjectDomain::CirclePackage {
            reference: CirclePackageRef {
                circle_id: CircleId::from_bytes([9; 16]),
                control: test_circle_control_coord(
                    &fixture,
                    ObjectHash::digest(b"substituted control"),
                ),
                package,
                key_fingerprint: KeyFingerprint::from_bytes([7; 8]),
            },
        };
    assert!(matches!(
        records[0].validate(),
        Err(super::super::remote_object::RemoteObjectRecordError::DomainMismatch)
    ));
}

#[test]
fn candidate_manifest_rejects_one_exact_object_reached_twice() {
    let fixture = fixture();
    let mut operations = fixture
        .commit
        .operations()
        .expect("fixture commit carries operations")
        .clone();
    let package = operations
        .store_package
        .clone()
        .expect("fixture commit carries a Store package");
    operations.circle_packages.push(CirclePackageRef {
        circle_id: CircleId::from_bytes([8; 16]),
        control: test_circle_control_coord(
            &fixture,
            ObjectHash::digest(b"duplicate exact object control"),
        ),
        package,
        key_fingerprint: KeyFingerprint::from_bytes([9; 8]),
    });

    assert!(matches!(
        candidate_manifest(
            fixture.commit.candidate_family(),
            &StoreCommitBody::Operations(operations),
        ),
        Err(StoreProtocolError::Malformed(reason))
            if reason.contains("repeats an exact object reference")
    ));
}

#[test]
fn candidate_manifest_rejects_duplicate_circle_access_with_distinct_provider_ids() {
    let fixture = fixture();
    let family = fixture.commit.candidate_family();
    let circle_id = CircleId::from_bytes([7; 16]);
    let owner_pubkey = keys::public_key_hex(&fixture.signer);
    let recipient_slot = "recipient-slot".to_string();
    let ids = crate::id_provider::SequentialIdProvider::new("duplicate Circle access");
    let epoch_id = CircleEpochId::generate(&ids);
    let leaf_id = AccessLeafId::generate(&ids);
    let leaf_hash = ObjectHash::digest(b"sealed access leaf");
    let control_hash = ObjectHash::digest(b"Circle access control");
    let leaf_key = circle_access_leaf_semantic_prefix(
        circle_id,
        family,
        &owner_pubkey,
        epoch_id,
        &recipient_slot,
        leaf_id,
    );
    let envelope_key = format!(
        "{}.json",
        circle_access_envelope_semantic_prefix(
            circle_id,
            family,
            &owner_pubkey,
            &recipient_slot,
            control_hash,
        )
    );
    let access = |provider_id: &str| CircleAccessObjectRef {
        leaf: CircleAccessLeafObjectRef {
            owner_pubkey: owner_pubkey.clone(),
            epoch_id,
            recipient_slot: recipient_slot.clone(),
            leaf_id,
            leaf_hash,
            object: ExactObjectRef::new(
                ObjectSlot::opaque(leaf_key.clone(), format!("{provider_id}-leaf")).unwrap(),
                18,
                leaf_hash,
            ),
        },
        envelope: CircleAccessEnvelopeObjectRef {
            owner_pubkey: owner_pubkey.clone(),
            recipient_slot: recipient_slot.clone(),
            control_hash,
            leaf_id,
            leaf_hash,
            object: ExactObjectRef::new(
                ObjectSlot::opaque(envelope_key.clone(), format!("{provider_id}-envelope"))
                    .unwrap(),
                20,
                ObjectHash::digest(provider_id.as_bytes()),
            ),
        },
    };
    let control = test_circle_control_coord(&fixture, control_hash);
    let mut operations = fixture
        .commit
        .operations()
        .expect("fixture commit carries operations")
        .clone();
    operations.circle_controls.push(CircleControlRef {
        circle_id,
        control,
        head_hash: ObjectHash::digest(b"duplicate Circle access head"),
        head_object: exact(
            "circle-control-head.json".to_string(),
            b"duplicate Circle access head",
        ),
        objects: CircleActivationObjects {
            control: exact("circle-control.json".to_string(), b"control"),
            roster_entries: BTreeMap::new(),
            roster_heads: Vec::new(),
            roster_resolutions: BTreeMap::new(),
            metadata_entries: BTreeMap::new(),
            metadata_heads: Vec::new(),
            access: vec![access("drive-file-a"), access("drive-file-b")],
        },
    });

    assert!(matches!(
        candidate_manifest(family, &StoreCommitBody::Operations(operations)),
        Err(StoreProtocolError::Malformed(reason))
            if reason.contains("repeats a Circle access semantic key")
    ));
}

#[test]
fn commit_reference_constructor_rejects_relocated_exact_object() {
    let fixture = fixture();
    let bytes = fixture.commit.to_bytes();
    let relocated = exact(
        format!(
            "store-v1/candidates/{}/packages/relocated.json",
            fixture.commit.candidate_family().as_hash()
        ),
        &bytes,
    );

    assert!(matches!(
        StoreBatchCommitRef::from_commit(
            &fixture.commit,
            fixture.commit_ref.coord.clone(),
            relocated,
        ),
        Err(StoreProtocolError::RelocatedSlot { .. })
    ));
}

#[path = "tests/device_state.rs"]
mod device_state_tests;
