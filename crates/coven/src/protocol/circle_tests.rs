use std::collections::BTreeMap;

use super::*;
use crate::keys::{self, UserKeypair};
use crate::protocol::circle_control::{merkle_root_and_proofs, verify_merkle_proof};
use crate::protocol::{membership, store_commit};

fn candidate_family(label: &str) -> store_commit::CandidateFamilyId {
    store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(label.as_bytes()))
}

fn exact_object(label: &str, bytes: &[u8]) -> crate::protocol::objects::ExactObjectRef {
    crate::protocol::objects::ExactObjectRef::new(
        crate::protocol::objects::ObjectSlot::logical(format!("store-v1/test/{label}.json"))
            .unwrap(),
        bytes.len() as u64,
        ObjectHash::digest(bytes),
    )
}

fn exact_logical_object(
    logical_key: String,
    bytes: &[u8],
) -> crate::protocol::objects::ExactObjectRef {
    crate::protocol::objects::ExactObjectRef::new(
        crate::protocol::objects::ObjectSlot::logical(logical_key).unwrap(),
        bytes.len() as u64,
        ObjectHash::digest(bytes),
    )
}

fn test_founder_entry(
    label: &str,
    owner: &UserKeypair,
    membership: store_commit::GrantStreamAnchor,
) -> membership::MembershipEntry {
    membership::founder_entry(
        label,
        owner,
        crate::protocol::causal_grants::MembershipGrantId::from_test_label(label),
        "founder",
        membership,
        crate::protocol::provider::FounderProviderAdminGrant::from_test_label(label),
    )
}

fn merge_membership_ref(
    owner: &UserKeypair,
    members: &[(String, membership::MemberRole)],
    label: &str,
) -> (
    StoreMembershipStateRef,
    membership::MembershipGrantCreationAuthority,
) {
    let founder = test_founder_entry(
        label,
        owner,
        store_commit::GrantStreamAnchor::StoreMembership {
            first_slot: crate::protocol::objects::ObjectSlot::logical(format!(
                "store-v1/test/{label}/membership/1.json"
            ))
            .unwrap(),
        },
    );
    let founder_coord = founder.coord();
    let mut chain = membership::MembershipChain::from_entries(vec![founder])
        .expect("found merge-concurrent membership");
    for (index, (pubkey, role)) in members.iter().enumerate() {
        if pubkey == &keys::public_key_hex(owner) {
            continue;
        }
        if role == &membership::MemberRole::Owner {
            chain
                .add_owner_for_test(
                    owner,
                    founder_coord.stream_id,
                    pubkey.clone(),
                    format!("member-{index}"),
                )
                .expect("promote merge-concurrent Owner");
            continue;
        }
        let entry = chain
            .signed_set_member_in_stream(
                owner,
                founder_coord.stream_id,
                pubkey.clone(),
                None,
                role.clone(),
                format!("member-{index}"),
            )
            .expect("sign merge-concurrent member");
        chain
            .add_entry(entry)
            .expect("apply merge-concurrent member");
    }
    let resolved = match chain.status() {
        membership::MembershipStatus::Resolved(resolved) => resolved,
        membership::MembershipStatus::Conflict(_) => {
            panic!("membership fixture must resolve")
        }
    };
    let tip = chain.entries().last().expect("membership tip").coord();
    let head = membership::MembershipHeadRef {
        coord: tip,
        head_hash: ObjectHash::digest(format!("{label} head").as_bytes()),
        object: exact_object(&format!("{label}/membership-head"), b"membership head"),
    };
    (
        StoreMembershipStateRef::from_parts(
            vec![head],
            Vec::new(),
            Vec::new(),
            resolved.state_hash,
        )
        .expect("valid merge-concurrent membership reference"),
        membership::MembershipGrantCreationAuthority::Entry(founder_coord),
    )
}

struct MergeDeviceAuthority {
    registration: store_commit::StoreDeviceRegistration,
    reference: store_commit::StoreDeviceRegistrationRef,
    device_signer: UserKeypair,
    stream_id: membership::AuthorStreamId,
}

impl MergeDeviceAuthority {
    fn circle_control_reference(
        &self,
        control: &PreparedCircleControl,
        label: &str,
    ) -> store_commit::CircleControlRef {
        let control_object = exact_object(&format!("{label}/control"), &control.bytes);
        let head_slot = crate::protocol::objects::ObjectSlot::logical(format!(
            "store-v1/test/{label}/control-head/1.json"
        ))
        .expect("valid test Circle control-head slot");
        let activation = store_commit::StreamActivation::grant_authorized(
            control.value.store_root_hash,
            self.reference.clone(),
            control.value.author_grant_id(),
            store_commit::GrantStreamAnchor::CircleControl {
                circle_id: control.value.circle_id,
                first_slot: head_slot.clone(),
            },
        );
        let head = CircleControlHead::signed(
            &control.value,
            control_object.clone(),
            store_commit::SuccessorLink {
                activation: activation.activation_id(),
                predecessor: None,
                next_slot: crate::protocol::objects::ObjectSlot::logical(format!(
                    "store-v1/test/{label}/control-head/2.json"
                ))
                .expect("valid next test Circle control-head slot"),
            },
            &self.device_signer,
        );
        let head_bytes = serde_json::to_vec(&head).expect("serialize test Circle control head");
        let head_object = crate::protocol::objects::ExactObjectRef::new(
            head_slot,
            head_bytes.len() as u64,
            ObjectHash::digest(&head_bytes),
        );
        let objects = store_commit::CircleActivationObjects {
            control: control_object,
            close_intent: None,
            close_outcome: None,
            close_cancellation: None,
            roster_entries: BTreeMap::new(),
            roster_heads: Vec::new(),
            roster_resolutions: BTreeMap::new(),
            metadata_entries: BTreeMap::new(),
            metadata_heads: Vec::new(),
            access: Vec::new(),
        };
        store_commit::CircleControlRef {
            circle_id: control.value.circle_id,
            control: control.coord.clone(),
            head_hash: head.head_hash(),
            head_object,
            objects,
        }
    }
}

fn merge_device_authority(
    identity: &UserKeypair,
    store_root_hash: ObjectHash,
    label: &str,
) -> MergeDeviceAuthority {
    let root = store_commit::StoreRootRef {
        store_root_id: ObjectHash::digest(format!("{label} identity").as_bytes()),
        store_root_hash,
        object: exact_object(&format!("{label}/root"), label.as_bytes()),
    };
    let slot = |stream: &str| {
        crate::protocol::objects::ObjectSlot::logical(format!(
            "store-v1/test/{label}/{stream}/1.json"
        ))
        .unwrap()
    };
    let registration = store_commit::StoreDeviceRegistration::signed(
        root.clone(),
        store_commit::StoreDeviceRegistrationOrigin::Founder {
            creation_id: store_commit::StoreCreationId::from_nonce(label),
        },
        crate::protocol::objects::ProviderDeviceBinding {
            principal: crate::protocol::objects::ProviderPrincipalId::CustomS3Credential {
                access_key_id_hash: ObjectHash::digest(label.as_bytes()),
            },
        },
        store_commit::DeviceStreamAnchor::StoreAnnouncements {
            first_slot: slot("announcements"),
        },
        store_commit::DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: slot("acknowledgements"),
        },
        store_commit::DeviceStreamAnchor::StoreSnapshots {
            first_slot: slot("snapshots"),
        },
        identity,
    )
    .expect("sign test device registration");
    let bytes = registration.to_bytes();
    let reference = store_commit::StoreDeviceRegistrationRef::from_registration(
        &registration,
        exact_object(&format!("{label}/registration"), &bytes),
    );
    let device_signer = registration
        .device_signer(identity)
        .expect("derive registered device signer");
    let stream_id = store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        &reference,
        store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    MergeDeviceAuthority {
        registration,
        reference,
        device_signer,
        stream_id,
    }
}

#[test]
fn merkle_proofs_verify_for_every_leaf_in_even_and_odd_layers() {
    for leaf_count in 1..=9 {
        let leaves = (0..leaf_count)
            .map(|index| ObjectHash::digest(format!("leaf-{index}").as_bytes()))
            .collect::<Vec<_>>();
        let (root, proofs) = merkle_root_and_proofs(&leaves);
        assert_eq!(proofs.len(), leaves.len());
        for (index, (leaf, proof)) in leaves.iter().zip(&proofs).enumerate() {
            assert!(
                verify_merkle_proof(*leaf, proof, root),
                "leaf {index} of {leaf_count} failed its canonical proof"
            );
        }
    }
}

#[test]
fn founder_payload_is_complete_and_acyclic() {
    let owner = crate::keys::UserKeypair::generate();
    let peer = crate::keys::UserKeypair::generate();
    let owner_pubkey = crate::keys::public_key_hex(&owner);
    let peer_pubkey = crate::keys::public_key_hex(&peer);
    let members = vec![
        (owner_pubkey.clone(), membership::MemberRole::Owner),
        (peer_pubkey.clone(), membership::MemberRole::Member),
    ];

    let (membership, membership_authority) =
        merge_membership_ref(&owner, &members, "founder-circle-merge");
    let ids = crate::id_provider::SequentialIdProvider::new("founder-circle");
    let candidate_family = candidate_family("founder-circle");
    let creation = CircleTransitionDraft::founder(
        ObjectHash::digest(b"store-root"),
        candidate_family,
        "device-a",
        "Household",
        "0000000001000-0000-device-a",
        membership,
        membership_authority,
        members.clone(),
        &ids,
        &owner,
    )
    .expect("construct founder circle");

    assert!(creation.control.verify());
    assert!(creation.metadata.verify());
    assert!(creation.roster.verify());
    assert_eq!(creation.access.len(), 2);
    for access in &creation.access {
        assert!(access.leaf.verify(&creation.control, candidate_family));
        assert!(access.envelope.verify(&creation.control, candidate_family));
        assert!(access
            .leaf
            .verify_envelope(&creation.control, &access.envelope, candidate_family));
        assert!(!access.leaf.bytes.windows(64).any(|window| {
            window == creation.control.coord.control_hash().to_string().as_bytes()
        }));
    }
    assert!(matches!(
        creation
            .access
            .iter()
            .find(|access| access.leaf.value.recipient_pubkey == owner_pubkey)
            .unwrap()
            .leaf
            .value
            .disposition,
        CircleAccessDisposition::Active { .. }
    ));
    assert!(matches!(
        creation
            .access
            .iter()
            .find(|access| access.leaf.value.recipient_pubkey == peer_pubkey)
            .unwrap()
            .leaf
            .value
            .disposition,
        CircleAccessDisposition::Inactive
    ));

    let mut seized = creation.control.value.clone();
    seized.circle_id = CircleId::from_bytes([0x5a; 16]);
    seized.signature = keys::sign_hex(&owner, &seized.canonical_bytes()).1;
    assert!(
        !seized.verify(),
        "a founder control must not choose an arbitrary Circle ID"
    );

    let mut discontinuous = creation.control.value.clone();
    discontinuous.value.order.seq = 2;
    discontinuous.signature = keys::sign_hex(&owner, &discontinuous.canonical_bytes()).1;
    assert!(
        !discontinuous.verify(),
        "a control without a predecessor must be genesis"
    );
}

#[test]
fn access_verification_rejects_signed_context_and_proof_substitution() {
    let owner = crate::keys::UserKeypair::generate();
    let peer = crate::keys::UserKeypair::generate();
    let owner_pubkey = crate::keys::public_key_hex(&owner);
    let peer_pubkey = crate::keys::public_key_hex(&peer);
    let members = vec![
        (owner_pubkey.clone(), membership::MemberRole::Owner),
        (peer_pubkey.clone(), membership::MemberRole::Member),
    ];
    let (membership, authority) = merge_membership_ref(&owner, &members, "access-verification");
    let ids = crate::id_provider::SequentialIdProvider::new("access-verification");
    let candidate_family = candidate_family("access-verification");
    let creation = CircleTransitionDraft::founder(
        ObjectHash::digest(b"store-root"),
        candidate_family,
        "device-a",
        "Household",
        "0000000001000-0000-device-a",
        membership,
        authority,
        members.clone(),
        &ids,
        &owner,
    )
    .expect("construct founder circle");

    let mut wrong_store = creation.access[0].envelope.clone();
    wrong_store.store_root_hash = ObjectHash::digest(b"other-store");

    wrong_store.signature = keys::sign_hex(&owner, &wrong_store.canonical_bytes()).1;
    assert!(!wrong_store.verify(&creation.control, candidate_family));

    let mut wrong_family_envelope = creation.access[0].envelope.clone();
    wrong_family_envelope.candidate_family =
        store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(b"other access family"));
    wrong_family_envelope.signature =
        keys::sign_hex(&owner, &wrong_family_envelope.canonical_bytes()).1;
    assert!(!wrong_family_envelope.verify(&creation.control, candidate_family));

    let mut wrong_family_leaf = creation.access[0].leaf.clone();
    wrong_family_leaf.value.candidate_family =
        store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(b"other leaf family"));
    wrong_family_leaf.value.signature =
        keys::sign_hex(&owner, &wrong_family_leaf.value.canonical_bytes()).1;
    assert!(!wrong_family_leaf.verify(&creation.control, candidate_family));

    let mut non_owner = creation.access[0].envelope.clone();
    non_owner.owner_pubkey = peer_pubkey;
    non_owner.signature = keys::sign_hex(&peer, &non_owner.canonical_bytes()).1;
    assert!(!non_owner.verify(&creation.control, candidate_family));

    let mut substituted_proof = creation.access[0].envelope.clone();
    substituted_proof.proof = creation.access[1].envelope.proof.clone();
    substituted_proof.signature = keys::sign_hex(&owner, &substituted_proof.canonical_bytes()).1;
    assert!(!substituted_proof.verify(&creation.control, candidate_family));

    let mut substituted_leaf_id = creation.access[0].envelope.clone();
    substituted_leaf_id.leaf_id = creation.access[1].leaf.value.leaf_id;
    substituted_leaf_id.signature =
        keys::sign_hex(&owner, &substituted_leaf_id.canonical_bytes()).1;
    assert!(substituted_leaf_id.verify(&creation.control, candidate_family));
    assert!(!creation.access[0].leaf.verify_envelope(
        &creation.control,
        &substituted_leaf_id,
        candidate_family,
    ));

    let mut wrong_membership_leaf = creation.access[0].leaf.value.clone();
    wrong_membership_leaf.store_membership =
        merge_membership_ref(&owner, &members, "wrong-membership-leaf").0;
    wrong_membership_leaf.signature =
        keys::sign_hex(&owner, &wrong_membership_leaf.canonical_bytes()).1;
    let recipient_key =
        keys::ed25519_to_x25519_public_key(&owner.public_key()).expect("convert recipient key");
    let bytes = keys::seal_box_encrypt(
        &serde_json::to_vec(&wrong_membership_leaf).expect("serialize forged leaf"),
        &recipient_key,
    );
    let wrong_membership_leaf = PreparedAccessLeaf {
        leaf_hash: ObjectHash::digest(&bytes),
        bytes,
        value: wrong_membership_leaf,
    };
    assert!(!wrong_membership_leaf.verify(&creation.control, candidate_family));

    let mut wrong_keyring_leaf = creation
        .access
        .iter()
        .find(|access| {
            matches!(
                &access.leaf.value.disposition,
                CircleAccessDisposition::Active { .. }
            )
        })
        .expect("founder access")
        .leaf
        .value
        .clone();
    let CircleAccessDisposition::Active { keyring, .. } = &mut wrong_keyring_leaf.disposition
    else {
        panic!("founder access must be active")
    };
    *keyring = crate::encryption::MasterKeyring::generate().to_serialized();
    wrong_keyring_leaf.signature = keys::sign_hex(&owner, &wrong_keyring_leaf.canonical_bytes()).1;
    let bytes = keys::seal_box_encrypt(
        &serde_json::to_vec(&wrong_keyring_leaf).expect("serialize wrong-keyring leaf"),
        &recipient_key,
    );
    let wrong_keyring_leaf = PreparedAccessLeaf {
        leaf_hash: ObjectHash::digest(&bytes),
        bytes,
        value: wrong_keyring_leaf,
    };
    assert!(!wrong_keyring_leaf.verify(&creation.control, candidate_family));
}

#[test]
fn circle_id_round_trips_only_its_canonical_lowercase_base32() {
    let id = CircleId::from_bytes([0; 16]);
    let encoded = id.to_string();
    assert_eq!(encoded.len(), CIRCLE_ID_LENGTH);
    assert_eq!(encoded.parse::<CircleId>().unwrap(), id);
    assert!(encoded.to_uppercase().parse::<CircleId>().is_err());
    assert!("local".parse::<CircleId>().is_err());
    assert!(format!("{}b", &encoded[..25]).parse::<CircleId>().is_err());
}

#[test]
fn recipient_slot_rejects_the_ed25519_identity_point() {
    let local = UserKeypair::generate();
    let mut identity = [0; keys::SIGN_PUBLICKEYBYTES];
    identity[0] = 1;
    let recipient = hex::encode(identity);

    assert_eq!(
        recipient_slot_with_peer(&local, &recipient, CircleId::from_bytes([9; 16])),
        Err(CircleTransitionError::InvalidRecipient(recipient))
    );
}

#[test]
fn row_routing_id_is_stable_across_store_key_rotation() {
    let root = ObjectHash::digest(b"store-root");
    let before = EncryptionService::from_key([1u8; 32]);
    let after = before
        .with_appended_generation(2, [2u8; 32])
        .expect("append generation");
    let before_id = row_routing_id(
        &derive_row_routing_key(&before, root).unwrap(),
        "accounts",
        "row-1",
    );
    let after_id = row_routing_id(
        &derive_row_routing_key(&after, root).unwrap(),
        "accounts",
        "row-1",
    );
    assert_eq!(before_id, after_id);
    assert_ne!(
        before_id,
        row_routing_id(
            &derive_row_routing_key(&after, root).unwrap(),
            "accounts",
            "row-2",
        )
    );
}

#[test]
fn row_routing_key_requires_exactly_one_generation_one_key() {
    let root = ObjectHash::digest(b"store-root");
    let missing = EncryptionService::from_key_at_generation(2, [2u8; 32]);
    assert!(matches!(
        derive_row_routing_key(&missing, root),
        Err(RowRoutingKeyError::MissingGenerationOne)
    ));

    let ambiguous = EncryptionService::from_keyring([(1, [1u8; 32]), (1, [2u8; 32])])
        .expect("build forked generation one");
    assert!(matches!(
        derive_row_routing_key(&ambiguous, root),
        Err(RowRoutingKeyError::AmbiguousGenerationOne)
    ));
}

#[tokio::test]
async fn control_history_caches_the_verified_access_owner_and_rejects_second_genesis() {
    let author = crate::keys::UserKeypair::generate();
    let author_pubkey = crate::keys::public_key_hex(&author);
    let earlier_owner = loop {
        let candidate = crate::keys::UserKeypair::generate();
        if crate::keys::public_key_hex(&candidate) < author_pubkey {
            break candidate;
        }
    };
    let earlier_owner_pubkey = crate::keys::public_key_hex(&earlier_owner);
    let members = vec![
        (author_pubkey.clone(), membership::MemberRole::Owner),
        (earlier_owner_pubkey.clone(), membership::MemberRole::Owner),
    ];
    let store_root_hash = ObjectHash::digest(b"multi-owner-store-root");
    let (membership, membership_authority) =
        merge_membership_ref(&author, &members, "multi-owner-control");
    let device = merge_device_authority(&author, store_root_hash, "multi-owner-device");
    let ids = crate::id_provider::SequentialIdProvider::new("multi-owner-control");
    let operation_id = crate::WriteId::from_generated("multi-owner-control-commit".to_string());
    let order = store_commit::StoreCommitOrder {
        seq: 1,
        predecessor: None,
        dependencies: BTreeMap::new(),
    };
    let candidate_family = store_commit::CandidateFamilyId::derive(
        store_root_hash,
        &device.reference,
        &operation_id,
        &order,
    );
    let creation = CircleTransitionDraft::founder(
        store_root_hash,
        candidate_family,
        &device.reference.device_id.to_string(),
        "Household",
        "0000000001000-0000-device-a",
        membership.clone(),
        membership_authority.clone(),
        members,
        &ids,
        &author,
    )
    .expect("construct founder circle");
    let mut control = creation.control.value.clone();
    let active_epoch = control
        .value
        .state
        .active_epoch_mut()
        .expect("test control has an active epoch");
    active_epoch.common.owners = vec![earlier_owner_pubkey, author_pubkey.clone()];
    active_epoch.common.owners.sort();
    assert_ne!(active_epoch.common.owners[0], control.author_pubkey);
    control.signature = keys::sign_hex(&author, &control.canonical_bytes()).1;
    let control = PreparedCircleControl {
        coord: control.coord(),
        bytes: serde_json::to_vec(&control).expect("serialize control"),
        value: control,
    };
    let reference = device.circle_control_reference(&control, "multi-owner");
    let first_coord = store_commit::StoreCommitCoord {
        stream_id: device.stream_id,
        sequence: 1,
    };
    let commit = store_commit::StoreBatchCommit::signed_operations(
        store_root_hash,
        operation_id,
        first_coord.clone(),
        device.reference.clone(),
        &device.registration,
        order,
        membership.clone(),
        store_commit::StoreDeviceStateRef::from_resolved(
            store_commit::CommitFrontier(BTreeMap::new()),
            &store_commit::ResolvedStoreDeviceState {
                devices: BTreeMap::new(),
                recovery: Vec::new(),
                state_hash: ObjectHash::digest(b"multi-owner initial device state"),
            },
        )
        .expect("bind initial device state"),
        store_commit::StoreOperationMembershipAuthority {
            predecessor: membership_authority.clone(),
        },
        store_commit::StoreCommitOperationsInput {
            acknowledgement: None,
            circle_acknowledgements: Vec::new(),
            control: None,
            device_join_attempt_decisions: Vec::new(),
            device_join_outcomes: Vec::new(),
            device_join_cleanup_receipts: Vec::new(),
            provider_access_grants: Vec::new(),
            device_registrations: Vec::new(),
            device_exclusion_proposals: Vec::new(),
            device_exclusion_outcomes: Vec::new(),
            stream_activations: Vec::new(),
            circle_controls: vec![reference.clone()],
            store_package: None,
            circle_packages: &[],
        },
        &device.device_signer,
    )
    .expect("sign Store commit");
    let first_commit_path = format!(
        "{}.json",
        store_commit::commit_semantic_prefix(
            commit.candidate_family(),
            &device.stream_id.to_string(),
            1,
            commit.commit_hash(),
        )
    );
    let commit_ref = store_commit::StoreBatchCommitRef::from_commit(
        &commit,
        first_coord,
        exact_logical_object(first_commit_path, &commit.to_bytes()),
    )
    .expect("reference first Store commit");
    let verified_commit = store_commit::VerifiedStoreBatchCommit::parse(
        &commit.to_bytes(),
        store_root_hash,
        &commit_ref,
        &device.registration,
    )
    .expect("authenticate first Store commit");
    let own_access = creation
        .access
        .iter()
        .find(|access| access.leaf.value.recipient_pubkey == author_pubkey)
        .expect("author access");
    let verified = crate::protocol::circle_activation::VerifiedCircleReference {
        reference,
        circle_id: creation.circle_id,
        control: control.clone(),
        local_access: Some(crate::sync::store::VerifiedCircleAccess {
            envelope: own_access.envelope.clone(),
            leaf: own_access.leaf.clone(),
            active: Some(crate::sync::store::VerifiedCircleActive {
                roster: creation.roster.clone(),
                metadata: creation.metadata.clone(),
            }),
        }),
    };
    let db = crate::sync::test_helpers::open_test_db();
    let store_database = crate::database::StoreDatabase::new(&db);
    let first_commit = verified_commit.clone();
    db.test_sql(move |database| {
        database.record_verified_circle_activations(&first_commit, &[verified])
    })
    .await
    .expect("record multi-Owner control");
    let cached_owner = db
        .test_sql(move |database| database.circle_access_owner(creation.circle_id))
        .await
        .expect("read cached access owner");
    assert_eq!(cached_owner, author_pubkey);
    db.test_sql(|database| {
        database.clear_table(crate::database::DatabaseTestTable::named(
            "circle_access_cache",
        ))
    })
    .await
    .expect("remove historical Circle projections");
    let circles = store_database
        .get_circles(
            &author_pubkey,
            std::collections::BTreeSet::from([author_pubkey.clone()]),
        )
        .await
        .expect("list Circle from its derived current state");
    assert_eq!(
        circles,
        vec![CircleInfo::Active {
            id: creation.circle_id,
            name: creation.metadata.name.clone(),
            role: CircleRole::Owner,
            rotation_required: false,
        }]
    );
    let publication = store_database
        .circle_publication_context(creation.circle_id, control.coord.clone())
        .await
        .expect("load publication authority from derived current state");
    let publication_fingerprint = publication.key_fingerprint();
    assert_eq!(publication_fingerprint, control.value.key_fingerprint());

    let mut second_value = control.value.clone();
    let active_epoch = second_value
        .value
        .state
        .active_epoch_mut()
        .expect("test control has an active epoch");
    active_epoch.common.access_root = ObjectHash::digest(b"different founder access root");
    second_value.signature = keys::sign_hex(&author, &second_value.canonical_bytes()).1;
    let second_control = PreparedCircleControl {
        coord: second_value.coord(),
        bytes: serde_json::to_vec(&second_value).expect("serialize second founder control"),
        value: second_value,
    };
    let second_reference = device.circle_control_reference(&second_control, "second-founder");
    let second_coord = store_commit::StoreCommitCoord {
        stream_id: device.stream_id,
        sequence: 2,
    };
    let second_commit = store_commit::StoreBatchCommit::signed_operations(
        store_root_hash,
        crate::WriteId::from_generated("second-founder-control-commit".to_string()),
        second_coord.clone(),
        device.reference,
        &device.registration,
        store_commit::StoreCommitOrder {
            seq: 2,
            predecessor: Some(commit_ref.clone()),
            dependencies: BTreeMap::new(),
        },
        membership,
        store_commit::StoreDeviceStateRef::from_resolved(
            store_commit::CommitFrontier(BTreeMap::from([(device.stream_id, commit_ref.clone())])),
            &store_commit::ResolvedStoreDeviceState {
                devices: BTreeMap::new(),
                recovery: Vec::new(),
                state_hash: ObjectHash::digest(b"multi-owner second device state"),
            },
        )
        .expect("bind second device state"),
        store_commit::StoreOperationMembershipAuthority {
            predecessor: control.value.membership_authority().clone(),
        },
        store_commit::StoreCommitOperationsInput {
            acknowledgement: None,
            circle_acknowledgements: Vec::new(),
            control: None,
            device_join_attempt_decisions: Vec::new(),
            device_join_outcomes: Vec::new(),
            device_join_cleanup_receipts: Vec::new(),
            provider_access_grants: Vec::new(),
            device_registrations: Vec::new(),
            device_exclusion_proposals: Vec::new(),
            device_exclusion_outcomes: Vec::new(),
            stream_activations: Vec::new(),
            circle_controls: vec![second_reference.clone()],
            store_package: None,
            circle_packages: &[],
        },
        &device.device_signer,
    )
    .expect("sign second founder Store commit");
    let second_commit_path = format!(
        "{}.json",
        store_commit::commit_semantic_prefix(
            second_commit.candidate_family(),
            &device.stream_id.to_string(),
            2,
            second_commit.commit_hash(),
        )
    );
    let second_commit_ref = store_commit::StoreBatchCommitRef::from_commit(
        &second_commit,
        second_coord,
        exact_logical_object(second_commit_path, &second_commit.to_bytes()),
    )
    .expect("reference second Store commit");
    let second_commit = store_commit::VerifiedStoreBatchCommit::parse(
        &second_commit.to_bytes(),
        store_root_hash,
        &second_commit_ref,
        &device.registration,
    )
    .expect("authenticate second Store commit");
    let error = db
        .test_sql(move |database| {
            database.record_verified_circle_activations(
                &second_commit,
                &[
                    crate::protocol::circle_activation::VerifiedCircleReference {
                        reference: second_reference,
                        circle_id: creation.circle_id,
                        control: second_control,
                        local_access: None,
                    },
                ],
            )
        })
        .await
        .expect_err("a Circle cannot accept a second founder control");
    assert!(
        error.to_string().contains("already has a founder"),
        "{error}"
    );
}
