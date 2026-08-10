use std::collections::BTreeMap;

use coven_protocol::circle::{
    CircleInfo, CircleRole, CircleTransitionDraft, PreparedCircleControl,
};
use coven_protocol::circle_test_fixtures::{
    exact_logical_object, merge_device_authority, merge_membership_ref,
};
use coven_protocol::store_commit::ObjectHash;
use coven_protocol::{membership, store_commit};

#[tokio::test]
async fn control_history_caches_the_verified_access_owner_and_rejects_second_genesis() {
    let author = coven_keys::keys::UserKeypair::generate();
    let author_pubkey = coven_keys::keys::public_key_hex(&author);
    let earlier_owner = loop {
        let candidate = coven_keys::keys::UserKeypair::generate();
        if coven_keys::keys::public_key_hex(&candidate) < author_pubkey {
            break candidate;
        }
    };
    let earlier_owner_pubkey = coven_keys::keys::public_key_hex(&earlier_owner);
    let members = vec![
        (author_pubkey.clone(), membership::MemberRole::Owner),
        (earlier_owner_pubkey.clone(), membership::MemberRole::Owner),
    ];
    let store_root_hash = ObjectHash::digest(b"multi-owner-store-root");
    let (membership, membership_authority) =
        merge_membership_ref(&author, &members, "multi-owner-control");
    let device = merge_device_authority(&author, store_root_hash, "multi-owner-device");
    let ids = coven_foundation::id_provider::SequentialIdProvider::new("multi-owner-control");
    let operation_id =
        coven_protocol::write::WriteId::from_generated("multi-owner-control-commit".to_string());
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
    let control_author_pubkey = control.author_pubkey.clone();
    let active_epoch = control
        .body_mut()
        .value
        .state
        .active_epoch_mut()
        .expect("test control has an active epoch");
    active_epoch.common.owners = vec![earlier_owner_pubkey, author_pubkey.clone()];
    active_epoch.common.owners.sort();
    assert_ne!(active_epoch.common.owners[0], control_author_pubkey);
    control.resign(&author);
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
            circle_controls: vec![reference.clone()],
            ..store_commit::StoreCommitOperationsInput::empty()
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
    let verified = coven_protocol::circle_activation::VerifiedCircleReference {
        reference,
        circle_id: creation.circle_id,
        control: control.clone(),
        local_access: Some(coven_protocol::circle_activation::VerifiedCircleAccess {
            envelope: own_access.envelope.clone(),
            leaf: own_access.leaf.clone(),
            active: Some(coven_protocol::circle_activation::VerifiedCircleActive {
                roster: creation.roster.clone(),
                metadata: creation.metadata.clone(),
            }),
        }),
    };
    let db = crate::synthetic_store::open_test_db();
    let store_database = crate::StoreDatabase::new(&db.database);
    let first_commit = verified_commit.clone();
    db.database
        .record_verified_circle_activations_for_test(first_commit, vec![verified])
        .await
        .expect("record multi-Owner control");
    let cached_owner = db
        .database
        .circle_access_owner_for_test(creation.circle_id)
        .await
        .expect("read cached access owner");
    assert_eq!(cached_owner, author_pubkey);
    db.database
        .clear_circle_access_cache_for_test()
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
        .body_mut()
        .value
        .state
        .active_epoch_mut()
        .expect("test control has an active epoch");
    active_epoch.common.access_root = ObjectHash::digest(b"different founder access root");
    second_value.resign(&author);
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
        coven_protocol::write::WriteId::from_generated("second-founder-control-commit".to_string()),
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
            circle_controls: vec![second_reference.clone()],
            ..store_commit::StoreCommitOperationsInput::empty()
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
        .database
        .record_verified_circle_activations_for_test(
            second_commit,
            vec![coven_protocol::circle_activation::VerifiedCircleReference {
                reference: second_reference,
                circle_id: creation.circle_id,
                control: second_control,
                local_access: None,
            }],
        )
        .await
        .expect_err("a Circle cannot accept a second founder control");
    assert!(
        error.to_string().contains("already has a founder"),
        "{error}"
    );
}
