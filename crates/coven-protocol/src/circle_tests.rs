use super::*;
use crate::circle_test_fixtures::merge_membership_ref;
use crate::{membership, store_commit};
use coven_keys::keys::{self, UserKeypair};

fn candidate_family(label: &str) -> store_commit::CandidateFamilyId {
    store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(label.as_bytes()))
}

#[test]
fn founder_payload_is_complete_and_acyclic() {
    let owner = coven_keys::keys::UserKeypair::generate();
    let peer = coven_keys::keys::UserKeypair::generate();
    let owner_pubkey = coven_keys::keys::public_key_hex(&owner);
    let peer_pubkey = coven_keys::keys::public_key_hex(&peer);
    let members = vec![
        (owner_pubkey.clone(), membership::MemberRole::Owner),
        (peer_pubkey.clone(), membership::MemberRole::Member),
    ];

    let (membership, membership_authority) =
        merge_membership_ref(&owner, &members, "founder-circle-merge");
    let ids = coven_foundation::id_provider::SequentialIdProvider::new("founder-circle");
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
    let map = &creation.control.value.value.access;
    assert_eq!(map.len(), 2);
    for access in &creation.access {
        assert!(access.verify(&creation.control, candidate_family));
        assert_eq!(
            map.entry(&access.value.recipient_slot),
            Some(&access.entry())
        );
        let sealed = hex::decode(&map.entry(&access.value.recipient_slot).unwrap().sealed)
            .expect("access entry is hex");
        assert!(!sealed.windows(64).any(|window| {
            window == creation.control.coord.control_hash().to_string().as_bytes()
        }));
    }
    assert!(matches!(
        creation
            .access
            .iter()
            .find(|access| access.value.recipient_pubkey == owner_pubkey)
            .unwrap()
            .value
            .disposition,
        CircleAccessDisposition::Active { .. }
    ));
    assert!(matches!(
        creation
            .access
            .iter()
            .find(|access| access.value.recipient_pubkey == peer_pubkey)
            .unwrap()
            .value
            .disposition,
        CircleAccessDisposition::Inactive
    ));

    let mut seized = creation.control.value.clone();
    seized.body_mut().circle_id = CircleId::from_bytes([0x5a; 16]);
    seized.resign(&owner);
    assert!(
        !seized.verify(),
        "a founder control must not choose an arbitrary Circle ID"
    );

    let mut discontinuous = creation.control.value.clone();
    discontinuous.body_mut().value.order.seq = 2;
    discontinuous.resign(&owner);
    assert!(
        !discontinuous.verify(),
        "a control without a predecessor must be genesis"
    );
}

#[test]
fn access_verification_rejects_signed_context_and_entry_substitution() {
    let owner = coven_keys::keys::UserKeypair::generate();
    let peer = coven_keys::keys::UserKeypair::generate();
    let owner_pubkey = coven_keys::keys::public_key_hex(&owner);
    let peer_pubkey = coven_keys::keys::public_key_hex(&peer);
    let members = vec![
        (owner_pubkey.clone(), membership::MemberRole::Owner),
        (peer_pubkey.clone(), membership::MemberRole::Member),
    ];
    let (membership, authority) = merge_membership_ref(&owner, &members, "access-verification");
    let ids = coven_foundation::id_provider::SequentialIdProvider::new("access-verification");
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

    let mut wrong_family_leaf = creation.access[0].value.clone();
    wrong_family_leaf.body_mut().candidate_family =
        store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(b"other leaf family"));
    wrong_family_leaf.resign(&owner);
    let wrong_family_leaf = PreparedAccessLeaf::seal(wrong_family_leaf).expect("seal forged leaf");
    assert!(!wrong_family_leaf.verify(&creation.control, candidate_family));

    let mut wrong_membership_leaf = creation.access[0].value.clone();
    wrong_membership_leaf.body_mut().store_membership =
        merge_membership_ref(&owner, &members, "wrong-membership-leaf").0;
    wrong_membership_leaf.resign(&owner);
    let wrong_membership_leaf =
        PreparedAccessLeaf::seal(wrong_membership_leaf).expect("seal forged leaf");
    assert!(!wrong_membership_leaf.verify(&creation.control, candidate_family));

    let mut wrong_keyring_leaf = creation
        .access
        .iter()
        .find(|access| {
            matches!(
                &access.value.disposition,
                CircleAccessDisposition::Active { .. }
            )
        })
        .expect("founder access")
        .value
        .clone();
    let CircleAccessDisposition::Active { keyring, .. } =
        &mut wrong_keyring_leaf.body_mut().disposition
    else {
        panic!("founder access must be active")
    };
    *keyring = coven_keys::encryption::MasterKeyring::generate().to_serialized();
    wrong_keyring_leaf.resign(&owner);
    let wrong_keyring_leaf =
        PreparedAccessLeaf::seal(wrong_keyring_leaf).expect("seal wrong-keyring leaf");
    assert!(!wrong_keyring_leaf.verify(&creation.control, candidate_family));

    // Sealing is randomized, so re-sealing the same signed leaf produces bytes
    // the control's entry does not name.
    let resealed =
        PreparedAccessLeaf::seal(creation.access[0].value.clone()).expect("re-seal own leaf");
    assert_ne!(resealed.bytes, creation.access[0].bytes);
    assert!(!resealed.verify(&creation.control, candidate_family));

    let mut other_slot = creation.access[0].value.clone();
    other_slot.body_mut().recipient_slot = creation.access[1].value.recipient_slot.clone();
    other_slot.resign(&owner);
    let other_slot = PreparedAccessLeaf {
        bytes: creation.access[0].bytes.clone(),
        value: other_slot,
    };
    assert!(!other_slot.verify(&creation.control, candidate_family));

    let mut duplicated = creation.access.clone();
    duplicated[1].value.body_mut().recipient_slot = duplicated[0].value.recipient_slot.clone();
    assert_eq!(
        CircleAccessMap::from_leaves(&duplicated),
        Err(CircleTransitionError::InvalidCurrentState)
    );

    let mut deleted_with_access = creation.control.value.clone();
    deleted_with_access.body_mut().value.state =
        CircleControlState::Deleted(crate::circle::DeletedCircle {
            frozen_epoch: creation.control.value.access_epoch().clone(),
        });
    deleted_with_access.resign(&owner);
    assert!(!deleted_with_access.verify());

    let mut active_without_access = creation.control.value.clone();
    active_without_access.body_mut().value.access = CircleAccessMap::empty();
    active_without_access.resign(&owner);
    assert!(!active_without_access.verify());
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
