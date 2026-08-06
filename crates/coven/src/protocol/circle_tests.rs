use super::*;
use crate::protocol::circle_control::{merkle_root_and_proofs, verify_merkle_proof};
use crate::protocol::circle_test_fixtures::merge_membership_ref;
use crate::protocol::{membership, store_commit};
use coven_keys::keys::{self, UserKeypair};

fn candidate_family(label: &str) -> store_commit::CandidateFamilyId {
    store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(label.as_bytes()))
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
fn access_verification_rejects_signed_context_and_proof_substitution() {
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

    let mut wrong_store = creation.access[0].envelope.clone();
    wrong_store.body_mut().store_root_hash = ObjectHash::digest(b"other-store");
    wrong_store.resign(&owner);
    assert!(!wrong_store.verify(&creation.control, candidate_family));

    let mut wrong_family_envelope = creation.access[0].envelope.clone();
    wrong_family_envelope.body_mut().candidate_family =
        store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(b"other access family"));
    wrong_family_envelope.resign(&owner);
    assert!(!wrong_family_envelope.verify(&creation.control, candidate_family));

    let mut wrong_family_leaf = creation.access[0].leaf.clone();
    wrong_family_leaf.value.body_mut().candidate_family =
        store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(b"other leaf family"));
    wrong_family_leaf.value.resign(&owner);
    assert!(!wrong_family_leaf.verify(&creation.control, candidate_family));

    let mut non_owner = creation.access[0].envelope.clone();
    non_owner.body_mut().owner_pubkey = peer_pubkey;
    non_owner.resign(&peer);
    assert!(!non_owner.verify(&creation.control, candidate_family));

    let mut substituted_proof = creation.access[0].envelope.clone();
    substituted_proof.body_mut().proof = creation.access[1].envelope.proof.clone();
    substituted_proof.resign(&owner);
    assert!(!substituted_proof.verify(&creation.control, candidate_family));

    let mut substituted_leaf_id = creation.access[0].envelope.clone();
    substituted_leaf_id.body_mut().leaf_id = creation.access[1].leaf.value.leaf_id;
    substituted_leaf_id.resign(&owner);
    assert!(substituted_leaf_id.verify(&creation.control, candidate_family));
    assert!(!creation.access[0].leaf.verify_envelope(
        &creation.control,
        &substituted_leaf_id,
        candidate_family,
    ));

    let mut wrong_membership_leaf = creation.access[0].leaf.value.clone();
    wrong_membership_leaf.body_mut().store_membership =
        merge_membership_ref(&owner, &members, "wrong-membership-leaf").0;
    wrong_membership_leaf.resign(&owner);
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
    let CircleAccessDisposition::Active { keyring, .. } =
        &mut wrong_keyring_leaf.body_mut().disposition
    else {
        panic!("founder access must be active")
    };
    *keyring = coven_keys::encryption::MasterKeyring::generate().to_serialized();
    wrong_keyring_leaf.resign(&owner);
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
