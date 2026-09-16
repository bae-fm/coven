use super::*;

#[test]
fn membership_removals_require_exact_sealed_key_recipient_coverage() {
    let owner = key();
    let member = key();
    let owner_pubkey = keys::public_key_hex(&owner);
    let member_pubkey = keys::public_key_hex(&member);
    let mut chain = founded("store", &owner);
    let add = chain
        .signed_set_member_in_stream(
            &owner,
            stream(1),
            member_pubkey.clone(),
            None,
            MemberRole::Member,
            "add member".to_string(),
        )
        .unwrap();
    chain.add_entry(add).unwrap();

    assert!(matches!(
        chain.signed_remove_member_with_sealed_keys_in_stream(
            &owner,
            stream(1),
            member_pubkey.clone(),
            BTreeMap::new(),
            "missing owner key".to_string(),
        ),
        Err(MembershipError::InvalidSealedKeys(_))
    ));
    assert!(matches!(
        chain.signed_remove_member_with_sealed_keys_in_stream(
            &owner,
            stream(1),
            member_pubkey.clone(),
            BTreeMap::from([
                (owner_pubkey, test_sealed_store_key(b"owner rotation")),
                (
                    member_pubkey.clone(),
                    test_sealed_store_key(b"removed member rotation")
                ),
            ]),
            "sealing to the removed member".to_string(),
        ),
        Err(MembershipError::InvalidSealedKeys(_))
    ));
}

#[test]
fn rotation_generations_follow_the_causal_membership_history() {
    let owner = key();
    let first_member = key();
    let second_member = key();
    let owner_pubkey = keys::public_key_hex(&owner);
    let first_pubkey = keys::public_key_hex(&first_member);
    let second_pubkey = keys::public_key_hex(&second_member);
    let mut chain = founded("rotation-generation-history", &owner);
    for member in [&first_pubkey, &second_pubkey] {
        let add = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                member.clone(),
                None,
                MemberRole::Member,
                format!("add {member}"),
            )
            .unwrap();
        chain.add_entry(add).unwrap();
    }
    let first_rotation = chain
        .signed_remove_member_with_sealed_keys_in_stream(
            &owner,
            stream(1),
            first_pubkey,
            BTreeMap::from([
                (
                    owner_pubkey.clone(),
                    test_sealed_store_key(b"first owner rotation"),
                ),
                (
                    second_pubkey.clone(),
                    test_sealed_store_key(b"first member rotation"),
                ),
            ]),
            "first rotation".to_string(),
        )
        .unwrap();
    let StoreAuthorityChange::RemoveMember {
        rotation_generation,
        ..
    } = &first_rotation.change
    else {
        panic!("the authored change removes a member");
    };
    assert_eq!(*rotation_generation, 2);
    chain.add_entry(first_rotation).unwrap();

    // A removal asserts which rotation it performs, so a second removal that
    // reuses the generation the first one established is refused.
    let removes = chain.active_grant_ids(&second_pubkey);
    let retirement_barriers = chain
        .membership_retirement_barriers(&removes, None)
        .unwrap();
    assert!(matches!(
        chain.signed_change_in_stream(
            &owner,
            stream(1),
            StoreAuthorityChange::RemoveMember {
                user_pubkey: second_pubkey,
                removes,
                retirement_barriers,
                retirement_device_state: None,
                rotation_generation: 2,
                sealed_keys: BTreeMap::from([(
                    owner_pubkey,
                    test_sealed_store_key(b"reused rotation generation"),
                )]),
            },
            "reused rotation generation".to_string(),
        ),
        Err(MembershipError::InvalidSealedKeys(_))
    ));
}

#[test]
fn concurrent_add_and_rotation_has_incomplete_sealed_key_authority() {
    let owner = key();
    let removed = key();
    let concurrent_member = key();
    let owner_pubkey = keys::public_key_hex(&owner);
    let removed_pubkey = keys::public_key_hex(&removed);
    let concurrent_pubkey = keys::public_key_hex(&concurrent_member);
    let mut chain = founded("concurrent-add-rotation", &owner);
    let add_removed = chain
        .signed_set_member_in_stream(
            &owner,
            stream(1),
            removed_pubkey.clone(),
            None,
            MemberRole::Member,
            "add member that will be removed".to_string(),
        )
        .unwrap();
    chain.add_entry(add_removed).unwrap();

    let add_concurrent = chain
        .signed_set_member_in_stream(
            &owner,
            stream(2),
            concurrent_pubkey.clone(),
            None,
            MemberRole::Member,
            "concurrent add".to_string(),
        )
        .unwrap();
    let remove = chain
        .signed_remove_member_with_sealed_keys_in_stream(
            &owner,
            stream(3),
            removed_pubkey,
            BTreeMap::from([(
                owner_pubkey,
                test_sealed_store_key(b"rotation missing concurrent member"),
            )]),
            "concurrent removal".to_string(),
        )
        .unwrap();
    chain.add_entry(add_concurrent).unwrap();
    chain.add_entry(remove).unwrap();

    assert!(matches!(
        chain.sealed_key_authority_for(&concurrent_pubkey),
        Err(MembershipError::MissingSealedKeyCoverage { .. })
    ));

    let replacement_key = test_sealed_store_key(b"post-rotation replacement invitation");
    let replacement = chain
        .signed_set_member_with_anchor_and_sealed_key_in_stream(
            &owner,
            stream(4),
            concurrent_pubkey.clone(),
            None,
            MemberRole::Member,
            None,
            replacement_key.clone(),
            "replace concurrent invitation after rotation".to_string(),
        )
        .unwrap();
    let replacement_coord = replacement.coord();
    chain.add_entry(replacement).unwrap();
    assert_eq!(
        chain.sealed_key_authority_for(&concurrent_pubkey).unwrap(),
        vec![ActivatedSealedKey {
            coord: replacement_coord,
            generation: 2,
            key: replacement_key,
        }],
    );
}
