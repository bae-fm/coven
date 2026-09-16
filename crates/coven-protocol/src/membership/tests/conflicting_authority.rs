use super::*;

#[test]
fn concurrent_member_assignments_are_rejected_as_conflicting_authority() {
    let owner = key();
    let target = key();
    let target_pubkey = keys::public_key_hex(&target);
    let mut chain = founded("store", &owner);
    let member = chain
        .signed_set_member_in_stream(
            &owner,
            stream(1),
            target_pubkey.clone(),
            None,
            MemberRole::Member,
            "initial Member".to_string(),
        )
        .unwrap();
    chain.add_entry(member).unwrap();
    let first = chain
        .signed_set_member_in_stream(
            &owner,
            stream(21),
            target_pubkey.clone(),
            None,
            MemberRole::Follower,
            "first".to_string(),
        )
        .unwrap();
    let second = chain
        .signed_promote_member_in_stream_for_test(
            &owner,
            stream(22),
            target_pubkey.clone(),
            "second".to_string(),
        )
        .unwrap();
    let mut entries = chain.entries().to_vec();
    entries.extend([first.clone(), second.clone()]);
    let heads = entries
        .iter()
        .filter(|entry| {
            !entries.iter().any(|candidate| {
                candidate
                    .dependencies
                    .iter()
                    .any(|dependency| dependency == &entry.coord())
                    && candidate.stream_id == entry.stream_id
            })
        })
        .map(|entry| exact_head(entry, &owner))
        .collect();

    assert!(matches!(
        MembershipChain::from_test_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            heads,
        ),
        Err(MembershipError::Conflict)
    ));

    chain.add_entry(first.clone()).unwrap();
    chain
        .activate_head_ref(exact_head(&first, &owner).0)
        .unwrap();
    let retained = chain.clone();
    assert!(matches!(
        chain.add_entry_at(second.coord(), second),
        Err(MembershipError::Conflict)
    ));
    assert_eq!(chain.entries(), retained.entries());
    assert_eq!(chain.coords, retained.coords);
    assert_eq!(chain.included, retained.included);
    assert_eq!(chain.head_refs(), retained.head_refs());
    assert_eq!(chain.resolved(), retained.resolved());
}

#[test]
fn concurrent_cross_revocation_is_rejected_as_conflicting_authority() {
    let first_owner = key();
    let second_owner = key();
    let first_pubkey = keys::public_key_hex(&first_owner);
    let second_pubkey = keys::public_key_hex(&second_owner);
    let mut base = founded("store", &first_owner);
    base.add_owner_for_test(
        &first_owner,
        stream(1),
        second_pubkey.clone(),
        "add second".to_string(),
    )
    .unwrap();
    let remove_second = base
        .signed_remove_member_in_stream(
            &first_owner,
            stream(1),
            second_pubkey.clone(),
            "remove second".to_string(),
        )
        .unwrap();
    let remove_first = base
        .signed_remove_member_in_stream(
            &second_owner,
            stream(23),
            first_pubkey.clone(),
            "remove first".to_string(),
        )
        .unwrap();
    let mut entries = base.entries().to_vec();
    entries.extend([remove_second.clone(), remove_first.clone()]);
    let heads = vec![
        exact_head(
            base.entries().first().expect("founder membership entry"),
            &first_owner,
        ),
        exact_head(&remove_second, &first_owner),
        exact_head(&remove_first, &second_owner),
    ];

    assert!(matches!(
        MembershipChain::from_test_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            heads,
        ),
        Err(MembershipError::Conflict)
    ));
}
