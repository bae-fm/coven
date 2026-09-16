use super::*;

#[test]
fn dependency_frontier_must_be_strictly_ordered_by_author_stream() {
    let founder = key();
    let second_owner = key();
    let mut chain = founded("store", &founder);
    chain
        .add_owner_for_test(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "add owner".to_string(),
        )
        .unwrap();
    let second_stream = chain
        .signed_set_member_in_stream(
            &second_owner,
            stream(31),
            keys::public_key_hex(&key()),
            None,
            MemberRole::Member,
            "second stream".to_string(),
        )
        .unwrap();
    chain.add_entry(second_stream).unwrap();
    let mut unsorted = chain
        .signed_set_member_in_stream(
            &founder,
            stream(1),
            keys::public_key_hex(&key()),
            None,
            MemberRole::Member,
            "candidate".to_string(),
        )
        .unwrap();
    assert!(unsorted.dependencies.len() > 1);
    unsorted.body_mut().dependencies.reverse();
    unsorted.resign(&founder);

    assert!(matches!(
        chain.add_entry(unsorted),
        Err(MembershipError::NonCanonicalDependencyFrontier { .. })
    ));
}

#[test]
fn owner_barrier_must_be_strictly_ordered_by_author_stream() {
    let founder = key();
    let second_owner = key();
    let second_owner_pubkey = keys::public_key_hex(&second_owner);
    let mut chain = founded("store", &founder);
    chain
        .add_owner_for_test(
            &founder,
            stream(1),
            second_owner_pubkey.clone(),
            "add owner".to_string(),
        )
        .unwrap();
    for (stream_id, timestamp) in [(stream(41), "first stream"), (stream(42), "second stream")] {
        let authored = chain
            .signed_set_member_in_stream(
                &second_owner,
                stream_id,
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                timestamp.to_string(),
            )
            .unwrap();
        chain.add_entry(authored).unwrap();
    }
    let mut removal = chain
        .signed_remove_member_in_stream(
            &founder,
            stream(1),
            second_owner_pubkey,
            "remove owner".to_string(),
        )
        .unwrap();
    let StoreAuthorityChange::RemoveMember {
        retirement_barriers,
        ..
    } = &mut removal.body_mut().change
    else {
        unreachable!();
    };
    let observed = &mut retirement_barriers
        .values_mut()
        .next()
        .expect("owner removal barrier")
        .author_streams()
        .observed_streams
        .clone();
    assert!(observed.len() > 1);
    let barrier = retirement_barriers
        .values_mut()
        .next()
        .expect("owner removal barrier");
    match barrier {
        MergeMembershipGrantRetirementBarrier::Owner { barrier } => {
            barrier.author_streams.observed_streams.reverse();
        }
        MergeMembershipGrantRetirementBarrier::NonOwner { .. } => {
            panic!("Owner removal carries non-Owner barrier")
        }
    }
    removal.resign(&founder);

    assert!(matches!(
        chain.add_entry(removal),
        Err(MembershipError::InvalidOwnerRevocationBarrier { .. })
    ));
}

#[test]
fn owner_readd_uses_a_new_sequence_one_stream() {
    let owner = key();
    let second = key();
    let mut chain = founded("store", &owner);
    chain
        .add_owner_for_test(
            &owner,
            stream(1),
            keys::public_key_hex(&second),
            "add".to_string(),
        )
        .unwrap();
    let old_grant = chain
        .active_owner_grant(&keys::public_key_hex(&second))
        .unwrap();
    let remove = chain
        .signed_remove_member_in_stream(
            &owner,
            stream(1),
            keys::public_key_hex(&second),
            "remove".to_string(),
        )
        .unwrap();
    chain.add_entry(remove).unwrap();
    chain
        .add_owner_for_test(
            &owner,
            stream(1),
            keys::public_key_hex(&second),
            "readd".to_string(),
        )
        .unwrap();
    let new_grant = chain
        .active_owner_grant(&keys::public_key_hex(&second))
        .unwrap();
    assert_ne!(old_grant, new_grant);
    let authored = chain
        .signed_set_member_in_stream(
            &second,
            stream(32),
            keys::public_key_hex(&key()),
            None,
            MemberRole::Member,
            "authored".to_string(),
        )
        .unwrap();
    assert_eq!(authored.seq, 1);
    assert_eq!(authored.author_owner_grant, new_grant);
}

#[test]
fn owner_self_removal_remains_effective_when_its_grant_is_capped_before_first() {
    let founder = key();
    let departing_owner = key();
    let departing_pubkey = keys::public_key_hex(&departing_owner);
    let mut chain = founded("store", &founder);
    chain
        .add_owner_for_test(
            &founder,
            stream(1),
            departing_pubkey.clone(),
            "add owner".to_string(),
        )
        .unwrap();

    let self_removal = chain
        .signed_remove_member_in_stream(
            &departing_owner,
            stream(33),
            departing_pubkey.clone(),
            "self removal".to_string(),
        )
        .unwrap();
    assert!(matches!(
        &self_removal.change,
        StoreAuthorityChange::RemoveMember { retirement_barriers, .. }
            if retirement_barriers.values().all(|barrier| barrier.author_streams().observed_streams.is_empty())
    ));
    chain.add_entry(self_removal).unwrap();

    assert!(!chain.is_owner_now(&departing_pubkey));
}

#[test]
fn before_first_barrier_excludes_every_entry_from_the_revoked_owner_stream() {
    let founder = key();
    let second_owner = key();
    let target = key();
    let mut observed = founded("store", &founder);
    observed
        .add_owner_for_test(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "add owner".to_string(),
        )
        .unwrap();

    let stale_entry = observed
        .signed_set_member_in_stream(
            &second_owner,
            stream(34),
            keys::public_key_hex(&target),
            None,
            MemberRole::Member,
            "stale entry".to_string(),
        )
        .unwrap();
    let removal = observed
        .signed_remove_member_in_stream(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "remove owner".to_string(),
        )
        .unwrap();
    assert!(matches!(
        &removal.change,
        StoreAuthorityChange::RemoveMember { retirement_barriers, .. }
            if retirement_barriers.values().all(|barrier| barrier.author_streams().observed_streams.is_empty())
    ));

    let mut entries = observed.entries().to_vec();
    let stale_coord = stale_entry.coord();
    entries.extend([removal, stale_entry]);
    let chain = MembershipChain::from_entries(entries).unwrap();
    assert!(chain.contains_coord(&stale_coord));
    assert!(!MembershipCausalFloor {
        effective_coordinates: vec![stale_coord],
    }
    .is_included_in(&chain));
    assert!(!chain.can_write_now(&keys::public_key_hex(&target)));
    assert!(chain
        .author_heads()
        .iter()
        .any(|coord| coord.author_pubkey == keys::public_key_hex(&second_owner)));
    assert!(chain
        .effective_frontier()
        .iter()
        .all(|coord| coord.author_pubkey != keys::public_key_hex(&second_owner)));
}

#[test]
fn through_barrier_keeps_its_exact_prefix_and_prunes_the_stale_suffix() {
    let founder = key();
    let second_owner = key();
    let first_target = key();
    let second_target = key();
    let third_target = key();
    let mut observed = founded("store", &founder);
    observed
        .add_owner_for_test(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "add owner".to_string(),
        )
        .unwrap();
    let first = observed
        .signed_set_member_in_stream(
            &second_owner,
            stream(35),
            keys::public_key_hex(&first_target),
            None,
            MemberRole::Member,
            "first".to_string(),
        )
        .unwrap();
    observed.add_entry(first.clone()).unwrap();

    let removal = observed
        .signed_remove_member_in_stream(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "remove owner".to_string(),
        )
        .unwrap();
    assert!(matches!(
        &removal.change,
        StoreAuthorityChange::RemoveMember { retirement_barriers, .. }
            if retirement_barriers.values().any(|barrier| barrier.author_streams().observed_streams == vec![first.coord()])
    ));

    let second = observed
        .signed_set_member_in_stream(
            &second_owner,
            stream(35),
            keys::public_key_hex(&second_target),
            None,
            MemberRole::Member,
            "second".to_string(),
        )
        .unwrap();
    let mut exact_entries = observed.entries().to_vec();
    exact_entries.extend([removal.clone(), second.clone()]);
    let exact = MembershipChain::from_entries(exact_entries).unwrap();
    assert!(exact.can_write_now(&keys::public_key_hex(&first_target)));
    assert!(!exact.can_write_now(&keys::public_key_hex(&second_target)));

    let mut stale = observed;
    stale.add_entry(second).unwrap();
    let third = stale
        .signed_set_member_in_stream(
            &second_owner,
            stream(35),
            keys::public_key_hex(&third_target),
            None,
            MemberRole::Member,
            "third".to_string(),
        )
        .unwrap();
    stale.add_entry(third.clone()).unwrap();
    let mut beyond_entries = stale.entries().to_vec();
    beyond_entries.push(removal);
    let pruned = MembershipChain::from_entries(beyond_entries).unwrap();
    assert!(pruned.can_write_now(&keys::public_key_hex(&first_target)));
    assert!(!pruned.can_write_now(&keys::public_key_hex(&second_target)));
    assert!(!pruned.can_write_now(&keys::public_key_hex(&third_target)));
}

#[test]
fn through_barrier_rejects_a_coordinate_hash_that_is_not_its_dependency() {
    let founder = key();
    let second_owner = key();
    let mut chain = founded("store", &founder);
    chain
        .add_owner_for_test(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "add owner".to_string(),
        )
        .unwrap();
    let authored = chain
        .signed_set_member_in_stream(
            &second_owner,
            stream(36),
            keys::public_key_hex(&key()),
            None,
            MemberRole::Member,
            "authored".to_string(),
        )
        .unwrap();
    chain.add_entry(authored).unwrap();
    let mut removal = chain
        .signed_remove_member_in_stream(
            &founder,
            stream(1),
            keys::public_key_hex(&second_owner),
            "remove owner".to_string(),
        )
        .unwrap();
    let StoreAuthorityChange::RemoveMember {
        retirement_barriers,
        ..
    } = &mut removal.body_mut().change
    else {
        unreachable!();
    };
    let barrier = retirement_barriers
        .values_mut()
        .next()
        .expect("owner removal barrier");
    let MergeMembershipGrantRetirementBarrier::Owner { barrier } = barrier else {
        panic!("Owner removal carries non-Owner barrier")
    };
    let barrier = barrier
        .author_streams
        .observed_streams
        .first_mut()
        .expect("observed owner stream");
    barrier.entry_hash = ObjectHash::digest(b"wrong barrier hash");
    removal.resign(&founder);
    assert!(matches!(
        chain.add_entry(removal),
        Err(MembershipError::InvalidOwnerRevocationBarrier { .. })
    ));
}
