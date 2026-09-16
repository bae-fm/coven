use super::*;

#[test]
fn provider_administration_resolves_to_the_root_administrator_without_a_transfer() {
    let owner = key();
    let chain = founded("store", &owner);
    let root_administrator =
        test_root_administrator(chain.entries()).expect("test root administrator");

    assert_eq!(chain.provider_administrator(), &root_administrator);
}

#[test]
fn the_latest_transfer_overrides_the_root_administrator() {
    let owner = key();
    let mut chain = founded("store", &owner);
    let root = test_root("store");
    let (_, first_target) = registration(&root, "store-first-target", &key());
    let (_, second_target) = registration(&root, "store-second-target", &key());
    let root_administrator =
        test_root_administrator(chain.entries()).expect("test root administrator");
    let root_state = chain.resolved().state_hash;

    let first = chain
        .signed_provider_administration_transfer_in_stream(
            &owner,
            stream(1),
            first_target.clone(),
            "first transfer".to_string(),
        )
        .unwrap();
    chain.add_entry(first).unwrap();
    assert_eq!(chain.provider_administrator(), &first_target);
    let first_state = chain.resolved().state_hash;
    assert_ne!(first_state, root_state);

    let second = chain
        .signed_provider_administration_transfer_in_stream(
            &owner,
            stream(1),
            second_target.clone(),
            "second transfer".to_string(),
        )
        .unwrap();
    chain.add_entry(second).unwrap();

    assert_eq!(chain.provider_administrator(), &second_target);
    assert_ne!(chain.resolved().state_hash, first_state);
    assert_ne!(chain.provider_administrator(), &root_administrator);
}

#[test]
fn concurrent_transfers_are_refused_rather_than_ordered() {
    let owner = key();
    let mut chain = founded("store", &owner);
    let root = test_root("store");
    let (_, first_target) = registration(&root, "store-concurrent-first", &key());
    let (_, second_target) = registration(&root, "store-concurrent-second", &key());
    let first = chain
        .signed_provider_administration_transfer_in_stream(
            &owner,
            stream(21),
            first_target,
            "first".to_string(),
        )
        .unwrap();
    let second = chain
        .signed_provider_administration_transfer_in_stream(
            &owner,
            stream(22),
            second_target,
            "second".to_string(),
        )
        .unwrap();
    chain.add_entry(first).unwrap();
    let retained = chain.clone();

    assert!(matches!(
        chain.add_entry(second),
        Err(MembershipError::ConcurrentProviderAdministrationTransfer)
    ));
    assert_eq!(chain.resolved(), retained.resolved());
}

#[test]
fn a_transfer_consumes_the_authority_it_was_prepared_against() {
    let owner = key();
    let member = key();
    let mut chain = founded("store", &owner);
    let root = test_root("store");
    let (_, target) = registration(&root, "store-stale-target", &key());
    let transfer = chain
        .signed_provider_administration_transfer_in_stream(
            &owner,
            stream(1),
            target,
            "prepared before the grant".to_string(),
        )
        .unwrap();
    let grant = chain
        .signed_set_member_in_stream(
            &owner,
            stream(2),
            keys::public_key_hex(&member),
            None,
            MemberRole::Member,
            "accepted first".to_string(),
        )
        .unwrap();
    chain.add_entry(grant).unwrap();

    assert!(matches!(
        chain.validate_publication_predecessor(&transfer),
        Err(MembershipError::PublicationPredecessorChanged { .. })
    ));
}
