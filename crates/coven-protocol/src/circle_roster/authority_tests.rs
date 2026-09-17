use super::reduction::*;
use super::*;
use coven_keys::keys::UserKeypair;

fn grant(label: &[u8]) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(label))
}

#[test]
fn roster_sequence_exhaustion_fails_instead_of_reusing_the_last_sequence() {
    let owner = UserKeypair::generate();
    let owner_pubkey = keys::public_key_hex(&owner);
    let owner_grant = grant(b"sequence-exhaustion-owner-grant");
    let store_root_hash = ObjectHash::digest(b"sequence-exhaustion-store");
    let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &owner_grant);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "owner-device",
        owner_grant,
        &owner,
    );
    let mut terminal = founder.clone();
    terminal.body_mut().seq = u64::MAX;
    terminal.body_mut().previous_hash = Some(founder.entry_hash());
    terminal.resign(&owner);
    let terminal_coord = terminal.coord();
    let stream = terminal_coord.stream_key();
    let mut chain = CircleRosterChain::from_entries(vec![founder]).expect("founder roster");
    chain.entries.push(terminal);
    chain
        .reduced
        .as_mut()
        .expect("resolved founder roster")
        .included
        .insert(terminal_coord);

    assert!(matches!(
        chain.next_position(&stream),
        Err(CircleRosterError::SequenceExhausted { current: u64::MAX })
    ));
}

struct ThreeOwnerCycle {
    third: UserKeypair,
    chain: CircleRosterChain,
    removals: Vec<CircleRosterCoord>,
    revoked_owner_grants: BTreeSet<MembershipGrantId>,
}

fn three_owner_cycle() -> ThreeOwnerCycle {
    let first = UserKeypair::generate();
    let second = UserKeypair::generate();
    let third = UserKeypair::generate();
    let first_pubkey = keys::public_key_hex(&first);
    let second_pubkey = keys::public_key_hex(&second);
    let third_pubkey = keys::public_key_hex(&third);
    let store_root_hash = ObjectHash::digest(b"three-owner Circle conflict Store");
    let founder_grant = grant(b"three-owner Circle founder grant");
    let circle_id = CircleId::founder(store_root_hash, &first_pubkey, &founder_grant);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "first-device",
        founder_grant.clone(),
        &first,
    );
    let mut base = vec![founder];
    let add_second = CircleRosterChain::from_entries(base.clone())
        .expect("founder roster")
        .signed_set_member(
            "first-device",
            second_pubkey.clone(),
            CircleRole::Owner,
            &first,
        )
        .expect("add second Owner");
    let CircleRosterChange::SetMember {
        grant_id: second_grant,
        ..
    } = &add_second.change
    else {
        panic!("adding an Owner creates a grant")
    };
    let second_grant = second_grant.clone();
    base.push(add_second);
    let add_third = CircleRosterChain::from_entries(base.clone())
        .expect("two-Owner roster")
        .signed_set_member("first-device", third_pubkey, CircleRole::Owner, &first)
        .expect("add third Owner");
    base.push(add_third);
    let remove_second = CircleRosterChain::from_entries(base.clone())
        .expect("three-Owner roster")
        .signed_remove_member("first-device", second_pubkey, &first)
        .expect("first branch");
    let remove_first = CircleRosterChain::from_entries(base.clone())
        .expect("three-Owner roster")
        .signed_remove_member("second-device", first_pubkey, &second)
        .expect("second branch");
    base.extend([remove_second.clone(), remove_first.clone()]);
    let mut removals = vec![remove_second.coord(), remove_first.coord()];
    removals.sort();
    let chain = CircleRosterChain::from_entries(base).expect("three-Owner revocation conflict");
    ThreeOwnerCycle {
        third,
        chain,
        removals,
        revoked_owner_grants: BTreeSet::from([founder_grant, second_grant]),
    }
}

#[test]
fn a_revocation_cycle_is_a_terminal_roster_conflict() {
    let ThreeOwnerCycle {
        third,
        chain,
        removals,
        revoked_owner_grants,
    } = three_owner_cycle();

    let CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
        raw_frontier,
        cyclic_sources,
        involved_owner_grants,
    }) = chain.status()
    else {
        panic!("concurrent Owner revocations are a revocation cycle")
    };
    // The conflict reports the raw author-stream frontier the branches reached,
    // which is what an Owner resolves the cycle against.
    assert_eq!(raw_frontier, &chain.author_heads());
    assert_eq!(raw_frontier.len(), 2);
    assert_eq!(cyclic_sources, &removals);
    assert_eq!(involved_owner_grants, &revoked_owner_grants);
    assert!(matches!(
        chain.try_resolved(),
        Err(CircleRosterError::Conflict)
    ));
    assert!(matches!(
        chain.signed_set_member(
            "third-device",
            keys::public_key_hex(&UserKeypair::generate()),
            CircleRole::Member,
            &third,
        ),
        Err(CircleRosterError::Conflict)
    ));
}

#[test]
fn historical_roster_authorizes_the_exact_grant_at_its_creation_coordinate() {
    let owner = UserKeypair::generate();
    let owner_pubkey = keys::public_key_hex(&owner);
    let owner_grant = grant(b"historical-owner-grant");
    let founder = CircleRosterEntry::founder(
        ObjectHash::digest(b"historical-authority-store"),
        CircleId::founder(
            ObjectHash::digest(b"historical-authority-store"),
            &owner_pubkey,
            &owner_grant,
        ),
        "owner-device",
        owner_grant.clone(),
        &owner,
    );
    let created_at = founder.coord();
    let roster = CircleRosterChain::from_entries(vec![founder])
        .expect("load founder roster")
        .resolved();

    assert!(roster.authorizes_owner_grant(&owner_pubkey, &owner_grant, &created_at,));
}

#[test]
fn removed_owner_grant_stays_unauthorized_after_the_identity_is_readded() {
    let first_owner = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let first_pubkey = keys::public_key_hex(&first_owner);
    let second_pubkey = keys::public_key_hex(&second_owner);
    let first_grant = grant(b"first-owner-grant");
    let store_root_hash = ObjectHash::digest(b"remove-readd-store");
    let circle_id = CircleId::founder(store_root_hash, &first_pubkey, &first_grant);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "first-device",
        first_grant.clone(),
        &first_owner,
    );
    let first_created_at = founder.coord();
    let mut entries = vec![founder];
    let add_second = CircleRosterChain::from_entries(entries.clone())
        .expect("load founder roster")
        .signed_set_member(
            "first-device",
            second_pubkey.clone(),
            CircleRole::Owner,
            &first_owner,
        )
        .expect("add second Owner");
    entries.push(add_second);
    let remove_first = CircleRosterChain::from_entries(entries.clone())
        .expect("load two-Owner roster")
        .signed_remove_member("second-device", first_pubkey.clone(), &second_owner)
        .expect("remove first Owner");
    let retirement_authority = remove_first.coord();
    let CircleRosterChange::RemoveMember { owner_barriers, .. } = &remove_first.change else {
        unreachable!()
    };
    let owner_barrier = owner_barriers[&first_grant].clone();
    entries.push(remove_first);
    let readd_first = CircleRosterChain::from_entries(entries.clone())
        .expect("load removed-Owner roster")
        .signed_set_member(
            "second-device",
            first_pubkey.clone(),
            CircleRole::Owner,
            &second_owner,
        )
        .expect("re-add first Owner");
    let replacement_grant = match &readd_first.change {
        CircleRosterChange::SetMember { grant_id, .. } => grant_id.clone(),
        _ => panic!("re-add must create a grant"),
    };
    let replacement_created_at = readd_first.coord();
    entries.push(readd_first);
    let roster = CircleRosterChain::from_entries(entries)
        .expect("load re-added roster")
        .resolved();

    assert!(!roster.authorizes_owner_grant(&first_pubkey, &first_grant, &first_created_at,));
    assert!(roster.authorizes_owner_grant(
        &first_pubkey,
        &replacement_grant,
        &replacement_created_at,
    ));
    assert!(matches!(
        &roster.grants[&first_grant],
        GrantState::Tombstoned { record, retirements }
            if record.member_pubkey == first_pubkey
                && retirements.as_set() == &BTreeSet::from([CircleGrantRetirement {
                    authority: retirement_authority.clone(),
                    owner_barrier: Some(owner_barrier.clone()),
                }])
    ));
    let mut altered = roster.grants.clone();
    let GrantState::Tombstoned { retirements, .. } = altered
        .get_mut(&first_grant)
        .expect("retired Circle grant remains present")
    else {
        unreachable!()
    };
    retirements.insert(CircleGrantRetirement {
        authority: CircleRosterCoord {
            entry_hash: ObjectHash::digest(b"different Circle retirement entry"),
            ..retirement_authority
        },
        owner_barrier: Some(owner_barrier),
    });
    assert_ne!(roster.state_hash, circle_roster_state_hash(&altered));
}

#[test]
fn roster_state_hash_changes_when_only_the_active_grant_identity_changes() {
    let owner = UserKeypair::generate();
    let owner_pubkey = keys::public_key_hex(&owner);
    let store_root_hash = ObjectHash::digest(b"grant-hash-store");
    let build = |grant_id: MembershipGrantId| {
        let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &grant_id);
        CircleRosterChain::from_entries(vec![CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "owner-device",
            grant_id,
            &owner,
        )])
        .expect("load founder roster")
        .resolved()
    };

    let first = build(grant(b"state-hash-grant-a"));
    let second = build(grant(b"state-hash-grant-b"));

    assert_ne!(first.state_hash, second.state_hash);
}

/// Two entries at one author-stream position are the equivocation the removed
/// create-once successor slot used to make impossible. The reduction refuses
/// them loudly rather than picking one, and it refuses identically wherever the
/// pair is assembled, so every device reaches the same answer.
#[test]
fn two_entries_at_one_author_stream_position_are_a_loud_reduction_failure() {
    let owner = UserKeypair::generate();
    let owner_pubkey = keys::public_key_hex(&owner);
    let member = keys::public_key_hex(&UserKeypair::generate());
    let other = keys::public_key_hex(&UserKeypair::generate());
    let owner_grant = grant(b"equivocation-owner-grant");
    let store_root_hash = ObjectHash::digest(b"equivocation-store");
    let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &owner_grant);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "owner-device",
        owner_grant,
        &owner,
    );
    let base = vec![founder];
    let chain = CircleRosterChain::from_entries(base.clone()).expect("founder roster");
    let first = chain
        .signed_set_member("owner-device", member, CircleRole::Member, &owner)
        .expect("add one member at sequence two");
    let second = chain
        .signed_set_member("owner-device", other, CircleRole::Member, &owner)
        .expect("add another member at the same sequence");
    assert_eq!(first.coord().stream_key(), second.coord().stream_key());
    assert_eq!(first.seq, second.seq);
    assert_ne!(first.entry_hash(), second.entry_hash());

    let stream = first.coord().stream_key();
    let expected = |result: Result<CircleRosterChain, CircleRosterError>| match result {
        Err(CircleRosterError::CausalConflictingSequence { stream: s, seq }) => (s, seq),
        other => panic!("two entries at one position must be a conflicting sequence: {other:?}"),
    };
    let forward = {
        let mut entries = base.clone();
        entries.extend([first.clone(), second.clone()]);
        expected(CircleRosterChain::from_entries(entries))
    };
    let reverse = {
        let mut entries = base;
        entries.extend([second, first]);
        expected(CircleRosterChain::from_entries(entries))
    };

    assert_eq!(
        forward, reverse,
        "the refusal does not depend on arrival order"
    );
    assert_eq!(forward, (stream, 2));
}
