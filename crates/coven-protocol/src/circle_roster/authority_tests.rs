use super::reduction::*;
use super::*;
use crate::store_commit;

fn grant(label: &[u8]) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(label))
}

fn exact_object(logical_key: String, bytes: &[u8]) -> crate::objects::ExactObjectRef {
    crate::objects::ExactObjectRef::new(
        crate::objects::ObjectSlot::logical(logical_key).expect("valid test Circle roster slot"),
        bytes.len() as u64,
        ObjectHash::digest(bytes),
    )
}

fn signed_exact_head(
    entry: &CircleRosterEntry,
    device_signer: &UserKeypair,
) -> ExactCircleRosterHead {
    let (head, reference) = signed_head_pair(entry, device_signer);
    ExactCircleRosterHead::bind(head, reference).expect("bind test Circle roster head")
}

fn signed_head_pair(
    entry: &CircleRosterEntry,
    device_signer: &UserKeypair,
) -> (CircleRosterHead, CircleRosterHeadRef) {
    let entry_bytes = serde_json::to_vec(entry).expect("serialize test Circle roster entry");
    let tip = exact_object(
        format!(
            "store-v1/test/circle-roster/{}/entry.json",
            entry.entry_hash()
        ),
        &entry_bytes,
    );
    let head_slot = crate::objects::ObjectSlot::logical(format!(
        "store-v1/test/circle-roster/{}/{}/head.json",
        entry.stream_id, entry.seq
    ))
    .expect("valid test Circle roster-head slot");
    let registration_bytes = format!("{} registration", entry.device_id);
    let registration = store_commit::StoreDeviceRegistrationRef {
        device_id: ObjectHash::digest(entry.device_id.as_bytes())
            .to_string()
            .parse()
            .expect("valid test Circle device id"),
        registration_hash: ObjectHash::digest(registration_bytes.as_bytes()),
        object: exact_object(
            format!(
                "store-v1/test/circle-roster/{}/registration.json",
                entry.device_id
            ),
            registration_bytes.as_bytes(),
        ),
    };
    let activation = store_commit::StreamActivation::grant_authorized(
        entry.store_root_hash,
        registration,
        entry.author_owner_grant.clone(),
        store_commit::GrantStreamAnchor::CircleRoster {
            circle_id: entry.circle_id,
            first_slot: head_slot.clone(),
        },
    );
    let head = CircleRosterHead::signed(
        entry,
        tip,
        SuccessorLink {
            activation: activation.activation_id(),
            predecessor: None,
            next_slot: crate::objects::ObjectSlot::logical(format!(
                "store-v1/test/circle-roster/{}/{}/next-head.json",
                entry.stream_id,
                entry
                    .seq
                    .checked_add(1)
                    .expect("test Circle roster sequence remains representable")
            ))
            .expect("valid next test Circle roster-head slot"),
        },
        device_signer,
    );
    let head_bytes = serde_json::to_vec(&head).expect("serialize test Circle roster head");
    let object = crate::objects::ExactObjectRef::new(
        head_slot,
        head_bytes.len() as u64,
        ObjectHash::digest(&head_bytes),
    );
    let reference = CircleRosterHeadRef::from_stored_head(&head, object);
    (head, reference)
}

#[test]
fn roster_sequence_exhaustion_fails_instead_of_reusing_the_last_sequence() {
    let owner = UserKeypair::generate();
    let owner_pubkey = keys::public_key_hex(&owner);
    let owner_grant = grant(b"sequence-exhaustion-owner-grant");
    let store_root_hash = ObjectHash::digest(b"sequence-exhaustion-store");
    let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &owner_grant);
    let stream_id = AuthorStreamId::from_bytes([122; 32]);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "owner-device",
        stream_id,
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
    heads: Vec<CircleRosterHeadRef>,
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
    let first_stream = AuthorStreamId::from_bytes([81; 32]);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "first-device",
        first_stream,
        founder_grant.clone(),
        &first,
    );
    let mut base = vec![founder];
    let add_second = CircleRosterChain::from_entries(base.clone())
        .expect("founder roster")
        .signed_set_member(
            "first-device",
            first_stream,
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
        .signed_set_member(
            "first-device",
            first_stream,
            third_pubkey,
            CircleRole::Owner,
            &first,
        )
        .expect("add third Owner");
    base.push(add_third);
    let remove_second = CircleRosterChain::from_entries(base.clone())
        .expect("three-Owner roster")
        .signed_remove_member("first-device", first_stream, second_pubkey, &first)
        .expect("first branch");
    let remove_first = CircleRosterChain::from_entries(base.clone())
        .expect("three-Owner roster")
        .signed_remove_member(
            "second-device",
            AuthorStreamId::from_bytes([82; 32]),
            first_pubkey,
            &second,
        )
        .expect("second branch");
    base.extend([remove_second.clone(), remove_first.clone()]);
    let exact_heads = vec![
        signed_exact_head(&remove_second, &first),
        signed_exact_head(&remove_first, &second),
    ];
    let heads = exact_heads
        .iter()
        .map(|head| head.reference().clone())
        .collect::<Vec<_>>();
    let mut removals = vec![remove_second.coord(), remove_first.coord()];
    removals.sort();
    let chain = CircleRosterChain::from_entries_with_heads(base, exact_heads)
        .expect("three-Owner revocation conflict");
    ThreeOwnerCycle {
        third,
        chain,
        heads,
        removals,
        revoked_owner_grants: BTreeSet::from([founder_grant, second_grant]),
    }
}

#[test]
fn a_revocation_cycle_is_a_terminal_roster_conflict() {
    let ThreeOwnerCycle {
        third,
        chain,
        mut heads,
        removals,
        revoked_owner_grants,
    } = three_owner_cycle();
    heads.sort();

    let CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
        heads: conflict_heads,
        cyclic_sources,
        involved_owner_grants,
    }) = chain.status()
    else {
        panic!("concurrent Owner revocations are a revocation cycle")
    };
    assert_eq!(conflict_heads, &heads);
    assert_eq!(cyclic_sources, &removals);
    assert_eq!(involved_owner_grants, &revoked_owner_grants);
    assert!(matches!(
        chain.try_resolved(),
        Err(CircleRosterError::Conflict)
    ));
    assert!(matches!(
        chain.signed_set_member(
            "third-device",
            AuthorStreamId::from_bytes([83; 32]),
            keys::public_key_hex(&UserKeypair::generate()),
            CircleRole::Member,
            &third,
        ),
        Err(CircleRosterError::Conflict)
    ));
}

#[test]
fn a_bound_head_must_match_its_exact_entry() {
    let owner = UserKeypair::generate();
    let owner_pubkey = keys::public_key_hex(&owner);
    let store_root_hash = ObjectHash::digest(b"Circle head-binding Store");
    let owner_grant = grant(b"Circle head-binding grant");
    let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &owner_grant);
    let entry = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "owner-device",
        AuthorStreamId::from_bytes([99; 32]),
        owner_grant,
        &owner,
    );
    let (head, reference) = signed_head_pair(&entry, &owner);
    let altered = CircleRosterHeadRef {
        head_hash: ObjectHash::digest(b"another Circle roster head"),
        ..reference
    };

    assert!(matches!(
        ExactCircleRosterHead::bind(head, altered),
        Err(CircleRosterError::HeadEntryMismatch)
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
        AuthorStreamId::from_bytes([1; 32]),
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
    let first_stream = AuthorStreamId::from_bytes([2; 32]);
    let second_stream = AuthorStreamId::from_bytes([3; 32]);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "first-device",
        first_stream,
        first_grant.clone(),
        &first_owner,
    );
    let first_created_at = founder.coord();
    let mut entries = vec![founder];
    let add_second = CircleRosterChain::from_entries(entries.clone())
        .expect("load founder roster")
        .signed_set_member(
            "first-device",
            first_stream,
            second_pubkey.clone(),
            CircleRole::Owner,
            &first_owner,
        )
        .expect("add second Owner");
    entries.push(add_second);
    let remove_first = CircleRosterChain::from_entries(entries.clone())
        .expect("load two-Owner roster")
        .signed_remove_member(
            "second-device",
            second_stream,
            first_pubkey.clone(),
            &second_owner,
        )
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
            second_stream,
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
    let build = |grant_id: MembershipGrantId, stream_byte| {
        let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &grant_id);
        CircleRosterChain::from_entries(vec![CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "owner-device",
            AuthorStreamId::from_bytes([stream_byte; 32]),
            grant_id,
            &owner,
        )])
        .expect("load founder roster")
        .resolved()
    };

    let first = build(grant(b"state-hash-grant-a"), 4);
    let second = build(grant(b"state-hash-grant-b"), 5);

    assert_ne!(first.state_hash, second.state_hash);
}
