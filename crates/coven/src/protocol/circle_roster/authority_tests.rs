use super::reduction::*;
use super::*;
use crate::protocol::store_commit;

fn grant(label: &[u8]) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(label))
}

#[test]
fn circle_grant_mapping_rejects_missing_checkpoint_evidence() {
    let grant = grant(b"missing Circle checkpoint evidence");
    let coord = CircleRosterCoord {
        author_pubkey: "owner".to_string(),
        device_id: "owner-device".to_string(),
        stream_id: AuthorStreamId::from_bytes([91; 32]),
        author_owner_grant: grant.clone(),
        seq: 1,
        entry_hash: ObjectHash::digest(b"Circle grant creation"),
    };
    let checkpoint_creation = GrantState::Active {
        record: causal_grants::GrantRecord {
            member_pubkey: "owner".to_string(),
            assignment: CircleRole::Owner,
            creation: causal_grants::CausalGrantCreation::Checkpoint,
        },
    };
    assert!(matches!(
        map_circle_grant_state(&grant, &checkpoint_creation, None),
        Err(CircleRosterError::MissingCheckpointGrant { grant: missing })
            if missing == grant
    ));

    let checkpoint_retirement = GrantState::Tombstoned {
        record: causal_grants::GrantRecord {
            member_pubkey: "owner".to_string(),
            assignment: CircleRole::Owner,
            creation: causal_grants::CausalGrantCreation::Entry(coord),
        },
        retirements: GrantRetirements::new(causal_grants::CausalGrantRetirement::Checkpoint),
    };
    assert!(matches!(
        map_circle_grant_state(&grant, &checkpoint_retirement, None),
        Err(CircleRosterError::MissingCheckpointRetirementEvidence { grant: missing })
            if missing == grant
    ));
}

fn exact_object(logical_key: String, bytes: &[u8]) -> crate::protocol::objects::ExactObjectRef {
    crate::protocol::objects::ExactObjectRef::new(
        crate::protocol::objects::ObjectSlot::logical(logical_key)
            .expect("valid test Circle roster slot"),
        bytes.len() as u64,
        ObjectHash::digest(bytes),
    )
}

fn signed_head(entry: &CircleRosterEntry, device_signer: &UserKeypair) -> CircleRosterHeadRef {
    signed_head_with_resolutions(entry, entry.resolution_dependencies.clone(), device_signer).1
}

fn signed_exact_head(
    entry: &CircleRosterEntry,
    device_signer: &UserKeypair,
) -> ExactCircleRosterHead {
    let (head, reference) =
        signed_head_with_resolutions(entry, entry.resolution_dependencies.clone(), device_signer);
    ExactCircleRosterHead::bind(head, reference).expect("bind test Circle roster head")
}

fn signed_head_with_resolutions(
    entry: &CircleRosterEntry,
    resolutions: Vec<CircleRosterConflictResolutionRef>,
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
    let head_slot = crate::protocol::objects::ObjectSlot::logical(format!(
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
    let head = CircleRosterHead::signed_with_resolutions(
        entry,
        tip,
        SuccessorLink {
            activation: activation.activation_id(),
            predecessor: None,
            next_slot: crate::protocol::objects::ObjectSlot::logical(format!(
                "store-v1/test/circle-roster/{}/{}/next-head.json",
                entry.stream_id,
                entry
                    .seq
                    .checked_add(1)
                    .expect("test Circle roster sequence remains representable")
            ))
            .expect("valid next test Circle roster-head slot"),
        },
        resolutions,
        device_signer,
    );
    let head_bytes = serde_json::to_vec(&head).expect("serialize test Circle roster head");
    let object = crate::protocol::objects::ExactObjectRef::new(
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
    terminal.seq = u64::MAX;
    terminal.previous_hash = Some(founder.entry_hash());
    terminal.signature = keys::sign_hex(&owner, &terminal.canonical_bytes()).1;
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

fn three_owner_cycle() -> (
    ObjectHash,
    CircleId,
    UserKeypair,
    UserKeypair,
    UserKeypair,
    CircleRosterChain,
    Vec<CircleRosterHeadRef>,
) {
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
        founder_grant,
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
    let conflict = CircleRosterChain::from_entries_with_heads(base, exact_heads)
        .expect("three-Owner revocation conflict");
    (
        store_root_hash,
        circle_id,
        first,
        second,
        third,
        conflict,
        heads,
    )
}

#[test]
fn sequential_circle_resolution_checkpoints_retain_every_exact_reference() {
    let (_store_root_hash, _circle_id, first, _second, third, conflicted, first_heads) =
        three_owner_cycle();
    let first_pubkey = keys::public_key_hex(&first);
    let third_pubkey = keys::public_key_hex(&third);
    let first_branch = match conflicted.status() {
        CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        }) => maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants().any(|(_, record)| {
                    record.member_pubkey == first_pubkey && record.role == CircleRole::Owner
                })
            })
            .expect("first Owner branch")
            .heads
            .clone(),
        _ => panic!("expected first revocation conflict"),
    };
    let first_resolution = conflicted
        .signed_cycle_resolution(first_branch, &first)
        .expect("first resolution");
    let mut resumed = conflicted;
    resumed
        .apply_resolutions(std::slice::from_ref(&first_resolution))
        .expect("apply first resolution");

    let remove_third = resumed
        .signed_remove_member(
            "first-device",
            AuthorStreamId::from_bytes([83; 32]),
            third_pubkey,
            &first,
        )
        .expect("replacement Owner removes third Owner");
    let remove_first = resumed
        .signed_remove_member(
            "third-device",
            AuthorStreamId::from_bytes([84; 32]),
            first_pubkey.clone(),
            &third,
        )
        .expect("third Owner removes replacement Owner");
    let mut entries = resumed.entries().to_vec();
    entries.extend([remove_third.clone(), remove_first.clone()]);
    let mut heads = first_heads;
    heads.extend([
        signed_head(&remove_third, &first),
        signed_head(&remove_first, &third),
    ]);
    heads.sort_by_key(|head| head.coord.clone());
    let mut second_conflict = resumed
        .replay_resolved_history_to_heads(entries, heads)
        .expect("load second conflict from first checkpoint");
    let second_branch = match second_conflict.status() {
        CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        }) => maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants().any(|(_, record)| {
                    record.member_pubkey == first_pubkey && record.role == CircleRole::Owner
                })
            })
            .expect("replacement Owner branch")
            .heads
            .clone(),
        _ => panic!("expected second revocation conflict"),
    };
    let second_resolution = second_conflict
        .signed_cycle_resolution(second_branch, &first)
        .expect("second resolution");
    second_conflict
        .apply_resolutions(std::slice::from_ref(&second_resolution))
        .expect("apply second resolution");

    let mut expected = vec![
        first_resolution.resolution_ref(),
        second_resolution.resolution_ref(),
    ];
    expected.sort();
    assert_eq!(second_conflict.resolution_refs(), expected);
}

#[test]
fn unaffected_circle_owner_resolution_retires_its_selected_branch_grant() {
    let (store_root_hash, circle_id, _first, _second, third, conflicted, _) = three_owner_cycle();
    let third_pubkey = keys::public_key_hex(&third);
    let (branch, old_grant) = match conflicted.status() {
        CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        }) => {
            let branch = maximal_valid_branches
                .iter()
                .find(|branch| {
                    branch.active_grants().any(|(_, record)| {
                        record.member_pubkey == third_pubkey && record.role == CircleRole::Owner
                    })
                })
                .expect("unaffected Owner branch");
            let old_grant = branch
                .active_grants()
                .find_map(|(grant, record)| {
                    (record.member_pubkey == third_pubkey).then_some(grant.clone())
                })
                .expect("unaffected Owner grant");
            (branch.heads.clone(), old_grant)
        }
        _ => panic!("expected revocation conflict"),
    };
    let resolution = conflicted
        .signed_cycle_resolution(branch, &third)
        .expect("unaffected Owner resolution");
    let resolved = conflicted
        .resolved_with(std::slice::from_ref(&resolution))
        .expect("unaffected Owner resolution is valid");

    assert!(resolution.retired_owner_grants.contains(&old_grant));
    assert!(resolved.grants[&old_grant].active().is_none());
    assert!(resolved
        .grants
        .get(&resolution.replacement_grant)
        .and_then(GrantState::active)
        .is_some());
    assert!(matches!(
        &resolved.grants[&old_grant],
        GrantState::Tombstoned { retirements, .. }
            if retirements.contains(&CircleGrantRetirement::ConflictResolution(
                resolution.resolution_ref()
            ))
    ));
    assert!(resolution.verify_against(
        store_root_hash,
        circle_id,
        match conflicted.status() {
            CircleRosterStatus::Conflict(conflict) => conflict,
            _ => unreachable!(),
        }
    ));
}

#[test]
fn circle_revocation_cycle_over_protocol_bound_is_typed() {
    let owners = (0..13).map(|_| UserKeypair::generate()).collect::<Vec<_>>();
    let pubkeys = owners.iter().map(keys::public_key_hex).collect::<Vec<_>>();
    let store_root_hash = ObjectHash::digest(b"bounded Circle cycle Store");
    let founder_grant = grant(b"bounded Circle cycle founder");
    let circle_id = CircleId::founder(store_root_hash, &pubkeys[0], &founder_grant);
    let founder_stream = AuthorStreamId::from_bytes([121; 32]);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "owner-0-device",
        founder_stream,
        founder_grant,
        &owners[0],
    );
    let mut base = vec![founder];
    for (index, pubkey) in pubkeys.iter().enumerate().skip(1) {
        let add = CircleRosterChain::from_entries(base.clone())
            .expect("load ring roster")
            .signed_set_member(
                "owner-0-device",
                founder_stream,
                pubkey.clone(),
                CircleRole::Owner,
                &owners[0],
            )
            .expect("add ring Owner");
        assert_eq!(index as u64 + 1, add.seq);
        base.push(add);
    }
    let base_chain = CircleRosterChain::from_entries(base.clone()).expect("13-Owner roster");
    let removals = owners
        .iter()
        .enumerate()
        .map(|(index, owner)| {
            let stream = if index == 0 {
                founder_stream
            } else {
                AuthorStreamId::from_bytes([index as u8; 32])
            };
            base_chain
                .signed_remove_member(
                    &format!("owner-{index}-device"),
                    stream,
                    pubkeys[(index + 1) % pubkeys.len()].clone(),
                    owner,
                )
                .expect("sign ring removal")
        })
        .collect::<Vec<_>>();
    base.extend(removals.iter().cloned());
    let heads = removals
        .iter()
        .zip(&owners)
        .map(|(entry, owner)| signed_exact_head(entry, owner))
        .collect();

    assert!(matches!(
        CircleRosterChain::from_entries_with_heads(base, heads),
        Err(CircleRosterError::RevocationCycleTooWide {
            sources: 13,
            maximum: 12,
        })
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
                && retirements.as_set() == &BTreeSet::from([CircleGrantRetirement::Entry {
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
    retirements.insert(CircleGrantRetirement::Entry {
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

#[tokio::test]
async fn cross_revocation_cycle_is_signed_from_an_exact_maximal_branch() {
    let first_owner = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let outsider = UserKeypair::generate();
    let first_pubkey = keys::public_key_hex(&first_owner);
    let second_pubkey = keys::public_key_hex(&second_owner);
    let store_root_hash = ObjectHash::digest(b"Circle conflict Store root");
    let first_grant = grant(b"Circle conflict founder grant");
    let circle_id = CircleId::founder(store_root_hash, &first_pubkey, &first_grant);
    let first_stream = AuthorStreamId::from_bytes([31; 32]);
    let second_stream = AuthorStreamId::from_bytes([32; 32]);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "first-device",
        first_stream,
        first_grant,
        &first_owner,
    );
    let mut base = vec![founder];
    let add_second = CircleRosterChain::from_entries(base.clone())
        .unwrap()
        .signed_set_member(
            "first-device",
            first_stream,
            second_pubkey.clone(),
            CircleRole::Owner,
            &first_owner,
        )
        .unwrap();
    base.push(add_second);
    let remove_second = CircleRosterChain::from_entries(base.clone())
        .unwrap()
        .signed_remove_member(
            "first-device",
            first_stream,
            second_pubkey.clone(),
            &first_owner,
        )
        .unwrap();
    let remove_first = CircleRosterChain::from_entries(base.clone())
        .unwrap()
        .signed_remove_member(
            "second-device",
            second_stream,
            first_pubkey.clone(),
            &second_owner,
        )
        .unwrap();
    base.extend([remove_second.clone(), remove_first.clone()]);
    let conflicted = CircleRosterChain::from_entries_with_heads(
        base,
        vec![
            signed_exact_head(&remove_second, &first_owner),
            signed_exact_head(&remove_first, &second_owner),
        ],
    )
    .expect("well-formed Circle roster conflict");
    let CircleRosterStatus::Conflict(
        conflict @ CircleRosterConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        },
    ) = conflicted.status()
    else {
        panic!("expected revocation cycle");
    };
    assert_eq!(maximal_valid_branches.len(), 2);
    let branch_state = maximal_valid_branches
        .iter()
        .find(|branch| {
            branch.active_grants().any(|(_, record)| {
                record.member_pubkey == first_pubkey && record.role == CircleRole::Owner
            })
        })
        .expect("first Owner branch")
        .clone();
    let branch = branch_state.heads.clone();
    let second_branch = maximal_valid_branches
        .iter()
        .find(|branch| {
            branch.active_grants().any(|(_, record)| {
                record.member_pubkey == second_pubkey && record.role == CircleRole::Owner
            })
        })
        .expect("second Owner branch")
        .heads
        .clone();
    let resolution = conflicted
        .signed_cycle_resolution(branch.clone(), &first_owner)
        .expect("branch Owner resolution");
    let second_resolution = conflicted
        .signed_cycle_resolution(second_branch, &second_owner)
        .expect("other branch Owner resolution");
    assert_eq!(
        resolution,
        conflicted
            .signed_cycle_resolution(branch, &first_owner)
            .expect("idempotent retry")
    );
    assert!(resolution.verify_against(store_root_hash, circle_id, conflict));
    let resolved_once = conflicted
        .resolved_with(std::slice::from_ref(&resolution))
        .expect("one resolution applies");
    let resolved_duplicate = conflicted
        .resolved_with(&[resolution.clone(), resolution.clone()])
        .expect("an exact retry is idempotent");
    assert_eq!(resolved_once, resolved_duplicate);
    assert!(resolved_once
        .grants
        .get(&resolution.replacement_grant)
        .and_then(GrantState::active)
        .is_some());
    assert!(resolution
        .retired_owner_grants
        .iter()
        .all(|grant| resolved_once
            .grants
            .get(grant)
            .and_then(GrantState::active)
            .is_none()));

    let resolved_union = conflicted
        .resolved_with(&[resolution.clone(), second_resolution.clone()])
        .expect("distinct resolvers are unioned");
    assert!(resolved_union
        .grants
        .get(&resolution.replacement_grant)
        .and_then(GrantState::active)
        .is_some());
    assert!(resolved_union
        .grants
        .get(&second_resolution.replacement_grant)
        .and_then(GrantState::active)
        .is_some());

    let mut branch_specific = conflict.clone();
    let CircleRosterConflict::RevocationCycle {
        maximal_valid_branches,
        ..
    } = &mut branch_specific
    else {
        unreachable!()
    };
    let branch_only_grant = grant(b"Circle branch-only grant");
    let branch_only_creation = maximal_valid_branches[0].effective_frontier[0].clone();
    maximal_valid_branches[0].grants.insert(
        branch_only_grant.clone(),
        GrantState::Active {
            record: CircleGrantRecord {
                member_pubkey: keys::public_key_hex(&UserKeypair::generate()),
                role: CircleRole::Member,
                creation_authority: CircleGrantCreationAuthority::Entry(branch_only_creation),
            },
        },
    );
    let composed = resolve_circle_roster_conflict(
        store_root_hash,
        circle_id,
        &branch_specific,
        &[resolution.clone(), second_resolution.clone()],
    )
    .expect("retire grants not agreed by every valid branch");
    let branch_only_retirements = composed
        .grants
        .get(&branch_only_grant)
        .and_then(GrantState::retirements)
        .expect("branch-only grant is retained as retired");
    assert!(
        branch_only_retirements.contains(&CircleGrantRetirement::ConflictResolution(
            resolution.resolution_ref()
        ))
    );
    assert!(
        branch_only_retirements.contains(&CircleGrantRetirement::ConflictResolution(
            second_resolution.resolution_ref()
        ))
    );

    let mut duplicate_member = branch_specific;
    let CircleRosterConflict::RevocationCycle {
        maximal_valid_branches,
        ..
    } = &mut duplicate_member
    else {
        unreachable!()
    };
    let duplicate_pubkey = keys::public_key_hex(&UserKeypair::generate());
    let duplicate_creation = resolution.conflicting_heads[0].coord.clone();
    for branch in maximal_valid_branches {
        for suffix in [b'a', b'b'] {
            branch.grants.insert(
                grant(&[suffix]),
                GrantState::Active {
                    record: CircleGrantRecord {
                        member_pubkey: duplicate_pubkey.clone(),
                        role: CircleRole::Member,
                        creation_authority: CircleGrantCreationAuthority::Entry(
                            duplicate_creation.clone(),
                        ),
                    },
                },
            );
        }
    }
    assert!(matches!(
        resolve_circle_roster_conflict(
            store_root_hash,
            circle_id,
            &duplicate_member,
            &[resolution.clone(), second_resolution.clone()],
        ),
        Err(CircleRosterError::InvalidConflictResolution)
    ));

    let mut resumed = conflicted.clone();
    let raw_heads = resumed.author_heads();
    resumed
        .apply_resolutions(std::slice::from_ref(&resolution))
        .expect("resolution activates replacement Owner grant");
    assert_eq!(resumed.author_heads(), raw_heads);
    assert_eq!(
        resumed.effective_frontier(),
        branch_state.effective_frontier
    );
    assert_eq!(resumed.resolution_refs(), &[resolution.resolution_ref()]);
    let reusable = resumed.reusable_author_streams(
        &first_pubkey,
        "first-device",
        &resolution.replacement_grant,
    );
    assert!(reusable.is_empty());
    let db = crate::sync::test_helpers::open_test_db();
    let store_database = crate::database::StoreDatabase::new(&db);
    let stream_state_key = format!(
        "circle_roster_author_stream/{circle_id}/{first_pubkey}/{}",
        resolution.replacement_grant
    );
    let selected_stream = store_database
        .select_causal_author_stream(stream_state_key.clone(), reusable)
        .await
        .unwrap();
    assert_eq!(
        store_database
            .select_causal_author_stream(stream_state_key, BTreeSet::from([selected_stream]),)
            .await
            .unwrap(),
        selected_stream
    );
    let after_resolution = resumed
        .signed_set_member(
            "first-device",
            selected_stream,
            keys::public_key_hex(&UserKeypair::generate()),
            CircleRole::Member,
            &first_owner,
        )
        .expect("replacement Owner can author from a fresh stream");
    assert_eq!(
        after_resolution.author_owner_grant,
        resolution.replacement_grant
    );
    assert!(matches!(
        conflicted.signed_cycle_resolution(resolution.resolver_branch_heads.clone(), &outsider,),
        Err(CircleRosterError::SignerIsNotOwner(_))
    ));
}

#[test]
fn circle_head_resolution_cut_must_equal_its_tip_entry_cut() {
    let owner = UserKeypair::generate();
    let owner_pubkey = keys::public_key_hex(&owner);
    let store_root_hash = ObjectHash::digest(b"Circle head-tip Store");
    let owner_grant = grant(b"Circle head-tip grant");
    let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &owner_grant);
    let entry = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "owner-device",
        AuthorStreamId::from_bytes([99; 32]),
        owner_grant,
        &owner,
    );
    let fake = CircleRosterConflictResolutionRef {
        conflict_hash: ObjectHash::digest(b"Circle head-tip conflict"),
        resolver_pubkey: owner_pubkey,
        resolution_hash: ObjectHash::digest(b"Circle head-tip resolution"),
    };
    let (head, reference) = signed_head_with_resolutions(&entry, vec![fake], &owner);
    let head = ExactCircleRosterHead::bind(head, reference).unwrap();

    assert!(matches!(
        CircleRosterChain::from_entries_with_heads(vec![entry], vec![head]),
        Err(CircleRosterError::MissingConflictHeads)
    ));
}

#[test]
fn circle_entry_rejects_unsorted_or_duplicate_resolution_dependencies() {
    let owner = UserKeypair::generate();
    let owner_pubkey = keys::public_key_hex(&owner);
    let store_root_hash = ObjectHash::digest(b"Circle entry resolution cut Store");
    let owner_grant = grant(b"Circle entry resolution cut grant");
    let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &owner_grant);
    let stream_id = AuthorStreamId::from_bytes([100; 32]);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "owner-device",
        stream_id,
        owner_grant,
        &owner,
    );
    let entry = CircleRosterChain::from_entries(vec![founder])
        .unwrap()
        .signed_set_member(
            "owner-device",
            stream_id,
            keys::public_key_hex(&UserKeypair::generate()),
            CircleRole::Member,
            &owner,
        )
        .unwrap();
    let mut refs = [b"first".as_slice(), b"second".as_slice()]
        .into_iter()
        .map(|label| CircleRosterConflictResolutionRef {
            conflict_hash: ObjectHash::digest(label),
            resolver_pubkey: owner_pubkey.clone(),
            resolution_hash: ObjectHash::digest(&[label, b" resolution"].concat()),
        })
        .collect::<Vec<_>>();
    refs.sort();

    let mut unsorted = entry.clone();
    unsorted.resolution_dependencies = refs.iter().rev().cloned().collect();
    unsorted.signature = keys::sign_hex(&owner, &unsorted.canonical_bytes()).1;
    assert!(!unsorted.verify());

    let mut duplicate = entry;
    duplicate.resolution_dependencies = vec![refs[0].clone(), refs[0].clone()];
    duplicate.signature = keys::sign_hex(&owner, &duplicate.canonical_bytes()).1;
    assert!(!duplicate.verify());
}
