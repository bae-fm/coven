use super::*;
use crate::protocol::causal_grants::AuthorStreamId;
use crate::protocol::circle::{
    CircleControlValue, CircleRole, CircleRosterChain, CircleRosterConflict, CircleRosterEntry,
    CircleRosterHead, CircleRosterHeadRef, CircleRosterStatus, ExactCircleRosterHead,
    MergeCircleOwnerAuthorityRef,
};
use crate::protocol::membership::MembershipGrantId;
use crate::sync::test_helpers::user_keypair_from_seed;

fn exact_ref(label: &str) -> ExactObjectRef {
    let bytes = label.as_bytes();
    ExactObjectRef::new(
        crate::protocol::objects::ObjectSlot::logical(format!(
            "store-v1/test-circle-objects/{label}.json"
        ))
        .unwrap(),
        bytes.len() as u64,
        ObjectHash::digest(bytes),
    )
}

fn roster_head(
    label: &str,
    entry: &CircleRosterEntry,
    signer: &UserKeypair,
) -> (CircleRosterHead, CircleRosterHeadRef) {
    let head = CircleRosterHead::signed(
        entry,
        exact_ref(&format!("{label}-tip")),
        crate::protocol::store_commit::SuccessorLink {
            activation: crate::protocol::store_commit::StreamActivationId::from_digest(
                ObjectHash::digest(format!("{label}-activation").as_bytes()),
            ),
            predecessor: (entry.seq > 1).then(|| exact_ref(&format!("{label}-predecessor"))),
            next_slot: crate::protocol::objects::ObjectSlot::logical(format!(
                "store-v1/test-circle-successors/{label}.json"
            ))
            .unwrap(),
        },
        signer,
    );
    let reference =
        CircleRosterHeadRef::from_stored_head(&head, exact_ref(&format!("{label}-head")));
    (head, reference)
}

#[test]
fn resolution_replay_uses_circle_conflict_closure_independently_of_current_suffix() {
    let first_owner = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let first_pubkey = keys::public_key_hex(&first_owner);
    let second_pubkey = keys::public_key_hex(&second_owner);
    let store_root_hash = ObjectHash::digest(b"Circle replay Store root");
    let founder_grant = MembershipGrantId(ObjectHash::digest(b"Circle replay founder grant"));
    let circle_id = CircleId::founder(store_root_hash, &first_pubkey, &founder_grant);
    let first_stream = AuthorStreamId::from_bytes([71; 32]);
    let second_stream = AuthorStreamId::from_bytes([72; 32]);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "first-device",
        first_stream,
        founder_grant,
        &first_owner,
    );
    let mut base = vec![founder];
    let add_second = CircleRosterChain::from_entries(base.clone())
        .expect("founder roster")
        .signed_set_member(
            "first-device",
            first_stream,
            second_pubkey,
            CircleRole::Owner,
            &first_owner,
        )
        .expect("add second owner");
    base.push(add_second);
    let remove_second = CircleRosterChain::from_entries(base.clone())
        .expect("two-owner roster")
        .signed_remove_member(
            "first-device",
            first_stream,
            keys::public_key_hex(&second_owner),
            &first_owner,
        )
        .expect("first branch");
    let remove_first = CircleRosterChain::from_entries(base.clone())
        .expect("two-owner roster")
        .signed_remove_member(
            "second-device",
            second_stream,
            first_pubkey.clone(),
            &second_owner,
        )
        .expect("second branch");
    let mut conflict_entries = base;
    conflict_entries.extend([remove_second.clone(), remove_first.clone()]);
    let (first_head, first_head_ref) = roster_head("first-conflict", &remove_second, &first_owner);
    let (second_head, second_head_ref) =
        roster_head("second-conflict", &remove_first, &second_owner);
    let conflict_heads = vec![first_head_ref, second_head_ref];
    let conflicted = CircleRosterChain::from_entries_with_heads(
        conflict_entries.clone(),
        vec![
            ExactCircleRosterHead::bind(first_head, conflict_heads[0].clone()).unwrap(),
            ExactCircleRosterHead::bind(second_head, conflict_heads[1].clone()).unwrap(),
        ],
    )
    .expect("cross-revocation conflict");
    let resolver_branch = match conflicted.status() {
        CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        }) => maximal_valid_branches
            .iter()
            .find(|branch| {
                branch
                    .active_grants()
                    .any(|(_, grant)| grant.member_pubkey == first_pubkey)
            })
            .expect("first owner's branch")
            .heads
            .clone(),
        _ => panic!("expected revocation cycle"),
    };
    let resolution = conflicted
        .signed_cycle_resolution(resolver_branch, &first_owner)
        .expect("resolution");
    let mut resumed = conflicted.clone();
    resumed
        .apply_resolutions(std::slice::from_ref(&resolution))
        .expect("apply resolution");
    let suffix = resumed
        .signed_set_member(
            "first-device",
            AuthorStreamId::from_bytes([73; 32]),
            keys::public_key_hex(&UserKeypair::generate()),
            CircleRole::Member,
            &first_owner,
        )
        .expect("post-resolution suffix");
    let mut current_heads = conflict_heads.clone();
    current_heads.push(roster_head("suffix", &suffix, &first_owner).1);
    current_heads.sort_by_key(|head| head.coord.clone());
    let mut resumed_entries = resumed.entries().to_vec();
    resumed_entries.push(suffix.clone());
    resumed = resumed
        .replay_resolved_history_to_heads(resumed_entries, current_heads.clone())
        .expect("apply suffix");

    assert_eq!(
        resumed.author_heads(),
        current_heads
            .iter()
            .map(|head| head.coord.clone())
            .collect::<Vec<_>>()
    );
    assert!(resumed.resolved().members().contains_key(&first_pubkey));
}

#[test]
fn resolution_replay_orders_circle_checkpoints_by_signed_head_references() {
    let first = user_keypair_from_seed([11; 32]);
    let second = user_keypair_from_seed([12; 32]);
    let third = user_keypair_from_seed([13; 32]);
    let fourth = user_keypair_from_seed([14; 32]);
    let pubkeys = [&first, &second, &third, &fourth]
        .into_iter()
        .map(keys::public_key_hex)
        .collect::<Vec<_>>();
    let store_root_hash = ObjectHash::digest(b"ordered Circle replay Store");
    let founder_grant = MembershipGrantId(ObjectHash::digest(b"ordered Circle founder"));
    let circle_id = CircleId::founder(store_root_hash, &pubkeys[0], &founder_grant);
    let founder_stream = AuthorStreamId::from_bytes([151; 32]);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "first-device",
        founder_stream,
        founder_grant,
        &first,
    );
    let mut history = vec![founder];
    for pubkey in pubkeys.iter().skip(1) {
        let add = CircleRosterChain::from_entries(history.clone())
            .expect("load roster")
            .signed_set_member(
                "first-device",
                founder_stream,
                pubkey.clone(),
                CircleRole::Owner,
                &first,
            )
            .expect("add Owner");
        history.push(add);
    }
    let base = CircleRosterChain::from_entries(history.clone()).expect("four-Owner roster");
    let remove_second = base
        .signed_remove_member("first-device", founder_stream, pubkeys[1].clone(), &first)
        .expect("first conflict branch");
    let remove_first = base
        .signed_remove_member(
            "second-device",
            AuthorStreamId::from_bytes([153; 32]),
            pubkeys[0].clone(),
            &second,
        )
        .expect("second conflict branch");
    history.extend([remove_second.clone(), remove_first.clone()]);
    let first_bound_heads = [
        roster_head("ordered-first", &remove_second, &first),
        roster_head("ordered-second", &remove_first, &second),
    ];
    let first_heads = first_bound_heads
        .iter()
        .map(|(_, reference)| reference.clone())
        .collect::<Vec<_>>();
    let first_conflict = CircleRosterChain::from_entries_with_heads(
        history.clone(),
        first_bound_heads
            .into_iter()
            .map(|(head, reference)| ExactCircleRosterHead::bind(head, reference).unwrap())
            .collect(),
    )
    .expect("first conflict");
    let first_branch = match first_conflict.status() {
        CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        }) => maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants().any(|(_, record)| {
                    record.member_pubkey == pubkeys[0] && record.role == CircleRole::Owner
                })
            })
            .expect("first Owner branch")
            .heads
            .clone(),
        _ => panic!("expected first conflict"),
    };
    let first_resolution = first_conflict
        .signed_cycle_resolution(first_branch, &first)
        .expect("first resolution");
    let mut resumed = first_conflict;
    resumed
        .apply_resolutions(std::slice::from_ref(&first_resolution))
        .expect("apply first resolution");

    let remove_fourth = resumed
        .signed_remove_member(
            "third-device",
            AuthorStreamId::from_bytes([7; 32]),
            pubkeys[3].clone(),
            &third,
        )
        .expect("third Owner removes fourth");
    let remove_third = resumed
        .signed_remove_member(
            "fourth-device",
            AuthorStreamId::from_bytes([102; 32]),
            pubkeys[2].clone(),
            &fourth,
        )
        .expect("fourth Owner removes third");
    let refs = vec![first_resolution.resolution_ref()];
    assert_eq!(remove_fourth.resolution_dependencies, refs);
    assert_eq!(remove_third.resolution_dependencies, refs);
    let remove_fourth_ref = roster_head("ordered-third-alternate", &remove_fourth, &third).1;
    let remove_third_ref = roster_head("ordered-fourth", &remove_third, &fourth).1;
    let second_heads = vec![remove_fourth_ref, remove_third_ref];
    let mut entries = resumed.entries().to_vec();
    entries.extend([remove_fourth.clone(), remove_third.clone()]);
    let mut heads = first_heads.clone();
    heads.extend(second_heads.clone());
    let mut second_conflict = resumed
        .replay_resolved_history_to_heads(entries, heads)
        .expect("second conflict");
    let second_branch = match second_conflict.status() {
        CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        }) => maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants().any(|(_, record)| {
                    record.member_pubkey == pubkeys[2] && record.role == CircleRole::Owner
                })
            })
            .expect("third Owner branch")
            .heads
            .clone(),
        _ => panic!("expected second revocation cycle"),
    };
    let second_resolution = second_conflict
        .signed_cycle_resolution(second_branch, &third)
        .expect("second resolution");
    assert!(
        second_resolution.conflict_hash < first_resolution.conflict_hash,
        "fixture must put the causally later Circle resolution first by canonical key"
    );
    let second_entries = vec![remove_fourth, remove_third];
    second_conflict
        .apply_resolutions(std::slice::from_ref(&second_resolution))
        .expect("apply second resolution");
    let suffix = second_conflict
        .signed_set_member(
            "third-device",
            AuthorStreamId::from_bytes([250; 32]),
            keys::public_key_hex(&user_keypair_from_seed([15; 32])),
            CircleRole::Member,
            &third,
        )
        .expect("suffix");
    let mut final_entries = second_conflict.entries().to_vec();
    final_entries.push(suffix.clone());
    let mut current_heads = first_heads.clone();
    current_heads.extend(second_heads.clone());
    assert_eq!(
        suffix.resolution_dependencies,
        second_conflict.resolution_refs()
    );
    current_heads.push(roster_head("ordered-suffix", &suffix, &third).1);
    current_heads.sort_by_key(|head| head.coord.clone());
    second_conflict = second_conflict
        .replay_resolved_history_to_heads(final_entries, current_heads.clone())
        .expect("apply suffix");
    history.extend(second_entries);

    assert_eq!(
        second_conflict.author_heads(),
        current_heads
            .iter()
            .map(|head| head.coord.clone())
            .collect::<Vec<_>>()
    );
    let mut expected_resolutions = vec![
        first_resolution.resolution_ref(),
        second_resolution.resolution_ref(),
    ];
    expected_resolutions.sort();
    assert_eq!(second_conflict.resolution_refs(), expected_resolutions);
}

#[test]
fn control_authority_uses_the_pre_transition_roster_for_self_demotion() {
    let author = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let author_pubkey = keys::public_key_hex(&author);
    let author_grant = MembershipGrantId(ObjectHash::digest(b"self-demotion grant"));
    let store_root_hash = ObjectHash::digest(b"self-demotion Store");
    let circle_id = CircleId::founder(store_root_hash, &author_pubkey, &author_grant);
    let stream_id = AuthorStreamId::from_bytes([21; 32]);
    let founder = CircleRosterEntry::founder(
        store_root_hash,
        circle_id,
        "author-device",
        stream_id,
        author_grant.clone(),
        &author,
    );
    let author_created_at = founder.coord();
    let mut entries = vec![founder];
    let add_second_owner = CircleRosterChain::from_entries(entries.clone())
        .expect("load founder roster")
        .signed_set_member(
            "author-device",
            stream_id,
            keys::public_key_hex(&second_owner),
            CircleRole::Owner,
            &author,
        )
        .expect("add second Owner");
    entries.push(add_second_owner);
    let before = CircleRosterChain::from_entries(entries.clone())
        .expect("load pre-demotion roster")
        .resolved();
    let demotion = CircleRosterChain::from_entries(entries.clone())
        .expect("load pre-demotion roster")
        .signed_set_member(
            "author-device",
            stream_id,
            author_pubkey.clone(),
            CircleRole::Member,
            &author,
        )
        .expect("self-demote while another Owner remains");
    entries.push(demotion);
    let after = CircleRosterChain::from_entries(entries)
        .expect("load post-demotion roster")
        .resolved();
    let authority = MergeCircleOwnerAuthorityRef::Roster {
        roster: crate::protocol::circle::MergeCircleRosterStateRef {
            heads: Vec::new(),
            resolutions: Vec::new(),
            state_hash: before.state_hash,
        },
        grant_id: author_grant,
        created_at: author_created_at,
    };

    assert!(verify_merge_circle_owner_authority(
        &author_pubkey,
        &authority,
        &before,
    ));
    assert!(!verify_merge_circle_owner_authority(
        &author_pubkey,
        &authority,
        &after,
    ));
}

#[tokio::test]
async fn current_state_reducer_retains_each_concurrent_control_branch() {
    fn control_head_ref(
        label: &str,
        control: &CircleCurrentControl,
    ) -> crate::protocol::circle::MergeCircleControlHeadRef {
        crate::protocol::circle::MergeCircleControlHeadRef {
            coord: control.coordinate().clone(),
            head_hash: ObjectHash::digest(format!("{label}-head").as_bytes()),
            object: exact_ref(&format!("{label}-head")),
        }
    }

    fn branch(
        mut state: CircleCurrentState,
        owner: &UserKeypair,
        device_id: &str,
        stream_id: AuthorStreamId,
    ) -> CircleCurrentState {
        let current = state
            .active_current_mut_for_test()
            .expect("branch source must be active");
        let predecessor = current.clone();
        let current = current.control_mut_for_test();
        let CircleControlValue {
            order,
            state: control_state,
            ..
        } = &mut current.value.value;
        let active_epoch = control_state
            .active_epoch_mut()
            .expect("test branch has an active epoch");
        order.device_id = device_id.to_string();
        order.stream_id = stream_id;
        order.seq = 1;
        order.previous_control_hash = None;
        order.dependencies = vec![predecessor.coordinate().clone()];
        active_epoch.covered_control_heads = vec![control_head_ref(device_id, &predecessor)];
        current.value.signature = keys::sign_hex(owner, &current.value.canonical_bytes()).1;
        current.coord = current.value.coord();
        current.bytes = serde_json::to_vec(&current.value).expect("serialize branch control");
        assert!(state.verify(), "branch current state must verify");
        state
    }

    fn successor(
        mut state: CircleCurrentState,
        owner: &UserKeypair,
        observed: &[(&str, &CircleCurrentState)],
    ) -> CircleCurrentState {
        let current = state
            .active_current_mut_for_test()
            .expect("successor source must be active");
        let predecessor = current.clone();
        let predecessor_stream = predecessor.coordinate().stream_key();
        let current = current.control_mut_for_test();
        let CircleControlValue {
            order,
            state: control_state,
            ..
        } = &mut current.value.value;
        let active_epoch = control_state
            .active_epoch_mut()
            .expect("test successor has an active epoch");
        let mut frontier = active_epoch.covered_control_heads.clone();
        frontier.retain(|head| head.coord.stream_key() != predecessor_stream);
        frontier.push(control_head_ref("own-predecessor", &predecessor));
        for (label, observed) in observed {
            let observed = observed
                .resolved_control()
                .expect("observed control is resolved");
            let stream = observed.coordinate().stream_key();
            frontier.retain(|head| head.coord.stream_key() != stream);
            frontier.push(control_head_ref(label, observed));
        }
        frontier.sort_by_key(|head| head.coord.stream_key());
        order.seq = order.seq.checked_add(1).expect("control sequence fits u64");
        order.previous_control_hash = Some(predecessor.control_hash_for_test());
        order.dependencies = frontier
            .iter()
            .filter(|head| head.coord.stream_key() != predecessor_stream)
            .map(|head| head.coord.clone())
            .collect();
        active_epoch.covered_control_heads = frontier;
        current.value.signature = keys::sign_hex(owner, &current.value.canonical_bytes()).1;
        current.coord = current.value.coord();
        current.bytes = serde_json::to_vec(&current.value).expect("serialize successor control");
        assert!(state.verify(), "successor current state must verify");
        state
    }

    let db = crate::sync::test_helpers::open_test_db();
    let circle_id = db
        .test_sql(|conn| {
            Ok(conn
                .install_test_active_circle("current-control-conflict")
                .0)
        })
        .await
        .expect("install founder current state");
    let founder = db
        .test_sql(move |database| {
            database.circle_current_state(circle_id)?.ok_or_else(|| {
                crate::database::DbError::Message("test Circle current state is absent".to_string())
            })
        })
        .await
        .expect("load founder current state");
    let owner = crate::database::test_circle_owner_keypair();
    let first = branch(
        founder.clone(),
        &owner,
        "first-successor-device",
        AuthorStreamId::from_bytes([41; 32]),
    );
    let second = branch(
        founder,
        &owner,
        "second-successor-device",
        AuthorStreamId::from_bytes([42; 32]),
    );
    let first_current = first
        .clone()
        .advance(first.clone())
        .expect_err("a control cannot advance itself");
    assert!(first_current.contains("duplicate branch"));

    let conflict = first
        .clone()
        .advance(second.clone())
        .expect("concurrent successors form a conflict");
    assert!(conflict.verify());
    assert_eq!(conflict.active_record_count(), 2);
    assert!(conflict.active().is_none());

    let first_descendant = successor(first.clone(), &owner, &[]);
    let advanced_conflict = conflict
        .clone()
        .advance(first_descendant)
        .expect("a branch descendant replaces its branch tip");
    assert!(advanced_conflict.verify());
    assert_eq!(advanced_conflict.active_record_count(), 2);

    let resolution = successor(first, &owner, &[("second-branch", &second)]);
    let resolved = conflict
        .advance(resolution)
        .expect("a control covering every branch resolves the conflict");
    assert!(resolved.verify());
    assert_eq!(resolved.active_record_count(), 1);
    assert!(resolved.active().is_some());
}
