use super::*;
use coven_protocol::causal_grants::AuthorStreamId;
use coven_protocol::circle::{
    CircleControlValue, CircleRole, CircleRosterChain, CircleRosterEntry,
    MergeCircleOwnerAuthorityRef,
};
use coven_protocol::membership::MembershipGrantId;

fn exact_ref(label: &str) -> ExactObjectRef {
    let bytes = label.as_bytes();
    ExactObjectRef::new(
        coven_protocol::objects::ObjectSlot::logical(format!(
            "store-v1/test-circle-objects/{label}.json"
        ))
        .unwrap(),
        bytes.len() as u64,
        ObjectHash::digest(bytes),
    )
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
    let authority = MergeCircleOwnerAuthorityRef {
        roster: coven_protocol::circle::MergeCircleRosterStateRef {
            heads: Vec::new(),
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
    ) -> coven_protocol::circle::MergeCircleControlHeadRef {
        coven_protocol::circle::MergeCircleControlHeadRef {
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
        } = &mut current.value.body_mut().value;
        let active_epoch = control_state
            .active_epoch_mut()
            .expect("test branch has an active epoch");
        order.device_id = device_id.to_string();
        order.stream_id = stream_id;
        order.seq = 1;
        order.previous_control_hash = None;
        order.dependencies = vec![predecessor.coordinate().clone()];
        active_epoch.covered_control_heads = vec![control_head_ref(device_id, &predecessor)];
        current.value.resign(owner);
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
        } = &mut current.value.body_mut().value;
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
        current.value.resign(owner);
        current.coord = current.value.coord();
        current.bytes = serde_json::to_vec(&current.value).expect("serialize successor control");
        assert!(state.verify(), "successor current state must verify");
        state
    }

    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let circle_id = coven_database::StoreDatabase::new(&db)
        .install_test_active_circle("current-control-conflict".to_string())
        .await
        .expect("install founder current state");
    let founder = db
        .circle_current_state_for_test(circle_id)
        .await
        .expect("load founder current state")
        .expect("test Circle current state exists");
    let owner = coven_protocol::circle_activation_test_fixtures::test_circle_owner_keypair();
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
    assert!(first_current.to_string().contains("duplicate branch"));

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
