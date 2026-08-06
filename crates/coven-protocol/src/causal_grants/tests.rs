use super::fixed_sets::*;
use super::*;

#[test]
fn fixed_set_search_propagates_an_unattacked_graph_without_subset_enumeration() {
    let attacks = (0..64)
        .map(|index| (index, BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    let (sets, explored_states) =
        fixed_sets_from_attack_graph(&attacks, &BTreeSet::new(), 0).unwrap();

    assert_eq!(sets, vec![(0..64).collect()]);
    assert!(
        explored_states <= 2,
        "constraint propagation explored {explored_states} states"
    );
}

#[test]
fn fixed_set_search_rejects_disjoint_cycles_beyond_the_signed_protocol_bound() {
    let attacks = (0..14)
        .map(|index| (index, BTreeSet::from([index ^ 1])))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        fixed_sets_from_attack_graph(&attacks, &BTreeSet::new(), attacks.len()),
        Err(14)
    );
}

#[test]
fn independent_resolution_checkpoints_replay_in_canonical_order() {
    let first = ObjectHash::digest(b"first independent checkpoint");
    let second = ObjectHash::digest(b"second independent checkpoint");
    let dependencies = BTreeMap::from([(first, BTreeSet::new()), (second, BTreeSet::new())]);
    let expected = *dependencies.keys().next().expect("two checkpoints");

    assert_eq!(
        canonical_ready_checkpoint(dependencies.iter(), &BTreeSet::new()),
        Some(expected)
    );
}

#[test]
fn canonical_checkpoint_order_skips_a_lower_key_until_its_dependency_is_applied() {
    let dependencies = BTreeMap::from([(1u8, BTreeSet::from([2u8])), (2u8, BTreeSet::new())]);
    let mut applied = BTreeSet::new();

    let first = canonical_ready_checkpoint(dependencies.iter(), &applied)
        .expect("dependency checkpoint is ready");
    assert_eq!(first, 2);
    applied.insert(first);
    let second = canonical_ready_checkpoint(dependencies.iter(), &applied)
        .expect("dependent checkpoint becomes ready");
    assert_eq!(second, 1);
}

#[test]
fn cyclic_resolution_checkpoints_have_no_ready_cut() {
    let first = ObjectHash::digest(b"first cyclic checkpoint");
    let second = ObjectHash::digest(b"second cyclic checkpoint");
    let dependencies = BTreeMap::from([
        (first, BTreeSet::from([second])),
        (second, BTreeSet::from([first])),
    ]);

    assert_eq!(
        canonical_ready_checkpoint(dependencies.iter(), &BTreeSet::new()),
        None
    );
}

#[test]
fn full_checkpoint_merge_preserves_a_removal_seen_by_only_one_branch() {
    let grant = MembershipGrantId(ObjectHash::digest(b"removed checkpoint grant"));
    let retired = BTreeMap::from([(
        grant.clone(),
        GrantState::Tombstoned {
            record: "member",
            retirements: GrantRetirements::new("first retirement"),
        },
    )]);
    let active = BTreeMap::from([(grant.clone(), GrantState::Active { record: "member" })]);
    let mut merged_grants = BTreeMap::new();
    let mut merged_included = BTreeSet::new();

    assert!(merge_checkpoint_evidence(
        &mut merged_grants,
        &mut merged_included,
        &retired,
        &BTreeSet::from([1]),
    ));
    assert!(merge_checkpoint_evidence(
        &mut merged_grants,
        &mut merged_included,
        &active,
        &BTreeSet::from([2]),
    ));

    assert_eq!(merged_grants, retired);
    assert_eq!(merged_included, BTreeSet::from([1, 2]));
}

#[test]
fn tombstoned_grant_rejects_an_empty_retirement_set() {
    let encoded = serde_json::json!({
        "tombstoned": {
            "record": "member",
            "retirements": [],
        }
    });

    assert!(serde_json::from_value::<GrantState<String, String>>(encoded).is_err());
}

#[test]
fn checkpoint_merge_unions_concurrent_retirement_evidence() {
    let grant = MembershipGrantId(ObjectHash::digest(b"concurrently retired grant"));
    let branch = |retirement| {
        BTreeMap::from([(
            grant.clone(),
            GrantState::Tombstoned {
                record: "member",
                retirements: GrantRetirements::new(retirement),
            },
        )])
    };
    let mut merged = BTreeMap::new();
    let mut included = BTreeSet::new();

    assert!(merge_checkpoint_evidence(
        &mut merged,
        &mut included,
        &branch("first retirement"),
        &BTreeSet::from([1]),
    ));
    assert!(merge_checkpoint_evidence(
        &mut merged,
        &mut included,
        &branch("second retirement"),
        &BTreeSet::from([2]),
    ));

    let GrantState::Tombstoned { retirements, .. } = &merged[&grant] else {
        panic!("retirement must dominate an active branch")
    };
    assert_eq!(
        retirements.as_set(),
        &BTreeSet::from(["first retirement", "second retirement"])
    );
}
