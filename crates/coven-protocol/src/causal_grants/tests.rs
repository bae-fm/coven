use super::*;

#[test]
fn independent_nodes_become_ready_in_canonical_order() {
    let first = ObjectHash::digest(b"first independent node");
    let second = ObjectHash::digest(b"second independent node");
    let dependencies = BTreeMap::from([(first, BTreeSet::new()), (second, BTreeSet::new())]);
    let expected = *dependencies.keys().next().expect("two nodes");

    assert_eq!(
        canonical_ready_node(dependencies.iter(), &BTreeSet::new()),
        Some(expected)
    );
}

#[test]
fn canonical_ready_order_skips_a_lower_key_until_its_dependency_is_applied() {
    let dependencies = BTreeMap::from([(1u8, BTreeSet::from([2u8])), (2u8, BTreeSet::new())]);
    let mut applied = BTreeSet::new();

    let first =
        canonical_ready_node(dependencies.iter(), &applied).expect("dependency node is ready");
    assert_eq!(first, 2);
    applied.insert(first);
    let second =
        canonical_ready_node(dependencies.iter(), &applied).expect("dependent node becomes ready");
    assert_eq!(second, 1);
}

#[test]
fn cyclic_dependencies_have_no_ready_node() {
    let first = ObjectHash::digest(b"first cyclic node");
    let second = ObjectHash::digest(b"second cyclic node");
    let dependencies = BTreeMap::from([
        (first, BTreeSet::from([second])),
        (second, BTreeSet::from([first])),
    ]);

    assert_eq!(
        canonical_ready_node(dependencies.iter(), &BTreeSet::new()),
        None
    );
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
