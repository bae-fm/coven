use super::*;

#[test]
fn composition_roots_are_listed_by_file_as_methods_and_free_functions() {
    let policy = Policy::fixture(
        r#"
        [composition_roots]
        "crates/app/src/builder.rs" = ["Builder::open", "restore"]
        "crates/app/src/handle.rs" = ["Handle::new"]
        "#,
    );
    assert!(policy.is_composition_root("crates/app/src/builder.rs", "Builder", "open"));
    assert!(policy.is_composition_root("crates/app/src/builder.rs", "<free>", "restore"));
    assert!(policy.is_composition_root("crates/app/src/handle.rs", "Handle", "new"));
    assert!(!policy.is_composition_root("crates/app/src/handle.rs", "Builder", "open"));
    assert!(!policy.is_composition_root("crates/app/src/builder.rs", "Builder", "restore"));
}

#[test]
fn capability_factories_are_listed_by_file_as_receiver_methods_and_products() {
    let policy = Policy::fixture(
        r#"
        [capability_factories."crates/app/src/directory.rs"]
        "StoreDir::file" = "AtomicFile"
        "StoreDir::lock" = "StoreLock"
        "#,
    );
    assert_eq!(
        policy.capability_factories,
        [("file", "AtomicFile"), ("lock", "StoreLock")].map(|(method, product)| {
            CapabilityFactory {
                path: "crates/app/src/directory.rs".into(),
                owner: "StoreDir".into(),
                method: method.into(),
                product: product.into(),
            }
        })
    );
}

#[test]
fn gate_paths_are_rust_paths() {
    let policy = Policy::fixture(
        r#"
        [[background_starts]]
        kind = "spawn"
        paths = ["tokio::spawn", "OsRng"]
        methods = ["spawn"]
        "#,
    );
    assert_eq!(policy.background_starts[0].path_patterns, [vec!["tokio", "spawn"], vec!["OsRng"]]);
    assert_eq!(policy.background_starts[0].method_patterns, ["spawn"]);
}

#[test]
fn malformed_rows_and_unknown_keys_are_refused() {
    for text in [
        "[composition_roots]\n\"a.rs\" = [\"Type::method::extra\"]",
        "[composition_roots]\n\"a.rs\" = [\"Type::\"]",
        "[capability_factories.\"a.rs\"]\n\"method\" = \"Product\"",
        "[[background_starts]]\nkind = \"spawn\"\npaths = [\"tokio::\"]",
        "rules = [\"no-such-rule\"]",
        "capability_type = [\"Clock\"]",
        "[database]\ncapability = \"SQLite\"\nhomes = []",
    ] {
        assert!(toml::from_str::<Policy>(text).is_err(), "{text}");
    }
}

#[test]
fn the_database_homes_are_its_capabilitys() {
    let policy = Policy::fixture(
        r#"
        [database]
        capability = "SQLite"
        [[capabilities]]
        name = "network"
        homes = ["crates/app-storage/src/"]
        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-database/src/"]
        "#,
    );
    assert_eq!(policy.database_homes(), ["crates/app-database/src/"]);
    assert!(Policy::fixture("").database_homes().is_empty());
}

#[test]
fn only_owner_graph_rules_need_unique_type_names() {
    let graph_rules = [
        Rule::CrateDependencies,
        Rule::CapabilityBoundaries,
        Rule::DatabaseBoundary,
        Rule::OwnerDependencies,
        Rule::RetainedCapabilityParameters,
        Rule::ComponentBundles,
        Rule::CapabilityConstruction,
        Rule::Conventions,
    ]
    .into_iter()
    .filter(|rule| rule.reads_owner_graph())
    .collect::<Vec<_>>();
    assert_eq!(
        graph_rules,
        [Rule::OwnerDependencies, Rule::RetainedCapabilityParameters, Rule::CapabilityConstruction]
    );
}
