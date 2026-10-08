use super::*;
use crate::owner_graph::OwnerGraph;

#[test]
fn callbacks_consume_the_retained_capability_but_factories_supply_one() {
    const POLICY: Policy = Policy {
        capability_types: &["DatabaseConnection"],
        construction_only_capability_types: &["DatabaseConnection"],
        ..Policy::EMPTY
    };
    let files = [RustFile::fixture(
        "crates/coven-database/src/database.rs",
        r#"
        struct DatabaseConnection;
        struct Database { connection: DatabaseConnection }
        impl Database {
            fn consume(&self, run: impl FnOnce(&DatabaseConnection)) {}
            fn consume_boxed(&self, run: Box<dyn Fn(&DatabaseConnection)>) {}
            fn consume_pointer(&self, run: fn(&DatabaseConnection)) {}
            fn supply(&self, build: impl FnOnce() -> DatabaseConnection) {}
            fn supply_pointer(&self, build: fn() -> DatabaseConnection) {}
            fn mixed(&self, run: (fn(&DatabaseConnection), DatabaseConnection)) {}
        }
        "#,
    )];
    let graph = OwnerGraph::collect(&files, &POLICY);
    let violations = find_retained_capability_parameter_violations(&files, &graph.owners, &POLICY);
    let methods: BTreeSet<_> = violations
        .iter()
        .map(|v| {
            v.message
                .split("::")
                .nth(1)
                .unwrap()
                .split(' ')
                .next()
                .unwrap()
        })
        .collect();
    assert_eq!(methods, ["supply", "supply_pointer", "mixed"].into());
}

#[test]
fn retained_owner_runtime_method_cannot_accept_the_store_directory() {
    const POLICY: Policy = Policy {
        capability_types: &["Database", "StoreDir"],
        construction_only_capability_types: &["StoreDir"],
        ..Policy::EMPTY
    };
    let files = vec![RustFile::fixture(
        "crates/coven/src/rows.rs",
        r#"
        struct StoreDir;
        struct Database;
        struct Rows { database: Database, store_dir: StoreDir }

        impl Rows {
            fn new(database: Database, store_dir: StoreDir) -> Self {
                Self { database, store_dir }
            }

            fn execute(&self, store_dir: &StoreDir) {}
        }
        "#,
    )];
    let graph = OwnerGraph::collect(&files, &POLICY);
    let violations = find_retained_capability_parameter_violations(&files, &graph.owners, &POLICY);

    assert_eq!(
        violations,
        [Finding::new(
            "crates/coven/src/rows.rs",
            11,
            "Rows::execute accepts construction-only capability StoreDir at runtime",
            "a method never takes a raw capability; it uses the one its owner was built with"
        )]
    );
}

#[test]
fn trait_implementations_have_the_same_parameter_boundary_as_explicit_capabilities() {
    const POLICY: Policy = Policy {
        capability_types: &["Clock"],
        capability_traits: &["Clock"],
        composition_roots: &[("crates/coven/src/runtime.rs", "Runtime", "open")],
        ..Policy::EMPTY
    };
    let files = vec![
        RustFile::fixture(
            "crates/coven-foundation/src/clock.rs",
            r#"
            #[cfg(feature = "test-utils")]
            impl<T> Clock for SuppliedClock<T> {}
        "#,
        ),
        RustFile::fixture(
            "crates/coven/src/runtime.rs",
            r#"
            struct Runtime { clock: Box<dyn Clock> }
            impl Runtime {
                fn new(clock: SuppliedClock<u64>) -> Self { todo!() }
                fn open(clock: SuppliedClock<u64>) { todo!() }
                fn run(&self, clock: &SuppliedClock<u64>) {}
                #[cfg(test)] fn fixture(clock: SuppliedClock<u64>) {}
            }
        "#,
        ),
    ];
    let graph = OwnerGraph::collect(&files, &POLICY);
    let violations = find_retained_capability_parameter_violations(&files, &graph.owners, &POLICY);
    assert_eq!(
        violations,
        [Finding::new(
            "crates/coven/src/runtime.rs",
            6,
            "Runtime::run accepts construction-only capability SuppliedClock at runtime",
            "a method never takes a raw capability; it uses the one its owner was built with"
        )]
    );
}
