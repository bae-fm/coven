use super::*;
use crate::owner_construction::{collect_constructors, infer_owners};

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
    let owners = infer_owners(&crate::syntax::collect_structs(&files), &POLICY);
    let constructors = collect_constructors(&files, &owners);
    let violations =
        find_retained_capability_parameter_violations(&files, &owners, &constructors, &POLICY);
    let methods: BTreeSet<_> = violations.iter().map(|v| v.method.as_str()).collect();
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
    let structs = crate::syntax::collect_structs(&files);
    let owners = infer_owners(&structs, &POLICY);
    let constructors = collect_constructors(&files, &owners);
    let violations =
        find_retained_capability_parameter_violations(&files, &owners, &constructors, &POLICY);

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].owner, "Rows");
    assert_eq!(violations[0].method, "execute");
    assert_eq!(violations[0].capability, "StoreDir");
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
    let owners = infer_owners(&crate::syntax::collect_structs(&files), &POLICY);
    let constructors = collect_constructors(&files, &owners);
    let violations =
        find_retained_capability_parameter_violations(&files, &owners, &constructors, &POLICY);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert_eq!(violations[0].capability, "SuppliedClock");
    assert_eq!(violations[0].method, "run");
}
