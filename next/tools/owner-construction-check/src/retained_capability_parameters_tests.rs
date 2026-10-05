use super::*;
use crate::owner_construction::{collect_constructors, infer_owners};

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
