use super::*;

const POLICY: Policy = Policy {
    capability_types: &["Database"],
    task_types: &["SyncPass", "UploadStep"],
    ..Policy::EMPTY
};

fn violations(source: &str) -> Vec<OwnerConstructionViolation> {
    let files = vec![RustFile::fixture(
        "crates/coven-sync/src/fixture.rs",
        source,
    )];
    let structs = crate::syntax::collect_structs(&files);
    let owners = infer_owners(&structs, &POLICY);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);
    find_owner_construction_violations(&files, &owners, &constructors, &free_constructors, &POLICY)
}

#[test]
fn nested_owner_constructor_is_rejected() {
    let violations = violations(
        r#"
        struct Database;
        struct Child { database: Database }
        impl Child { fn new(database: Database) -> Self { Self { database } } }
        struct Parent { child: Child }
        impl Parent { fn new(database: Database) -> Self { Self { child: Child::new(database) } } }
        "#,
    );
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].parent, "Parent::new");
    assert_eq!(violations[0].child, "Child");
}

#[test]
fn tasks_can_compose_tasks() {
    let violations = violations(
        r#"
        struct Database;
        struct UploadStep { database: Database }
        impl UploadStep {
            fn new(database: Database) -> Self { Self { database } }
        }
        struct SyncPass { step: UploadStep }
        impl SyncPass {
            fn new(database: Database) -> Self {
                Self { step: UploadStep::new(database) }
            }
        }
        "#,
    );
    assert!(violations.is_empty());
}

#[test]
fn injected_owner_is_accepted() {
    let violations = violations(
        r#"
        struct Database;
        struct Child { database: Database }
        impl Child { fn new(database: Database) -> Self { Self { database } } }
        struct Parent { child: Child }
        impl Parent { fn new(child: Child) -> Self { Self { child } } }
        "#,
    );
    assert!(violations.is_empty());
}

#[test]
fn private_inner_representation_is_accepted() {
    let violations = violations(
        r#"
        struct Database;
        struct ParentInner { database: Database }
        struct Parent { inner: ParentInner }
        impl Parent { fn new(database: Database) -> Self { Self { inner: ParentInner { database } } } }
        "#,
    );
    assert!(violations.is_empty());
}

#[test]
fn owner_constructor_cannot_hide_child_construction_behind_a_free_function() {
    let violations = violations(
        r#"
        struct Database;
        struct Child { database: Database }
        fn build_child(database: Database) -> Child { Child { database } }
        struct Parent { child: Child }
        impl Parent { fn new(database: Database) -> Self { Self { child: build_child(database) } } }
        "#,
    );
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].child, "Child");
}

#[test]
fn module_qualified_free_factory_is_rejected() {
    let violations = violations(
        r#"
        struct Database;
        struct Child { database: Database }
        mod factory {
            use super::*;
            pub(super) fn build_child(database: Database) -> Child { Child { database } }
        }
        struct Parent { child: Child }
        impl Parent {
            fn new(database: Database) -> Self {
                Self { child: crate::factory::build_child(database) }
            }
        }
        "#,
    );
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].child, "Child");
}

#[test]
fn owner_constructor_cannot_hide_child_construction_behind_an_associated_factory() {
    let violations = violations(
        r#"
        struct Database;
        struct Child { database: Database }
        struct Parent { child: Child }
        struct ChildFactory;

        impl ChildFactory {
            fn build(database: Database) -> Child { Child { database } }
        }

        impl Parent {
            fn new(database: Database) -> Self {
                Self { child: ChildFactory::build(database) }
            }
        }
        "#,
    );
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].parent, "Parent::new");
    assert_eq!(violations[0].child, "Child");
}

#[test]
fn qualified_call_does_not_match_an_unrelated_free_constructor() {
    let violations = violations(
        r#"
        struct Database;
        struct DatabaseOwner { database: Database }
        fn open(database: Database) -> DatabaseOwner { DatabaseOwner { database } }

        struct ParsedValue;
        impl ParsedValue { fn open() -> Self { Self } }

        struct Parent { database: Database, value: ParsedValue }
        impl Parent {
            fn new(database: Database) -> Self {
                Self { database, value: ParsedValue::open() }
            }
        }
        "#,
    );
    assert!(violations.is_empty());
}

#[test]
fn a_composition_root_builds_the_owner_graph() {
    const ROOTED: Policy = Policy {
        capability_types: &["Database"],
        composition_roots: &[("crates/coven/src/builder.rs", "Handle", "open")],
        ..Policy::EMPTY
    };
    let source = r#"
        struct Database;
        struct Child { database: Database }
        impl Child { fn new(database: Database) -> Self { Self { database } } }
        struct Handle { child: Child }
        impl Handle { fn open(database: Database) -> Self { Self { child: Child::new(database) } } }
    "#;
    let check = |path: &str| {
        let files = vec![RustFile::fixture(path, source)];
        let structs = crate::syntax::collect_structs(&files);
        let owners = infer_owners(&structs, &ROOTED);
        let constructors = collect_constructors(&files, &owners);
        let free_constructors = collect_free_constructors(&files, &owners);
        find_owner_construction_violations(
            &files,
            &owners,
            &constructors,
            &free_constructors,
            &ROOTED,
        )
    };
    assert!(check("crates/coven/src/builder.rs").is_empty());
    assert_eq!(check("crates/coven/src/elsewhere.rs").len(), 1);
}

#[test]
fn an_owner_built_inside_a_macro_call_is_rejected() {
    let violations = violations(
        r#"
        struct Database;
        struct Child { database: Database }
        impl Child { fn new(database: Database) -> Self { Self { database } } }
        struct Parent { children: Vec<Child> }
        impl Parent {
            fn new(database: Database) -> Self { Self { children: vec![Child::new(database)] } }
        }
        "#,
    );
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].parent, "Parent::new");
    assert_eq!(violations[0].child, "Child");
}

#[test]
fn an_owner_built_inside_a_macro_rules_body_is_rejected() {
    let violations = violations(
        r#"
        struct Database;
        struct Child { database: Database }
        impl Child { fn new(database: Database) -> Self { Self { database } } }
        struct Parent { first: Child, second: Child }
        impl Parent {
            fn new(database: Database, other: Database) -> Self {
                macro_rules! child {
                    ($database:expr) => { Child::new($database) };
                    (literal $database:expr) => { Child { database: $database } };
                }
                Self { first: child!(database), second: child!(literal other) }
            }
        }
        "#,
    );
    assert_eq!(violations.len(), 2);
    assert!(violations
        .iter()
        .all(|violation| violation.child == "Child"));
}
