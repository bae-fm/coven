use super::*;
use crate::test_code::production_code;

/// The rule as the checker runs it: on the files' production code.
fn find_unique_type_name_violations(files: &[RustFile], policy: &Policy) -> Vec<Finding> {
    let files = production_code(files);
    super::find_unique_type_name_violations(&files, policy, &OwnerGraph::collect(&files, policy))
}

fn duplicate_at(name: &str, paths: &[&str]) -> Vec<Finding> {
    vec![Finding::new(paths[0], 1, format!("type {name} is declared more than once: {}", paths.join(", ")),
        "type names used by the policy, construction-only capabilities and inferred owners must be unique across the workspace's crates")]
}

const FIRST: &str = "crates/first/src/lib.rs";
const SECOND: &str = "crates/second/src/lib.rs";

fn violations(first: &str, second: &str, policy: &Policy) -> Vec<Finding> {
    find_unique_type_name_violations(
        &[RustFile::fixture(FIRST, first), RustFile::fixture(SECOND, second)],
        policy,
    )
}

fn duplicate(name: &str) -> Vec<Finding> {
    duplicate_at(name, &[FIRST, SECOND])
}

#[test]
fn every_type_bearing_policy_row_requires_unique_names() {
    let policies = [
        r#"capability_types = ["Subject"]"#.to_string(),
        r#"capability_traits = ["Subject"]"#.to_string(),
        r#"construction_only_capability_types = ["Subject"]"#.to_string(),
        r#"non_owner_types = ["Subject"]"#.to_string(),
        r#"borrowed_facade_types = ["Subject"]"#.to_string(),
        r#"task_types = ["Subject"]"#.to_string(),
        r#"internal_dependency_types = ["Subject"]"#.to_string(),
        r#"closed_session_types = ["Subject"]"#.to_string(),
        format!("[composition_roots]\n{FIRST:?} = [\"Subject::open\"]"),
        format!("[capability_factories.{FIRST:?}]\n\"Subject::build\" = \"Product\""),
        format!("[capability_factories.{FIRST:?}]\n\"Factory::build\" = \"Subject\""),
    ]
    .map(|rows| Policy::fixture(&rows));
    for policy in policies {
        assert_eq!(
            violations("struct Subject;", "struct Subject;", &policy),
            duplicate("Subject"),
            "policy rows: {:?}",
            policy.named_types(),
        );
    }
}

#[test]
fn all_declaration_kinds_count_including_nested_modules() {
    let policy = Policy::fixture(r#"capability_types = ["Subject"]"#);
    let declarations = [
        "struct Subject;",
        "enum Subject { Value }",
        "union Subject { value: u64 }",
        "trait Subject {}",
        "type Subject = u64;",
    ];
    for first in declarations {
        for second in declarations {
            assert_eq!(
                violations(first, &format!("mod outer {{ mod inner {{ {second} }} }}"), &policy),
                duplicate("Subject"),
                "{first} / {second}",
            );
        }
    }
}

#[test]
fn capability_trait_implementations_need_no_explicit_type_row() {
    let policy = Policy::fixture(r#"capability_traits = ["Clock"]"#);
    let source = r#"
        trait Clock {}
        #[cfg(feature = "test-utils")]
        mod fakes {
            struct SuppliedClock<T>(T);
            impl<T> foundation::Clock for SuppliedClock<T> {}
        }
    "#;
    let mut expected = duplicate("SuppliedClock");
    expected[0].line = 5;
    assert_eq!(violations(source, "struct SuppliedClock;", &policy), expected);
}

#[test]
fn inferred_owners_include_aliases_enums_unions_and_transitive_holders() {
    let policy = Policy::fixture(r#"capability_types = ["Clock"]"#);
    for owner in [
        "struct Worker { clock: Box<dyn Clock> }",
        "enum Worker { Active(Box<dyn Clock>) }",
        "union Worker { clock: *const dyn Clock }",
        "type Worker = Box<dyn Clock>;",
        "struct Inner { clock: Box<dyn Clock> } struct Worker { inner: Inner }",
    ] {
        let source = format!("trait Clock {{}} {owner}");
        assert_eq!(violations(&source, "struct Worker;", &policy), duplicate("Worker"), "{owner}",);
        // Input order cannot let a plain value overwrite an owner's fields.
        assert_eq!(violations("struct Worker;", &source, &policy), duplicate("Worker"), "{owner}",);
    }
}

#[test]
fn test_only_items_and_their_nested_declarations_do_not_count() {
    let policy = Policy::fixture(r#"capability_types = ["Subject"]"#);
    for source in [
        "#[cfg(test)] struct Subject;",
        "#[cfg(test)] enum Subject {}",
        "#[cfg(test)] union Subject { value: u64 }",
        "#[cfg(test)] trait Subject {}",
        "#[cfg(test)] type Subject = u64;",
        "#[cfg(all(test, unix))] mod nested { struct Subject; }",
        "#[test] fn fixture() { struct Subject; }",
        "#[tokio::test] async fn fixture() { struct Subject; }",
        "#[cfg(test)] impl Fixture { fn fixture() { struct Subject; } }",
        "impl Fixture { #[cfg(test)] fn fixture() { struct Subject; } }",
        "impl Fixture { #[cfg(test)] const VALUE: () = { struct Subject; }; }",
        "trait Fixture { #[cfg(test)] fn fixture() { struct Subject; } }",
        "trait Fixture { #[cfg(test)] const VALUE: () = { struct Subject; }; }",
        "#[cfg(test)] const VALUE: () = { struct Subject; };",
        "#[cfg(test)] static VALUE: () = { struct Subject; };",
        "#[cfg(test)] declare! { struct Subject; }",
        "impl Fixture { #[cfg(test)] declare! { fn fixture() { struct Subject; } } }",
        "trait Fixture { #[cfg(test)] declare! { fn fixture() { struct Subject; } } }",
        "#![cfg(test)]\nstruct Subject;",
    ] {
        assert!(violations("struct Subject;", source, &policy).is_empty(), "{source}");
    }
}

#[test]
fn test_sources_do_not_count() {
    let policy = Policy::fixture(r#"capability_types = ["Subject"]"#);
    for path in [
        "crates/second/src/lib_tests.rs",
        "crates/second/tests/integration.rs",
        "crates/second/tests/support/fixture.rs",
    ] {
        assert!(
            find_unique_type_name_violations(
                &[
                    RustFile::fixture(FIRST, "struct Subject;"),
                    RustFile::fixture(path, "struct Subject;"),
                ],
                &policy,
            )
            .is_empty(),
            "{path}"
        );
    }
}

#[test]
fn feature_gates_that_allow_production_count() {
    let policy = Policy::fixture(r#"capability_types = ["Subject"]"#);
    for attribute in [
        "#[cfg(feature = \"test-utils\")]",
        "#[cfg(any(test, feature = \"test-utils\"))]",
        "#[cfg(not(test))]",
        "#[cfg(unix)]",
    ] {
        for source in [
            format!("{attribute} struct Subject;"),
            format!("{attribute} mod nested {{ struct Subject; }}"),
        ] {
            assert_eq!(
                violations("struct Subject;", &source, &policy),
                duplicate("Subject"),
                "{source}"
            );
        }
    }
}

#[test]
fn declarations_inside_functions_and_macro_arguments_count() {
    let policy = Policy::fixture(r#"capability_types = ["Subject"]"#);
    for source in ["fn run() { struct Subject; }", "declare! { struct Subject; }"] {
        assert_eq!(
            violations("struct Subject;", source, &policy),
            duplicate("Subject"),
            "{source}"
        );
    }
}

#[test]
fn repeated_declarations_in_one_file_are_not_collapsed_before_counting() {
    let policy = Policy::fixture(r#"capability_types = ["Subject"]"#);
    assert_eq!(
        violations("mod first { struct Subject; } mod second { struct Subject; }", "", &policy),
        duplicate_at("Subject", &[FIRST]),
    );
}

#[test]
fn every_declaring_file_is_reported_in_path_order() {
    let policy = Policy::fixture(r#"capability_types = ["Subject"]"#);
    let third = "crates/third/src/lib.rs";
    assert_eq!(
        find_unique_type_name_violations(
            &[
                RustFile::fixture(third, "struct Subject;"),
                RustFile::fixture(SECOND, "struct Subject;"),
                RustFile::fixture(FIRST, "struct Subject;"),
            ],
            &policy,
        ),
        duplicate_at("Subject", &[FIRST, SECOND, third]),
    );
}

#[test]
fn imports_and_reexports_are_not_declarations() {
    let policy = Policy::fixture(r#"capability_types = ["Subject"]"#);
    assert!(violations("struct Subject;", "pub use first::Subject;", &policy).is_empty());
}
