use super::*;

fn conventions(path: &str, source: &str) -> Vec<Convention> {
    find_convention_violations(&[RustFile::fixture(path, source)])
        .into_iter()
        .map(|violation| violation.convention)
        .collect()
}

#[test]
fn paths_cannot_skip_over_their_parent_module() {
    let violations = find_convention_violations(&[RustFile::fixture(
        "crates/coven-sync/src/fixture.rs",
        r#"
        use super::super::Sibling;
        fn call() { super::super::run(); }
        "#,
    )]);
    assert_eq!(violations.len(), 2);
    assert!(violations
        .iter()
        .all(|violation| violation.convention == Convention::DeepParentPath));
}

#[test]
fn a_single_parent_step_is_allowed() {
    assert!(conventions(
        "crates/coven-sync/src/fixture.rs",
        "use super::Sibling; fn call() { super::run(); }",
    )
    .is_empty());
}

#[test]
fn restricted_path_visibility_is_rejected() {
    assert_eq!(
        conventions(
            "tools/owner-construction-check/src/fixture.rs",
            r#"
            pub(in crate::sync) struct Hidden;
            pub(crate) struct Crate;
            pub(super) struct Parent;
            pub(self) struct Private;
            "#,
        ),
        vec![Convention::RestrictedVisibility],
    );
}

#[test]
fn restricted_visibility_and_deep_parents_inside_macros_are_rejected() {
    let violations = conventions(
        "crates/coven-sync/src/fixture.rs",
        r#"
        macro_rules! declare {
            () => {
                pub(in crate::sync) struct Hidden;
            };
        }
        fn call() { assert!(super::super::ready()); }
        "#,
    );
    assert_eq!(
        violations,
        vec![Convention::RestrictedVisibility, Convention::DeepParentPath],
    );
}

#[test]
fn the_api_crate_keeps_its_modules_private() {
    assert_eq!(
        conventions(
            "crates/coven/src/lib.rs",
            r#"
            mod builder;
            pub mod rows;
            pub(crate) mod files;
            pub use builder::Builder;
            "#,
        ),
        vec![Convention::PublicApiModule, Convention::PublicApiModule],
    );
    assert!(conventions("crates/coven-sync/src/lib.rs", "pub mod sync_loop;").is_empty());
}
