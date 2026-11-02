use super::*;

#[test]
fn forbidden_syntax_has_a_finding_and_a_remedy() {
    for (path, source, rule) in [
        (
            "crates/app-sync/src/fixture.rs",
            "use super::super::Sibling;",
            Convention::DeepParentPath,
        ),
        (
            "crates/app-sync/src/fixture.rs",
            "fn call() { super::super::run(); }",
            Convention::DeepParentPath,
        ),
        (
            "crates/app-sync/src/fixture.rs",
            "pub(in crate::sync) struct Hidden;",
            Convention::RestrictedVisibility,
        ),
        (
            "tools/check/src/main.rs",
            "macro_rules! m { () => { pub(in crate::sync) struct Hidden; }; }",
            Convention::RestrictedVisibility,
        ),
        (
            "crates/app-sync/src/fixture.rs",
            "fn run() { assert!(super::super::ready()); }",
            Convention::DeepParentPath,
        ),
    ] {
        assert_eq!(
            find_convention_violations(&[RustFile::fixture(path, source)], Some("app")),
            [rule.finding(path, 1)]
        );
    }
}

#[test]
fn the_api_crate_keeps_every_module_private() {
    for source in ["pub mod rows;", "pub(crate) mod rows;"] {
        assert_eq!(
            find_convention_violations(
                &[RustFile::fixture("crates/app/src/lib.rs", source)],
                Some("app")
            ),
            [Finding::new(
                "crates/app/src/lib.rs",
                1,
                "the app crate keeps every module private and re-exports its API at its root",
                PUBLIC_API_MODULE_REMEDY,
            )]
        );
        for (path, api_crate) in [
            ("crates/app/src/lib.rs", None),
            ("crates/app-sync/src/lib.rs", Some("app")),
            ("crates/app/tests/support.rs", Some("app")),
        ] {
            assert!(
                find_convention_violations(&[RustFile::fixture(path, source)], api_crate)
                    .is_empty(),
                "{path}: {source}"
            );
        }
    }
}

#[test]
fn local_visibility_and_reexports_pass() {
    for (path, source) in [
        ("crates/app-sync/src/fixture.rs", "use super::Sibling; fn call() { super::run(); } pub(crate) struct A; pub(super) struct B; pub(self) struct C;"),
        ("crates/app/src/lib.rs", "mod rows; pub use rows::Row;"),
        ("crates/app-sync/src/lib.rs", "pub mod sync_loop;"),
    ] {
        assert!(find_convention_violations(&[RustFile::fixture(path, source)], Some("app"))
            .is_empty());
    }
}

#[test]
fn file_length_applies_to_crates_tools_and_tests() {
    for path in ["crates/app-sync/src/long.rs", "tools/check/tests/long.rs"] {
        let source = |lines| "fn f() {}\n".repeat(lines);
        assert!(
            find_convention_violations(&[RustFile::fixture(path, &source(1000))], None).is_empty()
        );
        let found = find_convention_violations(&[RustFile::fixture(path, &source(1001))], None);
        assert_eq!(found, [Convention::LongFile { lines: 1001 }.finding(path, 1001)]);
        assert_eq!(found[0].message, "holds 1001 lines; a source file holds at most 1000");
    }
}
