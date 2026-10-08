use super::*;

#[test]
fn forbidden_syntax_has_a_finding_and_a_remedy() {
    for (path, source, rule) in [
        (
            "crates/coven-sync/src/fixture.rs",
            "use super::super::Sibling;",
            Convention::DeepParentPath,
        ),
        (
            "crates/coven-sync/src/fixture.rs",
            "fn call() { super::super::run(); }",
            Convention::DeepParentPath,
        ),
        (
            "crates/coven-sync/src/fixture.rs",
            "pub(in crate::sync) struct Hidden;",
            Convention::RestrictedVisibility,
        ),
        (
            "crates/coven/src/lib.rs",
            "pub mod rows;",
            Convention::PublicApiModule,
        ),
        (
            "crates/coven/src/lib.rs",
            "pub(crate) mod rows;",
            Convention::PublicApiModule,
        ),
        (
            "tools/check/src/main.rs",
            "macro_rules! m { () => { pub(in crate::sync) struct Hidden; }; }",
            Convention::RestrictedVisibility,
        ),
        (
            "crates/coven-sync/src/fixture.rs",
            "fn run() { assert!(super::super::ready()); }",
            Convention::DeepParentPath,
        ),
    ] {
        assert_eq!(
            find_convention_violations(&[RustFile::fixture(path, source)]),
            [rule.finding(path, 1)]
        );
    }
}

#[test]
fn local_visibility_and_reexports_pass() {
    for (path, source) in [
        ("crates/coven-sync/src/fixture.rs", "use super::Sibling; fn call() { super::run(); } pub(crate) struct A; pub(super) struct B; pub(self) struct C;"),
        ("crates/coven/src/lib.rs", "mod rows; pub use rows::Row;"),
        ("crates/coven-sync/src/lib.rs", "pub mod sync_loop;"),
    ] {
        assert!(find_convention_violations(&[RustFile::fixture(path, source)]).is_empty());
    }
}

#[test]
fn file_length_applies_to_crates_tools_and_tests() {
    for path in ["crates/coven-sync/src/long.rs", "tools/check/tests/long.rs"] {
        let source = |lines| "fn f() {}\n".repeat(lines);
        assert!(find_convention_violations(&[RustFile::fixture(path, &source(1000))]).is_empty());
        let found = find_convention_violations(&[RustFile::fixture(path, &source(1001))]);
        assert_eq!(
            found,
            [Convention::LongFile { lines: 1001 }.finding(path, 1001)]
        );
        assert_eq!(
            found[0].message,
            "holds 1001 lines; a source file holds at most 1000"
        );
    }
}
