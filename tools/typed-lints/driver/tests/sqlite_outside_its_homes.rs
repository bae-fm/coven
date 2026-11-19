//! `clippy::sqlite_outside_its_homes` run by the driver, as cargo runs it,
//! over the fixture package `fixtures/sqlite/`, whose policy keeps SQLite in
//! `src/store.rs`: the lint finds what each line ending in a `finding`
//! comment holds, and nothing else, in production code outside the home; it
//! finds nothing in test code; and the fixture's mark holds.

mod support;

use support::{Fixture, extern_library, out_dir};

fn compiled_rusqlite(out_dir: &std::path::Path) -> Vec<String> {
    let rusqlite = Fixture { source: "rusqlite.rs", crate_name: "rusqlite", manifest_dir: "." };
    rusqlite.compile(out_dir, ["--cap-lints=allow"]);
    extern_library("rusqlite", out_dir)
}

#[test]
fn finds_sqlite_reached_outside_its_homes_and_nothing_else() {
    let out_dir = out_dir("sqlite_outside_its_homes");
    let rusqlite = compiled_rusqlite(&out_dir);
    let fixture =
        Fixture { source: "sqlite/src/lib.rs", crate_name: "sqlite", manifest_dir: "sqlite" };
    let test_builds = ["--cfg", "test", "--cfg", "feature=\"test-utils\""].map(String::from);
    let diagnostics = fixture.compile(&out_dir, rusqlite.into_iter().chain(test_builds));
    fixture.assert_findings("clippy::sqlite_outside_its_homes", &diagnostics);
}

#[test]
fn finds_nothing_in_a_test_under_the_packages_tests_folder() {
    let out_dir = out_dir("sqlite_outside_its_homes_tests");
    let rusqlite = compiled_rusqlite(&out_dir);
    let fixture = Fixture {
        source: "sqlite/tests/opens_its_own.rs",
        crate_name: "opens_its_own",
        manifest_dir: "sqlite",
    };
    let diagnostics = fixture.compile(&out_dir, rusqlite);
    fixture.assert_findings("clippy::sqlite_outside_its_homes", &diagnostics);
}
