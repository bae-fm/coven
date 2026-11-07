use super::*;

#[test]
fn joined_module_paths_match_source_keys_on_every_platform() {
    // Source keys always use `/`; a joined path must too, whatever the native separator.
    let directory = Path::new("crates/app-storage/src").join("providers");
    assert_eq!(
        normalize(&directory.join("s3_error.rs")),
        "crates/app-storage/src/providers/s3_error.rs"
    );
    assert_eq!(
        normalize(&directory.join("../mod_tests.rs")),
        "crates/app-storage/src/mod_tests.rs"
    );
    assert_eq!(normalize(&directory.join("./a/../b.rs")), "crates/app-storage/src/providers/b.rs");
}

#[test]
fn files_only_test_only_declarations_reach_are_test_modules() {
    let files = [
        (
            "crates/app/src/lib.rs",
            r#"mod work; #[cfg(test)] mod tests;
            #[cfg(test)] #[path = "both.rs"] mod both_in_tests; #[path = "both.rs"] mod both;"#,
        ),
        (
            "crates/app/src/work.rs",
            "#[cfg(test)] mod tests; mod inner { #[cfg(all(test, unix))] mod deep; }",
        ),
        ("crates/app/src/work/tests.rs", ""),
        ("crates/app/src/work/inner/deep.rs", ""),
        ("crates/app/src/tests.rs", "mod helpers;"),
        ("crates/app/src/tests/helpers.rs", ""),
        ("crates/app/src/both.rs", ""),
        ("crates/app/src/unreached.rs", ""),
    ]
    .map(|(path, source)| RustFile::fixture(path, source));
    assert_eq!(
        test_module_files(&files),
        [
            "crates/app/src/tests.rs",
            "crates/app/src/tests/helpers.rs",
            "crates/app/src/work/inner/deep.rs",
            "crates/app/src/work/tests.rs",
        ]
        .map(str::to_string)
        .into()
    );
}

/// `include!` makes the included file's items the includer's, child modules
/// and all, so an included file and what it declares belong where the call is.
#[test]
fn included_files_belong_to_the_including_module() {
    let files = [
        ("crates/app/src/lib.rs", "mod work; #[cfg(test)] mod tests;"),
        ("crates/app/src/work.rs", r#"include!("work/parts.rs");"#),
        ("crates/app/src/work/parts.rs", "mod nested; fn part() {}"),
        ("crates/app/src/work/nested.rs", ""),
        ("crates/app/src/tests.rs", r#"include!("tests/list.rs");"#),
        ("crates/app/src/tests/list.rs", "mod case;"),
        ("crates/app/src/tests/case.rs", ""),
    ]
    .map(|(path, source)| RustFile::fixture(path, source));
    let modules = module_sources(&files);
    assert_eq!(modules["crates/app/src/work/parts.rs"], [vec!["app", "work"]]);
    assert_eq!(modules["crates/app/src/work/nested.rs"], [vec!["app", "work", "nested"]]);
    assert_eq!(
        test_module_files(&files),
        ["crates/app/src/tests.rs", "crates/app/src/tests/case.rs", "crates/app/src/tests/list.rs"]
            .map(str::to_string)
            .into()
    );
}

/// rustc resolves a module declared in an included file against that file's
/// directory, `#[path]` and plain declarations alike, not the includer's.
#[test]
fn modules_an_included_file_declares_resolve_from_its_directory() {
    let files = [
        ("crates/app/src/lib.rs", "mod work; #[cfg(test)] #[path = \"lib_tests.rs\"] mod tests;"),
        ("crates/app/src/work.rs", r#"include!("work/parts/listed.rs");"#),
        ("crates/app/src/work/parts/listed.rs", r#"#[path = "pathed.rs"] mod pathed; mod plain;"#),
        ("crates/app/src/work/parts/pathed.rs", ""),
        ("crates/app/src/work/parts/plain.rs", ""),
        ("crates/app/src/lib_tests.rs", r#"include!("cases/listed.rs");"#),
        ("crates/app/src/cases/listed.rs", r#"#[path = "round_trips.rs"] mod round_trips;"#),
        ("crates/app/src/cases/round_trips.rs", ""),
    ]
    .map(|(path, source)| RustFile::fixture(path, source));
    let modules = module_sources(&files);
    assert_eq!(modules["crates/app/src/work/parts/pathed.rs"], [vec!["app", "work", "pathed"]]);
    assert_eq!(modules["crates/app/src/work/parts/plain.rs"], [vec!["app", "work", "plain"]]);
    assert!(test_module_files(&files).contains("crates/app/src/cases/round_trips.rs"));
}

/// Cargo makes each file directly under `examples/`, `benches/` and `src/bin/`
/// a crate root, so `mod x;` in one is `x.rs` beside it, not in a directory
/// named for the root.
#[test]
fn example_bench_and_bin_files_are_crate_roots() {
    let files = [
        ("crates/app/src/lib.rs", ""),
        ("crates/app/examples/restore_bench.rs", "mod bench_library;"),
        ("crates/app/examples/bench_library.rs", ""),
        ("crates/app/benches/speed.rs", "mod fixtures;"),
        ("crates/app/benches/fixtures.rs", ""),
        ("crates/app/src/bin/tool.rs", "mod helper;"),
        ("crates/app/src/bin/helper.rs", ""),
        ("crates/app/examples/multi/main.rs", "mod part;"),
        ("crates/app/examples/multi/part.rs", ""),
    ]
    .map(|(path, source)| RustFile::fixture(path, source));
    let modules = module_sources(&files);
    assert_eq!(
        modules["crates/app/examples/bench_library.rs"],
        [vec!["app", "restore_bench", "bench_library"]]
    );
    assert_eq!(modules["crates/app/benches/fixtures.rs"], [vec!["app", "speed", "fixtures"]]);
    assert_eq!(modules["crates/app/src/bin/helper.rs"], [vec!["app", "bin", "tool", "helper"]]);
    assert_eq!(modules["crates/app/examples/multi/part.rs"], [vec!["app", "multi", "part"]]);
}

/// Two roots that declare the same file each compile it as their own module,
/// so the file has a path under each.
#[test]
fn a_file_two_roots_declare_is_a_module_of_each() {
    let files = [
        ("crates/app/src/lib.rs", ""),
        ("crates/app/examples/first.rs", "mod shared;"),
        ("crates/app/examples/second.rs", "mod shared;"),
        ("crates/app/examples/shared/mod.rs", ""),
    ]
    .map(|(path, source)| RustFile::fixture(path, source));
    assert_eq!(
        module_sources(&files)["crates/app/examples/shared/mod.rs"],
        [vec!["app", "first", "shared"], vec!["app", "second", "shared"]]
    );
}
