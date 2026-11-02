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
