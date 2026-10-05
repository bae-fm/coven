use super::*;

fn conventions(files: &[(&str, &str)]) -> Vec<(String, usize, Convention)> {
    let files = files
        .iter()
        .map(|(path, source)| RustFile::fixture(path, source))
        .collect::<Vec<_>>();
    find_test_layout_violations(&files)
        .into_iter()
        .map(|violation| (violation.path, violation.line, violation.convention))
        .collect()
}

#[test]
fn the_prescribed_layout_passes() {
    assert!(conventions(&[
        (
            "crates/coven-sync/src/pull.rs",
            r#"
            fn pull() {}
            #[cfg(test)]
            #[path = "pull_tests.rs"]
            mod tests;
            "#,
        ),
        (
            "crates/coven-sync/src/pull_tests.rs",
            "#[test] fn pulls() {} mod helpers { fn fixture() {} }",
        ),
        (
            "crates/coven-sync/tests/sync_tests.rs",
            "#[test] fn syncs() {}"
        ),
        (
            "crates/coven-sync/tests/two_devices.rs",
            "#[test] fn converges() {}"
        ),
    ])
    .is_empty());
}

#[test]
fn a_singular_test_file_is_rejected_with_its_rename() {
    let files = [RustFile::fixture("crates/coven-sync/src/pull_test.rs", "")];
    let violations = find_test_layout_violations(&files);
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].convention, Convention::SingularTestFile);
    assert_eq!(
        violations[0].message(),
        "test files are named <name>_tests.rs; rename pull_test.rs to pull_tests.rs"
    );
}

#[test]
fn a_test_file_without_its_subject_is_rejected() {
    assert_eq!(
        conventions(&[(
            "crates/coven-sync/src/pull_tests.rs",
            "#[test] fn pulls() {}"
        )]),
        vec![(
            "crates/coven-sync/src/pull_tests.rs".to_string(),
            1,
            Convention::OrphanTestFile
        )],
    );
}

#[test]
fn an_inline_test_module_is_rejected() {
    assert_eq!(
        conventions(&[(
            "crates/coven-sync/src/pull.rs",
            r#"
            fn pull() {}
            #[cfg(test)]
            mod tests {
                #[test]
                fn pulls() {}
            }
            mod nested {
                #[cfg(all(test, unix))]
                mod unix_tests {}
            }
            "#,
        )]),
        vec![
            (
                "crates/coven-sync/src/pull.rs".to_string(),
                4,
                Convention::InlineTestModule
            ),
            (
                "crates/coven-sync/src/pull.rs".to_string(),
                10,
                Convention::InlineTestModule
            ),
        ],
    );
}

#[test]
fn a_test_module_declared_elsewhere_than_the_sibling_is_rejected() {
    let source = r#"
        #[cfg(test)]
        mod tests;
        #[cfg(test)]
        #[path = "other_tests.rs"]
        mod tests;
        #[cfg(test)]
        #[path = "pull_tests.rs"]
        mod checks;
    "#;
    let violations = conventions(&[("crates/coven-sync/src/pull.rs", source)]);
    assert_eq!(
        violations
            .iter()
            .map(|(_, line, convention)| (*line, *convention))
            .collect::<Vec<_>>(),
        vec![
            (3, Convention::MisplacedTestModule),
            (6, Convention::MisplacedTestModule),
            (9, Convention::MisplacedTestModule),
        ],
    );
}

#[test]
fn modules_that_compile_into_production_are_not_test_modules() {
    assert!(conventions(&[(
        "crates/coven-sync/src/pull.rs",
        r#"
        #[cfg(not(test))]
        mod live {}
        #[cfg(feature = "test-utils")]
        mod fakes {}
        #[cfg(any(test, feature = "test-utils"))]
        mod shared {}
        "#,
    )])
    .is_empty());
}
