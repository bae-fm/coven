use super::*;
use crate::crate_code::checked;
use crate::policy::Policy;

fn policy() -> Policy {
    Policy::fixture(
        r#"
        rules = ["read-shapes"]
        [database]
        capability = "SQLite"
        test_support_features = ["test-utils"]
        [read_shapes]
        read_types = ["SqlReadContext"]
        rule = "the read rule"
        "#,
    )
}

const CONSTANTS: &str = r#"
    const COST: u64 = 7;
    const LIMIT: usize = 100;
    const LABEL: &str = "x";
    fn threshold(kept: u64) -> u64 { LIMIT as u64 * kept }
    fn plain(kept: u64) -> u64 { kept + 1 }
"#;

/// The cutoffs found in a read function with this body, and in the helper
/// functions given.
fn cutoffs_in(body: &str, outside: &str) -> Vec<String> {
    let source = format!(
        "{CONSTANTS}\nfn read(sql: &SqlReadContext<'_>, rows: Vec<u8>, found: u64, kept: u64, passed: u64, total: u64) {{ {body} }}\n{outside}"
    );
    let files = [RustFile::fixture("crates/app/src/a.rs", &source)];
    let files = checked(&files);
    let policy = policy();
    let read = ReadCode::collect(&files, &policy, policy.read_shapes.as_ref().expect("rows"));
    let mut found = cutoffs(&read, &files)
        .into_iter()
        .map(|hit| {
            assert_eq!(hit.shape.id(), "cutoff");
            assert_eq!(hit.item, "read");
            hit.what
        })
        .collect::<Vec<_>>();
    found.sort();
    found
}

#[test]
fn a_comparison_against_a_constant_expression_as_a_match_scrutinee_is_found() {
    let found =
        cutoffs_in("match COST * kept * kept < passed * total { true => {}, false => {} }", "");
    assert_eq!(found, ["compares with `<` against the constant COST"]);
}

#[test]
fn a_comparison_with_a_call_to_a_function_naming_a_constant_is_found() {
    let found = cutoffs_in("if found <= threshold(kept) {}", "");
    assert_eq!(found, ["compares with `<=` against the constant LIMIT in `threshold`"]);
    assert_eq!(cutoffs_in("if found <= plain(kept) {}", ""), Vec::<String>::new());
}

#[test]
fn a_comparison_with_an_integer_literal_of_two_or_more_is_found() {
    assert_eq!(
        cutoffs_in("if rows.len() > 500 {} if rows.len() >= 2 {}", ""),
        ["compares with `>=` against the literal 2", "compares with `>` against the literal 500"]
    );
}

#[test]
fn comparisons_with_nothing_fixed_are_not_found() {
    assert_eq!(
        cutoffs_in(
            "if rows.is_empty() {} if rows.len() > 0 {} if rows.len() >= 1 {} if found < kept {}
             if found == 5 {} if found > LABEL.len() as u64 {} let _ = found + 5;",
            ""
        ),
        Vec::<String>::new()
    );
}

#[test]
fn the_same_comparison_outside_read_code_is_not_found() {
    let files = [RustFile::fixture(
        "crates/app/src/a.rs",
        "const LIMIT: usize = 100; fn write(rows: Vec<u8>) { if rows.len() > 500 || rows.len() > LIMIT {} }",
    )];
    let files = checked(&files);
    let policy = policy();
    let read = ReadCode::collect(&files, &policy, policy.read_shapes.as_ref().expect("rows"));
    assert!(cutoffs(&read, &files).is_empty());
}
