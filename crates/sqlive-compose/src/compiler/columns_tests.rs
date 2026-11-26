use crate::compiler::crate_compile::tests::{code_of, compile_lib, messages};

#[test]
fn a_rowid_is_no_option_and_tells_a_missing_row_from_a_row() {
    let output = compile_lib(
        r##"
sqlive_compose::fragment!(Verdict, r#"SELECT v.verdict_id FROM verdicts AS v"#);
"##,
    )
    .unwrap();
    let code = code_of(&output, "Verdict.rs");
    assert!(code.contains("pub (crate) verdict_id : i64"), "{code}");
    let manifest: serde_json::Value = serde_json::from_str(&output.manifest).unwrap();
    assert_eq!(manifest["fragments"][0]["presence"], 0);
}

#[test]
fn a_left_join_gives_a_rowid_null() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::read!(Judged { verdict_id: i64 }, r#"
    SELECT v.verdict_id FROM issues AS i LEFT JOIN verdicts AS v ON v.kind = i.state
"#);
"##,
    ));
    assert_eq!(
        errors,
        ["src/lib.rs:2:1: verdict_id can be NULL, and the header types it i64, which holds no NULL"]
    );
}
