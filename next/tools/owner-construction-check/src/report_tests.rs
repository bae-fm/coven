use super::*;
use crate::owner_policy::POLICY;

fn workspace(files: Vec<RustFile>) -> Workspace {
    Workspace {
        files,
        manifests: Vec::new(),
        workspace_dependencies: toml::Table::new(),
    }
}

#[test]
fn an_empty_workspace_passes() {
    assert!(check(&workspace(Vec::new()), &POLICY).is_empty());
}

#[test]
fn tools_answer_to_the_conventions_but_not_to_the_capability_table() {
    let report = check(
        &workspace(vec![RustFile::fixture(
            "tools/report/src/main.rs",
            r#"
            use std::fs;
            pub(in crate::sources) fn read() {}
            "#,
        )]),
        &POLICY,
    );
    assert!(report.capability_boundaries.is_empty());
    assert_eq!(report.conventions.len(), 1);
    assert_eq!(
        report.lines(),
        vec![
            "tools/report/src/main.rs:3: pub(in path) visibility is forbidden".to_string(),
            "move an item needed elsewhere to where both callers can see it".to_string(),
        ],
    );
}

#[test]
fn crates_answer_to_every_rule() {
    let report = check(
        &workspace(vec![RustFile::fixture(
            "crates/coven-merge/src/merge.rs",
            "fn now() { let _ = std::time::SystemTime::now(); }",
        )]),
        &POLICY,
    );
    assert_eq!(
        report.lines(),
        vec![
            "crates/coven-merge/src/merge.rs:1: system clock (current time) is used directly only in crates/coven-foundation/src/clock.rs".to_string(),
            "reach a capability through the owner that holds it, given to you when you are built".to_string(),
        ],
    );
}
