use super::*;

const POLICY: Policy = Policy {
    crate_order: &[
        "coven-foundation",
        "coven-crypto",
        "coven-format",
        "coven-merge",
        "coven-database",
        "coven-storage",
        "coven-sync",
        "coven",
    ],
    separated_crates: &[("coven-database", "coven-storage")],
    ..Policy::EMPTY
};

fn manifest(directory: &str, package: &str, dependencies: &str) -> Manifest {
    Manifest {
        relative_path: format!("{directory}/Cargo.toml"),
        table: format!("[package]\nname = \"{package}\"\n\n{dependencies}")
            .parse()
            .expect("parse fixture manifest"),
    }
}

fn workspace(manifests: Vec<Manifest>) -> Workspace {
    Workspace {
        files: Vec::new(),
        manifests,
        workspace_dependencies: r#"
            syn = "3"
            coven-foundation = { path = "crates/coven-foundation" }
            foundation = { path = "crates/coven-foundation", package = "coven-foundation" }
            coven-sync = { path = "crates/coven-sync" }
            coven-storage = { path = "crates/coven-storage" }
            coven-database = { path = "crates/coven-database" }
            "#
        .parse()
        .expect("parse fixture workspace dependencies"),
    }
}

fn violations(manifests: Vec<Manifest>) -> Vec<CrateDependencyViolation> {
    find_crate_dependency_violations(&workspace(manifests), &POLICY)
}

#[test]
fn a_crate_may_depend_on_the_crates_above_it() {
    assert!(violations(vec![manifest(
        "crates/coven-sync",
        "coven-sync",
        r#"
        [dependencies]
        coven-foundation.workspace = true
        coven-database = { workspace = true, features = ["test-utils"] }
        coven-storage = { workspace = true, optional = true }
        "#,
    )])
    .is_empty());
}

#[test]
fn a_crate_may_not_depend_on_a_crate_below_it_even_for_tests() {
    assert_eq!(
        violations(vec![manifest(
            "crates/coven-foundation",
            "coven-foundation",
            r#"
            [dev-dependencies]
            coven-sync.workspace = true
            "#,
        )]),
        vec![CrateDependencyViolation::Upward {
            manifest: "crates/coven-foundation/Cargo.toml".to_string(),
            from: "coven-foundation".to_string(),
            to: "coven-sync".to_string(),
        }],
    );
}

#[test]
fn the_database_and_storage_never_depend_on_each_other() {
    let violations = violations(vec![
        manifest(
            "crates/coven-storage",
            "coven-storage",
            "[dependencies]\ncoven-database.workspace = true\n",
        ),
        manifest(
            "crates/coven-database",
            "coven-database",
            "[target.'cfg(unix)'.dependencies]\ncoven-storage.workspace = true\n",
        ),
    ]);
    assert_eq!(violations.len(), 2);
    assert!(violations
        .iter()
        .all(|violation| matches!(violation, CrateDependencyViolation::Separated { .. })));
}

#[test]
fn a_renamed_workspace_dependency_resolves_to_its_package() {
    assert_eq!(
        violations(vec![manifest(
            "crates/coven-crypto",
            "coven-crypto",
            "[dependencies]\nfoundation.workspace = true\n",
        )]),
        Vec::new(),
    );
    assert_eq!(
        violations(vec![manifest(
            "crates/coven-foundation",
            "coven-foundation",
            "[dependencies]\nfoundation.workspace = true\n",
        )])
        .len(),
        1,
    );
}

#[test]
fn a_crate_without_a_row_is_unplaced() {
    assert_eq!(
        violations(vec![manifest("crates/coven-extra", "coven-extra", "")]),
        vec![CrateDependencyViolation::Unplaced {
            manifest: "crates/coven-extra/Cargo.toml".to_string(),
            package: "coven-extra".to_string(),
        }],
    );
    assert!(violations(vec![manifest("tools/report", "report", "")]).is_empty());
}

#[test]
fn members_take_every_dependency_from_the_workspace() {
    let violations = violations(vec![manifest(
        "tools/report",
        "report",
        r#"
        [dependencies]
        syn = "3"
        quote = { version = "1", features = ["proc-macro"] }
        local = { path = "../local" }
        proc-macro2 = { workspace = true, default-features = false }
        toml.workspace = true

        [build-dependencies]
        cc = { workspace = false }
        "#,
    )]);
    let named = violations
        .iter()
        .map(|violation| match violation {
            CrateDependencyViolation::NotFromWorkspace { dependency, .. } => dependency.as_str(),
            other => panic!("unexpected finding: {other:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        named,
        BTreeSet::from(["cc", "local", "proc-macro2", "quote", "syn"])
    );
}
