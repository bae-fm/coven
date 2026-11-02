use super::*;
use crate::sources::Manifest;

fn policy() -> Policy {
    Policy::fixture(
        r#"
        tools = ["report"]
        crate_order = [
            "app-foundation",
            "app-crypto",
            "app-format",
            "app-merge",
            "app-database",
            "app-storage",
            "app-sync",
            "app",
        ]
        separated_crates = [["app-database", "app-storage"]]
        "#,
    )
}

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
            app-foundation = { path = "crates/app-foundation" }
            foundation = { path = "crates/app-foundation", package = "app-foundation" }
            app-sync = { path = "crates/app-sync" }
            app-storage = { path = "crates/app-storage" }
            app-database = { path = "crates/app-database" }
            "#
        .parse()
        .expect("parse fixture workspace dependencies"),
    }
}

fn violations(manifests: Vec<Manifest>) -> Vec<Finding> {
    find_crate_dependency_violations(&workspace(manifests), &policy())
}

#[test]
fn a_crate_may_depend_on_the_crates_above_it() {
    assert!(violations(vec![manifest(
        "crates/app-sync",
        "app-sync",
        r#"
        [dependencies]
        app-foundation.workspace = true
        app-database = { workspace = true, features = ["test-utils"] }
        app-storage = { workspace = true, optional = true }
        "#,
    )])
    .is_empty());
}

#[test]
fn a_crate_may_not_depend_on_a_crate_below_it_even_for_tests() {
    assert_eq!(
        violations(vec![manifest(
            "crates/app-foundation",
            "app-foundation",
            r#"
            [dev-dependencies]
            app-sync.workspace = true
            "#,
        )]),
        vec![Finding::new(
            "crates/app-foundation/Cargo.toml",
            1,
            "app-foundation depends on app-sync, which is not above it in crate_order",
            REMEDY
        )],
    );
}

#[test]
fn the_database_and_storage_never_depend_on_each_other() {
    let violations = violations(vec![
        manifest(
            "crates/app-storage",
            "app-storage",
            "[dependencies]\napp-database.workspace = true\n",
        ),
        manifest(
            "crates/app-database",
            "app-database",
            "[target.'cfg(unix)'.dependencies]\napp-storage.workspace = true\n",
        ),
    ]);
    assert_eq!(
        violations,
        [("app-database", "app-storage"), ("app-storage", "app-database"),].map(|(from, to)| {
            Finding::new(
                &format!("crates/{from}/Cargo.toml"),
                1,
                format!("{from} depends on {to}; the two never depend on each other"),
                REMEDY,
            )
        })
    );
}

#[test]
fn a_renamed_workspace_dependency_resolves_to_its_package() {
    assert_eq!(
        violations(vec![manifest(
            "crates/app-crypto",
            "app-crypto",
            "[dependencies]\nfoundation.workspace = true\n",
        )]),
        Vec::new(),
    );
    assert_eq!(
        violations(vec![manifest(
            "crates/app-foundation",
            "app-foundation",
            "[dependencies]\nfoundation.workspace = true\n",
        )]),
        [Finding::new(
            "crates/app-foundation/Cargo.toml",
            1,
            "app-foundation depends on app-foundation, which is not above it in crate_order",
            REMEDY
        )],
    );
}

#[test]
fn a_crate_without_a_row_is_unplaced() {
    assert_eq!(
        violations(vec![manifest("crates/app-extra", "app-extra", "")]),
        vec![Finding::new(
            "crates/app-extra/Cargo.toml",
            1,
            "crate app-extra has no row in the policy's crate_order",
            REMEDY
        )],
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
        .map(|violation| violation.message.split(' ').nth(1).expect("dependency name"))
        .collect::<BTreeSet<_>>();
    assert_eq!(named, BTreeSet::from(["cc", "local", "proc-macro2", "quote", "syn"]));
}
