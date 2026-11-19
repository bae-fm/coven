use super::*;
use crate::crate_code::CrateCode;
use owner_construction_policy::CompositionRoot;

/// A policy running every rule, with the given rows.
fn policy(rows: &str) -> Policy {
    Policy::fixture(&format!(
        r#"rules = ["crate-dependencies", "capability-boundaries", "database-boundary",
            "owner-dependencies", "retained-capability-parameters", "component-bundles",
            "capability-construction", "typed-errors", "conventions"]
        {rows}"#
    ))
}

fn root(path: &str, owner: &str, method: &str) -> CompositionRoot {
    CompositionRoot { path: path.into(), owner: owner.into(), method: method.into() }
}

fn check(workspace: &Workspace, policy: &Policy) -> Report {
    check_selected(workspace, policy, &[]).unwrap()
}

/// The report on `packages`, read as the checker reads the workspace.
fn check_selected(
    workspace: &Workspace,
    policy: &Policy,
    packages: &[String],
) -> Result<Report, UnknownPackage> {
    let code = CrateCode::read(workspace, policy);
    check_packages(workspace, &code, &OwnerGraph::collect(&code.expanded, policy), policy, packages)
}

fn workspace(files: Vec<RustFile>) -> Workspace {
    Workspace {
        files,
        sql_files: Vec::new(),
        manifests: Vec::new(),
        workspace_dependencies: toml::Table::new(),
    }
}

#[test]
fn an_empty_workspace_passes() {
    assert!(check(&workspace(Vec::new()), &crate::gated_policy()).is_empty());
}

#[test]
fn duplicate_capability_and_owner_names_report_every_declaration() {
    let policy = policy(r#"capability_types = ["Clock"]"#);
    for (name, first, second) in [
        ("Clock", "trait Clock {}", "struct Clock;"),
        ("Worker", "trait Clock {} struct Worker { clock: Box<dyn Clock> }", "struct Worker;"),
    ] {
        let report = check(
            &workspace(vec![
                RustFile::fixture("crates/first/src/lib.rs", first),
                RustFile::fixture("crates/second/src/lib.rs", second),
            ]),
            &policy,
        );
        assert!(!report.is_empty(), "accepted duplicate {name}");
        assert_eq!(
            report.lines(),
            [
                format!("crates/first/src/lib.rs:1: type {name} is declared more than once: crates/first/src/lib.rs, crates/second/src/lib.rs"),
                "type names used by the policy and inferred owners must be unique across the workspace's crates".to_string(),
            ],
        );
    }
}

#[test]
fn duplicate_plain_value_names_pass() {
    let report = check(
        &workspace(vec![
            RustFile::fixture("crates/first/src/lib.rs", "struct Value(u64);"),
            RustFile::fixture("crates/second/src/lib.rs", "struct Value(String);"),
        ]),
        &policy(""),
    );
    assert!(report.is_empty(), "{:?}", report.lines());
}

#[test]
fn tool_declarations_do_not_make_crate_names_ambiguous() {
    let policy = policy(
        r#"capability_types = ["Clock"]
        tools = ["second"]"#,
    );
    let report = check(
        &workspace(vec![
            RustFile::fixture("crates/first/src/lib.rs", "trait Clock {}"),
            RustFile::fixture("tools/second/src/main.rs", "struct Clock;"),
        ]),
        &policy,
    );
    assert!(report.is_empty(), "{:?}", report.lines());
}

#[test]
fn test_only_duplicate_capability_names_pass() {
    let policy = policy(r#"capability_types = ["Clock"]"#);
    let report = check(
        &workspace(vec![
            RustFile::fixture("crates/first/src/lib.rs", "trait Clock {}"),
            RustFile::fixture("crates/second/src/lib.rs", "#[cfg(test)] struct Clock;"),
            RustFile::fixture("crates/second/src/lib_tests.rs", "struct Clock;"),
        ]),
        &policy,
    );
    assert!(report.is_empty(), "{:?}", report.lines());
}

/// Every rule but the conventions reads the same production code: a test
/// source and a test-only item may hold raw material and hand out what an
/// owner holds.
#[test]
fn test_code_answers_only_to_the_conventions() {
    let mut policy = crate::gated_policy();
    policy.capability_types.insert("DatabaseConnection".into());
    policy.internal_dependency_types.insert("DatabaseConnection".into());
    let report = check(
        &workspace(vec![
            RustFile::fixture(
                "crates/app-sync/src/lib.rs",
                r#"
                struct DatabaseConnection;
                struct Store { connection: DatabaseConnection }
                impl Store {
                    #[cfg(test)]
                    pub(crate) fn connection(&self) -> &DatabaseConnection { &self.connection }
                }
                #[cfg(test)] #[path = "lib_tests.rs"] mod tests;
                "#,
            ),
            RustFile::fixture(
                "crates/app-sync/src/lib_tests.rs",
                "use rusqlite::Connection; pub(crate) struct Probe { pub(crate) store: Store }",
            ),
        ]),
        &policy,
    );
    assert!(report.is_empty(), "{:?}", report.lines());
}

#[test]
fn tools_answer_to_the_conventions_but_not_to_the_capability_table() {
    let mut policy = crate::gated_policy();
    policy.tools.insert("report".into());
    let report = check(
        &workspace(vec![RustFile::fixture(
            "tools/report/src/main.rs",
            r#"
            use std::fs;
            pub(in crate::sources) fn read() {}
            "#,
        )]),
        &policy,
    );
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
            "crates/app-merge/src/merge.rs",
            "fn now() { let _ = std::time::SystemTime::now(); }",
        )]),
        &crate::gated_policy(),
    );
    assert_eq!(
        report.lines(),
        vec![
            "crates/app-merge/src/merge.rs:1: system clock (current time) is used directly only in crates/app-foundation/src/clock.rs".to_string(),
            "reach a capability through the owner that holds it, given to you when you are built".to_string(),
        ],
    );
}

#[test]
fn one_exact_list_entry_allows_background_starts() {
    const PATH: &str = "crates/app-sync/src/fixture.rs";
    let mut rooted = policy(r#"capability_types = ["Clock"]"#);
    rooted.composition_roots.push(root(PATH, "Worker", "open"));
    rooted.background_starts = crate::gated_policy().background_starts;
    for (path, owner, method, allowed) in [
        (PATH, "Worker", "open", true),
        (PATH, "Worker", "other", false),
        (PATH, "Other", "open", false),
        ("crates/app-sync/src/elsewhere.rs", "Worker", "open", false),
    ] {
        let source = format!(
            "extern crate tokio; struct Other; struct Worker {{ clock: Clock }} impl {owner} {{ fn {method}(clock: Clock) {{ let _ = Worker {{ clock }}; tokio::spawn(async {{}}); }} }}"
        );
        let report = check(&workspace(vec![RustFile::fixture(path, &source)]), &rooted);
        let expected = if allowed { Vec::new() } else { unrooted_spawn(path) };
        assert_eq!(report.0, expected);
    }
    let source = "extern crate tokio; struct Worker { clock: Clock } impl Worker { fn open() { fn hidden(clock: Clock) { Worker { clock }; tokio::spawn(async {}); } } }";
    assert_eq!(
        check(&workspace(vec![RustFile::fixture(PATH, source)]), &rooted).0,
        unrooted_spawn(PATH)
    );
}

fn unrooted_spawn(path: &str) -> Vec<Finding> {
    vec![Finding::new(
        path,
        1,
        "thread or future spawn outside a composition root",
        "start background work only at a listed root, which must keep and stop it",
    )]
}

fn member(directory: &str, name: &str) -> crate::sources::Manifest {
    crate::sources::Manifest {
        relative_path: format!("{directory}/Cargo.toml"),
        table: format!("[package]\nname = {name:?}").parse().unwrap(),
    }
}

#[test]
fn package_selection_uses_manifest_names_and_directory_boundaries() {
    let mut workspace = workspace(vec![
        RustFile::fixture("tools/selected/src/main.rs", "pub(in crate::sources) fn read() {}"),
        RustFile::fixture(
            "tools/selected-other/src/main.rs",
            "pub(in crate::sources) fn read() {}",
        ),
    ]);
    workspace.manifests = vec![
        member("tools/selected", "selected-package"),
        member("tools/selected-other", "other-package"),
    ];
    let policy = policy(r#"tools = ["selected-package", "other-package"]"#);
    let all = check(&workspace, &policy);
    assert_eq!(all.0.len(), 2);
    let selected = check_selected(&workspace, &policy, &["selected-package".into()]).unwrap();
    assert_eq!(selected.0.len(), 1);
    assert_eq!(selected.0[0].path, "tools/selected/src/main.rs");
    let both =
        check_selected(&workspace, &policy, &["selected-package".into(), "other-package".into()])
            .unwrap();
    assert_eq!(both.0, all.0);
}

#[test]
fn package_selection_cannot_hide_an_ambiguous_workspace_graph() {
    let policy = policy(r#"capability_types = ["Clock"]"#);
    let mut workspace = workspace(vec![
        RustFile::fixture("crates/first/src/lib.rs", "trait Clock {}"),
        RustFile::fixture("crates/second/src/lib.rs", "struct Clock;"),
    ]);
    workspace.manifests = vec![member("crates/second", "second")];
    let selected = check_selected(&workspace, &policy, &["second".into()]).unwrap();
    assert!(selected.0.iter().any(|finding| finding.path == "crates/first/src/lib.rs"
        && finding.message.contains("declared more than once")));
}

#[test]
fn an_unknown_package_is_an_error() {
    let (workspace, policy) = (workspace(Vec::new()), policy(""));
    let result = check_selected(&workspace, &policy, &["missing".into()]);
    assert!(matches!(result, Err(UnknownPackage(package)) if package == "missing"));
}

#[test]
fn only_the_selected_rules_run() {
    let files = || {
        workspace(vec![
            RustFile::fixture("crates/first/src/lib.rs", "trait Clock {}"),
            RustFile::fixture(
                "crates/second/src/lib.rs",
                "struct Clock; pub(in crate::x) fn now() { let _ = std::time::SystemTime::now(); }",
            ),
        ])
    };
    let mut selected = crate::gated_policy();
    selected.capability_types.insert("Clock".into());
    let messages = |policy: &Policy| {
        check(&files(), policy).0.into_iter().map(|finding| finding.message).collect::<Vec<_>>()
    };
    selected.rules = [Rule::Conventions].into();
    assert_eq!(messages(&selected), ["pub(in path) visibility is forbidden"]);
    selected.rules = [Rule::CapabilityBoundaries].into();
    assert_eq!(
        messages(&selected),
        ["system clock (current time) is used directly only in crates/app-foundation/src/clock.rs"]
    );
    // Name-based owner rules need every name they reason about declared once.
    selected.rules = [Rule::OwnerDependencies].into();
    assert_eq!(
        messages(&selected),
        [
            "type Clock is declared more than once: crates/first/src/lib.rs, crates/second/src/lib.rs"
        ]
    );
}

#[test]
fn string_errors_are_refused_in_production_code_and_not_in_tests() {
    let report = check(
        &workspace(vec![
            RustFile::fixture(
                "crates/first/src/lib.rs",
                "fn g() -> Result<(), u8> { Ok(()) } fn run() -> Result<(), String> { g().map_err(|e| e.to_string()) }",
            ),
            RustFile::fixture(
                "crates/first/src/lib_tests.rs",
                "fn g() -> Result<(), u8> { Ok(()) } fn run() -> Result<(), String> { g().map_err(|e| e.to_string()) }",
            ),
        ]),
        &policy(""),
    );
    let lines = report.lines();
    assert!(lines[0].starts_with("crates/first/src/lib.rs:1: map_err"), "{lines:?}");
    assert!(lines[1].starts_with("crates/first/src/lib.rs:1: run returns"), "{lines:?}");
    assert_eq!(lines.len(), 3, "{lines:?}");
}

#[test]
fn a_policy_selecting_read_shapes_reports_the_work_a_read_does_on_rows() {
    let policy = Policy::fixture(
        r#"rules = ["read-shapes"]
        [read_shapes]
        read_types = ["SqlReadContext"]
        rule = "the read rule"
        "#,
    );
    let files = vec![RustFile::fixture(
        "crates/app/src/reads.rs",
        "fn read(sql: &SqlReadContext<'_>) {\n    rows.sort();\n}",
    )];
    let lines = check(&workspace(files), &policy).lines();
    assert_eq!(lines[0], "crates/app/src/reads.rs:2: read sorts with `sort` (sort)");
}
