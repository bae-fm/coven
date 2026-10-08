use super::*;
use crate::owner_policy::POLICY;

fn workspace(files: Vec<RustFile>) -> Workspace {
    Workspace {
        files,
        manifests: Vec::new(),
        workspace_dependencies: toml::Table::new(),
    }
}

fn capability_implementations() -> RustFile {
    RustFile::fixture(
        "crates/coven-foundation/src/implementations.rs",
        r#"
        impl Clock for SystemClock {}
        impl IdSource for UuidIds {}
    "#,
    )
}

#[test]
fn an_empty_workspace_passes() {
    assert!(check(&workspace(Vec::new()), &POLICY).is_empty());
}

#[test]
fn duplicate_capability_and_owner_names_report_every_declaration() {
    const POLICY: Policy = Policy {
        capability_types: &["Clock"],
        ..Policy::EMPTY
    };
    for (name, first, second) in [
        ("Clock", "trait Clock {}", "struct Clock;"),
        (
            "Worker",
            "trait Clock {} struct Worker { clock: Box<dyn Clock> }",
            "struct Worker;",
        ),
    ] {
        let report = check(
            &workspace(vec![
                RustFile::fixture("crates/first/src/lib.rs", first),
                RustFile::fixture("crates/second/src/lib.rs", second),
            ]),
            &POLICY,
        );
        assert!(!report.is_empty(), "accepted duplicate {name}");
        assert_eq!(
            report.lines(),
            [
                format!("crates/first/src/lib.rs:1: type {name} is declared more than once: crates/first/src/lib.rs, crates/second/src/lib.rs"),
                "type names used by the policy, construction-only capabilities and inferred owners must be unique across crates/".to_string(),
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
        &Policy::EMPTY,
    );
    assert!(report.is_empty(), "{:?}", report.lines());
}

#[test]
fn tool_declarations_do_not_make_crate_names_ambiguous() {
    const POLICY: Policy = Policy {
        capability_types: &["Clock"],
        ..Policy::EMPTY
    };
    let report = check(
        &workspace(vec![
            RustFile::fixture("crates/first/src/lib.rs", "trait Clock {}"),
            RustFile::fixture("tools/second/src/main.rs", "struct Clock;"),
        ]),
        &POLICY,
    );
    assert!(report.is_empty(), "{:?}", report.lines());
}

#[test]
fn test_only_duplicate_capability_names_pass() {
    const POLICY: Policy = Policy {
        capability_types: &["Clock"],
        ..Policy::EMPTY
    };
    let report = check(
        &workspace(vec![
            RustFile::fixture("crates/first/src/lib.rs", "trait Clock {}"),
            RustFile::fixture("crates/second/src/lib.rs", "#[cfg(test)] struct Clock;"),
            RustFile::fixture("crates/second/src/lib_tests.rs", "struct Clock;"),
        ]),
        &POLICY,
    );
    assert!(report.is_empty(), "{:?}", report.lines());
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

#[test]
fn concrete_clock_and_ids_cannot_be_constructed_outside_composition_roots() {
    for expression in [
        "SystemClock.now()",
        "UuidIds.new_id()",
        "std::sync::Arc::new(SystemClock)",
        "std::sync::Arc::new(UuidIds)",
    ] {
        let source = format!("fn acquire() {{ let _ = {expression}; }}");
        let report = check(
            &workspace(vec![
                capability_implementations(),
                RustFile::fixture("crates/coven/src/runtime.rs", &source),
            ]),
            &POLICY,
        );
        assert!(!report.is_empty(), "accepted {expression}");
        assert!(report
            .lines()
            .iter()
            .any(|line| line.contains("outside a composition root")));
    }
}

#[test]
fn composition_roots_can_construct_and_use_concrete_clock_and_ids() {
    const ROOT_POLICY: crate::policy::Policy = crate::policy::Policy {
        composition_roots: &[("crates/coven/src/builder.rs", "Builder", "open")],
        ..POLICY
    };
    let report = check(
        &workspace(vec![
            capability_implementations(),
            RustFile::fixture(
                "crates/coven/src/builder.rs",
                r#"
            struct Builder;
            impl Builder {
                fn open() {
                    let clock = std::sync::Arc::new(SystemClock);
                    let ids = std::sync::Arc::new(UuidIds);
                    let _ = SystemClock.now();
                    let _ = UuidIds.new_id();
                }
            }
            "#,
            ),
        ]),
        &ROOT_POLICY,
    );
    assert!(report.is_empty(), "{:?}", report.lines());
}

#[test]
fn inferred_default_construction_is_prevented_at_its_implementation() {
    let report = check(
        &workspace(vec![
            RustFile::fixture(
                "crates/coven-foundation/src/clock.rs",
                r#"
                struct SuppliedClock(u64);
                #[cfg(feature = "test-utils")] impl Clock for SuppliedClock {}
                impl Default for SuppliedClock { fn default() -> Self { Self(17) } }
            "#,
            ),
            RustFile::fixture(
                "crates/coven/src/runtime.rs",
                "fn run() { let clock: SuppliedClock = Default::default(); }",
            ),
        ]),
        &POLICY,
    );
    assert_eq!(report.0, [Finding::new("crates/coven-foundation/src/clock.rs", 4,
        "implements Default for capability SuppliedClock, which permits implicit construction through Default::default()",
        "construct owners and capabilities explicitly at the listed roots; inject them elsewhere; do not implement or derive Default")]);
}

#[test]
fn one_exact_list_entry_allows_construction_and_task_starts() {
    const PATH: &str = "crates/coven-sync/src/fixture.rs";
    const ROOTED: Policy = Policy {
        capability_types: &["Clock"],
        composition_roots: &[(PATH, "Worker", "open")],
        task_starts: POLICY.task_starts,
        ..Policy::EMPTY
    };
    for (path, owner, method, allowed) in [
        (PATH, "Worker", "open", true),
        (PATH, "Worker", "other", false),
        (PATH, "Other", "open", false),
        (
            "crates/coven-sync/src/elsewhere.rs",
            "Worker",
            "open",
            false,
        ),
    ] {
        let source = format!("struct Worker {{ clock: Clock }} impl {owner} {{ fn {method}(clock: Clock) {{ let _ = Worker {{ clock }}; tokio::spawn(async {{}}); }} }}");
        let report = check(&workspace(vec![RustFile::fixture(path, &source)]), &ROOTED);
        let expected = if allowed {
            Vec::new()
        } else {
            unrooted_worker(path, &format!("{owner}::{method}"))
        };
        assert_eq!(report.0, expected);
    }
    let source = "struct Worker { clock: Clock } impl Worker { fn open() { fn hidden(clock: Clock) { Worker { clock }; tokio::spawn(async {}); } } }";
    assert_eq!(
        check(&workspace(vec![RustFile::fixture(PATH, source)]), &ROOTED).0,
        unrooted_worker(PATH, "<free>::hidden")
    );
}

fn unrooted_worker(path: &str, caller: &str) -> Vec<Finding> {
    vec![
        Finding::new(path, 1, format!("{caller} constructs owner Worker outside a composition root"),
            "construct owners and capabilities explicitly at the listed roots; inject them elsewhere; do not implement or derive Default"),
        Finding::new(path, 1, "thread or task spawn outside a composition root",
            "start long-lived work only at a listed root, which must retain and stop it"),
    ]
}
