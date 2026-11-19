//! `clippy::task_state_outside_task` run by the driver, as cargo runs it,
//! over the fixture package `fixtures/tasks/`: the lint finds each type that
//! holds state a task makes for its operation, whether the task is built in
//! this crate or, in a function no other crate can call, in the earlier
//! crate, and each place this crate's task makes state the earlier crate's
//! holder holds; it finds nothing in test code, in a handle the task makes,
//! or for a plain value. Over `fixtures/tasks_apart/`, a holder and a task
//! in crates that never depend on each other, it finds the holder in the
//! first crate that depends on both, and nowhere else.

mod support;

use support::{Fixture, assert_findings_marked, extern_library, out_dir};

#[test]
fn finds_what_holds_a_tasks_state_and_nothing_else() {
    let out_dir = out_dir("task_state_outside_task");
    let base = Fixture { source: "tasks/base.rs", crate_name: "tasks_base", manifest_dir: "tasks" };
    let diagnostics = base.compile(&out_dir, [] as [&str; 0]);
    base.assert_findings("clippy::task_state_outside_task", &diagnostics);
    let fixture =
        Fixture { source: "tasks/src/lib.rs", crate_name: "tasks", manifest_dir: "tasks" };
    let test_builds = ["--cfg", "feature=\"test-utils\""].map(String::from);
    let arguments = extern_library("tasks_base", &out_dir).into_iter().chain(test_builds);
    let diagnostics = fixture.compile(&out_dir, arguments);
    fixture.assert_findings("clippy::task_state_outside_task", &diagnostics);
}

#[test]
fn finds_a_holder_and_task_of_crates_apart_in_the_first_crate_that_sees_both() {
    const LINT: &str = "clippy::task_state_outside_task";
    let out_dir = out_dir("task_state_outside_task_apart");
    // Each package of the fixture, its dependencies and the sources whose
    // marks the compile of it finds.
    let packages: [(&str, &[&str], &[&str]); 5] = [
        ("state", &[], &[]),
        ("holder", &["state"], &[]),
        ("task", &["state"], &[]),
        ("joined", &["holder", "task"], &["tasks_apart/holder/src/lib.rs"]),
        ("user", &["joined"], &[]),
    ];
    for (package, dependencies, marked) in packages {
        let source = format!("tasks_apart/{package}/src/lib.rs");
        let crate_name = format!("apart_{package}");
        let manifest_dir = format!("tasks_apart/{package}");
        let fixture =
            Fixture { source: &source, crate_name: &crate_name, manifest_dir: &manifest_dir };
        let arguments = dependencies
            .iter()
            .flat_map(|dependency| extern_library(&format!("apart_{dependency}"), &out_dir));
        let diagnostics = fixture.compile(&out_dir, arguments);
        assert_findings_marked(LINT, &diagnostics, marked);
    }
}
