//! `clippy::task_state_outside_task` run by the driver, as cargo runs it,
//! over the fixture package `fixtures/tasks/`: the lint finds each type that
//! holds state a task makes for its operation, whether the task is built in
//! this crate or, in a function no other crate can call, in the earlier
//! crate, and each place this crate's task makes state the earlier crate's
//! holder holds; it finds nothing in test code, in a handle the task makes,
//! or for a plain value.

mod support;

use support::{Fixture, extern_library, out_dir};

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
