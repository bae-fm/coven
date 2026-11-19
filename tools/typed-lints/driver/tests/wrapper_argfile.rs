//! The driver's wrapper, `typed-lints-rustc`, when cargo passes a compile's
//! arguments in an argfile (`rustc @<file>`, one argument a line), as it does
//! for a command line too long for the system: the wrapper still has rustc
//! write every function's MIR, so `clippy::task_state_outside_task`, which
//! reads the bodies of the earlier crates, compiles the fixture package
//! `fixtures/tasks/` from an argfile and finds what it finds when the
//! arguments are on the command line.

mod support;

use std::ffi::OsString;

use support::{Fixture, extern_library, out_dir};

#[test]
fn a_compile_whose_arguments_are_in_an_argfile_reads_the_earlier_crates_bodies() {
    let out_dir = out_dir("wrapper_argfile");
    let base = Fixture { source: "tasks/base.rs", crate_name: "tasks_base", manifest_dir: "tasks" };
    base.compile(&out_dir, [] as [&str; 0]);
    let fixture =
        Fixture { source: "tasks/src/lib.rs", crate_name: "tasks", manifest_dir: "tasks" };
    let test_builds = ["--cfg", "feature=\"test-utils\""].map(String::from);
    let arguments = extern_library("tasks_base", &out_dir).into_iter().chain(test_builds);

    let mut lines = String::new();
    for argument in fixture.arguments(&out_dir, arguments) {
        lines.push_str(argument.to_str().expect("cargo writes an argfile in UTF-8"));
        lines.push('\n');
    }
    let argfile = out_dir.join("tasks.args");
    std::fs::write(&argfile, lines).expect("the argfile is written");
    let mut argfile_argument = OsString::from("@");
    argfile_argument.push(&argfile);
    let output = fixture.run_with([argfile_argument]);

    let diagnostics = String::from_utf8(output.stderr).expect("rustc writes UTF-8");
    assert!(output.status.success(), "the compile from an argfile failed:\n{diagnostics}");
    fixture.assert_findings("clippy::task_state_outside_task", &diagnostics);
}
