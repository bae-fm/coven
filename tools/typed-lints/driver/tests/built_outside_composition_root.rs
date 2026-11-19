//! `clippy::built_outside_composition_root` run by the driver, as cargo runs
//! it, over the fixture package `fixtures/owners/`, whose policy names its
//! types by path, some in a private module of the crate of the workspace the
//! fixture depends on: the lint finds each owner, construction-only
//! capability and handle built outside a composition root, and each static
//! that shares state and is no root, and nothing else; it finds nothing in
//! test code; and the fixture's mark holds. The fixture's dependency is a
//! crate of the same package, which compiles none of the files the policy's
//! capability factories name, and the lint finds nothing there. A function an
//! attribute macro writes, as uniffi writes one that exports a function to
//! the app, builds nothing by calling a composition root.

mod support;

use support::{Fixture, extern_library, out_dir};

#[test]
fn finds_what_is_built_outside_a_composition_root_and_nothing_else() {
    let out_dir = out_dir("built_outside_composition_root");
    let base =
        Fixture { source: "owners/base.rs", crate_name: "owners_base", manifest_dir: "owners" };
    let diagnostics = base.compile(&out_dir, [] as [&str; 0]);
    base.assert_findings("clippy::built_outside_composition_root", &diagnostics);
    let export = compile_export(&out_dir);
    let fixture =
        Fixture { source: "owners/src/lib.rs", crate_name: "owners", manifest_dir: "owners" };
    let test_builds = ["--cfg", "feature=\"test-utils\""].map(String::from);
    let arguments =
        extern_library("owners_base", &out_dir).into_iter().chain(export).chain(test_builds);
    let diagnostics = fixture.compile(&out_dir, arguments);
    fixture.assert_findings("clippy::built_outside_composition_root", &diagnostics);
}

/// Compiles the fixture `owners/export.rs`, an attribute macro crate, with
/// plain rustc, as cargo compiles one: no workspace wrapper runs on it.
/// Returns the arguments that let a crate use it.
fn compile_export(out_dir: &std::path::Path) -> Vec<String> {
    let source = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/owners/export.rs");
    let output = std::process::Command::new("rustc")
        .args(["--edition=2021", "--crate-type=proc-macro", "--crate-name=owners_export"])
        .arg("--out-dir")
        .arg(out_dir)
        .arg(source)
        .output()
        .expect("rustc runs");
    let stderr = String::from_utf8(output.stderr).expect("rustc writes UTF-8");
    assert!(output.status.success(), "owners_export did not compile:\n{stderr}");
    let library = format!(
        "{}owners_export.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    );
    vec!["--extern".to_string(), format!("owners_export={}", out_dir.join(library).display())]
}
