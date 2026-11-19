//! `clippy::async_export_off_runtime` run by the driver, as cargo runs it,
//! over `fixtures/async_export_off_runtime.rs` under the policy of
//! `fixtures/async_exports/`: the lint finds each async export whose body
//! calls none of the policy's runs, and nothing else, and the fixture's mark
//! holds. A run the policy names and the crate doesn't define stops the
//! compile.

mod support;

use support::{Fixture, extern_library, out_dir};

fn compiled_uniffi(out_dir: &std::path::Path) -> Vec<String> {
    let uniffi = Fixture { source: "uniffi.rs", crate_name: "uniffi", manifest_dir: "." };
    uniffi.compile(out_dir, ["--cap-lints=allow"]);
    extern_library("uniffi", out_dir)
}

fn bridge(manifest_dir: &str) -> Fixture<'_> {
    Fixture {
        source: "async_export_off_runtime.rs",
        crate_name: "async_export_off_runtime",
        manifest_dir,
    }
}

#[test]
fn finds_each_async_export_off_the_owned_runtime_and_nothing_else() {
    let out_dir = out_dir("async_export_off_runtime");
    let uniffi = compiled_uniffi(&out_dir);
    let bridge = bridge("async_exports");
    let diagnostics = bridge.compile(&out_dir, uniffi);
    bridge.assert_findings("clippy::async_export_off_runtime", &diagnostics);
}

#[test]
fn a_run_the_crate_does_not_define_stops_the_compile() {
    let out_dir = out_dir("async_export_off_runtime_stale");
    let uniffi = compiled_uniffi(&out_dir);
    let output = bridge("stale_run").run(&out_dir, uniffi);
    let diagnostics = String::from_utf8(output.stderr).expect("rustc writes UTF-8");
    assert!(!output.status.success(), "the compile went on:\n{diagnostics}");
    assert!(
        diagnostics.contains(
            "async_exports.runs names async_export_off_runtime::runtime::OperationRuntime::run_later, \
             which defines nothing"
        ),
        "{diagnostics}"
    );
}
