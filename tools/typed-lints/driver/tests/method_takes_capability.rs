//! `clippy::method_takes_capability` run by the driver, as cargo runs it,
//! over the fixture crate `fixtures/owners/parameters.rs`, under the policy
//! of its folder: the lint finds each function of an impl, no constructor or
//! composition root, that takes a construction-only capability or a handle,
//! and nothing else; it finds nothing in test code; and the fixture's mark
//! holds.

mod support;

use support::{Fixture, extern_library, out_dir};

#[test]
fn finds_the_methods_that_take_a_capability_or_a_handle_and_nothing_else() {
    let out_dir = out_dir("method_takes_capability");
    let base =
        Fixture { source: "owners/base.rs", crate_name: "owners_base", manifest_dir: "owners" };
    base.compile(&out_dir, ["--cap-lints=allow"]);
    let fixture = Fixture {
        source: "owners/parameters.rs",
        crate_name: "owners_parameters",
        manifest_dir: "owners",
    };
    let test_builds = ["--cfg", "feature=\"test-utils\""].map(String::from);
    let arguments = extern_library("owners_base", &out_dir).into_iter().chain(test_builds);
    let diagnostics = fixture.compile(&out_dir, arguments);
    fixture.assert_findings("clippy::method_takes_capability", &diagnostics);
}
