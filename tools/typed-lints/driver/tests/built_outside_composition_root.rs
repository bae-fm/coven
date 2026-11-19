//! `clippy::built_outside_composition_root` run by the driver, as cargo runs
//! it, over the fixture package `fixtures/owners/`, whose policy names its
//! types by path, some in a private module of the crate of the workspace the
//! fixture depends on: the lint finds each owner, construction-only
//! capability and handle built outside a composition root, and each static
//! that shares state and is no root, and nothing else; it finds nothing in
//! test code; and the fixture's mark holds.

mod support;

use support::{Fixture, extern_library, out_dir};

#[test]
fn finds_what_is_built_outside_a_composition_root_and_nothing_else() {
    let out_dir = out_dir("built_outside_composition_root");
    let base =
        Fixture { source: "owners/base.rs", crate_name: "owners_base", manifest_dir: "owners" };
    base.compile(&out_dir, ["--cap-lints=allow"]);
    let fixture =
        Fixture { source: "owners/src/lib.rs", crate_name: "owners", manifest_dir: "owners" };
    let test_builds = ["--cfg", "feature=\"test-utils\""].map(String::from);
    let arguments = extern_library("owners_base", &out_dir).into_iter().chain(test_builds);
    let diagnostics = fixture.compile(&out_dir, arguments);
    fixture.assert_findings("clippy::built_outside_composition_root", &diagnostics);
}
