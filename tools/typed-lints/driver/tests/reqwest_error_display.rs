//! `clippy::reqwest_error_display` run by the driver, as cargo runs it, over
//! `fixtures/reqwest_error_display.rs`: the lint finds what each line ending
//! in `// finding` prints, and nothing else, and the fixture's mark holds.

mod support;

use support::{Fixture, extern_library, out_dir};

#[test]
fn finds_each_printed_reqwest_error_and_nothing_else() {
    let out_dir = out_dir("reqwest_error_display");
    let reqwest = Fixture { source: "reqwest.rs", crate_name: "reqwest", manifest_dir: "." };
    reqwest.compile(&out_dir, ["--cap-lints=allow"]);
    let fixture = Fixture {
        source: "reqwest_error_display.rs",
        crate_name: "reqwest_error_display",
        manifest_dir: ".",
    };
    let diagnostics = fixture.compile(&out_dir, extern_library("reqwest", &out_dir));
    fixture.assert_findings("clippy::reqwest_error_display", &diagnostics);
}
