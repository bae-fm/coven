//! The hand-out rules run by the driver, as cargo runs it, over the fixture
//! package `fixtures/dependencies/`: `clippy::owner_hands_out_dependency`,
//! `clippy::handle_not_from_its_owner` and `clippy::shared_state_outside_handle`
//! each find what the lines of their sources mark, and nothing else, in the
//! crate the fixture depends on as in the fixture; a value of the earlier
//! crate's type hands out what that type's methods hand out; and test code
//! answers to none of them.

mod support;

use support::{Fixture, assert_findings_marked, extern_library, out_dir};

#[test]
fn finds_what_owners_hand_out_and_nothing_else() {
    let out_dir = out_dir("owner_hands_out_dependency");
    let rusqlite = Fixture { source: "rusqlite.rs", crate_name: "rusqlite", manifest_dir: "." };
    rusqlite.compile(&out_dir, ["--cap-lints=allow"]);
    let base = Fixture {
        source: "dependencies/base.rs",
        crate_name: "dependencies_base",
        manifest_dir: "dependencies",
    };
    let diagnostics = base.compile(&out_dir, extern_library("rusqlite", &out_dir));
    base.assert_findings("clippy::owner_hands_out_dependency", &diagnostics);
    let fixture = Fixture {
        source: "dependencies/src/lib.rs",
        crate_name: "dependencies",
        manifest_dir: "dependencies",
    };
    let arguments = extern_library("rusqlite", &out_dir)
        .into_iter()
        .chain(extern_library("dependencies_base", &out_dir))
        .chain(["--cfg", "feature=\"test-utils\""].map(String::from));
    let diagnostics = fixture.compile(&out_dir, arguments);
    for (lint, marked) in [
        (
            "clippy::owner_hands_out_dependency",
            &["dependencies/src/lib.rs", "dependencies/src/database.rs"][..],
        ),
        ("clippy::handle_not_from_its_owner", &["dependencies/src/handles.rs"]),
        ("clippy::shared_state_outside_handle", &["dependencies/src/shared.rs"]),
    ] {
        assert_findings_marked(lint, &diagnostics, marked);
    }
}
