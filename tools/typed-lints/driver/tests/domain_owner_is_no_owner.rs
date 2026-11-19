//! `clippy::domain_owner_is_no_owner` run by the driver, as cargo runs it,
//! over the fixture package `fixtures/domains/`, whose policy names two
//! database domains' owners by path: the lint finds the owner that holds
//! nothing an owner holds, and not the one that holds the capability, a
//! connection of the fixture's `rusqlite`.

mod support;

use support::{Fixture, extern_library, out_dir};

#[test]
fn finds_a_domain_owner_that_is_no_owner_and_nothing_else() {
    let out_dir = out_dir("domain_owner_is_no_owner");
    let rusqlite = Fixture { source: "rusqlite.rs", crate_name: "rusqlite", manifest_dir: "." };
    rusqlite.compile(&out_dir, ["--cap-lints=allow"]);
    let fixture =
        Fixture { source: "domains/src/lib.rs", crate_name: "domains", manifest_dir: "domains" };
    let diagnostics = fixture.compile(&out_dir, extern_library("rusqlite", &out_dir));
    fixture.assert_findings("clippy::domain_owner_is_no_owner", &diagnostics);
}
