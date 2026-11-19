//! The read rule's lints run by the driver, as cargo runs it, over the
//! fixture package `fixtures/reads/`: each lint finds its shape of work on a
//! read's rows in read code, and nothing on text, a constant table, one
//! optional value, values no read returned or a workspace method of the
//! same name, nothing in code no read reaches, and nothing in what only a
//! live query's `process` runs; and the fixture's mark holds.

mod support;

use support::{Fixture, extern_library, out_dir};

/// The read rule's lints, each named as the fixture's comments name it.
const LINTS: [&str; 10] = [
    "clippy::read_sort",
    "clippy::read_filter",
    "clippy::read_group",
    "clippy::read_aggregate",
    "clippy::read_pick",
    "clippy::read_join",
    "clippy::read_length_branch",
    "clippy::read_drop_row",
    "clippy::read_query_per_row",
    "clippy::read_keyed_statement",
];

#[test]
fn finds_each_shape_of_row_work_in_read_code_and_nothing_else() {
    let out_dir = out_dir("read_rule");
    let base = Fixture { source: "reads/base.rs", crate_name: "reads_base", manifest_dir: "reads" };
    base.compile(&out_dir, [] as [&str; 0]);
    let fixture =
        Fixture { source: "reads/src/lib.rs", crate_name: "reads", manifest_dir: "reads" };
    let diagnostics = fixture.compile(&out_dir, extern_library("reads_base", &out_dir));
    for lint in LINTS {
        fixture.assert_findings(lint, &diagnostics);
    }
}
