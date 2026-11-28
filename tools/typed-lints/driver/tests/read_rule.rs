//! The read rule's lints run by the driver, as cargo runs it, over the
//! fixture package `fixtures/reads/`: each lint finds its shape of work on a
//! read's rows in read code, the rows of a generated statement and the type
//! read code makes of each among them, and nothing on text, a constant table, one
//! optional value, values no read returned or a workspace method of the
//! same name, nothing in code no read reaches, and nothing in what only a
//! live query's `process` runs; and the fixture's mark holds. A helper of
//! an earlier crate that no read code of its own crate reaches gets its
//! findings where a later crate's read code calls it, through a crate
//! between them too, and a compile stops when the earlier crate's record is
//! missing. The compile names the fixture's policy file, which states the
//! read rule's shapes, among its inputs in the dep-info cargo reads, so cargo
//! lints the crate again when the policy changes.

mod support;

use std::path::Path;

use support::{FIXTURES, Fixture, extern_library, out_dir, slash_path};

/// The read rule's lints, each named as the fixture's comments name it.
const LINTS: [&str; 13] = [
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
    "clippy::read_kept_state",
    "clippy::read_cutoff",
    "clippy::read_request_bump",
];

#[test]
fn finds_each_shape_of_the_read_rule_and_nothing_else() {
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

/// The crates of the fixture's helpers, in the order they compile: each
/// one's source, crate name and the crates it depends on.
const HELPER_CRATES: [(&str, &str, &[&str]); 4] = [
    ("reads/base.rs", "reads_base", &[]),
    ("reads/helpers.rs", "reads_helpers", &["reads_base"]),
    ("reads/relay.rs", "reads_relay", &["reads_base", "reads_helpers"]),
    ("reads/helped.rs", "reads_helped", &["reads_base", "reads_helpers", "reads_relay"]),
];

/// One of `HELPER_CRATES`, and the arguments that let it use the crates it
/// depends on from `out_dir`.
fn helper_crate(
    (source, crate_name, dependencies): (&'static str, &'static str, &[&str]),
    out_dir: &Path,
) -> (Fixture<'static>, Vec<String>) {
    let fixture = Fixture { source, crate_name, manifest_dir: "reads" };
    let arguments =
        dependencies.iter().flat_map(|dependency| extern_library(dependency, out_dir)).collect();
    (fixture, arguments)
}

#[test]
fn finds_what_an_earlier_crates_helper_does_where_read_code_calls_it() {
    let out_dir = out_dir("read_rule_helpers");
    for package in HELPER_CRATES {
        let (fixture, arguments) = helper_crate(package, &out_dir);
        let diagnostics = fixture.compile(&out_dir, arguments);
        if fixture.crate_name != "reads_base" {
            for lint in LINTS {
                fixture.assert_findings(lint, &diagnostics);
            }
        }
    }
}

#[test]
fn stops_a_compile_whose_earlier_crate_has_no_record() {
    let out_dir = out_dir("read_rule_helpers_unrecorded");
    let [base, helpers, relay, helped] = HELPER_CRATES;
    for package in [base, helpers, relay] {
        let (fixture, arguments) = helper_crate(package, &out_dir);
        fixture.compile(&out_dir, arguments);
    }
    std::fs::remove_file(out_dir.join("libreads_helpers.read-rule.toml"))
        .expect("the helpers' compile wrote its record");
    let (fixture, arguments) = helper_crate(helped, &out_dir);
    let output = fixture.run(&out_dir, arguments);
    let diagnostics = String::from_utf8(output.stderr).expect("rustc writes UTF-8");
    assert!(!output.status.success(), "the compile went on without the record:\n{diagnostics}");
    assert!(
        diagnostics.contains("the read rule's record of the crate reads_helpers"),
        "the compile stopped for another reason:\n{diagnostics}"
    );
}

/// Whether the dep-info <dep_info> lists <input> as an input, by the line
/// `<input>:` that rustc writes for each, whatever separators it uses.
pub fn dep_info_names_input(dep_info: &str, input: &Path) -> bool {
    let input = slash_path(&input.display().to_string());
    dep_info
        .lines()
        .filter_map(|line| line.strip_suffix(':'))
        .any(|listed| slash_path(listed) == input)
}

#[test]
fn names_the_workspaces_policy_file_among_the_compiles_inputs() {
    let out_dir = out_dir("read_rule_policy_input");
    let base = Fixture { source: "reads/base.rs", crate_name: "reads_base", manifest_dir: "reads" };
    base.compile(&out_dir, ["--emit=dep-info,metadata"]);
    let dep_info = std::fs::read_to_string(out_dir.join("reads_base.d"))
        .expect("the compile wrote its dep-info");
    let policy = Path::new(FIXTURES).join("reads/owner-construction-check.toml");
    assert!(
        dep_info_names_input(&dep_info, &policy),
        "the dep-info names no {}:\n{dep_info}",
        policy.display()
    );
}

#[test]
fn reads_a_windows_dep_info_with_mixed_separators_and_a_verbatim_prefix() {
    let dep_info = "D:\\a\\coven\\target/tmp\\reads_base.d: \\\\?\\D:\\a\\coven\\clippy.toml D:\\a\\coven\\driver/tests/fixtures\\reads/base.rs\n\
\\\\?\\D:\\a\\coven\\clippy.toml:\n\
D:\\a\\coven\\driver/tests/fixtures\\reads\\owner-construction-check.toml:\n";
    assert!(dep_info_names_input(
        dep_info,
        Path::new("D:\\a\\coven\\driver/tests/fixtures/reads/owner-construction-check.toml")
    ));
    assert!(dep_info_names_input(dep_info, Path::new("D:/a/coven/clippy.toml")));
    assert!(!dep_info_names_input(
        dep_info,
        Path::new("D:/a/coven/driver/tests/fixtures/reads/base.rs")
    ));
}
