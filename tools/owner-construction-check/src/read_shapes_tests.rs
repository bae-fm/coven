use super::*;
use crate::crate_code::CrateCode;
use crate::report::Report;
use crate::syntax::RustFile;

const PATH: &str = "crates/app/src/a.rs";
const SOURCE: &str = "fn read(sql: &SqlReadContext<'_>) {\n    a.sort();\n    b.sort();\n}\n";
const ENDING: &str = "Frozen rows only shrink: never add one or raise a count. A use over values that are not a read's rows goes in `allowed` with what they are.";

fn policy(rows: &str) -> Policy {
    Policy::fixture(&format!(
        r#"
        rules = ["read-shapes"]
        [database]
        capability = "SQLite"
        test_support_features = ["test-utils"]
        [read_shapes]
        read_types = ["SqlReadContext"]
        plumbing = ["crates/app/src/plumbing.rs"]
        rule = "projects/rule.md in agent-instructions"
        {rows}
        "#
    ))
}

fn row(list: &str, count: usize) -> String {
    let note = if list == "frozen" { "finding" } else { "reason" };
    format!(
        r#"{list} = [{{ path = "{PATH}", item = "read", shape = "sort", count = {count}, {note} = "two sorts" }}]"#
    )
}

fn run(files: &[(&str, &str)], rows: &str) -> Vec<Finding> {
    let files = files.iter().map(|(path, text)| RustFile::fixture(path, text)).collect::<Vec<_>>();
    find_read_shape_violations(&CrateCode::of(&files, &[]), &policy(rows))
}

fn lines(findings: Vec<Finding>) -> Vec<String> {
    Report(findings).lines()
}

#[test]
fn an_unlisted_finding_names_its_line_its_item_and_the_remedy() {
    let found = run(&[(PATH, SOURCE)], "");
    assert_eq!(found.len(), 2);
    let lines = lines(found);
    assert_eq!(lines[0], "crates/app/src/a.rs:2: read sorts with `sort` (sort)");
    assert_eq!(lines[1], "crates/app/src/a.rs:3: read sorts with `sort` (sort)");
    assert_eq!(
        lines[2],
        format!(
            "projects/rule.md in agent-instructions: the statement filters, sorts, joins, groups, counts and sums, in the same query or its own live query beside it; a read returns what the screen shows. {ENDING}"
        )
    );
    assert_eq!(lines.len(), 3);
}

#[test]
fn a_statement_run_for_each_row_has_its_own_remedy() {
    let found = run(
        &[(
            PATH,
            "fn read(sql: &SqlReadContext<'_>) { for id in ids { sql.query_row(\"S\", [], f); } }",
        )],
        "",
    );
    let lines = lines(found);
    assert_eq!(
        lines[0],
        "crates/app/src/a.rs:1: read runs SQL with `query_row` once for each row (query-per-row)"
    );
    assert_eq!(
        lines[1],
        format!(
            "projects/rule.md in agent-instructions: join in the statement instead of a query per row. {ENDING}"
        )
    );
}

#[test]
fn a_frozen_row_with_the_exact_count_passes() {
    assert!(run(&[(PATH, SOURCE)], &row("frozen", 2)).is_empty());
}

#[test]
fn a_frozen_row_over_fewer_findings_fails_with_the_lower_count() {
    let found =
        run(&[(PATH, "fn read(sql: &SqlReadContext<'_>) { a.sort(); }")], &row("frozen", 2));
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].path, "owner-construction-check.toml");
    assert!(found[0].message.contains("lower the count to 1"), "{}", found[0].message);
    assert!(found[0].message.contains("read_shapes.frozen"), "{}", found[0].message);
    assert!(found[0].remedy.ends_with(ENDING));
}

#[test]
fn a_frozen_row_with_no_findings_left_is_removed() {
    let found = run(&[(PATH, "fn read(sql: &SqlReadContext<'_>) {}")], &row("frozen", 2));
    assert_eq!(found.len(), 1);
    assert!(found[0].message.contains("remove the row"), "{}", found[0].message);
}

#[test]
fn a_frozen_row_over_more_findings_fails_listing_each() {
    let source =
        "fn read(sql: &SqlReadContext<'_>) {\n    a.sort();\n    b.sort();\n    c.sort();\n}";
    let found = run(&[(PATH, source)], &row("frozen", 2));
    assert_eq!(found.iter().map(|finding| finding.line).collect::<Vec<_>>(), [2, 3, 4]);
    assert!(found.iter().all(|finding| finding.path == PATH));
}

#[test]
fn an_allowed_row_is_exact_as_a_frozen_row_is() {
    assert!(run(&[(PATH, SOURCE)], &row("allowed", 2)).is_empty());
    let fewer =
        run(&[(PATH, "fn read(sql: &SqlReadContext<'_>) { a.sort(); }")], &row("allowed", 2));
    assert!(fewer[0].message.contains("read_shapes.allowed"), "{}", fewer[0].message);
    assert!(fewer[0].message.contains("lower the count to 1"), "{}", fewer[0].message);
    let more = run(&[(PATH, SOURCE)], &row("allowed", 1));
    assert_eq!(more.len(), 2);
}

#[test]
fn a_row_covers_only_its_own_path_item_and_shape() {
    let rows = format!(
        r#"frozen = [
            {{ path = "{PATH}", item = "other", shape = "sort", count = 2, finding = "x" }},
            {{ path = "{PATH}", item = "read", shape = "filter", count = 2, finding = "x" }},
        ]"#
    );
    let found = run(&[(PATH, SOURCE)], &rows);
    // Two unlisted sorts, and two rows with no findings left.
    assert_eq!(found.iter().filter(|finding| finding.path == PATH).count(), 2);
    assert_eq!(
        found.iter().filter(|finding| finding.path == "owner-construction-check.toml").count(),
        2
    );
}

#[test]
fn plumbing_test_code_and_a_workspace_without_the_rows_have_no_findings() {
    assert!(run(&[("crates/app/src/plumbing.rs", SOURCE)], "").is_empty());
    let tests = "#[cfg(test)] mod tests { fn read(sql: &SqlReadContext<'_>) { a.sort(); } }";
    assert!(run(&[(PATH, tests)], "").is_empty());
    let files = [RustFile::fixture(PATH, SOURCE)];
    let none = Policy::fixture(r#"rules = ["read-shapes"]"#);
    assert!(find_read_shape_violations(&CrateCode::of(&files, &[]), &none).is_empty());
}

#[test]
fn kept_state_has_its_own_remedy_and_is_keyed_by_the_type() {
    let policy = Policy::fixture(
        r#"
        rules = ["read-shapes"]
        [read_shapes]
        read_types = ["SqlReadContext"]
        live_query_types = ["LiveQuery"]
        kept_state_types = ["Mutex"]
        rule = "projects/rule.md in agent-instructions"
        frozen = [{ path = "crates/app/src/a.rs", item = "Holder", shape = "kept-state", count = 1, finding = "keeps a lock" }]
        "#,
    );
    let source = "struct Holder { query: LiveQuery<u8>, told: Mutex<u8> }\nstruct Other { query: LiveQuery<u8>, told: Mutex<u8> }";
    let files = [RustFile::fixture(PATH, source)];
    let found = find_read_shape_violations(&CrateCode::of(&files, &[]), &policy);
    let lines = lines(found);
    assert_eq!(
        lines[0],
        "crates/app/src/a.rs:2: Other keeps `told`, which holds `Mutex` state, beside its live query (kept-state)"
    );
    assert_eq!(
        lines[1],
        format!(
            "projects/rule.md in agent-instructions: a read starts from the database every time: drop the state and read it in SQL; a value no table holds that changes many times a second travels on a keyed stream. {ENDING}"
        )
    );
    assert_eq!(lines.len(), 2);
}
