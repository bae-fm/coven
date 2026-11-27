use super::{SchemaProbe, StandInFunction};
use crate::{ColumnNulls, ReadReport, ReadRows};

const SCHEMA: &str =
    "CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, note TEXT) STRICT;
CREATE TABLE issues (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, assignee_id TEXT) STRICT;
CREATE INDEX issues_folded ON issues (fold_case(title) COLLATE folded);";

fn probe() -> SchemaProbe {
    SchemaProbe::open(
        &[SCHEMA.to_owned()],
        &[StandInFunction { name: "fold_case".into(), arguments: 1, deterministic: true }],
        &["folded".to_owned()],
        &[],
    )
    .unwrap()
}

#[test]
fn a_stand_in_function_and_collation_let_the_schema_and_statements_prepare() {
    let probe = probe();
    probe.prepare("SELECT fold_case(title) FROM issues ORDER BY title COLLATE folded").unwrap();
    let missing = probe.prepare("SELECT unknown_function(1)").unwrap_err();
    assert!(missing.to_string().contains("unknown_function"), "{missing}");
}

#[test]
fn a_column_names_its_table_column_unless_it_is_computed() {
    let statement = probe()
        .prepare("SELECT p.id AS person_id, p.note, upper(p.name) AS shout, i.title FROM people AS p, issues AS i")
        .unwrap();
    let origins: Vec<_> = statement
        .columns
        .iter()
        .map(|column| {
            let origin = column.origin.as_ref();
            (
                column.name.as_str(),
                origin.map(|origin| (origin.table.as_str(), origin.declared_type.as_deref())),
                column.nulls.as_ref().is_some_and(|nulls| !nulls.nullable()),
            )
        })
        .collect();
    assert_eq!(
        origins,
        [
            ("person_id", Some(("people", Some("TEXT"))), true),
            ("note", Some(("people", Some("TEXT"))), false),
            ("shout", None, false),
            ("title", Some(("issues", Some("TEXT"))), true),
        ]
    );
}

/// Whether the one column of `sql` never holds NULL, on a
/// schema of every kind of primary key.
fn never_null(sql: &str) -> bool {
    let schema = "
        CREATE TABLE verdicts (verdict_id INTEGER PRIMARY KEY, kind TEXT) STRICT;
        CREATE TABLE loose (id INT PRIMARY KEY, name TEXT);
        CREATE TABLE backwards (id INTEGER PRIMARY KEY DESC, name TEXT);
        CREATE TABLE paired (first INTEGER, second INTEGER, PRIMARY KEY (first, second));
        CREATE TABLE named (rowid TEXT, name TEXT);
        CREATE VIRTUAL TABLE docs USING fts5(body);";
    let probe = SchemaProbe::open(&[schema.to_owned()], &[], &[], &[]).unwrap();
    let statement = probe.prepare(sql).unwrap();
    assert!(statement.columns[0].origin.is_some(), "{sql}");
    statement.columns[0].nulls.as_ref().is_some_and(|nulls| !nulls.nullable())
}

#[test]
fn the_rowid_never_holds_null_under_any_of_its_names() {
    for sql in [
        "SELECT verdict_id FROM verdicts",
        "SELECT rowid FROM verdicts",
        "SELECT _rowid_ FROM paired",
        "SELECT oid FROM loose",
        "SELECT rowid FROM docs",
    ] {
        assert!(never_null(sql), "{sql}");
    }
}

#[test]
fn a_primary_key_that_is_not_the_rowid_can_hold_null() {
    for sql in [
        "SELECT id FROM loose",
        "SELECT id FROM backwards",
        "SELECT first FROM paired",
        "SELECT rowid FROM named",
        "SELECT kind FROM verdicts",
    ] {
        assert!(!never_null(sql), "{sql}");
    }
}

#[test]
fn a_statement_lists_its_parameters_once_each() {
    let statement = probe()
        .prepare("SELECT 1 FROM issues WHERE id = :issue OR title = :issue OR assignee_id = :who")
        .unwrap();
    assert_eq!(statement.parameters, [":issue", ":who"]);
    assert_eq!(statement.unnamed_parameters, 0);
    assert_eq!(probe().prepare("SELECT :a, ?").unwrap().unnamed_parameters, 1);
}

#[test]
fn a_statement_says_whether_it_writes() {
    let probe = probe();
    assert!(!probe.prepare("SELECT title FROM issues").unwrap().writes);
    for sql in [
        "INSERT INTO people (id, name) VALUES (:id, :name)",
        "UPDATE issues SET title = :title WHERE id = :id",
        "DELETE FROM issues WHERE id = :id",
        "REPLACE INTO people (id, name) VALUES (:id, :name)",
        "INSERT INTO people (id, name) VALUES (:id, :name) RETURNING id",
        "WITH gone AS (SELECT id FROM issues) DELETE FROM issues WHERE id IN gone",
    ] {
        assert!(probe.prepare(sql).unwrap().writes, "{sql}");
    }
}

#[test]
fn a_table_lists_its_columns_with_their_not_null() {
    let columns: Vec<_> = probe()
        .table_columns("People")
        .unwrap()
        .into_iter()
        .map(|column| (column.name, column.not_null))
        .collect();
    assert_eq!(
        columns,
        [("id".to_owned(), true), ("name".to_owned(), true), ("note".to_owned(), false)]
    );
    assert!(probe().table_columns("missing").unwrap().is_empty());
}

#[test]
fn a_read_report_says_what_the_statement_depends_on() {
    let probe = probe();
    let ReadReport::Entries(entries) =
        probe.read_report("SELECT title FROM issues WHERE id = :issue").unwrap()
    else {
        panic!("the planner reads the statement")
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].rows, ReadRows::Keyed { column: "id".into() });
    let ReadReport::Entries(entries) =
        probe.read_report("SELECT (SELECT count(*) FROM issues AS every) AS total").unwrap()
    else {
        panic!("the planner reads the statement")
    };
    assert!(matches!(entries[0].rows, ReadRows::Whole { .. }));
    assert!(matches!(
        probe
            .read_report(
                "WITH RECURSIVE c(n) AS (SELECT 1 UNION SELECT n + 1 FROM c) SELECT n FROM c"
            )
            .unwrap(),
        ReadReport::Whole { .. }
    ));
}

#[test]
fn a_table_functions_column_names_the_function_and_can_hold_null() {
    let probe = probe();
    for sql in [
        "SELECT value FROM json_each('[1]')",
        "SELECT s.value FROM (SELECT j.value FROM json_each(:list) AS j) AS s",
        "INSERT INTO people (id, name) VALUES (:id, :name)
         RETURNING (SELECT value FROM json_each('[1]')) AS value",
    ] {
        let statement = probe.prepare(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        let column = &statement.columns[0];
        let origin = column.origin.as_ref().unwrap_or_else(|| panic!("{sql}"));
        assert_eq!(
            (origin.table.as_str(), origin.column.as_str(), origin.declared_type.as_deref()),
            ("json_each", "value", None),
            "{sql}"
        );
        assert!(column.nulls.as_ref().is_some_and(ColumnNulls::nullable), "{sql}");
    }
    let every = probe.prepare("SELECT * FROM json_each('[1]')").unwrap();
    let names: Vec<_> = every.columns.iter().map(|column| column.name.as_str()).collect();
    assert_eq!(names, ["key", "value", "type", "atom", "id", "parent", "fullkey", "path"]);
}

#[test]
fn a_column_of_sqlites_own_schema_table_names_its_declared_type() {
    let statement = probe().prepare("SELECT name FROM sqlite_schema").unwrap();
    let origin = statement.columns[0].origin.as_ref().unwrap();
    assert_eq!(origin.declared_type.as_deref(), Some("TEXT"));
}
