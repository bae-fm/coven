use super::{SchemaProbe, StandInFunction};
use crate::{ReadReport, ReadRows};

const SCHEMA: &str =
    "CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, note TEXT) STRICT;
CREATE TABLE issues (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, assignee_id TEXT) STRICT;
CREATE INDEX issues_folded ON issues (fold_case(title) COLLATE folded);";

fn probe() -> SchemaProbe {
    SchemaProbe::open(
        &[SCHEMA.to_owned()],
        &[StandInFunction { name: "fold_case".into(), arguments: 1, deterministic: true }],
        &["folded".to_owned()],
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
                origin.map(|origin| {
                    (
                        origin.table.as_str(),
                        origin.column.as_str(),
                        origin.declared_type.as_deref(),
                        origin.not_null,
                    )
                }),
            )
        })
        .collect();
    assert_eq!(
        origins,
        [
            ("person_id", Some(("people", "id", Some("TEXT"), true))),
            ("note", Some(("people", "note", Some("TEXT"), false))),
            ("shout", None),
            ("title", Some(("issues", "title", Some("TEXT"), true))),
        ]
    );
}

#[test]
fn a_statement_lists_its_parameters_once_each() {
    let statement = probe()
        .prepare("SELECT 1 FROM issues WHERE id = :issue OR title = :issue OR assignee_id = :who")
        .unwrap();
    assert_eq!(statement.parameters, [":issue", ":who"]);
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
