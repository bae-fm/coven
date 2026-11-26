use crate::SchemaProbe;

const SCHEMA: &str = "
CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, note TEXT) STRICT;
CREATE TABLE issues (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, assignee_id TEXT) STRICT;
CREATE TABLE verdicts (verdict_id INTEGER PRIMARY KEY, kind TEXT NOT NULL) STRICT;
CREATE TABLE totals (id TEXT NOT NULL PRIMARY KEY, twice TEXT NOT NULL GENERATED ALWAYS AS (id || id)) STRICT;
CREATE VIEW named (person, called) AS SELECT id, name FROM people;
CREATE VIEW assigned AS SELECT i.title, p.name FROM issues AS i LEFT JOIN people AS p ON p.id = i.assignee_id;
CREATE VIEW assigned_back AS SELECT i.title, p.name FROM people AS p RIGHT JOIN issues AS i ON p.id = i.assignee_id;
CREATE VIEW counted AS WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT n FROM c;
CREATE VIRTUAL TABLE docs USING fts5(body);";

/// Whether the probe says of each column of `sql` that no row of the
/// statement holds NULL in it.
fn never_null(sql: &str) -> Vec<bool> {
    let probe = SchemaProbe::open(&[SCHEMA.to_owned()], &[], &[]).unwrap();
    let statement = probe.prepare(sql).unwrap();
    statement.columns.iter().map(|column| !column.nulls.nullable).collect()
}

#[track_caller]
fn assert_never_null(cases: &[(&str, &[bool])]) {
    for (sql, expected) in cases {
        assert_eq!(never_null(sql), *expected, "{sql}");
    }
}

#[test]
fn a_column_that_each_query_passes_on_stays_never_null() {
    assert_never_null(&[
        ("SELECT s.name, s.note FROM (SELECT name, note FROM people) AS s", &[true, false]),
        ("SELECT name FROM (SELECT * FROM (SELECT p.name FROM people AS p))", &[true]),
        ("WITH s(called) AS (SELECT name FROM people) SELECT called FROM s", &[true]),
        (
            "WITH s AS (SELECT name FROM people), t AS (SELECT * FROM s) SELECT t.* FROM t",
            &[true],
        ),
        ("SELECT person, called FROM named", &[true, true]),
        ("SELECT \"Called\" FROM main.named", &[true]),
        ("SELECT s.twice, s.id FROM (SELECT * FROM totals) AS s", &[true, true]),
        ("SELECT s.name FROM (SELECT name FROM people) AS s JOIN issues AS i ON 1", &[true]),
        (
            "SELECT s.name FROM (SELECT name FROM people UNION SELECT title FROM issues) AS s",
            &[true],
        ),
        (
            "SELECT s.name FROM (SELECT 'none' AS name UNION ALL SELECT name FROM people) AS s",
            &[true],
        ),
        (
            "SELECT s.name, s.n FROM (SELECT name, count(*) AS n FROM people GROUP BY name) AS s",
            &[true, false],
        ),
        ("SELECT s.name FROM (SELECT name, max(1, 2) FROM people) AS s", &[true]),
        ("SELECT s.name FROM (SELECT name, count(*) OVER () FROM people) AS s", &[true]),
        ("SELECT s._rowid_ FROM (SELECT _rowid_ FROM verdicts) AS s", &[true]),
        (
            "SELECT s.name FROM (SELECT name, (SELECT count(i.id) FROM issues AS i) FROM people) AS s",
            &[true],
        ),
        ("SELECT s.rowid FROM (SELECT rowid, body FROM docs) AS s", &[true]),
        ("SELECT _rowid_, name FROM people", &[true, true]),
        ("SELECT s.column1 FROM (VALUES (1), (2)) AS s", &[true]),
        (
            "SELECT i.title, p.name FROM issues AS i LEFT JOIN people AS p ON p.id = i.assignee_id",
            &[true, false],
        ),
        (
            "SELECT id, title, name FROM issues JOIN (SELECT id, name FROM people) USING (id)",
            &[true, true, true],
        ),
        (
            "SELECT * FROM issues LEFT JOIN (SELECT id, name FROM people) USING (id)",
            &[true, true, false, false],
        ),
    ]);
}

#[test]
fn a_column_is_null_where_any_query_it_passes_through_gives_null() {
    assert_never_null(&[
        ("SELECT title, name FROM assigned", &[true, false]),
        ("SELECT title, name FROM assigned_back", &[true, false]),
        ("SELECT n FROM counted", &[false]),
        (
            "SELECT * FROM (SELECT id FROM issues) NATURAL LEFT JOIN (SELECT id, name FROM people)",
            &[true, false],
        ),
        (
            "SELECT s.title, s.name FROM
             (SELECT i.title, p.name FROM issues AS i FULL JOIN people AS p ON 0) AS s",
            &[false, false],
        ),
        (
            "SELECT s.title, s.name, s.kind FROM
             (SELECT i.title, p.name, v.kind FROM issues AS i
              LEFT JOIN (people AS p LEFT JOIN verdicts AS v ON v.kind = p.id)
              ON p.id = i.assignee_id) AS s",
            &[true, false, false],
        ),
        (
            "SELECT s.name FROM issues AS i LEFT JOIN
             (SELECT p.name FROM people AS p) AS s ON 0",
            &[false],
        ),
        ("SELECT s.rowid FROM issues LEFT JOIN (SELECT rowid FROM people) AS s ON 0", &[false]),
        ("SELECT title, count(*) FROM issues HAVING count(*) = 0", &[false, false]),
        ("SELECT i.title, (SELECT count(i.id)) FROM issues AS i", &[false, false]),
        (
            "SELECT i.title, (SELECT (SELECT max(i.id)) FROM people) FROM issues AS i",
            &[false, false],
        ),
        ("SELECT s.title FROM (SELECT (SELECT title FROM issues) AS title) AS s", &[false]),
        ("SELECT s.column1 FROM (VALUES (1), (NULL)) AS s", &[false]),
        (
            "WITH s AS (SELECT name FROM people EXCEPT SELECT note FROM people) SELECT name FROM s",
            &[false],
        ),
    ]);
}

#[test]
fn a_table_function_beside_a_table_leaves_the_tables_columns_alone() {
    assert_never_null(&[
        ("SELECT name, upper(value) FROM people, json_each('[1]')", &[true, false]),
        ("SELECT p.name FROM json_each('[]') LEFT JOIN people AS p ON 0", &[false]),
        ("SELECT * FROM docs", &[false]),
    ]);
}

#[test]
fn a_returning_column_holds_what_its_table_column_holds() {
    assert_never_null(&[
        ("INSERT INTO people (id, name) VALUES ('a', 'b') RETURNING name, note", &[true, false]),
        ("UPDATE verdicts SET kind = 'a' RETURNING rowid, kind", &[true, true]),
        ("DELETE FROM issues RETURNING title, assignee_id", &[true, false]),
        ("DELETE FROM issues RETURNING 1, upper(title)", &[false, false]),
    ]);
}

#[test]
fn a_column_says_whether_its_from_item_can_have_no_row() {
    let probe = SchemaProbe::open(&[SCHEMA.to_owned()], &[], &[]).unwrap();
    // Each case's first two columns are of the items `a` and `b`.
    for (sql, expected) in [
        (
            "SELECT a.name, b.name FROM issues LEFT JOIN (people AS a JOIN people AS b) ON 1",
            [true, true],
        ),
        (
            "SELECT a.name, b.name FROM issues LEFT JOIN ((people AS a JOIN people AS b)) ON 1",
            [true, true],
        ),
        (
            "SELECT a.name, b.name FROM (people AS a JOIN people AS b) RIGHT JOIN issues ON 1",
            [true, true],
        ),
        (
            "SELECT a.name, b.name FROM issues JOIN (people AS a RIGHT JOIN people AS b ON 1) ON 1",
            [true, false],
        ),
        (
            "SELECT a.name, b.name FROM issues JOIN (people AS a FULL JOIN people AS b ON 1) ON 1",
            [true, true],
        ),
        (
            "SELECT a.name, b.name FROM (people AS a JOIN people AS b) FULL JOIN issues ON 1",
            [true, true],
        ),
        (
            "SELECT a.name, b.name FROM (people AS a JOIN people AS b) LEFT JOIN issues ON 1",
            [false, false],
        ),
        // An aggregate with no GROUP BY gives a row over no rows.
        ("SELECT a.name, b.name, count(*) FROM people AS a, people AS b", [true, true]),
        (
            "SELECT a.name, b.name, count(*) FROM people AS a, people AS b GROUP BY a.id",
            [false, false],
        ),
        // A later SELECT's items are not the ones a column comes from.
        (
            "SELECT a.name, b.name FROM people AS a, people AS b \
             UNION ALL SELECT p.name, p.name FROM issues LEFT JOIN people AS p ON 1",
            [false, false],
        ),
    ] {
        let statement = probe.prepare(sql).unwrap();
        let found = [0, 1].map(|position| statement.columns[position].nulls.row_can_be_missing);
        assert_eq!(found, expected, "{sql}");
    }
    // A subquery's own missing rows are its own, and a computed column comes
    // from no item.
    let statement = probe
        .prepare(
            "SELECT s.name, 1, upper(s.name) FROM \
             (SELECT p.name FROM issues LEFT JOIN people AS p ON 1) AS s",
        )
        .unwrap();
    assert!(statement.columns.iter().all(|column| !column.nulls.row_can_be_missing));
}

#[test]
fn a_column_says_whether_a_later_select_can_give_it_null() {
    let probe = SchemaProbe::open(&[SCHEMA.to_owned()], &[], &[]).unwrap();
    for (sql, expected) in [
        // The first SELECT's own NULLs are none of a later one's.
        ("SELECT note, id FROM people", [false, false]),
        ("SELECT note, id FROM people UNION ALL SELECT 'a', 'b'", [false, false]),
        ("SELECT id, name FROM people UNION ALL SELECT NULL, 'b'", [true, false]),
        ("SELECT id, name FROM people UNION SELECT id, note FROM people", [false, true]),
        (
            "SELECT id, name FROM people UNION ALL SELECT 'a', 'b' UNION ALL VALUES ('c', NULL)",
            [false, true],
        ),
        (
            "SELECT id, name FROM people \
             UNION ALL SELECT p.id, p.name FROM issues LEFT JOIN people AS p ON 1",
            [true, true],
        ),
        // A subquery's compound is its own.
        (
            "SELECT s.id, s.name FROM (SELECT id, name FROM people UNION SELECT NULL, NULL) AS s",
            [false, false],
        ),
    ] {
        let statement = probe.prepare(sql).unwrap();
        let found = [0, 1].map(|position| statement.columns[position].nulls.null_in_later_select);
        assert_eq!(found, expected, "{sql}");
    }
}
