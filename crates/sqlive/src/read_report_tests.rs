use super::{read_report, ReadReport};
use crate::read_capture::WholeViews;
use rusqlite::Connection;

const SCHEMA: &str =
    "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, album TEXT, year INTEGER);
CREATE TABLE tracks(id INTEGER PRIMARY KEY, release_id TEXT, title TEXT, position INTEGER);
CREATE TABLE picked(id TEXT);
CREATE VIRTUAL TABLE notes USING fts5(body);
CREATE VIEW later AS WITH RECURSIVE c(id) AS (
    SELECT min(id) FROM releases UNION SELECT r.id FROM releases r JOIN c ON r.id > c.id
) SELECT id FROM c;
CREATE VIEW shown AS WITH c AS (SELECT * FROM releases) SELECT id FROM c;";

/// A view whose SQL SQLite reads and the parser doesn't: its comment never
/// ends.
const UNPARSED: &str = "CREATE VIEW unparsed AS SELECT id FROM releases /* never ends";

/// The report of `sql` for a live query that reads `whole` whole.
fn report_with(sql: &str, whole: &[&str]) -> ReadReport {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(SCHEMA).unwrap();
    connection.execute_batch(UNPARSED).unwrap();
    read_report(&connection, sql, &WholeViews::new(whole.iter().copied())).unwrap()
}

fn report(sql: &str) -> String {
    report_with(sql, &[]).to_string()
}

#[track_caller]
fn assert_whole(sql: &str, why: &str) {
    assert_eq!(report_with(sql, &[]), ReadReport::Whole { why: why.to_owned() });
}

#[test]
fn a_hidden_column_keeps_the_statement_whole() {
    let why = "an FTS5 table's hidden column";
    assert_whole("SELECT body FROM notes ORDER BY rank", why);
    assert_whole("SELECT body FROM notes WHERE rank < 0", why);
    assert_whole("SELECT body FROM notes WHERE body IN (VALUES (rank))", why);
}

#[test]
fn in_a_table_keeps_the_statement_whole() {
    assert_whole("SELECT id FROM tracks WHERE release_id IN picked", "`x IN table`");
}

#[test]
fn a_natural_or_right_join_keeps_the_statement_whole() {
    let why = "a NATURAL or RIGHT join";
    assert_whole("SELECT title FROM tracks NATURAL JOIN releases", why);
    assert_whole("SELECT title FROM tracks t RIGHT JOIN releases r ON r.id = t.release_id", why);
}

#[test]
fn a_parenthesized_join_keeps_the_statement_whole() {
    let why = "a parenthesized join under an alias, starting with another, or joined by USING";
    assert_whole("SELECT 1 FROM (tracks JOIN picked) AS j", why);
    assert_whole("SELECT 1 FROM ((tracks JOIN picked) JOIN releases)", why);
    assert_whole("SELECT 1 FROM releases JOIN (picked JOIN tracks) USING (id)", why);
    assert_whole(
        "SELECT 1 FROM releases r LEFT JOIN (tracks t JOIN picked p) ON r.id = t.release_id",
        "a parenthesized join whose rows aren't its tables joined in turn",
    );
    assert_whole(
        "SELECT 1 FROM picked p JOIN (tracks JOIN releases USING (id))",
        "USING inside a parenthesized join",
    );
}

#[test]
fn what_the_main_schema_lacks_keeps_the_statement_whole() {
    assert_whole("SELECT name FROM temp.sqlite_schema", "a table outside the main schema");
    assert_whole(
        "SELECT value FROM temp.json_each('[1]')",
        "a table function outside the main schema",
    );
    assert_whole(
        "SELECT name FROM pragma_database_list",
        "a name that is no CTE, ordinary table, view or contentful FTS5 table",
    );
}

#[test]
fn a_recursive_cte_keeps_the_statement_whole() {
    assert_whole(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3)
         SELECT x FROM c",
        "a recursive CTE",
    );
}

#[test]
fn a_column_list_of_another_length_keeps_the_statement_whole() {
    // The planner counts the FTS5 table's hidden columns, which `*` leaves out.
    assert_whole(
        "WITH c(body) AS (SELECT * FROM notes) SELECT body FROM c",
        "a CTE or view whose column list differs in length from its SELECT's columns",
    );
}

#[test]
fn a_statement_the_parser_cannot_read_is_whole() {
    // SQLite reads a comment that never ends and the parser doesn't.
    let report = report("SELECT id FROM tracks /* never ends");
    let why = "every table: whole: the parser can't read the statement: ";
    assert!(report.starts_with(why), "{report}");
}

#[test]
fn a_statement_that_is_no_select_is_whole() {
    assert_whole(
        "DELETE FROM tracks RETURNING id",
        "the statement isn't one SELECT the parser reads",
    );
}

#[test]
fn a_view_sqlive_cannot_read_is_whole_beside_a_narrowed_table() {
    assert_eq!(
        report("SELECT t.title FROM tracks t JOIN later l ON l.id = t.release_id WHERE t.id = ?1"),
        "later: whole: a recursive CTE\ntracks: keyed by id"
    );
}

#[test]
fn a_view_the_live_query_reads_whole_is_whole() {
    assert_eq!(
        report_with("SELECT id FROM shown", &["shown"]).to_string(),
        "shown: whole: a view the live query reads whole"
    );
}

#[test]
fn a_table_that_a_value_fixes_a_column_of_is_keyed() {
    assert_eq!(report("SELECT title FROM tracks WHERE id = ?1"), "tracks: keyed by id");
    assert_eq!(report("SELECT title FROM tracks WHERE id IN (1, 2)"), "tracks: keyed by id");
    assert_eq!(
        report("SELECT title FROM tracks WHERE release_id IS ?1"),
        "tracks: keyed by release_id"
    );
    assert_eq!(report("SELECT title FROM tracks WHERE (id = 5) = 1"), "tracks: keyed by id");
}

#[test]
fn a_join_is_keyed_by_its_own_partner_and_not_by_the_partners_other_reads() {
    assert_eq!(
        report(
            "SELECT t.title, (SELECT count(*) FROM releases b)
             FROM releases r JOIN tracks t ON t.release_id = r.id WHERE r.id = ?1"
        ),
        "releases: whole: nothing narrows `b`\ntracks: keyed by release_id"
    );
}

#[test]
fn a_join_of_a_keyed_table_is_looked_up() {
    assert_eq!(
        report(
            "SELECT t.title FROM releases r JOIN tracks t ON t.release_id = r.id WHERE r.album = ?1"
        ),
        "releases: keyed by album\ntracks: looked up"
    );
}

#[test]
fn a_join_on_the_column_its_partner_is_keyed_by_is_keyed() {
    assert_eq!(
        report(
            "SELECT t.title FROM releases r JOIN tracks t ON t.release_id = r.id WHERE r.id = ?1"
        ),
        "releases: keyed by id\ntracks: keyed by release_id"
    );
}

#[test]
fn a_table_narrowed_by_no_one_column_is_looked_up() {
    for sql in [
        "SELECT title FROM tracks WHERE position > ?1",
        "SELECT title FROM tracks WHERE id = ?1 OR release_id = ?2",
        "SELECT title FROM tracks WHERE length(title) = ?1",
        "SELECT title, id FROM tracks ORDER BY title, id LIMIT 10",
        "SELECT title, id FROM tracks ORDER BY title, id LIMIT ?1",
        "SELECT title FROM tracks WHERE (id = 5) = ?1",
        "SELECT title FROM tracks WHERE (position > 2) > ?1",
        "SELECT title FROM tracks WHERE (release_id IS NULL) = ?1",
        "SELECT title FROM tracks WHERE id = ?1 UNION ALL
         SELECT title FROM tracks WHERE release_id = ?2",
    ] {
        assert_eq!(report(sql), "tracks: looked up", "{sql}");
    }
}

#[test]
fn a_subquerys_window_is_looked_up() {
    assert_eq!(
        report(
            "SELECT (SELECT t.title FROM tracks t WHERE t.release_id = r.id
                     ORDER BY t.position, t.id LIMIT 1)
             FROM releases r WHERE r.album = ?1"
        ),
        "releases: keyed by album\ntracks: looked up"
    );
}

#[test]
fn a_table_one_read_narrows_and_another_does_not_is_whole() {
    assert_eq!(
        report(
            "SELECT (SELECT a.title FROM tracks a WHERE a.id = ?1),
                    (SELECT count(*) FROM tracks b)"
        ),
        "tracks: whole: nothing narrows `b`"
    );
}

#[test]
fn a_table_read_through_a_name_of_a_view_read_whole_is_whole() {
    assert_eq!(
        report_with(
            "WITH c AS (SELECT id, title FROM tracks WHERE id = ?1)
             SELECT c.title FROM c JOIN shown s",
            &["shown"]
        )
        .to_string(),
        "shown: whole: a view the live query reads whole\n\
         tracks: whole: read through `c`, which a view read whole also names"
    );
}

#[test]
fn a_table_read_through_a_cte_beside_a_view_the_parser_cannot_read_is_whole() {
    assert_eq!(
        report(
            "WITH c AS (SELECT id, title FROM tracks WHERE id = ?1)
             SELECT c.title FROM c JOIN unparsed u"
        ),
        "tracks: whole: a view read whole that the parser can't read\n\
         unparsed: whole: a view the parser can't read"
    );
}

#[test]
fn each_reason_a_table_is_whole_is_listed_once() {
    assert_eq!(
        report("SELECT count(*) FROM tracks a, tracks b, tracks c WHERE c.id = ?1"),
        "tracks: whole: nothing narrows `a`, nothing narrows `b`"
    );
    assert_eq!(
        report("SELECT (SELECT count(*) FROM tracks a), (SELECT max(id) FROM tracks a)"),
        "tracks: whole: nothing narrows `a`"
    );
}

#[test]
fn a_root_equality_narrows_whichever_side_each_column_stands_on() {
    let root = "(SELECT ?1 AS rel) AS page";
    for (join, on) in [
        ("JOIN", "t.release_id = page.rel"),
        ("LEFT JOIN", "t.release_id = page.rel"),
        (",", "WHERE t.release_id = page.rel"),
    ] {
        let sql = |on: &str| {
            let on = if join == "," { on.to_owned() } else { format!("ON {on}") };
            format!("SELECT t.title FROM {root} {join} tracks t {on}")
        };
        let swapped = on.replace("t.release_id = page.rel", "page.rel = t.release_id");
        assert_eq!(report(&sql(on)), "tracks: looked up", "{join}");
        assert_eq!(report(&sql(&swapped)), report(&sql(on)), "{join} swapped");
    }
}

#[test]
fn a_clock_column_of_a_root_taking_the_first_side_narrows_as_an_expression_does() {
    let report_of = |root: &str| {
        report(&format!(
            "SELECT t.title FROM ({root} AS at) AS page JOIN tracks t ON page.at = t.title"
        ))
    };
    assert_eq!(report_of("SELECT CURRENT_TIMESTAMP"), report_of("SELECT ?1 || 'a'"));
    assert_eq!(report_of("SELECT CURRENT_TIMESTAMP"), "tracks: whole: nothing narrows `t`");
}
