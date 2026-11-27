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

/// The report of `sql` as `unplaced` writes it.
fn report(sql: &str) -> String {
    unplaced(report_with(sql, &[]))
}

/// `report`'s text without its places, for what each place's rows are; the
/// tests of places are in `read_report_tests/places.rs`.
fn unplaced(mut report: ReadReport) -> String {
    if let ReadReport::Entries(entries) = &mut report {
        entries.iter_mut().for_each(|entry| entry.place = None);
    }
    report.to_string()
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

/// A `*` leaves out an FTS5 table's hidden columns, as SQLite's does, so a
/// CTE's column list names the columns the `*` gives.
#[test]
fn a_star_of_an_fts5_table_leaves_out_its_hidden_columns() {
    assert!(matches!(
        report_with("WITH c(body) AS (SELECT * FROM notes) SELECT body FROM c", &[]),
        ReadReport::Entries(_)
    ));
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
        unplaced(report_with("SELECT id FROM shown", &["shown"])),
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
        "releases: whole: nothing narrows `b`\nreleases: keyed by id\ntracks: keyed by release_id"
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
fn a_table_read_through_a_name_of_a_view_read_whole_is_whole() {
    assert_eq!(
        unplaced(report_with(
            "WITH c AS (SELECT id, title FROM tracks WHERE id = ?1)
             SELECT c.title FROM c JOIN shown s",
            &["shown"]
        )),
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
fn a_clock_column_of_a_root_narrows_nothing_either_way_round() {
    for on in ["t.title = page.at", "page.at = t.title"] {
        assert_eq!(
            report(&format!(
                "SELECT t.title FROM (SELECT CURRENT_TIMESTAMP AS at) AS page
                 JOIN tracks t ON {on}"
            )),
            "tracks: whole: nothing narrows `t`",
            "{on}"
        );
    }
}

#[test]
fn a_derived_column_that_reads_the_clock_in_any_form_narrows_nothing() {
    let whole = "tracks: whole: nothing narrows `t`";
    for sql in [
        "SELECT t.title FROM tracks t JOIN (SELECT CURRENT_TIMESTAMP AS title) AS page
         USING (title)",
        "SELECT t.title FROM (VALUES (CURRENT_TIMESTAMP)) AS page
         JOIN tracks t ON t.title = page.column1",
        "SELECT t.title FROM (SELECT (SELECT CURRENT_TIMESTAMP) AS at) AS page
         JOIN tracks t ON t.title = page.at",
        "SELECT t.title FROM (SELECT x.at || '' AS at FROM (SELECT CURRENT_TIMESTAMP AS at) AS x)
         AS page JOIN tracks t ON t.title = page.at",
        "SELECT t.title FROM (SELECT random() AS at) AS page JOIN tracks t ON t.title = page.at",
        "WITH c AS (SELECT CURRENT_TIMESTAMP AS at), g AS (SELECT (SELECT at FROM c) AS n)
         SELECT t.title FROM g JOIN tracks t ON t.title = g.n",
        "WITH c AS (SELECT random() AS r), g AS (SELECT (SELECT r FROM c) AS n)
         SELECT t.title FROM g JOIN tracks t ON t.position = g.n",
        "WITH g AS (SELECT (SELECT id FROM releases ORDER BY random() LIMIT 1) AS n)
         SELECT t.title FROM g JOIN tracks t ON t.id = g.n",
        "SELECT t.title FROM (SELECT (SELECT count(*) FROM
                              (SELECT 1 AS one FROM releases WHERE album < CURRENT_TIMESTAMP)) AS n)
         AS page JOIN tracks t ON t.position = page.n",
        "SELECT t.title FROM (SELECT at || '' AS n
                              FROM (SELECT *, CURRENT_TIMESTAMP AS at FROM json_each(?1)) AS x)
         AS page JOIN tracks t ON t.title = page.n",
        "SELECT t.title FROM (SELECT *, CURRENT_TIMESTAMP AS at FROM json_each(?1)) AS page
         JOIN tracks t ON t.title = page.at",
        "SELECT t.title FROM (SELECT (SELECT at || '') AS at
                              FROM (SELECT CURRENT_TIMESTAMP AS at) AS x, json_each(?1)) AS page
         JOIN tracks t ON t.title = page.at",
        "SELECT t.title FROM (SELECT x.at || '' AS at
                              FROM (SELECT CURRENT_TIMESTAMP AS at) AS x, json_each(?1)) AS page
         JOIN tracks t ON t.title = page.at",
    ] {
        let report = report(sql);
        assert!(report.ends_with(whole), "{sql}: {report}");
    }
}

#[test]
fn an_aggregate_column_of_a_cte_still_gives_its_value() {
    assert_eq!(
        report(
            "WITH g AS (SELECT count(*) AS n FROM releases)
             SELECT t.title FROM g JOIN tracks t ON t.position = g.n"
        ),
        "releases: whole: nothing narrows `releases`\ntracks: looked up"
    );
}

#[test]
fn a_subquery_column_of_a_cte_still_gives_its_value() {
    assert_eq!(
        report(
            "WITH g AS (SELECT (SELECT count(*) FROM releases) AS n)
             SELECT t.title FROM g JOIN tracks t ON t.position = g.n"
        ),
        "releases: whole: nothing narrows `releases`\ntracks: looked up"
    );
}

#[test]
fn a_table_functions_column_of_a_cte_still_gives_its_value() {
    assert_eq!(
        report(
            "WITH ids AS (SELECT value AS id FROM json_each(?1))
             SELECT t.title FROM ids JOIN tracks t ON t.id = ids.id"
        ),
        "tracks: looked up"
    );
}

#[test]
fn a_column_is_volatile_by_its_own_subquery() {
    assert_eq!(
        report(
            "WITH g AS (SELECT (SELECT count(*) FROM releases) AS n, (SELECT random()) AS r)
             SELECT t.title FROM g JOIN tracks t ON t.position = g.n"
        ),
        "releases: whole: nothing narrows `releases`\ntracks: looked up"
    );
}

#[test]
fn a_column_of_a_view_read_whole_is_one_a_subquery_cannot_judge() {
    for sql in [
        "SELECT t.title FROM (SELECT (SELECT id FROM later) AS n) AS page
         JOIN tracks t ON t.position = page.n",
        "SELECT t.title FROM (SELECT id AS n FROM later) AS page
         JOIN tracks t ON t.position = page.n",
        "SELECT t.title FROM (SELECT id || '' AS n FROM unparsed) AS page
         JOIN tracks t ON t.title = page.n",
        "SELECT t.title FROM (SELECT r.id AS n FROM releases r, later) AS page
         JOIN tracks t ON t.release_id = page.n",
        "SELECT t.title FROM (SELECT 1 AS n FROM later) AS page
         JOIN tracks t ON t.position = page.n",
        "SELECT t.title FROM (SELECT count(*) AS n FROM unparsed) AS page
         JOIN tracks t ON t.position = page.n",
        "SELECT t.title FROM (SELECT (SELECT * FROM unparsed) AS n) AS page
         JOIN tracks t ON t.position = page.n",
        "SELECT t.title FROM (SELECT (SELECT count(*) FROM unparsed) AS n) AS page
         JOIN tracks t ON t.position = page.n",
        "SELECT t.title FROM (SELECT EXISTS (SELECT 1 FROM unparsed) AS n) AS page
         JOIN tracks t ON t.position = page.n",
    ] {
        let report = report(sql);
        assert!(report.contains("tracks: whole: nothing narrows `t`"), "{sql}: {report}");
    }
}

#[test]
fn a_table_function_over_the_clock_or_a_random_value_narrows_nothing() {
    for arguments in ["CURRENT_TIMESTAMP", "random()", "json_array(CURRENT_TIMESTAMP)"] {
        for sql in [
            format!(
                "SELECT t.title FROM json_each({arguments}) AS w JOIN tracks t ON t.title = w.value"
            ),
            format!(
                "SELECT t.title FROM tracks t JOIN json_each({arguments}) AS w ON t.title = w.value"
            ),
        ] {
            let report = report(&sql);
            assert!(report.contains("tracks: whole: nothing narrows `t`"), "{sql}: {report}");
        }
    }
}

#[test]
fn a_table_function_over_a_parameter_still_narrows() {
    let report =
        report("SELECT t.title FROM json_each(?1) AS w JOIN tracks t ON t.title = w.value");
    assert_eq!(report, "tracks: looked up");
}

#[test]
fn a_table_function_over_a_column_of_an_earlier_item_narrows_nothing() {
    for argument in
        ["r.id", "r.rowid", "json_array(r.rowid)", "json_array(CURRENT_TIMESTAMP, r.id)"]
    {
        let sql = format!(
            "SELECT t.title FROM releases r, json_each({argument}) AS w
             JOIN tracks t ON t.title = w.value WHERE r.id = ?1"
        );
        let report = report(&sql);
        assert!(report.contains("tracks: whole: nothing narrows `t`"), "{sql}: {report}");
    }
}

#[test]
fn a_subquery_in_a_left_joined_parenthesized_join_narrows_as_a_table_does() {
    let tracks = "(SELECT id, release_id FROM tracks) AS t";
    let picked = "(SELECT id FROM picked) AS p";
    for (first, second, unnarrowed) in [
        ("tracks AS t", "picked AS p", "p"),
        (tracks, "picked AS p", "p"),
        ("tracks AS t", picked, "picked"),
        (tracks, picked, "picked"),
    ] {
        let sql = format!(
            "SELECT r.album, t.id, p.id FROM releases r \
             LEFT JOIN ({first} LEFT JOIN {second} ON p.id = t.id) ON t.release_id = r.id \
             WHERE r.id = ?1"
        );
        assert_eq!(
            report(&sql),
            format!(
                "picked: whole: nothing narrows `{unnarrowed}`\n\
                 releases: keyed by id\ntracks: keyed by release_id"
            ),
            "{sql}"
        );
    }
}

#[path = "read_report_tests/places.rs"]
mod places;
