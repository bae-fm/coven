//! Each place a statement reads a table has its own entry.

use super::{report_of, SCHEMA};
use crate::read_report::read_report;

#[track_caller]
fn assert_report(sql: &str, entries: &[&str]) {
    assert_eq!(report_of(sql).to_string(), entries.join("\n"), "{sql}");
}

#[test]
fn a_place_that_narrows_a_table_and_one_that_does_not_have_their_own_entries() {
    assert_report(
        "SELECT (SELECT a.title FROM tracks a WHERE a.id = ?1),
  (SELECT count(*) FROM tracks b)",
        &[
            "tracks at line 1, column 29: keyed by id",
            "tracks at line 2, column 25: whole: nothing narrows `b`",
        ],
    );
}

#[test]
fn two_places_that_read_a_table_by_one_name_have_their_own_entries() {
    assert_report(
        "SELECT (SELECT count(*) FROM tracks a), (SELECT max(id) FROM tracks a)",
        &[
            "tracks at line 1, column 30: whole: nothing narrows `a`",
            "tracks at line 1, column 62: whole: nothing narrows `a`",
        ],
    );
}

#[test]
fn a_cte_that_two_names_read_gives_its_place_one_entry() {
    assert_report(
        "WITH c AS (SELECT id FROM tracks) SELECT x.id FROM c x, c y",
        &["tracks at line 1, column 27: whole: nothing narrows `tracks`"],
    );
}

/// The parameters the plan numbers, a schema's name, quotes and a column of
/// the table's name leave each place where the SQL writes it, and a column is
/// counted in characters.
#[test]
fn a_place_is_where_the_sql_writes_the_tables_name() {
    assert_report(
        "SELECT 'é' AS tracks FROM main.\"tracks\" WHERE title = :a_long_parameter_name
UNION ALL SELECT [tracks].title FROM [tracks]",
        &[
            "tracks at line 1, column 32: keyed by title",
            "tracks at line 2, column 38: whole: nothing narrows `[tracks]`",
        ],
    );
}

#[test]
fn a_table_a_view_names_is_in_the_view() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection.execute_batch(SCHEMA).unwrap();
    connection.execute_batch("CREATE VIEW every AS SELECT id FROM releases").unwrap();
    let report = read_report(&connection, "SELECT id FROM every").unwrap();
    assert_eq!(report.to_string(), "releases in `every`: whole: nothing narrows `releases`");
}
