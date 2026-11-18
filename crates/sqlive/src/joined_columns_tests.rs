use super::{joined_columns, JoinedColumns};
use sqlite3_parser::{
    ast::{Cmd, Stmt},
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

fn connection() -> rusqlite::Connection {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE a(id INTEGER PRIMARY KEY, K INTEGER, v INTEGER);
             CREATE TABLE b(id INTEGER PRIMARY KEY, k INTEGER, w INTEGER);
             CREATE VIEW ab AS SELECT a.v, b.w FROM a NATURAL JOIN b",
        )
        .unwrap();
    connection
}

fn pairs(found: &[(&str, &str)]) -> super::ColumnSet {
    found.iter().map(|(table, column)| ((*table).to_owned(), (*column).to_owned())).collect()
}

fn joined(connection: &rusqlite::Connection, sql: &str) -> JoinedColumns {
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    let Ok(Some(Cmd::Stmt(Stmt::Select(select)))) = parser.next() else {
        panic!("the parser reads {sql}");
    };
    joined_columns(connection, select).unwrap()
}

#[test]
fn a_using_join_names_its_column_of_each_side_and_a_view_its_natural_join() {
    let connection = connection();
    let JoinedColumns::Known(found) =
        joined(&connection, "SELECT a.v FROM a JOIN b USING (k) WHERE a.v IN (SELECT v FROM ab)")
    else {
        panic!("the joins are known");
    };
    assert_eq!(
        found,
        pairs(&[("a", "id"), ("a", "k"), ("a", "v"), ("b", "id"), ("b", "k"), ("b", "w"),])
    );
}

#[test]
fn unknown_joins_count_every_column_of_every_table() {
    let connection = connection();
    let unknown = JoinedColumns::unknown(&connection, "a test".to_owned()).unwrap();
    assert!(matches!(unknown, JoinedColumns::Unknown(_)));
    assert_eq!(
        unknown.columns(),
        &pairs(&[("a", "id"), ("a", "k"), ("a", "v"), ("b", "id"), ("b", "k"), ("b", "w"),])
    );
}

/// A USING column compares with the leftmost item before the join that has
/// it: a table after that item compares nothing, and a CTE of a table's name
/// is not that table.
#[test]
fn a_using_join_names_the_leftmost_column_before_it() {
    let connection = connection();
    for (sql, expected) in [
        ("SELECT a.v FROM a CROSS JOIN b AS c JOIN b USING (k)", &[("a", "k"), ("b", "k")][..]),
        (
            "WITH a AS (SELECT 1 AS id) SELECT * FROM a CROSS JOIN b AS c JOIN b USING (k)",
            &[("b", "k")][..],
        ),
        ("SELECT * FROM (SELECT 1 AS k) AS s CROSS JOIN a JOIN b USING (k)", &[("b", "k")][..]),
        (
            "SELECT * FROM (SELECT a.id FROM a) AS s CROSS JOIN a JOIN b USING (k)",
            &[("a", "k"), ("b", "k")][..],
        ),
        (
            "SELECT * FROM (SELECT k + 1 FROM a) AS s CROSS JOIN a JOIN b USING (k)",
            &[("a", "k"), ("b", "k")][..],
        ),
    ] {
        let JoinedColumns::Known(found) = joined(&connection, sql) else {
            panic!("the joins are known");
        };
        assert_eq!(found, pairs(expected), "{sql}");
    }
}
