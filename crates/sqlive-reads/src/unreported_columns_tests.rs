use super::{unreported_columns, UnreportedColumns};
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

fn unreported(connection: &rusqlite::Connection, sql: &str) -> UnreportedColumns {
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    let Ok(Some(Cmd::Stmt(Stmt::Select(select)))) = parser.next() else {
        panic!("the parser reads {sql}");
    };
    let functions = crate::select_syntax::Functions::read(connection).unwrap();
    let schema = crate::MainSchema::read(connection).unwrap();
    unreported_columns(connection, &schema, &functions, select).unwrap()
}

#[test]
fn a_using_join_names_its_column_of_each_side_and_a_view_its_natural_join() {
    let connection = connection();
    let UnreportedColumns::Known(found) = unreported(
        &connection,
        "SELECT a.v FROM a JOIN b USING (k) WHERE a.v IN (SELECT v FROM ab)",
    ) else {
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
    let schema = crate::MainSchema::read(&connection).unwrap();
    let unknown = UnreportedColumns::unknown(&schema, "a test".to_owned()).unwrap();
    assert!(matches!(unknown, UnreportedColumns::Unknown(_)));
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
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the joins are known");
        };
        assert_eq!(found, pairs(expected), "{sql}");
    }
}

/// A query whose rows SQLite picks reads every column of each table it reads:
/// through a LIMIT, an OFFSET, a scalar subquery or a row value compared
/// with a subquery, in its FROM, a view, a CTE, or a query inside it.
#[test]
fn a_query_sqlite_picks_rows_of_reads_every_column_of_its_tables() {
    let connection = connection();
    let a = [("a", "id"), ("a", "k"), ("a", "v")];
    let b = [("b", "id"), ("b", "k"), ("b", "w")];
    let both: Vec<_> = a.iter().chain(&b).copied().collect();
    for (sql, expected) in [
        ("SELECT a.v FROM a LIMIT 1", &a[..]),
        ("SELECT a.v FROM a LIMIT 1 OFFSET 1", &a[..]),
        ("SELECT a.v FROM a WHERE a.v = (SELECT b.w FROM b)", &b[..]),
        ("SELECT a.v FROM a WHERE (a.k, a.v) = (SELECT b.k, b.w FROM b)", &b[..]),
        ("SELECT a.v FROM a WHERE a.id IN (SELECT b.id FROM b LIMIT 1)", &b[..]),
        ("SELECT (SELECT b.w FROM b WHERE b.k = a.k) FROM a", &b[..]),
        (
            "SELECT x.v FROM (SELECT a.v FROM a WHERE a.k IN (SELECT b.k FROM b)) AS x LIMIT 2",
            &both[..],
        ),
        // The view's NATURAL join compares every column of both tables too.
        ("SELECT v FROM ab LIMIT 1", &both[..]),
        (
            "WITH c AS (SELECT * FROM d), d AS (SELECT b.id FROM b) \
             SELECT a.v FROM a WHERE a.id IN (SELECT c.id FROM c LIMIT 1)",
            &b[..],
        ),
        ("SELECT a.v FROM a ORDER BY a.v LIMIT 1", &a[..]),
        ("SELECT a.v FROM a JOIN b ON b.k = a.k ORDER BY a.id LIMIT 1", &both[..]),
        ("SELECT a.k, max(a.v) FROM a GROUP BY a.k ORDER BY a.k LIMIT 1", &a[..]),
        ("SELECT DISTINCT a.v FROM a ORDER BY a.id LIMIT 1", &a[..]),
        // With two arguments, max is SQLite's scalar max, so the SELECT
        // returns a row of each row of `a`.
        ("SELECT max(a.k, a.v) FROM a LIMIT 1", &a[..]),
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert_eq!(found, pairs(expected), "{sql}");
    }
}

/// An ORDER BY that sorts by the rowid of each table of its SELECT tells
/// every two rows apart, so the rows a LIMIT keeps depend only on the columns
/// the statement names: by the INTEGER PRIMARY KEY column, named or by its
/// result column's position or alias, or by `rowid`. An aggregate with no
/// GROUP BY returns one row, which every order keeps. A WITHOUT ROWID
/// table's primary key holds no NULL, so an ORDER BY over it tells rows apart
/// too. A primary key that isn't the rowid can otherwise hold NULL in many
/// rows.
#[test]
fn an_order_by_each_tables_rowid_reads_only_the_named_columns() {
    let connection = connection();
    connection
        .execute_batch(
            "CREATE TABLE p(id INT PRIMARY KEY, x INTEGER);
             CREATE TABLE q(id INTEGER PRIMARY KEY DESC, x INTEGER);
             CREATE TABLE r(id INTEGER PRIMARY KEY, x INTEGER) WITHOUT ROWID;
             CREATE TABLE s(id INTEGER, x INTEGER, PRIMARY KEY (id DESC))",
        )
        .unwrap();
    for sql in [
        "SELECT a.v FROM a",
        "SELECT a.v FROM a ORDER BY a.id LIMIT 1",
        "SELECT a.v, a.id FROM a ORDER BY 2 DESC LIMIT 1",
        "SELECT a.v, a.id AS key FROM a ORDER BY key LIMIT 1",
        "SELECT a.v FROM a ORDER BY rowid LIMIT 1",
        "SELECT a.v FROM a JOIN b ON b.k = a.k ORDER BY a.v, b.id, a.id LIMIT 1",
        "SELECT a.v FROM a WHERE a.v = (SELECT b.w FROM b ORDER BY b.id DESC LIMIT 1)",
        "SELECT a.v FROM a WHERE a.v = (SELECT max(b.w) FROM b WHERE b.k = a.k)",
        "SELECT count(*) FROM a LIMIT 1",
        "SELECT s.x FROM s ORDER BY s.id LIMIT 1",
        "SELECT r.x FROM r ORDER BY r.id LIMIT 1",
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert_eq!(found, pairs(&[]), "{sql}");
    }
    for (sql, table) in [
        ("SELECT p.x FROM p ORDER BY p.id LIMIT 1", "p"),
        ("SELECT q.x FROM q ORDER BY q.id LIMIT 1", "q"),
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert_eq!(found, pairs(&[(table, "id"), (table, "x")]), "{sql}");
    }
}

/// Tables with each kind of unique key a row can be told apart by, and some
/// that tell none apart.
const KEYED: &str = "
    CREATE TABLE pair(l INTEGER NOT NULL, r INTEGER NOT NULL, x INTEGER, UNIQUE (l, r));
    CREATE TABLE loose(l INTEGER UNIQUE, x INTEGER);
    CREATE TABLE named(name TEXT PRIMARY KEY, x INTEGER);
    CREATE TABLE folded(name TEXT NOT NULL, x INTEGER);
    CREATE UNIQUE INDEX folded_name ON folded(name COLLATE NOCASE);
    CREATE TABLE some(l INTEGER NOT NULL, x INTEGER);
    CREATE UNIQUE INDEX some_l ON some(l) WHERE x > 0;
    CREATE UNIQUE INDEX some_sum ON some(l + x);
    CREATE TABLE words(id INTEGER PRIMARY KEY, word TEXT, shout TEXT COLLATE NOCASE, x);
    CREATE VIEW seen AS SELECT words.id, words.word FROM words";

/// A query whose ORDER BY and equalities cover a unique key of each FROM item
/// depends only on the columns it names: by the sort's columns that hold no
/// NULL, by equalities with the items told apart before, and through a FROM
/// subquery that such equalities let return one row at most.
#[test]
fn a_query_whose_sort_and_joins_tell_its_rows_apart_reads_only_the_named_columns() {
    let connection = connection();
    connection.execute_batch(KEYED).unwrap();
    for sql in [
        "SELECT pair.x FROM pair ORDER BY pair.l, pair.r LIMIT 1",
        "SELECT pair.x FROM pair WHERE pair.l = ?1 ORDER BY pair.r LIMIT 1",
        "SELECT a.v FROM a LEFT JOIN b ON b.id = a.k ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a, b WHERE a.k = b.id ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a LEFT JOIN loose ON loose.l = a.k ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a LEFT JOIN pair ON pair.l = a.k AND pair.r = a.v + 1 \
         ORDER BY a.id LIMIT 1",
        // The third table takes its key from the second.
        "SELECT a.v FROM a LEFT JOIN words AS w ON w.id = a.k \
         LEFT JOIN named AS n ON n.name = w.word ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN named AS n ON n.name = ?1 ORDER BY a.id LIMIT 1",
        // A subquery that equalities with the row around it let return one
        // row at most picks none.
        "SELECT (SELECT b.w FROM b WHERE b.id = a.k) FROM a",
        "SELECT (SELECT n.x FROM named AS n WHERE n.name = w.word) FROM words AS w",
        // A FROM subquery: one row for each group key, one for each key of
        // the table it passes on, or one in all.
        "SELECT a.v FROM a LEFT JOIN (SELECT b.k, count(*) AS n FROM b GROUP BY b.k) AS g \
         ON g.k = a.k ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a LEFT JOIN (SELECT g.* FROM (SELECT s.k, count(*) AS n \
         FROM seen AS v JOIN b AS s ON s.id = v.id GROUP BY s.k) AS g) AS g ON g.k = a.k \
         ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN (SELECT b.id AS key, b.w FROM b) AS d ON d.key = a.k \
         ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a LEFT JOIN (SELECT count(*) AS n FROM b) AS c ON c.n = a.k \
         ORDER BY a.id LIMIT 1",
        // A key compared with a subquery's value.
        "SELECT a.v FROM a LEFT JOIN b ON b.id = (SELECT max(w.id) FROM words AS w \
         WHERE w.x = a.v) ORDER BY a.id LIMIT 1",
        "SELECT count(*), group_concat(a.v ORDER BY a.id) FROM a LEFT JOIN b ON b.id = a.k",
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert_eq!(found, pairs(&[]), "{sql}");
    }
}

/// A query reads every column of its tables when a sort or an equality leaves
/// some key of a FROM item open: the sort leaves a column of the key out, or
/// sorts one that can hold NULL in many rows; the comparison converts the
/// key's values, or calls equal what the key tells apart; the index leaves
/// rows out or indexes an expression; the condition is no equality; or only
/// the ON of another item's LEFT JOIN holds it.
#[test]
fn a_query_whose_sort_or_joins_leave_rows_tied_reads_every_column() {
    let connection = connection();
    connection.execute_batch(KEYED).unwrap();
    for sql in [
        "SELECT pair.x FROM pair ORDER BY pair.l LIMIT 1",
        "SELECT loose.x FROM loose ORDER BY loose.l LIMIT 1",
        "SELECT folded.x FROM folded ORDER BY folded.name COLLATE RTRIM LIMIT 1",
        "SELECT a.v FROM a JOIN named AS n ON n.name = a.k ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN words AS w ON w.id = a.k \
         JOIN named AS n ON w.shout = n.name ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN words AS w ON w.id = a.k \
         JOIN folded AS f ON w.shout = f.name COLLATE RTRIM ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN some ON some.l = a.k ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN b ON b.id = a.k OR b.id = a.v ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN b ON b.id >= a.k ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN b ON b.k = a.k ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM b LEFT JOIN a ON b.id = a.id ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a RIGHT JOIN b ON b.id = a.k ORDER BY a.id, b.id LIMIT 1",
        "SELECT a.v FROM a JOIN seen AS s ON s.id = a.k ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN (SELECT b.k, count(*) AS n FROM b GROUP BY b.k, b.w) AS g \
         ON g.k = a.k ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN (SELECT w.shout, count(*) AS n FROM words AS w \
         GROUP BY w.shout) AS g ON g.shout = a.k ORDER BY a.id LIMIT 1",
        "SELECT a.v FROM a JOIN (SELECT b.id AS key FROM b UNION ALL SELECT b.k FROM b) AS d \
         ON d.key = a.k ORDER BY a.id LIMIT 1",
        "SELECT (SELECT b.w FROM b WHERE b.k = a.k) FROM a",
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert!(!found.is_empty(), "{sql}");
    }
}

/// A core that aggregates reads every column of each table it reads when its
/// rows follow the order SQLite reads its rows in: a result column, the
/// HAVING or the ORDER BY names a column or holds a subquery outside every
/// aggregate call and GROUP BY term, it returns a `*`, a result column, the
/// HAVING or the ORDER BY shows a GROUP BY term, or it calls min or max, of
/// anything but a column whose equal values are the same, or it calls an
/// aggregate other than count, min and max with no ORDER BY of its own that
/// tells its rows apart.
#[test]
fn a_core_whose_rows_follow_the_order_it_reads_them_in_reads_every_column() {
    let connection = connection();
    let a = [("a", "id"), ("a", "k"), ("a", "v")];
    let b = [("b", "id"), ("b", "k"), ("b", "w")];
    let both: Vec<_> = a.iter().chain(&b).copied().collect();
    connection
        .execute_batch(
            "CREATE TABLE c(id INTEGER PRIMARY KEY, u, t TEXT COLLATE NOCASE, x TEXT, r REAL)",
        )
        .unwrap();
    let c = [("c", "id"), ("c", "r"), ("c", "t"), ("c", "u"), ("c", "x")];
    for (sql, expected) in [
        ("SELECT a.v, count(*) FROM a", &a[..]),
        ("SELECT a.k, a.v FROM a GROUP BY a.k", &a[..]),
        ("SELECT count(*) FROM a GROUP BY a.k HAVING a.v > 1", &a[..]),
        ("SELECT count(*) FROM a GROUP BY a.k ORDER BY a.v", &a[..]),
        ("SELECT *, count(*) FROM a", &a[..]),
        ("SELECT group_concat(a.v) FROM a", &a[..]),
        ("SELECT coalesce(group_concat(a.v, ', '), '') FROM a GROUP BY a.k", &a[..]),
        ("SELECT sum(a.v) FROM a GROUP BY a.k", &a[..]),
        ("SELECT json_group_array(a.v ORDER BY a.k) FROM a", &a[..]),
        ("SELECT group_concat(a.v ORDER BY a.id) FROM a JOIN b ON b.k = a.k", &both[..]),
        // The core reads b in its subquery, whose value could name a row of a.
        ("SELECT max(a.v), (SELECT b.w FROM b ORDER BY b.id LIMIT 1) FROM a", &both[..]),
        ("SELECT x.n FROM (SELECT a.k, count(*) AS n, a.v FROM a GROUP BY a.k) AS x", &a[..]),
        ("SELECT b.w FROM b WHERE b.k IN (SELECT a.v FROM a GROUP BY a.k)", &a[..]),
        // The scalar max's columns sit in no aggregate.
        ("SELECT a.k, count(*), max(a.k, a.v) FROM a GROUP BY a.k", &a[..]),
        // Values that compare equal can differ: `1` and `1.0` in an untyped
        // column, `'Abc'` and `'ABC'` under NOCASE, and any value of an
        // expression.
        ("SELECT max(c.u) FROM c", &c[..]),
        ("SELECT min(c.t) FROM c GROUP BY c.x", &c[..]),
        ("SELECT max(c.x COLLATE NOCASE) FROM c", &c[..]),
        ("SELECT max(a.v + 1) FROM a", &a[..]),
        ("SELECT max(s.v) FROM (SELECT a.v FROM a) AS s", &a[..]),
        ("SELECT c.t, count(*) FROM c GROUP BY c.t", &c[..]),
        ("SELECT t FROM c GROUP BY 1", &c[..]),
        ("SELECT upper(c.u) FROM c GROUP BY c.u", &c[..]),
        ("SELECT count(*) FROM c GROUP BY c.t HAVING c.t > 'a'", &c[..]),
        ("SELECT count(*) FROM c GROUP BY c.u ORDER BY c.u", &c[..]),
        ("SELECT c.x || '' FROM c GROUP BY c.x || ''", &c[..]),
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert_eq!(found, pairs(expected), "{sql}");
    }
    for sql in [
        "SELECT a.k, count(*) FROM a GROUP BY a.k",
        "SELECT a.k + 1, max(a.v) FROM a GROUP BY a.k",
        "SELECT a.k AS g, min(a.v) FROM a GROUP BY g",
        "SELECT a.k, count(*) FROM a GROUP BY 1",
        "SELECT count(*), max(a.v), 7 FROM a",
        "SELECT count(*) AS n FROM a GROUP BY a.k ORDER BY n",
        "SELECT group_concat(a.v ORDER BY a.id) FROM a",
        "SELECT group_concat(a.v ORDER BY a.k, rowid DESC) FROM a",
        "SELECT json_group_array(a.v ORDER BY a.id, b.id) FROM a JOIN b ON b.k = a.k",
        "SELECT a.v FROM a",
        "SELECT max(c.x), min(c.r), max(c.id), min(rowid) FROM c",
        "SELECT count(*) FROM c GROUP BY c.t",
        "SELECT c.x, c.r + 1, c.id, count(*) FROM c GROUP BY c.x, c.r, c.id",
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert_eq!(found, pairs(&[]), "{sql}");
    }
}

/// DISTINCT, UNION, INTERSECT and EXCEPT keep one of each set of rows that
/// compare equal, which the order SQLite reads them in decides, so a DISTINCT
/// core or a compound reads every column of each table it reads unless each
/// result column of the core, or of each arm, is a column whose equal values
/// are the same.
#[test]
fn a_distinct_or_compound_of_values_that_compare_equal_and_differ_reads_every_column() {
    let connection = connection();
    connection
        .execute_batch(
            "CREATE TABLE c(id INTEGER PRIMARY KEY, u, t TEXT COLLATE NOCASE, x TEXT, r REAL)",
        )
        .unwrap();
    let a = [("a", "id"), ("a", "k"), ("a", "v")];
    let b = [("b", "id"), ("b", "k"), ("b", "w")];
    let c = [("c", "id"), ("c", "r"), ("c", "t"), ("c", "u"), ("c", "x")];
    let a_b: Vec<_> = a.iter().chain(&b).copied().collect();
    let a_c: Vec<_> = a.iter().chain(&c).copied().collect();
    for (sql, expected) in [
        ("SELECT DISTINCT c.t FROM c", &c[..]),
        ("SELECT DISTINCT c.x, c.u FROM c", &c[..]),
        ("SELECT DISTINCT a.v + 1 FROM a", &a[..]),
        ("SELECT DISTINCT * FROM a", &a[..]),
        ("SELECT DISTINCT count(*) FROM a GROUP BY a.k", &a[..]),
        ("SELECT a.v FROM a WHERE a.k IN (SELECT DISTINCT c.t FROM c)", &c[..]),
        (
            "SELECT s.w FROM (SELECT DISTINCT d.w FROM a JOIN (SELECT b.w FROM b) AS d) AS s",
            &a_b[..],
        ),
        ("SELECT c.t FROM c UNION SELECT a.v FROM a", &a_c[..]),
        ("SELECT a.v FROM a INTERSECT SELECT c.u FROM c", &a_c[..]),
        ("SELECT a.v + 1 FROM a EXCEPT SELECT b.w FROM b", &a_b[..]),
        ("SELECT a.v FROM a UNION VALUES (1)", &a[..]),
        ("SELECT * FROM a UNION SELECT * FROM a", &a[..]),
        (
            "SELECT a.v FROM a UNION ALL SELECT b.w FROM b UNION SELECT c.t FROM c",
            &[&a_b[..], &c[..]].concat()[..],
        ),
        ("SELECT b.w FROM b WHERE b.k IN (SELECT a.v FROM a UNION SELECT c.t FROM c)", &a_c[..]),
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert_eq!(found, pairs(expected), "{sql}");
    }
    for sql in [
        "SELECT DISTINCT c.x, c.r, c.id FROM c",
        "SELECT DISTINCT a.v, b.w FROM a JOIN b ON b.k = a.k",
        // A table's column compares as it does beside any other FROM item.
        "SELECT s.v FROM (SELECT DISTINCT a.v FROM a JOIN (SELECT b.w FROM b) AS d) AS s",
        "SELECT a.v FROM a UNION SELECT b.w FROM b",
        "SELECT c.t FROM c UNION ALL SELECT a.v FROM a",
        "SELECT c.x, c.r, c.id FROM c EXCEPT SELECT a.k, a.v, rowid FROM a",
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert_eq!(found, pairs(&[]), "{sql}");
    }
}

/// A window function numbers or reads the rows of a partition in the order
/// SQLite reads them among those its ORDER BY sorts the same, so a core that
/// calls one reads every column of each table it reads, unless the window's
/// ORDER BY sorts by the rowid of each of its tables and the core doesn't
/// aggregate.
#[test]
fn a_window_that_doesnt_tell_its_rows_apart_reads_every_column() {
    let connection = connection();
    let a = [("a", "id"), ("a", "k"), ("a", "v")];
    let b = [("b", "id"), ("b", "k"), ("b", "w")];
    let both: Vec<_> = a.iter().chain(&b).copied().collect();
    for (sql, expected) in [
        ("SELECT a.v, row_number() OVER () FROM a", &a[..]),
        ("SELECT a.v, row_number() OVER (PARTITION BY a.k ORDER BY a.v) FROM a", &a[..]),
        ("SELECT a.v FROM a ORDER BY row_number() OVER (ORDER BY a.k)", &a[..]),
        ("SELECT a.v, rank() OVER w FROM a WINDOW w AS (ORDER BY a.v)", &a[..]),
        ("SELECT a.k, count(*), row_number() OVER (ORDER BY a.k) FROM a GROUP BY a.k", &a[..]),
        ("SELECT sum(count(*)) OVER (ORDER BY a.id) FROM a", &a[..]),
        ("SELECT a.v, row_number() OVER (ORDER BY a.id) FROM a JOIN b ON b.k = a.k", &both[..]),
        ("SELECT a.v, row_number() OVER (ORDER BY s.v) FROM (SELECT a.v FROM a) AS s", &a[..]),
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert_eq!(found, pairs(expected), "{sql}");
    }
    for sql in [
        "SELECT a.v, row_number() OVER (ORDER BY a.id) FROM a",
        "SELECT a.v, row_number() OVER (PARTITION BY a.k ORDER BY a.v, a.id DESC) FROM a",
        "SELECT a.v, row_number() OVER w FROM a WINDOW w AS (ORDER BY rowid)",
        "SELECT a.v, row_number() OVER (w ORDER BY a.id) FROM a WINDOW w AS (PARTITION BY a.k)",
        "SELECT row_number() OVER (ORDER BY a.id, b.id) FROM a JOIN b ON b.k = a.k",
    ] {
        let UnreportedColumns::Known(found) = unreported(&connection, sql) else {
            panic!("the columns are known: {sql}");
        };
        assert_eq!(found, pairs(&[]), "{sql}");
    }
}
