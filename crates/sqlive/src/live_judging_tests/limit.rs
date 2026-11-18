//! A statement that orders by columns of one table and returns as many rows
//! as its LIMIT depends only on the rows that sort at or before its last
//! row, as a list window read as one live query does (§4). A LIMIT whose
//! ORDER BY doesn't tell its rows apart depends on every column of its
//! tables, and so does an aggregate whose rows follow the order SQLite reads
//! its rows in.

use super::lookup::follows;
use crate::tests::TestDatabase;
use crate::SqlValue::{Integer, Text};

const NOTES: &str = "CREATE TABLE notes(id INTEGER PRIMARY KEY, title TEXT, body TEXT);
INSERT INTO notes VALUES (1,'b',''),(2,'d',''),(3,'f',''),(4,'h',''),(5,'j',''),(6,'l','')";

/// The window from `c` holds d, f and h. A change after h reruns nothing, and
/// a change inside the window reruns it. Each rerun moves the bound to the
/// window's new last row.
#[tokio::test]
async fn a_full_window_reruns_only_for_rows_up_to_its_last() {
    let db = TestDatabase::open(NOTES);
    follows(
        &db,
        "SELECT title, id, body FROM notes WHERE title >= ?1 ORDER BY title LIMIT ?2",
        vec![Text(b"c".to_vec()), Integer(3)],
        &[
            ("INSERT INTO notes VALUES (7,'k','')", false),
            ("INSERT INTO notes VALUES (8,'z','')", false),
            ("UPDATE notes SET body = 'x' WHERE title = 'j'", false),
            ("INSERT INTO notes VALUES (9,'a','')", false),
            ("UPDATE notes SET body = 'x' WHERE title = 'f'", true),
            // The window now holds d, e and f.
            ("INSERT INTO notes VALUES (10,'e','')", true),
            ("INSERT INTO notes VALUES (11,'g','')", false),
            ("UPDATE notes SET body = 'y' WHERE title = 'h'", false),
            // After deleting e, the window holds d, f and g.
            ("DELETE FROM notes WHERE title = 'e'", true),
            ("UPDATE notes SET body = 'y' WHERE title = 'g'", true),
            ("UPDATE notes SET body = 'z' WHERE title = 'h'", false),
            // A row equal to the last one could sort before it.
            ("INSERT INTO notes VALUES (12,'g','')", true),
        ],
    )
    .await;
    db.close();
}

/// A row that moves into the window from after it, or out of the window,
/// reruns it.
#[tokio::test]
async fn a_row_moving_into_or_out_of_a_full_window_reruns_it() {
    let db = TestDatabase::open(NOTES);
    follows(
        &db,
        "SELECT title, id, body FROM notes WHERE title >= 'c' ORDER BY title LIMIT 3",
        vec![],
        &[
            // From after the window into it: d, e and f.
            ("UPDATE notes SET title = 'e' WHERE id = 6", true),
            // Out of the window: e, f and h.
            ("UPDATE notes SET title = 'y' WHERE id = 2", true),
            ("UPDATE notes SET body = 'x' WHERE id = 2", false),
            ("UPDATE notes SET body = 'x' WHERE id = 4", true),
        ],
    )
    .await;
    db.close();
}

/// A window that returned fewer rows than its LIMIT already holds every row
/// after its start, so a row added anywhere after the start reruns it.
#[tokio::test]
async fn a_short_window_reruns_for_a_row_anywhere_after_its_start() {
    let db = TestDatabase::open(NOTES);
    follows(
        &db,
        "SELECT title, id, body FROM notes WHERE title >= 'i' ORDER BY title LIMIT 5",
        vec![],
        &[
            ("INSERT INTO notes VALUES (7,'a','')", false),
            ("INSERT INTO notes VALUES (8,'z','')", true),
            ("INSERT INTO notes VALUES (9,'zz','')", true),
        ],
    )
    .await;
    db.close();
}

/// A descending window ends at its smallest row, and sorts NULLs last.
#[tokio::test]
async fn a_descending_window_ends_at_its_smallest_row() {
    let db = TestDatabase::open(NOTES);
    // j, h and f.
    follows(
        &db,
        "SELECT title, id, body FROM notes WHERE title <= 'k' OR title IS NULL ORDER BY title DESC LIMIT 3",
        vec![],
        &[
            ("INSERT INTO notes VALUES (7,'c','')", false),
            ("INSERT INTO notes VALUES (8,NULL,'')", false),
            ("UPDATE notes SET body = 'x' WHERE title = 'd'", false),
            ("UPDATE notes SET body = 'x' WHERE title = 'h'", true),
            ("INSERT INTO notes VALUES (9,'g','')", true),
            ("INSERT INTO notes VALUES (10,'k','')", true),
        ],
    )
    .await;
    db.close();
}

/// A window over two columns, whose last row ties with later rows on the
/// first column, ends at that row's second column. NULLs sort first.
#[tokio::test]
async fn a_window_over_two_columns_ends_within_a_tie() {
    let db = TestDatabase::open(
        "CREATE TABLE albums(id INTEGER PRIMARY KEY, year INTEGER, title TEXT);
INSERT INTO albums VALUES (10,2000,''),(20,2000,''),(30,2005,''),(40,2005,''),(50,2010,'')",
    );
    // 10, 20 and 30.
    follows(
        &db,
        "SELECT title, year, id FROM albums ORDER BY year, id LIMIT 3",
        vec![],
        &[
            ("INSERT INTO albums VALUES (35,2005,'')", false),
            ("UPDATE albums SET title = 'x' WHERE id = 40", false),
            ("UPDATE albums SET title = 'x' WHERE id = 30", true),
            // 10, 20 and 25.
            ("INSERT INTO albums VALUES (25,2005,'')", true),
            ("UPDATE albums SET title = 'y' WHERE id = 30", false),
            // 5, 10 and 20.
            ("INSERT INTO albums VALUES (5,NULL,'')", true),
            ("INSERT INTO albums VALUES (60,2000,'')", false),
            ("INSERT INTO albums VALUES (15,2000,'')", true),
        ],
    )
    .await;
    db.close();
}

/// When sqlive narrows `tracks` by the releases the window joins them to, it
/// looks up only the releases up to the window's last row: a change to a track
/// of a release after the window reruns nothing.
#[tokio::test]
async fn a_join_looks_up_only_the_sorted_rows_up_to_the_last_row() {
    let db = TestDatabase::open(
        "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT, title TEXT);
INSERT INTO releases VALUES ('r1','a'),('r2','b'),('r3','c');
INSERT INTO tracks VALUES (1,'r1','one'),(2,'r2','two'),(3,'r3','three')",
    );
    // a's track and b's.
    follows(
        &db,
        "SELECT t.title, r.title, r.id FROM releases r JOIN tracks t ON t.release = r.id ORDER BY r.title, r.id LIMIT 2",
        vec![],
        &[
            ("INSERT INTO tracks VALUES (4,'r3','four')", false),
            ("UPDATE tracks SET title = 'tres' WHERE id = 3", false),
            ("UPDATE tracks SET title = 'dos' WHERE id = 2", true),
            // a's two tracks.
            ("INSERT INTO tracks VALUES (5,'r1','five')", true),
            ("UPDATE tracks SET title = 'two' WHERE id = 2", false),
        ],
    )
    .await;
    db.close();
}

/// With an OFFSET, an ORDER BY column the statement doesn't return, an ORDER
/// BY expression or a COLLATE, the scope stays as the WHERE gives it, so a row
/// added after the window's last row reruns the window.
#[tokio::test]
async fn an_offset_an_unreturned_column_or_an_expression_narrows_nothing_more() {
    let db = TestDatabase::open(NOTES);
    for sql in [
        "SELECT title, id FROM notes ORDER BY title LIMIT 2 OFFSET 1",
        "SELECT id FROM notes ORDER BY title LIMIT 2",
        "SELECT title, id FROM notes ORDER BY lower(title) LIMIT 2",
        "SELECT title, id FROM notes ORDER BY title COLLATE NOCASE LIMIT 2",
    ] {
        follows(&db, sql, vec![], &[("INSERT INTO notes VALUES (7,'z','')", true)]).await;
        db.commit("DELETE FROM notes WHERE id = 7");
    }
    db.close();
}

/// Two tables with an index on a column the statements below never name,
/// `u.a`, and rows whose order in that index differs from their rowid order.
const PICKED: &str =
    "CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b, c INTEGER, n INTEGER COLLATE NOCASE);
CREATE TABLE w(id INTEGER PRIMARY KEY, a INTEGER, b, c INTEGER, n INTEGER COLLATE NOCASE);
CREATE INDEX u_a ON u(a);
CREATE INDEX w_c ON w(c);
INSERT INTO u VALUES (0,3,0,5,3),(1,3,0,4,3),(2,3,3,1,2),(3,1,1,2,2),(4,1,0,2,0),(5,3,1,3,3);
INSERT INTO w VALUES (0,1,NULL,5,2),(1,0,1,0,2),(2,3,1,5,0),(3,3,0,4,1),(4,2,NULL,0,2),(5,3,3,1,0)";

/// A LIMIT whose ORDER BY doesn't tell its rows apart keeps the rows SQLite
/// reads first, in the order of the plan it picks: here the index on `u.a`,
/// which the statement never names, so a write to `u.a` changes the row the
/// subquery keeps, and a write to any column of `u` reruns the query. A
/// scalar subquery keeps its first row the same way.
#[tokio::test]
async fn a_limit_that_doesnt_tell_its_rows_apart_reruns_on_any_column() {
    let db = TestDatabase::open(PICKED);
    follows(
        &db,
        "SELECT x.id, d.a, y.c FROM w AS x JOIN (SELECT x.id, x.a, x.b, x.c FROM w AS x) AS d \
         ON d.b = x.b LEFT JOIN u AS y ON y.c = d.c \
         WHERE x.id IN (SELECT q.id FROM u AS q LIMIT 1) AND y.id >= 1",
        vec![],
        &[("UPDATE u SET a = 1 WHERE id = 1", true), ("UPDATE u SET n = 1 WHERE id = 2", true)],
    )
    .await;
    db.close();
    let db = TestDatabase::open(PICKED);
    follows(
        &db,
        "SELECT x.id FROM w AS x WHERE x.id = (SELECT q.id FROM u AS q)",
        vec![],
        &[("UPDATE u SET a = 0 WHERE id = 5", true)],
    )
    .await;
    db.close();
}

/// An ORDER BY that sorts by the rowid tells the rows apart, so a write to a
/// column the statement doesn't name reruns nothing.
#[tokio::test]
async fn a_limit_ordered_by_the_rowid_depends_only_on_the_named_columns() {
    let db = TestDatabase::open(PICKED);
    follows(
        &db,
        "SELECT x.id FROM w AS x WHERE x.id IN (SELECT q.id FROM u AS q ORDER BY q.id LIMIT 1)",
        vec![],
        &[("UPDATE u SET a = 0 WHERE id = 5", false), ("DELETE FROM u WHERE id = 0", true)],
    )
    .await;
    db.close();
}

/// A core that aggregates takes a column outside every aggregate call and
/// GROUP BY term from one row of its group, and `group_concat` joins its
/// values, in the order SQLite reads the rows: here through the index on
/// `u.a`, which the statement never names, so a write to `u.a` changes the
/// result, and a write to any column of `u` reruns the query.
#[tokio::test]
async fn an_aggregate_that_follows_the_order_sqlite_reads_reruns_on_any_column() {
    for sql in ["SELECT x.id, count(*) FROM u AS x", "SELECT group_concat(x.id, '') FROM u AS x"] {
        let db = TestDatabase::open(PICKED);
        follows(
            &db,
            sql,
            vec![],
            &[("UPDATE u SET a = 0 WHERE id = 5", true), ("UPDATE u SET n = 1 WHERE id = 2", true)],
        )
        .await;
        db.close();
    }
}

/// Two rows whose values compare equal and differ: `1` and `1.0` in the
/// untyped `b`, and `'abc'` and `'ABC'` in the NOCASE `t`. SQLite reads `v`
/// through the covering index on `v(a, b, t)`, ordered by `a`, which the
/// statements never name.
const EQUAL: &str = "CREATE TABLE v(id INTEGER PRIMARY KEY, a INTEGER, b, t TEXT COLLATE NOCASE, \
x INTEGER);
CREATE INDEX v_abt ON v(a, b, t);
INSERT INTO v VALUES (1, 2, 1, 'abc', 0), (2, 1, 1.0, 'ABC', 0)";

/// Among values that compare equal, min and max keep the one SQLite reads
/// first, so a write to `v.a` changes the value they keep, and a write to
/// any column of `v` reruns the query. An INTEGER column's equal values are
/// the same, so max of one depends only on the columns the statement names.
#[tokio::test]
async fn min_and_max_over_values_that_compare_equal_and_differ_rerun_on_any_column() {
    for sql in [
        "SELECT quote(max(x.b)) FROM v AS x",
        "SELECT max(x.t) FROM v AS x",
        "SELECT min(x.t) FROM v AS x",
    ] {
        let db = TestDatabase::open(EQUAL);
        follows(
            &db,
            sql,
            vec![],
            &[("UPDATE v SET a = 3 WHERE id = 2", true), ("UPDATE v SET x = 1 WHERE id = 1", true)],
        )
        .await;
        db.close();
    }
    let db = TestDatabase::open(EQUAL);
    follows(
        &db,
        "SELECT max(x.id) FROM v AS x",
        vec![],
        &[("UPDATE v SET x = 1 WHERE id = 1", false), ("DELETE FROM v WHERE id = 2", true)],
    )
    .await;
    db.close();
}

/// An aggregate's own ORDER BY that sorts by the rowid fixes the order it
/// joins its values in, and count, and max of an INTEGER column, give the
/// same value in every order, so a write to a column the statement doesn't
/// name reruns nothing.
#[tokio::test]
async fn an_aggregate_in_a_fixed_order_depends_only_on_the_named_columns() {
    for sql in [
        "SELECT group_concat(x.id, '' ORDER BY x.id) FROM u AS x",
        "SELECT count(*), max(x.id) FROM u AS x",
    ] {
        let db = TestDatabase::open(PICKED);
        follows(
            &db,
            sql,
            vec![],
            &[("UPDATE u SET a = 0 WHERE id = 5", false), ("DELETE FROM u WHERE id = 0", true)],
        )
        .await;
        db.close();
    }
}
