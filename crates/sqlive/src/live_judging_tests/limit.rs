//! A statement that orders by columns of its tables and returns as many rows
//! as its LIMIT depends only on the rows that sort at or before its last
//! row, as a list window read as one live query does (§4). A LIMIT whose
//! ORDER BY doesn't tell its rows apart depends on every column of its
//! tables, and so do an aggregate, a DISTINCT and a compound whose rows
//! follow the order SQLite reads its rows in.

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

/// A window ordered by expressions it returns, as an album grid sorted by
/// year lists the albums with no year last, ends at its last row's values of
/// them, which SQLite computes for each changed row.
#[tokio::test]
async fn a_window_ordered_by_expressions_ends_at_its_last_rows_values() {
    let db = TestDatabase::open(
        "CREATE TABLE albums(id INTEGER PRIMARY KEY, year INTEGER, title TEXT);
INSERT INTO albums VALUES (1,1990,''),(2,1995,''),(3,NULL,''),(4,2000,''),(5,2005,'')",
    );
    // 1, 2 and 4.
    follows(
        &db,
        "SELECT id, title, year IS NULL, year FROM albums ORDER BY year IS NULL, year, id LIMIT 3",
        vec![],
        &[
            ("INSERT INTO albums VALUES (6,2010,'')", false),
            // No year sorts after every year.
            ("INSERT INTO albums VALUES (7,NULL,'')", false),
            ("UPDATE albums SET title = 'x' WHERE id = 5", false),
            ("UPDATE albums SET title = 'x' WHERE id = 2", true),
            // 1, 8 and 2.
            ("INSERT INTO albums VALUES (8,1992,'')", true),
            ("UPDATE albums SET title = 'y' WHERE id = 4", false),
            // 1, 2 and 4.
            ("UPDATE albums SET year = NULL WHERE id = 8", true),
            // 1, 2 and 3.
            ("UPDATE albums SET year = 1999 WHERE id = 3", true),
            ("UPDATE albums SET title = 'z' WHERE id = 4", false),
        ],
    )
    .await;
    db.close();
}

/// A window ordered by a generated column ends at its last row's value of
/// it, which the column's expression gives each changed row.
#[tokio::test]
async fn a_window_ordered_by_a_generated_column_ends_at_its_last_rows_value() {
    let db = TestDatabase::open(
        "CREATE TABLE releases(id INTEGER PRIMARY KEY, stored INTEGER, title TEXT,
    rank INTEGER GENERATED ALWAYS AS (CASE WHEN stored THEN 0 ELSE 1 END));
INSERT INTO releases(id, stored, title) VALUES (1,1,''),(2,0,''),(3,1,''),(4,0,''),(5,1,'')",
    );
    // 2 and 4, whose rank is 1.
    follows(
        &db,
        "SELECT id, title, rank FROM releases ORDER BY rank DESC, id LIMIT 2",
        vec![],
        &[
            ("INSERT INTO releases(id, stored, title) VALUES (6,1,'')", false),
            ("UPDATE releases SET title = 'x' WHERE id = 1", false),
            ("INSERT INTO releases(id, stored, title) VALUES (7,0,'')", false),
            // 2 and 3.
            ("UPDATE releases SET stored = 0 WHERE id = 3", true),
            ("UPDATE releases SET title = 'x' WHERE id = 4", false),
            // 3 and 4.
            ("UPDATE releases SET stored = 1 WHERE id = 2", true),
        ],
    )
    .await;
    db.close();
}

/// A generated column's value takes the column's affinity: a TEXT column of
/// `n * 10` holds `'100'`, which sorts before `'20'`, though 100 is greater
/// than 20.
#[tokio::test]
async fn a_generated_columns_value_sorts_in_the_columns_type() {
    let db = TestDatabase::open(
        "CREATE TABLE files(id INTEGER PRIMARY KEY, n INTEGER,
    label TEXT GENERATED ALWAYS AS (n * 10));
INSERT INTO files(id, n) VALUES (1,1),(2,2),(3,3),(4,5)",
    );
    // '10' and '20'.
    follows(
        &db,
        "SELECT id, label FROM files ORDER BY label LIMIT 2",
        vec![],
        &[
            // '10' and '100'.
            ("INSERT INTO files(id, n) VALUES (5,10)", true),
            ("INSERT INTO files(id, n) VALUES (6,4)", false),
            ("INSERT INTO files(id, n) VALUES (7,2)", false),
            // '10' and '10'.
            ("UPDATE files SET n = 1 WHERE id = 3", true),
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

const ARTISTS: &str = "CREATE TABLE artists(id INTEGER PRIMARY KEY, sort_key TEXT);
CREATE TABLE albums(id INTEGER PRIMARY KEY, artist INTEGER, title TEXT, note TEXT);
INSERT INTO artists VALUES (1,'b'),(2,'d'),(3,'f');
INSERT INTO albums VALUES (1,1,'x',''),(2,1,'y',''),(3,2,'m',''),(4,2,'n',''),(5,3,'a','')";

/// A window ordered by its artists' sort keys, then by its albums' titles,
/// holds b's x and y and d's m. An artist after d, an album of an artist
/// after d, and an album of d after m rerun nothing. An album of an artist
/// before d reruns the window, wherever its title sorts.
#[tokio::test]
async fn a_window_ordered_across_a_join_ends_at_its_last_rows_values() {
    let db = TestDatabase::open(ARTISTS);
    follows(
        &db,
        "SELECT ar.sort_key, al.title, al.id FROM artists ar JOIN albums al ON al.artist = ar.id \
         ORDER BY ar.sort_key, al.title, al.id LIMIT 3",
        vec![],
        &[
            ("INSERT INTO artists VALUES (4,'z')", false),
            ("INSERT INTO albums VALUES (6,3,'a','')", false),
            ("INSERT INTO albums VALUES (7,2,'z','')", false),
            ("UPDATE albums SET note = 'x' WHERE id = 4", false),
            ("UPDATE albums SET note = 'x' WHERE id = 3", true),
            // The window now ends at d's c.
            ("INSERT INTO albums VALUES (8,2,'c','')", true),
            ("UPDATE artists SET sort_key = 'e' WHERE id = 3", false),
            // f's albums, now a's, come first: a's two albums, then b's x.
            ("UPDATE artists SET sort_key = 'a' WHERE id = 3", true),
            ("INSERT INTO albums VALUES (9,2,'a','')", false),
            ("INSERT INTO albums VALUES (10,1,'w','')", true),
        ],
    )
    .await;
    db.close();
}

/// When the planner reads the albums before the artists, as a condition on
/// the albums alone makes it, the albums take no values from the artists, so
/// only the artists' sort keys bound the window: an artist after the last
/// row's reruns nothing, and an album of that artist reruns it.
#[tokio::test]
async fn a_later_runs_table_that_takes_no_values_from_an_earlier_ones_has_no_bound() {
    let db = TestDatabase::open(ARTISTS);
    follows(
        &db,
        "SELECT ar.sort_key, al.title, al.id FROM artists ar JOIN albums al ON al.artist = ar.id \
         WHERE al.title >= 'a' ORDER BY ar.sort_key, al.title, al.id LIMIT 3",
        vec![],
        &[
            ("INSERT INTO artists VALUES (4,'z')", false),
            ("INSERT INTO albums VALUES (6,4,'b','')", true),
        ],
    )
    .await;
    db.close();
}

/// With an OFFSET, an ORDER BY column or expression the statement doesn't
/// return, or a COLLATE, the scope stays as the WHERE gives it, so a row added
/// after the window's last row reruns the window.
#[tokio::test]
async fn an_offset_an_unreturned_term_or_a_collate_narrows_nothing_more() {
    let db = TestDatabase::open(NOTES);
    for sql in [
        "SELECT title, id FROM notes ORDER BY title LIMIT 2 OFFSET 1",
        "SELECT id FROM notes ORDER BY title LIMIT 2",
        "SELECT title, id FROM notes ORDER BY lower(title) LIMIT 2",
        "SELECT title, id FROM notes ORDER BY title COLLATE NOCASE LIMIT 2",
        "SELECT title, id, lower(title) COLLATE NOCASE FROM notes ORDER BY lower(title) COLLATE NOCASE LIMIT 2",
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

/// Among values that compare equal, a GROUP BY term keeps the one SQLite
/// reads first, so a write to `v.a` changes the value a result column that
/// shows the term holds, and a write to any column of `v` reruns the query. A
/// GROUP BY term the result doesn't show, or an INTEGER one, depends only on
/// the columns the statement names.
#[tokio::test]
async fn a_grouped_value_that_compares_equal_to_another_and_differs_reruns_on_any_column() {
    for sql in ["SELECT x.b FROM v AS x GROUP BY x.b", "SELECT x.t FROM v AS x GROUP BY x.t"] {
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
    for (sql, write) in [
        ("SELECT count(*) FROM v AS x GROUP BY x.t", "UPDATE v SET t = 'abd' WHERE id = 2"),
        ("SELECT x.a FROM v AS x GROUP BY x.a", "DELETE FROM v WHERE id = 2"),
    ] {
        let db = TestDatabase::open(EQUAL);
        follows(&db, sql, vec![], &[("UPDATE v SET x = 1 WHERE id = 1", false), (write, true)])
            .await;
        db.close();
    }
}

/// DISTINCT keeps the first of each set of rows that compare equal that
/// SQLite reads, so a write to `v.a` changes the value a DISTINCT core of
/// `v.b` or `v.t` returns, and a write to any column of `v` reruns the query.
/// A DISTINCT core of INTEGER columns depends only on the columns the
/// statement names.
#[tokio::test]
async fn a_distinct_value_that_compares_equal_to_another_and_differs_reruns_on_any_column() {
    for sql in ["SELECT DISTINCT x.b FROM v AS x", "SELECT DISTINCT x.t FROM v AS x"] {
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
        "SELECT DISTINCT x.a, x.id FROM v AS x",
        vec![],
        &[("UPDATE v SET x = 1 WHERE id = 1", false), ("DELETE FROM v WHERE id = 2", true)],
    )
    .await;
    db.close();
}

/// UNION, INTERSECT and EXCEPT keep one of each set of rows that compare
/// equal, which the order SQLite reads them in decides, so a write to `v.a`
/// changes the value a compound of `v.b` or `v.t` returns, and a write to any
/// column of `v` reruns the query. A compound of INTEGER columns depends only
/// on the columns the statement names.
#[tokio::test]
async fn a_compound_of_values_that_compare_equal_and_differ_reruns_on_any_column() {
    for sql in [
        "SELECT x.b FROM v AS x UNION SELECT 5",
        "SELECT x.t FROM v AS x UNION SELECT 'zzz'",
        "SELECT x.t FROM v AS x INTERSECT SELECT 'abc'",
        "SELECT x.t FROM v AS x EXCEPT SELECT 'zzz'",
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
        "SELECT x.a FROM v AS x UNION SELECT x.id FROM v AS x",
        vec![],
        &[("UPDATE v SET x = 1 WHERE id = 1", false), ("DELETE FROM v WHERE id = 2", true)],
    )
    .await;
    db.close();
}

/// `row_number()` with no ORDER BY numbers the rows in the order SQLite
/// reads them: here through the covering index on `u.a`, which the statement
/// never names, so a write to `u.a` changes the numbers, and a write to any
/// column of `u` reruns the query. Ordered by the rowid, the numbers depend
/// only on the columns the statement names.
#[tokio::test]
async fn a_window_that_doesnt_tell_its_rows_apart_reruns_on_any_column() {
    let db = TestDatabase::open(PICKED);
    follows(
        &db,
        "SELECT x.id || ':' || row_number() OVER () FROM u AS x",
        vec![],
        &[("UPDATE u SET a = 0 WHERE id = 5", true), ("UPDATE u SET n = 1 WHERE id = 2", true)],
    )
    .await;
    db.close();
    let db = TestDatabase::open(PICKED);
    follows(
        &db,
        "SELECT x.id || ':' || row_number() OVER (ORDER BY x.id) FROM u AS x",
        vec![],
        &[("UPDATE u SET a = 0 WHERE id = 5", false), ("DELETE FROM u WHERE id = 0", true)],
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

/// SQLite never runs a CTE that no query of the statement reads, so its
/// LIMIT, its USING join and the views it reads add no column, nor does a
/// CTE that only such a CTE reads: a write to `u` reruns nothing. Read by
/// the statement, the same CTE depends on every column of `u`.
#[tokio::test]
async fn a_cte_no_query_reads_adds_no_column() {
    let unread = [
        "WITH k AS (SELECT q.id FROM u AS q LIMIT 1) SELECT x.id FROM w AS x WHERE x.id = 1",
        "WITH k AS (SELECT q.id FROM u AS q JOIN w AS r USING (c)) \
         SELECT x.id FROM w AS x WHERE x.id = 1",
        "WITH k AS (SELECT q.id FROM u AS q LIMIT 1), m AS (SELECT k.id FROM k) \
         SELECT x.id FROM w AS x WHERE x.id = 1",
        "WITH k AS (SELECT f.id FROM first_u AS f) SELECT x.id FROM w AS x WHERE x.id = 1",
    ];
    let schema = format!("{PICKED}; CREATE VIEW first_u AS SELECT q.id FROM u AS q LIMIT 1");
    for sql in unread {
        let db = TestDatabase::open(&schema);
        follows(
            &db,
            sql,
            vec![],
            &[
                ("UPDATE u SET a = 0, c = 0 WHERE id = 5", false),
                ("DELETE FROM w WHERE id = 1", true),
            ],
        )
        .await;
        db.close();
    }
    let db = TestDatabase::open(&schema);
    follows(
        &db,
        "WITH k AS (SELECT q.id FROM u AS q LIMIT 1) SELECT x.id FROM w AS x \
         WHERE x.id IN (SELECT k.id FROM k)",
        vec![],
        &[("UPDATE u SET a = 0 WHERE id = 5", true)],
    )
    .await;
    db.close();
}
