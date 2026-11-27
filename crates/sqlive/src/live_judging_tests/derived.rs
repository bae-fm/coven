//! A subquery in FROM, a CTE or one of the app's views is read as cores of
//! its own, and the conditions on its rows narrow its tables.

use super::lookup::follows;
use crate::tests::TestDatabase;

fn write(db: &TestDatabase, sql: &str) {
    db.commit(sql);
}

const MUSIC: &str = "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, album TEXT, year INTEGER);
CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT, position INTEGER, title TEXT);
CREATE VIEW release_tracks AS SELECT r.id AS release, r.album, t.id AS track, t.title FROM releases r JOIN tracks t ON t.release = r.id;
CREATE VIEW album_releases AS SELECT * FROM release_tracks;
CREATE VIEW names(key, name) AS SELECT release, title FROM tracks UNION ALL SELECT id, album FROM releases;
CREATE VIEW later AS WITH RECURSIVE c(id) AS (SELECT min(id) FROM releases UNION SELECT r.id FROM releases r JOIN c ON r.id > c.id) SELECT id FROM c;
INSERT INTO releases VALUES ('r1','x',2001),('r2','x',2002),('r3','y',2003);
INSERT INTO tracks VALUES (1,'r1',1,'one'),(2,'r1',2,'two'),(3,'r2',1,'three'),(4,'r3',1,'four')";

/// A condition on a column that passes a table's column on unchanged
/// narrows that table, through a CTE, a subquery or a view, and through a
/// view of a view: another album's tracks wake nothing.
#[tokio::test]
async fn a_condition_on_a_passed_column_narrows_the_table_it_comes_from() {
    let db = TestDatabase::open(MUSIC);
    for sql in [
        "WITH r AS (SELECT id, album FROM releases) SELECT t.title FROM tracks t JOIN r ON t.release = r.id WHERE r.album = 'x' ORDER BY t.id",
        "SELECT t.title FROM tracks t JOIN (SELECT id, album FROM releases) AS r ON t.release = r.id WHERE r.album = 'x' ORDER BY t.id",
        "SELECT title FROM release_tracks WHERE album = 'x' ORDER BY track",
        "SELECT title FROM album_releases WHERE album = 'x' ORDER BY track",
        "SELECT v.title FROM releases r JOIN release_tracks v ON v.release = r.id WHERE r.album = 'x' ORDER BY v.track",
    ] {
        follows(&db, sql, vec![], &[
            ("UPDATE tracks SET title = title || '.' WHERE id = 4", false),
            ("UPDATE releases SET album = 'z' WHERE id = 'r3'", false),
            ("UPDATE tracks SET title = title || '.' WHERE id = 3", true),
            ("INSERT INTO releases VALUES ('r4','x',2004)", true),
            ("INSERT INTO tracks VALUES (5,'r4',1,'five')", true),
        ]).await;
        write(&db, "DELETE FROM tracks WHERE id = 5; DELETE FROM releases WHERE id = 'r4'; UPDATE releases SET album = 'y' WHERE id = 'r3'; UPDATE tracks SET title = 'three' WHERE id = 3; UPDATE tracks SET title = 'four' WHERE id = 4");
    }
    db.close();
}

/// A name SQLite reads in single quotes, a CTE's or an output column's,
/// names what its bare spelling does: the condition on it narrows the same.
#[tokio::test]
async fn a_single_quoted_name_narrows_as_its_bare_spelling_does() {
    let db = TestDatabase::open(MUSIC);
    for sql in [
        "WITH 'r' AS (SELECT id, album AS 'al' FROM releases) SELECT t.title FROM tracks t JOIN r ON t.release = r.id WHERE r.al = 'x' ORDER BY t.id",
        "SELECT t.title FROM tracks t JOIN (SELECT id, album AS 'al' FROM releases) AS r ON t.release = r.id WHERE r.al = 'x' ORDER BY t.id",
    ] {
        follows(&db, sql, vec![], &[
            ("UPDATE tracks SET title = title || '.' WHERE id = 4", false),
            ("UPDATE releases SET album = 'z' WHERE id = 'r3'", false),
            ("UPDATE tracks SET title = title || '.' WHERE id = 3", true),
        ]).await;
        write(&db, "UPDATE releases SET album = 'y' WHERE id = 'r3'; UPDATE tracks SET title = 'three' WHERE id = 3; UPDATE tracks SET title = 'four' WHERE id = 4");
    }
    db.close();
}

/// A CTE that reads one release's row gives the rows joined to it their
/// values, as a candidate's state gives its matches theirs; each arm of a
/// compound view takes the condition on its own column.
#[tokio::test]
async fn a_ctes_rows_give_values_and_each_arm_takes_the_condition() {
    let db = TestDatabase::open(MUSIC);
    follows(
        &db,
        "WITH c AS MATERIALIZED (SELECT r.id, r.album FROM releases r WHERE r.id = ?1) SELECT coalesce(t.title, c.album) FROM c LEFT JOIN tracks t ON t.release = c.id ORDER BY t.id",
        vec![crate::SqlValue::Text(b"r2".to_vec())],
        &[
            ("UPDATE tracks SET title = '.' WHERE id = 1", false),
            ("UPDATE releases SET album = 'z' WHERE id = 'r1'", false),
            ("INSERT INTO tracks VALUES (6,'r2',2,'six')", true),
            ("UPDATE releases SET album = 'w' WHERE id = 'r2'", true),
        ],
    )
    .await;
    follows(
        &db,
        "SELECT name FROM names WHERE key = 'r3' ORDER BY name",
        vec![],
        &[
            ("UPDATE tracks SET title = 'uno' WHERE id = 1", false),
            ("UPDATE releases SET album = 'v' WHERE id = 'r2'", false),
            ("UPDATE tracks SET title = 'cuatro' WHERE id = 4", true),
            ("UPDATE releases SET album = 'u' WHERE id = 'r3'", true),
        ],
    )
    .await;
    db.close();
}

/// A subquery's rows that come of many of its sources', or from a LIMIT
/// over all of them, say nothing of each source row: a write to a row
/// outside the condition can still change them, and reruns the query.
#[tokio::test]
async fn grouped_or_limited_rows_keep_their_tables_whole() {
    let db = TestDatabase::open(MUSIC);
    for sql in [
        "SELECT n FROM (SELECT release, count(*) AS n, max(title) AS title FROM tracks GROUP BY release) WHERE title = 'two'",
        "SELECT n FROM (SELECT title, count(*) OVER (PARTITION BY release) AS n FROM tracks) WHERE title = 'two'",
        "SELECT title FROM (SELECT release, title FROM tracks ORDER BY id LIMIT 2) WHERE title = 'two'",
        "SELECT n FROM (SELECT max(title) AS title, count(*) AS n FROM tracks) WHERE title = 'two'",
    ] {
        follows(
            &db,
            sql,
            vec![],
            &[("INSERT INTO tracks VALUES (0,'r1',0,'a')", true)],
        )
        .await;
        write(&db, "DELETE FROM tracks WHERE id = 0");
    }
    db.close();
}

/// A condition on a column a grouped subquery's GROUP BY lists passes into
/// it: every row of a group holds the group's value there, so a cover of
/// another hash is in a group the join leaves out. A column computed from
/// the key passes nothing, and a window over the groups keeps the table
/// whole.
#[tokio::test]
async fn a_condition_on_a_group_key_passes_into_the_group() {
    let db = TestDatabase::open(SCANS);
    for sql in [
        "SELECT g.n FROM scan c LEFT JOIN (SELECT hash, count(*) AS n FROM covers GROUP BY hash) g ON g.hash = c.hash WHERE c.path = ?1",
        "WITH g AS (SELECT hash, count(*) AS n, max(url) AS url FROM covers GROUP BY hash HAVING count(*) > 0) SELECT g.n || g.url FROM scan c JOIN g ON g.hash = c.hash WHERE c.path = ?1",
    ] {
        follows(&db, sql, path("/m/a"), &[
            ("INSERT INTO covers VALUES (3,'h2','three')", false),
            ("UPDATE covers SET url = 'two2' WHERE id = 2", false),
            ("INSERT INTO covers VALUES (4,'h1','four')", true),
            ("UPDATE covers SET hash = 'h1' WHERE id = 2", true),
            ("UPDATE scan SET hash = 'h2' WHERE id = 1", true),
        ])
        .await;
        write(&db, "DELETE FROM covers WHERE id IN (3, 4); UPDATE covers SET url = 'two', hash = 'h2' WHERE id = 2; UPDATE scan SET hash = 'h1' WHERE id = 1");
    }
    for sql in [
        "SELECT g.n FROM (SELECT hash || '' AS hash, count(*) AS n FROM covers GROUP BY hash) g WHERE g.hash = 'h1'",
        "SELECT g.n FROM (SELECT hash, count(*) OVER () AS n FROM covers GROUP BY hash) g WHERE g.hash = 'h1'",
    ] {
        follows(&db, sql, vec![], &[("INSERT INTO covers VALUES (3,'h2','three')", true)]).await;
        write(&db, "DELETE FROM covers WHERE id = 3");
    }
    db.close();
}

/// A subquery inside a correlated one reads the names around it, so sqlive
/// reads none of its rows alone; its tables still take the values the
/// statement around it matched, and the table joined to it takes the values
/// its arm passes on from those tables.
#[tokio::test]
async fn a_correlated_subquery_in_from_narrows_without_being_read_alone() {
    let db = TestDatabase::open(MUSIC);
    follows(
        &db,
        "SELECT r.id FROM releases r WHERE r.album = 'x' AND EXISTS (SELECT 1 FROM tracks o JOIN (SELECT t.id FROM tracks t WHERE t.release = r.id AND t.position = 2) AS x ON o.id = x.id) ORDER BY r.id",
        vec![],
        &[
            ("UPDATE tracks SET position = 2 WHERE id = 4", false),
            ("UPDATE tracks SET position = 2 WHERE id = 3", true),
        ],
    )
    .await;
    db.close();
}

/// A view sqlive can't read as cores of its own, as one with a recursive
/// CTE, gives the tables beside it values and keeps its own tables whole.
#[tokio::test]
async fn a_view_with_a_recursive_cte_keeps_its_tables_whole() {
    let db = TestDatabase::open(MUSIC);
    follows(
        &db,
        "SELECT t.title FROM tracks t JOIN later l ON l.id = t.release WHERE t.id = 1",
        vec![],
        &[
            ("UPDATE tracks SET title = '.' WHERE id = 4", false),
            ("INSERT INTO releases VALUES ('r0','q',1999)", true),
            ("UPDATE tracks SET title = 'uno' WHERE id = 1", true),
        ],
    )
    .await;
    db.close();
}

const SCANS: &str = "CREATE TABLE scan(id INTEGER PRIMARY KEY, path TEXT, root TEXT, hash TEXT);
CREATE TABLE files(id INTEGER PRIMARY KEY, root TEXT, path TEXT, name TEXT);
CREATE TABLE covers(id INTEGER PRIMARY KEY, hash TEXT, url TEXT);
INSERT INTO scan VALUES (1,'/m/a','/m','h1'),(2,'/m/b','/m','h2');
INSERT INTO files VALUES (1,'/m','/m/a','a.flac'),(2,'/m','/m/b','b.flac');
INSERT INTO covers VALUES (1,'h1','one'),(2,'h2','two')";

fn path(path: &str) -> Vec<crate::SqlValue> {
    vec![crate::SqlValue::Text(path.as_bytes().to_vec())]
}

/// A CTE of the statement's own WITH gives the tables joined to it their
/// values wherever the statement reads it: in a scalar subquery, as a
/// subquery's value, and inside another CTE read only in a subquery.
#[tokio::test]
async fn a_cte_read_in_a_subquery_gives_values() {
    let db = TestDatabase::open(SCANS);
    for sql in [
        "WITH c AS (SELECT root, path FROM scan WHERE path = ?1) SELECT (SELECT count(*) || ':' || group_concat(f.name) FROM files f JOIN c ON f.root = c.root AND f.path = c.path)",
        "WITH c AS (SELECT root FROM scan WHERE path = ?1 LIMIT 1) SELECT group_concat(name) FROM files WHERE root = (SELECT root FROM c) AND path = ?1",
        "WITH c AS (SELECT root, path FROM scan WHERE path = ?1), f AS (SELECT files.name FROM files JOIN c ON files.root = c.root AND files.path = c.path) SELECT (SELECT group_concat(name) FROM f)",
    ] {
        follows(&db, sql, path("/m/a"), &[
            ("INSERT INTO files VALUES (3,'/m','/m/b','c.flac')", false),
            ("INSERT INTO files VALUES (4,'/n','/m/a','d.flac')", false),
            ("UPDATE files SET name = 'a2.flac' WHERE id = 1", true),
            ("UPDATE scan SET root = '/n' WHERE id = 1", true),
        ])
        .await;
        write(&db, "DELETE FROM files WHERE id IN (3, 4); UPDATE files SET name = 'a.flac' WHERE id = 1; UPDATE scan SET root = '/m' WHERE id = 1");
    }
    db.close();
}

/// A CTE declared inside a subquery, or a subquery in FROM there, gives the
/// tables joined to it their values when its SQL names no row around it.
#[tokio::test]
async fn a_with_inside_a_subquery_gives_values_when_it_names_nothing_around_it() {
    let db = TestDatabase::open(SCANS);
    for sql in [
        "SELECT (WITH c AS (SELECT hash FROM scan WHERE path = ?1) SELECT count(*) FROM covers cv JOIN c ON cv.hash = c.hash)",
        "SELECT (SELECT count(*) FROM covers cv JOIN (SELECT hash FROM scan WHERE path = ?1) AS c ON cv.hash = c.hash)",
    ] {
        follows(&db, sql, path("/m/a"), &[
            ("INSERT INTO covers VALUES (3,'h2','three')", false),
            ("INSERT INTO covers VALUES (4,'h1','four')", true),
            ("UPDATE scan SET hash = 'h2' WHERE id = 1", true),
        ])
        .await;
        write(&db, "DELETE FROM covers WHERE id IN (3, 4); UPDATE scan SET hash = 'h1' WHERE id = 1");
    }
    db.close();
}

/// A CTE that names the row around it lists rows only beside that row, so
/// a lookup can't ask it alone, and one whose column is the outer row's, not
/// a column of a table of its own, passes on no table's values: the tables
/// joined to it narrow by their own conditions only, and every lookup a
/// write runs still prepares.
#[tokio::test]
async fn a_correlated_cte_gives_no_values() {
    let db = TestDatabase::open(SCANS);
    follows(
        &db,
        "SELECT (WITH c AS (SELECT s.hash AS hash) SELECT count(*) FROM covers cv JOIN c ON cv.hash = c.hash) FROM scan s WHERE s.path = ?1",
        path("/m/a"),
        &[
            ("UPDATE scan SET root = '/n' WHERE id = 2", false),
            ("INSERT INTO covers VALUES (3,'h2','three')", true),
            ("INSERT INTO covers VALUES (4,'h1','four')", true),
        ],
    )
    .await;
    db.close();
}

/// A CTE declared inside a subquery under an outer CTE's name stands for
/// the inner one there, as SQLite resolves it, and an outer CTE that names
/// the shadowed name reads the outer one: each narrows its tables by the
/// CTE it reads, and a lookup that needs both prepares.
#[tokio::test]
async fn an_inner_cte_of_an_outer_cte_s_name_narrows_by_the_inner_one() {
    let db = TestDatabase::open(MUSIC);
    for (sql, outer_read) in [
        (
            "WITH placed AS (SELECT id FROM releases WHERE id = 'r1') SELECT title FROM (WITH placed AS (SELECT id FROM releases WHERE id = 'r2') SELECT t.title FROM tracks t JOIN placed ON t.release = placed.id) ORDER BY title",
            false,
        ),
        (
            "WITH placed AS (SELECT id FROM releases WHERE id = 'r1') SELECT (WITH placed AS (SELECT id FROM releases WHERE id = 'r2') SELECT group_concat(t.title) FROM tracks t JOIN placed ON t.release = placed.id) FROM placed",
            false,
        ),
        (
            "WITH placed AS (SELECT id FROM releases WHERE id = 'r1'), owned AS (SELECT id FROM placed) SELECT title FROM (WITH placed AS (SELECT id FROM releases WHERE id = 'r2'), held AS (SELECT id FROM owned UNION ALL SELECT id FROM placed) SELECT t.title FROM tracks t JOIN held ON t.release = held.id) ORDER BY title",
            true,
        ),
    ] {
        follows(&db, sql, vec![], &[
            ("UPDATE tracks SET title = title || '.' WHERE id = 4", false),
            ("UPDATE releases SET album = 'z' WHERE id = 'r3'", false),
            ("UPDATE tracks SET title = title || '.' WHERE id = 1", outer_read),
            ("UPDATE tracks SET title = title || '.' WHERE id = 3", true),
        ])
        .await;
        write(&db, "UPDATE releases SET album = 'y' WHERE id = 'r3'; UPDATE tracks SET title = 'one' WHERE id = 1; UPDATE tracks SET title = 'three' WHERE id = 3; UPDATE tracks SET title = 'four' WHERE id = 4");
    }
    db.close();
}

/// A view read whole, by the app's choice or as one sqlive can't read as
/// cores, keeps every table it reads whole, even when its SQL reads them
/// through a CTE or a view whose name the statement's own CTE or view
/// shares, which narrows the same table: a write to a row only the view
/// reads reruns the query.
#[tokio::test]
async fn a_view_read_whole_keeps_its_tables_whole_beside_a_name_it_uses_inside() {
    let schema = format!(
        "{MUSIC};
CREATE VIEW albums AS WITH c AS (SELECT id, album FROM releases) SELECT id, album FROM c;
CREATE VIEW first_release AS WITH RECURSIVE c(id) AS (SELECT min(id) FROM releases UNION SELECT r.id FROM releases r JOIN c ON r.id < c.id) SELECT id FROM c;
CREATE VIEW all_tracks AS SELECT title FROM release_tracks"
    );
    let db = TestDatabase::open_with(
        &schema,
        crate::tests::TestOptions {
            whole_views: vec!["albums", "all_tracks"],
            ..Default::default()
        },
    );
    let releases: &[(&str, bool)] = &[
        ("UPDATE releases SET album = 'z' WHERE id = 'r3'", true),
        ("INSERT INTO releases VALUES ('r0','q',1999)", true),
    ];
    let tracks: &[(&str, bool)] = &[("UPDATE tracks SET title = 'cuatro' WHERE id = 4", true)];
    for (sql, writes) in [
        ("WITH c AS (SELECT id, album FROM releases WHERE id = 'r1') SELECT c.album || ':' || group_concat(a.album, ',') FROM c, albums a", releases),
        ("WITH c AS (SELECT id, album FROM releases WHERE id = 'r1') SELECT c.album || ':' || group_concat(f.id, ',') FROM c, first_release f", releases),
        ("SELECT v.title || ':' || (SELECT group_concat(title, ',') FROM all_tracks) FROM release_tracks v WHERE v.track = 1", tracks),
    ] {
        follows(&db, sql, vec![], writes).await;
        write(&db, "DELETE FROM releases WHERE id = 'r0'; UPDATE releases SET album = 'y' WHERE id = 'r3'; UPDATE tracks SET title = 'four' WHERE id = 4");
    }
    db.close();
}

/// A CTE may have the name of a view the app marks as read whole: the
/// statement reads the CTE under that name, not the view, so the CTE
/// narrows its tables, and another view the statement reads whole, whose
/// SQL uses no name the statement's CTEs have, keeps its own tables whole.
#[tokio::test]
async fn a_cte_of_a_whole_views_name_narrows_its_tables_beside_a_view_read_whole() {
    let schema = format!(
        "{MUSIC};
CREATE VIEW albums AS SELECT id, album FROM releases;
CREATE VIEW release_years AS SELECT id, year FROM releases"
    );
    let db = TestDatabase::open_with(
        &schema,
        crate::tests::TestOptions {
            whole_views: vec!["albums", "release_years"],
            ..Default::default()
        },
    );
    follows(
        &db,
        "WITH albums AS (SELECT id, title FROM tracks WHERE id = 1) SELECT a.title || ':' || (SELECT group_concat(year, ',') FROM release_years) FROM albums a",
        vec![],
        &[
            ("UPDATE tracks SET title = '.' WHERE id = 4", false),
            ("UPDATE releases SET year = 2000 WHERE id = 'r3'", true),
            ("UPDATE tracks SET title = 'uno' WHERE id = 1", true),
        ],
    )
    .await;
    db.close();
}

/// Each condition sqlive reads as a term on a subquery's column passes into
/// the subquery's table: a comparison by each operator, an IN list, an OR of
/// terms, an AND inside an OR that keeps the side sqlive reads, a NULL test
/// of each spelling, and the truth of a comparison compared with a value,
/// either way round. SQLite can't judge a subquery's rows one by one, so
/// without the term a write to a row outside the condition would rerun the
/// query.
#[tokio::test]
async fn each_term_on_a_subquerys_column_narrows_its_table() {
    let db = TestDatabase::open(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER, v INTEGER);
INSERT INTO t VALUES (1,1,0),(2,2,0),(3,3,0),(4,4,0),(5,5,0),(6,NULL,0)",
    );
    // The statement, a row its condition leaves out and a row it keeps.
    let cases: [(&'static str, i64, i64); 13] = [
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE s.x < 3 ORDER BY s.id", 4, 1),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE s.x <= 2 ORDER BY s.id", 3, 2),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE s.x > 3 ORDER BY s.id", 2, 4),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE s.x >= 4 ORDER BY s.id", 3, 4),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE s.x IN (1, 3) ORDER BY s.id", 2, 3),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE s.x = 1 OR s.x = 3 ORDER BY s.id", 2, 1),
        (
            "SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE (s.x = 1 AND abs(s.v) >= 0) OR s.x = 3 ORDER BY s.id",
            2,
            3,
        ),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE s.x ISNULL ORDER BY s.id", 2, 6),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE s.x NOTNULL ORDER BY s.id", 6, 2),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE (s.x = 1) = 1 ORDER BY s.id", 2, 1),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE 1 = (s.x = 1) ORDER BY s.id", 2, 1),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE (s.x <= 2) = 1 ORDER BY s.id", 3, 1),
        ("SELECT s.v FROM (SELECT id, x, v FROM t) AS s WHERE (s.x > 3) = 1 ORDER BY s.id", 2, 5),
    ];
    for (sql, outside, inside) in cases {
        follows(
            &db,
            sql,
            vec![],
            &[
                (&format!("UPDATE t SET v = v + 1 WHERE id = {outside}"), false),
                (&format!("UPDATE t SET v = v + 1 WHERE id = {inside}"), true),
            ],
        )
        .await;
    }
    db.close();
}

/// The terms on a subquery's columns pass each to the table its column comes
/// from: the truth of a comparison on one table's column, after a comparison
/// on another table's, narrows its own table.
#[tokio::test]
async fn each_term_on_a_subquerys_columns_narrows_the_table_its_column_comes_from() {
    let db = TestDatabase::open(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER, v INTEGER);
CREATE TABLE u(id INTEGER PRIMARY KEY, t INTEGER, y INTEGER);
INSERT INTO t VALUES (1,1,0),(2,2,0);
INSERT INTO u VALUES (1,1,1),(2,2,1)",
    );
    follows(
        &db,
        "SELECT s.v FROM (SELECT t.id, t.x, u.y, t.v FROM t JOIN u ON u.t = t.id) AS s WHERE s.y = 1 AND (s.x = 1) = 1 ORDER BY s.id",
        vec![],
        &[
            ("UPDATE t SET v = v + 1 WHERE id = 2", false),
            ("UPDATE t SET v = v + 1 WHERE id = 1", true),
        ],
    )
    .await;
    db.close();
}

/// A condition SQLite judges on a subquery's columns passes into the table
/// the columns come from, as that condition on the table's own columns would,
/// though the subquery names them its own way: a write to a row it leaves out
/// wakes nothing. A lookup of the table reads it too, so a row joined to such
/// a row alone wakes nothing.
#[tokio::test]
async fn a_judged_condition_on_a_subquerys_columns_narrows_its_table() {
    let db = TestDatabase::open(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, v INTEGER);
CREATE TABLE u(id INTEGER PRIMARY KEY, t INTEGER, w INTEGER);
INSERT INTO t VALUES (1,1,1,0),(2,2,3,0);
INSERT INTO u VALUES (1,1,0),(2,2,0)",
    );
    for sql in [
        "SELECT s.p FROM (SELECT id, x AS p, y AS q, v FROM t) AS s WHERE s.p = s.q ORDER BY s.id",
        // The subquery's alias is the table's name inside it.
        "SELECT t.p FROM (SELECT id, x AS p, y AS q, v FROM t) AS t WHERE max(t.p, t.q) < 2 ORDER BY t.id",
        "SELECT r.p FROM (SELECT * FROM (SELECT id, x AS p, y AS q FROM t) AS i) AS r WHERE r.p = r.q ORDER BY r.id",
    ] {
        follows(&db, sql, vec![], &[
            ("INSERT INTO t VALUES (3,4,5,0)", false),
            ("DELETE FROM t WHERE id = 3", false),
            ("INSERT INTO t VALUES (3,1,1,0)", true),
            ("DELETE FROM t WHERE id = 3", true),
        ]).await;
    }
    follows(
        &db,
        "SELECT u.w FROM (SELECT id, x AS p, y AS q FROM t) AS s JOIN u ON u.t = s.id WHERE s.p = s.q ORDER BY u.id",
        vec![],
        &[
            ("UPDATE u SET w = w + 1 WHERE id = 2", false),
            ("UPDATE u SET w = w + 1 WHERE id = 1", true),
        ],
    )
    .await;
    db.close();
}

/// A value SQLite computes from a subquery's columns narrows the table the
/// columns come from in a join, as the same value of the table's own columns
/// would: a write to a row whose value no joined row holds wakes nothing. A
/// value that is other than NULL where the columns are passes to no table a
/// LEFT JOIN adds, where it could match with no row.
#[tokio::test]
async fn a_computed_value_of_a_subquerys_columns_narrows_its_table() {
    let db = TestDatabase::open(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER);
CREATE TABLE u(id INTEGER PRIMARY KEY, t INTEGER);
INSERT INTO t VALUES (1,1),(2,2);
INSERT INTO u VALUES (1,2),(2,3)",
    );
    follows(
        &db,
        "SELECT s.id FROM (SELECT id, x AS p FROM t) AS s, u WHERE u.id < 10 AND u.t = s.p + 1 ORDER BY s.id",
        vec![],
        &[
            ("INSERT INTO t VALUES (3,9)", false),
            ("DELETE FROM t WHERE id = 3", false),
            ("INSERT INTO t VALUES (3,1)", true),
            ("DELETE FROM t WHERE id = 3", true),
        ],
    )
    .await;
    // A row of `u` that no row of `t` joins matches `z`'s 0 inside the
    // statement: the value says nothing of `t`'s rows there, so `t` stays a
    // LEFT JOIN's, and `u` keeps every row.
    db.commit("INSERT INTO u VALUES (3,0)");
    follows(
        &db,
        "SELECT s.k FROM (SELECT u.id AS k, t.x AS p FROM u LEFT JOIN t ON t.id = u.id) AS s, u AS z WHERE z.id = 3 AND z.t = CASE WHEN s.p IS NULL THEN 0 ELSE s.p END ORDER BY s.k",
        vec![],
        &[("INSERT INTO u VALUES (4,9)", true), ("DELETE FROM u WHERE id = 4", true)],
    )
    .await;
    db.close();
}

/// A join looks a subquery's table up in the order it would the table
/// written out: a subquery with no condition of its own, a LEFT JOIN's right
/// side here, comes after the table it takes its values from, and one with a
/// condition inside comes first. So a write to a row no joined row holds wakes
/// nothing.
#[tokio::test]
async fn a_join_looks_a_subquerys_table_up_as_the_table_written_out() {
    let db = TestDatabase::open(
        "CREATE TABLE s(id INTEGER PRIMARY KEY);
CREATE TABLE t(id INTEGER PRIMARY KEY, c INTEGER);
INSERT INTO s VALUES (1),(2);
INSERT INTO t VALUES (1,1),(2,2)",
    );
    follows(
        &db,
        "SELECT x.id, y.id FROM s AS x LEFT JOIN (SELECT * FROM t) AS y ON y.c = x.id ORDER BY 1, 2",
        vec![],
        &[
            ("INSERT INTO t VALUES (3,9)", false),
            ("DELETE FROM t WHERE id = 3", false),
            ("INSERT INTO t VALUES (3,1)", true),
            ("DELETE FROM t WHERE id = 3", true),
        ],
    )
    .await;
    follows(
        &db,
        "SELECT x.id FROM s AS x JOIN (SELECT * FROM t WHERE c < 10) AS y ON y.c = x.id ORDER BY 1",
        vec![],
        &[("INSERT INTO s VALUES (7)", false), ("DELETE FROM s WHERE id = 7", false)],
    )
    .await;
    db.close();
}

/// A deck's list names its cards, each found row a card, and each label a
/// found row. Two decks, `a` and `b`, each with 60 cards, each of which one
/// row found.
const DECKS: &str = "CREATE TABLE lists(list INTEGER PRIMARY KEY, owner TEXT NOT NULL);
CREATE TABLE cards(list INTEGER NOT NULL, card INTEGER NOT NULL, PRIMARY KEY (list, card));
CREATE TABLE found(id INTEGER PRIMARY KEY, card INTEGER NOT NULL);
INSERT INTO lists VALUES (1, 'a'), (2, 'b');
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 60)
INSERT INTO cards SELECT l.list, l.list * 1000 + n.i FROM lists AS l, n;
CREATE TABLE labels(id INTEGER PRIMARY KEY, found INTEGER NOT NULL);
INSERT INTO found SELECT card, card FROM cards";

/// A subquery gives its found rows' card under eight names, each joined to
/// the same column of a partner. Each join passes into `found`, and gives it
/// one condition. A question about a changed label reads the found rows in
/// their scope, where eight copies of the condition would take it past its
/// steps, and then a label of the other deck's found row would run the query
/// again.
#[tokio::test]
async fn a_column_passed_on_under_several_names_takes_each_condition_once() {
    let db = TestDatabase::open(DECKS);
    let counter = crate::ConnectionWorkCounter::new();
    db.track_work(&counter);
    follows(
        &db,
        "SELECT group_concat(shown.id) FROM lists AS deck \
         JOIN cards AS held ON held.list = deck.list \
         JOIN (SELECT label.id, found.card AS c1, found.card AS c2, found.card AS c3, \
                      found.card AS c4, found.card AS c5, found.card AS c6, \
                      found.card AS c7, found.card AS c8 \
               FROM found JOIN labels AS label ON label.found = found.id) AS shown \
           ON shown.c1 = held.card AND shown.c2 = held.card AND shown.c3 = held.card \
              AND shown.c4 = held.card AND shown.c5 = held.card AND shown.c6 = held.card \
              AND shown.c7 = held.card AND shown.c8 = held.card \
         WHERE deck.owner = ?1",
        vec![crate::SqlValue::Text("a".into())],
        &[
            ("INSERT INTO labels VALUES (1, 2001)", false),
            ("INSERT INTO labels VALUES (2, 1001)", true),
        ],
    )
    .await;
    assert_eq!(
        counter.take().stopped_questions,
        0,
        "no question about a changed row ran past its steps"
    );
    db.close();
}

/// An ordered window that joins a one-table subquery looks the window's table
/// up first, as it would the subquery's table written out, so the subquery's
/// table narrows to the rows that join the window.
#[tokio::test]
async fn an_ordered_window_joining_a_subquery_narrows_its_table() {
    let db = TestDatabase::open(
        "CREATE TABLE listing(release TEXT PRIMARY KEY, title TEXT NOT NULL);
CREATE TABLE storage(release TEXT PRIMARY KEY, stored INTEGER NOT NULL);
CREATE INDEX by_title ON listing(title, release);
INSERT INTO listing VALUES ('b','B'),('c','C'),('d','D');
INSERT INTO storage VALUES ('b',1),('c',1),('d',1)",
    );
    follows(
        &db,
        "SELECT l.release, s.stored FROM listing AS l CROSS JOIN (SELECT release, stored FROM storage) AS s ON s.release = l.release ORDER BY l.title, l.release LIMIT 2",
        vec![],
        &[
            ("INSERT INTO storage VALUES ('e',1)", false),
            ("UPDATE storage SET stored = 0 WHERE release = 'b'", true),
        ],
    )
    .await;
    db.close();
}

/// A column of a subquery that nothing reads changes nothing the statement
/// returns, so a table that only its subquery reads is no table the statement
/// depends on, through each wrapper that passes the column on unread. A
/// statement that reads the column depends on it.
#[tokio::test]
async fn a_subquerys_unread_column_adds_no_table_the_statement_depends_on() {
    let db = TestDatabase::open(
        "CREATE TABLE a(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE covers(id INTEGER PRIMARY KEY, album INTEGER);
INSERT INTO a VALUES (1,0),(2,0);
CREATE VIEW covered AS SELECT a.id, (SELECT c.id FROM covers AS c WHERE c.album = a.id) AS cover FROM a",
    );
    let covered =
        "SELECT a.id, a.v, (SELECT c.id FROM covers AS c WHERE c.album = a.id) AS cover FROM a";
    for (sql, reads_cover) in [
        ("SELECT id FROM covered WHERE id = 1".to_owned(), false),
        ("SELECT cover FROM covered WHERE id = 1".to_owned(), true),
        (format!("SELECT s.id FROM ({covered}) AS s WHERE s.id = 1"), false),
        (format!("SELECT s.id FROM (SELECT w.* FROM ({covered}) AS w) AS s WHERE s.id = 1"), false),
        (format!("WITH s AS ({covered}) SELECT s.v FROM s ORDER BY s.id LIMIT 1"), false),
        (format!("SELECT s.id FROM ({covered} UNION ALL {covered}) AS s WHERE s.id = 1"), false),
        (format!("SELECT s.id, s.cover FROM (SELECT w.* FROM ({covered}) AS w) AS s WHERE s.id = 1"), true),
        (format!("SELECT s.id FROM (SELECT * FROM ({covered}) AS w WHERE w.cover IS NULL) AS s WHERE s.id = 1"), true),
        (format!("SELECT s.id FROM ({covered} ORDER BY cover) AS s WHERE s.id = 1"), true),
        (format!("SELECT count(*) FROM (SELECT DISTINCT * FROM ({covered})) AS s"), true),
    ] {
        follows(
            &db,
            sql.leak(),
            vec![],
            &[("INSERT INTO covers VALUES (9,1)", reads_cover), ("DELETE FROM covers WHERE id = 9", reads_cover)],
        )
        .await;
    }
    db.close();
}

/// A view read whole in a column nothing reads adds no table the statement
/// depends on, nor does one sqlive can't read as cores of its own. A table
/// the view reads stays whole when the statement also reads it through a
/// view read whole or in a column it reads, and keeps the scope of the
/// statement's other reads of it otherwise.
#[tokio::test]
async fn a_view_read_whole_in_an_unread_column_adds_no_table() {
    let db = TestDatabase::open_with(
        "CREATE TABLE a(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE covers(id INTEGER PRIMARY KEY, album INTEGER);
INSERT INTO a VALUES (1,0),(2,0);
INSERT INTO covers VALUES (5,2);
CREATE VIEW sending AS SELECT id, album FROM covers;
CREATE VIEW phases AS SELECT album FROM sending;
CREATE VIEW first_cover AS WITH RECURSIVE c(id) AS (SELECT min(id) FROM covers UNION SELECT r.id FROM covers r JOIN c ON r.id < c.id) SELECT id FROM c",
        crate::tests::TestOptions { whole_views: vec!["sending", "phases"], ..Default::default() },
    );
    let wrapped =
        |view: &str| format!("(SELECT a.id, (SELECT count(*) FROM {view}) AS n FROM a) AS s");
    for (sql, reads_cover) in [
        (format!("SELECT s.id FROM {} WHERE s.id = 1", wrapped("sending")), false),
        (format!("SELECT s.id FROM {} WHERE s.id = 1", wrapped("phases")), false),
        (format!("SELECT s.id FROM {} WHERE s.id = 1", wrapped("first_cover")), false),
        (
            format!(
                "SELECT s.id || (SELECT count(*) FROM covers WHERE covers.id = 5) FROM {} WHERE s.id = 1",
                wrapped("sending")
            ),
            false,
        ),
        (format!("SELECT s.n FROM {} WHERE s.id = 1", wrapped("sending")), true),
        (format!("SELECT s.n FROM {} WHERE s.id = 1", wrapped("first_cover")), true),
        (
            format!(
                "SELECT s.id || (SELECT count(*) FROM phases) FROM {} WHERE s.id = 1",
                wrapped("sending")
            ),
            true,
        ),
        (
            format!(
                "SELECT s.id || (SELECT count(*) FROM sending) FROM {} WHERE s.id = 1",
                wrapped("phases")
            ),
            true,
        ),
    ] {
        follows(
            &db,
            sql.leak(),
            vec![],
            &[
                ("INSERT INTO covers VALUES (9,1)", reads_cover),
                ("DELETE FROM covers WHERE id = 9", reads_cover),
            ],
        )
        .await;
    }
    db.close();
}

/// An OR of equalities with another table's columns, as the one sqlive reads
/// from a coalesce equality, narrows the table behind a FROM subquery, CTE or
/// view as it narrows that table written out.
#[tokio::test]
async fn an_or_narrows_the_table_behind_a_subquery() {
    let db = TestDatabase::open(
        "CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);
CREATE TABLE t(id INTEGER PRIMARY KEY, c INTEGER, v INTEGER);
INSERT INTO u VALUES (1,2,10),(2,3,11);
INSERT INTO t VALUES (1,10,0),(2,20,0),(3,30,0),(4,40,0),(11,50,0);
CREATE VIEW passed AS SELECT * FROM t",
    );
    for from in [
        "t AS y",
        "(SELECT * FROM t) AS y",
        "(SELECT * FROM (SELECT * FROM t) AS inner_rows) AS y",
        "(SELECT * FROM t UNION ALL SELECT * FROM t) AS y",
        "passed AS y",
    ] {
        for on in ["y.id = coalesce(x.a, 1)", "(y.id = x.a OR y.id = x.b)"] {
            for join in ["JOIN", "LEFT JOIN"] {
                let sql = format!("SELECT x.id, y.v FROM u AS x {join} {from} ON {on}");
                follows(
                    &db,
                    sql.leak(),
                    vec![],
                    &[
                        ("UPDATE t SET v = v + 1 WHERE id = 4", false),
                        ("UPDATE t SET v = v + 1 WHERE id = 2", true),
                        ("UPDATE t SET v = v + 1 WHERE id = 3", true),
                    ],
                )
                .await;
            }
        }
        let sql = format!("WITH y AS (SELECT * FROM {from}) SELECT x.id, y.v FROM u AS x, y WHERE y.id = x.a OR y.id = x.b");
        follows(
            &db,
            sql.leak(),
            vec![],
            &[
                ("UPDATE t SET v = v + 1 WHERE id = 4", false),
                ("UPDATE t SET v = v + 1 WHERE id = 11", true),
            ],
        )
        .await;
    }
    db.close();
}

/// A term an equality carries onto a FROM subquery, CTE or view narrows the
/// table its column comes from, as it narrows that table written out.
#[tokio::test]
async fn a_carried_term_narrows_the_table_behind_a_subquery() {
    let db = TestDatabase::open(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, v INTEGER);
CREATE TABLE u(id INTEGER PRIMARY KEY, c INTEGER);
INSERT INTO t VALUES (1,1,0),(2,2,0);
INSERT INTO u VALUES (1,1),(2,2);
CREATE VIEW passed AS SELECT * FROM t",
    );
    for from in [
        "t AS x",
        "(SELECT * FROM t) AS x",
        "(SELECT * FROM (SELECT * FROM t) AS inner_rows) AS x",
        "(SELECT * FROM t UNION ALL SELECT * FROM t) AS x",
        "passed AS x",
    ] {
        let sql = format!(
            "SELECT x.v FROM {from} JOIN (SELECT * FROM u) AS y ON y.c = x.a WHERE (x.id < 0 OR x.id >= 1) AND y.c = 1"
        );
        follows(
            &db,
            sql.leak(),
            vec![],
            &[
                ("UPDATE t SET v = v + 1 WHERE id = 2", false),
                ("UPDATE t SET v = v + 1 WHERE id = 1", true),
                ("INSERT INTO t VALUES (3,2,0)", false),
                ("INSERT INTO t VALUES (4,1,0)", true),
                ("DELETE FROM t WHERE id IN (3,4)", true),
            ],
        )
        .await;
    }
    db.close();
}

/// A statement that names a table in several places depends, through each, on
/// the columns it reads there of the rows its conditions there allow: a write
/// reruns it only when one place reads a column the write changes in a row
/// that place allows.
#[tokio::test]
async fn each_reference_to_a_table_narrows_on_its_own() {
    let db = TestDatabase::open(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER, a INTEGER, b INTEGER);
CREATE TABLE s(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1,1,0,0),(2,1,0,0),(3,2,0,0);
INSERT INTO s VALUES (1);
CREATE VIEW passed AS SELECT * FROM t",
    );
    for narrow in [
        "SELECT max(x.b) FROM t AS x WHERE x.g = 1",
        "SELECT max(x.b) FROM (SELECT * FROM t) AS x WHERE x.g = 1",
        "SELECT max(x.b) FROM passed AS x WHERE x.g = 1",
    ] {
        let sql = format!("SELECT ({narrow}), (SELECT max(y.a) FROM t AS y) FROM s");
        follows(
            &db,
            sql.leak(),
            vec![],
            &[
                // Only the narrow place reads b, of the rows of group 1.
                ("UPDATE t SET b = b + 1 WHERE id = 3", false),
                ("UPDATE t SET b = b + 1 WHERE id = 1", true),
                // The other place reads a of every row.
                ("UPDATE t SET a = a + 1 WHERE id = 3", true),
                ("INSERT INTO t VALUES (4,2,0,0)", true),
                ("DELETE FROM t WHERE id = 4", true),
            ],
        )
        .await;
    }
    // A place that reads no column depends only on which rows there are.
    follows(
        &db,
        "SELECT (SELECT max(x.b) FROM t AS x WHERE x.g = 1), EXISTS (SELECT 1 FROM t) FROM s",
        vec![],
        &[
            ("UPDATE t SET a = a + 1 WHERE id = 1", false),
            ("UPDATE t SET b = b + 1 WHERE id = 3", false),
            ("UPDATE t SET b = b + 1 WHERE id = 1", true),
            ("INSERT INTO t VALUES (5,2,0,0)", true),
            ("DELETE FROM t WHERE id = 5", true),
        ],
    )
    .await;
    // Two narrow places, each reading its own column of its own rows.
    let sql = "SELECT (SELECT max(x.a) FROM t AS x WHERE x.g = 1),
        (SELECT max(y.b) FROM t AS y WHERE y.g = 2) FROM s";
    follows(
        &db,
        sql,
        vec![],
        &[
            ("UPDATE t SET a = a + 1 WHERE id = 3", false),
            ("UPDATE t SET b = b + 1 WHERE id = 1", false),
            ("UPDATE t SET a = a + 1 WHERE id = 1", true),
            ("UPDATE t SET b = b + 1 WHERE id = 3", true),
        ],
    )
    .await;
    db.close();
}
