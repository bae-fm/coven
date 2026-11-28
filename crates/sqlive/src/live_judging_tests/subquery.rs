//! A column compared with a subquery's value narrows by the subquery's source:
//! the rows whose value that source holds in its scope (SPEC.md section 8).

use super::lookup::follows;
use crate::tests::{next, no_values, rerun, TestDatabase};

fn write(db: &TestDatabase, sql: &str) {
    db.commit(sql);
}

const ALBUMS: &str = "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, album_id TEXT, album_order INTEGER);
CREATE TABLE covers(id TEXT NOT NULL PRIMARY KEY, file TEXT);
CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY, name TEXT);
INSERT INTO albums VALUES ('a1','One'),('a2','Two');
INSERT INTO releases VALUES ('r1','a1',1),('r2','a1',2),('r3','a2',1);
INSERT INTO covers VALUES ('r1','f1'),('r2','f2'),('r3','f3');
INSERT INTO files VALUES ('f1','n1'),('f2','n2'),('f3','n3')";

/// An album's cover, picked as the album grid picks it: the cover of the
/// album's first release, with its file's name read in a subquery beside the
/// pick.
const COVER: &str = "SELECT (SELECT (SELECT f.name FROM files AS f WHERE f.id = cover.file)
  FROM covers AS cover
  WHERE cover.id = (SELECT r.id FROM releases AS r WHERE r.album_id = a.id
                    ORDER BY r.album_order, r.id LIMIT 1)) AS cover, a.title
FROM albums AS a WHERE a.id = 'a1'";

/// `covers` narrows to the covers of the album's releases, and the files
/// beside them to those covers' files: a cover or file written for another
/// album's release wakes nothing. One for any release of this album does,
/// whether or not it is the first. Moving a release out of the album when
/// it is not the album's first wakes nothing, since the first release's
/// cover is unchanged; moving it back as the first release does.
#[tokio::test]
async fn a_cover_pick_narrows_covers_and_files_to_the_albums_releases() {
    let db = TestDatabase::open(ALBUMS);
    follows(
        &db,
        COVER,
        vec![],
        &[
            ("UPDATE covers SET file = 'f9' WHERE id = 'r3'", false),
            ("UPDATE files SET name = 'n3b' WHERE id = 'f3'", false),
            ("UPDATE releases SET album_order = 5 WHERE id = 'r3'", false),
            ("UPDATE files SET name = 'n1b' WHERE id = 'f1'", true),
            ("UPDATE covers SET file = 'f2b' WHERE id = 'r2'", true),
            ("UPDATE releases SET album_id = 'a2' WHERE id = 'r2'", false),
            ("UPDATE covers SET file = 'f9' WHERE id = 'r2'", false),
            ("UPDATE releases SET album_id = 'a1', album_order = 0 WHERE id = 'r2'", true),
            ("UPDATE covers SET file = 'f1c' WHERE id = 'r1'", true),
        ],
    )
    .await;
    db.close();
}

/// A cover and its file's version read through subqueries in FROM, as a
/// statement spliced from other statements reads them, and keyed by the same
/// pick: they narrow as the tables read directly do.
#[tokio::test]
async fn a_pick_keys_the_tables_a_subquery_in_from_reads() {
    let db = TestDatabase::open(ALBUMS);
    follows(
        &db,
        "SELECT (SELECT shown.name
  FROM (SELECT cover.id,
               (SELECT stored.name
                FROM (SELECT f.id AS file, f.name FROM files AS f) AS stored
                WHERE stored.file = cover.file) AS name
        FROM covers AS cover) AS shown
  WHERE shown.id = (SELECT r.id FROM releases AS r WHERE r.album_id = a.id
                    ORDER BY r.album_order, r.id LIMIT 1)) AS cover, a.title
FROM albums AS a WHERE a.id = 'a1'",
        vec![],
        &[
            ("UPDATE covers SET file = 'f9' WHERE id = 'r3'", false),
            ("INSERT INTO covers VALUES ('r9','f9')", false),
            ("UPDATE files SET name = 'n3b' WHERE id = 'f3'", false),
            ("UPDATE files SET name = 'n1b' WHERE id = 'f1'", true),
            ("UPDATE covers SET file = 'f1' WHERE id = 'r2'", true),
            ("UPDATE covers SET file = 'f2' WHERE id = 'r1'", true),
        ],
    )
    .await;
    db.close();
}

/// A column compared with a subquery's value that passes through another
/// lookup table: the credits shape, where an artist is shown as another.
#[tokio::test]
async fn credits_narrow_artists_to_the_ones_they_show() {
    let db = TestDatabase::open(
        "CREATE TABLE credits(id INTEGER PRIMARY KEY, track TEXT, artist TEXT);
CREATE TABLE shown_as(artist TEXT NOT NULL PRIMARY KEY, shown TEXT);
CREATE TABLE artists(id TEXT NOT NULL PRIMARY KEY, name TEXT);
INSERT INTO credits VALUES (1,'t1','ar1'),(2,'t2','ar2');
INSERT INTO shown_as VALUES ('ar1','s1'),('ar2','s2');
INSERT INTO artists VALUES ('s1','S1'),('s2','S2')",
    );
    follows(
        &db,
        "SELECT artists.name FROM credits AS c
         JOIN artists ON artists.id = (SELECT shown FROM shown_as WHERE shown_as.artist = c.artist)
         WHERE c.track = 't1' ORDER BY c.id",
        vec![],
        &[
            ("UPDATE artists SET name = 'S2b' WHERE id = 's2'", false),
            ("UPDATE shown_as SET shown = 's3' WHERE artist = 'ar2'", false),
            ("INSERT INTO artists VALUES ('s3','S3')", false),
            ("UPDATE artists SET name = 'S1b' WHERE id = 's1'", true),
            ("UPDATE shown_as SET shown = 's3' WHERE artist = 'ar1'", true),
            ("UPDATE artists SET name = 'S3b' WHERE id = 's3'", true),
            ("UPDATE artists SET name = 'S1c' WHERE id = 's1'", false),
        ],
    )
    .await;
    db.close();
}

/// `col IN (SELECT x.c …)` narrows as `col = (SELECT x.c …)` does.
#[tokio::test]
async fn a_column_in_a_subquerys_column_narrows() {
    let db = TestDatabase::open(
        "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, album_id TEXT);
CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT, title TEXT);
CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER);
INSERT INTO releases VALUES ('r1','a1'),('r2','a1'),('r3','a2');
INSERT INTO tracks VALUES (1,'r1','one'),(2,'r2','two'),(3,'r3','three');
INSERT INTO items VALUES (1,0),(9,0)",
    );
    follows(
        &db,
        "SELECT t.title FROM tracks AS t
         WHERE t.release IN (SELECT r.id FROM releases AS r WHERE r.album_id = 'a1') ORDER BY t.id",
        vec![],
        &[
            ("UPDATE tracks SET title = 'three b' WHERE id = 3", false),
            ("UPDATE releases SET album_id = 'a3' WHERE id = 'r3'", false),
            ("UPDATE tracks SET title = 'one b' WHERE id = 1", true),
            ("UPDATE tracks SET release = 'r1' WHERE id = 3", true),
            ("UPDATE releases SET album_id = 'a1' WHERE id = 'r3'", true),
            ("UPDATE tracks SET release = 'r9' WHERE id = 3", true),
            ("UPDATE tracks SET title = 'two b' WHERE id = 2", true),
        ],
    )
    .await;
    follows(
        &db,
        "SELECT value FROM items WHERE id IN (SELECT id FROM items WHERE id = 1)",
        vec![],
        &[
            ("UPDATE items SET value = value + 1 WHERE id = 9", false),
            ("UPDATE items SET value = value + 1 WHERE id = 1", true),
        ],
    )
    .await;
    db.close();
}

const PAIRS: &str = "CREATE TABLE x(id INTEGER PRIMARY KEY, k INTEGER, g INTEGER);
CREATE TABLE y(id INTEGER PRIMARY KEY, k INTEGER, g INTEGER);
INSERT INTO x VALUES (1,1,1),(2,2,2),(3,3,1);
INSERT INTO y VALUES (1,1,1),(2,2,2),(3,3,1)";

/// A subquery correlated with the compared column's own source loses that
/// correlation's narrowing of its source, but the query still answers, and
/// reruns whenever its result changes.
#[tokio::test]
async fn a_subquery_correlated_with_the_compared_columns_source_still_follows_its_result() {
    let db = TestDatabase::open(PAIRS);
    follows(
        &db,
        "SELECT x.id FROM x WHERE x.k = (SELECT y.k FROM y WHERE y.g = x.g ORDER BY y.id LIMIT 1) ORDER BY x.id",
        vec![],
        &[
            ("UPDATE y SET k = 7 WHERE id = 2", true),
            ("UPDATE x SET k = 7 WHERE id = 2", true),
            ("UPDATE y SET k = 3 WHERE id = 1", true),
            ("UPDATE x SET g = 2 WHERE id = 1", true),
        ],
    )
    .await;
    db.close();
}

/// A row value compared with a subquery's row narrows each of its columns
/// as that column compared alone would: a state row of another hash, or of
/// this hash at another revision, wakes nothing.
#[tokio::test]
async fn a_row_value_compared_with_a_subquery_narrows_by_each_column() {
    let db = TestDatabase::open(
        "CREATE TABLE scan(id INTEGER PRIMARY KEY, path TEXT, hash TEXT, rev INTEGER);
CREATE TABLE state(id INTEGER PRIMARY KEY, hash TEXT, rev INTEGER, value TEXT);
INSERT INTO scan VALUES (1,'/a','h1',1),(2,'/b','h2',1);
INSERT INTO state VALUES (1,'h1',1,'one'),(2,'h1',2,'old'),(3,'h2',1,'two')",
    );
    for sql in [
        "SELECT value FROM state WHERE (hash, rev) = (SELECT hash, rev FROM scan WHERE path = '/a') ORDER BY id",
        "SELECT value FROM state WHERE (SELECT hash, rev FROM scan WHERE path = '/a') = (hash, rev) ORDER BY id",
        "SELECT value FROM state WHERE (hash, rev) IN (SELECT hash, rev FROM scan WHERE path = '/a') ORDER BY id",
    ] {
        follows(&db, sql, vec![], &[
            ("UPDATE state SET value = 'deux' WHERE id = 3", false),
            ("UPDATE state SET value = 'older' WHERE id = 2", false),
            ("UPDATE state SET value = 'uno' WHERE id = 1", true),
            ("UPDATE scan SET rev = 2 WHERE id = 1", true),
            ("UPDATE state SET hash = 'h1', rev = 2 WHERE id = 3", true),
        ])
        .await;
        write(&db, "UPDATE scan SET rev = 1 WHERE id = 1; UPDATE state SET hash = 'h2', rev = 1, value = 'two' WHERE id = 3; UPDATE state SET value = 'one' WHERE id = 1; UPDATE state SET value = 'old' WHERE id = 2");
    }
    db.close();
    // A pair SQLite compares by NOCASE narrows nothing; the others still do.
    let db = TestDatabase::open(
        "CREATE TABLE x(id INTEGER PRIMARY KEY, k INTEGER, g TEXT, n TEXT COLLATE NOCASE);
CREATE TABLE y(id INTEGER PRIMARY KEY, k INTEGER, g TEXT, n TEXT COLLATE NOCASE);
INSERT INTO x VALUES (1,1,'a','a'),(2,2,'b','b'),(9,9,'z','z');
INSERT INTO y VALUES (1,1,'a','a'),(2,2,'b','b')",
    );
    follows(
        &db,
        "SELECT x.g FROM x WHERE (x.n, x.k) = (SELECT y.n, y.k FROM y WHERE y.id = 1)",
        vec![],
        &[
            ("UPDATE x SET g = g || '.' WHERE id = 9", false),
            ("UPDATE x SET n = 'A' WHERE id = 1", true),
        ],
    )
    .await;
    db.close();
}

/// What isn't a row's own value, or can't be compared as SQLite does, keeps
/// the compared column's table whole: a write to any row of it reruns.
#[tokio::test]
async fn other_subquery_values_keep_the_compared_table_whole() {
    let db = TestDatabase::open(
        "CREATE TABLE x(id INTEGER PRIMARY KEY, k INTEGER, g TEXT, n TEXT COLLATE NOCASE);
CREATE TABLE y(id INTEGER PRIMARY KEY, k INTEGER, g TEXT, n TEXT COLLATE NOCASE);
INSERT INTO x VALUES (1,1,'a','a'),(2,2,'b','b'),(9,9,'z','z');
INSERT INTO y VALUES (1,1,'a','a'),(2,2,'b','b')",
    );
    for sql in [
        "SELECT x.g FROM x WHERE x.k = (SELECT y.k + 1 FROM y UNION SELECT 5)",
        "SELECT x.g FROM x WHERE x.k = (SELECT max(y.k) FROM y)",
        "SELECT x.g FROM x WHERE x.k = (SELECT y.k + 1 FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE x.k = (SELECT x.id)",
        "SELECT x.g FROM x WHERE x.k = (SELECT y.g FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE x.g = (SELECT y.n FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE x.n = (SELECT y.g FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE x.k NOT IN (SELECT y.k FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE x.k < (SELECT y.k FROM y WHERE y.id = 2)",
        "SELECT x.g FROM x WHERE (x.k, x.g) <> (SELECT y.k, y.g FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE (x.k, x.g) NOT IN (SELECT y.k, y.g FROM y)",
    ] {
        let mut query = db.subscribe_rows(sql, no_values(), |r| r.get::<_, String>(0));
        next(&mut query).await;
        write(&db, "UPDATE x SET g = g || '.' WHERE id = 9");
        assert_eq!(rerun(&mut query), 2, "{sql}");
    }
    db.close();
}

/// A column IN a compound narrows by each arm: a cover of a hash no arm
/// returns wakes nothing, and one of either arm's hash does.
#[tokio::test]
async fn an_in_over_a_union_narrows_by_each_arm() {
    for sql in [
        "SELECT url FROM covers WHERE hash IN (SELECT hash FROM scan WHERE path = '/a' UNION ALL SELECT hash FROM scan WHERE path = '/b') ORDER BY id",
        "SELECT url FROM covers WHERE hash IN (SELECT hash FROM scan WHERE path = '/a' UNION SELECT 'h2') ORDER BY id",
    ] {
        let db = TestDatabase::open(
            "CREATE TABLE scan(id INTEGER PRIMARY KEY, path TEXT, hash TEXT);
            CREATE TABLE covers(id INTEGER PRIMARY KEY, hash TEXT, url TEXT);
            INSERT INTO scan VALUES (1,'/a','h1'),(2,'/b','h2'),(3,'/c','h3');
            INSERT INTO covers VALUES (1,'h1','u1'),(2,'h2','u2'),(3,'h3','u3')",
        );
        follows(&db, sql, vec![], &[
            ("UPDATE covers SET url = 'v3' WHERE id = 3", false),
            ("UPDATE covers SET url = 'v1' WHERE id = 1", true),
            ("UPDATE covers SET url = 'v2' WHERE id = 2", true),
        ])
        .await;
        db.close();
    }
    // The write to `x` id 9, whose `k` no arm returns, wakes nothing.
    let db = TestDatabase::open(
        "CREATE TABLE x(id INTEGER PRIMARY KEY, k INTEGER, g TEXT);
        CREATE TABLE y(id INTEGER PRIMARY KEY, k INTEGER);
        INSERT INTO x VALUES (1,1,'a'),(5,5,'e'),(9,9,'z');
        INSERT INTO y VALUES (1,1)",
    );
    follows(
        &db,
        "SELECT x.g FROM x WHERE x.k = (SELECT y.k FROM y UNION SELECT 5) ORDER BY x.id",
        vec![],
        &[
            ("UPDATE x SET g = 'zz' WHERE id = 9", false),
            ("UPDATE x SET g = 'aa' WHERE id = 1", true),
        ],
    )
    .await;
    db.close();
}

/// A write to a cover for another album's release runs the same SQLite
/// steps over 10 albums as over 1,000: the lookups ask a table by its key,
/// and don't run the cover pick for each album.
#[tokio::test]
async fn a_cover_write_for_another_album_runs_as_many_lookup_steps_over_any_number_of_albums() {
    let mut lookups = Vec::new();
    for albums in [10, 1000] {
        let db = TestDatabase::open(ALBUMS);
        for (table, row) in [
            ("releases", "'r' || i, 'a' || i, 1"),
            ("covers", "'r' || i, 'f' || i"),
            ("files", "'f' || i, 'n' || i"),
        ] {
            write(
                &db,
                &format!(
                    "WITH RECURSIVE n(i) AS (SELECT 4 UNION ALL SELECT i + 1 FROM n WHERE i < {})
                     INSERT INTO {table} SELECT {row} FROM n",
                    albums + 3
                ),
            );
        }
        let counter = crate::ConnectionWorkCounter::new();
        db.track_work(&counter);
        let mut query = db.subscribe_rows(COVER, no_values(), |r| r.get::<_, String>(0));
        next(&mut query).await;
        counter.take();
        write(&db, "UPDATE covers SET file = 'f9' WHERE id = 'r5'");
        assert!(!query.is_marked_for_rerun());
        lookups.push(counter.take().lookup_steps);
        drop(query);
        db.close();
    }
    assert!(lookups[0] > 0, "the write asks a lookup");
    assert_eq!(lookups[0], lookups[1]);
}

/// A column compared with the column of a subquery in FROM that names a row
/// around it gives no edge: sqlive can't read that subquery's rows alone, so
/// the column's table stays whole.
#[tokio::test]
async fn a_column_compared_with_a_correlated_subquerys_column_keeps_its_table_whole() {
    let db = TestDatabase::open(
        "CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT, title TEXT);
CREATE TABLE credits(id INTEGER PRIMARY KEY, track INTEGER, release TEXT);
INSERT INTO tracks VALUES (1,'r1','one'),(2,'r2','two');
INSERT INTO credits VALUES (1,1,'r1')",
    );
    follows(&db, "SELECT t.title FROM tracks t WHERE t.id IN (SELECT s.track FROM (SELECT track FROM credits c WHERE c.release = t.release) AS s) ORDER BY t.id", vec![], &[
        ("UPDATE tracks SET title = 'deux' WHERE id = 2", true),
    ])
    .await;
    db.close();
}
