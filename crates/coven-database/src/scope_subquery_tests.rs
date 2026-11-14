//! A column compared with a subquery's value narrows by the subquery's
//! source: the rows whose value that source holds in its scope (§4).

use crate::scope_lookup::tests::follows;
use crate::tests::next;
use crate::tests::TestStore;
use crate::DbError;
use crate::{Database, LiveQuery};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn write(db: &Database, sql: &str) {
    db.commit_writer(|db| db.batch(sql).unwrap());
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
/// whether or not it is the first, and so does moving a release between
/// albums or making another release the first.
#[tokio::test]
async fn a_cover_pick_narrows_covers_and_files_to_the_albums_releases() {
    let store = TestStore::new();
    let db = store.schema(vec![], ALBUMS).await.unwrap();
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
            ("UPDATE releases SET album_id = 'a2' WHERE id = 'r2'", true),
            ("UPDATE covers SET file = 'f9' WHERE id = 'r2'", false),
            ("UPDATE releases SET album_id = 'a1', album_order = 0 WHERE id = 'r2'", true),
            ("UPDATE covers SET file = 'f1c' WHERE id = 'r1'", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// A column compared with a subquery's value that passes through another
/// lookup table: the credits shape, where an artist is shown as another.
#[tokio::test]
async fn credits_narrow_artists_to_the_ones_they_show() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE credits(id INTEGER PRIMARY KEY, track TEXT, artist TEXT);
CREATE TABLE shown_as(artist TEXT NOT NULL PRIMARY KEY, shown TEXT);
CREATE TABLE artists(id TEXT NOT NULL PRIMARY KEY, name TEXT);
INSERT INTO credits VALUES (1,'t1','ar1'),(2,'t2','ar2');
INSERT INTO shown_as VALUES ('ar1','s1'),('ar2','s2');
INSERT INTO artists VALUES ('s1','S1'),('s2','S2')",
        )
        .await
        .unwrap();
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
    db.close().await.unwrap();
}

/// `col IN (SELECT x.c …)` narrows as `col = (SELECT x.c …)` does.
#[tokio::test]
async fn a_column_in_a_subquerys_column_narrows() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, album_id TEXT);
CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT, title TEXT);
CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER);
INSERT INTO releases VALUES ('r1','a1'),('r2','a1'),('r3','a2');
INSERT INTO tracks VALUES (1,'r1','one'),(2,'r2','two'),(3,'r3','three');
INSERT INTO items VALUES (1,0),(9,0)",
        )
        .await
        .unwrap();
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
    db.close().await.unwrap();
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
    let store = TestStore::new();
    let db = store.schema(vec![], PAIRS).await.unwrap();
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
    db.close().await.unwrap();
}

/// What isn't a row's own value, or can't be compared as SQLite does, keeps
/// the compared column's table whole: a write to any row of it reruns.
#[tokio::test]
async fn other_subquery_values_keep_the_compared_table_whole() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE x(id INTEGER PRIMARY KEY, k INTEGER, g TEXT, n TEXT COLLATE NOCASE);
CREATE TABLE y(id INTEGER PRIMARY KEY, k INTEGER, g TEXT, n TEXT COLLATE NOCASE);
INSERT INTO x VALUES (1,1,'a','a'),(2,2,'b','b'),(9,9,'z','z');
INSERT INTO y VALUES (1,1,'a','a'),(2,2,'b','b')",
        )
        .await
        .unwrap();
    for sql in [
        "SELECT x.g FROM x WHERE x.k = (SELECT y.k FROM y UNION SELECT 5)",
        "SELECT x.g FROM x WHERE x.k = (SELECT max(y.k) FROM y)",
        "SELECT x.g FROM x WHERE x.k = (SELECT y.k + 1 FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE x.k = (SELECT x.id)",
        "SELECT x.g FROM x WHERE x.k = (SELECT y.g FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE x.g = (SELECT y.n FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE x.n = (SELECT y.g FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE x.k NOT IN (SELECT y.k FROM y WHERE y.id = 1)",
        "SELECT x.g FROM x WHERE x.k < (SELECT y.k FROM y WHERE y.id = 2)",
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query: LiveQuery<_, DbError> = db.subscribe(move |db| {
            let run = count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok((run, db.query(sql, [], |r| r.get::<_, String>(0))?))
        });
        next(&mut query).await;
        write(&db, "UPDATE x SET g = g || '.' WHERE id = 9");
        assert_eq!(next(&mut query).await.0, 2, "{sql}");
    }
    db.close().await.unwrap();
}

/// A write to a cover for another album's release runs the same SQLite
/// steps over 10 albums as over 1,000: the lookups ask a table by its key,
/// and don't run the cover pick for each album.
#[tokio::test]
async fn a_cover_write_for_another_album_runs_as_many_lookup_steps_over_any_number_of_albums() {
    let mut lookups = Vec::new();
    for albums in [10, 1000] {
        let store = TestStore::new();
        let db = store.schema(vec![], ALBUMS).await.unwrap();
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
        let counter = crate::test_utils::WorkCounter::new();
        db.test_track_work(counter.clone()).await.unwrap();
        let mut query: LiveQuery<_, DbError> =
            db.subscribe(|sql| Ok(sql.query(COVER, [], |r| r.get::<_, String>(0))?));
        next(&mut query).await;
        counter.take();
        write(&db, "UPDATE covers SET file = 'f9' WHERE id = 'r5'");
        assert!(!query.is_marked_for_rerun());
        lookups.push(counter.take().lookup_steps);
        drop(query);
        db.close().await.unwrap();
    }
    assert!(lookups[0] > 0, "the write asks a lookup");
    assert_eq!(lookups[0], lookups[1]);
}
