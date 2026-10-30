//! A subquery in FROM, a CTE or one of the app's views is read as cores of
//! its own, and the conditions on its rows narrow its tables.

use crate::tests::TestStore;
use crate::{Database, LiveQuery};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut LiveQuery<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("query must answer")
        .expect("the live query answers")
        .unwrap()
}

fn write(db: &Database, sql: &str) {
    db.commit_writer(|db| db.batch(sql).unwrap());
}

const MUSIC: &str = "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, album TEXT, year INTEGER);
CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT, position INTEGER, title TEXT);
CREATE VIEW release_tracks AS SELECT r.id AS release, r.album, t.id AS track, t.title FROM releases r JOIN tracks t ON t.release = r.id;
CREATE VIEW album_releases AS SELECT * FROM release_tracks;
CREATE VIEW names(key, name) AS SELECT release, title FROM tracks UNION ALL SELECT id, album FROM releases;
CREATE VIEW later AS WITH RECURSIVE c(id) AS (SELECT min(id) FROM releases UNION SELECT r.id FROM releases r JOIN c ON r.id > c.id) SELECT id FROM c;
INSERT INTO releases VALUES ('r1','x',2001),('r2','x',2002),('r3','y',2003);
INSERT INTO tracks VALUES (1,'r1',1,'one'),(2,'r1',2,'two'),(3,'r2',1,'three'),(4,'r3',1,'four')";

/// Run `sql` live, then each write: `false` must leave it unmarked, `true`
/// must rerun it, and the rerun must read what a one-shot read does.
async fn follows(
    db: &Database,
    sql: &'static str,
    parameters: Vec<crate::sql_value::SqlValue>,
    writes: &[(&str, bool)],
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let bound = parameters.clone();
    let mut query = db.subscribe(move |db| {
        let run = count.fetch_add(1, Ordering::SeqCst);
        let rows = db.query(sql, rusqlite::params_from_iter(&bound), |r| {
            r.get::<_, crate::sql_value::SqlValue>(0)
        })?;
        Ok((run, rows))
    });
    let mut runs = next(&mut query).await.0;
    for (write_sql, reruns) in writes {
        write(db, write_sql);
        if *reruns {
            let (run, rows) = next(&mut query).await;
            assert!(run > runs, "{sql} after {write_sql}");
            runs = run;
            let parameters = parameters.clone();
            let fresh = db
                .read(move |db| {
                    Ok(db.query(sql, rusqlite::params_from_iter(&parameters), |r| {
                        r.get::<_, crate::sql_value::SqlValue>(0)
                    })?)
                })
                .await
                .unwrap();
            assert_eq!(rows, fresh, "{sql} after {write_sql}");
        } else {
            assert!(!query.is_marked_for_rerun(), "{sql} after {write_sql}");
        }
    }
}

/// A condition on a column that passes a table's column on unchanged
/// narrows that table, through a CTE, a subquery or a view, and through a
/// view of a view: another album's tracks wake nothing.
#[tokio::test]
async fn a_condition_on_a_passed_column_narrows_the_table_it_comes_from() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
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
    db.close().await.unwrap();
}

/// A CTE that reads one release's row gives the rows joined to it their
/// values, as a candidate's state gives its matches theirs; each arm of a
/// compound view takes the condition on its own column.
#[tokio::test]
async fn a_ctes_rows_give_values_and_each_arm_takes_the_condition() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    follows(
        &db,
        "WITH c AS MATERIALIZED (SELECT r.id, r.album FROM releases r WHERE r.id = ?1) SELECT coalesce(t.title, c.album) FROM c LEFT JOIN tracks t ON t.release = c.id ORDER BY t.id",
        vec![crate::sql_value::SqlValue::Text(b"r2".to_vec())],
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
    db.close().await.unwrap();
}

/// A subquery's rows that come of many of its sources', or from a LIMIT
/// over all of them, say nothing of each source row: a write to a row
/// outside the condition can still change them, and reruns the query.
#[tokio::test]
async fn grouped_or_limited_rows_keep_their_tables_whole() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
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
    db.close().await.unwrap();
}

/// A subquery inside a correlated one reads the names around it, so coven
/// reads none of its rows alone; its tables still take the values the
/// statement around it matched.
#[tokio::test]
async fn a_correlated_subquery_in_from_narrows_without_being_read_alone() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    follows(
        &db,
        "SELECT r.id FROM releases r WHERE r.album = 'x' AND EXISTS (SELECT 1 FROM tracks o JOIN (SELECT t.id FROM tracks t WHERE t.release = r.id AND t.position = 2) AS x ON o.id = x.id) ORDER BY r.id",
        vec![],
        &[
            ("UPDATE tracks SET position = 2 WHERE id = 4", true),
            ("UPDATE tracks SET position = 2 WHERE id = 3", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// A view coven can't read as cores of its own, as one with a recursive
/// CTE, gives the tables beside it values and keeps its own tables whole.
#[tokio::test]
async fn a_view_with_a_recursive_cte_keeps_its_tables_whole() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
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
    db.close().await.unwrap();
}
