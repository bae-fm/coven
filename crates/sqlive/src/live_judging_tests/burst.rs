//! A folder scan's burst of writes to other keys reruns no live query of
//! one key, whose statement holds the shapes the import pane and the cover
//! picker read: a USING join, CTEs read inside subqueries, a row value
//! compared with a subquery, and a row value equality between two tables.

use crate::tests::{next, TestDatabase};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const SCHEMA: &str =
    "CREATE TABLE scan(id INTEGER PRIMARY KEY, root TEXT, path TEXT, hash TEXT, rev INTEGER);
CREATE INDEX scan_path ON scan(path);
CREATE INDEX scan_hash ON scan(hash);
CREATE TABLE files(id INTEGER PRIMARY KEY, root TEXT, path TEXT, name TEXT);
CREATE INDEX files_place ON files(root, path);
CREATE TABLE state(id INTEGER PRIMARY KEY, hash TEXT, rev INTEGER, value TEXT);
CREATE INDEX state_hash ON state(hash, rev);
CREATE TABLE covers(id INTEGER PRIMARY KEY, hash TEXT, url TEXT);
CREATE INDEX covers_hash ON covers(hash);
CREATE TABLE cue_tracks(id INTEGER PRIMARY KEY, root TEXT, path TEXT, position INTEGER);
CREATE INDEX cue_tracks_place ON cue_tracks(root, path, position);
CREATE TABLE cue_index(id INTEGER PRIMARY KEY, root TEXT, path TEXT, position INTEGER, at INTEGER);
CREATE INDEX cue_index_place ON cue_index(root, path, position)";

/// The writes a folder scan makes for the key `key`: its scan row, a file,
/// its state, a cover and a CUE sheet's track and index.
fn scan(key: usize) -> String {
    format!(
        "INSERT INTO scan(root, path, hash, rev) VALUES ('/m', '/m/{key}', 'h{key}', 1);
        INSERT INTO files(root, path, name) VALUES ('/m', '/m/{key}', 'one.flac');
        INSERT INTO state(hash, rev, value) VALUES ('h{key}', 1, 'new');
        INSERT INTO covers(hash, url) VALUES ('h{key}', 'cover.jpg');
        INSERT INTO cue_tracks(root, path, position) VALUES ('/m', '/m/{key}', 1);
        INSERT INTO cue_index(root, path, position, at) VALUES ('/m', '/m/{key}', 1, 0)"
    )
}

const PANE: &str = "WITH candidate AS (SELECT root, path, hash FROM scan WHERE path = ?1)
SELECT
  (SELECT count(*) FROM files f JOIN candidate c ON f.root = c.root AND f.path = c.path),
  (SELECT group_concat(s.value) FROM state s
     WHERE (s.hash, s.rev) = (SELECT k.hash, k.rev FROM scan k WHERE k.path = ?1)),
  (SELECT group_concat(cv.url) FROM covers cv JOIN candidate USING (hash)),
  (SELECT count(*) FROM cue_tracks t JOIN candidate c ON t.root = c.root AND t.path = c.path
     JOIN cue_index i ON (i.root, i.path, i.position) = (t.root, t.path, t.position))";

#[tokio::test]
async fn a_burst_of_writes_to_other_keys_reruns_no_query_of_one_key() {
    let db = TestDatabase::open(SCHEMA);
    db.commit(&scan(0));
    let runs = Arc::new(AtomicUsize::new(0));
    let count = runs.clone();
    let mut query = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        let row = db.query(PANE, ["/m/0"], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        Ok(row)
    });
    let first = next(&mut query).await;
    assert_eq!(first, vec![(1, Some("new".to_owned()), Some("cover.jpg".to_owned()), 1)]);
    for key in 1..=1_000 {
        db.commit(&scan(key));
        assert!(!query.is_marked_for_rerun(), "the scan of key {key} reran the query");
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    db.commit("UPDATE state SET value = 'seen' WHERE hash = 'h0'");
    let rerun = next(&mut query).await;
    assert_eq!(rerun, vec![(1, Some("seen".to_owned()), Some("cover.jpg".to_owned()), 1)]);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    db.close();
}
