//! What a synced insert costs through coven (§4). The library is built here
//! as it ships, without its test-only statement profile; the same statement
//! and triggers in plain SQLite are measured by `a_triggered_statements_cost`
//! inside the crate. Run explicitly, in the release profile:
//!
//!   cargo test --release -p coven-database --test insert_cost -- --ignored --nocapture

use coven_database::{Database, DatabaseBuilder, Migration, RowIdentity, SyncedTable};
use coven_foundation::{
    files::StoreDirectories,
    id_source::{IdSource, SequentialIds, StoreId},
};
use std::time::{Duration, Instant};

/// A scanned folder listed through a trigger that reads a view of six
/// tables, as bae's import list keeps its rows.
const TRIGGERED: &str = "CREATE TABLE scans(id TEXT NOT NULL PRIMARY KEY, path TEXT NOT NULL, title TEXT NOT NULL);
CREATE TABLE edits(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE verdicts(id TEXT NOT NULL PRIMARY KEY, kind TEXT, position INTEGER);
CREATE TABLE matches(id TEXT NOT NULL, position INTEGER NOT NULL, pressing TEXT, failure TEXT, PRIMARY KEY(id, position));
CREATE TABLE origins(id TEXT NOT NULL PRIMARY KEY, created_at INTEGER);
CREATE TABLE skipped(path TEXT NOT NULL PRIMARY KEY);
CREATE TABLE listing(id TEXT NOT NULL PRIMARY KEY, tab TEXT NOT NULL, state TEXT, title TEXT NOT NULL, unit_path TEXT NOT NULL);
CREATE VIEW placed AS
  SELECT s.id, CASE WHEN o.id IS NOT NULL THEN 'done' WHEN k.path IS NOT NULL THEN 'skipped' ELSE 'pending' END AS tab,
         CASE WHEN v.kind IS NULL THEN 'not_looked_up'
              WHEN (SELECT count(DISTINCT pressing) FROM matches m WHERE m.id=s.id) <> 1 THEN 'several'
              WHEN EXISTS(SELECT 1 FROM matches m WHERE m.id=s.id AND m.failure IS NOT NULL) THEN 'unread'
              WHEN v.kind = 'found' THEN 'identified' ELSE 'needs_you' END AS state,
         coalesce(e.title, s.title) AS title,
         CASE WHEN instr(s.path,'/')>0 THEN substr(s.path,1,instr(s.path,'/')-1) ELSE s.path END AS unit_path
  FROM scans s LEFT JOIN edits e ON e.id=s.id LEFT JOIN verdicts v ON v.id=s.id
  LEFT JOIN origins o ON o.id=s.id LEFT JOIN skipped k ON k.path=s.path;
CREATE TRIGGER scans_listed AFTER INSERT ON scans BEGIN
  INSERT OR REPLACE INTO listing SELECT * FROM placed WHERE id=NEW.id; END;";

const INSERT: &str = "INSERT INTO scans VALUES (?1, ?2, ?3)";
/// Rows per write, and writes measured.
const ROWS: usize = 500;
const WRITES: usize = 40;

fn row(i: usize) -> (String, String, String) {
    (
        format!("id-{i}"),
        format!("Artist/Album {i}"),
        format!("title {i}"),
    )
}

/// The median write's time per row.
fn median(mut times: Vec<Duration>) -> Duration {
    times.sort();
    times[times.len() / 2] / ROWS as u32
}

async fn open(directory: &tempfile::TempDir) -> Database {
    let ids = SequentialIds::new();
    let store = StoreDirectories::new(directory.path().into())
        .create_store_dir(StoreId(ids.new_id()), &ids)
        .unwrap();
    DatabaseBuilder::new(store)
        .synced_tables(vec![SyncedTable::new("scans", RowIdentity::SharedKey)])
        .migrations(vec![Migration::addition(1, "scans", TRIGGERED)])
        .open()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one timing measurement, run explicitly in the release profile"]
async fn a_synced_inserts_cost() {
    let directory = tempfile::tempdir().unwrap();
    let db = open(&directory).await;
    let mut coven_times = Vec::new();
    for write in 0..WRITES {
        let started = Instant::now();
        db.write(move |sql| {
            for i in write * ROWS..(write + 1) * ROWS {
                sql.execute(INSERT, row(i))?;
            }
            Ok(())
        })
        .await
        .unwrap();
        coven_times.push(started.elapsed());
    }
    let listed: i64 = db
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM listing", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(listed, (ROWS * WRITES) as i64);
    db.close().await.unwrap();
    eprintln!(
        "per synced insert, committed in writes of {ROWS}: {:?}",
        median(coven_times)
    );
}
