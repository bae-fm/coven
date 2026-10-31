//! What filling and clearing a device-local table costs through a write and
//! through a bulk write (§4). The library is built here as it ships, without
//! its test-only statement profile. Run explicitly, in the release profile:
//!
//!   cargo test --release -p coven-database --test bulk_write_cost -- --ignored --nocapture

use coven_database::{Database, DatabaseBuilder, Migration, RowIdentity, SyncedTable};
use coven_foundation::{
    files::StoreDirectories,
    id_source::{IdSource, SequentialIds, StoreId},
};
use std::time::{Duration, Instant};

const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
CREATE TABLE queue(position INTEGER PRIMARY KEY, track TEXT NOT NULL UNIQUE);";

/// The rows the queue holds before it's cleared.
const ROWS: u32 = 100_000;

const FILL: &str = "WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<99999) \
     INSERT INTO queue SELECT i, printf('track-%08d', i) FROM n";
const CLEAR: &str = "DELETE FROM queue";

async fn open(directory: &tempfile::TempDir) -> Database {
    let ids = SequentialIds::new();
    let store = StoreDirectories::new(directory.path().into())
        .create_store_dir(StoreId(ids.new_id()), &ids)
        .unwrap();
    DatabaseBuilder::new(store)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::addition(1, "schema", SCHEMA)])
        .open()
        .await
        .unwrap()
}

async fn write(db: &Database, sql: &'static str) -> Duration {
    let started = Instant::now();
    db.write(move |context| Ok(context.execute_batch(sql)?)).await.unwrap();
    started.elapsed()
}

async fn bulk(db: &Database, sql: &'static str) -> Duration {
    let started = Instant::now();
    db.write_bulk(move |context| Ok::<_, coven_database::DbError>(context.execute_batch(sql)?))
        .await
        .unwrap();
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one timing measurement, run explicitly in the release profile"]
async fn clearing_a_local_table_through_a_write_and_a_bulk_write() {
    let directory = tempfile::tempdir().unwrap();
    let db = open(&directory).await;
    let mut live = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM queue", [], |r| r.get::<_, i64>(0))?)
    });
    assert_eq!(live.next().await.unwrap().unwrap(), 0);

    let write_fill = write(&db, FILL).await;
    assert_eq!(live.next().await.unwrap().unwrap(), i64::from(ROWS));
    let write_clear = write(&db, CLEAR).await;
    assert_eq!(live.next().await.unwrap().unwrap(), 0);

    let bulk_fill = bulk(&db, FILL).await;
    assert_eq!(live.next().await.unwrap().unwrap(), i64::from(ROWS));
    let bulk_clear = bulk(&db, CLEAR).await;
    assert_eq!(live.next().await.unwrap().unwrap(), 0);
    // What committing alone costs, `F_FULLFSYNC` included on macOS (§3).
    let commit = bulk(&db, "SELECT 1").await;

    drop(live);
    db.close().await.unwrap();
    eprintln!(
        "{ROWS} rows: fill {write_fill:?} through a write, {bulk_fill:?} through a bulk write; \
         clear {write_clear:?} through a write, {bulk_clear:?} through a bulk write; \
         an empty bulk write {commit:?}"
    );
}
