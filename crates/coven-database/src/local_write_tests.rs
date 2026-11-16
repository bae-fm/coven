//! What coven's own writes cost: an app write through coven's channel,
//! policy and capture, against the same SQL in plain SQLite, and while live
//! queries and readers run. The measurements run explicitly, in the release
//! profile, as scenarios of `scripts/perf.sh`.

use crate::tests::TestStore;
use crate::{DbError, RowIdentity, SqliteSettings, SyncedTable, WalAutocheckpoint};
use sqlive::triggered_inserts::SCHEMA as TRIGGERED;
use std::time::Duration;

/// What one app statement whose triggers reach a view costs through coven,
/// against plain SQLite preparing it each time and from its cache. Run
/// explicitly.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn a_triggered_statements_cost() {
    const STATEMENTS: usize = 500;
    const INSERT: &str = "INSERT INTO scans VALUES (?1, ?2, ?3)";
    let directory = tempfile::tempdir().unwrap();
    let plain = sqlive::rusqlite::Connection::open(directory.path().join("plain.db")).unwrap();
    plain
        .execute_batch(&format!("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; {TRIGGERED}"))
        .unwrap();
    let row = |i: usize| (format!("id-{i}"), format!("Artist/Album {i}"), format!("title {i}"));
    let plain_run = |cached: bool, start: usize| {
        plain.execute_batch("BEGIN").unwrap();
        let started = std::time::Instant::now();
        for i in start..start + STATEMENTS {
            if cached {
                plain.prepare_cached(INSERT).unwrap().execute(row(i)).unwrap();
            } else {
                plain.prepare(INSERT).unwrap().execute(row(i)).unwrap();
            }
        }
        let elapsed = started.elapsed();
        plain.execute_batch("COMMIT").unwrap();
        elapsed / STATEMENTS as u32
    };
    let fresh = plain_run(false, 0);
    let cached = plain_run(true, STATEMENTS);
    // The same triggers compiled into a statement that changes no row, so
    // its cost is preparing it.
    const MISS: &str = "UPDATE scans SET title=?2 WHERE id=?1";
    let plain_miss = |cached: bool| {
        let started = std::time::Instant::now();
        for i in 0..STATEMENTS {
            let row = (format!("absent-{i}"), "t");
            if cached {
                plain.prepare_cached(MISS).unwrap().execute(row).unwrap();
            } else {
                plain.prepare(MISS).unwrap().execute(row).unwrap();
            }
        }
        started.elapsed() / STATEMENTS as u32
    };
    let (fresh_miss, cached_miss) = (plain_miss(false), plain_miss(true));

    let store = TestStore::new();
    let db = store
        .builder(
            vec![SyncedTable::new("scans", RowIdentity::SharedKey)],
            vec![crate::Migration::addition(1, "scans", TRIGGERED)],
        )
        .open()
        .await
        .unwrap();
    let started = std::time::Instant::now();
    db.write(move |sql| {
        for i in 0..STATEMENTS {
            sql.execute(INSERT, row(i))?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let coven = started.elapsed() / STATEMENTS as u32;
    let started = std::time::Instant::now();
    db.write(move |sql| {
        for i in 0..STATEMENTS {
            sql.execute(MISS, (format!("absent-{i}"), "t"))?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let coven_miss = started.elapsed() / STATEMENTS as u32;
    db.close().await.unwrap();
    for (metric, time) in [
        ("triggered_insert/coven", coven),
        ("triggered_insert/plain_preparing_each", fresh),
        ("triggered_insert/plain_cached", cached),
        ("triggered_miss/coven", coven_miss),
        ("triggered_miss/plain_preparing_each", fresh_miss),
        ("triggered_miss/plain_cached", cached_miss),
    ] {
        crate::test_utils::report_time(metric, time);
    }
    eprintln!(
        "insert per statement: coven {coven:?}; plain SQLite preparing each {fresh:?}, from its cache {cached:?}"
    );
    eprintln!(
        "statement changing nothing: coven {coven_miss:?}; plain SQLite preparing each {fresh_miss:?}, from its cache {cached_miss:?}"
    );
}

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("parent", RowIdentity::SharedKey),
        SyncedTable::new("child", RowIdentity::SharedKey),
    ]
}

/// What a write to a device-local table costs through coven, against the
/// same SQL in plain SQLite with coven's durability settings, with a live
/// query that reads the whole table and with one that narrows by a column,
/// whose values each write then records. Run explicitly.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn local_table_writes_against_plain_sqlite() {
    const QUEUE: &str = "CREATE TABLE queue(lane INTEGER NOT NULL, position INTEGER NOT NULL, track TEXT NOT NULL, PRIMARY KEY(lane, position))";
    let fill = |rows: u32| {
        format!(
            "WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<{}) INSERT INTO queue SELECT 0, i, printf('track-%08d', i) FROM n",
            rows - 1
        )
    };
    for rows in [5_000, 50_000, 1_000_000] {
        let directory = tempfile::tempdir().unwrap();
        let plain = sqlive::rusqlite::Connection::open(directory.path().join("plain.db")).unwrap();
        plain
            .execute_batch(&format!(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; {QUEUE}"
            ))
            .unwrap();
        let started = std::time::Instant::now();
        plain.execute_batch(&format!("BEGIN; {}; COMMIT", fill(rows))).unwrap();
        let plain_insert = started.elapsed();
        let started = std::time::Instant::now();
        plain.execute_batch("BEGIN; DELETE FROM queue; COMMIT").unwrap();
        let plain_delete = started.elapsed();

        let store = TestStore::new();
        let db = store
            .builder(tables(), vec![crate::Migration::addition(1, "schema", "CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES parent(id) ON DELETE CASCADE)"), crate::Migration::addition(2, "queue", QUEUE)])
            .open()
            .await
            .unwrap();
        let mut live: crate::LiveQuery<_, DbError> = db.subscribe(|sql| {
            Ok(sql.query_row("SELECT count(*) FROM queue", [], |r| r.get::<_, i64>(0))?)
        });
        assert_eq!(live.next_inline().await.expect("the live query answers").unwrap(), 0);
        let insert: &'static str = Box::leak(fill(rows).into_boxed_str());
        let started = std::time::Instant::now();
        crate::write::tests::sql(&db, insert).await.unwrap();
        let coven_insert = started.elapsed();
        assert_eq!(
            live.next_inline().await.expect("the live query answers").unwrap(),
            i64::from(rows)
        );
        let started = std::time::Instant::now();
        crate::write::tests::sql(&db, "DELETE FROM queue").await.unwrap();
        let coven_delete = started.elapsed();
        assert_eq!(live.next_inline().await.expect("the live query answers").unwrap(), 0);
        drop(live);
        let mut narrow: crate::LiveQuery<_, DbError> = db.subscribe(|sql| {
            Ok(sql.query("SELECT position FROM queue WHERE track = 'none'", [], |r| {
                r.get::<_, i64>(0)
            })?)
        });
        assert!(narrow.next_inline().await.expect("the live query answers").unwrap().is_empty());
        let started = std::time::Instant::now();
        crate::write::tests::sql(&db, insert).await.unwrap();
        let narrowed_insert = started.elapsed();
        let started = std::time::Instant::now();
        crate::write::tests::sql(&db, "DELETE FROM queue").await.unwrap();
        let narrowed_delete = started.elapsed();
        // Past the bound, the write keeps only that the table changed.
        assert_eq!(narrow.is_marked_for_rerun(), rows as usize > sqlive::ROWS_PER_TABLE);
        db.close().await.unwrap();
        for (metric, time) in [
            ("coven_insert", coven_insert),
            ("narrowed_insert", narrowed_insert),
            ("plain_insert", plain_insert),
            ("coven_delete", coven_delete),
            ("narrowed_delete", narrowed_delete),
            ("plain_delete", plain_delete),
        ] {
            crate::test_utils::report_time(&format!("local_table_{rows}_rows/{metric}"), time);
        }
        eprintln!(
            "{rows} rows: insert {coven_insert:?} through coven, {narrowed_insert:?} narrowed, {plain_insert:?} plain; delete {coven_delete:?} through coven, {narrowed_delete:?} narrowed, {plain_delete:?} plain"
        );
    }
}

/// Who reads while the writer commits.
#[derive(Clone, Copy, Debug)]
enum Reading {
    /// Nobody.
    None,
    /// A live query over the whole table, run again after each commit.
    LiveQuery,
    /// A read of the whole table, again and again with no pause, for as
    /// long as the writes go on.
    Unpaused,
}

/// How long `commits` small writes take, each adding four rows of 4 KB,
/// while reads run as `reading` says; the largest size the WAL file reached;
/// and its size when the writes ended.
async fn timed_writes(
    commits: usize,
    reading: Reading,
    settings: SqliteSettings,
) -> (Duration, u64, u64) {
    let store = TestStore::new();
    let db = store
        .builder(
            Vec::new(),
            vec![crate::Migration::addition(
                1,
                "blobs",
                "CREATE TABLE blobs(id INTEGER PRIMARY KEY, body BLOB NOT NULL)",
            )],
        )
        .sqlite_settings(settings)
        .open()
        .await
        .unwrap();
    let wal = {
        let mut path = store.database_path().into_os_string();
        path.push("-wal");
        std::path::PathBuf::from(path)
    };
    let size = || std::fs::metadata(&wal).map_or(0, |m| m.len());
    const WHOLE: &str = "SELECT count(*),sum(length(body)) FROM blobs";
    let read = |sql: crate::SqlReadContext<'_>| -> Result<(i64, Option<i64>), DbError> {
        Ok(sql.query_row(WHOLE, [], |r| Ok((r.get(0)?, r.get(1)?)))?)
    };
    let reader = match reading {
        Reading::None => None,
        Reading::LiveQuery => {
            let mut query = db.subscribe(read);
            Some(tokio::spawn(async move { while let Some(Ok(_)) = query.next_inline().await {} }))
        }
        Reading::Unpaused => {
            let reading = db.clone();
            Some(tokio::spawn(async move { while reading.read(read).await.is_ok() {} }))
        }
    };
    let mut largest = 0;
    let started = std::time::Instant::now();
    for _ in 0..commits {
        db.write(|sql| {
            sql.execute(
                "INSERT INTO blobs(body) SELECT randomblob(4000) FROM (VALUES(1),(2),(3),(4))",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        largest = largest.max(size());
    }
    let elapsed = started.elapsed();
    let ended = size();
    if let Some(reader) = reader {
        reader.abort();
        drop(reader.await);
    }
    db.close().await.unwrap();
    (elapsed, largest, ended)
}

/// What a writer's commits cost while a live query over its table runs
/// again after each one, or a reader reads it with no pause at all, against
/// nobody reading: a reader keeps a snapshot open nearly all the time, which
/// a checkpoint must wait for. Run explicitly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "one timing measurement, run explicitly"]
async fn commits_under_a_rerunning_live_query_cost() {
    let off = SqliteSettings {
        wal_autocheckpoint: Some(WalAutocheckpoint::Off),
        ..SqliteSettings::default()
    };
    for (name, settings) in [("coven's checkpoints", SqliteSettings::default()), ("none", off)] {
        for reading in [Reading::None, Reading::LiveQuery, Reading::Unpaused] {
            let (elapsed, largest, ended) = timed_writes(2000, reading, settings.clone()).await;
            let metric = format!("commits/{name}/{reading:?}");
            crate::test_utils::report_time(&format!("{metric}/elapsed"), elapsed);
            crate::test_utils::report_cost(
                &format!("{metric}/wal_largest_kb"),
                (largest / 1024) as f64,
                "count",
            );
            crate::test_utils::report_cost(
                &format!("{metric}/wal_end_kb"),
                (ended / 1024) as f64,
                "count",
            );
            eprintln!(
                "{name}, reading {reading:?}: 2,000 commits in {elapsed:?}, WAL at most {} KB, {} KB at the end",
                largest / 1024,
                ended / 1024
            );
        }
    }
}
