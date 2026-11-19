use super::*;
use crate::tests::{QueryError, TestDatabase, TestError, TestOptions, TestRead};
use crate::{HostSql, NoPolicy};
use std::path::Path;
use std::sync::Arc;

/// A choice for every setting, each unlike SQLite's default and within what
/// SQLite keeps as given.
fn chosen() -> SqliteSettings {
    SqliteSettings {
        busy_timeout: Some(Duration::from_millis(1234)),
        cache_size: Some(CacheSize::Kibibytes(4096)),
        cache_spill: Some(CacheSpill::Pages(NonZeroU32::new(5000).unwrap())),
        mmap_size: Some(1 << 20),
        temp_store: Some(TempStore::Memory),
        threads: Some(2),
        journal_size_limit: Some(JournalSizeLimit::Bytes(1 << 16)),
        wal_autocheckpoint: Some(WalAutocheckpoint::Pages(NonZeroU32::new(500).unwrap())),
        automatic_index: Some(false),
        cell_size_check: Some(true),
        secure_delete: Some(SecureDelete::Fast),
        fullfsync: Some(true),
        checkpoint_fullfsync: Some(true),
        analysis_limit: Some(AnalysisLimit::Rows(NonZeroU32::new(400).unwrap())),
        page_size: Some(PageSize::Bytes8192),
        auto_vacuum: Some(AutoVacuum::Incremental),
    }
}

fn in_effect() -> SqliteSettingsInEffect {
    SqliteSettingsInEffect {
        busy_timeout: Duration::from_millis(1234),
        cache_size: CacheSize::Kibibytes(4096),
        cache_spill: CacheSpill::Pages(NonZeroU32::new(5000).unwrap()),
        mmap_size: 1 << 20,
        temp_store: TempStore::Memory,
        threads: 2,
        journal_size_limit: JournalSizeLimit::Bytes(1 << 16),
        wal_autocheckpoint: WalAutocheckpoint::Pages(NonZeroU32::new(500).unwrap()),
        automatic_index: false,
        cell_size_check: true,
        secure_delete: SecureDelete::Fast,
        fullfsync: true,
        checkpoint_fullfsync: true,
        analysis_limit: AnalysisLimit::Rows(NonZeroU32::new(400).unwrap()),
        page_size: PageSize::Bytes8192,
        auto_vacuum: AutoVacuum::Incremental,
    }
}

const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '')";

/// A database of notes under `settings`, checkpointed as they ask.
fn notes(settings: SqliteSettings) -> TestDatabase {
    TestDatabase::open_with(NOTES, options(settings))
}

/// The options of a database under `settings`, checkpointed as they ask.
fn options(settings: SqliteSettings) -> TestOptions {
    TestOptions {
        checkpoint_pages: settings.checkpoint_pages(),
        settings,
        ..TestOptions::default()
    }
}

/// The options of a database under `settings` whose opening must not write:
/// its first reader opens before its writer, and no thread of sqlive's
/// checkpoints it.
fn reading(settings: SqliteSettings) -> TestOptions {
    TestOptions { prepare_on_reader: true, settings, ..TestOptions::default() }
}

/// Each connection's settings in effect: the writer's first, then the
/// readers'.
fn each_in_effect(db: &TestDatabase) -> Vec<SqliteSettingsInEffect> {
    let mut settings = vec![db.host(|writer| writer.sqlite_settings().unwrap())];
    db.with_database(|database| {
        database.each_reader(|reader| settings.push(reader.sqlite_settings().unwrap()))
    });
    settings
}

/// A writable database's writer never checkpoints as it commits: sqlive's
/// checkpointer does, at the size its readers report (§4).
fn never_as_it_commits(settings: SqliteSettingsInEffect) -> SqliteSettingsInEffect {
    SqliteSettingsInEffect { wal_autocheckpoint: WalAutocheckpoint::Off, ..settings }
}

/// sqlive's settings that no host choice changes, on any connection (§4).
fn assert_fixed(connection: &HostSql<NoPolicy>) {
    let read = |pragma: &str| -> i64 {
        connection.query_row(&format!("PRAGMA {pragma}"), [], |r| r.get(0)).unwrap()
    };
    assert_eq!(read("foreign_keys"), 1);
    assert_eq!(read("recursive_triggers"), 1);
    assert_eq!(read("trusted_schema"), 0);
    // Defensive mode keeps a connection from writing the schema as a table.
    connection.batch("PRAGMA writable_schema = ON").unwrap();
    assert_eq!(read("writable_schema"), 0);
    let mode = |pragma: &str| -> String {
        connection.query_row(&format!("PRAGMA {pragma}"), [], |r| r.get(0)).unwrap()
    };
    assert_eq!(mode("journal_mode"), "wal");
    assert_eq!(mode("locking_mode"), "normal");
}

/// Every connection takes the host's settings, and keeps sqlive's fixed
/// ones: a writable database's and one whose opening must not write.
#[test]
fn every_connection_takes_the_hosts_settings_and_keeps_sqlives() {
    let db = notes(chosen());
    let settings = each_in_effect(&db);
    assert_eq!(settings[0], never_as_it_commits(in_effect()));
    assert!(settings[1..].iter().all(|reader| *reader == in_effect()), "{settings:#?}");
    db.host(|writer| {
        assert_fixed(writer);
        let synchronous: i64 = writer.query_row("PRAGMA synchronous", [], |r| r.get(0)).unwrap();
        assert_eq!(synchronous, 2, "FULL");
    });
    db.with_database(|database| database.each_reader(|reader| assert_fixed(reader)));

    let db = db.reopen(reading(chosen()));
    let settings = each_in_effect(&db);
    assert!(settings.iter().all(|connection| *connection == in_effect()), "{settings:#?}");
    db.host(|writer| assert_fixed(writer));
    db.with_database(|database| database.each_reader(|reader| assert_fixed(reader)));
    db.close();
}

#[test]
fn unset_settings_keep_sqlites_defaults_except_sqlives_own() {
    let db = notes(SqliteSettings::default());
    let settings = db.read(|reader| reader.sqlite_settings().unwrap());
    // The spill threshold is the cache's own page count, which depends on
    // SQLite's per-page overhead.
    assert!(matches!(settings.cache_spill, CacheSpill::Pages(_)));
    assert_eq!(
        settings,
        SqliteSettingsInEffect {
            // rusqlite's, set as it opens each connection.
            busy_timeout: Duration::from_secs(5),
            cache_size: CacheSize::Kibibytes(2000),
            cache_spill: settings.cache_spill,
            mmap_size: 0,
            temp_store: TempStore::Default,
            threads: 0,
            // sqlive's own, which commits leave alone (§4).
            journal_size_limit: JournalSizeLimit::Bytes(4 << 20),
            wal_autocheckpoint: WalAutocheckpoint::Pages(NonZeroU32::new(1000).unwrap()),
            automatic_index: true,
            cell_size_check: false,
            secure_delete: SecureDelete::Off,
            fullfsync: false,
            // sqlive's own on macOS and iOS (§3); SQLite's off elsewhere.
            checkpoint_fullfsync: cfg!(any(target_os = "macos", target_os = "ios")),
            analysis_limit: AnalysisLimit::Unlimited,
            page_size: PageSize::Bytes4096,
            // sqlive's own, so a host can give back the pages it frees.
            auto_vacuum: AutoVacuum::Incremental,
        }
    );
    let each = each_in_effect(&db);
    assert_eq!(each[0], never_as_it_commits(settings.clone()));
    assert!(each[1..].iter().all(|reader| *reader == settings), "{each:#?}");

    let db = db.reopen(reading(SqliteSettings::default()));
    let each = each_in_effect(&db);
    assert!(each.iter().all(|connection| *connection == settings), "{each:#?}");
    db.close();
}

#[test]
fn the_hosts_sync_choices_replace_sqlives() {
    let settings = || SqliteSettings {
        fullfsync: Some(true),
        checkpoint_fullfsync: Some(false),
        ..SqliteSettings::default()
    };
    let syncs = |db: &TestDatabase| -> Vec<(bool, bool)> {
        each_in_effect(db)
            .into_iter()
            .map(|settings| (settings.fullfsync, settings.checkpoint_fullfsync))
            .collect()
    };
    let db = notes(settings());
    assert!(syncs(&db).iter().all(|syncs| *syncs == (true, false)));
    let db = db.reopen(reading(settings()));
    assert!(syncs(&db).iter().all(|syncs| *syncs == (true, false)));
    db.close();
}

#[test]
fn a_database_keeps_the_page_size_and_auto_vacuum_it_was_created_with() {
    let created = SqliteSettings {
        page_size: Some(PageSize::Bytes16384),
        auto_vacuum: Some(AutoVacuum::Full),
        ..SqliteSettings::default()
    };
    let db = notes(created);
    let settings = db.read(|reader| reader.sqlite_settings().unwrap());
    assert_eq!(
        (settings.page_size, settings.auto_vacuum),
        (PageSize::Bytes16384, AutoVacuum::Full)
    );
    let db = db.reopen(options(SqliteSettings {
        page_size: Some(PageSize::Bytes1024),
        auto_vacuum: Some(AutoVacuum::Incremental),
        ..SqliteSettings::default()
    }));
    let settings = db.read(|reader| reader.sqlite_settings().unwrap());
    assert_eq!(
        (settings.page_size, settings.auto_vacuum),
        (PageSize::Bytes16384, AutoVacuum::Full)
    );
    db.close();
}

/// A lone connection to a new file in a directory of its own, under
/// `settings`.
fn lone(settings: &SqliteSettings) -> Result<HostSql<NoPolicy>, TestError> {
    let directory = tempfile::tempdir().unwrap();
    HostSql::open(
        &directory.path().join("lone.sqlite"),
        false,
        NoPolicy,
        Arc::new(crate::WholeViews::new([])),
        settings,
        &|_, _| Ok(()),
    )
}

#[test]
fn a_setting_sqlite_cannot_hold_fails_the_open() {
    let opened = lone(&SqliteSettings {
        busy_timeout: Some(Duration::from_millis(i32::MAX as u64 + 1)),
        ..SqliteSettings::default()
    });
    assert!(matches!(
        opened.err().unwrap(),
        crate::Error::SqliteSettingOutOfRange {
            setting: "busy_timeout",
            maximum,
        } if maximum == i32::MAX as u64
    ));
}

/// The WAL file's length in bytes.
fn wal_bytes(db: &TestDatabase) -> u64 {
    let mut path = db.path().to_owned().into_os_string();
    path.push("-wal");
    std::fs::metadata(path).unwrap().len()
}

/// Write `count` notes, one to a write, titled after `round`.
fn write_notes(db: &TestDatabase, round: &str, count: usize) {
    for n in 0..count {
        let (id, title) = (format!("{round}-{n}"), format!("{round} {n}"));
        db.write_app(|sql| {
            sql.app_execute("INSERT INTO notes(id,title,body) VALUES (?1,?2,'')", (&id, &title))
        })
        .unwrap();
    }
}

/// A checkpoint of mode `mode` on the writer, between writes.
fn checkpoint(db: &TestDatabase, mode: CheckpointMode) -> WalCheckpoint {
    db.host(|writer| writer.wal_checkpoint(mode)).unwrap()
}

fn manual_checkpoints() -> SqliteSettings {
    SqliteSettings { wal_autocheckpoint: Some(WalAutocheckpoint::Off), ..SqliteSettings::default() }
}

#[test]
fn a_checkpoint_reports_sqlites_frame_counts() {
    let db = notes(manual_checkpoints());
    write_notes(&db, "first", 10);
    let WalCheckpoint::Ran { busy, log_frames, checkpointed_frames } =
        checkpoint(&db, CheckpointMode::Passive)
    else {
        panic!("the checkpoint read the WAL");
    };
    assert!(!busy);
    assert!(log_frames > 0);
    assert_eq!(checkpointed_frames, log_frames);
    // A WAL file is a 32-byte header, then frames of a 24-byte header and a page.
    assert_eq!(wal_bytes(&db), 32 + u64::from(log_frames) * (24 + 4096));

    assert_eq!(
        checkpoint(&db, CheckpointMode::Truncate),
        WalCheckpoint::Ran { busy: false, log_frames: 0, checkpointed_frames: 0 }
    );
    assert_eq!(wal_bytes(&db), 0);
    db.close();
}

#[test]
fn after_a_restart_checkpoint_the_next_writes_reuse_the_wal() {
    let db = notes(manual_checkpoints());
    write_notes(&db, "first", 20);
    // With no checkpoint, each write appends to the WAL.
    let written = wal_bytes(&db);
    write_notes(&db, "unchecked", 1);
    assert!(wal_bytes(&db) > written);

    let WalCheckpoint::Ran { busy, log_frames: restart_frames, checkpointed_frames } =
        checkpoint(&db, CheckpointMode::Restart)
    else {
        panic!("the restart read the WAL");
    };
    assert!(!busy);
    assert_eq!(checkpointed_frames, restart_frames);
    let restarted = wal_bytes(&db);
    write_notes(&db, "second", 5);
    let WalCheckpoint::Ran { log_frames: second_frames, .. } =
        checkpoint(&db, CheckpointMode::Passive)
    else {
        panic!("the checkpoint read the WAL");
    };
    assert_eq!(wal_bytes(&db), restarted, "the WAL did not grow");
    // The log holds only the writes since the restart.
    assert!(second_frames < restart_frames);
    db.close();
}

#[test]
fn a_restart_checkpoint_reports_busy_while_a_read_uses_the_wal() {
    let db = notes(SqliteSettings {
        busy_timeout: Some(Duration::from_millis(20)),
        ..manual_checkpoints()
    });
    write_notes(&db, "first", 3);
    let (reading, read) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    std::thread::scope(|scope| {
        let shared = &db;
        let reader = scope.spawn(move || {
            shared.read_app(|sql| {
                sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?;
                reading.send(()).unwrap();
                released.recv().unwrap();
                Ok(())
            })
        });
        read.recv().unwrap();
        let WalCheckpoint::Ran { busy, log_frames, .. } = checkpoint(&db, CheckpointMode::Restart)
        else {
            panic!("the restart read the WAL");
        };
        assert!(busy);
        assert!(log_frames > 0);
        release.send(()).unwrap();
        reader.join().unwrap().unwrap();
    });
    let done = checkpoint(&db, CheckpointMode::Restart);
    assert!(matches!(done, WalCheckpoint::Ran { busy: false, .. }), "{done:?}");
    db.close();
}

#[test]
fn an_incremental_vacuum_gives_back_free_pages() {
    let db = notes(SqliteSettings {
        auto_vacuum: Some(AutoVacuum::Incremental),
        ..SqliteSettings::default()
    });
    let free = || db.value::<i64>("PRAGMA freelist_count");
    db.host(|writer| {
        writer
            .batch(
                "CREATE TABLE scratch(x); INSERT INTO scratch VALUES (zeroblob(100000)); DROP TABLE scratch;",
            )
            .unwrap()
    });
    let freed = free();
    assert!(freed > 2);
    db.host(|writer| {
        writer.incremental_vacuum(IncrementalVacuum::Pages(NonZeroU32::new(2).unwrap()))
    })
    .unwrap();
    assert_eq!(free(), freed - 2);
    db.host(|writer| writer.incremental_vacuum(IncrementalVacuum::All)).unwrap();
    assert_eq!(free(), 0);
    db.close();
}

#[test]
fn wal_refusal_names_the_selected_journal_mode() {
    let sql = HostSql::open(
        Path::new(":memory:"),
        false,
        NoPolicy,
        Arc::new(crate::WholeViews::new([])),
        &SqliteSettings::default(),
        &|_, _| Ok(()),
    )
    .unwrap();
    assert!(
        matches!(sql.enable_wal(), Err(crate::Error::WalUnavailable { mode }) if mode == "memory")
    );
}

/// The options of a database whose planner's statistics sqlive keeps.
fn kept_statistics() -> TestOptions {
    TestOptions { statistics: true, ..TestOptions::default() }
}

/// Notes with an index the planner keeps statistics for.
fn indexed_notes() -> TestDatabase {
    TestDatabase::open_with(
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL); CREATE INDEX notes_title ON notes(title)",
        kept_statistics(),
    )
}

/// A connection of the test's own to `db`'s file, which reads its
/// statistics as an open does.
fn file(db: &TestDatabase) -> rusqlite::Connection {
    rusqlite::Connection::open(db.path()).unwrap()
}

/// The rows the index `notes_title` holds by its statistics in `db`'s file.
fn counted_titles(db: &TestDatabase) -> Option<i64> {
    file(db)
        .query_row("SELECT stat FROM sqlite_stat1 WHERE idx='notes_title'", [], |r| {
            r.get::<_, String>(0)
        })
        .ok()
        .map(|stat| stat.split(' ').next().unwrap().parse().unwrap())
}

/// The ANALYZE statements `PRAGMA optimize` would run on `db`'s file now,
/// without running them.
fn would_analyze(db: &TestDatabase) -> Vec<String> {
    let file = file(db);
    let mut statement = file.prepare("PRAGMA optimize=0x10013").unwrap();
    let rows = statement.query_map([], |r| r.get::<_, String>(0)).unwrap();
    rows.collect::<rusqlite::Result<_>>().unwrap()
}

/// Opening gathers statistics for a table that has none (§4).
#[test]
fn opening_gathers_statistics_a_table_lacks() {
    let db = indexed_notes();
    insert_titles(&db, 1..=200);
    let db = db.reopen(TestOptions::default());
    file(&db).execute_batch("DELETE FROM sqlite_stat1 WHERE tbl='notes'").unwrap();
    assert_eq!(counted_titles(&db), None);
    let db = db.reopen(kept_statistics());
    let counted: String = db.value("SELECT stat FROM sqlite_stat1 WHERE idx='notes_title'");
    assert_eq!(counted, "200 29");
    // `PRAGMA optimize` gathers no samples, so the planner never evaluates a
    // constant expression against them while it plans.
    assert_eq!(db.value::<i64>("SELECT count(*) FROM sqlite_stat4"), 0);
    db.close();
}

/// Insert notes `ids`, titled by their id modulo 7.
fn insert_titles(db: &TestDatabase, ids: std::ops::RangeInclusive<i64>) {
    db.write_app(|sql| {
        for id in ids {
            sql.app_execute(
                "INSERT INTO notes VALUES (?1, ?2)",
                (id.to_string(), format!("title {}", id % 7)),
            )?;
        }
        Ok(())
    })
    .unwrap();
}

/// What `PRAGMA optimize` would analyze on `db`'s file is small: sqlive keeps
/// no statistics for a table of fewer than 100 rows, so SQLite names each of
/// those every time, and reads fewer than 100 rows of each.
fn assert_only_small_tables_to_read(db: &TestDatabase) {
    let file = file(db);
    for statement in would_analyze(db) {
        let table = statement.strip_prefix("ANALYZE ").unwrap();
        let rows: i64 =
            file.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap();
        assert!(rows < SMALL_TABLE, "{statement}: {rows} rows");
    }
}

/// Closing keeps no statistics for a table of one row, and gathers them for
/// a table that grew to 200 during the session. A second open of tables that
/// kept their size gathers none for the large ones. The test reads the file
/// while the database is open again without keeping statistics of its own.
#[test]
fn closing_follows_the_sessions_writes_and_an_unchanged_database_analyzes_only_small_tables() {
    let db = indexed_notes();
    insert_titles(&db, 1..=1);
    let db = db.reopen(kept_statistics());
    assert_eq!(counted_titles(&db), None);
    insert_titles(&db, 2..=200);
    let db = db.reopen(TestOptions::default());
    assert_eq!(counted_titles(&db), Some(200));
    assert_only_small_tables_to_read(&db);
    // Reopening finds every table's statistics current.
    let db = db.reopen(kept_statistics()).reopen(TestOptions::default());
    assert_eq!(counted_titles(&db), Some(200));
    db.close();
}

/// An app write can't run ANALYZE: the write's session table filter admits
/// `sqlite_stat1`, so the write's session would capture the statistics
/// among its changes.
#[test]
fn an_app_write_cant_analyze() {
    let db = indexed_notes();
    db.write("INSERT INTO notes VALUES ('1','a'),('2','b')").unwrap();
    let db = db.reopen(kept_statistics());
    // The test adds rows, so analyzing in the write would change the
    // statistics.
    db.write("INSERT INTO notes VALUES ('3','c'),('4','c')").unwrap();
    match db.write("ANALYZE notes") {
        Err(crate::Error::StatementForbidden { operation }) => {
            assert_eq!(operation, "ANALYZE in a write")
        }
        result => panic!("expected the refusal, received {result:?}"),
    }
    db.close();
}

/// What gathering statistics costs on a database of a music library's
/// size: four tables of 20,000 rows, each with its key and two indexes. Run
/// explicitly.
#[test]
#[ignore = "one timing measurement, run explicitly"]
fn gathering_statistics_on_a_library_sized_database() {
    let tables = ["releases", "tracks", "files", "artists"];
    let schema = tables
        .iter()
        .map(|table| {
            format!(
                "CREATE TABLE {table}(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, position INTEGER NOT NULL);
                 CREATE INDEX {table}_title ON {table}(title);
                 CREATE INDEX {table}_position ON {table}(position, title);"
            )
        })
        .collect::<String>();
    let db = TestDatabase::open(&schema);
    db.host(|writer| {
        for table in tables {
            writer
                .batch(&format!(
                    "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<20000)
                     INSERT INTO {table} SELECT printf('id-%06d',i), printf('title %05d', i % 4999), i % 97 FROM n"
                ))
                .unwrap();
        }
    });
    let path = db.path().to_owned();
    let _directory = db.close_keeping_file();
    // SQLite decides what to analyze from the statistics a connection loaded
    // as it read the schema, so each run starts on a fresh connection to the
    // closed database's file, as an open does.
    let fresh = || rusqlite::Connection::open(&path).unwrap();
    let timed = |sql: &str| {
        let connection = fresh();
        connection.execute_batch("SELECT 1 FROM sqlite_schema").unwrap();
        let started = std::time::Instant::now();
        connection.execute_batch(sql).unwrap();
        started.elapsed()
    };
    // A fresh database has no statistics tables yet, so each run starts from
    // none, as the first one does.
    let forget = || {
        fresh()
            .execute_batch("DROP TABLE IF EXISTS sqlite_stat1; DROP TABLE IF EXISTS sqlite_stat4")
            .unwrap()
    };
    forget();
    let first = timed("PRAGMA optimize=0x10012");
    let unchanged = timed("PRAGMA optimize=0x10012");
    forget();
    let whole = timed("PRAGMA analysis_limit=0; ANALYZE");
    for (metric, time) in [
        ("first_optimize", first),
        ("optimize_nothing_changed", unchanged),
        ("analyze_every_row", whole),
    ] {
        crate::report_time(&format!("statistics/{metric}"), time);
    }
    eprintln!(
        "first optimize {first:?}; again, nothing changed {unchanged:?}; ANALYZE of every row {whole:?}"
    );
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
    let db = Arc::new(TestDatabase::open_with(
        "CREATE TABLE blobs(id INTEGER PRIMARY KEY, body BLOB NOT NULL)",
        options(settings),
    ));
    let wal = {
        let mut path = db.path().to_owned().into_os_string();
        path.push("-wal");
        std::path::PathBuf::from(path)
    };
    let size = || std::fs::metadata(&wal).map_or(0, |m| m.len());
    const WHOLE: &str = "SELECT count(*),sum(length(body)) FROM blobs";
    let read = |sql: TestRead<'_>| -> Result<(i64, Option<i64>), QueryError> {
        Ok(sql.query_row(WHOLE, [], |r| Ok((r.get(0)?, r.get(1)?)))?)
    };
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = match reading {
        Reading::None => None,
        Reading::LiveQuery => {
            let mut query = db.subscribe(read);
            Some(tokio::spawn(async move { while let Some(Ok(_)) = query.next_inline().await {} }))
        }
        Reading::Unpaused => {
            let reading = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            Some(tokio::task::spawn_blocking(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    reading.read_app(read).unwrap();
                }
            }))
        }
    };
    let mut largest = 0;
    let started = std::time::Instant::now();
    for _ in 0..commits {
        db.write("INSERT INTO blobs(body) SELECT randomblob(4000) FROM (VALUES(1),(2),(3),(4))")
            .unwrap();
        largest = largest.max(size());
    }
    let elapsed = started.elapsed();
    let ended = size();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    if let Some(reader) = reader {
        reader.abort();
        drop(reader.await);
    }
    Arc::into_inner(db).expect("the readers let go of the database").close();
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
    for (name, settings) in [("default checkpoints", SqliteSettings::default()), ("none", off)] {
        for reading in [Reading::None, Reading::LiveQuery, Reading::Unpaused] {
            let (elapsed, largest, ended) = timed_writes(2000, reading, settings.clone()).await;
            let metric = format!("commits/{name}/{reading:?}");
            crate::report_time(&format!("{metric}/elapsed"), elapsed);
            crate::report_cost(
                &format!("{metric}/wal_largest_kb"),
                (largest / 1024) as f64,
                "count",
            );
            crate::report_cost(&format!("{metric}/wal_end_kb"), (ended / 1024) as f64, "count");
            eprintln!(
                "{name}, reading {reading:?}: 2,000 commits in {elapsed:?}, WAL at most {} KB, {} KB at the end",
                largest / 1024,
                ended / 1024
            );
        }
    }
}

/// Notes with an FTS5 index that a trigger keeps. FTS5 writes two rows of its
/// own into the index's data table as it creates the index.
const SEARCHED_NOTES: &str =
    "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
     CREATE VIRTUAL TABLE note_search USING fts5(title);
     CREATE TRIGGER notes_searched AFTER INSERT ON notes BEGIN
       INSERT INTO note_search(title) VALUES(NEW.title);
     END";

fn searched_notes() -> TestDatabase {
    TestDatabase::open_with(SEARCHED_NOTES, kept_statistics())
}

/// How the writer plans FTS5's cleanup of a merged segment's pages, the
/// statement it runs as nearly every write to the index commits. Defensive
/// mode refuses to prepare FTS5's own `DELETE` anywhere but inside FTS5, so
/// `index_cleanup_plan` plans a read of the same range, whose `WHERE` SQLite
/// plans alike.
fn index_cleanup_plan(db: &TestDatabase) -> String {
    db.host(|writer| {
        writer
            .query(
                "EXPLAIN QUERY PLAN SELECT id FROM note_search_data WHERE id>=?1 AND id<=?2",
                [1, 2],
                |r| r.get::<_, String>(3),
            )
            .unwrap()
            .join("; ")
    })
}

/// The rows `note_search_data` holds by the writer's statistics, if it has
/// any.
fn counted_index_pages(db: &TestDatabase) -> Option<i64> {
    db.host(|writer| {
        writer
            .query(
                "SELECT stat FROM sqlite_stat1 WHERE tbl='note_search_data' \
                 AND EXISTS(SELECT 1 FROM sqlite_schema WHERE name='sqlite_stat1')",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap_or_default()
            .first()
            .map(|stat| stat.split(' ').next().unwrap().parse().unwrap())
    })
}

/// Write `notes` searched notes, ten to a write, after the `from`th.
fn write_searched(db: &TestDatabase, from: usize, notes: usize) {
    for write in 0..notes / 10 {
        db.write_app(|sql| {
            for note in from + write * 10..from + (write + 1) * 10 {
                sql.app_execute(
                    "INSERT INTO notes VALUES (?1, ?2)",
                    (format!("note {note:05}"), format!("title {note} of the notes")),
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
}

/// A database's first open creates it, and it then fills. Statistics
/// gathered as it opens would say the FTS5 index's data table holds its two
/// rows, and SQLite would read the whole table to clean up each merged
/// segment instead of looking up the segment's range. sqlive keeps no
/// statistics for a table that small, as it opens or while the index grows,
/// so the cleanup stays a range lookup (§4).
#[test]
fn a_new_databases_index_cleanup_stays_a_range_lookup_as_it_fills() {
    let db = searched_notes();
    assert_eq!(counted_index_pages(&db), None);
    let range = "SEARCH note_search_data USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)";
    assert_eq!(index_cleanup_plan(&db), range);
    write_searched(&db, 0, 2000);
    assert_eq!(index_cleanup_plan(&db), range);
    db.close();
}

/// Ana created her library and closed it while its index held its two rows.
/// Closing keeps no statistics for a table that small, so when her next
/// session imports her notes, the cleanup looks up a range from the start
/// (§4).
#[test]
fn closing_a_small_database_keeps_no_statistics_for_its_small_tables() {
    let db = searched_notes();
    write_searched(&db, 0, 10);
    let db = db.reopen(kept_statistics());
    assert_eq!(counted_index_pages(&db), None);
    let range = "SEARCH note_search_data USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)";
    assert_eq!(index_cleanup_plan(&db), range);
    write_searched(&db, 10, 2000);
    assert_eq!(index_cleanup_plan(&db), range);
    db.close();
}

/// Defensive mode refuses SQL that could damage the database file, on every
/// connection sqlive opens. Ana's notes app keeps its search index through
/// `note_search`, as FTS5 does; writing the index's own `note_search_data`
/// table directly would leave the index disagreeing with itself, so it fails
/// in an app's write and in the host's own transaction alike (§4).
#[test]
fn sql_that_could_damage_the_file_fails() {
    let refused = |result: Result<(), TestError>| match result {
        Err(crate::Error::Sqlite(error)) => {
            assert!(error.to_string().contains("may not be modified"), "{error}")
        }
        result => panic!("expected SQLite to refuse, received {result:?}"),
    };
    let db = searched_notes();
    write_searched(&db, 0, 10);
    for statement in [
        "DELETE FROM note_search_data",
        "INSERT INTO note_search_data VALUES (99, x'00')",
        "UPDATE note_search_config SET v = 9",
    ] {
        refused(db.write(statement));
        refused(db.host(|writer| writer.transaction(|writer| writer.batch(statement))));
    }
    // Writing through `note_search` itself still works, as the trigger did.
    let found: i64 = db.host(|writer| {
        writer
            .query_row(
                "SELECT count(*) FROM note_search WHERE note_search MATCH 'notes'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    });
    assert_eq!(found, 10);
    db.close();
}
