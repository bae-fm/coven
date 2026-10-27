use super::*;
use crate::authorization::SqlAuthorization;
use crate::test_utils::{notes_migrations, notes_tables};
use crate::tests::TestStore;
use std::path::Path;

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

/// The notes schema and a local table whose declared `rowid` column hides its
/// address, which coven guards with a temporary trigger on every connection.
fn guarded_migrations() -> Vec<crate::Migration> {
    let mut migrations = notes_migrations();
    migrations.push(crate::Migration::addition(
        2,
        "shadowed",
        "CREATE TABLE shadowed(id TEXT NOT NULL PRIMARY KEY, rowid INTEGER)",
    ));
    migrations
}

/// A writable handle's writer never checkpoints as it commits: coven's
/// checkpointer does, at the size its readers report (§4).
fn never_as_it_commits(settings: SqliteSettingsInEffect) -> SqliteSettingsInEffect {
    SqliteSettingsInEffect {
        wal_autocheckpoint: WalAutocheckpoint::Off,
        ..settings
    }
}

/// Coven's settings that no app choice changes, on any connection (§4).
fn assert_fixed(connection: &DatabaseConnection) {
    let read = |pragma: &str| -> i64 {
        connection
            .query_row(&format!("PRAGMA {pragma}"), [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(read("foreign_keys"), 1);
    assert_eq!(read("recursive_triggers"), 1);
    assert_eq!(read("trusted_schema"), 0);
    let mode = |pragma: &str| -> String {
        connection
            .query_row(&format!("PRAGMA {pragma}"), [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(mode("journal_mode"), "wal");
    assert_eq!(mode("locking_mode"), "normal");
    // Changing temp_store drops temporary objects, so the settings precede
    // coven's temporary rowid guards rather than removing them.
    let guards: i64 = connection
        .query_row(
            "SELECT count(*) FROM temp.sqlite_schema WHERE name GLOB '_coven_rowid_guard_*'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(guards > 0);
}

#[tokio::test]
async fn every_connection_takes_the_apps_settings_and_keeps_covens() {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), guarded_migrations())
        .sqlite_settings(chosen())
        .open()
        .await
        .unwrap();
    assert_eq!(database.sqlite_settings().await.unwrap(), in_effect());
    database.inspect_writer(|writer| {
        assert_eq!(
            writer.sqlite_settings().unwrap(),
            never_as_it_commits(in_effect())
        );
        assert_fixed(writer);
        let synchronous: i64 = writer
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .unwrap();
        assert_eq!(synchronous, 2, "FULL");
    });
    database.inspect_readers(|reader| {
        assert_eq!(reader.sqlite_settings().unwrap(), in_effect());
        assert_fixed(reader);
    });

    let reading = store
        .builder(notes_tables(), guarded_migrations())
        .sqlite_settings(chosen())
        .open_read_only()
        .await
        .unwrap();
    assert_eq!(reading.sqlite_settings().await.unwrap(), in_effect());
    reading.inspect_writer(|writer| {
        assert_eq!(writer.sqlite_settings().unwrap(), in_effect());
        assert_fixed(writer);
    });
    reading.inspect_readers(|reader| {
        assert_eq!(reader.sqlite_settings().unwrap(), in_effect());
        assert_fixed(reader);
    });
    reading.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn unset_settings_keep_sqlites_defaults_but_covens_checkpoint_sync() {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let settings = database.sqlite_settings().await.unwrap();
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
            // Coven's own, which commits leave alone (§4).
            journal_size_limit: JournalSizeLimit::Bytes(4 << 20),
            wal_autocheckpoint: WalAutocheckpoint::Pages(NonZeroU32::new(1000).unwrap()),
            automatic_index: true,
            cell_size_check: false,
            secure_delete: SecureDelete::Off,
            fullfsync: false,
            // Coven's own on macOS and iOS (§3); SQLite's off elsewhere.
            checkpoint_fullfsync: cfg!(any(target_os = "macos", target_os = "ios")),
            analysis_limit: AnalysisLimit::Unlimited,
            page_size: PageSize::Bytes4096,
            auto_vacuum: AutoVacuum::Off,
        }
    );
    database.inspect_writer(|writer| {
        assert_eq!(
            writer.sqlite_settings().unwrap(),
            never_as_it_commits(settings.clone())
        )
    });
    database.inspect_readers(|reader| assert_eq!(reader.sqlite_settings().unwrap(), settings));

    let reading = store
        .builder(notes_tables(), notes_migrations())
        .open_read_only()
        .await
        .unwrap();
    assert_eq!(reading.sqlite_settings().await.unwrap(), settings);
    reading.inspect_writer(|writer| assert_eq!(writer.sqlite_settings().unwrap(), settings));
    reading.inspect_readers(|reader| assert_eq!(reader.sqlite_settings().unwrap(), settings));
    reading.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn the_apps_sync_choices_replace_covens() {
    let store = TestStore::new();
    let settings = || SqliteSettings {
        fullfsync: Some(true),
        checkpoint_fullfsync: Some(false),
        ..SqliteSettings::default()
    };
    let syncs = |connection: &DatabaseConnection| {
        let settings = connection.sqlite_settings().unwrap();
        (settings.fullfsync, settings.checkpoint_fullfsync)
    };
    let database = store
        .builder(notes_tables(), notes_migrations())
        .sqlite_settings(settings())
        .open()
        .await
        .unwrap();
    database.inspect_writer(|writer| assert_eq!(syncs(writer), (true, false)));
    database.inspect_readers(|reader| assert_eq!(syncs(reader), (true, false)));

    let reading = store
        .builder(notes_tables(), notes_migrations())
        .sqlite_settings(settings())
        .open_read_only()
        .await
        .unwrap();
    reading.inspect_writer(|writer| assert_eq!(syncs(writer), (true, false)));
    reading.inspect_readers(|reader| assert_eq!(syncs(reader), (true, false)));
    reading.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn a_store_keeps_the_page_size_and_auto_vacuum_it_was_created_with() {
    let store = TestStore::new();
    let open = |settings: SqliteSettings| {
        store
            .builder(notes_tables(), notes_migrations())
            .sqlite_settings(settings)
            .open()
    };
    let created = SqliteSettings {
        page_size: Some(PageSize::Bytes16384),
        auto_vacuum: Some(AutoVacuum::Full),
        ..SqliteSettings::default()
    };
    let database = open(created).await.unwrap();
    let settings = database.sqlite_settings().await.unwrap();
    assert_eq!(
        (settings.page_size, settings.auto_vacuum),
        (PageSize::Bytes16384, AutoVacuum::Full)
    );
    database.close().await.unwrap();

    let database = open(SqliteSettings {
        page_size: Some(PageSize::Bytes1024),
        auto_vacuum: Some(AutoVacuum::Incremental),
        ..SqliteSettings::default()
    })
    .await
    .unwrap();
    let settings = database.sqlite_settings().await.unwrap();
    assert_eq!(
        (settings.page_size, settings.auto_vacuum),
        (PageSize::Bytes16384, AutoVacuum::Full)
    );
    database.close().await.unwrap();
}

#[tokio::test]
async fn a_setting_sqlite_cannot_hold_fails_the_open() {
    let store = TestStore::new();
    let error = store
        .builder(notes_tables(), notes_migrations())
        .sqlite_settings(SqliteSettings {
            busy_timeout: Some(Duration::from_millis(i32::MAX as u64 + 1)),
            ..SqliteSettings::default()
        })
        .open()
        .await
        .err()
        .unwrap();
    assert!(matches!(
        crate::tests::database_error(error),
        DbError::SqliteSettingOutOfRange {
            setting: "busy_timeout",
            maximum,
        } if maximum == i32::MAX as u64
    ));
    store.assert_writer_unlocked();
}

/// The WAL file's length in bytes.
fn wal_bytes(store: &TestStore) -> u64 {
    let mut path = store.database_path().into_os_string();
    path.push("-wal");
    std::fs::metadata(path).unwrap().len()
}

async fn write_notes(database: &crate::Database, round: &str, count: usize) {
    for n in 0..count {
        let id = uuid::Uuid::new_v4().to_string();
        let title = format!("{round} {n}");
        database
            .write(move |sql| {
                sql.execute(
                    "INSERT INTO notes(id,title,body) VALUES (?1,?2,'')",
                    (id, title),
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }
}

fn manual_checkpoints() -> SqliteSettings {
    SqliteSettings {
        wal_autocheckpoint: Some(WalAutocheckpoint::Off),
        ..SqliteSettings::default()
    }
}

#[tokio::test]
async fn a_checkpoint_reports_sqlites_frame_counts() {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .sqlite_settings(manual_checkpoints())
        .open()
        .await
        .unwrap();
    write_notes(&database, "first", 10).await;
    let checkpoint = database.checkpoint(CheckpointMode::Passive).await.unwrap();
    assert!(!checkpoint.busy);
    assert!(checkpoint.log_frames > 0);
    assert_eq!(checkpoint.checkpointed_frames, checkpoint.log_frames);
    // A WAL file is a 32-byte header, then frames of a 24-byte header and a page.
    assert_eq!(
        wal_bytes(&store),
        32 + u64::from(checkpoint.log_frames) * (24 + 4096)
    );

    let checkpoint = database.checkpoint(CheckpointMode::Truncate).await.unwrap();
    assert_eq!(
        checkpoint,
        WalCheckpoint {
            busy: false,
            log_frames: 0,
            checkpointed_frames: 0
        }
    );
    assert_eq!(wal_bytes(&store), 0);
    database.close().await.unwrap();
}

#[tokio::test]
async fn after_a_restart_checkpoint_the_next_writes_reuse_the_wal() {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .sqlite_settings(manual_checkpoints())
        .open()
        .await
        .unwrap();
    write_notes(&database, "first", 20).await;
    // With no checkpoint, each write appends to the WAL.
    let written = wal_bytes(&store);
    write_notes(&database, "unchecked", 1).await;
    assert!(wal_bytes(&store) > written);

    let restart = database.checkpoint(CheckpointMode::Restart).await.unwrap();
    assert!(!restart.busy);
    assert_eq!(restart.checkpointed_frames, restart.log_frames);
    let restarted = wal_bytes(&store);
    write_notes(&database, "second", 5).await;
    let second = database.checkpoint(CheckpointMode::Passive).await.unwrap();
    assert_eq!(wal_bytes(&store), restarted, "the WAL did not grow");
    // The log holds only the writes since the restart.
    assert!(second.log_frames < restart.log_frames);
    database.close().await.unwrap();
}

#[tokio::test]
async fn a_restart_checkpoint_reports_busy_while_a_read_uses_the_wal() {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .sqlite_settings(SqliteSettings {
            busy_timeout: Some(Duration::from_millis(20)),
            ..manual_checkpoints()
        })
        .open()
        .await
        .unwrap();
    write_notes(&database, "first", 3).await;
    // The read waits on its blocking thread; the test awaits it starting.
    let (reading, read) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let reader = database.clone();
    let task = tokio::spawn(async move {
        reader
            .read(move |sql| {
                sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?;
                reading.send(()).unwrap();
                released.recv().unwrap();
                Ok(())
            })
            .await
    });
    read.await.unwrap();
    let busy = database.checkpoint(CheckpointMode::Restart).await.unwrap();
    assert!(busy.busy);
    assert!(busy.log_frames > 0);
    release.send(()).unwrap();
    task.await.unwrap().unwrap();
    let done = database.checkpoint(CheckpointMode::Restart).await.unwrap();
    assert!(!done.busy);
    database.close().await.unwrap();
}

#[tokio::test]
async fn an_incremental_vacuum_gives_back_free_pages() {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .sqlite_settings(SqliteSettings {
            auto_vacuum: Some(AutoVacuum::Incremental),
            ..SqliteSettings::default()
        })
        .open()
        .await
        .unwrap();
    let free = || {
        database.inspect_writer(|writer| {
            writer
                .query_row("PRAGMA freelist_count", [], |r| r.get::<_, i64>(0))
                .unwrap()
        })
    };
    database.inspect_writer(|writer| {
        writer
            .batch(
                "CREATE TABLE scratch(x); INSERT INTO scratch VALUES (zeroblob(100000)); DROP TABLE scratch;",
            )
            .unwrap()
    });
    let freed = free();
    assert!(freed > 2);
    database
        .incremental_vacuum(IncrementalVacuum::Pages(NonZeroU32::new(2).unwrap()))
        .await
        .unwrap();
    assert_eq!(free(), freed - 2);
    database
        .incremental_vacuum(IncrementalVacuum::All)
        .await
        .unwrap();
    assert_eq!(free(), 0);
    database.close().await.unwrap();
}

#[test]
fn wal_refusal_names_the_selected_journal_mode() {
    let sql = DatabaseConnection::open(
        Path::new(":memory:"),
        false,
        SqlAuthorization::new(&[]),
        &SqliteSettings::default(),
        &crate::SqlFunctions::default(),
    )
    .unwrap();
    assert!(matches!(sql.enable_wal(), Err(DbError::WalUnavailable { mode }) if mode == "memory"));
}

/// A synced table with an index the planner keeps statistics for.
fn indexed_notes(store: &TestStore) -> crate::DatabaseBuilder {
    store.builder(
        vec![crate::SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
        vec![crate::Migration::addition(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL); CREATE INDEX notes_title ON notes(title)",
        )],
    )
}

/// The rows the index `notes_title` holds by its statistics, read from the
/// closed store's file.
fn counted_titles(store: &TestStore) -> Option<i64> {
    let file = rusqlite::Connection::open(store.database_path()).unwrap();
    file.query_row(
        "SELECT stat FROM sqlite_stat1 WHERE idx='notes_title'",
        [],
        |r| r.get::<_, String>(0),
    )
    .ok()
    .map(|stat| stat.split(' ').next().unwrap().parse().unwrap())
}

/// The ANALYZE statements `PRAGMA optimize` would run on the closed store's
/// file now, without running them.
fn would_analyze(store: &TestStore) -> Vec<String> {
    let file = rusqlite::Connection::open(store.database_path()).unwrap();
    let mut statement = file.prepare("PRAGMA optimize=0x10013").unwrap();
    let rows = statement.query_map([], |r| r.get::<_, String>(0)).unwrap();
    rows.collect::<rusqlite::Result<_>>().unwrap()
}

/// Opening gathers statistics for a table that has none, after migrating
/// (§4).
#[tokio::test]
async fn opening_gathers_statistics_a_table_lacks() {
    let store = TestStore::new();
    let db = indexed_notes(&store).open().await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES ('1','a'),('2','b')")
        .await
        .unwrap();
    db.close().await.unwrap();
    rusqlite::Connection::open(store.database_path())
        .unwrap()
        .execute_batch("DELETE FROM sqlite_stat1 WHERE tbl='notes'")
        .unwrap();
    assert_eq!(counted_titles(&store), None);
    let db = indexed_notes(&store).open().await.unwrap();
    let counted: String = db.inspect_writer(|writer| {
        writer
            .query_row(
                "SELECT stat FROM sqlite_stat1 WHERE idx='notes_title'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    });
    assert_eq!(counted, "2 1");
    // `PRAGMA optimize` gathers no samples, so the planner never evaluates a
    // constant expression against them while it plans.
    let samples: i64 = db.inspect_writer(|writer| {
        writer
            .query_row("SELECT count(*) FROM sqlite_stat4", [], |r| r.get(0))
            .unwrap()
    });
    assert_eq!(samples, 0);
    db.close().await.unwrap();
}

/// What `PRAGMA optimize` would analyze on the closed store's file holds no
/// rows: SQLite records no statistics for an empty table, so it names those
/// each time, and reading them reads nothing.
fn assert_nothing_to_read(store: &TestStore) {
    let file = rusqlite::Connection::open(store.database_path()).unwrap();
    for statement in would_analyze(store) {
        let table = statement.strip_prefix("ANALYZE ").unwrap();
        let rows: i64 = file
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0, "{statement}");
    }
}

/// Closing gathers statistics again for a table whose size changed tenfold
/// during the session, and a second open of tables that kept their size has
/// none to gather.
#[tokio::test]
async fn closing_follows_the_sessions_writes_and_an_unchanged_store_analyzes_nothing() {
    let store = TestStore::new();
    let db = indexed_notes(&store).open().await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES ('1','a')")
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = indexed_notes(&store).open().await.unwrap();
    assert_eq!(counted_titles(&store), Some(1));
    db.write(|sql| {
        for id in 2..=200 {
            sql.execute(
                "INSERT INTO notes VALUES (?1, ?2)",
                (id.to_string(), format!("title {}", id % 7)),
            )?;
        }
        Ok(())
    })
    .await
    .unwrap();
    db.close().await.unwrap();
    assert_eq!(counted_titles(&store), Some(200));
    assert_nothing_to_read(&store);
    // Reopening finds every table's statistics current.
    let db = indexed_notes(&store).open().await.unwrap();
    db.close().await.unwrap();
    assert_eq!(counted_titles(&store), Some(200));
}

/// An app write can't run ANALYZE: a write's changes are captured, and SQLite's
/// session records statistics in a form coven doesn't read.
#[tokio::test]
async fn an_app_write_cant_analyze() {
    let store = TestStore::new();
    let db = indexed_notes(&store).open().await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES ('1','a'),('2','b')")
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = indexed_notes(&store).open().await.unwrap();
    // New rows, so analyzing in the write would change the statistics.
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES ('3','c'),('4','c')")
        .await
        .unwrap();
    match crate::write::tests::sql(&db, "ANALYZE notes").await {
        Err(DbError::StatementForbidden { operation }) => {
            assert_eq!(operation, "ANALYZE in a write")
        }
        result => panic!("expected the refusal, received {result:?}"),
    }
    db.close().await.unwrap();
}

/// What gathering statistics costs on a store of a music library's size:
/// four synced tables of 20,000 rows, each with its key and two indexes,
/// beside coven's own tables. Run explicitly.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn gathering_statistics_on_a_library_sized_store() {
    let store = TestStore::new();
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
    let schema: &'static str = Box::leak(schema.into_boxed_str());
    let open = || {
        store.builder(
            tables
                .iter()
                .map(|table| crate::SyncedTable::new(*table, crate::RowIdentity::SharedKey))
                .collect(),
            vec![crate::Migration::addition(1, "library", schema)],
        )
    };
    let db = open().open().await.unwrap();
    db.inspect_writer(|writer| {
        for table in tables {
            writer
                .batch(&format!(
                    "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<20000)
                     INSERT INTO {table} SELECT printf('id-%06d',i), printf('title %05d', i % 4999), i % 97 FROM n"
                ))
                .unwrap();
        }
    });
    db.close().await.unwrap();
    // SQLite decides what to analyze from the statistics a connection loaded
    // as it read the schema, so each run starts on a fresh connection to the
    // closed store's file, as an open does.
    let path = store.database_path();
    let fresh = || rusqlite::Connection::open(&path).unwrap();
    let timed = |sql: &str| {
        let connection = fresh();
        connection
            .execute_batch("SELECT 1 FROM sqlite_schema")
            .unwrap();
        let started = std::time::Instant::now();
        connection.execute_batch(sql).unwrap();
        started.elapsed()
    };
    let forget = || {
        fresh()
            .execute_batch("DELETE FROM sqlite_stat1; DELETE FROM sqlite_stat4")
            .unwrap()
    };
    forget();
    let first = timed("PRAGMA optimize=0x10012");
    let unchanged = timed("PRAGMA optimize=0x10012");
    forget();
    let whole = timed("PRAGMA analysis_limit=0; ANALYZE");
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
/// while `reading`; and how large the WAL file grew, and ended.
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
    let read = |sql: crate::SqlReadContext<'_>| -> crate::CovenResult<(i64, Option<i64>)> {
        Ok(sql.query_row(WHOLE, [], |r| Ok((r.get(0)?, r.get(1)?)))?)
    };
    let reader = match reading {
        Reading::None => None,
        Reading::LiveQuery => {
            let mut query = db.subscribe(read);
            Some(tokio::spawn(async move {
                while let Some(Ok(_)) = query.next().await {}
            }))
        }
        Reading::Unpaused => {
            let reading = db.clone();
            Some(tokio::spawn(async move {
                while reading.read(read).await.is_ok() {}
            }))
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
    for (name, settings) in [
        ("coven's checkpoints", SqliteSettings::default()),
        ("none", off),
    ] {
        for reading in [Reading::None, Reading::LiveQuery, Reading::Unpaused] {
            let (elapsed, largest, ended) = timed_writes(2000, reading, settings.clone()).await;
            eprintln!(
                "{name}, reading {reading:?}: 2,000 commits in {elapsed:?}, WAL at most {} KB, {} KB at the end",
                largest / 1024,
                ended / 1024
            );
        }
    }
}
