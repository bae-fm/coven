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
        assert_eq!(writer.sqlite_settings().unwrap(), in_effect());
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
            journal_size_limit: JournalSizeLimit::Unlimited,
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
    database.inspect_writer(|writer| assert_eq!(writer.sqlite_settings().unwrap(), settings));
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
        &crate::Collations::default(),
    )
    .unwrap();
    assert!(matches!(sql.enable_wal(), Err(DbError::WalUnavailable { mode }) if mode == "memory"));
}
