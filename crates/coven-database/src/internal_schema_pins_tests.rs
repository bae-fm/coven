use crate::file_write::tests::{complete_uploads, local_count, namespaces, stage, tables};
use crate::tests::{database_error, TestStore};
use crate::{DbError, FileDatabase, Migration, Provenance, SchemaError};

/// The app's files table, and a local table its triggers keep: the rows
/// whose file this device has pinned.
const FOLLOWED: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,title TEXT);
     CREATE TABLE pinned_files(id TEXT NOT NULL PRIMARY KEY);
     CREATE TRIGGER file_pinned AFTER INSERT ON coven_pins BEGIN
         INSERT INTO pinned_files SELECT id FROM files WHERE file=NEW.file;
     END;
     CREATE TRIGGER file_unpinned AFTER DELETE ON coven_pins BEGIN
         DELETE FROM pinned_files WHERE id IN (SELECT id FROM files WHERE file=OLD.file);
     END;";

/// Whether `coven_pins` holds exactly what its source says.
fn agrees(db: &crate::Database) -> bool {
    db.inspect_writer(|writer| {
        writer
            .query_row(
                "SELECT NOT EXISTS(SELECT * FROM coven_pins EXCEPT SELECT * FROM _coven_pin_source)
                    AND NOT EXISTS(SELECT * FROM _coven_pin_source EXCEPT SELECT * FROM coven_pins)",
                [],
                |r| r.get(0),
            )
            .unwrap()
    })
}

async fn open(store: &TestStore, migrations: Vec<Migration>) -> crate::Database {
    store
        .builder(tables(), migrations)
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap()
}

/// Stage row 7's file and store it, then pin a whole copy of it.
async fn pin_stored(db: &crate::Database) -> crate::FileRef {
    let file = stage(db, b"take".to_vec(), true).await.unwrap();
    // Queued, the file is on this device only: it can't be pinned yet.
    let owner = FileDatabase::new(db.clone());
    assert!(owner.whole_kept(&db.file_ref(file).await.unwrap()).await.is_err());
    complete_uploads(db).await;
    let file = db.file_ref(file).await.unwrap();
    crate::database::file_database::tests::pin_whole(db, &file, "whole").await.unwrap();
    file
}

/// A local trigger on `coven_pins` keeps the app's own table of pinned
/// files, in the transaction that changes the pin, and a live query over
/// that table reruns with it (§9.4, §17.8). The unpin is a step of the
/// app's write, beside its own change to the row: the trigger follows it in
/// that write, the row and the pin change together, and a live query over
/// `coven_pins` runs once for the write. The namespace's budget then holds
/// nothing, so the same write evicts the copy, whose name's removal is
/// settled once it has committed.
#[tokio::test]
async fn a_local_trigger_follows_each_pin_as_it_changes() {
    let store = TestStore::new();
    let db = open(&store, vec![Migration::addition(1, "files", FOLLOWED)]).await;
    let mut pinned: crate::LiveQuery<_, DbError> = db.subscribe(|sql| {
        Ok(sql.query("SELECT id FROM pinned_files ORDER BY id", [], |r| r.get::<_, String>(0))?)
    });
    let mut pins: crate::LiveQuery<_, DbError> = db
        .subscribe(|sql| {
            Ok(sql.query("SELECT file FROM coven_pins", [], |r| r.get::<_, String>(0))?)
        })
        .process(Ok);
    assert!(pinned.next().await.unwrap().unwrap().is_empty());
    assert!(pins.next().await.unwrap().unwrap().is_empty());
    let file = pin_stored(&db).await;
    assert_eq!(pinned.next().await.unwrap().unwrap(), ["7"]);
    assert_eq!(pins.next().await.unwrap().unwrap(), [file.id().to_string()]);
    assert!(agrees(&db));
    let writes = pins.runs().writes();
    let id = file.id();
    assert_eq!(local_count(&db, "_coven_cache"), 1);
    db.write(|sql| sql.set_cache_budget("files", 0)).await.unwrap();
    db.write(move |sql| {
        sql.execute("UPDATE files SET title='kept elsewhere' WHERE id='7'", [])?;
        // The write's later SQL sees the step: the trigger already ran.
        assert_eq!(
            sql.query_row("SELECT count(*) FROM pinned_files", [], |r| r.get::<_, i64>(0))?,
            1
        );
        sql.unpin(&[id])?;
        assert_eq!(
            sql.query_row("SELECT count(*) FROM pinned_files", [], |r| r.get::<_, i64>(0))?,
            0
        );
        Ok(())
    })
    .await
    .unwrap();
    assert!(pinned.next().await.unwrap().unwrap().is_empty());
    assert!(pins.next().await.unwrap().unwrap().is_empty());
    assert_eq!(pins.runs().writes(), writes + 1);
    assert!(agrees(&db));
    assert_eq!(local_count(&db, "_coven_cache"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    let title = db
        .read(|sql| {
            Ok::<_, DbError>(
                sql.query_row("SELECT title FROM files WHERE id='7'", [], |r| {
                    r.get::<_, String>(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(title, "kept elsewhere");
    db.close().await.unwrap();
}

/// Evicting a pinned file is a step of the app's write too: its copy and
/// its pin go in that write, beside the app's change, which a local trigger
/// on `coven_pins` follows. A write that evicts and then fails keeps both.
#[tokio::test]
async fn an_eviction_goes_with_its_write() {
    let store = TestStore::new();
    let db = open(&store, vec![Migration::addition(1, "files", FOLLOWED)]).await;
    let id = pin_stored(&db).await.id();
    let failed = db
        .write(move |sql| {
            sql.evict_file(id)?;
            Err::<(), _>(DbError::Invariant { what: "the app gives up" })
        })
        .await;
    assert!(matches!(failed, Err(DbError::Invariant { .. })));
    assert_eq!(local_count(&db, "_coven_cache"), 1);
    db.write(move |sql| {
        sql.execute("UPDATE files SET title='not kept' WHERE id='7'", [])?;
        sql.evict_file(id)?;
        assert_eq!(
            sql.query_row("SELECT count(*) FROM pinned_files", [], |r| r.get::<_, i64>(0))?,
            0
        );
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(local_count(&db, "_coven_cache"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert_eq!(db.files_pinned(vec![id]).await.unwrap(), vec![Some(false)]);
    assert!(agrees(&db));
    db.close().await.unwrap();
}

/// A write that unpins a file and then fails keeps the pin: the step rolls
/// back with the app's statements.
#[tokio::test]
async fn an_unpin_rolls_back_with_its_write() {
    let store = TestStore::new();
    let db = open(&store, vec![Migration::addition(1, "files", FOLLOWED)]).await;
    let file = pin_stored(&db).await;
    let id = file.id();
    let failed = db
        .write(move |sql| {
            sql.unpin(&[id])?;
            sql.execute("UPDATE files SET title='never kept' WHERE id='7'", [])?;
            Err::<(), _>(DbError::Invariant { what: "the app gives up" })
        })
        .await;
    assert!(matches!(failed, Err(DbError::Invariant { .. })));
    assert_eq!(db.files_pinned(vec![id]).await.unwrap(), vec![Some(true)]);
    db.close().await.unwrap();
}

/// A migration leaves a pin that stayed the same alone: the app's triggers
/// see no change.
#[tokio::test]
async fn a_migration_leaves_each_unchanged_pin_alone() {
    let store = TestStore::new();
    let migrations = |extra: bool| {
        let mut migrations = vec![Migration::addition(
            1,
            "files",
            "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,title TEXT);
             CREATE TABLE pin_changes(id INTEGER PRIMARY KEY, kind TEXT NOT NULL);
             CREATE TRIGGER pin_added AFTER INSERT ON coven_pins BEGIN
                 INSERT INTO pin_changes(kind) VALUES('added');
             END;
             CREATE TRIGGER pin_removed AFTER DELETE ON coven_pins BEGIN
                 INSERT INTO pin_changes(kind) VALUES('removed');
             END;",
        )];
        if extra {
            migrations.push(Migration::addition(
                2,
                "notes",
                "ALTER TABLE files ADD COLUMN note TEXT",
            ));
        }
        migrations
    };
    let db = open(&store, migrations(false)).await;
    pin_stored(&db).await;
    async fn changes(db: &crate::Database) -> Result<Vec<String>, DbError> {
        db.read(|sql| {
            Ok(sql
                .query("SELECT kind FROM pin_changes ORDER BY id", [], |r| r.get::<_, String>(0))?)
        })
        .await
    }
    let pinned = changes(&db).await.unwrap();
    assert_eq!(pinned, ["added"]);
    db.close().await.unwrap();
    let db = open(&store, migrations(true)).await;
    assert_eq!(changes(&db).await.unwrap(), pinned);
    assert!(agrees(&db));
    db.close().await.unwrap();
}

/// A trigger on `coven_pins` is the app's local trigger: it runs after the
/// change, writes only local tables, itself or through the triggers it
/// fires, and reads and calls only what app SQL may. Coven refuses any
/// other as the migration declaring it ends, and the app still can't write
/// the table (§9.4).
#[tokio::test]
async fn coven_refuses_a_pin_trigger_that_could_break_its_rules() {
    const FILES: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,title TEXT)";
    let timing = || {
        DbError::Schema(SchemaError::TriggerTiming {
            trigger: "skip".into(),
            table: "coven_pins".into(),
        })
    };
    let target =
        |trigger: &str| DbError::TriggerTarget { trigger: trigger.into(), table: "files".into() };
    for (trigger, expected) in [
        (
            "CREATE TRIGGER skip BEFORE INSERT ON coven_pins BEGIN SELECT RAISE(IGNORE); END",
            timing(),
        ),
        // A trigger that names no time runs before its change.
        (
            "CREATE TRIGGER skip INSERT ON coven_pins BEGIN SELECT 1; END",
            timing(),
        ),
        (
            "CREATE TRIGGER title AFTER INSERT ON coven_pins BEGIN UPDATE files SET title='pinned'; END",
            target("title"),
        ),
        (
            "CREATE TABLE seen(file TEXT);
             CREATE TRIGGER seen AFTER DELETE ON coven_pins BEGIN INSERT INTO seen VALUES(OLD.file); END;
             CREATE TRIGGER titled AFTER INSERT ON seen BEGIN UPDATE files SET title='unpinned'; END",
            target("titled"),
        ),
        (
            "CREATE TRIGGER forget AFTER UPDATE ON coven_pins BEGIN DELETE FROM _coven_cache; END",
            DbError::InternalTable {
                table: "_coven_cache".into(),
            },
        ),
        // It reads only what app SQL may: coven's cache stays out of reach.
        (
            "CREATE TABLE cached(path TEXT);
             CREATE TRIGGER copy AFTER INSERT ON coven_pins BEGIN
                 INSERT INTO cached SELECT path FROM _coven_cache;
             END",
            DbError::InternalTable {
                table: "_coven_cache".into(),
            },
        ),
        (
            "CREATE TABLE sending(files TEXT);
             CREATE TRIGGER sent AFTER DELETE ON coven_pins BEGIN
                 INSERT INTO sending VALUES(_coven_sending_objects());
             END",
            DbError::StatementForbidden {
                operation: "reading what is being transferred outside coven_uploads, coven_sending and coven_downloading",
            },
        ),
    ] {
        let store = TestStore::new();
        let error = store
            .builder(
                tables(),
                vec![
                    Migration::addition(1, "files", FILES),
                    Migration::addition(2, "follow", trigger),
                ],
            )
            .open()
            .await
            .err()
            .expect("the trigger is refused");
        let error = database_error(error);
        assert_eq!(error.to_string(), expected.to_string(), "{trigger}");
    }
    let store = TestStore::new();
    let db = open(&store, vec![Migration::addition(1, "files", FOLLOWED)]).await;
    for statement in ["INSERT INTO coven_pins VALUES('file')", "DELETE FROM coven_pins"] {
        let error = crate::write::tests::sql(&db, statement).await.unwrap_err();
        assert!(
            matches!(&error, DbError::InternalTable { table } if table == "coven_pins"),
            "{statement}: {error:?}"
        );
    }
    db.close().await.unwrap();
}

/// 10,000 releases of 12 files, each release with its pinned files counted
/// by the app's local triggers.
const RELEASES: &str =
    "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, stored INTEGER NOT NULL);
     CREATE TABLE release_files(id TEXT NOT NULL PRIMARY KEY, release_id TEXT NOT NULL,
         file TEXT);
     CREATE INDEX release_files_release ON release_files(release_id);
     CREATE INDEX release_files_file ON release_files(file);
     CREATE TABLE release_pins(release_id TEXT NOT NULL PRIMARY KEY,
         files INTEGER NOT NULL, pinned INTEGER NOT NULL);
     CREATE INDEX release_pins_cloud_only ON release_pins(release_id)
         WHERE pinned<files OR files=0;
     CREATE TRIGGER release_added AFTER INSERT ON releases BEGIN
         INSERT INTO release_pins VALUES(NEW.id,0,0);
     END;
     CREATE TRIGGER release_file_added AFTER INSERT ON release_files BEGIN
         UPDATE release_pins SET files=files+1 WHERE release_id=NEW.release_id;
     END;
     CREATE TRIGGER release_file_pinned AFTER INSERT ON coven_pins BEGIN
         UPDATE release_pins SET pinned=pinned+1 WHERE release_id=
             (SELECT release_id FROM release_files WHERE file=NEW.file);
     END;
     CREATE TRIGGER release_file_unpinned AFTER DELETE ON coven_pins BEGIN
         UPDATE release_pins SET pinned=pinned-1 WHERE release_id=
             (SELECT release_id FROM release_files WHERE file=OLD.file);
     END;";

/// The store's rows and files, then its pins: every file but the first of
/// every tenth release. Without `follow`, with none of the triggers behind
/// `coven_pins`, coven's or the app's, as before coven kept the table.
async fn releases(
    follow: bool,
) -> (TestStore, crate::Database, std::time::Duration, std::time::Duration) {
    use crate::{FileColumn, RowIdentity, SyncedTable};
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                SyncedTable::new("releases", RowIdentity::SharedKey),
                SyncedTable::new("release_files", RowIdentity::SharedKey)
                    .file_column(FileColumn::new("file")),
            ],
            RELEASES,
        )
        .await
        .unwrap();
    if !follow {
        db.inspect_writer(|writer| {
            let triggers = writer
                .query(
                    "SELECT name FROM sqlite_schema WHERE type='trigger'
                     AND (name GLOB '_coven_pins_*' OR tbl_name='coven_pins')",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .unwrap();
            for trigger in triggers {
                writer.batch(&format!("DROP TRIGGER {trigger}")).unwrap();
            }
        });
    }
    let timed = |sql: &str| {
        let started = std::time::Instant::now();
        db.inspect_writer(|writer| writer.batch(sql).unwrap());
        started.elapsed()
    };
    let rows = timed(
        "BEGIN;
         WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000)
           INSERT INTO releases SELECT 'r' || i, 1 FROM n;
         WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<120000)
           INSERT INTO release_files(id,release_id,file) SELECT 'f' || i, 'r' || ((i-1)/12+1), 'file f' || i FROM n;
         INSERT INTO _coven_stored_files(file,namespace,device,object,object_path,hash)
           SELECT file, 'files', x'0000000000000001', randomblob(16),
                  'cache ' || id, zeroblob(32) FROM release_files;
         COMMIT;",
    );
    let pins = timed(
        "BEGIN;
         INSERT INTO _coven_cache(namespace,object_path,chunk,path,size,last_read,pinned,checked_hash)
           SELECT f.namespace, f.object_path, -2, f.object_path, 1, 1, 1, f.hash FROM _coven_stored_files AS f
           WHERE CAST(substr(f.object_path,8) AS INTEGER)%120<>1;
         COMMIT;",
    );
    (store, db, rows, pins)
}

/// What counting the releases whose files aren't all pinned costs: probing
/// `coven_pins` for every file, against reading the count the app's local
/// triggers keep per release; and what keeping the table and that count
/// costs as files and pins are written. Run explicitly.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn counting_unpinned_releases_costs() {
    let (_store, db, rows, pins) = releases(true).await;
    let (_bare_store, bare, bare_rows, bare_pins) = releases(false).await;
    let count = |db: &crate::Database, sql: String| {
        let db = db.clone();
        async move {
            let mut best = std::time::Duration::MAX;
            let mut answer = 0;
            for _ in 0..5 {
                let started = std::time::Instant::now();
                let sql = sql.clone();
                answer = db
                    .read(move |read| {
                        Ok::<_, DbError>(read.query_row(&sql, [], |r| r.get::<_, i64>(0))?)
                    })
                    .await
                    .unwrap();
                best = best.min(started.elapsed());
            }
            (answer, best)
        }
    };
    let probe = "SELECT count(*) FROM releases AS r WHERE r.stored=1
           AND (NOT EXISTS(SELECT 1 FROM release_files AS f WHERE f.release_id=r.id)
                OR EXISTS(SELECT 1 FROM release_files AS f WHERE f.release_id=r.id
                          AND NOT EXISTS(SELECT 1 FROM coven_pins AS p WHERE p.file=f.file)))";
    let (probed, probing) = count(&db, probe.into()).await;
    let (kept, keeping) = count(
        &db,
        "SELECT count(*) FROM release_pins AS p JOIN releases AS r ON r.id=p.release_id
         WHERE (p.pinned<p.files OR p.files=0) AND r.stored=1"
            .into(),
    )
    .await;
    // The view coven_pins was, computed for every probe.
    let source = probe.replace("coven_pins", "_coven_pin_source");
    let mut computing = std::time::Duration::MAX;
    let mut computed = 0;
    for _ in 0..5 {
        let started = std::time::Instant::now();
        computed =
            bare.inspect_writer(|writer| writer.query_row(&source, [], |r| r.get(0)).unwrap());
        computing = computing.min(started.elapsed());
    }
    assert_eq!((probed, kept, computed), (1000, 1000, 1000));
    assert!(agrees(&db));
    for (metric, time) in [
        ("write_120000_files", rows),
        ("write_120000_files_without_the_table", bare_rows),
        ("write_110000_pins", pins),
        ("write_110000_pins_without_the_table", bare_pins),
        ("probe_the_view", computing),
        ("probe_the_table", probing),
        ("read_the_apps_count", keeping),
    ] {
        crate::test_utils::report_time(&format!("unpinned_releases/{metric}"), time);
    }
    eprintln!(
        "120,000 files {rows:?}, against {bare_rows:?} without the table and the app's count; \
         110,000 pins {pins:?}, against {bare_pins:?}; probing the view {computing:?}, \
         probing the table {probing:?}, reading the app's count {keeping:?}"
    );
    db.close().await.unwrap();
    bare.close().await.unwrap();
}
