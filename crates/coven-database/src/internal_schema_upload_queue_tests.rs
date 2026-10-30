use crate::file_write::tests::{namespaces, stage, tables};
use crate::tests::{database_error, TestStore};
use crate::{DbError, FileDatabase, Migration, Provenance, SchemaError};

/// The app's files table, and a local table its triggers keep: how many of
/// its files wait in the queue, and the first one's place.
const FOLLOWED: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,title TEXT);
     CREATE TABLE queued(id INTEGER PRIMARY KEY CHECK(id=1), files INTEGER NOT NULL, first INTEGER);
     INSERT INTO queued VALUES(1,0,NULL);
     CREATE TRIGGER file_queued AFTER INSERT ON coven_upload_queue BEGIN
         UPDATE queued SET files=files+1, first=min(coalesce(first,NEW.position),NEW.position);
     END;
     CREATE TRIGGER file_left AFTER DELETE ON coven_upload_queue BEGIN
         UPDATE queued SET files=files-1,
             first=(SELECT min(position) FROM coven_upload_queue);
     END;";

async fn open(store: &TestStore, schema: &'static str) -> crate::Database {
    store
        .builder(tables(), vec![Migration::addition(1, "files", schema)])
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap()
}

/// Whether `coven_upload_queue` holds exactly the files `coven_uploads` lists.
fn agrees(db: &crate::Database) -> bool {
    db.inspect_writer(|writer| {
        writer
            .query_row(
                "SELECT NOT EXISTS(SELECT file,position FROM coven_upload_queue
                                   EXCEPT SELECT file,position FROM coven_uploads)
                    AND NOT EXISTS(SELECT file,position FROM coven_uploads
                                   EXCEPT SELECT file,position FROM coven_upload_queue)",
                [],
                |r| r.get(0),
            )
            .unwrap()
    })
}

/// A local trigger on `coven_upload_queue` keeps the app's own count of its
/// queued files, in the transaction that queues or retires each, and a live
/// query over that count reruns with it (§4, §9.4).
#[tokio::test]
async fn a_local_trigger_follows_each_file_into_and_out_of_the_queue() {
    let store = TestStore::new();
    let db = open(&store, FOLLOWED).await;
    let owner = FileDatabase::new(db.clone());
    let mut queued = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT files,first FROM queued", [], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, Option<i64>>(1)?))
        })?)
    });
    assert_eq!(queued.next().await.unwrap().unwrap(), (0, None));
    stage(&db, b"take".to_vec(), true).await.unwrap();
    let first = owner.uploads().await.unwrap()[0].id;
    assert_eq!(queued.next().await.unwrap().unwrap(), (1, Some(first)));
    assert!(agrees(&db));
    // A newer file for row 7 leaves the first with no reference, which
    // deletes its record and takes it out of the queue: one file is queued,
    // at the newer upload's place.
    stage(&db, b"retake".to_vec(), false).await.unwrap();
    let uploads = owner.uploads().await.unwrap();
    assert_eq!(uploads.len(), 1);
    let second = uploads[0].id;
    assert_ne!(second, first);
    assert_eq!(queued.next().await.unwrap().unwrap(), (1, Some(second)));
    assert!(agrees(&db));
    owner.complete_upload(second, ()).await.unwrap();
    assert_eq!(queued.next().await.unwrap().unwrap(), (0, None));
    assert!(agrees(&db));
    db.close().await.unwrap();
}

/// A trigger on `coven_upload_queue` is the app's local trigger, as one on
/// `coven_pins` is: it runs after the change and writes only local tables.
/// Coven refuses any other as the migration declaring it ends, and the app
/// still can't write the table (§9.4).
#[tokio::test]
async fn coven_refuses_a_queue_trigger_that_could_break_its_rules() {
    const FILES: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,title TEXT)";
    let timing = DbError::Schema(SchemaError::TriggerTiming {
        trigger: "skip".into(),
        table: "coven_upload_queue".into(),
    });
    for (trigger, expected) in [
        (
            "CREATE TRIGGER skip BEFORE INSERT ON coven_upload_queue BEGIN SELECT RAISE(IGNORE); END",
            timing,
        ),
        (
            "CREATE TRIGGER title AFTER INSERT ON coven_upload_queue BEGIN UPDATE files SET title='queued'; END",
            DbError::TriggerTarget {
                trigger: "title".into(),
                table: "files".into(),
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
    let db = open(&store, FOLLOWED).await;
    for statement in [
        "INSERT INTO coven_upload_queue VALUES('f',1)",
        "DELETE FROM coven_upload_queue",
    ] {
        let error = crate::write::tests::sql(&db, statement).await.unwrap_err();
        assert!(
            matches!(&error, DbError::InternalTable { table } if table == "coven_upload_queue"),
            "{statement}: {error:?}"
        );
    }
    db.close().await.unwrap();
}

/// A music library of `files` tracks, 10 per release, and the app's local
/// count of each release's queued tracks, first place and bytes, kept by
/// triggers on `coven_upload_queue`.
const LIBRARY: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, release_id TEXT NOT NULL,
         file TEXT, bytes INTEGER NOT NULL);
     CREATE INDEX tracks_release ON tracks(release_id);
     CREATE INDEX tracks_file ON tracks(file);
     CREATE TABLE release_queue(release_id TEXT NOT NULL PRIMARY KEY, queued INTEGER NOT NULL,
         first INTEGER, bytes INTEGER NOT NULL);
     CREATE INDEX release_queue_queued ON release_queue(queued) WHERE queued>0;
     CREATE TRIGGER track_queued AFTER INSERT ON coven_upload_queue BEGIN
         INSERT INTO release_queue(release_id,queued,first,bytes)
             SELECT release_id,1,NEW.position,bytes FROM tracks WHERE file=NEW.file
             ON CONFLICT(release_id) DO UPDATE SET queued=queued+1,
                 first=min(coalesce(first,excluded.first),excluded.first), bytes=bytes+excluded.bytes;
     END;
     CREATE TRIGGER track_left AFTER DELETE ON coven_upload_queue BEGIN
         UPDATE release_queue SET queued=queued-1,
             bytes=bytes-(SELECT bytes FROM tracks WHERE file=OLD.file),
             first=(SELECT min(q.position) FROM tracks AS t JOIN coven_upload_queue AS q
                    ON q.file=t.file
                    WHERE t.release_id=release_queue.release_id)
         WHERE release_id=(SELECT release_id FROM tracks WHERE file=OLD.file);
     END;";

/// The library's tracks, all queued, and how long queuing them took. Without
/// `follow`, with no trigger keeping `coven_upload_queue` or the app's count,
/// as before coven kept the table.
async fn library(files: usize, follow: bool) -> (TestStore, crate::Database, std::time::Duration) {
    use crate::{FileColumn, RowIdentity, SyncedTable};
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("tracks", RowIdentity::SharedKey)
                .file_column(FileColumn::new("file"))],
            LIBRARY,
        )
        .await
        .unwrap();
    db.inspect_writer(|writer| {
        writer
            .batch(&format!(
                "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<{files})
                   INSERT INTO tracks(id,release_id,file,bytes) SELECT 't' || i, 'r' || ((i-1)/10+1), 'file t' || i, i FROM n;"
            ))
            .unwrap();
        if !follow {
            let triggers = writer
                .query(
                    "SELECT name FROM sqlite_schema WHERE type='trigger'
                     AND (name GLOB '_coven_upload_queue_*' OR tbl_name='coven_upload_queue')",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .unwrap();
            for trigger in triggers {
                writer.batch(&format!("DROP TRIGGER {trigger}")).unwrap();
            }
        }
    });
    let started = std::time::Instant::now();
    db.inspect_writer(|writer| {
        writer
            .batch(
                "BEGIN;
                 INSERT INTO _coven_file_uploads(id,location,device,file,size,hash,queued_at)
                   SELECT rowid,'upload ' || rowid,zeroblob(8),unhex(printf('%032x',rowid)),bytes,
                          zeroblob(32),zeroblob(13) FROM tracks;
                 INSERT INTO _coven_upload_files(file,upload) SELECT file,rowid FROM tracks;
                 COMMIT;",
            )
            .unwrap()
    });
    (store, db, started.elapsed())
}

/// What reading the queue by release costs at 10,000 and 20,000 queued
/// files: through `coven_uploads`, through `coven_upload_queue`, and from
/// the app's count its triggers keep; and what keeping the table and that
/// count adds to queuing. Run explicitly.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn reading_the_queue_by_release_costs() {
    for files in [10_000, 20_000] {
        let (_store, db, queuing) = library(files, true).await;
        let (_bare_store, bare, bare_queuing) = library(files, false).await;
        assert!(agrees(&db));
        let mut timings = vec![format!(
            "queuing {queuing:?}, against {bare_queuing:?} without the table and the app's count"
        )];
        let by_release = "SELECT count(*),sum(queued),sum(first) FROM (
             SELECT t.release_id,count(*) AS queued,min(u.position) AS first
             FROM {queue} AS u JOIN tracks AS t ON u.file=t.file
             GROUP BY t.release_id)";
        for (name, sql) in [
            (
                "whole queue, view",
                "SELECT count(*),0,0 FROM coven_uploads".to_owned(),
            ),
            (
                "whole queue, table",
                "SELECT count(*),0,0 FROM coven_upload_queue".to_owned(),
            ),
            (
                "by release, view",
                by_release.replace("{queue}", "coven_uploads"),
            ),
            (
                "by release, table",
                by_release.replace("{queue}", "coven_upload_queue"),
            ),
            (
                "by release, app's count",
                "SELECT count(*),sum(queued),sum(first) FROM release_queue WHERE queued>0"
                    .to_owned(),
            ),
            (
                "pause",
                "SELECT paused,0,0 FROM coven_upload_pause".to_owned(),
            ),
            (
                "one release, app's count",
                "SELECT queued,first,bytes FROM release_queue WHERE release_id='r500'".to_owned(),
            ),
        ] {
            let mut best = std::time::Duration::MAX;
            let mut answer = (0, 0, 0);
            for _ in 0..5 {
                let started = std::time::Instant::now();
                let sql = sql.clone();
                answer = db
                    .read(move |read| {
                        Ok(read.query_row(&sql, [], |r| {
                            Ok((
                                r.get::<_, i64>(0)?,
                                r.get::<_, i64>(1)?,
                                r.get::<_, i64>(2)?,
                            ))
                        })?)
                    })
                    .await
                    .unwrap();
                best = best.min(started.elapsed());
            }
            timings.push(format!("{name} {answer:?} in {best:?}"));
        }
        eprintln!("{files} queued: {}", timings.join("; "));
        db.close().await.unwrap();
        bare.close().await.unwrap();
    }
}
