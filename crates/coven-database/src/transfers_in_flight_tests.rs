use crate::file_write::tests::{complete_uploads, namespaces, open, stage, tables, SCHEMA};
use crate::tests::TestStore;
use crate::{Migration, Provenance};
use coven_foundation::id_source::{DeviceId, FileId};

/// Stage one file for row `7`: the file, and its upload's uploader and
/// stored object, which transfers in flight name.
async fn queued(db: &crate::Database) -> (FileId, DeviceId, FileId) {
    let file = stage(db, b"take".to_vec(), true).await.unwrap();
    let upload = crate::FileDatabase::new(db.clone()).uploads().await.unwrap().remove(0);
    (file, upload.location.uploader(), upload.location.object())
}
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Where an attempt starts, which these views don't show.
const PROGRESS: crate::TransferProgress =
    crate::TransferProgress { bytes_transferred: 0, bytes_total: 1 };

/// How many queued files there are, and how many of them this process is
/// sending: one live query over `coven_uploads` (E4).
const COUNTS: &str = "SELECT count(*) FILTER (WHERE sending),count(*) FROM coven_uploads";

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut crate::LiveQuery<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("the live query answers")
        .expect("the live query is open")
        .unwrap()
}

/// The `sending` column says which queued files this process is sending,
/// and a live query that reads it runs again as each upload starts or ends,
/// with no write: nothing about it is stored (§4).
#[tokio::test]
async fn a_live_query_of_sending_runs_again_as_an_upload_starts_or_ends() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let (file, uploader, object) = queued(&db).await;
    let mut counts = db.subscribe(|sql| {
        Ok(sql.query_row(COUNTS, [], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?)
    });
    // A row read by its key reruns too: the change names no key.
    let mut row = db.subscribe(|sql| {
        Ok(sql.query_row(
            "SELECT u.sending FROM coven_uploads AS u JOIN files AS f ON f.file=u.file WHERE f.id='7'",
            [],
            |r| r.get::<_, bool>(0),
        )?)
    });
    assert_eq!(next(&mut counts).await, (0, 1));
    assert!(!next(&mut row).await);
    let sending = db.uploads_in_flight().start(uploader, object, vec![file], PROGRESS);
    assert_eq!(next(&mut counts).await, (1, 1));
    assert!(next(&mut row).await);
    let listed = db
        .read(|sql| {
            Ok(sql.query("SELECT file FROM coven_sending", [], |r| {
                r.get::<_, coven_foundation::id_source::FileId>(0)
            })?)
        })
        .await
        .unwrap();
    assert_eq!(listed, [file]);
    drop(sending);
    assert_eq!(next(&mut counts).await, (0, 1));
    assert!(!next(&mut row).await);
    db.close().await.unwrap();
}

/// A live query that reads none of what is being sent doesn't run again
/// when an upload starts.
#[tokio::test]
async fn a_live_query_that_reads_no_sending_state_stays_idle() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let (file, uploader, object) = queued(&db).await;
    let runs = Arc::new(AtomicUsize::new(0));
    let counted = runs.clone();
    let mut queued = db.subscribe(move |sql| {
        counted.fetch_add(1, Ordering::SeqCst);
        Ok(sql.query_row("SELECT count(*),max(position) FROM coven_uploads", [], |r| {
            r.get::<_, i64>(0)
        })?)
    });
    assert_eq!(queued.next().await.unwrap().unwrap(), 1);
    let mut sending_list = db.subscribe(|sql| {
        Ok(sql.query("SELECT file FROM coven_sending", [], |r| r.get::<_, String>(0))?)
    });
    assert!(sending_list.next().await.unwrap().unwrap().is_empty());
    let _sending = db.uploads_in_flight().start(uploader, object, vec![file], PROGRESS);
    assert_eq!(sending_list.next().await.unwrap().unwrap(), [file.to_string()]);
    assert!(tokio::time::timeout(Duration::from_millis(100), queued.next()).await.is_err());
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    db.close().await.unwrap();
}

/// A read-only handle, which sends nothing, reads every file as waiting.
#[tokio::test]
async fn a_read_only_handle_reads_every_file_as_waiting() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let (file, uploader, object) = queued(&db).await;
    let _sending = db.uploads_in_flight().start(uploader, object, vec![file], PROGRESS);
    let reader = store
        .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
        .namespaces(namespaces(Provenance::AppProvided))
        .open_read_only()
        .await
        .unwrap();
    let counts = reader
        .read(|sql| {
            Ok(sql.query_row(
                "SELECT count(*) FILTER (WHERE sending),count(*),(SELECT count(*) FROM coven_sending)
                 FROM coven_uploads",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(counts, (0, 1, 0));
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

/// A music library's queue: `files` tracks of 10 per release, every one
/// queued, and three of them being sent.
async fn queued_tracks(files: usize) -> (TestStore, crate::Database, Vec<crate::Transferring>) {
    use crate::{FileColumn, RowIdentity, SyncedTable};
    let store = TestStore::new();
    let db = store
        .builder(
            vec![SyncedTable::new("tracks", RowIdentity::SharedKey)
                .file_column(FileColumn::new("file"))],
            vec![Migration::addition(
                1,
                "tracks",
                "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, release_id TEXT NOT NULL,
                 file TEXT);
                 CREATE INDEX tracks_release ON tracks(release_id);
                 CREATE INDEX tracks_file ON tracks(file);",
            )],
        )
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    db.inspect_writer(|writer| {
        writer
            .batch(&format!(
                "BEGIN;
                 WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<{files})
                   INSERT INTO tracks(id,release_id,file) SELECT 't' || i, 'r' || ((i-1)/10+1),
                       lower(printf('%08x-0000-4000-8000-%012x',i,i)) FROM n;
                 INSERT INTO _coven_file_uploads(id,location,device,file,size,hash,queued_at)
                   SELECT rowid,'file 0 ' || file || ' ' || hex(zeroblob(32)),zeroblob(8),
                          unhex(printf('%032x',rowid)),0,zeroblob(32),zeroblob(13) FROM tracks;
                 INSERT INTO _coven_upload_files(file,upload) SELECT file,rowid FROM tracks;
                 COMMIT;"
            ))
            .unwrap()
    });
    let sending = (1..=3)
        .map(|i| {
            let id = uuid::Uuid::from_u128(i * (files as u128 / 4));
            db.uploads_in_flight().start(DeviceId(0), FileId(id), Vec::new(), PROGRESS)
        })
        .collect();
    (store, db, sending)
}

/// What reading the queue's sending state costs at 10,000 and 20,000 queued
/// files: the whole queue counted, the `sending` column counted, and the
/// releases being sent, found from `coven_sending`. Run explicitly.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn reading_what_is_being_sent_costs() {
    for files in [10_000, 20_000] {
        let (_store, db, _sending) = queued_tracks(files).await;
        let mut timings = Vec::new();
        for (name, sql) in [
            ("whole queue", "SELECT count(*) FROM coven_uploads"),
            ("sending column", "SELECT count(*) FROM coven_uploads WHERE sending"),
            (
                "releases being sent",
                "SELECT count(DISTINCT t.release_id) FROM coven_sending AS s
                 JOIN tracks AS t ON t.file=s.file",
            ),
            (
                "one release's sending files",
                "SELECT count(*) FROM coven_sending AS s
                 JOIN tracks AS t ON t.file=s.file
                 WHERE t.release_id=(SELECT release_id FROM tracks
                                     WHERE rowid=(SELECT max(rowid)/4 FROM tracks))",
            ),
        ] {
            let mut best = Duration::MAX;
            let mut answer = 0;
            for _ in 0..5 {
                let started = std::time::Instant::now();
                answer = db
                    .read(move |read| Ok(read.query_row(sql, [], |r| r.get::<_, i64>(0))?))
                    .await
                    .unwrap();
                best = best.min(started.elapsed());
            }
            timings.push(format!("{name} {answer} in {best:?}"));
        }
        eprintln!("{files} queued: {}", timings.join("; "));
        db.close().await.unwrap();
    }
}

/// App SQL reads what is being sent only through coven's views, whose
/// columns tell a live query to run again as an upload starts or ends.
#[tokio::test]
async fn app_sql_reads_what_is_being_sent_only_through_the_views() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    for statement in [
        "SELECT _coven_sending_files()",
        "SELECT _coven_sending(zeroblob(16))",
        "SELECT value FROM json_each(_coven_sending_files())",
        "SELECT _coven_downloading_files()",
    ] {
        let error =
            db.read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?)).await.unwrap_err();
        assert!(
            matches!(error, crate::CovenError::Database(crate::DbError::StatementForbidden { .. })),
            "{statement}: {error:?}"
        );
    }
    db.close().await.unwrap();
    // Nor through a view of the app's own.
    let store = TestStore::new();
    let error = store
        .builder(
            tables(),
            vec![
                Migration::addition(1, "schema", SCHEMA),
                Migration::addition(
                    2,
                    "sending",
                    "CREATE VIEW mine AS SELECT _coven_sending_files() AS files;
                     CREATE TABLE copied(files TEXT); INSERT INTO copied SELECT files FROM mine",
                ),
            ],
        )
        .open()
        .await
        .err()
        .expect("an app view of what is being sent is refused");
    assert!(
        matches!(crate::tests::database_error(error), crate::DbError::StatementForbidden { .. }),
        "app view"
    );
}

/// A live query that reaches `coven_sending` only to count its rows, or to
/// ask whether it has any, reads none of its columns, yet follows each
/// upload as it starts and ends.
#[tokio::test]
async fn counting_the_files_being_sent_follows_each_start_and_end() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let (file, uploader, object) = queued(&db).await;
    let mut counted = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM coven_sending", [], |r| r.get::<_, i64>(0))?)
    });
    let mut any = db.subscribe(|sql| {
        Ok(sql
            .query_row("SELECT EXISTS(SELECT 1 FROM coven_sending)", [], |r| r.get::<_, bool>(0))?)
    });
    assert_eq!(next(&mut counted).await, 0);
    assert!(!next(&mut any).await);
    let sending = db.uploads_in_flight().start(uploader, object, vec![file], PROGRESS);
    assert_eq!(next(&mut counted).await, 1);
    assert!(next(&mut any).await);
    drop(sending);
    assert_eq!(next(&mut counted).await, 0);
    assert!(!next(&mut any).await);
    db.close().await.unwrap();
}

/// A count over a view of coven's tables follows them, as one that reads
/// the view's columns does: queueing a file reruns a count of the queue.
#[tokio::test]
async fn counting_a_view_follows_its_tables() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let mut queued = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM coven_uploads", [], |r| r.get::<_, i64>(0))?)
    });
    assert_eq!(next(&mut queued).await, 0);
    stage(&db, b"take".to_vec(), true).await.unwrap();
    assert_eq!(next(&mut queued).await, 1);
    db.close().await.unwrap();
}

/// `coven_downloading` lists each file over the stored bytes this process is
/// downloading to keep whole, and a live query of it runs again as the
/// download starts and ends. The same bytes kept twice at once stay listed
/// until both downloads end; bytes no file names list nothing.
#[tokio::test]
async fn a_live_query_of_downloading_runs_again_as_a_download_starts_or_ends() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let (file, uploader, object) = queued(&db).await;
    complete_uploads(&db).await;
    let mut rows = db.subscribe(|sql| {
        Ok(sql.query("SELECT file FROM coven_downloading", [], |r| r.get::<_, FileId>(0))?)
    });
    let mut sending = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM coven_sending", [], |r| r.get::<_, i64>(0))?)
    });
    assert!(next(&mut rows).await.is_empty());
    assert_eq!(next(&mut sending).await, 0);
    let downloads = db.downloads_in_flight();
    let first = downloads.start(uploader, object, vec![file], PROGRESS);
    assert_eq!(next(&mut rows).await, [file]);
    let second = downloads.start(uploader, object, vec![file], PROGRESS);
    drop(first);
    assert_eq!(downloads.current().into_iter().collect::<Vec<_>>(), [object]);
    drop(second);
    assert!(next(&mut rows).await.is_empty());
    let _unnamed = downloads.start(uploader, FileId(uuid::Uuid::nil()), Vec::new(), PROGRESS);
    let listed = db
        .read(|sql| {
            Ok(sql
                .query_row("SELECT count(*) FROM coven_downloading", [], |r| r.get::<_, i64>(0))?)
        })
        .await
        .unwrap();
    assert_eq!(listed, 0);
    // Downloading sends nothing.
    assert!(tokio::time::timeout(Duration::from_millis(100), sending.next()).await.is_err());
    db.close().await.unwrap();
}
