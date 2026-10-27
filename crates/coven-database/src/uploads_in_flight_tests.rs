use crate::file_write::tests::{attach, tables, SCHEMA};
use crate::tests::TestStore;
use crate::{Migration, Provenance};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"take".to_vec(), true).await.unwrap();
    let file = db.file_ref("files", "7").await.unwrap().id();
    let mut counts = db.subscribe(|sql| {
        Ok(sql.query_row(COUNTS, [], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
        })?)
    });
    // A row read by its key reruns too: the change names no key.
    let mut row = db.subscribe(|sql| {
        Ok(sql.query_row(
            "SELECT sending FROM coven_uploads WHERE table_name='files' AND key=coven_key('7')",
            [],
            |r| r.get::<_, bool>(0),
        )?)
    });
    assert_eq!(next(&mut counts).await, (0, 1));
    assert!(!next(&mut row).await);
    let sending = db.uploads_in_flight().start(file);
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
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"take".to_vec(), true).await.unwrap();
    let file = db.file_ref("files", "7").await.unwrap().id();
    let runs = Arc::new(AtomicUsize::new(0));
    let counted = runs.clone();
    let mut queued = db.subscribe(move |sql| {
        counted.fetch_add(1, Ordering::SeqCst);
        Ok(sql.query_row(
            "SELECT count(*),max(position) FROM coven_uploads",
            [],
            |r| r.get::<_, i64>(0),
        )?)
    });
    assert_eq!(queued.next().await.unwrap().unwrap(), 1);
    let mut sending_list = db.subscribe(|sql| {
        Ok(sql.query("SELECT file FROM coven_sending", [], |r| {
            r.get::<_, String>(0)
        })?)
    });
    assert!(sending_list.next().await.unwrap().unwrap().is_empty());
    let _sending = db.uploads_in_flight().start(file);
    assert_eq!(
        sending_list.next().await.unwrap().unwrap(),
        [file.to_string()]
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), queued.next())
            .await
            .is_err()
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    db.close().await.unwrap();
}

/// A read-only handle, which sends nothing, reads every file as waiting.
#[tokio::test]
async fn a_read_only_handle_reads_every_file_as_waiting() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"take".to_vec(), true).await.unwrap();
    let file = db.file_ref("files", "7").await.unwrap().id();
    let _sending = db.uploads_in_flight().start(file);
    let reader = store
        .builder(
            tables(Provenance::AppProvided),
            vec![Migration::addition(1, "schema", SCHEMA)],
        )
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
async fn queued_tracks(files: usize) -> (TestStore, crate::Database, Vec<crate::Sending>) {
    use crate::{FileDecl, RowIdentity, SyncedTable};
    let store = TestStore::new();
    let db =
        store
            .schema(
                vec![SyncedTable::new("tracks", RowIdentity::SharedKey)
                    .carries_files(FileDecl::new("files", 1 << 30, Provenance::AppProvided))],
                "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, release_id TEXT NOT NULL,
                 size INTEGER, hash BLOB, location TEXT);
             CREATE INDEX tracks_release ON tracks(release_id);
             CREATE INDEX tracks_key ON tracks(coven_key(id));",
            )
            .await
            .unwrap();
    db.inspect_writer(|writer| {
        writer
            .batch(&format!(
                "BEGIN;
                 WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<{files})
                   INSERT INTO tracks(id,release_id) SELECT 't' || i, 'r' || ((i-1)/10+1) FROM n;
                 INSERT INTO _coven_file_uploads(id,reference,device,file,queued_at,pinned)
                   SELECT rowid,unhex(printf('%032x',rowid)),zeroblob(8),unhex(printf('%032x',rowid)),
                          zeroblob(13),0 FROM tracks;
                 INSERT INTO _coven_pending_attachments(upload,table_name,key,audience,generation,setters)
                   SELECT rowid,'tracks',coven_key(id),'store',zeroblob(8),x'' FROM tracks;
                 COMMIT;"
            ))
            .unwrap()
    });
    let sending = (1..=3)
        .map(|i| {
            let id = uuid::Uuid::from_u128(i * (files as u128 / 4));
            db.uploads_in_flight()
                .start(coven_foundation::id_source::FileId(id))
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
            (
                "sending column",
                "SELECT count(*) FROM coven_uploads WHERE sending",
            ),
            (
                "releases being sent",
                "SELECT count(DISTINCT t.release_id) FROM coven_sending AS s
                 JOIN tracks AS t ON s.table_name='tracks' AND s.key=coven_key(t.id)",
            ),
            (
                "one release's sending files",
                "SELECT count(*) FROM coven_sending AS s
                 JOIN tracks AS t ON s.table_name='tracks' AND s.key=coven_key(t.id)
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
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    for statement in [
        "SELECT _coven_sending_files()",
        "SELECT _coven_sending(zeroblob(16))",
        "SELECT value FROM json_each(_coven_sending_files())",
    ] {
        let error = db
            .read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                crate::CovenError::Database(crate::DbError::StatementForbidden { .. })
            ),
            "{statement}: {error:?}"
        );
    }
    db.close().await.unwrap();
    // Nor through a view of the app's own.
    let store = TestStore::new();
    let error = store
        .builder(
            tables(Provenance::AppProvided),
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
        matches!(
            crate::tests::database_error(error),
            crate::DbError::StatementForbidden { .. }
        ),
        "app view"
    );
}
