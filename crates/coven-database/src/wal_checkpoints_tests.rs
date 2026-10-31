use crate::tests::TestStore;
use crate::{CheckpointMode, Migration, SqliteSettings, WalAutocheckpoint};
use std::num::NonZeroU32;
use std::time::Duration;

impl super::WalCheckpointer {
    /// Visit the checkpointer's connection, after any checkpoint in progress,
    /// unless it has stopped.
    pub(crate) fn with_connection(
        &self,
        visit: impl FnOnce(&crate::sqlite::DatabaseConnection) -> Result<(), crate::DbError>,
    ) -> Result<(), crate::DbError> {
        self.shared
            .connection
            .lock()
            .expect("checkpointer lock poisoned")
            .as_ref()
            .map_or(Ok(()), visit)
    }
}

fn blobs(store: &TestStore, settings: SqliteSettings) -> crate::DatabaseBuilder {
    store
        .builder(
            Vec::new(),
            vec![Migration::addition(
                1,
                "blobs",
                "CREATE TABLE blobs(id INTEGER PRIMARY KEY, body BLOB NOT NULL)",
            )],
        )
        .sqlite_settings(settings)
}

fn wal(store: &TestStore) -> std::path::PathBuf {
    let mut path = store.database_path().into_os_string();
    path.push("-wal");
    path.into()
}

async fn add(db: &crate::Database) {
    db.write(|sql| {
        sql.execute(
            "INSERT INTO blobs(body) SELECT randomblob(4000) FROM (VALUES(1),(2),(3),(4))",
            [],
        )?;
        Ok(())
    })
    .await
    .unwrap();
}

/// Frames the WAL holds now.
async fn frames(db: &crate::Database) -> u32 {
    db.checkpoint(CheckpointMode::Noop).await.unwrap().log_frames
}

/// The writer never checkpoints as it commits; coven's checkpointer does,
/// off the commit path, once the WAL passes the threshold, and the WAL then
/// starts over from its beginning, a live query rerunning or not (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coven_checkpoints_the_wal_past_its_threshold() {
    let store = TestStore::new();
    let settings = SqliteSettings {
        wal_autocheckpoint: Some(WalAutocheckpoint::Pages(NonZeroU32::new(100).unwrap())),
        ..SqliteSettings::default()
    };
    let db = blobs(&store, settings).open().await.unwrap();
    db.inspect_writer(|writer| {
        let pages: i64 = writer.query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0)).unwrap();
        assert_eq!(pages, 0, "the writer never checkpoints as it commits");
    });
    let mut query = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get::<_, i64>(0))?)
    });
    let reader = tokio::spawn(async move { while let Some(Ok(_)) = query.next().await {} });
    let mut largest = 0;
    for _ in 0..1000 {
        add(&db).await;
        largest = largest.max(frames(&db).await);
    }
    reader.abort();
    // 1,000 commits of about 5 frames each would be 5,000 frames without
    // it. Commits go on while each checkpoint runs, so the WAL passes the
    // threshold by what they add meanwhile.
    assert!(largest < 500, "the WAL grew to {largest} frames");
    db.close().await.unwrap();
}

/// With `Off`, coven never checkpoints: the WAL grows until the app does.
#[tokio::test]
async fn off_leaves_checkpoints_to_the_app() {
    let store = TestStore::new();
    let settings = SqliteSettings {
        wal_autocheckpoint: Some(WalAutocheckpoint::Off),
        ..SqliteSettings::default()
    };
    let db = blobs(&store, settings).open().await.unwrap();
    for _ in 0..100 {
        add(&db).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(frames(&db).await >= 400);
    let done = db.checkpoint(CheckpointMode::Truncate).await.unwrap();
    assert!(!done.busy);
    assert_eq!(frames(&db).await, 0);
    db.close().await.unwrap();
}

/// Closing the store still checkpoints the whole WAL and removes its file,
/// as the last connection to close does: the checkpointer's closes first.
#[tokio::test]
async fn closing_checkpoints_and_removes_the_wal() {
    let store = TestStore::new();
    let db = blobs(&store, SqliteSettings::default()).open().await.unwrap();
    for _ in 0..50 {
        add(&db).await;
    }
    assert!(wal(&store).exists());
    db.close().await.unwrap();
    assert!(!wal(&store).exists(), "closing removes the WAL");
    let db = blobs(&store, SqliteSettings::default()).open().await.unwrap();
    let rows: i64 = db
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(rows, 200);
    db.close().await.unwrap();
}
