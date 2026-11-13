use super::*;
use crate::raise_capture::tests::connect;
use crate::snapshot_write::tests::{author, frames, SnapshotFrames};
use crate::tests::TestStore;
use crate::write::tests::{count, sql};
use crate::{Migration, OldWrites, RowIdentity, SyncedTable};
use coven_foundation::files::FileArea;
use coven_foundation::id_source::CircleId;

const PINS: &str =
    "CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL, title TEXT NOT NULL)";

fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("pins", RowIdentity::IndependentUuid).audience_column("audience")]
}

fn garden() -> CircleId {
    CircleId(uuid::Uuid::from_u128(11))
}

/// Version 1 makes the tables; 2 renames a column (breaking); 3 adds a table.
fn migrations(starts_from: Vec<SyncedTable>) -> Vec<Migration> {
    vec![
        Migration::addition(1, "pins", PINS),
        Migration::breaking(
            2,
            "rename",
            "ALTER TABLE pins RENAME COLUMN title TO name",
            OldWrites::Drop,
            starts_from,
        ),
        Migration::addition(3, "extra", "CREATE TABLE extra(id TEXT)"),
    ]
}

async fn open_app(store: &TestStore) -> Database {
    store.builder(tables(), migrations(tables())).open().await.unwrap()
}

/// The folder a rebuild's store takes in the real store's.
fn rebuild_folder(store: &TestStore) -> std::path::PathBuf {
    store.directory().area_path(FileArea::Cache).parent().unwrap().join("rebuild")
}

fn schema_of(rebuild: &RebuildDatabase, table: &'static str) -> String {
    rebuild.database.as_ref().unwrap().inspect_writer(|db| {
        db.query_row("SELECT sql FROM main.sqlite_schema WHERE name=?1", [table], |r| r.get(0))
            .unwrap()
    })
}

/// A device at version 1 holding a pin in Garden, and its snapshots.
async fn version_one() -> (TestStore, Database, SnapshotFrames, SnapshotFrames) {
    let store = TestStore::new();
    let db =
        store.builder(tables(), vec![Migration::addition(1, "pins", PINS)]).open().await.unwrap();
    connect(&db).await;
    store.join_circle(&db, garden());
    let statement = format!(
        "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','{}','song')",
        Audience::Circle(garden())
    );
    db.write(move |context| {
        context.execute_batch(&statement)?;
        Ok(())
    })
    .await
    .unwrap();
    let circle = frames(&db, Audience::Circle(garden())).await;
    let store_frames = crate::snapshot_write::tests::capture(
        &db,
        crate::snapshot_write::tests::id(Audience::Store),
    )
    .await;
    (store, db, circle, store_frames)
}

async fn stage(rebuild: &RebuildDatabase, snapshot: &SnapshotFrames) -> crate::SnapshotStaging {
    let (id, prefix, _, _) = snapshot.input();
    let mut staging = rebuild.stage_snapshot(id, prefix).await.unwrap();
    for chunk in snapshot.concat().chunks(coven_format::chunks::CHUNK_SIZE) {
        staging.stage(chunk.to_vec()).await.unwrap();
    }
    staging.finish().await.unwrap();
    staging
}

#[tokio::test]
async fn a_circle_at_version_one_opens_a_rebuild_at_that_version() {
    let store = TestStore::new();
    let app = open_app(&store).await;
    let rebuild = app.open_rebuild(Audience::Circle(garden()), 1).await.unwrap();
    assert!(schema_of(&rebuild, "pins").contains("title"), "the scratch is at version 1");
    assert!(rebuild.database.as_ref().unwrap().schema_version().await.unwrap() <= 1);
    assert!(rebuild_folder(&store).exists());
    // The real store reads and writes meanwhile.
    sql(&app, "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000002','store','real')")
        .await
        .unwrap();
    assert_eq!(count(&app, "pins"), 1);
    assert_eq!(app.schema_version().await.unwrap(), 3);
    rebuild.discard().await.unwrap();
    assert!(!rebuild_folder(&store).exists(), "discard leaves no folder");
    app.close().await.unwrap();
}

#[tokio::test]
async fn snapshots_and_slots_of_version_one_load_into_it() {
    let store = TestStore::new();
    let app = open_app(&store).await;
    let (_source_store, source, circle, store_frames) = version_one().await;
    let rebuild = app.open_rebuild(Audience::Circle(garden()), 1).await.unwrap();
    let state = source.store_state().await.unwrap();
    let target = circle.prefix.covers.max(store_frames.prefix.covers);
    let mut snapshots = Vec::new();
    for snapshot in [&store_frames, &circle] {
        let (id, _, _, _) = snapshot.input();
        snapshots.push(crate::SnapshotSource::Stored {
            id,
            input: stage(&rebuild, snapshot).await,
            author: author(),
        });
    }
    rebuild
        .load::<crate::pending::tests::Record, std::io::Empty>(crate::SnapshotReload {
            snapshots,
            slots: vec![],
            target,
            state,
            provider_access: Vec::new(),
            cutoffs: vec![],
            boundaries: vec![],
            operation: None,
            authorship: std::sync::Arc::new(crate::test_utils::TestAuthorship),
        })
        .await
        .unwrap();
    assert_eq!(count(rebuild.database.as_ref().unwrap(), "pins"), 1);
    rebuild.discard().await.unwrap();
    source.close().await.unwrap();
    app.close().await.unwrap();
}

#[tokio::test]
async fn a_circle_not_below_a_breaking_migration_has_nothing_to_rebuild() {
    let store = TestStore::new();
    let app = open_app(&store).await;
    for version in [2, 3] {
        match app.open_rebuild(Audience::Circle(garden()), version).await {
            Err(RebuildError::NotBelow { version: found, .. }) => assert_eq!(found, version),
            other => panic!("expected NotBelow, found {:?}", other.err()),
        }
    }
    assert!(!rebuild_folder(&store).exists());
    app.close().await.unwrap();
}

#[tokio::test]
async fn a_folder_a_crash_left_is_replaced() {
    let store = TestStore::new();
    let app = open_app(&store).await;
    let left = store.directory().rebuild_directory().await.unwrap();
    let file = left.area_path(FileArea::Cache).join("left");
    std::fs::write(&file, b"left").unwrap();
    let rebuild = app.open_rebuild(Audience::Circle(garden()), 1).await.unwrap();
    assert!(!file.exists());
    rebuild.discard().await.unwrap();
    app.close().await.unwrap();
}

#[tokio::test]
async fn a_starts_from_naming_a_missing_table_fails_the_open_and_leaves_no_folder() {
    let store = TestStore::new();
    open_app(&store).await.close().await.unwrap();
    // Opened again with every migration applied, so no check runs.
    let ghost = vec![SyncedTable::new("ghost", RowIdentity::SharedKey)];
    let app = store.builder(tables(), migrations(ghost)).open().await.unwrap();
    match app.open_rebuild(Audience::Circle(garden()), 1).await {
        Err(RebuildError::Open(_)) => (),
        other => panic!("expected Open, found {:?}", other.err()),
    }
    assert!(!rebuild_folder(&store).exists());
    app.close().await.unwrap();
}
