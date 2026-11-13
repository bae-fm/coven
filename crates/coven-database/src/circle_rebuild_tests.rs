use super::*;
use crate::raise_capture::tests::connect;
use crate::snapshot_write::tests::{author, frames, SnapshotFrames};
use crate::tests::TestStore;
use crate::write::tests::{count, sql};
use crate::{Migration, OldWrites, RowIdentity, SyncedTable};
use coven_foundation::files::FileArea;
use coven_foundation::id_source::{CircleId, SequentialIds};

const PINS: &str =
    "CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL, title TEXT NOT NULL)";

fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("pins", RowIdentity::IndependentUuid).audience_column("audience")]
}

pub(crate) fn garden() -> CircleId {
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
pub(crate) fn rebuild_folder(store: &TestStore) -> std::path::PathBuf {
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
    version_one_with(&SequentialIds::new()).await
}

async fn version_one_with(
    ids: &SequentialIds,
) -> (TestStore, Database, SnapshotFrames, SnapshotFrames) {
    let store = TestStore::with_ids(ids);
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
        .load::<crate::pending::tests::Record, std::io::Empty>(
            crate::SnapshotReload {
                snapshots,
                slots: vec![],
                target,
                state,
                provider_access: Vec::new(),
                cutoffs: vec![],
                boundaries: vec![],
                operation: None,
                rebuilds: Vec::new(),
                authorship: std::sync::Arc::new(crate::test_utils::TestAuthorship),
            },
            target,
        )
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

fn pin_ids(rebuild: &RebuildDatabase) -> Vec<String> {
    rebuild.database.as_ref().unwrap().inspect_writer(|db| {
        db.query("SELECT id FROM pins ORDER BY id", [], |r| r.get::<_, String>(0)).unwrap()
    })
}

/// A reload into the rebuild of the version one `snapshots` and `writes` as
/// slots after them, with the store state of `source`.
pub(crate) async fn reload_into(
    rebuild: &RebuildDatabase,
    source: &Database,
    snapshots: [&SnapshotFrames; 2],
    writes: &[coven_format::write::WriteRecord],
) -> crate::SnapshotReload<
    crate::SnapshotStaging,
    crate::DownloadedWriteStream<std::io::Cursor<Vec<u8>>>,
> {
    let scratch = rebuild.database.as_ref().unwrap();
    let inputs = snapshots
        .iter()
        .map(|snapshot| {
            let (id, prefix, input, author) = snapshot.input();
            (id, prefix, input, author)
        })
        .collect();
    let mut reload = crate::tests::reload(
        scratch,
        inputs,
        writes.iter().map(crate::snapshot_write::tests::stream).collect(),
        std::sync::Arc::new(crate::test_utils::TestAuthorship),
    )
    .await;
    reload.state = source.store_state().await.unwrap();
    reload
}

fn pin(position: u32, audience: &Audience) -> String {
    format!("INSERT INTO pins VALUES('00000000-0000-4000-8000-00000000000{position}','{audience}','later')")
}

#[tokio::test]
async fn a_rebuild_reads_the_store_only_through_its_input_slot() {
    let store = TestStore::new();
    let app = open_app(&store).await;
    let (_source_store, source, circle, store_frames) = version_one().await;
    let first = circle.prefix.covers.min(store_frames.prefix.covers);
    let mut writes = Vec::new();
    for (position, audience) in
        [(2, Audience::Store), (3, Audience::Store), (4, Audience::Circle(garden()))]
    {
        let statement = pin(position, &audience);
        source
            .write(move |context| {
                context.execute_batch(&statement)?;
                Ok(())
            })
            .await
            .unwrap();
        writes.push(crate::write::tests::records(&source).pop().unwrap());
    }
    let rebuild = app.open_rebuild(Audience::Circle(garden()), 1).await.unwrap();
    let reload = reload_into(&rebuild, &source, [&store_frames, &circle], &writes).await;
    // The store's input slot is the first of the three slots.
    rebuild.load::<crate::pending::tests::Record, _>(reload, first + 1).await.unwrap();
    let ids = pin_ids(&rebuild);
    assert_eq!(
        ids,
        [
            "00000000-0000-4000-8000-000000000001",
            "00000000-0000-4000-8000-000000000002",
            "00000000-0000-4000-8000-000000000004"
        ],
        "the store's second pin comes after its input slot and isn't read"
    );
    rebuild.discard().await.unwrap();
    source.close().await.unwrap();
    app.close().await.unwrap();
}

#[tokio::test]
async fn writes_above_the_rebuilds_version_are_refused_not_waited_for() {
    let store = TestStore::new();
    let app = open_app(&store).await;
    let (_source_store, source, circle, store_frames) = version_one().await;
    // A device on the newest app: its writes carry schema version 3.
    let newest_store = TestStore::new();
    let newest = open_app(&newest_store).await;
    newest_store.join_circle(&newest, garden());
    let mut writes = Vec::new();
    for audience in [Audience::Store, Audience::Circle(garden())] {
        let statement = format!(
            "INSERT INTO pins VALUES('00000000-0000-4000-8000-00000000000{}','{audience}','ahead')",
            writes.len() + 5
        );
        newest
            .write(move |context| {
                context.execute_batch(&statement)?;
                Ok(())
            })
            .await
            .unwrap();
        let write = crate::write::tests::records(&newest).pop().unwrap();
        assert_eq!(write.header.schema_version, 3);
        writes.push(write);
    }
    let rebuild = app.open_rebuild(Audience::Circle(garden()), 1).await.unwrap();
    let reload = reload_into(&rebuild, &source, [&store_frames, &circle], &writes).await;
    let through = reload.target;
    rebuild.load::<crate::pending::tests::Record, _>(reload, through).await.unwrap();
    assert_eq!(
        pin_ids(&rebuild),
        ["00000000-0000-4000-8000-000000000001"],
        "neither write's part applies in the rebuild"
    );
    rebuild.discard().await.unwrap();
    for db in [newest, source, app] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn the_rebuilds_files_count_in_the_storage_figures_while_it_lives() {
    let store = TestStore::new();
    let app = open_app(&store).await;
    let mut watch = app.subscribe_local_bytes();
    let before = watch.next().await.unwrap().unwrap().library;
    let rebuild = app.open_rebuild(Audience::Circle(garden()), 1).await.unwrap();
    let during = watch.next().await.unwrap().unwrap().library;
    assert!(during > before, "the rebuild's database is on disk: {during} against {before}");
    rebuild.discard().await.unwrap();
    let after = watch.next().await.unwrap().unwrap().library;
    assert!(after < during, "its bytes go with it: {after} against {during}");
    app.close().await.unwrap();
}

/// The writes of a device on the newest app (schema version 3) that has
/// joined Garden: its migration marker, then a write into each audience.
async fn newest_writes(
    ids: &SequentialIds,
    audience_note: &'static str,
) -> (TestStore, Database, Vec<coven_format::write::WriteRecord>) {
    let store = TestStore::with_ids(ids);
    let newest = open_app(&store).await;
    store.join_circle(&newest, garden());
    for (position, audience) in [(5, Audience::Store), (6, Audience::Circle(garden()))] {
        let statement = format!(
            "INSERT INTO pins VALUES('00000000-0000-4000-8000-00000000000{position}','{audience}','{audience_note}')"
        );
        newest
            .write(move |context| {
                context.execute_batch(&statement)?;
                Ok(())
            })
            .await
            .unwrap();
    }
    let writes = crate::write::tests::records(&newest);
    (store, newest, writes)
}

#[tokio::test]
async fn this_devices_migration_marker_is_consumed_without_waiting() {
    let ids = SequentialIds::new();
    use coven_format::write::WriteDisposition;
    let store = TestStore::with_ids(&ids);
    let app = open_app(&store).await;
    let (_source_store, source, circle, store_frames) = version_one_with(&ids).await;
    let (_newest_store, newest, writes) = newest_writes(&ids, "ahead").await;
    let marker: Vec<_> = writes
        .into_iter()
        .filter(|w| w.header.disposition == WriteDisposition::Migration)
        .collect();
    assert_eq!(marker.len(), 1, "the newest app's breaking migration queued a marker");
    assert_eq!(marker[0].header.schema_version, 3);
    let rebuild = app.open_rebuild(Audience::Circle(garden()), 1).await.unwrap();
    let reload = reload_into(&rebuild, &source, [&store_frames, &circle], &marker).await;
    let through = reload.target;
    rebuild.load::<crate::pending::tests::Record, _>(reload, through).await.unwrap();
    assert_eq!(pin_ids(&rebuild), ["00000000-0000-4000-8000-000000000001"]);
    rebuild.discard().await.unwrap();
    for db in [newest, source, app] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_write_ahead_of_its_audience_is_refused_and_the_writes_after_it_still_apply() {
    let ids = SequentialIds::new();
    for audience in [Audience::Store, Audience::Circle(garden())] {
        let store = TestStore::with_ids(&ids);
        let app = open_app(&store).await;
        let (_source_store, source, circle, store_frames) = version_one_with(&ids).await;
        let (_newest_store, newest, writes) = newest_writes(&ids, "ahead").await;
        let ahead = writes
            .into_iter()
            .find(|w| {
                w.header.disposition != coven_format::write::WriteDisposition::Migration
                    && w.parts.iter().any(|part| part.audience == audience)
            })
            .unwrap();
        let statement = pin(7, &audience);
        source
            .write(move |context| {
                context.execute_batch(&statement)?;
                Ok(())
            })
            .await
            .unwrap();
        let after = crate::write::tests::records(&source).pop().unwrap();
        let rebuild = app.open_rebuild(Audience::Circle(garden()), 1).await.unwrap();
        let reload =
            reload_into(&rebuild, &source, [&store_frames, &circle], &[ahead, after]).await;
        let through = reload.target;
        rebuild.load::<crate::pending::tests::Record, _>(reload, through).await.unwrap();
        assert_eq!(
            pin_ids(&rebuild),
            ["00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000007"],
            "{audience}: the ahead write's row is absent and the one after it applies"
        );
        rebuild.discard().await.unwrap();
        for db in [newest, source, app] {
            db.close().await.unwrap();
        }
    }
}
