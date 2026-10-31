use crate::snapshot_write::tests::{frames, id, SnapshotFrames};
use crate::tests::TestStore;
use crate::write::tests::{notes, sql, NOTES};
use crate::{Database, DbError, StagedSnapshot};
use coven_merge::Audience;
use std::io::Cursor;

async fn fixture(store: &TestStore) -> (Database, SnapshotFrames) {
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','kept','')").await.unwrap();
    let snapshot = frames(&db, Audience::Store).await;
    (db, snapshot)
}

async fn staged(db: &Database, snapshot: &SnapshotFrames) -> StagedSnapshot {
    crate::test_utils::stage_plaintext(
        db,
        id(Audience::Store),
        snapshot.prefix.clone(),
        Cursor::new(snapshot.concat()),
    )
    .await
    .unwrap()
}

fn reload(
    db: &Database,
    snapshots: &[&StagedSnapshot],
) -> crate::SnapshotReload<crate::StagedRecords, crate::DownloadedWriteStream<std::io::Empty>> {
    crate::SnapshotReload {
        target: snapshots[0].header().covers,
        snapshots: snapshots
            .iter()
            .map(|snapshot| crate::SnapshotSource::Stored {
                id: id(Audience::Store),
                input: snapshot.records(),
                author: crate::snapshot_write::tests::author(),
            })
            .collect(),
        slots: vec![],
        state: db.inspect_writer(|db| crate::store_state_tables::read(db).unwrap()),
        cutoffs: vec![],
        provider: Default::default(),
        boundaries: vec![],
        operation: None,
        authorship: std::sync::Arc::new(crate::test_utils::TestAuthorship),
    }
}

/// Loading reads a staged snapshot once and removes its records with the
/// load's own commit (§16.2).
#[tokio::test]
async fn a_load_removes_the_records_it_staged() {
    let store = TestStore::new();
    let (db, snapshot) = fixture(&store).await;
    let staged = staged(&db, &snapshot).await;
    assert!(db.staged_snapshot_records().await.unwrap() > 0);
    db.load_snapshots::<crate::pending::tests::Record, _>(reload(&db, &[&staged]), None)
        .await
        .unwrap();
    assert_eq!(db.staged_snapshot_records().await.unwrap(), 0);
    assert!(matches!(
        db.load_snapshots::<crate::pending::tests::Record, _>(reload(&db, &[&staged]), None).await,
        Err(DbError::Invariant { .. })
    ));
    staged.discard().await.unwrap();
    db.close().await.unwrap();
}

/// A failed load leaves the records for their owner, which discards them.
#[tokio::test]
async fn a_failed_load_leaves_the_records_for_their_owner_to_discard() {
    let store = TestStore::new();
    let (db, snapshot) = fixture(&store).await;
    let first = staged(&db, &snapshot).await;
    let second = staged(&db, &snapshot).await;
    let staged_records = db.staged_snapshot_records().await.unwrap();
    assert!(matches!(
        db.load_snapshots::<crate::pending::tests::Record, _>(
            reload(&db, &[&first, &second]),
            None
        )
        .await,
        Err(DbError::Snapshot(crate::SnapshotError::Inconsistent("reload repeats an audience")))
    ));
    assert_eq!(db.staged_snapshot_records().await.unwrap(), staged_records);
    first.discard().await.unwrap();
    second.discard().await.unwrap();
    assert_eq!(db.staged_snapshot_records().await.unwrap(), 0);
    db.close().await.unwrap();
}

/// A batch that fails its checks ends the staging: no later batch stages,
/// and discarding removes what earlier batches committed.
#[tokio::test]
async fn a_failed_batch_ends_its_staging() {
    let store = TestStore::new();
    let (db, snapshot) = fixture(&store).await;
    let bytes = snapshot.concat();
    let half = bytes.len() / 2;
    let mut staging =
        db.stage_snapshot(id(Audience::Store), snapshot.prefix.clone()).await.unwrap();
    staging.stage(bytes[..half].to_vec()).await.unwrap();
    assert!(db.staged_snapshot_records().await.unwrap() > 0);
    let mut damaged = bytes[half..].to_vec();
    damaged.iter_mut().for_each(|byte| *byte = !*byte);
    assert!(staging.stage(damaged).await.is_err());
    assert!(matches!(staging.stage(bytes[half..].to_vec()).await, Err(DbError::Invariant { .. })));
    staging.discard().await.unwrap();
    assert_eq!(db.staged_snapshot_records().await.unwrap(), 0);
    db.close().await.unwrap();
}

/// A staging its caller abandoned, as a cancelled restore does, removes its
/// records once its last owner drops.
#[tokio::test]
async fn an_abandoned_staging_removes_its_records() {
    let store = TestStore::new();
    let (db, snapshot) = fixture(&store).await;
    let staged = staged(&db, &snapshot).await;
    let records = staged.records();
    drop(staged);
    assert!(db.staged_snapshot_records().await.unwrap() > 0);
    drop(records);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while db.staged_snapshot_records().await.unwrap() > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the abandoned staging was removed");
    db.close().await.unwrap();
}

/// Records staged on one store's input connection aren't another's to load.
#[tokio::test]
async fn another_store_cannot_load_a_staged_snapshot() {
    let store = TestStore::new();
    let other = TestStore::new();
    let (db, snapshot) = fixture(&store).await;
    let (elsewhere, _) = fixture(&other).await;
    let staged = staged(&db, &snapshot).await;
    assert!(matches!(
        elsewhere
            .load_snapshots::<crate::pending::tests::Record, _>(
                reload(&elsewhere, &[&staged]),
                None
            )
            .await,
        Err(DbError::Invariant { .. })
    ));
    staged.discard().await.unwrap();
    assert_eq!(db.staged_snapshot_records().await.unwrap(), 0);
    db.close().await.unwrap();
    elsewhere.close().await.unwrap();
}
