//! An original's record while its row is hidden, returns or is deleted
//! (§14.7, §16.5, `RetentionSafety.LocalSources`).
use crate::file_write::tests::{complete_uploads, local_count, owned_paths};
use crate::pending::tests::Record;
use crate::test_utils::published;
use crate::write::tests::records;
use crate::{tests::TestStore, *};
use coven_format::value::StorageTime;
use coven_foundation::id_source::{CircleId, SequentialIds};
use serde_json::json;

const CIRCLE: &str = "00000000-0000-4000-8000-00000000000c";
const ROW: &str = "00000000-0000-4000-8000-000000000001";

/// A circle deletion hides rows without deleting them: their originals' records
/// and queued copies stay, the hidden files still upload, and a dropped
/// deletion returns them whole (§14.7, §16.5). A real row delete releases the
/// original record once its verdict is final
/// (`a_received_delete_keeps_the_original_until_its_verdict_is_final`).
#[tokio::test]
async fn a_circle_deletion_keeps_sources_until_the_deletion_drops() {
    let circle = CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap());
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                SyncedTable::new("originals", RowIdentity::IndependentUuid)
                    .audience_column("audience")
                    .carries_files(FileDecl::new(
                        "originals",
                        Provenance::UserProvided,
                        CacheFill::CacheLazy,
                    )),
                SyncedTable::new("copies", RowIdentity::IndependentUuid)
                    .audience_column("audience")
                    .carries_files(FileDecl::new(
                        "copies",
                        Provenance::AppProvided,
                        CacheFill::CacheLazy,
                    )),
            ],
            "CREATE TABLE originals(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,audience TEXT NOT NULL);
             CREATE TABLE copies(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,audience TEXT NOT NULL)",
        )
        .await
        .unwrap();
    store.join_circle(&db, circle);
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"original").unwrap();
    let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
    db.write(move |sql| {
        sql.insert_user_file(
            "originals",
            ROW,
            prepared,
            &format!("INSERT INTO originals(id,size,audience) VALUES('{ROW}',8,'{CIRCLE}')"),
            &[],
        )
    })
    .await
    .unwrap();
    db.write_with_files::<_, _, _, crate::DbError>(
        |batch| {
            batch.put_file("copies", ROW, b"copy".to_vec());
            Ok(())
        },
        |sql| {
            sql.execute(
                &format!("INSERT INTO copies(id,size,audience) VALUES('{ROW}',4,'{CIRCLE}')"),
                [],
            )?;
            Ok(())
        },
    )
    .await
    .unwrap();
    let paths = owned_paths(&store);
    let recorded = db.user_file("originals", ROW).await.unwrap();
    assert!(recorded.is_some());
    crate::store_log::tests::delete_circle(&db, circle)
        .await
        .unwrap();
    assert_eq!(local_count(&db, "originals"), 0);
    assert_eq!(local_count(&db, "copies"), 0);
    // Hiding released nothing and paused nothing: both files stay queued
    // with their sources.
    assert_eq!(local_count(&db, "_coven_user_files"), 1);
    assert_eq!(local_count(&db, "_coven_device_files"), 1);
    assert_eq!(owned_paths(&store), paths);
    let queued = crate::FileDatabase::new(db.clone())
        .uploads()
        .await
        .unwrap();
    assert_eq!(queued.len(), 2);
    for upload in &queued {
        assert!(crate::FileDatabase::new(db.clone())
            .open_upload_source(&upload.file)
            .await
            .unwrap()
            .is_some());
    }
    // The hidden rows' files upload like any other; that releases only the
    // queue's own copy.
    complete_uploads(&db).await;
    assert_eq!(local_count(&db, "_coven_device_files"), 0);
    assert!(owned_paths(&store).is_empty());
    assert_eq!(local_count(&db, "_coven_user_files"), 1);
    crate::store_log::tests::drop_circle_deletion(&db, circle)
        .await
        .unwrap();
    assert_eq!(local_count(&db, "originals"), 1);
    assert_eq!(local_count(&db, "copies"), 1);
    assert_eq!(db.user_file("originals", ROW).await.unwrap(), recorded);
    assert_eq!(std::fs::read(original.path()).unwrap(), b"original");
    db.close().await.unwrap();
}

/// A device of the fixture store holding the `originals` table, drawing its
/// id from `ids`, and a member of `circle`.
async fn originals_device(ids: &SequentialIds, circle: CircleId) -> (TestStore, Database) {
    let store = TestStore::with_ids(ids);
    let db = store
        .schema(
            vec![SyncedTable::new("originals", RowIdentity::IndependentUuid)
                .audience_column("audience")
                .carries_files(FileDecl::new(
                    "originals",
                    Provenance::UserProvided,
                    CacheFill::CacheLazy,
                ))],
            "CREATE TABLE originals(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,audience TEXT NOT NULL)",
        )
        .await
        .unwrap();
    store.join_circle(&db, circle);
    (store, db)
}

/// A store whose `originals` rows live in one circle, holding one row that
/// names a user-provided original, recorded on this device.
async fn original_in_circle(
    ids: &SequentialIds,
) -> (TestStore, Database, CircleId, tempfile::NamedTempFile) {
    let circle = CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap());
    let (store, db) = originals_device(ids, circle).await;
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"original").unwrap();
    let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
    db.write(move |sql| {
        sql.insert_user_file(
            "originals",
            ROW,
            prepared,
            &format!("INSERT INTO originals(id,size,audience) VALUES('{ROW}',8,'{CIRCLE}')"),
            &[],
        )
    })
    .await
    .unwrap();
    (store, db, circle, original)
}

/// Another device of the circle receives the row from `db`, deletes it, and
/// `db` receives that delete as a write first published at `stored`: its own
/// window verdict, which a later retirement could still change, decides it
/// (§10). Returns the peer, whose store must outlive the call's use.
async fn receive_delete(
    db: &Database,
    ids: &SequentialIds,
    circle: CircleId,
    stored: StorageTime,
) -> (TestStore, Database) {
    let (store, peer) = originals_device(ids, circle).await;
    for write in records(db) {
        peer.apply_downloaded::<Record>(write.into(), published(StorageTime::UNIX_EPOCH))
            .await
            .unwrap();
    }
    peer.write(|sql| {
        sql.execute(&format!("DELETE FROM originals WHERE id='{ROW}'"), [])?;
        Ok(())
    })
    .await
    .unwrap();
    let delete = records(&peer).remove(0);
    db.apply_downloaded::<Record>(delete.into(), published(stored))
        .await
        .unwrap();
    assert_eq!(local_count(db, "originals"), 0);
    (store, peer)
}

/// The horizon every earlier entry and window verdict falls under (§9).
fn horizon() -> StorageTime {
    StorageTime::new(4_200_000_000, 0).unwrap()
}

async fn make_final(db: &Database) {
    db.advance_store_log_horizon(db.store_log().await.unwrap(), horizon(), horizon())
        .await
        .unwrap();
}

/// A hidden row keeps its original's record until its circle's deletion is
/// kept and final; it is then cleaned up and the record goes, while its
/// merge row and loss record stay for later writes (§14.7,
/// `hidden_released_when_final`).
#[tokio::test]
async fn a_final_circle_deletion_releases_its_rows_originals() {
    let ids = SequentialIds::new();
    let (_store, db, circle, original) = original_in_circle(&ids).await;
    crate::store_log::tests::delete_circle(&db, circle)
        .await
        .unwrap();
    assert_eq!(local_count(&db, "_coven_user_files"), 1);
    let lost = local_count(&db, "_coven_lost");
    let rows = local_count(&db, "_coven_rows");
    make_final(&db).await;
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    assert_eq!(local_count(&db, "_coven_lost"), lost);
    assert_eq!(local_count(&db, "_coven_rows"), rows);
    // Forgetting the record never touches the person's file.
    assert_eq!(std::fs::read(original.path()).unwrap(), b"original");
    db.close().await.unwrap();
}

/// A received delete keeps the original's record while its window verdict
/// can change, and releases it in the transaction that makes the verdict
/// final (§10, §16.5, `deleted_kept_until_final`,
/// `deleted_released_when_final`). This device's own delete is never
/// rejudged here, so it releases the record at once.
#[tokio::test]
async fn a_received_delete_keeps_the_original_until_its_verdict_is_final() {
    let ids = SequentialIds::new();
    let (_store, db, circle, _original) = original_in_circle(&ids).await;
    let _peer = receive_delete(
        &db,
        &ids,
        circle,
        StorageTime::new(4_100_000_000, 0).unwrap(),
    )
    .await;
    assert_eq!(local_count(&db, "_coven_user_files"), 1);
    make_final(&db).await;
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    db.close().await.unwrap();

    let (_store, own, _, _original) = original_in_circle(&ids).await;
    own.write(|sql| {
        sql.execute(&format!("DELETE FROM originals WHERE id='{ROW}'"), [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(local_count(&own, "_coven_user_files"), 0);
    own.close().await.unwrap();
}

/// `RetentionSafety.LocalSources`: after each event, whether this device
/// still records the original, against the model's stated rule (§14.7,
/// §16.5). The row delete is a received write: a delete published after a
/// final horizon has an open verdict again (`deleted_kept_until_final`).
#[tokio::test]
#[ignore = "requires COVEN_STORELOG_DATA_LEAN, supplied by scripts/check.sh"]
async fn data_differential_local_sources() {
    let runner = std::env::var_os("COVEN_STORELOG_DATA_LEAN").expect("built storelogDataRunner");
    let histories = [
        vec!["deleteCircle", "final"],
        vec!["deleteCircle", "dropDeletion"],
        vec!["deleteCircle", "dropDeletion", "final"],
        vec!["deleteRow"],
        vec!["deleteRow", "final"],
        vec!["final", "deleteRow"],
    ];
    let mut cases = Vec::new();
    let mut observed = Vec::new();
    for events in histories {
        let ids = SequentialIds::new();
        let (_store, db, circle, _original) = original_in_circle(&ids).await;
        let mut peers = Vec::new();
        let mut finalized = false;
        let mut recorded = Vec::new();
        for event in &events {
            match *event {
                "deleteCircle" => crate::store_log::tests::delete_circle(&db, circle)
                    .await
                    .unwrap(),
                "dropDeletion" => crate::store_log::tests::drop_circle_deletion(&db, circle)
                    .await
                    .unwrap(),
                "final" => {
                    make_final(&db).await;
                    finalized = true;
                }
                // Published within the horizon's settling window, or more
                // than thirty days past it once the horizon stands.
                "deleteRow" => peers.push(
                    receive_delete(
                        &db,
                        &ids,
                        circle,
                        StorageTime::new(
                            if finalized {
                                4_300_000_000
                            } else {
                                4_100_000_000
                            },
                            0,
                        )
                        .unwrap(),
                    )
                    .await,
                ),
                _ => unreachable!(),
            }
            // The record itself: `user_file` reads through the visible row,
            // which a deletion hides.
            recorded.push(local_count(&db, "_coven_user_files") == 1);
        }
        cases.push(json!({"events": events}));
        observed.push(recorded);
        db.close().await.unwrap();
    }
    let mut child = std::process::Command::new(runner)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    use std::io::Write;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&json!({"sources": cases})).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let model: Vec<Vec<bool>> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(observed, model);
}
