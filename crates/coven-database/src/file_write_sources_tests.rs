//! An original's record while its row is hidden, returns or is deleted
//! (§14.7, §16.5, `RetentionSafety.LocalSources`).
use crate::file_write::tests::{complete_uploads, local_count, owned_paths};
use crate::{tests::TestStore, *};
use coven_foundation::id_source::CircleId;
use serde_json::json;

const CIRCLE: &str = "00000000-0000-4000-8000-00000000000c";
const ROW: &str = "00000000-0000-4000-8000-000000000001";

/// A circle deletion hides rows without deleting them: their originals' records
/// and queued copies stay, the hidden files still upload, and a dropped
/// deletion returns them whole (§14.7, §16.5). A real row delete releases the
/// original record, as `original_size_is_checked_against_the_final_row_and_clearing_keeps_the_original` shows.
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

/// A store whose `originals` rows live in one circle, holding one row that
/// names a user-provided original, recorded on this device.
async fn original_in_circle() -> (TestStore, Database, CircleId, tempfile::NamedTempFile) {
    let circle = CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap());
    let store = TestStore::new();
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

/// `RetentionSafety.LocalSources`: after each event, whether this device
/// still records the original, against the model's implemented rule. The
/// model's stated rule differs in two places this test pins: a hidden row's
/// record outlives its deletion's finality, since nothing physically removes a
/// deleted circle's rows (`implemented_keeps_hidden_forever`), and a row
/// delete forgets it before the deleting write is final
/// (`implemented_releases_returning_row`).
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
        let (_store, db, circle, _original) = original_in_circle().await;
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
                    let after = coven_format::value::StorageTime::new(4_000_000_000, 0).unwrap();
                    db.advance_store_log_horizon(db.store_log().await.unwrap(), after, after)
                        .await
                        .unwrap();
                }
                "deleteRow" => db
                    .write(|sql| {
                        sql.execute(&format!("DELETE FROM originals WHERE id='{ROW}'"), [])?;
                        Ok(())
                    })
                    .await
                    .unwrap(),
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
    let model: Vec<Vec<serde_json::Value>> = serde_json::from_slice(&output.stdout).unwrap();
    let implemented: Vec<Vec<bool>> = model
        .iter()
        .map(|steps| {
            steps
                .iter()
                .map(|s| s["implemented"].as_bool().unwrap())
                .collect()
        })
        .collect();
    let stated: Vec<Vec<bool>> = model
        .iter()
        .map(|steps| {
            steps
                .iter()
                .map(|s| s["stated"].as_bool().unwrap())
                .collect()
        })
        .collect();
    assert_eq!(observed, implemented);
    // Where the stated rule differs: the final hidden row, and the deletes
    // before their write is final.
    assert_eq!(stated[0], [true, false]);
    assert_eq!(observed[0], [true, true]);
    assert_eq!(stated[3], [true]);
    assert_eq!(observed[3], [false]);
}
