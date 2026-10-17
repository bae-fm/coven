//! An original's record while its row is hidden, returns or is deleted
//! (§15.9, §17.7, `RetentionSafety.LocalSources`).
use crate::file_write::tests::{complete_uploads, local_count, owned_paths};
use crate::write::tests::records;
use crate::{tests::TestStore, *};
use coven_foundation::id_source::{CircleId, SequentialIds};

const CIRCLE: &str = "00000000-0000-4000-8000-00000000000c";
const ROW: &str = "00000000-0000-4000-8000-000000000001";

/// Applied circle deletion releases originals while queued uploads keep their own sources.
#[tokio::test]
async fn a_circle_deletion_releases_originals_and_keeps_queued_sources() {
    let circle = CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap());
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                SyncedTable::new("originals", RowIdentity::IndependentUuid)
                    .audience_column("audience")
                    .carries_files(FileDecl::new(
                        "originals",
                        1 << 30,
                        Provenance::UserProvided,
                        CacheFill::CacheLazy,
                    )),
                SyncedTable::new("copies", RowIdentity::IndependentUuid)
                    .audience_column("audience")
                    .carries_files(FileDecl::new(
                        "copies",
                        1 << 30,
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
    crate::tests::delete_circle(&db, circle).await.unwrap();
    assert_eq!(local_count(&db, "originals"), 0);
    assert_eq!(local_count(&db, "copies"), 0);
    // Hiding released nothing and paused nothing: both files stay queued
    // with their sources.
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
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
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    assert_eq!(local_count(&db, "originals"), 0);
    assert_eq!(local_count(&db, "copies"), 0);
    assert_eq!(db.user_file("originals", ROW).await.unwrap(), None);
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
                    1 << 30,
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
/// (§14.1). Returns the peer, whose store must outlive the call's use.
async fn receive_delete(
    db: &Database,
    ids: &SequentialIds,
    circle: CircleId,
) -> (TestStore, Database) {
    let (store, peer) = originals_device(ids, circle).await;
    for write in records(db) {
        peer.receive(write.into()).await.unwrap();
    }
    crate::tests::publish(db).await.unwrap();
    peer.write(|sql| {
        sql.execute(&format!("DELETE FROM originals WHERE id='{ROW}'"), [])?;
        Ok(())
    })
    .await
    .unwrap();
    let delete = records(&peer).remove(0);
    db.receive(delete.into()).await.unwrap();
    assert_eq!(local_count(db, "originals"), 0);
    (store, peer)
}

/// Applied deletion preserves merge evidence and losses while releasing sources.
#[tokio::test]
async fn a_circle_deletion_preserves_merge_evidence_without_original_sources() {
    let ids = SequentialIds::new();
    let (_store, db, circle, original) = original_in_circle(&ids).await;
    crate::tests::delete_circle(&db, circle).await.unwrap();
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    let lost = local_count(&db, "_coven_lost");
    let rows = local_count(&db, "_coven_rows");
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    assert_eq!(local_count(&db, "_coven_lost"), lost);
    assert_eq!(local_count(&db, "_coven_rows"), rows);
    // Forgetting the record never touches the person's file.
    assert_eq!(std::fs::read(original.path()).unwrap(), b"original");
    db.close().await.unwrap();
}

/// Received deletion commits its source release with the row change.
#[tokio::test]
async fn received_and_counted_local_deletes_release_original_sources() {
    let ids = SequentialIds::new();
    let (_store, db, circle, _original) = original_in_circle(&ids).await;
    let _peer = receive_delete(&db, &ids, circle).await;
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    db.close().await.unwrap();

    let (_store, own, _, _original) = original_in_circle(&ids).await;
    crate::tests::publish(&own).await.unwrap();
    own.write(|sql| {
        sql.execute(&format!("DELETE FROM originals WHERE id='{ROW}'"), [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(local_count(&own, "_coven_user_files"), 1);
    crate::tests::publish(&own).await.unwrap();
    assert_eq!(local_count(&own, "_coven_user_files"), 0);
    own.close().await.unwrap();
}
