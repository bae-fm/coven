//! Completing an upload names its file in the row, or abandons a pending
//! attachment a newer change superseded (§17.2, §17.7).
use crate::{
    file_write::tests::{attach, tables},
    tests::TestStore,
    DbError, FileDatabase, FileRef, FileRefJournal, Provenance,
};

#[tokio::test]
async fn a_where_column_trigger_cannot_remove_the_row_its_completion_names_a_file_in() {
    let store = TestStore::new();
    let mut declarations = tables(Provenance::AppProvided);
    declarations[0] = declarations[0].clone().shared_trigger("remove_attached");
    let database=store.schema(declarations,"CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,title TEXT); CREATE TRIGGER remove_attached AFTER UPDATE OF location ON files WHEN NOT coven_applying() AND NEW.location LIKE 'file %' BEGIN DELETE FROM files WHERE id=NEW.id; END;").await.unwrap();
    // The completing write is the only write that sets the where-column; a
    // trigger removing the row refuses it, and the upload stays queued.
    attach(&database, b"source".to_vec(), true).await.unwrap();
    let pending = database.file_ref("files", "7").await.unwrap();
    let owner = FileDatabase::new(database.clone());
    let id = owner.uploads().await.unwrap()[0].id;
    assert!(matches!(
        owner.complete_upload(id, (), None).await,
        Err(DbError::FileRowRemoved { .. })
    ));
    assert_eq!(database.file_ref("files", "7").await.unwrap(), pending);
    assert_eq!(owner.uploads().await.unwrap().len(), 1);
    database.close().await.unwrap();
}

/// Row 7's where-column, as the app's SQL reads it.
async fn location(db: &crate::Database) -> Vec<Option<String>> {
    db.read(|sql| {
        Ok(
            sql.query("SELECT location FROM files WHERE id='7'", [], |r| {
                r.get::<_, Option<String>>(0)
            })?,
        )
    })
    .await
    .unwrap()
}

/// The file's fixed uploader and object id.
fn fixed(
    file: &FileRef,
) -> (
    coven_foundation::id_source::DeviceId,
    coven_foundation::id_source::FileId,
) {
    (file.uploader(), file.id())
}

/// §17.2: completion names a current pending attachment's file in its row,
/// with one write, and the same `FileRef` stays valid. A newer change to the
/// row's file abandons the attachment there and then; its completion
/// retires the upload with no write.
#[tokio::test]
async fn completion_names_a_current_attachment_and_retires_a_superseded_one() {
    use crate::file_write::tests::{local_count, owned_paths, SCHEMA};
    for change in ["none", "retarget", "delete", "edit"] {
        let store = TestStore::new();
        let db = store
            .schema(tables(Provenance::AppProvided), SCHEMA)
            .await
            .unwrap();
        let owner = FileDatabase::new(db.clone());
        attach(&db, b"first".to_vec(), true).await.unwrap();
        let first = db.file_ref("files", "7").await.unwrap();
        let first_id = owner.uploads().await.unwrap()[0].id;
        assert_eq!(first.reference().unwrap().device, first.uploader());
        // The row itself names no file until completion.
        assert_eq!(location(&db).await, vec![None]);
        assert!(owner.open_local(&first).await.unwrap().is_some());
        match change {
            "retarget" => attach(&db, b"second".to_vec(), false).await.unwrap(),
            "delete" => db
                .write(|sql| {
                    sql.execute("DELETE FROM files", [])?;
                    Ok(())
                })
                .await
                .unwrap(),
            "edit" => db
                .write(|sql| {
                    sql.execute("UPDATE files SET title='renamed'", [])?;
                    Ok(())
                })
                .await
                .unwrap(),
            _ => {}
        }
        let abandoned = db.abandoned_attachments().await.unwrap();
        let current = db.file_ref("files", "7").await.ok();
        // The queue's source facts outlive an abandonment until completion.
        assert!(owner.open_upload_source(&first).await.unwrap().is_some());
        if change == "retarget" {
            assert_eq!(owned_paths(&store).len(), 2);
        }
        let writes = db.test_queued_writes().await.unwrap().len();
        owner.complete_upload(first_id, (), None).await.unwrap();
        assert!(owner.open_upload_source(&first).await.unwrap().is_none());
        let queue = owner.uploads().await.unwrap();
        match change {
            "none" | "edit" => {
                assert!(abandoned.is_empty());
                assert_eq!(db.test_queued_writes().await.unwrap().len(), writes + 1);
                // The same FileRef names the stored file and reads remotely.
                assert_eq!(db.file_ref("files", "7").await.unwrap(), first);
                assert!(location(&db).await[0].is_some());
                owner.validate(&first).await.unwrap();
                assert!(owner.open_local(&first).await.unwrap().is_none());
                assert!(queue.is_empty());
                assert_eq!(local_count(&db, "_coven_device_files"), 0);
                assert!(owned_paths(&store).is_empty());
            }
            "retarget" => {
                let second = current.clone().unwrap();
                assert_ne!(fixed(&second), fixed(&first));
                assert_eq!(
                    abandoned
                        .iter()
                        .map(|a| (a.file, a.reason))
                        .collect::<Vec<_>>(),
                    [(first.id(), crate::AbandonReason::Replaced)]
                );
                assert_eq!(db.test_queued_writes().await.unwrap().len(), writes);
                assert_eq!(db.file_ref("files", "7").await.ok(), current);
                assert_eq!(queue.len(), 1);
                assert_eq!(queue[0].file, second);
                // The newer attachment keeps its own copy and hashes.
                assert!(owner.open_local(&second).await.unwrap().is_some());
                assert_eq!(local_count(&db, "_coven_device_files"), 1);
                assert_eq!(owned_paths(&store).len(), 1);
            }
            _ => {
                assert_eq!(
                    abandoned
                        .iter()
                        .map(|a| (a.file, a.reason))
                        .collect::<Vec<_>>(),
                    [(first.id(), crate::AbandonReason::RowGone)]
                );
                assert_eq!(db.test_queued_writes().await.unwrap().len(), writes);
                assert!(current.is_none());
                assert!(queue.is_empty());
                assert_eq!(local_count(&db, "_coven_file_chunks"), 0);
                assert!(owned_paths(&store).is_empty());
            }
        }
        // The retired path awaits a deletion decision unless a row names it.
        assert_eq!(
            local_count(&db, "_coven_file_candidates"),
            i64::from(change == "retarget" || change == "delete")
        );
        // Completing it again breaks the queue's own rule; nothing is damaged.
        assert!(matches!(
            owner.complete_upload(first_id, (), None).await,
            Err(DbError::Invariant { .. })
        ));
        // Dismissing the record forgets it.
        db.dismiss_abandoned_attachments(abandoned).await.unwrap();
        assert!(db.abandoned_attachments().await.unwrap().is_empty());
        db.close().await.unwrap();
    }
}

/// §17.2, Ana and Ben. Ana attaches photo A to row 7; Ben replaces row 7's
/// file with photo B. If Ana's device applies Ben's write before A's upload
/// completes, A is abandoned and B stays. If B arrives only after A's
/// completion, the two writes are concurrent: the later stamp, A's
/// completion, wins all four columns, and B stays visible as a lost value.
#[tokio::test]
async fn ana_and_ben_race_for_row_seven() {
    use crate::file_write::tests::{complete_uploads, SCHEMA};
    use crate::write::tests::records;
    use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
    use std::time::{Duration, UNIX_EPOCH};
    for seen in [true, false] {
        let ids = SequentialIds::new();
        let clock = std::sync::Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
        let open = |store: &TestStore| {
            store
                .builder(
                    tables(Provenance::AppProvided),
                    vec![crate::Migration::addition(1, "files", SCHEMA)],
                )
                .clock(clock.clone())
                .open()
        };
        let (ana_store, ben_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let ana = open(&ana_store).await.unwrap();
        let ben = open(&ben_store).await.unwrap();
        ana.write(|sql| {
            sql.execute("INSERT INTO files(id) VALUES('7')", [])?;
            Ok(())
        })
        .await
        .unwrap();
        for record in records(&ana) {
            ben.receive(record.into()).await.unwrap();
        }
        crate::tests::publish(&ana).await.unwrap();
        clock.set(UNIX_EPOCH + Duration::from_secs(2));
        attach(&ana, b"photo A".to_vec(), false).await.unwrap();
        let photo_a = ana.file_ref("files", "7").await.unwrap();
        clock.set(UNIX_EPOCH + Duration::from_secs(3));
        attach(&ben, b"photo B, larger".to_vec(), false)
            .await
            .unwrap();
        complete_uploads(&ben).await;
        let photo_b = ben.file_ref("files", "7").await.unwrap();
        let ben_location = location(&ben).await;
        let from_ben = records(&ben);
        if seen {
            for record in from_ben.clone() {
                ana.receive(record.into()).await.unwrap();
            }
            assert_eq!(
                ana.abandoned_attachments()
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|a| (a.file, a.reason))
                    .collect::<Vec<_>>(),
                [(photo_a.id(), crate::AbandonReason::Replaced)]
            );
        }
        clock.set(UNIX_EPOCH + Duration::from_secs(4));
        let writes = records(&ana).len();
        complete_uploads(&ana).await;
        if !seen {
            assert_eq!(records(&ana).len(), writes + 1);
            for record in from_ben {
                ana.receive(record.into()).await.unwrap();
            }
        }
        let current = ana.file_ref("files", "7").await.unwrap();
        let lost = ana.lost_values().await.unwrap();
        let lost_location = lost.iter().any(|value| match &value.lost {
            crate::Lost::Cell(cell) => {
                cell.column == "location"
                    && Some(&cell.value)
                        == ben_location[0]
                            .as_ref()
                            .map(|text| crate::types::Value::Text(text.clone()))
                            .as_ref()
            }
            crate::Lost::Row(_) => false,
        });
        if seen {
            // B stays; A's completion wrote nothing.
            assert_eq!(fixed(&current), fixed(&photo_b));
            assert_eq!(records(&ana).len(), writes);
            assert!(!lost_location);
        } else {
            // A's completion, stamped later, wins; B is a lost value.
            assert_eq!(fixed(&current), fixed(&photo_a));
            assert!(lost_location, "{lost:?}");
        }
        ana.close().await.unwrap();
        ben.close().await.unwrap();
    }
}
