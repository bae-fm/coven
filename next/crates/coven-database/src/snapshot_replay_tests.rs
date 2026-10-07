use crate::snapshot_write::tests::{frames, id};
use crate::tests::TestStore;
use crate::write::tests::{notes, records, sql, NOTES};
use crate::ApplyOutcome;
use coven_foundation::id_source::SequentialIds;
use coven_merge::Audience;
use std::io::Cursor;

#[tokio::test]
async fn waiting_changes_apply_when_the_history_missing_from_a_reload_arrives() {
    for uploaded in [false, true] {
        let ids = SequentialIds::new();
        let a_store = TestStore::with_ids(&ids);
        let b_store = TestStore::with_ids(&ids);
        let c_store = TestStore::with_ids(&ids);
        let a = a_store.schema(notes(), NOTES).await.unwrap();
        let b = b_store.schema(notes(), NOTES).await.unwrap();
        let c = c_store.schema(notes(), NOTES).await.unwrap();
        sql(&a, "INSERT INTO notes VALUES('n','first','')")
            .await
            .unwrap();
        let first = records(&a).remove(0);
        for db in [&b, &c] {
            db.apply_downloaded(first.clone().into()).await.unwrap();
        }
        let snapshot = frames(&a, Audience::Store).await.concat();
        sql(&a, "UPDATE notes SET title='second'").await.unwrap();
        let missing = records(&a).pop().unwrap();
        for db in [&b, &c] {
            db.apply_downloaded(missing.clone().into()).await.unwrap();
        }
        sql(&b, "UPDATE notes SET title='waiting'").await.unwrap();
        sql(&b, "UPDATE notes SET body='next waiting write'")
            .await
            .unwrap();
        let waiting = records(&b);
        for write in &waiting {
            c.apply_downloaded(write.clone().into()).await.unwrap();
        }
        b.load_snapshot(id(Audience::Store), Cursor::new(snapshot))
            .await
            .unwrap();
        assert_eq!(records(&b), waiting);
        assert_eq!(
            b.read(
                |db| Ok(db.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
            )
            .await
            .unwrap(),
            "first"
        );
        if uploaded {
            for write in &waiting {
                upload(&b, write).await;
            }
        }
        b.close().await.unwrap();
        let b = b_store.schema(notes(), NOTES).await.unwrap();
        let mut query = b.subscribe(|db| {
            Ok(db.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
        });
        assert_eq!(query.next().await.unwrap(), "first");
        b.inspect_writer(|db| db.batch("CREATE TEMP TRIGGER refuse_waiting AFTER UPDATE ON notes WHEN new.title='waiting' BEGIN SELECT RAISE(ABORT,'replay failed'); END").unwrap());
        let before = b.sync_state(vec![]).await.unwrap().positions;
        assert!(b.apply_downloaded(missing.clone().into()).await.is_err());
        assert_eq!(b.sync_state(vec![]).await.unwrap().positions, before);
        assert!(!query.is_marked_for_rerun());
        b.inspect_writer(|db| db.batch("DROP TRIGGER refuse_waiting").unwrap());
        assert_eq!(
            b.apply_downloaded(missing.into()).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(query.next().await.unwrap(), "waiting");
        assert!(!query.is_marked_for_rerun());
        assert_eq!(records(&b), if uploaded { vec![] } else { waiting });
        assert_eq!(crate::write::tests::count(&b, "coven_snapshot_waiting"), 0);
        assert_eq!(
            frames(&b, Audience::Store).await,
            frames(&c, Audience::Store).await
        );
        for db in [a, b, c] {
            db.close().await.unwrap();
        }
    }
}

async fn upload(db: &crate::Database, record: &coven_format::write::WriteRecord) {
    let encoder = coven_format::write_stream::WriteEncoder::new(record).unwrap();
    let length = coven_format::sealed_write::sealed_length(
        encoder.header_frame().len(),
        &encoder
            .header()
            .parts
            .iter()
            .map(|p| p.plaintext_length)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    db.keep_upload_sealed(record.header.position, vec![17; length as usize])
        .await
        .unwrap();
    assert!(db.upload_succeeded(record.header.position).await.unwrap());
}

#[tokio::test]
async fn waiting_changes_keep_their_file_bytes_until_their_past_arrives() {
    use crate::file_write::tests::{attach, owned_paths, tables, SCHEMA};
    let store = TestStore::new();
    let db = store
        .schema(tables(crate::Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"original".to_vec(), true).await.unwrap();
    let snapshot = frames(&db, Audience::Store).await.concat();
    sql(&db, "UPDATE files SET title='missing'").await.unwrap();
    let writes = records(&db);
    for write in &writes {
        upload(&db, write).await;
    }
    attach(&db, b"waiting bytes".to_vec(), false).await.unwrap();
    let waiting = records(&db).pop().unwrap();
    let paths = owned_paths(&store);
    let expected = frames(&db, Audience::Store).await;
    db.load_snapshot(id(Audience::Store), Cursor::new(snapshot))
        .await
        .unwrap();
    assert_eq!(owned_paths(&store), paths);
    upload(&db, &waiting).await;
    db.close().await.unwrap();
    let db = store
        .schema(tables(crate::Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    db.apply_downloaded(writes[1].clone().into()).await.unwrap();
    assert_eq!(frames(&db, Audience::Store).await, expected);
    assert_eq!(owned_paths(&store), paths);
    assert_eq!(std::fs::read(&paths[0]).unwrap(), b"waiting bytes");
    db.close().await.unwrap();
}
