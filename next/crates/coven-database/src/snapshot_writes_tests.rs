use crate::snapshot_load::tests::contents;
use crate::snapshot_write::tests::{frames, id, load_one, stream};
use crate::tests::TestStore;
use crate::write::tests::{notes, records, sql, NOTES};
use coven_foundation::id_source::SequentialIds;
use coven_merge::Audience;
use std::io::Cursor;

#[tokio::test]
async fn waiting_changes_and_their_missing_history_commit_in_one_reload() {
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
    let original = contents(&b);
    let mut query = b.subscribe(|db| {
        Ok(db.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
    });
    assert_eq!(query.next().await.unwrap(), "waiting");
    assert!(matches!(
        load_one(&b, id(Audience::Store), Cursor::new(snapshot.clone())).await,
        Err(crate::DbError::Snapshot(
            crate::SnapshotError::MissingWrites { .. }
        ))
    ));
    assert_eq!(contents(&b), original);
    assert!(!query.is_marked_for_rerun());
    b.inspect_writer(|db| db.batch("CREATE TEMP TRIGGER refuse_waiting AFTER UPDATE ON notes WHEN new.title='waiting' BEGIN SELECT RAISE(ABORT,'replay failed'); END").unwrap());
    assert!(b
        .load_snapshots(crate::SnapshotReload::new(
            vec![(id(Audience::Store), Cursor::new(snapshot.clone()))],
            vec![stream(&missing)]
        ))
        .await
        .is_err());
    assert_eq!(contents(&b), original);
    assert!(!query.is_marked_for_rerun());
    b.inspect_writer(|db| db.batch("DROP TRIGGER refuse_waiting").unwrap());
    drop(query);
    for duplicate in [false, true] {
        let mut supplied = vec![stream(&missing)];
        if duplicate {
            supplied.push(stream(&waiting[0]));
        }
        b.load_snapshots(crate::SnapshotReload::new(
            vec![(id(Audience::Store), Cursor::new(snapshot.clone()))],
            supplied,
        ))
        .await
        .unwrap();
        assert_eq!(records(&b), waiting);
        assert_eq!(
            frames(&b, Audience::Store).await,
            frames(&c, Audience::Store).await
        );
    }
    for write in &waiting {
        upload(&b, write).await;
    }
    b.close().await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    assert_eq!(
        frames(&b, Audience::Store).await,
        frames(&c, Audience::Store).await
    );
    sql(&b, "UPDATE notes SET title='after reload'")
        .await
        .unwrap();
    let next = records(&b).pop().unwrap();
    assert_eq!(
        next.header.position.number,
        waiting.last().unwrap().header.position.number + 1
    );
    c.apply_downloaded(next.into()).await.unwrap();
    assert_eq!(
        frames(&b, Audience::Store).await,
        frames(&c, Audience::Store).await
    );
    for db in [a, b, c] {
        db.close().await.unwrap();
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
    crate::upload::tests::attempt(db, vec![17; length as usize])
        .await
        .unwrap();
    assert!(db.upload_succeeded(record.header.position).await.unwrap());
}

#[tokio::test]
async fn waiting_files_survive_intermediate_snapshot_and_gap_write_states() {
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
    let before = contents(&db);
    assert!(
        load_one(&db, id(Audience::Store), Cursor::new(snapshot.clone()))
            .await
            .is_err()
    );
    assert_eq!(contents(&db), before);
    assert_eq!(owned_paths(&store), paths);
    db.load_snapshots(crate::SnapshotReload::new(
        vec![(id(Audience::Store), Cursor::new(snapshot))],
        vec![stream(&writes[1])],
    ))
    .await
    .unwrap();
    assert_eq!(frames(&db, Audience::Store).await, expected);
    assert_eq!(owned_paths(&store), paths);
    assert_eq!(std::fs::read(&paths[0]).unwrap(), b"waiting bytes");
    upload(&db, &waiting).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn supplied_write_streams_are_bounded_and_fail_atomically() {
    use crate::{DownloadedPartStream, DownloadedWriteStream};
    use std::io::{Read, Seek, Write};
    struct BoundedFile(std::fs::File);
    impl Read for BoundedFile {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            assert!(buffer.len() <= coven_format::chunks::CHUNK_SIZE);
            let length = buffer.len().min(701);
            self.0.read(&mut buffer[..length])
        }
    }
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','before','')")
        .await
        .unwrap();
    let snapshot = frames(&a, Audience::Store).await.concat();
    a.write(|db| {
        db.execute("UPDATE notes SET body=?1", ["x".repeat(200_000)])?;
        Ok(())
    })
    .await
    .unwrap();
    let missing = records(&a).pop().unwrap();
    b.apply_downloaded(records(&a)[0].clone().into())
        .await
        .unwrap();
    b.apply_downloaded(missing.clone().into()).await.unwrap();
    sql(&b, "UPDATE notes SET title='waiting'").await.unwrap();
    let before = contents(&b);
    let input = stream(&missing);
    let mut bytes = match input.parts.into_iter().next().unwrap() {
        DownloadedPartStream::Opened(bytes) => bytes.into_inner(),
        DownloadedPartStream::Skipped => panic!("opened test write"),
    };
    let original = bytes.clone();
    for damage in 0..3 {
        bytes = original.clone();
        match damage {
            0 => {
                bytes.truncate(bytes.len() - 1);
            }
            1 => {
                bytes.push(0);
            }
            _ => {
                bytes[0] ^= 0xff;
            }
        }
        assert!(b
            .load_snapshots(crate::SnapshotReload::new(
                vec![(id(Audience::Store), Cursor::new(snapshot.clone()))],
                vec![DownloadedWriteStream {
                    header: input.header.clone(),
                    parts: vec![DownloadedPartStream::Opened(Cursor::new(bytes.clone()))],
                }]
            ))
            .await
            .is_err());
        assert_eq!(contents(&b), before);
    }
    assert!(b
        .load_snapshots(crate::SnapshotReload::new(
            vec![(id(Audience::Store), Cursor::new(snapshot.clone()))],
            vec![DownloadedWriteStream::<Cursor<Vec<u8>>> {
                header: input.header.clone(),
                parts: vec![DownloadedPartStream::Skipped],
            }]
        ))
        .await
        .is_err());
    assert_eq!(contents(&b), before);
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&original).unwrap();
    file.rewind().unwrap();
    b.load_snapshots(crate::SnapshotReload::new(
        vec![(id(Audience::Store), Cursor::new(snapshot))],
        vec![DownloadedWriteStream {
            header: input.header,
            parts: vec![DownloadedPartStream::Opened(BoundedFile(file))],
        }],
    ))
    .await
    .unwrap();
    a.apply_downloaded(records(&b)[0].clone().into())
        .await
        .unwrap();
    assert_eq!(
        frames(&b, Audience::Store).await,
        frames(&a, Audience::Store).await
    );
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn snapshots_with_the_same_key_in_different_audiences_recompute_together() {
    use crate::{RowIdentity, SyncedTable};
    use coven_foundation::id_source::CircleId;
    let tables = || {
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")]
    };
    const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL, title TEXT NOT NULL)";
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let c_store = TestStore::with_ids(&ids);
    let a = a_store.schema(tables(), SCHEMA).await.unwrap();
    let b = b_store.schema(tables(), SCHEMA).await.unwrap();
    let c = c_store.schema(tables(), SCHEMA).await.unwrap();
    let circle = Audience::Circle(CircleId(uuid::Uuid::from_u128(10)));
    sql(
        &a,
        "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','store','store')",
    )
    .await
    .unwrap();
    sql(&b, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','circle')").await.unwrap();
    let store = frames(&a, Audience::Store).await.concat();
    let gifts = frames(&b, circle.clone()).await.concat();
    let a_write = records(&a)[0].clone();
    let b_write = records(&b)[0].clone();
    a.apply_downloaded(b_write.clone().into()).await.unwrap();
    let original = contents(&c);
    assert!(c
        .load_snapshots(crate::SnapshotReload::new(
            vec![
                (id(Audience::Store), Cursor::new(store.clone())),
                (
                    id(circle.clone()),
                    Cursor::new(gifts[..gifts.len() - 1].to_vec())
                )
            ],
            vec![stream(&a_write), stream(&b_write)]
        ))
        .await
        .is_err());
    assert_eq!(contents(&c), original);
    for mode in [2, 0, 1] {
        let mut snapshots = vec![
            (id(Audience::Store), Cursor::new(store.clone())),
            (id(circle.clone()), Cursor::new(gifts.clone())),
        ];
        match mode {
            0 => {}
            1 => snapshots.reverse(),
            _ => {
                snapshots.pop();
            }
        }
        c.load_snapshots(crate::SnapshotReload::new(
            snapshots,
            vec![stream(&b_write), stream(&a_write)],
        ))
        .await
        .unwrap();
        for audience in [Audience::Store, circle.clone()] {
            assert_eq!(
                frames(&c, audience.clone()).await,
                frames(&a, audience).await
            );
        }
    }
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn supplied_dismissals_apply_before_waiting_changes_and_validate_their_past() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let c_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    let c = c_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','initial','')")
        .await
        .unwrap();
    for db in [&b, &c] {
        db.apply_downloaded(records(&a)[0].clone().into())
            .await
            .unwrap();
    }
    sql(&a, "UPDATE notes SET title='a'").await.unwrap();
    sql(&b, "UPDATE notes SET title='b'").await.unwrap();
    a.apply_downloaded(records(&b)[0].clone().into())
        .await
        .unwrap();
    b.apply_downloaded(records(&a)[1].clone().into())
        .await
        .unwrap();
    c.apply_downloaded(records(&a)[1].clone().into())
        .await
        .unwrap();
    c.apply_downloaded(records(&b)[0].clone().into())
        .await
        .unwrap();
    let snapshot = frames(&a, Audience::Store).await.concat();
    let losses = a.lost_values().await.unwrap();
    assert_eq!(losses.len(), 1);
    a.dismiss_lost_values(&losses).await.unwrap();
    let dismissal = records(&a).pop().unwrap();
    assert_eq!(dismissal.parts[0].dismissals.len(), 1);
    for db in [&b, &c] {
        db.apply_downloaded(dismissal.clone().into()).await.unwrap();
    }
    sql(&b, "UPDATE notes SET body='waiting'").await.unwrap();
    let waiting = records(&b);
    c.apply_downloaded(waiting.last().unwrap().clone().into())
        .await
        .unwrap();
    let before = contents(&b);
    let mut invalid = stream(&dismissal);
    invalid.header.header.had_read.0.clear();
    invalid.header.header.position.number = 1;
    assert!(matches!(
        b.load_snapshots(crate::SnapshotReload::new(
            vec![(id(Audience::Store), Cursor::new(snapshot.clone()))],
            vec![invalid],
        ))
        .await,
        Err(crate::DbError::Snapshot(crate::SnapshotError::Format(_)))
    ));
    assert_eq!(contents(&b), before);
    b.load_snapshots(crate::SnapshotReload::new(
        vec![(id(Audience::Store), Cursor::new(snapshot))],
        vec![stream(&dismissal)],
    ))
    .await
    .unwrap();
    assert_eq!(records(&b), waiting);
    assert!(b.lost_values().await.unwrap().is_empty());
    assert_eq!(
        frames(&b, Audience::Store).await,
        frames(&c, Audience::Store).await
    );
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}
