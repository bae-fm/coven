use crate::snapshot_write::tests::{frames, id, load_one, stream};
use crate::tests::contents;
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
    sql(&a, "INSERT INTO notes VALUES('n','first','')").await.unwrap();
    let first = records(&a).remove(0);
    for db in [&b, &c] {
        db.receive(first.clone().into()).await.unwrap();
    }
    let snapshot = frames(&a, Audience::Store).await;
    let snapshot_prefix = snapshot.prefix.clone();
    let snapshot = snapshot.concat();
    sql(&a, "UPDATE notes SET title='second'").await.unwrap();
    let missing = records(&a).pop().unwrap();
    for db in [&b, &c] {
        db.receive(missing.clone().into()).await.unwrap();
    }
    sql(&b, "UPDATE notes SET title='waiting'").await.unwrap();
    sql(&b, "UPDATE notes SET body='next waiting write'").await.unwrap();
    let waiting = records(&b);
    for write in &waiting {
        c.receive(write.clone().into()).await.unwrap();
    }
    let original = contents(&b);
    let mut query = b.subscribe(|db| {
        Ok(db.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
    });
    assert_eq!(query.next().await.expect("the live query answers").unwrap(), "waiting");
    assert!(matches!(
        load_one(&b, id(Audience::Store), snapshot_prefix.clone(), Cursor::new(snapshot.clone()))
            .await,
        Err(crate::DbError::Snapshot(crate::SnapshotError::Inconsistent(_)))
    ));
    assert_eq!(contents(&b), original);
    assert!(!query.is_marked_for_rerun());
    b.inspect_writer(|db| db.batch("CREATE TEMP TRIGGER refuse_waiting AFTER UPDATE ON notes WHEN new.title='waiting' BEGIN SELECT RAISE(ABORT,'replay failed'); END").unwrap());
    b.load_snapshots::<crate::pending::tests::Record, _>(
        crate::tests::reload(
            &b,
            vec![(
                id(Audience::Store),
                snapshot_prefix.clone(),
                Cursor::new(snapshot.clone()),
                crate::snapshot_write::tests::author(),
            )],
            vec![stream(&missing)],
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await,
        None,
    )
    .await
    .unwrap();
    assert_eq!(records(&b), waiting);
    assert!(!query.is_marked_for_rerun());
    b.inspect_writer(|db| db.batch("DROP TRIGGER refuse_waiting").unwrap());
    drop(query);
    for duplicate in [false, true] {
        let mut supplied = vec![stream(&missing)];
        if duplicate {
            supplied.push(stream(&waiting[0]));
        }
        b.load_snapshots::<crate::pending::tests::Record, _>(
            crate::tests::reload(
                &b,
                vec![(
                    id(Audience::Store),
                    snapshot_prefix.clone(),
                    Cursor::new(snapshot.clone()),
                    crate::snapshot_write::tests::author(),
                )],
                supplied,
                std::sync::Arc::new(crate::test_utils::TestAuthorship),
            )
            .await,
            None,
        )
        .await
        .unwrap();
        assert_eq!(records(&b), waiting);
        assert_eq!(
            crate::tests::data(&b, Audience::Store),
            crate::tests::data(&c, Audience::Store)
        );
    }
    for write in &waiting {
        upload(&b, write).await;
    }
    b.close().await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    assert_eq!(frames(&b, Audience::Store).await, frames(&c, Audience::Store).await);
    sql(&b, "UPDATE notes SET title='after reload'").await.unwrap();
    let next = records(&b).pop().unwrap();
    assert_eq!(next.header.position.number, waiting.last().unwrap().header.position.number + 1);
    c.receive(next.into()).await.unwrap();
    assert_eq!(frames(&b, Audience::Store).await, frames(&c, Audience::Store).await);
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

async fn upload(db: &crate::Database, record: &coven_format::write::WriteRecord) {
    crate::upload::tests::attempt(db, 17).await.unwrap();
    crate::tests::publish_through(db, record.header.position.number).await.unwrap();
}

#[tokio::test]
async fn waiting_files_survive_intermediate_snapshot_and_gap_write_states() {
    use crate::file_write::tests::{has_record, open, owned_paths, stage};
    let store = TestStore::new();
    let db = open(&store, crate::Provenance::AppProvided).await;
    let original = stage(&db, b"original".to_vec(), true).await.unwrap();
    let snapshot = frames(&db, Audience::Store).await;
    let snapshot_prefix = snapshot.prefix.clone();
    let snapshot = snapshot.concat();
    sql(&db, "UPDATE files SET title='missing'").await.unwrap();
    let writes = records(&db);
    for write in &writes {
        upload(&db, write).await;
    }
    let file = stage(&db, b"waiting bytes".to_vec(), false).await.unwrap();
    // The write naming the new file, on this device only while it uploads,
    // waits to publish; it deleted the original's record, which nothing
    // references now (§17.2).
    assert_eq!(records(&db).len(), 1);
    assert!(!has_record(&db, original));
    let waiting = db.file_ref(file).await.unwrap();
    let paths = owned_paths(&store);
    let expected = crate::tests::data(&db, Audience::Store);
    let before = contents(&db);
    assert!(load_one(
        &db,
        id(Audience::Store),
        snapshot_prefix.clone(),
        Cursor::new(snapshot.clone())
    )
    .await
    .is_err());
    assert_eq!(contents(&db), before);
    assert_eq!(owned_paths(&store), paths);
    db.load_snapshots::<crate::pending::tests::Record, _>(
        crate::tests::reload(
            &db,
            vec![(
                id(Audience::Store),
                snapshot_prefix.clone(),
                Cursor::new(snapshot),
                crate::snapshot_write::tests::author(),
            )],
            vec![stream(&writes[1])],
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await,
        None,
    )
    .await
    .unwrap();
    assert_eq!(crate::tests::data(&db, Audience::Store), expected);
    // Both uploads keep their own copies until they end.
    assert_eq!(owned_paths(&store), paths);
    let mut bytes: Vec<_> = paths.iter().map(|path| std::fs::read(path).unwrap()).collect();
    bytes.sort();
    assert_eq!(bytes, [b"original".to_vec(), b"waiting bytes".to_vec()]);
    assert_eq!(db.file_ref(file).await.unwrap(), waiting);
    assert!(!has_record(&db, original));
    // Its upload completing after the reload switches the file to the cloud
    // in a write of its own.
    crate::file_write::tests::complete_uploads(&db).await;
    assert_eq!(records(&db).len(), 2);
    assert!(matches!(db.file_ref(file).await.unwrap().place(), crate::FilePlace::Cloud { .. }));
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
    sql(&a, "INSERT INTO notes VALUES('n','before','')").await.unwrap();
    let snapshot = frames(&a, Audience::Store).await;
    let snapshot_prefix = snapshot.prefix.clone();
    let snapshot = snapshot.concat();
    a.write(|db| {
        db.execute("UPDATE notes SET body=?1", ["x".repeat(200_000)])?;
        Ok(())
    })
    .await
    .unwrap();
    let missing = records(&a).pop().unwrap();
    b.receive(records(&a)[0].clone().into()).await.unwrap();
    b.receive(missing.clone().into()).await.unwrap();
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
            .load_snapshots::<crate::pending::tests::Record, _>(
                crate::tests::reload(
                    &b,
                    vec![(
                        id(Audience::Store),
                        snapshot_prefix.clone(),
                        Cursor::new(snapshot.clone()),
                        crate::snapshot_write::tests::author()
                    )],
                    vec![DownloadedWriteStream {
                        signer: crate::test_utils::signer(),
                        header: input.header.clone(),
                        parts: vec![DownloadedPartStream::Opened(Cursor::new(bytes.clone()))],
                        authorship: input.authorship.clone(),
                    }],
                    std::sync::Arc::new(crate::test_utils::TestAuthorship)
                )
                .await,
                None
            )
            .await
            .is_err());
        assert_eq!(contents(&b), before);
    }
    assert!(b
        .load_snapshots::<crate::pending::tests::Record, _>(
            crate::tests::reload(
                &b,
                vec![(
                    id(Audience::Store),
                    snapshot_prefix.clone(),
                    Cursor::new(snapshot.clone()),
                    crate::snapshot_write::tests::author()
                )],
                vec![DownloadedWriteStream::<Cursor<Vec<u8>>> {
                    signer: crate::test_utils::signer(),
                    header: input.header.clone(),
                    parts: vec![DownloadedPartStream::Skipped],
                    authorship: input.authorship.clone(),
                }],
                std::sync::Arc::new(crate::test_utils::TestAuthorship)
            )
            .await,
            None
        )
        .await
        .is_err());
    assert_eq!(contents(&b), before);
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&original).unwrap();
    file.rewind().unwrap();
    b.load_snapshots::<crate::pending::tests::Record, _>(
        crate::tests::reload(
            &b,
            vec![(
                id(Audience::Store),
                snapshot_prefix.clone(),
                Cursor::new(snapshot),
                crate::snapshot_write::tests::author(),
            )],
            vec![DownloadedWriteStream {
                signer: crate::test_utils::signer(),
                authorship: input.authorship.clone(),
                header: input.header,
                parts: vec![DownloadedPartStream::Opened(BoundedFile(file))],
            }],
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await,
        None,
    )
    .await
    .unwrap();
    for write in records(&a) {
        a.receive(write.into()).await.unwrap();
    }
    a.receive(records(&b)[0].clone().into()).await.unwrap();
    assert_eq!(frames(&b, Audience::Store).await, frames(&a, Audience::Store).await);
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
    b_store.join_circle(&b, CircleId(uuid::Uuid::from_u128(10)));
    sql(&a, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','store','store')")
        .await
        .unwrap();
    sql(&b, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','circle')").await.unwrap();
    let store_frames = frames(&a, Audience::Store).await;
    let store_prefix = store_frames.prefix.clone();
    let store = store_frames.concat();
    b.receive(records(&a)[0].clone().into()).await.unwrap();
    let gifts = frames(&b, circle.clone()).await;
    let gifts_prefix = gifts.prefix.clone();
    let gifts = gifts.concat();
    let b_write = records(&b)[0].clone();
    a.receive(b_write.clone().into()).await.unwrap();
    let original = contents(&c);
    // A truncated body never stages, so no reload can install it.
    assert!(crate::test_utils::stage_plaintext(
        &c,
        id(circle.clone()),
        gifts_prefix.clone(),
        Cursor::new(gifts[..gifts.len() - 1].to_vec()),
    )
    .await
    .is_err());
    assert_eq!(contents(&c), original);
    // Both bodies stage, and installing the store's fails after the circle's
    // installed: its synced value disagrees with its merge record. The whole
    // load rolls back, so neither audience changes.
    let (header, mut records) = crate::snapshot_write::tests::decode(&store_frames);
    let row = records
        .iter_mut()
        .find_map(|record| match record {
            coven_format::snapshot::SnapshotRecord::Synced(row) => Some(row),
            _ => None,
        })
        .expect("the store snapshot carries its row");
    row.columns.get_mut("title").unwrap().value =
        coven_format::value::Value::Text("changed".into());
    let (mut encoder, mut disagreeing) =
        coven_format::snapshot::SnapshotEncoder::start(header).unwrap();
    for record in records {
        disagreeing.extend(encoder.record(record).unwrap());
    }
    disagreeing.extend(encoder.finish().unwrap());
    let failed = c
        .load_snapshots::<crate::pending::tests::Record, _>(
            crate::tests::reload(
                &c,
                vec![
                    (
                        id(circle.clone()),
                        gifts_prefix.clone(),
                        Cursor::new(gifts.clone()),
                        crate::snapshot_write::tests::author(),
                    ),
                    (
                        id(Audience::Store),
                        store_prefix.clone(),
                        Cursor::new(disagreeing),
                        crate::snapshot_write::tests::author(),
                    ),
                ],
                vec![stream(&b_write)],
                std::sync::Arc::new(crate::test_utils::TestAuthorship),
            )
            .await,
            None,
        )
        .await
        .map(drop);
    assert!(
        matches!(
            &failed,
            Err(crate::DbError::Snapshot(crate::SnapshotError::Body { snapshot, error }))
                if *snapshot == id(Audience::Store)
                    && matches!(**error, crate::DbError::Snapshot(
                        crate::SnapshotError::Inconsistent("synced and merge values disagree")
                    ))
        ),
        "{failed:?}"
    );
    assert_eq!(contents(&c), original);
    for audience in [Audience::Store, circle.clone()] {
        assert_eq!(crate::tests::data(&c, audience), (Vec::new(), Vec::new()));
    }
    for mode in [0, 1] {
        let mut snapshots = vec![
            (
                id(Audience::Store),
                store_prefix.clone(),
                Cursor::new(store.clone()),
                crate::snapshot_write::tests::author(),
            ),
            (
                id(circle.clone()),
                gifts_prefix.clone(),
                Cursor::new(gifts.clone()),
                crate::snapshot_write::tests::author(),
            ),
        ];
        match mode {
            0 => {}
            1 => snapshots.reverse(),
            _ => {
                snapshots.pop();
            }
        }
        c.load_snapshots::<crate::pending::tests::Record, _>(
            crate::tests::reload(
                &c,
                snapshots,
                vec![stream(&b_write)],
                std::sync::Arc::new(crate::test_utils::TestAuthorship),
            )
            .await,
            None,
        )
        .await
        .unwrap();
        for audience in [Audience::Store, circle.clone()] {
            assert_eq!(frames(&c, audience.clone()).await, frames(&a, audience).await);
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
    sql(&a, "INSERT INTO notes VALUES('n','initial','')").await.unwrap();
    for db in [&a, &b, &c] {
        db.receive(records(&a)[0].clone().into()).await.unwrap();
    }
    sql(&a, "UPDATE notes SET title='a'").await.unwrap();
    sql(&b, "UPDATE notes SET title='b'").await.unwrap();
    for write in records(&a) {
        a.receive(write.into()).await.unwrap();
    }
    a.receive(records(&b)[0].clone().into()).await.unwrap();
    b.receive(records(&a)[1].clone().into()).await.unwrap();
    b.receive(records(&b)[0].clone().into()).await.unwrap();
    c.receive(records(&a)[1].clone().into()).await.unwrap();
    c.receive(records(&b)[0].clone().into()).await.unwrap();
    let snapshot = frames(&a, Audience::Store).await;
    let snapshot_prefix = snapshot.prefix.clone();
    let snapshot = snapshot.concat();
    let losses = a.lost_values().await.unwrap();
    assert_eq!(losses.len(), 1);
    a.dismiss_lost_values(&losses).await.unwrap();
    let dismissal = records(&a).pop().unwrap();
    assert_eq!(dismissal.parts[0].dismissals.len(), 1);
    for db in [&b, &c] {
        db.receive(dismissal.clone().into()).await.unwrap();
    }
    sql(&b, "UPDATE notes SET body='waiting'").await.unwrap();
    let waiting = records(&b);
    c.receive(waiting.last().unwrap().clone().into()).await.unwrap();
    let before = contents(&b);
    let mut invalid = stream(&dismissal);
    invalid.header.header.read_through = 0;
    let device = coven_foundation::id_source::DeviceId(50_000);
    invalid.header.header.position = coven_merge::WriteId { device, number: 1 };
    invalid.header.header.timestamp =
        coven_merge::Timestamp::new(dismissal.header.timestamp.milliseconds() + 1, 0, device)
            .unwrap();
    assert!(matches!(
        b.load_snapshots::<crate::pending::tests::Record, _>(
            crate::tests::reload(
                &b,
                vec![(
                    id(Audience::Store),
                    snapshot_prefix.clone(),
                    Cursor::new(snapshot.clone()),
                    crate::snapshot_write::tests::author()
                )],
                vec![invalid],
                std::sync::Arc::new(crate::test_utils::TestAuthorship),
            )
            .await,
            None
        )
        .await,
        Err(crate::DbError::Snapshot(crate::SnapshotError::Inconsistent(
            "dismissal names a write outside authored past"
        )))
    ));
    assert_eq!(contents(&b), before);
    b.load_snapshots::<crate::pending::tests::Record, _>(
        crate::tests::reload(
            &b,
            vec![(
                id(Audience::Store),
                snapshot_prefix.clone(),
                Cursor::new(snapshot),
                crate::snapshot_write::tests::author(),
            )],
            vec![stream(&dismissal)],
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await,
        None,
    )
    .await
    .unwrap();
    assert_eq!(records(&b), waiting);
    assert!(b.lost_values().await.unwrap().is_empty());
    assert_eq!(frames(&b, Audience::Store).await, frames(&c, Audience::Store).await);
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}
