//! Bound references at commit (§17.4): what a write setting one may name,
//! and the write its file's upload holds.
use crate::{tests::TestStore, write::tests::records, *};
use coven_foundation::id_source::FileId;

const SCHEMA: &str = "CREATE TABLE covers(id TEXT NOT NULL PRIMARY KEY,file TEXT);
    CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT);
    CREATE TABLE originals(id TEXT NOT NULL PRIMARY KEY,file TEXT);";

/// `covers` binds its file at `limit` bytes, `photos` trails, and
/// `originals` holds the user's files, in namespaces of their names.
async fn open(store: &TestStore, cover: FileColumn) -> Database {
    store
        .builder(
            vec![
                SyncedTable::new("covers", RowIdentity::SharedKey).file_column(cover),
                SyncedTable::new("photos", RowIdentity::SharedKey)
                    .file_column(FileColumn::new("file")),
                SyncedTable::new("originals", RowIdentity::SharedKey)
                    .file_column(FileColumn::new("file").bound()),
            ],
            vec![Migration::addition(1, "files", SCHEMA)],
        )
        .namespaces(vec![
            Namespace::new("covers", Provenance::AppProvided, 1 << 30),
            Namespace::new("photos", Provenance::AppProvided, 1 << 30),
            Namespace::new("originals", Provenance::UserProvided, 1 << 30),
        ])
        .open()
        .await
        .unwrap()
}

/// Stage `bytes` into `table`'s namespace, where `place` says, and point
/// row `id` of `table` at the new file.
async fn stage(
    db: &Database,
    table: &'static str,
    id: &'static str,
    bytes: Vec<u8>,
    place: Place,
) -> Result<FileId, DbError> {
    db.write_with_files::<_, _, _, _, DbError>(
        move |batch| Ok(batch.put_file(table, bytes, place)),
        move |sql, file| {
            sql.execute(
                &format!("INSERT INTO {table}(id,file) VALUES(?1,?2)"),
                (id, file),
            )?;
            Ok(file)
        },
    )
    .await
}

/// Point row `id` of `table` at `file`, a file staged earlier.
async fn point(
    db: &Database,
    table: &'static str,
    id: &'static str,
    file: FileId,
) -> Result<(), DbError> {
    db.write(move |sql| {
        sql.execute(
            &format!("INSERT INTO {table}(id,file) VALUES(?1,?2)"),
            (id, file),
        )?;
        Ok(())
    })
    .await
}

/// Each queued upload's files, with the write it holds.
fn holds(db: &Database) -> Vec<(Vec<FileId>, Option<u64>)> {
    db.inspect_writer(|db| crate::file_queue::read_all(db).unwrap())
        .into_iter()
        .map(|upload| (upload.files, upload.holds))
        .collect()
}

/// The numbers of this device's waiting writes, oldest first.
fn waiting(db: &Database) -> Vec<u64> {
    records(db)
        .iter()
        .map(|record| record.header.position.number)
        .collect()
}

#[tokio::test]
async fn a_bound_file_one_byte_over_its_limit_fails_the_write() {
    assert_eq!(BOUND_FILE_LIMIT, 1_572_864, "1.5 MiB");
    for (cover, limit) in [
        (FileColumn::new("file").bound(), BOUND_FILE_LIMIT),
        (FileColumn::new("file").bound_with_limit(100), 100),
    ] {
        let store = TestStore::new();
        let db = open(&store, cover).await;
        let over = vec![7; usize::try_from(limit).unwrap() + 1];
        assert!(matches!(
            stage(&db, "covers", "1", over, Place::Cloud).await,
            Err(DbError::BoundFileTooLarge { table, column, size, limit: refused })
                if table == "covers" && column == "file" && size == limit + 1 && refused == limit
        ));
        assert!(waiting(&db).is_empty());
        assert!(holds(&db).is_empty());
        let exact = vec![7; usize::try_from(limit).unwrap()];
        stage(&db, "covers", "1", exact, Place::Cloud)
            .await
            .unwrap();
        // A trailing reference takes a file of any size.
        let large = vec![7; usize::try_from(limit).unwrap() + 1];
        stage(&db, "photos", "1", large, Place::Cloud)
            .await
            .unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_bound_reference_to_a_users_file_fails_the_write() {
    let store = TestStore::new();
    let db = open(&store, FileColumn::new("file").bound()).await;
    let path = store.database_path().parent().unwrap().join("original");
    std::fs::write(&path, b"the person's bytes").unwrap();
    let sources = [
        UserSource::DeviceOnly(observe_user_file(&path).await.unwrap()),
        UserSource::Cloud(prepare_user_file(&path, |_| {}).await.unwrap()),
    ];
    for source in sources {
        let staged = db
            .write_with_files::<_, _, _, _, DbError>(
                move |batch| Ok(batch.put_user_file("originals", source)),
                move |sql, file| {
                    sql.execute("INSERT INTO originals(id,file) VALUES('1',?1)", [file])?;
                    Ok(())
                },
            )
            .await;
        assert!(matches!(
            staged,
            Err(DbError::NamespaceKind { namespace, kind: Provenance::UserProvided })
                if namespace == "originals"
        ));
    }
    assert!(waiting(&db).is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_bound_reference_to_a_file_no_bound_upload_stores_fails_the_write() {
    let store = TestStore::new();
    let db = open(&store, FileColumn::new("file").bound()).await;
    // On this device only, with no upload at all.
    assert!(matches!(
        stage(&db, "covers", "1", b"local".to_vec(), Place::DeviceOnly).await,
        Err(DbError::DeviceOnly { .. })
    ));
    // Uploading for a trailing reference, whose write went out at once: its
    // upload holds no write, so nothing would wait for it.
    let photo = stage(&db, "photos", "1", b"photo".to_vec(), Place::Cloud)
        .await
        .unwrap();
    assert!(matches!(
        point(&db, "covers", "1", photo).await,
        Err(DbError::DeviceOnly { file }) if file == photo
    ));
    // Once stored, any write may bind it.
    crate::file_write::tests::complete_uploads(&db).await;
    point(&db, "covers", "1", photo).await.unwrap();
    assert!(holds(&db).is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_bound_upload_holds_its_write_and_every_later_one_until_it_is_stored() {
    let store = TestStore::new();
    let db = open(&store, FileColumn::new("file").bound()).await;
    let first = stage(&db, "photos", "1", b"before".to_vec(), Place::Cloud)
        .await
        .unwrap();
    // One write staging a bound cover and a trailing photo: the cover is a
    // move of its own, which its upload holds this write in.
    let (cover, photo) = db
        .write_with_files::<_, _, _, _, DbError>(
            |batch| {
                Ok((
                    batch.put_file("covers", b"cover".to_vec(), Place::Cloud),
                    batch.put_file("photos", b"photo".to_vec(), Place::Cloud),
                ))
            },
            |sql, (cover, photo)| {
                sql.execute("INSERT INTO covers(id,file) VALUES('1',?1)", [cover])?;
                sql.execute("INSERT INTO photos(id,file) VALUES('2',?1)", [photo])?;
                Ok((cover, photo))
            },
        )
        .await
        .unwrap();
    let [before, held] = waiting(&db)[..] else {
        panic!("two writes wait: {:?}", waiting(&db));
    };
    let uploads = db.inspect_writer(|db| crate::file_queue::read_all(db).unwrap());
    let of = |file: FileId| {
        uploads
            .iter()
            .find(|upload| upload.files.contains(&file))
            .unwrap()
    };
    assert_eq!(of(first).holds, None);
    assert_eq!(of(photo).holds, None);
    assert_eq!(of(cover).holds, Some(held));
    assert_ne!(of(cover).of, of(photo).of);
    // A later write naming the held cover, and one changing nothing about
    // it, wait behind it; the write before it may go.
    point(&db, "covers", "2", cover).await.unwrap();
    point(&db, "photos", "3", first).await.unwrap();
    assert_eq!(waiting(&db).len(), 4);
    assert_eq!(holds(&db).iter().filter(|(_, h)| h.is_some()).count(), 1);
    assert_eq!(db.sendable_through().await.unwrap(), Some(before));
    // The completion stores the cover and lifts the hold in one write.
    crate::file_write::tests::complete_uploads(&db).await;
    let newest = *waiting(&db).last().unwrap();
    assert!(newest > held);
    assert_eq!(db.sendable_through().await.unwrap(), Some(newest));
    assert!(matches!(
        db.file_ref(cover).await.unwrap().place(),
        FilePlace::Cloud { .. }
    ));
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_held_write_whose_file_a_later_write_drops_goes_with_that_write() {
    let store = TestStore::new();
    let db = open(&store, FileColumn::new("file").bound()).await;
    let cover = stage(&db, "covers", "1", b"cover".to_vec(), Place::Cloud)
        .await
        .unwrap();
    let held = waiting(&db)[0];
    assert_eq!(db.sendable_through().await.unwrap(), Some(held - 1));
    // Deleting the row deletes the cover's record: no file waits for the
    // upload any more, so it holds nothing, and both writes may go together.
    db.write(|sql| {
        sql.execute("DELETE FROM covers WHERE id='1'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(!crate::file_write::tests::has_record(&db, cover));
    let newest = *waiting(&db).last().unwrap();
    assert_eq!(db.sendable_through().await.unwrap(), Some(newest));
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_pause_shows_on_trailing_uploads_only() {
    let store = TestStore::new();
    let db = open(&store, FileColumn::new("file").bound()).await;
    let photo = stage(&db, "photos", "1", b"photo".to_vec(), Place::Cloud)
        .await
        .unwrap();
    let cover = stage(&db, "covers", "1", b"cover".to_vec(), Place::Cloud)
        .await
        .unwrap();
    let paused = |db: Database| async move {
        db.read(|sql| {
            Ok(sql.query(
                "SELECT file,paused FROM coven_uploads ORDER BY position",
                [],
                |r| Ok((r.get::<_, FileId>(0)?, r.get::<_, bool>(1)?)),
            )?)
        })
        .await
        .unwrap()
    };
    assert_eq!(paused(db.clone()).await, [(photo, false), (cover, false)]);
    crate::FileDatabase::new(db.clone())
        .set_uploads_paused(true)
        .await
        .unwrap();
    assert_eq!(paused(db.clone()).await, [(photo, true), (cover, false)]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn overlapping_bound_uploads_hold_from_the_first_held_write_until_both_are_stored() {
    let store = TestStore::new();
    let db = open(&store, FileColumn::new("file").bound()).await;
    let first = stage(&db, "covers", "1", b"first".to_vec(), Place::Cloud)
        .await
        .unwrap();
    let second = stage(&db, "covers", "2", b"second".to_vec(), Place::Cloud)
        .await
        .unwrap();
    let held = waiting(&db)[0];
    // The second cover's completion can follow the first's held write only
    // after the second's own held write, so it holds from the first's too.
    assert_eq!(
        holds(&db),
        [(vec![first], Some(held)), (vec![second], Some(held))]
    );
    let owner = crate::FileDatabase::new(db.clone());
    let upload = |file: FileId| {
        db.inspect_writer(|db| crate::file_queue::upload_of(db, file).unwrap().unwrap())
    };
    // The first cover stored alone: its completion waits behind the second
    // cover's held write, so its own held write still waits too.
    owner.complete_upload(upload(first), ()).await.unwrap();
    assert_eq!(waiting(&db).len(), 3);
    assert_eq!(db.sendable_through().await.unwrap(), Some(held - 1));
    owner.complete_upload(upload(second), ()).await.unwrap();
    let newest = *waiting(&db).last().unwrap();
    assert_eq!(waiting(&db).len(), 4);
    assert_eq!(db.sendable_through().await.unwrap(), Some(newest));
    // A bound upload queued once none is holds from its own write.
    let third = stage(&db, "covers", "3", b"third".to_vec(), Place::Cloud)
        .await
        .unwrap();
    assert_eq!(holds(&db), [(vec![third], Some(newest + 1))]);
    assert_eq!(db.sendable_through().await.unwrap(), Some(newest));
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_bound_files_move_cant_be_cancelled() {
    let store = TestStore::new();
    let db = open(&store, FileColumn::new("file").bound()).await;
    let cover = stage(&db, "covers", "1", b"cover".to_vec(), Place::Cloud)
        .await
        .unwrap();
    let held = waiting(&db)[0];
    let of = db.inspect_writer(|db| {
        crate::file_queue::read_all(db)
            .unwrap()
            .into_iter()
            .find(|upload| upload.files.contains(&cover))
            .unwrap()
            .of
    });
    // The app never gets this id, but may pass any: cancelling would leave
    // the cover on this device only and let its held write go out.
    assert!(matches!(
        db.write(move |sql| sql.cancel_move(of)).await,
        Err(DbError::MoveBound { id }) if id == of
    ));
    assert_eq!(holds(&db), [(vec![cover], Some(held))]);
    assert_eq!(db.sendable_through().await.unwrap(), Some(held - 1));
    db.close().await.unwrap();
}
