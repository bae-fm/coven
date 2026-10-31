//! Moves (§17.3): a write's cloud files and a move's files switch to the
//! cloud together, in one write once every one is stored; a move survives
//! reopening; a cancelled move switches nothing.

use crate::{
    file_write::tests::{has_record, local_count, open},
    tests::TestStore,
    write::tests::records,
    Database, DbError, FileDatabase, FilePlace, MoveFile, MoveId, Place, Provenance,
};
use coven_foundation::id_source::{FileId, SequentialIds};

/// Stage one file per row id, each where `place` says, in one write.
async fn stage(db: &Database, rows: &[&'static str], place: Place) -> Vec<FileId> {
    let rows = rows.to_vec();
    let count = rows.len();
    db.write_with_files::<_, _, _, _, DbError>(
        move |batch| {
            Ok((0..count)
                .map(|index| batch.put_file("files", format!("bytes {index}").into_bytes(), place))
                .collect::<Vec<_>>())
        },
        move |sql, files| {
            for (row, file) in rows.iter().zip(&files) {
                sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (row, file))?;
            }
            Ok(files)
        },
    )
    .await
    .unwrap()
}

fn cloud(db: &Database, file: FileId) -> bool {
    db.inspect_writer(|sql| {
        sql.query_row(
            "SELECT coven_file_device(coordinates) IS NULL FROM main.coven_files WHERE id=?1",
            [file],
            |r| r.get(0),
        )
        .unwrap()
    })
}

/// Complete the queued upload storing `file`, as confirmed storage would.
async fn complete(db: &Database, file: FileId) {
    let files = FileDatabase::new(db.clone());
    let upload = files
        .uploads()
        .await
        .unwrap()
        .into_iter()
        .find(|upload| upload.files.contains(&file))
        .expect("the file waits for an upload");
    files.complete_upload(upload.id, ()).await.unwrap();
}

/// The files the newest write of this device switched to the cloud.
fn switched_in_last_write(db: &Database) -> usize {
    records(db)
        .pop()
        .unwrap()
        .parts
        .iter()
        .flat_map(|part| &part.rows)
        .filter(|row| row.row.table == "coven_files")
        .count()
}

async fn move_files(db: &Database, files: Vec<MoveFile>) -> Result<MoveId, DbError> {
    db.write(move |sql| sql.move_to_cloud(files)).await
}

/// One write's cloud files are one move: storing one switches nothing, and
/// storing the last switches every one in one write, changing no row.
#[tokio::test]
async fn a_writes_cloud_files_switch_together_once_all_are_stored() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let files = stage(&db, &["a", "b", "c"], Place::Cloud).await;
    let uploads = FileDatabase::new(db.clone()).uploads().await.unwrap();
    assert_eq!(uploads.len(), 3);
    assert!(uploads.iter().all(|upload| upload.of == uploads[0].of));
    let writes = records(&db).len();
    complete(&db, files[0]).await;
    complete(&db, files[2]).await;
    assert!(files.iter().all(|file| !cloud(&db, *file)));
    assert_eq!(records(&db).len(), writes);
    complete(&db, files[1]).await;
    assert!(files.iter().all(|file| cloud(&db, *file)));
    assert_eq!(records(&db).len(), writes + 1);
    assert_eq!(switched_in_last_write(&db), 3);
    assert!(FileDatabase::new(db.clone()).uploads().await.unwrap().is_empty());
    db.close().await.unwrap();
}

/// A move of files staged on this device only survives reopening, and its
/// files switch together once all are stored. The rows don't change.
#[tokio::test]
async fn a_move_resumes_after_reopening_and_switches_its_files_in_one_write() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let files = stage(&db, &["a", "b"], Place::DeviceOnly).await;
    let moved = files.clone();
    move_files(&db, moved.into_iter().map(MoveFile::App).collect()).await.unwrap();
    let writes = records(&db).len();
    assert_eq!(local_count(&db, "_coven_file_uploads"), 2);
    db.close().await.unwrap();
    let db = open(&store, Provenance::AppProvided).await;
    complete(&db, files[1]).await;
    assert!(!cloud(&db, files[1]));
    complete(&db, files[0]).await;
    assert!(files.iter().all(|file| cloud(&db, *file)));
    assert_eq!(records(&db).len(), writes + 1);
    assert_eq!(switched_in_last_write(&db), 2);
    // Each switched file's copy goes with its upload.
    assert_eq!(local_count(&db, "_coven_device_files"), 0);
    db.close().await.unwrap();
}

/// A file whose last reference goes drops out of its move; the rest still
/// switch together.
#[tokio::test]
async fn a_file_whose_last_reference_goes_drops_out_of_its_move() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let files = stage(&db, &["a", "b"], Place::Cloud).await;
    complete(&db, files[0]).await;
    db.write(|sql| {
        sql.execute("DELETE FROM files WHERE id='b'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(!has_record(&db, files[1]));
    assert!(!cloud(&db, files[0]));
    FileDatabase::new(db.clone()).retire_unlinked(Default::default(), |_| ()).await.unwrap();
    assert!(cloud(&db, files[0]));
    assert_eq!(local_count(&db, "_coven_file_uploads"), 0);
    db.close().await.unwrap();
}

/// Cancelling a move switches nothing: every file stays on this device
/// only, its uploads go, and cancelling again does nothing. A finished move
/// and one this device never made can't be cancelled.
#[tokio::test]
async fn cancelling_a_move_switches_nothing() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let files = stage(&db, &["a", "b"], Place::DeviceOnly).await;
    let id = move_files(&db, files.iter().copied().map(MoveFile::App).collect()).await.unwrap();
    complete(&db, files[0]).await;
    for _ in 0..2 {
        db.write(move |sql| sql.cancel_move(id)).await.unwrap();
    }
    let uploads = FileDatabase::new(db.clone());
    assert!(uploads.uploads().await.unwrap().is_empty());
    uploads.retire_unlinked(Default::default(), |_| ()).await.unwrap();
    assert_eq!(local_count(&db, "_coven_file_uploads"), 0);
    let device = store.device();
    for file in &files {
        assert_eq!(db.file_ref(*file).await.unwrap().place(), FilePlace::DeviceOnly { device });
    }
    // Their copies stay, and a new move can take them.
    assert_eq!(local_count(&db, "_coven_device_files"), 2);
    let again = move_files(&db, vec![MoveFile::App(files[0])]).await.unwrap();
    complete(&db, files[0]).await;
    assert!(cloud(&db, files[0]));
    assert!(matches!(
        db.write(move |sql| sql.cancel_move(again)).await,
        Err(DbError::MoveFinished { id }) if id == again
    ));
    let unknown = MoveId(again.0 + 100);
    assert!(matches!(
        db.write(move |sql| sql.cancel_move(unknown)).await,
        Err(DbError::MoveUnknown { id }) if id == unknown
    ));
    db.close().await.unwrap();
}

/// `coven_uploads` names each queued file's move by the id `move_to_cloud`
/// returned, and a write's own cloud files by their write's move, so app
/// SQL finds what to cancel (E4).
#[tokio::test]
async fn coven_uploads_names_each_files_move() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let local = stage(&db, &["a", "b"], Place::DeviceOnly).await;
    let moved = move_files(&db, local.iter().copied().map(MoveFile::App).collect()).await.unwrap();
    let staged = stage(&db, &["c"], Place::Cloud).await;
    let listed = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT f.id,u.move FROM coven_uploads AS u JOIN files AS f ON f.file=u.file
                 ORDER BY f.id",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<MoveId>>(1)?)),
            )?)
        })
        .await
        .unwrap();
    let written = FileDatabase::new(db.clone())
        .uploads()
        .await
        .unwrap()
        .into_iter()
        .find(|upload| upload.files == staged)
        .unwrap()
        .of;
    assert_ne!(written, moved);
    assert_eq!(
        listed,
        [
            ("a".to_owned(), Some(moved)),
            ("b".to_owned(), Some(moved)),
            ("c".to_owned(), Some(written)),
        ]
    );
    db.close().await.unwrap();
}

/// A move takes only files on this device only with no upload queued:
/// another device's file, a file in the cloud or already moving, and one
/// this device has no record of are refused.
#[tokio::test]
async fn a_move_takes_only_this_devices_files_waiting_for_no_upload() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = open(&a_store, Provenance::AppProvided).await;
    let b = open(&b_store, Provenance::AppProvided).await;
    let local = stage(&a, &["local"], Place::DeviceOnly).await[0];
    let uploading = stage(&a, &["uploading"], Place::Cloud).await[0];
    for record in records(&a) {
        crate::tests::publish_through(&a, record.header.position.number).await.unwrap();
        b.receive(record.into()).await.unwrap();
    }
    let refused = |db: &Database, file| {
        let db = db.clone();
        async move {
            match move_files(&db, vec![MoveFile::App(file)]).await {
                Err(error) => error,
                other => panic!("{other:?}"),
            }
        }
    };
    assert!(matches!(refused(&b, local).await, DbError::NotThisDevice { file } if file == local));
    assert!(matches!(
        refused(&a, uploading).await,
        DbError::NotDeviceOnlyHere { file } if file == uploading
    ));
    complete(&a, uploading).await;
    assert!(matches!(
        refused(&a, uploading).await,
        DbError::NotDeviceOnlyHere { file } if file == uploading
    ));
    let absent = FileId(uuid::Uuid::from_u128(77));
    assert!(matches!(refused(&a, absent).await, DbError::FileAbsent { file } if file == absent));
    // Twice in one write, or in two moves, a file moves once.
    a.write(move |sql| {
        sql.move_to_cloud(vec![MoveFile::App(local)])?;
        assert!(matches!(
            sql.move_to_cloud(vec![MoveFile::App(local)]),
            Err(DbError::NotDeviceOnlyHere { .. })
        ));
        Ok(())
    })
    .await
    .unwrap();
    assert!(matches!(
        refused(&a, local).await,
        DbError::NotDeviceOnlyHere { file } if file == local
    ));
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// A user's original to move must still be as recorded: its path, size
/// and modification time.
#[tokio::test]
async fn a_moved_original_must_be_as_recorded() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"recorded").unwrap();
    let observed = crate::observe_user_file(original.path()).await.unwrap();
    let file = db
        .write_with_files::<_, _, _, _, DbError>(
            move |batch| Ok(batch.put_user_file("files", crate::UserSource::DeviceOnly(observed))),
            |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    std::fs::write(original.path(), b"rewritten, longer").unwrap();
    let prepared = crate::prepare_user_file(original.path(), |_| {}).await.unwrap();
    assert!(matches!(
        move_files(&db, vec![MoveFile::User(file, prepared)]).await,
        Err(DbError::UserFileChanged { .. })
    ));
    assert_eq!(local_count(&db, "_coven_file_uploads"), 0);
    db.close().await.unwrap();
}

/// A file a move keeps here is pinned by the write switching the move: the
/// switch records its pin request, keeping coven's copy for it; the other
/// file switches with it and asks for nothing.
#[tokio::test]
async fn a_move_kept_here_asks_for_its_pins_in_the_switch() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let files = stage(&db, &["kept", "plain"], Place::DeviceOnly).await;
    move_files(&db, vec![MoveFile::AppKeptHere(files[0]), MoveFile::App(files[1])]).await.unwrap();
    let requests = FileDatabase::new(db.clone());
    complete(&db, files[0]).await;
    assert!(requests.pin_requests().await.unwrap().is_empty());
    complete(&db, files[1]).await;
    let asked = requests.pin_requests().await.unwrap();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].file, files[0]);
    assert!(asked[0].copy.is_some());
    // The plain file's copy went with its upload; the kept one's stays.
    assert_eq!(local_count(&db, "_coven_device_files"), 0);
    let copies: i64 = db.inspect_writer(|sql| {
        sql.query_row(
            "SELECT count(*) FROM _coven_file_removals WHERE path=?1",
            [asked[0].copy.as_ref().unwrap().as_str()],
            |r| r.get(0),
        )
        .unwrap()
    });
    assert_eq!(copies, 0);
    // Deleting the row ends the request, and the copy goes.
    db.write(|sql| {
        sql.execute("DELETE FROM files WHERE id='kept'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(requests.pin_requests().await.unwrap().is_empty());
    db.close().await.unwrap();
}
