//! Moves (§17.3): a write's cloud files and a move's files switch to the
//! cloud together, in one write once every one is stored; a move survives
//! reopening; a cancelled move switches nothing.

use crate::{
    file_write::tests::{has_record, local_count, open},
    tests::TestStore,
    write::tests::records,
    Database, DbError, FileDatabase, FilePlace, MoveFile, MoveId, Place, Provenance,
};
use coven_foundation::files::FileLease;
use coven_foundation::id_source::{FileId, SequentialIds};

/// Stage one file per row id, each where `place` says, in one write.
async fn stage(db: &Database, rows: &[&'static str], place: Place) -> Vec<FileId> {
    let rows = rows.to_vec();
    let count = rows.len();
    db.staged_write::<_, _, _, _, DbError>(
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

/// The system refuses the key of a move's upload. The write fails with
/// nothing of it committed, so the file stays on this device only and in no
/// move, and moving it again isn't `FileInMove` (§17.3).
#[tokio::test(flavor = "current_thread")]
async fn a_refused_file_key_fails_a_move_and_leaves_its_file_in_none() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let local = stage(&db, &["local"], Place::DeviceOnly).await[0];
    let error = db
        .write(move |sql| {
            let moved = sql.move_to_cloud(vec![MoveFile::App(local)])?;
            coven_crypto::refuse_randomness(1);
            Ok(moved)
        })
        .await
        .unwrap_err();
    assert!(
        matches!(&error, DbError::FileKey(coven_crypto::CryptoError::Unavailable(_))),
        "{error:?}"
    );
    assert!(!cloud(&db, local));
    assert!(listed(&db).await.is_empty());
    assert_eq!(db.unsent().await.unwrap().files, 0);
    let moved = move_files(&db, vec![MoveFile::App(local)]).await.unwrap();
    assert_eq!(listed(&db).await, listing(moved, "moving", &[local]));
    db.close().await.unwrap();
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
            Ok::<_, DbError>(sql.query(
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

/// `coven_pending.move` names the move a record is about, so app SQL joins a
/// move's record to the move's uploads in `coven_uploads`, and is NULL for a
/// record about no move (E4).
#[tokio::test]
async fn coven_pending_names_the_move_a_record_is_about() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let local = stage(&db, &["a"], Place::DeviceOnly).await;
    let moved = move_files(&db, local.iter().copied().map(MoveFile::App).collect()).await.unwrap();
    db.record_pending(crate::pending::tests::moving(1, moved)).await.unwrap();
    db.record_pending(crate::pending::tests::missing(2)).await.unwrap();
    let listed = db
        .read(|sql| {
            Ok::<_, DbError>(sql.query(
                "SELECT p.position,p.move FROM coven_pending AS p ORDER BY p.position",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<MoveId>>(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(listed.iter().map(|(_, of)| *of).collect::<Vec<_>>(), [Some(moved), None]);
    let joined = db
        .read(|sql| {
            Ok::<_, DbError>(sql.query(
                "SELECT f.id FROM coven_pending AS p JOIN coven_uploads AS u ON u.move=p.move
                 JOIN files AS f ON f.file=u.file",
                [],
                |r| r.get::<_, String>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(joined, ["a"]);
    db.close().await.unwrap();
}

/// A move takes only files on this device only and in no move: another
/// device's file, a file in the cloud, one already in a move, named with
/// that move, and one this device has no record of are refused.
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
    let staged = FileDatabase::new(a.clone()).uploads().await.unwrap()[0].of;
    assert!(matches!(
        refused(&a, uploading).await,
        DbError::FileInMove { file, id } if file == uploading && id == staged
    ));
    complete(&a, uploading).await;
    assert!(matches!(
        refused(&a, uploading).await,
        DbError::NotDeviceOnlyHere { file } if file == uploading
    ));
    let absent = FileId(uuid::Uuid::from_u128(77));
    assert!(matches!(refused(&a, absent).await, DbError::FileAbsent { file } if file == absent));
    // Twice in one write, or in two moves, a file moves once. The second
    // move in one write is refused, which fails that write (§4), so nothing
    // of it moves.
    let twice = a
        .write(move |sql| {
            let first = sql.move_to_cloud(vec![MoveFile::App(local)])?;
            assert!(matches!(
                sql.move_to_cloud(vec![MoveFile::App(local)]),
                Err(DbError::FileInMove { file, id }) if file == local && id == first
            ));
            Ok(())
        })
        .await;
    assert!(matches!(twice, Err(DbError::StepFailed)), "{twice:?}");
    let moving = move_files(&a, vec![MoveFile::App(local)]).await.unwrap();
    assert!(matches!(
        refused(&a, local).await,
        DbError::FileInMove { file, id } if file == local && id == moving
    ));
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// Moving a user's original reads nothing: its upload carries the size,
/// modification time, content hash and chunk hashes its staging recorded,
/// so it checks the bytes it sends against the import, however the
/// original stands at the move. Once the move switches the file to the
/// cloud, the chunk hashes go and the original stays recorded.
#[tokio::test]
async fn a_moved_original_uploads_with_what_its_staging_recorded() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let chunk = coven_format::chunks::CHUNK_SIZE;
    let imported: Vec<u8> = (0..chunk + 9).map(|i| (i % 249) as u8).collect();
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), &imported).unwrap();
    let prepared = crate::prepare_user_file(original.path(), &FileLease::for_test().await, |_| {})
        .await
        .unwrap();
    let modified_at = prepared.modified_at();
    let file = db
        .write_as::<_, _, DbError>(move |sql| {
            let file = sql.put_user_file("files", prepared, crate::Place::DeviceOnly)?;
            sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
            Ok(file)
        })
        .await
        .unwrap();
    let hash = |bytes: &[u8]| {
        let mut hasher = coven_crypto::ContentHasher::new();
        hasher.update(bytes);
        hasher.finish()
    };
    // The move reads nothing: not even a missing original stops its write.
    let moved_away = original.path().with_extension("moved");
    std::fs::rename(original.path(), &moved_away).unwrap();
    move_files(&db, vec![MoveFile::User(file)]).await.unwrap();
    std::fs::rename(&moved_away, original.path()).unwrap();
    let [upload] = FileDatabase::new(db.clone()).uploads().await.unwrap().try_into().unwrap();
    assert_eq!(upload.files, [file]);
    assert_eq!(upload.size, imported.len() as u64);
    assert_eq!(upload.content_hash, hash(&imported));
    let (queued_time, queued_chunks) = db.inspect_writer(|sql| {
        let time = sql
            .query_row(
                "SELECT modified_at FROM _coven_file_uploads WHERE id=?1",
                [upload.id],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .unwrap();
        let chunks = sql
            .query(
                "SELECT c.hash FROM _coven_file_chunks AS c JOIN _coven_file_uploads AS u
                     ON u.device=c.device AND u.object=c.object
                 WHERE u.id=?1 ORDER BY c.chunk",
                [upload.id],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .unwrap();
        (time, chunks)
    });
    assert_eq!(crate::user_file::decode_time(&queued_time).unwrap(), modified_at);
    let imported_chunks: Vec<Vec<u8>> =
        imported.chunks(chunk).map(|bytes| hash(bytes).as_bytes().to_vec()).collect();
    assert_eq!(imported_chunks.len(), 2);
    assert_eq!(queued_chunks, imported_chunks);
    complete(&db, file).await;
    assert!(cloud(&db, file));
    assert_eq!(local_count(&db, "_coven_user_chunks"), 0);
    assert_eq!(local_count(&db, "_coven_user_files"), 1);
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

/// The app's local count of the pin requests coven has not yet made, kept
/// by its triggers on `coven_pin_requests`.
const KEEPING: &str =
    "CREATE TABLE keeping(id INTEGER PRIMARY KEY CHECK(id=1), files INTEGER NOT NULL);
     INSERT INTO keeping VALUES(1,0);
     CREATE TRIGGER pin_requested AFTER INSERT ON coven_pin_requests BEGIN
         UPDATE keeping SET files=files+1;
     END;
     CREATE TRIGGER pin_request_ended AFTER DELETE ON coven_pin_requests BEGIN
         UPDATE keeping SET files=files-1;
     END;";

/// The app reads `coven_pin_requests`, and a local trigger on it follows
/// coven as it adds a request (the switch to the cloud) and as it ends one
/// (the file's row is deleted), each in the statement that changes it.
#[tokio::test]
async fn a_local_trigger_follows_each_pin_request_coven_adds_and_ends() {
    let store = TestStore::new();
    let db = store
        .builder(
            crate::file_write::tests::tables(),
            vec![
                crate::Migration::addition(1, "files", crate::file_write::tests::SCHEMA),
                crate::Migration::addition(2, "keeping", KEEPING),
            ],
        )
        .namespaces(crate::file_write::tests::namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    let kept = |db: &Database| {
        let db = db.clone();
        async move {
            db.read(|sql| {
                Ok::<_, DbError>((
                    sql.query_row("SELECT files FROM keeping", [], |r| r.get::<_, i64>(0))?,
                    sql.query("SELECT file FROM coven_pin_requests", [], |r| {
                        r.get::<_, String>(0)
                    })?,
                ))
            })
            .await
            .unwrap()
        }
    };
    let files = stage(&db, &["kept", "plain"], Place::DeviceOnly).await;
    move_files(&db, vec![MoveFile::AppKeptHere(files[0]), MoveFile::App(files[1])]).await.unwrap();
    assert_eq!(kept(&db).await, (0, vec![]));
    complete(&db, files[0]).await;
    complete(&db, files[1]).await;
    assert_eq!(kept(&db).await, (1, vec![files[0].to_string()]));
    db.write(|sql| {
        sql.execute("DELETE FROM files WHERE id='kept'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(kept(&db).await, (0, vec![]));
    db.close().await.unwrap();
}

/// What app SQL reads of `coven_moves`: each file a move lists, with the
/// move's state, by move and file.
async fn listed(db: &Database) -> Vec<(MoveId, String, FileId)> {
    db.read(|sql| {
        Ok::<_, DbError>(sql.query(
            "SELECT move,state,file FROM coven_moves ORDER BY move,file",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?)
    })
    .await
    .unwrap()
}

/// `files`, each listed under move `of` in `state`, as `listed` orders them.
fn listing(of: MoveId, state: &str, files: &[FileId]) -> Vec<(MoveId, String, FileId)> {
    let mut listing: Vec<_> = files.iter().map(|file| (of, state.to_owned(), *file)).collect();
    listing.sort_by_key(|(_, _, file)| file.to_string());
    listing
}

/// A move lists its files in `coven_moves`, moving, from the write that
/// records it, and so does a write's own move of the files it stages for the
/// cloud. Storing one upload changes nothing; the switch ends the list.
#[tokio::test]
async fn a_move_lists_its_files_from_its_write_until_its_switch() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let files = stage(&db, &["a", "b"], Place::DeviceOnly).await;
    assert!(listed(&db).await.is_empty());
    let id = move_files(&db, files.iter().copied().map(MoveFile::App).collect()).await.unwrap();
    assert_eq!(listed(&db).await, listing(id, "moving", &files));
    let staged = stage(&db, &["c"], Place::Cloud).await;
    let written = FileDatabase::new(db.clone())
        .uploads()
        .await
        .unwrap()
        .into_iter()
        .find(|upload| upload.files == staged)
        .unwrap()
        .of;
    let both = [listing(id, "moving", &files), listing(written, "moving", &staged)].concat();
    assert_eq!(listed(&db).await, both);
    complete(&db, files[0]).await;
    assert_eq!(listed(&db).await, both);
    complete(&db, files[1]).await;
    assert_eq!(listed(&db).await, listing(written, "moving", &staged));
    complete(&db, staged[0]).await;
    assert!(listed(&db).await.is_empty());
    db.close().await.unwrap();
}

/// A failed move keeps listing its files, failed, and ends only their
/// uploads. Each stays in it: moving one again is refused until the app
/// cancels the move. A file whose last reference goes leaves the list in
/// that write; cancelling the move ends the rest. Moved again, a file is
/// listed under the new move, and cancelling that ends its list too.
#[tokio::test]
async fn a_failed_move_lists_its_files_until_the_app_cancels_it() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let files = stage(&db, &["a", "b", "c"], Place::DeviceOnly).await;
    let id = move_files(&db, files.iter().copied().map(MoveFile::App).collect()).await.unwrap();
    complete(&db, files[0]).await;
    let uploads = FileDatabase::new(db.clone());
    let failing = uploads.uploads().await.unwrap()[0].id;
    uploads.fail_move(failing, crate::pending::tests::moving(1, id)).await.unwrap();
    assert_eq!(listed(&db).await, listing(id, "failed", &files));
    assert!(uploads.uploads().await.unwrap().is_empty());
    assert_eq!(local_count(&db, "_coven_upload_files"), 0);
    assert!(matches!(
        move_files(&db, vec![MoveFile::App(files[1])]).await,
        Err(DbError::FileInMove { file, id: of }) if file == files[1] && of == id
    ));
    assert_eq!(listed(&db).await, listing(id, "failed", &files));
    db.write(|sql| {
        sql.execute("DELETE FROM files WHERE id='c'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(listed(&db).await, listing(id, "failed", &files[..2]));
    db.write(move |sql| sql.cancel_move(id)).await.unwrap();
    assert!(listed(&db).await.is_empty());
    let again = move_files(&db, vec![MoveFile::App(files[0])]).await.unwrap();
    assert_eq!(listed(&db).await, listing(again, "moving", &files[..1]));
    db.write(move |sql| sql.cancel_move(again)).await.unwrap();
    assert!(listed(&db).await.is_empty());
    db.close().await.unwrap();
}

/// The app's local log of what coven does to `coven_moves`, kept by its
/// triggers on the table.
const FOLLOWING: &str = "CREATE TABLE followed(n INTEGER PRIMARY KEY, change TEXT NOT NULL,
         move INTEGER NOT NULL, state TEXT NOT NULL, file TEXT NOT NULL);
     CREATE TRIGGER move_listed AFTER INSERT ON coven_moves BEGIN
         INSERT INTO followed(change,move,state,file) VALUES('listed',NEW.move,NEW.state,NEW.file);
     END;
     CREATE TRIGGER move_changed AFTER UPDATE ON coven_moves BEGIN
         INSERT INTO followed(change,move,state,file) VALUES('changed',NEW.move,NEW.state,NEW.file);
     END;
     CREATE TRIGGER move_ended AFTER DELETE ON coven_moves BEGIN
         INSERT INTO followed(change,move,state,file) VALUES('ended',OLD.move,OLD.state,OLD.file);
     END;";

/// A local trigger on `coven_moves` follows each file coven lists under a
/// move as the move starts, each state change as it fails, and each file
/// it stops listing, as a cancel or a switch ends the move.
#[tokio::test]
async fn a_local_trigger_follows_each_move_coven_lists_fails_and_ends() {
    let store = TestStore::new();
    let db = store
        .builder(
            crate::file_write::tests::tables(),
            vec![
                crate::Migration::addition(1, "files", crate::file_write::tests::SCHEMA),
                crate::Migration::addition(2, "following", FOLLOWING),
            ],
        )
        .namespaces(crate::file_write::tests::namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    let files = stage(&db, &["a", "b"], Place::DeviceOnly).await;
    let id = move_files(&db, files.iter().copied().map(MoveFile::App).collect()).await.unwrap();
    let uploads = FileDatabase::new(db.clone());
    let failing = uploads.uploads().await.unwrap()[0].id;
    uploads.fail_move(failing, crate::pending::tests::moving(1, id)).await.unwrap();
    db.write(move |sql| sql.cancel_move(id)).await.unwrap();
    let again = move_files(&db, vec![MoveFile::App(files[0])]).await.unwrap();
    complete(&db, files[0]).await;
    let followed = db
        .read(|sql| {
            Ok::<_, DbError>(sql.query(
                "SELECT change,move,state,file FROM followed ORDER BY n",
                [],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, MoveId>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, FileId>(3)?,
                    ))
                },
            )?)
        })
        .await
        .unwrap();
    let mut each = files.clone();
    each.sort_by_key(ToString::to_string);
    let step = |change: &str, of: MoveId, state: &str, file: FileId| {
        (change.to_owned(), of, state.to_owned(), file)
    };
    let mut expected = Vec::new();
    for (change, state) in [("listed", "moving"), ("changed", "failed"), ("ended", "failed")] {
        let mut changes: Vec<_> = each.iter().map(|file| step(change, id, state, *file)).collect();
        expected.append(&mut changes);
    }
    expected.push(step("listed", again, "moving", files[0]));
    expected.push(step("ended", again, "moving", files[0]));
    let mut seen = followed;
    // Within one statement SQLite visits a move's files in no promised order.
    for chunk in seen.chunks_mut(2).take(3) {
        chunk.sort_by_key(|(_, _, _, file)| file.to_string());
    }
    assert_eq!(seen, expected);
    db.close().await.unwrap();
}

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_moves(id,state)
           SELECT i,CASE i%4 WHEN 0 THEN 'moving' WHEN 1 THEN 'failed' WHEN 2 THEN 'cancelled' ELSE 'finished' END FROM fixture;
         INSERT INTO _coven_move_files(move,file)
           SELECT m.id,'file_' || (2*m.id+c.k) FROM _coven_moves AS m,(SELECT 0 AS k UNION ALL SELECT 1) AS c
           WHERE m.state IN ('moving','failed');",
    )
    .unwrap();
}

/// Record move `id` as still moving.
pub(crate) fn moving(db: &crate::sqlite::DatabaseConnection, id: i64) {
    db.internal_execute("INSERT INTO _coven_moves(id,state) VALUES(?1,'moving')", [id]).unwrap();
}

/// The scans this module's statements may make, with their reasons, for the
/// plan audit (`internal_schema_scan_reasons.rs`): `template` is the statement as
/// written, `sql` as run.
pub(crate) fn scan_reason(
    file: &str,
    template: &str,
    _sql: &str,
    relation: &str,
) -> Option<&'static str> {
    match (file, template, relation) {
        ("file_moves.rs", "INSERT INTO _coven_move_files(move,file) VALUES{values}", "2-ROW") => {
            Some("the listed files' rows are the statement's own values")
        }
        _ => None,
    }
}
