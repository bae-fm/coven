//! Files staged on this device only (§17.3): their records say so, nothing
//! is uploaded, and this device keeps their bytes by file, the user's
//! original or coven's copy, while their records exist.

use crate::{
    file_write::tests::{has_record, local_count, open, owned_paths},
    tests::TestStore,
    write::tests::records,
    DbError, FilePlace, LocalFileError, Place, Provenance, UserSource,
};
use coven_foundation::id_source::{FileId, SequentialIds};
use std::time::Duration;

/// Stage `source` in the `files` namespace for row `7`.
async fn stage_row(
    db: &crate::Database,
    stage: impl FnOnce(&mut crate::WriteBatch<'_>) -> FileId + Send + 'static,
) -> FileId {
    db.write_with_files::<_, _, _, _, DbError>(
        move |batch| Ok(stage(batch)),
        |sql, file| {
            sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
            Ok(file)
        },
    )
    .await
    .unwrap()
}

async fn read_local(db: &crate::Database, file: FileId) -> Result<Vec<u8>, LocalFileError> {
    let reference = db.file_ref(file).await.unwrap();
    let stream = db
        .open_local_file(&reference)
        .await?
        .expect("its bytes are here");
    stream.read_at(0, reference.size()).await
}

/// An app file staged on this device only keeps coven's copy and queues no
/// upload. Another device reads the row with the file on this device only;
/// the last reference going deletes the copy.
#[tokio::test]
async fn an_app_file_staged_on_this_device_only_keeps_coven_copy_and_uploads_nothing() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = open(&a_store, Provenance::AppProvided).await;
    let b = open(&b_store, Provenance::AppProvided).await;
    let file = stage_row(&a, |batch| {
        batch.put_file("files", b"local bytes".to_vec(), Place::DeviceOnly)
    })
    .await;
    assert_eq!(local_count(&a, "_coven_file_uploads"), 0);
    assert_eq!(local_count(&a, "_coven_device_files"), 1);
    assert_eq!(owned_paths(&a_store).len(), 1);
    let device = a_store.device();
    assert_eq!(
        a.file_ref(file).await.unwrap().place(),
        FilePlace::DeviceOnly { device }
    );
    assert_eq!(read_local(&a, file).await.unwrap(), b"local bytes");
    for record in records(&a) {
        b.receive(record.into()).await.unwrap();
    }
    assert_eq!(
        b.file_ref(file).await.unwrap().place(),
        FilePlace::DeviceOnly { device }
    );
    // Only this device's own files are read from bytes here.
    assert!(b
        .open_local_file(&b.file_ref(file).await.unwrap())
        .await
        .unwrap()
        .is_none());
    a.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(!has_record(&a, file));
    assert_eq!(local_count(&a, "_coven_device_files"), 0);
    assert!(owned_paths(&a_store).is_empty());
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// A user's original staged on this device only is observed, not read: its
/// path, size and modification time are what `coven_user_files` lists, its
/// reads return what's on disk while those facts hold, and its last
/// reference going forgets it without touching it.
#[tokio::test]
async fn an_original_staged_on_this_device_only_is_observed_and_read_from_disk() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"recorded original").unwrap();
    let observed = crate::observe_user_file(original.path()).await.unwrap();
    let file = stage_row(&db, |batch| {
        batch.put_user_file("files", UserSource::DeviceOnly(observed))
    })
    .await;
    assert_eq!(local_count(&db, "_coven_file_uploads"), 0);
    let (modified_seconds, modified_nanos) = crate::unix_seconds_and_nanos(
        std::fs::metadata(original.path())
            .unwrap()
            .modified()
            .unwrap(),
    );
    let listed = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT file,path,size,modified_seconds,modified_nanos FROM coven_user_files",
                [],
                |r| {
                    Ok((
                        r.get::<_, FileId>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, u32>(4)?,
                    ))
                },
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        listed,
        [(
            file,
            coven_foundation::files::path_bytes(original.path()),
            17,
            modified_seconds,
            modified_nanos
        )]
    );
    assert_eq!(read_local(&db, file).await.unwrap(), b"recorded original");
    std::fs::write(original.path(), b"edited original!!").unwrap();
    let later = std::time::SystemTime::now() + Duration::from_secs(5);
    std::fs::File::options()
        .write(true)
        .open(original.path())
        .unwrap()
        .set_modified(later)
        .unwrap();
    assert!(matches!(
        read_local(&db, file).await,
        Err(LocalFileError::UserFileChanged { .. })
    ));
    db.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(!has_record(&db, file));
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    assert_eq!(
        std::fs::read(original.path()).unwrap(),
        b"edited original!!"
    );
    db.close().await.unwrap();
}

/// An app compares an original with what its own scan recorded before
/// staging it: observing and reading it give the size and modification time
/// coven then records for it, to the nanosecond, for this device only and
/// for the cloud alike.
#[tokio::test]
async fn observing_or_reading_an_original_gives_the_facts_coven_records() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let folder = tempfile::tempdir().unwrap();
    // Within one millisecond, at a precision every platform keeps: Windows
    // keeps 100 ns.
    let scanned = std::time::UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_700);
    let original = |name: &str, bytes: &[u8]| {
        let path = folder.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(scanned)
            .unwrap();
        path
    };
    let observed_path = original("observed.flac", b"observed original");
    let observed = crate::observe_user_file(&observed_path).await.unwrap();
    assert_eq!((observed.size(), observed.modified_at()), (17, scanned));
    let prepared_path = original("prepared.flac", b"prepared original!");
    let prepared = crate::prepare_user_file(&prepared_path, |_| {})
        .await
        .unwrap();
    assert_eq!((prepared.size(), prepared.modified_at()), (18, scanned));
    let device_only = stage_row(&db, |batch| {
        batch.put_user_file("files", UserSource::DeviceOnly(observed))
    })
    .await;
    let cloud = db
        .write_with_files::<_, _, _, _, DbError>(
            move |batch| Ok(batch.put_user_file("files", UserSource::Cloud(prepared))),
            |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES('8',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    for (file, path, size) in [(device_only, observed_path, 17), (cloud, prepared_path, 18)] {
        assert_eq!(
            db.user_file(file).await.unwrap(),
            Some(crate::UserFile {
                path,
                size,
                modified_at: scanned
            })
        );
    }
    db.close().await.unwrap();
}

/// `coven_user_files` gives each original's modification time exactly, as
/// whole seconds since the Unix epoch, rounded down, and the nanoseconds past
/// that second. Two originals of one size changed within one millisecond
/// show different times, and a time before the epoch counts its nanoseconds
/// forward from the second before it.
#[tokio::test]
async fn coven_user_files_gives_each_modification_time_exactly() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let folder = tempfile::tempdir().unwrap();
    // 100 ns apart, a precision every platform keeps: Windows keeps 100 ns.
    let times = [
        (
            "a",
            std::time::UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_700),
        ),
        (
            "b",
            std::time::UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_800),
        ),
        ("c", std::time::UNIX_EPOCH - Duration::new(1, 123_456_700)),
    ];
    for (row, time) in times {
        let path = folder.path().join(row);
        std::fs::write(&path, b"one size").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(time)
            .unwrap();
        let observed = crate::observe_user_file(&path).await.unwrap();
        db.write_with_files::<_, _, _, _, DbError>(
            move |batch| Ok(batch.put_user_file("files", UserSource::DeviceOnly(observed))),
            move |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (row, file))?;
                Ok(())
            },
        )
        .await
        .unwrap();
    }
    let listed = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT f.id,u.size,u.modified_seconds,u.modified_nanos
                 FROM files AS f JOIN coven_user_files AS u ON u.file=f.file ORDER BY f.id",
                [],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        listed,
        [
            ("a".into(), 8, 1_700_000_000, 123_456_700),
            ("b".into(), 8, 1_700_000_000, 123_456_800),
            ("c".into(), 8, -2, 876_543_300),
        ]
    );
    db.close().await.unwrap();
}

/// An original whose name isn't valid Unicode is staged like any other:
/// coven records its path's own bytes, which `coven_user_files` lists and
/// `coven_path_text` shows with each byte that isn't UTF-8 escaped, and its
/// reads return what's on disk.
#[cfg(any(target_os = "linux", windows))]
async fn an_original_named(name: &std::ffi::OsStr, shown: &str) {
    let folder = tempfile::tempdir().unwrap();
    let path = folder.path().join(name);
    std::fs::write(&path, b"original bytes").unwrap();
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let observed = crate::observe_user_file(&path).await.unwrap();
    let file = stage_row(&db, |batch| {
        batch.put_user_file("files", UserSource::DeviceOnly(observed))
    })
    .await;
    let (bytes, text) = db
        .read(|sql| {
            Ok(sql.query_row(
                "SELECT path,coven_path_text(path) FROM coven_user_files",
                [],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(bytes, coven_foundation::files::path_bytes(&path));
    assert_eq!(text, coven_foundation::files::path_text(&path));
    assert!(text.ends_with(shown), "{text}");
    assert_eq!(db.user_file(file).await.unwrap().unwrap().path, path);
    assert_eq!(read_local(&db, file).await.unwrap(), b"original bytes");
    db.close().await.unwrap();
}

/// Linux names a file with any bytes; macOS's volumes refuse a name that
/// isn't UTF-8.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_original_whose_name_isnt_utf8_is_staged_listed_and_read() {
    use std::os::unix::ffi::OsStrExt;
    an_original_named(
        std::ffi::OsStr::from_bytes(b"Caf\xE9.flac"),
        r"Caf\xE9.flac",
    )
    .await;
}

/// Windows names a file with any UTF-16, an unpaired surrogate included,
/// which coven records in WTF-8.
#[cfg(windows)]
#[tokio::test]
async fn an_original_whose_name_has_an_unpaired_surrogate_is_staged_listed_and_read() {
    use std::os::windows::ffi::OsStringExt;
    let name = std::ffi::OsString::from_wide(&[0x43, 0x61, 0x66, 0xD800, 0x2E, 0x66]);
    an_original_named(&name, r"Caf\xED\xA0\x80.f").await;
}

const CIRCLE: &str = "00000000-0000-4000-8000-000000000002";

/// A store whose `photos` rows each reference one file and name their
/// audience, with this device in `CIRCLE`.
async fn photos(store: &TestStore) -> crate::Database {
    use crate::{FileColumn, Migration, RowIdentity, SyncedTable};
    let declaration = SyncedTable::new("photos", RowIdentity::IndependentUuid)
        .audience_column("audience")
        .file_column(FileColumn::new("file"));
    let schema = "CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT,audience TEXT NOT NULL); CREATE INDEX photos_file ON photos(file)";
    let db = store
        .builder(
            vec![declaration],
            vec![Migration::addition(1, "photos", schema)],
        )
        .namespaces(crate::file_write::tests::namespaces(
            Provenance::AppProvided,
        ))
        .open()
        .await
        .unwrap();
    store.join_circle(
        &db,
        coven_foundation::id_source::CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap()),
    );
    db
}

async fn photo_file(db: &crate::Database) -> FileId {
    db.read(|sql| Ok(sql.query_row("SELECT file FROM photos", [], |r| r.get(0))?))
        .await
        .unwrap()
}

async fn move_to_circle(db: &crate::Database) {
    db.write(move |sql| {
        sql.execute("UPDATE photos SET audience=?1", [CIRCLE])?;
        Ok(())
    })
    .await
    .unwrap();
}

/// A row moved to another audience gets a new file there over the same
/// bytes (§17.2). For a file on this device only, with no upload, the new
/// file is on this device only too and shares coven's copy, which stays
/// while either file names it.
#[tokio::test]
async fn a_moved_rows_device_only_file_shares_coven_copy() {
    let store = TestStore::new();
    let db = photos(&store).await;
    let before = db
        .write_with_files::<_, _, _, _, DbError>(
            |batch| Ok(batch.put_file("files", b"photo".to_vec(), Place::DeviceOnly)),
            |sql, file| {
                sql.execute(
                    "INSERT INTO photos(id,file,audience) VALUES('00000000-0000-4000-8000-000000000011',?1,'store')",
                    [file],
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    move_to_circle(&db).await;
    let after = photo_file(&db).await;
    assert_ne!(after, before);
    assert!(!has_record(&db, before));
    assert_eq!(
        db.file_ref(after).await.unwrap().place(),
        FilePlace::DeviceOnly {
            device: store.device()
        }
    );
    assert_eq!(local_count(&db, "_coven_file_uploads"), 0);
    assert_eq!(owned_paths(&store).len(), 1);
    assert_eq!(read_local(&db, after).await.unwrap(), b"photo");
    db.close().await.unwrap();
}

/// Only the device holding a file's bytes can make a new file over them.
/// Ben's laptop moving a row whose file is on Ana's laptop only would make
/// a file no device has bytes for, so the move fails with `NotThisDevice`
/// and changes nothing.
#[tokio::test]
async fn a_move_of_a_file_on_another_device_only_fails() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = photos(&a_store).await;
    let b = photos(&b_store).await;
    let file = a
        .write_with_files::<_, _, _, _, DbError>(
            |batch| Ok(batch.put_file("files", b"photo".to_vec(), Place::DeviceOnly)),
            |sql, file| {
                sql.execute(
                    "INSERT INTO photos(id,file,audience) VALUES('00000000-0000-4000-8000-000000000011',?1,'store')",
                    [file],
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    for record in records(&a) {
        crate::tests::publish_through(&a, record.header.position.number)
            .await
            .unwrap();
        b.receive(record.into()).await.unwrap();
    }
    let error = b
        .write(move |sql| {
            sql.execute("UPDATE photos SET audience=?1", [CIRCLE])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, DbError::NotThisDevice { file: refused } if refused == file),
        "{error:?}"
    );
    assert_eq!(photo_file(&b).await, file);
    let audience: String = b
        .read(|sql| Ok(sql.query_row("SELECT audience FROM photos", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(audience, "store");
    assert_eq!(records(&b).len(), 0);
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// A circle's file never leaves it. Ana's laptop moving a Gifts row that
/// names a stored file to the store fails with `FileLeavesCircle` and
/// changes nothing; the app stages the bytes again for the store.
#[tokio::test]
async fn a_move_out_of_a_circle_of_a_row_with_a_file_fails() {
    let store = TestStore::new();
    let db = photos(&store).await;
    let file = db
        .write_with_files::<_, _, _, _, DbError>(
            |batch| Ok(batch.put_file("files", b"photo".to_vec(), Place::DeviceOnly)),
            |sql, file| {
                sql.execute(
                    "INSERT INTO photos(id,file,audience) VALUES('00000000-0000-4000-8000-000000000011',?1,?2)",
                    rusqlite::params![file, CIRCLE],
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    let error = db
        .write(|sql| {
            sql.execute("UPDATE photos SET audience='store'", [])?;
            Ok(())
        })
        .await
        .unwrap_err();
    let circle = coven_foundation::id_source::CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap());
    assert!(
        matches!(error, DbError::FileLeavesCircle { file: refused, circle: of }
            if refused == file && of == circle),
        "{error:?}"
    );
    assert_eq!(photo_file(&db).await, file);
    let audience: String = db
        .read(|sql| Ok(sql.query_row("SELECT audience FROM photos", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(audience, CIRCLE);
    db.close().await.unwrap();
}

/// When the device a file is on is removed, coven does nothing: the rows
/// keep their references and the file stays on that device only (§17.3).
#[tokio::test]
async fn a_removed_devices_files_stay_on_it() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = open(&a_store, Provenance::AppProvided).await;
    let b = open(&b_store, Provenance::AppProvided).await;
    let file = stage_row(&a, |batch| {
        batch.put_file("files", b"on a".to_vec(), Place::DeviceOnly)
    })
    .await;
    for record in records(&a) {
        crate::tests::publish_through(&a, record.header.position.number)
            .await
            .unwrap();
        b.receive(record.into()).await.unwrap();
    }
    let device = a_store.device();
    crate::tests::remove_device(&b, device).await;
    assert!(has_record(&b, file));
    assert_eq!(
        b.file_ref(file).await.unwrap().place(),
        FilePlace::DeviceOnly { device }
    );
    let referenced: Option<FileId> = b
        .read(|sql| Ok(sql.query_row("SELECT file FROM files WHERE id='7'", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(referenced, Some(file));
    a.close().await.unwrap();
    b.close().await.unwrap();
}
