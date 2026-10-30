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
    let modified = std::fs::metadata(original.path())
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let listed = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT file,path,size,modified_at FROM coven_user_files",
                [],
                |r| {
                    Ok((
                        r.get::<_, FileId>(0)?,
                        r.get::<_, String>(1)?,
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
        [(
            file,
            original.path().to_str().unwrap().to_owned(),
            17,
            i64::try_from(modified.as_millis()).unwrap()
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

/// An original whose path isn't valid Unicode can't be listed as text, so
/// observing or reading it is refused.
#[cfg(unix)]
#[tokio::test]
async fn an_original_whose_path_is_not_unicode_is_refused() {
    use std::os::unix::ffi::OsStrExt;
    let folder = tempfile::tempdir().unwrap();
    let path = folder
        .path()
        .join(std::ffi::OsStr::from_bytes(b"not-\xff-unicode"));
    if std::fs::write(&path, b"bytes").is_err() {
        // The filesystem refuses such names itself.
        return;
    }
    assert!(matches!(
        crate::observe_user_file(&path).await,
        Err(DbError::Disk(_))
    ));
    assert!(matches!(
        crate::prepare_user_file(&path, |_| {}).await,
        Err(DbError::Disk(_))
    ));
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

/// Another device moving the row makes the new file on the device that has
/// the bytes, which, applying the move, keeps the same copy for it.
#[tokio::test]
async fn a_move_another_device_makes_shares_this_devices_copy() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = photos(&a_store).await;
    let b = photos(&b_store).await;
    a.write_with_files::<_, _, _, _, DbError>(
        |batch| Ok(batch.put_file("files", b"photo".to_vec(), Place::DeviceOnly)),
        |sql, file| {
            sql.execute(
                "INSERT INTO photos(id,file,audience) VALUES('00000000-0000-4000-8000-000000000011',?1,'store')",
                [file],
            )?;
            Ok(())
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
    move_to_circle(&b).await;
    let moved = photo_file(&b).await;
    let device = a_store.device();
    assert_eq!(
        b.file_ref(moved).await.unwrap().place(),
        FilePlace::DeviceOnly { device }
    );
    for record in records(&b) {
        a.receive(record.into()).await.unwrap();
    }
    assert_eq!(photo_file(&a).await, moved);
    assert_eq!(read_local(&a, moved).await.unwrap(), b"photo");
    assert_eq!(local_count(&a, "_coven_file_uploads"), 0);
    assert_eq!(owned_paths(&a_store).len(), 1);
    a.close().await.unwrap();
    b.close().await.unwrap();
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
