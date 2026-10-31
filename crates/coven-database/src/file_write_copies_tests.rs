//! New files over existing files' bytes in another audience (§17.2): a
//! circle's row naming a store file's bytes through a new file of its own,
//! with nothing uploaded again; each record changing on its own; the stored
//! bytes kept while either names them; a file on this device only giving a
//! new file over the same original; a new file over a file its own write
//! stages sharing that file's upload; a circle's file giving a new file in
//! the store; and the refusal of a file on another device only.

use crate::{
    file_write::tests::{complete_uploads, has_record, local_count, owned_paths},
    tests::TestStore,
    write::tests::records,
    DbError, FileCandidate, FileColumn, FilePlace, Migration, Place, Provenance, RowIdentity,
    SyncedTable, UserSource,
};
use coven_foundation::id_source::{CircleId, FileId, SequentialIds};
use std::collections::BTreeSet;

const CIRCLE: &str = "00000000-0000-4000-8000-000000000002";
const SCHEMA: &str =
    "CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT,audience TEXT NOT NULL);
    CREATE INDEX photos_file ON photos(file);
    CREATE TABLE covers(id TEXT NOT NULL PRIMARY KEY,file TEXT,audience TEXT NOT NULL);
    CREATE INDEX covers_file ON covers(file);";

/// A store whose `photos` rows reference a file each, trailing, and whose
/// `covers` rows a bound one, every row naming its audience, with this
/// device in `CIRCLE` and one namespace, `files`, of this kind.
async fn open(store: &TestStore, kind: Provenance) -> crate::Database {
    let db = store
        .builder(
            vec![
                SyncedTable::new("photos", RowIdentity::IndependentUuid)
                    .audience_column("audience")
                    .file_column(FileColumn::new("file")),
                SyncedTable::new("covers", RowIdentity::IndependentUuid)
                    .audience_column("audience")
                    .file_column(FileColumn::new("file").bound()),
            ],
            vec![Migration::addition(1, "photos", SCHEMA)],
        )
        .namespaces(crate::file_write::tests::namespaces(kind))
        .open()
        .await
        .unwrap();
    store.join_circle(&db, CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap()));
    db
}

/// Row `n` of `table`, in `audience`, naming `file`.
fn insert(
    sql: &crate::SqlContext<'_, '_>,
    table: &str,
    n: u8,
    audience: &str,
    file: FileId,
) -> Result<(), DbError> {
    sql.execute(
        &format!("INSERT INTO {table}(id,file,audience) VALUES(?1,?2,?3)"),
        (format!("00000000-0000-4000-8000-0000000000{n:02}"), file, audience),
    )?;
    Ok(())
}

/// Write a `photos` row `n` in `audience` naming a new file over `from`'s
/// bytes, returning the new file.
async fn share_into(db: &crate::Database, n: u8, audience: &'static str, from: FileId) -> FileId {
    db.write_with_files::<_, _, _, _, DbError>(
        move |batch| Ok(batch.existing_file(from)),
        move |sql, file| {
            insert(&sql, "photos", n, audience, file)?;
            Ok(file)
        },
    )
    .await
    .unwrap()
}

/// Stage `bytes` for the cloud in a store `photos` row `1` and store them.
async fn stored(db: &crate::Database, bytes: &[u8]) -> FileId {
    let bytes = bytes.to_vec();
    let file = db
        .write_with_files::<_, _, _, _, DbError>(
            move |batch| Ok(batch.put_file("files", bytes, Place::Cloud)),
            |sql, file| {
                insert(&sql, "photos", 1, "store", file)?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    complete_uploads(db).await;
    file
}

fn record(db: &crate::Database, file: FileId) -> crate::file_table::Record {
    db.inspect_writer(|sql| crate::file_table::Record::read(sql, file).unwrap()).unwrap()
}

/// The candidates of this device's own stored bytes nothing local protects.
async fn unprotected(
    db: &crate::Database,
    device: coven_foundation::id_source::DeviceId,
) -> Vec<FileId> {
    db.file_candidates(BTreeSet::from([device]))
        .await
        .unwrap()
        .into_iter()
        .filter(|FileCandidate { protected, .. }| !protected)
        .map(|candidate| candidate.file)
        .collect()
}

/// Ana's laptop puts a stored store photo in a Gifts row. The new file is
/// in Gifts, over the same stored bytes and key, and nothing is uploaded
/// again. Ben's phone, in Gifts, reads it from the same stored bytes.
#[tokio::test]
async fn a_circle_row_names_a_store_files_bytes_through_a_new_file_with_no_upload() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = open(&a_store, Provenance::AppProvided).await;
    let b = open(&b_store, Provenance::AppProvided).await;
    let photo = stored(&a, b"photo").await;
    let owned = owned_paths(&a_store).len();
    let copy = share_into(&a, 2, CIRCLE, photo).await;
    assert_ne!(copy, photo);
    let (source, shared) = (record(&a, photo), record(&a, copy));
    assert_eq!(
        shared.audience,
        coven_merge::Audience::Circle(CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap()))
    );
    assert_eq!(shared.place, source.place);
    assert_eq!(shared.hash, source.hash);
    assert_eq!(shared.namespace, source.namespace);
    assert_eq!(local_count(&a, "_coven_file_uploads"), 0, "nothing uploads");
    assert_eq!(owned_paths(&a_store).len(), owned);
    for write in records(&a) {
        b.receive(write.into()).await.unwrap();
    }
    let received = b.file_ref(copy).await.unwrap();
    let FilePlace::Cloud { location, .. } = received.place() else {
        panic!("the new file is stored");
    };
    assert_eq!(crate::file_location::Place::Cloud(location.clone()), source.place);
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// A file this device has no record of gives no new file: the write fails
/// with `FileAbsent`, naming it.
#[tokio::test]
async fn a_new_file_over_a_file_this_device_has_no_record_of_fails() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let unknown = FileId(uuid::Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0099));
    let error = db
        .write_with_files::<_, _, _, _, DbError>(
            move |batch| Ok(batch.existing_file(unknown)),
            move |sql, file| insert(&sql, "photos", 2, CIRCLE, file),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::FileAbsent { file } if file == unknown));
    db.close().await.unwrap();
}

/// Ana makes the store's track device-only on her laptop. Gifts' file over
/// the same bytes is its own record: it stays stored at the same location,
/// and once her switch is published the stored bytes are still no deletion
/// candidate, since Gifts' record names them.
#[tokio::test]
async fn making_the_store_file_device_only_leaves_the_circles_stored() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let originals = tempfile::tempdir().unwrap();
    let original = originals.path().join("track.flac");
    std::fs::write(&original, b"track").unwrap();
    let prepared = crate::prepare_user_file(&original, |_| {}).await.unwrap();
    let track = db
        .write_with_files::<_, _, _, _, DbError>(
            move |batch| Ok(batch.put_user_file("files", UserSource::Cloud(prepared))),
            |sql, file| {
                insert(&sql, "photos", 1, "store", file)?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    complete_uploads(&db).await;
    let copy = share_into(&db, 2, CIRCLE, track).await;
    crate::tests::publish(&db).await.unwrap();
    let crate::file_location::Place::Cloud(location) = record(&db, track).place else {
        panic!("the track is stored");
    };
    let owner = crate::FileDatabase::new(db.clone());
    let music = tempfile::tempdir().unwrap();
    let path = music.path().join("track.flac");
    let reservation = owner
        .record_copies(vec![crate::FolderCopy {
            path: path.clone(),
            temporary: music.path().join(".coven-1.download"),
            folders: vec![],
        }])
        .await
        .unwrap();
    std::fs::write(&path, b"track").unwrap();
    owner
        .place_copy(&coven_foundation::files::observe_metadata(&path).await.unwrap())
        .await
        .unwrap();
    // The test wrote the copy itself, outside any batch.
    let flushed = coven_foundation::files::FlushBatch::new().flush().await.unwrap();
    owner
        .make_device_only(
            vec![crate::DeviceOnlyCopy { file: track, location: location.clone(), path }],
            flushed,
        )
        .await
        .unwrap();
    drop(reservation);
    crate::tests::publish(&db).await.unwrap();
    assert_eq!(record(&db, track).place, crate::file_location::Place::DeviceOnly(store.device()));
    assert_eq!(record(&db, copy).place, crate::file_location::Place::Cloud(location.clone()));
    assert!(!unprotected(&db, store.device()).await.contains(&location.object()));
    db.close().await.unwrap();
}

/// The store's photo and Gifts' file over its bytes each go with their
/// last reference. The stored bytes become a deletion candidate only once
/// neither record names them and both deletes are published.
#[tokio::test]
async fn stored_bytes_go_only_when_neither_record_names_them() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let photo = stored(&db, b"photo").await;
    let copy = share_into(&db, 2, CIRCLE, photo).await;
    let crate::file_location::Place::Cloud(location) = record(&db, photo).place else {
        panic!("the photo is stored");
    };
    crate::tests::publish(&db).await.unwrap();
    let delete = |n: u8| {
        db.write(move |sql| {
            sql.execute(
                "DELETE FROM photos WHERE id=?1",
                [format!("00000000-0000-4000-8000-0000000000{n:02}")],
            )?;
            Ok(())
        })
    };
    delete(1).await.unwrap();
    crate::tests::publish(&db).await.unwrap();
    assert!(!has_record(&db, photo));
    assert!(has_record(&db, copy));
    assert!(!unprotected(&db, store.device()).await.contains(&location.object()));
    delete(2).await.unwrap();
    crate::tests::publish(&db).await.unwrap();
    assert!(!has_record(&db, copy));
    assert!(unprotected(&db, store.device()).await.contains(&location.object()));
    db.close().await.unwrap();
}

/// Ana imports a track she keeps on her laptop only, and puts it in a Gifts
/// row. The new file is on her laptop only too, over the same original,
/// which she reads it from.
#[tokio::test]
async fn a_device_only_source_gives_a_file_on_this_device_with_the_same_original() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let originals = tempfile::tempdir().unwrap();
    let original = originals.path().join("track.flac");
    std::fs::write(&original, b"local track").unwrap();
    let observed = crate::observe_user_file(&original).await.unwrap();
    let track = db
        .write_with_files::<_, _, _, _, DbError>(
            move |batch| Ok(batch.put_user_file("files", UserSource::DeviceOnly(observed))),
            |sql, file| {
                insert(&sql, "photos", 1, "store", file)?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    let copy = share_into(&db, 2, CIRCLE, track).await;
    assert_eq!(
        db.file_ref(copy).await.unwrap().place(),
        FilePlace::DeviceOnly { device: store.device() }
    );
    assert_eq!(db.user_file(copy).await.unwrap().map(|file| file.path), Some(original.clone()));
    let reference = db.file_ref(copy).await.unwrap();
    let local = db.open_local_file(&reference).await.unwrap().expect("its original is here");
    assert_eq!(local.read_at(0, reference.size()).await.unwrap(), b"local track");
    db.close().await.unwrap();
}

/// Only the device holding a file's bytes makes a new file over them. Ana
/// keeps a track on her laptop only; Ben putting it in a Gifts row would
/// make a file no device has bytes for, so his write fails with
/// `NotThisDevice` and changes nothing.
#[tokio::test]
async fn a_new_file_over_a_file_on_another_device_only_fails() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = open(&a_store, Provenance::UserProvided).await;
    let b = open(&b_store, Provenance::UserProvided).await;
    let originals = tempfile::tempdir().unwrap();
    let original = originals.path().join("track.flac");
    std::fs::write(&original, b"local track").unwrap();
    let observed = crate::observe_user_file(&original).await.unwrap();
    let track = a
        .write_with_files::<_, _, _, _, DbError>(
            move |batch| Ok(batch.put_user_file("files", UserSource::DeviceOnly(observed))),
            |sql, file| {
                insert(&sql, "photos", 1, "store", file)?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    for write in records(&a) {
        crate::tests::publish_through(&a, write.header.position.number).await.unwrap();
        b.receive(write.into()).await.unwrap();
    }
    let error = b
        .write_with_files::<_, _, _, _, DbError>(
            move |batch| Ok(batch.existing_file(track)),
            move |sql, file| insert(&sql, "photos", 2, CIRCLE, file),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::NotThisDevice { file } if file == track), "{error:?}");
    assert_eq!(local_count(&b, "photos"), 1);
    assert_eq!(local_count(&b, "coven_files"), 1);
    assert!(records(&b).is_empty());
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// Ana's laptop puts a stored Gifts photo in a store row. The new file is
/// in the store, over the same stored bytes and key, and nothing is
/// uploaded again; Gifts' file stays as it was.
#[tokio::test]
async fn a_store_row_names_a_circles_files_bytes_through_a_new_file() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let photo = db
        .write_with_files::<_, _, _, _, DbError>(
            |batch| Ok(batch.put_file("files", b"photo".to_vec(), Place::Cloud)),
            |sql, file| {
                insert(&sql, "photos", 1, CIRCLE, file)?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    complete_uploads(&db).await;
    let owned = owned_paths(&store).len();
    let copy = share_into(&db, 2, "store", photo).await;
    assert_ne!(copy, photo);
    let (source, shared) = (record(&db, photo), record(&db, copy));
    assert_eq!(
        source.audience,
        coven_merge::Audience::Circle(CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap()))
    );
    assert_eq!(shared.audience, coven_merge::Audience::Store);
    assert!(matches!(shared.place, crate::file_location::Place::Cloud(_)));
    assert_eq!(shared.place, source.place);
    assert_eq!(shared.hash, source.hash);
    assert_eq!(shared.namespace, source.namespace);
    assert_eq!(local_count(&db, "_coven_file_uploads"), 0, "nothing uploads");
    assert_eq!(owned_paths(&store).len(), owned);
    db.close().await.unwrap();
}

/// One write stages a cover for the cloud in a store row and puts it in a
/// Gifts row's bound reference too. The new file shares the staged file's
/// upload: the cover uploads once, its upload holds the write, and its
/// completion switches both files to the same stored bytes.
#[tokio::test]
async fn a_new_file_over_a_file_the_same_write_stages_shares_its_upload() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let (cover, copy) = db
        .write_with_files::<_, _, _, _, DbError>(
            |batch| {
                let cover = batch.put_file("files", b"cover".to_vec(), Place::Cloud);
                Ok((cover, batch.existing_file(cover)))
            },
            |sql, (cover, copy)| {
                insert(&sql, "photos", 1, "store", cover)?;
                insert(&sql, "covers", 2, CIRCLE, copy)?;
                Ok((cover, copy))
            },
        )
        .await
        .unwrap();
    assert_eq!(local_count(&db, "_coven_file_uploads"), 1);
    let write = records(&db).pop().unwrap().header.position.number;
    assert_eq!(
        db.inspect_writer(|sql| crate::file_queue::first_held(sql).unwrap()),
        Some(write),
        "the upload holds the write"
    );
    complete_uploads(&db).await;
    let stored = record(&db, cover).place;
    assert!(matches!(stored, crate::file_location::Place::Cloud(_)));
    assert_eq!(record(&db, copy).place, stored);
    db.close().await.unwrap();
}

/// One write stages a photo for the cloud in a store row, puts it in a
/// Gifts row as a new file over its bytes, and puts that one in another
/// Gifts row as a new file over it. All three share the photo's one upload,
/// whose completion switches them to the same stored bytes.
#[tokio::test]
async fn a_new_file_over_a_new_file_over_a_staged_file_shares_its_upload() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let (photo, gifts, again) = db
        .write_with_files::<_, _, _, _, DbError>(
            |batch| {
                let photo = batch.put_file("files", b"photo".to_vec(), Place::Cloud);
                let gifts = batch.existing_file(photo);
                Ok((photo, gifts, batch.existing_file(gifts)))
            },
            |sql, (photo, gifts, again)| {
                insert(&sql, "photos", 1, "store", photo)?;
                insert(&sql, "photos", 2, CIRCLE, gifts)?;
                insert(&sql, "photos", 3, CIRCLE, again)?;
                Ok((photo, gifts, again))
            },
        )
        .await
        .unwrap();
    assert_eq!(local_count(&db, "_coven_file_uploads"), 1);
    complete_uploads(&db).await;
    let stored = record(&db, photo).place;
    assert!(matches!(stored, crate::file_location::Place::Cloud(_)));
    assert_eq!(record(&db, gifts).place, stored);
    assert_eq!(record(&db, again).place, stored);
    db.close().await.unwrap();
}
