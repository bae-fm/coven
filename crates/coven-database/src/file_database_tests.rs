use super::*;
use crate::{
    file_write::tests::{attach, tables},
    tests::TestStore,
    Provenance,
};

#[tokio::test]
async fn an_uploading_write_refuses_a_trigger_that_removes_its_file() {
    let store = TestStore::new();
    let mut declarations = tables(Provenance::AppProvided);
    declarations[0] = declarations[0].clone().shared_trigger("remove_uploaded");
    let database=store.schema(declarations,"CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,title TEXT); CREATE TRIGGER remove_uploaded AFTER UPDATE OF location ON files WHEN NOT coven_applying() AND NEW.location LIKE 'uploaded %' BEGIN DELETE FROM files WHERE id=NEW.id; END;").await.unwrap();
    attach(&database, b"source".to_vec(), true).await.unwrap();
    let owner = FileDatabase::new(database.clone());
    let file = database.file_ref("files", "7").await.unwrap();
    owner
        .enqueue(std::slice::from_ref(&file), SystemTime::UNIX_EPOCH)
        .await
        .unwrap();
    let id = owner.uploads().await.unwrap()[0].id;
    owner
        .record_upload_identity(id, SecretBytes::new(b"opaque identity".to_vec()))
        .await
        .unwrap();
    owner.record_stored(id).await.unwrap();
    let location = SecretText::new(format!(
        "uploaded 1 00000000-0000-4000-8000-000000000123 {}",
        "11".repeat(32)
    ));
    assert!(matches!(
        owner.finish_upload(id, &file, location).await,
        Err(DbError::FileRowRemoved { .. })
    ));
    assert_eq!(database.file_ref("files", "7").await.unwrap(), file);
    assert!(owner.uploads().await.unwrap()[0].stored);
    database.close().await.unwrap();
}

#[tokio::test]
async fn chunk_hashes_belong_to_the_queued_version_and_retire_with_it() {
    use crate::file_write::tests::{local_count, SCHEMA};
    use coven_crypto::ContentHasher;
    use coven_format::file::DEFAULT_CHUNK_SIZE;
    for provenance in [Provenance::UserProvided, Provenance::AppProvided] {
        for remove in [false, true] {
            let store = TestStore::new();
            let db = store
                .schema(tables(provenance.clone()), SCHEMA)
                .await
                .unwrap();
            let bytes = vec![17; DEFAULT_CHUNK_SIZE as usize + 1];
            let original = tempfile::NamedTempFile::new().unwrap();
            match provenance {
                Provenance::AppProvided => attach(&db, bytes.clone(), true).await.unwrap(),
                Provenance::UserProvided => {
                    std::fs::write(original.path(), &bytes).unwrap();
                    let prepared = crate::prepare_user_file(original.path(), |_| {})
                        .await
                        .unwrap();
                    let size = bytes.len() as i64;
                    db.write(move |sql| {
                        sql.execute("INSERT INTO files(id,size) VALUES('7',?1)", [size])?;
                        sql.register_user_file("files", "7", prepared)
                    })
                    .await
                    .unwrap();
                }
            }
            assert_eq!(local_count(&db, "_coven_file_chunks"), 2);
            let file = db.file_ref("files", "7").await.unwrap();
            let owner = FileDatabase::new(db.clone());
            for _ in 0..2 {
                owner
                    .enqueue(std::slice::from_ref(&file), SystemTime::UNIX_EPOCH)
                    .await
                    .unwrap();
            }
            let id = owner.uploads().await.unwrap()[0].id;
            assert_eq!(local_count(&db, "_coven_file_upload_chunks"), 2);
            owner
                .record_upload_identity(id, SecretBytes::new(b"identity".to_vec()))
                .await
                .unwrap();
            assert!(owner
                .record_upload_identity(id, SecretBytes::new(b"different".to_vec()))
                .await
                .is_err());
            assert_eq!(
                owner.uploads().await.unwrap()[0]
                    .identity
                    .as_ref()
                    .unwrap()
                    .as_bytes(),
                b"identity"
            );
            if remove {
                db.write(|sql| {
                    sql.execute("DELETE FROM files", [])?;
                    Ok(())
                })
                .await
                .unwrap();
                assert_eq!(local_count(&db, "_coven_file_chunks"), 0);
            }
            for (index, chunk) in bytes.chunks(DEFAULT_CHUNK_SIZE as usize).enumerate() {
                let mut expected = ContentHasher::new();
                expected.update(chunk);
                assert_eq!(
                    owner.upload_chunk_hash(id, index as u64).await.unwrap(),
                    expected.finish()
                );
            }
            owner.record_stored(id).await.unwrap();
            let location = SecretText::new(format!(
                "uploaded 1 00000000-0000-4000-8000-000000000123 {}",
                "11".repeat(32)
            ));
            assert_eq!(
                owner.finish_upload(id, &file, location).await.unwrap(),
                !remove
            );
            if remove {
                assert_eq!(local_count(&db, "_coven_file_upload_chunks"), 2);
                db.retire_unused_file(id).await.unwrap();
            }
            assert_eq!(local_count(&db, "_coven_file_chunks"), 0);
            assert_eq!(local_count(&db, "_coven_file_upload_chunks"), 0);
            assert!(owner.uploads().await.unwrap().is_empty());
            db.close().await.unwrap();
        }
    }
}
