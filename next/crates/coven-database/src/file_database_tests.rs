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
    let name = FileName::new("prepared").unwrap();
    let reservation = owner.reserve_upload_bytes(name.clone()).await.unwrap();
    {
        let slot = database.inner.read().unwrap();
        slot.as_ref()
            .unwrap()
            .directory
            .file(FileArea::AppProvided, &name)
            .replace(b"provider bytes")
            .unwrap();
    }
    reservation
        .publish(id, SecretBytes::new(b"opaque identity".to_vec()))
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
