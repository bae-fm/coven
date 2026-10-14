use coven_storage::{test_utils::MemoryStorage, Storage};
use std::sync::Arc;

#[tokio::test]
async fn authored_headers_survive_publication_and_reopen_without_a_download() {
    use crate::device_log_sync::tests::{group, STORE_ID};
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut devices = group(storage.clone(), 1).await;
    devices[0]
        .db
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('id','title','body')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let record = devices[0]
        .db
        .test_queued_writes()
        .await
        .unwrap()
        .pop()
        .unwrap();
    let header = coven_format::write_stream::WriteEncoder::new(&record)
        .unwrap()
        .header()
        .clone();
    devices[0].writes.upload_writes().await.unwrap();
    devices[0].reopen(storage.clone(),vec![coven_database::SyncedTable::new("notes",coven_database::RowIdentity::SharedKey)],vec![coven_database::Migration::sql(1,"notes","CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL)")]).await;
    let path = crate::write_seal::path(STORE_ID, record.header.position);
    let object = storage.status(&path).await.unwrap().unwrap();
    let before = storage.reads().await;
    assert_eq!(
        devices[0].sync.open_write_header(&object).await.unwrap(),
        header
    );
    assert_eq!(
        storage.reads().await,
        before,
        "authored facts need no remote header read"
    );
}
