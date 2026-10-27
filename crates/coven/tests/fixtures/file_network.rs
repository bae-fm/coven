//! A network whose one synced table carries app-provided files, and the
//! calls its tests attach, await and find those files with.
use super::{Device, Network};
use crate::*;
use coven_storage::Storage;
use std::time::Duration;

/// One plain table and one carrying app-provided files.
pub(crate) fn file_schema() -> (Vec<SyncedTable>, Vec<Migration>) {
    (
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
                "files",
                1 << 30,
                Provenance::AppProvided
            )),
        ],
        vec![Migration::addition(1, "records", "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY, label TEXT NOT NULL UNIQUE, value INTEGER NOT NULL);
             CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY, size INTEGER, hash BLOB, location TEXT);")],
    )
}

pub(crate) async fn file_network() -> Network {
    let (tables, migrations) = file_schema();
    Network::with_schema(coven_storage::CloudProvider::S3, tables, migrations).await
}

pub(crate) async fn attach(handle: &CovenHandle, id: &str, bytes: Vec<u8>) {
    let (file, row) = (id.to_owned(), id.to_owned());
    handle
        .write_with_files(
            move |batch| {
                batch.put_file(
                    "files",
                    file,
                    FileSource::Stream(Box::pin(std::io::Cursor::new(bytes))),
                );
                Ok(())
            },
            move |sql| {
                sql.execute("INSERT INTO files(id) VALUES(?1)", [&row])?;
                Ok(())
            },
        )
        .await
        .unwrap();
}

pub(crate) async fn wait_for_empty_queue(handle: &CovenHandle) {
    let mut uploads = handle.subscribe_uploads();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !uploads
            .next()
            .await
            .expect("the live query answers")
            .unwrap()
            .files
            .is_empty()
        {}
    })
    .await
    .unwrap();
}

pub(crate) async fn file_stored(device: &Device, id: &str) -> bool {
    let file = device.handle.file_ref("files", id).await.unwrap();
    let path = coven_storage::ObjectPath::file(device.store, file.uploader(), file.id());
    device.storage.status(&path).await.unwrap().is_some()
}
