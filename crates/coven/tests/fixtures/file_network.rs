//! A network whose one synced table references app-provided files, and the
//! calls its tests stage, await and find those files with.
use super::{Device, Network};
use crate::*;
use coven_storage::Storage;
use std::time::Duration;

/// One plain table and one whose rows reference app-provided files in the
/// `files` namespace every network declares.
pub(crate) fn file_schema() -> (Vec<SyncedTable>, Vec<Migration>) {
    (
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("files", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
        ],
        vec![Migration::addition(1, "records", "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY, label TEXT NOT NULL UNIQUE, value INTEGER NOT NULL);
             CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY, file TEXT);")],
    )
}

/// The namespaces every network declares: app-provided files, and the
/// user's own tracks.
pub(crate) fn namespaces() -> Vec<Namespace> {
    vec![
        Namespace::new("files", Provenance::AppProvided, 1 << 30),
        Namespace::new("tracks", Provenance::UserProvided, 1 << 30),
    ]
}

pub(crate) async fn file_network() -> Network {
    let (tables, migrations) = file_schema();
    Network::with_schema(coven_storage::CloudProvider::S3, tables, migrations).await
}

/// Stage these bytes for row `id`; returns the new file's id.
pub(crate) async fn attach(handle: &CovenHandle, id: &str, bytes: Vec<u8>) -> FileId {
    let row = id.to_owned();
    handle
        .write_with_files(
            move |batch| {
                Ok(batch.put_file(
                    "files",
                    FileSource::Stream(Box::pin(std::io::Cursor::new(bytes))),
                    Place::Cloud,
                ))
            },
            move |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (&row, file))?;
                Ok(file)
            },
        )
        .await
        .unwrap()
}

pub(crate) async fn wait_for_empty_queue(handle: &CovenHandle) {
    // How many files the queue holds, as coven_uploads lists them.
    let mut uploads = handle.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM coven_uploads", [], |r| r.get::<_, i64>(0))?)
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while uploads.next().await.expect("the live query answers").unwrap() != 0 {}
    })
    .await
    .unwrap();
}

/// Whether the device's record of `file` says it is stored, and storage
/// holds it.
pub(crate) async fn file_stored(device: &Device, file: FileId) -> bool {
    let file = device.handle.file_ref(file).await.unwrap();
    let FilePlace::Cloud { location, .. } = file.place() else {
        return false;
    };
    let path =
        coven_storage::ObjectPath::file(device.store, location.uploader(), location.object());
    device.storage.status(&path).await.unwrap().is_some()
}

/// Where the upload of `file` sends its bytes, while it waits.
pub(crate) async fn upload_path(device: &Device, file: FileId) -> coven_storage::ObjectPath {
    let location = device.handle.test_upload_location(file).await;
    coven_storage::ObjectPath::file(device.store, location.uploader(), location.object())
}
