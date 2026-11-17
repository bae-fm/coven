//! The installations resets kept: how many there are, their sizes in
//! `coven_archive_bytes`, that deleting them leaves the store working, and
//! that a deletion that stops partway finishes on the next call (§19.3).
use crate::*;
use coven_foundation::files::StoreDirAccess;
use coven_storage::test_utils::MemoryStorage;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

struct Fixture {
    app: Coven,
    clock: Arc<FixedClock>,
    connector: Arc<crate::tests::Connector>,
    storage: Arc<MemoryStorage>,
    directory: StoreDir,
    _root: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let app = Coven::in_memory(root.path().into());
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
        let storage = Arc::new(MemoryStorage::builder().clock(clock.clone()).build().unwrap());
        let directory = app.create_closed_store("Kept", Arc::new(UuidIds)).await.unwrap();
        let connector = crate::tests::Connector::new(storage.clone());
        Self { app, clock, connector, storage, directory, _root: root }
    }

    fn builder(&self) -> CovenBuilder {
        self.app
            .builder()
            .clock(self.clock.clone())
            .storage_connector(self.connector.clone())
            .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
            .migrations(vec![Migration::addition(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,body TEXT NOT NULL)",
            )])
    }

    /// Set the store up on the fixture's storage, as a reset needs, and
    /// close it.
    async fn connect(&self) {
        let handle = self.open().await;
        handle
            .setup_s3_storage(
                self.storage.config(),
                "member-key".into(),
                SecretText::new("secret".into()),
                |_| {},
            )
            .await
            .unwrap();
        handle.close().await.unwrap();
    }

    async fn open(&self) -> CovenHandle {
        self.builder().open(self.directory.id(), |_| ()).await.unwrap()
    }

    /// Reset the store's device, which moves the installation it had into an
    /// archive, and return the open replacement.
    async fn reset(&self) -> CovenHandle {
        self.builder().reset_device(self.directory.id(), "Replacement", |_| ()).await.unwrap()
    }

    fn archives(&self) -> Vec<PathBuf> {
        let folder = self.directory.database_path().parent().unwrap().to_owned();
        let mut found: Vec<PathBuf> = std::fs::read_dir(folder)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name().unwrap().to_str().unwrap().starts_with("discarded-device-")
            })
            .collect();
        found.sort();
        found
    }
}

async fn note(handle: &CovenHandle, id: &'static str) {
    handle
        .write(move |sql| {
            sql.execute("INSERT INTO notes VALUES(?1,'body')", [id])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
}

async fn notes(handle: &CovenHandle) -> Vec<String> {
    handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query("SELECT id FROM notes ORDER BY id", [], |r| r.get(0))?)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn deleting_removes_the_archive_a_reset_kept_and_the_store_still_works() {
    let f = Fixture::new().await;
    f.connect().await;
    let handle = f.reset().await;
    note(&handle, "before").await;
    assert_eq!(f.archives().len(), 1);
    handle.delete_discarded_devices().await.unwrap();
    assert!(f.archives().is_empty());
    note(&handle, "after").await;
    assert_eq!(notes(&handle).await, ["after", "before"]);
    handle.close().await.unwrap();
    let reopened = f.open().await;
    assert_eq!(notes(&reopened).await, ["after", "before"]);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn deleting_removes_the_archives_of_every_reset() {
    let f = Fixture::new().await;
    f.connect().await;
    f.reset().await.close().await.unwrap();
    let handle = f.reset().await;
    assert_eq!(f.archives().len(), 2);
    handle.delete_discarded_devices().await.unwrap();
    assert!(f.archives().is_empty());
    handle.close().await.unwrap();
}

#[tokio::test]
async fn deleting_with_no_archive_succeeds_and_can_repeat() {
    let f = Fixture::new().await;
    let handle = f.open().await;
    handle.delete_discarded_devices().await.unwrap();
    handle.delete_discarded_devices().await.unwrap();
    note(&handle, "kept").await;
    handle.close().await.unwrap();
    let f = Fixture::new().await;
    f.connect().await;
    let handle = f.reset().await;
    handle.delete_discarded_devices().await.unwrap();
    handle.delete_discarded_devices().await.unwrap();
    assert!(f.archives().is_empty());
    handle.close().await.unwrap();
}

/// A folder in the archive that refuses removal of its contents stops the
/// removal after part of the archive has gone. The archive and its row left
/// the store's directory first, and the next call removes the rest.
#[cfg(unix)]
#[tokio::test]
async fn a_deletion_that_stops_partway_leaves_the_rest_for_the_next_call() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new().await;
    f.connect().await;
    let handle = f.reset().await;
    let archive = f.archives().remove(0);
    let stuck = archive.join("stuck");
    std::fs::create_dir(&stuck).unwrap();
    std::fs::write(stuck.join("inside"), b"bytes").unwrap();
    std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o500)).unwrap();
    let first = handle.delete_discarded_devices().await;
    if first.is_ok() {
        // The process can remove what its permissions refuse, as root does.
        assert!(f.archives().is_empty());
        handle.close().await.unwrap();
        return;
    }
    assert!(matches!(first, Err(CovenError::File(_))), "{first:?}");
    assert!(f.archives().is_empty());
    assert!(archive_rows(&handle).await.is_empty());
    let removing = archive.parent().unwrap().join("removing-discarded-devices");
    let stuck = removing.join(archive.file_name().unwrap()).join("stuck");
    assert!(stuck.join("inside").exists());
    // The store itself is untouched by the failed call.
    note(&handle, "still writes").await;
    std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o700)).unwrap();
    handle.delete_discarded_devices().await.unwrap();
    assert!(f.archives().is_empty());
    assert!(!removing.exists());
    handle.close().await.unwrap();
}

/// Each archive's row in `coven_archive_bytes`, by name.
async fn archive_rows(handle: &CovenHandle) -> Vec<(String, i64)> {
    handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query(
                "SELECT archive,bytes FROM coven_archive_bytes ORDER BY archive",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
        })
        .await
        .unwrap()
}

/// The size of everything under `path`.
fn tree(path: &std::path::Path) -> i64 {
    if path.is_file() {
        return i64::try_from(std::fs::metadata(path).unwrap().len()).unwrap();
    }
    std::fs::read_dir(path).unwrap().map(|entry| tree(&entry.unwrap().path())).sum()
}

/// A reset's archive appears in `coven_archive_bytes` with its size as the
/// reset's notice commits, and leaves it as coven removes the archive
/// (§17.8, §19.3).
#[tokio::test]
async fn each_archive_has_a_row_with_its_size_until_it_is_removed() {
    let f = Fixture::new().await;
    f.connect().await;
    f.reset().await.close().await.unwrap();
    let handle = f.reset().await;
    let archives = f.archives();
    assert_eq!(archives.len(), 2);
    let expected: Vec<(String, i64)> = archives
        .iter()
        .map(|path| (path.file_name().unwrap().to_str().unwrap().to_owned(), tree(path)))
        .collect();
    assert!(expected.iter().all(|(_, bytes)| *bytes > 0));
    assert_eq!(archive_rows(&handle).await, expected);
    handle.delete_discarded_devices().await.unwrap();
    assert!(f.archives().is_empty());
    assert!(archive_rows(&handle).await.is_empty());
    handle.close().await.unwrap();
}
