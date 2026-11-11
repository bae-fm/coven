//! The installations resets kept: how many there are, that deleting them
//! leaves the store working, and that a deletion that stops partway finishes
//! on the next call (§19.3).
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
        let connector = crate::tests::Connector::new(storage);
        Self { app, clock, connector, directory, _root: root }
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
    f.open().await.close().await.unwrap();
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
    f.open().await.close().await.unwrap();
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
    f.open().await.close().await.unwrap();
    let handle = f.reset().await;
    handle.delete_discarded_devices().await.unwrap();
    handle.delete_discarded_devices().await.unwrap();
    assert!(f.archives().is_empty());
    handle.close().await.unwrap();
}

/// A folder in the archive that refuses removal of its contents stops the
/// removal after part of the archive has gone.
#[cfg(unix)]
#[tokio::test]
async fn a_deletion_that_stops_partway_leaves_the_archive_for_the_next_call() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new().await;
    f.open().await.close().await.unwrap();
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
    assert_eq!(f.archives(), std::slice::from_ref(&archive));
    assert!(stuck.join("inside").exists());
    // The store itself is untouched by the failed call.
    note(&handle, "still writes").await;
    std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o700)).unwrap();
    handle.delete_discarded_devices().await.unwrap();
    assert!(f.archives().is_empty());
    handle.close().await.unwrap();
}
