//! What the store keeps on this device, measured (§17.8, §19.3): the
//! directory's own files, the declared namespaces with their budgets, and
//! the archives resets kept, as one measurement and as they change.
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
            .namespaces(vec![Namespace::new("files", Provenance::AppProvided, 123)])
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

    fn folder(&self) -> PathBuf {
        self.directory.database_path().parent().unwrap().to_owned()
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

/// The sizes of every file under `path`.
fn tree(path: &std::path::Path) -> u64 {
    let metadata = std::fs::symlink_metadata(path).unwrap();
    if metadata.is_file() {
        return metadata.len();
    }
    std::fs::read_dir(path).unwrap().map(|entry| tree(&entry.unwrap().path())).sum()
}

/// The sizes of the directory's files outside the cache and the archives.
fn library(folder: &std::path::Path) -> u64 {
    std::fs::read_dir(folder)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            name != "cache"
                && name != ".coven-replacement"
                && !name.starts_with("discarded-device-")
        })
        .map(|path| tree(&path))
        .sum()
}

#[tokio::test]
async fn the_library_is_the_directorys_files_outside_the_cache_and_the_archives() {
    let f = Fixture::new().await;
    let handle = f.open().await;
    std::fs::write(f.folder().join("cache").join("cached"), vec![0; 1000]).unwrap();
    let measured = handle.local_bytes().await.unwrap();
    assert_eq!(measured.library, library(&f.folder()));
    assert!(measured.library > 0);
    assert_eq!(
        measured.namespaces,
        [NamespaceBytes { namespace: "files".into(), budget: 123, cached: 0, pinned: 0 }]
    );
    assert_eq!(measured.pinning, 0);
    assert_eq!(measured.before_reset, None);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn the_archives_a_reset_kept_are_measured_until_they_are_deleted() {
    let f = Fixture::new().await;
    f.connect().await;
    f.reset().await.close().await.unwrap();
    let handle = f.reset().await;
    let kept: u64 = f.archives().iter().map(|archive| tree(archive)).sum();
    assert!(kept > 0);
    let measured = handle.local_bytes().await.unwrap();
    assert_eq!(measured.before_reset, Some(kept));
    assert_eq!(measured.library, library(&f.folder()), "the archives are not the library");
    let mut watch = handle.subscribe_local_bytes();
    let first = watch.next().await.unwrap().unwrap();
    assert_eq!(first.before_reset, Some(kept));
    handle.delete_discarded_devices().await.unwrap();
    let after = watch.next().await.unwrap().unwrap();
    assert_eq!(after.before_reset, None);
    assert_eq!(handle.local_bytes().await.unwrap().before_reset, None);
    watch.cancel();
    handle.close().await.unwrap();
}

#[tokio::test]
async fn a_budget_change_reaches_the_watch() {
    let f = Fixture::new().await;
    let handle = f.open().await;
    let mut watch = handle.subscribe_local_bytes();
    assert_eq!(watch.next().await.unwrap().unwrap().namespaces[0].budget, 123);
    handle.set_cache_budget("files", 77).await.unwrap();
    assert_eq!(watch.next().await.unwrap().unwrap().namespaces[0].budget, 77);
    watch.cancel();
    handle.close().await.unwrap();
}
