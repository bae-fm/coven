use super::*;
use crate::files::{
    FileArea, FileName, StoreDirAccess, StoreDirectories, StoreDirectoriesAccess, StoreFile,
};
use crate::id_source::UuidIds;

struct Fixture {
    _temp: tempfile::TempDir,
    layout: StoreDirectories,
    store: StoreDir,
    path: PathBuf,
}

/// A published store with a database, a stored location and an unsent file.
fn store(n: u128) -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(temp.path().into());
    let id = StoreId(uuid::Uuid::from_u128(n));
    let store = layout.create_store_dir(id, &UuidIds).unwrap();
    std::fs::write(store.database_path(), b"old database").unwrap();
    store
        .owned_file(StoreFile::StorageSettings)
        .replace(b"location")
        .unwrap();
    store
        .file(FileArea::AppProvided, &FileName::new("photo").unwrap())
        .replace(b"unsent")
        .unwrap();
    let path = temp.path().join("stores").join(id.to_string());
    Fixture {
        _temp: temp,
        layout,
        store,
        path,
    }
}

/// The replacement's loaded installation: a database and a cached file.
fn load(replacement: &ReplacementStore) {
    let directory = replacement.directory();
    std::fs::write(directory.database_path(), b"new database").unwrap();
    directory
        .file(FileArea::Cache, &FileName::new("chunk").unwrap())
        .replace(b"cached")
        .unwrap();
}

fn archive(f: &Fixture, replacement: &ReplacementStore) -> PathBuf {
    f.path.join(format!(
        "discarded-device-{}",
        replacement.device_reset().old_device.0
    ))
}

#[tokio::test]
async fn the_store_stays_as_it_was_until_the_loaded_replacement_swaps_in() {
    for discard in [false, true] {
        let f = store(1);
        let old = f.store.settings().unwrap().device_id;
        let replacement = f
            .layout
            .begin_replacement(f.store.id(), &UuidIds)
            .await
            .unwrap();
        let reset = replacement.device_reset();
        assert_eq!(reset.old_device, old);
        assert_ne!(reset.new_device, old);
        // While it loads, the store's writer lock is held, its files are
        // untouched and it still lists; nothing names a reset.
        assert!(matches!(
            f.store.lock_exclusive(),
            Err(StoreLockError::AlreadyOpen(_))
        ));
        load(&replacement);
        assert_eq!(
            std::fs::read(f.store.database_path()).unwrap(),
            b"old database"
        );
        assert_eq!(f.layout.store_ids().await.unwrap(), [f.store.id()]);
        assert_eq!(f.store.device_reset().unwrap(), None);
        assert_eq!(
            replacement.directory().settings().unwrap().device_id,
            reset.new_device
        );
        replacement.swap().unwrap();
        assert!(replacement.is_swapped());
        // The marker now refuses other opens; the new installation opens
        // through the replacement's own directory.
        assert!(matches!(
            f.store.lock_exclusive(),
            Err(StoreLockError::BootstrapPending(_))
        ));
        drop(replacement.directory().lock_exclusive().unwrap());
        assert_eq!(f.store.device_reset().unwrap(), Some(reset));
        assert!(f.layout.store_ids().await.unwrap().is_empty());
        let directory = replacement.directory();
        assert_eq!(directory.settings().unwrap().device_id, reset.new_device);
        assert_eq!(
            std::fs::read(directory.database_path()).unwrap(),
            b"new database"
        );
        assert_eq!(
            directory
                .file(FileArea::Cache, &FileName::new("chunk").unwrap())
                .read_optional()
                .unwrap(),
            Some(b"cached".to_vec())
        );
        // The store's location stays; its unsent file moved to the archive.
        assert_eq!(
            directory
                .owned_file(StoreFile::StorageSettings)
                .read_optional()
                .unwrap(),
            Some(b"location".to_vec())
        );
        assert_eq!(
            directory
                .file(FileArea::AppProvided, &FileName::new("photo").unwrap())
                .read_optional()
                .unwrap(),
            None
        );
        assert_eq!(
            std::fs::read(replacement.discarded_database_path()).unwrap(),
            b"old database"
        );
        assert_eq!(
            std::fs::read(archive(&f, &replacement).join("files").join("photo")).unwrap(),
            b"unsent"
        );
        if discard {
            replacement.remove_replaced().unwrap();
        }
        replacement.publish().unwrap();
        assert_eq!(archive(&f, &replacement).exists(), !discard);
        assert!(!f.path.join(".coven-replacement").exists());
        assert_eq!(f.store.device_reset().unwrap(), None);
        drop(replacement);
        assert_eq!(f.layout.store_ids().await.unwrap(), [f.store.id()]);
        assert_eq!(f.store.settings().unwrap().device_id, reset.new_device);
        f.store.lock_exclusive().unwrap();
    }
}

#[tokio::test]
async fn an_unswapped_replacement_resumes_with_its_id_or_is_cancelled() {
    let f = store(2);
    let first = f
        .layout
        .begin_replacement(f.store.id(), &UuidIds)
        .await
        .unwrap();
    let reset = first.device_reset();
    load(&first);
    assert!(matches!(
        f.layout.begin_replacement(f.store.id(), &UuidIds).await,
        Err(BootstrapDirectoryError::Lock(StoreLockError::AlreadyOpen(
            _
        )))
    ));
    drop(first);
    // The store opens meanwhile, as before.
    drop(f.store.lock_exclusive().unwrap());
    let resumed = f
        .layout
        .begin_replacement(f.store.id(), &UuidIds)
        .await
        .unwrap();
    assert_eq!(resumed.device_reset(), reset);
    assert_eq!(
        std::fs::read(resumed.directory().database_path()).unwrap(),
        b"new database"
    );
    resumed.cancel().unwrap();
    assert!(!f.path.join(".coven-replacement").exists());
    assert_eq!(
        std::fs::read(f.store.database_path()).unwrap(),
        b"old database"
    );
    let fresh = f
        .layout
        .begin_replacement(f.store.id(), &UuidIds)
        .await
        .unwrap();
    assert_ne!(fresh.device_reset().new_device, reset.new_device);
}

#[tokio::test]
async fn a_reader_refuses_the_swap_and_moves_nothing() {
    let f = store(3);
    let replacement = f
        .layout
        .begin_replacement(f.store.id(), &UuidIds)
        .await
        .unwrap();
    load(&replacement);
    let reader = f.store.lock_read_only().unwrap();
    assert!(matches!(
        replacement.swap(),
        Err(BootstrapDirectoryError::Lock(StoreLockError::AlreadyOpen(
            _
        )))
    ));
    drop(reader);
    assert!(!replacement.is_swapped());
    assert_eq!(f.store.device_reset().unwrap(), None);
    assert_eq!(
        std::fs::read(f.store.database_path()).unwrap(),
        b"old database"
    );
    replacement.swap().unwrap();
}

#[tokio::test]
async fn unswapping_puts_the_old_installation_back() {
    let f = store(4);
    let old = f.store.settings().unwrap();
    let replacement = f
        .layout
        .begin_replacement(f.store.id(), &UuidIds)
        .await
        .unwrap();
    let reset = replacement.device_reset();
    load(&replacement);
    replacement.swap().unwrap();
    replacement.unswap().await.unwrap();
    assert_eq!(f.store.settings().unwrap(), old);
    assert_eq!(f.store.device_reset().unwrap(), None);
    assert_eq!(
        std::fs::read(f.store.database_path()).unwrap(),
        b"old database"
    );
    assert_eq!(
        f.store
            .file(FileArea::AppProvided, &FileName::new("photo").unwrap())
            .read_optional()
            .unwrap(),
        Some(b"unsent".to_vec())
    );
    assert_eq!(f.layout.store_ids().await.unwrap(), [f.store.id()]);
    // The loaded installation waits for the next call, with its id.
    let resumed = f
        .layout
        .begin_replacement(f.store.id(), &UuidIds)
        .await
        .unwrap();
    assert_eq!(resumed.device_reset(), reset);
    assert_eq!(
        std::fs::read(resumed.directory().database_path()).unwrap(),
        b"new database"
    );
}

#[tokio::test]
async fn an_interrupted_swap_is_finished_by_the_next_call() {
    let f = store(5);
    let replacement = f
        .layout
        .begin_replacement(f.store.id(), &UuidIds)
        .await
        .unwrap();
    let reset = replacement.device_reset();
    load(&replacement);
    drop(replacement);
    // The swap recorded its decision and moved the database aside, then stopped.
    device_reset::record(&f.path, &reset).unwrap();
    let archive = f
        .path
        .join(format!("discarded-device-{}", reset.old_device.0));
    std::fs::create_dir(&archive).unwrap();
    std::fs::rename(f.store.database_path(), archive.join("store.db")).unwrap();
    assert!(matches!(
        f.store.unfinished_device_reset(),
        Ok(Some(found)) if found == reset
    ));
    let resumed = f
        .layout
        .begin_replacement(f.store.id(), &UuidIds)
        .await
        .unwrap();
    assert!(resumed.is_swapped());
    assert_eq!(resumed.device_reset(), reset);
    let directory = resumed.directory();
    assert_eq!(directory.settings().unwrap().device_id, reset.new_device);
    assert_eq!(
        std::fs::read(directory.database_path()).unwrap(),
        b"new database"
    );
    assert_eq!(
        std::fs::read(archive.join("files").join("photo")).unwrap(),
        b"unsent"
    );
    assert_eq!(
        std::fs::read(archive.join("store.db")).unwrap(),
        b"old database"
    );
    resumed.publish().unwrap();
    assert_eq!(f.store.device_reset().unwrap(), None);
}

#[tokio::test]
async fn a_record_left_by_interrupted_publication_begins_a_new_replacement() {
    let f = store(6);
    let replacement = f
        .layout
        .begin_replacement(f.store.id(), &UuidIds)
        .await
        .unwrap();
    let reset = replacement.device_reset();
    load(&replacement);
    replacement.swap().unwrap();
    // Publication removed the marker; the record removal did not happen.
    std::fs::remove_dir_all(f.path.join(".coven-replacement")).unwrap();
    std::fs::remove_file(f.path.join(".coven-bootstrap")).unwrap();
    drop(replacement);
    assert_eq!(f.store.device_reset().unwrap(), Some(reset));
    let next = f
        .layout
        .begin_replacement(f.store.id(), &UuidIds)
        .await
        .unwrap();
    assert!(!next.is_swapped());
    assert_eq!(next.device_reset().old_device, reset.new_device);
    assert_eq!(f.store.device_reset().unwrap(), None);
    drop(next);
    assert_eq!(f.store.unfinished_device_reset().unwrap(), None);
}
