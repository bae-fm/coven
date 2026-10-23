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
            .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
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
        assert_eq!(f.store.unfinished_replacement().unwrap(), None);
        assert_eq!(
            replacement.directory().settings().unwrap().device_id,
            reset.new_device
        );
        replacement.swap("Household".into()).unwrap();
        assert!(replacement.is_swapped());
        // The marker now refuses other opens, naming the call that finishes
        // the swap; the new installation opens through the replacement's own
        // directory. The store still lists, so the app can offer to finish it.
        assert!(matches!(
            f.store.lock_exclusive(),
            Err(StoreLockError::ReplacementUnfinished { store, by: ReplacedBy::Reset })
                if store == f.store.id()
        ));
        drop(replacement.directory().lock_exclusive().unwrap());
        assert_eq!(
            f.store.unfinished_replacement().unwrap(),
            Some(ReplacementRecord {
                reset,
                by: ReplacedBy::Reset,
                name: "Household".into(),
            })
        );
        assert_eq!(f.layout.store_ids().await.unwrap(), [f.store.id()]);
        // A replacement under way is the store's, not a first installation.
        assert!(f
            .layout
            .unfinished_first_installations()
            .await
            .unwrap()
            .is_empty());
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
        assert_eq!(f.store.unfinished_replacement().unwrap(), None);
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
        .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
        .await
        .unwrap();
    let reset = first.device_reset();
    load(&first);
    assert!(matches!(
        f.layout
            .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
            .await,
        Err(BootstrapDirectoryError::Lock(StoreLockError::AlreadyOpen(
            _
        )))
    ));
    drop(first);
    // The store opens meanwhile, as before.
    drop(f.store.lock_exclusive().unwrap());
    let resumed = f
        .layout
        .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
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
        .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
        .await
        .unwrap();
    assert_ne!(fresh.device_reset().new_device, reset.new_device);
}

#[tokio::test]
async fn a_reader_refuses_the_swap_and_moves_nothing() {
    let f = store(3);
    let replacement = f
        .layout
        .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
        .await
        .unwrap();
    load(&replacement);
    let reader = f.store.lock_read_only().unwrap();
    assert!(matches!(
        replacement.swap("Household".into()),
        Err(BootstrapDirectoryError::Lock(StoreLockError::AlreadyOpen(
            _
        )))
    ));
    drop(reader);
    assert!(!replacement.is_swapped());
    assert_eq!(f.store.unfinished_replacement().unwrap(), None);
    assert_eq!(
        std::fs::read(f.store.database_path()).unwrap(),
        b"old database"
    );
    replacement.swap("Household".into()).unwrap();
}

#[tokio::test]
async fn unswapping_puts_the_old_installation_back() {
    let f = store(4);
    let old = f.store.settings().unwrap();
    let replacement = f
        .layout
        .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
        .await
        .unwrap();
    let reset = replacement.device_reset();
    load(&replacement);
    replacement.swap("Household".into()).unwrap();
    replacement.unswap().await.unwrap();
    assert_eq!(f.store.settings().unwrap(), old);
    assert_eq!(f.store.unfinished_replacement().unwrap(), None);
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
        .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
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
        .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
        .await
        .unwrap();
    let reset = replacement.device_reset();
    load(&replacement);
    drop(replacement);
    // The swap recorded its decision and moved the database aside, then stopped.
    device_reset::record(
        &f.path,
        &ReplacementRecord {
            reset,
            by: ReplacedBy::Reset,
            name: "Household".into(),
        },
    )
    .unwrap();
    let archive = f
        .path
        .join(format!("discarded-device-{}", reset.old_device.0));
    std::fs::create_dir(&archive).unwrap();
    std::fs::rename(f.store.database_path(), archive.join("store.db")).unwrap();
    assert!(matches!(
        f.store.unfinished_replacement(),
        Ok(Some(found)) if found.reset == reset
    ));
    // Before the marker, only the record keeps the store from listing as
    // finished; it lists with the call that finishes it.
    assert_eq!(f.layout.store_ids().await.unwrap(), [f.store.id()]);
    let resumed = f
        .layout
        .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
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
    assert_eq!(f.store.unfinished_replacement().unwrap(), None);
}

#[tokio::test]
async fn a_record_left_by_interrupted_publication_begins_a_new_replacement() {
    let f = store(6);
    let replacement = f
        .layout
        .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
        .await
        .unwrap();
    let reset = replacement.device_reset();
    load(&replacement);
    replacement.swap("Household".into()).unwrap();
    // Publication removed the marker; the record removal did not happen.
    std::fs::remove_dir_all(f.path.join(".coven-replacement")).unwrap();
    std::fs::remove_file(f.path.join(".coven-bootstrap")).unwrap();
    drop(replacement);
    assert_eq!(
        device_reset::read(&f.path)
            .unwrap()
            .map(|record| record.reset),
        Some(reset)
    );
    assert_eq!(f.store.unfinished_replacement().unwrap(), None);
    drop(f.store.lock_exclusive().unwrap());
    let next = f
        .layout
        .begin_replacement(f.store.id(), ReplacedBy::Reset, &UuidIds)
        .await
        .unwrap();
    assert!(!next.is_swapped());
    assert_eq!(next.device_reset().old_device, reset.new_device);
    assert_eq!(device_reset::read(&f.path).unwrap(), None);
}

/// A swap one call began is finished only by that call: the other is refused
/// before anything moves, whichever began it (§19.3).
#[tokio::test]
async fn the_other_call_is_refused_by_a_swap_under_way() {
    for (began, other) in [
        (ReplacedBy::Reset, ReplacedBy::Restore),
        (ReplacedBy::Restore, ReplacedBy::Reset),
    ] {
        let f = store(7);
        let replacement = f
            .layout
            .begin_replacement(f.store.id(), began, &UuidIds)
            .await
            .unwrap();
        let reset = replacement.device_reset();
        load(&replacement);
        replacement.swap("Household".into()).unwrap();
        drop(replacement);
        let before = walk(&f.path);
        assert!(matches!(
            f.layout.begin_replacement(f.store.id(), other, &UuidIds).await,
            Err(BootstrapDirectoryError::Lock(StoreLockError::ReplacementUnfinished { store, by }))
                if store == f.store.id() && by == began
        ));
        assert_eq!(walk(&f.path), before);
        assert!(matches!(
            f.store.lock_exclusive(),
            Err(StoreLockError::ReplacementUnfinished { by, .. }) if by == began
        ));
        let resumed = f
            .layout
            .begin_replacement(f.store.id(), began, &UuidIds)
            .await
            .unwrap();
        assert!(resumed.is_swapped());
        assert_eq!(resumed.device_reset(), reset);
        resumed.publish().unwrap();
        drop(resumed);
        f.store.lock_exclusive().unwrap();
    }
}

/// A store whose swap is unfinished can be deleted, but not while the call
/// that swapped still runs: after the swap that call holds only its
/// bootstrap lock. Deleting holds that lock too, so no replacement begins
/// while the directory goes (§19.3).
#[tokio::test]
async fn deletion_removes_an_unfinished_swap_once_its_call_stops() {
    for by in [ReplacedBy::Reset, ReplacedBy::Restore] {
        let f = store(8);
        let replacement = f
            .layout
            .begin_replacement(f.store.id(), by, &UuidIds)
            .await
            .unwrap();
        load(&replacement);
        replacement.swap("Household".into()).unwrap();
        assert!(matches!(
            f.store.lock_for_deletion(),
            Err(StoreLockError::AlreadyOpen(store)) if store == f.store.id()
        ));
        drop(replacement);
        let deletion = f.store.lock_for_deletion().unwrap().unwrap();
        assert!(matches!(
            f.layout.begin_replacement(f.store.id(), by, &UuidIds).await,
            Err(BootstrapDirectoryError::Lock(StoreLockError::AlreadyOpen(
                _
            )))
        ));
        deletion.remove_directory().unwrap();
        assert!(!f.path.exists());
        assert_eq!(
            super::lock::store_entries(f.path.parent().unwrap()),
            0,
            "no lock file named for the store remains"
        );
        assert!(f.layout.store_ids().await.unwrap().is_empty());
        assert!(f.store.lock_for_deletion().unwrap().is_none());
    }
}

/// Every entry under `path` with its contents, relative to it and sorted,
/// for comparing a tree before and after.
fn walk(path: &Path) -> Vec<(PathBuf, Option<Vec<u8>>)> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let relative = PathBuf::from(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            found.extend(
                walk(&entry.path())
                    .into_iter()
                    .map(|(inner, bytes)| (relative.join(inner), bytes)),
            );
            found.push((relative, None));
        } else {
            found.push((relative, Some(std::fs::read(entry.path()).unwrap())));
        }
    }
    found.sort();
    found
}
