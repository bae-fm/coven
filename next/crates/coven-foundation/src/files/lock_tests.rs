use super::*;
use crate::files::StoreLayout;
use crate::id_source::UuidIds;
use uuid::Uuid;

#[test]
fn deletion_holds_only_a_sibling_lock_and_removes_it_after_the_directory() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let store = layout.create_store_dir(id, "store", &UuidIds).unwrap();
    let lock = store.lock_for_deletion().unwrap().unwrap();
    let path = root.path().join("stores").join(format!(".{id}.lock"));
    assert!(path.is_file(), "the held lock must be beside the store");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    assert!(matches!(file.try_lock(), Err(TryLockError::WouldBlock)));
    drop(file);
    let mut contents: Vec<_> = std::fs::read_dir(&lock.store.directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    contents.sort();
    assert_eq!(contents, ["cache", "files", "settings.json"]);
    lock.remove_directory().unwrap();
    assert!(!path.exists());
    assert_eq!(
        std::fs::read_dir(root.path().join("stores"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn a_second_writer_is_refused_until_the_first_guard_drops() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(directory.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let store = layout.create_store_dir(id, "store", &UuidIds).unwrap();
    let guard = store.lock_exclusive().unwrap();
    let second = layout.store_dir(&id);
    assert!(
        matches!(second.lock_exclusive(), Err(StoreLockError::AlreadyOpen(found)) if found == id)
    );
    // The read-only opening path reads settings alongside a writer, without
    // touching or acquiring the exclusive lock.
    assert_eq!(second.settings().unwrap().id, id);
    drop(guard);
    let next_guard = second.lock_exclusive().unwrap();
    assert!(matches!(
        store.lock_exclusive(),
        Err(StoreLockError::AlreadyOpen(_))
    ));
    drop(next_guard);
    store.lock_exclusive().unwrap();
}

#[test]
fn a_missing_store_is_an_io_error_and_is_not_created_by_locking() {
    let directory = tempfile::tempdir().unwrap();
    let store = StoreLayout::new(directory.path().to_owned()).store_dir(&StoreId(Uuid::nil()));
    assert!(
        matches!(store.lock_exclusive(), Err(StoreLockError::File(FileError::Io { source, .. })) if source.kind() == std::io::ErrorKind::NotFound)
    );
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn dropping_the_guard_unlocks_even_while_an_inherited_handle_survives() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(directory.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let store = layout.create_store_dir(id, "store", &UuidIds).unwrap();
    let guard = store.lock_exclusive().unwrap();
    // A fork before exec retains a reference to this same open file. Duplicating
    // it makes that descriptor lifetime deterministic without racing a process.
    let inherited = guard.store._file.file.try_clone().unwrap();
    drop(guard);
    let next = store.lock_exclusive().unwrap();
    drop(inherited);
    assert!(matches!(
        store.lock_exclusive(),
        Err(StoreLockError::AlreadyOpen(_))
    ));
    drop(next);
    store.lock_exclusive().unwrap();
}

#[test]
fn only_the_matching_lock_can_authorize_open_and_change_the_device() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let first = layout
        .create_store_dir(StoreId(Uuid::from_u128(1)), "first", &UuidIds)
        .unwrap();
    let second = layout
        .create_store_dir(StoreId(Uuid::from_u128(2)), "second", &UuidIds)
        .unwrap();
    let lock = first.lock_exclusive().unwrap();
    first.verify_lock(&lock).unwrap();
    assert!(matches!(
        second.verify_lock(&lock),
        Err(StoreLockError::WrongDirectory(_))
    ));
    lock.set_device_id(crate::id_source::DeviceId(42)).unwrap();
    assert_eq!(
        first.settings().unwrap().device_id,
        crate::id_source::DeviceId(42)
    );
}

#[test]
fn deletion_requires_the_lock_and_retries_an_unpublished_directory() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let store = layout.create_store_dir(id, "first", &UuidIds).unwrap();
    let lock = store.lock_exclusive().unwrap();
    assert!(matches!(
        store.lock_for_deletion(),
        Err(StoreLockError::AlreadyOpen(_))
    ));
    // An interrupted removal has already unpublished its directory.
    let destination = deletion_path(&lock.store.directory, id);
    std::fs::rename(&lock.store.directory, &destination).unwrap();
    assert!(matches!(
        store.lock_for_deletion(),
        Err(StoreLockError::AlreadyOpen(_))
    ));
    assert!(matches!(
        layout.create_store_dir(id, "replacement", &UuidIds),
        Err(crate::files::StoreCreationError::AlreadyExists(_))
    ));
    drop(lock);
    store
        .lock_for_deletion()
        .unwrap()
        .unwrap()
        .remove_directory()
        .unwrap();
    assert!(store.lock_for_deletion().unwrap().is_none());
    assert!(!destination.exists());
    assert_eq!(
        std::fs::read_dir(root.path().join("stores"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn readers_coexist_with_the_writer_but_all_must_close_before_deletion() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let store = layout.create_store_dir(id, "store", &UuidIds).unwrap();
    let first = store.lock_read_only().unwrap();
    let second = store.lock_read_only().unwrap();
    let writer = store.lock_exclusive().unwrap();
    drop(writer);
    assert!(matches!(
        store.lock_for_deletion(),
        Err(StoreLockError::AlreadyOpen(_))
    ));
    drop(first);
    assert!(matches!(
        store.lock_for_deletion(),
        Err(StoreLockError::AlreadyOpen(_))
    ));
    drop(second);
    let deletion = store.lock_for_deletion().unwrap().unwrap();
    assert!(matches!(
        store.lock_read_only(),
        Err(StoreLockError::AlreadyOpen(_))
    ));
    assert!(matches!(
        store.lock_exclusive(),
        Err(StoreLockError::AlreadyOpen(_))
    ));
    deletion.remove_directory().unwrap();
}

#[test]
fn deletion_retries_when_only_some_lock_files_remain_before_id_reuse() {
    for removed_locks in [0, 1] {
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().to_owned());
        let id = StoreId(Uuid::from_u128(1));
        let store = layout.create_store_dir(id, "store", &UuidIds).unwrap();
        let deletion = store.lock_for_deletion().unwrap().unwrap();
        let path = deletion.store.directory.clone();
        let paths = lock_paths(&path, id);
        // Interruption after removing the directory, before removing all locks.
        std::fs::remove_dir_all(&path).unwrap();
        drop(deletion);
        for path in paths.iter().take(removed_locks) {
            std::fs::remove_file(path).unwrap();
        }
        assert!(matches!(
            layout.create_store_dir(id, "replacement", &UuidIds),
            Err(crate::files::StoreCreationError::AlreadyExists(_))
        ));
        store
            .lock_for_deletion()
            .unwrap()
            .unwrap()
            .remove_directory()
            .unwrap();
        assert!(store.lock_for_deletion().unwrap().is_none());
        assert_eq!(
            std::fs::read_dir(root.path().join("stores"))
                .unwrap()
                .count(),
            0
        );
        let replacement = layout
            .create_store_dir(id, "replacement", &UuidIds)
            .unwrap();
        let writer = replacement.lock_exclusive().unwrap();
        assert!(matches!(
            store.lock_exclusive(),
            Err(StoreLockError::AlreadyOpen(_))
        ));
        drop(writer);
    }
}

#[test]
fn failed_unpublication_keeps_the_store_and_its_locks_for_retry() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let store = layout.create_store_dir(id, "store", &UuidIds).unwrap();
    let deletion = store.lock_for_deletion().unwrap().unwrap();
    let path = deletion.store.directory.clone();
    let destination = deletion_path(&path, id);
    std::fs::create_dir(&destination).unwrap();
    assert!(matches!(
        deletion.remove_directory(),
        Err(FileError::Io {
            operation: "unpublish store",
            ..
        })
    ));
    assert!(path.is_dir());
    for path in lock_paths(&path, id) {
        assert!(path.is_file());
    }
    std::fs::remove_dir(&destination).unwrap();
    store
        .lock_for_deletion()
        .unwrap()
        .unwrap()
        .remove_directory()
        .unwrap();
    assert!(store.lock_for_deletion().unwrap().is_none());
}

#[cfg(unix)]
#[test]
fn deleting_a_store_link_never_removes_its_target() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let stores = root.path().join("stores");
    std::fs::create_dir(&stores).unwrap();
    let path = stores.join(id.to_string());
    std::os::unix::fs::symlink(outside.path(), &path).unwrap();
    let store = layout.store_dir(&id);
    assert!(matches!(
        store.lock_for_deletion(),
        Err(StoreLockError::File(_))
    ));
    assert!(outside.path().exists());
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
}
