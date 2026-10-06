use super::*;
use crate::files::StoreLayout;
use crate::id_source::UuidIds;
use uuid::Uuid;

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
    let inherited = guard.file.try_clone().unwrap();
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
    let destination = deletion_path(&lock.directory, id);
    std::fs::rename(&lock.directory, &destination).unwrap();
    drop(lock);
    store
        .lock_for_deletion()
        .unwrap()
        .unwrap()
        .remove_directory()
        .unwrap();
    assert!(store.lock_for_deletion().unwrap().is_none());
    assert!(!destination.exists());
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
    assert!(!outside.path().join(".coven-lock").exists());
}
