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
