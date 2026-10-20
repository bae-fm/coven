use super::*;
use crate::files::StoreDirAccess;
use crate::files::StoreDirectories;
use crate::id_source::UuidIds;
use uuid::Uuid;

#[test]
fn creation_never_overwrites_a_store_or_even_an_empty_directory() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(directory.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let original = layout
        .create_store_dir(id, &UuidIds)
        .unwrap()
        .settings()
        .unwrap();
    assert!(
        matches!(layout.create_store_dir(id, &UuidIds), Err(DirectoryCreationError::AlreadyExists(found)) if found == id)
    );
    assert_eq!(layout.store_dir(&id).settings().unwrap(), original);
    let occupied = StoreId(Uuid::from_u128(2));
    fs::create_dir(directory.path().join("stores").join(occupied.to_string())).unwrap();
    assert!(matches!(
        layout.create_store_dir(occupied, &UuidIds),
        Err(DirectoryCreationError::AlreadyExists(_))
    ));
    // Both refused publications remove their unpublished stages.
    assert_eq!(lock::store_entries(&directory.path().join("stores")), 2);
}

#[test]
fn restored_and_joined_directories_get_fresh_device_ids_for_the_same_store() {
    let directory = tempfile::tempdir().unwrap();
    let [first, restored] = ["first-device", "restored-device"].map(|name| {
        let app_dir = directory.path().join(name);
        fs::create_dir(&app_dir).unwrap();
        StoreDirectories::new(app_dir)
    });
    let id = StoreId(Uuid::from_u128(1));
    let one = first
        .create_store_dir(id, &UuidIds)
        .unwrap()
        .settings()
        .unwrap();
    let two = restored
        .create_store_dir(id, &UuidIds)
        .unwrap()
        .settings()
        .unwrap();
    assert_eq!(one.id, two.id);
    assert_ne!(one.device_id, two.device_id);
}

#[cfg(feature = "test-utils")]
#[test]
fn creation_uses_the_supplied_id_source_for_the_device_id() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(directory.path().to_owned());
    let ids = crate::id_source::SequentialIds::new();
    let id = StoreId(ids.new_id());
    let store = layout.create_store_dir(id, &ids).unwrap();
    assert_eq!(
        store.settings().unwrap().device_id,
        crate::id_source::DeviceId(2)
    );
}

#[test]
fn concurrent_creation_of_the_same_id_publishes_exactly_one_store() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(directory.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let layout = &layout;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    layout.create_store_dir(id, &UuidIds)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(DirectoryCreationError::AlreadyExists(_))))
            .count(),
        1
    );
    assert_eq!(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(layout.store_ids())
            .unwrap()
            .len(),
        1
    );
    assert_eq!(lock::store_entries(&directory.path().join("stores")), 1);
}

#[cfg(unix)]
#[test]
fn retrying_directory_creation_repeats_a_failed_durability_barrier() {
    use std::cell::RefCell;
    let app_dir = tempfile::tempdir().unwrap();
    let stores = app_dir.path().join("stores");
    let first = create_directory_with_sync(&stores, &|_| {
        Err(io::Error::other("injected parent sync failure"))
    });
    assert!(first.is_err());
    assert!(stores.is_dir());
    let synced = RefCell::new(Vec::new());
    create_directory_with_sync(&stores, &|path| {
        synced.borrow_mut().push(path.to_owned());
        crate::files::atomic_file::sync_directory(path)
    })
    .unwrap();
    assert_eq!(
        *synced.borrow(),
        [app_dir.path().to_owned()],
        "retry must sync the parent whose first sync failed, and nothing above it"
    );
}

#[test]
fn creation_needs_the_app_directory_to_exist() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("app");
    let layout = StoreDirectories::new(missing.clone());
    let id = StoreId(Uuid::from_u128(1));
    let Err(DirectoryCreationError::File(error)) = layout.create_store_dir(id, &UuidIds) else {
        panic!("creating a store inside a missing app directory must fail");
    };
    assert!(error.to_string().contains("create stores directory"));
    assert!(!missing.exists(), "coven must not create the app directory");
}
