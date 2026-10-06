use super::*;
use crate::files::StoreLayout;
use crate::id_source::UuidIds;
use uuid::Uuid;

#[test]
fn creation_never_overwrites_a_store_or_even_an_empty_directory() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(directory.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let original = layout
        .create_store_dir(id, "Original", &UuidIds)
        .unwrap()
        .settings()
        .unwrap();
    assert!(
        matches!(layout.create_store_dir(id, "Replacement", &UuidIds), Err(StoreCreationError::AlreadyExists(found)) if found == id)
    );
    assert_eq!(layout.store_dir(&id).settings().unwrap(), original);
    let occupied = StoreId(Uuid::from_u128(2));
    fs::create_dir(directory.path().join("stores").join(occupied.to_string())).unwrap();
    assert!(matches!(
        layout.create_store_dir(occupied, "Replacement", &UuidIds),
        Err(StoreCreationError::AlreadyExists(_))
    ));
    // Both refused publications remove their unpublished stages.
    assert_eq!(
        fs::read_dir(directory.path().join("stores"))
            .unwrap()
            .count(),
        2
    );
}

#[test]
fn restored_and_joined_directories_get_fresh_device_ids_for_the_same_store() {
    let directory = tempfile::tempdir().unwrap();
    let first = StoreLayout::new(directory.path().join("first-device"));
    let restored = StoreLayout::new(directory.path().join("restored-device"));
    let id = StoreId(Uuid::from_u128(1));
    let one = first
        .create_store_dir(id, "Store", &UuidIds)
        .unwrap()
        .settings()
        .unwrap();
    let two = restored
        .create_store_dir(id, "Store", &UuidIds)
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
    let layout = StoreLayout::new(directory.path().to_owned());
    let ids = crate::id_source::SequentialIds::new();
    let id = StoreId(ids.new_id());
    let store = layout.create_store_dir(id, "Store", &ids).unwrap();
    assert_eq!(
        store.settings().unwrap().device_id,
        crate::id_source::DeviceId(2)
    );
}

#[test]
fn concurrent_creation_of_the_same_id_publishes_exactly_one_store() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(directory.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = ["First", "Second"]
            .into_iter()
            .map(|name| {
                let layout = &layout;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    layout.create_store_dir(id, name, &UuidIds)
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
            .filter(|r| matches!(r, Err(StoreCreationError::AlreadyExists(_))))
            .count(),
        1
    );
    assert_eq!(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(layout.stores())
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        fs::read_dir(directory.path().join("stores"))
            .unwrap()
            .count(),
        1
    );
}

#[cfg(unix)]
#[test]
fn retrying_directory_creation_repeats_a_failed_durability_barrier() {
    use std::cell::RefCell;
    let directory = tempfile::tempdir().unwrap();
    let app_dir = directory.path().join("app");
    let stores = app_dir.join("stores");
    let first = create_directory_tree_with_sync(&stores, &|path| {
        if path == directory.path() {
            Err(io::Error::other("injected parent sync failure"))
        } else {
            crate::files::atomic_file::sync_directory(path)
        }
    });
    assert!(first.is_err());
    assert!(app_dir.is_dir());
    let synced = RefCell::new(Vec::new());
    create_directory_tree_with_sync(&stores, &|path| {
        synced.borrow_mut().push(path.to_owned());
        crate::files::atomic_file::sync_directory(path)
    })
    .unwrap();
    assert!(
        synced.borrow().contains(&directory.path().to_owned()),
        "retry must sync the parent whose first sync failed"
    );
}
