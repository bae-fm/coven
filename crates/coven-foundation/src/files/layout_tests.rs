use super::*;
use crate::files::StoreDirAccess;
use crate::files::StoreSettings;
use crate::id_source::UuidIds;

#[tokio::test]
async fn an_absent_layout_lists_no_stores_without_creating_it() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(directory.path().join("not-created"));
    assert!(layout.store_ids().await.unwrap().is_empty());
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn discovery_lists_only_valid_store_directories_in_id_order() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(directory.path().to_owned());
    let mut locks = Vec::new();
    for n in [2, 1] {
        let store = layout.create_store_dir(StoreId(Uuid::from_u128(n)), &UuidIds).unwrap();
        locks.push((store.lock_exclusive().unwrap(), store.lock_read_only().unwrap()));
    }
    let root = directory.path().join("stores");
    fs::create_dir(root.join("not-a-store-id")).unwrap();
    fs::write(root.join(Uuid::from_u128(3).to_string()), b"not a directory").unwrap();
    let missing = root.join(Uuid::from_u128(4).to_string());
    fs::create_dir(&missing).unwrap();
    let corrupt = root.join(Uuid::from_u128(5).to_string());
    fs::create_dir(&corrupt).unwrap();
    fs::write(corrupt.join(settings::SETTINGS_FILE), b"corrupt").unwrap();
    let mismatch = root.join(Uuid::from_u128(6).to_string());
    fs::create_dir(&mismatch).unwrap();
    settings::write(
        &mismatch,
        &StoreSettings {
            id: StoreId(Uuid::from_u128(7)),
            device_id: crate::id_source::DeviceId(1),
        },
    )
    .unwrap();
    let stage = tempfile::Builder::new().prefix(".coven-create-").tempdir_in(&root).unwrap();
    settings::write(
        stage.path(),
        &StoreSettings {
            id: StoreId(Uuid::from_u128(8)),
            device_id: crate::id_source::DeviceId(1),
        },
    )
    .unwrap();
    assert_eq!(
        layout.store_ids().await.unwrap(),
        vec![StoreId(Uuid::from_u128(1)), StoreId(Uuid::from_u128(2)),]
    );
}

#[tokio::test]
async fn directory_listing_errors_are_not_hidden_as_an_empty_layout() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(directory.path().join("stores"), b"not a directory").unwrap();
    let layout = StoreDirectories::new(directory.path().to_owned());
    assert!(matches!(layout.store_ids().await, Err(StoreDirectoriesError::File(_))));
    assert!(matches!(
        layout.create_store_dir(StoreId(Uuid::nil()), &UuidIds),
        Err(DirectoryCreationError::File(_))
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn discovery_does_not_follow_directory_or_settings_symlinks() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(directory.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    layout.create_store_dir(id, &UuidIds).unwrap();
    let root = directory.path().join("stores");
    std::os::unix::fs::symlink(
        root.join(id.to_string()),
        root.join(Uuid::from_u128(2).to_string()),
    )
    .unwrap();
    let linked_settings = root.join(Uuid::from_u128(3).to_string());
    fs::create_dir(&linked_settings).unwrap();
    std::os::unix::fs::symlink(
        root.join(id.to_string()).join(settings::SETTINGS_FILE),
        linked_settings.join(settings::SETTINGS_FILE),
    )
    .unwrap();
    assert_eq!(layout.store_ids().await.unwrap(), vec![id]);
}
