use super::*;
use crate::files::StoreSettings;
use crate::id_source::UuidIds;

#[test]
fn an_absent_layout_lists_no_stores_without_creating_it() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(directory.path().join("not-created"));
    assert!(layout.stores().unwrap().is_empty());
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn discovery_lists_only_valid_store_directories_in_id_order() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(directory.path().to_owned());
    for (n, name) in [(2, "Second"), (1, "First")] {
        layout
            .create_store_dir(StoreId(Uuid::from_u128(n)), name, &UuidIds)
            .unwrap();
    }
    let root = directory.path().join("stores");
    fs::create_dir(root.join("not-a-store-id")).unwrap();
    fs::write(
        root.join(Uuid::from_u128(3).to_string()),
        b"not a directory",
    )
    .unwrap();
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
            name: "wrong id".into(),
            device_id: crate::id_source::DeviceId(1),
        },
    )
    .unwrap();
    let stage = tempfile::Builder::new()
        .prefix(".coven-create-")
        .tempdir_in(&root)
        .unwrap();
    settings::write(
        stage.path(),
        &StoreSettings {
            id: StoreId(Uuid::from_u128(8)),
            name: "unpublished".into(),
            device_id: crate::id_source::DeviceId(1),
        },
    )
    .unwrap();
    assert_eq!(
        layout.stores().unwrap(),
        vec![
            StoreInfo {
                id: StoreId(Uuid::from_u128(1)),
                name: "First".into()
            },
            StoreInfo {
                id: StoreId(Uuid::from_u128(2)),
                name: "Second".into()
            },
        ]
    );
}

#[test]
fn directory_listing_errors_are_not_hidden_as_an_empty_layout() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(directory.path().join("stores"), b"not a directory").unwrap();
    let layout = StoreLayout::new(directory.path().to_owned());
    assert!(matches!(layout.stores(), Err(StoreLayoutError::File(_))));
    assert!(matches!(
        layout.create_store_dir(StoreId(Uuid::nil()), "Store", &UuidIds),
        Err(StoreCreationError::File(_))
    ));
}

#[cfg(unix)]
#[test]
fn discovery_does_not_follow_directory_or_settings_symlinks() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(directory.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    layout.create_store_dir(id, "Original", &UuidIds).unwrap();
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
    assert_eq!(
        layout.stores().unwrap(),
        vec![StoreInfo {
            id,
            name: "Original".into()
        }]
    );
}
