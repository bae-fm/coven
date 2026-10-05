use super::*;
use crate::files::StoreLayout;
use crate::id_source::UuidIds;
use uuid::Uuid;

#[test]
fn named_file_capabilities_stay_in_their_own_areas() {
    let directory = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(directory.path().to_owned());
    let id = StoreId(Uuid::from_u128(123));
    let store = layout.create_store_dir(id, "Household", &UuidIds).unwrap();
    let name = FileName::new("attachment.data").unwrap();
    store
        .file(FileArea::AppProvided, &name)
        .replace(b"app-provided")
        .unwrap();
    store
        .file(FileArea::Cache, &name)
        .replace(b"cache")
        .unwrap();
    let storage = store.owned_file(StoreFile::StorageSettings);
    assert_eq!(storage.read_optional().unwrap(), None);
    storage.replace(b"provider-owned format").unwrap();
    assert_eq!(
        storage.read_optional().unwrap().unwrap(),
        b"provider-owned format"
    );
    assert_eq!(
        store
            .file(FileArea::AppProvided, &name)
            .read_optional()
            .unwrap()
            .unwrap(),
        b"app-provided"
    );
    assert_eq!(
        store
            .file(FileArea::Cache, &name)
            .read_optional()
            .unwrap()
            .unwrap(),
        b"cache"
    );
    assert_eq!(store.settings().unwrap().name, "Household");
    assert_eq!(store.database_path().file_name().unwrap(), "store.db");
    assert_eq!(store.id(), id);
}

#[test]
fn filenames_reject_path_traversal_and_platform_aliases() {
    for bad in [
        "",
        ".",
        "..",
        "/absolute",
        "../escape",
        "nested/file",
        r"nested\file",
        "C:drive",
        "nul",
        "NUL.txt",
        "com1.log",
        "LPT9",
        "trailing.",
        "trailing ",
        "nul\0byte",
        "with space",
        ".hidden",
    ] {
        assert!(FileName::new(bad).is_err(), "accepted {bad:?}");
    }
    assert_eq!(FileName::new("x".repeat(256)), Err(FileNameError::Length));
    for good in [
        "attachment",
        "sha256-0123456789",
        "chunk_42.data",
        "COM10",
        "file.name",
    ] {
        assert!(FileName::new(good).is_ok(), "refused {good}");
    }
}
