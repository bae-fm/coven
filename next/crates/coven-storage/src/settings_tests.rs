use super::*;
use coven_foundation::{
    files::StoreLayout,
    id_source::{SequentialIds, StoreId},
};
#[test]
fn settings_use_the_reserved_atomic_file_and_refuse_corrupt_values() {
    let temp = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(temp.path().to_owned());
    let directory = layout
        .create_store_dir(
            StoreId(uuid::Uuid::from_u128(1)),
            "store",
            &SequentialIds::new(),
        )
        .unwrap();
    let file = directory.owned_file(StoreFile::StorageSettings);
    let settings = StorageSettings::new(directory);
    assert_eq!(settings.read().unwrap(), None);
    let config = StorageConfig::Dropbox {
        namespace_id: "namespace".into(),
    };
    settings.commit(&config).unwrap();
    assert_eq!(settings.read().unwrap(), Some(config));
    file.replace(br#"{"Dropbox":{"namespace_id":"","secret_access_key":"forbidden"}}"#)
        .unwrap();
    assert!(matches!(settings.read(), Err(StorageError::Encoding(_))));
    settings.remove().unwrap();
    assert_eq!(settings.read().unwrap(), None);
}
