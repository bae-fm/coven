use super::*;
use crate::StorageFailure;
use coven_foundation::{
    files::StoreDirectories,
    id_source::{SequentialIds, StoreId},
};
use std::error::Error;
#[test]
fn settings_use_the_reserved_atomic_file_and_refuse_corrupt_values() {
    let temp = tempfile::tempdir().unwrap();
    let layout =
        StoreDirectories::new(temp.path().to_owned(), coven_foundation::task::HeardBugs::shared());
    let directory =
        layout.create_store_dir(StoreId(uuid::Uuid::from_u128(1)), &SequentialIds::new()).unwrap();
    let file = directory.owned_file(StoreFile::StorageSettings);
    assert_eq!(crate::recorded_config(&directory).unwrap(), None);
    let config = StorageConfig::Dropbox {
        account_label: None,
        location: crate::DropboxLocation::SharedFolder { namespace_id: "namespace".into() },
        folder_label: None,
    };
    crate::record_config(&directory, &config).unwrap();
    assert_eq!(crate::recorded_config(&directory).unwrap(), Some(config));
    file.replace(br#"{"Dropbox":{"location":{"SharedFolder":{"namespace_id":"namespace"}},"secret_access_key":"forbidden"}}"#)
        .unwrap();
    let error = crate::recorded_config(&directory).unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Encoding);
    assert!(error.source().unwrap().is::<serde_json::Error>());
    crate::remove_recorded_config(&directory).unwrap();
    assert_eq!(crate::recorded_config(&directory).unwrap(), None);
}

/// `coven_sync_status.location` holds the location's text with its labels,
/// and reads back as the same location; text that names a credential reads
/// as no location (api.md E4).
#[test]
fn a_recorded_location_reads_back_from_its_column() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    let config = StorageConfig::Dropbox {
        account_label: Some("ana@example.com".into()),
        location: crate::DropboxLocation::SharedFolder { namespace_id: "namespace".into() },
        folder_label: Some(crate::FolderLabel {
            name: "Household".into(),
            web_url: "https://www.dropbox.com/home/Household".parse().unwrap(),
        }),
    };
    let text = config.recorded_text().unwrap();
    let read: StorageConfig = db.query_row("SELECT ?1", [&text], |r| r.get(0)).unwrap();
    assert_eq!(read, config);
    let secret = r#"{"Dropbox":{"location":{"SharedFolder":{"namespace_id":"namespace"}},"secret_access_key":"forbidden"}}"#;
    let refused = db.query_row("SELECT ?1", [secret], |r| r.get::<_, StorageConfig>(0));
    assert!(refused.is_err());
}
