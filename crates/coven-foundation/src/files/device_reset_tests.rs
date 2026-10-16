use super::*;
use crate::files::{FileArea, FileName, StoreFile, StoreLayout};
use crate::id_source::UuidIds;

fn reset(old: u64, new: u64) -> DeviceReset {
    DeviceReset {
        old_device: DeviceId(old),
        new_device: DeviceId(new),
    }
}

#[tokio::test]
async fn a_recorded_reset_moves_local_state_aside_and_keeps_its_fresh_id_across_retries() {
    let temp = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(temp.path().into());
    let id = StoreId(uuid::Uuid::from_u128(5));
    let directory = layout.create_store_dir(id, "Home", &UuidIds).unwrap();
    let old = directory.settings().unwrap().device_id;
    std::fs::write(directory.database_path(), b"old database").unwrap();
    directory
        .owned_file(StoreFile::StorageSettings)
        .replace(b"location")
        .unwrap();
    directory
        .file(FileArea::AppProvided, &FileName::new("photo").unwrap())
        .replace(b"unsent")
        .unwrap();
    let decided = reset(old.0, old.0 + 1);
    assert_eq!(directory.record_device_reset(&decided).unwrap(), decided);
    // A retried reset never chooses another id.
    assert_eq!(
        directory
            .record_device_reset(&reset(old.0, old.0 + 2))
            .unwrap(),
        decided
    );
    let pending = layout.begin_bootstrap(id, "Home", &UuidIds).await.unwrap();
    assert_eq!(pending.device_reset().unwrap(), Some(decided));
    let directory = pending.directory();
    assert_eq!(directory.settings().unwrap().device_id, decided.new_device);
    assert!(!directory.database_path().exists());
    assert_eq!(
        directory
            .owned_file(StoreFile::StorageSettings)
            .read_optional()
            .unwrap(),
        Some(b"location".to_vec())
    );
    assert_eq!(
        directory
            .file(FileArea::AppProvided, &FileName::new("photo").unwrap())
            .read_optional()
            .unwrap(),
        None
    );
    let archive = temp
        .path()
        .join("stores")
        .join(id.to_string())
        .join(format!("discarded-device-{}", old.0));
    assert_eq!(
        std::fs::read(archive.join("store.db")).unwrap(),
        b"old database"
    );
    assert_eq!(
        std::fs::read(archive.join("files").join("photo")).unwrap(),
        b"unsent"
    );
    assert!(layout.stores().await.unwrap().is_empty());
    drop(directory);
    drop(pending);
    // A retry finds the same bootstrap and the same id.
    let pending = layout.begin_bootstrap(id, "Home", &UuidIds).await.unwrap();
    assert_eq!(
        pending.directory().settings().unwrap().device_id,
        decided.new_device
    );
    pending.publish().unwrap();
    let directory = layout.store_dir(&id);
    assert_eq!(directory.device_reset().unwrap(), None);
    assert_eq!(directory.unfinished_device_reset().unwrap(), None);
    assert_eq!(layout.stores().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_record_left_by_interrupted_publication_is_finished_not_repeated() {
    let temp = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(temp.path().into());
    let id = StoreId(uuid::Uuid::from_u128(6));
    let directory = layout.create_store_dir(id, "Home", &UuidIds).unwrap();
    let old = directory.settings().unwrap().device_id;
    let decided = reset(old.0, old.0 + 1);
    directory.record_device_reset(&decided).unwrap();
    assert_eq!(directory.unfinished_device_reset().unwrap(), Some(decided));
    let pending = layout.begin_bootstrap(id, "Home", &UuidIds).await.unwrap();
    // Publication removed the marker; the record removal did not happen.
    std::fs::remove_file(
        temp.path()
            .join("stores")
            .join(id.to_string())
            .join(".coven-bootstrap"),
    )
    .unwrap();
    drop(pending);
    assert_eq!(directory.device_reset().unwrap(), Some(decided));
    assert_eq!(directory.unfinished_device_reset().unwrap(), None);
    assert_eq!(directory.device_reset().unwrap(), None);
    assert_eq!(directory.settings().unwrap().device_id, decided.new_device);
}
