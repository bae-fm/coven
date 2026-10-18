use super::*;
use crate::files::StoreDirectoriesAccess;
use crate::{
    files::{StoreDirectories, StoreFile},
    id_source::UuidIds,
};

#[test]
fn creation_failures_preserve_recursive_error_sources() {
    let failure = DirectoryCreationError::Rollback {
        operation: Box::new(DirectoryCreationError::AlreadyExists(StoreId(
            uuid::Uuid::nil(),
        ))),
        cleanup: io::Error::other("rollback refused"),
    };
    let error: &dyn std::error::Error = &failure;
    assert!(error.to_string().contains("rollback refused"));
    assert!(error
        .source()
        .unwrap()
        .to_string()
        .contains("already exists"));
}

#[tokio::test]
async fn interrupted_bootstrap_retains_identity_without_publishing() {
    let temp = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(temp.path().into());
    let id = StoreId(uuid::Uuid::from_u128(17));
    let pending = layout.begin_bootstrap(id, &UuidIds).await.unwrap();
    let directory = pending.directory();
    let device = directory.settings().unwrap().device_id;
    directory
        .owned_file(StoreFile::Bootstrap)
        .replace(b"fixed request")
        .unwrap();
    assert!(layout.store_ids().await.unwrap().is_empty());
    assert!(matches!(
        layout.begin_bootstrap(id, &UuidIds).await,
        Err(BootstrapDirectoryError::Lock(StoreLockError::AlreadyOpen(
            _
        )))
    ));
    drop(directory);
    drop(pending);
    let pending = layout.begin_bootstrap(id, &UuidIds).await.unwrap();
    assert_eq!(pending.directory().settings().unwrap().device_id, device);
    assert_eq!(
        pending
            .directory()
            .owned_file(StoreFile::Bootstrap)
            .read_optional()
            .unwrap(),
        Some(b"fixed request".to_vec())
    );
    assert!(matches!(
        layout.store_dir(&id).lock_exclusive(),
        Err(StoreLockError::BootstrapPending(_))
    ));
    assert!(matches!(
        layout.store_dir(&id).lock_read_only(),
        Err(StoreLockError::BootstrapPending(_))
    ));
    assert!(matches!(
        layout.store_dir(&id).lock_for_deletion(),
        Err(StoreLockError::BootstrapPending(_))
    ));
    assert!(matches!(
        layout.create_store_dir(id, &UuidIds),
        Err(DirectoryCreationError::AlreadyExists(_))
    ));
    let directory = pending.directory();
    let path = directory.database_path();
    std::fs::write(&path, b"loaded database").unwrap();
    let writer = directory.lock_exclusive().unwrap();
    let reader = directory.lock_read_only().unwrap();
    pending.publish().unwrap();
    assert_eq!(
        std::fs::canonicalize(layout.store_dir(&id).database_path()).unwrap(),
        path
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"loaded database");
    assert_eq!(directory.settings().unwrap().device_id, device);
    assert_eq!(layout.store_ids().await.unwrap().len(), 1);
    assert!(matches!(
        pending.cancel(),
        Err(BootstrapDirectoryError::Lock(StoreLockError::AlreadyOpen(
            _
        )))
    ));
    drop((writer, reader, directory));
    assert!(matches!(
        pending.cancel(),
        Err(BootstrapDirectoryError::Create(
            DirectoryCreationError::AlreadyExists(_)
        ))
    ));
    drop(pending);
    assert!(matches!(
        layout.begin_bootstrap(id, &UuidIds).await,
        Err(BootstrapDirectoryError::Create(
            DirectoryCreationError::AlreadyExists(_)
        ))
    ));
}

#[tokio::test]
async fn bootstrap_refuses_a_live_writer_before_reporting_an_existing_store() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(root.path().into());
    let id = StoreId(UuidIds.new_id());
    let directory = layout.create_store_dir(id, &UuidIds).unwrap();
    let writer = directory.lock_for_open().await.unwrap();
    assert!(matches!(
        layout.begin_bootstrap(id, &UuidIds).await,
        Err(BootstrapDirectoryError::Lock(StoreLockError::AlreadyOpen(found))) if found == id
    ));
    drop(writer);
    assert!(matches!(
        layout.begin_bootstrap(id, &UuidIds).await,
        Err(BootstrapDirectoryError::Create(DirectoryCreationError::AlreadyExists(found))) if found == id
    ));
}

#[tokio::test]
async fn cancellation_removes_unpublished_bytes_and_permits_a_new_identity() {
    let temp = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(temp.path().into());
    let id = StoreId(uuid::Uuid::from_u128(18));
    let pending = layout.begin_bootstrap(id, &UuidIds).await.unwrap();
    let device = pending.directory().settings().unwrap().device_id;
    pending
        .directory()
        .owned_file(StoreFile::Bootstrap)
        .replace(b"encrypted")
        .unwrap();
    pending.cancel().unwrap();
    pending.cancel().unwrap();
    assert!(layout.store_ids().await.unwrap().is_empty());
    drop(pending);
    let fresh = layout.begin_bootstrap(id, &UuidIds).await.unwrap();
    assert_ne!(fresh.directory().settings().unwrap().device_id, device);
    assert!(fresh
        .directory()
        .owned_file(StoreFile::Bootstrap)
        .read_optional()
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn finished_bootstrap_releases_its_lease_even_when_directory_clones_remain() {
    for publish in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let layout = StoreDirectories::new(temp.path().into());
        let id = StoreId(UuidIds.new_id());
        let pending = layout.begin_bootstrap(id, &UuidIds).await.unwrap();
        let directory = pending.directory();
        if publish {
            pending.publish().unwrap();
            layout
                .store_dir(&id)
                .lock_for_deletion()
                .unwrap()
                .unwrap()
                .remove_directory()
                .unwrap();
        } else {
            pending.cancel().unwrap();
        }
        let replacement = layout.begin_bootstrap(id, &UuidIds).await.unwrap();
        assert!(matches!(
            directory.lock_exclusive(),
            Err(StoreLockError::BootstrapPending(_))
        ));
        assert!(matches!(
            pending.cancel(),
            Err(BootstrapDirectoryError::Lock(
                StoreLockError::BootstrapPending(_)
            ))
        ));
        assert!(matches!(
            pending.publish(),
            Err(BootstrapDirectoryError::Lock(
                StoreLockError::BootstrapPending(_)
            ))
        ));
        replacement.cancel().unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn resumed_bootstrap_refuses_a_symlink_to_another_installation() {
    let root = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let id = StoreId(uuid::Uuid::from_u128(19));
    let layout = StoreDirectories::new(root.path().into());
    let other = StoreDirectories::new(target.path().into())
        .create_store_dir(id, &UuidIds)
        .unwrap();
    let staged = root.path().join("stores");
    std::fs::create_dir_all(&staged).unwrap();
    std::os::unix::fs::symlink(
        target.path().join("stores").join(id.to_string()),
        staged.join(id.to_string()),
    )
    .unwrap();
    assert!(layout.begin_bootstrap(id, &UuidIds).await.is_err());
    assert_eq!(other.settings().unwrap().id, id);
}
