use super::*;
use crate::{
    files::{StoreFile, StoreLayout},
    id_source::UuidIds,
};

#[test]
fn creation_failures_preserve_recursive_error_sources() {
    let failure = StoreCreationError::<std::convert::Infallible>::Rollback {
        operation: Box::new(StoreCreationError::AlreadyExists(
            StoreId(uuid::Uuid::nil()),
        )),
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
    let layout = StoreLayout::new(temp.path().into());
    let id = StoreId(uuid::Uuid::from_u128(17));
    let pending = layout.begin_bootstrap(id, "Shared", &UuidIds).unwrap();
    let directory = pending.directory();
    let device = directory.settings().unwrap().device_id;
    directory
        .owned_file(StoreFile::Bootstrap)
        .replace(b"fixed request")
        .unwrap();
    assert!(layout.stores().await.unwrap().is_empty());
    assert!(matches!(
        layout.begin_bootstrap(id, "Shared", &UuidIds),
        Err(BootstrapDirectoryError::Lock(StoreLockError::AlreadyOpen(
            _
        )))
    ));
    drop(directory);
    drop(pending);
    let pending = layout.begin_bootstrap(id, "Shared", &UuidIds).unwrap();
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
        layout.create_store_dir(id, "Collision", &UuidIds),
        Err(StoreCreationError::AlreadyExists(_))
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
    assert_eq!(layout.stores().await.unwrap().len(), 1);
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
            StoreCreationError::AlreadyExists(_)
        ))
    ));
    drop(pending);
    assert!(matches!(
        layout.begin_bootstrap(id, "Shared", &UuidIds),
        Err(BootstrapDirectoryError::Create(
            StoreCreationError::AlreadyExists(_)
        ))
    ));
}

#[tokio::test]
async fn cancellation_removes_unpublished_bytes_and_permits_a_new_identity() {
    let temp = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(temp.path().into());
    let id = StoreId(uuid::Uuid::from_u128(18));
    let pending = layout.begin_bootstrap(id, "Shared", &UuidIds).unwrap();
    let device = pending.directory().settings().unwrap().device_id;
    pending
        .directory()
        .owned_file(StoreFile::Bootstrap)
        .replace(b"encrypted")
        .unwrap();
    pending.cancel().unwrap();
    pending.cancel().unwrap();
    assert!(layout.stores().await.unwrap().is_empty());
    drop(pending);
    let fresh = layout.begin_bootstrap(id, "Shared", &UuidIds).unwrap();
    assert_ne!(fresh.directory().settings().unwrap().device_id, device);
    assert!(fresh
        .directory()
        .owned_file(StoreFile::Bootstrap)
        .read_optional()
        .unwrap()
        .is_none());
}

#[test]
fn finished_bootstrap_releases_its_lease_even_when_directory_clones_remain() {
    for publish in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(temp.path().into());
        let id = StoreId(UuidIds.new_id());
        let pending = layout.begin_bootstrap(id, "Shared", &UuidIds).unwrap();
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
        let replacement = layout.begin_bootstrap(id, "Shared", &UuidIds).unwrap();
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
    let layout = StoreLayout::new(root.path().into());
    let other = StoreLayout::new(target.path().into())
        .create_store_dir(id, "Other", &UuidIds)
        .unwrap();
    let staged = root.path().join("stores");
    std::fs::create_dir_all(&staged).unwrap();
    std::os::unix::fs::symlink(
        target.path().join("stores").join(id.to_string()),
        staged.join(id.to_string()),
    )
    .unwrap();
    assert!(layout.begin_bootstrap(id, "Household", &UuidIds).is_err());
    assert_eq!(other.settings().unwrap().name, "Other");
}
