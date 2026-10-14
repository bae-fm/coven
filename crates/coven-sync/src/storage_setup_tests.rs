use super::*;
use crate::store_log_sync::tests::{device, member, storage, store};
use coven_storage::ObjectPrefix;

fn access() -> coven_format::MemberAccess {
    coven_format::MemberAccess::S3AccessKey {
        access_key_id: "fixture-access-key".into(),
        principal: coven_format::S3Principal::Console,
    }
}

/// A never-connected store's original installation (device 1) and its
/// storage-less reset copy (device 2) set up one location in turn. Without
/// reading the store folder both created it (storage
/// `StoreCreation.blind_setup_two_creations`); the second now finds the
/// first's entry and refuses before reserving a creation of its own
/// (`listed_setup_one_creation`).
#[tokio::test]
async fn a_second_installation_never_creates_a_store_already_created_here() {
    let storage = storage();
    let mut original = device(storage.clone(), 1, member(1), store(1)).await;
    let mut copy = device(storage.clone(), 2, member(1), store(1)).await;
    original
        .sync
        .setup_storage(
            storage.clone(),
            access(),
            "phone".into(),
            Box::new(|| Ok(Box::new(|| Ok(())))),
        )
        .await
        .unwrap();
    let refused = copy
        .sync
        .setup_storage(
            storage.clone(),
            access(),
            "restored".into(),
            Box::new(|| panic!("a refused setup commits no connection state")),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&refused, SyncError::Storage(error)
            if error.failure() == coven_storage::StorageFailure::StoreExists),
        "{refused:?}"
    );
    let entries: Vec<_> = storage
        .list(&ObjectPrefix::store_logs(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path.store_log_position().unwrap().0)
        .collect();
    assert_eq!(entries, [coven_foundation::id_source::DeviceId(1)]);
    let local = copy.db.local_store_log().await.unwrap();
    assert!(local.upload.is_none() && local.log.entries.is_empty());
}
