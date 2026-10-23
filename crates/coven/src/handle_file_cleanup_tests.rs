//! Cleaning up a removed device's stored files: at its removal, and again
//! when the app asks (§17.8). The device's provider access still works, as
//! an S3 key the person has not deleted in the console does, but nothing it
//! writes after the removal counts.
use crate::tests::{attach, file_network, file_stored, wait_for_empty_queue, Network};
use crate::*;
use coven_foundation::id_source::IdSource;
use coven_storage::Storage;

/// Ana's store on S3 with Ben joined, Ben's file "kept" in a row of his,
/// then Ben removed, his key still working.
async fn removed_uploader() -> (Network, MemberId) {
    let mut network = file_network().await;
    network
        .join_as(
            1,
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
        )
        .await;
    let ben = &network.devices[1];
    attach(&ben.handle, "kept", vec![5; 1024]).await;
    wait_for_empty_queue(&ben.handle).await;
    network.sync(1).await;
    network.sync(0).await;
    let removed = network.devices[0]
        .handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|member| !member.is_self)
        .unwrap()
        .id;
    (network, removed)
}

/// A file in Ben's folder that no row names, written straight to storage.
async fn orphan(network: &Network) -> ObjectPath {
    let ben = &network.devices[1];
    let path = ObjectPath::file(
        ben.store,
        ben.handle.device_id(),
        FileId(coven_foundation::id_source::UuidIds.new_id()),
    );
    ben.storage.create(&path, b"never named").await.unwrap();
    path
}

/// The removal's pass lists Ben's folder once: his orphan goes and the file
/// his row names stays.
#[tokio::test]
async fn a_removed_members_orphan_is_deleted_when_the_removal_applies() {
    let (network, removed) = removed_uploader().await;
    let orphan = orphan(&network).await;
    let ana = &network.devices[0];
    ana.handle.remove_member(&removed).await.unwrap();
    network.sync(0).await;
    assert!(ana.storage.status(&orphan).await.unwrap().is_none());
    assert!(file_stored(&network.devices[1], "kept").await);
    network.close().await;
}

/// Ben uploads after his removal. A pass doesn't list his folder again, but
/// the app's cleanup does and deletes the upload; the file his row named
/// before the removal stays.
#[tokio::test]
async fn an_upload_after_removal_is_deleted_by_the_cleanup_call() {
    let (network, removed) = removed_uploader().await;
    let ana = &network.devices[0];
    ana.handle.remove_member(&removed).await.unwrap();
    network.sync(0).await;
    let late = orphan(&network).await;
    network.sync(0).await;
    assert!(ana.storage.status(&late).await.unwrap().is_some());
    ana.handle.clean_up_removed_files().await.unwrap();
    assert!(ana.storage.status(&late).await.unwrap().is_none());
    assert!(file_stored(&network.devices[1], "kept").await);
    // Running it again finds nothing more and keeps what is referenced.
    ana.handle.clean_up_removed_files().await.unwrap();
    assert!(file_stored(&network.devices[1], "kept").await);
    network.close().await;
}

/// A listing the provider fails reaches the caller with its cause, and the
/// call can be made again.
#[tokio::test]
async fn a_failed_cleanup_listing_reaches_the_caller() {
    use coven_storage::test_utils::Faults;
    let (network, removed) = removed_uploader().await;
    let ana = &network.devices[0];
    ana.handle.remove_member(&removed).await.unwrap();
    network.sync(0).await;
    let late = orphan(&network).await;
    let mut faults = Faults::none();
    faults.list_failures.insert(
        ObjectPrefix::device_files(ana.store, network.devices[1].handle.device_id()),
        StorageFailure::Network,
    );
    ana.storage.set_faults(faults).await;
    assert!(matches!(
        ana.handle.clean_up_removed_files().await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Network
    ));
    assert!(ana.storage.status(&late).await.unwrap().is_some());
    ana.storage.set_faults(Faults::none()).await;
    ana.handle.clean_up_removed_files().await.unwrap();
    assert!(ana.storage.status(&late).await.unwrap().is_none());
    network.close().await;
}
