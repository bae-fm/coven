//! Cleaning up the stored files of a device gone for good: when the entry
//! that stopped it applies, and again when the app asks (§17.10). The
//! device's provider access still works, as an S3 key the person has not
//! deleted in the console does, but nothing it writes after that entry
//! counts.
use crate::tests::{attach, file_network, file_stored, wait_for_empty_queue, Network};
use crate::*;
use coven_foundation::id_source::IdSource;
use coven_storage::Storage;

/// Ana's store on S3 with Ben joined and his file "kept" in a row of his.
/// Returns Ben's member id and the file.
async fn bens_upload() -> (Network, MemberId, FileId) {
    let mut network = file_network().await;
    network
        .join_as(1, InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() })
        .await;
    let ben = &network.devices[1];
    let kept = attach(&ben.handle, "kept", vec![5; 1024]).await;
    wait_for_empty_queue(&ben.handle).await;
    network.sync(1).await;
    network.sync(0).await;
    let ben = network.devices[0]
        .handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|member| !member.is_self)
        .unwrap()
        .id;
    (network, ben, kept)
}

/// A file in Ben's folder that no record names, written straight to storage.
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
/// his row references stays.
#[tokio::test]
async fn a_removed_members_orphan_is_deleted_when_the_removal_applies() {
    let (network, ben, kept) = bens_upload().await;
    let orphan = orphan(&network).await;
    let ana = &network.devices[0];
    ana.handle.remove_member(&ben).await.unwrap();
    network.sync(0).await;
    assert!(ana.storage.status(&orphan).await.unwrap().is_none());
    assert!(file_stored(&network.devices[1], kept).await);
    network.close().await;
}

/// Ben uploads after his removal. A pass doesn't list his folder again, but
/// the app's cleanup does and deletes the upload; the file his row
/// referenced before the removal stays.
#[tokio::test]
async fn an_upload_after_removal_is_deleted_by_the_cleanup_call() {
    let (network, ben, kept) = bens_upload().await;
    let ana = &network.devices[0];
    ana.handle.remove_member(&ben).await.unwrap();
    network.sync(0).await;
    let late = orphan(&network).await;
    network.sync(0).await;
    assert!(ana.storage.status(&late).await.unwrap().is_some());
    ana.handle.clean_up_gone_devices_files().await.unwrap();
    assert!(ana.storage.status(&late).await.unwrap().is_none());
    assert!(file_stored(&network.devices[1], kept).await);
    // Running it again finds nothing more and keeps what is referenced.
    ana.handle.clean_up_gone_devices_files().await.unwrap();
    assert!(file_stored(&network.devices[1], kept).await);
    network.close().await;
}

/// A listing the provider fails reaches the caller with its cause, and the
/// call can be made again.
#[tokio::test]
async fn a_failed_cleanup_listing_reaches_the_caller() {
    use coven_storage::test_utils::Faults;
    let (network, ben, _) = bens_upload().await;
    let ana = &network.devices[0];
    ana.handle.remove_member(&ben).await.unwrap();
    network.sync(0).await;
    let late = orphan(&network).await;
    let mut faults = Faults::none();
    faults.list_failures.insert(
        ObjectPrefix::device_files(ana.store, network.devices[1].handle.device_id()),
        StorageFailure::Network,
    );
    ana.storage.set_faults(faults).await;
    assert!(matches!(
        ana.handle.clean_up_gone_devices_files().await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Network
    ));
    assert!(ana.storage.status(&late).await.unwrap().is_some());
    ana.storage.set_faults(Faults::none()).await;
    ana.handle.clean_up_gone_devices_files().await.unwrap();
    assert!(ana.storage.status(&late).await.unwrap().is_none());
    network.close().await;
}

/// Ana writes and sends it, so her published writes read through every slot
/// she has applied: each candidate Ben's device found before then has been
/// seen by her (§17.10).
async fn ana_writes(network: &Network, id: &'static str) {
    network.devices[0]
        .handle
        .write(move |sql| {
            sql.execute("INSERT INTO parents VALUES(?1,?1,1)", [id])?;
            Ok(())
        })
        .await
        .unwrap();
    network.sync(0).await;
}

/// Ben's phone is reset: the installation registered in its place lists the
/// old phone's folder and deletes its orphan once Ana has seen the listing,
/// and the file Ben's row references stays (§17.10, §19.3).
#[tokio::test]
async fn a_reset_devices_orphan_is_deleted_by_its_replacement() {
    let (mut network, _, kept) = bens_upload().await;
    let orphan = orphan(&network).await;
    network.devices[1].handle.close().await.unwrap();
    let handle = network
        .reopen(&network.devices[1])
        .reset_device(network.devices[1].store, "Ben's reset phone", |_| ())
        .await
        .unwrap();
    handle.start_sync().await.unwrap();
    network.devices[1].handle = handle;
    network.sync(1).await;
    network.sync(0).await;
    ana_writes(&network, "after the reset").await;
    network.sync(1).await;
    let ben = &network.devices[1];
    assert!(ben.storage.status(&orphan).await.unwrap().is_none());
    assert!(file_stored(&network.devices[0], kept).await);
    network.close().await;
}

/// Ana approves Ben's laptop with a new pair for him, so his phone, holding
/// only the old one, is gone for good. His laptop lists the phone's folder,
/// deletes its orphan once Ana has seen the listing, and keeps the file the
/// phone's row references (§14.1, §17.10).
#[tokio::test]
async fn a_member_key_replacement_lets_the_new_pairs_device_clean_up() {
    let (mut network, ben, kept) = bens_upload().await;
    let orphan = orphan(&network).await;
    network.join_as(2, InviteTarget::ReplaceMemberKeys { member: ben }).await;
    network.sync(0).await;
    ana_writes(&network, "after the replacement").await;
    network.sync(2).await;
    let laptop = &network.devices[2];
    assert!(laptop.storage.status(&orphan).await.unwrap().is_none());
    assert!(file_stored(&network.devices[0], kept).await);
    network.close().await;
}

/// Ana removes Ben's phone, his only device, and Ben stays a member. No
/// device of his is left to clean up, so Ana's, the storage owner's, lists
/// the phone's folder: the orphan goes and the file his row references stays
/// (§17.10).
#[tokio::test]
async fn the_storage_owner_cleans_up_after_a_members_last_device() {
    let (network, _, kept) = bens_upload().await;
    let orphan = orphan(&network).await;
    let ana = &network.devices[0];
    ana.handle.remove_device(network.devices[1].handle.device_id()).await.unwrap();
    network.sync(0).await;
    assert!(ana.storage.status(&orphan).await.unwrap().is_none());
    assert!(file_stored(ana, kept).await);
    network.close().await;
}
