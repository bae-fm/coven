use crate::sequence_sync::tests::*;
use coven_format::admin::StoreChange;
use coven_foundation::id_source::IdSource;
use coven_storage::{ObjectPath, Storage};

#[tokio::test]
async fn a_removed_uploader_is_scanned_once_only_after_provider_cutoff() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let orphan = ObjectPath::file(
        group.store,
        ben.sync.device,
        coven_foundation::id_source::FileId(group.ids.new_id()),
    );
    group
        .storage
        .create(&orphan, b"orphan upload")
        .await
        .unwrap();
    ana.sync
        .administer(StoreChange::RemoveDevice {
            device: ben.sync.device,
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana
        .sync
        .database
        .file_scan(ben.sync.device)
        .await
        .unwrap()
        .is_none());
    ana.sync
        .administer(StoreChange::RemoveMember {
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana
        .sync
        .database
        .file_scan(ben.sync.device)
        .await
        .unwrap()
        .is_none());
    ana.sync
        .confirm_access_key_deleted("ben".into())
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana
        .sync
        .database
        .file_scan(ben.sync.device)
        .await
        .unwrap()
        .is_some());
    assert!(group.storage.status(&orphan).await.unwrap().is_none());
    let listings = || {
        ana.storage.request_trace().iter().filter(|request| matches!(request,
        coven_storage::test_utils::MemoryRequest::List(prefix) if *prefix == coven_storage::ObjectPrefix::device_files(group.store, ben.sync.device))).count()
    };
    assert_eq!(listings(), 1);
    let count = ana.storage.request_count();
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.storage.request_count() - count, 2);
    assert_eq!(listings(), 1);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}
