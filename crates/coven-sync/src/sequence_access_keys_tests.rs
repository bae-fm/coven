use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::admin::StoreChange;
use coven_storage::Storage;
use std::sync::Arc;

#[tokio::test]
async fn an_admin_replacement_delivers_records_and_leaves_the_console_key_owed() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let admin = Arc::new(group.storage.key_admin());
    let replaced = ana
        .sync
        .replace_member_access_key(ben.identity.member(), admin.clone())
        .await
        .unwrap();
    let OldKeyDeletion::Console { operation } = replaced.old_key else {
        panic!("a console key is deleted in the console");
    };
    assert_eq!(admin.calls(), ["mint", "probe"]);
    let deletion: crate::sequence_sync::access_keys::Deletion = serde_json::from_slice(
        &ana.sync
            .database
            .operation(operation)
            .await
            .unwrap()
            .unwrap()
            .data,
    )
    .unwrap();
    assert_eq!(deletion.key.access_key_id, "ben");
    ben.sync.pass().await.unwrap();
    let delivered = ben
        .sync
        .delivered_access_key(Some("ben"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        delivered.access_key_id,
        replaced.key.credentials.access_key_id
    );
    assert!(ben
        .sync
        .delivered_access_key(Some(&delivered.access_key_id))
        .await
        .unwrap()
        .is_none());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_replacement_invalidated_by_contention_undoes_its_key_and_delivery() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    carol.sync.pass().await.unwrap();
    let admin = Arc::new(group.storage.key_admin());
    let (delivery_started, delivery_observed) = tokio::sync::oneshot::channel();
    let (delivery_resume, delivery_held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(
            coven_storage::ObjectPrefix::all(group.store),
            delivery_started,
            delivery_held,
        )
        .await;
    let replace = ana
        .sync
        .replace_member_access_key(ben.identity.member(), admin.clone());
    let remove = async {
        delivery_observed.await.unwrap();
        let (claim_started, claim_observed) = tokio::sync::oneshot::channel();
        let (claim_resume, claim_held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_next_creation(
                coven_storage::ObjectPrefix::all(group.store),
                claim_started,
                claim_held,
            )
            .await;
        delivery_resume.send(()).unwrap();
        claim_observed.await.unwrap();
        carol
            .sync
            .administer(StoreChange::RemoveMember {
                member: ben.identity.member(),
            })
            .await
            .unwrap();
        claim_resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(replace, remove);
    assert!(
        matches!(
            &result,
            Err(SyncError::Rejected(OperationRejection::TargetGone(_)))
        ),
        "{result:?}"
    );
    assert_eq!(admin.calls(), ["mint", "probe", "revoke", "key_exists"]);
    let deliveries = group
        .storage
        .list(&coven_storage::ObjectPrefix::access_keys(
            group.store,
            &ben.identity.member(),
        ))
        .await
        .unwrap();
    assert!(deliveries.is_empty(), "undo deletes its delivery");
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.kind != "replace-access-key"));
    for device in [&ana, &ben, &carol] {
        device.sync.database.close().await.unwrap();
    }
}
