//! Stray S3 keys are taken only from retired, drained devices (§13, T237).
use super::*;
use crate::device_log_sync::drain::tests::{removed, Removed, FINAL, WAIT};
use crate::device_log_sync::tests::STORE_ID;
use crate::operations::{Begun, Command};
use crate::store_log_sync::tests::{access_keys, store};
use coven_format::{store_log::StoreChange, S3Principal};
use coven_foundation::id_source::StoreId;
use coven_storage::{test_utils::MemoryKeyAdmin, MintScope, S3MemberKey};
use std::sync::Arc;

async fn mint(admin: &MemoryKeyAdmin, store: StoreId, device: u64, attempt: u128) -> S3MemberKey {
    admin
        .mint(&MintScope::new(
            store,
            "member",
            DeviceId(device),
            uuid::Uuid::from_u128(attempt << 100),
        ))
        .await
        .unwrap()
}

async fn reclaim(removed: &mut Removed, admin: &MemoryKeyAdmin) -> Result<(), SyncError> {
    match removed
        .ana
        .sync
        .begin_operation_call(Command::ReclaimKeys(Arc::new(admin.clone())))
        .await?
    {
        Begun::Value(_) => Ok(()),
        Begun::Operation(_) => panic!("a reclaim is one call"),
    }
}

/// The key ids pending deletion.
async fn pending(removed: &Removed) -> Vec<String> {
    access_keys(&removed.ana.db)
        .await
        .into_iter()
        .map(|key| key.access_key_id)
        .collect()
}

/// Ben's laptop (device 2) made two keys before Ana removed it; an entry
/// records one. Ana's desktop (device 1) is still making a key, and the laptop
/// also made a key for another store. Until the laptop is drained nothing is
/// taken; then only its unrecorded key of this store becomes pending, once.
#[tokio::test]
async fn only_an_unrecorded_key_of_a_drained_device_becomes_pending() {
    let mut removed = removed().await;
    let admin = MemoryKeyAdmin::default();
    let stray_key = mint(&admin, STORE_ID, 2, 1).await;
    let recorded = mint(&admin, STORE_ID, 2, 2).await;
    let in_flight = mint(&admin, STORE_ID, 1, 3).await;
    let elsewhere = mint(&admin, store(7 << 100), 2, 4).await;
    let me = removed.ana.member.member();
    removed
        .ana
        .sync
        .make_and_upload_entry(StoreChange::SetAccess {
            member: me,
            access: MemberAccess::S3AccessKey {
                access_key_id: recorded.credentials.access_key_id.clone(),
                principal: recorded.principal.clone(),
            },
        })
        .await
        .unwrap();
    let before = pending(&removed).await;
    reclaim(&mut removed, &admin).await.unwrap();
    assert_eq!(pending(&removed).await, before, "the laptop is not drained");
    removed.pass(FINAL).await.unwrap();
    removed.pass(FINAL + WAIT + 1).await.unwrap();
    assert!(removed.drained().await);
    reclaim(&mut removed, &admin).await.unwrap();
    let taken: Vec<_> = access_keys(&removed.ana.db)
        .await
        .into_iter()
        .filter(|key| !before.contains(&key.access_key_id))
        .collect();
    assert_eq!(
        taken,
        [crate::AccessKeyToDelete {
            access_key_id: stray_key.credentials.access_key_id.clone(),
            principal: stray_key.principal.clone(),
            member: None,
        }]
    );
    for kept in [&recorded, &in_flight, &elsewhere] {
        assert!(!pending(&removed)
            .await
            .contains(&kept.credentials.access_key_id));
    }
    let after = pending(&removed).await;
    reclaim(&mut removed, &admin).await.unwrap();
    assert_eq!(
        pending(&removed).await,
        after,
        "a second listing adds nothing"
    );
    assert!(matches!(stray_key.principal, S3Principal::IamUser { .. }));
}

/// A failed listing records nothing and reaches the caller.
#[tokio::test]
async fn a_failed_listing_records_nothing() {
    let mut removed = removed().await;
    let admin = MemoryKeyAdmin::default();
    mint(&admin, STORE_ID, 2, 1).await;
    removed.pass(FINAL).await.unwrap();
    removed.pass(FINAL + WAIT + 1).await.unwrap();
    let before = pending(&removed).await;
    admin.fail_next_list();
    assert!(matches!(
        reclaim(&mut removed, &admin).await,
        Err(SyncError::Storage(_))
    ));
    assert_eq!(pending(&removed).await, before);
}

/// A drained device with a received entry not yet applied may still record a
/// key it made, so its keys wait; a live device's keys are never stray.
#[tokio::test]
async fn keys_wait_while_their_device_has_an_unapplied_entry() {
    let removed = removed().await;
    let log = removed.ana.db.local_store_log().await.unwrap().log;
    let key = |device| ListedKey {
        access_key_id: format!("key-{device}"),
        principal: S3Principal::Console,
        device: DeviceId(device),
    };
    let drained = BTreeSet::from([DeviceId(2)]);
    let applied: Vec<_> = log
        .entries
        .iter()
        .map(|entry| (entry.entry.position.device, entry.entry.position.number))
        .collect();
    assert_eq!(
        stray(&log, &applied, &drained, vec![key(1), key(2)]),
        [key(2)]
    );
    let mut received = applied.clone();
    received.push((DeviceId(2), 99));
    assert!(stray(&log, &received, &drained, vec![key(1), key(2)]).is_empty());
}
