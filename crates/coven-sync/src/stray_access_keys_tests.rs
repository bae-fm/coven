//! Stray S3 keys are taken only from retired, drained devices (§13, T237).
use super::*;
use crate::device_log_sync::drain::tests::{removed, Removed, FINAL, WAIT};
use crate::device_log_sync::tests::STORE_ID;
use crate::store_log_sync::tests::{access_keys, store};
use coven_format::{store_log::StoreChange, S3Principal};
use coven_foundation::id_source::StoreId;
use coven_storage::{test_utils::MemoryKeyAdmin, AccessKeyAdmin, MintScope, S3MemberKey};
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

/// A pass's sweep while an operation holds `admin`.
async fn sweep(removed: &mut Removed, admin: &MemoryKeyAdmin) -> Result<(), SyncError> {
    let sync = &mut removed.ana.sync;
    sync.hold_admin_access(crate::OperationId(1), Arc::new(admin.clone()));
    sync.sweep_stray_access_keys().await
}

fn listings(admin: &MemoryKeyAdmin) -> usize {
    admin
        .calls()
        .into_iter()
        .filter(|call| *call == "list_keys")
        .count()
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
/// also made a key for another store. Until the laptop is drained no sweep
/// lists keys; then one listing makes only its unrecorded key of this store
/// pending, and later sweeps list nothing until another device can have
/// stray keys. Without admin access a sweep does nothing.
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
    sweep(&mut removed, &admin).await.unwrap();
    assert_eq!(listings(&admin), 0, "the laptop is not drained");
    removed.pass(FINAL).await.unwrap();
    removed.pass(FINAL + WAIT + 1).await.unwrap();
    assert!(removed.drained().await);
    removed.ana.sync.release_admin_access(crate::OperationId(1));
    removed.ana.sync.sweep_stray_access_keys().await.unwrap();
    assert_eq!(pending(&removed).await, before, "no admin access is held");
    sweep(&mut removed, &admin).await.unwrap();
    assert_eq!(listings(&admin), 1);
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
    sweep(&mut removed, &admin).await.unwrap();
    assert_eq!(listings(&admin), 1, "no device became reclaimable since");
    assert!(matches!(stray_key.principal, S3Principal::IamUser { .. }));
}

/// A failed listing records nothing, fails its pass, and the next sweep
/// lists again.
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
        sweep(&mut removed, &admin).await,
        Err(SyncError::Storage(_))
    ));
    assert_eq!(pending(&removed).await, before);
    sweep(&mut removed, &admin).await.unwrap();
    assert_eq!(listings(&admin), 2);
    assert_ne!(pending(&removed).await, before);
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
    let devices = reclaimable(&log, &applied, &drained);
    assert_eq!(stray(&log, &devices, vec![key(1), key(2)]), [key(2)]);
    let mut received = applied.clone();
    received.push((DeviceId(2), 99));
    assert!(reclaimable(&log, &received, &drained).is_empty());
}
