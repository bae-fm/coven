//! The one-time listing of a removed uploader's file folder (§16.5).
use crate::operations::Command;
use crate::store_log_sync::tests::operations::{accounts, drive_access, publish};
use crate::store_log_sync::tests::{store, Device};
use coven_format::store_log::StoreChange;
use coven_foundation::clock::FixedClock;
use coven_foundation::id_source::{DeviceId, FileId};
use coven_storage::test_utils::{Faults, MemoryRequest, MemoryStorage};
use coven_storage::{ObjectPath, ObjectPrefix, Storage, StorageFailure};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

async fn fail_folder_listing(storage: &MemoryStorage, device: DeviceId) {
    storage
        .set_faults(Faults {
            list_failures: [(
                ObjectPrefix::device_files(store(1), device),
                StorageFailure::Network,
            )]
            .into(),
            ..Faults::none()
        })
        .await;
}

/// Google Drive storage whose provider time the test advances.
fn google() -> (Arc<MemoryStorage>, Arc<FixedClock>) {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let storage = MemoryStorage::builder()
        .provider(coven_storage::CloudProvider::GoogleDrive)
        .clock(clock.clone())
        .build()
        .unwrap();
    (Arc::new(storage), clock)
}

/// Thirty-one storage days on, Ana's catch-up makes the day-0 removal final:
/// only then may she delete what its uploader left (§9, §16.5).
async fn finalize(a: &mut Device, clock: &FixedClock) {
    let later = UNIX_EPOCH + Duration::from_secs(31 * 86400);
    clock.set(later);
    a.clock.set(later);
    a.sync().await;
}

fn folder_listings(storage: &MemoryStorage, device: DeviceId) -> usize {
    let folder = ObjectPrefix::device_files(store(1), device);
    storage
        .request_trace()
        .iter()
        .filter(|request| matches!(request, MemoryRequest::List(prefix) if *prefix == folder))
        .count()
}

/// Carol uploaded a photo whose attaching row never reached Ana. Ana removes
/// her; removal alone is no cut-off, so nothing lists her folder. Once Ana's
/// device confirms Carol's account revoked, it lists the folder once, and
/// deletes what it found once her removal is final: a
/// failed listing records nothing and the next retention lists again. The
/// completed scan survives reopening. Ben taking Carol's account restores
/// her access and forgets the scan; revoking it again is a new cut-off, which
/// lists again.
#[tokio::test]
async fn a_removed_uploaders_folder_is_listed_once_per_cut_off() {
    let (storage, clock) = google();
    let [mut a, mut b, c] = accounts(storage.clone()).await;
    let carol = c.device().await;
    let orphan = |n| ObjectPath::file(store(1), carol, FileId(uuid::Uuid::from_u128(n)));
    storage.create(&orphan(1), b"photo").await.unwrap();
    a.sync.run_retention().await.unwrap();
    publish(&mut a, Command::RemoveMember(c.member.member())).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(folder_listings(&storage, carol), 0);
    assert!(storage.read(&orphan(1)).await.is_ok());

    drive_access(&mut a).await;
    finalize(&mut a, &clock).await;
    fail_folder_listing(&storage, carol).await;
    assert!(a.sync.run_retention().await.is_err());
    assert_eq!(a.db.file_scan(carol).await.unwrap(), None);
    assert!(storage.read(&orphan(1)).await.is_ok());
    storage.set_faults(Faults::none()).await;
    a.elapse_retry_delays();
    a.sync.run_retention().await.unwrap();
    assert_eq!(folder_listings(&storage, carol), 2);
    assert!(a.db.file_scan(carol).await.unwrap().is_some());
    assert!(storage.read(&orphan(1)).await.is_err());

    // Nothing lists the folder again, across reopening.
    storage.create(&orphan(2), b"photo").await.unwrap();
    a.sync.run_retention().await.unwrap();
    a.reopen(storage.clone(), vec![], vec![]).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(folder_listings(&storage, carol), 2);
    assert!(storage.read(&orphan(2)).await.is_ok());

    // Ben moves to Carol's account and back, reaching storage through the
    // account he held before each move, caught up to the same day.
    b.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    b.sync().await;
    for (held, account, scanned) in [
        ("ben@example.com", "cat@example.com", false),
        ("cat@example.com", "ben@example.com", true),
    ] {
        b.sync.storage = Some(Arc::new(
            MemoryStorage::for_recipient(&storage, held).unwrap(),
        ));
        b.sync
            .make_and_upload_entry(StoreChange::SetAccess {
                member: b.member.member(),
                access: coven_format::MemberAccess::ProviderAccount(account.into()),
            })
            .await
            .unwrap();
        a.sync().await;
        drive_access(&mut a).await;
        a.sync.run_retention().await.unwrap();
        assert_eq!(a.db.file_scan(carol).await.unwrap().is_some(), scanned);
    }
    assert_eq!(folder_listings(&storage, carol), 3);
    assert!(storage.read(&orphan(2)).await.is_err());
}

/// IO `Retired.observeFiles`: after Carol's cut-off, each retention lists her
/// folder until one listing completes; that completion's paths are the scan,
/// and no later listing changes it. Before each step another of her uploads
/// lands, so a later listing would find more. A failed step drops the
/// listing with a network failure; once a scan completed nothing lists, so
/// nothing fails.
#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER, supplied by scripts/check.sh"]
async fn io_differential_file_scan() {
    use serde_json::json;
    let mut cases = Vec::new();
    let mut results = Vec::new();
    for steps in [
        [false, true, true],
        [true, false, true],
        [false, false, true],
        [true, true, false],
    ] {
        let (storage, clock) = google();
        let [mut a, _, c] = accounts(storage.clone()).await;
        let carol = c.device().await;
        publish(&mut a, Command::RemoveMember(c.member.member())).await;
        drive_access(&mut a).await;
        finalize(&mut a, &clock).await;
        let mut listings = Vec::new();
        let mut observed = Vec::new();
        let mut landed = Vec::new();
        for completes in steps {
            let id = landed.len() as u64 + 1;
            storage
                .create(
                    &ObjectPath::file(store(1), carol, FileId(uuid::Uuid::from_u128(id.into()))),
                    b"photo",
                )
                .await
                .unwrap();
            landed.push(id);
            if completes {
                listings.push(json!({ "found": landed }));
                a.sync.run_retention().await.unwrap();
            } else {
                listings.push(json!({ "failed": true }));
                fail_folder_listing(&storage, carol).await;
                let failed = a.sync.run_retention().await.is_err();
                storage.set_faults(Faults::none()).await;
                a.elapse_retry_delays();
                // Once a scan completed nothing lists the folder, so nothing fails.
                assert_eq!(failed, a.db.file_scan(carol).await.unwrap().is_none());
            }
            let mut found = Vec::new();
            for id in &landed {
                let path =
                    ObjectPath::file(store(1), carol, FileId(uuid::Uuid::from_u128((*id).into())));
                if storage.read(&path).await.is_err() {
                    found.push(*id);
                }
            }
            observed.push(match a.db.file_scan(carol).await.unwrap() {
                Some(_) => json!(found),
                None => serde_json::Value::Null,
            });
        }
        cases.push(json!({"check":"file_scan","writer":carol.0,"listings":listings}));
        results.push(json!(observed));
    }
    assert_eq!(results, crate::discovery::tests::io_runner(&cases));
}
