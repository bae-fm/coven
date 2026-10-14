//! A pass catches up on membership, then makes keys and access match, then
//! syncs data (§6, SP phases 1–3).

use crate::device_log_sync::tests::{group, sql, storage, STORE_ID};
use crate::store_log_sync::tests::{member, Device};
use crate::{operation_data::Data, Files, Operations, TransferLimits};
use coven_format::sealed_single::SingleChunkPrefix;
use coven_format::store_log::{MemberRole, StoreChange};
use coven_foundation::id_source::{DeviceId, KeyId};
use coven_storage::test_utils::{MemoryRequest, MemoryStorage};
use coven_storage::{ObjectPath, Storage, StorageFailure};
use std::num::NonZeroU64;
use std::sync::Arc;

/// Hand one test device's owners to a running operation worker. The device's
/// temporary directory stays alive with the returned value.
fn start(device: Device) -> (Operations, Device) {
    let files = Files::new(
        coven_database::FileDatabase::new(device.db.clone()),
        device.directory.clone(),
        Some(device.storage.clone() as Arc<dyn Storage>),
        device.clock.clone(),
        device.send.clone(),
        device.ids.clone(),
        TransferLimits::default(),
    );
    let mut device = device;
    let sync = std::mem::replace(
        &mut device.sync,
        crate::StoreLogSync::disconnected(
            device.db.clone(),
            device.custody.clone(),
            device.identity.clone(),
            device.clock.clone(),
            device.send.clone(),
            device.ids.clone(),
            device.directory.clone(),
        ),
    );
    let spare = device.writes();
    let writes = std::mem::replace(&mut device.writes, spare);
    let operations = Operations::new(sync, files, writes, device.clock.clone());
    (operations, device)
}

fn write_path(number: u64) -> ObjectPath {
    ObjectPath::device_log(STORE_ID, DeviceId(1), number.try_into().unwrap())
}

/// An entry nobody can read stops its writer's log with a refusal: the
/// membership observation completes no catch-up (SP step 2).
async fn plant_refused_entry(storage: &MemoryStorage) {
    let truncated = SingleChunkPrefix::StoreLog {
        key: KeyId(uuid::Uuid::from_u128(1)),
    }
    .encode()
    .unwrap();
    storage
        .create(
            &ObjectPath::store_log(STORE_ID, DeviceId(9), NonZeroU64::MIN),
            &truncated,
        )
        .await
        .unwrap();
}

/// An incomplete first phase sends no first attempt and permits no
/// administrative entry; a complete one permits both.
#[tokio::test]
async fn an_incomplete_first_phase_holds_first_attempts_and_administrative_work() {
    for incomplete in [false, true] {
        let storage = storage();
        let ana = group(storage.clone(), 1).await.remove(0);
        sql(&ana.db, "INSERT INTO notes VALUES('one','title','body')").await;
        if incomplete {
            plant_refused_entry(&storage).await;
        }
        let (operations, ana) = start(ana);
        let pass = operations.sync().await;
        let circle = operations.create_circle("Gifts").await;
        // The refusal is the entry's own record; the pass itself finishes.
        pass.unwrap();
        assert_eq!(circle.is_ok(), !incomplete, "{circle:?}");
        let sent = storage.status(&write_path(1)).await.unwrap().is_some();
        assert_eq!(sent, !incomplete);
        let attempted = ana
            .db
            .read_oldest_upload(|upload| Ok::<_, coven_database::DbError>(upload.keys.is_some()))
            .await
            .unwrap();
        assert_eq!(attempted, incomplete.then_some(false));
        operations.close().await.unwrap();
    }
}

/// After a removal exposes the store key, the pass publishes the rotation's
/// copies and entry before the first attempt that seals with its key.
#[tokio::test]
async fn rotation_receipts_precede_new_sealing() {
    let storage = storage();
    let mut ana = group(storage.clone(), 1).await.remove(0);
    let outsider = member(9);
    ana.add(&outsider, MemberRole::Member).await;
    ana.sync().await;
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: outsider.member(),
        })
        .await
        .unwrap();
    sql(&ana.db, "INSERT INTO notes VALUES('one','title','body')").await;
    let before = storage.request_trace().len();
    let (operations, ana) = start(ana);
    operations.sync().await.unwrap();
    let trace = &storage.request_trace()[before..];
    let created = |matches: &dyn Fn(&ObjectPath) -> bool| {
        trace
            .iter()
            .position(|request| matches!(request, MemoryRequest::Create(path) if matches(path)))
    };
    let rotation = ana
        .log()
        .await
        .entries
        .iter()
        .rev()
        .find(|entry| matches!(entry.entry.change, StoreChange::RotateKey { .. }))
        .expect("the pass rotated the exposed key")
        .entry
        .position;
    let entry = created(&|path| {
        path.store_log_position() == Some((rotation.device, rotation.number.try_into().unwrap()))
    })
    .expect("rotation entry created in this pass");
    let copy = created(&|path| {
        path.key_copy_position()
            .is_some_and(|(writer, _)| writer == DeviceId(1))
    })
    .expect("rotation copy created in this pass");
    let write = created(&|path| *path == write_path(1)).expect("write sent in this pass");
    assert!(copy < entry && entry < write, "{trace:?}");
    operations.close().await.unwrap();
}

/// Provider access the owner cannot settle yet is its own pending work; data
/// protected by usable keys still goes up in the same pass (SP phase 2).
#[tokio::test]
async fn a_provider_access_wait_does_not_hold_unrelated_data() {
    let storage = storage();
    let ana = group(storage.clone(), 1).await.remove(0);
    ana.db
        .start_operation(
            Data::ProviderAccess {
                account: "someone@example.com".into(),
            }
            .new_operation("coven")
            .unwrap(),
        )
        .await
        .unwrap();
    sql(&ana.db, "INSERT INTO notes VALUES('one','title','body')").await;
    let (operations, ana) = start(ana);
    operations.sync().await.unwrap();
    assert!(storage.status(&write_path(1)).await.unwrap().is_some());
    assert_eq!(ana.db.operations().await.unwrap().len(), 1);
    operations.close().await.unwrap();
}

/// SD `KeyPhases.begin` over IO `readLog`: a first phase whose entry or copy
/// log does not end in a terminal miss (a failed read, or an entry its reader
/// refuses) permits no first attempt; the real pass agrees.
#[test]
#[ignore = "requires COVEN_IO_RUNNER"]
fn io_differential_pass_phases() {
    use serde_json::json;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut cases = Vec::new();
    let mut results = Vec::new();
    for (entry_failed, entry_refused, copy_failed) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (false, false, true),
    ] {
        let first_attempt = runtime.block_on(async {
            let storage = storage();
            let ana = group(storage.clone(), 1).await.remove(0);
            sql(&ana.db, "INSERT INTO notes VALUES('one','title','body')").await;
            if entry_refused {
                plant_refused_entry(&storage).await;
            }
            let mut faults = coven_storage::test_utils::Faults::none();
            if entry_failed {
                faults.read_failures.insert(
                    ObjectPath::store_log(STORE_ID, DeviceId(1), NonZeroU64::new(2).unwrap()),
                    StorageFailure::Network,
                );
            }
            if copy_failed {
                faults.read_failures.insert(
                    ObjectPath::key_copy(STORE_ID, DeviceId(1), NonZeroU64::new(2).unwrap()),
                    StorageFailure::Network,
                );
            }
            storage.set_faults(faults).await;
            let (operations, _ana) = start(ana);
            let _ = operations.sync().await;
            let sent = storage.status(&write_path(1)).await.unwrap().is_some();
            operations.close().await.unwrap();
            sent
        });
        cases.push(json!({"check":"phases",
            "entry":{"count":usize::from(entry_refused),"failed":entry_failed,"refused":entry_refused},
            "copy":{"count":0,"failed":copy_failed,"refused":false}}));
        results.push(json!({"first_attempt":first_attempt}));
    }
    assert_eq!(results, crate::discovery::tests::io_runner(&cases));
}
