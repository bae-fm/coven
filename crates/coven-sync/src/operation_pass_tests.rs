//! A pass catches up on membership, then makes keys and access match, then
//! syncs data (§6, SP phases 1–3).

use crate::device_log_sync::tests::{group, sql, storage, STORE_ID};
use crate::store_log_sync::tests::{member, Device};
use crate::{
    operation_data::Data, Files, OperationKind, Operations, PendingReason, PendingSubject, Retry,
    SyncError, TransferLimits,
};
use coven_format::sealed_single::SingleChunkPrefix;
use coven_format::store_log::{MemberRole, StoreChange};
use coven_foundation::id_source::{DeviceId, KeyId};
use coven_storage::test_utils::{Faults, MemoryRequest, MemoryStorage};
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
        device.send.clone(),
        device.waits.clone(),
        device.ids.clone(),
        TransferLimits::default(),
    );
    let mut device = device;
    let waits = device.waits.clone();
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
            waits,
            crate::RestoreCodeCustody::Unkept,
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

/// Device 2's next key copy has no readable clear prefix: the copy
/// observation is incomplete (§11). Device 2 must be in the group.
async fn plant_unreadable_copy(storage: &MemoryStorage) {
    storage
        .create(
            &ObjectPath::key_copy(STORE_ID, DeviceId(2), NonZeroU64::MIN),
            b"not a sealed key",
        )
        .await
        .unwrap();
}

/// A first phase whose entries complete but whose copy observation meets an
/// unreadable prefix is incomplete, even while the previous pass's catch-up
/// is still fresh: no first attempt uses that earlier pass's copies
/// (`PassPhases.entries_only_reuses_previous`).
#[tokio::test]
async fn an_unreadable_copy_prefix_holds_first_attempts_after_a_fresh_pass() {
    let storage = storage();
    let ana = group(storage.clone(), 2).await.remove(0);
    let (operations, ana) = start(ana);
    operations.sync().await.unwrap();
    plant_unreadable_copy(&storage).await;
    sql(&ana.db, "INSERT INTO notes VALUES('one','title','body')").await;
    operations.sync().await.unwrap();
    assert!(storage.status(&write_path(1)).await.unwrap().is_none());
    let attempted = ana
        .db
        .read_oldest_upload(|upload| Ok::<_, coven_database::DbError>(upload.keys.is_some()))
        .await
        .unwrap();
    assert_eq!(attempted, Some(false));
    operations.close().await.unwrap();
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

/// A failed retention deletion still lets the pass reach step 9: positions
/// are posted and the return checkpoint moves, and the deletion stays its
/// own `Retention` record (§15, SP step 9).
#[tokio::test]
async fn a_failed_retention_deletion_still_posts_positions() {
    use coven_storage::test_utils::Faults;
    let storage = storage();
    let ana = group(storage.clone(), 1).await.remove(0);
    let orphan = ObjectPath::file(
        STORE_ID,
        DeviceId(1),
        coven_foundation::id_source::FileId(uuid::Uuid::from_u128(999)),
    );
    storage.create(&orphan, b"orphan").await.unwrap();
    ana.db
        .add_file_candidates(std::collections::BTreeSet::from([(
            orphan.device().unwrap(),
            orphan.file_id().unwrap(),
        )]))
        .await
        .unwrap();
    storage
        .set_faults(Faults {
            delete_failures: [(orphan.clone(), StorageFailure::PermissionDenied)].into(),
            ..Faults::none()
        })
        .await;
    let (operations, ana) = start(ana);
    let pass = operations.sync().await;
    assert!(
        matches!(&pass, Err(crate::SyncError::Storage(error))
            if error.failure() == StorageFailure::PermissionDenied),
        "{pass:?}"
    );
    assert!(storage.status(&orphan).await.unwrap().is_some());
    assert!(ana.db.return_checkpoint().await.unwrap().is_some());
    operations.close().await.unwrap();
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

/// A rotation whose entry the provider refuses: Ana's device with the
/// removal published, the rotation's entry path refused with `failure`, and
/// a write waiting on the rotated key.
async fn refused_rotation(
    failure: StorageFailure,
    status: u16,
) -> (Operations, Device, Arc<MemoryStorage>, ObjectPath) {
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
    let own = ana
        .log()
        .await
        .entries
        .iter()
        .filter(|entry| entry.entry.position.device == DeviceId(1))
        .map(|entry| entry.entry.position.number)
        .max()
        .unwrap();
    let rotation = ObjectPath::store_log(STORE_ID, DeviceId(1), NonZeroU64::new(own + 1).unwrap());
    let mut faults = Faults::none();
    faults
        .create_failures
        .insert(rotation.clone(), (failure, status));
    storage.set_faults(faults).await;
    let (operations, ana) = start(ana);
    (operations, ana, storage, rotation)
}

/// Coven's own rotation is never `Synced` while it fails, and the app
/// cannot retry it, nor needs to. A failure that clears on its own retries
/// under backoff; one a person must clear waits, shown, for a new
/// connection or credentials; one that never clears stays shown (E5).
#[tokio::test]
async fn a_refused_rotation_is_reported_and_retried_as_its_failure_clears() {
    use coven_foundation::clock::Clock;
    for (failure, status, retry) in [
        (StorageFailure::QuotaExceeded, 507, Retry::Automatic),
        (StorageFailure::PermissionDenied, 403, Retry::AppAction),
        (StorageFailure::Refused, 400, Retry::Never),
    ] {
        let (operations, ana, storage, rotation) = refused_rotation(failure, status).await;
        let failing = operations
            .sync()
            .await
            .unwrap()
            .expect("the rotation is reported");
        let PendingSubject::Operation {
            id,
            kind: OperationKind::RotateKey,
        } = failing.subject
        else {
            panic!("{failing:?}");
        };
        assert!(
            matches!(failing.reason, PendingReason::Storage { failure: found, .. } if found == failure)
        );
        assert_eq!(failing.retry(), retry, "{failure:?}");
        assert!(matches!(
            operations.retry_pending_operation(id).await,
            Err(SyncError::NotPending(found)) if found == id
        ));
        assert!(storage.status(&write_path(1)).await.unwrap().is_none());
        storage.set_faults(Faults::none()).await;
        // The reserved entry also waits in the store-log queue, which every
        // pass sends; only the rotation's own retry finishes the operation.
        ana.clock
            .set(ana.clock.now() + std::time::Duration::from_secs(600));
        let after_backoff = operations.sync().await.unwrap();
        if retry == Retry::Automatic {
            assert_eq!(after_backoff, None);
        } else {
            assert!(after_backoff.is_some_and(|record| record.subject == failing.subject));
            // A new connection is what a person's action brings.
            operations
                .set_storage(Some(storage.clone() as Arc<dyn Storage>))
                .await
                .unwrap();
            let reconnected = operations.sync().await.unwrap();
            assert_eq!(
                reconnected.is_none(),
                retry == Retry::AppAction,
                "{failure:?}"
            );
        }
        assert_eq!(
            ana.db.operation(id).await.unwrap().is_none(),
            retry != Retry::Never,
            "{failure:?}"
        );
        assert_eq!(
            storage.status(&rotation).await.unwrap().is_some(),
            retry != Retry::Never,
            "{failure:?}"
        );
        operations.close().await.unwrap();
    }
}

/// Coven's own work for an audience this member is not in has nothing left
/// to do: a rotation or a snapshot of it finishes without publishing.
#[tokio::test]
async fn coven_work_for_an_audience_left_finishes_without_publishing() {
    use crate::operation_data::{EntryWork, Intent};
    use crate::snapshot_data::{SnapshotJob, SnapshotTask, SnapshotTrigger};
    let storage = storage();
    let ana = group(storage.clone(), 1).await.remove(0);
    let left = coven_merge::Audience::Circle(coven_foundation::id_source::CircleId(
        uuid::Uuid::from_u128(77),
    ));
    for data in [
        Data::Entry(EntryWork {
            intent: Intent::RotateKey {
                audience: left.clone(),
            },
            entry: None,
        }),
        Data::Snapshots(SnapshotTask {
            job: SnapshotJob::Write {
                audience: left.clone(),
                device: DeviceId(1),
                trigger: SnapshotTrigger::Requested,
                attempt: None,
                abandoned: Vec::new(),
            },
            temporary: Vec::new(),
        }),
    ] {
        ana.db
            .start_operation(data.new_operation("coven").unwrap())
            .await
            .unwrap();
    }
    let before = storage.request_trace().len();
    let (operations, ana) = start(ana);
    assert_eq!(operations.sync().await.unwrap(), None);
    assert!(ana.db.operations().await.unwrap().is_empty());
    assert!(!storage.request_trace()[before..]
        .iter()
        .any(|request| matches!(request, MemoryRequest::Create(path)
            if path.store_log_position().is_some() || path.key_copy_position().is_some())));
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

/// IO `PassPhases.run` over `readLog`: a first phase whose entry log reaches
/// a refused entry, or whose copies include an unreadable prefix, is
/// incomplete, so the pass makes no first attempt and only
/// resends an attempted write, under the previous pass's catch-up while it is
/// fresh. A failed entry or copy read ends the pass. The real pass agrees.
#[test]
#[ignore = "requires COVEN_IO_RUNNER"]
fn io_differential_pass_phases() {
    use coven_foundation::clock::Clock;
    use coven_storage::test_utils::Faults;
    use serde_json::json;
    use std::time::Duration;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut cases = Vec::new();
    let mut results = Vec::new();
    for (entry_failed, entry_refused, copy_failed, unreadable, attempted, expired) in [
        (false, false, false, false, false, false),
        (true, false, false, false, false, false),
        (false, true, false, false, false, false),
        (false, false, true, false, false, false),
        (false, false, false, false, true, false),
        (false, true, false, false, true, false),
        (false, true, false, false, true, true),
        (false, false, false, false, true, true),
        (true, false, false, false, true, false),
        (false, false, true, false, true, false),
        (false, false, false, true, false, false),
        (false, false, false, true, true, false),
        (false, false, false, true, true, true),
    ] {
        let (first_attempt, retried) = runtime.block_on(async {
            let storage = storage();
            let devices = if unreadable { 2 } else { 1 };
            let mut ana = group(storage.clone(), devices).await.remove(0);
            sql(&ana.db, "INSERT INTO notes VALUES('one','title','body')").await;
            if attempted {
                // Write 1's first attempt fixes its keys, then fails.
                let mut faults = Faults::none();
                faults.fail_next = 1;
                storage.set_faults(faults).await;
                assert!(ana.writes.upload_writes().await.is_err());
                storage.set_faults(Faults::none()).await;
                let fixed = ana
                    .db
                    .read_oldest_upload(|upload| {
                        Ok::<_, coven_database::DbError>(upload.keys.is_some())
                    })
                    .await
                    .unwrap();
                assert_eq!(fixed, Some(true));
                sql(&ana.db, "INSERT INTO notes VALUES('two','title','body')").await;
            }
            if expired {
                // The last completed catch-up started five minutes ago.
                ana.clock.set(ana.clock.now() + Duration::from_secs(300));
            }
            if entry_refused {
                plant_refused_entry(&storage).await;
            }
            if unreadable {
                plant_unreadable_copy(&storage).await;
            }
            let mut faults = Faults::none();
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
            let present = |number| {
                let storage = storage.clone();
                async move { storage.status(&write_path(number)).await.unwrap().is_some() }
            };
            let untried = if attempted { 2 } else { 1 };
            let sent = (present(untried).await, attempted && present(1).await);
            operations.close().await.unwrap();
            sent
        });
        cases.push(json!({"check":"phases",
            "entry":{"count":usize::from(entry_refused),"failed":entry_failed,"refused":entry_refused},
            "copy":{"count":0,"failed":copy_failed,"refused":false,"unreadable":unreadable},
            "attempted":attempted,"elapsed":if expired { 300 } else { 0 }}));
        results.push(json!({"first_attempt":first_attempt,"retried":retried}));
    }
    assert_eq!(results, crate::discovery::tests::io_runner(&cases));
}
