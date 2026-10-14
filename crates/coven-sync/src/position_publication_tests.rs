use crate::device_log_sync::tests::{group, storage, STORE_ID};
use crate::posted_positions::{observe, open, tests::signed_positions};
use crate::store_log_sync::tests::Device;
use crate::{PendingReason as Reason, PendingRecord, PendingSubject as Subject};
use coven_format::{
    pending::{PendingReason, PendingReport, PendingSubject, RefusalCode},
    sealed_single::SingleChunkPrefix,
    Object,
};
use coven_foundation::id_source::{DeviceId, KeyId};
use coven_storage::{
    test_utils::{Faults, MemoryRequest, MemoryStorage},
    ObjectPath, ObjectPrefix, Storage,
};
use std::sync::Arc;
use uuid::Uuid;

use serde_json::json;

async fn empty_device(storage: Arc<MemoryStorage>) -> Device {
    use crate::store_log_sync::tests::{device, key, member, store};
    let mut device = device(storage, 1, member(1), store(1)).await;
    device.create(key(1)).await;
    device
}

async fn pending(device: &Device) -> Vec<PendingRecord> {
    device.db.pending::<PendingRecord>().await.unwrap()
}

#[tokio::test]
async fn agreement_and_retention_share_one_observation_and_equal_revision_has_no_writes() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    devices[1].writes.post_positions().await.unwrap();
    let peer = ObjectPath::positions(STORE_ID, DeviceId(2));
    let receiver = &mut devices[0];
    let start = storage.request_trace().len();
    {
        let _pass = receiver.sync.begin_pass(&mut receiver.writes);
        receiver.writes.observe_positions().await.unwrap();
        receiver.writes.download_writes().await.unwrap();
        receiver.sync.run_retention().await.unwrap();
    }
    let trace = storage.request_trace();
    assert_eq!(
        trace[start..]
            .iter()
            .filter(
                |r| matches!(r, MemoryRequest::List(p) if p == &ObjectPrefix::positions(STORE_ID))
            )
            .count(),
        1
    );
    assert_eq!(
        trace[start..]
            .iter()
            .filter(|r| matches!(r, MemoryRequest::Read(p, _) if p == &peer))
            .count(),
        1
    );
    let work = coven_database::test_utils::WorkCounter::default();
    receiver.db.test_track_work(work.clone()).await.unwrap();
    let start = storage.request_trace().len();
    receiver.writes.observe_positions().await.unwrap();
    assert_eq!(work.take().commits, 0);
    assert_eq!(
        &storage.request_trace()[start..],
        &[MemoryRequest::List(ObjectPrefix::positions(STORE_ID))]
    );
}

#[tokio::test]
async fn same_size_same_time_replacement_changes_checked_positions() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    devices[1].writes.post_positions().await.unwrap();
    let path = ObjectPath::positions(STORE_ID, DeviceId(2));
    let first = storage.status(&path).await.unwrap().unwrap();
    devices[0].writes.download_writes().await.unwrap();
    let mut peer = devices[1]
        .writes
        .current_positions()
        .await
        .unwrap()
        .unwrap();
    peer.fingerprints[0].bytes = coven_crypto::Fingerprint::from_bytes([47; 32]);
    storage
        .replace(&path, &signed_positions(&devices[1], peer))
        .await
        .unwrap();
    let second = storage.status(&path).await.unwrap().unwrap();
    assert_eq!(
        (first.size, first.stored_at),
        (second.size, second.stored_at)
    );
    assert_ne!(first.revision, second.revision);
    devices[0].writes.download_writes().await.unwrap();
    assert!(pending(&devices[0])
        .await
        .iter()
        .any(|r| r.reason == Reason::Disagrees));
}

#[tokio::test]
async fn invalid_revision_and_confirmed_post_survive_reopening() {
    let storage = storage();
    let mut device = empty_device(storage.clone()).await;
    device.writes.post_positions().await.unwrap();
    device.writes.observe_positions().await.unwrap();
    device.reopen(storage.clone(), vec![], vec![]).await;
    // Current fingerprints need a completed catch-up's copy observation.
    device.catch_up().await;
    let start = storage.request_trace().len();
    assert!(device.writes.post_positions().await.unwrap());
    assert_eq!(
        &storage.request_trace()[start..],
        &[MemoryRequest::List(ObjectPrefix::positions(STORE_ID))]
    );
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
    let incomplete = SingleChunkPrefix::PostedPositions(KeyId(Uuid::from_u128(1)))
        .encode()
        .unwrap();
    storage.replace(&path, &incomplete).await.unwrap();
    device.writes.observe_positions().await.unwrap();
    assert!(pending(&device)
        .await
        .iter()
        .any(|r| matches!(r.reason, Reason::InvalidPositions(_))));
    device.reopen(storage.clone(), vec![], vec![]).await;
    let start = storage.request_trace().len();
    let work = coven_database::test_utils::WorkCounter::default();
    device.db.test_track_work(work.clone()).await.unwrap();
    device.writes.observe_positions().await.unwrap();
    assert_eq!(work.take().commits, 0);
    assert_eq!(
        &storage.request_trace()[start..],
        &[MemoryRequest::List(ObjectPrefix::positions(STORE_ID))]
    );
    storage.delete(&path).await.unwrap();
    device.writes.observe_positions().await.unwrap();
    device.catch_up().await;
    assert!(device.writes.post_positions().await.unwrap());
    assert!(!pending(&device)
        .await
        .iter()
        .any(|r| matches!(r.subject, Subject::Positions(_))));
}

#[tokio::test]
async fn failed_scan_retains_reports_and_the_preceding_checked_observation() {
    use crate::device_log_sync::tests::sql;
    let storage = storage();
    let devices = group(storage.clone(), 3).await;
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','first','body')",
    )
    .await;
    let own = coven_merge::WriteId {
        device: DeviceId(1),
        number: 1,
    };
    let report = PendingReport {
        subject: PendingSubject::Write(own),
        reason: PendingReason::Refused(RefusalCode::WrongIdentity),
    };
    for device in &devices[1..] {
        let mut peer = device.writes.current_positions().await.unwrap().unwrap();
        peer.pending = vec![report.clone()];
        storage
            .replace(
                &ObjectPath::positions(STORE_ID, peer.device),
                &signed_positions(device, peer),
            )
            .await
            .unwrap();
    }
    devices[0].writes.observe_positions().await.unwrap();
    let before = devices[0]
        .db
        .positions_observation::<serde_json::Value>()
        .await
        .unwrap();
    let reports: Vec<_> = pending(&devices[0])
        .await
        .into_iter()
        .filter(|r| r.reported_by != DeviceId(1))
        .collect();
    assert_eq!(reports.len(), 2);
    for device in &devices[1..] {
        let peer = device.writes.current_positions().await.unwrap().unwrap();
        storage
            .replace(
                &ObjectPath::positions(STORE_ID, peer.device),
                &signed_positions(device, peer),
            )
            .await
            .unwrap();
    }
    storage
        .set_faults(Faults {
            read_failures: [(
                ObjectPath::positions(STORE_ID, DeviceId(3)),
                coven_storage::StorageFailure::Network,
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    assert!(devices[0].writes.observe_positions().await.is_err());
    assert_eq!(
        devices[0]
            .db
            .positions_observation::<serde_json::Value>()
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        pending(&devices[0])
            .await
            .into_iter()
            .filter(|r| r.reported_by != DeviceId(1))
            .collect::<Vec<_>>(),
        reports
    );
    assert!(pending(&devices[0])
        .await
        .iter()
        .any(|r| r.subject == Subject::Positions(DeviceId(3))
            && matches!(r.reason, Reason::Storage { .. })));
    storage.set_faults(Faults::none()).await;
    devices[0].writes.observe_positions().await.unwrap();
    assert!(pending(&devices[0]).await.is_empty());
}

#[tokio::test]
async fn incomparable_peers_do_not_create_or_clear_disagreement() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    let matching = devices[1]
        .writes
        .current_positions()
        .await
        .unwrap()
        .unwrap();
    let path = ObjectPath::positions(STORE_ID, DeviceId(2));
    for variant in 0..5 {
        let mut peer = matching.clone();
        peer.fingerprints[0].bytes = coven_crypto::Fingerprint::from_bytes([72; 32]);
        match variant {
            0 => peer.writes.0.push(coven_merge::WriteId {
                device: DeviceId(2),
                number: 1,
            }),
            1 => peer.store_log.0.clear(),
            2 => peer.schema_version += 1,
            3 => peer.fingerprints[0].key = KeyId(Uuid::from_u128(999)),
            _ => peer.fingerprints.clear(),
        }
        storage
            .replace(&path, &signed_positions(&devices[1], peer))
            .await
            .unwrap();
        devices[0].writes.download_writes().await.unwrap();
        assert!(!pending(&devices[0])
            .await
            .iter()
            .any(|r| r.reason == Reason::Disagrees));
    }
    let mut peer = matching.clone();
    peer.fingerprints[0].bytes = coven_crypto::Fingerprint::from_bytes([72; 32]);
    storage
        .replace(&path, &signed_positions(&devices[1], peer.clone()))
        .await
        .unwrap();
    devices[0].writes.download_writes().await.unwrap();
    assert!(pending(&devices[0])
        .await
        .iter()
        .any(|r| r.reason == Reason::Disagrees));
    peer.schema_version += 1;
    storage
        .replace(&path, &signed_positions(&devices[1], peer))
        .await
        .unwrap();
    devices[0].writes.download_writes().await.unwrap();
    assert!(pending(&devices[0])
        .await
        .iter()
        .any(|r| r.reason == Reason::Disagrees));
    storage
        .replace(&path, &signed_positions(&devices[1], matching))
        .await
        .unwrap();
    devices[0].writes.download_writes().await.unwrap();
    assert!(!pending(&devices[0])
        .await
        .iter()
        .any(|r| r.reason == Reason::Disagrees));
}

#[tokio::test]
async fn failed_publication_retains_confirmation_and_reopening_confirms_a_lost_reply() {
    let storage = storage();
    let mut device = empty_device(storage.clone()).await;
    device.writes.post_positions().await.unwrap();
    let confirmed = device
        .db
        .positions_post::<serde_json::Value>()
        .await
        .unwrap()
        .unwrap()["confirmed"]
        .clone();
    device
        .db
        .record_pending(PendingRecord {
            subject: Subject::Write(coven_merge::WriteId {
                device: DeviceId(2),
                number: 1,
            }),
            reason: Reason::Refused(crate::Refusal::WrongIdentity { cause: None }),
            reported_by: DeviceId(1),
        })
        .await
        .unwrap();
    {
        let _pass = device.sync.begin_pass(&mut device.writes);
        device.writes.observe_positions().await.unwrap();
        storage
            .set_faults(Faults {
                fail_next: 1,
                ..Faults::none()
            })
            .await;
        assert!(device.writes.post_positions().await.is_err());
    }
    let attempt = device
        .db
        .positions_post::<serde_json::Value>()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt["confirmed"], confirmed);
    let bytes: Vec<u8> = serde_json::from_value(attempt["attempt"]["sealed"].clone()).unwrap();
    // Materialize an accepted replacement whose reply the sender never received.
    storage
        .replace(&ObjectPath::positions(STORE_ID, DeviceId(1)), &bytes)
        .await
        .unwrap();
    device.reopen(storage.clone(), vec![], vec![]).await;
    device.writes.observe_positions().await.unwrap();
    assert!(pending(&device)
        .await
        .iter()
        .any(|r| matches!(r.reason, Reason::Storage { .. })));
    // Current fingerprints need a completed catch-up's copy observation.
    device.catch_up().await;
    let start = storage.request_trace().len();
    assert!(device.writes.post_positions().await.unwrap());
    assert!(!storage.request_trace()[start..]
        .iter()
        .any(|r| matches!(r, MemoryRequest::Replace(_))));
    let after = device
        .db
        .positions_post::<serde_json::Value>()
        .await
        .unwrap()
        .unwrap();
    assert!(after["attempt"].is_null());
    assert_eq!(after["confirmed"], attempt["attempt"]["plaintext"]);
}

#[tokio::test]
async fn idle_positions_and_pending_reports_do_not_commit_or_echo_received_reports() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    let mut peer = devices[1]
        .writes
        .current_positions()
        .await
        .unwrap()
        .unwrap();
    peer.pending = vec![PendingReport {
        subject: PendingSubject::Positions(DeviceId(1)),
        reason: PendingReason::InvalidPositions(RefusalCode::Parse),
    }];
    storage
        .replace(
            &ObjectPath::positions(STORE_ID, DeviceId(2)),
            &signed_positions(&devices[1], peer),
        )
        .await
        .unwrap();
    devices[0].writes.post_positions().await.unwrap();
    devices[0].writes.observe_positions().await.unwrap();
    let work = coven_database::test_utils::WorkCounter::default();
    devices[0].db.test_track_work(work.clone()).await.unwrap();
    let start = storage.request_trace().len();
    assert!(devices[0].writes.post_positions().await.unwrap());
    assert_eq!(work.take().commits, 0);
    assert_eq!(
        &storage.request_trace()[start..],
        &[MemoryRequest::List(ObjectPrefix::positions(STORE_ID))]
    );
    assert_eq!(pending(&devices[0]).await.len(), 1);
    let bytes = storage
        .read(&ObjectPath::positions(STORE_ID, DeviceId(1)))
        .await
        .unwrap();
    let posted = open(
        &bytes,
        &ObjectPath::positions(STORE_ID, DeviceId(1)),
        devices[0].custody.read().unwrap().as_ref(),
        &devices[0].db.store_log().await.unwrap(),
    )
    .unwrap()
    .unwrap();
    assert!(posted.pending.is_empty());
}

#[tokio::test]
async fn an_initial_post_with_own_uploads_claims_only_the_empty_past() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    crate::device_log_sync::tests::sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','title','body')",
    )
    .await;
    assert!(!devices[0].writes.post_positions().await.unwrap());
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
    let posted = open(
        &storage.read(&path).await.unwrap(),
        &path,
        devices[0].custody.read().unwrap().as_ref(),
        &devices[0].db.store_log().await.unwrap(),
    )
    .unwrap()
    .unwrap();
    assert!(posted.writes.0.is_empty());
    assert!(posted.store_log.0.is_empty());
    assert!(posted.fingerprints.is_empty());
    devices[0].writes.upload_writes().await.unwrap();
    assert!(devices[0].writes.post_positions().await.unwrap());
    let posted = open(
        &storage.read(&path).await.unwrap(),
        &path,
        devices[0].custody.read().unwrap().as_ref(),
        &devices[0].db.store_log().await.unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        posted.writes.0,
        [coven_merge::WriteId {
            device: DeviceId(1),
            number: 1
        }]
    );
}

#[tokio::test]
async fn waiting_author_and_key_reuse_retained_bytes_after_the_prerequisite_arrives() {
    use coven_format::store_log::StoreChange;
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    let device = &mut devices[0];
    let mut peer = device.writes.current_positions().await.unwrap().unwrap();
    peer.device = DeviceId(2);
    let path = ObjectPath::positions(STORE_ID, peer.device);
    storage
        .replace(&path, &signed_positions(device, peer))
        .await
        .unwrap();
    device.writes.observe_positions().await.unwrap();
    assert!(pending(device)
        .await
        .iter()
        .any(|r| r.reason == Reason::Waits(crate::Prerequisite::DeviceRegistration(DeviceId(2)))));
    let reads = storage.reads().await;
    device.writes.observe_positions().await.unwrap();
    assert_eq!(storage.reads().await, reads);
    device
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(2),
            name: "two".into(),
            replaces: None,
        })
        .await
        .unwrap();
    let reads = storage.reads().await;
    assert!(device
        .writes
        .observe_positions()
        .await
        .unwrap()
        .positions
        .contains_key(&DeviceId(2)));
    assert_eq!(storage.reads().await, reads);
    assert!(pending(device).await.is_empty());
    let mut peer = device.writes.current_positions().await.unwrap().unwrap();
    peer.device = DeviceId(2);
    storage
        .replace(&path, &signed_positions(device, peer))
        .await
        .unwrap();
    let local = device.db.local_store_log().await.unwrap();
    let pass_reads = crate::pass_reads::PassReads::default();
    observe(&device.db, &pass_reads, storage.as_ref(), &local, None)
        .await
        .unwrap();
    assert!(pending(device)
        .await
        .iter()
        .any(|r| matches!(r.reason, Reason::KeyUnavailable { .. })));
    let reads = storage.reads().await;
    device.writes.observe_positions().await.unwrap();
    assert_eq!(storage.reads().await, reads);
    assert!(pending(device).await.is_empty());
}

#[tokio::test]
async fn retained_reports_become_relevant_when_local_work_changes_within_the_pass() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    let mut peer = devices[1]
        .writes
        .current_positions()
        .await
        .unwrap()
        .unwrap();
    peer.pending = vec![PendingReport {
        subject: PendingSubject::Write(coven_merge::WriteId {
            device: DeviceId(1),
            number: 1,
        }),
        reason: PendingReason::Refused(RefusalCode::Parse),
    }];
    storage
        .replace(
            &ObjectPath::positions(STORE_ID, DeviceId(2)),
            &signed_positions(&devices[1], peer),
        )
        .await
        .unwrap();
    let device = &mut devices[0];
    let _pass = device.sync.begin_pass(&mut device.writes);
    device.writes.observe_positions().await.unwrap();
    assert!(pending(device).await.is_empty());
    crate::device_log_sync::tests::sql(
        &device.db,
        "INSERT INTO notes VALUES('one','title','body')",
    )
    .await;
    let requests = storage.request_trace();
    device.writes.observe_positions().await.unwrap();
    assert_eq!(storage.request_trace(), requests);
    assert_eq!(pending(device).await.len(), 1);
    assert_eq!(pending(device).await[0].reported_by, DeviceId(2));
}

#[tokio::test]
async fn unfinished_reload_keeps_confirmation_while_new_reports_travel() {
    use crate::snapshot_data::{ReloadScope, SnapshotJob, SnapshotTask};
    let storage = storage();
    let mut device = empty_device(storage.clone()).await;
    assert!(device.writes.post_positions().await.unwrap());
    let before = device.writes.current_positions().await.unwrap().unwrap();
    let reload = crate::operation_data::Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Reload {
            scope: ReloadScope::All,
            files: None,
        },
        temporary: Vec::new(),
    });
    device
        .db
        .start_operation(reload.new_operation("test").unwrap())
        .await
        .unwrap();
    device
        .db
        .record_pending(PendingRecord {
            subject: Subject::Write(coven_merge::WriteId {
                device: DeviceId(2),
                number: 1,
            }),
            reason: Reason::Refused(crate::Refusal::Signature { cause: None }),
            reported_by: DeviceId(1),
        })
        .await
        .unwrap();
    assert!(device.writes.current_positions().await.unwrap().is_none());
    assert!(!device.writes.post_positions().await.unwrap());
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
    let after = open(
        &storage.read(&path).await.unwrap(),
        &path,
        device.custody.read().unwrap().as_ref(),
        &device.db.store_log().await.unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(after.writes, before.writes);
    assert_eq!(after.store_log, before.store_log);
    assert_eq!(after.fingerprints, before.fingerprints);
    assert_eq!(after.pending.len(), 1);
}

#[tokio::test]
async fn a_retry_settles_its_exact_bytes_before_posting_newer_pending_records() {
    let storage = storage();
    let mut device = empty_device(storage.clone()).await;
    assert!(device.writes.post_positions().await.unwrap());
    let subject = Subject::Write(coven_merge::WriteId {
        device: DeviceId(2),
        number: 1,
    });
    device
        .db
        .record_pending(PendingRecord {
            subject: subject.clone(),
            reason: Reason::Refused(crate::Refusal::Signature { cause: None }),
            reported_by: DeviceId(1),
        })
        .await
        .unwrap();
    {
        let _pass = device.sync.begin_pass(&mut device.writes);
        device.writes.observe_positions().await.unwrap();
        storage
            .set_faults(Faults {
                fail_next: 1,
                ..Faults::none()
            })
            .await;
        assert!(device.writes.post_positions().await.is_err());
    }
    let attempted = device
        .db
        .positions_post::<serde_json::Value>()
        .await
        .unwrap()
        .unwrap();
    let expected: Vec<u8> = serde_json::from_value(attempted["attempt"]["sealed"].clone()).unwrap();
    device.db.clear_pending(subject).await.unwrap();
    device.reopen(storage.clone(), vec![], vec![]).await;
    device.catch_up().await;
    let start = storage.request_trace().len();
    assert!(!device.writes.post_positions().await.unwrap());
    assert_eq!(
        storage.request_trace()[start..]
            .iter()
            .filter(|r| matches!(r, MemoryRequest::Replace(_)))
            .count(),
        1
    );
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
    assert_eq!(storage.read(&path).await.unwrap(), expected);
    assert!(device.writes.post_positions().await.unwrap());
    let after = open(
        &storage.read(&path).await.unwrap(),
        &path,
        device.custody.read().unwrap().as_ref(),
        &device.db.store_log().await.unwrap(),
    )
    .unwrap()
    .unwrap();
    assert!(after.pending.is_empty());
}

#[tokio::test]
async fn unsupported_positions_version_is_retained_until_replaced() {
    let storage = storage();
    let mut device = empty_device(storage.clone()).await;
    device.writes.post_positions().await.unwrap();
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
    let original = storage.read(&path).await.unwrap();
    let mut unsupported = original.clone();
    unsupported[1..3].copy_from_slice(&u16::MAX.to_be_bytes());
    storage.replace(&path, &unsupported).await.unwrap();
    device.writes.observe_positions().await.unwrap();
    assert!(pending(&device).await.iter().any(|r| matches!(
        r.reason,
        Reason::UpdateRequired(crate::RequiredUpdate::CovenFormat { version: u16::MAX })
    )));
    device.reopen(storage.clone(), vec![], vec![]).await;
    let start = storage.request_trace().len();
    device.writes.observe_positions().await.unwrap();
    assert_eq!(
        &storage.request_trace()[start..],
        &[MemoryRequest::List(ObjectPrefix::positions(STORE_ID))]
    );
    storage.replace(&path, &original).await.unwrap();
    device.writes.observe_positions().await.unwrap();
    // Current fingerprints need a completed catch-up's copy observation.
    device.catch_up().await;
    assert!(device.writes.post_positions().await.unwrap());
    assert!(pending(&device).await.is_empty());
}

#[tokio::test]
async fn replacing_own_invalid_positions_withdraws_that_report_in_the_same_post() {
    let storage = storage();
    let mut device = empty_device(storage.clone()).await;
    device.writes.post_positions().await.unwrap();
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
    let incomplete = SingleChunkPrefix::PostedPositions(KeyId(Uuid::from_u128(1)))
        .encode()
        .unwrap();
    storage.replace(&path, &incomplete).await.unwrap();
    device.writes.observe_positions().await.unwrap();
    assert!(!pending(&device).await.is_empty());
    assert!(device.writes.post_positions().await.unwrap());
    let after = open(
        &storage.read(&path).await.unwrap(),
        &path,
        device.custody.read().unwrap().as_ref(),
        &device.db.store_log().await.unwrap(),
    )
    .unwrap()
    .unwrap();
    assert!(after.pending.is_empty());
    assert!(pending(&device).await.is_empty());
    let before = storage.request_trace().len();
    device.writes.post_positions().await.unwrap();
    assert!(!storage.request_trace()[before..]
        .iter()
        .any(|r| matches!(r, MemoryRequest::Replace(_))));
}

#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER"]
async fn io_differential_positions() {
    let storage = storage();
    let mut device = empty_device(storage.clone()).await;
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
    let mut actions = Vec::new();
    let mut fetched = Vec::new();
    let mut positions = device.writes.current_positions().await.unwrap().unwrap();
    for (index, changed) in [true, false, true, false, true, false]
        .into_iter()
        .enumerate()
    {
        if changed {
            positions.fingerprints[0].bytes =
                coven_crypto::Fingerprint::from_bytes([index as u8; 32]);
            storage
                .replace(&path, &signed_positions(&device, positions.clone()))
                .await
                .unwrap();
        }
        if index % 2 == 1 {
            device.reopen(storage.clone(), vec![], vec![]).await;
        }
        let metadata = storage.status(&path).await.unwrap().unwrap();
        let before = storage.reads().await.len();
        device.writes.observe_positions().await.unwrap();
        fetched.push(
            storage.reads().await[before..]
                .iter()
                .map(|(_, _, size)| size)
                .sum::<u64>(),
        );
        actions.push(
            json!({"size":metadata.size,"revision":metadata.revision.parse::<u64>().unwrap()}),
        );
    }
    let result =
        crate::discovery::tests::io_runner(&[json!({"check":"positions","actions":actions})]);
    assert_eq!(result, [json!(fetched)]);

    let storage = crate::device_log_sync::tests::storage();
    let mut device = empty_device(storage.clone()).await;
    let mut confirmed: Option<Vec<u8>> = None;
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for action in [
        "initial",
        "idle",
        "pending",
        "idle",
        "reopen",
        "disappear",
        "idle",
    ] {
        if action == "pending" {
            device
                .db
                .record_pending(PendingRecord {
                    subject: Subject::Write(coven_merge::WriteId {
                        device: DeviceId(2),
                        number: 1,
                    }),
                    reason: Reason::Missing {
                        path: ObjectPath::device_log(STORE_ID, DeviceId(2), 1.try_into().unwrap()),
                    },
                    reported_by: DeviceId(1),
                })
                .await
                .unwrap();
        }
        if action == "reopen" {
            device.reopen(storage.clone(), vec![], vec![]).await;
            device.catch_up().await;
        }
        if action == "disappear" {
            storage.delete(&path).await.unwrap();
        }
        let current =
            Object::PostedPositions(device.writes.current_positions().await.unwrap().unwrap())
                .encode()
                .unwrap();
        let present = action != "initial" && action != "disappear";
        cases.push(
            json!({"check":"post","confirmed":confirmed,"present":present,"current":current}),
        );
        let before = storage.request_trace().len();
        assert!(device.writes.post_positions().await.unwrap());
        let replacements = storage.request_trace()[before..]
            .iter()
            .filter(|r| matches!(r, MemoryRequest::Replace(_)))
            .count();
        assert!(replacements <= 1);
        actual.push(json!(replacements == 1));
        confirmed = Some(current);
    }
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}

/// The posted positions' envelope key and fingerprint keys, as stored.
async fn posted_keys(
    storage: &MemoryStorage,
    device: &Device,
) -> (KeyId, Vec<(coven_merge::Audience, KeyId)>) {
    let local = device.db.local_store_log().await.unwrap();
    let path = ObjectPath::positions(local.store, local.device);
    let bytes = storage.read(&path).await.unwrap();
    let sealed = coven_format::sealed_single::SingleChunkObject::decode(&bytes).unwrap();
    let SingleChunkPrefix::PostedPositions(envelope) = sealed.prefix().clone() else {
        panic!("positions prefix");
    };
    let ring = device.custody.read().unwrap().unwrap();
    let plain = ring
        .store_key(envelope)
        .unwrap()
        .derive()
        .open_object_chunk(
            path.as_str(),
            &sealed.prefix().encode().unwrap(),
            0,
            0,
            sealed.chunk(),
        )
        .unwrap();
    let Object::PostedPositions(posted) = Object::decode(&plain).unwrap() else {
        panic!("posted positions");
    };
    let keys = posted
        .fingerprints
        .into_iter()
        .map(|f| (f.audience, f.key))
        .collect();
    (envelope, keys)
}

/// Lean `PositionFingerprints.readable_store_fingerprint_reveals_write`: Ana
/// removes Ben and rotates to K2. Carol, not having read the removal, rotates
/// to K3 and seals it to Ben too. K3 is newer and Ben holds it, so a store
/// fingerprint under K3 would let him test guesses of writes sealed with K2.
/// Ana's positions may be sealed with K3 (§11), but fingerprint with K2.
#[tokio::test]
async fn store_fingerprint_skips_a_newer_key_sealed_to_a_removed_member() {
    use crate::store_log_sync::tests::{device, find_copy, key, member, storage, store};
    use coven_format::store_log::{MemberRole, StoreChange};
    use coven_merge::Audience;
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let ben = member(2);
    let mut carol = device(storage.clone(), 3, member(3), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben, MemberRole::Member).await;
    ana.add(&carol.member, MemberRole::Member).await;
    carol.sync().await;
    carol.sync.load_new_device("Carol", None).await.unwrap();
    ana.sync().await;
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: ben.member(),
        })
        .await
        .unwrap();
    ana.sync.test_rotate(Audience::Store, key(2)).await;
    // Her rotation's timestamp is the newer one.
    let now = coven_foundation::clock::Clock::now(carol.clock.as_ref());
    carol.clock.set(now + std::time::Duration::from_secs(1));
    carol.sync.test_rotate(Audience::Store, key(3)).await;
    assert!(
        find_copy(storage.as_ref(), Audience::Store, key(3), &ben.member())
            .await
            .is_some(),
        "Carol's view still has Ben"
    );
    ana.sync().await;
    let log = ana.log().await;
    assert!(crate::effects::member(&log.replay.state, &ben.member()).is_none());
    let ring = ana.custody.read().unwrap().unwrap();
    assert!(ring.store_key(key(2)).is_ok() && ring.store_key(key(3)).is_ok());
    assert_eq!(
        crate::write_seal::readable_key(&log, &ring, &Audience::Store).unwrap(),
        key(3)
    );
    assert!(ana.writes.post_positions().await.unwrap());
    assert_eq!(
        posted_keys(&storage, &ana).await,
        (key(3), vec![(Audience::Store, key(2))])
    );
}

/// Lean `PositionFingerprints.readable_circle_fingerprint_reveals_write`: Ben
/// leaves Gifts but stays in the store, so storage revocation never ends his
/// reads of positions. Ana rotates Gifts to G2; Carol, not having read the
/// removal, rotates to G3 and seals it to Ben. Ana's Gifts fingerprint uses
/// G2, which Ben cannot obtain, not G3, which he holds.
#[tokio::test]
async fn circle_fingerprint_skips_a_newer_key_sealed_to_a_former_member() {
    use crate::store_log_sync::tests::{device, key, member, storage, store};
    use coven_format::store_log::{MemberRole, StoreChange};
    use coven_foundation::id_source::CircleId;
    use coven_merge::Audience;
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    let mut carol = device(storage.clone(), 3, member(3), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Member).await;
    ana.add(&carol.member, MemberRole::Member).await;
    for device in [&mut ben, &mut carol] {
        device.sync().await;
        device.sync.load_new_device("other", None).await.unwrap();
    }
    ana.sync().await;
    let circle = CircleId(Uuid::from_u128(10));
    let gifts = Audience::Circle(circle);
    let key_hash = ana.sync.test_key(gifts.clone(), key(10));
    ana.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle,
            name: "Gifts".into(),
            key: key(10),
            key_hash,
        })
        .await
        .unwrap();
    for other in [&ben.member, &carol.member] {
        ana.sync
            .make_and_upload_entry(StoreChange::AddCircleMember {
                circle,
                member: other.member(),
            })
            .await
            .unwrap();
    }
    carol.sync().await;
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle,
            member: ben.member.member(),
        })
        .await
        .unwrap();
    ana.sync.test_rotate(gifts.clone(), key(11)).await;
    // Her rotation's timestamp is the newer one.
    let now = coven_foundation::clock::Clock::now(carol.clock.as_ref());
    carol.clock.set(now + std::time::Duration::from_secs(1));
    carol.sync.test_rotate(gifts.clone(), key(12)).await;
    ana.sync().await;
    ben.sync().await;
    // Ben is still a store member and holds G3, but not G2.
    let state = ben.log().await.replay.state;
    assert!(crate::effects::member(&state, &ben.member.member()).is_some());
    let held = ben.custody.read().unwrap().unwrap();
    assert!(held.circle_key(circle, key(12)).is_ok());
    assert!(held.circle_key(circle, key(11)).is_err());
    let ring = ana.custody.read().unwrap().unwrap();
    assert_eq!(
        crate::write_seal::readable_key(&ana.log().await, &ring, &gifts).unwrap(),
        key(12)
    );
    assert!(ana.writes.post_positions().await.unwrap());
    assert_eq!(
        posted_keys(&storage, &ana).await,
        (key(1), vec![(Audience::Store, key(1)), (gifts, key(11))])
    );
}
