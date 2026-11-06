use super::*;

#[test]
fn cleanup_retains_both_causes_and_obeys_the_unfinished_causes_retry_gate() {
    assert_eq!(
        PendingReason::Rejected(crate::OperationRejection::NotAllowed).retry(),
        Retry::Never
    );
    let update = RequiredUpdate::AppSchema { version: 3 };
    for (cause, retry, before, after) in [
        (
            PendingReason::Storage {
                provider: CloudProvider::S3,
                failure: StorageFailure::Network,
                answered: false,
            },
            Retry::Automatic,
            true,
            true,
        ),
        (PendingReason::Paused, Retry::AppAction, false, false),
        (PendingReason::UpdateRequired(update), Retry::AfterUpdate, false, true),
        (PendingReason::Removed, Retry::Never, false, false),
    ] {
        let error = crate::SyncError::Cleanup {
            operation: Box::new(crate::SyncError::Rejected(crate::OperationRejection::NotAllowed)),
            cleanup: Box::new(crate::SyncError::Pending(Box::new(PendingRecord::new(
                PendingSubject::Connection,
                cause.clone(),
            )))),
        };
        let record = PendingRecord::new(
            PendingSubject::Operation { id: OperationId(1), kind: OperationKind::Invite },
            error.pending_reason(CloudProvider::S3),
        );
        let encoded = serde_json::to_vec(&record).unwrap();
        let reopened: PendingRecord = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(reopened, record);
        assert_eq!(
            reopened.reason,
            PendingReason::Cleanup {
                operation: Box::new(PendingReason::Rejected(crate::OperationRejection::NotAllowed)),
                cleanup: Box::new(cause),
            }
        );
        assert_eq!(reopened.retry(), retry);
        assert_eq!(reopened.can_retry(PendingRecord::BUILD, 2), before);
        assert_eq!(reopened.can_retry(PendingRecord::BUILD, 3), after);
    }
}

#[test]
fn conditional_conflicts_retry_without_becoming_throttling() {
    let record = PendingRecord::new(
        PendingSubject::Connection,
        PendingReason::Storage {
            provider: CloudProvider::S3,
            failure: StorageFailure::Conflict,
            answered: true,
        },
    );
    assert_eq!(record.retry(), Retry::Automatic);
}

/// Coven's own operation retries as its failure clears: by itself under
/// backoff, after a person acts, or never (E5).
#[test]
fn operation_failures_retry_as_they_clear_and_only_app_operations_wait_for_it() {
    let record = |subject, reason| PendingRecord::new(subject, reason);
    let operation = |kind| PendingSubject::Operation { id: OperationId(1), kind };
    let storage =
        |failure| PendingReason::Storage { provider: CloudProvider::S3, failure, answered: true };
    for (reason, coven) in [
        (PendingReason::Failed(LocalFailure::Disk), Retry::Automatic),
        (PendingReason::Failed(LocalFailure::Database), Retry::Automatic),
        (PendingReason::Failed(LocalFailure::KeyCustody), Retry::Automatic),
        (PendingReason::Failed(LocalFailure::Operation), Retry::Never),
        (storage(StorageFailure::Network), Retry::Automatic),
        (storage(StorageFailure::QuotaExceeded), Retry::Automatic),
        (storage(StorageFailure::File), Retry::Automatic),
        (storage(StorageFailure::Authentication), Retry::AppAction),
        (storage(StorageFailure::PermissionDenied), Retry::AppAction),
        (storage(StorageFailure::Refused), Retry::Never),
        (storage(StorageFailure::Protocol), Retry::Never),
    ] {
        for kind in [
            OperationKind::WriteSnapshot,
            OperationKind::RequiredReload,
            OperationKind::RotateKey,
            OperationKind::RevokeAccess,
        ] {
            assert_eq!(
                record(operation(kind), reason.clone()).retry(),
                coven,
                "{kind:?} {reason:?}"
            );
        }
        let app = match &reason {
            PendingReason::Storage { failure: StorageFailure::Network, .. } => Retry::Automatic,
            _ => Retry::AppAction,
        };
        for kind in [
            OperationKind::AddCircleMember,
            OperationKind::ReloadFromSnapshot,
            OperationKind::SchemaChange,
            OperationKind::Reset,
        ] {
            assert_eq!(record(operation(kind), reason.clone()).retry(), app, "{kind:?} {reason:?}");
        }
    }
    assert_eq!(record(PendingSubject::Slot(Slot(1)), PendingReason::Removed).retry(), Retry::Never);
}

/// No app call asks again for a slot or a pass, so a local failure that
/// clears by itself, such as a locked keychain, retries them under the shared
/// backoff. A lasting one still waits for the app (E5).
#[test]
fn local_failures_retry_slots_and_passes_as_they_clear() {
    for subject in [PendingSubject::Slot(Slot(4)), PendingSubject::Connection] {
        for (failure, retry) in [
            (LocalFailure::Database, Retry::Automatic),
            (LocalFailure::Disk, Retry::Automatic),
            (LocalFailure::KeyCustody, Retry::Automatic),
            (LocalFailure::Crypto, Retry::AppAction),
            (LocalFailure::Operation, Retry::AppAction),
        ] {
            let record = PendingRecord::new(subject.clone(), PendingReason::Failed(failure));
            assert_eq!(record.retry(), retry, "{record:?}");
            assert_eq!(record.can_retry(PendingRecord::BUILD, 1), retry == Retry::Automatic);
        }
    }
}

/// An upload retries by itself as its failure clears; a download only on the
/// app's next read or pin, which its call's failure tells it to make (E5).
#[test]
fn uploads_retry_by_themselves_and_downloads_on_the_apps_next_call() {
    let (device, object) = (DeviceId(1), ObjectId(uuid::Uuid::from_u128(1)));
    let upload = PendingSubject::Upload { device, object };
    let download = PendingSubject::Download { device, object };
    let storage =
        |failure| PendingReason::Storage { provider: CloudProvider::S3, failure, answered: false };
    for (reason, uploads, downloads) in [
        (storage(StorageFailure::Network), Retry::Automatic, Retry::AppAction),
        (storage(StorageFailure::QuotaExceeded), Retry::Automatic, Retry::AppAction),
        (storage(StorageFailure::File), Retry::Automatic, Retry::AppAction),
        (storage(StorageFailure::Authentication), Retry::AppAction, Retry::AppAction),
        (storage(StorageFailure::Protocol), Retry::Never, Retry::Never),
        (PendingReason::Failed(LocalFailure::Disk), Retry::Automatic, Retry::AppAction),
        (PendingReason::Waits(Prerequisite::SequenceCatchUp), Retry::Automatic, Retry::AppAction),
        (PendingReason::NoStorage, Retry::AppAction, Retry::AppAction),
        (
            PendingReason::FileUnavailable(FileMissingReason::Lost),
            Retry::AppAction,
            Retry::AppAction,
        ),
        (
            PendingReason::Refused(crate::Refusal::ContentHash),
            Retry::AfterUpdate,
            Retry::AfterUpdate,
        ),
    ] {
        let uploaded = PendingRecord::new(upload.clone(), reason.clone());
        let downloaded = PendingRecord::new(download.clone(), reason);
        assert_eq!(uploaded.retry(), uploads, "{uploaded:?}");
        assert_eq!(downloaded.retry(), downloads, "{downloaded:?}");
        assert!(!downloaded.can_retry(PendingRecord::BUILD, 1));
    }
    let source = PendingReason::FileUnavailable(FileMissingReason::Source(
        crate::FileSourceFailure::Missing,
    ));
    assert_eq!(PendingRecord::new(upload, source).retry(), Retry::Automatic);
}

#[test]
fn pending_subjects_keep_their_variant_and_snapshot_order() {
    use coven_foundation::id_source::{CircleId, StoreId};
    use uuid::Uuid;

    let snapshot = |audience, device, number| {
        PendingSubject::Snapshot(SnapshotId { audience, device: DeviceId(device), number })
    };
    let expected = vec![
        PendingSubject::Write(WriteId { device: DeviceId(9), number: 9 }),
        PendingSubject::Slot(Slot(1)),
        PendingSubject::KeyCopy {
            audience: Audience::Store,
            key: KeyId(Uuid::from_u128(1)),
            member: MemberId(Uuid::from_u128(1)),
            sealing: coven_crypto::SealingKey::from_bytes([1; 32]),
        },
        snapshot(Audience::Store, 1, 1),
        snapshot(Audience::Store, 1, 2),
        snapshot(Audience::Store, 2, 1),
        snapshot(Audience::Circle(CircleId(Uuid::from_u128(1))), 1, 9),
        snapshot(Audience::Circle(CircleId(Uuid::from_u128(2))), 1, 1),
        PendingSubject::Upload { device: DeviceId(1), object: ObjectId(Uuid::from_u128(1)) },
        PendingSubject::Download { device: DeviceId(1), object: ObjectId(Uuid::from_u128(1)) },
        PendingSubject::Operation { id: OperationId(1), kind: OperationKind::CreateStore },
        PendingSubject::Retention { path: ObjectPath::floor(StoreId(Uuid::from_u128(1))) },
        PendingSubject::Audience(Audience::Store),
        PendingSubject::Connection,
    ];
    let mut subjects: Vec<_> = expected.iter().rev().cloned().collect();
    subjects.sort();
    assert_eq!(subjects, expected);
    let encoded = r#"{"Snapshot":["store",2,1]}"#;
    assert_eq!(serde_json::to_string(&subjects[5]).unwrap(), encoded);
    assert_eq!(serde_json::from_str::<PendingSubject>(encoded).unwrap(), subjects[5]);
}

/// Only a network failure no provider answered is offline (E5); its wait is
/// the one a network report ends (§6.7).
#[test]
fn only_an_unanswered_network_failure_is_offline() {
    let storage = |failure, answered| PendingReason::Storage {
        provider: CloudProvider::S3,
        failure,
        answered,
    };
    assert!(storage(StorageFailure::Network, false).offline());
    // A server's error response is an answer.
    assert!(!storage(StorageFailure::Network, true).offline());
    assert!(!storage(StorageFailure::RateLimited, true).offline());
    assert!(!storage(StorageFailure::File, false).offline());
    assert!(!PendingReason::Failed(LocalFailure::Disk).offline());
}

/// A refusal is a verdict on bytes that never change, so only coven's own
/// code can change it: the record names the build that refused it, and each
/// other build checks the slot again, once (§19.1). Ben's device refuses
/// damaged bytes at slot `n`: the same build never checks them again, and
/// another build does, and refusing again records its own build. Cal's device
/// holds a refusal of the honest slot, as a build with a wrong check recorded
/// it: the same build keeps the slot refused, and another build applies it.
#[tokio::test]
async fn a_refused_slot_is_rechecked_once_by_another_build() {
    use crate::sequence_sync::tests::{insert, Device, Group};
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let cal = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &cal).await;
    ben.sync.pass().await.unwrap();
    cal.sync.pass().await.unwrap();
    insert(ana.database(), "a", "refused").await;
    ana.sync.pass().await.unwrap();
    let n = ana.database().applied_slot().await.unwrap();
    let path = ObjectPath::slot(group.store, n.try_into().unwrap());
    let subject = PendingSubject::Slot(Slot(n));
    let another = !PendingRecord::BUILD;
    let blocked = |result: Result<crate::SequenceStatus, crate::SyncError>| {
        matches!(result, Err(crate::SyncError::Pending(record))
            if record.subject == subject && matches!(record.reason, PendingReason::Refused(_)))
    };
    let rechecks = |device: &Device| {
        let (subject, database) = (subject.clone(), device.database().clone());
        async move { database.pending_can_retry::<PendingRecord>(subject).await.unwrap() }
    };

    group.storage.corrupt_byte(&path, 0).await.unwrap();
    let refused = ben.sync.pass().await.unwrap_err();
    assert!(matches!(refused, crate::SyncError::Damaged(_)), "{refused:?}");
    // The same build doesn't check the slot again, reopened or not.
    assert!(!rechecks(&ben).await);
    let ben = group.reopen(ben).await;
    assert!(blocked(ben.sync.pass().await));
    // Another build checks it again; refused again, the record names that
    // build, which doesn't check it a second time.
    ben.database().set_pending_build(another).await.unwrap();
    let ben = group.reopen(ben).await;
    assert!(rechecks(&ben).await);
    let refused = ben.sync.pass().await.unwrap_err();
    assert!(matches!(refused, crate::SyncError::Damaged(_)), "{refused:?}");
    assert!(!rechecks(&ben).await);
    assert!(blocked(ben.sync.pass().await));
    group.storage.corrupt_byte(&path, 0).await.unwrap();

    // Cal's build refused the honest slot. It keeps it refused; another
    // build applies it.
    let wrong = crate::Refusal::WrongIdentity { cause: None };
    cal.database()
        .record_pending(PendingRecord::new(subject.clone(), PendingReason::Refused(wrong)))
        .await
        .unwrap();
    let cal = group.reopen(cal).await;
    assert!(blocked(cal.sync.pass().await));
    assert_eq!(cal.database().applied_slot().await.unwrap(), n - 1);
    cal.database().set_pending_build(another).await.unwrap();
    let cal = group.reopen(cal).await;
    cal.sync.pass().await.unwrap();
    assert_eq!(cal.database().applied_slot().await.unwrap(), n);
    let pending = cal.database().pending::<PendingRecord>().await.unwrap();
    assert!(pending.iter().all(|record| record.subject != subject), "{pending:?}");
    for device in [&ana, &ben, &cal] {
        device.database().close().await.unwrap();
    }
}

/// A refusal judges the stored slot, not one read of it, and nothing checks
/// a slot's transfer: a slot whose bytes a fault changed on the way is read
/// again, and the second read decides (§19.1). Ben's first read of slot `n`
/// arrives garbled: his pass fails as a read, records no refusal, and his
/// next pass applies the slot. Cal kept a garbled read of slot `n` that an
/// earlier build refused: the build he installs next rechecks it, finds the
/// stored slot differs from his copy, and applies the slot on the pass after.
#[tokio::test]
async fn a_slot_garbled_on_the_way_is_read_again_not_refused() {
    use crate::sequence_sync::tests::{insert, Group};
    use coven_storage::test_utils::{Faults, Garble};
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let cal = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &cal).await;
    ben.sync.pass().await.unwrap();
    cal.sync.pass().await.unwrap();
    insert(ana.database(), "a", "garbled").await;
    ana.sync.pass().await.unwrap();
    let n = ana.database().applied_slot().await.unwrap();
    let path = ObjectPath::slot(group.store, n.try_into().unwrap());
    let subject = PendingSubject::Slot(Slot(n));
    let garble = |reads| {
        let garble = Garble { at: 0, reads };
        let (storage, garbled) = (group.storage.clone(), [(path.clone(), garble)].into());
        async move { storage.set_faults(Faults { garbled_reads: garbled, ..Faults::none() }).await }
    };
    let refused = |device: &crate::sequence_sync::tests::Device| {
        let (subject, database) = (subject.clone(), device.database().clone());
        async move {
            database.pending::<PendingRecord>().await.unwrap().into_iter().any(|record| {
                record.subject == subject && matches!(record.reason, PendingReason::Refused(_))
            })
        }
    };

    garble(1).await;
    let failed = ben.sync.pass().await.unwrap_err();
    assert!(
        matches!(&failed, crate::SyncError::Storage(error)
        if error.failure() == StorageFailure::Corrupted),
        "{failed:?}"
    );
    assert!(!refused(&ben).await);
    assert_eq!(ben.database().applied_slot().await.unwrap(), n - 1);
    ben.sync.pass().await.unwrap();
    assert_eq!(ben.database().applied_slot().await.unwrap(), n);

    // Cal's copy is a garbled read, refused by an earlier build.
    garble(1).await;
    cal.sync.read_slot(&path).await.unwrap().unwrap();
    let wrong = crate::Refusal::Decryption { cause: None };
    cal.database()
        .record_pending(PendingRecord::new(subject.clone(), PendingReason::Refused(wrong)))
        .await
        .unwrap();
    cal.database().set_pending_build(!PendingRecord::BUILD).await.unwrap();
    let cal = group.reopen(cal).await;
    let failed = cal.sync.pass().await.unwrap_err();
    assert!(
        matches!(&failed, crate::SyncError::Storage(error)
        if error.failure() == StorageFailure::Corrupted),
        "{failed:?}"
    );
    assert!(!refused(&cal).await);
    cal.sync.pass().await.unwrap();
    assert_eq!(cal.database().applied_slot().await.unwrap(), n);
    for device in [&ana, &ben, &cal] {
        device.database().close().await.unwrap();
    }
}
