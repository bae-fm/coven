use super::*;
use coven_database::DropReason;

#[tokio::test]
async fn failed_administrative_catch_up_reserves_nothing() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    a.add(&member(2), MemberRole::Member).await;
    let before = a.db.local_store_log().await.unwrap();
    storage.set_online(false);
    for command in [
        Command::CreateCircle("Offline".into()),
        Command::RemoveMember(member(2).member()),
        Command::SetRole(member(2).member(), MemberRole::Admin),
        Command::Reset(Audience::Store),
        Command::Invite(
            MemberRole::Member,
            InviteAccess::S3AccessKey {
                access_key_id: "invite".into(),
                secret_access_key: coven_crypto::SecretText::new("secret".into()),
            },
        ),
    ] {
        assert!(matches!(a.sync.begin_operation_call(command).await,
            Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Network));
        assert_eq!(a.db.local_store_log().await.unwrap(), before);
        assert!(a.db.operations().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn sole_admin_demotion_returns_replays_reason() {
    let mut a = device(storage(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    assert!(matches!(
        a.sync
            .begin_operation_call(Command::SetRole(a.member.member(), MemberRole::Member))
            .await,
        Err(SyncError::Rejected(DropReason::NoAdminLeft))
    ));
    assert!(a.db.local_store_log().await.unwrap().upload.is_none());
    assert!(a.db.operations().await.unwrap().is_empty());
}

#[tokio::test]
async fn absent_targets_return_replays_reason() {
    let mut a = device(storage(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    for (command, change) in [
        (
            Command::SetRole(member(9).member(), MemberRole::Admin),
            StoreChange::ChangeRole {
                member: member(9).member(),
                role: MemberRole::Admin,
            },
        ),
        (
            Command::RemoveDevice(DeviceId(99)),
            StoreChange::RemoveDevice {
                device: DeviceId(99),
            },
        ),
        (
            Command::RenameCircle(circle(99), "Absent".into()),
            StoreChange::RenameCircle {
                circle: circle(99),
                name: "Absent".into(),
            },
        ),
    ] {
        let local = a.db.local_store_log().await.unwrap();
        let mut candidate = local.log.entries[0].entry.clone();
        candidate.position.number = 2;
        candidate.timestamp = coven_merge::Timestamp::new(100_000, 0, local.device).unwrap();
        candidate.change = change;
        let (_, replay) = crate::replay::tests::replay_entry(&local.log, candidate.clone());
        let EntryOutcome::Dropped(expected) = &replay.entries[&candidate.position] else {
            panic!("absent target must reject this change");
        };
        assert!(matches!(a.sync.begin_operation_call(command).await,
            Err(SyncError::Rejected(reason)) if reason == *expected));
        assert_eq!(a.db.local_store_log().await.unwrap(), local);
        assert!(a.db.operations().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn incomplete_or_damaged_catch_up_reserves_nothing() {
    for damaged in [false, true] {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        let mut b = device(storage.clone(), 2, member(1), store(1)).await;
        a.create(key(1)).await;
        b.sync().await;
        let missing = a.add(&member(2), MemberRole::Member).await;
        let later = a.add(&member(3), MemberRole::Member).await;
        if damaged {
            storage
                .corrupt_byte(&object::path(store(1), missing), 0)
                .await
                .unwrap();
        } else {
            // Entries are never deleted; an incomplete catch-up is a failed read.
            storage
                .set_faults(coven_storage::test_utils::Faults {
                    read_failures: [(object::path(store(1), missing), StorageFailure::Network)]
                        .into(),
                    ..coven_storage::test_utils::Faults::none()
                })
                .await;
        }
        let error = b
            .sync
            .begin_operation_call(Command::CreateCircle("Blocked".into()))
            .await;
        if damaged {
            assert!(matches!(error, Err(SyncError::Pending(_))));
        } else {
            assert!(
                matches!(error, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Network)
            );
        }
        let local = b.db.local_store_log().await.unwrap();
        assert!(!local.log.replay.entries.contains_key(&later));
        assert!(local.upload.is_none());
        assert!(b.db.operations().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn dropped_attempt_revalidates_before_reserving_a_retry() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    a.create(key(1)).await;
    a.add(&b.member, MemberRole::Admin).await;
    a.add(&member(3), MemberRole::Member).await;
    let create = begin(&mut a, Command::CreateCircle("Shared".into())).await;
    let Output::CircleId(circle) = finish(&mut a, create).await else {
        panic!()
    };
    let add = begin(&mut a, Command::AddCircleMember(circle, b.member.member())).await;
    finish(&mut a, add).await;
    b.sync().await;
    b.sync.load_new_device("Ben", None).await.unwrap();
    let operation = begin(&mut a, Command::AddCircleMember(circle, member(3).member())).await;
    step(&mut a, operation).await.unwrap();
    let fixed = a.db.local_store_log().await.unwrap().upload.unwrap().entry;
    b.sync
        .make_and_upload_entry(StoreChange::DeleteCircle { circle })
        .await
        .unwrap();
    while Data::read(&a.operation(operation).await)
        .unwrap()
        .entry()
        .unwrap()
        .is_some()
    {
        assert!(matches!(
            step(&mut a, operation).await.unwrap(),
            Progress::Advanced
        ));
    }
    let before = a.db.local_store_log().await.unwrap();
    let EntryOutcome::Dropped(reason) = &before.log.replay.entries[&fixed.position] else {
        panic!("the attempted entry must be dropped");
    };
    let dropped = PendingRecord {
        subject: PendingSubject::Entry(fixed.position),
        reason: PendingReason::Dropped(reason.clone()),
        reported_by: fixed.position.device,
    };
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    for attempt in 0..3 {
        if attempt > 0 {
            assert!(matches!(
                operations.retry_pending_operation(operation).await,
                Err(SyncError::Rejected(DropReason::NotAllowed))
            ));
            assert_eq!(a.db.local_store_log().await.unwrap(), before);
        }
        let conditions = pending(&operations, &a.db).await;
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0], dropped);
        assert_eq!(pending_id(&conditions[1]), operation);
    }
    operations.close().await.unwrap();
}

#[tokio::test]
async fn removal_downloads_authority_changes() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage, 2, member(2), store(1)).await;
    a.create(key(1)).await;
    a.add(&b.member, MemberRole::Admin).await;
    a.add(&member(3), MemberRole::Member).await;
    b.sync().await;
    b.sync.load_new_device("Ben", None).await.unwrap();
    b.sync
        .make_and_upload_entry(StoreChange::ChangeRole {
            member: a.member.member(),
            role: MemberRole::Member,
        })
        .await
        .unwrap();
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    assert!(matches!(
        operations.remove_member(&member(3).member()).await,
        Err(SyncError::Rejected(DropReason::NotAllowed))
    ));
    assert!(a.db.local_store_log().await.unwrap().upload.is_none());
    b.sync
        .make_and_upload_entry(StoreChange::ChangeRole {
            member: a.member.member(),
            role: MemberRole::Admin,
        })
        .await
        .unwrap();
    operations.remove_member(&member(3).member()).await.unwrap();
    assert!(a.db.store_log().await.unwrap().replay.state.members[&member(3).member()].removed);
    // Only the removed member's key remains, awaiting deletion (§13).
    assert_eq!(
        pending(&operations, &a.db)
            .await
            .into_iter()
            .map(|record| record.reason)
            .collect::<Vec<_>>(),
        [PendingReason::DeleteAccessKey(console_key(
            "fixture-access-key",
            Some(&member(3))
        ))]
    );
    operations.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_app_future_keeps_running_and_permanent_failure_retries_or_discards() {
    let storage = Arc::new(
        MemoryStorage::builder()
            .transfer_limits(65536, 65536)
            .build()
            .unwrap(),
    );
    let mut d = device(storage.clone(), 1, member(1), store(1)).await;
    d.create(key(1)).await;
    d.add(&member(2), MemberRole::Member).await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_creation(ObjectPrefix::keys(store(1)), started, held)
        .await;
    let files = file_owner(&d);
    let operations = { operation_owner(d.sync, files) };
    let mut waiting = Box::pin(operations.create_circle("Retained"));
    std::future::poll_fn(|cx| {
        use std::future::Future;
        assert!(waiting.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    observed.await.unwrap();
    assert_eq!(d.db.operations().await.unwrap().len(), 1);
    drop(waiting);
    resume.send(()).unwrap();
    operations.barrier().await.unwrap();
    assert_eq!(d.db.circles().await.unwrap()[0].name, "Retained");
    // Invalid names fail during format validation, after their intent is durable.
    assert!(matches!(
        operations.create_circle("").await,
        Err(SyncError::Failed {
            failure: LocalFailure::Database,
            ..
        })
    ));
    let conditions = pending(&operations, &d.db).await;
    assert_eq!(conditions.len(), 1);
    let id = pending_id(&conditions[0]);
    assert!(matches!(
        operations.retry_pending_operation(id).await,
        Err(SyncError::Failed {
            failure: LocalFailure::Database,
            ..
        })
    ));
    operations.discard_pending_operation(id).await.unwrap();
    assert!(pending(&operations, &d.db).await.is_empty());
    operations.close().await.unwrap();
}
