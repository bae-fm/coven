use crate::{
    store_log_object as object,
    store_log_sync::tests::{device, key, member, public, store},
    SyncError,
};
use coven_database::EntryOutcome;
use coven_format::{
    store_log::{MemberRole, StoreChange, StoreLogEntry},
    value::EntryId,
};
use coven_foundation::{clock::FixedClock, id_source::DeviceId};
use coven_merge::Audience;
use coven_storage::{
    test_utils::{Faults, MemoryStorage},
    Storage,
};
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

#[tokio::test]
async fn stale_registration_is_consumed_after_an_unknown_devices_entry() {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    let mut c = device(storage.clone(), 3, member(1), store(1)).await;
    a.create(key(1)).await;
    b.sync().await;
    c.sync().await;
    b.interrupt_entry(1).await;
    assert!(b
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(2),
            name: "restored".into(),
            replaces: None,
        })
        .await
        .is_err());
    let fixed = b.db.local_store_log().await.unwrap().upload.unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(86400));
    let witness = c
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(3),
            name: "new".into(),
            replaces: None,
        })
        .await
        .unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400) + Duration::from_nanos(1));
    b.sync().await;
    let log = b.log().await;
    assert!(!log.replay.state.devices.contains_key(&DeviceId(2)));
    assert_eq!(log.replay.entries[&witness], EntryOutcome::Kept);
    assert!(log.positions().covers(fixed.entry.position));
    assert!(b.db.local_store_log().await.unwrap().upload.is_none());
    assert_eq!(
        log.replay.entries[&fixed.entry.position],
        EntryOutcome::Dropped(coven_database::DropReason::LandedTooLate)
    );
    assert!(crate::pending_logs::local_refusals(&b.db)
        .await
        .unwrap()
        .is_empty());
    let dropped = crate::PendingRecord {
        subject: crate::PendingSubject::Entry(fixed.entry.position),
        reason: crate::PendingReason::Dropped(coven_database::DropReason::LandedTooLate),
        reported_by: DeviceId(2),
    };
    assert_eq!(
        b.db.pending::<crate::PendingRecord>().await.unwrap(),
        std::slice::from_ref(&dropped)
    );
    assert_eq!(dropped.retry(), crate::Retry::Never);
    b.restart(storage.clone()).await;
    assert_eq!(b.log().await, log);
    assert_eq!(
        b.db.pending::<crate::PendingRecord>().await.unwrap(),
        std::slice::from_ref(&dropped)
    );
    // A caught-up new attempt reads the dropped entry implicitly on its own
    // device. Its consumed number and first publication time survive reopening.
    clock.set(UNIX_EPOCH + Duration::from_secs(100 * 86400));
    let replacement = b
        .sync
        .make_and_upload_entry(fixed.entry.change.clone())
        .await
        .unwrap();
    assert_eq!(replacement.number, fixed.entry.position.number + 1);
    let after = b.log().await;
    assert_eq!(
        after.replay.entries[&fixed.entry.position],
        log.replay.entries[&fixed.entry.position]
    );
    assert_eq!(after.replay.entries[&replacement], EntryOutcome::Kept);
    assert_eq!(
        b.db.pending::<crate::PendingRecord>().await.unwrap(),
        [dropped]
    );
    a.sync().await;
    assert_eq!(a.log().await, after);
}

#[tokio::test]
async fn equality_at_thirty_days_is_kept_and_lost_reply_preserves_publication_time() {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    a.create(key(1)).await;
    b.sync().await;
    clock.set(UNIX_EPOCH + Duration::from_secs(86400));
    a.sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(9),
            name: "witness".into(),
            replaces: None,
        })
        .await
        .unwrap();
    let landed = UNIX_EPOCH + Duration::from_secs(31 * 86400);
    clock.set(landed);
    storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(b
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(2),
            name: "registration".into(),
            replaces: None,
        })
        .await
        .is_err());
    let fixed = b.db.local_store_log().await.unwrap().upload.unwrap();
    let path = object::path(store(1), fixed.entry.position);
    let bytes = storage.read(&path).await.unwrap();
    b.restart(storage.clone()).await;
    clock.set(UNIX_EPOCH + Duration::from_secs(90 * 86400));
    b.sync().await;
    let log = b.log().await;
    let applied = log
        .entries
        .iter()
        .find(|e| e.entry.position == fixed.entry.position)
        .unwrap();
    assert_eq!(applied.stored_at, landed);
    assert_eq!(
        log.replay.entries[&fixed.entry.position],
        EntryOutcome::Kept
    );
    assert_eq!(storage.read(&path).await.unwrap(), bytes);
    b.restart(storage.clone()).await;
    assert_eq!(b.log().await, log);
}

#[tokio::test]
async fn a_dropped_witness_arriving_after_the_late_entry_cannot_grant_authority() {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    a.create(key(1)).await;
    b.sync().await;
    b.sync.load_new_device("delayed writer").await.unwrap();
    // Writer 3 has no membership authority, but publishes a correctly signed
    // entry. Writer 2's delayed addition is decoded and consumed before it.
    let witness = StoreLogEntry {
        position: EntryId {
            device: DeviceId(3),
            number: 1,
        },
        timestamp: coven_merge::Timestamp::new(2000, 0, DeviceId(3)).unwrap(),
        author: member(9).member_id(),
        had_read: a.log().await.positions(),
        change: StoreChange::AddDevice {
            device: DeviceId(3),
            name: "outsider".into(),
            replaces: None,
        },
    };
    let ring = a.custody.read().unwrap().unwrap();
    let bytes = object::seal(
        store(1),
        &witness,
        coven_format::FormatVersion::CURRENT,
        ring.store_key(key(1)).unwrap(),
        &member(9),
    )
    .unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(86400));
    storage
        .create(&object::path(store(1), witness.position), &bytes)
        .await
        .unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(32 * 86400));
    assert!(matches!(
        b.sync
            .make_and_upload_entry(StoreChange::AddMember {
                keys: public(&member(2)),
                role: MemberRole::Member,
                access: coven_format::MemberAccess::ProviderAccount("member2@example.org".into()),
            })
            .await,
        Err(SyncError::Rejected(
            coven_database::DropReason::LandedTooLate
        ))
    ));
    let late = b.log().await.entries.last().unwrap().entry.position;
    let mut c = device(storage.clone(), 4, member(1), store(1)).await;
    c.sync().await;
    let log = c.log().await;
    assert_eq!(
        log.replay.entries[&late],
        EntryOutcome::Dropped(coven_database::DropReason::LandedTooLate)
    );
    assert_eq!(
        log.replay.entries[&witness.position],
        EntryOutcome::Dropped(coven_database::DropReason::NotAllowed)
    );
    assert!(!log
        .replay
        .state
        .members
        .contains_key(&member(2).member_id()));
    c.restart(storage).await;
    assert_eq!(c.log().await, log);
}

#[tokio::test]
async fn operations_retry_late_entries_with_new_numbers_and_introduced_keys() {
    use crate::{
        operation_data::Data,
        operations::{Begun, Command, Output, Progress},
    };
    for command in [
        Command::CreateCircle("Delayed".into()),
        Command::Reset(Audience::Store),
    ] {
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
        let storage = Arc::new(
            MemoryStorage::builder()
                .clock(clock.clone())
                .build()
                .unwrap(),
        );
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        let mut b = device(storage.clone(), 2, member(1), store(1)).await;
        a.create(key(1)).await;
        b.sync().await;
        b.sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(2),
                name: "snapshot writer".into(),
                replaces: None,
            })
            .await
            .unwrap();
        let Begun::Operation(id) = b.sync.begin_operation_call(command).await.unwrap() else {
            panic!("operation")
        };
        while b.db.local_store_log().await.unwrap().upload.is_none() {
            assert!(matches!(b.step(id).await.unwrap(), Progress::Advanced));
        }
        let fixed = b.db.local_store_log().await.unwrap().upload.unwrap();
        b.step(id).await.unwrap(); // prerequisite copies
        clock.set(UNIX_EPOCH + Duration::from_secs(86400));
        a.sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(9),
                name: "witness".into(),
                replaces: None,
            })
            .await
            .unwrap();
        clock.set(UNIX_EPOCH + Duration::from_secs(32 * 86400));
        b.step(id).await.unwrap(); // publish the fixed entry
        let bytes = storage
            .read(&object::path(store(1), fixed.entry.position))
            .await
            .unwrap();
        b.restart(storage.clone()).await;
        b.step(id).await.unwrap(); // consume the late entry and reset operation progress
        assert!(Data::read(&b.operation(id).await)
            .unwrap()
            .entry()
            .unwrap()
            .is_none());
        assert!(b.db.local_store_log().await.unwrap().upload.is_none());
        assert!(b.log().await.replay.state.circles.is_empty());
        b.restart(storage.clone()).await;
        b.step(id).await.unwrap(); // catch up and reserve a new entry
        let retry = b.db.local_store_log().await.unwrap().upload.unwrap();
        assert_eq!(retry.entry.position.number, fixed.entry.position.number + 1);
        match (&fixed.entry.change, &retry.entry.change) {
            (
                StoreChange::CreateCircle { key: old_key, .. },
                StoreChange::CreateCircle { key: new_key, .. },
            ) => assert_ne!(old_key, new_key),
            (StoreChange::Reset { snapshot: old }, StoreChange::Reset { snapshot: new }) => {
                assert_eq!(old, new)
            }
            _ => panic!("retry changed operation"),
        }
        loop {
            match b.step(id).await.unwrap() {
                Progress::Advanced => (),
                Progress::Waiting(crate::PendingReason::Waits(crate::Prerequisite::Reload(_))) => {
                    b.sync().await;
                }
                Progress::Waiting(reason) => panic!("unexpected wait: {reason:?}"),
                Progress::Reply(_) => panic!("unexpected operation reply"),
                Progress::Finished(output) => {
                    match (&retry.entry.change, output) {
                        (StoreChange::CreateCircle { circle, .. }, Output::CircleId(created)) => {
                            assert_eq!(created, *circle)
                        }
                        (StoreChange::Reset { .. }, Output::Unit) => (),
                        _ => panic!("unexpected operation output"),
                    }
                    break;
                }
            }
        }
        let log = b.log().await;
        assert_eq!(
            log.replay.entries[&fixed.entry.position],
            EntryOutcome::Dropped(coven_database::DropReason::LandedTooLate)
        );
        assert_eq!(
            log.replay.entries[&retry.entry.position],
            EntryOutcome::Kept
        );
        assert_eq!(
            storage
                .read(&object::path(store(1), fixed.entry.position))
                .await
                .unwrap(),
            bytes
        );
        let reports = b.db.pending::<crate::PendingRecord>().await.unwrap();
        if matches!(retry.entry.change, StoreChange::Reset { .. }) {
            assert!(reports.is_empty(), "committed reset clears the report");
        } else {
            assert_eq!(
                reports,
                [crate::PendingRecord {
                    subject: crate::PendingSubject::Entry(fixed.entry.position),
                    reason: crate::PendingReason::Dropped(
                        coven_database::DropReason::LandedTooLate
                    ),
                    reported_by: DeviceId(2),
                }]
            );
        }
        b.sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(10),
                name: "later device".into(),
                replaces: None,
            })
            .await
            .unwrap();
        assert_eq!(
            b.db.pending::<crate::PendingRecord>().await.unwrap(),
            reports,
            "a new replay preserves both an outstanding report and a reset's clearing"
        );
        b.restart(storage.clone()).await;
        b.sync().await;
        assert_eq!(
            b.db.pending::<crate::PendingRecord>().await.unwrap(),
            reports
        );
        assert_eq!(
            b.log().await.replay.entries[&fixed.entry.position],
            EntryOutcome::Dropped(coven_database::DropReason::LandedTooLate)
        );
        for copy in fixed.sealing.keys {
            assert_eq!(
                storage.read(&copy.path(store(1))).await.unwrap(),
                copy.bytes
            );
        }
    }
}

#[tokio::test]
async fn setup_consumes_a_late_access_entry_before_committing_credentials() {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    a.create(key(1)).await;
    b.sync().await;
    b.sync.load_new_device("access writer").await.unwrap();
    let access = coven_format::MemberAccess::S3AccessKey {
        access_key_id: "replacement".into(),
    };
    b.interrupt_entry(2).await;
    assert!(b
        .sync
        .make_and_upload_entry(StoreChange::SetAccess {
            access: access.clone()
        })
        .await
        .is_err());
    let fixed = b.db.local_store_log().await.unwrap().upload.unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(86400));
    a.sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(9),
            name: "witness".into(),
            replaces: None,
        })
        .await
        .unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(32 * 86400));
    assert!(matches!(
        b.sync
            .setup_storage(
                storage.clone(),
                access.clone(),
                "device".into(),
                Box::new(|| panic!("late access must not commit credentials"))
            )
            .await,
        Err(SyncError::Rejected(
            coven_database::DropReason::LandedTooLate
        ))
    ));
    b.restart(storage.clone()).await;
    let local = b.db.local_store_log().await.unwrap();
    assert!(local.upload.is_none());
    assert_eq!(
        local.log.replay.entries[&fixed.entry.position],
        EntryOutcome::Dropped(coven_database::DropReason::LandedTooLate)
    );
    b.sync
        .begin_operation_call(crate::operations::Command::Storage(None))
        .await
        .unwrap();
    b.custody.forget().unwrap();
    assert!(matches!(
        b.sync
            .setup_storage(
                storage.clone(),
                access.clone(),
                "device".into(),
                Box::new(|| Err(SyncError::NoStorage)),
            )
            .await,
        Err(SyncError::NoStorage)
    ));
    assert!(b.custody.read().unwrap().is_none());
    let reserved = b.db.local_store_log().await.unwrap().upload.unwrap();
    assert_eq!(
        reserved.entry.position.number,
        fixed.entry.position.number + 1
    );
    let committed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let did_commit = committed.clone();
    b.sync
        .setup_storage(
            storage,
            access.clone(),
            "device".into(),
            Box::new(move || {
                did_commit.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(Box::new(|| Ok(())))
            }),
        )
        .await
        .unwrap();
    assert!(committed.load(std::sync::atomic::Ordering::SeqCst));
    let log = b.log().await;
    let retry = EntryId {
        number: fixed.entry.position.number + 1,
        ..fixed.entry.position
    };
    assert_eq!(log.replay.entries[&retry], EntryOutcome::Kept);
    assert_eq!(
        log.replay.state.members[&member(1).member_id()].access,
        access
    );
    assert_eq!(
        log.replay.entries[&fixed.entry.position],
        EntryOutcome::Dropped(coven_database::DropReason::LandedTooLate)
    );
}

#[tokio::test]
async fn a_late_operation_revalidates_changed_authority_before_a_new_number() {
    use crate::operations::{Begun, Command};
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    a.create(key(1)).await;
    for (keys, role) in [
        (member(2), MemberRole::Admin),
        (member(3), MemberRole::Member),
    ] {
        a.sync
            .make_and_upload_entry(StoreChange::AddMember {
                keys: public(&keys),
                role,
                access: coven_format::MemberAccess::ProviderAccount(format!(
                    "{role:?}@example.org"
                )),
            })
            .await
            .unwrap();
    }
    b.sync().await;
    b.sync.load_new_device("delayed admin").await.unwrap();
    let Begun::Operation(id) = b
        .sync
        .begin_operation_call(Command::RemoveMember(member(3).member_id()))
        .await
        .unwrap()
    else {
        panic!("operation")
    };
    b.step(id).await.unwrap(); // reserve
    let fixed = b.db.local_store_log().await.unwrap().upload.unwrap();
    b.step(id).await.unwrap(); // copies
    clock.set(UNIX_EPOCH + Duration::from_secs(86400));
    a.sync
        .make_and_upload_entry(StoreChange::ChangeRole {
            member: member(2).member_id(),
            role: MemberRole::Member,
        })
        .await
        .unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(32 * 86400));
    b.step(id).await.unwrap(); // publish
    b.step(id).await.unwrap(); // consume and restart
    assert_eq!(
        b.log().await.replay.entries[&fixed.entry.position],
        EntryOutcome::Dropped(coven_database::DropReason::LandedTooLate)
    );
    b.restart(storage).await;
    let before = b.db.local_store_log().await.unwrap();
    assert!(before.upload.is_none());
    for _ in 0..2 {
        assert!(matches!(
            b.step(id).await,
            Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
        ));
        assert_eq!(b.db.local_store_log().await.unwrap(), before);
    }
}

#[tokio::test]
async fn retained_publication_time_survives_absence_and_refuses_a_changed_listing() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let created = a.create(key(1)).await;
    let log = a.log().await;
    let retained = super::publication_times(&log, &[]).unwrap();
    assert_eq!(retained[&created], log.entries[0].stored_at);
    let mut listed = storage
        .list(&coven_storage::ObjectPrefix::store_logs(store(1)))
        .await
        .unwrap();
    assert_eq!(super::publication_times(&log, &listed).unwrap(), retained);
    listed[0].stored_at += Duration::from_nanos(1);
    let error = super::publication_times(&log, &listed).unwrap_err();
    let SyncError::Failed {
        failure: crate::LocalFailure::Database,
        source,
    } = &error
    else {
        panic!("unexpected metadata failure: {error:?}")
    };
    assert!(matches!(source.downcast_ref::<coven_database::DbError>(),
        Some(coven_database::DbError::StoreLogEntryChanged(id)) if *id == created));
}
