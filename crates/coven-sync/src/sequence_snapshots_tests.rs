use crate::sequence_sync::tests::*;
use coven_storage::{test_utils::Faults, ObjectPath, Storage, StorageFailure};

#[tokio::test]
async fn a_snapshot_is_uploaded_before_its_counted_announcement() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "recorded").await;
    ana.sync.pass().await.unwrap();
    let covered = ana.sync.database.applied_slot().await.unwrap();
    let announced = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    assert_eq!(announced.0, covered + 1);
    ben.sync.pass().await.unwrap();
    let session = ben.sync.session.lock().await;
    let announcement = &session.history.announcements[&announced];
    assert_eq!(announcement.covers, covered);
    let path = crate::snapshot_object::path(group.store, &announcement.snapshot).unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_some());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn sealed_slot_growth_requests_a_snapshot_and_an_idle_pass_reuses_its_size() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let title = "x".repeat(1_048_577);
    ana.sync
        .database
        .write(move |sql| {
            sql.execute("INSERT INTO notes VALUES('large',?1)", [title])?;
            Ok(())
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let session = ana.sync.session.lock().await;
    let (_, announcement) =
        session.history.announcements.last_key_value().expect("growth requests a snapshot");
    assert_eq!(announcement.covers, 3);
    drop(session);
    // The same pass wrote the floor naming the snapshot; the next pass reads
    // that floor's bytes once.
    ana.sync.pass().await.unwrap();
    assert_idle_work(&ana).await;
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn aged_history_requests_missing_coverage_below_the_growth_threshold() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "old", "retention needs this prefix").await;
    ana.sync.pass().await.unwrap();
    assert!(ana.sync.session.lock().await.history.announcements.is_empty());
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.session.lock().await.history.announcements.last_key_value().unwrap().1.covers,
        3
    );
    ana.sync.database.close().await.unwrap();
}

/// The sealed bytes of the slots after `after` through `through`, as a new
/// device reads them.
async fn sealed(group: &Group, after: u64, through: u64) -> u64 {
    let mut bytes = 0;
    for slot in after + 1..=through {
        let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
        bytes += group.storage.status(&path).await.unwrap().unwrap().size;
    }
    bytes
}

/// The newest announcement's slot, coverage and snapshot.
async fn newest(
    device: &Device,
) -> Option<(coven_merge::Slot, u64, coven_format::admin::SnapshotId)> {
    let session = device.sync.session.lock().await;
    let (slot, announcement) = session.history.announcements.last_key_value()?;
    Some((*slot, announcement.covers, announcement.snapshot.clone()))
}

/// Snapshots follow the slots written since the last one: k × S sealed
/// bytes, S being the last snapshot's size, however large S is. A store
/// whose snapshot is past 8 MiB, where a cap once stood, still waits for
/// k × S, so snapshot work stays a fixed share of what is written.
#[tokio::test]
async fn a_large_snapshot_is_followed_after_k_times_its_size_in_slots() {
    use super::SNAPSHOT_GAP_PERCENT;
    const OLD_CAP: u64 = 8 * 1024 * 1024;
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    const NOTE: usize = 1024 * 1024;
    let mut written = 0usize;
    let write = |ana: &Device, index: usize| {
        let database = ana.sync.database.clone();
        async move {
            let title = "x".repeat(NOTE);
            database
                .write(move |sql| {
                    sql.execute("INSERT INTO notes VALUES(?1,?2)", (index.to_string(), title))?;
                    Ok(())
                })
                .await
                .unwrap();
        }
    };
    // Grow the store until its newest snapshot is well past the old cap.
    let (slot, covers, size) = loop {
        write(&ana, written).await;
        written += 1;
        ana.sync.pass().await.unwrap();
        if let Some((slot, covers, id)) = newest(&ana).await {
            let path = crate::snapshot_object::path(group.store, &id).unwrap();
            let size = group.storage.status(&path).await.unwrap().unwrap().size;
            if size > OLD_CAP + 2 * NOTE as u64 {
                break (slot, covers, size);
            }
        }
        assert!(written < 64, "no snapshot grew past the old cap");
    };
    let gap = size * SNAPSHOT_GAP_PERCENT / 100;
    let mut before;
    let next = loop {
        before = sealed(&group, covers, ana.sync.database.applied_slot().await.unwrap()).await;
        assert!(before < gap, "no snapshot after {before} bytes of slots");
        write(&ana, written).await;
        written += 1;
        ana.sync.pass().await.unwrap();
        match newest(&ana).await {
            Some((newer, next, _)) if newer != slot => break next,
            _ => {}
        }
    };
    let growth = sealed(&group, covers, next).await;
    assert!(
        growth >= gap,
        "a snapshot after {growth} bytes of slots, {before} before its pass, the last one {size} bytes"
    );
    ana.sync.database.close().await.unwrap();
}

/// A young store of small writes. Each write seals to more bytes than its
/// parts hold: its header, roots and signatures, and its slot's envelope. A
/// new device reads those sealed bytes, so they request the first snapshot:
/// this store's slots reach 1 MiB while its parts are still below it.
#[tokio::test]
async fn sealed_slot_bytes_request_the_first_snapshot_while_the_parts_are_below_it() {
    use super::FIRST_SNAPSHOT_BYTES;
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let mut written = 0;
    let mut before;
    let covers = loop {
        before = sealed(&group, 0, ana.sync.database.applied_slot().await.unwrap()).await;
        assert!(before < FIRST_SNAPSHOT_BYTES, "no snapshot after {before} bytes of slots");
        for _ in 0..250 {
            let id = written.to_string();
            ana.sync
                .database
                .write(move |sql| {
                    sql.execute("INSERT INTO notes VALUES(?1,?2)", (id, "x".repeat(900)))?;
                    Ok(())
                })
                .await
                .unwrap();
            written += 1;
        }
        ana.sync.pass().await.unwrap();
        if let Some((_, covers, _)) = newest(&ana).await {
            break covers;
        }
    };
    let covered = sealed(&group, 0, covers).await;
    assert!(
        covered >= FIRST_SNAPSHOT_BYTES,
        "a first snapshot covering {covered} bytes of slots, {before} before its pass"
    );
    // The rows themselves, about 0.9 KiB each, stay well below 1 MiB.
    assert!(written * 1000 < FIRST_SNAPSHOT_BYTES, "{written} writes");
    ana.sync.database.close().await.unwrap();
}

/// Once Ben reads his removal from Gifts, his device no longer takes a
/// snapshot of it; it still snapshots the circle he stays in (§15.8).
#[tokio::test]
async fn a_member_removed_from_a_circle_no_longer_snapshots_it() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let gifts = ana.sync.create_circle("Gifts".into()).await.unwrap();
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    for circle in [gifts, garden] {
        ana.sync
            .administer(coven_format::admin::StoreChange::AddCircleMember {
                circle,
                member: ben.identity.member(),
            })
            .await
            .unwrap();
    }
    ana.sync
        .administer(coven_format::admin::StoreChange::RemoveCircleMember {
            circle: gifts,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ben.sync.pass().await.unwrap();
    let mine =
        |audience: coven_merge::Audience| {
            let ben = &ben;
            async move {
                ben.sync.session.lock().await.history.announcements.values().any(|a| {
                    a.snapshot.device == ben.sync.device && a.snapshot.audience == audience
                })
            }
        };
    assert!(mine(coven_merge::Audience::Circle(garden)).await, "Ben snapshots Garden");
    assert!(!mine(coven_merge::Audience::Circle(gifts)).await, "Ben no longer snapshots Gifts");
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// A reset preparation that a later announcement covers is abandoned: the
/// journal deletes its uploaded body, and a deletion that fails stays in the
/// journal with a pending record until the retry deletes it (§16.3, §11.3).
#[tokio::test]
async fn an_abandoned_preparation_deletes_its_body_and_keeps_a_failed_deletion() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let prepared = ana.sync.database.applied_slot().await.unwrap();
    let activation = ObjectPath::slot(group.store, (prepared + 2).try_into().unwrap());
    group
        .storage
        .set_faults(Faults {
            create_failures: [(activation, (StorageFailure::ProviderUnavailable, 503))].into(),
            ..Faults::none()
        })
        .await;
    assert!(ana.sync.reset(coven_merge::Audience::Store).await.is_err());
    group.storage.set_faults(Faults::none()).await;
    let body = ana
        .storage
        .request_trace()
        .into_iter()
        .find_map(|request| match request {
            coven_storage::test_utils::MemoryRequest::Create(path)
                if path.snapshot_id().is_some() =>
            {
                Some(path)
            }
            _ => None,
        })
        .expect("the preparation's body was uploaded");
    assert!(group.storage.status(&body).await.unwrap().is_some());
    // Ben's ordinary snapshot covers more than the preparation, so Ana's
    // retry abandons the preparation and starts another.
    ben.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let operation = ana.sync.database.operations().await.unwrap()[0].id;
    group
        .storage
        .set_faults(Faults {
            delete_failures: [(body.clone(), StorageFailure::Network)].into(),
            ..Faults::none()
        })
        .await;
    assert!(ana.sync.retry_pending_operation(operation).await.is_err());
    assert!(group.storage.status(&body).await.unwrap().is_some());
    assert_eq!(ana.sync.database.operations().await.unwrap().len(), 1);
    let pending = ana.sync.database.pending::<crate::PendingRecord>().await.unwrap();
    assert!(
        pending.iter().any(|record| matches!(
            record.reason,
            crate::PendingReason::Storage { failure: StorageFailure::Network, .. }
        )),
        "{pending:?}"
    );
    group.storage.set_faults(Faults::none()).await;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert!(group.storage.status(&body).await.unwrap().is_none());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana and Ben are admitted and have read through the same slot. Ben takes
/// an ordinary snapshot of it whose announcement can't land yet: the claim's
/// slot is refused, so his journal keeps the uploaded body and its
/// announcement. Returns the group, both devices, the slot Ben read through
/// and his operation.
async fn ben_with_an_unannounced_snapshot(
) -> (Group, Device, Device, u64, coven_database::OperationId) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before the snapshot").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let covers = ben.sync.database.applied_slot().await.unwrap();
    let claimed = ObjectPath::slot(group.store, (covers + 1).try_into().unwrap());
    group
        .storage
        .set_faults(Faults {
            create_failures: [(claimed, (StorageFailure::ProviderUnavailable, 503))].into(),
            ..Faults::none()
        })
        .await;
    assert!(ben.sync.snapshot(coven_merge::Audience::Store).await.is_err());
    group.storage.set_faults(Faults::none()).await;
    let operation = ben.sync.database.operations().await.unwrap()[0].id;
    (group, ana, ben, covers, operation)
}

/// The announcement `ben` could send for the store, covering `covers`.
fn announcement_by(
    ben: &Device,
    covers: u64,
) -> (coven_format::sequence::SnapshotAnnouncement, coven_format::sequence::SlotHeader) {
    (
        coven_format::sequence::SnapshotAnnouncement {
            snapshot: coven_format::admin::SnapshotId {
                audience: coven_merge::Audience::Store,
                device: ben.sync.device,
                number: 99,
            },
            covers,
            breaking_version: 0,
            purpose: coven_format::sequence::Purpose::Ordinary,
            digest: [7; 32],
        },
        coven_format::sequence::SlotHeader {
            device: ben.sync.device,
            claim: 99,
            author: ben.identity.member(),
            after: 0,
        },
    )
}

/// An announcement counts only if its coverage is no lower than what its
/// audience already announced and no lower than the latest raise or reset's
/// entry; equal coverage counts, and the later slot is the one used. An
/// honest device recaptures a stale snapshot before claiming (so only a
/// stale claim already on its way reaches this rule), hence the rule is
/// asked about the history a device holds (§16.2, §16.3).
#[tokio::test]
async fn an_announcement_counts_only_at_or_above_the_coverage_and_the_latest_reset() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before the snapshot").await;
    ana.sync.pass().await.unwrap();
    let announced = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let covers = ana.sync.session.lock().await.history.announcements[&announced].covers;
    let allowed = |history: &crate::history::History, covers: u64| {
        let (announcement, header) = announcement_by(&ben, covers);
        history.announcement_allowed(coven_merge::Slot(1_000), &header, &announcement)
    };
    let history = ana.sync.session.lock().await.history.clone();
    assert!(allowed(&history, covers), "equal coverage counts");
    assert!(allowed(&history, covers + 1));
    assert!(!allowed(&history, covers - 1), "lower coverage does not");

    ana.sync.reset(coven_merge::Audience::Store).await.unwrap();
    let history = ana.sync.session.lock().await.history.clone();
    let entry = history.boundaries.last().unwrap().boundary.entry().0;
    assert!(allowed(&history, entry), "covering the reset's entry counts");
    assert!(!allowed(&history, entry - 1), "covering less than the reset does not");
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Of two announcements with the same coverage, the later one is the one a
/// reload uses (§16.2).
#[tokio::test]
async fn of_two_announcements_with_equal_coverage_a_reload_uses_the_later() {
    let (group, ana, ben, covers, operation) = ben_with_an_unannounced_snapshot().await;
    // Ana snapshots the same slot Ben did, and her announcement lands first.
    let first = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    ben.sync.retry_pending_operation(operation).await.unwrap();
    ben.sync.pass().await.unwrap();
    let (later, bodies) = {
        let session = ben.sync.session.lock().await;
        let counted: Vec<_> = session
            .history
            .announcements
            .iter()
            .map(|(slot, announcement)| (*slot, announcement.covers))
            .collect();
        assert_eq!(counted.len(), 2, "{counted:?}");
        assert_eq!(counted[0], (first, covers));
        assert_eq!(counted[1].1, covers);
        let paths: Vec<_> = session
            .history
            .announcements
            .values()
            .map(|announcement| {
                crate::snapshot_object::path(group.store, &announcement.snapshot).unwrap()
            })
            .collect();
        (counted[1].0, paths)
    };
    assert!(later > first);
    let before = ben.storage.request_trace().len();
    ben.sync.reload_from_snapshot().await.unwrap();
    let read: Vec<_> = ben.storage.request_trace()[before..]
        .iter()
        .filter_map(|request| match request {
            coven_storage::test_utils::MemoryRequest::Read(path, ..)
                if path.snapshot_id().is_some() =>
            {
                Some(path.clone())
            }
            _ => None,
        })
        .collect();
    assert!(!read.is_empty());
    assert!(read.iter().all(|path| *path == bodies[1]), "{read:?}");
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A snapshot larger than the provider's single-request limit goes up in
/// parts, never as one create at its path, and a device reloading from it
/// reads the same rows (§20.6).
#[tokio::test]
async fn a_snapshot_past_the_single_request_limit_goes_up_in_parts() {
    use coven_storage::test_utils::{MemoryRequest, MemoryStorage};
    let mut group = Group::new();
    group.storage = MemoryStorage::builder()
        .clock(group.clock.clone())
        .transfer_limits(64 * 1024, 64 * 1024)
        .build()
        .unwrap();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    for i in 0..30u32 {
        let title: String =
            (0..4096u32).map(|n| char::from(b'a' + ((n * 7 + i * 13) % 26) as u8)).collect();
        ana.sync
            .database
            .write(move |sql| {
                sql.execute("INSERT INTO notes VALUES(?1,?2)", (i.to_string(), title))?;
                Ok(())
            })
            .await
            .unwrap();
        ana.sync.pass().await.unwrap();
    }
    let before = ana.storage.request_trace().len();
    let announced = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let trace = ana.storage.request_trace().split_off(before);
    let body = {
        let session = ana.sync.session.lock().await;
        crate::snapshot_object::path(
            group.store,
            &session.history.announcements[&announced].snapshot,
        )
        .unwrap()
    };
    assert!(group.storage.status(&body).await.unwrap().unwrap().size > 64 * 1024);
    assert!(
        !trace
            .iter()
            .any(|request| matches!(request, MemoryRequest::Create(path) if *path == body)),
        "{trace:?}"
    );
    assert!(trace.contains(&MemoryRequest::Other("begin_upload")), "{trace:?}");
    assert!(trace.contains(&MemoryRequest::Other("finish_upload")), "{trace:?}");
    ben.sync.pass().await.unwrap();
    ben.sync.reload_from_snapshot().await.unwrap();
    assert_eq!(rows(&ben.sync.database).await, rows(&ana.sync.database).await);
    assert_eq!(rows(&ben.sync.database).await.len(), 30);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana's app updates with a breaking migration whose raise of the store has
/// not landed. An ordinary snapshot she captured now would be written at
/// version 2 while the store's history holds no raise to it, and a device
/// on the old app could not load it. So no ordinary capture is taken while
/// her raise waits: an explicit request is refused, and so is a request
/// journaled before the update that resumes at its capture after it. A
/// reset's snapshot is refused the same way. None announces anything
/// (§16.2, §18.5).
#[tokio::test]
async fn no_ordinary_snapshot_is_captured_while_a_raise_of_its_audience_waits() {
    use crate::sequence_sync::{sequence_operations::Request, snapshot_capture::SnapshotRequest};
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before the update").await;
    ana.sync.pass().await.unwrap();
    let journaled = ana
        .sync
        .start_request_row(
            Request::Snapshot(SnapshotRequest {
                audience: coven_merge::Audience::Store,
                purpose: coven_format::sequence::Purpose::Ordinary,
            }),
            coven_database::operation_kind::COVEN,
        )
        .await
        .unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    assert_eq!(ana.sync.database.raise_captures().await.unwrap().len(), 1);
    let announced = async |ana: &Device| ana.sync.session.lock().await.history.announcements.len();
    let before = announced(&ana).await;

    let explicit = ana.sync.snapshot(coven_merge::Audience::Store).await;
    assert!(
        matches!(explicit, Err(crate::SyncError::Rejected(crate::OperationRejection::NotAllowed))),
        "{explicit:?}"
    );
    let reset = ana.sync.reset(coven_merge::Audience::Store).await;
    assert!(
        matches!(reset, Err(crate::SyncError::Rejected(crate::OperationRejection::NotAllowed))),
        "{reset:?}"
    );
    ana.sync.pass().await.unwrap();
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.id != journaled));
    assert_eq!(announced(&ana).await, before);
    assert!(ana.sync.database.store_state().await.unwrap().schema.is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// An announcement counts only if its breaking version is one its audience
/// has reached: no higher than the version the audience was raised to by its
/// coverage, zero before any raise, and for a raise's own snapshot no higher
/// than the version it raises to. Otherwise a floor could name a snapshot a
/// device on the old app can't read. A device's own snapshots name the newest
/// breaking migration it ran (§16.3).
#[tokio::test]
async fn an_announcement_counts_only_at_a_breaking_version_its_audience_reached() {
    use coven_format::sequence::Purpose;
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before the raise").await;
    ana.sync.pass().await.unwrap();
    let before = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let allowed = async |ana: &Device, ben: &Device, breaking_version, purpose| {
        let history = ana.sync.session.lock().await.history.clone();
        let covers = ana.sync.database.applied_slot().await.unwrap();
        let (announcement, header) = announcement_by(ben, covers);
        let announcement = coven_format::sequence::SnapshotAnnouncement {
            breaking_version,
            purpose,
            ..announcement
        };
        history.announcement_allowed(coven_merge::Slot(1_000), &header, &announcement)
    };
    let history = ana.sync.session.lock().await.history.clone();
    assert_eq!(history.announcements[&before].breaking_version, 0);
    assert!(allowed(&ana, &ben, 0, Purpose::Ordinary).await);
    assert!(!allowed(&ana, &ben, 2, Purpose::Ordinary).await, "no raise yet");
    assert!(!allowed(&ana, &ben, 2, Purpose::Reset).await, "no raise yet");
    assert!(allowed(&ana, &ben, 2, Purpose::Raise(2)).await);
    assert!(!allowed(&ana, &ben, 3, Purpose::Raise(2)).await);

    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let (ana, ben) = (group.reopen(ana).await, group.reopen(ben).await);
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let state = ana.sync.database.store_state().await.unwrap();
    assert_eq!(state.schema[&coven_merge::Audience::Store].number, 2);
    assert!(allowed(&ana, &ben, 2, Purpose::Ordinary).await);
    assert!(!allowed(&ana, &ben, 3, Purpose::Ordinary).await);
    assert!(!allowed(&ana, &ben, 3, Purpose::Reset).await);
    let after = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let history = ana.sync.session.lock().await.history.clone();
    assert_eq!(history.announcements[&after].breaking_version, 2);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana makes Garden after her app reached breaking version 2. Her raise
/// covered only the audiences she read then, so nothing raises Garden: it
/// starts at the breaking version her creation entry records, and her Garden
/// snapshot at version 2 counts (§15.5, §16.3).
#[tokio::test]
async fn a_circle_made_after_its_makers_migration_counts_snapshots_at_its_base() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let state = ana.sync.database.store_state().await.unwrap();
    assert_eq!(state.schema[&coven_merge::Audience::Store].number, 2);
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    let announced = ana.sync.snapshot(coven_merge::Audience::Circle(garden)).await.unwrap();
    let history = ana.sync.session.lock().await.history.clone();
    assert_eq!(history.announcements[&announced].breaking_version, 2);
    assert!(!history.state.schema.contains_key(&coven_merge::Audience::Circle(garden)));
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's app updates with a breaking migration while he reads only the
/// store, so his raise covers the store alone. Ana, still on the old app,
/// then adds him to Garden, which nobody has raised. Every device would
/// refuse a Garden snapshot he announced, its breaking version being above
/// Garden's, so he asks for none: aged history brings a snapshot of the store
/// and none of Garden (§16.2, §16.3).
#[tokio::test]
async fn no_snapshot_is_asked_for_an_audience_not_raised_to_the_devices_migration() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.schema[&coven_merge::Audience::Store].number, 2);
    assert!(!state.schema.contains_key(&coven_merge::Audience::Circle(garden)));
    assert!(state.reads_circle(garden, &ben.identity.member()));
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ben.sync.pass().await.unwrap();
    let uploaded = |audience: coven_merge::Audience| {
        ben.storage.request_trace().into_iter().any(|request| {
            matches!(request, coven_storage::test_utils::MemoryRequest::Create(path)
            if path.snapshot_id().is_some_and(|snapshot| {
                snapshot.device == ben.sync.device && snapshot.audience == audience
            }))
        })
    };
    assert!(uploaded(coven_merge::Audience::Store), "Ben snapshots the store");
    assert!(!uploaded(coven_merge::Audience::Circle(garden)), "Ben takes no Garden snapshot");
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A refusal judges the stored snapshot, not one read of it (§19.1). Ben's
/// reload reads Ana's store snapshot with one byte changed on the way, in its
/// signed prefix or in its body: the prefix's kept bytes, or the body's
/// digest, disagree with a second read, so the reload fails as a read with
/// `Corrupted` and records no refusal, and his next reload loads it.
#[tokio::test]
async fn a_snapshot_changed_on_the_way_is_read_again_not_refused() {
    use coven_storage::test_utils::{Faults, Garble};
    for prefix in [true, false] {
        let group = Group::new();
        let ana = group.device(1).await;
        let ben = group.device(2).await;
        group.create(&ana).await;
        group.admit(&ana, &ben).await;
        insert(&ana.sync.database, "a", "in the snapshot").await;
        ana.sync.pass().await.unwrap();
        let slot = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
        ben.sync.pass().await.unwrap();
        let snapshot = ben.sync.session.lock().await.history.announcements[&slot].snapshot.clone();
        let path = crate::snapshot_object::path(group.store, &snapshot).unwrap();
        let size = group.storage.status(&path).await.unwrap().unwrap().size;
        // Whatever of the prefix Ben's pass kept goes, so the reload reads it.
        if let Some(kept) =
            crate::object_input::ObjectInput::retained(ben.database(), &path).await.unwrap()
        {
            kept.discard().await.unwrap();
        }
        let at = if prefix { 0 } else { size - 1 };
        let garbled = [(path.clone(), Garble { at, reads: 1 })].into();
        group.storage.set_faults(Faults { garbled_reads: garbled, ..Faults::none() }).await;
        let failed = ben.sync.reload_from_snapshot().await.unwrap_err();
        assert!(
            matches!(&failed, crate::SyncError::Storage(error)
                if error.failure() == StorageFailure::Corrupted),
            "{failed:?}"
        );
        let pending = ben.database().pending::<crate::PendingRecord>().await.unwrap();
        assert!(
            pending.iter().all(|record| !matches!(record.reason, crate::PendingReason::Refused(_))),
            "{pending:?}"
        );
        ben.sync.reload_from_snapshot().await.unwrap();
        assert_eq!(rows(&ben.sync.database).await.len(), 1);
        for device in [&ana, &ben] {
            device.sync.database.close().await.unwrap();
        }
    }
}

/// Ana's phone prepares Garden on the old app, so its creation entry records
/// base 0, and her app updates with a breaking migration before the entry
/// applies. Her migration raised only what she read then, the store, and that
/// raise has finished. Garden has no rows when its creation applies, so her
/// phone holds its state at version 2 and raises it; without that, Garden
/// would stay below her version and none of her Garden snapshots would count
/// (§15.5, §18.3).
#[tokio::test]
async fn a_circle_its_maker_migrated_before_creating_is_raised_by_the_maker() {
    use crate::sequence_sync::sequence_operations::Request;
    use coven_foundation::id_source::{CircleId, IdSource, KeyId};
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.database.store_state().await.unwrap().reached(&coven_merge::Audience::Store),
        2
    );
    assert!(ana.sync.database.raise_captures().await.unwrap().is_empty());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());

    // The entry as her phone prepared it before the update, its key in
    // custody.
    let garden = CircleId(group.ids.new_id());
    let key = coven_crypto::CircleKey::from_bytes(garden, KeyId(group.ids.new_id()), [9; 32]);
    let mut ring = ana.sync.keys.read().unwrap().unwrap();
    let (key_id, hash) = (key.id(), key.hash());
    ring.insert_circle_key(key).unwrap();
    ana.sync.keys.persist(&ring).unwrap();
    ana.sync
        .start_request_row(
            Request::Change(coven_format::admin::StoreChange::CreateCircle {
                circle: garden,
                name: "Garden".into(),
                breaking_version: 0,
                key: key_id,
                key_hash: hash,
            }),
            coven_database::operation_kind::COVEN,
        )
        .await
        .unwrap();
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    let state = ana.sync.database.store_state().await.unwrap();
    assert!(state.reads_circle(garden, &ana.identity.member()));
    assert_eq!(state.circles[&garden].base, 0);
    assert_eq!(state.reached(&coven_merge::Audience::Circle(garden)), 2);
    assert!(ana.sync.database.raise_captures().await.unwrap().is_empty());
    let announced = ana.sync.snapshot(coven_merge::Audience::Circle(garden)).await.unwrap();
    let history = ana.sync.session.lock().await.history.clone();
    assert_eq!(history.announcements[&announced].breaking_version, 2);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's app migrates while he reads only the store, and Ana, still on the old
/// app, then adds him to Garden. Ben never held Garden's rows at version 0, so
/// he can't make its raise body and doesn't raise it. Ana's update captures
/// Garden, which she held, and raises it; Ben reloads Garden from her raise,
/// and his Garden snapshots count from then on (§15.7, §18.3).
#[tokio::test]
async fn a_circle_joined_after_migrating_is_raised_by_a_member_who_held_it() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let state = ben.sync.database.store_state().await.unwrap();
    assert!(state.reads_circle(garden, &ben.identity.member()));
    assert_eq!(state.reached(&coven_merge::Audience::Store), 2);
    assert_eq!(state.reached(&coven_merge::Audience::Circle(garden)), 0);
    assert!(ben.sync.database.raise_captures().await.unwrap().is_empty());

    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&coven_merge::Audience::Circle(garden)), 2);
    assert_eq!(
        state.schema[&coven_merge::Audience::Circle(garden)].snapshot.device,
        ana.sync.device
    );
    let announced = ben.sync.snapshot(coven_merge::Audience::Circle(garden)).await.unwrap();
    let history = ben.sync.session.lock().await.history.clone();
    assert_eq!(history.announcements[&announced].breaking_version, 2);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
