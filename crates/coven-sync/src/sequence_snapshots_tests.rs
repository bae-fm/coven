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
