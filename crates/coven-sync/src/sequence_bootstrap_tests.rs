use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::admin::StoreChange;
use coven_storage::{ObjectPath, Storage};

#[tokio::test]
async fn a_restored_device_opens_its_copy_loads_the_sequence_and_registers() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "before restore").await;
    ana.sync.pass().await.unwrap();
    let laptop = group
        .newcomer(ana.identity.clone(), group.storage.for_device())
        .await;
    assert!(laptop.sync.keys.read().unwrap().is_none());
    assert!(matches!(
        laptop.sync.pass().await.unwrap(),
        SequenceStatus::Synced { through: 3 }
    ));
    assert_eq!(
        rows(&laptop.sync.database).await,
        rows(&ana.sync.database).await
    );
    laptop
        .sync
        .register_device("Ana's laptop".into(), None)
        .await
        .unwrap();
    insert(&laptop.sync.database, "b", "from the laptop").await;
    laptop.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(rows(&ana.sync.database).await.len(), 2);
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&laptop.sync.database).await
    );
    assert!(laptop.sync.keys.read().unwrap().is_some());
    for device in [&ana, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_restored_device_opens_the_floor_copies_when_old_slots_are_deleted() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "covered by the snapshot").await;
    ana.sync.pass().await.unwrap();
    let covered = ana.sync.database.applied_slot().await.unwrap();
    ana.sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, 1.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, covered.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    let laptop = group
        .newcomer(ana.identity.clone(), group.storage.for_device())
        .await;
    laptop.sync.pass().await.unwrap();
    assert_eq!(
        rows(&laptop.sync.database).await,
        [("a".into(), "covered by the snapshot".into())]
    );
    laptop
        .sync
        .register_device("Ana's laptop".into(), None)
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        laptop.sync.database.applied_slot().await.unwrap()
    );
    for device in [&ana, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

/// The floor names a snapshot as soon as it is announced, and a new device
/// starts from it even while `first` is still slot 1: it reads only the
/// slots after the snapshot's coverage.
#[tokio::test]
async fn a_restored_device_starts_from_a_fresh_snapshot_while_every_slot_is_kept() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    for (id, title) in [("a", "one"), ("b", "two"), ("c", "three")] {
        insert(&ana.sync.database, id, title).await;
        ana.sync.pass().await.unwrap();
    }
    let announced = ana
        .sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    let covers = ana.sync.session.lock().await.history.announcements[&announced].covers;
    insert(&ana.sync.database, "d", "after the snapshot").await;
    ana.sync.pass().await.unwrap();
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, 1.try_into().unwrap()))
        .await
        .unwrap()
        .is_some());
    let laptop = group
        .newcomer(ana.identity.clone(), group.storage.for_device())
        .await;
    laptop.sync.pass().await.unwrap();
    assert_eq!(
        rows(&laptop.sync.database).await,
        rows(&ana.sync.database).await
    );
    let read: Vec<_> = laptop
        .storage
        .request_trace()
        .into_iter()
        .filter_map(|request| match request {
            coven_storage::test_utils::MemoryRequest::Read(path, _) => path.slot_number(),
            _ => None,
        })
        .collect();
    assert!(
        read.iter().all(|slot| slot.0 > covers),
        "read {read:?} at or below coverage {covers}"
    );
    for device in [&ana, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana's notes, about 3 MiB of them, so the store snapshot is larger than a
/// lowered input budget.
async fn large_notes(db: &coven_database::Database) {
    db.write(|sql| {
        for row in 0..96 {
            let title = format!("{row:04}").repeat(8 * 1024);
            sql.execute(
                "INSERT INTO notes VALUES(?1,?2)",
                (format!("{row:04}"), title),
            )?;
        }
        Ok(())
    })
    .await
    .unwrap();
}

/// Ana's store snapshot of her large notes, past the grace period, so the
/// floor names it and every slot it covers is deleted.
async fn floor_snapshot_of_large_notes(group: &Group, ana: &Device) {
    group.create(ana).await;
    large_notes(&ana.sync.database).await;
    ana.sync.pass().await.unwrap();
    ana.sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, 1.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
}

/// The store snapshot the floor names, and its size.
async fn floor_store_snapshot(group: &Group, ana: &Device) -> (ObjectPath, u64) {
    let mut ring = ana.sync.keys.read().unwrap().unwrap();
    let floor = crate::floor_object::FloorObject::read(
        &ana.sync.database,
        ana.sync.storage.as_ref(),
        &ObjectPath::floor(group.store),
        None,
        &mut ring,
        &ana.identity,
    )
    .await
    .unwrap()
    .expect("retention wrote a floor");
    let session = ana.sync.session.lock().await;
    let announcement = &session.history.announcements[&floor.header.snapshots[0]];
    let path = crate::snapshot_object::path(group.store, &announcement.snapshot).unwrap();
    let size = group.storage.status(&path).await.unwrap().unwrap().size;
    (path, size)
}

/// A newcomer whose input budget, 1 MiB, is a third of the snapshot.
async fn small_budget_newcomer(group: &Group, ana: &Device) -> Device {
    let laptop = group
        .newcomer(ana.identity.clone(), group.storage.for_device())
        .await;
    laptop
        .sync
        .database
        .set_input_budget(1024 * 1024)
        .await
        .unwrap();
    laptop
}

/// A snapshot larger than the input budget streams through it in one pass:
/// its sealed body is never retained, and only its staged records grow while
/// it streams (§16.2, §21.1).
#[tokio::test]
async fn a_restored_device_loads_a_floor_snapshot_larger_than_its_input_budget() {
    let group = Group::new();
    let ana = group.device(1).await;
    floor_snapshot_of_large_notes(&group, &ana).await;
    let (path, size) = floor_store_snapshot(&group, &ana).await;
    let laptop = small_budget_newcomer(&group, &ana).await;
    assert!(
        size > 3 * 1024 * 1024
            && size > 3 * laptop.sync.database.input_budget().await.unwrap().maximum
    );
    // Every input write from here on samples what it retains.
    let database = laptop.sync.database.clone();
    database.input_peak().await.unwrap();
    let (observed, held) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_body_at(path.clone(), size / 2, observed, resume)
        .await;
    let restoring = laptop.sync.pass();
    tokio::pin!(restoring);
    within(async {
        tokio::select! {
            result = &mut restoring => panic!("the restore ended with the body held: {result:?}"),
            held = held => held.unwrap(),
        }
    })
    .await;
    // Halfway through the body, about half its records are staged and
    // none of its sealed bytes are retained.
    assert!(database.staged_snapshot_records().await.unwrap() > 0);
    assert!(database.input_budget().await.unwrap().retained < 4096);
    release.send(()).unwrap();
    within(restoring).await.unwrap();
    assert_eq!(rows(&laptop.sync.database).await.len(), 96);
    assert_eq!(
        rows(&laptop.sync.database).await,
        rows(&ana.sync.database).await
    );
    assert_eq!(database.staged_snapshot_records().await.unwrap(), 0);
    // The body streamed once, from the end of its prefix.
    let bodies: Vec<_> = group
        .storage
        .reads()
        .await
        .into_iter()
        .filter(|(read, start, _)| *read == path && *start > 64)
        .collect();
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    // What any input write retained stayed below one chunk and the kept
    // slots, which are all that can be in flight beside the snapshot.
    let mut slots = 0;
    for number in 1..=ana.sync.database.applied_slot().await.unwrap() {
        if let Some(slot) = group
            .storage
            .status(&ObjectPath::slot(group.store, number.try_into().unwrap()))
            .await
            .unwrap()
        {
            slots += slot.size;
        }
    }
    let peak = database.input_peak().await.unwrap();
    assert!(
        peak <= coven_format::chunks::CHUNK_SIZE as u64 + slots,
        "{peak} > {} + {slots}",
        coven_format::chunks::CHUNK_SIZE
    );
    for device in [&ana, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

/// A body interrupted halfway fails the pass and leaves nothing staged;
/// the next pass streams it again from the start (§16.2).
#[tokio::test]
async fn an_interrupted_snapshot_stream_stages_nothing_and_starts_over() {
    let group = Group::new();
    let ana = group.device(1).await;
    floor_snapshot_of_large_notes(&group, &ana).await;
    let (path, size) = floor_store_snapshot(&group, &ana).await;
    let laptop = small_budget_newcomer(&group, &ana).await;
    let mut faults = coven_storage::test_utils::Faults::none();
    faults.body_failures.insert(
        path.clone(),
        coven_storage::test_utils::BodyFailure::Interrupted(size / 2),
    );
    group.storage.set_faults(faults).await;
    let result = laptop.sync.pass().await;
    assert!(
        matches!(&result, Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::Network),
        "{result:?}"
    );
    assert_eq!(
        laptop
            .sync
            .database
            .staged_snapshot_records()
            .await
            .unwrap(),
        0
    );
    assert!(rows(&laptop.sync.database).await.is_empty());
    group
        .storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    laptop.sync.pass().await.unwrap();
    assert_eq!(
        rows(&laptop.sync.database).await,
        rows(&ana.sync.database).await
    );
    assert_eq!(
        laptop
            .sync
            .database
            .staged_snapshot_records()
            .await
            .unwrap(),
        0
    );
    for device in [&ana, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

/// A changed byte in the body, whose chunk then fails to open, is refused
/// for the announced digest once the body has streamed, and installs and
/// stages nothing (§16.2).
#[tokio::test]
async fn a_tampered_snapshot_body_is_refused_for_its_digest() {
    let group = Group::new();
    let ana = group.device(1).await;
    floor_snapshot_of_large_notes(&group, &ana).await;
    let (path, size) = floor_store_snapshot(&group, &ana).await;
    let laptop = small_budget_newcomer(&group, &ana).await;
    group
        .storage
        .corrupt_byte(&path, size as usize / 2)
        .await
        .unwrap();
    let result = laptop.sync.pass().await;
    assert!(
        matches!(
            &result,
            Err(SyncError::Damaged(crate::DamagedObject {
                failure: Refusal::WrongIdentity { .. },
                ..
            }))
        ),
        "{result:?}"
    );
    assert_eq!(
        laptop
            .sync
            .database
            .staged_snapshot_records()
            .await
            .unwrap(),
        0
    );
    assert!(rows(&laptop.sync.database).await.is_empty());
    assert_eq!(laptop.sync.database.applied_slot().await.unwrap(), 0);
    for device in [&ana, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_old_restore_code_cannot_register_after_its_keys_were_replaced() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let replacement = coven_format::test_utils::keys_from_seed(9).public_keys();
    ben.sync
        .administer(StoreChange::ReplaceMemberKeys {
            member: ana.identity.member(),
            expected: ana.identity.keys().public_keys(),
            keys: replacement,
        })
        .await
        .unwrap();
    let laptop = group
        .newcomer(ana.identity.clone(), group.storage.for_device())
        .await;
    laptop.sync.pass().await.unwrap();
    let count = laptop.storage.request_count();
    let result = laptop
        .sync
        .register_device("Ana's laptop".into(), None)
        .await;
    assert!(
        matches!(result, Err(SyncError::MemberKeysReplaced { member }) if member == ana.identity.member()),
        "{result:?}"
    );
    assert!(laptop
        .sync
        .database
        .current_claim()
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        laptop.storage.request_count() - count,
        2,
        "only the next-slot read and the floor read"
    );
    for device in [&ana, &ben, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_newcomer_without_a_copy_reports_the_key_it_lacks() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let stranger = coven_crypto::MemberIdentity::new(
        coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(77)),
        coven_format::test_utils::keys_from_seed(5),
    );
    let device = group.newcomer(stranger, group.storage.for_device()).await;
    let key = group.ring.store_key_ids().next().unwrap();
    assert!(
        matches!(device.sync.pass().await, Err(SyncError::KeyUnavailable(missing)) if missing == key)
    );
    assert_eq!(device.sync.database.applied_slot().await.unwrap(), 0);
    for device in [&ana, &device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Custody that lost its store keys after this installation applied history
/// opens them again from this member's copies, then keeps them (§12.1).
#[tokio::test]
async fn an_installation_whose_custody_lost_its_keys_unlocks_from_its_copies() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "kept").await;
    ana.sync.pass().await.unwrap();
    let applied = ana.sync.database.applied_slot().await.unwrap();
    ana.sync.database.close().await.unwrap();
    let identity = ana.identity.clone();
    let locked = group
        .open_with(ana._temp, identity, None, group.storage.for_device())
        .await;
    assert!(locked.sync.keys.read().unwrap().is_none());
    locked.sync.unlock().await.unwrap();
    assert!(locked.sync.keys.read().unwrap().is_some());
    assert_eq!(locked.sync.database.applied_slot().await.unwrap(), applied);
    insert(&locked.sync.database, "b", "after unlocking").await;
    locked.sync.pass().await.unwrap();
    assert_eq!(
        locked.sync.database.applied_slot().await.unwrap(),
        applied + 1
    );
    locked.sync.database.close().await.unwrap();
}

/// The whole-object reads a connection sent for each slot, by slot number.
fn slot_reads(storage: &coven_storage::test_utils::MemoryStorage) -> Vec<u64> {
    let mut slots: Vec<u64> = storage
        .request_trace()
        .into_iter()
        .filter_map(|request| match request {
            coven_storage::test_utils::MemoryRequest::Read(path, None) => {
                path.slot_number().map(|slot| slot.0)
            }
            _ => None,
        })
        .collect();
    slots.sort_unstable();
    slots
}

#[tokio::test]
async fn a_restore_reads_each_slot_once() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    for id in ["a", "b", "c"] {
        insert(&ana.sync.database, id, "before restore").await;
        ana.sync.pass().await.unwrap();
    }
    let through = ana.sync.database.applied_slot().await.unwrap();
    let storage = group.storage.for_device();
    let laptop = group.newcomer(ana.identity.clone(), storage.clone()).await;
    // Without a floor, the key scan reads every slot; reading forward then
    // uses the bytes the scan retained, with no second request.
    within(laptop.sync.unlock()).await.unwrap();
    assert_eq!(rows(&laptop.sync.database).await.len(), 3);
    let reads = slot_reads(&storage);
    for slot in 1..=through {
        assert_eq!(
            reads.iter().filter(|read| **read == slot).count(),
            1,
            "{reads:?}"
        );
    }
    for device in [&ana, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

/// Wait for `work`, failing the test if it takes longer than ten seconds.
async fn within<T>(work: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(10), work)
        .await
        .expect("restore work did not finish within 10 s")
}

#[tokio::test]
async fn a_restore_reads_slots_ahead_and_a_quiet_pass_still_sends_one_miss() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    for id in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"] {
        insert(&ana.sync.database, id, "before restore").await;
        ana.sync.pass().await.unwrap();
    }
    let through = ana.sync.database.applied_slot().await.unwrap();
    let storage = group.storage.for_device();
    let laptop = within(group.newcomer(ana.identity.clone(), storage.clone())).await;
    // Reads ahead share the download limit with file reads (api.md E5).
    laptop
        .files()
        .set_transfer_limits(coven_storage::TransferLimits {
            uploads: std::num::NonZeroUsize::MIN,
            downloads: 16.try_into().unwrap(),
        });
    // While slot 2's read is held, a reader one slot at a time sends
    // nothing more; one reading ahead also asks for slot 3.
    let (observed, _held) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    storage
        .hold_next_read(
            ObjectPath::slot(group.store, 2.try_into().unwrap()),
            observed,
            resume,
        )
        .await;
    let mut requests = storage.subscribe_requests();
    let restoring = laptop.sync.unlock();
    tokio::pin!(restoring);
    within(async {
        while !slot_reads(&storage).contains(&3) {
            tokio::select! {
                result = &mut restoring => panic!("the restore ended with slot 2 held: {result:?}"),
                changed = requests.changed() => changed.unwrap(),
            }
        }
    })
    .await;
    release.send(()).unwrap();
    within(restoring).await.unwrap();
    assert_eq!(within(rows(&laptop.sync.database)).await.len(), 10);
    let reads = slot_reads(&storage);
    for slot in 1..=through {
        assert_eq!(reads.iter().filter(|read| **read == slot).count(), 1);
    }
    // Past the end, at most the reads already in flight find nothing.
    assert!(reads.len() as u64 <= through + 16, "{reads:?}");
    let before = slot_reads(&storage).len();
    within(laptop.sync.pass()).await.unwrap();
    assert_eq!(slot_reads(&storage).len(), before + 1);
    for device in [&ana, &laptop] {
        within(device.sync.database.close()).await.unwrap();
    }
}
