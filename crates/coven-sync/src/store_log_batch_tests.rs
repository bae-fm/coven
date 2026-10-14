use crate::store_log_sync::tests::{device, key, member, store};
use coven_database::test_utils::WorkCounter;
use coven_format::store_log::StoreChange;
use coven_foundation::id_source::DeviceId;
use coven_storage::test_utils::MemoryStorage;
use std::sync::Arc;

#[tokio::test]
async fn ready_entries_commit_one_replay_and_retain_decoded_state() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut author = device(storage.clone(), 1, member(1), store(1)).await;
    author.create(key(1)).await;
    for number in 10..12 {
        author
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(number),
                name: number.to_string(),
                replaces: None,
            })
            .await
            .unwrap();
    }
    let receiver = device(storage, 2, member(1), store(1)).await;
    let work = WorkCounter::default();
    receiver.db.test_track_work(work.clone()).await.unwrap();
    receiver.sync.step().await.unwrap();
    let counts = work.take();
    assert_eq!(
        counts.store_log_commits, 1,
        "one replay commit for the whole ready batch"
    );
    // The replay and its file cleanup each commit once. Received input is
    // retained before application: each log with new input retains its reads
    // together, and a listed writer folder unknown locally is remembered.
    assert_eq!(
        counts.commits,
        2 + 2 + 1,
        "replay, cleanup, entries, key copies and the listed writer folder"
    );
    assert_eq!(counts.full_log_decodes, 1, "decode the saved log once");
    assert_eq!(receiver.log().await, author.log().await);
    receiver.db.local_store_log().await.unwrap();
    receiver.db.local_store_log().await.unwrap();
    let reads = work.take();
    assert_eq!(reads.full_log_decodes, 0, "reuse the committed decoded log");
    assert_eq!(
        reads.writer_holds, 0,
        "saved-log reads leave the writer available"
    );
}

#[tokio::test]
async fn arriving_registration_extends_discovery_before_the_batch_commits() {
    use coven_foundation::clock::FixedClock;
    use coven_storage::{test_utils::Faults, ObjectPrefix, Storage};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, UNIX_EPOCH};
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut author = device(storage.clone(), 1, member(1), store(1)).await;
    author.create(key(1)).await;
    author
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(2),
            name: "second".into(),
            replaces: None,
        })
        .await
        .unwrap();
    let mut second = device(storage.clone(), 2, member(1), store(1)).await;
    second.sync.step().await.unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(10));
    let new = second
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(10),
            name: "third".into(),
            replaces: None,
        })
        .await
        .unwrap();
    storage
        .set_faults(Faults {
            listing_lag: Some(Duration::from_secs(1)),
            ..Faults::none()
        })
        .await;
    let listed = storage
        .list(&ObjectPrefix::store_logs(store(1)))
        .await
        .unwrap();
    assert!(listed
        .iter()
        .all(|object| object.path.store_log_position().unwrap().0 == DeviceId(1)));
    let mut receiver = device(storage, 3, member(1), store(1)).await;
    let decodes = Arc::new(AtomicUsize::new(0));
    receiver.sync.test_track_received_entries(decodes.clone());
    let work = WorkCounter::default();
    receiver.db.test_track_work(work.clone()).await.unwrap();
    receiver.sync.step().await.unwrap();
    let counts = work.take();
    assert_eq!(counts.store_log_commits, 1);
    assert_eq!(decodes.load(Ordering::Relaxed), 3);
    let log = receiver.log().await;
    assert!(log.replay.entries.contains_key(&new));
    assert!(log.replay.state.devices.contains_key(&DeviceId(10)));
}

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential_batch_partitions() {
    use crate::replay::tests as history;
    use crate::tests::{input, lean_results, orders, projected, publication_times};
    use coven_database::StoreLog;
    let mut histories = crate::conflicts::tests::independent_histories();
    histories.extend(
        crate::conflicts::tests::contradictory_histories()
            .into_iter()
            .map(|(history, _)| history),
    );
    let mut reversal = history::gifts();
    reversal.push(0, 0, &[0, 1, 2, 3, 4], history::remove(1));
    reversal.push(1, 1, &[0, 1, 2, 3, 4], history::delete(0));
    histories.push(reversal);
    for h in histories {
        let stored: Vec<_> = (0..h.entries.len() as u64).collect();
        let publications = publication_times(&h, &stored);
        for order in orders(&h) {
            for width in [1, 2, 3, h.entries.len()] {
                let mut received = Vec::new();
                let mut all = Vec::new();
                let mut expected = Vec::new();
                let mut log = StoreLog::default();
                for chunk in order.chunks(width) {
                    let mut batch = crate::replay::ReplayBatch::new(&log);
                    for &index in chunk {
                        batch.push_checked(h.entries[index].clone(), &publications);
                        all.push(index);
                    }
                    let (entries, replay) = batch.finish().unwrap();
                    log.entries.extend(entries);
                    log.replay = replay;
                    received.push(all.clone());
                    expected.push(projected(&h, &log.replay));
                }
                let query = input(&h, &stored, &received);
                assert_eq!(
                    lean_results(&[query]),
                    vec![expected],
                    "batch width {width}, order {order:?}"
                );
                assert_eq!(log.replay, crate::replay(&h.entries, &publications));
            }
        }
    }
}

#[tokio::test]
async fn a_queued_publication_follows_the_committed_read_only_catch_up() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut author = device(storage.clone(), 1, member(1), store(1)).await;
    author.create(key(1)).await;
    let mut receiver = device(storage.clone(), 2, member(1), store(1)).await;
    receiver.sync.step().await.unwrap();
    receiver.sync.load_new_device("second").await.unwrap();
    author.sync.step().await.unwrap();
    receiver.interrupt_entry(2).await;
    assert!(receiver
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(20),
            name: "queued".into(),
            replaces: None,
        })
        .await
        .is_err());
    let queued = receiver
        .db
        .local_store_log()
        .await
        .unwrap()
        .upload
        .unwrap()
        .entry
        .position;
    let remote = author
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(30),
            name: "remote".into(),
            replaces: None,
        })
        .await
        .unwrap();
    receiver.sync.storage = Some(storage);
    let work = WorkCounter::default();
    receiver.db.test_track_work(work.clone()).await.unwrap();
    receiver.sync.step().await.unwrap();
    assert_eq!(work.take().store_log_commits, 2);
    let current = receiver.db.local_store_log().await.unwrap();
    assert!(current.upload.is_none());
    assert!(current.log.replay.entries.contains_key(&queued));
    assert!(current.log.replay.entries.contains_key(&remote));
}

#[tokio::test]
async fn intermediate_removal_cannot_stop_the_owner_before_batch_replay() {
    use crate::store_log_sync::tests::public;
    use coven_database::EntryOutcome;
    use coven_format::{store_log::MemberRole, MemberAccess};
    use std::time::{Duration, UNIX_EPOCH};
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut ana = device(storage.clone(), 10, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 1, member(2), store(1)).await;
    ana.create(key(1)).await;
    ana.sync
        .make_and_upload_entry(StoreChange::AddMember {
            keys: public(&ben.member),
            role: MemberRole::Admin,
            access: MemberAccess::S3AccessKey {
                access_key_id: "ben".into(),
                principal: coven_format::S3Principal::Console,
            },
        })
        .await
        .unwrap();
    ben.sync.step().await.unwrap();
    ben.sync.load_new_device("Ben").await.unwrap();
    ana.sync.step().await.unwrap();
    let receiver = device(storage, 3, member(1), store(1)).await;
    receiver.sync.step().await.unwrap();
    let winner = ana
        .sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: ben.member.member_id(),
        })
        .await
        .unwrap();
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    let loser = ben
        .sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: ana.member.member_id(),
        })
        .await
        .unwrap();
    let work = WorkCounter::default();
    receiver.db.test_track_work(work.clone()).await.unwrap();
    receiver.sync.step().await.unwrap();
    assert_eq!(work.take().store_log_commits, 1);
    let log = receiver.log().await;
    assert_eq!(log.replay.entries[&winner], EntryOutcome::Kept);
    assert!(matches!(
        log.replay.entries[&loser],
        EntryOutcome::Dropped(_)
    ));
    assert!(!log.replay.state.members[&ana.member.member_id()].removed);
    assert!(log.replay.state.members[&ben.member.member_id()].removed);
}

mod waits {
    //! A received entry that cannot apply yet is its own subject's record,
    //! replaced by each scan and ended when it applies (§19.1).
    use crate::store_log_object as object;
    use crate::store_log_sync::tests::{device, find_copy, key, member, storage, store, Device};
    use crate::{PendingReason, PendingRecord, PendingSubject, Prerequisite};
    use coven_format::store_log::{MemberRole, StoreChange};
    use coven_format::value::EntryId;
    use coven_foundation::id_source::DeviceId;
    use coven_merge::Audience;
    use coven_storage::Storage;

    async fn entry_records(device: &Device) -> Vec<(EntryId, PendingReason)> {
        let own = device.db.local_store_log().await.unwrap().device;
        device
            .db
            .pending::<PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|record| match record.subject {
                PendingSubject::Entry(id) if record.reported_by == own => Some((id, record.reason)),
                _ => None,
            })
            .collect()
    }

    fn add_device(number: u64) -> StoreChange {
        StoreChange::AddDevice {
            device: DeviceId(number),
            name: number.to_string(),
            replaces: None,
        }
    }

    #[tokio::test]
    async fn an_entry_without_its_key_waits_for_it_until_the_copy_arrives() {
        let storage = storage();
        let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
        let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
        ana.create(key(1)).await;
        ana.add(&ben.member, MemberRole::Member).await;
        ben.sync().await;
        ana.sync.test_rotate(Audience::Store, key(3)).await;
        let sealed = ana
            .sync
            .make_and_upload_entry(add_device(20))
            .await
            .unwrap();
        let later = ana
            .sync
            .make_and_upload_entry(add_device(21))
            .await
            .unwrap();
        let copy = find_copy(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &ben.member.member_id(),
        )
        .await
        .unwrap()
        .path;
        let bytes = storage.read(&copy).await.unwrap();
        storage.delete(&copy).await.unwrap();
        ben.sync().await;
        // The first entry sealed with the missing key waits for it; the
        // later one waits behind it in its log and has no record of its own.
        assert!(!ben.log().await.replay.entries.contains_key(&sealed));
        assert_eq!(
            entry_records(&ben).await,
            [(
                sealed,
                PendingReason::KeyUnavailable {
                    audience: Audience::Store,
                    key: key(3),
                }
            )]
        );
        // An unchanged scan keeps the same record.
        ben.sync().await;
        assert_eq!(entry_records(&ben).await.len(), 1);
        storage.create(&copy, &bytes).await.unwrap();
        ben.sync().await;
        assert!(ben.log().await.replay.entries.contains_key(&later));
        assert!(entry_records(&ben).await.is_empty());
    }

    #[tokio::test]
    async fn an_entry_waits_for_the_first_unapplied_entry_of_its_past() {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        let mut b = device(storage.clone(), 2, member(1), store(1)).await;
        a.create(key(1)).await;
        let read = a.sync.make_and_upload_entry(add_device(20)).await.unwrap();
        b.sync().await;
        b.sync.load_new_device("second").await.unwrap();
        let later = b.sync.make_and_upload_entry(add_device(21)).await.unwrap();
        let own = EntryId {
            device: later.device,
            number: 1,
        };
        let missing = object::path(store(1), read);
        let bytes = storage.read(&missing).await.unwrap();
        storage.delete(&missing).await.unwrap();
        let mut c = device(storage.clone(), 3, member(1), store(1)).await;
        c.sync().await;
        // Its device's first entry read the missing one; the later entry
        // waits first for its own log's previous entry (§19.1).
        let mut records = entry_records(&c).await;
        records.sort_by_key(|(id, _)| (id.device, id.number));
        let mut expected = vec![(
            own,
            PendingReason::Waits(Prerequisite::Object(missing.clone())),
        )];
        for number in 2..=later.number {
            expected.push((
                EntryId {
                    device: later.device,
                    number,
                },
                PendingReason::Waits(Prerequisite::Object(object::path(
                    store(1),
                    EntryId {
                        device: later.device,
                        number: number - 1,
                    },
                ))),
            ));
        }
        assert_eq!(records, expected);
        storage.create(&missing, &bytes).await.unwrap();
        c.sync().await;
        assert!(entry_records(&c).await.is_empty());
        assert_eq!(c.log().await, b.log().await);
    }
}
