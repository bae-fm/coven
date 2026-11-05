use crate::sequence_sync::tests::*;
use crate::{PendingSubject, SequenceStatus, SyncError};
use coven_storage::{ObjectPath, Storage};

/// A device far behind brackets the end of the sequence with doubling
/// existence checks and narrows the gap by halving: the checks grow with the
/// logarithm of the sequence's length, not with its length (§6.1, §21.1).
#[tokio::test]
async fn a_device_far_behind_finds_the_end_in_logarithmic_existence_checks() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let behind = ben.database().applied_slot().await.unwrap();
    for i in 0..100u32 {
        let id: &'static str = Box::leak(i.to_string().into_boxed_str());
        insert(ana.database(), id, "far ahead").await;
        ana.sync.pass().await.unwrap();
    }
    let end = ana.database().applied_slot().await.unwrap();
    let missing = end - behind;
    assert!(missing >= 100);

    let traced = ben.storage.request_trace().len();
    ben.sync.pass().await.unwrap();
    assert_eq!(ben.database().applied_slot().await.unwrap(), end);

    let trace = &ben.storage.request_trace()[traced..];
    let checks = trace
        .iter()
        .filter(|request| {
            matches!(request, coven_storage::test_utils::MemoryRequest::Status(path)
                if path.slot_number().is_some())
        })
        .count();
    let log = u64::BITS - missing.leading_zeros();
    assert!(checks > 0, "a pass this far behind checks where the sequence ends");
    assert!(checks as u32 <= 2 * log + 2, "{checks} existence checks for {missing} missing slots");
    for slot in behind + 1..=end {
        let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
        let reads = trace
            .iter()
            .filter(|request| {
                matches!(request, coven_storage::test_utils::MemoryRequest::Read(read, _)
                    if *read == path)
            })
            .count();
        assert_eq!(reads, 1, "slot {slot} is read once");
    }
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// A quiet store costs the same two reads with three devices or twenty
/// (§21.1).
#[tokio::test]
async fn a_quiet_pass_makes_two_requests_with_twenty_devices() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let mut others = Vec::new();
    for seed in 2..=20u8 {
        let device = group.device(seed).await;
        group.admit(&ana, &device).await;
        others.push(device);
    }
    for device in std::iter::once(&ana).chain(&others) {
        device.sync.pass().await.unwrap();
    }
    for device in std::iter::once(&ana).chain(&others) {
        device.sync.pass().await.unwrap();
    }
    assert_eq!(others.len() + 1, 20);
    assert_idle_work(&ana).await;
    assert_idle_work(others.last().unwrap()).await;
    for device in std::iter::once(&ana).chain(&others) {
        device.database().close().await.unwrap();
    }
}

/// After Ana adds one note, Ben's next pass reads the slot, the miss after
/// it, and the floor (§21.1).
#[tokio::test]
async fn one_new_note_costs_its_reader_three_reads() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    insert(ana.database(), "added", "one note").await;
    ana.sync.pass().await.unwrap();

    let before = ben.storage.request_count();
    ben.sync.pass().await.unwrap();
    assert_eq!(ben.storage.request_count() - before, 3);
    assert_eq!(rows(ben.database()).await, [("added".into(), "one note".into())]);
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// Finding the sequence never lists a folder: slots, snapshots and members are
/// found by exact path, through writing, catching up across more slots than one
/// batch holds (which locates the end) and a newcomer's restore (§20.4).
#[tokio::test]
async fn finding_the_sequence_never_lists() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    for i in 0..40u32 {
        let id: &'static str = Box::leak(i.to_string().into_boxed_str());
        insert(ana.database(), id, "written").await;
        ana.sync.pass().await.unwrap();
    }
    ben.sync.pass().await.unwrap();
    let laptop = group.newcomer(ana.identity.clone(), group.storage.for_device()).await;
    laptop.sync.pass().await.unwrap();
    assert_eq!(rows(laptop.database()).await, rows(ana.database()).await);
    for device in [&ana, &ben, &laptop] {
        let trace = device.storage.request_trace();
        assert!(!trace.is_empty());
        let listed: Vec<_> = trace
            .iter()
            .filter(|request| matches!(request, coven_storage::test_utils::MemoryRequest::List(_)))
            .collect();
        assert!(listed.is_empty(), "{listed:?}");
        device.database().close().await.unwrap();
    }
}

/// Locating the end of the sequence only tests whether slots exist: each
/// check is a status request for one slot's path, none reads a slot, and the
/// applied prefix is where it was (§6.1).
#[tokio::test]
async fn locating_the_end_checks_existence_and_applies_nothing() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let behind = ben.database().applied_slot().await.unwrap();
    for i in 0..20u32 {
        let id: &'static str = Box::leak(i.to_string().into_boxed_str());
        insert(ana.database(), id, "ahead").await;
        ana.sync.pass().await.unwrap();
    }
    let end = ana.database().applied_slot().await.unwrap();
    let traced = ben.storage.request_trace().len();
    assert_eq!(ben.sync.locate_end(behind).await.unwrap(), end);
    let trace = &ben.storage.request_trace()[traced..];
    assert!(!trace.is_empty());
    assert!(
        trace.iter().all(|request| matches!(request,
            coven_storage::test_utils::MemoryRequest::Status(path) if path.slot_number().is_some())),
        "{trace:?}"
    );
    assert_eq!(ben.database().applied_slot().await.unwrap(), behind);
    assert!(rows(ben.database()).await.is_empty());
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// A missing slot before a slot known to exist is damage, not the end: the
/// device applies the slots before it, blocks on the missing slot naming its
/// path, and stops sending and reading. A device knows a later slot exists
/// once it has read a batch of slots and located the end (§6.1).
#[tokio::test]
async fn a_missing_slot_before_a_known_later_one_is_damage_named_by_its_path() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let through = ben.database().applied_slot().await.unwrap();
    for i in 0..40u32 {
        let id: &'static str = Box::leak(i.to_string().into_boxed_str());
        insert(ana.database(), id, "written").await;
        ana.sync.pass().await.unwrap();
    }
    // After the first 32 slots the end search probes 1, 2, 4 and 8 slots past
    // the last: the eighth is known to exist, and the third, the gap, is
    // never probed.
    let gap = through + 35;
    let missing = ObjectPath::slot(group.store, gap.try_into().unwrap());
    group.storage.delete(&missing).await.unwrap();

    let result = ben.sync.pass().await;
    let Err(SyncError::Damaged(damaged)) = &result else {
        panic!("a gap before a known slot is damage: {result:?}");
    };
    assert_eq!(damaged.path, missing.as_str());
    assert_eq!(ben.database().applied_slot().await.unwrap(), gap - 1);
    assert_eq!(rows(ben.database()).await.len() as u64, gap - 1 - through);
    let SequenceStatus::Blocked(record) = ben.sync.state.subscribe().borrow().clone() else {
        panic!("the missing slot blocks the device");
    };
    assert!(matches!(&record.subject, PendingSubject::Slot(slot) if slot.0 == gap), "{record:?}");
    insert(ben.database(), "local", "kept").await;
    let count = ben.storage.request_count();
    assert!(ben.sync.pass().await.is_err());
    assert_eq!(ben.storage.request_count(), count);
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// No slot commits before its complete signature verifies: a slot whose last
/// byte is damaged applies none of its writes, and the device blocks on it
/// (§6.5).
#[tokio::test]
async fn a_slot_damaged_in_its_last_byte_commits_nothing() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let through = ben.database().applied_slot().await.unwrap();
    insert(ana.database(), "a", "one").await;
    insert(ana.database(), "b", "two").await;
    ana.sync.pass().await.unwrap();
    let slot = ana.database().applied_slot().await.unwrap();
    assert_eq!(slot, through + 1);
    let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
    let size = group.storage.read(&path).await.unwrap().len();
    group.storage.corrupt_byte(&path, size - 1).await.unwrap();

    let result = ben.sync.pass().await;
    assert!(matches!(result, Err(SyncError::Damaged(_))), "{result:?}");
    assert!(matches!(&*ben.sync.state.subscribe().borrow(), SequenceStatus::Blocked(_)));
    assert_eq!(ben.database().applied_slot().await.unwrap(), through);
    assert!(rows(ben.database()).await.is_empty());
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// The requests a pass adds for each kind of work: a claim is its create and
/// a floor read after it lands; losing a race adds the occupied slot's read
/// and a further attempt; a snapshot is its upload, its announcement claim,
/// the floor write naming it and the next pass's one read of that floor;
/// deleting history is the floor's upload and one delete per slot below its
/// new `first` (§21.1).
#[tokio::test]
async fn each_kind_of_work_adds_the_requests_the_cost_table_names() {
    use coven_storage::test_utils::MemoryRequest as Request;
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let floor = ObjectPath::floor(group.store);
    let slot = |number: u64| ObjectPath::slot(group.store, number.try_into().unwrap());
    let since = |traced: usize| ana.storage.request_trace().split_off(traced);

    // A claim: its create, and a floor read after it lands, beside the quiet
    // pass's two reads.
    let next = ana.database().applied_slot().await.unwrap() + 1;
    insert(ana.database(), "a", "one").await;
    let traced = ana.storage.request_trace().len();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        since(traced),
        [
            Request::Read(slot(next), None),
            Request::Read(floor.clone(), None),
            Request::Create(slot(next)),
            Request::Read(floor.clone(), None),
        ]
    );

    // Losing a race: Ben takes the slot Ana's create was about to take. Ana
    // reads the occupied slot, then makes a further attempt.
    let next = ana.database().applied_slot().await.unwrap() + 1;
    insert(ana.database(), "b", "two").await;
    insert(ben.database(), "c", "three").await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let traced = ana.storage.request_trace().len();
    let racing = async { ana.sync.pass().await.unwrap() };
    let winning = async {
        observed.await.unwrap();
        ben.sync.pass().await.unwrap();
        resume.send(()).unwrap();
    };
    tokio::join!(racing, winning);
    let trace = since(traced);
    let creates: Vec<_> = trace
        .iter()
        .filter_map(|request| match request {
            Request::Create(path) => Some(path.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(creates, [slot(next), slot(next + 1)], "{trace:?}");
    let occupied = trace.iter().position(|request| *request == Request::Create(slot(next)));
    let reread = trace[occupied.unwrap() + 1..]
        .iter()
        .position(|request| *request == Request::Read(slot(next), None));
    assert!(reread.is_some(), "the occupied slot is read after the lost create: {trace:?}");

    // A snapshot: its upload and its announcement claim.
    let traced = ana.storage.request_trace().len();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let creates: Vec<_> = since(traced)
        .into_iter()
        .filter_map(|request| match request {
            Request::Create(path) => Some(path),
            _ => None,
        })
        .collect();
    assert_eq!(creates.len(), 2);
    assert!(creates[0].snapshot_id().is_some() && creates[1].slot_number().is_some());

    // The floor write naming it, and the writer's next pass reading that
    // floor's bytes once.
    let traced = ana.storage.request_trace().len();
    ana.sync.pass().await.unwrap();
    let trace = since(traced);
    let count = |wanted: &Request| trace.iter().filter(|request| *request == wanted).count();
    assert_eq!(count(&Request::Read(floor.clone(), None)), 1, "{trace:?}");
    assert_eq!(count(&Request::Other("begin_upload")), 1, "{trace:?}");
    assert_eq!(count(&Request::Other("finish_upload")), 1, "{trace:?}");

    // Deleting history: once the snapshot passes grace, the floor upload and
    // one delete per slot below its new `first`.
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    let traced = ana.storage.request_trace().len();
    ana.sync.pass().await.unwrap();
    let trace = since(traced);
    let deleted: Vec<_> = trace
        .iter()
        .filter_map(|request| match request {
            Request::Delete(path) => path.slot_number().map(|slot| slot.0),
            _ => None,
        })
        .collect();
    ana.sync.pass().await.unwrap();
    let first = ana.sync.session.lock().await.floor.as_ref().unwrap().header.first.0;
    assert!(first > 1);
    assert_eq!(deleted, (1..first).collect::<Vec<_>>(), "{trace:?}");
    assert_eq!(
        trace.iter().filter(|request| **request == Request::Other("finish_upload")).count(),
        1,
        "{trace:?}"
    );
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}
