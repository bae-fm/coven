use crate::sequence_sync::tests::*;
use crate::{PendingSubject, SequenceStatus, SyncError};
use coven_storage::{test_utils::Faults, ObjectPath, Storage, StorageFailure};

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

/// A gap below the floor's `at` is the gap's damage, not the floor's: Ben
/// reports the missing slot's path, applies the slot before it and stops
/// there (§6.1, `gap_report_wins`). Checked against the slots before the gap,
/// the honest floor names an announcement Ben can't reach.
#[tokio::test]
async fn a_gap_below_the_floor_s_at_reports_the_gap() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let applied = ben.database().applied_slot().await.unwrap();
    for id in ["a", "b", "c"] {
        insert(ana.database(), id, "written").await;
        ana.sync.pass().await.unwrap();
    }
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    ana.sync.pass().await.unwrap();
    let gap = applied + 2;
    let mut ring = ben.sync.keys.read().unwrap().unwrap();
    let floor = ben.sync.read_floor(None, &mut ring, &ben.identity).await.unwrap().unwrap();
    assert!(floor.header.first.0 <= applied + 1 && floor.header.at >= gap);
    let path = ObjectPath::slot(group.store, gap.try_into().unwrap());
    group.storage.delete(&path).await.unwrap();

    let result = ben.sync.pass().await;
    assert!(
        matches!(&result, Err(SyncError::Damaged(object)) if object.path == path.as_str()),
        "{result:?}"
    );
    let blocked = ben.sync.session.lock().await.blocked.clone();
    assert_eq!(
        blocked.map(|record| record.subject),
        Some(PendingSubject::Slot(coven_merge::Slot(gap)))
    );
    assert_eq!(ben.database().applied_slot().await.unwrap(), gap - 1);
    assert_eq!(rows(ben.database()).await, [("a".into(), "written".into())]);
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// A floor read that fails proves neither that there is no floor nor that the
/// missing slot may be skipped: the pass fails with the read's own failure,
/// applies nothing and reports no damage (§6.1).
#[tokio::test]
async fn a_failed_floor_read_while_the_next_slot_is_missing_skips_nothing() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let applied = ben.database().applied_slot().await.unwrap();
    for id in ["a", "b", "c"] {
        insert(ana.database(), id, "written").await;
        ana.sync.pass().await.unwrap();
    }
    let missing = ObjectPath::slot(group.store, (applied + 1).try_into().unwrap());
    group.storage.delete(&missing).await.unwrap();
    ben.storage
        .set_faults(Faults {
            read_failures: [(ObjectPath::floor(group.store), StorageFailure::PermissionDenied)]
                .into(),
            ..Faults::none()
        })
        .await;
    let result = ben.sync.pass().await;
    assert!(
        matches!(&result, Err(SyncError::Storage(failure)) if failure.failure() == StorageFailure::PermissionDenied),
        "{result:?}"
    );
    assert_eq!(ben.database().applied_slot().await.unwrap(), applied);
    assert!(rows(ben.database()).await.is_empty());
    assert!(ben.sync.session.lock().await.blocked.is_none(), "no slot is reported damaged");
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// Where the lost slot lies against the floor in a read-pass comparison.
#[derive(Clone, Copy, Debug)]
enum Lost {
    /// No snapshot, so no floor.
    NoFloor,
    /// At or below the floor's `at`.
    BelowAt,
    /// In slots written after the floor.
    AfterFloor,
}

/// Ben, caught up, misses or can't open slot `applied + offset` of Ana's
/// next slots. The real pass's applied slot and report against `readPass`;
/// false when the slot doesn't lie where `lost` says.
async fn compare_read_pass(writes: usize, offset: u64, lost: Lost, corrupt: bool) -> bool {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let applied = ben.database().applied_slot().await.unwrap();
    for id in &["a", "b", "c", "d"][..writes] {
        insert(ana.database(), id, "written").await;
        ana.sync.pass().await.unwrap();
    }
    if !matches!(lost, Lost::NoFloor) {
        ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
        ana.sync.pass().await.unwrap();
    }
    let mut ring = ben.sync.keys.read().unwrap().unwrap();
    let floor = ben.sync.read_floor(None, &mut ring, &ben.identity).await.unwrap();
    if matches!(lost, Lost::AfterFloor) {
        for id in ["g", "h"] {
            insert(ana.database(), id, "after the floor").await;
            ana.sync.pass().await.unwrap();
        }
    }
    let end = ana.database().applied_slot().await.unwrap();
    let stop = applied + offset;
    let fits = stop <= end
        && match (&floor, lost) {
            (None, Lost::NoFloor) => true,
            (Some(floor), Lost::BelowAt) => stop <= floor.header.at,
            (Some(floor), Lost::AfterFloor) => stop > floor.header.at,
            _ => panic!("{lost:?} with a floor present: {}", floor.is_some()),
        };
    if !fits {
        for device in [&ana, &ben] {
            device.database().close().await.unwrap();
        }
        return false;
    }
    let path = ObjectPath::slot(group.store, stop.try_into().unwrap());
    if corrupt {
        group.storage.corrupt_byte(&path, 0).await.unwrap();
    } else {
        group.storage.delete(&path).await.unwrap();
    }
    let floor = floor.map_or(serde_json::Value::Null, |floor| {
        let named: Vec<u64> = floor.announcements.iter().map(|original| original.slot.0).collect();
        serde_json::json!({"first":floor.header.first.0,"at":floor.header.at,"named":named})
    });
    let input = serde_json::json!({"kind":"readPass","applied":applied,"stop":stop,
        "why":if corrupt {"fault"} else {"missing"},"known":stop - 1,"floor":floor});
    let expected = crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input);

    let result = ben.sync.pass().await;
    let through = ben.database().applied_slot().await.unwrap();
    let blocked = ben.sync.session.lock().await.blocked.clone();
    let actual = match (&result, blocked) {
        (Ok(_), None) => serde_json::json!({"synced":through}),
        (Err(SyncError::Damaged(object)), Some(record)) => {
            let PendingSubject::Slot(slot) = record.subject else {
                panic!("blocked on {:?}; {input}", record.subject)
            };
            assert_eq!(object.path, path.as_str(), "{input}");
            assert_eq!(through, slot.0 - 1, "{input}");
            serde_json::json!({"stops":slot.0})
        }
        (Err(SyncError::Damaged(object)), None)
            if object.path == ObjectPath::floor(group.store).as_str() =>
        {
            serde_json::json!("floorDamaged")
        }
        (result, blocked) => panic!("{result:?} {blocked:?}; {input}"),
    };
    assert_eq!(actual, expected, "{input}");
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
    true
}

/// Ana's slots past Ben's, with one lost or damaged, a floor whose `at` is
/// past it, one written before it, or none (§6.1, `ReadPass.pass`).
#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_read_pass_matches_lean() {
    let mut cases = 0;
    for writes in 1..=4usize {
        for offset in 1..=writes as u64 + 3 {
            for lost in [Lost::NoFloor, Lost::BelowAt, Lost::AfterFloor] {
                for corrupt in [false, true] {
                    cases += usize::from(compare_read_pass(writes, offset, lost, corrupt).await);
                }
            }
        }
    }
    eprintln!("read pass: {cases} cases");
}

/// The real end search over slots `1..=20` with one missing, from each
/// start through 12, against `endSearch` (§6.1, `ReadPass.locateEnd`).
#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_end_search_matches_lean() {
    let group = Group::new();
    let ana = group.device(1).await;
    let path = |slot: u64| ObjectPath::slot(group.store, slot.try_into().unwrap());
    for slot in 1..=20 {
        group.storage.plant(&path(slot), b"slot").await.unwrap();
    }
    for lost in 0..=21u64 {
        if (1..=20).contains(&lost) {
            group.storage.delete(&path(lost)).await.unwrap();
        }
        let present: Vec<u64> = (1..=20).filter(|slot| *slot != lost).collect();
        for start in 0..=12 {
            let input = serde_json::json!({"kind":"endSearch","start":start,"present":present});
            let expected = crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input);
            let found = ana.sync.locate_end(start).await.unwrap();
            assert_eq!(serde_json::json!(found), expected, "{input}");
        }
        if (1..=20).contains(&lost) {
            group.storage.plant(&path(lost), b"slot").await.unwrap();
        }
    }
    ana.database().close().await.unwrap();
}

/// A claim of Ana's lost under a floor of Ben's, and her pass that settles it.
struct SettledUnderFloor {
    group: Group,
    ana: Device,
    ben: Device,
    /// The slot Ana had applied when she claimed the next one.
    applied: u64,
    /// Ana's pass.
    result: Result<crate::SequenceStatus, SyncError>,
    /// Whether the lost slot is at or below the floor's `at`.
    fits: bool,
    /// The floor check's `floorCheck` input.
    input: serde_json::Value,
}

impl SettledUnderFloor {
    async fn close(self) {
        for device in [&self.ana, &self.ben] {
            device.sync.database.close().await.unwrap();
        }
    }
}

/// Ana's create of the slot after hers is held while Ben's snapshot takes
/// it. Ben writes `later` slots after it and writes a floor; with `compacted`,
/// he writes another, snapshots again and writes a floor whose first kept slot
/// has passed Ana's claim. With `lost`, the slot `offset` after Ana's claim
/// slot is then gone, when it is at or below the floor's `at`. Ana's pass then
/// settles her claim against that floor.
async fn settle_under_floor(later: usize, compacted: bool, lost: Option<u64>) -> SettledUnderFloor {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let applied = ana.sync.database.applied_slot().await.unwrap();
    assert_eq!(ben.sync.database.applied_slot().await.unwrap(), applied);
    insert(&ana.sync.database, "ana", "waiting").await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let grace = crate::sequence_sync::sequence_retention::GRACE;
    let second = std::time::Duration::from_secs(1);
    let retention = async {
        observed.await.unwrap();
        ben.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
        for id in &["later-0", "later-1"][..later] {
            insert(&ben.sync.database, id, "after the snapshot").await;
            ben.sync.pass().await.unwrap();
        }
        group.clock.set(std::time::UNIX_EPOCH + grace + second);
        ben.sync.pass().await.unwrap();
        if compacted {
            insert(&ben.sync.database, "past", "the claim").await;
            ben.sync.pass().await.unwrap();
            ben.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
            group.clock.set(std::time::UNIX_EPOCH + grace * 2 + second * 3);
            ben.sync.pass().await.unwrap();
        }
        let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ben).await;
        let covers = ben
            .sync
            .session
            .lock()
            .await
            .history
            .announcements
            .get(&floor.announcements[0].slot)
            .expect("floor's store announcement")
            .covers;
        assert_eq!(floor.header.first.0 > applied + 1, compacted, "{:?}", floor.header);
        let lost = lost.map(|offset| applied + 1 + offset);
        let fits = lost.is_none_or(|slot| slot <= floor.header.at);
        if let Some(slot) = lost.filter(|_| fits) {
            group
                .storage
                .delete(&ObjectPath::slot(group.store, slot.try_into().unwrap()))
                .await
                .unwrap();
        }
        let mut missing = Vec::new();
        for slot in 1..=floor.header.at {
            let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
            if group.storage.status(&path).await.unwrap().is_none() {
                missing.push(slot);
            }
        }
        resume.send(()).unwrap();
        let named: Vec<u64> = floor.announcements.iter().map(|original| original.slot.0).collect();
        let input = serde_json::json!({"kind":"floorCheck","applied":applied,"covers":covers,
            "floor":{"first":floor.header.first.0,"at":floor.header.at,"named":named},
            "missing":missing});
        (fits, input)
    };
    let (result, (fits, input)) = tokio::join!(ana.sync.pass(), retention);
    SettledUnderFloor { group, ana, ben, applied, result, fits, input }
}

/// Ana's settle under Ben's floor, as `settle_under_floor` sets it up: what
/// it reports against `floorCheck`, returning that report; none when the lost
/// slot is past the floor's `at`.
async fn compare_settle_floor(
    later: usize,
    compacted: bool,
    lost: Option<u64>,
) -> Option<serde_json::Value> {
    let settled = settle_under_floor(later, compacted, lost).await;
    let SettledUnderFloor { group, ana, applied, result, fits, input, .. } = &settled;
    let report = fits.then(|| crate::tests::run_lean("COVEN_SEQUENCE_LEAN", input));
    if let Some(expected) = &report {
        // Once the floor checks, the claim settles and the pass moves on, to
        // a later slot's claim or to work that can meet the lost slot; only
        // a claim still unsettled at its slot shows the check's own failure.
        let contested = ObjectPath::slot(group.store, (*applied + 1).try_into().unwrap());
        let unsettled = ana.sync.database.current_claim().await.unwrap().is_some_and(|claim| {
            claim.attempts.last() == Some(&contested)
                && claim.settlement == coven_database::ClaimSettlement::Unknown
        });
        let actual = match &result {
            _ if !unsettled => serde_json::json!("accepted"),
            Ok(_) => panic!("an unsettled claim after a successful pass; {input}"),
            Err(SyncError::Damaged(object))
                if object.path == ObjectPath::floor(group.store).as_str() =>
            {
                serde_json::json!("floorDamaged")
            }
            Err(SyncError::Damaged(object)) => {
                let slot = (1..)
                    .find(|slot: &u64| {
                        object.path
                            == ObjectPath::slot(group.store, (*slot).try_into().unwrap()).as_str()
                    })
                    .expect("a slot's path");
                serde_json::json!({"stops":slot})
            }
            Err(error) => panic!("{error:?}; {input}"),
        };
        assert_eq!(&actual, expected, "{input}");
    }
    settled.close().await;
    report
}

/// Ana's claim is lost under a floor of Ben's whose first kept slot has passed
/// it, and slot 9 is then lost from storage: kept, below the slots Ana applies,
/// and the slot of an announcement her history holds. Settling reads past it
/// from the floor's store snapshot; retention's floor proposal reads the
/// announcement's publication time, finds the slot missing and reports it as
/// damaged, as a read of a kept slot does (§6.1, §16.6).
#[tokio::test]
async fn a_floor_proposal_reports_a_missing_kept_announcement_slot_damaged() {
    let settled = settle_under_floor(0, true, Some(3)).await;
    assert!(settled.fits);
    let lost = ObjectPath::slot(settled.group.store, (settled.applied + 4).try_into().unwrap());
    assert!(
        matches!(&settled.result, Err(SyncError::Damaged(object)) if object.path == lost.as_str()),
        "{:?}; {}",
        settled.result,
        settled.input
    );
    settled.close().await;
}

/// A claim lost to Ben's snapshot, under floors whose `at` is past the slots
/// Ana has applied, whose first kept slot has passed her claim or not, with
/// one slot after her claim gone or none (§5.2, §5.3, `FloorCheck.check`).
#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_settle_floor_matches_lean() {
    let mut reports = std::collections::BTreeMap::new();
    for later in 0..=2 {
        for compacted in [false, true] {
            for lost in [None, Some(1), Some(2), Some(3), Some(4), Some(5)] {
                if let Some(report) = compare_settle_floor(later, compacted, lost).await {
                    let kind = if report.get("stops").is_some() { "stops" } else { "accepted" };
                    *reports.entry((compacted, kind)).or_insert(0) += 1;
                }
            }
        }
    }
    eprintln!("settle floor: {reports:?}");
    // Each floor, compacting Ana's claim slot or not, is accepted in some
    // cases and stopped at a lost slot in others.
    for compacted in [false, true] {
        for kind in ["accepted", "stops"] {
            assert!(reports.contains_key(&(compacted, kind)), "{reports:?}");
        }
    }
}

/// Ben follows his notes with a live query. Ana writes a note and passes;
/// Ben's pass applies her slot, and his query reports the new result (E4).
#[tokio::test]
async fn a_live_query_reruns_as_a_pass_applies_another_devices_slot() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let mut notes = coven_database::test_utils::Followed::new(ben.database().subscribe(|sql| {
        Ok::<_, coven_database::DbError>(sql.query(
            "SELECT id FROM notes ORDER BY id",
            [],
            |row| row.get::<_, String>(0),
        )?)
    }));
    assert!(notes.next().await.is_empty());
    insert(ana.database(), "from-ana", "written on another device").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(notes.next().await, ["from-ana"]);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
