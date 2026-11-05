use crate::sequence_sync::tests::*;
use coven_storage::ObjectPath;

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
