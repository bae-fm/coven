use super::*;

const DAY: u64 = 86400;

fn at(seconds: u64) -> StorageTime {
    StorageTime::new(seconds, 0).unwrap()
}

fn reach(number: u64) -> WritePositions {
    WritePositions(vec![WriteId {
        device: DeviceId(1),
        number,
    }])
}

fn write(number: u64) -> WriteId {
    WriteId {
        device: DeviceId(1),
        number,
    }
}

/// A later snapshot reaching further does not replace an earlier witness of
/// the writes both cover: superseding a snapshot keeps its age (§15; io
/// `maturity_survives_new_snapshots`).
#[test]
fn a_replacement_keeps_the_earlier_witness_of_what_both_cover() {
    let mut witnesses = SnapshotWitnesses::default();
    assert!(witnesses.record(&Audience::Store, &reach(3), at(100)));
    assert!(witnesses.record(&Audience::Store, &reach(5), at(200)));
    assert_eq!(witnesses.landed(&Audience::Store, write(2)), Some(at(100)));
    assert_eq!(witnesses.landed(&Audience::Store, write(3)), Some(at(100)));
    assert_eq!(witnesses.landed(&Audience::Store, write(4)), Some(at(200)));
    assert_eq!(witnesses.landed(&Audience::Store, write(6)), None);
    // An earlier landing reaching as far makes the later row redundant.
    assert!(witnesses.record(&Audience::Store, &reach(5), at(50)));
    assert_eq!(witnesses.landed(&Audience::Store, write(4)), Some(at(50)));
    assert_eq!(witnesses.0[&Audience::Store][&DeviceId(1)], [(5, at(50))]);
    // Seeing the same snapshot again changes nothing.
    assert!(!witnesses.record(&Audience::Store, &reach(5), at(50)));
    assert!(!witnesses.record(&Audience::Store, &reach(2), at(60)));
}

/// Coverage matures strictly more than one storage day after the landing:
/// at exactly a day it does not, one nanosecond later it does (§15).
#[test]
fn a_witness_matures_strictly_after_one_day() {
    let mut witnesses = SnapshotWitnesses::default();
    witnesses.record(&Audience::Store, &reach(1), at(100));
    assert!(!witnesses.mature(&Audience::Store, write(1), Some(at(100 + DAY))));
    assert!(witnesses.mature(
        &Audience::Store,
        write(1),
        Some(StorageTime::new(100 + DAY, 1).unwrap())
    ));
    assert!(!witnesses.mature(&Audience::Store, write(1), None));
    let circle = Audience::Circle(coven_foundation::id_source::CircleId(
        uuid::Uuid::from_u128(1),
    ));
    assert!(!witnesses.mature(&circle, write(1), Some(at(10 * DAY))));
}

/// Rows below a mature row add nothing once it is mature, so pruning keeps
/// every write's verdict while bounding the rows.
#[test]
fn pruning_below_a_mature_row_keeps_every_verdict() {
    let mut witnesses = SnapshotWitnesses::default();
    for (number, landed) in [(1, 0), (2, 10), (3, 20), (4, 2 * DAY)] {
        witnesses.record(&Audience::Store, &reach(number), at(landed));
    }
    let now = at(DAY + 15);
    let before: Vec<_> = (1..6)
        .map(|n| witnesses.mature(&Audience::Store, write(n), Some(now)))
        .collect();
    assert!(witnesses.prune(now));
    assert_eq!(
        witnesses.0[&Audience::Store][&DeviceId(1)],
        [(2, at(10)), (3, at(20)), (4, at(2 * DAY))]
    );
    let after: Vec<_> = (1..6)
        .map(|n| witnesses.mature(&Audience::Store, write(n), Some(now)))
        .collect();
    assert_eq!(before, after);
    assert!(!witnesses.prune(now));
}

/// The evidence is durable: a snapshot recorded once still witnesses after
/// reopening, though no listing names it any more.
#[tokio::test]
async fn witnesses_survive_reopening() {
    let store = crate::tests::TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.record_snapshot_witnesses(Audience::Store, vec![(reach(2), at(100))], None)
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let witnesses = db.snapshot_witnesses().await.unwrap();
    assert_eq!(witnesses.landed(&Audience::Store, write(1)), Some(at(100)));
    assert!(witnesses.mature(&Audience::Store, write(2), Some(at(DAY + 101))));
    // Unchanged evidence takes no writer.
    let work = crate::test_utils::WorkCounter::default();
    db.test_track_work(work.clone()).await.unwrap();
    db.record_snapshot_witnesses(Audience::Store, vec![(reach(2), at(100))], None)
        .await
        .unwrap();
    assert_eq!(work.take(), crate::test_utils::WorkCounts::default());
}
