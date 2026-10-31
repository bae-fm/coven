use super::*;
use crate::test_utils::{notes_migrations, notes_tables};
use crate::tests::TestStore;
use coven_merge::Slot;

/// Write `number` of `device`, in `slot`, stamped at `milliseconds`, having
/// read through slot `through`.
fn write(device: u64, number: u64, slot: u64, through: u64, milliseconds: u64) -> AppliedWrite {
    AppliedWrite {
        id: WriteId {
            device: DeviceId(device),
            number,
        },
        slot: Slot(slot),
        timestamp: Timestamp::new(milliseconds, 0, DeviceId(device)).unwrap(),
        read_through: through,
    }
}

/// Stage `writes` as one snapshot's on a store's writer, then run `check`.
async fn staged<T>(
    writes: &[AppliedWrite],
    check: impl FnOnce(&DatabaseConnection, &SnapshotMetadata<'_>) -> T,
) -> T {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let checked = database.inspect_writer(|db| {
        SnapshotMetadata::create_table(db).unwrap();
        let metadata = SnapshotMetadata::staged(db, 1);
        for write in writes {
            metadata.stage(write).unwrap();
        }
        check(db, &metadata)
    });
    database.close().await.unwrap();
    checked
}

fn refused_as_causal(result: Result<(), DbError>) -> WriteId {
    match result {
        Err(DbError::Snapshot(SnapshotError::Format(coven_format::Error::Merge(
            MergeError::CausalTimestamp(id),
        )))) => id,
        other => panic!("expected a causal timestamp refusal, got {other:?}"),
    }
}

fn refused_as_uncovered(result: Result<(), DbError>) {
    match result {
        Err(DbError::Snapshot(SnapshotError::Inconsistent(
            "applied write is outside snapshot coverage",
        ))) => {}
        other => panic!("expected a coverage refusal, got {other:?}"),
    }
}

/// A write in slot 0, past the snapshot's coverage, or having read through
/// its own slot is refused.
#[tokio::test]
async fn a_write_outside_the_snapshots_coverage_is_refused() {
    for (slot, through) in [(0, 0), (3, 1), (2, 2)] {
        let writes = [write(1, 1, slot, through, 1_000), write(2, 1, 1, 0, 2_000)];
        refused_as_uncovered(staged(&writes, |_, metadata| metadata.validate(2)).await);
    }
}

/// Ben's write in slot 2 read Ana's in slot 1, yet is stamped before it.
#[tokio::test]
async fn a_write_stamped_before_one_it_read_is_refused() {
    let writes = [write(1, 1, 1, 0, 2_000), write(2, 1, 2, 1, 1_500)];
    let refused = refused_as_causal(staged(&writes, |_, metadata| metadata.validate(2)).await);
    assert_eq!(refused, writes[1].id);
}

/// Ana's second write read no slot, but follows her first, which is stamped
/// after it.
#[tokio::test]
async fn a_write_stamped_before_its_devices_earlier_write_is_refused() {
    let writes = [write(1, 1, 1, 0, 2_000), write(1, 2, 2, 0, 1_500)];
    let refused = refused_as_causal(staged(&writes, |_, metadata| metadata.validate(2)).await);
    assert_eq!(refused, writes[1].id);
}

/// Ben's write in slot 2 read no slot, so Ana's in slot 1 may be stamped
/// after it.
#[tokio::test]
async fn a_write_need_not_follow_an_earlier_slot_it_did_not_read() {
    let writes = [write(1, 1, 1, 0, 2_000), write(2, 1, 2, 0, 1_500)];
    staged(&writes, |_, metadata| metadata.validate(2))
        .await
        .unwrap();
}

/// Checking a snapshot's writes reads each write a bounded number of times,
/// however many there are. A library whose every upload wrote a slot holds
/// tens of thousands, and checking each against every other took most of a
/// restore.
#[tokio::test]
async fn checking_a_snapshots_writes_takes_steps_in_proportion_to_them() {
    const WRITES: u64 = 2_000;
    // Ten devices take turns, each write having read every slot before it.
    let writes: Vec<_> = (1..=WRITES)
        .map(|slot| write(slot % 10 + 1, slot / 10 + 1, slot, slot - 1, 1_000 + slot))
        .collect();
    let steps = staged(&writes, |db, metadata| {
        let profile = db.profile_statements();
        metadata.validate(WRITES).unwrap();
        drop(profile);
        // Each statement's count is its total so far, so its last one counts.
        let mut steps: BTreeMap<String, i32> = BTreeMap::new();
        for statement in db.profiled_statements() {
            steps.insert(statement.sql, statement.vm_steps);
        }
        steps.values().map(|steps| i64::from(*steps)).sum::<i64>()
    })
    .await;
    assert!(
        steps < 50 * WRITES as i64,
        "{steps} steps checking {WRITES} writes"
    );
}
