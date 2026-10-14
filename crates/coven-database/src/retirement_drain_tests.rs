use super::*;
use crate::pending::tests::Record;
use crate::store_log::tests::checked;
use crate::tests::TestStore;
use crate::write_landing::tests::retire;
use crate::{DeviceState, DropReason, EntryOutcome};
use coven_format::store_log::{StoreChange, StoreLogEntry};
use coven_format::value::EntryPositions;
use std::time::UNIX_EPOCH;

const DAY: u64 = 24 * 60 * 60;

fn at(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

async fn advance(db: &crate::Database, horizon: u64, sample: SystemTime) {
    db.advance_store_log_horizon(
        db.store_log().await.unwrap(),
        Duration::from_secs(horizon),
        sample,
    )
    .await
    .unwrap();
}

/// Drop `entry` in a replay that also applies an unrelated entry, as a replay
/// reversing a non-final retirement would: the device is active again.
async fn drop_retirement(db: &crate::Database, entry: EntryId, device: DeviceId) {
    let log = db.store_log().await.unwrap();
    let mut replay = log.replay;
    replay.entries.insert(
        entry,
        EntryOutcome::Dropped(DropReason::BeatenBy(EntryId {
            device: DeviceId(98),
            number: 1,
        })),
    );
    replay.state.devices.get_mut(&device).unwrap().state = DeviceState::Active;
    let other = StoreLogEntry {
        position: EntryId {
            device: DeviceId(98),
            number: 1,
        },
        timestamp: coven_merge::Timestamp::new(1, 0, DeviceId(98)).unwrap(),
        author: crate::store_log::tests::member(1),
        had_read: EntryPositions(Vec::new()),
        change: StoreChange::RenameCircle {
            circle: coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(1)),
            name: "renamed".into(),
        },
    };
    replay.entries.insert(other.position, EntryOutcome::Kept);
    db.apply_store_log::<Record>(checked(other), replay, Vec::new())
        .await
        .unwrap();
}

/// F is the sample before the observation whose horizon first made a kept
/// retirement final, never its publication time, and a later observation
/// never refreshes it (§6). Device removal, member removal and replacement
/// each retire their device.
#[tokio::test]
async fn the_first_final_observation_records_its_sample_as_f() {
    for kind in ["device", "member", "replacement"] {
        let store = TestStore::new();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        let writer = DeviceId(5);
        let entry = retire(&db, writer, 1, kind, 100).await;
        advance(&db, 100, at(200)).await;
        assert_eq!(db.retirement_finalities().await.unwrap(), vec![], "{kind}");
        advance(&db, 101, at(300)).await;
        let expected = vec![RetirementFinality {
            device: writer,
            entry,
            final_at: at(300),
        }];
        assert_eq!(
            db.retirement_finalities().await.unwrap(),
            expected,
            "{kind}"
        );
        advance(&db, 500, at(900)).await;
        assert_eq!(
            db.retirement_finalities().await.unwrap(),
            expected,
            "{kind}"
        );
        db.close().await.unwrap();
        let reopened = store.builder(vec![], vec![]).open().await.unwrap();
        assert_eq!(reopened.retirement_finalities().await.unwrap(), expected);
    }
}

/// The drain needs storage time strictly greater than F + one day + five
/// minutes; equality is not enough. A completed mark survives reopening and
/// leaves the waiting evidence list.
#[tokio::test]
async fn a_drain_needs_a_sample_strictly_past_the_wait() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let writer = DeviceId(5);
    retire(&db, writer, 1, "device", 100).await;
    advance(&db, 101, at(40 * DAY)).await;
    let finality = db.retirement_finalities().await.unwrap()[0];
    assert_eq!(finality.drain_due(), at(41 * DAY + 300));
    let deadline = finality.final_at + DRAIN_WAIT;
    assert!(!finality.drain_permitted(deadline));
    assert!(finality.drain_permitted(deadline + Duration::from_nanos(1)));
    assert!(!db.mark_drained(finality, deadline).await.unwrap());
    assert!(db.drained_devices().await.unwrap().is_empty());
    let stale = RetirementFinality {
        final_at: at(0),
        ..finality
    };
    assert!(!db.mark_drained(stale, at(50 * DAY)).await.unwrap());
    assert!(db
        .mark_drained(finality, deadline + Duration::from_secs(1))
        .await
        .unwrap());
    assert_eq!(
        db.drained_devices().await.unwrap(),
        BTreeSet::from([writer])
    );
    assert!(db.retirement_finalities().await.unwrap().is_empty());
    db.close().await.unwrap();
    let reopened = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(
        reopened.drained_devices().await.unwrap(),
        BTreeSet::from([writer])
    );
}

/// Replay removes the mark and its timing evidence in the transaction that
/// stops keeping the retiring entry; the device is then polled again.
#[tokio::test]
async fn dropping_the_retirement_forgets_its_drain_in_the_replay_transaction() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let writer = DeviceId(5);
    let entry = retire(&db, writer, 1, "device", 100).await;
    advance(&db, 101, at(40 * DAY)).await;
    let finality = db.retirement_finalities().await.unwrap()[0];
    assert!(db.mark_drained(finality, at(42 * DAY)).await.unwrap());
    drop_retirement(&db, entry, writer).await;
    assert!(db.drained_devices().await.unwrap().is_empty());
    assert!(db.retirement_finalities().await.unwrap().is_empty());
    assert!(!db.mark_drained(finality, at(43 * DAY)).await.unwrap());
}
