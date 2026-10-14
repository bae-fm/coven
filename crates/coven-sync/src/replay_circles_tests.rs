use crate::replay::tests::*;
use coven_database::{DropReason, EntryOutcome};
use coven_format::store_log::MemberRole;

// CurrentExamples.emptyCircleLog's prefix; entry indices match the proof.
pub(crate) fn circle_history() -> History {
    let mut h = History::new();
    h.all(0, 0, add(1, MemberRole::Member));
    h.all(0, 0, add(2, MemberRole::Member));
    h.all(0, 0, add(3, MemberRole::Admin));
    h.all(1, 1, device(1));
    h.all(0, 0, device(2));
    h.all(0, 0, make(0, "Gifts"));
    h.all(0, 0, join(0, 1));
    h
}

pub(crate) fn empty_circle() -> History {
    let mut h = circle_history();
    let past: Vec<_> = (0..8).collect();
    h.push(0, 0, &past, leave(0, 1));
    h.push(0, 2, &past, remove(0));
    h
}

#[test]
fn collectively_empty_circle_names_the_latest_kept_removal_including_noops() {
    let mut h = empty_circle();
    for change in [leave(0, 1), remove(0)] {
        h.push(0, 10, &(0..8).collect::<Vec<_>>(), change);
        h.every_order(|r| {
            assert!(h.drops(r).is_empty());
            assert_eq!(
                r.state.circle_deletions.get(&circle(0)).copied(),
                Some(h.entries.last().unwrap().position)
            );
            assert!(r.state.circles[&circle(0)].members.is_empty());
        });
    }
    // A member absent in the recorded view contributes no removal cause.
    h.push(0, 11, &(0..8).collect::<Vec<_>>(), leave(0, 2));
    h.every_order(|r| {
        assert!(h.drops(r).is_empty());
        assert_eq!(
            r.state.circle_deletions.get(&circle(0)).copied(),
            Some(h.entries[11].position)
        );
    });
}

#[test]
fn completed_author_views_hide_collectively_empty_circles() {
    let mut h = empty_circle();
    h.all(1, 1, join(0, 2));
    h.every_order(|r| {
        assert_eq!(h.drops(r), [10]);
        assert_eq!(
            r.entries[&h.entries[10].position],
            EntryOutcome::Dropped(DropReason::NotAllowed)
        );
        assert_eq!(
            r.state.circle_deletions.get(&circle(0)).copied(),
            Some(h.entries[9].position)
        );
    });
}

#[test]
fn explicit_and_author_view_deletions_use_the_latest_kept_deleting_entry() {
    for store_removal in [false, true] {
        let mut h = circle_history().prefix(7);
        let past: Vec<_> = (0..7).collect();
        h.push(
            0,
            0,
            &past,
            if store_removal {
                remove(0)
            } else {
                leave(0, 0)
            },
        );
        h.push(0, 2, &past, delete(0));
        h.every_order(|r| {
            assert!(h.drops(r).is_empty());
            assert_eq!(
                r.state.circle_deletions.get(&circle(0)).copied(),
                Some(h.entries[8].position)
            );
        });
    }
}

#[test]
fn dropped_deletion_restores_a_circle_even_when_the_store_removal_has_a_later_stamp() {
    let mut h = gifts();
    let past: Vec<_> = (0..5).collect();
    h.push(1, 1, &past, delete(0));
    assert_eq!(
        replay(&h.entries)
            .state
            .circle_deletions
            .get(&circle(0))
            .copied(),
        Some(h.entries[5].position)
    );
    h.push(0, 0, &past, remove(1));
    h.every_order(|r| {
        assert_eq!(h.drops(r), [5]);
        assert_eq!(r.state.circle_deletions.get(&circle(0)).copied(), None);
        assert_eq!(r.state.circles[&circle(0)].members, [member(0)].into());
    });
}

pub(crate) fn dropped_creation() -> History {
    let mut h = circle_history().prefix(7);
    h.push(3, 3, &(0..6).collect::<Vec<_>>(), remove(0));
    h.push(0, 2, &(0..7).collect::<Vec<_>>(), delete(0));
    h
}

fn deletion_then_shared_removal() -> History {
    let mut h = gifts();
    h.push(0, 0, &[0, 1, 2, 3, 4], delete(0));
    h.push(1, 1, &[0, 1, 2, 3, 4], leave(0, 1));
    h
}

#[test]
fn a_later_shared_removal_does_not_replace_an_explicit_deletion_cause() {
    let h = deletion_then_shared_removal();
    h.every_order(|r| {
        assert!(h.drops(r).is_empty());
        assert_eq!(r.state.circle_deletions[&circle(0)], h.entries[5].position);
    });
}

fn deletion_then_dropped_removal() -> History {
    let mut h = History::new();
    h.all(0, 0, make(0, "Gifts"));
    h.push(0, 0, &[0, 1], leave(0, 0));
    h.push(0, 2, &[0, 1], remove(0));
    h
}

#[test]
fn a_later_dropped_removal_cannot_be_the_circle_deletion_cause() {
    let h = deletion_then_dropped_removal();
    h.every_order(|r| {
        assert_eq!(h.drops(r), [3]);
        assert_eq!(
            r.entries[&h.entries[3].position],
            EntryOutcome::Dropped(DropReason::NoAdminLeft)
        );
        assert_eq!(r.state.circle_deletions[&circle(0)], h.entries[2].position);
    });
}

#[test]
fn kept_deletion_records_its_cause_even_when_circle_creation_drops() {
    let h = dropped_creation();
    h.every_order(|r| {
        assert_eq!(h.drops(r), [6]);
        assert!(!r.state.circles.contains_key(&circle(0)));
        assert_eq!(
            r.state.circle_deletions.get(&circle(0)).copied(),
            Some(h.entries[8].position)
        );
    });
}

pub(crate) fn cause_histories() -> Vec<History> {
    let mut histories = vec![
        dropped_creation(),
        deletion_then_shared_removal(),
        deletion_then_dropped_removal(),
    ];
    let mut empty = empty_circle();
    histories.push(empty.clone());
    for (writer, change) in [(10, leave(0, 1)), (10, remove(0)), (11, leave(0, 2))] {
        empty.push(0, writer, &(0..8).collect::<Vec<_>>(), change);
        histories.push(empty.clone());
    }
    for store in [false, true] {
        let mut h = circle_history().prefix(7);
        h.push(
            0,
            0,
            &(0..7).collect::<Vec<_>>(),
            if store { remove(0) } else { leave(0, 0) },
        );
        h.push(0, 2, &(0..7).collect::<Vec<_>>(), delete(0));
        histories.push(h);
    }
    for removal_first in [false, true] {
        let mut h = gifts();
        let deletion = (1, 1, delete(0));
        let removal = (0, 0, remove(1));
        for (author, writer, change) in if removal_first {
            [removal, deletion]
        } else {
            [deletion, removal]
        } {
            h.push(author, writer, &(0..5).collect::<Vec<_>>(), change);
        }
        histories.push(h);
    }
    histories
}

#[tokio::test]
async fn circle_causes_and_author_view_classifications_survive_reopening() {
    use coven_database::StoreLog;
    use coven_foundation::{
        files::StoreLayout,
        id_source::{SequentialIds, StoreId},
    };
    for h in cause_histories() {
        let temporary = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(temporary.path().to_owned());
        let directory = layout
            .create_store_dir(
                StoreId(uuid::Uuid::from_u128(1)),
                "Home",
                &SequentialIds::new(),
            )
            .unwrap();
        let mut log = StoreLog::default();
        for entry in &h.entries {
            let database = crate::tests::open(directory.clone()).await;
            assert_eq!(database.store_log().await.unwrap(), log);
            let (checked, result) = crate::replay::tests::replay_entry(&log, entry.clone());
            database
                .apply_store_log::<crate::PendingRecord>(checked.clone(), result.clone())
                .await
                .unwrap();
            log.entries.push(checked);
            log.replay = result;
            assert_eq!(database.store_log().await.unwrap(), log);
            database.close().await.unwrap();
        }
        let database = crate::tests::open(directory).await;
        assert_eq!(database.store_log().await.unwrap(), log);
        assert_eq!(log.replay, replay(&h.entries));
        for entry in log.replay.state.circle_deletions.values() {
            assert_eq!(log.replay.entries[entry], EntryOutcome::Kept);
        }
        database.close().await.unwrap();
    }
}

#[test]
fn concurrent_addition_refills_a_collectively_emptied_circle() {
    let mut h = circle_history();
    let past: Vec<_> = (0..8).collect();
    h.push(0, 0, &past, leave(0, 1));
    h.push(0, 2, &past, remove(0));
    h.push(1, 1, &past, join(0, 2));
    h.every_order(|r| {
        assert!(h.drops(r).is_empty());
        assert_eq!(r.state.circles[&circle(0)].members, [member(2)].into());
        assert!(!r.state.circle_deletions.contains_key(&circle(0)));
    });
}

#[test]
fn circle_deletion_outranks_an_earlier_shared_member_removal() {
    let mut h = gifts();
    h.push(0, 0, &[0, 1, 2, 3, 4], leave(0, 1));
    h.push(1, 1, &[0, 1, 2, 3, 4], delete(0));
    h.every_order(|r| {
        assert!(r.state.circle_deletions.contains_key(&circle(0)));
        assert_eq!(h.drops(r), [5]);
        assert_eq!(
            r.entries[&h.entries[5].position],
            EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[6].position))
        );
    });
}
