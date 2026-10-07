use super::*;
use crate::replay::{at, replay_entry, tests as history};
use coven_format::store_log::StoreLogEntry;
use coven_format::value::EntryPositions;
use std::{hint::black_box, time::Instant};

#[test]
fn historical_views_match_uncached_and_full_replay_in_each_arrival_order() {
    let mut h = history::gifts();
    h.push(0, 0, &[0, 1, 2, 3, 4], history::leave(0, 1));
    h.push(1, 1, &[0, 1, 2, 3, 4], history::delete(0));
    let views = [
        vec![],
        vec![0],
        vec![0, 1, 2, 3, 4],
        vec![0, 1, 2, 3, 4, 5],
        vec![0, 1, 2, 3, 4, 6],
        vec![0, 1, 2, 3, 4, 5, 6],
    ];
    for order in [[0, 1, 2, 3, 4, 5, 6], [0, 1, 2, 3, 4, 6, 5]] {
        let entries: Vec<_> = order.iter().map(|&i| h.entries[i].clone()).collect();
        let log = applied(&entries);
        let mut cache = ReplayCache::new(&log);
        for _ in 0..3 {
            for selected in &views {
                let entries: Vec<_> = selected.iter().map(|&i| h.entries[i].clone()).collect();
                let frontier = positions(&entries);
                let cached = cache.at(&frontier);
                assert_eq!(cached, &at(&log, &frontier));
                assert_eq!(cached, &crate::replay(&entries));
            }
        }
        assert_eq!(cache.views.len(), views.len());
        // The deletion is kept in its author's branch and dropped in the full
        // view. Reusing the latest replay or keying only by entry count is wrong.
        let deletion = positions(
            &h.entries[..5]
                .iter()
                .chain([&h.entries[6]])
                .cloned()
                .collect::<Vec<_>>(),
        );
        assert!(cache.at(&deletion).state.circles[&history::circle(0)].deleted);
        assert!(!cache.at(&log.positions()).state.circles[&history::circle(0)].deleted);
    }
}

#[test]
fn cache_reuses_the_logs_immutable_checks() {
    let mut h = history::gifts();
    h.all(0, 0, history::rename(0, "Birthdays"));
    let mut log = applied(&h.entries);
    // Give the projection a retained refusal: it must consume that check,
    // rather than derive a fresh permission from the selected entries.
    log.entries.last_mut().unwrap().check = coven_database::StoreLogCheck::NotAllowed;
    let frontier = log.positions();
    let mut cache = ReplayCache::new(&log);
    for _ in 0..2 {
        assert_eq!(cache.at(&frontier), &at(&log, &frontier));
        assert_eq!(
            cache.at(&frontier).state.circles[&history::circle(0)].name,
            "Gifts"
        );
    }
    assert_eq!(cache.views.len(), 1);
}

fn applied(entries: &[StoreLogEntry]) -> StoreLog {
    let mut log = StoreLog::default();
    for entry in entries {
        let (checked, replay) = replay_entry(&log, entry.clone());
        log.entries.push(checked);
        log.replay = replay;
    }
    log
}

fn positions(entries: &[StoreLogEntry]) -> EntryPositions {
    let mut frontier = BTreeMap::new();
    for entry in entries {
        let id = entry.position;
        frontier
            .entry(id.device)
            .and_modify(|number: &mut u64| *number = (*number).max(id.number))
            .or_insert(id.number);
    }
    EntryPositions(
        frontier
            .into_iter()
            .map(|(device, number)| EntryId { device, number })
            .collect(),
    )
}

#[test]
#[ignore = "release timing, run by scripts/check.sh"]
fn read_view_replay_cost() {
    let history = history::realistic_history();
    assert_eq!(history.entries.len(), 2_000);
    let log = applied(&history.entries);
    let views: Vec<_> = [400, 800, 1_200, 1_600, 2_000]
        .into_iter()
        .map(|length| positions(&history.entries[..length]))
        .collect();
    let writes: Vec<_> = (0..10_000)
        .map(|i| views[i % views.len()].clone())
        .collect();
    let started = Instant::now();
    for view in &writes {
        black_box(at(black_box(&log), black_box(view)));
    }
    let before = started.elapsed();
    let started = Instant::now();
    let mut cache = ReplayCache::new(black_box(&log));
    for view in &writes {
        black_box(cache.at(black_box(view)));
    }
    let after = started.elapsed();
    assert_eq!(cache.views.len(), views.len());
    for view in &writes {
        assert_eq!(cache.at(view), &at(&log, view));
    }
    println!(
        "2,000 entries, 10,000 writes, 5 read views: per-write replay {before:?}; cached replay {after:?}; speedup {:.1}x",
        before.as_secs_f64() / after.as_secs_f64()
    );
}
