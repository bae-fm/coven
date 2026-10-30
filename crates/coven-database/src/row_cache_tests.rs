use super::*;

#[derive(Clone, Debug, PartialEq)]
struct Row(usize);

impl Weight for Row {
    fn weight(&self) -> usize {
        self.0
    }
}

/// Rows past the budget let go of the one least recently used, and a row
/// larger than the whole budget isn't kept.
#[test]
fn a_recent_cache_keeps_the_rows_last_used_within_its_budget() {
    let cache = RowCache::<u32, Row>::recent(3 * (100 + ENTRY));
    for key in 0..3 {
        cache.insert(key, Row(100));
    }
    assert_eq!(cache.get(&0), Some(Row(100)));
    cache.insert(3, Row(100));
    assert_eq!(cache.get(&1), None);
    for key in [0, 2, 3] {
        assert_eq!(cache.get(&key), Some(Row(100)));
    }
    cache.insert(4, Row(4 * (100 + ENTRY)));
    assert_eq!(cache.get(&4), None);
    assert_eq!(cache.get(&0), Some(Row(100)));
    // Putting a row again replaces it and counts its new weight alone: it
    // takes two rows' room, so the least recently used other one goes.
    cache.insert(0, Row(2 * 100 + ENTRY));
    assert_eq!(cache.get(&0), Some(Row(2 * 100 + ENTRY)));
    assert_eq!(cache.get(&2), None);
    assert_eq!(cache.get(&3), Some(Row(100)));
}

#[test]
fn an_unbounded_cache_keeps_every_row_and_an_off_one_none() {
    let all = RowCache::<u32, Row>::all();
    let off = RowCache::<u32, Row>::Off;
    for key in 0..100 {
        all.insert(key, Row(1 << 20));
        off.insert(key, Row(1));
    }
    assert!((0..100).all(|key| all.get(&key).is_some()));
    assert!((0..100).all(|key| off.get(&key).is_none()));
}
