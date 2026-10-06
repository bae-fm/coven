use coven_database::{DropReason, EntryOutcome};
use coven_format::store_log::{MemberRole::Admin, StoreChange};
use coven_merge::Audience;

use crate::{
    effects::tests::{raise, snapshot},
    replay::tests::*,
};

fn two_circles() -> History {
    let mut h = household(Admin, Admin);
    for (id, name) in [(0, "Gifts"), (1, "Notes")] {
        h.all(0, 0, make(id, name));
        h.all(0, 0, join(id, 1));
        h.all(0, 0, join(id, 2));
    }
    h
}

#[test]
fn circle_removals_conflict_only_when_they_replace_the_same_audiences_key() {
    for second_circle in [0, 1] {
        let mut h = two_circles();
        h.push(0, 0, &(0..11).collect::<Vec<_>>(), leave(0, 1));
        h.push(
            2,
            2,
            &(0..11).collect::<Vec<_>>(),
            StoreChange::RemoveCircleMember {
                circle: circle(second_circle),
                member: member(2),
                key: key(99),
            },
        );
        h.every_order(|r| {
            assert_eq!(
                r.state.circles[&circle(0)].members,
                [member(0), member(2)].into()
            );
            assert_eq!(r.state.circles[&circle(0)].key, key(20));
            if second_circle == 0 {
                assert_eq!(h.drops(r), [12]);
                assert_eq!(
                    r.entries[&h.entries[12].position],
                    EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[11].position))
                );
            } else {
                assert_eq!(
                    r.state.circles[&circle(1)].members,
                    [member(0), member(1)].into()
                );
                assert_eq!(r.state.circles[&circle(1)].key, key(99));
                assert!(h.drops(r).is_empty());
            }
        });
    }
}

#[test]
fn store_removal_beats_a_later_concurrent_circle_rotation() {
    let mut h = two_circles();
    h.push(0, 0, &(0..11).collect::<Vec<_>>(), remove(2, &[0, 1]));
    h.push(2, 2, &(0..11).collect::<Vec<_>>(), leave(0, 1));
    h.every_order(|r| {
        assert!(r.state.members[&member(2)].removed);
        assert_eq!(
            r.state.circles[&circle(0)].members,
            [member(0), member(1)].into()
        );
        assert_eq!(r.state.circles[&circle(0)].key, key(200));
        assert_eq!(h.reports(r, 2), [12]);
    });
}

#[test]
fn repeated_removals_keep_both_entries_without_changing_the_key_again() {
    for store in [false, true] {
        let mut h = two_circles();
        let change = if store {
            remove(1, &[0, 1])
        } else {
            leave(0, 1)
        };
        h.push(0, 0, &(0..11).collect::<Vec<_>>(), change.clone());
        let mut repeated = change;
        match &mut repeated {
            StoreChange::RemoveMember {
                key: replacement, ..
            }
            | StoreChange::RemoveCircleMember {
                key: replacement, ..
            } => *replacement = key(99),
            _ => unreachable!(),
        }
        h.push(2, 2, &(0..11).collect::<Vec<_>>(), repeated);
        h.every_order(|r| {
            assert!(h.drops(r).is_empty());
            assert_eq!(
                r.state.store.as_ref().unwrap().key,
                key(if store { 101 } else { 1 })
            );
            assert_eq!(
                r.state.circles[&circle(0)].key,
                key(if store { 200 } else { 20 })
            );
        });
    }
}

#[test]
fn a_store_removal_does_not_replace_an_unlisted_circles_key() {
    let mut h = two_circles();
    h.all(0, 0, add(3, Admin));
    h.push(0, 0, &(0..12).collect::<Vec<_>>(), remove(3, &[]));
    h.push(2, 2, &(0..12).collect::<Vec<_>>(), leave(0, 1));
    h.every_order(|r| {
        assert!(r.state.members[&member(3)].removed);
        assert_eq!(
            r.state.circles[&circle(0)].members,
            [member(0), member(2)].into()
        );
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn concurrent_store_reset_and_version_raise_use_the_earlier_entry() {
    for format in [false, true] {
        for reset_first in [false, true] {
            for number in [30, 50] {
                let mut h = household(Admin, Admin).prefix(3);
                let reset = StoreChange::Reset {
                    snapshot: snapshot(number, Audience::Store),
                };
                let raise = raise(format, 2, 30);
                let changes = if reset_first {
                    [reset, raise]
                } else {
                    [raise, reset]
                };
                for (i, change) in changes.into_iter().enumerate() {
                    h.push(i as u8, i as u64, &[0, 1, 2], change);
                }
                h.every_order(|r| {
                    let version = if format {
                        r.state
                            .format
                            .as_ref()
                            .map(|v| (u32::from(v.number), v.snapshot.number))
                    } else {
                        r.state
                            .schema
                            .as_ref()
                            .map(|v| (v.number, v.snapshot.number))
                    };
                    assert_eq!(version, if reset_first { None } else { Some((2, 30)) });
                    assert_eq!(
                        r.state.resets.get(&Audience::Store).map(|s| s.number),
                        reset_first.then_some(number)
                    );
                    assert_eq!(h.reports(r, 1), [4]);
                    assert_eq!(
                        r.entries[&h.entries[4].position],
                        EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[3].position))
                    );
                });
            }
        }
    }
}

#[test]
fn a_store_raise_and_a_causal_store_reset_or_concurrent_circle_reset_both_apply() {
    for format in [false, true] {
        for reset_first in [false, true] {
            for audience in [Audience::Store, Audience::Circle(circle(0))] {
                let mut h = two_circles();
                let reset = StoreChange::Reset {
                    snapshot: snapshot(50, audience.clone()),
                };
                let raise = raise(format, 2, 30);
                let changes = if reset_first {
                    [reset, raise]
                } else {
                    [raise, reset]
                };
                for (i, change) in changes.into_iter().enumerate() {
                    let past = 11 + usize::from(i == 1 && audience == Audience::Store);
                    h.push(i as u8, i as u64, &(0..past).collect::<Vec<_>>(), change);
                }
                h.every_order(|r| {
                    let number = if format {
                        u32::from(r.state.format.as_ref().unwrap().number)
                    } else {
                        r.state.schema.as_ref().unwrap().number
                    };
                    assert_eq!(number, 2);
                    assert_eq!(r.state.resets[&audience].number, 50);
                    assert!(h.drops(r).is_empty());
                });
            }
        }
    }
}
