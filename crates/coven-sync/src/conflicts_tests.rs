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
fn different_member_circle_removals_both_apply_in_shared_or_separate_audiences() {
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
            },
        );
        h.every_order(|r| {
            let remaining = if second_circle == 0 {
                [member(0)].into()
            } else {
                [member(0), member(2)].into()
            };
            assert_eq!(r.state.circles[&circle(0)].members, remaining);
            assert_eq!(r.state.circles[&circle(0)].key, key(10));
            if second_circle == 1 {
                assert_eq!(
                    r.state.circles[&circle(1)].members,
                    [member(0), member(1)].into()
                );
                assert_eq!(r.state.circles[&circle(1)].key, key(11));
            }
            assert!(h.drops(r).is_empty());
        });
    }
}

#[test]
fn store_removal_and_circle_removal_have_no_key_conflict() {
    let mut h = two_circles();
    h.push(0, 0, &(0..11).collect::<Vec<_>>(), remove(2));
    h.push(2, 2, &(0..11).collect::<Vec<_>>(), leave(0, 1));
    h.every_order(|r| {
        assert!(r.state.members[&member(2)].removed);
        assert_eq!(r.state.circles[&circle(0)].members, [member(0)].into());
        assert_eq!(r.state.circles[&circle(0)].key, key(10));
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn already_in_place_removals_bring_no_key_in() {
    for store in [false, true] {
        let mut h = two_circles();
        let change = if store { remove(1) } else { leave(0, 1) };
        h.push(0, 0, &(0..11).collect::<Vec<_>>(), change.clone());
        h.push(2, 2, &(0..11).collect::<Vec<_>>(), change);
        h.every_order(|r| {
            assert!(h.drops(r).is_empty());
            for entry in &h.entries[11..] {
                assert_eq!(r.entries[&entry.position], EntryOutcome::Kept);
            }
            if store {
                assert_eq!(r.state.circles[&circle(1)].key, key(11));
            }
            assert_eq!(r.state.store.as_ref().unwrap().key, key(1));
            assert_eq!(r.state.circles[&circle(0)].key, key(10));
        });
    }
}

#[test]
fn a_store_removal_does_not_replace_a_circles_key() {
    let mut h = two_circles();
    h.all(0, 0, add(3, Admin));
    h.push(0, 0, &(0..12).collect::<Vec<_>>(), remove(3));
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
fn concurrent_audience_reset_and_version_raise_use_the_earlier_entry() {
    for audience in [Audience::Store, Audience::Circle(circle(0))] {
        for reset_first in [false, true] {
            for number in [30, 50] {
                let mut h = two_circles();
                let base = h.entries.len();
                let past: Vec<_> = (0..base).collect();
                let reset = StoreChange::Reset {
                    snapshot: snapshot(number, audience.clone()),
                };
                let raise = raise(2, 30, audience.clone());
                let changes = if reset_first {
                    [reset, raise]
                } else {
                    [raise, reset]
                };
                for (i, change) in changes.into_iter().enumerate() {
                    h.push(i as u8, i as u64, &past, change);
                }
                h.every_order(|r| {
                    let version = r
                        .state
                        .schema
                        .get(&audience)
                        .map(|v| (v.number, v.snapshot.number));
                    assert_eq!(version, if reset_first { None } else { Some((2, 30)) });
                    assert_eq!(
                        r.state.resets.get(&audience).map(|s| s.number),
                        reset_first.then_some(number)
                    );
                    assert_eq!(h.drops(r), [base + 1]);
                    assert_eq!(
                        r.entries[&h.entries[base + 1].position],
                        EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[base].position))
                    );
                });
            }
        }
    }
}

#[test]
fn a_raise_and_a_causal_reset_or_reset_of_another_audience_both_apply() {
    for raised in [Audience::Store, Audience::Circle(circle(0))] {
        for reset_first in [false, true] {
            for audience in [
                Audience::Store,
                Audience::Circle(circle(0)),
                Audience::Circle(circle(1)),
            ] {
                let mut h = two_circles();
                let reset = StoreChange::Reset {
                    snapshot: snapshot(50, audience.clone()),
                };
                let raise = raise(2, 30, raised.clone());
                let changes = if reset_first {
                    [reset, raise]
                } else {
                    [raise, reset]
                };
                for (i, change) in changes.into_iter().enumerate() {
                    let past = 11 + usize::from(i == 1 && audience == raised);
                    h.push(i as u8, i as u64, &(0..past).collect::<Vec<_>>(), change);
                }
                h.every_order(|r| {
                    let number = r.state.schema.get(&raised).unwrap().number;
                    assert_eq!(number, 2);
                    assert_eq!(r.state.resets[&audience].number, 50);
                    assert!(h.drops(r).is_empty());
                });
            }
        }
    }
}

#[test]
fn deleting_a_circle_defeats_its_concurrent_raise() {
    use coven_format::store_log::MemberRole::Member;
    for deletion in 0..3 {
        for deletion_first in [false, true] {
            let mut h = household(Member, Member).prefix(3);
            h.all(1, 1, make(0, "Ben’s notes"));
            let audience = Audience::Circle(circle(0));
            let remove = match deletion {
                0 => (1, 4, delete(0)),
                1 => (1, 4, leave(0, 1)),
                2 => (0, 0, remove(1)),
                _ => unreachable!(),
            };
            let raising = (1, 1, raise(2, 30, audience.clone()));
            let entries = if deletion_first {
                [remove, raising]
            } else {
                [raising, remove]
            };
            for (author, device, change) in entries {
                h.push(author, device, &[0, 1, 2, 3], change);
            }
            h.every_order(|r| {
                assert!(r.state.circles[&circle(0)].deleted);
                assert!(!r.state.schema.contains_key(&audience));
                assert_eq!(h.drops(r), [if deletion_first { 5 } else { 4 }]);
            });
        }
    }
}

#[test]
fn concurrent_member_removal_defeats_access_replacement_in_every_arrival_order() {
    for removal_first in [false, true] {
        let mut h = household(Admin, Admin).prefix(3);
        let replacement = (
            1,
            1,
            StoreChange::SetAccess {
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "replacement".into(),
                },
            },
        );
        let removal = (0, 0, remove(1));
        for (author, device, change) in if removal_first {
            [removal, replacement]
        } else {
            [replacement, removal]
        } {
            h.push(author, device, &[0, 1, 2], change);
        }
        h.every_order(|r| {
            assert!(r.state.members[&member(1)].removed);
            assert_eq!(
                r.state.members[&member(1)].access,
                coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "fixture-access-key".into(),
                }
            );
            assert_eq!(h.drops(r), [if removal_first { 4 } else { 3 }]);
        });
    }
}

#[test]
fn access_entries_conflict_by_member_and_combine_only_equal_access() {
    for same_member in [false, true] {
        for same_access in [false, true] {
            let mut h = household(Admin, Admin);
            h.push(
                1,
                1,
                &[0, 1, 2, 3, 4],
                StoreChange::SetAccess {
                    access: coven_format::MemberAccess::ProviderAccount("first@example.com".into()),
                },
            );
            let second_member = if same_member { 1 } else { 2 };
            h.push(
                second_member,
                4,
                &[0, 1, 2, 3, 4],
                StoreChange::SetAccess {
                    access: coven_format::MemberAccess::ProviderAccount(if same_access {
                        "first@example.com".into()
                    } else {
                        "second@example.com".into()
                    }),
                },
            );
            h.every_order(|r| {
                assert_eq!(
                    h.drops(r),
                    if same_member && !same_access {
                        vec![6]
                    } else {
                        vec![]
                    }
                );
                assert_eq!(
                    r.state.members[&member(1)].access,
                    coven_format::MemberAccess::ProviderAccount("first@example.com".into())
                );
            });
        }
    }
}

pub(crate) fn independent_histories() -> Vec<History> {
    let access = StoreChange::SetAccess {
        access: coven_format::MemberAccess::ProviderAccount("ben@example.com".into()),
    };
    let remove_device = |id| StoreChange::RemoveDevice {
        device: coven_foundation::id_source::DeviceId(id),
    };
    let pairs = [
        [(1, device(8)), (1, device(9))],
        [(1, device(8)), (1, access.clone())],
        [
            (1, device(8)),
            (0, role(1, coven_format::store_log::MemberRole::Member)),
        ],
        [(1, device(8)), (0, join(0, 1))],
        [(1, device(8)), (0, remove_device(1))],
        [(0, remove_device(1)), (0, remove_device(4))],
        [(1, access.clone()), (0, join(0, 1))],
        [
            (1, access),
            (0, role(1, coven_format::store_log::MemberRole::Member)),
        ],
        [(0, join(0, 1)), (2, join(1, 1))],
        [(0, join(0, 1)), (2, leave(0, 2))],
        [(0, join(0, 1)), (2, leave(1, 2))],
        [(0, add(3, Admin)), (2, remove(1))],
        [(0, remove(1)), (2, remove(2))],
        [(0, leave(0, 0)), (2, leave(0, 2))],
    ];
    pairs
        .into_iter()
        .flat_map(|pair| {
            [false, true].map(|reverse| {
                let mut h = household(Admin, Admin);
                h.all(1, 1, device(4));
                for c in [0, 1] {
                    h.all(0, 0, make(c, "Shared"));
                    h.all(0, 0, join(c, 2));
                }
                let past: Vec<_> = (0..h.entries.len()).collect();
                let mut pair = pair.clone();
                if reverse {
                    pair.swap(0, 1);
                }
                for ((author, change), writer) in pair.into_iter().zip([20, 21]) {
                    h.push(author, writer, &past, change);
                }
                h
            })
        })
        .collect()
}

#[test]
fn independent_changes_commute_in_timestamp_and_arrival_order() {
    for pair in independent_histories().chunks_exact(2) {
        let states: Vec<_> = pair
            .iter()
            .map(|h| {
                let base = h.entries.len() - 2;
                let view = crate::replay(&h.entries[..base]).state;
                let a = &h.entries[base];
                let b = &h.entries[base + 1];
                let va = crate::effects::check(&view, a);
                let vb = crate::effects::check(&view, b);
                assert!(!super::conflict(a, &va, b, &vb));
                assert!(!super::conflict(b, &vb, a, &va));
                h.every_order(|r| {
                    assert!(h.drops(r).is_empty(), "{:?}", h.entries);
                })
                .state
            })
            .collect();
        assert_eq!(states[0], states[1]);
    }
}

#[test]
fn same_members_concurrent_device_additions_both_survive() {
    let mut h = household(Admin, Admin);
    h.push(1, 8, &[0, 1, 2, 3, 4], device(8));
    h.push(1, 9, &[0, 1, 2, 3, 4], device(9));
    h.every_order(|r| {
        assert!(h.drops(r).is_empty());
        for id in [8, 9] {
            let added = &r.state.devices[&coven_foundation::id_source::DeviceId(id)];
            assert_eq!(added.member, member(1));
            assert!(!added.removed);
        }
    });
}

pub(crate) fn contradictory_histories() -> Vec<(History, usize)> {
    let mut histories = Vec::new();
    for (addition, removal) in [
        ((0, add(1, Admin)), (2, remove(1))),
        (
            (1, device(1)),
            (
                0,
                StoreChange::RemoveDevice {
                    device: coven_foundation::id_source::DeviceId(1),
                },
            ),
        ),
        ((0, join(0, 1)), (2, leave(0, 1))),
    ] {
        for removal_first in [false, true] {
            let mut h = two_circles();
            h.all(0, 0, role(1, coven_format::store_log::MemberRole::Member));
            let base = h.entries.len();
            let past: Vec<_> = (0..base).collect();
            let pair = if removal_first {
                [removal.clone(), addition.clone()]
            } else {
                [addition.clone(), removal.clone()]
            };
            for ((author, change), writer) in pair.into_iter().zip([20, 21]) {
                h.push(author, writer, &past, change);
            }
            histories.push((h, base + usize::from(removal_first)));
        }
    }
    histories
}

#[test]
fn contradictory_membership_changes_keep_the_removal_in_either_timestamp_order() {
    for (h, loser) in contradictory_histories() {
        let winner = if loser == h.entries.len() - 1 {
            loser - 1
        } else {
            loser + 1
        };
        h.every_order(|r| {
            assert_eq!(h.drops(r), [loser]);
            assert_eq!(
                r.entries[&h.entries[loser].position],
                EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[winner].position))
            );
        });
    }
}

#[test]
fn removing_a_member_contradicts_only_changes_requiring_that_member() {
    let base = two_circles();
    let view = crate::replay(&base.entries).state;
    for (author, change, contradicts) in [
        (1, device(8), true),
        (0, role(1, Admin), true),
        (0, join(0, 1), true),
        (0, leave(0, 1), true),
        (
            0,
            StoreChange::RemoveDevice {
                device: coven_foundation::id_source::DeviceId(1),
            },
            true,
        ),
        (1, make(2, "Ben's notes"), true),
        (1, delete(0), true),
        (1, rename(0, "Renamed"), false),
        (1, raise(2, 30, Audience::Circle(circle(0))), false),
        (
            1,
            crate::effects::tests::rotation(Audience::Circle(circle(0)), 30),
            false,
        ),
        (1, remove(2), false),
    ] {
        let mut h = base.clone();
        let past: Vec<_> = (0..h.entries.len()).collect();
        h.push(0, 20, &past, remove(1));
        h.push(author, 21, &past, change);
        let a = &h.entries[past.len()];
        let b = &h.entries[past.len() + 1];
        let va = crate::effects::check(&view, a);
        let vb = crate::effects::check(&view, b);
        for (a, va, b, vb) in [(a, &va, b, &vb), (b, &vb, a, &va)] {
            assert_eq!(super::conflict(a, va, b, vb), contradicts, "{:?}", b.change);
        }
    }
}
