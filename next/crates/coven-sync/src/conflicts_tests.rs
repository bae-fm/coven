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
fn already_in_place_removals_bring_no_key_in() {
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
                key: replacement,
                circle_keys,
                ..
            } => {
                *replacement = key(99);
                for (index, replacement) in circle_keys.iter_mut().enumerate() {
                    replacement.key = key(300 + index as u64);
                }
            }
            StoreChange::RemoveCircleMember {
                key: replacement, ..
            } => *replacement = key(99),
            _ => unreachable!(),
        }
        h.push(2, 2, &(0..11).collect::<Vec<_>>(), repeated);
        h.every_order(|r| {
            assert!(h.drops(r).is_empty());
            for entry in &h.entries[11..] {
                assert_eq!(r.entries[&entry.position], EntryOutcome::Kept);
            }
            if store {
                assert_eq!(r.state.circles[&circle(1)].key, key(201));
            }
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
fn concurrent_audience_reset_and_version_raise_use_the_earlier_entry() {
    for audience in [Audience::Store, Audience::Circle(circle(0))] {
        for format in [false, true] {
            for reset_first in [false, true] {
                for number in [30, 50] {
                    let mut h = two_circles();
                    let base = h.entries.len();
                    let past: Vec<_> = (0..base).collect();
                    let reset = StoreChange::Reset {
                        snapshot: snapshot(number, audience.clone()),
                    };
                    let raise = raise(format, 2, 30, audience.clone());
                    let changes = if reset_first {
                        [reset, raise]
                    } else {
                        [raise, reset]
                    };
                    for (i, change) in changes.into_iter().enumerate() {
                        h.push(i as u8, i as u64, &past, change);
                    }
                    h.every_order(|r| {
                        let version = if format {
                            r.state
                                .format
                                .get(&audience)
                                .map(|v| (u32::from(v.number), v.snapshot.number))
                        } else {
                            r.state
                                .schema
                                .get(&audience)
                                .map(|v| (v.number, v.snapshot.number))
                        };
                        assert_eq!(version, if reset_first { None } else { Some((2, 30)) });
                        assert_eq!(
                            r.state.resets.get(&audience).map(|s| s.number),
                            reset_first.then_some(number)
                        );
                        assert_eq!(h.reports(r, 1), [base + 1]);
                        assert_eq!(
                            r.entries[&h.entries[base + 1].position],
                            EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[base].position))
                        );
                    });
                }
            }
        }
    }
}

#[test]
fn a_raise_and_a_causal_reset_or_reset_of_another_audience_both_apply() {
    for raised in [Audience::Store, Audience::Circle(circle(0))] {
        for format in [false, true] {
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
                    let raise = raise(format, 2, 30, raised.clone());
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
                        let number = if format {
                            u32::from(r.state.format.get(&raised).unwrap().number)
                        } else {
                            r.state.schema.get(&raised).unwrap().number
                        };
                        assert_eq!(number, 2);
                        assert_eq!(r.state.resets[&audience].number, 50);
                        assert!(h.drops(r).is_empty());
                    });
                }
            }
        }
    }
}

#[test]
fn deleting_a_circle_defeats_its_concurrent_raise() {
    use coven_format::store_log::MemberRole::Member;
    for format in [false, true] {
        for deletion in 0..3 {
            for deletion_first in [false, true] {
                let mut h = household(Member, Member).prefix(3);
                h.all(1, 1, make(0, "Ben’s notes"));
                let audience = Audience::Circle(circle(0));
                let remove = match deletion {
                    0 => (1, 4, delete(0)),
                    1 => (1, 4, leave(0, 1)),
                    2 => (0, 0, remove(1, &[])),
                    _ => unreachable!(),
                };
                let raising = (1, 1, raise(format, 2, 30, audience.clone()));
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
                    assert!(!r.state.format.contains_key(&audience));
                    assert_eq!(h.drops(r), [if deletion_first { 5 } else { 4 }]);
                });
            }
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
        let removal = (0, 0, remove(1, &[]));
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
