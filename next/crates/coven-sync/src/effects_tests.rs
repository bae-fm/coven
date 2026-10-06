use coven_format::store_log::{
    MemberRole::{Admin, Member},
    SnapshotId, StoreChange,
};
use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;

use crate::replay::tests::*;

pub(crate) fn snapshot(number: u64, audience: Audience) -> SnapshotId {
    SnapshotId {
        device: DeviceId(900),
        number,
        audience,
    }
}

pub(crate) fn raise(format: bool, version: u16, number: u64, audience: Audience) -> StoreChange {
    let snapshot = snapshot(number, audience);
    if format {
        StoreChange::RaiseFormat { version, snapshot }
    } else {
        StoreChange::RaiseSchema {
            version: u32::from(version),
            snapshot,
        }
    }
}

#[test]
fn equal_raises_combine_and_different_snapshots_use_the_earlier_entry() {
    for format in [false, true] {
        for second in [30, 40] {
            let mut h = household(Member, Member).prefix(3);
            h.push(0, 0, &[0, 1, 2], raise(format, 2, 30, Audience::Store));
            h.push(1, 1, &[0, 1, 2], raise(format, 2, second, Audience::Store));
            h.every_order(|r| {
                if format {
                    let version = r.state.format.get(&Audience::Store).unwrap();
                    assert_eq!(
                        (version.number, version.snapshot.number, version.entry),
                        (2, 30, h.entries[3].position)
                    );
                } else {
                    let version = r.state.schema.get(&Audience::Store).unwrap();
                    assert_eq!(
                        (version.number, version.snapshot.number, version.entry),
                        (2, 30, h.entries[3].position)
                    );
                }
                assert_eq!(h.reports(r, 1), if second == 30 { vec![] } else { vec![4] });
            });
            h.push(1, 1, &[0, 1, 2, 4], raise(format, 3, 50, Audience::Store));
            h.every_order(|r| {
                let (number, snapshot, entry) = if format {
                    let v = r.state.format.get(&Audience::Store).unwrap();
                    (u32::from(v.number), v.snapshot.number, v.entry)
                } else {
                    let v = r.state.schema.get(&Audience::Store).unwrap();
                    (v.number, v.snapshot.number, v.entry)
                };
                assert_eq!((number, snapshot, entry), (3, 50, h.entries[5].position));
                assert_eq!(h.drops(r), if second == 30 { vec![] } else { vec![4] });
            });
        }
    }
}

#[test]
fn store_and_circle_resets_choose_a_snapshot_and_later_resets_supersede_it() {
    for audience in [Audience::Store, Audience::Circle(circle(0))] {
        for same in [false, true] {
            let mut h = if audience == Audience::Store {
                household(Admin, Member)
            } else {
                gifts()
            };
            h.push(
                0,
                0,
                &[0, 1, 2, 3, 4],
                StoreChange::Reset {
                    snapshot: snapshot(50, audience.clone()),
                },
            );
            h.push(
                1,
                1,
                &[0, 1, 2, 3, 4],
                StoreChange::Reset {
                    snapshot: snapshot(if same { 50 } else { 60 }, audience.clone()),
                },
            );
            h.every_order(|r| {
                assert_eq!(r.state.resets[&audience].number, 50);
                assert_eq!(h.drops(r), if same { vec![] } else { vec![6] });
            });
            h.all(
                0,
                0,
                StoreChange::Reset {
                    snapshot: snapshot(70, audience.clone()),
                },
            );
            h.every_order(|r| {
                assert_eq!(r.state.resets[&audience].number, 70);
                assert_eq!(h.drops(r), if same { vec![] } else { vec![6] });
            });
        }
    }
}

#[test]
fn older_raises_are_kept_without_lowering_versions_and_members_cannot_reset_store() {
    let mut h = household(Member, Member).prefix(3);
    h.all(1, 1, raise(false, 3, 50, Audience::Store));
    h.all(1, 1, raise(false, 2, 30, Audience::Store));
    h.all(
        1,
        1,
        StoreChange::Reset {
            snapshot: snapshot(70, Audience::Store),
        },
    );
    h.every_order(|r| {
        assert_eq!(r.state.schema.get(&Audience::Store).unwrap().number, 3);
        assert_eq!(h.drops(r), [5]);
        assert!(r.state.resets.is_empty());
    });
}

#[test]
fn every_snapshotted_audience_selects_its_own_version_and_snapshot() {
    for format in [false, true] {
        let mut h = gifts();
        h.all(1, 1, make(1, "Notes"));
        let past: Vec<_> = (0..6).collect();
        h.push(0, 0, &past, raise(format, 9, 90, Audience::Store));
        h.push(
            1,
            1,
            &past,
            raise(format, 3, 30, Audience::Circle(circle(0))),
        );
        h.push(
            1,
            4,
            &past,
            raise(format, 3, 40, Audience::Circle(circle(1))),
        );
        h.every_order(|r| {
            for (audience, expected) in [
                (Audience::Store, (9, 90)),
                (Audience::Circle(circle(0)), (3, 30)),
                (Audience::Circle(circle(1)), (3, 40)),
            ] {
                let actual = if format {
                    let v = &r.state.format[&audience];
                    (u32::from(v.number), v.snapshot.number)
                } else {
                    let v = &r.state.schema[&audience];
                    (v.number, v.snapshot.number)
                };
                assert_eq!(actual, expected);
            }
            assert!(h.drops(r).is_empty());
        });
    }
}

#[test]
fn audience_raises_combine_equal_snapshots_and_choose_the_higher_version() {
    for audience in [Audience::Store, Audience::Circle(circle(0))] {
        for format in [false, true] {
            for (first, second, number, winner) in
                [(2, 2, 30, 5), (2, 2, 40, 5), (2, 3, 40, 6), (3, 2, 40, 5)]
            {
                let mut h = gifts();
                h.push(
                    0,
                    0,
                    &[0, 1, 2, 3, 4],
                    raise(format, first, 30, audience.clone()),
                );
                h.push(
                    1,
                    1,
                    &[0, 1, 2, 3, 4],
                    raise(format, second, number, audience.clone()),
                );
                h.every_order(|r| {
                    let (version, snapshot, entry) = if format {
                        let v = &r.state.format[&audience];
                        (u32::from(v.number), v.snapshot.number, v.entry)
                    } else {
                        let v = &r.state.schema[&audience];
                        (v.number, v.snapshot.number, v.entry)
                    };
                    assert_eq!(
                        (version, snapshot, entry),
                        (
                            u32::from(first.max(second)),
                            if winner == 5 { 30 } else { number },
                            h.entries[winner].position
                        )
                    );
                    assert_eq!(
                        h.drops(r),
                        if first == second && number != 30 {
                            vec![6]
                        } else {
                            vec![]
                        }
                    );
                });
            }
        }
    }
}

#[test]
fn circle_raise_authority_uses_membership_in_the_authors_view() {
    for format in [false, true] {
        let mut h = household(Member, Member).prefix(3);
        h.all(1, 1, make(0, "Ben’s notes"));
        let change = raise(format, 2, 30, Audience::Circle(circle(0)));
        h.all(1, 1, change.clone());
        h.all(0, 0, change); // An outside admin cannot repeat an already-kept raise.
        h.every_order(|r| {
            assert_eq!(
                r.entries[&h.entries[5].position],
                coven_database::EntryOutcome::Dropped(coven_database::DropReason::NotAllowed)
            );
        });
        let mut h = gifts();
        h.push(0, 0, &[0, 1, 2, 3, 4], leave(0, 1));
        h.push(
            1,
            1,
            &[0, 1, 2, 3, 4],
            raise(format, 2, 30, Audience::Circle(circle(0))),
        );
        h.every_order(|r| {
            assert!(!r.state.circles[&circle(0)].members.contains(&member(1)));
            assert!(h.drops(r).is_empty());
        });
    }
}
