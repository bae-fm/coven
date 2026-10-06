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

pub(crate) fn raise(format: bool, version: u16, number: u64) -> StoreChange {
    let snapshot = snapshot(number, Audience::Store);
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
            h.push(0, 0, &[0, 1, 2], raise(format, 2, 30));
            h.push(1, 1, &[0, 1, 2], raise(format, 2, second));
            h.every_order(|r| {
                if format {
                    let version = r.state.format.as_ref().unwrap();
                    assert_eq!(
                        (version.number, version.snapshot.number, version.entry),
                        (2, 30, h.entries[3].position)
                    );
                } else {
                    let version = r.state.schema.as_ref().unwrap();
                    assert_eq!(
                        (version.number, version.snapshot.number, version.entry),
                        (2, 30, h.entries[3].position)
                    );
                }
                assert_eq!(h.reports(r, 1), if second == 30 { vec![] } else { vec![4] });
            });
            h.push(1, 1, &[0, 1, 2, 4], raise(format, 3, 50));
            h.every_order(|r| {
                let (number, snapshot, entry) = if format {
                    let v = r.state.format.as_ref().unwrap();
                    (u32::from(v.number), v.snapshot.number, v.entry)
                } else {
                    let v = r.state.schema.as_ref().unwrap();
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
    h.all(1, 1, raise(false, 3, 50));
    h.all(1, 1, raise(false, 2, 30));
    h.all(
        1,
        1,
        StoreChange::Reset {
            snapshot: snapshot(70, Audience::Store),
        },
    );
    h.every_order(|r| {
        assert_eq!(r.state.schema.as_ref().unwrap().number, 3);
        assert_eq!(h.drops(r), [5]);
        assert!(r.state.resets.is_empty());
    });
}
