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

pub(crate) fn raise(version: u32, number: u64, audience: Audience) -> StoreChange {
    let snapshot = snapshot(number, audience);
    StoreChange::RaiseSchema { version, snapshot }
}

#[test]
fn equal_raises_combine_and_different_snapshots_use_the_earlier_entry() {
    for second in [30, 40] {
        let mut h = household(Member, Member).prefix(3);
        h.push(0, 0, &[0, 1, 2], raise(2, 30, Audience::Store));
        h.push(1, 1, &[0, 1, 2], raise(2, second, Audience::Store));
        h.every_order(|r| {
            let version = r.state.schema.get(&Audience::Store).unwrap();
            assert_eq!(
                (version.number, version.snapshot.number, version.entry),
                (2, 30, h.entries[3].position)
            );
            assert_eq!(h.reports(r, 1), if second == 30 { vec![] } else { vec![4] });
        });
        h.push(1, 1, &[0, 1, 2, 4], raise(3, 50, Audience::Store));
        h.every_order(|r| {
            let version = r.state.schema.get(&Audience::Store).unwrap();
            assert_eq!(
                (version.number, version.snapshot.number, version.entry),
                (3, 50, h.entries[5].position)
            );
            assert_eq!(h.drops(r), if second == 30 { vec![] } else { vec![4] });
        });
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
    h.all(1, 1, raise(3, 50, Audience::Store));
    h.all(1, 1, raise(2, 30, Audience::Store));
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
    let mut h = gifts();
    h.all(1, 1, make(1, "Notes"));
    let past: Vec<_> = (0..6).collect();
    h.push(0, 0, &past, raise(9, 90, Audience::Store));
    h.push(1, 1, &past, raise(3, 30, Audience::Circle(circle(0))));
    h.push(1, 4, &past, raise(3, 40, Audience::Circle(circle(1))));
    h.every_order(|r| {
        for (audience, expected) in [
            (Audience::Store, (9, 90)),
            (Audience::Circle(circle(0)), (3, 30)),
            (Audience::Circle(circle(1)), (3, 40)),
        ] {
            let version = &r.state.schema[&audience];
            assert_eq!((version.number, version.snapshot.number), expected);
        }
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn audience_raises_combine_equal_snapshots_and_choose_the_higher_version() {
    for audience in [Audience::Store, Audience::Circle(circle(0))] {
        for (first, second, number, winner) in
            [(2, 2, 30, 5), (2, 2, 40, 5), (2, 3, 40, 6), (3, 2, 40, 5)]
        {
            let mut h = gifts();
            h.push(0, 0, &[0, 1, 2, 3, 4], raise(first, 30, audience.clone()));
            h.push(
                1,
                1,
                &[0, 1, 2, 3, 4],
                raise(second, number, audience.clone()),
            );
            h.every_order(|r| {
                let version = &r.state.schema[&audience];
                assert_eq!(
                    (version.number, version.snapshot.number, version.entry),
                    (
                        first.max(second),
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

#[test]
fn circle_raise_authority_uses_membership_in_the_authors_view() {
    let mut h = household(Member, Member).prefix(3);
    h.all(1, 1, make(0, "Ben’s notes"));
    let change = raise(2, 30, Audience::Circle(circle(0)));
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
        raise(2, 30, Audience::Circle(circle(0))),
    );
    h.every_order(|r| {
        assert!(!r.state.circles[&circle(0)].members.contains(&member(1)));
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn access_updates_are_member_authored_and_causal_replacements_keep_the_latest() {
    let mut h = household(Member, Member).prefix(3);
    for name in ["first", "first", "second"] {
        h.all(
            1,
            1,
            StoreChange::SetAccess {
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: name.into(),
                },
            },
        );
    }
    h.all(
        3,
        3,
        StoreChange::SetAccess {
            access: coven_format::MemberAccess::ProviderAccount("outsider@example.com".into()),
        },
    );
    h.all(0, 0, remove(1, &[]));
    h.all(
        1,
        1,
        StoreChange::SetAccess {
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "after-removal".into(),
            },
        },
    );
    h.every_order(|r| {
        let target = &r.state.members[&member(1)];
        assert!(target.removed);
        assert_eq!(
            target.access,
            coven_format::MemberAccess::S3AccessKey {
                access_key_id: "second".into(),
            }
        );
        assert_eq!(h.drops(r), [6, 8]);
    });
}
