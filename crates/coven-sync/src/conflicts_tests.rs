use coven_database::{DeviceState, DropReason, EntryOutcome, EntryTarget, StoreLogCheck};
use coven_format::store_log::{
    MemberPublicKeys,
    MemberRole::{Admin, Member},
    StoreChange,
};
use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;

use crate::{
    effects::tests::{add_with, raise, snapshot},
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
            if second_circle == 1 {
                assert_eq!(
                    r.state.circles[&circle(1)].members,
                    [member(0), member(1)].into()
                );
            }
            assert!(h.drops(r).is_empty());
        });
        assert_eq!(newest_key(&h, Audience::Circle(circle(0))), Some(key(10)));
        assert_eq!(newest_key(&h, Audience::Circle(circle(1))), Some(key(11)));
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
        assert!(h.drops(r).is_empty());
    });
    assert_eq!(newest_key(&h, Audience::Circle(circle(0))), Some(key(10)));
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
        });
        assert_eq!(newest_key(&h, Audience::Store), Some(key(1)));
        assert_eq!(newest_key(&h, Audience::Circle(circle(0))), Some(key(10)));
        assert_eq!(newest_key(&h, Audience::Circle(circle(1))), Some(key(11)));
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
                assert!(r.state.circle_deletions.contains_key(&circle(0)));
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
                member: member(1),
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "replacement".into(),
                    principal: coven_format::S3Principal::Console,
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
                    principal: coven_format::S3Principal::Console,
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
                    member: member(1),
                    access: coven_format::MemberAccess::ProviderAccount("first@example.com".into()),
                },
            );
            let second_member = if same_member { 1 } else { 2 };
            h.push(
                second_member,
                4,
                &[0, 1, 2, 3, 4],
                StoreChange::SetAccess {
                    member: member(second_member),
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
        member: member(1),
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
                h.all(1, 4, device(4));
                for c in [0, 1] {
                    h.all(0, 0, make(c, "Shared"));
                    h.all(0, 0, join(c, 2));
                }
                let past: Vec<_> = (0..h.entries.len()).collect();
                // A device addition registers its writer (§10), so it keeps
                // its own writer whichever change is stamped first.
                let mut pair: Vec<_> = pair.clone().into_iter().zip([30, 31]).collect();
                if reverse {
                    pair.swap(0, 1);
                }
                for (((author, change), own), writer) in pair.into_iter().zip([20, 21]) {
                    let writer = match change {
                        StoreChange::AddDevice { .. } => own,
                        _ => writer,
                    };
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
                let view = crate::replay::tests::replay(&h.entries[..base]).state;
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
            assert!(!added.removed());
        }
    });
}

pub(crate) fn contradictory_histories() -> Vec<(History, usize)> {
    let mut histories = Vec::new();
    for (addition, removal) in [
        // A re-addition repeats Ben's payload, his role now member (§9).
        (
            (0, add(1, coven_format::store_log::MemberRole::Member)),
            (2, remove(1)),
        ),
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
            for ((author, change), other) in pair.into_iter().zip([20, 21]) {
                // Ben's re-addition of his phone 1 is that device's own entry.
                let writer = match change {
                    StoreChange::AddDevice { .. } => 1,
                    _ => other,
                };
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
    let view = crate::replay::tests::replay(&base.entries).state;
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

#[test]
fn every_priority_boundary_uses_the_author_view_before_the_timestamp() {
    use coven_database::{CircleRemoval, StoreLogCheck};
    let shared = StoreLogCheck::MemberRemoval([(circle(0), CircleRemoval::Member)].into());
    let sole = StoreLogCheck::MemberRemoval([(circle(0), CircleRemoval::Circle)].into());
    let allowed = StoreLogCheck::Allowed;
    let cases = [
        (remove(1), shared.clone(), 0),
        (
            StoreChange::RemoveDevice {
                device: coven_foundation::id_source::DeviceId(1),
            },
            StoreLogCheck::DeviceOwner(member(1)),
            0,
        ),
        (delete(0), allowed.clone(), 1),
        (leave(0, 0), sole, 1),
        (leave(0, 1), shared, 2),
        (join(0, 2), allowed.clone(), 3),
        (
            add(3, coven_format::store_log::MemberRole::Member),
            allowed.clone(),
            3,
        ),
        (
            role(1, coven_format::store_log::MemberRole::Member),
            allowed.clone(),
            3,
        ),
        (add(3, Admin), allowed.clone(), 4),
        (role(1, Admin), allowed, 4),
    ];
    for (a, va, ta) in &cases {
        for (b, vb, tb) in &cases {
            let mut h = gifts();
            h.push(0, 20, &[0, 1, 2, 3, 4], a.clone());
            h.push(0, 21, &[0, 1, 2, 3, 4], b.clone());
            assert_eq!(
                super::before(&h.entries[5], va, &h.entries[6], vb),
                ta <= tb
            );
            assert_eq!(super::before(&h.entries[6], vb, &h.entries[5], va), tb < ta);
        }
    }
}

/// An addition's meaning is its whole payload (`Keyed.Additions`). Re-adding
/// an active member must repeat its keys, role and access, so an admin cannot
/// set another member's access through it (`overwrite_sets_anothers_access`).
/// Two concurrent additions of one id combine only when all three are equal,
/// so no addition stays kept with an access that never applied
/// (`overwrite_keeps_unapplied_addition`).
#[test]
fn additions_of_one_id_combine_only_with_the_same_keys_role_and_access() {
    use coven_format::store_log::MemberRole::Member;
    let access = |id: &str| coven_format::MemberAccess::S3AccessKey {
        access_key_id: id.into(),
        principal: coven_format::S3Principal::Console,
    };
    let dan = |role, access_key: &str, keys| StoreChange::AddMember {
        member: member(3),
        keys,
        role,
        access: access(access_key),
    };
    for concurrent in [false, true] {
        for (role, access_key, pair, same) in [
            (Member, "dan", keys(3), true),
            (Admin, "dan", keys(3), false),
            (Member, "ana", keys(3), false),
            (Member, "dan", keys(4), false),
        ] {
            let mut h = household(Admin, Member);
            let past = [0, 1, 2, 3, 4];
            h.push(0, 0, &past, dan(Member, "dan", keys(3)));
            if concurrent {
                h.push(0, 5, &past, dan(role, access_key, pair));
            } else {
                h.all(0, 0, dan(role, access_key, pair));
            }
            h.every_order(|r| {
                assert_eq!(h.drops(r), if same { vec![] } else { vec![6] });
                if !same {
                    let reason = match r.entries[&h.entries[6].position] {
                        EntryOutcome::Dropped(ref reason) => reason.clone(),
                        EntryOutcome::Kept => unreachable!(),
                    };
                    if concurrent {
                        assert_eq!(reason, DropReason::BeatenBy(h.entries[5].position));
                    } else {
                        assert_eq!(reason, DropReason::NotAllowed);
                    }
                }
                let added = &r.state.members[&member(3)];
                assert_eq!(
                    (added.role, added.keys, &added.access),
                    (Member, keys(3), &access("dan"))
                );
            });
        }
    }
}

// Member-key replacements (§9, D6; Appendix C13 `Keyed`): authority in the
// recorded view, fresh pairs, the replacement conflicts and their winners,
// and the target required at the replay position.

/// Replace member `m`'s pair with fixture pair `k`.
pub(crate) fn rekey(m: u8, k: u8) -> StoreChange {
    StoreChange::ReplaceMemberKeys {
        member: member(m),
        keys: keys(k),
    }
}

fn outcome(h: &History, r: &coven_database::StoreLogReplay, index: usize) -> EntryOutcome {
    r.entries[&h.entries[index].position].clone()
}

fn beaten_by(h: &History, index: usize) -> EntryOutcome {
    EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[index].position))
}

/// Ben replaces his own pair: only the pair changes, and both pairs stay
/// bound (`replacement_preserves_membership`, `Keyed.effect`).
#[test]
fn a_replacement_changes_only_the_pair_and_binds_it_for_good() {
    let mut h = household(Member, Member);
    h.all(1, 1, make(0, "Gifts"));
    h.all(1, 1, rekey(1, 7));
    h.every_order(|r| {
        let ben = &r.state.members[&member(1)];
        assert_eq!((ben.keys, ben.role, ben.removed), (keys(7), Member, false));
        assert_eq!(r.state.devices[&DeviceId(1)].state, DeviceState::Active);
        assert!(r.state.circles[&circle(0)].members.contains(&member(1)));
        assert!(r.state.bound_keys.contains(&keys(1)));
        assert!(r.state.bound_keys.contains(&keys(7)));
        assert!(h.drops(r).is_empty());
    });
}

/// The author replaces their own keys or is an admin in the recorded view;
/// another member cannot, and a removed member is no target
/// (`replacement_authority`).
#[test]
fn only_the_member_or_an_admin_in_the_recorded_view_replaces_keys() {
    for (author, device, allowed) in [(2, 2, false), (0, 0, true), (1, 1, true)] {
        let mut h = household(Member, Member);
        h.all(author, device, rekey(1, 7));
        h.every_order(|r| {
            let expected = if allowed {
                (EntryOutcome::Kept, keys(7))
            } else {
                (EntryOutcome::Dropped(DropReason::NotAllowed), keys(1))
            };
            assert_eq!(
                (outcome(&h, r, 5), r.state.members[&member(1)].keys),
                expected
            );
        });
    }
    let mut h = household(Member, Member);
    h.all(0, 0, remove(1));
    h.all(0, 0, rekey(1, 7));
    h.every_order(|r| {
        assert_eq!(
            outcome(&h, r, 6),
            EntryOutcome::Dropped(DropReason::NotAllowed)
        );
        assert_eq!(r.state.members[&member(1)].keys, keys(1));
    });
}

/// Neither key may repeat a pair the view bound: the member's current pair,
/// another member's keys, or a pair an earlier replacement retired. Reusing
/// a leaked credential would revoke nothing (`Keyed.freshKeys`).
#[test]
fn a_replacement_needs_keys_no_pair_has_bound() {
    use crate::effects::validate_change;
    let carol = keys(2);
    for pair in [
        keys(1),
        carol,
        MemberPublicKeys {
            signing: keys(7).signing,
            sealing: carol.sealing,
        },
        MemberPublicKeys {
            signing: carol.signing,
            sealing: keys(7).sealing,
        },
    ] {
        let mut h = household(Member, Member);
        let change = StoreChange::ReplaceMemberKeys {
            member: member(1),
            keys: pair,
        };
        assert_eq!(
            validate_change(&replay(&h.entries).state, &member(1), DeviceId(1), &change),
            Err(DropReason::KeysAlreadyUsed)
        );
        h.all(1, 1, change);
        h.every_order(|r| {
            assert_eq!(
                outcome(&h, r, 5),
                EntryOutcome::Dropped(DropReason::KeysAlreadyUsed)
            );
            assert_eq!(r.state.members[&member(1)].keys, keys(1));
        });
    }
    // Ben's retired pair can neither return to him nor go to a new id.
    let mut h = household(Member, Member);
    h.all(1, 1, rekey(1, 7));
    h.all(1, 1, rekey(1, 1));
    h.all(0, 0, add_with(3, keys(1)));
    h.every_order(|r| {
        let reused = EntryOutcome::Dropped(DropReason::KeysAlreadyUsed);
        assert_eq!(
            (outcome(&h, r, 6), outcome(&h, r, 7)),
            (reused.clone(), reused)
        );
        assert_eq!(r.state.members[&member(1)].keys, keys(7));
        assert!(!r.state.members.contains_key(&member(3)));
    });
}

/// Concurrent replacements of one id: another admin beats self-replacement,
/// classified by stable ids even when the member is an admin; otherwise the
/// earlier wins (`admin_preferred`, `same_kind_timestamp`).
#[test]
fn another_admins_replacement_beats_self_replacement_then_the_earlier_wins() {
    let all: Vec<usize> = (0..5).collect();
    for ben in [Member, Admin] {
        for self_first in [true, false] {
            let mut h = household(ben, Admin);
            if self_first {
                h.push(1, 1, &all, rekey(1, 7));
                h.push(0, 0, &all, rekey(1, 8));
            } else {
                h.push(0, 0, &all, rekey(1, 8));
                h.push(1, 1, &all, rekey(1, 7));
            }
            let (admin, own) = if self_first { (6, 5) } else { (5, 6) };
            h.every_order(|r| {
                assert_eq!(r.state.members[&member(1)].keys, keys(8));
                assert_eq!(outcome(&h, r, admin), EntryOutcome::Kept);
                assert_eq!(outcome(&h, r, own), beaten_by(&h, admin));
                // The losing pair was never bound by a kept entry.
                assert!(!r.state.bound_keys.contains(&keys(7)));
            });
        }
    }
    // Two admins replacing Ben: the earlier timestamp wins.
    let mut h = household(Member, Admin);
    h.push(2, 2, &all, rekey(1, 7));
    h.push(0, 0, &all, rekey(1, 8));
    h.every_order(|r| {
        assert_eq!(r.state.members[&member(1)].keys, keys(7));
        assert_eq!(outcome(&h, r, 6), beaten_by(&h, 5));
    });
}

/// Equal payloads are still two events and conflict; the winner may keep the
/// pair its defeated contender installed first (`Keyed.effect`,
/// `equal_payload_admin_wins`).
#[test]
fn equal_replacements_still_conflict_and_the_winner_keeps_the_pair() {
    let all: Vec<usize> = (0..5).collect();
    let mut h = household(Member, Admin);
    h.push(1, 1, &all, rekey(1, 7));
    h.push(0, 0, &all, rekey(1, 7));
    h.every_order(|r| {
        assert_eq!(r.state.members[&member(1)].keys, keys(7));
        assert_eq!(outcome(&h, r, 6), EntryOutcome::Kept);
        assert_eq!(outcome(&h, r, 5), beaten_by(&h, 6));
    });
    let mut h = household(Member, Admin);
    h.push(0, 0, &all, rekey(1, 7));
    h.push(2, 2, &all, rekey(1, 7));
    h.every_order(|r| {
        assert_eq!(outcome(&h, r, 6), beaten_by(&h, 5));
    });
}

/// Removing the member beats every replacement of their keys, earlier or
/// later; the pair stays the one the removal saw (`removal_beats_replacement`).
#[test]
fn removing_the_member_beats_every_replacement_of_their_keys() {
    let all: Vec<usize> = (0..5).collect();
    for (author, device) in [(1, 1), (2, 2)] {
        for replacement_first in [true, false] {
            let mut h = household(Member, Admin);
            if replacement_first {
                h.push(author, device, &all, rekey(1, 7));
                h.push(0, 0, &all, remove(1));
            } else {
                h.push(0, 0, &all, remove(1));
                h.push(author, device, &all, rekey(1, 7));
            }
            let (removal, replacement) = if replacement_first { (6, 5) } else { (5, 6) };
            // A later replacement finds no target at its replay position; an
            // earlier one loses the conflict.
            let lost = if replacement_first {
                beaten_by(&h, removal)
            } else {
                EntryOutcome::Dropped(DropReason::TargetGone(EntryTarget::Member(member(1))))
            };
            h.every_order(|r| {
                let ben = &r.state.members[&member(1)];
                assert_eq!((ben.keys, ben.removed), (keys(1), true));
                assert_eq!(outcome(&h, r, replacement), lost);
            });
        }
    }
}

/// A replacement conflicts with no role, access, device, circle or other
/// member's change, and replacements of different members both apply.
#[test]
fn replacements_conflict_with_nothing_else() {
    let mut h = household(Member, Admin);
    h.all(1, 1, make(0, "Gifts"));
    let all: Vec<usize> = (0..6).collect();
    h.push(1, 1, &all, rekey(1, 7));
    h.push(0, 0, &all, role(1, Admin));
    h.push(2, 2, &all, rekey(2, 8));
    h.push(1, 3, &all, device(3));
    h.push(
        0,
        4,
        &all,
        crate::effects::tests::rotation(coven_merge::Audience::Store, 50),
    );
    h.every_order(|r| {
        assert!(h.drops(r).is_empty());
        let ben = &r.state.members[&member(1)];
        assert_eq!((ben.keys, ben.role), (keys(7), Admin));
        assert_eq!(r.state.members[&member(2)].keys, keys(8));
        assert_eq!(r.state.devices[&DeviceId(3)].member, member(1));
    });
}

/// A pair bound by a concurrent kept entry earlier in replay is no longer
/// fresh at the replacement's position (`Keyed.effect`).
#[test]
fn a_pair_bound_earlier_in_replay_drops_the_replacement() {
    let all: Vec<usize> = (0..5).collect();
    let mut h = household(Member, Member);
    h.push(0, 0, &all, add_with(3, keys(7)));
    h.push(1, 1, &all, rekey(1, 7));
    h.every_order(|r| {
        assert_eq!(r.state.members[&member(3)].keys, keys(7));
        assert_eq!(
            outcome(&h, r, 6),
            EntryOutcome::Dropped(DropReason::KeysAlreadyUsed)
        );
        assert_eq!(r.state.members[&member(1)].keys, keys(1));
    });
}

/// Authorized in an older view, a replacement whose target is gone at its
/// replay position does not restore it (`removed_cannot_be_replaced`).
#[test]
fn a_replacement_never_restores_a_removed_member() {
    let mut h = household(Member, Member);
    h.all(0, 0, remove(1));
    let state = replay(&h.entries).state;
    assert_eq!(
        crate::effects::validate_effect(
            &state,
            &StoreLogCheck::Allowed,
            &member(0),
            DeviceId(0),
            &rekey(1, 7),
        ),
        Err(DropReason::TargetGone(EntryTarget::Member(member(1))))
    );
}
