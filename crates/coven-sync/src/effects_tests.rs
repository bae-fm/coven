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
            assert_eq!(h.drops(r), if second == 30 { vec![] } else { vec![4] });
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
                member: member(1),
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: name.into(),
                    principal: coven_format::S3Principal::Console,
                },
            },
        );
    }
    h.all(
        3,
        3,
        StoreChange::SetAccess {
            member: member(3),
            access: coven_format::MemberAccess::ProviderAccount("outsider@example.com".into()),
        },
    );
    h.all(0, 0, remove(1));
    h.all(
        1,
        1,
        StoreChange::SetAccess {
            member: member(1),
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "after-removal".into(),
                principal: coven_format::S3Principal::Console,
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
                principal: coven_format::S3Principal::Console,
            }
        );
        assert_eq!(h.drops(r), [6, 8]);
    });
}

pub(crate) fn rotation(audience: Audience, id: u64) -> StoreChange {
    StoreChange::RotateKey {
        audience,
        key: key(id),
        key_hash: [id as u8; 32],
    }
}

#[test]
fn rotations_preserve_membership_and_do_not_conflict_with_removals_or_each_other() {
    for audience in [Audience::Store, Audience::Circle(circle(0))] {
        let mut h = gifts();
        let past: Vec<_> = (0..h.entries.len()).collect();
        let removal = if audience == Audience::Store {
            remove(1)
        } else {
            leave(0, 1)
        };
        h.push(0, 0, &past, removal);
        let removed = crate::replay::tests::replay(&h.entries).state;
        h.push(1, 1, &past, rotation(audience.clone(), 20));
        h.push(0, 3, &past, rotation(audience.clone(), 21));
        h.every_order(|r| {
            assert!(h.drops(r).is_empty());
            assert_eq!(r.state.members, removed.members);
            assert_eq!(r.state.devices, removed.devices);
            assert_eq!(
                r.state.circles[&circle(0)].members,
                removed.circles[&circle(0)].members
            );
            assert_eq!(r.state.schema, removed.schema);
            assert_eq!(r.state.resets, removed.resets);
        });
    }
}

#[test]
fn rotation_authority_comes_from_audience_membership_in_the_authors_past() {
    let mut h = household(Member, Member);
    h.all(1, 1, make(0, "Private"));
    h.all(1, 1, rotation(Audience::Store, 20));
    h.all(1, 1, rotation(Audience::Circle(circle(0)), 21));
    h.all(0, 0, rotation(Audience::Circle(circle(0)), 22));
    h.all(0, 0, remove(1));
    h.all(1, 1, rotation(Audience::Store, 23));
    h.every_order(|r| {
        assert_eq!(h.drops(r), [8, 10]);
        for index in [8, 10] {
            assert_eq!(
                r.entries[&h.entries[index].position],
                coven_database::EntryOutcome::Dropped(coven_database::DropReason::NotAllowed)
            );
        }
    });
}

#[test]
fn concurrent_circle_deletion_does_not_drop_an_authorized_rotation() {
    let mut h = gifts();
    let past: Vec<_> = (0..h.entries.len()).collect();
    h.push(0, 0, &past, delete(0));
    h.push(1, 1, &past, rotation(Audience::Circle(circle(0)), 20));
    h.every_order(|r| {
        assert!(h.drops(r).is_empty());
        assert!(r.state.circle_deletions.contains_key(&circle(0)));
        assert!(r.state.circles[&circle(0)].members.is_empty());
    });
}

#[test]
fn replay_rejections_identify_each_target() {
    use crate::replay::{circles::tests::circle_history, tests::replay};
    use coven_database::{DropReason, EntryOutcome, EntryTarget};
    for (change, target) in [
        (role(4, Admin), EntryTarget::Member(member(4))),
        (device(1), EntryTarget::Device(DeviceId(1))),
        (make(0, "Collision"), EntryTarget::Circle(circle(0))),
    ] {
        let mut h = circle_history();
        let current = replay(&h.entries);
        let expected = DropReason::TargetGone(target);
        assert_eq!(
            crate::effects::validate_change(&current.state, &member(0), &change),
            Err(expected.clone())
        );
        h.all(0, 0, change);
        assert_eq!(
            replay(&h.entries).entries[&h.entries.last().unwrap().position],
            EntryOutcome::Dropped(expected)
        );
    }
}

fn state(r: &coven_database::StoreLogReplay, device: u64) -> coven_database::DeviceState {
    r.state.devices[&DeviceId(device)].state
}

/// Ben's phone 1 restored twice from one backup: both copies register.
#[test]
fn concurrent_replacements_keep_both_new_ids_and_the_old_id_replaced() {
    use coven_database::DeviceState::{Active, Replaced};
    let mut h = household(Admin, Member);
    h.push(1, 5, &[0, 1, 2, 3, 4], replace(5, 1));
    h.push(1, 6, &[0, 1, 2, 3, 4], replace(6, 1));
    h.every_order(|r| {
        assert_eq!(h.drops(r), Vec::<usize>::new());
        assert_eq!(
            (state(r, 1), state(r, 5), state(r, 6)),
            (Replaced, Active, Active)
        );
        assert_eq!(r.state.devices[&DeviceId(5)].member, member(1));
    });
    // A later replacement of an already replaced id still registers its copy.
    h.all(1, 7, replace(7, 1));
    h.every_order(|r| {
        assert_eq!(h.drops(r), Vec::<usize>::new());
        assert_eq!((state(r, 1), state(r, 7)), (Replaced, Active));
    });
}

/// Only the old id's member replaces it, and only an id its view has (D6).
#[test]
fn only_the_old_ids_member_replaces_an_observed_id() {
    use coven_database::{DeviceState::Active, DropReason, EntryOutcome};
    for (author, new, old) in [(2, 7, 1), (0, 8, 1), (1, 9, 33)] {
        let mut h = household(Admin, Member);
        h.all(author, new, replace(new, old));
        h.every_order(|r| {
            assert_eq!(
                r.entries[&h.entries[5].position],
                EntryOutcome::Dropped(DropReason::NotAllowed)
            );
            if old == 1 {
                assert_eq!(state(r, 1), Active);
            }
        });
    }
}

/// An old id's entry counts only before its recorded past, including its own
/// earlier entries, reads a kept replacement of that id (§9, §10).
#[test]
fn old_id_entries_count_only_before_reading_their_replacement() {
    use coven_database::{DropReason, EntryOutcome};
    for read in [false, true] {
        let mut h = household(Admin, Member);
        h.push(1, 5, &[0, 1, 2, 3, 4], replace(5, 1));
        let past: Vec<usize> = if read {
            vec![0, 1, 2, 3, 4, 5]
        } else {
            vec![0, 1, 2, 3, 4]
        };
        h.push(1, 1, &past, make(3, "Gifts"));
        h.push(1, 1, &past, rename(3, "Presents"));
        h.every_order(|r| {
            let outcome = |i: usize| r.entries[&h.entries[i].position].clone();
            if read {
                for i in [6, 7] {
                    assert_eq!(outcome(i), EntryOutcome::Dropped(DropReason::NotAllowed));
                }
            } else {
                assert_eq!(
                    (outcome(6), outcome(7)),
                    (EntryOutcome::Kept, EntryOutcome::Kept)
                );
            }
        });
    }
}

/// A removal stays a removal: replacement neither revives nor softens it,
/// and a later removal stops a replaced id's copies for good (§10, §13).
#[test]
fn removal_and_replacement_leave_a_removed_id_removed() {
    use coven_database::DeviceState::{Active, Removed};
    let remove_phone = StoreChange::RemoveDevice {
        device: DeviceId(1),
    };
    for removal_first in [true, false] {
        let mut h = household(Admin, Member);
        if removal_first {
            h.all(1, 1, remove_phone.clone());
            h.all(1, 5, replace(5, 1));
        } else {
            h.all(1, 5, replace(5, 1));
            h.all(1, 5, remove_phone.clone());
        }
        h.every_order(|r| {
            assert_eq!(h.drops(r), Vec::<usize>::new());
            assert_eq!((state(r, 1), state(r, 5)), (Removed, Active));
        });
    }
    let mut h = household(Admin, Member);
    h.all(1, 5, replace(5, 1));
    h.all(0, 0, remove(1));
    h.every_order(|r| assert_eq!((state(r, 1), state(r, 5)), (Removed, Removed)));
}

/// Replay that later drops a kept replacement restores the old id, while an
/// object judged by a recorded past that kept it stays excluded (§9, §10).
#[test]
fn a_dropped_replacement_restores_the_old_id_without_rejudging_recorded_pasts() {
    use coven_database::{
        DeviceState::{Active, Replaced},
        DropReason, EntryOutcome,
    };
    let mut h = household(Admin, Member);
    // Ana registers id 5 for herself; Ben's restored copy, unaware, reuses it.
    h.push(0, 0, &[0, 1, 2, 3, 4], device(5));
    h.push(1, 5, &[0, 1, 2, 3, 4], replace(5, 1));
    h.push(1, 1, &[0, 1, 2, 3, 4, 6], make(3, "Gifts"));
    let before = checked_log(&[&h.entries[..5], &h.entries[6..7]].concat());
    assert_eq!(before.replay.state.devices[&DeviceId(1)].state, Replaced);
    assert!(crate::identity::replaced(&before, DeviceId(1)));
    h.every_order(|r| {
        assert_eq!(
            r.entries[&h.entries[6].position],
            EntryOutcome::Dropped(DropReason::TargetGone(coven_database::EntryTarget::Device(
                DeviceId(5)
            )))
        );
        assert_eq!((state(r, 1), state(r, 5)), (Active, Active));
        assert_eq!(r.state.devices[&DeviceId(5)].member, member(0));
        assert_eq!(
            r.entries[&h.entries[7].position],
            EntryOutcome::Dropped(DropReason::NotAllowed)
        );
    });
    assert!(!crate::identity::replaced(
        &checked_log(&h.entries),
        DeviceId(1)
    ));
}

#[test]
fn a_member_key_replacement_is_not_yet_authorized_by_any_view() {
    let mut h = household(Admin, Member);
    // Sentinel coverage until T105 replays replacements (§9).
    h.all(
        1,
        1,
        StoreChange::ReplaceMemberKeys {
            member: member(1),
            keys: keys(3),
        },
    );
    h.every_order(|r| {
        assert_eq!(
            r.entries[&h.entries[5].position],
            coven_database::EntryOutcome::Dropped(coven_database::DropReason::NotAllowed)
        );
        assert_eq!(r.state.members[&member(1)].keys, keys(1));
    });
}

/// The pair an addition of `member` uses in `log`'s view: an active member's
/// own, or else the first pair sharing no key with one that view bound, as a
/// removed member's return needs (§9).
pub(crate) fn addition_keys(
    log: &coven_database::StoreLog,
    member: &coven_crypto::MemberIdentity,
) -> coven_format::store_log::MemberPublicKeys {
    let state = &log.replay.state;
    if let Some(active) = crate::effects::member(state, &member.member()) {
        return active.keys;
    }
    let unbound = |keys: &coven_format::store_log::MemberPublicKeys| {
        !state
            .bound_keys
            .iter()
            .any(|bound| bound.signing == keys.signing || bound.sealing == keys.sealing)
    };
    std::iter::once(member.keys().public_keys())
        .chain((200..=u8::MAX).map(|seed| {
            let mut bytes = b"CVMK\x01".to_vec();
            bytes.extend([seed; 64]);
            coven_crypto::MemberKeys::from_secret_bytes(&bytes)
                .unwrap()
                .public_keys()
        }))
        .find(unbound)
        .expect("an unbound fixture pair")
}

/// `add(n, Member)` with another public pair.
pub(crate) fn add_with(n: u8, keys: coven_format::store_log::MemberPublicKeys) -> StoreChange {
    let StoreChange::AddMember {
        member,
        role,
        access,
        ..
    } = add(n, Member)
    else {
        unreachable!("add makes an addition")
    };
    StoreChange::AddMember {
        member,
        keys,
        role,
        access,
    }
}

/// An addition of an id not active in its author's view needs a pair sharing
/// no key with any pair that view bound (§9,
/// `Keyed.Additions.return_needs_fresh_keys`, `new_id_needs_fresh_keys`).
#[test]
fn an_addition_of_an_absent_id_needs_keys_no_pair_has_bound() {
    use crate::effects::validate_change;
    use coven_database::{DropReason, EntryOutcome};
    use coven_format::store_log::MemberPublicKeys;
    let refused = Err(DropReason::KeysAlreadyUsed);
    // Ben (1) is removed, then returns with his old pair or a fresh one.
    for (pair, fresh) in [(keys(1), false), (keys(9), true)] {
        let mut h = household(Member, Member);
        h.all(0, 0, remove(1));
        let current = replay(&h.entries);
        assert!(current.state.bound_keys.contains(&keys(1)));
        let change = add_with(1, pair);
        let expected = if fresh { Ok(false) } else { refused.clone() };
        assert_eq!(
            validate_change(&current.state, &member(0), &change),
            expected
        );
        h.all(0, 0, change);
        let last = h.entries.last().unwrap().position;
        h.every_order(|r| {
            if fresh {
                assert_eq!(r.entries[&last], EntryOutcome::Kept);
                let ben = &r.state.members[&member(1)];
                assert_eq!((ben.keys, ben.removed), (pair, false));
                assert!(
                    r.state.bound_keys.contains(&keys(1)) && r.state.bound_keys.contains(&pair)
                );
            } else {
                assert_eq!(
                    r.entries[&last],
                    EntryOutcome::Dropped(DropReason::KeysAlreadyUsed)
                );
                assert!(r.state.members[&member(1)].removed);
            }
        });
    }
    // A new id, Dan (3), borrows Ben's whole pair, his signing key or his
    // sealing key; with his own pair he is added.
    let (ben, dan) = (keys(1), keys(3));
    for (pair, fresh) in [
        (ben, false),
        (
            MemberPublicKeys {
                signing: ben.signing,
                sealing: dan.sealing,
            },
            false,
        ),
        (
            MemberPublicKeys {
                signing: dan.signing,
                sealing: ben.sealing,
            },
            false,
        ),
        (dan, true),
    ] {
        let mut h = household(Member, Member);
        let change = add_with(3, pair);
        let current = replay(&h.entries);
        let expected = if fresh { Ok(false) } else { refused.clone() };
        assert_eq!(
            validate_change(&current.state, &member(0), &change),
            expected
        );
        h.all(0, 0, change);
        let last = h.entries.last().unwrap().position;
        h.every_order(|r| {
            assert_eq!(r.state.members.contains_key(&member(3)), fresh);
            if !fresh {
                assert_eq!(
                    r.entries[&last],
                    EntryOutcome::Dropped(DropReason::KeysAlreadyUsed)
                );
            }
        });
    }
}
