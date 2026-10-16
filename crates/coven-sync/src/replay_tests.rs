use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use crate::effects::tests::snapshot;
use coven_database::{DatabaseBuilder, DropReason, EntryOutcome, StoreLog, StoreLogReplay};
use coven_format::{
    store_log::{MemberPublicKeys, MemberRole, StoreChange, StoreLogEntry},
    value::{EntryId, EntryPositions},
    Object,
};
use coven_foundation::id_source::MemberId;
use coven_foundation::{
    files::StoreLayout,
    id_source::{CircleId, DeviceId, KeyId, SequentialIds, StoreId},
};
use coven_merge::{Audience, Timestamp};

#[path = "../tests/fixtures/replay_history.rs"]
mod history;
pub(crate) use history::realistic_history;
use MemberRole::{Admin, Member};

// Untimed membership fixtures all land at the same provider tick. They still
// exercise production admission and replay, with no alternate replay engine.
pub(crate) fn replay(entries: &[StoreLogEntry]) -> StoreLogReplay {
    let publications = entries
        .iter()
        .map(|e| (e.position, coven_format::value::StorageTime::UNIX_EPOCH))
        .collect();
    super::replay(entries, &publications)
}

pub(crate) fn replay_entry(
    log: &StoreLog,
    entry: StoreLogEntry,
) -> (coven_database::ReplayEntry, StoreLogReplay) {
    let publications = log
        .entries
        .iter()
        .map(|e| (e.entry.position, e.stored_at))
        .chain([(entry.position, coven_format::value::StorageTime::UNIX_EPOCH)])
        .collect();
    super::replay_entry(log, entry, &publications)
}

/// The applied log of a history in its recorded order, with its checks.
pub(crate) fn checked_log(entries: &[StoreLogEntry]) -> StoreLog {
    let mut log = StoreLog::default();
    for entry in entries {
        let (checked, replay) = replay_entry(&log, entry.clone());
        log.entries.push(checked);
        log.entries.sort_by_key(|applied| applied.entry.timestamp);
        log.replay = replay;
    }
    log
}

/// The newest acceptable introduction of an audience after the whole history.
pub(crate) fn newest_key(h: &History, audience: Audience) -> Option<KeyId> {
    crate::key_introductions::newest(&checked_log(&h.entries), &audience)
}

/// Member `n`'s public pair, from seeds `n + 1` like the device fixtures.
pub(crate) fn keys(n: u8) -> MemberPublicKeys {
    crate::store_log_sync::tests::member(n + 1)
        .keys()
        .public_keys()
}
/// Member `n`'s stable id, independent of its keys.
pub(crate) fn member(n: u8) -> MemberId {
    crate::store_log_sync::tests::member(n + 1).member()
}
pub(crate) fn circle(n: u64) -> CircleId {
    CircleId(uuid::Uuid::from_u128(u128::from(n)))
}
pub(crate) fn key(n: u64) -> KeyId {
    KeyId(uuid::Uuid::from_u128(u128::from(n)))
}
pub(crate) fn add(n: u8, role: MemberRole) -> StoreChange {
    StoreChange::AddMember {
        member: member(n),
        access: coven_format::MemberAccess::S3AccessKey {
            access_key_id: "fixture-access-key".into(),
            principal: coven_format::S3Principal::Console,
        },
        keys: keys(n),
        role,
    }
}
pub(crate) fn role(n: u8, role: MemberRole) -> StoreChange {
    StoreChange::ChangeRole {
        member: member(n),
        role,
    }
}
pub(crate) fn remove(n: u8) -> StoreChange {
    StoreChange::RemoveMember { member: member(n) }
}
/// A registration of device `n`, which only device `n` writes (§10).
pub(crate) fn device(n: u64) -> StoreChange {
    StoreChange::AddDevice {
        name: format!("Device {n}"),
        replaces: None,
    }
}
/// Restored copy `new` registering itself, naming the old id it replaces (D6).
pub(crate) fn replace(new: u64, old: u64) -> StoreChange {
    StoreChange::AddDevice {
        name: format!("Device {new}"),
        replaces: Some(DeviceId(old)),
    }
}

#[derive(Clone)]
pub(crate) struct History {
    pub(crate) entries: Vec<StoreLogEntry>,
}

impl History {
    pub(crate) fn new() -> Self {
        let mut history = Self { entries: vec![] };
        history.push(
            0,
            0,
            &[],
            StoreChange::CreateStore {
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "fixture-access-key".into(),
                    principal: coven_format::S3Principal::Console,
                },
                store: StoreId(uuid::Uuid::from_u128(1)),
                name: "Home".into(),
                admin: keys(0),
                key: key(1),
                device_name: "Ana’s phone".into(),
                key_hash: [17; 32],
            },
        );
        history
    }

    pub(crate) fn push(&mut self, author: u8, device: u64, past: &[usize], change: StoreChange) {
        let device = DeviceId(device);
        let number = self
            .entries
            .iter()
            .filter(|entry| entry.position.device == device)
            .count() as u64
            + 1;
        let mut positions = BTreeMap::<DeviceId, u64>::new();
        for &index in past {
            let id = self.entries[index].position;
            if id.device != device {
                positions
                    .entry(id.device)
                    .and_modify(|n| *n = (*n).max(id.number))
                    .or_insert(id.number);
            }
        }
        let entry = StoreLogEntry {
            position: EntryId { device, number },
            timestamp: Timestamp::new(self.entries.len() as u64 + 1, 0, device).unwrap(),
            author: member(author),
            had_read: EntryPositions(
                positions
                    .into_iter()
                    .map(|(device, number)| EntryId { device, number })
                    .collect(),
            ),
            change,
        };
        Object::StoreLog(entry.clone()).encode().unwrap();
        self.entries.push(entry);
    }

    pub(crate) fn all(&mut self, author: u8, device: u64, change: StoreChange) {
        self.push(
            author,
            device,
            &(0..self.entries.len()).collect::<Vec<_>>(),
            change,
        );
    }

    pub(crate) fn prefix(&self, length: usize) -> Self {
        Self {
            entries: self.entries[..length].to_vec(),
        }
    }

    pub(crate) fn every_order(&self, check: impl FnOnce(&StoreLogReplay)) -> StoreLogReplay {
        let expected = replay(&self.entries);
        check(&expected);
        let mut arrivals = 0;
        self.arrive(
            &mut Vec::new(),
            &mut BTreeSet::new(),
            &StoreLog::default(),
            &expected,
            &mut arrivals,
        );
        assert!(arrivals > 0, "history must admit a causal arrival order");
        expected
    }

    fn arrive(
        &self,
        order: &mut Vec<StoreLogEntry>,
        present: &mut BTreeSet<usize>,
        applied: &StoreLog,
        expected: &StoreLogReplay,
        arrivals: &mut usize,
    ) {
        if order.len() == self.entries.len() {
            assert_eq!(
                &replay(order),
                expected,
                "arrival positions: {:?}",
                order.iter().map(|e| e.position).collect::<Vec<_>>()
            );
            *arrivals += 1;
            return;
        }
        for (index, entry) in self.entries.iter().enumerate() {
            if present.contains(&index)
                || self.entries.iter().enumerate().any(|(prior, e)| {
                    crate::replay::had_read(entry, e) && !present.contains(&prior)
                })
            {
                continue;
            }
            present.insert(index);
            order.push(entry.clone());
            let prefix = replay(order);
            assert_eq!(prefix.entries.len(), order.len());
            let (checked, incremental) = crate::replay::tests::replay_entry(applied, entry.clone());
            assert_eq!(incremental, prefix);
            let mut next = applied.clone();
            next.entries.push(checked);
            next.replay = incremental;
            self.arrive(order, present, &next, expected, arrivals);
            order.pop();
            present.remove(&index);
        }
    }

    pub(crate) fn drops(&self, result: &StoreLogReplay) -> Vec<usize> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                matches!(result.entries[&e.position], EntryOutcome::Dropped(_)).then_some(i)
            })
            .collect()
    }
}

pub(crate) fn household(ben: MemberRole, carol: MemberRole) -> History {
    let mut h = History::new();
    h.all(0, 0, add(1, ben));
    h.all(1, 1, device(1));
    h.all(0, 0, add(2, carol));
    h.all(2, 2, device(2));
    h
}

#[test]
fn creation_registers_its_author_and_named_device() {
    assert_eq!(replay(&[]), StoreLogReplay::default());
    History::new().every_order(|r| {
        assert_eq!(r.state.members[&member(0)].role, Admin);
        assert_eq!(r.state.members[&member(0)].keys, keys(0));
        assert_eq!(r.state.devices[&DeviceId(0)].member, member(0));
        assert_eq!(r.state.devices[&DeviceId(0)].name, "Ana’s phone");
        assert!(r.state.schema.is_empty());
    });
}

#[test]
fn opening_log_device_addition_and_concurrent_admin_grant_both_apply() {
    let mut h = household(Member, Member).prefix(2);
    h.push(1, 1, &[0, 1], device(1));
    h.push(0, 0, &[0, 1], role(1, Admin));
    h.every_order(|r| {
        assert_eq!(r.state.members[&member(1)].role, Admin);
        assert_eq!(r.state.devices[&DeviceId(1)].member, member(1));
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn independent_member_changes_both_apply() {
    let mut h = household(Admin, Member).prefix(4);
    h.push(0, 0, &[0, 1, 2, 3], add(3, Member));
    h.push(1, 1, &[0, 1, 2, 3], role(2, Admin));
    h.every_order(|r| {
        assert_eq!(r.state.members[&member(2)].role, Admin);
        assert!(!r.state.members[&member(3)].removed);
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn ben_registers_his_phone_and_anas_concurrent_member_removal_defeats_it() {
    let mut h = household(Admin, Member).prefix(3);
    h.push(1, 4, &[0, 1, 2], device(4));
    h.every_order(|r| {
        assert_eq!(r.state.devices[&DeviceId(4)].member, member(1));
        assert!(h.drops(r).is_empty());
    });
    h.push(0, 0, &[0, 1, 2], remove(1));
    h.every_order(|r| {
        assert!(r.state.members[&member(1)].removed);
        assert!(r.state.devices[&DeviceId(1)].removed());
        assert!(!r.state.devices.contains_key(&DeviceId(4)));
        assert_eq!(h.drops(r), [3]);
    });
}

#[test]
fn member_wins_over_admin_grant() {
    let mut h = household(Member, Admin);
    h.push(0, 0, &[0, 1, 2, 3, 4], role(1, Admin));
    h.push(2, 2, &[0, 1, 2, 3, 4], role(1, Member));
    h.every_order(|r| {
        assert_eq!(r.state.members[&member(1)].role, Member);
        assert_eq!(h.drops(r), [5]);
    });
}

#[test]
fn earlier_mutual_removal_keeps_one_admin() {
    let mut h = household(Admin, Member).prefix(3);
    h.push(0, 0, &[0, 1, 2], remove(1));
    h.push(1, 1, &[0, 1, 2], remove(0));
    h.every_order(|r| {
        assert!(!r.state.members[&member(0)].removed);
        assert!(r.state.members[&member(1)].removed);
        assert_eq!(h.drops(r), [4]);
        assert_eq!(
            r.entries[&h.entries[4].position],
            EntryOutcome::Dropped(DropReason::NoAdminLeft)
        );
    });
}

#[test]
fn independent_store_removal_keeps_carols_addition_and_her_device() {
    let mut h = household(Admin, Member).prefix(3);
    h.all(0, 0, add(3, Member));
    h.push(0, 0, &[0, 1, 2, 3], add(2, Member));
    h.push(1, 1, &[0, 1, 2, 3], remove(3));
    h.every_order(|r| {
        assert!(!r.state.members[&member(2)].removed);
        assert!(r.state.members[&member(3)].removed);
        assert!(h.drops(r).is_empty());
    });
    h.push(2, 2, &[0, 1, 2, 3, 4], device(2));
    h.every_order(|r| {
        assert_eq!(r.state.devices[&DeviceId(2)].member, member(2));
        assert!(!r.state.devices[&DeviceId(2)].removed());
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn repeated_additions_keep_both_identities() {
    let mut h = household(Admin, Member).prefix(3);
    h.push(0, 0, &[0, 1, 2], add(3, Member));
    h.push(1, 1, &[0, 1, 2], add(3, Member));
    h.every_order(|r| {
        assert_eq!(r.state.members.len(), 3);
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn three_admins_removing_one_another_keep_two_removals_and_one_admin() {
    let mut h = household(Admin, Admin);
    h.push(0, 0, &[0, 1, 2, 3, 4], remove(1));
    h.push(1, 1, &[0, 1, 2, 3, 4], remove(2));
    h.push(2, 2, &[0, 1, 2, 3, 4], remove(0));
    h.every_order(|r| {
        assert_eq!(
            r.state
                .members
                .iter()
                .filter_map(|(id, m)| (!m.removed).then_some(*id))
                .collect::<BTreeSet<_>>(),
            [member(0)].into()
        );
        assert_eq!(h.drops(r), [7]);
        assert_eq!(
            r.entries[&h.entries[7].position],
            EntryOutcome::Dropped(DropReason::NoAdminLeft)
        );
        assert_eq!(r.state.members[&member(1)].keys, keys(1));
    });
}

#[test]
fn removed_author_keeps_authority_for_a_concurrent_role_change() {
    let mut h = household(Admin, Admin);
    h.push(0, 0, &[0, 1, 2, 3, 4], remove(1));
    h.push(1, 1, &[0, 1, 2, 3, 4], role(2, Member));
    h.every_order(|r| {
        assert!(r.state.members[&member(1)].removed);
        assert_eq!(r.state.members[&member(2)].role, Member);
        assert!(h.drops(r).is_empty());
    });
}

pub(crate) fn losing_removal() -> History {
    let mut h = History::new();
    h.all(0, 0, add(1, Admin));
    h.all(1, 1, device(1));
    h.all(1, 4, device(4));
    h
}

#[test]
fn dropped_removal_does_not_block_the_phone() {
    let mut h = losing_removal();
    h.push(1, 1, &[0, 1, 2, 3], remove(0));
    h.push(0, 0, &[0, 1, 2, 3], remove(1));
    h.push(1, 5, &[0, 1, 2, 3], device(5));
    h.every_order(|r| {
        assert_eq!(r.state.devices[&DeviceId(5)].member, member(1));
        assert_eq!(h.drops(r), [5]);
    });
}

#[test]
fn a_drop_persists_after_its_winner_drops_on_restart() {
    let mut h = household(Member, Admin);
    let past: Vec<_> = (0..5).collect();
    h.push(0, 0, &past, role(1, Admin));
    h.push(2, 2, &past, role(1, Member));
    h.push(0, 4, &past, remove(1));
    h.every_order(|r| {
        assert!(r.state.members[&member(1)].removed);
        assert_eq!(h.drops(r), [5, 6]);
        for (loser, winner) in [(5, 6), (6, 7)] {
            assert_eq!(
                r.entries[&h.entries[loser].position],
                EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[winner].position))
            );
        }
    });
}

#[test]
fn device_removal_requires_observed_ownership_even_when_already_absent() {
    let mut unseen = household(Admin, Member).prefix(3);
    unseen.push(1, 4, &[0, 1, 2], device(4));
    unseen.push(
        0,
        0,
        &[0, 1, 2],
        StoreChange::RemoveDevice {
            device: DeviceId(4),
        },
    );
    unseen.every_order(|r| {
        assert!(!r.state.devices[&DeviceId(4)].removed());
        assert_eq!(
            r.entries[&unseen.entries[4].position],
            EntryOutcome::Dropped(DropReason::NotAllowed)
        );
    });
    for author in [0, 1, 2] {
        let mut h = household(Admin, Member);
        h.all(
            author,
            u64::from(author),
            StoreChange::RemoveDevice {
                device: DeviceId(1),
            },
        );
        h.every_order(|r| {
            assert_eq!(r.state.devices[&DeviceId(1)].removed(), author != 2);
            assert_eq!(h.drops(r), if author == 2 { vec![5] } else { vec![] });
        });
    }
    let mut h = household(Admin, Member).prefix(3);
    h.all(
        0,
        0,
        StoreChange::RemoveDevice {
            device: DeviceId(9),
        },
    );
    h.every_order(|r| {
        assert_eq!(
            r.entries[&h.entries[3].position],
            EntryOutcome::Dropped(DropReason::NotAllowed)
        )
    });
}

pub(crate) fn make(c: u64, name: &str) -> StoreChange {
    StoreChange::CreateCircle {
        circle: circle(c),
        name: name.into(),
        key: key(c + 10),
        key_hash: [17; 32],
    }
}
pub(crate) fn join(c: u64, m: u8) -> StoreChange {
    StoreChange::AddCircleMember {
        circle: circle(c),
        member: member(m),
    }
}
pub(crate) fn leave(c: u64, m: u8) -> StoreChange {
    StoreChange::RemoveCircleMember {
        circle: circle(c),
        member: member(m),
    }
}
pub(crate) fn rename(c: u64, name: &str) -> StoreChange {
    StoreChange::RenameCircle {
        circle: circle(c),
        name: name.into(),
    }
}
pub(crate) fn delete(c: u64) -> StoreChange {
    StoreChange::DeleteCircle { circle: circle(c) }
}

pub(crate) fn gifts() -> History {
    let mut h = household(Member, Member).prefix(3);
    h.all(0, 0, make(0, "Gifts"));
    h.all(0, 0, join(0, 1));
    h
}

#[test]
fn concurrent_circle_renames_keep_both_entries_and_use_the_later_stamp() {
    for first_author in [0, 1] {
        for later_name in ["Presents", "Birthdays", "Gifts"] {
            let mut h = gifts();
            for (author, name) in [(first_author, "Birthdays"), (1 - first_author, later_name)] {
                h.push(author, u64::from(author), &[0, 1, 2, 3, 4], rename(0, name));
            }
            assert!(!crate::replay::had_read(&h.entries[5], &h.entries[6]));
            assert!(!crate::replay::had_read(&h.entries[6], &h.entries[5]));
            h.every_order(|r| {
                assert_eq!(r.state.circles[&circle(0)].name, later_name);
                assert!(h.drops(r).is_empty());
                for entry in &h.entries[5..] {
                    assert_eq!(r.entries[&entry.position], EntryOutcome::Kept);
                }
            });
        }
    }
}

#[test]
fn circle_deletion_beats_rename_and_reset() {
    for action in [
        rename(0, "Birthdays"),
        StoreChange::Reset {
            snapshot: snapshot(50, Audience::Circle(circle(0))),
        },
    ] {
        let mut h = gifts();
        h.push(0, 0, &[0, 1, 2, 3, 4], action);
        h.push(1, 1, &[0, 1, 2, 3, 4], delete(0));
        h.every_order(|r| {
            assert!(r.state.circle_deletions.contains_key(&circle(0)));
            assert!(r.state.circles[&circle(0)].members.is_empty());
            assert!(r.state.resets.is_empty());
            assert_eq!(h.drops(r), [5]);
        });
    }
}

#[test]
fn circle_removal_and_different_member_addition_both_apply() {
    let mut h = gifts();
    h.all(0, 0, add(2, Member));
    h.push(0, 0, &[0, 1, 2, 3, 4, 5], join(0, 2));
    h.push(1, 1, &[0, 1, 2, 3, 4, 5], leave(0, 0));
    h.every_order(|r| {
        assert_eq!(
            r.state.circles[&circle(0)].members,
            [member(1), member(2)].into()
        );
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn removed_circle_member_keeps_authority_for_a_concurrent_rename() {
    let mut h = gifts();
    h.push(0, 0, &[0, 1, 2, 3, 4], leave(0, 1));
    h.push(1, 1, &[0, 1, 2, 3, 4], rename(0, "Birthdays"));
    h.every_order(|r| {
        assert_eq!(r.state.circles[&circle(0)].name, "Birthdays");
        assert_eq!(r.state.circles[&circle(0)].members, [member(0)].into());
        assert!(!r.state.members[&member(1)].removed);
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn ordinary_members_make_circles_and_outside_admins_cannot_manage_them() {
    let mut base = household(Member, Member).prefix(3);
    base.all(1, 1, make(0, "Gifts"));
    base.every_order(|r| {
        assert_eq!(r.state.circles[&circle(0)].members, [member(1)].into());
        assert_eq!(r.state.members[&member(1)].role, Member);
    });
    for action in [
        rename(0, "Gifts"),
        rename(0, "Birthdays"),
        join(0, 0),
        leave(0, 1),
        delete(0),
        StoreChange::Reset {
            snapshot: snapshot(50, Audience::Circle(circle(0))),
        },
    ] {
        let mut h = base.clone();
        h.all(0, 0, action);
        h.every_order(|r| {
            assert_eq!(r.state.circles[&circle(0)].members, [member(1)].into());
            assert_eq!(r.state.circles[&circle(0)].name, "Gifts");
            assert!(!r.state.circle_deletions.contains_key(&circle(0)));
            assert_eq!(
                r.entries[&h.entries[4].position],
                EntryOutcome::Dropped(DropReason::NotAllowed)
            );
        });
    }
    base.all(1, 1, leave(0, 1));
    base.every_order(|r| {
        assert!(r.state.circle_deletions.contains_key(&circle(0)));
        assert!(!r.state.members[&member(1)].removed);
        assert!(base.drops(r).is_empty());
    });
}

fn carols_circles() -> History {
    let mut h = household(Member, Member);
    h.all(2, 2, make(0, "Gifts"));
    h.all(2, 2, join(0, 1));
    h.all(2, 2, make(1, "Carol's notes"));
    h
}

#[test]
fn removing_carol_preserves_shared_keys_and_deletes_her_private_circle() {
    let mut h = carols_circles();
    h.all(0, 0, remove(2));
    h.every_order(|r| {
        assert!(r.state.members[&member(2)].removed);
        assert!(r.state.devices[&DeviceId(2)].removed());
        assert_eq!(r.state.circles[&circle(0)].members, [member(1)].into());
        assert!(r.state.circle_deletions.contains_key(&circle(1)));
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn removing_carol_keeps_bens_concurrent_gift_invitation_for_dan() {
    let mut h = carols_circles().prefix(7);
    h.all(0, 0, add(3, Member));
    h.push(1, 1, &[0, 1, 2, 3, 4, 5, 6, 7], join(0, 3));
    h.push(0, 0, &[0, 1, 2, 3, 4, 5, 6, 7], remove(2));
    h.every_order(|r| {
        assert_eq!(
            r.state.circles[&circle(0)].members,
            [member(1), member(3)].into()
        );
        assert!(!r.state.members[&member(3)].removed);
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn repeated_member_removals_need_no_key_payload() {
    let mut h = carols_circles();
    h.all(0, 0, remove(2));
    h.all(0, 0, remove(2));
    h.every_order(|r| {
        assert!(h.drops(r).is_empty());
        assert!(r.state.circle_deletions.contains_key(&circle(1)));
    });
}

#[test]
fn removal_uses_membership_when_concurrent_circle_additions_arrive() {
    let mut h = History::new();
    h.all(0, 0, add(1, Member));
    h.all(0, 0, add(2, Member));
    h.all(0, 0, make(0, "Gifts"));
    h.push(0, 4, &[0, 1, 2, 3], join(0, 1));
    h.push(0, 4, &[0, 1, 2, 3, 4], join(0, 2));
    h.push(0, 0, &[0, 1, 2, 3], remove(1));
    h.every_order(|r| {
        assert_eq!(
            r.state.circles[&circle(0)].members,
            [member(0), member(2)].into()
        );
        assert!(r.state.members[&member(1)].removed);
        assert_eq!(h.drops(r), [4]);
    });
}

#[test]
fn circle_and_store_removals_do_not_conflict_over_a_key() {
    let mut h = household(Admin, Member).prefix(3);
    h.all(0, 0, make(0, "Gifts"));
    h.all(0, 0, join(0, 1));
    h.push(0, 0, &[0, 1, 2, 3, 4], leave(0, 1));
    h.push(0, 4, &[0, 1, 2, 3, 4], remove(0));
    h.every_order(|r| {
        assert!(r.state.members[&member(0)].removed);
        assert!(r.state.circle_deletions.contains_key(&circle(0)));
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn removing_a_circles_only_member_beats_their_concurrent_rename() {
    let mut h = household(Member, Member).prefix(3);
    h.all(1, 1, make(0, "Ben's notes"));
    h.push(1, 1, &[0, 1, 2, 3], rename(0, "Journal"));
    h.push(0, 0, &[0, 1, 2, 3], remove(1));
    h.every_order(|r| {
        assert!(r.state.circle_deletions.contains_key(&circle(0)));
        assert_eq!(h.drops(r), [4]);
    });
}

#[test]
fn a_store_removal_reverses_a_previously_kept_deletion() {
    let mut h = gifts();
    h.push(0, 0, &[0, 1, 2, 3, 4], remove(1));
    h.push(1, 1, &[0, 1, 2, 3, 4], delete(0));
    let mut arrived = h.entries[..5].to_vec();
    arrived.push(h.entries[6].clone());
    assert!(replay(&arrived)
        .state
        .circle_deletions
        .contains_key(&circle(0)));
    arrived.push(h.entries[5].clone());
    let expected = h.every_order(|r| {
        assert!(!r.state.circle_deletions.contains_key(&circle(0)));
        assert_eq!(r.state.circles[&circle(0)].members, [member(0)].into());
        assert_eq!(h.drops(r), [6]);
    });
    assert_eq!(replay(&arrived), expected);
}

#[test]
fn winning_removal_drops_all_opponents_before_restarting() {
    let mut h = household(Admin, Member).prefix(3);
    h.push(1, 4, &[0, 1, 2], device(4));
    h.push(1, 5, &[0, 1, 2], device(5));
    h.push(0, 0, &[0, 1, 2], remove(1));
    h.every_order(|r| {
        assert_eq!(h.drops(r), [3, 4]);
        assert!(!r.state.devices.contains_key(&DeviceId(4)));
        assert!(!r.state.devices.contains_key(&DeviceId(5)));
        for i in [3, 4] {
            assert_eq!(
                r.entries[&h.entries[i].position],
                EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[5].position))
            );
        }
    });
}

#[test]
fn a_new_arrival_reconsiders_entries_dropped_by_the_previous_replay() {
    let mut h = gifts();
    h.push(
        0,
        0,
        &[0, 1, 2, 3, 4],
        StoreChange::Reset {
            snapshot: snapshot(50, Audience::Circle(circle(0))),
        },
    );
    h.push(0, 0, &[0, 1, 2, 3, 4, 5], remove(1));
    h.push(1, 1, &[0, 1, 2, 3, 4], delete(0));
    let mut arrived = h.entries[..6].to_vec();
    arrived.push(h.entries[7].clone());
    let before = replay(&arrived);
    assert_eq!(
        before.entries[&h.entries[5].position],
        EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[7].position))
    );
    h.every_order(|r| {
        assert_eq!(h.drops(r), [7]);
        assert_eq!(r.state.resets[&Audience::Circle(circle(0))].number, 50);
        assert!(!r.state.circle_deletions.contains_key(&circle(0)));
        assert_eq!(
            r.entries[&h.entries[7].position],
            EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[6].position))
        );
    });
}

#[test]
fn concurrent_store_removals_preserve_keys() {
    let mut h = household(Admin, Member);
    h.all(0, 0, add(3, Member));
    h.all(3, 3, device(3));
    h.all(0, 0, add(4, Member));
    h.all(4, 4, device(4));
    h.all(0, 0, make(0, "Gifts"));
    h.all(0, 0, join(0, 3));
    h.all(0, 0, join(0, 4));
    let past: Vec<_> = (0..h.entries.len()).collect();
    for (author, removed) in [(0, 3), (1, 4)] {
        h.push(
            author,
            u64::from(author),
            &past,
            StoreChange::RemoveMember {
                member: member(removed),
            },
        );
    }
    h.every_order(|r| {
        assert_eq!(r.state.circles[&circle(0)].members, [member(0)].into());
        for m in [3, 4] {
            assert!(r.state.members[&member(m)].removed);
            let device = &r.state.devices[&DeviceId(u64::from(m))];
            assert!(device.removed());
            assert_eq!(device.member, member(m));
            assert_eq!(device.name, format!("Device {m}"));
        }
        assert!(h.drops(r).is_empty());
    });
    // Repeating an effective removal keeps its entry without changing membership.
    h.all(1, 1, h.entries[13].change.clone());
    h.every_order(|r| {
        assert_eq!(r.state.circles[&circle(0)].members, [member(0)].into());
        assert!(r.state.members[&member(3)].removed);
        assert!(r.state.members[&member(4)].removed);
        assert!(h.drops(r).is_empty());
    });
}

#[derive(Debug, Default)]
struct ReplayWork {
    author_checks: usize,
    entry_visits: usize,
    causal_checks: usize,
}

thread_local! {
    static WORK: RefCell<ReplayWork> = RefCell::new(ReplayWork::default());
}

pub(crate) fn author_check() {
    WORK.with_borrow_mut(|work| work.author_checks += 1);
}

pub(super) fn visit_entry() {
    WORK.with_borrow_mut(|work| work.entry_visits += 1);
}

pub(super) fn causal_check() {
    WORK.with_borrow_mut(|work| work.causal_checks += 1);
}

impl ReplayWork {
    fn take() -> Self {
        WORK.take()
    }

    fn assert_bounded(
        &self,
        authors: usize,
        selected: usize,
        past: usize,
        entries: usize,
        writers: usize,
    ) {
        assert_eq!(self.author_checks, authors, "{self:?}");
        // This history has three winning removals that can restart a scan.
        // Each author view and received-set replay therefore visits its input
        // at most four times. These are work bounds, independent of CPU speed.
        assert!(self.entry_visits <= 4 * selected, "{self:?}");
        // Finding concurrent opponents uses a binary search per writer, not
        // a scan of all prior entries. At most one opponent exists per entry
        // in this history, with two further causal checks inside conflict().
        let per_entry = writers * (entries.ilog2() as usize + 2) + 2;
        assert!(
            self.causal_checks <= past + self.entry_visits * per_entry,
            "{self:?}"
        );
    }
}

#[tokio::test]
#[ignore = "release work bounds and timing, run by scripts/check.sh"]
async fn replay_cost() {
    let h = realistic_history();
    let count = h.entries.len();
    let writers = h
        .entries
        .iter()
        .map(|entry| entry.position.device)
        .collect::<BTreeSet<_>>()
        .len();
    ReplayWork::take();
    let start = Instant::now();
    let result = replay(&h.entries);
    let elapsed = start.elapsed();
    let full_work = ReplayWork::take();
    // Author views select prefixes; the final received set adds one full scan.
    full_work.assert_bounded(
        count,
        count * (count + 1) / 2,
        count * (count - 1) / 2,
        count,
        writers,
    );
    println!("2,000 entries: one full replay {elapsed:?}; {full_work:?}");
    let temporary = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(temporary.path().to_owned());
    let directory = layout
        .create_store_dir(
            StoreId(uuid::Uuid::from_u128(1)),
            "Home",
            &SequentialIds::new(),
        )
        .unwrap();
    let database = DatabaseBuilder::new(directory)
        .synced_tables(vec![])
        .migrations(vec![])
        .open()
        .await
        .unwrap();
    let mut applied = StoreLog::default();
    let started = Instant::now();
    let mut last_replay = std::time::Duration::ZERO;
    let mut last_apply = std::time::Duration::ZERO;
    for entry in &h.entries {
        ReplayWork::take();
        let start = Instant::now();
        let (checked, replay) = replay_entry(&applied, entry.clone());
        last_replay = start.elapsed();
        let work = ReplayWork::take();
        let count = applied.entries.len() + 1;
        // Retained author checks are reused. Only the arriving author is
        // checked, with at most its past and the resulting received set replayed.
        work.assert_bounded(1, 2 * count - 1, count - 1, count, writers);
        database
            .apply_store_log::<crate::PendingRecord>(checked.clone(), replay.clone(), Vec::new())
            .await
            .unwrap();
        applied.entries.push(checked);
        applied.replay = replay;
        last_apply = start.elapsed();
    }
    let application = started.elapsed();
    println!("2,000 entries: sequential application {application:?}; last replay {last_replay:?}; last application {last_apply:?}");
    assert_eq!(applied.replay, result);
    let mut expected = applied;
    expected.entries.sort_by_key(|entry| entry.entry.timestamp);
    assert_eq!(database.store_log().await.unwrap(), expected);
    database.close().await.unwrap();
    assert_eq!(h.entries.len(), 2_000);
    assert_eq!(result.state.members.len(), 20);
    assert_eq!(result.state.circles.len(), 13);
    for m in 17..20 {
        assert!(result.state.members[&member(m)].removed);
        assert!(result
            .state
            .circle_deletions
            .contains_key(&circle(u64::from(m))));
    }
    // Only the renames of private circles deleted by member removals drop.
    assert_eq!(h.drops(&result), [600, 1_200, 1_800]);
}
