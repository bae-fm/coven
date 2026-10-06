use std::collections::{BTreeMap, BTreeSet};

use crate::effects::tests::snapshot;
use coven_crypto::MemberId;
use coven_database::{DropReason, EntryOutcome, StoreLogReplay};
use coven_format::{
    store_log::{CircleKeyId, MemberPublicKeys, MemberRole, StoreChange, StoreLogEntry},
    value::{EntryId, EntryPositions},
    Object,
};
use coven_foundation::id_source::{CircleId, DeviceId, KeyId, StoreId};
use coven_merge::{Audience, Timestamp};

use crate::replay;
use MemberRole::{Admin, Member};

pub(crate) fn keys(n: u8) -> MemberPublicKeys {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([n + 1; 64]);
    let keys = coven_crypto::MemberKeys::from_secret_bytes(&bytes).unwrap();
    MemberPublicKeys {
        signing: keys.member_id(),
        sealing: keys.sealing_public_key(),
    }
}
pub(crate) fn member(n: u8) -> MemberId {
    keys(n).signing
}
pub(crate) fn circle(n: u64) -> CircleId {
    CircleId(uuid::Uuid::from_u128(u128::from(n)))
}
pub(crate) fn key(n: u64) -> KeyId {
    KeyId(uuid::Uuid::from_u128(u128::from(n)))
}
pub(crate) fn add(n: u8, role: MemberRole) -> StoreChange {
    StoreChange::AddMember {
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
pub(crate) fn remove(n: u8, circles: &[u64]) -> StoreChange {
    StoreChange::RemoveMember {
        member: member(n),
        key: key(100 + u64::from(n)),
        circle_keys: circles
            .iter()
            .map(|&n| CircleKeyId {
                circle: circle(n),
                key: key(200 + n),
            })
            .collect(),
    }
}
pub(crate) fn device(n: u64) -> StoreChange {
    StoreChange::AddDevice {
        device: DeviceId(n),
        name: format!("Device {n}"),
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
                store: StoreId(uuid::Uuid::from_u128(1)),
                name: "Home".into(),
                admin: keys(0),
                key: key(1),
                device_name: "Ana’s phone".into(),
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
            self.arrive(order, present, expected, arrivals);
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

    pub(crate) fn reports(&self, result: &StoreLogReplay, author: u8) -> Vec<usize> {
        self.drops(result)
            .into_iter()
            .filter(|&i| self.entries[i].author == member(author))
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
        assert_eq!(r.state.members[&member(0)].sealing, keys(0).sealing);
        assert_eq!(r.state.devices[&DeviceId(0)].member, member(0));
        assert_eq!(r.state.devices[&DeviceId(0)].name, "Ana’s phone");
        assert_eq!(r.state.store.as_ref().unwrap().key, key(1));
        assert!(r.state.schema.is_none() && r.state.format.is_none());
    });
}

#[test]
fn opening_log_device_addition_beats_the_concurrent_admin_grant() {
    let mut h = household(Member, Member).prefix(2);
    h.push(1, 1, &[0, 1], device(2));
    h.push(0, 3, &[0, 1], role(1, Admin));
    h.every_order(|r| {
        assert_eq!(r.state.members[&member(1)].role, Member);
        assert_eq!(r.state.devices[&DeviceId(2)].member, member(1));
        assert_eq!(h.drops(r), [3]);
        assert_eq!(h.reports(r, 0), [3]);
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
    h.push(0, 0, &[0, 1, 2], remove(1, &[]));
    h.every_order(|r| {
        assert!(r.state.members[&member(1)].removed);
        assert!(r.state.devices[&DeviceId(1)].removed);
        assert!(!r.state.devices.contains_key(&DeviceId(4)));
        assert_eq!(h.drops(r), [3]);
        assert_eq!(h.reports(r, 1), [3]);
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
    h.push(0, 0, &[0, 1, 2], remove(1, &[]));
    h.push(1, 1, &[0, 1, 2], remove(0, &[]));
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
fn store_key_rotation_drops_carols_addition_and_her_device() {
    let mut h = household(Admin, Member).prefix(3);
    h.all(0, 0, add(3, Member));
    h.push(0, 0, &[0, 1, 2, 3], add(2, Member));
    h.push(1, 1, &[0, 1, 2, 3], remove(3, &[]));
    h.every_order(|r| {
        assert!(!r.state.members.contains_key(&member(2)));
        assert!(r.state.members[&member(3)].removed);
        assert_eq!(h.reports(r, 0), [4]);
        assert_eq!(r.state.store.as_ref().unwrap().key, key(103));
    });
    h.push(2, 2, &[0, 1, 2, 3, 4], device(2));
    h.every_order(|r| {
        assert!(!r.state.devices.contains_key(&DeviceId(2)));
        assert_eq!(h.drops(r), [4, 6]);
        assert_eq!(h.reports(r, 2), [6]);
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
fn three_admins_remove_in_their_author_views_but_one_must_remain() {
    let mut h = household(Admin, Admin);
    h.push(0, 0, &[0, 1, 2, 3, 4], remove(1, &[]));
    h.push(1, 1, &[0, 1, 2, 3, 4], remove(2, &[]));
    h.push(2, 2, &[0, 1, 2, 3, 4], remove(0, &[]));
    h.every_order(|r| {
        assert_eq!(
            r.state
                .members
                .iter()
                .filter_map(|(id, m)| (!m.removed).then_some(id.clone()))
                .collect::<Vec<_>>(),
            [member(0)]
        );
        assert_eq!(h.drops(r), [7]);
        assert_eq!(r.state.store.as_ref().unwrap().key, key(102));
        assert_eq!(r.state.members[&member(1)].sealing, keys(1).sealing);
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
    h.push(1, 1, &[0, 1, 2, 3], remove(0, &[]));
    h.push(0, 0, &[0, 1, 2, 3], remove(1, &[]));
    h.push(1, 4, &[0, 1, 2, 3], device(5));
    h.every_order(|r| {
        assert_eq!(r.state.devices[&DeviceId(5)].member, member(1));
        assert_eq!(h.drops(r), [5]);
    });
}

#[test]
fn a_drop_persists_after_its_winner_drops_on_restart() {
    let mut h = losing_removal();
    h.push(0, 0, &[0, 1, 2, 3], add(2, Admin));
    h.push(1, 1, &[0, 1, 2, 3], role(1, Member));
    h.push(1, 4, &[0, 1, 2, 3], remove(0, &[]));
    h.every_order(|r| {
        assert!(!r.state.members.contains_key(&member(2)));
        assert_eq!(h.drops(r), [4, 6]);
        assert_eq!(h.reports(r, 0), [4]);
        assert_eq!(h.reports(r, 1), [6]);
        assert_eq!(
            r.entries[&h.entries[4].position],
            EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[6].position))
        );
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
        assert!(!r.state.devices[&DeviceId(4)].removed);
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
            assert_eq!(r.state.devices[&DeviceId(1)].removed, author != 2);
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
        key: key(c + 20),
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
fn concurrent_circle_names_use_the_earlier_stamp() {
    let mut h = gifts();
    h.push(0, 0, &[0, 1, 2, 3, 4], rename(0, "Birthdays"));
    h.push(1, 1, &[0, 1, 2, 3, 4], rename(0, "Presents"));
    h.every_order(|r| {
        assert_eq!(r.state.circles[&circle(0)].name, "Birthdays");
        assert_eq!(h.drops(r), [6]);
    });
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
            assert!(r.state.circles[&circle(0)].deleted);
            assert!(r.state.circles[&circle(0)].members.is_empty());
            assert!(r.state.resets.is_empty());
            assert_eq!(h.drops(r), [5]);
        });
    }
}

#[test]
fn circle_key_rotation_defeats_a_concurrent_addition() {
    let mut h = gifts();
    h.all(0, 0, add(2, Member));
    h.push(0, 0, &[0, 1, 2, 3, 4, 5], join(0, 2));
    h.push(1, 1, &[0, 1, 2, 3, 4, 5], leave(0, 0));
    h.every_order(|r| {
        assert_eq!(r.state.circles[&circle(0)].members, [member(1)].into());
        assert_eq!(r.state.circles[&circle(0)].key, key(20));
        assert_eq!(h.drops(r), [6]);
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
            assert!(!r.state.circles[&circle(0)].deleted);
            assert_eq!(
                r.entries[&h.entries[4].position],
                EntryOutcome::Dropped(DropReason::NotAllowed)
            );
        });
    }
    base.all(1, 1, leave(0, 1));
    base.every_order(|r| {
        assert!(r.state.circles[&circle(0)].deleted);
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
fn removing_carol_rotates_shared_keys_and_deletes_her_private_circle() {
    let mut h = carols_circles();
    h.all(0, 0, remove(2, &[0]));
    h.every_order(|r| {
        assert!(r.state.members[&member(2)].removed);
        assert!(r.state.devices[&DeviceId(2)].removed);
        assert_eq!(r.state.circles[&circle(0)].members, [member(1)].into());
        assert_eq!(r.state.circles[&circle(0)].key, key(200));
        assert!(r.state.circles[&circle(1)].deleted);
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn removing_carol_defeats_bens_concurrent_gift_invitation_for_dan() {
    let mut h = carols_circles().prefix(7);
    h.all(0, 0, add(3, Member));
    h.push(1, 1, &[0, 1, 2, 3, 4, 5, 6, 7], join(0, 3));
    h.push(0, 0, &[0, 1, 2, 3, 4, 5, 6, 7], remove(2, &[0]));
    h.every_order(|r| {
        assert_eq!(r.state.circles[&circle(0)].members, [member(1)].into());
        assert!(!r.state.members[&member(3)].removed);
        assert_eq!(h.drops(r), [8]);
        assert_eq!(h.reports(r, 1), [8]);
    });
}

#[test]
fn circle_key_lists_are_checked_in_the_author_view_including_noops() {
    for circles in [vec![], vec![1], vec![0, 1], vec![0, 2]] {
        let mut h = carols_circles();
        h.all(0, 0, remove(2, &circles));
        h.every_order(|r| {
            assert_eq!(
                r.entries[&h.entries[8].position],
                EntryOutcome::Dropped(DropReason::WrongCircleKeys)
            )
        });
    }
    let mut h = carols_circles();
    h.all(0, 0, remove(2, &[0]));
    h.all(0, 0, remove(2, &[0]));
    h.every_order(|r| {
        assert_eq!(
            r.entries[&h.entries[9].position],
            EntryOutcome::Dropped(DropReason::WrongCircleKeys)
        )
    });
}

#[test]
fn gifts_key_list_does_not_grow_as_concurrent_additions_arrive() {
    let mut h = History::new();
    h.all(0, 0, add(1, Member));
    h.all(0, 0, add(2, Member));
    h.all(0, 0, make(0, "Gifts"));
    h.push(0, 4, &[0, 1, 2, 3], join(0, 1));
    h.push(0, 4, &[0, 1, 2, 3, 4], join(0, 2));
    h.push(0, 0, &[0, 1, 2, 3], remove(1, &[]));
    h.every_order(|r| {
        assert_eq!(
            r.state.circles[&circle(0)].members,
            [member(0), member(2)].into()
        );
        assert!(r.state.members[&member(1)].removed);
        assert_eq!(h.drops(r), [4]);
        assert_eq!(h.reports(r, 0), [4]);
    });
}

#[test]
fn two_removals_can_empty_gifts_without_conflicting_in_their_author_views() {
    let mut h = household(Admin, Member).prefix(3);
    h.all(0, 0, make(0, "Gifts"));
    h.all(0, 0, join(0, 1));
    h.push(0, 0, &[0, 1, 2, 3, 4], leave(0, 1));
    h.push(0, 4, &[0, 1, 2, 3, 4], remove(0, &[0]));
    h.every_order(|r| {
        assert!(r.state.members[&member(0)].removed);
        assert_eq!(r.state.members[&member(1)].role, Admin);
        assert!(r.state.circles[&circle(0)].deleted);
        assert!(h.drops(r).is_empty());
    });
}

#[test]
fn removing_a_circles_only_member_beats_their_concurrent_rename() {
    let mut h = household(Member, Member).prefix(3);
    h.all(1, 1, make(0, "Ben's notes"));
    h.push(1, 1, &[0, 1, 2, 3], rename(0, "Journal"));
    h.push(0, 0, &[0, 1, 2, 3], remove(1, &[]));
    h.every_order(|r| {
        assert!(r.state.circles[&circle(0)].deleted);
        assert_eq!(h.reports(r, 1), [4]);
    });
}

#[test]
fn an_earlier_circle_removal_reverses_a_previously_kept_deletion() {
    let mut h = gifts();
    h.push(0, 0, &[0, 1, 2, 3, 4], leave(0, 1));
    h.push(1, 1, &[0, 1, 2, 3, 4], delete(0));
    let mut arrived = h.entries[..5].to_vec();
    arrived.push(h.entries[6].clone());
    assert!(replay(&arrived).state.circles[&circle(0)].deleted);
    arrived.push(h.entries[5].clone());
    let expected = h.every_order(|r| {
        assert!(!r.state.circles[&circle(0)].deleted);
        assert_eq!(r.state.circles[&circle(0)].members, [member(0)].into());
        assert_eq!(h.reports(r, 1), [6]);
    });
    assert_eq!(replay(&arrived), expected);
}

#[test]
fn winning_removal_drops_all_opponents_before_restarting() {
    let mut h = household(Admin, Member).prefix(3);
    h.push(0, 0, &[0, 1, 2], add(2, Member));
    h.push(0, 0, &[0, 1, 2, 3], add(3, Member));
    h.push(1, 1, &[0, 1, 2], remove(1, &[]));
    h.every_order(|r| {
        assert_eq!(h.drops(r), [3, 4]);
        assert!(!r.state.members.contains_key(&member(2)));
        assert!(!r.state.members.contains_key(&member(3)));
    });
}

#[test]
fn a_new_arrival_reconsiders_entries_dropped_by_the_previous_replay() {
    let mut h = household(Admin, Member).prefix(3);
    h.all(0, 0, make(0, "Gifts"));
    h.all(0, 0, join(0, 1));
    h.push(0, 3, &[0, 1, 2, 3, 4], leave(0, 0));
    h.push(0, 0, &[0, 1, 2, 3, 4], add(2, Admin));
    h.push(
        1,
        1,
        &[0, 1, 2, 3, 4],
        crate::replay::tests::role(1, Member),
    );
    h.push(1, 4, &[0, 1, 2, 3, 4], remove(0, &[0]));
    let arrived: Vec<_> = h
        .entries
        .iter()
        .enumerate()
        .filter_map(|(i, e)| (i != 5).then_some(e.clone()))
        .collect();
    let before = replay(&arrived);
    assert_eq!(
        before.entries[&h.entries[6].position],
        EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[8].position))
    );
    assert_eq!(
        before.entries[&h.entries[8].position],
        EntryOutcome::Dropped(DropReason::NoAdminLeft)
    );
    h.every_order(|r| {
        assert_eq!(h.drops(r), [8]);
        assert_eq!(r.state.members[&member(2)].role, Admin);
        assert_eq!(
            r.entries[&h.entries[8].position],
            EntryOutcome::Dropped(DropReason::BeatenBy(h.entries[5].position))
        );
    });
}

#[test]
fn concurrent_removals_select_the_last_keys_and_retain_the_removed_devices() {
    let mut h = household(Admin, Member);
    h.all(0, 0, add(3, Member));
    h.all(3, 3, device(3));
    h.all(0, 0, add(4, Member));
    h.all(4, 4, device(4));
    h.all(0, 0, make(0, "Gifts"));
    h.all(0, 0, join(0, 3));
    h.all(0, 0, join(0, 4));
    let past: Vec<_> = (0..h.entries.len()).collect();
    for (author, removed, replacement) in [(0, 3, u64::MAX), (1, 4, 0)] {
        h.push(
            author,
            u64::from(author),
            &past,
            StoreChange::RemoveMember {
                member: member(removed),
                key: key(replacement),
                circle_keys: vec![coven_format::store_log::CircleKeyId {
                    circle: circle(0),
                    key: key(replacement),
                }],
            },
        );
    }
    h.every_order(|r| {
        assert_eq!(r.state.store.as_ref().unwrap().key, key(0));
        assert_eq!(r.state.circles[&circle(0)].key, key(0));
        assert_eq!(r.state.circles[&circle(0)].members, [member(0)].into());
        for m in [3, 4] {
            assert!(r.state.members[&member(m)].removed);
            let device = &r.state.devices[&DeviceId(u64::from(m))];
            assert!(device.removed);
            assert_eq!(device.member, member(m));
            assert_eq!(device.name, format!("Device {m}"));
        }
        assert!(h.drops(r).is_empty());
    });
}
