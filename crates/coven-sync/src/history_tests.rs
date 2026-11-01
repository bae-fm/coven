use super::*;
use coven_crypto::{CircleKey, SealingKey, StoreKey};
use coven_database::{StoreCircle, StoreMember};
use coven_format::{admin::MemberRole, sealed_key::CopyBox, sealed_slot::SlotKind};
use coven_foundation::id_source::StoreId;
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// Equality-preserving tokens at the JSON boundary, shared by selection and
/// publication comparisons. No selection or exposure decision is made here.
pub(crate) struct Projection {
    audiences: Vec<Audience>,
    keys: Vec<(Audience, KeyId)>,
    members: Vec<MemberId>,
    credentials: Vec<SealingKey>,
    hashes: Vec<[u8; 32]>,
}

fn token<T: PartialEq>(values: &[T], value: &T) -> usize {
    values.iter().position(|v| v == value).unwrap()
}

impl Projection {
    pub(crate) fn new(history: &History, ring: &StoreKeyring) -> Self {
        let mut audiences = BTreeSet::from([Audience::Store]);
        audiences.extend(history.state.circles.keys().copied().map(Audience::Circle));
        audiences.extend(history.introductions.iter().map(|i| i.audience.clone()));
        let mut keys: BTreeSet<_> =
            history.introductions.iter().map(|i| (i.audience.clone(), i.key)).collect();
        let mut members: BTreeSet<_> = history.state.members.keys().copied().collect();
        let mut credentials: BTreeSet<_> =
            history.state.members.values().map(|m| m.keys.sealing).collect();
        for copy in &history.copies {
            let SlotKind::KeyCopy(copy) = &copy.prefix.kind else {
                panic!("key routing");
            };
            audiences.insert(copy.routing.audience.clone());
            keys.insert((copy.routing.audience.clone(), copy.routing.key));
            members.insert(copy.routing.member);
            credentials.insert(copy.routing.sealing);
        }
        for audience in &audiences {
            let ids: Vec<_> = match audience {
                Audience::Store => ring.store_key_ids().collect(),
                Audience::Circle(circle) => ring.circle_key_ids(*circle).collect(),
            };
            keys.extend(ids.into_iter().map(|id| (audience.clone(), id)));
        }
        let mut hashes: BTreeSet<_> = history.introductions.iter().map(|i| i.hash).collect();
        for (audience, key) in &keys {
            if let Some(hash) = held_hash(ring, audience, *key) {
                hashes.insert(hash);
            }
        }
        Self {
            audiences: audiences.into_iter().collect(),
            keys: keys.into_iter().collect(),
            members: members.into_iter().collect(),
            credentials: credentials.into_iter().collect(),
            hashes: hashes.into_iter().collect(),
        }
    }

    pub(crate) fn audience(&self, audience: &Audience) -> usize {
        token(&self.audiences, audience)
    }

    pub(crate) fn key(&self, audience: &Audience, key: KeyId) -> usize {
        token(&self.keys, &(audience.clone(), key))
    }

    pub(crate) fn member(&self, member: MemberId) -> usize {
        token(&self.members, &member)
    }

    pub(crate) fn introduction(&self, i: &Introduction) -> Value {
        json!({"kind":"introduce", "audience":self.audience(&i.audience),
            "key":self.key(&i.audience, i.key), "hash":token(&self.hashes, &i.hash)})
    }

    pub(crate) fn slots(&self, history: &History, through: u64) -> Vec<Value> {
        let mut slots = vec![json!({"kind":"other"}); usize::try_from(through).unwrap()];
        for i in &history.introductions {
            slots[usize::try_from(i.slot.0 - 1).unwrap()] = self.introduction(i);
        }
        for copy in &history.copies {
            let SlotKind::KeyCopy(copy_box) = &copy.prefix.kind else {
                panic!("key routing");
            };
            let r = &copy_box.routing;
            let slot = copy.path.slot_number().unwrap().0;
            let index = usize::try_from(slot - 1).unwrap();
            assert_eq!(slots[index], json!({"kind":"other"}));
            slots[index] = json!({"kind":"copy", "audience":self.audience(&r.audience),
                "key":self.key(&r.audience, r.key), "member":self.member(r.member),
                "credential":token(&self.credentials, &r.sealing)});
        }
        slots
    }

    pub(crate) fn input(
        &self,
        history: &History,
        ring: &StoreKeyring,
        writer: MemberId,
        through: u64,
    ) -> Value {
        let mut current = Vec::new();
        for audience in &self.audiences {
            for (member, value) in &history.state.members {
                if effects::belongs(&history.state, audience, member) {
                    current.push(json!({"audience":self.audience(audience),
                        "member":self.member(*member), "credential":token(&self.credentials, &value.keys.sealing)}));
                }
            }
        }
        let held: Vec<_> = self
            .keys
            .iter()
            .filter_map(|(a, k)| {
                held_hash(ring, a, *k).map(|hash| {
                    json!({"audience":self.audience(a),
                "key":self.key(a, *k), "hash":token(&self.hashes, &hash)})
                })
            })
            .collect();
        json!({"kind":"keys", "slots":self.slots(history, through), "held":held,
            "current":current, "writer":self.member(writer), "store":self.audience(&Audience::Store),
            "audiences":(0..self.audiences.len()).collect::<Vec<_>>(),
            "keys":self.keys.iter().map(|(a,k)| json!({"audience":self.audience(a), "key":self.key(a,*k)})).collect::<Vec<_>>()})
    }

    pub(crate) fn result(&self, history: &History, ring: &StoreKeyring, writer: MemberId) -> Value {
        let selections: Vec<_> = self
            .audiences
            .iter()
            .map(|a| {
                let (selected, sending) = match history.sending_key(ring, writer, a) {
                    Ok(k) => (Some(self.key(a, k)), json!({"key":self.key(a,k)})),
                    Err(SyncError::Removed) => (None, json!({"error":"removed"})),
                    Err(SyncError::KeyUnavailable(k)) => {
                        (None, json!({"unavailable":self.key(a,k)}))
                    }
                    Err(SyncError::Rejected(OperationRejection::NotAllowed)) => {
                        (None, json!({"error":"notAllowed"}))
                    }
                    Err(error) => panic!("unexpected key-selection failure: {error:?}"),
                };
                let first =
                    if effects::belongs(&history.state, a, &writer) { selected } else { None };
                json!({"audience":self.audience(a), "first":first, "sending":sending})
            })
            .collect();
        let metadata = match history.metadata_key(ring) {
            Ok(k) => Some(self.key(&Audience::Store, k)),
            Err(SyncError::Rejected(OperationRejection::NotAllowed)) => None,
            Err(error) => panic!("unexpected metadata-key failure: {error:?}"),
        };
        json!({"keys":self.keys.iter().map(|(a,k)| json!({"audience":self.audience(a),
            "key":self.key(a,*k), "accepted":history.accepted(ring,a,*k),
            "exposed":history.exposed(a,*k)})).collect::<Vec<_>>(), "selection":selections, "metadata":metadata})
    }
}

fn key(n: u128) -> KeyId {
    KeyId(uuid::Uuid::from_u128(n))
}

fn member(n: u128) -> MemberId {
    MemberId(uuid::Uuid::from_u128(n))
}

fn copy(history: &mut History, ring: &StoreKeyring, slot: u64, routing: KeyCopyPrefix) {
    let path = coven_format::path::ObjectPath::slot(
        StoreId(uuid::Uuid::from_u128(1)),
        std::num::NonZeroU64::new(slot).unwrap(),
    );
    let outer = key(90);
    let format = coven_format::FormatVersion::CURRENT;
    let context = SlotObjectPrefix::copy_context(format, outer, &routing).unwrap();
    let sealed = match routing.audience {
        Audience::Store => coven_crypto::seal_store_key(
            ring.store_key(routing.key).unwrap(),
            &routing.sealing,
            path.as_str(),
            &context,
        )
        .unwrap(),
        Audience::Circle(circle) => coven_crypto::seal_circle_key(
            ring.circle_key(circle, routing.key).unwrap(),
            &routing.sealing,
            path.as_str(),
            &context,
        )
        .unwrap(),
    };
    history.copies.push(KeyCopy {
        path,
        prefix: SlotObjectPrefix {
            format,
            key: outer,
            kind: SlotKind::KeyCopy(CopyBox { routing, sealed }),
        },
    });
}

#[test]
#[ignore = "requires COVEN_SEQUENCE_LEAN"]
fn sequence_key_selection_matches_lean() {
    let circle = CircleId(uuid::Uuid::from_u128(1));
    let unused_circle = CircleId(uuid::Uuid::from_u128(2));
    let keys = [
        (Audience::Store, key(90)),
        (Audience::Store, key(10)),
        (Audience::Circle(circle), key(80)),
        (Audience::Circle(circle), key(20)),
    ];
    let ana = coven_format::test_utils::keys_from_seed(1).public_keys();
    let ben = coven_format::test_utils::keys_from_seed(2).public_keys();
    let replaced = coven_format::test_utils::keys_from_seed(3).public_keys();
    let candidate = coven_format::test_utils::keys_from_seed(4).public_keys();
    let mut material = StoreKeyring::new(StoreKey::from_bytes(key(90), [1; 32]));
    material.insert_store_key(StoreKey::from_bytes(key(10), [2; 32])).unwrap();
    material.insert_circle_key(CircleKey::from_bytes(circle, key(80), [3; 32])).unwrap();
    material.insert_circle_key(CircleKey::from_bytes(circle, key(20), [4; 32])).unwrap();
    material.insert_store_key(StoreKey::from_bytes(key(30), [5; 32])).unwrap();
    for mode in 0..9 {
        let mut history = History::default();
        for (id, public) in [(member(1), ana), (member(2), ben)] {
            history.state.members.insert(
                id,
                StoreMember {
                    keys: public,
                    access: crate::sequence_sync::tests::access("fixture"),
                    role: MemberRole::Member,
                    removed: id == member(2) && matches!(mode, 1 | 4),
                },
            );
        }
        if mode == 2 {
            history.state.members.get_mut(&member(2)).unwrap().keys = replaced;
        }
        history.state.circles.insert(
            circle,
            StoreCircle {
                name: "Circle".into(),
                members: if mode == 5 {
                    BTreeSet::from([member(1)])
                } else {
                    BTreeSet::from([member(1), member(2)])
                },
            },
        );
        history.state.circles.insert(
            unused_circle,
            StoreCircle { name: "No introduction".into(), members: BTreeSet::new() },
        );
        for (index, (audience, id)) in keys.iter().enumerate() {
            history.introductions.push(Introduction {
                slot: Slot(if index == 0 { 1 } else { 2 * index as u64 + 2 }),
                audience: audience.clone(),
                key: *id,
                hash: held_hash(&material, audience, *id).unwrap(),
            });
            let recipient = if mode == 3 && index % 2 == 1 {
                member(3)
            } else if mode == 4 || index % 2 == 1 {
                member(2)
            } else {
                member(1)
            };
            copy(
                &mut history,
                &material,
                if index == 0 { 2 } else { 2 * index as u64 + 1 },
                KeyCopyPrefix {
                    audience: audience.clone(),
                    key: *id,
                    member: recipient,
                    sealing: if recipient == member(1) {
                        ana.sealing
                    } else if recipient == member(3) {
                        candidate.sealing
                    } else {
                        ben.sealing
                    },
                },
            );
        }
        copy(
            &mut history,
            &material,
            9,
            KeyCopyPrefix {
                audience: Audience::Store,
                key: key(30),
                member: member(1),
                sealing: ana.sealing,
            },
        );
        let duplicate = history.copies[1].clone();
        let SlotKind::KeyCopy(duplicate) = duplicate.prefix.kind else {
            panic!("copy");
        };
        copy(&mut history, &material, 10, duplicate.routing);
        if matches!(mode, 6 | 7) {
            let mut repeated = history.introductions[0].clone();
            repeated.slot = Slot(11);
            if mode == 7 {
                repeated.hash = [99; 32];
            }
            history.introductions.push(repeated);
        }
        let through = if matches!(mode, 6 | 7) { 11 } else { 10 };
        for mask in 0..16 {
            // An unrelated tentative store key allows an empty accepted custody set.
            let mut ring = StoreKeyring::new(material.store_key(key(30)).unwrap().clone());
            for (index, (audience, id)) in keys.iter().enumerate() {
                if mask & (1 << index) != 0 {
                    match audience {
                        Audience::Store => ring
                            .insert_store_key(if mode == 8 && index == 1 {
                                StoreKey::from_bytes(*id, [99; 32])
                            } else {
                                material.store_key(*id).unwrap().clone()
                            })
                            .unwrap(),
                        Audience::Circle(c) => ring
                            .insert_circle_key(material.circle_key(*c, *id).unwrap().clone())
                            .unwrap(),
                    }
                }
            }
            let projection = Projection::new(&history, &ring);
            for writer in [member(1), member(2)] {
                let input = projection.input(&history, &ring, writer, through);
                assert_eq!(
                    crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
                    projection.result(&history, &ring, writer),
                    "mode {mode}, mask {mask}, writer {writer}"
                );
            }
        }
        // Copy-before-introduction and creation-before-copy prefixes, including
        // unaccepted custody and conflicting commitments, use the same selector.
        for end in 0..=through {
            let mut prefix = history.clone();
            prefix.introductions.retain(|i| i.slot.0 <= end);
            prefix.copies.retain(|c| c.path.slot_number().unwrap().0 <= end);
            let projection = Projection::new(&history, &material);
            let input = projection.input(&prefix, &material, member(1), end);
            assert_eq!(
                crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
                projection.result(&prefix, &material, member(1)),
                "mode {mode}, prefix {end}"
            );
        }
    }
}

/// Ana's phone (device 1) makes Household and adds Ben as an admin; Ben's
/// phone (device 2) and Ana's laptop (device 3) register. Each entry goes
/// through the checks a slot's does, so only what they accept counts.
struct Household {
    history: History,
    slots: u64,
    ana: MemberId,
    ben: MemberId,
}

impl Household {
    fn new() -> Self {
        Self::with_access(crate::sequence_sync::tests::access("ana"))
    }

    /// Household on a provider that shares it with accounts: Ana's account
    /// holds it.
    fn shared() -> Self {
        Self::with_access(coven_format::MemberAccess::ProviderAccount("ana@example.org".into()))
    }

    fn with_access(access: coven_format::MemberAccess) -> Self {
        let mut household =
            Self { history: History::default(), slots: 0, ana: member(1), ben: member(2) };
        let (ana, ben) = (household.ana, household.ben);
        household.entry(
            1,
            ana,
            StoreChange::CreateStore {
                store: StoreId(uuid::Uuid::from_u128(1)),
                name: "Household".into(),
                admin: coven_format::test_utils::keys_from_seed(1).public_keys(),
                access,
                device_name: "Ana's phone".into(),
                key: key(90),
                key_hash: [1; 32],
            },
        );
        household.entry(
            1,
            ana,
            StoreChange::AddMember {
                member: ben,
                keys: coven_format::test_utils::keys_from_seed(2).public_keys(),
                role: MemberRole::Admin,
                access: crate::sequence_sync::tests::access("ben"),
            },
        );
        household.entry(
            2,
            ben,
            StoreChange::AddDevice { name: "Ben's phone".into(), replaces: None },
        );
        household.entry(
            3,
            ana,
            StoreChange::AddDevice { name: "Ana's laptop".into(), replaces: None },
        );
        household
    }

    /// Claim the next slot with `change`, written by `device` for `author`.
    fn entry(&mut self, device: u64, author: MemberId, change: StoreChange) -> Slot {
        self.slots += 1;
        let slot = Slot(self.slots);
        let header =
            SlotHeader { device: DeviceId(device), claim: slot.0, author, after: slot.0 - 1 };
        let effect = self.history.validate(&header, slot, &change).unwrap().unwrap();
        self.history.accept_admin(slot, &header, &effect);
        slot
    }

    /// Validate `change` as the next slot, written by `device` for `author`.
    fn check(
        &self,
        device: u64,
        author: MemberId,
        change: StoreChange,
    ) -> Result<Option<AdminEffect>, OperationRejection> {
        let slot = Slot(self.slots + 1);
        let header =
            SlotHeader { device: DeviceId(device), claim: slot.0, author, after: slot.0 - 1 };
        self.history.validate(&header, slot, &change)
    }

    /// Replace Ana's keys with seed `seed`'s, written by `device`.
    fn replace_anas_keys(&mut self, device: u64, seed: u8) -> Slot {
        let expected = self.history.state.members[&self.ana].keys;
        let keys = coven_format::test_utils::keys_from_seed(seed).public_keys();
        self.entry(
            device,
            self.ana,
            StoreChange::ReplaceMemberKeys { member: self.ana, expected, keys },
        )
    }
}

/// A device's removal, or its member's, stops it for good; so does a reset
/// registering another installation in its place. The other devices go on
/// (§14.1).
#[test]
fn removal_and_replacement_stop_a_device_for_good() {
    let mut removed = Household::new();
    let ben = removed.ben;
    let at = removed.entry(2, ben, StoreChange::RemoveDevice { device: DeviceId(3) });
    assert_eq!(removed.history.gone_for_good(DeviceId(3)), Some((at, DeviceStop::Removed)));
    assert_eq!(removed.history.gone_for_good(DeviceId(1)), None);
    assert_eq!(removed.history.gone_for_good(DeviceId(2)), None);

    let mut left = Household::new();
    let (ana, ben) = (left.ana, left.ben);
    let at = left.entry(2, ben, StoreChange::RemoveMember { member: ana });
    for device in [DeviceId(1), DeviceId(3)] {
        assert_eq!(left.history.gone_for_good(device), Some((at, DeviceStop::Removed)));
    }
    assert_eq!(left.history.gone_for_good(DeviceId(2)), None);

    let mut reset = Household::new();
    let at = reset.entry(
        4,
        ana,
        StoreChange::AddDevice { name: "Ana's phone".into(), replaces: Some(DeviceId(1)) },
    );
    assert_eq!(reset.history.gone_for_good(DeviceId(1)), Some((at, DeviceStop::Replaced)));
    assert_eq!(reset.history.gone_for_good(DeviceId(4)), None);
    // A device history never added isn't gone: there is nothing of it to end.
    assert_eq!(reset.history.gone_for_good(DeviceId(9)), None);
}

/// Ana's phone replaces her keys: it holds the new pair, so it goes on, and
/// her laptop, added before and holding only the old pair, is gone for good.
/// A device added after the replacement registered with the new pair, so the
/// replacement doesn't stop it (§12.3, §14.1).
#[test]
fn a_member_key_replacement_stops_only_her_other_devices_added_before_it() {
    let mut household = Household::new();
    let ana = household.ana;
    let at = household.replace_anas_keys(1, 5);
    assert_eq!(household.history.gone_for_good(DeviceId(1)), None);
    assert_eq!(
        household.history.gone_for_good(DeviceId(3)),
        Some((at, DeviceStop::MemberKeysReplaced))
    );
    assert_eq!(household.history.gone_for_good(DeviceId(2)), None);
    household.entry(1, ana, StoreChange::RemoveDevice { device: DeviceId(3) });
    household.entry(5, ana, StoreChange::AddDevice { name: "Ana's tablet".into(), replaces: None });
    // The first entry that stopped the laptop is what stopped it.
    assert_eq!(
        household.history.gone_for_good(DeviceId(3)),
        Some((at, DeviceStop::MemberKeysReplaced))
    );
    assert_eq!(household.history.gone_for_good(DeviceId(5)), None);
}

fn invite(n: u128) -> coven_foundation::id_source::InviteId {
    coven_foundation::id_source::InviteId(uuid::Uuid::from_u128(n))
}

fn share(n: u128, account: &str) -> StoreChange {
    StoreChange::AddInviteShare { invite: invite(n), account: account.into() }
}

/// Ana's phone shares Household with Carol for an invite, and her laptop
/// for another. The account stays meant to be shared while either share is
/// open: the phone's ends when Ben removes the phone, the laptop's when the
/// laptop ends it. Accounts compare without case (§13.2, §14.2).
#[test]
fn an_invite_share_counts_until_it_ends_or_its_device_is_gone_for_good() {
    let mut household = Household::shared();
    let (ana, ben) = (household.ana, household.ben);
    assert!(!household.history.account_intended("carol@example.org"));
    household.entry(1, ana, share(1, "Carol@example.org"));
    assert!(household.history.account_intended("carol@example.org"));
    household.entry(3, ana, share(2, "carol@example.org"));
    household.entry(2, ben, StoreChange::RemoveDevice { device: DeviceId(1) });
    assert!(household.history.account_intended("carol@example.org"));
    household.entry(3, ana, StoreChange::EndInviteShare { invite: invite(2) });
    assert!(!household.history.account_intended("carol@example.org"));
}

/// Only a device of the member whose account holds the store records a
/// share, once for each invite, and only that device ends it. Recording it
/// again, or ending it again, changes nothing (§13.2, format.md D6).
#[test]
fn only_the_owners_device_records_an_invite_share_and_only_it_ends_it() {
    let mut household = Household::shared();
    let (ana, ben) = (household.ana, household.ben);
    // Ben is an admin, but his account doesn't hold the store.
    assert_eq!(
        household.check(2, ben, share(1, "carol@example.org")).err(),
        Some(OperationRejection::NotAllowed)
    );
    household.entry(1, ana, share(1, "carol@example.org"));
    assert!(household.check(1, ana, share(1, "CAROL@example.org")).unwrap().is_none());
    for (device, change) in [
        (3, share(1, "carol@example.org")),
        (1, share(1, "dan@example.org")),
        (3, StoreChange::EndInviteShare { invite: invite(1) }),
        (1, StoreChange::EndInviteShare { invite: invite(9) }),
    ] {
        assert_eq!(
            household.check(device, ana, change.clone()).err(),
            Some(OperationRejection::NotAllowed),
            "{change:?}"
        );
    }
    household.entry(1, ana, StoreChange::EndInviteShare { invite: invite(1) });
    assert!(household
        .check(1, ana, StoreChange::EndInviteShare { invite: invite(1) })
        .unwrap()
        .is_none());
    // An ended invite's id is never shared again.
    assert_eq!(
        household.check(1, ana, share(1, "carol@example.org")).err(),
        Some(OperationRejection::NotAllowed)
    );
}

/// On S3 nothing is shared with accounts, so no invite records a share
/// (§14.3).
#[test]
fn an_s3_store_records_no_invite_share() {
    let household = Household::new();
    let ana = household.ana;
    assert_eq!(
        household.check(1, ana, share(1, "carol@example.org")).err(),
        Some(OperationRejection::NotAllowed)
    );
}
