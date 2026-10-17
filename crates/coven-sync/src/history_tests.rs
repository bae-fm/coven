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
        let mut keys: BTreeSet<_> = history
            .introductions
            .iter()
            .map(|i| (i.audience.clone(), i.key))
            .collect();
        let mut members: BTreeSet<_> = history.state.members.keys().copied().collect();
        let mut credentials: BTreeSet<_> = history
            .state
            .members
            .values()
            .map(|m| m.keys.sealing)
            .collect();
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
                let first = if effects::belongs(&history.state, a, &writer) {
                    selected
                } else {
                    None
                };
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
    material
        .insert_store_key(StoreKey::from_bytes(key(10), [2; 32]))
        .unwrap();
    material
        .insert_circle_key(CircleKey::from_bytes(circle, key(80), [3; 32]))
        .unwrap();
    material
        .insert_circle_key(CircleKey::from_bytes(circle, key(20), [4; 32]))
        .unwrap();
    material
        .insert_store_key(StoreKey::from_bytes(key(30), [5; 32]))
        .unwrap();
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
            StoreCircle {
                name: "No introduction".into(),
                members: BTreeSet::new(),
            },
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
            prefix
                .copies
                .retain(|c| c.path.slot_number().unwrap().0 <= end);
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
