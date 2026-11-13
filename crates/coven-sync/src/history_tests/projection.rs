//! The JSON projection of a history the Lean differentials compare.

use super::*;
use coven_crypto::SealingKey;
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
            let route = copy.route();
            audiences.insert(route.audience.clone());
            keys.insert((route.audience.clone(), route.key));
            members.insert(route.member);
            credentials.insert(route.sealing);
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
            if let Some(hash) =
                crate::write_seal::held(ring, audience, *key).map(|held| held.hash())
            {
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

    /// The keys one slot introduces, as the model's event: one key in its own
    /// fields, several (store creation's) under `keys`.
    fn introduces(&self, introduced: &[&Introduction]) -> Value {
        let mut keys: Vec<Value> = introduced.iter().map(|i| self.introduction(i)).collect();
        if keys.len() == 1 {
            return keys.remove(0);
        }
        json!({"kind":"introduce", "keys":keys})
    }

    pub(crate) fn slots(&self, history: &History, through: u64) -> Vec<Value> {
        let mut slots = vec![json!({"kind":"other"}); usize::try_from(through).unwrap()];
        let mut introduced: std::collections::BTreeMap<u64, Vec<&Introduction>> =
            Default::default();
        for i in &history.introductions {
            introduced.entry(i.slot.get()).or_default().push(i);
        }
        for (slot, keys) in introduced {
            slots[usize::try_from(slot - 1).unwrap()] = self.introduces(&keys);
        }
        for copy in &history.copies {
            let r = copy.route();
            let slot = copy.path.slot_number().unwrap().get();
            let index = usize::try_from(slot - 1).unwrap();
            let route = json!({"audience":self.audience(&r.audience),
                "key":self.key(&r.audience, r.key), "member":self.member(r.member),
                "credential":token(&self.credentials, &r.sealing)});
            if slots[index] == json!({"kind":"other"}) {
                slots[index] = json!({"kind":"copy"});
                slots[index].as_object_mut().unwrap().extend(route.as_object().unwrap().clone());
            } else {
                // The slot also introduces keys, as an admission's and store
                // creation's do: its copies ride beside the event.
                let copies =
                    slots[index].as_object_mut().unwrap().entry("copies").or_insert(json!([]));
                copies.as_array_mut().unwrap().push(route);
            }
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
                crate::write_seal::held(ring, a, *k).map(|held| held.hash()).map(|hash| {
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
                    Err(SyncError::KeyUnavailable { key: k, .. }) => {
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
