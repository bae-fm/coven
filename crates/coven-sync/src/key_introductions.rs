//! Key provenance: authorized received introductions and their agreed
//! commitments (§11, Appendix C12).

use coven_crypto::{MemberId, StoreKeyring};
use coven_database::StoreLog;
use coven_format::{sealed_key::KeyCopyPrefix, store_log::StoreChange, value::EntryId};
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;
use std::collections::BTreeMap;

/// Only store creation, circle creation and rotation introduce keys (D6 tags 0, 6, 15).
pub(crate) fn introduction(change: &StoreChange) -> Option<(Audience, KeyId, [u8; 32])> {
    match change {
        StoreChange::CreateStore { key, key_hash, .. } => Some((Audience::Store, *key, *key_hash)),
        StoreChange::CreateCircle {
            circle,
            key,
            key_hash,
            ..
        } => Some((Audience::Circle(*circle), *key, *key_hash)),
        StoreChange::RotateKey {
            audience,
            key,
            key_hash,
        } => Some((audience.clone(), *key, *key_hash)),
        _ => None,
    }
}

/// A key named by a received entry its author's recorded view authorized (§11).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Introduction {
    pub(crate) entry: EntryId,
    pub(crate) audience: Audience,
    pub(crate) key: KeyId,
    pub(crate) key_hash: [u8; 32],
}

/// Appendix C12 `receivedIntroductions`, in timestamp order. Replay dropping an
/// entry, including for landing too late, does not revoke its introduction;
/// an unauthorized entry introduces nothing.
pub(crate) fn introductions(log: &StoreLog) -> Vec<Introduction> {
    log.entries
        .iter()
        .filter(|applied| applied.check.authorized())
        .filter_map(|applied| {
            let (audience, key, key_hash) = introduction(&applied.entry.change)?;
            Some(Introduction {
                entry: applied.entry.position,
                audience,
                key,
                key_hash,
            })
        })
        .collect()
}

/// Appendix C12 `hashFor`: the commitment every introduction of this key
/// agrees on. Conflicting commitments for one audience and id accept nothing.
pub(crate) fn hash_for(
    introductions: &[Introduction],
    audience: &Audience,
    key: KeyId,
) -> Option<[u8; 32]> {
    let mut matching = introductions
        .iter()
        .filter(|i| &i.audience == audience && i.key == key);
    let first = matching.next()?.key_hash;
    matching.all(|i| i.key_hash == first).then_some(first)
}

/// Every key that may be accepted: an authorized received introduction with
/// one agreed commitment. These are read, held and shared regardless of replay.
pub(crate) fn needed(log: &StoreLog) -> BTreeMap<(Audience, KeyId), [u8; 32]> {
    let introductions = introductions(log);
    introductions
        .iter()
        .filter_map(|i| {
            hash_for(&introductions, &i.audience, i.key)
                .map(|hash| ((i.audience.clone(), i.key), hash))
        })
        .collect()
}

/// The newest acceptable introduction of an audience, whether or not this
/// device holds it yet. Each entry introduces at most one key, so its entry's
/// timestamp alone orders them; the key-id tiebreak never applies.
pub(crate) fn newest(log: &StoreLog, audience: &Audience) -> Option<KeyId> {
    let needed = needed(log);
    introductions(log)
        .into_iter()
        .rev()
        .find(|i| &i.audience == audience && needed.contains_key(&(i.audience.clone(), i.key)))
        .map(|i| i.key)
}

/// What an opened sealed box names: its key's identity and the SHA-256 of the
/// key bytes. Only the recipient can produce this.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Opened {
    pub(crate) audience: Audience,
    pub(crate) key: KeyId,
    pub(crate) hash: [u8; 32],
}

/// Appendix C12 `acceptedCopy`: a copy addressed to `me` supplies its key only
/// when the opened box names the prefix's key and its bytes match the one
/// agreed commitment. Any other copy is refused by itself (§11).
pub(crate) fn accepts_copy(
    introductions: &[Introduction],
    prefix: &KeyCopyPrefix,
    me: &MemberId,
    opened: &Opened,
) -> bool {
    prefix.member == *me
        && opened.audience == prefix.audience
        && opened.key == prefix.key
        && hash_for(introductions, &prefix.audience, prefix.key) == Some(opened.hash)
}

/// Held material whose hash matches its agreed commitment.
pub(crate) fn accepted(
    ring: Option<&StoreKeyring>,
    needed: &BTreeMap<(Audience, KeyId), [u8; 32]>,
    audience: &Audience,
    key: KeyId,
) -> bool {
    let Some(hash) = needed.get(&(audience.clone(), key)) else {
        return false;
    };
    ring.is_some_and(|ring| match audience {
        Audience::Store => ring
            .store_key(key)
            .is_ok_and(|held| held.check_hash(hash).is_ok()),
        Audience::Circle(circle) => ring
            .circle_key(*circle, key)
            .is_ok_and(|held| held.check_hash(hash).is_ok()),
    })
}

#[cfg(test)]
#[path = "key_introductions_tests.rs"]
mod tests;
