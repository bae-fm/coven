//! Key introductions and sharing with their current audiences.

use crate::SyncError;
use coven_crypto::{
    seal_circle_key, seal_store_key, CircleKey, MemberId, MemberKeys, SealingPublicKey, StoreKey,
    StoreKeyring,
};
use coven_database::{EntryOutcome, StoreLog, StoreLogKeyUpload, StoreLogSealing};
use coven_format::{
    sealed_key::KeyCopyPrefix,
    store_log::{StoreChange, StoreLogEntry},
};
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;
use coven_storage::ObjectPath;
use std::collections::BTreeSet;

pub(crate) fn introduced(change: &StoreChange) -> Option<(Audience, KeyId)> {
    match change {
        StoreChange::CreateStore { key, .. } => Some((Audience::Store, *key)),
        StoreChange::CreateCircle { circle, key, .. } => Some((Audience::Circle(*circle), *key)),
        StoreChange::RotateKey { audience, key, .. } => Some((audience.clone(), *key)),
        _ => None,
    }
}

pub(crate) fn needed(log: &StoreLog) -> BTreeSet<(Audience, KeyId)> {
    log.entries
        .iter()
        .filter(|e| log.replay.entries[&e.entry.position] == EntryOutcome::Kept)
        .filter_map(|e| introduced(&e.entry.change))
        .collect()
}

pub(crate) fn recipients<'a>(
    log: &'a StoreLog,
    audience: &'a Audience,
) -> impl Iterator<Item = (&'a MemberId, &'a SealingPublicKey)> {
    log.replay
        .state
        .members
        .iter()
        .filter_map(move |(id, member)| {
            let included = !member.removed
                && match audience {
                    Audience::Store => true,
                    Audience::Circle(circle) => log
                        .replay
                        .state
                        .circles
                        .get(circle)
                        .is_some_and(|circle| !circle.deleted && circle.members.contains(id)),
                };
            included.then_some((id, &member.sealing))
        })
}

pub(crate) fn path(audience: &Audience, key: KeyId, member: &MemberId) -> ObjectPath {
    match audience {
        Audience::Store => ObjectPath::store_key(key, member),
        Audience::Circle(circle) => ObjectPath::circle_key(*circle, key, member),
    }
}

pub(crate) fn holds(ring: Option<&StoreKeyring>, audience: &Audience, key: KeyId) -> bool {
    ring.is_some_and(|ring| match audience {
        Audience::Store => ring.store_key_ids().any(|id| id == key),
        Audience::Circle(circle) => ring.circle_key_ids(*circle).any(|id| id == key),
    })
}

/// Sharing history requires every key introduced by the kept entries.
pub(crate) fn check_shared_keys(
    log: &StoreLog,
    change: &StoreChange,
    ring: &Option<StoreKeyring>,
) -> Result<(), SyncError> {
    for (audience, key) in needed(log) {
        let sharing = match change {
            StoreChange::AddMember { .. } => audience == Audience::Store,
            StoreChange::AddCircleMember { circle, .. } => audience == Audience::Circle(*circle),
            _ => false,
        };
        if sharing && !holds(ring.as_ref(), &audience, key) {
            return Err(SyncError::KeyUnavailable(key));
        }
    }
    Ok(())
}

pub(crate) fn store_copy(
    key: &StoreKey,
    member: &MemberId,
    recipient: &SealingPublicKey,
) -> Result<StoreLogKeyUpload, SyncError> {
    let path = ObjectPath::store_key(key.id(), member);
    let prefix = KeyCopyPrefix {
        audience: Audience::Store,
        key: key.id(),
        member: member.clone(),
    };
    let sealed = seal_store_key(key, recipient, path.as_str(), &prefix.encode()?)?;
    Ok(StoreLogKeyUpload {
        bytes: prefix.encode_box(&sealed)?,
        path: path.into(),
    })
}

pub(crate) fn circle_copy(
    key: &CircleKey,
    member: &MemberId,
    recipient: &SealingPublicKey,
) -> Result<StoreLogKeyUpload, SyncError> {
    let path = ObjectPath::circle_key(key.circle(), key.id(), member);
    let prefix = KeyCopyPrefix {
        audience: Audience::Circle(key.circle()),
        key: key.id(),
        member: member.clone(),
    };
    let sealed = seal_circle_key(key, recipient, path.as_str(), &prefix.encode()?)?;
    Ok(StoreLogKeyUpload {
        bytes: prefix.encode_box(&sealed)?,
        path: path.into(),
    })
}

pub(crate) fn seal(
    log: &StoreLog,
    entry: &StoreLogEntry,
    ring: Option<&StoreKeyring>,
    member: &MemberKeys,
) -> Result<StoreLogSealing, SyncError> {
    if !crate::store_log_object::author_matches_device(log, entry) {
        return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
    }
    let (_, result) = crate::replay_entry(log, entry.clone());
    if let EntryOutcome::Dropped(reason) = &result.entries[&entry.position] {
        return Err(SyncError::Rejected(reason.clone()));
    }
    // Reusing a name would publish different material at an immutable key path.
    let used: BTreeSet<_> = log
        .entries
        .iter()
        .flat_map(|e| introduced(&e.entry.change))
        .collect();
    if let Some((audience, key)) = introduced(&entry.change) {
        if used.contains(&(audience, key)) {
            return Err(SyncError::KeyAlreadyUsed(key));
        }
    }
    let state = &log.replay.state;
    let mut keys = Vec::new();
    let encryption = if let StoreChange::CreateStore { key, admin, .. } = &entry.change {
        if admin.signing != member.member_id() || admin.sealing != member.sealing_public_key() {
            return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
        }
        ring.ok_or(SyncError::KeyUnavailable(*key))?
            .store_key(*key)?
    } else {
        let id = state
            .store
            .as_ref()
            .expect("authorized entry has a store")
            .key;
        ring.ok_or(SyncError::KeyUnavailable(id))?.store_key(id)?
    };
    let ring = ring.expect("entry holds its encryption key");
    match &entry.change {
        StoreChange::AddMember {
            keys: recipient, ..
        } => {
            for id in ring.store_key_ids() {
                keys.push(store_copy(
                    ring.store_key(id)?,
                    &recipient.signing,
                    &recipient.sealing,
                )?);
            }
        }
        StoreChange::CreateStore {
            key,
            key_hash,
            admin,
            ..
        } => {
            let material = ring.store_key(*key)?;
            material.check_hash(key_hash)?;
            keys.push(store_copy(material, &admin.signing, &admin.sealing)?);
        }
        StoreChange::CreateCircle {
            circle,
            key,
            key_hash,
            ..
        } => {
            let material = ring.circle_key(*circle, *key)?;
            material.check_hash(key_hash)?;
            keys.push(circle_copy(
                material,
                &entry.author,
                &member.sealing_public_key(),
            )?);
        }
        StoreChange::RotateKey {
            audience,
            key,
            key_hash,
        } => match audience {
            Audience::Store => {
                let material = ring.store_key(*key)?;
                material.check_hash(key_hash)?;
                for (id, recipient) in recipients(log, audience) {
                    keys.push(store_copy(material, id, recipient)?);
                }
            }
            Audience::Circle(circle) => {
                let material = ring.circle_key(*circle, *key)?;
                material.check_hash(key_hash)?;
                for (id, recipient) in recipients(log, audience) {
                    keys.push(circle_copy(material, id, recipient)?);
                }
            }
        },
        StoreChange::AddCircleMember {
            circle,
            member: recipient,
        } => {
            for id in ring.circle_key_ids(*circle) {
                keys.push(circle_copy(
                    ring.circle_key(*circle, id)?,
                    recipient,
                    &state.members[recipient].sealing,
                )?);
            }
        }
        _ => (),
    }
    Ok(StoreLogSealing {
        key: encryption.id(),
        keys,
    })
}
