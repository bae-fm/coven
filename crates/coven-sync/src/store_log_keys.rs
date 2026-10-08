//! Key introductions, author-view sharing and dropped removals' current audience.

use crate::SyncError;
use coven_crypto::{
    seal_circle_key, seal_store_key, CircleKey, MemberId, MemberKeys, SealingPublicKey, StoreKey,
    StoreKeyring,
};
use coven_database::{EntryOutcome, StoreLog, StoreLogKeyUpload, StoreLogSealing};
use coven_format::store_log::{StoreChange, StoreLogEntry};
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;
use coven_storage::ObjectPath;
use std::collections::BTreeSet;

pub(crate) fn introduced(change: &StoreChange) -> Vec<(Audience, KeyId)> {
    match change {
        StoreChange::CreateStore { key, .. } => vec![(Audience::Store, *key)],
        StoreChange::RemoveMember {
            key, circle_keys, ..
        } => std::iter::once((Audience::Store, *key))
            .chain(
                circle_keys
                    .iter()
                    .map(|k| (Audience::Circle(k.circle), k.key)),
            )
            .collect(),
        StoreChange::CreateCircle { circle, key, .. }
        | StoreChange::RemoveCircleMember { circle, key, .. } => {
            vec![(Audience::Circle(*circle), *key)]
        }
        _ => Vec::new(),
    }
}

pub(crate) fn needed(log: &StoreLog) -> BTreeSet<(Audience, KeyId)> {
    log.entries
        .iter()
        .filter(|e| {
            log.replay.entries[&e.entry.position] == EntryOutcome::Kept
                || is_removal(&e.entry.change)
        })
        .flat_map(|e| introduced(&e.entry.change))
        .collect()
}

fn is_removal(change: &StoreChange) -> bool {
    matches!(
        change,
        StoreChange::RemoveMember { .. } | StoreChange::RemoveCircleMember { .. }
    )
}

pub(crate) fn dropped_removal_keys(log: &StoreLog) -> BTreeSet<(Audience, KeyId)> {
    log.entries
        .iter()
        .filter(|e| {
            matches!(
                log.replay.entries[&e.entry.position],
                EntryOutcome::Dropped(_)
            ) && is_removal(&e.entry.change)
        })
        .flat_map(|e| introduced(&e.entry.change))
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

/// Sharing history requires every key, including introductions from dropped removals.
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

fn store_copy(
    key: &StoreKey,
    member: &MemberId,
    recipient: &SealingPublicKey,
) -> Result<StoreLogKeyUpload, SyncError> {
    let path = ObjectPath::store_key(key.id(), member);
    Ok(StoreLogKeyUpload {
        bytes: seal_store_key(key, recipient, path.as_str())?,
        path: path.into(),
    })
}

fn circle_copy(
    key: &CircleKey,
    member: &MemberId,
    recipient: &SealingPublicKey,
) -> Result<StoreLogKeyUpload, SyncError> {
    let path = ObjectPath::circle_key(key.circle(), key.id(), member);
    Ok(StoreLogKeyUpload {
        bytes: seal_circle_key(key, recipient, path.as_str())?,
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
    for (audience, key) in introduced(&entry.change) {
        if used.contains(&(audience, key)) {
            return Err(SyncError::KeyAlreadyUsed(key));
        }
    }
    let state = &log.replay.state;
    let mut keys = Vec::new();
    let creation;
    let encryption = if let StoreChange::CreateStore { key, admin, .. } = &entry.change {
        if admin.signing != member.member_id() || admin.sealing != member.sealing_public_key() {
            return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
        }
        creation = match ring {
            Some(ring) if ring.store_key_ids().any(|id| id == *key) => {
                ring.store_key(*key)?.clone()
            }
            _ => StoreKey::generate(*key)?,
        };
        keys.push(store_copy(&creation, &admin.signing, &admin.sealing)?);
        &creation
    } else {
        let id = state
            .store
            .as_ref()
            .expect("authorized entry has a store")
            .key;
        ring.ok_or(SyncError::KeyUnavailable(id))?.store_key(id)?
    };
    match &entry.change {
        StoreChange::AddMember {
            keys: recipient, ..
        } => {
            let ring = ring.expect("non-creation entry holds its encryption key");
            for id in ring.store_key_ids() {
                keys.push(store_copy(
                    ring.store_key(id)?,
                    &recipient.signing,
                    &recipient.sealing,
                )?);
            }
        }
        StoreChange::RemoveMember {
            member: removed,
            key,
            circle_keys,
        } => {
            let key = StoreKey::generate(*key)?;
            for (id, recipient) in &state.members {
                if !recipient.removed && id != removed {
                    keys.push(store_copy(&key, id, &recipient.sealing)?);
                }
            }
            for replacement in circle_keys {
                let key = CircleKey::generate(replacement.circle, replacement.key)?;
                for id in &state.circles[&replacement.circle].members {
                    if id != removed {
                        keys.push(circle_copy(&key, id, &state.members[id].sealing)?);
                    }
                }
                // An admin outside this circle retains no plaintext copy.
            }
        }
        StoreChange::CreateCircle { circle, key, .. } => {
            keys.push(circle_copy(
                &CircleKey::generate(*circle, *key)?,
                &entry.author,
                &member.sealing_public_key(),
            )?);
        }
        StoreChange::AddCircleMember {
            circle,
            member: recipient,
        } => {
            let ring = ring.expect("non-creation entry holds its encryption key");
            for id in ring.circle_key_ids(*circle) {
                keys.push(circle_copy(
                    ring.circle_key(*circle, id)?,
                    recipient,
                    &state.members[recipient].sealing,
                )?);
            }
        }
        StoreChange::RemoveCircleMember {
            circle,
            member: removed,
            key,
        } => {
            let key = CircleKey::generate(*circle, *key)?;
            for id in &state.circles[circle].members {
                if id != removed {
                    keys.push(circle_copy(&key, id, &state.members[id].sealing)?);
                }
            }
        }
        _ => (),
    }
    Ok(StoreLogSealing {
        key: encryption.id(),
        keys,
    })
}
