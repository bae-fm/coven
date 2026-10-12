//! Key introductions and sharing with their current audiences.

use crate::SyncError;
use coven_crypto::{
    seal_circle_key, seal_store_key, CircleKey, MemberId, MemberKeys, SealingPublicKey, StoreKey,
    StoreKeyring,
};
use coven_database::{EntryOutcome, KeyUploadBatch, StoreLog, StoreLogSealing};
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
                    Audience::Circle(circle) => {
                        log.replay.state.circles.get(circle).is_some_and(|state| {
                            !log.replay.state.circle_deletions.contains_key(circle)
                                && state.members.contains(id)
                        })
                    }
                };
            included.then_some((id, &member.sealing))
        })
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
    path: &ObjectPath,
    key: &StoreKey,
    member: &MemberId,
    recipient: &SealingPublicKey,
) -> Result<Vec<u8>, SyncError> {
    let prefix = KeyCopyPrefix {
        audience: Audience::Store,
        key: key.id(),
        member: member.clone(),
    };
    let sealed = seal_store_key(key, recipient, path.as_str(), &prefix.encode()?)?;
    Ok(prefix.encode_box(&sealed)?)
}

pub(crate) fn circle_copy(
    path: &ObjectPath,
    key: &CircleKey,
    member: &MemberId,
    recipient: &SealingPublicKey,
) -> Result<Vec<u8>, SyncError> {
    let prefix = KeyCopyPrefix {
        audience: Audience::Circle(key.circle()),
        key: key.id(),
        member: member.clone(),
    };
    let sealed = seal_circle_key(key, recipient, path.as_str(), &prefix.encode()?)?;
    Ok(prefix.encode_box(&sealed)?)
}

pub(crate) fn seal(
    mut copies: KeyUploadBatch,
    log: &StoreLog,
    entry: &StoreLogEntry,
    ring: Option<&StoreKeyring>,
    member: &MemberKeys,
    observed: &[ObservedCopy],
) -> Result<StoreLogSealing, SyncError> {
    if !crate::store_log_object::author_matches_device(log, entry) {
        return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
    }
    let (_, result) = crate::replay_entry(log, entry.clone());
    if let EntryOutcome::Dropped(reason) = &result.entries[&entry.position] {
        return Err(SyncError::Rejected(reason.clone()));
    }
    // A key id identifies one introduction and one committed hash.
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
    let mut needs = Vec::new();
    let mut need =
        |audience: Audience, key: KeyId, member: &MemberId, recipient: &SealingPublicKey| {
            needs.push((
                KeyCopyPrefix {
                    audience,
                    key,
                    member: member.clone(),
                },
                *recipient,
            ));
        };
    match &entry.change {
        StoreChange::AddMember {
            keys: recipient, ..
        } => {
            for id in ring.store_key_ids() {
                need(Audience::Store, id, &recipient.signing, &recipient.sealing);
            }
        }
        StoreChange::CreateStore {
            key,
            key_hash,
            admin,
            ..
        } => {
            ring.store_key(*key)?.check_hash(key_hash)?;
            need(Audience::Store, *key, &admin.signing, &admin.sealing);
        }
        StoreChange::CreateCircle {
            circle,
            key,
            key_hash,
            ..
        } => {
            ring.circle_key(*circle, *key)?.check_hash(key_hash)?;
            need(
                Audience::Circle(*circle),
                *key,
                &entry.author,
                &member.sealing_public_key(),
            );
        }
        StoreChange::RotateKey {
            audience,
            key,
            key_hash,
        } => {
            match audience {
                Audience::Store => ring.store_key(*key)?.check_hash(key_hash)?,
                Audience::Circle(circle) => ring.circle_key(*circle, *key)?.check_hash(key_hash)?,
            }
            for (id, recipient) in recipients(log, audience) {
                need(audience.clone(), *key, id, recipient);
            }
        }
        StoreChange::AddCircleMember {
            circle,
            member: recipient,
        } => {
            for id in ring.circle_key_ids(*circle) {
                need(
                    Audience::Circle(*circle),
                    id,
                    recipient,
                    &state.members[recipient].sealing,
                );
            }
        }
        _ => (),
    }
    for (prefix, recipient) in needs {
        if observed.iter().any(|copy| copy.prefix == prefix) {
            tracing::debug!(?prefix, "entry prerequisite copy already observed");
            continue;
        }
        copies.push(|path| match prefix.audience {
            Audience::Store => store_copy(
                path,
                ring.store_key(prefix.key)?,
                &prefix.member,
                &recipient,
            ),
            Audience::Circle(circle) => circle_copy(
                path,
                ring.circle_key(circle, prefix.key)?,
                &prefix.member,
                &recipient,
            ),
        })?;
    }
    Ok(StoreLogSealing {
        key: encryption.id(),
        keys: copies.into_uploads(),
    })
}

/// A decoded copy observation. The recipient still has to authenticate its box.
pub(crate) struct ObservedCopy {
    pub path: ObjectPath,
    pub prefix: KeyCopyPrefix,
    pub bytes: Vec<u8>,
}

pub(crate) async fn read_copies(
    storage: &dyn coven_storage::Storage,
    store: coven_foundation::id_source::StoreId,
    damages: &mut Vec<crate::DamagedObject>,
) -> Result<Vec<ObservedCopy>, SyncError> {
    let mut copies = Vec::new();
    for object in storage
        .list(&coven_storage::ObjectPrefix::keys(store))
        .await?
    {
        if object.path.store() != store || object.path.key_copy_position().is_none() {
            return Err(coven_storage::StorageFailure::Protocol
                .with_source("key listing returned another layout")
                .into());
        }
        let bytes = storage.read(&object.path).await?;
        match coven_format::sealed_key::SealedKey::decode(&bytes) {
            Ok(sealed) => copies.push(ObservedCopy {
                path: object.path,
                prefix: sealed.prefix().clone(),
                bytes,
            }),
            Err(error) => {
                crate::StoreLogSync::damage(damages, &object.path, crate::Refusal::from(error))?
            }
        }
    }
    Ok(copies)
}

pub(crate) async fn read_store_key(
    storage: &dyn coven_storage::Storage,
    store: coven_foundation::id_source::StoreId,
    key: KeyId,
    member: &MemberKeys,
) -> Result<StoreKey, SyncError> {
    let mut damages = Vec::new();
    for copy in read_copies(storage, store, &mut damages).await? {
        if copy.prefix.audience == Audience::Store
            && copy.prefix.key == key
            && copy.prefix.member == member.member_id()
        {
            return Ok(coven_format::sealed_key::SealedKey::decode(&copy.bytes)?
                .open_store_key(member, copy.path.as_str())?);
        }
    }
    if let Some(damage) = damages.into_iter().next() {
        return Err(damage.into());
    }
    Err(SyncError::KeyUnavailable(key))
}
