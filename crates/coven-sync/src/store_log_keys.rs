//! Key introductions and sharing with their current audiences.

use crate::key_introductions::{accepted, needed, newest};
use crate::SyncError;
use coven_crypto::{
    seal_circle_key, seal_store_key, CircleKey, MemberId, MemberKeys, SealingPublicKey, StoreKey,
    StoreKeyring,
};
use coven_database::{KeyUploadBatch, StoreLog, StoreLogSealing};
use coven_format::{
    sealed_key::KeyCopyPrefix,
    store_log::{StoreChange, StoreLogEntry},
};
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;
use coven_storage::ObjectPath;
use std::collections::BTreeSet;

pub(crate) fn introduced(change: &StoreChange) -> Option<(Audience, KeyId)> {
    crate::key_introductions::introduction(change).map(|(audience, key, _)| (audience, key))
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

/// Sharing history requires every key the received introductions authorize.
pub(crate) fn check_shared_keys(
    log: &StoreLog,
    change: &StoreChange,
    ring: &Option<StoreKeyring>,
) -> Result<(), SyncError> {
    let needed = needed(log);
    for (audience, key) in needed.keys() {
        let sharing = match change {
            StoreChange::AddMember { .. } => *audience == Audience::Store,
            StoreChange::AddCircleMember { circle, .. } => *audience == Audience::Circle(*circle),
            _ => false,
        };
        if sharing && !accepted(ring.as_ref(), &needed, audience, *key) {
            return Err(SyncError::KeyUnavailable(*key));
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
        sealing: *recipient,
    };
    let sealed = seal_store_key(key, &prefix.sealing, path.as_str(), &prefix.encode()?)?;
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
        sealing: *recipient,
    };
    let sealed = seal_circle_key(key, &prefix.sealing, path.as_str(), &prefix.encode()?)?;
    Ok(prefix.encode_box(&sealed)?)
}

pub(crate) fn seal(
    mut copies: KeyUploadBatch,
    log: &StoreLog,
    entry: &StoreLogEntry,
    ring: Option<&StoreKeyring>,
    member: &MemberKeys,
    observation: &crate::key_introductions::CopyObservation,
) -> Result<StoreLogSealing, SyncError> {
    if !crate::store_log_object::author_matches_device(log, entry) {
        return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
    }
    // Preparation records the whole current log, so this unpublished candidate
    // has no concurrent opponent or publication time yet.
    crate::effects::validate_change(&log.replay.state, &entry.author, &entry.change)
        .map_err(SyncError::Rejected)?;
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
        let ring = ring.ok_or_else(|| {
            SyncError::KeyUnavailable(
                newest(log, &Audience::Store).expect("authorized entry has a store key"),
            )
        })?;
        // Entries are membership records, not app data: they use the newest
        // readable store key, so removal and rotation entries can always be
        // authored. Withholding app data from excluded members is §11's
        // first-attempt selection, applied to writes and snapshots.
        let id = crate::write_seal::readable_key(log, ring, &Audience::Store)?;
        ring.store_key(id)?
    };
    let ring = ring.expect("entry holds its encryption key");
    let shared = needed(log);
    let mut needs = Vec::new();
    let mut need =
        |audience: Audience, key: KeyId, member: &MemberId, recipient: &SealingPublicKey| {
            needs.push(KeyCopyPrefix {
                audience,
                key,
                member: member.clone(),
                sealing: *recipient,
            });
        };
    match &entry.change {
        StoreChange::AddMember {
            keys: recipient, ..
        } => {
            for id in ring.store_key_ids() {
                if accepted(Some(ring), &shared, &Audience::Store, id) {
                    need(Audience::Store, id, &recipient.signing, &recipient.sealing);
                }
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
            let audience = Audience::Circle(*circle);
            for id in ring.circle_key_ids(*circle) {
                if accepted(Some(ring), &shared, &audience, id) {
                    need(
                        audience.clone(),
                        id,
                        recipient,
                        &state.members[recipient].sealing,
                    );
                }
            }
        }
        _ => (),
    }
    let observed = match observation {
        crate::key_introductions::CopyObservation::Complete(copies) => copies.as_slice(),
        crate::key_introductions::CopyObservation::Incomplete(_) => &[],
    };
    for prefix in needs {
        // A copy to another of the member's sealing keys does not stand in.
        if observed.iter().any(|copy| copy.prefix == prefix) {
            tracing::debug!(?prefix, "entry prerequisite copy already observed");
            continue;
        }
        copies.push(|path| match prefix.audience {
            Audience::Store => store_copy(
                path,
                ring.store_key(prefix.key)?,
                &prefix.member,
                &prefix.sealing,
            ),
            Audience::Circle(circle) => circle_copy(
                path,
                ring.circle_key(circle, prefix.key)?,
                &prefix.member,
                &prefix.sealing,
            ),
        })?;
    }
    Ok(StoreLogSealing {
        key: encryption.id(),
        keys: copies.into_uploads(),
    })
}

/// A decoded copy observation. The recipient still has to authenticate its box.
#[derive(Clone, Debug)]
pub(crate) struct ObservedCopy {
    pub path: ObjectPath,
    pub prefix: KeyCopyPrefix,
    pub bytes: Vec<u8>,
}

/// One complete observation of the key-copy logs, or the failure that left it
/// incomplete (§11). A failed listing or read is returned as an error instead.
pub(crate) async fn observe(
    database: &coven_database::Database,
    reads: &crate::pass_reads::PassReads,
    limit: std::num::NonZeroUsize,
    storage: &dyn coven_storage::Storage,
    store: coven_foundation::id_source::StoreId,
) -> Result<crate::key_introductions::CopyObservation, SyncError> {
    let mut damages = Vec::new();
    let copies = read_copies(database, reads, limit, storage, store, &mut damages).await?;
    Ok(crate::key_introductions::CopyObservation::new(
        copies, damages,
    ))
}

pub(crate) async fn read_copies(
    database: &coven_database::Database,
    reads: &crate::pass_reads::PassReads,
    limit: std::num::NonZeroUsize,
    storage: &dyn coven_storage::Storage,
    store: coven_foundation::id_source::StoreId,
    damages: &mut Vec<crate::DamagedObject>,
) -> Result<Vec<ObservedCopy>, SyncError> {
    let mut copies = Vec::new();
    let writers = crate::discovery::writers(database, reads, storage, store).await?;
    for read in crate::discovery::scan(
        database,
        reads,
        storage,
        store,
        coven_database::LogKind::KeyCopy,
        &writers,
        limit,
    )
    .await?
    {
        let object = read.metadata;
        let bytes = read.bytes;
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

/// Open this member's copy of a store key, trying each retained copy in writer/number order.
/// A copy that fails to open, names another key or misses `expected` is refused
/// by itself and a later copy can still supply the key (§11). Without a
/// commitment the key is tentative and its introducing entry must check it.
pub(crate) async fn read_store_key(
    database: &coven_database::Database,
    reads: &crate::pass_reads::PassReads,
    limit: std::num::NonZeroUsize,
    storage: &dyn coven_storage::Storage,
    store: coven_foundation::id_source::StoreId,
    key: KeyId,
    expected: Option<&[u8; 32]>,
    member: &MemberKeys,
) -> Result<StoreKey, SyncError> {
    let mut damages = Vec::new();
    let mut refused = None;
    for copy in read_copies(database, reads, limit, storage, store, &mut damages).await? {
        if copy.prefix.audience != Audience::Store
            || copy.prefix.key != key
            || !copy
                .prefix
                .addressed_to(&member.member_id(), &member.sealing_public_key())
        {
            continue;
        }
        let opened = coven_format::sealed_key::SealedKey::decode(&copy.bytes)
            .map_err(crate::Refusal::from)
            .and_then(|sealed| {
                sealed
                    .open_store_key(member, copy.path.as_str())
                    .map_err(|error| crate::Refusal::Decryption {
                        cause: Some(std::sync::Arc::new(error)),
                    })
            })
            .and_then(|opened| {
                let committed = expected.is_none_or(|hash| opened.check_hash(hash).is_ok());
                if opened.id() == key && committed {
                    Ok(opened)
                } else {
                    Err(crate::Refusal::WrongIdentity { cause: None })
                }
            });
        match opened {
            Ok(opened) => return Ok(opened),
            Err(failure) => {
                refused.get_or_insert(crate::DamagedObject {
                    path: copy.path.as_str().into(),
                    failure,
                });
            }
        }
    }
    match refused {
        Some(damage) => Err(damage.into()),
        None => Err(SyncError::KeyUnavailable(key)),
    }
}
