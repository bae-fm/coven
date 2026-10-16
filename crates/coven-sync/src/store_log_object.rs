//! Authentication and causal checks before the replay's valid-input boundary.

use crate::{Refusal, SyncError};
use coven_crypto::{MemberIdentity, ObjectHasher, StoreKey, StoreKeyring};
use coven_database::{Database, StoreLog, StoreLogUpload};

pub(crate) use crate::entry_signature::OpenedEntry;
use coven_format::{
    sealed_key::SealedKey,
    sealed_single::{SingleChunkObject, SingleChunkPrefix},
    store_log::{StoreChange, StoreLogEntry},
    value::EntryId,
    Object,
};
use coven_foundation::id_source::StoreId;
use coven_storage::{ObjectPath, Storage};
use std::sync::Arc;

/// Reproduce an entry using the key fixed with its plaintext queue record.
/// Every attempt signs with the held key, which must be the one the entry
/// verifies with: creation's supplied pair, or else its author's key in the
/// replay of its recorded past in `log`. Newer custody never re-signs an
/// entry fixed under replaced keys (§10, D6).
pub(crate) fn seal_upload(
    store: StoreId,
    log: &StoreLog,
    upload: &StoreLogUpload,
    ring: Option<&StoreKeyring>,
    member: &MemberIdentity,
) -> Result<Vec<u8>, SyncError> {
    if upload.entry.author != member.member() {
        return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
    }
    let signer = match &upload.entry.change {
        StoreChange::CreateStore { admin, .. } => Some(admin.signing),
        _ => crate::object_author::recorded_signer(
            log,
            &crate::replay::author_positions(&upload.entry),
            &upload.entry.author,
        ),
    };
    crate::object_author::check_signer(member, signer)?;
    let id = upload.sealing.key;
    let creation;
    let key = if let StoreChange::CreateStore { key_hash, .. } = &upload.entry.change {
        // Before custody commits, creation can recover its key from the queue.
        // After confirmation the caller acquires the published copy into `ring`.
        if let Some(copy) = upload.sealing.keys.first() {
            if upload.sealing.keys.len() != 1 {
                return Err(coven_database::DbError::Invariant {
                    what: "a store's creation seals exactly its first key",
                }
                .into());
            }
            creation = SealedKey::decode(&copy.bytes)?
                .open_store_key(member, copy.path(store).as_str())?;
        } else {
            creation = ring
                .ok_or(SyncError::KeyUnavailable(id))?
                .store_key(id)?
                .clone();
        }
        if creation.id() != id {
            return Err(coven_database::DbError::Invariant {
                what: "a creation's sealed copy holds the key it introduces",
            }
            .into());
        }
        creation.check_hash(key_hash)?;
        &creation
    } else {
        ring.ok_or(SyncError::KeyUnavailable(id))?.store_key(id)?
    };
    seal(store, &upload.entry, upload.format, key, member)
}

/// Publish the sealed copies before the entry that introduces or shares them.
pub(crate) async fn upload(
    store: StoreId,
    storage: Option<&dyn Storage>,
    database: &Database,
    send: &crate::SendGate,
    upload: &StoreLogUpload,
    bytes: &[u8],
) -> Result<(), SyncError> {
    upload_keys(store, storage, database, send).await?;
    create_entry(
        store,
        storage.ok_or(SyncError::NoStorage)?,
        database,
        send,
        upload.entry.position,
        bytes,
    )
    .await
}

/// Create this device's entry. A create reply that carries the publication
/// joins the retained input, as a read would, so neither its landing check
/// nor discovery reads it again (SP steps 4 and 5).
pub(crate) async fn create_entry(
    store: StoreId,
    storage: &dyn Storage,
    database: &Database,
    send: &crate::SendGate,
    entry: EntryId,
    bytes: &[u8],
) -> Result<(), SyncError> {
    if let Some(created) = create_own(storage, send, &path(store, entry), bytes).await? {
        database
            .keep_log_inputs(
                coven_database::LogKind::Entry,
                vec![coven_database::LogInput {
                    writer: entry.device,
                    number: std::num::NonZeroU64::new(entry.number)
                        .ok_or(coven_storage::StorageFailure::InvalidPath)?,
                    observation: serde_json::to_vec(&created)?,
                    bytes: bytes.to_vec(),
                }],
            )
            .await?;
    }
    Ok(())
}

/// Create one of this device's immutable objects, returning the reply's
/// metadata when the provider sent it. An occupied path counts as stored
/// only with equal complete bytes; others reset this copy (§10).
pub(crate) async fn create_own(
    storage: &dyn Storage,
    send: &crate::SendGate,
    path: &ObjectPath,
    bytes: &[u8],
) -> Result<Option<coven_storage::StoredObject>, SyncError> {
    match storage.create(path, bytes).await {
        Ok(created) => Ok(created),
        Err(error @ coven_storage::StorageError::Cleanup { .. }) => Err(error.into()),
        Err(error) if error.failure() == coven_storage::StorageFailure::AlreadyExists => {
            crate::identity::settle_occupied(storage, send, path, crate::identity::digest(bytes))
                .await
                .map(|()| None)
        }
        Err(error) => Err(error.into()),
    }
}

/// Settle every reserved copy in writer/number order, regardless of its current
/// recipient or initiating operation. Each confirmation commits before the next send.
/// Each confirmed copy also extends the sender's observed copy set, so a later
/// failure cannot hide an exposure that already happened.
pub(crate) async fn upload_keys(
    store: StoreId,
    storage: Option<&dyn Storage>,
    database: &Database,
    send: &crate::SendGate,
) -> Result<(), SyncError> {
    for copy in database.key_uploads().await? {
        create_own(
            storage.ok_or(SyncError::NoStorage)?,
            send,
            &copy.path(store),
            &copy.bytes,
        )
        .await?;
        database.complete_key_upload(copy.clone()).await?;
        send.published_copy(store, &copy)?;
    }
    Ok(())
}

pub(crate) fn seal(
    store: StoreId,
    entry: &StoreLogEntry,
    format: coven_format::FormatVersion,
    key: &StoreKey,
    member: &MemberIdentity,
) -> Result<Vec<u8>, SyncError> {
    let coven_format::FormatVersion::V1 = format;
    let path = path(store, entry.position);
    let prefix = SingleChunkPrefix::StoreLog { key: key.id() };
    let plain = Object::StoreLog(entry.clone()).encode_in(format)?;
    let chunk =
        key.derive()
            .reseal_object_chunk(path.as_str(), &prefix.encode_in(format)?, 0, 0, &plain);
    let mut bytes = prefix.encode_chunk_in(format, &chunk)?;
    let mut hash = ObjectHasher::new();
    hash.update(&bytes);
    bytes.extend_from_slice(
        member
            .keys()
            .sign_object(path.as_str(), &hash.finish())
            .as_bytes(),
    );
    Ok(bytes)
}

pub(crate) fn path(store: StoreId, id: EntryId) -> ObjectPath {
    ObjectPath::store_log(
        store,
        id.device,
        id.number.try_into().expect("positive checked entry number"),
    )
}

/// Decrypt one entry and check its path and store identity. Its signature
/// is checked by [`OpenedEntry::verify`] once its recorded view is known.
pub(crate) fn open(
    object: &SingleChunkObject<'_>,
    path: &ObjectPath,
    key: &StoreKey,
) -> Result<OpenedEntry, Refusal> {
    let SingleChunkObject::StoreLog {
        key: declared,
        signature,
        ..
    } = object
    else {
        // The path names a store-log entry; another object kind there has the
        // wrong identity for its path.
        return Err(Refusal::WrongIdentity { cause: None });
    };
    if *declared != key.id() {
        return Err(Refusal::WrongIdentity { cause: None });
    }
    let prefix = object.prefix().encode().map_err(Refusal::from)?;
    let plain = key
        .derive()
        .open_object_chunk(path.as_str(), &prefix, 0, 0, object.chunk())
        .map_err(|error| Refusal::Decryption {
            cause: Some(Arc::new(error)),
        })?;
    let Object::StoreLog(entry) = Object::decode(&plain).map_err(Refusal::from)? else {
        return Err(Refusal::WrongIdentity { cause: None });
    };
    if *path != self::path(path.store(), entry.position) {
        return Err(Refusal::WrongIdentity { cause: None });
    }
    if matches!(entry.change, StoreChange::CreateStore { store, .. } if store != path.store()) {
        return Err(Refusal::WrongIdentity { cause: None });
    }
    let mut hash = ObjectHasher::new();
    hash.update(&object.signed_bytes().map_err(Refusal::from)?);
    Ok(OpenedEntry::new(
        entry,
        path.clone(),
        hash.finish(),
        signature.clone(),
    ))
}

/// Check the registered owner in the entry's complete, causally checked past.
pub(crate) fn author_matches_device(log: &StoreLog, entry: &StoreLogEntry) -> bool {
    let replay;
    let view = if log
        .entries
        .iter()
        .all(|prior| crate::replay::had_read(entry, &prior.entry))
    {
        &log.replay.state
    } else {
        let positions = crate::replay::author_positions(entry);
        replay = crate::replay::at(log, &positions);
        &replay.state
    };
    author_matches_view(view, entry)
}

pub(crate) fn author_matches_view(
    view: &coven_database::StoreLogState,
    entry: &StoreLogEntry,
) -> bool {
    match view.devices.get(&entry.position.device) {
        Some(device) => device.member == entry.author,
        // Creation and a device addition register their own writer (§10),
        // establishing an absent binding. Replay separately checks authority.
        None => matches!(
            entry.change,
            StoreChange::CreateStore { .. } | StoreChange::AddDevice { .. }
        ),
    }
}

/// Why a new registration of `device` is refused: one entry ever registers a
/// device id (§10, `Writers.RegistersOnce`). Once an entry registering it is
/// stored, whatever replay makes of it, no other entry may: a reader that has
/// drained the id after that registration's drop is final never reads a later
/// one. The refusal repeats the stored registration's own drop reason; a kept
/// one makes another registration only a repeat, which is not allowed either.
pub(crate) fn registration_refused(
    log: &StoreLog,
    device: coven_foundation::id_source::DeviceId,
) -> Option<coven_database::DropReason> {
    let registration = log.entries.iter().find(|applied| {
        applied.entry.position.device == device
            && matches!(
                applied.entry.change,
                StoreChange::CreateStore { .. } | StoreChange::AddDevice { .. }
            )
    })?;
    Some(match log.replay.entries.get(&registration.entry.position) {
        Some(coven_database::EntryOutcome::Dropped(reason)) => reason.clone(),
        _ => coven_database::DropReason::NotAllowed,
    })
}

/// An absent prerequisite waits; a present, inconsistent past is damaged.
pub(crate) fn ready(
    log: &StoreLog,
    entry: &StoreLogEntry,
    store: StoreId,
) -> Result<bool, Refusal> {
    if !causally_ready(&log.entries, entry, store)? {
        return Ok(false);
    }
    if !author_matches_device(log, entry) {
        return Err(Refusal::WrongIdentity { cause: None });
    }
    Ok(true)
}

/// The first entry of `entry`'s recorded past that `applied` lacks, in the
/// order readiness checks them: its own log's previous entry, then each
/// entry it read. It is the entry's causal wait (§19.1).
pub(crate) fn first_missing(
    entry: &StoreLogEntry,
    applied: impl Fn(EntryId) -> bool,
) -> Option<EntryId> {
    let previous = (entry.position.number > 1).then(|| EntryId {
        number: entry.position.number - 1,
        ..entry.position
    });
    previous
        .into_iter()
        .chain(entry.had_read.0.iter().copied())
        .find(|id| !applied(*id))
}

pub(crate) fn causally_ready(
    entries: &[coven_database::ReplayEntry],
    entry: &StoreLogEntry,
    store: StoreId,
) -> Result<bool, Refusal> {
    if first_missing(entry, |id| {
        entries.iter().any(|prior| prior.entry.position == id)
    })
    .is_some()
    {
        return Ok(false);
    }
    if let StoreChange::CreateStore { store: id, .. } = &entry.change {
        if *id != store || !entries.is_empty() {
            return Err(Refusal::WrongIdentity { cause: None });
        }
        return Ok(true);
    }
    let Some(creation) = entries
        .iter()
        .find(|applied| matches!(applied.entry.change, StoreChange::CreateStore { .. }))
    else {
        return Ok(false);
    };
    if !crate::replay::had_read(entry, &creation.entry) {
        return Err(Refusal::InvalidCausality { cause: None });
    }
    for prior in entries {
        let prior = &prior.entry;
        if crate::replay::had_read(entry, prior) {
            if prior.timestamp >= entry.timestamp {
                return Err(Refusal::InvalidCausality { cause: None });
            }
            // Prefix positions already include this prior's own earlier entries.
            // Its other-device positions are exactly the additional closure checks.
            if prior
                .had_read
                .0
                .iter()
                .any(|position| !crate::replay::had_read_position(entry, *position))
            {
                return Err(Refusal::InvalidCausality { cause: None });
            }
        }
    }
    Ok(true)
}
