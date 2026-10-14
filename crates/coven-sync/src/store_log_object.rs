//! Authentication and causal checks before the replay's valid-input boundary.

use crate::{write_object::invalid, Refusal, SyncError};
use coven_crypto::{MemberKeys, ObjectHasher, StoreKey, StoreKeyring};
use coven_database::{Database, StoreLog, StoreLogUpload};
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
pub(crate) fn seal_upload(
    store: StoreId,
    upload: &StoreLogUpload,
    ring: Option<&StoreKeyring>,
    member: &MemberKeys,
) -> Result<Vec<u8>, SyncError> {
    if upload.entry.author != member.member_id() {
        return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
    }
    let id = upload.sealing.key;
    let creation;
    let key = if let StoreChange::CreateStore { key_hash, .. } = &upload.entry.change {
        // Before custody commits, creation can recover its key from the queue.
        // After confirmation the caller acquires the published copy into `ring`.
        if let Some(copy) = upload.sealing.keys.first() {
            if upload.sealing.keys.len() != 1 {
                return Err(coven_database::DbError::DamagedDatabase.into());
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
            return Err(coven_database::DbError::DamagedDatabase.into());
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
    upload: &StoreLogUpload,
    bytes: &[u8],
) -> Result<(), SyncError> {
    upload_keys(store, storage, database).await?;
    storage
        .ok_or(SyncError::NoStorage)?
        .create_once(&path(store, upload.entry.position), bytes)
        .await?;
    Ok(())
}

/// Settle every reserved copy in writer/number order, regardless of its current
/// recipient or initiating operation. Each confirmation commits before the next send.
/// Publish reserved copies in number order. Returns the writer whose copy log
/// grew, so discovery can probe only that log again.
pub(crate) async fn upload_keys(
    store: StoreId,
    storage: Option<&dyn Storage>,
    database: &Database,
) -> Result<Option<coven_foundation::id_source::DeviceId>, SyncError> {
    let mut published = None;
    for copy in database.key_uploads().await? {
        let storage = storage.ok_or(SyncError::NoStorage)?;
        let path = copy.path(store);
        match storage.create(&path, &copy.bytes).await {
            Ok(()) => (),
            Err(error @ coven_storage::StorageError::Cleanup { .. }) => return Err(error.into()),
            Err(error) if error.failure() == coven_storage::StorageFailure::AlreadyExists => {
                if storage.read(&path).await? != copy.bytes {
                    return Err(crate::write_object::damaged(
                        &path,
                        Refusal::WrongIdentity { cause: None },
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        }
        published = Some(copy.writer);
        database.complete_key_upload(copy).await?;
    }
    Ok(published)
}

pub(crate) fn seal(
    store: StoreId,
    entry: &StoreLogEntry,
    format: coven_format::FormatVersion,
    key: &StoreKey,
    member: &MemberKeys,
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
    bytes.extend_from_slice(member.sign_object(path.as_str(), &hash.finish()).as_bytes());
    Ok(bytes)
}

pub(crate) fn path(store: StoreId, id: EntryId) -> ObjectPath {
    ObjectPath::store_log(
        store,
        id.device,
        id.number.try_into().expect("positive checked entry number"),
    )
}

pub(crate) fn open(
    object: &SingleChunkObject<'_>,
    path: &ObjectPath,
    key: &StoreKey,
) -> Result<StoreLogEntry, Refusal> {
    let SingleChunkObject::StoreLog {
        key: declared,
        signature,
        ..
    } = object
    else {
        return Err(invalid("expected a store-log envelope"));
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
        return Err(invalid("expected a store-log frame"));
    };
    let mut hash = ObjectHasher::new();
    hash.update(&object.signed_bytes().map_err(Refusal::from)?);
    entry
        .author
        .verify_object(path.as_str(), &hash.finish(), signature)
        .map_err(|error| Refusal::Signature {
            cause: Some(Arc::new(error)),
        })?;
    if *path != self::path(path.store(), entry.position) {
        return Err(Refusal::WrongIdentity { cause: None });
    }
    if matches!(entry.change, StoreChange::CreateStore { store, .. } if store != path.store()) {
        return Err(Refusal::WrongIdentity { cause: None });
    }
    Ok(entry)
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
        // Creation and self-registration establish an absent binding. Replay
        // separately checks authority, including a retry of a dropped registration.
        None => match &entry.change {
            StoreChange::CreateStore { .. } => true,
            StoreChange::AddDevice { device, .. } => *device == entry.position.device,
            _ => false,
        },
    }
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

pub(crate) fn causally_ready(
    entries: &[coven_database::ReplayEntry],
    entry: &StoreLogEntry,
    store: StoreId,
) -> Result<bool, Refusal> {
    let contains = |id: EntryId| entries.iter().any(|prior| prior.entry.position == id);
    if entry.position.number > 1
        && !contains(EntryId {
            number: entry.position.number - 1,
            ..entry.position
        })
    {
        return Ok(false);
    }
    if entry.had_read.0.iter().any(|id| !contains(*id)) {
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
