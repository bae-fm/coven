//! Authentication and causal checks before the replay's valid-input boundary.

use crate::{write_object::invalid, Refusal, SyncError};
use coven_crypto::{MemberKeys, ObjectHasher, StoreKey, StoreKeyring};
use coven_database::{DbError, StoreLog, StoreLogKeyUpload, StoreLogUpload};
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
        // Creation precedes custody: its queued sealed copy holds the key.
        let path = ObjectPath::store_key(id, &upload.entry.author);
        let copy = upload
            .sealing
            .keys
            .iter()
            .find(|key| key.path == path.as_str())
            .ok_or(DbError::DamagedDatabase)?;
        creation = SealedKey::decode(&copy.bytes)?.open_store_key(member, path.as_str())?;
        creation.check_hash(key_hash)?;
        if creation.id() != id {
            return Err(DbError::DamagedDatabase.into());
        }
        &creation
    } else {
        ring.ok_or(SyncError::KeyUnavailable(id))?.store_key(id)?
    };
    seal(&upload.entry, upload.format, key, member)
}

/// Publish the sealed copies before the entry that introduces or shares them.
pub(crate) async fn upload(
    storage: Option<&dyn Storage>,
    upload: &StoreLogUpload,
    bytes: &[u8],
) -> Result<(), SyncError> {
    upload_keys(storage, &upload.sealing.keys).await?;
    storage
        .ok_or(SyncError::NoStorage)?
        .create_once(&path(upload.entry.position), bytes)
        .await?;
    Ok(())
}

/// Journaled operations commit this step separately from entry publication.
pub(crate) async fn upload_keys(
    storage: Option<&dyn Storage>,
    keys: &[StoreLogKeyUpload],
) -> Result<(), SyncError> {
    for key in keys {
        storage
            .ok_or(SyncError::NoStorage)?
            .create_once(
                &ObjectPath::parse(&key.path).map_err(coven_storage::StorageError::from)?,
                &key.bytes,
            )
            .await?;
    }
    Ok(())
}

pub(crate) fn seal(
    entry: &StoreLogEntry,
    format: coven_format::FormatVersion,
    key: &StoreKey,
    member: &MemberKeys,
) -> Result<Vec<u8>, SyncError> {
    let coven_format::FormatVersion::V1 = format;
    let path = path(entry.position);
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

pub(crate) fn path(id: EntryId) -> ObjectPath {
    ObjectPath::store_log(
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
    if *path != self::path(entry.position) {
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
        let mut positions = entry.had_read.clone();
        if entry.position.number > 1 {
            let index = positions
                .0
                .partition_point(|id| id.device < entry.position.device);
            positions.0.insert(
                index,
                EntryId {
                    number: entry.position.number - 1,
                    ..entry.position
                },
            );
        }
        replay = crate::replay::at(log, &positions);
        &replay.state
    };
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
    let contains = |id: EntryId| log.replay.entries.contains_key(&id);
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
        if *id != store || !log.entries.is_empty() {
            return Err(Refusal::WrongIdentity { cause: None });
        }
        return Ok(true);
    }
    let Some(creation) = log
        .entries
        .iter()
        .find(|applied| matches!(applied.entry.change, StoreChange::CreateStore { .. }))
    else {
        return Ok(false);
    };
    if !crate::replay::had_read(entry, &creation.entry) {
        return Err(Refusal::InvalidCausality { cause: None });
    }
    for prior in &log.entries {
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
    if !author_matches_device(log, entry) {
        return Err(Refusal::WrongIdentity { cause: None });
    }
    Ok(true)
}
