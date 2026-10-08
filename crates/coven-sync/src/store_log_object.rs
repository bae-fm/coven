//! Authentication and causal checks before the replay's valid-input boundary.

use crate::{ObjectCheckFailure, SyncError};
use coven_crypto::{MemberKeys, ObjectHasher, StoreKey, StoreKeyring};
use coven_database::{DbError, StoreLog, StoreLogKeyUpload, StoreLogUpload};
use coven_format::{
    sealed_single::{SingleChunkObject, SingleChunkPrefix, StoreOrigin},
    store_log::{StoreChange, StoreLogEntry},
    value::EntryId,
    Object,
};
use coven_foundation::id_source::StoreId;
use coven_storage::{ObjectPath, Storage};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
#[error("invalid store-log object: {0}")]
pub(crate) struct InvalidEntry(pub(crate) &'static str);

pub(crate) fn invalid(reason: &'static str) -> ObjectCheckFailure {
    ObjectCheckFailure::Parse(Arc::new(InvalidEntry(reason)))
}

pub(crate) fn origin(entry: &StoreLogEntry) -> Option<StoreOrigin> {
    match &entry.change {
        StoreChange::CreateStore { store, .. } => Some(StoreOrigin {
            store: *store,
            timestamp: entry.timestamp,
            author: entry.author.clone(),
        }),
        _ => None,
    }
}

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
    let key = if matches!(upload.entry.change, StoreChange::CreateStore { .. }) {
        // Creation precedes custody: its queued sealed copy holds the key.
        let path = ObjectPath::store_key(id, &upload.entry.author);
        let copy = upload
            .sealing
            .keys
            .iter()
            .find(|key| key.path == path.as_str())
            .ok_or(DbError::DamagedDatabase)?;
        creation = member.open_store_key(path.as_str(), &copy.bytes)?;
        if creation.id() != id {
            return Err(DbError::DamagedDatabase.into());
        }
        &creation
    } else {
        ring.ok_or(SyncError::KeyUnavailable(id))?.store_key(id)?
    };
    seal(&upload.entry, key, member)
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
    key: &StoreKey,
    member: &MemberKeys,
) -> Result<Vec<u8>, SyncError> {
    let path = path(entry.position);
    let prefix = SingleChunkPrefix::StoreLog {
        key: key.id(),
        origin: origin(entry),
    };
    let plain = Object::StoreLog(entry.clone()).encode()?;
    let chunk = key
        .derive()
        .reseal_object_chunk(path.as_str(), &prefix.encode()?, 0, 0, &plain);
    let mut bytes = prefix.encode_chunk(&chunk)?;
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

pub(crate) fn check_origin(
    object: &SingleChunkObject<'_>,
    path: &ObjectPath,
) -> Result<(), ObjectCheckFailure> {
    let SingleChunkObject::StoreLog {
        origin, signature, ..
    } = object
    else {
        return Err(invalid("expected a store-log envelope"));
    };
    if let Some(origin) = origin {
        let (device, number) = path.store_log_position().expect("listed store-log path");
        if number.get() != 1 || origin.timestamp.device() != device {
            return Err(invalid("creation identity disagrees with its path"));
        }
        let mut hash = ObjectHasher::new();
        hash.update(
            &object
                .signed_bytes()
                .map_err(|e| ObjectCheckFailure::Parse(Arc::new(e)))?,
        );
        origin
            .author
            .verify_object(path.as_str(), &hash.finish(), signature)
            .map_err(ObjectCheckFailure::Signature)?;
    }
    Ok(())
}

pub(crate) fn open(
    object: &SingleChunkObject<'_>,
    path: &ObjectPath,
    key: &StoreKey,
) -> Result<StoreLogEntry, ObjectCheckFailure> {
    let prefix = object
        .prefix()
        .encode()
        .map_err(|e| ObjectCheckFailure::Parse(Arc::new(e)))?;
    let plain = key
        .derive()
        .open_object_chunk(path.as_str(), &prefix, 0, 0, object.chunk())
        .map_err(ObjectCheckFailure::Decryption)?;
    let Object::StoreLog(entry) =
        Object::decode(&plain).map_err(|e| ObjectCheckFailure::Parse(Arc::new(e)))?
    else {
        return Err(invalid("expected a store-log frame"));
    };
    let SingleChunkObject::StoreLog {
        origin: declared,
        signature,
        ..
    } = object
    else {
        return Err(invalid("expected a store-log envelope"));
    };
    let mut hash = ObjectHasher::new();
    hash.update(
        &object
            .signed_bytes()
            .map_err(|e| ObjectCheckFailure::Parse(Arc::new(e)))?,
    );
    entry
        .author
        .verify_object(path.as_str(), &hash.finish(), signature)
        .map_err(ObjectCheckFailure::Signature)?;
    if *path != self::path(entry.position) {
        return Err(invalid("entry position disagrees with its path"));
    }
    if *declared != origin(&entry) {
        return Err(invalid(
            "creation identity disagrees with its encrypted entry",
        ));
    }
    Ok(entry)
}

pub(crate) fn author_matches_device(log: &StoreLog, entry: &StoreLogEntry) -> bool {
    log.entries.iter().all(|prior| {
        prior.entry.position.device != entry.position.device || prior.entry.author == entry.author
    })
}

/// An absent prerequisite waits; a present, inconsistent past is damaged.
pub(crate) fn ready(
    log: &StoreLog,
    entry: &StoreLogEntry,
    store: StoreId,
) -> Result<bool, ObjectCheckFailure> {
    if !author_matches_device(log, entry) {
        return Err(invalid("a device changed its author"));
    }
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
            return Err(invalid("creation does not belong to this store"));
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
        return Err(invalid("entry did not read its store's creation"));
    }
    for prior in &log.entries {
        let prior = &prior.entry;
        if crate::replay::had_read(entry, prior) {
            if prior.timestamp >= entry.timestamp {
                return Err(invalid("timestamp does not follow the recorded past"));
            }
            // Prefix positions already include this prior's own earlier entries.
            // Its other-device positions are exactly the additional closure checks.
            if prior
                .had_read
                .0
                .iter()
                .any(|position| !crate::replay::had_read_position(entry, *position))
            {
                return Err(invalid("recorded past is not causally closed"));
            }
        }
    }
    Ok(true)
}
