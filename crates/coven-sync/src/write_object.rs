//! Authenticated device-log objects shared by direct application and snapshot reload.

use crate::{replay_cache::ReplayCache, DamagedObject, Refusal, SyncError};
use coven_crypto::{MemberId, ObjectHasher, StoreKeyring};
use coven_database::{StoreLog, WriteWait};
use coven_format::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
use coven_format::write_stream::WriteHeaderFrame;
use coven_storage::{ByteRange, ObjectPath, StoredObject};
use std::{io, sync::Arc};

pub(crate) struct OpenedWrite {
    pub(crate) header: WriteHeaderFrame,
    prefix: WriteObjectPrefix,
    layout: WriteObjectLayout,
    hash: ObjectHasher,
    offset: u64,
}

/// Each consumer chooses its opened parts and handles bounded plaintext chunks.
pub(crate) trait PartSink: Send {
    fn opens(&self, part: usize) -> bool;
    fn chunk(
        &mut self,
        part: usize,
        bytes: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), SyncError>> + Send;
    fn end(
        &mut self,
        part: usize,
    ) -> impl std::future::Future<Output = Result<(), SyncError>> + Send;
}

async fn piece(
    storage: &dyn coven_storage::Storage,
    object: &StoredObject,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, SyncError> {
    let end = offset
        .checked_add(length as u64)
        .ok_or_else(|| damaged(&object.path, invalid("object length overflow")))?;
    if end > object.size {
        return Err(damaged(
            &object.path,
            Refusal::from(coven_format::Error::Truncated),
        ));
    }
    crate::object_range::read(storage, &object.path, ByteRange::new(offset, end)?)
        .await
        .map_err(|error| match error {
            crate::object_range::ReadError::Storage(error) => error.into(),
            crate::object_range::ReadError::Length => coven_storage::StorageFailure::Protocol
                .with_source("range response has the wrong length")
                .into(),
        })
}

/// Persist a judgment only after the entire listed immutable object is readable.
pub(crate) async fn record_damage(
    database: &coven_database::Database,
    storage: &dyn coven_storage::Storage,
    object: &StoredObject,
    failure: &Refusal,
) -> Result<coven_database::LogRefusal, SyncError> {
    read_complete(storage, object).await?;
    let record = coven_database::LogRefusal {
        object: coven_database::LogObject::Write(object.path.write_id().expect("write path")),
        failure: failure.into(),
    };
    database.record_stuck_log(record).await?;
    tracing::warn!(path = object.path.as_str(), error = %failure, "write log is stuck");
    Ok(record)
}

/// Complete the transport check after an early permanent refusal, with bounded memory.
async fn read_complete(
    storage: &dyn coven_storage::Storage,
    object: &StoredObject,
) -> Result<(), SyncError> {
    let mut offset = 0;
    while offset < object.size {
        let length = (object.size - offset).min(64 * 1024) as usize;
        piece(storage, object, offset, length).await?;
        offset += length as u64;
    }
    // An empty listed object must still be readable; absence is transient.
    if object.size == 0 {
        let bytes = storage.read(&object.path).await?;
        if !bytes.is_empty() {
            return Err(coven_storage::StorageFailure::Protocol
                .with_source("listed object length differs from its bytes")
                .into());
        }
    }
    Ok(())
}

pub(crate) async fn open(
    storage: &dyn coven_storage::Storage,
    object: &StoredObject,
    ring: Option<&StoreKeyring>,
    reads: &crate::pass_reads::PassReads,
) -> Result<OpenedWrite, SyncError> {
    let bytes = match reads.metadata(object) {
        Some(bytes) => bytes,
        None => {
            let bytes = read_header(storage, object).await?;
            reads.keep_metadata(object, bytes.clone());
            bytes
        }
    };
    let prefix_length = checked(&object.path, WriteObjectPrefix::length(&bytes))?;
    let (aad, bytes) = bytes.split_at(prefix_length);
    let prefix = checked(&object.path, WriteObjectPrefix::decode(aad))?;
    let mut hash = ObjectHasher::new();
    hash.update(aad);
    hash.update(bytes);
    let sealed = checked(&object.path, WriteObjectPrefix::header_chunk(bytes))?;
    let key = crate::write_seal::derive(
        ring.ok_or(SyncError::KeyUnavailable(prefix.store_key))?,
        &coven_merge::Audience::Store,
        prefix.store_key,
    )?;
    let plain = key
        .open_object_chunk(object.path.as_str(), aad, 0, 0, sealed)
        .map_err(|e| {
            damaged(
                &object.path,
                Refusal::Decryption {
                    cause: Some(Arc::new(e)),
                },
            )
        })?;
    let header = checked(&object.path, WriteHeaderFrame::decode(&plain))?;
    if crate::write_seal::path(header.header.position) != object.path {
        return Err(damaged(
            &object.path,
            Refusal::WrongIdentity { cause: None },
        ));
    }
    let expected = checked(
        &object.path,
        coven_format::sealed_write::sealed_length(
            plain.len(),
            &header
                .parts
                .iter()
                .map(|part| part.plaintext_length)
                .collect::<Vec<_>>(),
        ),
    )?;
    if expected != object.size {
        return Err(damaged(
            &object.path,
            invalid("listed write size differs from its header"),
        ));
    }
    let layout = checked(
        &object.path,
        prefix.clone().opened_header(
            &plain,
            header.parts.iter().map(|p| p.plaintext_length).collect(),
        ),
    )?;
    Ok(OpenedWrite {
        header,
        prefix,
        layout,
        hash,
        offset: (prefix_length + bytes.len()) as u64,
    })
}

async fn read_header(
    storage: &dyn coven_storage::Storage,
    object: &StoredObject,
) -> Result<Vec<u8>, SyncError> {
    let mut bytes = piece(storage, object, 0, 23).await?;
    let prefix_length = checked(&object.path, WriteObjectPrefix::length(&bytes))?;
    if prefix_length > bytes.len() {
        bytes.extend(
            piece(
                storage,
                object,
                bytes.len() as u64,
                prefix_length - bytes.len(),
            )
            .await?,
        );
    }
    bytes.extend(piece(storage, object, prefix_length as u64, 4).await?);
    let length = checked(
        &object.path,
        WriteObjectPrefix::header_chunk_length(&bytes[prefix_length..]),
    )?;
    bytes.extend(piece(storage, object, bytes.len() as u64, length - 4).await?);
    Ok(bytes)
}

fn parts(
    opened: &OpenedWrite,
    ring: &StoreKeyring,
    log: &StoreLog,
    replays: &mut ReplayCache<'_>,
    member: &MemberId,
) -> Result<Vec<bool>, SyncError> {
    let mut selected = Vec::new();
    for (part, key) in opened.header.parts.iter().zip(&opened.prefix.part_keys) {
        let introduction = log.entries.iter().find(|e| {
            crate::store_log_keys::introduced(&e.entry.change)
                .contains(&(part.audience.clone(), *key))
        });
        let Some(introduction) = introduction else {
            return Err(SyncError::KeyUnavailable(*key));
        };
        let holds = crate::store_log_keys::holds(Some(ring), &part.audience, *key);
        // Dropped removals share their keys with the latest audience (§11,
        // §14.4). Its members wait for that copy before applying the part.
        if !holds
            && key_audience_contains(log, replays, &introduction.entry, &part.audience, member)
        {
            return Err(SyncError::KeyUnavailable(*key));
        }
        selected.push(holds);
    }
    Ok(selected)
}

pub(crate) async fn finish<S: PartSink>(
    storage: &dyn coven_storage::Storage,
    object: &StoredObject,
    ring: &StoreKeyring,
    author: &MemberId,
    mut opened: OpenedWrite,
    sink: &mut S,
) -> Result<(), SyncError> {
    let aad = opened.prefix.encode()?;
    while let Some(coordinate) = opened.layout.next_chunk() {
        let mut bytes = piece(storage, object, opened.offset, 4).await?;
        let length = checked(&object.path, opened.layout.chunk_length(&bytes))?;
        bytes.extend(piece(storage, object, opened.offset + 4, length - 4).await?);
        opened.offset += length as u64;
        opened.hash.update(&bytes);
        let (_, sealed) = checked(&object.path, opened.layout.decode_chunk(&bytes))?;
        let part = coordinate.section as usize - 1;
        if sink.opens(part) {
            let key = crate::write_seal::derive(
                ring,
                &opened.header.parts[part].audience,
                coordinate.key,
            )?;
            let plain = key
                .open_object_chunk(
                    object.path.as_str(),
                    &aad,
                    coordinate.section,
                    coordinate.index,
                    sealed,
                )
                .map_err(|e| {
                    damaged(
                        &object.path,
                        Refusal::Decryption {
                            cause: Some(Arc::new(e)),
                        },
                    )
                })?;
            sink.chunk(part, plain).await?;
        }
        if opened
            .layout
            .next_chunk()
            .is_none_or(|next| next.section != coordinate.section)
        {
            sink.end(part).await?;
        }
    }
    let bytes = piece(storage, object, opened.offset, 64).await?;
    let signature = checked(&object.path, opened.layout.read_signature(&bytes))?;
    if opened.offset + 64 != object.size {
        return Err(damaged(
            &object.path,
            Refusal::from(coven_format::Error::TrailingBytes),
        ));
    }
    checked(&object.path, opened.layout.finish(&[]))?;
    author
        .verify_object(object.path.as_str(), &opened.hash.finish(), &signature)
        .map_err(|e| {
            damaged(
                &object.path,
                Refusal::Signature {
                    cause: Some(Arc::new(e)),
                },
            )
        })
}

fn key_audience_contains(
    log: &StoreLog,
    replays: &mut ReplayCache<'_>,
    introduction: &coven_format::store_log::StoreLogEntry,
    audience: &coven_merge::Audience,
    member: &MemberId,
) -> bool {
    use coven_format::store_log::StoreChange;
    use coven_merge::Audience;
    if crate::store_log_keys::recipients(log, audience).any(|(id, _)| id == member) {
        return true;
    }
    // Membership after the introduction names its initial key recipients.
    // Later kept additions share the earlier keys too, even if that member left.
    let mut past = introduction.had_read.clone();
    past.0.push(introduction.position);
    past.0.sort_by_key(|id| id.device);
    let at_introduction = &replays.at(&past).state;
    let included = match audience {
        Audience::Store => crate::effects::member(at_introduction, member).is_some(),
        Audience::Circle(circle) => crate::effects::circle(at_introduction, *circle)
            .is_some_and(|state| state.members.contains(member)),
    };
    included
        || log.entries.iter().any(|applied| {
            log.replay.entries[&applied.entry.position] == coven_database::EntryOutcome::Kept
                && crate::replay::had_read_position(&applied.entry, introduction.position)
                && match (&applied.entry.change, audience) {
                    (StoreChange::AddMember { keys, .. }, Audience::Store) => {
                        &keys.signing == member
                    }
                    (
                        StoreChange::AddCircleMember {
                            circle,
                            member: added,
                        },
                        Audience::Circle(owner),
                    ) => circle == owner && added == member,
                    _ => false,
                }
        })
}

/// The header is authenticated before history, author and audience checks.
pub(crate) struct ReadyWrite {
    pub(crate) opened: OpenedWrite,
    pub(crate) author: MemberId,
    pub(crate) parts: Vec<bool>,
}

pub(crate) async fn download(
    storage: &dyn coven_storage::Storage,
    object: &StoredObject,
    ring: Option<&StoreKeyring>,
    reads: &crate::pass_reads::PassReads,
    log: &StoreLog,
    replays: &mut ReplayCache<'_>,
    member: impl FnOnce() -> Result<MemberId, SyncError>,
) -> Result<Result<ReadyWrite, WriteWait>, SyncError> {
    let opened = open(storage, object, ring, reads).await?;
    let missing: Vec<_> = opened
        .header
        .header
        .store_log_read
        .0
        .iter()
        .copied()
        .filter(|id| !log.replay.entries.contains_key(id))
        .collect();
    if !missing.is_empty() {
        return Ok(Err(WriteWait::StoreLog(missing)));
    }
    let author = authority(log, replays, &opened.header.header)
        .map_err(|failure| damaged(&object.path, failure))?;
    let parts = parts(
        &opened,
        ring.expect("opened header has its store key"),
        log,
        replays,
        &member()?,
    )?;
    Ok(Ok(ReadyWrite {
        opened,
        author,
        parts,
    }))
}

fn authority(
    log: &StoreLog,
    replays: &mut ReplayCache<'_>,
    header: &coven_format::write::WriteHeader,
) -> Result<MemberId, Refusal> {
    for applied in &log.entries {
        let entry = &applied.entry;
        if !header.store_log_read.covers(entry.position) {
            continue;
        }
        if entry.timestamp >= header.timestamp
            || entry
                .had_read
                .0
                .iter()
                .any(|id| !header.store_log_read.covers(*id))
        {
            return Err(Refusal::InvalidCausality { cause: None });
        }
    }
    let view = replays.at(&header.store_log_read);
    let device = view
        .state
        .devices
        .get(&header.position.device)
        .ok_or(Refusal::NotAuthorized)?;
    if device.removed
        || view
            .state
            .members
            .get(&device.member)
            .is_none_or(|m| m.removed)
    {
        return Err(Refusal::NotAuthorized);
    }
    Ok(device.member.clone())
}

/// Only object checks become permanent; local database and input I/O failures propagate.
pub(crate) fn database_failure(path: &ObjectPath, error: coven_database::DbError) -> SyncError {
    use coven_database::{DbError, SnapshotError};
    use coven_merge::MergeError;
    match error {
        DbError::WriteFormat(error) | DbError::Snapshot(SnapshotError::Format(error))
            if crate::error::newer_format(&error) =>
        {
            crate::SyncFailure::UpdateRequired.into()
        }
        DbError::InvalidWrite {
            error:
                error @ (MergeError::CausalTimestamp(_)
                | MergeError::CausalClosure(_)
                | MergeError::DuplicateTimestamp(_, _)),
            ..
        } => damaged(
            path,
            Refusal::InvalidCausality {
                cause: Some(Arc::new(error)),
            },
        ),
        DbError::InvalidWrite {
            error: error @ MergeError::TimestampDevice(_),
            ..
        } => damaged(
            path,
            Refusal::WrongIdentity {
                cause: Some(Arc::new(error)),
            },
        ),
        DbError::WriteFormat(error) | DbError::Snapshot(SnapshotError::Format(error)) => {
            damaged(path, Refusal::from(error))
        }
        error @ (DbError::InvalidWrite { .. }
        | DbError::Schema(_)
        | DbError::Snapshot(
            SnapshotError::Inconsistent(_) | SnapshotError::Schema { .. },
        )) => damaged(
            path,
            Refusal::InvalidWrite {
                cause: Some(Arc::new(error)),
            },
        ),
        error @ DbError::TooLarge { .. } => damaged(
            path,
            Refusal::Parse {
                cause: Some(Arc::new(error)),
            },
        ),
        error => error.into(),
    }
}

pub(crate) fn checked<T>(
    path: &ObjectPath,
    value: Result<T, coven_format::Error>,
) -> Result<T, SyncError> {
    match value {
        Err(error) if crate::error::newer_format(&error) => {
            Err(crate::SyncFailure::UpdateRequired.into())
        }
        value => value.map_err(|e| damaged(path, Refusal::from(e))),
    }
}
pub(crate) fn invalid(message: &'static str) -> Refusal {
    Refusal::Parse {
        cause: Some(Arc::new(io::Error::new(
            io::ErrorKind::InvalidData,
            message,
        ))),
    }
}
pub(crate) fn damaged(path: &ObjectPath, failure: Refusal) -> SyncError {
    DamagedObject {
        path: path.as_str().to_owned(),
        failure,
    }
    .into()
}
