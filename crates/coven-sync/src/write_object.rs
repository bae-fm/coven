//! Authenticated device-log objects shared by direct application and snapshot reload.

use crate::object_input::InputStream;
use crate::{replay_cache::ReplayCache, DamagedObject, Refusal, SyncError};
use coven_crypto::{MemberId, ObjectHasher, StoreKeyring};
use coven_database::{StoreLog, WriteWait};
use coven_format::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
use coven_format::write_stream::WriteHeaderFrame;
use coven_storage::{ObjectPath, StoredObject};
use std::{io, sync::Arc};

pub(crate) struct OpenedWrite {
    pub(crate) header: WriteHeaderFrame,
    prefix: WriteObjectPrefix,
    layout: WriteObjectLayout,
    hash: ObjectHasher,
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

/// Persist a judgment only after the entire listed immutable object is readable.
pub(crate) async fn record_damage(
    database: &coven_database::Database,
    storage: &dyn coven_storage::Storage,
    object: &StoredObject,
    failure: &Refusal,
) -> Result<coven_database::LogRefusal, SyncError> {
    let input = crate::object_input::ObjectInput::observe(database, object).await?;
    if !input.received().await? {
        input.stream(storage).await.drain().await?;
    }
    let record = coven_database::LogRefusal {
        object: coven_database::LogObject::Write(object.path.write_id().expect("write path")),
        failure: failure.into(),
    };
    crate::pending_logs::record_refusal(database, record).await?;
    input.release_ranges().await?;
    tracing::warn!(path = object.path.as_str(), error = %failure, "write log is stuck");
    Ok(record)
}

pub(crate) async fn open(
    storage: &dyn coven_storage::Storage,
    object: &StoredObject,
    ring: Option<&StoreKeyring>,
    input: &crate::object_input::ObjectInput,
) -> Result<OpenedWrite, SyncError> {
    let bytes = match input.metadata().await? {
        Some(bytes) => bytes,
        None => {
            input
                .prefix(
                    storage,
                    WriteObjectPrefix::MAX_HEADER_END,
                    WriteObjectPrefix::header_end,
                )
                .await?
        }
    };
    let opened = open_header(object, ring, &bytes)?;
    input.keep_metadata(bytes).await?;
    Ok(opened)
}

fn open_header(
    object: &StoredObject,
    ring: Option<&StoreKeyring>,
    bytes: &[u8],
) -> Result<OpenedWrite, SyncError> {
    let prefix_length = checked(&object.path, WriteObjectPrefix::length(bytes))?;
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
    if crate::write_seal::path(object.path.store(), header.header.position) != object.path {
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
    })
}

async fn stream_header(
    input: &mut InputStream<'_>,
    ring: Option<&StoreKeyring>,
) -> Result<OpenedWrite, SyncError> {
    let mut bytes = input.read_exact(23).await?;
    let prefix_length = checked(&input.object().path, WriteObjectPrefix::length(&bytes))?;
    bytes.extend(input.read_exact(prefix_length - bytes.len() + 4).await?);
    let end = checked(&input.object().path, WriteObjectPrefix::header_end(&bytes))?;
    bytes.extend(input.read_exact(end - bytes.len()).await?);
    let opened = open_header(input.object(), ring, &bytes)?;
    input.keep_metadata(bytes).await?;
    Ok(opened)
}

fn parts(
    opened: &OpenedWrite,
    ring: &StoreKeyring,
    log: &StoreLog,
    replays: &mut ReplayCache<'_>,
    member: &MemberId,
) -> Result<Vec<bool>, SyncError> {
    let mut selected = Vec::new();
    let needed = crate::key_introductions::needed(log);
    for (part, key) in opened.header.parts.iter().zip(&opened.prefix.part_keys) {
        // Only an authorized received introduction names a readable key (§11).
        let introduction = crate::key_introductions::introductions(log)
            .into_iter()
            .find(|i| i.audience == part.audience && i.key == *key)
            .and_then(|i| log.entries.iter().find(|e| e.entry.position == i.entry));
        let Some(introduction) = introduction else {
            return Err(SyncError::KeyUnavailable(*key));
        };
        let holds = crate::key_introductions::accepted(Some(ring), &needed, &part.audience, *key);
        // Current audience members wait for historical keys before applying a part.
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
    input: &mut InputStream<'_>,
    ring: &StoreKeyring,
    author: &MemberId,
    mut opened: OpenedWrite,
    sink: &mut S,
) -> Result<(), SyncError> {
    let object = input.object().clone();
    let result = async {
        let aad = opened.prefix.encode()?;
        while let Some(coordinate) = opened.layout.next_chunk() {
            let mut bytes = input.read_exact(4).await?;
            let length = checked(&object.path, opened.layout.chunk_length(&bytes))?;
            bytes.extend(input.read_exact(length - 4).await?);
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
        let bytes = input.read_exact(64).await?;
        let signature = checked(&object.path, opened.layout.read_signature(&bytes))?;
        if input.position() != object.size {
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
    .await;
    let result = input.checked(result).await;
    if result.is_ok() {
        input.drain().await?;
    }
    result
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
    input: &mut InputStream<'_>,
    ring: Option<&StoreKeyring>,
    log: &StoreLog,
    replays: &mut ReplayCache<'_>,
    member: impl FnOnce() -> Result<MemberId, SyncError>,
) -> Result<Result<ReadyWrite, WriteWait>, SyncError> {
    let object = input.object().clone();
    let result = async {
        let opened = stream_header(input, ring).await?;
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
    .await;
    if matches!(&result, Ok(Err(_))) {
        input.drain().await?;
    }
    input.checked(result).await
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
            SyncError::UpdateRequired(crate::error::required_format(&error).expect("newer format"))
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
        Err(error) if crate::error::newer_format(&error) => Err(SyncError::UpdateRequired(
            crate::error::required_format(&error).expect("newer format"),
        )),
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
