//! Authenticated device-log objects shared by direct application and snapshot reload.

use crate::object_input::InputStream;
use crate::{replay_cache::ReplayCache, DamagedObject, Refusal, SyncError};
use coven_crypto::{ObjectHasher, SigningKey, StoreKeyring};
use coven_database::{StoreLog, WriteWait};
use coven_format::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
use coven_format::write_stream::WriteHeaderFrame;
use coven_foundation::id_source::{KeyId, MemberId};
use coven_merge::Audience;
use coven_storage::{ObjectPath, StoredObject};
use std::sync::Arc;

pub(crate) struct OpenedWrite {
    pub(crate) header: WriteHeaderFrame,
    prefix: WriteObjectPrefix,
    layout: WriteObjectLayout,
    hash: ObjectHasher,
}

impl OpenedWrite {
    /// The audience and key of each part `opens` leaves closed: what must
    /// change before these bytes can name every file they refer to (§16.5).
    pub(crate) fn unopened(&self, opens: &[bool]) -> std::collections::BTreeSet<(Audience, KeyId)> {
        self.header
            .parts
            .iter()
            .zip(&self.prefix.part_keys)
            .zip(opens)
            .filter(|(_, opens)| !**opens)
            .map(|((part, key), _)| (part.audience.clone(), *key))
            .collect()
    }
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

/// Record a write's permanent refusal from the input that observed it. The
/// verdict needs the complete object (§19.1): a stream that already reached
/// EOF is not read again, and a header-only reader continues from its retained
/// prefix, so no earlier response is repeated to justify the verdict.
pub(crate) async fn refuse(
    database: &coven_database::Database,
    storage: &dyn coven_storage::Storage,
    input: crate::object_input::ObjectInput,
    failure: &Refusal,
) -> Result<coven_database::LogRefusal, SyncError> {
    let path = input.object().path.clone();
    if !input.received().await? {
        input.stream(storage).await.drain().await?;
    }
    let record = coven_database::LogRefusal {
        object: coven_database::LogObject::Write(path.write_id().expect("write path")),
        failure: failure.into(),
    };
    crate::pending_logs::record_refusal(database, record).await?;
    input.release_ranges().await?;
    tracing::warn!(path = path.as_str(), error = %failure, "write log is stuck");
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
        &Audience::Store,
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
    // The header's framing must describe exactly the listed object; a
    // different length is the bytes violating their format, with no native error.
    if expected != object.size {
        return Err(damaged(&object.path, Refusal::Parse { cause: None }));
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
) -> Result<Result<Vec<bool>, Unready>, SyncError> {
    let mut selected = Vec::new();
    let needed = crate::key_introductions::needed(log);
    for (part, key) in opened.header.parts.iter().zip(&opened.prefix.part_keys) {
        let waits = Unready::Key {
            audience: part.audience.clone(),
            key: *key,
        };
        // Only an authorized received introduction names a readable key (§11).
        let introduction = crate::key_introductions::introductions(log)
            .into_iter()
            .find(|i| i.audience == part.audience && i.key == *key)
            .and_then(|i| log.entries.iter().find(|e| e.entry.position == i.entry));
        let Some(introduction) = introduction else {
            return Ok(Err(waits));
        };
        let holds = crate::key_introductions::accepted(Some(ring), &needed, &part.audience, *key);
        // Current audience members wait for historical keys before applying a part.
        if !holds
            && key_audience_contains(log, replays, &introduction.entry, &part.audience, member)
        {
            return Ok(Err(waits));
        }
        selected.push(holds);
    }
    Ok(Ok(selected))
}

pub(crate) async fn finish<S: PartSink>(
    input: &mut InputStream<'_>,
    ring: &StoreKeyring,
    signer: &SigningKey,
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
        signer
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
    audience: &Audience,
    member: &MemberId,
) -> bool {
    use coven_format::store_log::StoreChange;
    if crate::key_introductions::belongs(&log.replay.state, audience, member) {
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
                    (StoreChange::AddMember { member: added, .. }, Audience::Store) => {
                        added == member
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
    /// The device owner's current signing key in the header's
    /// `store_log_read` view, which must verify the whole object (§10).
    pub(crate) signer: SigningKey,
    pub(crate) parts: Vec<bool>,
}

/// A write that cannot be checked further yet, by its first unmet condition
/// in processing order (§19.1). Its bytes stay retained for the next attempt.
#[derive(Debug)]
pub(crate) enum Unready {
    /// Recorded history the database requires has not applied.
    History(WriteWait),
    /// No usable copy of the header's store key or a part's key has arrived.
    Key { audience: Audience, key: KeyId },
}

impl Unready {
    /// Snapshot inputs keep their waits in the reload's own error vocabulary.
    pub(crate) fn into_snapshot_error(self) -> SyncError {
        match self {
            Self::History(wait) => {
                coven_database::DbError::Snapshot(coven_database::SnapshotError::WriteWaiting(wait))
                    .into()
            }
            Self::Key { key, .. } => SyncError::KeyUnavailable(key),
        }
    }
}

pub(crate) async fn download(
    input: &mut InputStream<'_>,
    ring: Option<&StoreKeyring>,
    log: &StoreLog,
    replays: &mut ReplayCache<'_>,
    member: impl FnOnce() -> Result<MemberId, SyncError>,
) -> Result<Result<ReadyWrite, Unready>, SyncError> {
    let object = input.object().clone();
    let result = async {
        let opened = match stream_header(input, ring).await {
            Ok(opened) => opened,
            Err(SyncError::KeyUnavailable(key)) => {
                return Ok(Err(Unready::Key {
                    audience: Audience::Store,
                    key,
                }))
            }
            Err(error) => return Err(error),
        };
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
            return Ok(Err(Unready::History(WriteWait::StoreLog(missing))));
        }
        let signer = authority(log, replays, &opened.header.header)
            .map_err(|failure| damaged(&object.path, failure))?;
        let parts = match parts(
            &opened,
            ring.expect("opened header has its store key"),
            log,
            replays,
            &member()?,
        )? {
            Ok(parts) => parts,
            Err(unready) => return Ok(Err(unready)),
        };
        Ok(Ok(ReadyWrite {
            opened,
            signer,
            parts,
        }))
    }
    .await;
    if matches!(&result, Ok(Err(_))) {
        input.drain().await?;
    }
    input.checked(result).await
}

pub(crate) fn authority(
    log: &StoreLog,
    replays: &mut ReplayCache<'_>,
    header: &coven_format::write::WriteHeader,
) -> Result<SigningKey, Refusal> {
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
    // A write made after reading its device's kept removal or replacement
    // never counts: its recorded view no longer has the device active (§7.1, §10).
    let view = replays.at(&header.store_log_read);
    let device = view
        .state
        .devices
        .get(&header.position.device)
        .ok_or(Refusal::NotAuthorized)?;
    match view.state.members.get(&device.member) {
        Some(owner) if device.active() && !owner.removed => Ok(owner.keys.signing),
        _ => Err(Refusal::NotAuthorized),
    }
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
pub(crate) fn damaged(path: &ObjectPath, failure: Refusal) -> SyncError {
    DamagedObject {
        path: path.as_str().to_owned(),
        failure,
    }
    .into()
}

#[cfg(test)]
#[path = "write_object_tests.rs"]
pub(crate) mod tests;
