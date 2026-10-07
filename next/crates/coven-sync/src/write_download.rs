//! Ranged envelope reads feed authenticated chunks into one database transaction.

use super::DeviceLogSync;
use crate::replay_cache::ReplayCache;
use crate::{DamagedObject, ObjectCheckFailure, SyncError};
use coven_crypto::{MemberId, ObjectHasher, StoreKeyring};
use coven_database::{
    ApplyOutcome, DbError, DownloadedPartStream, DownloadedWriteStream, StoreLog, WriteWait,
};
use coven_format::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
use coven_format::write_stream::WriteHeaderFrame;
use coven_storage::{ByteRange, ObjectPath, StoredObject};
use std::io::{self, Read};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

struct OpenedWrite {
    header: WriteHeaderFrame,
    prefix: WriteObjectPrefix,
    layout: WriteObjectLayout,
    hash: ObjectHasher,
    offset: u64,
}

struct PartInput {
    receiver: mpsc::Receiver<Vec<u8>>,
    chunk: io::Cursor<Vec<u8>>,
}

impl Read for PartInput {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.chunk.read(out)?;
            if n > 0 {
                return Ok(n);
            }
            match self.receiver.blocking_recv() {
                Some(bytes) => self.chunk = io::Cursor::new(bytes),
                None => return Ok(0),
            }
        }
    }
}

impl DeviceLogSync {
    async fn piece(
        &self,
        object: &StoredObject,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, SyncError> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| damaged(&object.path, invalid("object length overflow")))?;
        if end > object.size {
            return Err(damaged(&object.path, parse(coven_format::Error::Truncated)));
        }
        let bytes = self
            .storage
            .read_range(&object.path, ByteRange::new(offset, end)?)
            .await?;
        if bytes.len() != length {
            return Err(damaged(
                &object.path,
                invalid("range response has the wrong length"),
            ));
        }
        Ok(bytes)
    }

    async fn open_write(
        &self,
        object: &StoredObject,
        ring: &StoreKeyring,
    ) -> Result<OpenedWrite, SyncError> {
        let mut bytes = self.piece(object, 0, 23).await?;
        let prefix_length = checked(&object.path, WriteObjectPrefix::length(&bytes))?;
        if prefix_length > 23 {
            bytes.extend(self.piece(object, 23, prefix_length - 23).await?);
        }
        let prefix = checked(&object.path, WriteObjectPrefix::decode(&bytes))?;
        let mut hash = ObjectHasher::new();
        hash.update(&bytes);
        let aad = bytes;
        let mut bytes = self.piece(object, prefix_length as u64, 4).await?;
        let length = checked(&object.path, WriteObjectPrefix::header_chunk_length(&bytes))?;
        bytes.extend(
            self.piece(object, prefix_length as u64 + 4, length - 4)
                .await?,
        );
        hash.update(&bytes);
        let sealed = checked(&object.path, WriteObjectPrefix::header_chunk(&bytes))?;
        let key = ring
            .store_key(prefix.store_key)
            .map_err(|_| SyncError::KeyUnavailable(prefix.store_key))?;
        let plain = key
            .derive()
            .open_object_chunk(object.path.as_str(), &aad, 0, 0, sealed)
            .map_err(|e| damaged(&object.path, ObjectCheckFailure::Decryption(e)))?;
        let header = checked(&object.path, WriteHeaderFrame::decode(&plain))?;
        if crate::write_seal::path(header.header.position) != object.path {
            return Err(damaged(
                &object.path,
                invalid("write position disagrees with path"),
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
            offset: (prefix_length + length) as u64,
        })
    }

    pub(super) async fn receive_write(
        &self,
        object: &StoredObject,
        ring: &StoreKeyring,
        log: &StoreLog,
        replays: &mut ReplayCache<'_>,
        member: &MemberId,
    ) -> Result<ApplyOutcome, SyncError> {
        let opened = self.open_write(object, ring).await?;
        let header = &opened.header.header;
        let missing: Vec<_> = header
            .store_log_read
            .0
            .iter()
            .copied()
            .filter(|id| !log.replay.entries.contains_key(id))
            .collect();
        if !missing.is_empty() {
            return Ok(ApplyOutcome::Waiting(WriteWait::StoreLog(missing)));
        }
        let author = authority(log, replays, header).map_err(|e| damaged(&object.path, e))?;
        let mut senders = Vec::new();
        let mut parts = Vec::new();
        for (part, key) in opened.header.parts.iter().zip(&opened.prefix.part_keys) {
            let introduction = log.entries.iter().find(|e| {
                crate::store_log_keys::introduced(&e.entry.change)
                    .contains(&(part.audience.clone(), *key))
            });
            let Some(introduction) = introduction else {
                return Err(SyncError::KeyUnavailable(*key));
            };
            let holds = crate::write_seal::holds(ring, &part.audience, *key);
            // Dropped removals share their keys with the latest audience (§11,
            // §14.4). Its members wait for that copy before applying the part.
            if !holds
                && key_audience_contains(log, replays, &introduction.entry, &part.audience, member)
            {
                return Err(SyncError::KeyUnavailable(*key));
            }
            if holds {
                let (sender, receiver) = mpsc::channel(1);
                senders.push(Some(sender));
                parts.push(DownloadedPartStream::Opened(PartInput {
                    receiver,
                    chunk: io::Cursor::new(Vec::new()),
                }));
            } else {
                senders.push(None);
                parts.push(DownloadedPartStream::Skipped);
            }
        }
        let download = DownloadedWriteStream {
            header: opened.header.clone(),
            parts,
        };
        let (validation, validated) = oneshot::channel();
        let apply = self
            .database
            .apply_downloaded_stream(download, log.positions(), move || {
                validated
                    .blocking_recv()
                    .map_err(|e| DbError::SyncStream(Box::new(e)))?
            });
        let transfer = async {
            let result = self
                .write_body(object, ring, &author, opened, senders)
                .await;
            let checked = match &result {
                Ok(()) => Ok(()),
                Err(error) => Err(DbError::SyncStream(Box::new(io::Error::other(
                    error.to_string(),
                )))),
            };
            let _ = validation.send(checked); // An early database refusal needs no final check.
            result
        };
        let (applied, transferred) = tokio::join!(apply, transfer);
        transferred?;
        match applied {
            Ok(result) => Ok(result),
            Err(DbError::WriteFormat(coven_format::Error::UnsupportedVersion(version)))
                if version > coven_format::FORMAT_VERSION =>
            {
                Err(crate::SyncFailure::UpdateRequired.into())
            }
            Err(
                error @ (DbError::InvalidWrite { .. }
                | DbError::WriteFormat(_)
                | DbError::TooLarge { .. }
                | DbError::Snapshot(_)
                | DbError::Schema(_)),
            ) => Err(damaged(
                &object.path,
                ObjectCheckFailure::Parse(Arc::new(error)),
            )),
            Err(error) => Err(error.into()),
        }
    }

    async fn write_body(
        &self,
        object: &StoredObject,
        ring: &StoreKeyring,
        author: &MemberId,
        mut opened: OpenedWrite,
        mut senders: Vec<Option<mpsc::Sender<Vec<u8>>>>,
    ) -> Result<(), SyncError> {
        let aad = opened.prefix.encode()?;
        while let Some(coordinate) = opened.layout.next_chunk() {
            let mut bytes = self.piece(object, opened.offset, 4).await?;
            let length = checked(&object.path, opened.layout.chunk_length(&bytes))?;
            bytes.extend(self.piece(object, opened.offset + 4, length - 4).await?);
            opened.offset += length as u64;
            opened.hash.update(&bytes);
            let (_, sealed) = checked(&object.path, opened.layout.decode_chunk(&bytes))?;
            let part = coordinate.section as usize - 1;
            if let Some(sender) = &senders[part] {
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
                    .map_err(|e| damaged(&object.path, ObjectCheckFailure::Decryption(e)))?;
                // A waiting or refused database transaction closes its receiver.
                // Its awaited result retains that outcome; authenticate the rest
                // of the object even when there is no plaintext consumer.
                let _ = sender.send(plain).await;
            }
            if opened
                .layout
                .next_chunk()
                .is_none_or(|next| next.section != coordinate.section)
            {
                senders[part] = None;
            }
        }
        let bytes = self.piece(object, opened.offset, 64).await?;
        let signature = checked(&object.path, opened.layout.read_signature(&bytes))?;
        if opened.offset + 64 != object.size {
            return Err(damaged(
                &object.path,
                parse(coven_format::Error::TrailingBytes),
            ));
        }
        checked(&object.path, opened.layout.finish(&[]))?;
        author
            .verify_object(object.path.as_str(), &opened.hash.finish(), &signature)
            .map_err(|e| damaged(&object.path, ObjectCheckFailure::Signature(e)))
    }
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

fn authority(
    log: &StoreLog,
    replays: &mut ReplayCache<'_>,
    header: &coven_format::write::WriteHeader,
) -> Result<MemberId, ObjectCheckFailure> {
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
            return Err(invalid(
                "write's store-log past is not causally closed or precedes its timestamp",
            ));
        }
    }
    let view = replays.at(&header.store_log_read);
    let device = view
        .state
        .devices
        .get(&header.position.device)
        .ok_or_else(|| invalid("authoring device was not a member"))?;
    if device.removed
        || view
            .state
            .members
            .get(&device.member)
            .is_none_or(|m| m.removed)
    {
        return Err(invalid("write read its author's or device's removal"));
    }
    Ok(device.member.clone())
}

fn checked<T>(path: &ObjectPath, value: Result<T, coven_format::Error>) -> Result<T, SyncError> {
    match value {
        Err(coven_format::Error::UnsupportedVersion(version))
            if version > coven_format::FORMAT_VERSION =>
        {
            Err(crate::SyncFailure::UpdateRequired.into())
        }
        value => value.map_err(|e| damaged(path, parse(e))),
    }
}
fn parse(error: coven_format::Error) -> ObjectCheckFailure {
    ObjectCheckFailure::Parse(Arc::new(error))
}
fn invalid(message: &'static str) -> ObjectCheckFailure {
    ObjectCheckFailure::Parse(Arc::new(io::Error::new(
        io::ErrorKind::InvalidData,
        message,
    )))
}
fn damaged(path: &ObjectPath, failure: ObjectCheckFailure) -> SyncError {
    DamagedObject {
        path: path.as_str().to_owned(),
        failure,
    }
    .into()
}
