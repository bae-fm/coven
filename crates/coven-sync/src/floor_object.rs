//! A floor observation and the original evidence it carries.

use crate::{
    chunk_reader::SlotPlaintext,
    history::History,
    object_input::ObjectInput,
    slot_object::{read_exact, SignedDigest},
    snapshot_object,
    write_object::checked,
    SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::Database;
use coven_format::{
    floor::{FloorFrames, FloorHeader, OriginalSlot},
    floor_pointer::{FloorPointerHeader, FloorPointerPrefix},
    sealed_floor::FloorPrefixDecoder,
};
use coven_foundation::id_source::DeviceId;
use coven_merge::Slot;
use coven_storage::{ObjectPath, Storage, StorageFailure, StoredObject};
use std::{io::Seek, sync::Arc};

/// The floor: the pointer at `<store>/floor` and the body it names (§16.4).
/// The pointer's revision is what a conditional replacement observes.
#[derive(Clone)]
pub(crate) struct FloorObject {
    pointer: StoredObject,
    pointer_signed: SignedDigest,
    pointer_header: FloorPointerHeader,
    pointer_prefix: FloorPointerPrefix,
    body_signed: SignedDigest,
    pub(crate) header: FloorHeader,
    pub(crate) announcements: Vec<OriginalSlot>,
    pub(crate) copies: Vec<OriginalSlot>,
}

/// What a floor routes to this member, read before any store key is held
/// (§13). The scan position, the keys and the evidence of a replacement all
/// remain unauthenticated until the floor and its history authenticate.
pub(crate) struct FloorRouting {
    /// The floor's sealing key.
    pub(crate) key: coven_foundation::id_source::KeyId,
    /// Its first kept slot.
    pub(crate) first: coven_merge::Slot,
    /// The keys its copies routed to this member open.
    pub(crate) keys: Vec<snapshot_object::RoutedKey>,
    /// Whether a copy names this member but a pair it doesn't hold: its
    /// replacement's (§12.3).
    pub(crate) routed_past: bool,
}

/// How many times a read follows a pointer that moved before its body was
/// fetched. Each follow means another device replaced the floor meanwhile.
const FOLLOWS: usize = 8;

/// The pointer's clear prefix and revision, read without any key.
async fn pointer_prefix(
    database: &Database,
    storage: &dyn Storage,
    path: &ObjectPath,
) -> Result<Option<(FloorPointerPrefix, StoredObject)>, SyncError> {
    let response = match storage.stream_object(path, None).await {
        Ok(response) => response,
        Err(error) if error.failure() == StorageFailure::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let object = response.metadata().clone();
    let input = ObjectInput::receive_response(database, storage, path, response).await?;
    let mut raw = input.local_bytes().await?;
    let prefix = read_exact(&mut raw, FloorPointerPrefix::LENGTH, path)?;
    let prefix = checked(path, FloorPointerPrefix::decode(&prefix))?;
    input.release().await?;
    Ok(Some((prefix, object)))
}

/// The floor's routing for this member, or `None` without a floor: the
/// pointer's clear prefix names the body, and the body's clear prefix holds
/// the key copies.
pub(crate) async fn routed_keys(
    database: &Database,
    storage: &dyn Storage,
    path: &ObjectPath,
    member: &MemberIdentity,
) -> Result<Option<FloorRouting>, SyncError> {
    let mut seen: Option<StoredObject> = None;
    for _ in 0..FOLLOWS {
        let Some((pointer, object)) = pointer_prefix(database, storage, path).await? else {
            return Ok(None);
        };
        if seen.as_ref() == Some(&object) {
            return Err(crate::write_object::wrong_identity(&pointer.body_path(path.store())));
        }
        let body = pointer.body_path(path.store());
        let response = match storage.stream_object(&body, None).await {
            Ok(response) => response,
            // The pointer moved on and its writer deleted the body it named.
            Err(error) if error.failure() == StorageFailure::NotFound => {
                seen = Some(object);
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let input = ObjectInput::receive_response(database, storage, &body, response).await?;
        let mut raw = input.local_bytes().await?;
        let prefix = read_exact(&mut raw, FloorPrefixDecoder::HEADER_LENGTH, &body)?;
        let mut decoder = checked(&body, FloorPrefixDecoder::start(&prefix))?;
        let key = decoder.key;
        let first = decoder.first;
        let mut keys = Vec::new();
        let mut routed_past = false;
        while decoder.remaining() > 0 {
            let mut bytes = read_exact(&mut raw, 12, &body)?;
            let length = checked(&body, decoder.copy_length(&bytes))?;
            bytes.extend(read_exact(&mut raw, length - bytes.len(), &body)?);
            let original = checked(&body, decoder.copy(&bytes))?;
            let (opened, past) = snapshot_object::routed(&original, body.store(), member)?;
            keys.extend(opened);
            routed_past |= past;
        }
        checked(&body, decoder.finish())?;
        input.release().await?;
        if pointer.key != key || pointer.first != first {
            return Err(crate::write_object::wrong_identity(&body));
        }
        return Ok(Some(FloorRouting { key, first, keys, routed_past }));
    }
    Err(crate::write_object::wrong_identity(path))
}

impl FloorObject {
    /// Follow the pointer to its body. Unchanged pointer bytes reuse the
    /// authenticated floor read before; the fresh GET still observes the
    /// pointer's revision after the caller's sequence reads. A body that is
    /// gone because the pointer moved on is followed to the new pointer's.
    pub(crate) async fn read(
        database: &Database,
        storage: &dyn Storage,
        path: &ObjectPath,
        previous: Option<&Self>,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<Option<Self>, SyncError> {
        let mut seen: Option<StoredObject> = None;
        for _ in 0..FOLLOWS {
            let response = match storage.stream_object(path, None).await {
                Ok(response) => response,
                Err(error) if error.failure() == StorageFailure::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            if let Some(previous) = previous {
                if response.metadata() == &previous.pointer {
                    return Ok(Some(previous.clone()));
                }
            }
            let pointer = response.metadata().clone();
            if seen.as_ref() == Some(&pointer) {
                return Err(crate::write_object::wrong_identity(path));
            }
            let input = ObjectInput::receive_response(database, storage, path, response).await?;
            let mut raw = input.local_bytes().await?;
            let mut prefix_bytes = read_exact(&mut raw, FloorPointerPrefix::LENGTH, path)?;
            let prefix = checked(path, FloorPointerPrefix::decode(&prefix_bytes))?;
            let sealed = raw
                .len()
                .checked_sub(prefix_bytes.len() as u64 + 64)
                .ok_or(coven_format::Error::Truncated)?;
            let length = checked(path, coven_format::sealed_stream::plaintext_length(sealed))?;
            let key = crate::write_seal::derive(ring, &coven_merge::Audience::Store, prefix.key)?;
            let mut plain = SlotPlaintext::new(
                raw,
                prefix_bytes.len() as u64,
                length,
                Arc::from(path.as_str()),
                Arc::from(std::mem::take(&mut prefix_bytes)),
                Arc::new(key),
                0,
            );
            let pointer_header = checked(
                path,
                FloorPointerHeader::decode(&snapshot_object::frame(&mut plain, path)?),
            )?;
            if plain.stream_position().map_err(SyncError::disk)? < plain.len() {
                return Err(crate::write_object::damaged(
                    path,
                    coven_format::Error::Truncated.into(),
                ));
            }
            let pointer_signed = SignedDigest::read(input.local_bytes().await?, path)?;
            input.release().await?;
            let body = prefix.body_path(path.store());
            let Some(body_read) =
                Self::read_body(database, storage, &body, prefix.first, ring, member).await?
            else {
                // The pointer moved on and its writer deleted the body it
                // named: read the pointer again.
                seen = Some(pointer);
                continue;
            };
            if body_read.signed.complete() != &prefix.body {
                return Err(crate::write_object::wrong_identity(&body));
            }
            return Ok(Some(Self {
                pointer,
                pointer_signed,
                pointer_header,
                pointer_prefix: prefix,
                body_signed: body_read.signed,
                header: body_read.header,
                announcements: body_read.announcements,
                copies: body_read.copies,
            }));
        }
        Err(crate::write_object::wrong_identity(path))
    }

    /// The body at its own path: its clear key copies, its signed header and
    /// the announcements, each checked against the pointer's `first`.
    async fn read_body(
        database: &Database,
        storage: &dyn Storage,
        path: &ObjectPath,
        pointer_first: coven_merge::Slot,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<Option<Body>, SyncError> {
        let response = match storage.stream_object(path, None).await {
            Ok(response) => response,
            Err(error) if error.failure() == StorageFailure::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let input = ObjectInput::receive_response(database, storage, path, response).await?;
        let mut raw = input.local_bytes().await?;
        let mut prefix = read_exact(&mut raw, FloorPrefixDecoder::HEADER_LENGTH, path)?;
        let mut decoder = checked(path, FloorPrefixDecoder::start(&prefix))?;
        let key_id = decoder.key;
        let first = decoder.first;
        if first != pointer_first {
            return Err(crate::write_object::wrong_identity(path));
        }
        let mut copies = Vec::new();
        while decoder.remaining() > 0 {
            let mut bytes = read_exact(&mut raw, 12, path)?;
            let length = checked(path, decoder.copy_length(&bytes))?;
            bytes.extend(read_exact(&mut raw, length - bytes.len(), path)?);
            let original = checked(path, decoder.copy(&bytes))?;
            snapshot_object::open_routing(&original, path.store(), ring, member)?;
            copies.push(original);
            prefix.extend(bytes);
        }
        checked(path, decoder.finish())?;
        let sealed = raw
            .len()
            .checked_sub(prefix.len() as u64 + 64)
            .ok_or(coven_format::Error::Truncated)?;
        let length = checked(path, coven_format::sealed_stream::plaintext_length(sealed))?;
        let key = crate::write_seal::derive(ring, &coven_merge::Audience::Store, key_id)?;
        let mut plain = SlotPlaintext::new(
            raw,
            prefix.len() as u64,
            length,
            Arc::from(path.as_str()),
            Arc::from(prefix),
            Arc::new(key),
            0,
        );
        let mut decoder = checked(
            path,
            FloorFrames::decode_header(&snapshot_object::frame(&mut plain, path)?, first),
        )?;
        let header = decoder.header().clone();
        let mut announcements = Vec::new();
        while plain.stream_position().map_err(SyncError::disk)? < plain.len() {
            if let Some(original) =
                checked(path, decoder.frame(&snapshot_object::frame(&mut plain, path)?))?
            {
                announcements.push(original);
            }
        }
        checked(path, decoder.finish())?;
        let signed = SignedDigest::read(input.local_bytes().await?, path)?;
        input.release().await?;
        Ok(Some(Body { signed, header, announcements, copies }))
    }

    /// Call after reconstructing history through `at`, including bootstrap
    /// snapshots when the caller did not yet know this signer's binding. The
    /// pointer and the body are each signed by a member of the store at the
    /// slot they name.
    pub(crate) fn authenticate(&self, history: &History) -> Result<(), SyncError> {
        let path = self.pointer_path();
        let (_, signer) = history
            .owner(self.pointer_header.author, self.pointer_header.at)
            .map_err(|cause| crate::write_object::damaged(&path, cause))?;
        self.pointer_signed.verify(&path, &signer, None)?;
        let body = self.pointer_prefix.body_path(path.store());
        let (_, signer) = history
            .owner(self.header.author, self.header.at)
            .map_err(|cause| crate::write_object::damaged(&body, cause))?;
        self.body_signed.verify(&body, &signer, Some(&self.pointer_prefix.body))?;
        Ok(())
    }

    /// The observed revision a conditional replacement names.
    pub(crate) fn object(&self) -> &StoredObject {
        &self.pointer
    }

    /// The writer and number of the body the pointer names.
    pub(crate) fn body(&self) -> (DeviceId, Slot) {
        (self.pointer_prefix.device, self.pointer_prefix.number)
    }

    fn pointer_path(&self) -> ObjectPath {
        self.pointer.path.clone()
    }
}

/// A floor body as read: its signature and what it carries.
struct Body {
    signed: SignedDigest,
    header: FloorHeader,
    announcements: Vec<OriginalSlot>,
    copies: Vec<OriginalSlot>,
}

#[cfg(test)]
#[path = "floor_object_tests.rs"]
mod tests;
