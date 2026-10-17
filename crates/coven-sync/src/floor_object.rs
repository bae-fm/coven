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
    sealed_floor::FloorPrefixDecoder,
};
use coven_storage::{ObjectPath, Storage, StorageFailure, StoredObject};
use std::{io::Seek, sync::Arc};

#[derive(Clone)]
pub(crate) struct FloorObject {
    object: StoredObject,
    signed: SignedDigest,
    pub(crate) header: FloorHeader,
    pub(crate) announcements: Vec<OriginalSlot>,
    pub(crate) copies: Vec<OriginalSlot>,
}

/// The floor's sealing key and the key copies in its clear prefix routed to
/// this member, read before any store key is held (§13). The keys stay
/// tentative until the floor and its history authenticate.
pub(crate) async fn routed_keys(
    database: &Database,
    storage: &dyn Storage,
    path: &ObjectPath,
    member: &MemberIdentity,
) -> Result<
    Option<(
        coven_foundation::id_source::KeyId,
        Vec<snapshot_object::RoutedKey>,
    )>,
    SyncError,
> {
    let response = match storage.stream_object(path, None).await {
        Ok(response) => response,
        Err(error) if error.failure() == StorageFailure::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let input = ObjectInput::receive_response(database, storage, path, response).await?;
    let mut raw = input.local_bytes().await?;
    let prefix = read_exact(&mut raw, FloorPrefixDecoder::HEADER_LENGTH, path)?;
    let mut decoder = checked(path, FloorPrefixDecoder::start(&prefix))?;
    let key = decoder.key;
    let mut keys = Vec::new();
    while decoder.remaining() > 0 {
        let mut bytes = read_exact(&mut raw, 12, path)?;
        let length = checked(path, decoder.copy_length(&bytes))?;
        bytes.extend(read_exact(&mut raw, length - bytes.len(), path)?);
        let original = checked(path, decoder.copy(&bytes))?;
        keys.extend(snapshot_object::routed_key(
            &original,
            path.store(),
            member,
        )?);
    }
    checked(path, decoder.finish())?;
    Ok(Some((key, keys)))
}

impl FloorObject {
    /// Reuse authenticated unchanged bytes; the fresh GET still observes the
    /// floor revision after the caller's sequence reads.
    pub(crate) async fn read(
        database: &Database,
        storage: &dyn Storage,
        path: &ObjectPath,
        previous: Option<&Self>,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<Option<Self>, SyncError> {
        let response = match storage.stream_object(path, None).await {
            Ok(response) => response,
            Err(error) if error.failure() == StorageFailure::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if let Some(previous) = previous {
            if response.metadata() == &previous.object {
                return Ok(Some(previous.clone()));
            }
        }
        let input = ObjectInput::receive_response(database, storage, path, response).await?;
        let mut raw = input.local_bytes().await?;
        let mut prefix = read_exact(&mut raw, FloorPrefixDecoder::HEADER_LENGTH, path)?;
        let mut decoder = checked(path, FloorPrefixDecoder::start(&prefix))?;
        let key_id = decoder.key;
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
            FloorFrames::decode_header(&snapshot_object::frame(&mut plain, path)?),
        )?;
        let header = decoder.header().clone();
        let mut announcements = Vec::new();
        while plain.stream_position().map_err(SyncError::disk)? < plain.len() {
            if let Some(original) = checked(
                path,
                decoder.frame(&snapshot_object::frame(&mut plain, path)?),
            )? {
                announcements.push(original);
            }
        }
        checked(path, decoder.finish())?;
        let signed = SignedDigest::read(input.local_bytes().await?, path)?;
        let object = input.object().clone();
        input.release().await?;
        Ok(Some(Self {
            object,
            signed,
            header,
            announcements,
            copies,
        }))
    }

    /// Call after reconstructing history through `at`, including bootstrap
    /// snapshots when the caller did not yet know this signer's binding.
    pub(crate) fn authenticate(&self, history: &History) -> Result<(), SyncError> {
        let path = &self.object.path;
        let (_, signer) = history
            .owner(self.header.author, self.header.at)
            .map_err(|cause| crate::write_object::damaged(path, cause))?;
        self.signed.verify(path, &signer, None)?;
        Ok(())
    }

    /// The observed revision a conditional replacement names.
    pub(crate) fn object(&self) -> &StoredObject {
        &self.object
    }
}

#[cfg(test)]
#[path = "floor_object_tests.rs"]
mod tests;
