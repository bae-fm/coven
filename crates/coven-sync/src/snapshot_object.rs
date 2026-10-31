//! Signed snapshot prefixes, and their bodies read once in one stream.

use crate::{
    history::History,
    object_input::ObjectInput,
    slot_object::{read_exact, SignedDigest},
    write_object::{checked, damaged},
    Refusal, SyncError,
};
use coven_crypto::{MemberIdentity, Signature, StoreKeyring};
use coven_database::Database;
use coven_format::{
    admin::SnapshotId, floor::OriginalSlot, path::ObjectPath, sealed_snapshot::SnapshotObjectPrefix,
};
use coven_storage::{ByteRange, Storage};
use std::{
    io::{Read, Seek},
    sync::Arc,
};

#[path = "snapshot_body.rs"]
mod body;
pub(crate) use body::SnapshotBody;

pub(crate) struct SnapshotObject {
    input: ObjectInput,
    prefix: SnapshotObjectPrefix,
    signature: Signature,
    prefix_bytes: Vec<u8>,
}

pub(crate) fn path(
    store: coven_foundation::id_source::StoreId,
    id: &SnapshotId,
) -> Result<ObjectPath, SyncError> {
    Ok(ObjectPath::snapshot(
        store,
        id.audience.clone(),
        id.device,
        std::num::NonZeroU64::new(id.number).ok_or(coven_database::DbError::DamagedDatabase)?,
    ))
}

impl SnapshotObject {
    pub(crate) fn prefix(&self) -> &SnapshotObjectPrefix {
        &self.prefix
    }
    /// The sealed object's size in storage.
    pub(crate) fn size(&self) -> u64 {
        self.input.object().size
    }
    pub(crate) fn path(&self) -> &ObjectPath {
        &self.input.object().path
    }
    pub(crate) async fn files(&self) -> Result<Option<coven_database::FileEvidence>, SyncError> {
        self.input.files().await
    }
    pub(crate) async fn keep_files(
        &self,
        files: coven_database::FileEvidence,
    ) -> Result<(), SyncError> {
        self.input.keep_files(files).await
    }
    /// Release the retained prefix bytes for eviction; checked file
    /// evidence stays (§17.10).
    pub(crate) async fn release(&self) -> Result<(), SyncError> {
        self.input.clone().release().await
    }
    /// Read at most two bounded prefix ranges. The remaining body is streamed
    /// only when its rows or settlement evidence are needed.
    pub(crate) async fn read_prefix(
        database: &Database,
        storage: &dyn Storage,
        path: &ObjectPath,
    ) -> Result<Self, SyncError> {
        let input = match ObjectInput::retained(database, path).await? {
            Some(input) => input,
            None => {
                let response = storage.read_object(path, Some(ByteRange::new(0, 64)?)).await?;
                ObjectInput::receive_prefix(database, path, response).await?
            }
        };
        let bytes = input
            .prefix(
                storage,
                SnapshotObjectPrefix::MAX_SIGNED_PREFIX,
                SnapshotObjectPrefix::signed_length,
            )
            .await?;
        let (prefix, signature) = checked(path, SnapshotObjectPrefix::decode_signed(&bytes))?;
        let prefix_bytes = bytes[..bytes.len() - 64].to_vec();
        Ok(Self { input, prefix, signature, prefix_bytes })
    }

    pub(crate) fn verify_prefix(
        &self,
        history: &History,
        id: &SnapshotId,
    ) -> Result<coven_foundation::id_source::MemberId, SyncError> {
        let path = &self.input.object().path;
        if self.prefix.audience != id.audience {
            return Err(damaged(path, Refusal::WrongIdentity { cause: None }));
        }
        let (author, signer) =
            history.owner(id.device, self.prefix.covers).map_err(|cause| damaged(path, cause))?;
        if !history.belonged(&id.audience, &author, self.prefix.covers) {
            return Err(damaged(path, Refusal::NotAuthorized));
        }
        signer
            .verify_prefix(path.as_str(), &self.prefix_bytes, &self.signature)
            .map_err(|error| damaged(path, Refusal::Signature { cause: Some(Arc::new(error)) }))?;
        Ok(author)
    }

    /// Check the body's digests against the announced digest, then its
    /// signature and prefix (§16.2, D9).
    pub(crate) fn authenticate(
        &self,
        history: &History,
        id: &SnapshotId,
        body: &SignedDigest,
        announced: &[u8; 32],
    ) -> Result<coven_foundation::id_source::MemberId, SyncError> {
        let (_, signer) = history
            .owner(id.device, self.prefix.covers)
            .map_err(|cause| damaged(&self.input.object().path, cause))?;
        body.verify(&self.input.object().path, &signer, Some(announced))?;
        self.verify_prefix(history, id)
    }
}

pub(crate) fn frame(
    input: &mut (impl Read + Seek),
    path: &ObjectPath,
) -> Result<Vec<u8>, SyncError> {
    let mut bytes = read_exact(input, coven_format::FRAME_PREFIX_LEN, path)?;
    let length = checked(path, coven_format::frame_length(&bytes))?;
    bytes.extend(read_exact(input, length - bytes.len(), path)?);
    Ok(bytes)
}

/// Bootstrap may use a box before knowing its signer's history. The resulting
/// key is tentative until the original slots and enclosing object authenticate.
/// A store or circle key opened from a copy routed to this member.
pub(crate) enum RoutedKey {
    Store(coven_crypto::StoreKey),
    Circle(coven_crypto::CircleKey),
}

impl RoutedKey {
    pub(crate) fn insert(self, ring: &mut StoreKeyring) -> Result<(), SyncError> {
        match self {
            Self::Store(key) => ring.insert_store_key(key)?,
            Self::Circle(key) => ring.insert_circle_key(key)?,
        }
        Ok(())
    }
}

/// Bootstrap may open a box before knowing its signer's history. The key is
/// tentative until the original slots and enclosing object authenticate.
pub(crate) fn routed_key(
    original: &OriginalSlot,
    store: coven_foundation::id_source::StoreId,
    member: &MemberIdentity,
) -> Result<Option<RoutedKey>, SyncError> {
    use coven_format::sealed_slot::{BoundedSlot, SlotKind, SlotObjectPrefix};
    let path = ObjectPath::slot(
        store,
        std::num::NonZeroU64::new(original.slot.0).expect("original slot is positive"),
    );
    let object = checked(&path, BoundedSlot::decode(&original.object))?;
    let SlotKind::KeyCopy(copy) = &object.prefix.kind else {
        return Ok(None);
    };
    if !copy.routing.held_by(member) {
        return Ok(None);
    }
    let context =
        SlotObjectPrefix::copy_context(object.prefix.format, object.prefix.key, &copy.routing)?;
    Ok(Some(match copy.routing.audience {
        coven_merge::Audience::Store => {
            RoutedKey::Store(copy.open_store_key(member, path.as_str(), &context)?)
        }
        coven_merge::Audience::Circle(_) => {
            RoutedKey::Circle(copy.open_circle_key(member, path.as_str(), &context)?)
        }
    }))
}

pub(crate) fn open_routing(
    original: &OriginalSlot,
    store: coven_foundation::id_source::StoreId,
    ring: &mut StoreKeyring,
    member: &MemberIdentity,
) -> Result<(), SyncError> {
    match routed_key(original, store, member)? {
        Some(key) => key.insert(ring),
        None => Ok(()),
    }
}

#[cfg(test)]
#[path = "snapshot_object_tests.rs"]
mod tests;
