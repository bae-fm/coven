//! Signed snapshot prefixes, and their bodies read once in one stream.

use crate::{
    history::{History, KeyCopy},
    object_input::ObjectInput,
    slot_object::{read_exact, SignedDigest},
    write_object::{checked, damaged},
    Refusal, SyncError,
};
use coven_crypto::{MemberIdentity, Signature, StoreKeyring, SIGNATURE_LEN};
use coven_database::Database;
use coven_format::{
    admin::SnapshotId, floor::OriginalSlot, path::ObjectPath, sealed_slot::SlotObjectPrefix,
    sealed_snapshot::SnapshotObjectPrefix,
};
use coven_storage::{ByteRange, Storage};
use std::{
    io::{Read, Seek},
    sync::Arc,
};

#[path = "snapshot_body.rs"]
mod body;
pub(crate) use body::{BodyFailure, SnapshotBody};

pub(crate) struct SnapshotObject {
    input: ObjectInput,
    prefix: SnapshotObjectPrefix,
    signature: Signature,
    prefix_bytes: Vec<u8>,
    /// The object's first bytes, from its start, that reading the prefix
    /// moved and the body goes on from: on a provider whose ranged read
    /// moves whole parts, the first part; otherwise none (§16.2).
    head: Vec<u8>,
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
    /// only when its rows or settlement evidence are needed. On a provider
    /// whose ranged read moves whole parts, the prefix comes instead with
    /// the object's first part, read once in one stream that stops at its
    /// end and kept for the body to go on from (§16.2, Appendix E.1).
    pub(crate) async fn read_prefix(
        database: &Database,
        storage: &dyn Storage,
        path: &ObjectPath,
    ) -> Result<Self, SyncError> {
        let (input, head) = match ObjectInput::retained(database, path).await? {
            Some(input) => (input, Vec::new()),
            None => match storage.read_part_size() {
                Some(part) => {
                    let mut stream = storage.stream_object(path, None).await?;
                    let metadata = stream.metadata().clone();
                    let head = first_bytes(&mut stream, metadata.size.min(part.get())).await?;
                    drop(stream);
                    let kept = head.len().min(SnapshotObjectPrefix::MAX_SIGNED_PREFIX);
                    let response =
                        coven_storage::ObjectRead { metadata, bytes: head[..kept].to_vec() };
                    (
                        ObjectInput::receive_prefix(database, path, response, kept as u64).await?,
                        head,
                    )
                }
                None => {
                    let response = storage.read_object(path, Some(ByteRange::new(0, 64)?)).await?;
                    (ObjectInput::receive_prefix(database, path, response, 64).await?, Vec::new())
                }
            },
        };
        let decoded = async {
            let bytes = input
                .prefix(
                    storage,
                    SnapshotObjectPrefix::MAX_SIGNED_PREFIX,
                    SnapshotObjectPrefix::signed_length,
                )
                .await?;
            let (prefix, signature) = checked(path, SnapshotObjectPrefix::decode_signed(&bytes))?;
            Ok((prefix, signature, bytes[..bytes.len() - SIGNATURE_LEN].to_vec()))
        }
        .await;
        match decoded {
            Ok((prefix, signature, prefix_bytes)) => {
                Ok(Self { input, prefix, signature, prefix_bytes, head })
            }
            Err(refused) => Err(confirm_prefix(&input, storage, refused).await),
        }
    }

    /// The prefix covers what `announcement` says, for its purpose. A kept
    /// prefix that doesn't is refused only once a second read agrees.
    pub(crate) async fn check_announced(
        &self,
        storage: &dyn Storage,
        announcement: &coven_format::sequence::SnapshotAnnouncement,
    ) -> Result<(), SyncError> {
        if self.prefix.covers != announcement.covers || self.prefix.purpose != announcement.purpose
        {
            let refused = crate::write_object::wrong_identity(&self.input.object().path);
            return Err(confirm_prefix(&self.input, storage, refused).await);
        }
        Ok(())
    }

    pub(crate) fn verify_prefix(
        &self,
        history: &History,
        id: &SnapshotId,
    ) -> Result<coven_foundation::id_source::MemberId, SyncError> {
        let path = &self.input.object().path;
        if self.prefix.audience != id.audience {
            return Err(crate::write_object::wrong_identity(path));
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

/// The first `length` bytes `stream` delivers.
async fn first_bytes(
    stream: &mut coven_storage::ObjectStream<'_>,
    length: u64,
) -> Result<Vec<u8>, SyncError> {
    let mut bytes = vec![0; length as usize];
    let mut filled = 0;
    while filled < bytes.len() {
        match stream.read(&mut bytes[filled..]).await? {
            0 => {
                return Err(coven_storage::StorageFailure::Protocol
                    .with_source("stream ended before the object")
                    .into())
            }
            read => filled += read,
        }
    }
    Ok(bytes)
}

/// A refusal judges the stored snapshot, not one read of it, and nothing
/// checks a prefix's transfer until the whole object's digest is known: a
/// kept prefix a check refused is read again, and the refusal stands only
/// when the fresh prefix is the kept one. A different one is dropped, and
/// the read fails with `Corrupted` (§19.1).
async fn confirm_prefix(
    input: &ObjectInput,
    storage: &dyn Storage,
    refused: SyncError,
) -> SyncError {
    if !matches!(refused, SyncError::Damaged(_)) {
        return refused;
    }
    match input.kept_ranges_stored(storage).await {
        Ok(true) => refused,
        Ok(false) => match input.clone().discard().await {
            Ok(()) => crate::object_input::corrupted(&input.object().path),
            Err(error) => error,
        },
        Err(error) => error,
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

/// A store or circle key opened from a copy routed to this member.
pub(crate) enum RoutedKey {
    Store(coven_crypto::StoreKey),
    Circle(coven_crypto::CircleKey),
}

impl RoutedKey {
    /// Open the key `saved`'s box holds for `member`. A box sealed to a pair
    /// this member holds that doesn't open at its slot's path and place is
    /// that slot's damage, not a local failure.
    pub(crate) fn open(saved: &KeyCopy, member: &MemberIdentity) -> Result<Self, SyncError> {
        let (copy, path) = (&saved.copy, &saved.path);
        let context =
            SlotObjectPrefix::copy_context(saved.format, saved.key, saved.place, &copy.routing)?;
        match copy.routing.audience {
            coven_merge::Audience::Store => {
                copy.open_store_key(member, path.as_str(), &context).map(Self::Store)
            }
            coven_merge::Audience::Circle(_) => {
                copy.open_circle_key(member, path.as_str(), &context).map(Self::Circle)
            }
        }
        .map_err(|error| damaged(path, Refusal::Decryption { cause: Some(Arc::new(error)) }))
    }

    /// Check the key against the hash its introduction committed to.
    pub(crate) fn check_hash(&self, hash: &[u8; 32]) -> Result<(), coven_crypto::MaterialError> {
        match self {
            Self::Store(key) => key.check_hash(hash),
            Self::Circle(key) => key.check_hash(hash),
        }
    }

    pub(crate) fn insert(self, ring: &mut StoreKeyring) -> Result<(), SyncError> {
        match self {
            Self::Store(key) => ring.insert_store_key(key)?,
            Self::Circle(key) => ring.insert_circle_key(key)?,
        }
        Ok(())
    }
}

/// What `original`'s boxes route to this member: the keys those sealed to a
/// pair it holds open, and whether one names it but a pair it doesn't hold,
/// one its replacement's copies go to (§12.3). Bootstrap may open a box
/// before knowing its signer's history. The keys are tentative until the
/// original slots and enclosing object authenticate.
pub(crate) fn routed(
    original: &OriginalSlot,
    store: coven_foundation::id_source::StoreId,
    member: &MemberIdentity,
) -> Result<(Vec<RoutedKey>, bool), SyncError> {
    let path = original.path(store);
    let object = checked(&path, coven_format::sealed_slot::BoundedSlot::decode(&original.object))?;
    let mut keys = Vec::new();
    let mut past = false;
    for saved in KeyCopy::all(&path, &object.prefix) {
        if saved.route().held_by(member) {
            keys.push(RoutedKey::open(&saved, member)?);
        } else {
            past |= saved.route().member == member.member();
        }
    }
    Ok((keys, past))
}

pub(crate) fn open_routing(
    original: &OriginalSlot,
    store: coven_foundation::id_source::StoreId,
    ring: &mut StoreKeyring,
    member: &MemberIdentity,
) -> Result<(), SyncError> {
    routed(original, store, member)?.0.into_iter().try_for_each(|key| key.insert(ring))
}

#[cfg(test)]
#[path = "snapshot_object_tests.rs"]
mod tests;
