//! Signed snapshot prefixes and local readers of their retained sealed bodies.

use crate::{
    chunk_reader::SlotPlaintext,
    history::History,
    object_input::ObjectInput,
    slot_object::{read_error, read_exact, verify_complete},
    write_object::{checked, damaged},
    Refusal, SyncError,
};
use coven_crypto::{MemberIdentity, Signature, StoreKeyring};
use coven_database::Database;
use coven_format::{
    admin::SnapshotId,
    floor::OriginalSlot,
    path::ObjectPath,
    sealed_snapshot::SnapshotObjectPrefix,
    snapshot::{LastClaim, SnapshotDecoder, SnapshotHeader, SnapshotRecord},
};
use coven_storage::{ByteRange, Storage};
use std::{
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

pub(crate) struct SnapshotObject {
    input: ObjectInput,
    prefix: SnapshotObjectPrefix,
    signature: Signature,
    prefix_bytes: Vec<u8>,
}

/// The historical records needed to authenticate a store snapshot. These are
/// tentative until its complete signature and historical binding verify.
pub(crate) struct SnapshotHistory {
    pub(crate) header: SnapshotHeader,
    pub(crate) originals: Vec<OriginalSlot>,
    pub(crate) claims: Vec<LastClaim>,
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
    pub(crate) fn into_prefix(self) -> SnapshotObjectPrefix {
        self.prefix
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
    pub(crate) async fn release(self) -> Result<(), SyncError> {
        self.input.release().await
    }
    /// Read at most two bounded prefix ranges. The remaining body is fetched
    /// only when its rows or settlement evidence are needed.
    pub(crate) async fn read_prefix(
        database: &Database,
        storage: &dyn Storage,
        path: &ObjectPath,
    ) -> Result<Self, SyncError> {
        let input = match ObjectInput::retained(database, path).await? {
            Some(input) => input,
            None => {
                let response = storage
                    .read_object(path, Some(ByteRange::new(0, 64)?))
                    .await?;
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
        Ok(Self {
            input,
            prefix,
            signature,
            prefix_bytes,
        })
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
        let (author, signer) = history
            .owner(id.device, self.prefix.covers)
            .map_err(|cause| damaged(path, cause))?;
        if !history.belonged(&id.audience, &author, self.prefix.covers) {
            return Err(damaged(path, Refusal::NotAuthorized));
        }
        signer
            .verify_prefix(path.as_str(), &self.prefix_bytes, &self.signature)
            .map_err(|error| {
                damaged(
                    path,
                    Refusal::Signature {
                        cause: Some(Arc::new(error)),
                    },
                )
            })?;
        Ok(author)
    }

    /// Every returned cursor is local. Authentication still precedes loading.
    pub(crate) async fn plaintext(
        &self,
        storage: &dyn Storage,
        ring: &StoreKeyring,
    ) -> Result<SlotPlaintext, SyncError> {
        self.input.receive(storage).await?;
        let path = &self.input.object().path;
        let key = crate::write_seal::derive(ring, &self.prefix.audience, self.prefix.key)?;
        let length = checked(path, self.prefix.plaintext_length(self.input.object().size))?;
        Ok(SlotPlaintext::new(
            self.input.local_bytes().await?,
            self.prefix_bytes.len() as u64 + 64,
            length,
            Arc::from(path.as_str()),
            Arc::from(self.prefix_bytes.clone()),
            Arc::new(key),
            0,
        ))
    }

    pub(crate) async fn authenticate(
        &self,
        history: &History,
        id: &SnapshotId,
        digest: &[u8; 32],
    ) -> Result<coven_foundation::id_source::MemberId, SyncError> {
        let (_, signer) = history
            .owner(id.device, self.prefix.covers)
            .map_err(|cause| damaged(&self.input.object().path, cause))?;
        verify_complete(
            self.input.local_bytes().await?,
            &self.input.object().path,
            &signer,
            Some(digest),
        )?;
        self.verify_prefix(history, id)
    }
}

pub(crate) fn frame(input: &mut impl Read, path: &ObjectPath) -> Result<Vec<u8>, SyncError> {
    let mut bytes = read_exact(input, coven_format::FRAME_PREFIX_LEN, path)?;
    let length = checked(path, coven_format::frame_length(&bytes))?;
    bytes.extend(read_exact(input, length - bytes.len(), path)?);
    Ok(bytes)
}

pub(crate) fn history(
    mut input: SlotPlaintext,
    prefix: &SnapshotObjectPrefix,
    path: &ObjectPath,
) -> Result<SnapshotHistory, SyncError> {
    input.seek(SeekFrom::Start(0)).map_err(SyncError::disk)?;
    let mut decoder = checked(
        path,
        SnapshotDecoder::start(&frame(&mut input, path)?, prefix),
    )?;
    let header = decoder.header().clone();
    let mut originals = Vec::new();
    let mut claims = Vec::new();
    while input
        .stream_position()
        .map_err(|error| read_error(path, error))?
        < input.len()
    {
        match checked(path, decoder.history_frame(&frame(&mut input, path)?))? {
            Some(
                SnapshotRecord::Admin(original)
                | SnapshotRecord::KeyCopy(original)
                | SnapshotRecord::Announcement(original),
            ) => originals.push(original),
            Some(SnapshotRecord::Claim(claim)) => claims.push(claim),
            None => (),
            Some(_) => unreachable!("history decoder returns only history"),
        }
    }
    checked(path, decoder.finish())?;
    originals.sort_by_key(|original| original.slot);
    Ok(SnapshotHistory {
        header,
        originals,
        claims,
    })
}

/// Bootstrap may use a box before knowing its signer's history. The resulting
/// key is tentative until the original slots and enclosing object authenticate.
pub(crate) fn open_routing(
    original: &OriginalSlot,
    store: coven_foundation::id_source::StoreId,
    ring: &mut StoreKeyring,
    member: &MemberIdentity,
) -> Result<(), SyncError> {
    use coven_format::sealed_slot::{BoundedSlot, SlotKind, SlotObjectPrefix};
    let path = ObjectPath::slot(
        store,
        std::num::NonZeroU64::new(original.slot.0).expect("original slot is positive"),
    );
    let object = checked(&path, BoundedSlot::decode(&original.object))?;
    let SlotKind::KeyCopy(copy) = &object.prefix.kind else {
        return Ok(());
    };
    if !copy.routing.held_by(member) {
        return Ok(());
    }
    let context =
        SlotObjectPrefix::copy_context(object.prefix.format, object.prefix.key, &copy.routing)?;
    match copy.routing.audience {
        coven_merge::Audience::Store => {
            let key = copy.open_store_key(member, path.as_str(), &context)?;
            ring.insert_store_key(key)?;
        }
        coven_merge::Audience::Circle(_) => {
            let key = copy.open_circle_key(member, path.as_str(), &context)?;
            ring.insert_circle_key(key)?;
        }
    }
    Ok(())
}
