//! A sealed snapshot is checked before and after publication (§15, SP step 8):
//! first opened locally through the reader's own checks, then confirmed by the
//! provider's checksum of the complete stored object or one full readback.

use super::io::SnapshotInput;
use crate::SyncError;
use coven_crypto::{ContentHash, ContentHasher, DerivedKeys, ObjectHasher, SigningKey};
use coven_format::sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix};
use coven_storage::{ObjectPath, Storage, StorageFailure};
use std::io::{self, Read};

/// What sealing produced: the signed prefix and the hash of every plaintext
/// frame the database emitted, in order.
pub(super) struct Sealed {
    pub(super) prefix: SnapshotObjectPrefix,
    pub(super) plaintext: ContentHash,
}

/// The sealed temporary file as a reader opens it: the prefix and its
/// signature, each chunk decrypted in order, the plaintext equal to what was
/// sealed, and the whole-object signature. Any failure ends the stream with an
/// error, so the database check never accepts a damaged file.
pub(super) struct SealedPlaintext {
    input: SnapshotInput,
    path: String,
    prefix: Vec<u8>,
    key: DerivedKeys,
    signer: SigningKey,
    expected: ContentHash,
    layout: Option<SnapshotObjectLayout>,
    hash: ObjectHasher,
    plain: Option<ContentHasher>,
    offset: u64,
    end: u64,
    pending: Vec<u8>,
    position: usize,
}

impl SealedPlaintext {
    pub(super) fn open(
        input: SnapshotInput,
        path: &ObjectPath,
        sealed: &Sealed,
        key: DerivedKeys,
        signer: SigningKey,
    ) -> Result<Self, SyncError> {
        let prefix = sealed.prefix.encode()?;
        let size = input.size();
        let start = prefix.len() as u64 + 64;
        let end = size
            .checked_sub(64)
            .filter(|end| *end >= start)
            .ok_or(coven_format::Error::Truncated)?;
        let signed = input.read_at(0, start as usize)?;
        if signed[..prefix.len()] != prefix {
            return Err(damaged(
                "sealed snapshot prefix differs from the one signed",
            ));
        }
        let signature = coven_crypto::Signature::from_bytes(
            signed[prefix.len()..]
                .try_into()
                .map_err(|_| coven_format::Error::Truncated)?,
        );
        signer.verify_prefix(path.as_str(), &prefix, &signature)?;
        let mut hash = ObjectHasher::new();
        hash.update(&signed);
        Ok(Self {
            input,
            path: path.as_str().to_owned(),
            prefix,
            key,
            signer,
            expected: sealed.plaintext,
            layout: Some(SnapshotObjectLayout::new()),
            hash,
            plain: Some(ContentHasher::new()),
            offset: start,
            end,
            pending: Vec::new(),
            position: 0,
        })
    }

    /// Decrypt the next chunk, or check the object at its end.
    fn advance(&mut self) -> Result<bool, SyncError> {
        let Some(layout) = self.layout.as_mut() else {
            return Ok(false);
        };
        if self.offset < self.end {
            if self.end - self.offset < 4 {
                return Err(coven_format::Error::Truncated.into());
            }
            let length = layout.chunk_length(&self.input.read_at(self.offset, 4)?)?;
            if length as u64 > self.end - self.offset {
                return Err(coven_format::Error::Truncated.into());
            }
            let piece = self.input.read_at(self.offset, length)?;
            self.offset += length as u64;
            self.hash.update(&piece);
            let index = layout.index();
            let sealed = layout.decode_chunk(&piece)?;
            self.pending =
                self.key
                    .open_object_chunk(&self.path, &self.prefix, 0, index, sealed)?;
            self.position = 0;
            if let Some(plain) = self.plain.as_mut() {
                plain.update(&self.pending);
            }
            return Ok(true);
        }
        let layout = self.layout.take().expect("checked above");
        let signature = layout.read_signature(&self.input.read_at(self.end, 64)?)?;
        let hash = std::mem::replace(&mut self.hash, ObjectHasher::new());
        self.signer
            .verify_object(&self.path, &hash.finish(), &signature)?;
        if self.plain.take().map(ContentHasher::finish) != Some(self.expected) {
            return Err(damaged(
                "sealed snapshot opens to other plaintext than it sealed",
            ));
        }
        Ok(false)
    }
}

impl Read for SealedPlaintext {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        while self.position == self.pending.len() {
            if !self.advance().map_err(io::Error::other)? {
                return Ok(0);
            }
        }
        let length = bytes.len().min(self.pending.len() - self.position);
        bytes[..length].copy_from_slice(&self.pending[self.position..self.position + length]);
        self.position += length;
        Ok(length)
    }
}

fn damaged(reason: &'static str) -> SyncError {
    StorageFailure::Protocol.with_source(reason).into()
}

/// Confirm the provider stored exactly the local bytes: its checksum of the
/// complete object when it reports one in an algorithm coven computes, else one
/// full readback. A multipart composite is not a whole-object checksum. A
/// mismatch is an integrity failure, never a publication (§15).
pub(super) async fn confirm_stored(
    storage: &dyn Storage,
    path: &ObjectPath,
    size: u64,
    read: impl Fn(u64, usize) -> Result<Vec<u8>, SyncError>,
) -> Result<coven_storage::StoredObject, SyncError> {
    let stored = storage
        .status(path)
        .await?
        .ok_or_else(|| StorageFailure::NotFound.with_source("uploaded snapshot is absent"))?;
    if stored.size != size {
        return Err(integrity(
            "stored snapshot size differs from the sealed file",
        ));
    }
    let confirmed = match &stored.checksum {
        Some(checksum) => checksum.confirms(size, &read)?,
        None => None,
    };
    let equal = match confirmed {
        Some(equal) => equal,
        None => {
            let mut hasher = ObjectHasher::new();
            let mut offset = 0;
            while offset < size {
                let length = (size - offset).min(1024 * 1024) as usize;
                hasher.update(&read(offset, length)?);
                offset += length as u64;
            }
            crate::identity::stored_digest(storage, path).await? == (size, hasher.finish())
        }
    };
    if !equal {
        return Err(integrity(
            "stored snapshot bytes differ from the sealed file",
        ));
    }
    Ok(stored)
}

fn integrity(reason: &'static str) -> SyncError {
    StorageFailure::Protocol.with_source(reason).into()
}

#[cfg(test)]
#[path = "snapshot_verify_tests.rs"]
mod tests;
