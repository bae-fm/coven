//! A snapshot's body, read once in one stream after its prefix and kept
//! nowhere whole (§16.2, §21.1).

use super::SnapshotObject;
use crate::{
    slot_object::{DigestStream, SignedDigest},
    write_object::{checked, damaged, database_failure},
    Refusal, SyncError,
};
use coven_crypto::{DerivedKeys, StoreKeyring, SIGNATURE_LEN};
use coven_database::{LoadStep, SnapshotStaging};
use coven_format::{
    chunks::CHUNK_SIZE,
    floor::OriginalSlot,
    path::ObjectPath,
    sealed_stream::StreamObjectLayout,
    sequence::SnapshotAnnouncement,
    snapshot::{LastClaim, SnapshotHeader, SnapshotRecord},
    snapshot_stream::SnapshotChunkDecoder,
};
use coven_foundation::progress::ProgressReport;
use coven_storage::{ObjectStream, StorageFailure};
use std::sync::Arc;

/// Opened plaintext staged in one input transaction: 16 chunks, 1 MiB.
const STAGED_BATCH: usize = 16 * CHUNK_SIZE;

/// What one pass over a snapshot's body found. All of it is tentative until
/// the snapshot's signature verifies.
pub(crate) struct SnapshotBody {
    pub(crate) header: SnapshotHeader,
    /// Its original admin, key-copy and announcement slots, by slot number.
    pub(crate) originals: Vec<OriginalSlot>,
    pub(crate) claims: Vec<LastClaim>,
    /// The digests its signature is checked against; the complete one
    /// already matched the announcement.
    pub(crate) digest: SignedDigest,
}

/// A body read's failure, and whether it judged the stored object's bytes,
/// which need no second staging to confirm (§19.1): a mismatch with the
/// announced digest, refused once a second read's digest agrees, or a check
/// of bytes whose digest is the announced one. A failure to stage a record
/// judges a staged record, so it is not confirmed.
pub(crate) struct BodyFailure {
    pub(crate) error: SyncError,
    pub(crate) confirmed: bool,
}

impl<E: Into<SyncError>> From<E> for BodyFailure {
    fn from(error: E) -> Self {
        Self { error: error.into(), confirmed: false }
    }
}

impl SnapshotObject {
    /// `read_body_confirmed` for a caller that doesn't ask how its refusal
    /// was confirmed.
    pub(crate) async fn read_body(
        &self,
        storage: &dyn coven_storage::Storage,
        ring: &StoreKeyring,
        announcement: &SnapshotAnnouncement,
        staging: Option<&mut SnapshotStaging>,
        progress: Option<&mut ProgressReport<LoadStep>>,
    ) -> Result<SnapshotBody, SyncError> {
        self.read_body_confirmed(storage, ring, announcement, staging, progress)
            .await
            .map_err(|failure| failure.error)
    }

    /// Stream the body after the prefix once. Every byte is hashed for the
    /// announced digest and the signature, each chunk is opened, the history
    /// records are read, and each batch of plaintext goes to `staging`. No
    /// sealed byte is retained (§16.2). A body that fails its checks is read
    /// to its end, so it is refused for its digest first when that differs.
    /// An interrupted stream starts over. The object's bytes count toward
    /// the step `progress` is on as they arrive. The body must be the one
    /// `announcement` names, with its digest.
    pub(crate) async fn read_body_confirmed(
        &self,
        storage: &dyn coven_storage::Storage,
        ring: &StoreKeyring,
        announcement: &SnapshotAnnouncement,
        mut staging: Option<&mut SnapshotStaging>,
        mut progress: Option<&mut ProgressReport<LoadStep>>,
    ) -> Result<SnapshotBody, BodyFailure> {
        let path = &self.input.object().path;
        let size = self.input.object().size;
        let plaintext = checked(path, self.prefix.plaintext_length(size))?;
        let start = (self.prefix_bytes.len() + SIGNATURE_LEN) as u64;
        let key = crate::write_seal::derive(ring, &self.prefix.audience, self.prefix.key)?;
        let mut digest = DigestStream::new();
        digest.update(&self.prefix_bytes);
        digest.update(self.signature.as_bytes());
        // What the prefix's read moved of the body is read from there, and
        // the rest in one stream from where it ends (§16.2).
        let held = self.head.get(start as usize..).unwrap_or_default();
        let streamed = start + held.len() as u64;
        let response = if streamed < size {
            Some(self.input.stream_from(storage, streamed).await?)
        } else {
            None
        };
        let mut body = HashedBody {
            held,
            response,
            digest,
            remaining: size - start - SIGNATURE_LEN as u64,
            path,
        };
        let mut opening = Opening {
            prefix: &self.prefix_bytes,
            key,
            layout: StreamObjectLayout::new(),
            decoder: SnapshotChunkDecoder::new(self.prefix.clone()),
            opened: 0,
            originals: Vec::new(),
            claims: Vec::new(),
            batch: Vec::new(),
        };
        let mut refused = None;
        while body.remaining > 0 {
            match opening.chunk(&mut body, staging.as_deref_mut()).await {
                Ok(()) => (),
                Err(
                    failure @ BodyFailure {
                        error: SyncError::Damaged(_) | SyncError::UpdateRequired(_),
                        ..
                    },
                ) => {
                    refused = Some(failure);
                    body.pass_over().await?;
                }
                Err(failure) => return Err(failure.error.into()),
            }
            // Everything but the final signature and what is still to come.
            if let Some(progress) = progress.as_deref_mut() {
                progress.done(size - SIGNATURE_LEN as u64 - body.remaining);
            }
        }
        let signature: [u8; SIGNATURE_LEN] =
            body.read(SIGNATURE_LEN).await?.try_into().expect("a signature's length");
        if let Some(progress) = progress {
            progress.done(size);
        }
        if let Some(response) = &mut body.response {
            if response.read(&mut [0]).await? != 0 {
                return Err(StorageFailure::Protocol.with_source("stream has extra bytes").into());
            }
        }
        let digest = body.digest.finish(signature);
        if digest.complete() != &announcement.digest {
            // Not the announced bytes: refused only when a second read's
            // digest is this read's, else a fault on the way changed one
            // (§19.1).
            if self.input.stored_digest(storage).await? != *digest.complete() {
                self.input.clone().discard().await?;
                return Err(crate::object_input::corrupted(path).into());
            }
            return Err(BodyFailure {
                error: crate::write_object::wrong_identity(path),
                confirmed: true,
            });
        }
        // The digest is the announced one: the bytes are the announced bytes,
        // and each check from here judges them alone.
        let judged = |error: SyncError| BodyFailure { error, confirmed: true };
        if let Some(refused) = refused {
            return Err(refused);
        }
        if opening.opened != plaintext {
            return Err(judged(damaged(path, Refusal::Parse { cause: None })));
        }
        checked(path, opening.layout.read_signature(&signature)).map_err(judged)?;
        checked(path, opening.decoder.finish()).map_err(judged)?;
        let Opening { decoder, mut originals, claims, .. } = opening;
        let header = decoder.header().expect("finished snapshot").clone();
        if header.id != announcement.snapshot {
            return Err(judged(crate::write_object::wrong_identity(path)));
        }
        originals.sort_by_key(|original| original.slot);
        Ok(SnapshotBody { header, originals, claims, digest })
    }
}

/// The sealed bytes after the prefix, hashed as they arrive, up to the
/// object's final signature.
struct HashedBody<'a> {
    /// The bytes the prefix's read moved, ahead of `response`.
    held: &'a [u8],
    /// The rest, from where `held` ends; `None` when `held` reaches the end.
    response: Option<ObjectStream<'a>>,
    digest: DigestStream,
    /// Sealed bytes before the final signature still to arrive.
    remaining: u64,
    path: &'a ObjectPath,
}

impl HashedBody<'_> {
    /// The next `length` bytes, hashed unless they are the final signature.
    async fn read(&mut self, length: usize) -> Result<Vec<u8>, SyncError> {
        let signature = self.remaining == 0;
        if !signature && length as u64 > self.remaining {
            return Err(damaged(self.path, Refusal::from(coven_format::Error::Truncated)));
        }
        let mut bytes = vec![0; length];
        let mut filled = 0;
        while filled < length {
            if !self.held.is_empty() {
                let read = self.held.len().min(length - filled);
                bytes[filled..filled + read].copy_from_slice(&self.held[..read]);
                self.held = &self.held[read..];
                filled += read;
                continue;
            }
            let Some(response) = &mut self.response else {
                return Err(StorageFailure::Protocol
                    .with_source("stream ended before the object")
                    .into());
            };
            match response.read(&mut bytes[filled..]).await? {
                0 => {
                    return Err(StorageFailure::Protocol
                        .with_source("stream ended before the object")
                        .into())
                }
                read => filled += read,
            }
        }
        if !signature {
            self.digest.update(&bytes);
            self.remaining -= length as u64;
        }
        Ok(bytes)
    }

    /// Hash the rest of the chunks without opening them.
    async fn pass_over(&mut self) -> Result<(), SyncError> {
        while self.remaining > 0 {
            self.read(self.remaining.min(CHUNK_SIZE as u64) as usize).await?;
        }
        Ok(())
    }
}

/// Opening the chunks in order: the history records they hold, and the
/// plaintext waiting for its staging batch.
struct Opening<'a> {
    /// The clear prefix each chunk authenticates.
    prefix: &'a [u8],
    key: DerivedKeys,
    layout: StreamObjectLayout,
    decoder: SnapshotChunkDecoder,
    opened: u64,
    originals: Vec<OriginalSlot>,
    claims: Vec<LastClaim>,
    batch: Vec<u8>,
}

impl Opening<'_> {
    async fn chunk(
        &mut self,
        body: &mut HashedBody<'_>,
        staging: Option<&mut SnapshotStaging>,
    ) -> Result<(), BodyFailure> {
        let path = body.path;
        let chunk =
            self.open(body).await.map_err(|error| BodyFailure { error, confirmed: true })?;
        if let Some(staging) = staging {
            self.batch.extend_from_slice(&chunk);
            if self.batch.len() >= STAGED_BATCH || body.remaining == 0 {
                staging
                    .stage(std::mem::take(&mut self.batch))
                    .await
                    .map_err(|error| database_failure(path, error))?;
            }
        }
        Ok(())
    }

    /// The next chunk's plaintext, whose history records are read: a check
    /// of the bytes alone.
    async fn open(
        &mut self,
        body: &mut HashedBody<'_>,
    ) -> Result<zeroize::Zeroizing<Vec<u8>>, SyncError> {
        let path = body.path;
        let mut piece = body.read(4).await?;
        let length = checked(path, self.layout.chunk_length(&piece))?;
        piece.extend(body.read(length - 4).await?);
        let index = self.layout.index();
        let chunk = self
            .key
            .open_object_chunk(
                path.as_str(),
                self.prefix,
                0,
                index,
                checked(path, self.layout.decode_chunk(&piece))?,
            )
            .map(zeroize::Zeroizing::new)
            .map_err(|error| damaged(path, Refusal::Decryption { cause: Some(Arc::new(error)) }))?;
        self.opened += chunk.len() as u64;
        let mut bytes = chunk.as_slice();
        while let Some(record) = checked(path, self.decoder.next_history(&mut bytes))? {
            match record {
                SnapshotRecord::Admin(original)
                | SnapshotRecord::KeyCopy(original)
                | SnapshotRecord::Announcement(original) => self.originals.push(original),
                SnapshotRecord::Claim(claim) => self.claims.push(claim),
                _ => unreachable!("history decoding returns only history"),
            }
        }
        Ok(chunk)
    }
}
