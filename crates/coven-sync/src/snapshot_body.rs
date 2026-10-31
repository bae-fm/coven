//! A snapshot's body, read once in one stream after its prefix and kept
//! nowhere whole (§16.2, §21.1).

use super::SnapshotObject;
use crate::{
    slot_object::{DigestStream, SignedDigest},
    write_object::{checked, damaged, database_failure},
    Refusal, SyncError,
};
use coven_crypto::{DerivedKeys, StoreKeyring};
use coven_database::{LoadStep, SnapshotStaging};
use coven_format::{
    chunks::CHUNK_SIZE,
    floor::OriginalSlot,
    path::ObjectPath,
    sealed_stream::StreamObjectLayout,
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

impl SnapshotObject {
    /// Stream the body after the prefix once. Every byte is hashed for the
    /// announced digest and the signature, each chunk is opened, the history
    /// records are read, and each batch of plaintext goes to `staging`. No
    /// sealed byte is retained (§16.2). A body that fails its checks is read
    /// to its end, so it is refused for its digest first when that differs.
    /// An interrupted stream starts over. The object's bytes count toward
    /// the step `progress` is on as they arrive.
    pub(crate) async fn read_body(
        &self,
        storage: &dyn coven_storage::Storage,
        ring: &StoreKeyring,
        announced: &[u8; 32],
        mut staging: Option<&mut SnapshotStaging>,
        mut progress: Option<&mut ProgressReport<LoadStep>>,
    ) -> Result<SnapshotBody, SyncError> {
        let path = &self.input.object().path;
        let size = self.input.object().size;
        let plaintext = checked(path, self.prefix.plaintext_length(size))?;
        let start = self.prefix_bytes.len() as u64 + 64;
        let key = crate::write_seal::derive(ring, &self.prefix.audience, self.prefix.key)?;
        let mut digest = DigestStream::new();
        digest.update(&self.prefix_bytes);
        digest.update(self.signature.as_bytes());
        let mut body = HashedBody {
            response: self.input.stream_from(storage, start).await?,
            digest,
            remaining: size - start - 64,
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
                Err(error @ (SyncError::Damaged(_) | SyncError::UpdateRequired(_))) => {
                    refused = Some(error);
                    body.pass_over().await?;
                }
                Err(error) => return Err(error),
            }
            // Everything but the final signature and what is still to come.
            if let Some(progress) = progress.as_deref_mut() {
                progress.done(size - 64 - body.remaining);
            }
        }
        let signature: [u8; 64] = body.read(64).await?.try_into().expect("64 bytes");
        if let Some(progress) = progress {
            progress.done(size);
        }
        if body.response.read(&mut [0]).await? != 0 {
            return Err(StorageFailure::Protocol
                .with_source("stream has extra bytes")
                .into());
        }
        let digest = body.digest.finish(signature);
        if digest.complete() != announced {
            return Err(damaged(path, Refusal::WrongIdentity { cause: None }));
        }
        if let Some(refused) = refused {
            return Err(refused);
        }
        if opening.opened != plaintext {
            return Err(damaged(path, Refusal::Parse { cause: None }));
        }
        checked(path, opening.layout.read_signature(&signature))?;
        checked(path, opening.decoder.finish())?;
        let Opening {
            decoder,
            mut originals,
            claims,
            ..
        } = opening;
        originals.sort_by_key(|original| original.slot);
        Ok(SnapshotBody {
            header: decoder.header().expect("finished snapshot").clone(),
            originals,
            claims,
            digest,
        })
    }
}

/// The sealed bytes after the prefix, hashed as they arrive, up to the
/// object's final signature.
struct HashedBody<'a> {
    response: ObjectStream<'a>,
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
            return Err(damaged(
                self.path,
                Refusal::from(coven_format::Error::Truncated),
            ));
        }
        let mut bytes = vec![0; length];
        let mut filled = 0;
        while filled < length {
            match self.response.read(&mut bytes[filled..]).await? {
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
            self.read(self.remaining.min(CHUNK_SIZE as u64) as usize)
                .await?;
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
    ) -> Result<(), SyncError> {
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
            .map_err(|error| {
                damaged(
                    path,
                    Refusal::Decryption {
                        cause: Some(Arc::new(error)),
                    },
                )
            })?;
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
}
