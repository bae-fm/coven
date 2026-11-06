//! Fixed-size encryption of plaintext streams before provider publication.

use crate::SyncError;
use coven_crypto::{
    DerivedKeys, MemberIdentity, ObjectDigest, ObjectHasher, Signature, SIGNATURE_LEN,
};
use coven_format::{chunks::CHUNK_SIZE, sealed_stream::StreamObjectLayout};
use coven_storage::ObjectPath;
use std::io::{self, Read};

/// One stream object as it is sealed: its prefix, with the prefix's
/// signature when it has one, then each chunk of plaintext sealed at its
/// index, then the signature of every byte before it.
pub(crate) struct StreamSealer {
    path: ObjectPath,
    prefix: Vec<u8>,
    key: DerivedKeys,
    layout: StreamObjectLayout,
    hash: ObjectHasher,
    /// The object's bytes so far.
    size: u64,
}

impl StreamSealer {
    /// Begin the object at `path`, and return its first bytes: the prefix
    /// and its signature.
    pub(crate) fn start(
        path: ObjectPath,
        prefix: Vec<u8>,
        prefix_signature: Option<Signature>,
        key: DerivedKeys,
    ) -> (Self, Vec<u8>) {
        let mut bytes = prefix.clone();
        if let Some(signature) = prefix_signature {
            bytes.extend_from_slice(signature.as_bytes());
        }
        let mut hash = ObjectHasher::new();
        hash.update(&bytes);
        let size = bytes.len() as u64;
        (Self { path, prefix, key, layout: StreamObjectLayout::new(), hash, size }, bytes)
    }

    /// Seal the next chunk, of at most `CHUNK_SIZE` plaintext bytes. A
    /// slot's chunks seal the same on every attempt; a snapshot's or a
    /// floor's take a random nonce.
    pub(crate) fn chunk(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, SyncError> {
        let (path, index) = (self.path.as_str(), self.layout.index());
        let sealed = if self.path.slot_number().is_some() {
            self.key.reseal_object_chunk(path, &self.prefix, 0, index, plaintext)
        } else {
            self.key.seal_object_chunk(path, &self.prefix, 0, index, plaintext)?
        };
        let piece = self.layout.encode_chunk(&sealed)?;
        self.size = grown(self.size, piece.len())?;
        self.hash.update(&piece);
        Ok(piece)
    }

    /// The object's last bytes, the signature of every byte before them,
    /// the digest of the whole object, signature included, and its size.
    pub(crate) fn finish(self, member: &MemberIdentity) -> (Signature, ObjectDigest, u64) {
        let mut hash = self.hash;
        let signature = member.keys().sign_object(self.path.as_str(), &hash.clone().finish());
        hash.update(signature.as_bytes());
        (signature, hash.finish(), self.size + SIGNATURE_LEN as u64)
    }
}

/// An object of `size` bytes grown by a piece of `length`. Its size is a
/// `u64`, which must still hold the final signature.
fn grown(size: u64, length: usize) -> Result<u64, SyncError> {
    size.checked_add(length as u64)
        .filter(|grown| *grown <= u64::MAX - SIGNATURE_LEN as u64)
        .ok_or_else(|| {
            coven_format::Error::Invalid {
                field: "sealed stream size",
                rule: coven_format::error::Rule::Chunk,
            }
            .into()
        })
}

/// A sealed stream object read from its plaintext.
pub(crate) struct SealedStream<R> {
    input: R,
    member: MemberIdentity,
    sealer: Option<StreamSealer>,
    piece: io::Cursor<Vec<u8>>,
}
impl<R> SealedStream<R> {
    pub(crate) fn new(
        input: R,
        path: ObjectPath,
        prefix: Vec<u8>,
        prefix_signature: Option<Signature>,
        key: DerivedKeys,
        member: MemberIdentity,
    ) -> Self {
        let (sealer, bytes) = StreamSealer::start(path, prefix, prefix_signature, key);
        Self { input, member, sealer: Some(sealer), piece: io::Cursor::new(bytes) }
    }
}
impl<R: Read> Read for SealedStream<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let length = self.piece.read(output)?;
            if length > 0 {
                return Ok(length);
            }
            let Some(sealer) = self.sealer.as_mut() else {
                return Ok(0);
            };
            let mut plaintext = zeroize::Zeroizing::new(Vec::new());
            self.input.by_ref().take(CHUNK_SIZE as u64).read_to_end(&mut plaintext)?;
            let bytes = if plaintext.is_empty() {
                let sealer = self.sealer.take().expect("stream has a sealer");
                sealer.finish(&self.member).0.as_bytes().to_vec()
            } else {
                sealer.chunk(&plaintext).map_err(io::Error::other)?
            };
            self.piece = io::Cursor::new(bytes);
        }
    }
}

#[cfg(test)]
#[path = "sealed_stream_tests.rs"]
mod tests;
