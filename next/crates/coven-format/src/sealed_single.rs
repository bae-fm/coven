//! Bounded sealed objects carrying one opaque plaintext frame in section 0.

use crate::error::{bound, require, Error, Rule};
use crate::sealed;
use crate::wire::{Decoder, Encoder, Wire, MAX_OBJECT};
use coven_crypto::{Signature, SEALED_OBJECT_CHUNK_OVERHEAD};
use coven_foundation::id_source::KeyId;

/// Cleartext routing fields available before sealing the frame or signing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SingleChunkPrefix {
    /// Kind 33 and the store key sealing its entry.
    StoreLog(KeyId),
    /// Kind 35 and the store key sealing its positions.
    PostedPositions(KeyId),
    /// Kind 36; the invite key is identified by the object's path.
    JoinRequest,
}
impl SingleChunkPrefix {
    /// The complete cleartext prefix used as associated data.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = Encoder::new();
        let kind = match self {
            Self::StoreLog(_) => 33u8,
            Self::PostedPositions(_) => 35,
            Self::JoinRequest => 36,
        };
        kind.put(&mut out)?;
        crate::FORMAT_VERSION.put(&mut out)?;
        match self {
            Self::StoreLog(key) | Self::PostedPositions(key) => key.put(&mut out)?,
            Self::JoinRequest => (),
        }
        Ok(out.bytes)
    }

    /// Encode every byte before a signature: prefix, length and sealed chunk.
    pub fn encode_chunk(&self, chunk: &[u8]) -> Result<Vec<u8>, Error> {
        check_frame_length(chunk)?;
        let mut bytes = self.encode()?;
        bytes.extend_from_slice(&sealed::encode_chunk(chunk)?);
        Ok(bytes)
    }
}

/// The complete envelope, borrowing its nonce, ciphertext and tag. Opening the
/// chunk and decoding its frame are separate operations; no frame is parsed here.
#[derive(Debug)]
pub enum SingleChunkObject<'a> {
    /// A store-log entry, signed by its author.
    StoreLog {
        /// The store key sealing this entry.
        key: KeyId,
        /// Stored nonce, ciphertext and tag, without the length field.
        chunk: &'a [u8],
        /// Signature over the prefix and entire encoded chunk.
        signature: Signature,
    },
    /// A device's posted positions, authenticated by the store key.
    PostedPositions {
        /// The store key sealing these positions.
        key: KeyId,
        /// Stored nonce, ciphertext and tag, without the length field.
        chunk: &'a [u8],
    },
    /// A request sealed with the invite-derived key and signed by its new member.
    JoinRequest {
        /// Stored nonce, ciphertext and tag, without the length field.
        chunk: &'a [u8],
        /// Signature by the member whose keys the opened frame carries.
        signature: Signature,
    },
}

impl<'a> SingleChunkObject<'a> {
    /// Decode exactly one envelope, bounding its length before any allocation.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, Error> {
        let kind = *bytes.first().ok_or(Error::Truncated)?;
        let (prefix_length, signature_length) = match kind {
            33 => (19, 64),
            35 => (19, 0),
            36 => (3, 64),
            tag => {
                return Err(Error::UnknownTag {
                    field: "single-chunk object kind",
                    tag,
                })
            }
        };
        sealed::prefix(bytes, kind)?;
        let body = bytes.get(prefix_length..).ok_or(Error::Truncated)?;
        let chunk_length = sealed::chunk_length(body, MAX_OBJECT)?;
        let total = chunk_length + signature_length;
        if body.len() < total {
            return Err(Error::Truncated);
        }
        if body.len() > total {
            return Err(Error::TrailingBytes);
        }
        let chunk = &body[4..chunk_length];
        check_frame_length(chunk)?;
        if kind == 36 {
            return Ok(Self::JoinRequest {
                chunk,
                signature: Signature::from_bytes(
                    body[chunk_length..].try_into().expect("64 bytes"),
                ),
            });
        }
        let mut input = Decoder::new(&bytes[3..prefix_length])?;
        let key = KeyId::get(&mut input)?;
        input.finish()?;
        Ok(if kind == 33 {
            Self::StoreLog {
                key,
                chunk,
                signature: Signature::from_bytes(
                    body[chunk_length..].try_into().expect("64 bytes"),
                ),
            }
        } else {
            Self::PostedPositions { key, chunk }
        })
    }

    /// The routing fields, whose encoding is bound into the chunk's authentication.
    pub fn prefix(&self) -> SingleChunkPrefix {
        match self {
            Self::StoreLog { key, .. } => SingleChunkPrefix::StoreLog(*key),
            Self::PostedPositions { key, .. } => SingleChunkPrefix::PostedPositions(*key),
            Self::JoinRequest { .. } => SingleChunkPrefix::JoinRequest,
        }
    }

    /// The stored nonce, ciphertext and tag, to open at section 0 and index 0.
    pub fn chunk(&self) -> &'a [u8] {
        match self {
            Self::StoreLog { chunk, .. }
            | Self::PostedPositions { chunk, .. }
            | Self::JoinRequest { chunk, .. } => chunk,
        }
    }

    /// The author's signature, absent for posted positions.
    pub fn signature(&self) -> Option<&Signature> {
        match self {
            Self::StoreLog { signature, .. } | Self::JoinRequest { signature, .. } => {
                Some(signature)
            }
            Self::PostedPositions { .. } => None,
        }
    }

    /// Reproduce the envelope, including the original nonce and signature.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = self.signed_bytes()?;
        if let Some(signature) = self.signature() {
            bytes.extend_from_slice(signature.as_bytes());
        }
        Ok(bytes)
    }

    /// Encode every byte covered by the signature: prefix and framed ciphertext.
    pub fn signed_bytes(&self) -> Result<Vec<u8>, Error> {
        self.prefix().encode_chunk(self.chunk())
    }
}

fn check_frame_length(chunk: &[u8]) -> Result<(), Error> {
    let length = chunk
        .len()
        .checked_sub(SEALED_OBJECT_CHUNK_OVERHEAD)
        .ok_or(Error::Truncated)?;
    bound(length, MAX_OBJECT, "sealed frame")?;
    require(
        length >= crate::FRAME_PREFIX_LEN,
        "sealed frame",
        Rule::Chunk,
    )
}

#[cfg(test)]
#[path = "sealed_single_tests.rs"]
mod tests;
