//! The single-chunk join request envelope (D9).

use crate::error::{bound, require, Error, Rule};
use crate::sealed;
use crate::wire::MAX_OBJECT;
use coven_crypto::{Signature, SEALED_OBJECT_CHUNK_OVERHEAD};

/// Join requests have no clear routing beyond their kind and format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SingleChunkPrefix {
    /// Kind 36; the path identifies the invite key.
    JoinRequest,
}
impl SingleChunkPrefix {
    /// Encode the exact associated-data prefix.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.encode_in(crate::FormatVersion::CURRENT)
    }
    /// Encode in the recorded format.
    pub fn encode_in(&self, format: crate::FormatVersion) -> Result<Vec<u8>, Error> {
        Ok(vec![
            36,
            (format.number() >> 8) as u8,
            format.number() as u8,
        ])
    }
    /// Frame one sealed plaintext frame, before its signature.
    pub fn encode_chunk(&self, chunk: &[u8]) -> Result<Vec<u8>, Error> {
        self.encode_chunk_in(crate::FormatVersion::CURRENT, chunk)
    }
    /// Frame the chunk in the recorded format.
    pub fn encode_chunk_in(
        &self,
        format: crate::FormatVersion,
        chunk: &[u8],
    ) -> Result<Vec<u8>, Error> {
        check_frame_length(chunk)?;
        let mut bytes = self.encode_in(format)?;
        bytes.extend_from_slice(&sealed::encode_chunk(chunk)?);
        Ok(bytes)
    }
}

/// A borrowed, structurally checked join-request envelope.
#[derive(Debug)]
pub enum SingleChunkObject<'a> {
    /// The chunk and signature require authentication before using the request.
    JoinRequest {
        /// Nonce, ciphertext and tag.
        chunk: &'a [u8],
        /// Signature by the supplied member key.
        signature: Signature,
    },
}
impl<'a> SingleChunkObject<'a> {
    /// Decode exactly one join-request envelope.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, Error> {
        sealed::prefix(bytes, 36)?;
        let body = &bytes[3..];
        let length = sealed::chunk_length(body, MAX_OBJECT)?;
        let chunk = body.get(4..length).ok_or(Error::Truncated)?;
        check_frame_length(chunk)?;
        let signature = sealed::signature(body.get(length..).ok_or(Error::Truncated)?)?;
        Ok(Self::JoinRequest { chunk, signature })
    }
    /// Clear routing prefix.
    pub fn prefix(&self) -> SingleChunkPrefix {
        SingleChunkPrefix::JoinRequest
    }
    /// The sealed frame bytes.
    pub fn chunk(&self) -> &'a [u8] {
        let Self::JoinRequest { chunk, .. } = self;
        chunk
    }
    /// The required signature.
    pub fn signature(&self) -> &Signature {
        let Self::JoinRequest { signature, .. } = self;
        signature
    }
    /// Reproduce the complete envelope.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = self.signed_bytes()?;
        bytes.extend_from_slice(self.signature().as_bytes());
        Ok(bytes)
    }
    /// Every byte before the signature.
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
