//! A pairing's handover: the member's private pair, sealed to the sealing key
//! of the request the person approved (D8, D11).

use crate::error::{exact_length, Error};
use crate::path::ObjectPath;
use coven_crypto::{
    seal_handover, CryptoError, MemberKeys, SealingKey, KEY_BOX_HEAD_LEN, KEY_BOX_TAG_LEN,
};
use coven_foundation::id_source::MemberId;
use zeroize::Zeroizing;

const KIND: u8 = crate::kinds::HANDOVER;
const PREFIX_LEN: usize = 3;
// The plaintext is the member id, then the pair as a length-prefixed blob.
const PLAINTEXT_LEN: usize = 16 + 4 + MemberKeys::SECRET_LEN;
/// A handover's complete length in bytes.
pub const LENGTH: usize = PREFIX_LEN + KEY_BOX_HEAD_LEN + PLAINTEXT_LEN + KEY_BOX_TAG_LEN;

/// What the approving write puts at the request's path in the request's place
/// (§13.2). It is one anonymous box with no chunks and no signature: the box
/// authenticates every byte, and the pair it carries proves nothing until the
/// new device's loading checks it against history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handover {
    /// The box's ephemeral key, nonce, ciphertext and tag.
    sealed: Vec<u8>,
}

impl Handover {
    /// Seal `member`'s private pair to the sealing key of the request at
    /// `path`. The bytes are fixed here; a retry of the create repeats them.
    pub fn seal(
        member: MemberId,
        keys: &MemberKeys,
        recipient: &SealingKey,
        path: &ObjectPath,
    ) -> Result<Self, CryptoError> {
        let blob = keys.to_secret_bytes();
        let mut plaintext = Zeroizing::new(Vec::with_capacity(PLAINTEXT_LEN));
        plaintext.extend_from_slice(member.0.as_bytes());
        plaintext.extend_from_slice(&(blob.as_bytes().len() as u32).to_be_bytes());
        plaintext.extend_from_slice(blob.as_bytes());
        let prefix = crate::FormatVersion::CURRENT.prefix(KIND);
        let sealed = seal_handover(&plaintext, recipient, path.as_str(), &prefix)?;
        Ok(Self { sealed })
    }

    /// The object's complete bytes: its kind and format, then the box.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(LENGTH);
        bytes.extend_from_slice(&crate::FormatVersion::CURRENT.prefix(KIND));
        bytes.extend_from_slice(&self.sealed);
        bytes
    }

    /// Read a handover. Bytes of another kind, such as a join request or an
    /// ended pairing's marker, fail with `UnknownTag`; one of another length
    /// is truncated or has trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        crate::sealed::prefix(bytes, KIND)?;
        exact_length(bytes.len(), LENGTH)?;
        Ok(Self { sealed: bytes[PREFIX_LEN..].to_vec() })
    }

    /// Open it at `path` with the pair the request carried the sealing key of.
    /// Returns the member whose pair it is and the pair; the caller requires
    /// the member to be the pairing's and loading to check the pair.
    #[expect(
        clippy::map_err_ignore,
        reason = "a sealed value whose pair does not decode is malformed as a whole"
    )]
    pub fn open(
        &self,
        path: &ObjectPath,
        asking: &MemberKeys,
    ) -> Result<(MemberId, MemberKeys), CryptoError> {
        let prefix = crate::FormatVersion::CURRENT.prefix(KIND);
        let plaintext = asking.open_handover(path.as_str(), &prefix, &self.sealed)?;
        let (member, rest) = plaintext.split_first_chunk::<16>().ok_or(CryptoError::Malformed)?;
        let (length, rest) = rest.split_first_chunk::<4>().ok_or(CryptoError::Malformed)?;
        if u32::from_be_bytes(*length) as usize != rest.len() {
            return Err(CryptoError::Malformed);
        }
        let keys = MemberKeys::from_secret_bytes(rest).map_err(|_| CryptoError::Malformed)?;
        Ok((MemberId(uuid::Uuid::from_bytes(*member)), keys))
    }
}

#[cfg(test)]
#[path = "handover_tests.rs"]
mod tests;
