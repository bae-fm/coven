//! Public routing and recipient-checked anonymous key copies (D9/D11).

use crate::wire::{Decoder, Encoder, Wire};
use crate::Error;
use coven_crypto::{CircleKey, CryptoError, MemberIdentity, MemberKeys, SealingKey, StoreKey};
use coven_foundation::id_source::{KeyId, MemberId};
use coven_merge::Audience;

/// Routing visible to every reader, including readers without the recipient's keys.
/// These facts can indicate exposure; only the recipient can authenticate the box.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyCopyPrefix {
    /// The audience whose key this copy claims to carry.
    pub audience: Audience,
    /// The identity named by the key's introduction.
    pub key: KeyId,
    /// The member to whom this copy is addressed.
    pub member: MemberId,
    /// The member's exact sealing key the box is sealed to. A copy to another
    /// of the member's credentials is a different copy.
    pub sealing: SealingKey,
}

impl KeyCopyPrefix {
    /// Encode the exact clear prefix bound into both key derivation and authentication.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = Encoder::new();
        37u8.put(&mut out)?;
        crate::FORMAT_VERSION.put(&mut out)?;
        self.audience.put(&mut out)?;
        self.key.put(&mut out)?;
        self.member.put(&mut out)?;
        self.sealing.put(&mut out)?;
        Ok(out.bytes)
    }

    /// Whether this copy names exactly this recipient: its member id and the
    /// sealing key it is sealed to. Matching needs no box opening (§12.2).
    pub fn addressed_to(&self, member: &MemberId, sealing: &SealingKey) -> bool {
        self.member == *member && self.sealing == *sealing
    }

    /// Whether `member` holds the private credential this copy names: its
    /// stable id and exact sealing key (§11).
    pub fn held_by(&self, member: &MemberIdentity) -> bool {
        self.member == member.member() && member.credential(&self.sealing).is_some()
    }

    /// Frame the box produced by crypto with this prefix. Its original random
    /// bytes must be reserved with the copy number and retained for every retry.
    pub fn encode_box(&self, sealed: &[u8]) -> Result<Vec<u8>, Error> {
        exact_length(sealed.len(), self.box_length())?;
        let mut bytes = self.encode()?;
        bytes
            .try_reserve_exact(sealed.len())
            .map_err(|_| Error::Allocation)?;
        bytes.extend_from_slice(sealed);
        Ok(bytes)
    }

    fn length(&self) -> usize {
        match self.audience {
            Audience::Store => 68,
            Audience::Circle(_) => CIRCLE_PREFIX,
        }
    }

    fn box_length(&self) -> usize {
        BOX_OVERHEAD
            + match self.audience {
                Audience::Store => 48,
                Audience::Circle(_) => CIRCLE_MATERIAL,
            }
    }
}

const CIRCLE_PREFIX: usize = 84;
/// A circle key's identified material: its circle, key id and secret.
const CIRCLE_MATERIAL: usize = 64;
/// The box's ephemeral key, nonce and authentication tag.
const BOX_OVERHEAD: usize = 32 + coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD;

/// The longest copy, a circle key's. Every copy has an exact length, so a
/// longer object is no copy and readers never receive past this bound.
pub const MAX_KEY_COPY_OBJECT: usize = CIRCLE_PREFIX + BOX_OVERHEAD + CIRCLE_MATERIAL;

/// A complete kind-37 copy, borrowing the exact prefix and randomized box bytes.
#[derive(Debug)]
pub struct SealedKey<'a> {
    prefix: KeyCopyPrefix,
    bytes: &'a [u8],
}

impl<'a> SealedKey<'a> {
    /// Check the kind, version, routing and exact bounded length without decrypting.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, Error> {
        crate::sealed::prefix(bytes, 37)?;
        let mut input = Decoder::new(&bytes[3..])?;
        let prefix = KeyCopyPrefix {
            audience: Audience::get(&mut input)?,
            key: KeyId::get(&mut input)?,
            member: MemberId::get(&mut input)?,
            sealing: SealingKey::get(&mut input)?,
        };
        input.take(prefix.box_length())?;
        input.finish()?;
        Ok(Self { prefix, bytes })
    }

    /// Inspect public routing. This does not authenticate the box or its sender.
    pub fn prefix(&self) -> &KeyCopyPrefix {
        &self.prefix
    }

    /// Preserve the original prefix, ephemeral key, nonce, ciphertext and tag.
    pub fn encode(&self) -> Vec<u8> {
        self.bytes.to_vec()
    }

    /// Open a tentative store key and require the addressed member, its sealing key
    /// and the payload identity.
    /// Before accepting it, check its hash against the authorized introduction with
    /// [`StoreKey::check_hash`]. A candidate may open its own introduction for validation.
    pub fn open_store_key(
        &self,
        member: &MemberIdentity,
        path: &str,
    ) -> Result<StoreKey, CryptoError> {
        let credential = self.credential(member)?;
        if self.prefix.audience != Audience::Store {
            return Err(CryptoError::KeyIdentity);
        }
        let (prefix, sealed) = self.bytes.split_at(self.prefix.length());
        let key = credential.open_store_key(path, prefix, sealed)?;
        if key.id() != self.prefix.key {
            return Err(CryptoError::KeyIdentity);
        }
        Ok(key)
    }

    /// Open a tentative circle key and require the addressed member, its sealing key
    /// and the payload identity.
    /// Before accepting it, check its hash against the authorized introduction with
    /// [`CircleKey::check_hash`]. Decryption alone does not authorize a key.
    pub fn open_circle_key(
        &self,
        member: &MemberIdentity,
        path: &str,
    ) -> Result<CircleKey, CryptoError> {
        let credential = self.credential(member)?;
        let Audience::Circle(circle) = self.prefix.audience else {
            return Err(CryptoError::KeyIdentity);
        };
        let (prefix, sealed) = self.bytes.split_at(self.prefix.length());
        let key = credential.open_circle_key(path, prefix, sealed)?;
        if key.circle() != circle || key.id() != self.prefix.key {
            return Err(CryptoError::KeyIdentity);
        }
        Ok(key)
    }

    /// The member's held private credential this copy names; a copy to the
    /// member's other credential, or to another member, is not theirs to open.
    fn credential<'m>(&self, member: &'m MemberIdentity) -> Result<&'m MemberKeys, CryptoError> {
        if self.prefix.member != member.member() {
            return Err(CryptoError::Authentication);
        }
        member
            .credential(&self.prefix.sealing)
            .ok_or(CryptoError::Authentication)
    }
}

fn exact_length(actual: usize, expected: usize) -> Result<(), Error> {
    match actual.cmp(&expected) {
        std::cmp::Ordering::Less => Err(Error::Truncated),
        std::cmp::Ordering::Greater => Err(Error::TrailingBytes),
        std::cmp::Ordering::Equal => Ok(()),
    }
}

#[cfg(test)]
#[path = "sealed_key_tests.rs"]
mod tests;
