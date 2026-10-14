//! Public routing and recipient-checked anonymous key copies (D9/D11).

use crate::wire::{Decoder, Encoder, Wire};
use crate::Error;
use coven_crypto::{CircleKey, CryptoError, MemberId, MemberKeys, SealingPublicKey, StoreKey};
use coven_foundation::id_source::KeyId;
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
    pub sealing: SealingPublicKey,
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
    pub fn addressed_to(&self, member: &MemberId, sealing: &SealingPublicKey) -> bool {
        self.member == *member && self.sealing == *sealing
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
            Audience::Store => 84,
            Audience::Circle(_) => 100,
        }
    }

    fn box_length(&self) -> usize {
        // Ephemeral key, nonce, identified key material and authentication tag.
        32 + coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD
            + match self.audience {
                Audience::Store => 48,
                Audience::Circle(_) => 64,
            }
    }
}

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
            sealing: SealingPublicKey::get(&mut input)?,
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
    pub fn open_store_key(&self, member: &MemberKeys, path: &str) -> Result<StoreKey, CryptoError> {
        self.check_recipient(member)?;
        if self.prefix.audience != Audience::Store {
            return Err(CryptoError::KeyIdentity);
        }
        let (prefix, sealed) = self.bytes.split_at(self.prefix.length());
        let key = member.open_store_key(path, prefix, sealed)?;
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
        member: &MemberKeys,
        path: &str,
    ) -> Result<CircleKey, CryptoError> {
        self.check_recipient(member)?;
        let Audience::Circle(circle) = self.prefix.audience else {
            return Err(CryptoError::KeyIdentity);
        };
        let (prefix, sealed) = self.bytes.split_at(self.prefix.length());
        let key = member.open_circle_key(path, prefix, sealed)?;
        if key.circle() != circle || key.id() != self.prefix.key {
            return Err(CryptoError::KeyIdentity);
        }
        Ok(key)
    }

    fn check_recipient(&self, member: &MemberKeys) -> Result<(), CryptoError> {
        if !self
            .prefix
            .addressed_to(&member.member_id(), &member.sealing_public_key())
        {
            return Err(CryptoError::Authentication);
        }
        Ok(())
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
