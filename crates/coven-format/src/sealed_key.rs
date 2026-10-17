//! Anonymous key boxes embedded in the clear routing prefix of a slot (D9/D11).

use crate::error::{require, Error, Rule};
use crate::wire::{put_blob, wire_struct, Decoder, Encoder, Wire};
use coven_crypto::{CircleKey, CryptoError, MemberIdentity, MemberKeys, SealingKey, StoreKey};
use coven_foundation::id_source::{KeyId, MemberId};
use coven_merge::Audience;

/// Routing authenticated by the anonymous box and the enclosing slot signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyCopyPrefix {
    /// The copied key's audience.
    pub audience: Audience,
    /// The introduced key's identity.
    pub key: KeyId,
    /// The recipient's stable identity.
    pub member: MemberId,
    /// The exact historical sealing credential used for this copy.
    pub sealing: SealingKey,
}
wire_struct!(KeyCopyPrefix, audience, key, member, sealing);
impl KeyCopyPrefix {
    /// Whether this box is addressed to exactly this member and credential.
    pub fn addressed_to(&self, member: &MemberId, sealing: &SealingKey) -> bool {
        self.member == *member && self.sealing == *sealing
    }
    /// Whether a held historical private credential can open the box.
    pub fn held_by(&self, member: &MemberIdentity) -> bool {
        self.member == member.member() && member.credential(&self.sealing).is_some()
    }
    pub(crate) fn material_length(&self) -> usize {
        match self.audience {
            Audience::Store => 48,
            Audience::Circle(_) => 64,
        }
    }
}

/// An original anonymous box, retaining its random bytes across retries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyBox {
    /// Visible audience, key and recipient routing.
    pub routing: KeyCopyPrefix,
    /// Crypto's ephemeral key, nonce, ciphertext and tag, without wire lengths.
    pub sealed: Vec<u8>,
}
impl Wire for CopyBox {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.validate()?;
        self.routing.put(out)?;
        out.bytes(&self.sealed[..56])?;
        put_blob(&self.sealed[56..self.sealed.len() - 16], out)?;
        out.bytes(&self.sealed[self.sealed.len() - 16..])
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let routing = KeyCopyPrefix::get(input)?;
        let mut sealed = input.take(56)?.to_vec();
        let length = u32::get(input)? as usize;
        require(
            length == routing.material_length(),
            "key box ciphertext",
            Rule::StreamLength,
        )?;
        sealed.extend_from_slice(input.take(length)?);
        sealed.extend_from_slice(input.take(16)?);
        Ok(Self { routing, sealed })
    }
}
impl CopyBox {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        require(
            self.sealed.len() == 72 + self.routing.material_length(),
            "key box length",
            Rule::StreamLength,
        )
    }
    /// Open tentatively using the complete slot prefix through `sealing` as
    /// context. The caller verifies the slot signature and key introduction.
    pub fn open_store_key(
        &self,
        member: &MemberIdentity,
        path: &str,
        prefix: &[u8],
    ) -> Result<StoreKey, CryptoError> {
        if self.routing.audience != Audience::Store {
            return Err(CryptoError::KeyIdentity);
        }
        let key = self
            .credential(member)?
            .open_store_key(path, prefix, &self.sealed)?;
        if key.id() != self.routing.key {
            return Err(CryptoError::KeyIdentity);
        }
        Ok(key)
    }
    /// Open a circle box with its exact historical recipient credential.
    pub fn open_circle_key(
        &self,
        member: &MemberIdentity,
        path: &str,
        prefix: &[u8],
    ) -> Result<CircleKey, CryptoError> {
        let Audience::Circle(circle) = self.routing.audience else {
            return Err(CryptoError::KeyIdentity);
        };
        let key = self
            .credential(member)?
            .open_circle_key(path, prefix, &self.sealed)?;
        if key.circle() != circle || key.id() != self.routing.key {
            return Err(CryptoError::KeyIdentity);
        }
        Ok(key)
    }
    fn credential<'m>(&self, member: &'m MemberIdentity) -> Result<&'m MemberKeys, CryptoError> {
        if self.routing.member != member.member() {
            return Err(CryptoError::Authentication);
        }
        member
            .credential(&self.routing.sealing)
            .ok_or(CryptoError::Authentication)
    }
}

#[cfg(test)]
#[path = "sealed_key_tests.rs"]
mod tests;
