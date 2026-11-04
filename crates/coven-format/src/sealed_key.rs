//! Anonymous key boxes embedded in the clear routing prefix of a slot (D9/D11).

use crate::error::{require, Error, Rule};
use crate::wire::{put_blob, wire_struct, Decoder, Encoder, Wire};
use coven_crypto::{
    CircleKey, CryptoError, MemberIdentity, MemberKeys, SealingKey, StoreKey, KEY_BOX_HEAD_LEN,
    KEY_BOX_TAG_LEN,
};
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
    /// Whether a held historical private credential can open the box.
    pub fn held_by(&self, member: &MemberIdentity) -> bool {
        self.member == member.member() && member.credential(&self.sealing).is_some()
    }
    /// The encoded routing of a store copy: audience tag, key, member and
    /// sealing key.
    pub(crate) const STORE_LEN: usize = 1 + 16 + 16 + 32;
    /// A circle copy's routing adds the circle id to the audience.
    pub(crate) const CIRCLE_LEN: usize = Self::STORE_LEN + 16;

    pub(crate) fn material_length(&self) -> usize {
        match self.audience {
            Audience::Store => StoreKey::ENCODED_LEN,
            Audience::Circle(_) => CircleKey::ENCODED_LEN,
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
        let tag = self.sealed.len() - KEY_BOX_TAG_LEN;
        out.bytes(&self.sealed[..KEY_BOX_HEAD_LEN])?;
        put_blob(&self.sealed[KEY_BOX_HEAD_LEN..tag], out)?;
        out.bytes(&self.sealed[tag..])
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let routing = KeyCopyPrefix::get(input)?;
        let mut sealed = input.take(KEY_BOX_HEAD_LEN)?.to_vec();
        let length = u32::get(input)? as usize;
        require(length == routing.material_length(), "key box ciphertext", Rule::StreamLength)?;
        sealed.extend_from_slice(input.take(length)?);
        sealed.extend_from_slice(input.take(KEY_BOX_TAG_LEN)?);
        Ok(Self { routing, sealed })
    }
}
impl CopyBox {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        require(
            self.sealed.len()
                == KEY_BOX_HEAD_LEN + self.routing.material_length() + KEY_BOX_TAG_LEN,
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
        let key = self.credential(member)?.open_store_key(path, prefix, &self.sealed)?;
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
        let key = self.credential(member)?.open_circle_key(path, prefix, &self.sealed)?;
        if key.circle() != circle || key.id() != self.routing.key {
            return Err(CryptoError::KeyIdentity);
        }
        Ok(key)
    }
    fn credential<'m>(&self, member: &'m MemberIdentity) -> Result<&'m MemberKeys, CryptoError> {
        if self.routing.member != member.member() {
            return Err(CryptoError::Authentication);
        }
        member.credential(&self.routing.sealing).ok_or(CryptoError::Authentication)
    }
}

#[cfg(test)]
#[path = "sealed_key_tests.rs"]
mod tests;
