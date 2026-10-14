//! A member's new S3 key delivered to their remaining devices (§13, D9):
//! sealed anonymously to the member's sealing key, then signed by the admin
//! who made and recorded it.
//!
//! ```text
//! kind:u8 (40) | version:u16 | member:MemberId | sealing:SealingKey | author:MemberId |
//! box_length:u32 | ephemeral:32 | nonce:24 | ciphertext | tag:16 | signature:64
//! ```
//!
//! The clear prefix is `kind | version | member | sealing | author`; D11 binds it
//! into the box. The signature covers every preceding byte with the path.

use crate::wire::{Decoder, Encoder, Wire};
use crate::{Error, S3Principal};
use coven_crypto::{CryptoError, MemberId, MemberKeys, ObjectHasher, SealingPublicKey, SecretText};
use zeroize::Zeroizing;

const KIND: u8 = 40;
const MAX_TEXT: usize = 1024;

/// The key a delivery carries.
#[derive(Clone)]
pub struct DeliveredKey {
    /// The key's public id.
    pub access_key_id: String,
    /// Its secret.
    pub secret_access_key: SecretText,
    /// Who holds it at the provider.
    pub principal: S3Principal,
}

impl std::fmt::Debug for DeliveredKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeliveredKey")
            .field("access_key_id", &self.access_key_id)
            .field("principal", &self.principal)
            .finish_non_exhaustive()
    }
}

/// The public routing of a delivery: its recipient and the author to verify.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryRouting {
    /// The member whose key this is.
    pub member: MemberId,
    /// The member's sealing key the box is sealed to.
    pub sealing: SealingPublicKey,
    /// The admin who made the key and signed the delivery.
    pub author: MemberId,
}

impl DeliveryRouting {
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = Encoder::new();
        KIND.put(&mut out)?;
        crate::FORMAT_VERSION.put(&mut out)?;
        self.member.put(&mut out)?;
        self.sealing.put(&mut out)?;
        self.author.put(&mut out)?;
        Ok(out.bytes)
    }
}

fn encode_key(key: &DeliveredKey) -> Result<Zeroizing<Vec<u8>>, Error> {
    for text in [
        &key.access_key_id,
        &key.secret_access_key.as_str().to_owned(),
    ] {
        crate::error::bound(text.len(), MAX_TEXT, "delivered key")?;
    }
    let mut out = Encoder::new();
    key.access_key_id.put(&mut out)?;
    key.secret_access_key.as_str().to_owned().put(&mut out)?;
    key.principal.put(&mut out)?;
    Ok(Zeroizing::new(out.bytes))
}

fn decode_key(bytes: &[u8]) -> Result<DeliveredKey, Error> {
    let mut input = Decoder::new(bytes)?;
    let access_key_id = crate::wire::get_text(&mut input, MAX_TEXT, "delivered key")?;
    let secret = Zeroizing::new(crate::wire::get_text(
        &mut input,
        MAX_TEXT,
        "delivered key",
    )?);
    let principal = S3Principal::get(&mut input)?;
    input.finish()?;
    Ok(DeliveredKey {
        access_key_id,
        secret_access_key: SecretText::new(secret.to_string()),
        principal,
    })
}

/// Seal and sign a delivery of `key` to the member at `path`.
pub fn seal(
    author: &MemberKeys,
    member: &MemberId,
    sealing: &SealingPublicKey,
    path: &str,
    key: &DeliveredKey,
) -> Result<Vec<u8>, DeliveryError> {
    let routing = DeliveryRouting {
        member: member.clone(),
        sealing: *sealing,
        author: author.member_id(),
    };
    let mut bytes = routing.encode()?;
    let plaintext = encode_key(key)?;
    let sealed = coven_crypto::seal_access_key(&plaintext, sealing, path, &bytes)?;
    bytes.extend_from_slice(&(sealed.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&sealed);
    let mut hasher = ObjectHasher::new();
    hasher.update(&bytes);
    bytes.extend_from_slice(author.sign_object(path, &hasher.finish()).as_bytes());
    Ok(bytes)
}

/// Read a delivery's routing without opening or authenticating it.
pub fn routing(bytes: &[u8]) -> Result<DeliveryRouting, Error> {
    crate::sealed::prefix(bytes, KIND)?;
    let mut input = Decoder::new(&bytes[3..])?;
    Ok(DeliveryRouting {
        member: MemberId::get(&mut input)?,
        sealing: SealingPublicKey::get(&mut input)?,
        author: MemberId::get(&mut input)?,
    })
}

/// Verify the author's signature, then open the box for this member. The
/// caller decides whether the author may deliver this member's key.
pub fn open(member: &MemberKeys, path: &str, bytes: &[u8]) -> Result<DeliveredKey, DeliveryError> {
    let routing = routing(bytes)?;
    let prefix_length = 3 + 32 + 32 + 32;
    if routing.member != member.member_id() || routing.sealing != member.sealing_public_key() {
        return Err(DeliveryError::Crypto(CryptoError::Authentication));
    }
    let length_bytes: [u8; 4] = bytes
        .get(prefix_length..prefix_length + 4)
        .ok_or(Error::Truncated)?
        .try_into()
        .expect("four bytes");
    let length = u32::from_be_bytes(length_bytes) as usize;
    crate::error::bound(
        length,
        coven_crypto::MAX_ACCESS_KEY_BOX + 32 + coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD,
        "delivered key box",
    )?;
    let end = prefix_length + 4 + length;
    let signed = bytes.get(..end).ok_or(Error::Truncated)?;
    let signature = crate::sealed::signature(&bytes[end..])?;
    let mut hasher = ObjectHasher::new();
    hasher.update(signed);
    routing
        .author
        .verify_object(path, &hasher.finish(), &signature)?;
    let plaintext = member.open_access_key(
        path,
        &bytes[..prefix_length],
        &bytes[prefix_length + 4..end],
    )?;
    Ok(decode_key(&plaintext)?)
}

/// A delivery that cannot be made or opened.
#[derive(Debug)]
pub enum DeliveryError {
    /// The bytes break the delivery's format.
    Format(Error),
    /// Sealing, opening or the author's signature failed.
    Crypto(CryptoError),
}

impl From<Error> for DeliveryError {
    fn from(error: Error) -> Self {
        Self::Format(error)
    }
}

impl From<CryptoError> for DeliveryError {
    fn from(error: CryptoError) -> Self {
        Self::Crypto(error)
    }
}

impl std::fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Format(error) => write!(f, "delivered key format: {error:?}"),
            Self::Crypto(error) => write!(f, "delivered key: {error}"),
        }
    }
}

impl std::error::Error for DeliveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Format(_) => None,
            Self::Crypto(error) => Some(error),
        }
    }
}

#[cfg(test)]
#[path = "access_key_delivery_tests.rs"]
mod tests;
