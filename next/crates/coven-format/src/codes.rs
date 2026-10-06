//! Restore and invite codes keep every owned secret buffer zeroizing (§12).

use crate::error::{bound, require, Error, Rule};
use crate::value::name;
use crate::wire::{decode_frame, Decoder, Wire};
use crate::{FORMAT_VERSION, FRAME_PREFIX_LEN};
use coven_crypto::{InviteSecret, MemberKeys, SecretBytes};
use coven_foundation::id_source::{InviteId, StoreId};
use zeroize::Zeroizing;

const MAX_STORAGE: usize = 16 * 1024;
const MAX_CODE_BYTES: usize = 18 * 1024;
const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// A person's keys and the store's identity and credentials (§12.1).
#[derive(Debug)]
pub struct RestoreCode {
    /// The store's id.
    pub store: StoreId,
    /// The store's display name.
    pub name: String,
    /// Crypto owns both member key pairs, their encoding and erasure.
    pub member_keys: MemberKeys,
    /// Storage settings including credentials, interpreted by storage.
    pub storage: SecretBytes,
}
impl RestoreCode {
    /// Encode a bounded frame into a buffer erased on drop, allocated once.
    pub fn to_bytes(&self) -> Result<Zeroizing<Vec<u8>>, Error> {
        let keys = self.member_keys.to_secret_bytes();
        encode_code(
            10,
            self.store,
            &self.name,
            &[
                &(keys.as_bytes().len() as u32).to_be_bytes(),
                keys.as_bytes(),
            ],
            &self.storage,
        )
    }
    /// Decode a borrowed frame; every owned copy of private material erases on drop.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        bound(bytes.len() + 4, MAX_CODE_BYTES, "code bytes")?;
        let (kind, mut input) = decode_frame(bytes)?;
        require(kind == 10, "restore code kind", Rule::Kind)?;
        let store = Wire::get(&mut input)?;
        let label = String::get(&mut input)?;
        name(&label)?;
        let keys_len = u32::get(&mut input)? as usize;
        let member_keys =
            MemberKeys::from_secret_bytes(input.take(keys_len)?).map_err(Error::Material)?;
        let storage = storage(&mut input)?;
        input.finish()?;
        Ok(Self {
            store,
            name: label,
            member_keys,
            storage,
        })
    }
    /// Canonical uppercase base32, with CRC-32C, erased on drop.
    pub fn to_text(&self) -> Result<Zeroizing<String>, Error> {
        text_encode("CVR1-", &self.to_bytes()?)
    }
    /// Decode borrowed text without retaining any unerased private copies.
    pub fn from_text(text: &str) -> Result<Self, Error> {
        Self::from_bytes(&text_decode("CVR1-", text)?)
    }
}

/// A one-time invitation; the secret permits a join request, not store access.
#[derive(Debug)]
pub struct InviteCode {
    /// The store's id.
    pub store: StoreId,
    /// The store's display name.
    pub name: String,
    /// The invite named by the join request.
    pub invite: InviteId,
    /// Crypto owns the one-time secret and its erasure.
    pub secret: InviteSecret,
    /// Storage settings, including any credentials for the recipient.
    pub storage: SecretBytes,
}
impl InviteCode {
    /// Encode a bounded frame into a buffer erased on drop, allocated once.
    pub fn to_bytes(&self) -> Result<Zeroizing<Vec<u8>>, Error> {
        let secret = self.secret.to_secret_bytes();
        encode_code(
            11,
            self.store,
            &self.name,
            &[self.invite.0.as_bytes(), secret.as_bytes()],
            &self.storage,
        )
    }
    /// Decode a borrowed frame; every owned secret buffer erases on drop.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        bound(bytes.len() + 4, MAX_CODE_BYTES, "code bytes")?;
        let (kind, mut input) = decode_frame(bytes)?;
        require(kind == 11, "invite code kind", Rule::Kind)?;
        let store = Wire::get(&mut input)?;
        let label = String::get(&mut input)?;
        name(&label)?;
        let invite = Wire::get(&mut input)?;
        let mut secret = Zeroizing::new([0; 32]);
        secret.copy_from_slice(input.take(32)?);
        let secret = InviteSecret::from_bytes(*secret);
        let storage = storage(&mut input)?;
        input.finish()?;
        Ok(Self {
            store,
            name: label,
            invite,
            secret,
            storage,
        })
    }
    /// Canonical uppercase base32, with CRC-32C, erased on drop.
    pub fn to_text(&self) -> Result<Zeroizing<String>, Error> {
        text_encode("CVI1-", &self.to_bytes()?)
    }
    /// Decode borrowed text without retaining any unerased private copies.
    pub fn from_text(text: &str) -> Result<Self, Error> {
        Self::from_bytes(&text_decode("CVI1-", text)?)
    }
}

fn storage(input: &mut Decoder<'_>) -> Result<SecretBytes, Error> {
    let len = u32::get(input)? as usize;
    bound(len, MAX_STORAGE, "storage settings")?;
    let source = input.take(len)?;
    let mut bytes = Zeroizing::new(Vec::new());
    bytes
        .try_reserve_exact(len)
        .map_err(|_| Error::Allocation)?;
    bytes.extend_from_slice(source);
    Ok(SecretBytes::new(std::mem::take(&mut *bytes)))
}

fn encode_code(
    kind: u8,
    store: StoreId,
    label: &str,
    material: &[&[u8]],
    storage: &SecretBytes,
) -> Result<Zeroizing<Vec<u8>>, Error> {
    name(label)?;
    bound(storage.as_bytes().len(), MAX_STORAGE, "storage settings")?;
    let size = FRAME_PREFIX_LEN
        + 16
        + 4
        + label.len()
        + material.iter().map(|s| s.len()).sum::<usize>()
        + 4
        + storage.as_bytes().len();
    bound(size + 4, MAX_CODE_BYTES, "code bytes")?;
    let mut bytes = Zeroizing::new(Vec::new());
    bytes
        .try_reserve_exact(size)
        .map_err(|_| Error::Allocation)?;
    bytes.push(kind);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&((size - FRAME_PREFIX_LEN) as u32).to_be_bytes());
    bytes.extend_from_slice(store.0.as_bytes());
    bytes.extend_from_slice(&(label.len() as u32).to_be_bytes());
    bytes.extend_from_slice(label.as_bytes());
    for part in material {
        bytes.extend_from_slice(part);
    }
    bytes.extend_from_slice(&(storage.as_bytes().len() as u32).to_be_bytes());
    bytes.extend_from_slice(storage.as_bytes());
    debug_assert_eq!(bytes.len(), size);
    Ok(bytes)
}

// CRC-32C: reflected Castagnoli polynomial, init/final XOR all ones. This is
// accidental-error detection, not authentication; sealing/signing belong to crypto.
fn checksum(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

fn text_encode(prefix: &str, bytes: &[u8]) -> Result<Zeroizing<String>, Error> {
    bound(bytes.len() + 4, MAX_CODE_BYTES, "code bytes")?;
    let crc = Zeroizing::new(checksum(bytes).to_be_bytes());
    let mut text = Zeroizing::new(String::new());
    text.try_reserve_exact(prefix.len() + ((bytes.len() + 4) * 8).div_ceil(5))
        .map_err(|_| Error::Allocation)?;
    text.push_str(prefix);
    let mut pending = Zeroizing::new(0u16);
    let mut bits = 0;
    for byte in bytes.iter().chain(crc.iter()) {
        *pending = (*pending << 8) | u16::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            text.push(ALPHABET[((*pending >> bits) & 31) as usize] as char);
        }
    }
    if bits > 0 {
        text.push(ALPHABET[((*pending << (5 - bits)) & 31) as usize] as char);
    }
    Ok(text)
}

fn text_decode(prefix: &str, text: &str) -> Result<Zeroizing<Vec<u8>>, Error> {
    bound(
        text.len(),
        prefix.len() + (MAX_CODE_BYTES * 8).div_ceil(5),
        "code text",
    )?;
    require(text.starts_with(prefix), "code prefix", Rule::Kind)?;
    let symbols = &text.as_bytes()[prefix.len()..];
    require(
        matches!(symbols.len() % 8, 0 | 2 | 4 | 5 | 7),
        "base32 length",
        Rule::CodePadding,
    )?;
    let mut bytes = Zeroizing::new(Vec::new());
    bytes
        .try_reserve_exact(symbols.len() * 5 / 8)
        .map_err(|_| Error::Allocation)?;
    let mut pending = Zeroizing::new(0u16);
    let mut bits = 0;
    for (offset, symbol) in symbols.iter().enumerate() {
        let value = ALPHABET
            .iter()
            .position(|c| c == symbol)
            .ok_or(Error::CodeCharacter {
                offset: offset + prefix.len(),
            })?;
        *pending = (*pending << 5) | value as u16;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            bytes.push((*pending >> bits) as u8);
        }
    }
    require(
        *pending & ((1 << bits) - 1) == 0,
        "base32 padding bits",
        Rule::CodePadding,
    )?;
    let payload_len = bytes.len().checked_sub(4).ok_or(Error::Truncated)?;
    let mut expected = [0; 4];
    expected.copy_from_slice(&bytes[payload_len..]);
    if checksum(&bytes[..payload_len]) != u32::from_be_bytes(expected) {
        return Err(Error::Checksum);
    }
    bytes.truncate(payload_len);
    Ok(bytes)
}

#[cfg(test)]
#[path = "codes_tests.rs"]
mod tests;
