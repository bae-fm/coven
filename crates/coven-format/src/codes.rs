//! Restore and invite codes keep every owned secret buffer zeroizing (§13).

use crate::error::{bound, require, Error, Rule};
use crate::value::name;
use crate::wire::{decode_frame, Decoder, Wire};
use crate::{FORMAT_VERSION, FRAME_PREFIX_LEN};
use coven_crypto::{InviteSecret, MemberKeys, SecretBytes};
use coven_foundation::id_source::{DeviceId, InviteId, KeyId, MemberId, StoreId};
use zeroize::Zeroizing;

const MAX_STORAGE: usize = 16 * 1024;
const MAX_CODE_BYTES: usize = 18 * 1024;
// A saved code adds its frame prefix, a name of at most 1,024 bytes, its slot
// and the code's length to the code itself.
const MAX_KEYCHAIN_BYTES: usize = MAX_CODE_BYTES + crate::FRAME_PREFIX_LEN + 4 + 1024 + 8 + 4;
const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// A person's keys and the store's identity and location (§13.1).
#[derive(Debug)]
pub struct RestoreCode {
    /// The store's id.
    pub store: StoreId,
    /// The stable member the private pair belongs to. Restore requires this
    /// id's current pair; an old code cannot rebind it (D13).
    pub member: MemberId,
    /// Crypto owns both member key pairs, their encoding and erasure.
    pub member_keys: MemberKeys,
    /// Storage location only, interpreted by storage; never credentials.
    pub storage: SecretBytes,
}
impl RestoreCode {
    /// Encode a bounded frame into a buffer erased on drop, allocated once.
    pub fn to_bytes(&self) -> Result<Zeroizing<Vec<u8>>, Error> {
        let keys = self.member_keys.to_secret_bytes();
        encode_code(
            10,
            self.store,
            &[
                self.member.0.as_bytes(),
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
        let member = Wire::get(&mut input)?;
        let keys_len = u32::get(&mut input)? as usize;
        let member_keys =
            MemberKeys::from_secret_bytes(input.take(keys_len)?).map_err(Error::Material)?;
        let storage = storage(&mut input)?;
        input.finish()?;
        Ok(Self {
            store,
            member,
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

/// A restore code as coven saves it in iCloud Keychain: the store's name as the
/// saving device had it, the slot that set that name, and the code (D13). The
/// name is a label for the person's list; the code's store id finds the store.
#[derive(Debug)]
pub struct KeychainRestoreCode {
    /// The store's name; a rename made while no device of the person had the
    /// store open leaves it as they last saw it (§13.1).
    pub name: String,
    /// The slot that set `name`: 1 for creation, otherwise the rename's slot.
    /// Only a name from a later slot replaces it.
    pub named_at: u64,
    /// The member's current restore code.
    pub code: RestoreCode,
}
impl KeychainRestoreCode {
    /// Encode a bounded frame into a buffer erased on drop, allocated once.
    pub fn to_bytes(&self) -> Result<Zeroizing<Vec<u8>>, Error> {
        Self::encode(&self.name, self.named_at, &self.code)
    }
    /// Encode the frame for these fields without owning the code.
    pub fn encode(
        label: &str,
        named_at: u64,
        code: &RestoreCode,
    ) -> Result<Zeroizing<Vec<u8>>, Error> {
        name(label)?;
        require(named_at > 0, "name slot", Rule::Required)?;
        let code = code.to_bytes()?;
        let size = FRAME_PREFIX_LEN + 4 + label.len() + 8 + 4 + code.len();
        let mut bytes = Zeroizing::new(Vec::new());
        bytes.try_reserve_exact(size)?;
        bytes.push(17);
        bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&((size - FRAME_PREFIX_LEN) as u32).to_be_bytes());
        bytes.extend_from_slice(&(label.len() as u32).to_be_bytes());
        bytes.extend_from_slice(label.as_bytes());
        bytes.extend_from_slice(&named_at.to_be_bytes());
        bytes.extend_from_slice(&(code.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&code);
        debug_assert_eq!(bytes.len(), size);
        Ok(bytes)
    }
    /// Decode a borrowed frame; every owned copy of private material erases on drop.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        bound(bytes.len(), MAX_KEYCHAIN_BYTES, "saved code bytes")?;
        let (kind, mut input) = decode_frame(bytes)?;
        require(kind == 17, "saved restore code kind", Rule::Kind)?;
        let label = crate::wire::get_name(&mut input)?;
        let named_at = u64::get(&mut input)?;
        require(named_at > 0, "name slot", Rule::Required)?;
        let len = u32::get(&mut input)? as usize;
        let code = RestoreCode::from_bytes(input.take(len)?)?;
        input.finish()?;
        Ok(Self {
            name: label,
            named_at,
            code,
        })
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
    /// The member id the admin fixed: fresh for an addition, or an existing
    /// active id for a re-binding. The signed request must repeat it (D13).
    pub member: MemberId,
    /// Crypto owns the one-time secret and its erasure.
    pub secret: InviteSecret,
    /// The store key introduced by the creation entry, even after rotations.
    pub initial_key: KeyId,
    /// The device whose claimed approval supplies binding evidence.
    pub inviting_writer: DeviceId,
    /// The inviting device's last applied slot.
    pub read_through: u64,
    /// Storage settings, including any credentials for the recipient.
    pub storage: SecretBytes,
}
impl InviteCode {
    /// Encode a bounded frame into a buffer erased on drop, allocated once.
    pub fn to_bytes(&self) -> Result<Zeroizing<Vec<u8>>, Error> {
        let secret = self.secret.to_secret_bytes();
        name(&self.name)?;
        encode_code(
            11,
            self.store,
            &[
                &(self.name.len() as u32).to_be_bytes(),
                self.name.as_bytes(),
                self.invite.0.as_bytes(),
                self.member.0.as_bytes(),
                secret.as_bytes(),
                self.initial_key.0.as_bytes(),
                &self.inviting_writer.0.to_be_bytes(),
                &self.read_through.to_be_bytes(),
            ],
            &self.storage,
        )
    }
    /// Decode a borrowed frame; every owned secret buffer erases on drop.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        bound(bytes.len() + 4, MAX_CODE_BYTES, "code bytes")?;
        let (kind, mut input) = decode_frame(bytes)?;
        require(kind == 11, "invite code kind", Rule::Kind)?;
        let store = Wire::get(&mut input)?;
        let label = crate::wire::get_name(&mut input)?;
        let invite = Wire::get(&mut input)?;
        let member = Wire::get(&mut input)?;
        let mut secret = Zeroizing::new([0; 32]);
        secret.copy_from_slice(input.take(32)?);
        let secret = InviteSecret::from_bytes(*secret);
        let initial_key = Wire::get(&mut input)?;
        let inviting_writer = Wire::get(&mut input)?;
        let read_through = u64::get(&mut input)?;
        let storage = storage(&mut input)?;
        input.finish()?;
        Ok(Self {
            store,
            name: label,
            invite,
            member,
            secret,
            initial_key,
            inviting_writer,
            read_through,
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
    bytes.try_reserve_exact(len)?;
    bytes.extend_from_slice(source);
    Ok(SecretBytes::new(std::mem::take(&mut *bytes)))
}

fn encode_code(
    kind: u8,
    store: StoreId,
    material: &[&[u8]],
    storage: &SecretBytes,
) -> Result<Zeroizing<Vec<u8>>, Error> {
    bound(storage.as_bytes().len(), MAX_STORAGE, "storage settings")?;
    let size = FRAME_PREFIX_LEN
        + 16
        + material.iter().map(|s| s.len()).sum::<usize>()
        + 4
        + storage.as_bytes().len();
    bound(size + 4, MAX_CODE_BYTES, "code bytes")?;
    let mut bytes = Zeroizing::new(Vec::new());
    bytes.try_reserve_exact(size)?;
    bytes.push(kind);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&((size - FRAME_PREFIX_LEN) as u32).to_be_bytes());
    bytes.extend_from_slice(store.0.as_bytes());
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
    text.try_reserve_exact(prefix.len() + ((bytes.len() + 4) * 8).div_ceil(5))?;
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
    bytes.try_reserve_exact(symbols.len() * 5 / 8)?;
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
