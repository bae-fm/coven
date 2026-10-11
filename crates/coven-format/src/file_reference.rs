//! The fixed file identity carried by a synced file row (Appendix D12).

use crate::{error::Rule, Error};
use coven_crypto::{FileKey, SecretText};
use coven_foundation::id_source::{DeviceId, FileId};
use zeroize::Zeroizing;

/// A file's fixed uploader, random storage name and decryption key.
/// This identity carries neither upload status nor a device-local path.
#[derive(Debug)]
pub struct FileReference {
    /// Device responsible for uploading the object.
    pub device: DeviceId,
    /// Random object name.
    pub id: FileId,
    /// Key carried only inside encrypted rows and writes.
    pub key: FileKey,
}

#[cfg(test)]
#[path = "file_reference_tests.rs"]
mod tests;

impl FileReference {
    /// Decode canonical decimal device, UUID and lowercase hexadecimal key.
    pub fn decode(text: &str) -> Result<Self, Error> {
        let invalid = || Error::Invalid {
            field: "file reference",
            rule: Rule::KeyEncoding,
        };
        let rest = text.strip_prefix("file ").ok_or_else(invalid)?;
        let (device, rest) = rest.split_once(' ').ok_or_else(invalid)?;
        let (id, key) = rest.split_once(' ').ok_or_else(invalid)?;
        let number: u64 = device.parse().map_err(|_| invalid())?;
        let uuid: uuid::Uuid = id.parse().map_err(|_| invalid())?;
        if number.to_string() != device || uuid.to_string() != id {
            return Err(invalid());
        }
        let mut bytes = Zeroizing::new([0; 32]);
        hex::decode_to_slice(key, bytes.as_mut()).map_err(|_| invalid())?;
        if key.bytes().any(|b| b.is_ascii_uppercase()) {
            return Err(invalid());
        }
        Ok(Self {
            device: DeviceId(number),
            id: FileId(uuid),
            key: FileKey::from_bytes(*bytes),
        })
    }

    /// Encode into secret text so diagnostics cannot reveal the file key.
    pub fn encode(&self) -> SecretText {
        let mut text = format!("file {} {} ", self.device.0, self.id);
        let mut key = Zeroizing::new([0; 64]);
        hex::encode_to_slice(self.key.to_secret_bytes().as_bytes(), key.as_mut())
            .expect("64 hex digits for a 32-byte key");
        text.push_str(std::str::from_utf8(key.as_ref()).expect("hex is ASCII"));
        SecretText::new(text)
    }
}
