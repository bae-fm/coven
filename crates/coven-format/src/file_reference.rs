//! The fixed file identity carried by a synced file row (format.md D12).

use crate::{error::Rule, Error};
use coven_crypto::{FileKey, SecretText};
use coven_foundation::id_source::{DeviceId, ObjectId};
use zeroize::Zeroizing;

/// A file's fixed uploader, random storage name and decryption key.
/// This identity carries neither upload status nor a device-local path.
#[derive(Debug)]
pub struct FileReference {
    /// Device responsible for uploading the object.
    pub device: DeviceId,
    /// The stored object's random id, its name in storage (§17.5).
    pub object: ObjectId,
    /// Key carried only inside encrypted rows and writes.
    pub key: FileKey,
}

#[cfg(test)]
#[path = "file_reference_tests.rs"]
mod tests;

impl FileReference {
    /// Decode canonical decimal device, UUID and lowercase hexadecimal key.
    #[expect(clippy::map_err_ignore, reason = "a malformed reference is invalid as a whole")]
    pub fn decode(text: &str) -> Result<Self, Error> {
        let invalid = || Error::Invalid { field: "file reference", rule: Rule::KeyEncoding };
        let rest = text.strip_prefix("file ").ok_or_else(invalid)?;
        let (device, rest) = rest.split_once(' ').ok_or_else(invalid)?;
        let (object, key) = rest.split_once(' ').ok_or_else(invalid)?;
        let number = crate::path::canonical_decimal(device).ok_or_else(invalid)?;
        let uuid = coven_foundation::id_source::canonical_uuid(object).map_err(|_| invalid())?;
        let mut bytes = Zeroizing::new([0; 32]);
        hex::decode_to_slice(key, bytes.as_mut()).map_err(|_| invalid())?;
        if key.bytes().any(|b| b.is_ascii_uppercase()) {
            return Err(invalid());
        }
        Ok(Self {
            device: DeviceId(number),
            object: ObjectId(uuid),
            key: FileKey::from_bytes(*bytes),
        })
    }

    /// Encode into secret text so diagnostics cannot reveal the file key.
    pub fn encode(&self) -> SecretText {
        let mut text = format!("file {} {} ", self.device.0, self.object);
        let mut key = Zeroizing::new([0; 64]);
        hex::encode_to_slice(self.key.to_secret_bytes().as_bytes(), key.as_mut())
            .expect("64 hex digits for a 32-byte key");
        text.push_str(std::str::from_utf8(key.as_ref()).expect("hex is ASCII"));
        SecretText::new(text)
    }
}
