//! The uploaded location carried by a synced file row (Appendix D12).

use crate::{error::Rule, Error};
use coven_crypto::{FileKey, SecretText};
use coven_foundation::id_source::{DeviceId, FileId};
use zeroize::Zeroizing;

/// An uploaded object's owner, random name and decryption key.
/// The owner remains available after the last row referring to the file goes.
#[derive(Debug)]
pub struct UploadedFileReference {
    /// Device whose provider account published the object.
    pub device: DeviceId,
    /// Random object name.
    pub id: FileId,
    /// Key carried only inside encrypted rows and writes.
    pub key: FileKey,
}

#[cfg(test)]
#[path = "file_reference_tests.rs"]
mod tests;

impl UploadedFileReference {
    /// Decode canonical decimal device, UUID and lowercase hexadecimal key.
    pub fn decode(text: &str) -> Result<Self, Error> {
        let invalid = || Error::Invalid {
            field: "uploaded file reference",
            rule: Rule::KeyEncoding,
        };
        let parts: Vec<_> = text.split(' ').collect();
        let ["uploaded", device, id, key] = parts.as_slice() else {
            return Err(invalid());
        };
        let number: u64 = device.parse().map_err(|_| invalid())?;
        let uuid: uuid::Uuid = id.parse().map_err(|_| invalid())?;
        if number.to_string() != *device || uuid.to_string() != *id || key.len() != 64 {
            return Err(invalid());
        }
        let mut bytes = [0; 32];
        for (out, pair) in bytes.iter_mut().zip(key.as_bytes().chunks_exact(2)) {
            let digit = |b| match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'a'..=b'f' => Ok(b - b'a' + 10),
                _ => Err(invalid()),
            };
            *out = digit(pair[0])? * 16 + digit(pair[1])?;
        }
        Ok(Self {
            device: DeviceId(number),
            id: FileId(uuid),
            key: FileKey::from_bytes(bytes),
        })
    }

    /// Encode into secret text so diagnostics cannot reveal the file key.
    pub fn encode(&self) -> SecretText {
        let mut text = format!("uploaded {} {} ", self.device.0, self.id);
        let mut key = Zeroizing::new([0; 64]);
        hex::encode_to_slice(self.key.to_secret_bytes().as_bytes(), key.as_mut())
            .expect("64 hex digits for a 32-byte key");
        text.push_str(std::str::from_utf8(key.as_ref()).expect("hex is ASCII"));
        SecretText::new(text)
    }
}
