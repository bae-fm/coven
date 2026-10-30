//! The storage part of a restore code (format.md D13).

use crate::{CloudProvider, S3Credentials, StorageConfig, StorageError, StorageFailure};
use coven_crypto::SecretBytes;
use coven_format::code_fields::{pack, FieldReader};

/// What a restore code tells a new device about storage: the store's
/// location and, on S3, the member's key when the person's own device sends
/// the one it holds (§13.1). Never OAuth tokens or a share token.
#[derive(Clone)]
pub struct RestoreStorage {
    /// The store's location.
    pub location: StorageConfig,
    /// The member's S3 key; only an S3 location carries one.
    pub s3_key: Option<S3Credentials>,
}

impl RestoreStorage {
    /// The location with no key: what the code a person writes down, and the
    /// copy saved for the person's next devices, carry (§13.1).
    pub fn location_only(location: StorageConfig) -> Self {
        Self {
            location,
            s3_key: None,
        }
    }

    /// Pack the location and, on S3, whether a key follows and the key
    /// (format.md D13), in zeroizing storage.
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        self.location.validate()?;
        let s3 = self.location.provider() == CloudProvider::S3;
        match &self.s3_key {
            Some(_) if !s3 => {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("an S3 key restores only an S3 location"))
            }
            Some(key) => key.validate()?,
            None => {}
        }
        Ok(pack(|out| {
            self.location.pack(out)?;
            if s3 {
                out.option(self.s3_key.as_ref(), |out, key| key.pack(out))?;
            }
            Ok(())
        })?)
    }

    /// Decode a packed payload, checking the location and any key.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = FieldReader::new(bytes)?;
        let location = StorageConfig::unpack(&mut input)?;
        let s3_key = match location.provider() {
            CloudProvider::S3 => input.option(S3Credentials::unpack)?,
            _ => None,
        };
        input.finish()?;
        Ok(Self { location, s3_key })
    }
}

#[cfg(test)]
#[path = "restore_storage_tests.rs"]
mod tests;
