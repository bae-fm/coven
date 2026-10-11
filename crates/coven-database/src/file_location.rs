//! The where-column's canonical text (Appendix D12), with uploaded keys redacted.

use crate::DbError;
use coven_crypto::SecretText;
use coven_format::value::Value;
use coven_foundation::id_source::DeviceId;

/// Whether a row's file is waiting to upload or already stored (§16.1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileLocation {
    /// Stored encrypted under the file's own id and key.
    Uploaded,
    /// Waiting to upload on the device that attached it.
    OnDevice(DeviceId),
}

/// References retain the entire uploaded identity, so changing its id or key
/// invalidates an earlier reference. Diagnostics never expose the key.
#[derive(Clone, Debug)]
pub(crate) enum StoredLocation {
    Uploaded(SecretText),
    OnDevice(DeviceId),
}

impl StoredLocation {
    pub(crate) fn decode(value: &Value) -> Result<Self, DbError> {
        let Value::Text(text) = value else {
            return Err(DbError::DamagedDatabase);
        };
        if text.starts_with("file ") {
            coven_format::file_reference::FileReference::decode(text)
                .map_err(|_| DbError::DamagedDatabase)?;
            Ok(Self::Uploaded(SecretText::new(text.clone())))
        } else {
            let device: u64 = text.parse().map_err(|_| DbError::DamagedDatabase)?;
            if device.to_string() != *text {
                return Err(DbError::DamagedDatabase);
            }
            Ok(Self::OnDevice(DeviceId(device)))
        }
    }

    pub(crate) fn value(&self) -> Value {
        Value::Text(match self {
            Self::Uploaded(text) => text.as_str().to_owned(),
            Self::OnDevice(device) => device.0.to_string(),
        })
    }
    pub(crate) fn public(&self) -> FileLocation {
        match self {
            Self::Uploaded(_) => FileLocation::Uploaded,
            Self::OnDevice(device) => FileLocation::OnDevice(*device),
        }
    }
}

impl PartialEq for StoredLocation {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Uploaded(a), Self::Uploaded(b)) => a.as_str() == b.as_str(),
            (Self::OnDevice(a), Self::OnDevice(b)) => a == b,
            _ => false,
        }
    }
}
