//! The where-column's canonical text (Appendix D12), with uploaded keys redacted.

use crate::DbError;
use coven_crypto::SecretText;
use coven_format::value::Value;
use coven_foundation::id_source::DeviceId;

/// Where a row's file is kept (§16.1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileLocation {
    /// Stored encrypted under the file's own id and key.
    Uploaded,
    /// Only on the named device.
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
        if let Some(uploaded) = text.strip_prefix("uploaded ") {
            let (id, key) = uploaded.split_once(' ').ok_or(DbError::DamagedDatabase)?;
            let id_value = uuid::Uuid::parse_str(id).map_err(|_| DbError::DamagedDatabase)?;
            if id_value.to_string() != id
                || key.len() != 64
                || !key
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(DbError::DamagedDatabase);
            }
            Ok(Self::Uploaded(SecretText::new(text.clone())))
        } else {
            let device: u64 = text.parse().map_err(|_| DbError::DamagedDatabase)?;
            if device.to_string() != *text {
                return Err(DbError::DamagedDatabase);
            }
            Ok(Self::OnDevice(DeviceId(device)))
        }
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
