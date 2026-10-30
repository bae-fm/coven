//! Where a file is, as its record's `coordinates` cell holds it (format.md
//! D12): stored at a fixed location with its key, or only on one device.

use crate::DbError;
use coven_crypto::{FileKey, SecretText};
use coven_format::{coordinates::Coordinates, file_reference::FileReference, value::Value};
use coven_foundation::id_source::{DeviceId, FileId, IdSource};

/// A stored file's fixed location: the uploader, the stored object's id and
/// the file's key. Equality compares the whole text, so another object or
/// key is another location. Diagnostics never show the key.
#[derive(Clone, Debug)]
pub struct FileLocation {
    uploader: DeviceId,
    object: FileId,
    text: SecretText,
}

impl FileLocation {
    /// The device that uploaded the bytes, which owns deleting them.
    pub fn uploader(&self) -> DeviceId {
        self.uploader
    }

    /// The stored object's id, the last part of its path
    /// `<store>/files/<uploader>/<object>`.
    pub fn object(&self) -> FileId {
        self.object
    }

    /// A new location for bytes this device uploads: a random object id and
    /// a new key (§17.3).
    pub(crate) fn new(device: DeviceId, ids: &dyn IdSource) -> Result<Self, DbError> {
        Self::from_reference(&FileReference {
            device,
            id: FileId(ids.new_id()),
            key: FileKey::generate().map_err(DbError::FileKey)?,
        })
    }

    fn from_reference(reference: &FileReference) -> Result<Self, DbError> {
        Ok(Self {
            uploader: reference.device,
            object: reference.id,
            text: reference.encode(),
        })
    }

    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't decode is DamagedDatabase"
    )]
    pub(crate) fn decode(text: &str) -> Result<Self, DbError> {
        let reference = FileReference::decode(text).map_err(|_| DbError::DamagedDatabase)?;
        Self::from_reference(&reference)
    }

    pub(crate) fn text(&self) -> &str {
        self.text.as_str()
    }
}

impl PartialEq for FileLocation {
    fn eq(&self, other: &Self) -> bool {
        self.text.as_str() == other.text.as_str()
    }
}

/// Crate-to-crate access to a stored file's key, for coven's owners; not
/// re-exported by the app API.
pub trait FileLocationKey {
    /// The uploader, object id and key, which encrypt and decrypt the bytes.
    fn reference(&self) -> Result<FileReference, DbError>;
}

impl FileLocationKey for FileLocation {
    fn reference(&self) -> Result<FileReference, DbError> {
        Ok(FileReference::decode(self.text.as_str())?)
    }
}

/// A file's coordinates as coven keeps them.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Place {
    /// Stored at its location.
    Cloud(FileLocation),
    /// Only on one device, which may be this one.
    DeviceOnly(DeviceId),
}

impl Place {
    /// Read a `coordinates` cell. Anything but one canonical form is damage.
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't decode is DamagedDatabase"
    )]
    pub(crate) fn decode(value: &Value) -> Result<Self, DbError> {
        let Value::Text(text) = value else {
            return Err(DbError::DamagedDatabase);
        };
        match Coordinates::decode(text).map_err(|_| DbError::DamagedDatabase)? {
            Coordinates::Cloud(reference) => {
                Ok(Self::Cloud(FileLocation::from_reference(&reference)?))
            }
            Coordinates::DeviceOnly(device) => Ok(Self::DeviceOnly(device)),
        }
    }

    /// The `coordinates` cell's value.
    pub(crate) fn value(&self) -> Value {
        Value::Text(match self {
            Self::Cloud(location) => location.text().to_owned(),
            Self::DeviceOnly(device) => Coordinates::DeviceOnly(*device)
                .encode()
                .as_str()
                .to_owned(),
        })
    }
}

#[cfg(test)]
#[path = "file_location_tests.rs"]
mod tests;
