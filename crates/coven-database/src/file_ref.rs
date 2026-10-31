//! A file as this device has its record: its id, size, namespace, audience
//! and place (§17.2, E8). A file never changes its bytes, so a reference
//! taken earlier still reads what it named.

use crate::{
    file_location::{FileLocation, Place},
    file_table::Record,
    sqlite::DatabaseConnection,
    DbError,
};
use coven_crypto::ContentHash;
use coven_foundation::id_source::{DeviceId, FileId};
use coven_merge::Audience;

#[path = "local_file.rs"]
mod local_file;
pub(crate) use local_file::{open as open_local, open_source, queued_source, LocalSource};
pub use local_file::{LocalFileError, LocalFileStream};

/// One file as its record stood when it was read.
#[derive(Clone, Debug, PartialEq)]
pub struct FileRef {
    id: FileId,
    audience: Audience,
    namespace: String,
    size: u64,
    place: Stored,
}

#[derive(Clone, Debug, PartialEq)]
enum Stored {
    Cloud {
        location: FileLocation,
        hash: ContentHash,
    },
    DeviceOnly(DeviceId),
}

/// Where a file is (§17.2): in storage, or only on one device.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FilePlace<'a> {
    /// Stored at its location, with its content hash.
    Cloud {
        /// The stored object and its key.
        location: &'a FileLocation,
        /// The device that uploaded it.
        uploader: DeviceId,
        /// The SHA-256 of its bytes, which every download checks.
        content_hash: &'a ContentHash,
    },
    /// Only on one device, which may be this one: while a file uploads, it
    /// is on its uploader only.
    DeviceOnly {
        /// The device that has it.
        device: DeviceId,
    },
}

impl FileRef {
    /// The file's id, which rows name.
    pub fn id(&self) -> FileId {
        self.id
    }
    /// Its size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Its namespace, which says its kind and whose cache budget it counts
    /// against.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    /// Its audience: only that audience's rows reference it, and only its
    /// members read it.
    pub fn audience(&self) -> Audience {
        self.audience.clone()
    }
    /// Where it is.
    pub fn place(&self) -> FilePlace<'_> {
        match &self.place {
            Stored::Cloud { location, hash } => FilePlace::Cloud {
                location,
                uploader: location.uploader(),
                content_hash: hash,
            },
            Stored::DeviceOnly(device) => FilePlace::DeviceOnly { device: *device },
        }
    }

    pub(crate) fn of(record: Record) -> Result<Self, DbError> {
        let place = match (record.place, record.hash) {
            (Place::Cloud(location), Some(hash)) => Stored::Cloud { location, hash },
            (Place::DeviceOnly(device), None) => Stored::DeviceOnly(device),
            _ => return Err(DbError::DamagedDatabase),
        };
        Ok(Self {
            id: record.id,
            audience: record.audience,
            namespace: record.namespace,
            size: record.size,
            place,
        })
    }
}

/// The file this device has a record of under `file`.
pub(crate) fn read(db: &DatabaseConnection, file: FileId) -> Result<FileRef, DbError> {
    FileRef::of(Record::read(db, file)?.ok_or(DbError::FileAbsent { file })?)
}

#[cfg(test)]
#[path = "file_ref_tests.rs"]
mod tests;
