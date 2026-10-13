//! Provider observations identify replacements independently of timestamps and checksums.

use crate::{ByteRange, ObjectPath, StorageError, StorageFailure};
use std::time::{Duration, SystemTime};

/// Metadata of an object returned by a provider read, exact status or listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObject {
    /// Validated path relative to the store's location.
    pub path: ObjectPath,
    /// Complete encrypted length, including for ranged reads.
    pub size: u64,
    /// Provider creation time for immutable objects, replacement time otherwise.
    /// AWS multipart Last-Modified and OneDrive session createdDateTime report
    /// initiation, not complete publication. The adapter preserves those native
    /// values; the live publication checks expose the unmet provider guarantee.
    pub stored_at: SystemTime,
    /// Timestamp resolution. S3 uses its whole-second HEAD/GET representation
    /// for listings too; other providers retain their returned fractional digits.
    pub time_resolution: Duration,
    /// Native object id, or the complete key on S3.
    pub object_id: String,
    /// Opaque replacement identity, never a proof of byte equality. S3 immutable
    /// listings use the ETag; replaceable paths use exact status to obtain the
    /// generation or Coven's atomically stored replacement token.
    pub revision: String,
    /// Provider checksum, when returned. ETags are never inferred to be checksums.
    pub checksum: Option<ObjectChecksum>,
}

impl StoredObject {
    pub(crate) fn check(&self, path: &ObjectPath) -> Result<(), StorageError> {
        if &self.path != path
            || self.object_id.is_empty()
            || self.revision.is_empty()
            || self.time_resolution.is_zero()
        {
            return Err(StorageFailure::Protocol.with_source("invalid object metadata"));
        }
        Ok(())
    }
}

/// Encrypted bytes and the provider observation identifying those exact bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRead {
    /// Metadata describes the complete object, even when only a range was read.
    pub metadata: StoredObject,
    /// The requested complete body or byte range.
    pub bytes: Vec<u8>,
}

impl ObjectRead {
    pub(crate) fn check(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<(), StorageError> {
        self.metadata.check(path)?;
        let expected = match range {
            Some(range) => {
                if range.end() > self.metadata.size {
                    return Err(StorageFailure::InvalidRange.into());
                }
                range.len()
            }
            None => self.metadata.size,
        };
        if self.bytes.len() as u64 != expected {
            return Err(
                StorageFailure::Protocol.with_source("read length disagrees with object metadata")
            );
        }
        Ok(())
    }
}

/// The algorithm and native encoding of a provider checksum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChecksumValue {
    /// Hexadecimal MD5 of the complete content (Drive).
    Md5Hex(String),
    /// Hexadecimal SHA-1 of the complete content (OneDrive).
    Sha1Hex(String),
    /// Dropbox's hexadecimal SHA-256 of concatenated 4 MiB block hashes.
    DropboxContentHash(String),
    /// Base64 QuickXorHash (OneDrive), a provider checksum, not authentication.
    QuickXorBase64(String),
    /// Base64 SHA-256 (S3); its scope determines whether it hashes parts or content.
    Sha256Base64(String),
    /// Base64 CRC64NVME (S3).
    Crc64NvmeBase64(String),
    /// Base64 CRC32C (S3).
    Crc32cBase64(String),
    /// Base64 CRC32 (S3).
    Crc32Base64(String),
    /// Base64 SHA-1 (S3).
    Sha1Base64(String),
}

/// Full-content checksums and multipart composites have different comparison rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectChecksum {
    /// Comparable with the named algorithm run over the complete encrypted object.
    FullObject(ChecksumValue),
    /// Derived from multipart part checksums; cannot confirm a whole-object digest.
    Composite(ChecksumValue),
}

#[cfg(test)]
#[path = "object_metadata_tests.rs"]
mod tests;
