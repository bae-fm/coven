//! Provider observations identify replacements independently of timestamps and checksums.

use crate::{ByteRange, ObjectPath, StorageError, StorageFailure};
use coven_format::value::StorageTime;
use std::time::Duration;

/// Metadata of an object returned by a provider read, exact status or listing.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredObject {
    /// Validated path relative to the store's location.
    pub path: ObjectPath,
    /// Complete encrypted length, including for ranged reads.
    pub size: u64,
    /// Provider publication time for immutable objects, replacement time otherwise.
    /// OneDrive uses the copy's lastModifiedDateTime. AWS multipart Last-Modified
    /// still reports initiation; its live check exposes that unmet guarantee.
    pub stored_at: StorageTime,
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
    pub(crate) fn read_length(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<u64, StorageError> {
        self.check(path)?;
        match range {
            Some(range) if range.end() > self.size => Err(StorageFailure::InvalidRange.into()),
            Some(range) => Ok(range.len()),
            None => Ok(self.size),
        }
    }

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
        let expected = self.metadata.read_length(path, range)?;
        if self.bytes.len() as u64 != expected {
            return Err(
                StorageFailure::Protocol.with_source("read length disagrees with object metadata")
            );
        }
        Ok(())
    }
}

/// The algorithm and native encoding of a provider checksum.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ObjectChecksum {
    /// Comparable with the named algorithm run over the complete encrypted object.
    FullObject(ChecksumValue),
    /// Derived from multipart part checksums; cannot confirm a whole-object digest.
    Composite(ChecksumValue),
}

impl ObjectChecksum {
    /// Whether this checksum confirms that the complete stored object equals
    /// the `size` bytes `read` supplies, computed with the provider's own
    /// algorithm. `None` when it cannot: a multipart composite covers parts,
    /// not content, and coven does not compute every provider algorithm.
    /// Then the caller reads the stored bytes back instead (§15).
    pub fn confirms<E>(
        &self,
        size: u64,
        mut read: impl FnMut(u64, usize) -> Result<Vec<u8>, E>,
    ) -> Result<Option<bool>, E> {
        const BLOCK: u64 = 4 * 1024 * 1024;
        let ObjectChecksum::FullObject(value) = self else {
            return Ok(None);
        };
        match value {
            ChecksumValue::Sha256Base64(expected) => {
                let mut hasher = coven_crypto::ContentHasher::new();
                let mut offset = 0;
                while offset < size {
                    let length = (size - offset).min(BLOCK) as usize;
                    hasher.update(&read(offset, length)?);
                    offset += length as u64;
                }
                use base64::Engine;
                let local =
                    base64::engine::general_purpose::STANDARD.encode(hasher.finish().as_bytes());
                Ok(Some(local == *expected))
            }
            // SHA-256 of the concatenated SHA-256 of each 4 MiB block.
            ChecksumValue::DropboxContentHash(expected) => {
                let mut blocks = coven_crypto::ContentHasher::new();
                let mut offset = 0;
                while offset < size {
                    let length = (size - offset).min(BLOCK) as usize;
                    let mut block = coven_crypto::ContentHasher::new();
                    block.update(&read(offset, length)?);
                    blocks.update(block.finish().as_bytes());
                    offset += length as u64;
                }
                Ok(Some(
                    hex::encode(blocks.finish().as_bytes()) == expected.to_ascii_lowercase(),
                ))
            }
            ChecksumValue::Md5Hex(_)
            | ChecksumValue::Sha1Hex(_)
            | ChecksumValue::QuickXorBase64(_)
            | ChecksumValue::Crc64NvmeBase64(_)
            | ChecksumValue::Crc32cBase64(_)
            | ChecksumValue::Crc32Base64(_)
            | ChecksumValue::Sha1Base64(_) => Ok(None),
        }
    }
}

#[cfg(test)]
#[path = "object_metadata_tests.rs"]
mod tests;
