//! The cleartext file header, indexed ciphertext chunks and authenticated ranges.

use crate::chunks::CHUNK_SIZE;
use crate::error::{require, Error, Rule};
use coven_crypto::{CryptoError, FileKey, FILE_CHUNK_TAG_LEN};

/// Bytes before the first file chunk: kind, version and file size.
pub const FILE_HEADER_LEN: usize = 11;

/// Validated kind-38 cleartext header. It is authenticated by every file chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileHeader {
    size: u64,
}

impl FileHeader {
    /// Describe a file of this plaintext size, in fixed 64-KiB chunks.
    pub fn new(size: u64) -> Self {
        Self { size }
    }

    /// Decode exactly the 11-byte header, without reading any ciphertext.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        crate::sealed::prefix(bytes, 38)?;
        if bytes.len() < FILE_HEADER_LEN {
            return Err(Error::Truncated);
        }
        if bytes.len() > FILE_HEADER_LEN {
            return Err(Error::TrailingBytes);
        }
        Ok(Self::new(u64::from_be_bytes(bytes[3..11].try_into().expect("eight bytes"))))
    }

    /// Encode kind, version and size, all integers big-endian.
    pub fn encode(&self) -> [u8; FILE_HEADER_LEN] {
        let mut bytes = [0; FILE_HEADER_LEN];
        bytes[0] = 38;
        bytes[1..3].copy_from_slice(&crate::FORMAT_VERSION.to_be_bytes());
        bytes[3..].copy_from_slice(&self.size.to_be_bytes());
        bytes
    }

    /// The file's complete plaintext size.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// How many chunks follow this header; an empty file has none.
    pub fn chunk_count(&self) -> u64 {
        self.size.div_ceil(CHUNK_SIZE as u64)
    }

    /// Full stored size, refusing an offset that cannot be represented in u64.
    pub fn encrypted_size(&self) -> Result<u64, Error> {
        self.chunk_count()
            .checked_mul(FILE_CHUNK_TAG_LEN as u64)
            .and_then(|tags| tags.checked_add(self.size))
            .and_then(|body| body.checked_add(FILE_HEADER_LEN as u64))
            .ok_or_else(offset_error)
    }

    /// The exact storage offset and plaintext length of a particular chunk.
    pub fn chunk(&self, index: u64) -> Result<FileChunk, Error> {
        require(index < self.chunk_count(), "file chunk index", Rule::Chunk)?;
        let offset = index
            .checked_mul((CHUNK_SIZE + FILE_CHUNK_TAG_LEN) as u64)
            .and_then(|n| n.checked_add(FILE_HEADER_LEN as u64))
            .ok_or_else(offset_error)?;
        let plaintext_length =
            (self.size - index * CHUNK_SIZE as u64).min(CHUNK_SIZE as u64) as usize;
        offset
            .checked_add((plaintext_length + FILE_CHUNK_TAG_LEN) as u64)
            .ok_or_else(offset_error)?;
        Ok(FileChunk { index, offset, plaintext_length })
    }

    /// Validate a chunk's plaintext length, then seal it under the file's key.
    pub fn seal_chunk(
        &self,
        key: &FileKey,
        path: &str,
        index: u64,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, FileError> {
        let chunk = self.chunk(index)?;
        require(plaintext.len() == chunk.plaintext_length, "file chunk length", Rule::Chunk)?;
        Ok(key.seal_chunk(path, &self.encode(), index, plaintext))
    }

    /// Validate a chunk's stored length before authenticating or allocating.
    pub fn open_chunk(
        &self,
        key: &FileKey,
        path: &str,
        index: u64,
        bytes: &[u8],
    ) -> Result<Vec<u8>, FileError> {
        let chunk = self.chunk(index)?;
        exact_length(bytes.len() as u64, (chunk.plaintext_length + FILE_CHUNK_TAG_LEN) as u64)?;
        Ok(key.open_chunk(path, &self.encode(), index, bytes)?)
    }
}

/// One file chunk's coordinates, computed from its header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileChunk {
    /// Zero-based index; also the chunk's nonce.
    pub index: u64,
    /// Byte offset in the stored object, including its 11-byte header.
    pub offset: u64,
    /// Exact plaintext length; the stored length adds a 16-byte tag.
    pub plaintext_length: usize,
}

fn exact_length(actual: u64, expected: u64) -> Result<(), Error> {
    match actual.cmp(&expected) {
        std::cmp::Ordering::Less => Err(Error::Truncated),
        std::cmp::Ordering::Greater => Err(Error::TrailingBytes),
        std::cmp::Ordering::Equal => Ok(()),
    }
}
fn offset_error() -> Error {
    Error::Invalid { field: "file offset", rule: Rule::Chunk }
}

/// A malformed file layout or a chunk that failed authentication.
#[derive(Debug)]
pub enum FileError {
    /// The range, header or chunk length is invalid.
    Format(Error),
    /// The key, path, header, index or ciphertext did not authenticate.
    Crypto(CryptoError),
}
impl From<Error> for FileError {
    fn from(error: Error) -> Self {
        Self::Format(error)
    }
}
impl From<CryptoError> for FileError {
    fn from(error: CryptoError) -> Self {
        Self::Crypto(error)
    }
}
impl std::fmt::Display for FileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Format(e) => e.fmt(f),
            Self::Crypto(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Format(e) => Some(e),
            Self::Crypto(e) => Some(e),
        }
    }
}

#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
