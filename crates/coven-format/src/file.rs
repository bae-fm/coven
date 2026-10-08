//! The cleartext file header, indexed ciphertext chunks and authenticated ranges.

use std::ops::Range;

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
        Ok(Self::new(u64::from_be_bytes(
            bytes[3..11].try_into().expect("eight bytes"),
        )))
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
        Ok(FileChunk {
            index,
            offset,
            plaintext_length,
        })
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
        require(
            plaintext.len() == chunk.plaintext_length,
            "file chunk length",
            Rule::Chunk,
        )?;
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
        exact_length(
            bytes.len() as u64,
            (chunk.plaintext_length + FILE_CHUNK_TAG_LEN) as u64,
        )?;
        Ok(key.open_chunk(path, &self.encode(), index, bytes)?)
    }

    /// Select the chunks covering a half-open plaintext range. No I/O occurs.
    pub fn range(&self, range: Range<u64>) -> Result<FileRange, Error> {
        require(
            range.start <= range.end && range.end <= self.size,
            "file range",
            Rule::Chunk,
        )?;
        let result = FileRange {
            header: *self,
            range,
        };
        result.encrypted_range()?;
        Ok(result)
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

/// A requested plaintext range and the ciphertext needed to open it.
#[derive(Clone, Debug)]
pub struct FileRange {
    header: FileHeader,
    range: Range<u64>,
}
impl FileRange {
    /// Half-open storage range to fetch. An empty request fetches no bytes.
    pub fn encrypted_range(&self) -> Result<Range<u64>, Error> {
        if self.range.is_empty() {
            return Ok(FILE_HEADER_LEN as u64..FILE_HEADER_LEN as u64);
        }
        let first = self.header.chunk(self.range.start / CHUNK_SIZE as u64)?;
        let last = self
            .header
            .chunk((self.range.end - 1) / CHUNK_SIZE as u64)?;
        Ok(first.offset..last.offset + (last.plaintext_length + FILE_CHUNK_TAG_LEN) as u64)
    }

    /// Open exactly the fetched ciphertext range and return the requested bytes.
    /// Chunks outside this range are neither supplied nor opened. Lengths are
    /// checked against the supplied bytes before any plaintext is allocated.
    pub fn open(&self, key: &FileKey, path: &str, bytes: &[u8]) -> Result<Vec<u8>, FileError> {
        let stored = self.encrypted_range()?;
        exact_length(bytes.len() as u64, stored.end - stored.start)?;
        if self.range.is_empty() {
            return Ok(Vec::new());
        }
        let size = CHUNK_SIZE as u64;
        let first = self.range.start / size;
        let last = (self.range.end - 1) / size;
        let length =
            usize::try_from(self.range.end - self.range.start).map_err(|_| Error::Allocation)?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(length)
            .map_err(|_| Error::Allocation)?;
        let mut remaining = bytes;
        for index in first..=last {
            let chunk = self.header.chunk(index)?;
            let (sealed, rest) = remaining.split_at(chunk.plaintext_length + FILE_CHUNK_TAG_LEN);
            let plain = self.header.open_chunk(key, path, index, sealed)?;
            let start = self.range.start.saturating_sub(index * size) as usize;
            let end = (self.range.end - index * size).min(chunk.plaintext_length as u64) as usize;
            result.extend_from_slice(&plain[start..end]);
            remaining = rest;
        }
        Ok(result)
    }
}

/// A complete file envelope, borrowing its original ciphertext and tags.
#[derive(Debug)]
pub struct FileObject<'a> {
    header: FileHeader,
    chunks: &'a [u8],
}
impl<'a> FileObject<'a> {
    /// Check the header and exact stored size without allocating or decrypting.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, Error> {
        let header = FileHeader::decode(&bytes[..bytes.len().min(FILE_HEADER_LEN)])?;
        exact_length(bytes.len() as u64, header.encrypted_size()?)?;
        Ok(Self {
            header,
            chunks: &bytes[FILE_HEADER_LEN..],
        })
    }

    /// The validated header describing this file.
    pub fn header(&self) -> FileHeader {
        self.header
    }

    /// Reproduce the cleartext header, ciphertext and tags without resealing.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(FILE_HEADER_LEN + self.chunks.len())
            .map_err(|_| Error::Allocation)?;
        bytes.extend_from_slice(&self.header.encode());
        bytes.extend_from_slice(self.chunks);
        Ok(bytes)
    }

    /// Authenticate only the chunks covering the requested plaintext range.
    pub fn read_range(
        &self,
        key: &FileKey,
        path: &str,
        range: Range<u64>,
    ) -> Result<Vec<u8>, FileError> {
        let selected = self.header.range(range)?;
        let stored = selected.encrypted_range()?;
        let start = (stored.start - FILE_HEADER_LEN as u64) as usize;
        let end = (stored.end - FILE_HEADER_LEN as u64) as usize;
        selected.open(key, path, &self.chunks[start..end])
    }
}

fn exact_length(actual: u64, expected: u64) -> Result<(), Error> {
    match actual.cmp(&expected) {
        std::cmp::Ordering::Less => Err(Error::Truncated),
        std::cmp::Ordering::Greater => Err(Error::TrailingBytes),
        std::cmp::Ordering::Equal => Ok(()),
    }
}
fn offset_error() -> Error {
    Error::Invalid {
        field: "file offset",
        rule: Rule::Chunk,
    }
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
