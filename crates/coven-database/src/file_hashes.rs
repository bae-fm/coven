//! Whole-file and plaintext-chunk hashes captured from the same first read.

use crate::DbError;
use coven_crypto::{ContentHash, ContentHasher};
use coven_format::chunks::CHUNK_SIZE;

/// A file's content hash and the hash of each 64-KiB plaintext chunk, the
/// last possibly shorter, from one read of its bytes: what an upload checks
/// each chunk it sends against (§17.1, §17.9).
#[derive(Debug, Eq, PartialEq)]
pub struct FileHashes {
    pub(crate) content: ContentHash,
    chunks: Vec<ContentHash>,
}

/// A hash coven stored; one that isn't 32 bytes is damage.
pub(crate) fn stored_hash(bytes: Vec<u8>) -> Result<ContentHash, DbError> {
    let bytes = <[u8; 32]>::try_from(bytes).map_err(|_wrong_size| DbError::DamagedDatabase)?;
    Ok(ContentHash::from_bytes(bytes))
}

/// Hashes a file's bytes as they pass, in order, into its `FileHashes`.
pub struct FileHasher {
    content: ContentHasher,
    chunk: ContentHasher,
    in_chunk: usize,
    chunks: Vec<ContentHash>,
}

impl FileHasher {
    /// A hasher that has seen no bytes.
    #[expect(clippy::new_without_default, reason = "a hasher starts from no bytes, not a default")]
    pub fn new() -> Self {
        Self {
            content: ContentHasher::new(),
            chunk: ContentHasher::new(),
            in_chunk: 0,
            chunks: Vec::new(),
        }
    }

    /// Hash the file's next bytes.
    pub fn update(&mut self, mut bytes: &[u8]) {
        self.content.update(bytes);
        while !bytes.is_empty() {
            let length = bytes.len().min(CHUNK_SIZE - self.in_chunk);
            self.chunk.update(&bytes[..length]);
            self.in_chunk += length;
            bytes = &bytes[length..];
            if self.in_chunk == CHUNK_SIZE {
                self.finish_chunk();
            }
        }
    }

    fn finish_chunk(&mut self) {
        let chunk = std::mem::replace(&mut self.chunk, ContentHasher::new());
        self.chunks.push(chunk.finish());
        self.in_chunk = 0;
    }

    /// The hashes of every byte seen.
    pub fn finish(mut self) -> FileHashes {
        if self.in_chunk != 0 {
            self.finish_chunk();
        }
        FileHashes { content: self.content.finish(), chunks: self.chunks }
    }
}

impl FileHashes {
    /// Hashes as coven stored them: the content hash, then each chunk's in
    /// order.
    pub(crate) fn stored(content: Vec<u8>, chunks: Vec<Vec<u8>>) -> Result<Self, DbError> {
        Ok(Self {
            content: stored_hash(content)?,
            chunks: chunks.into_iter().map(stored_hash).collect::<Result<_, _>>()?,
        })
    }

    /// One hash per 64-KiB plaintext chunk, the last possibly shorter.
    pub(crate) fn chunks(&self) -> &[ContentHash] {
        &self.chunks
    }
}

#[cfg(test)]
#[path = "file_hashes_tests.rs"]
mod tests;
