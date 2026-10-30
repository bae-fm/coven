//! Whole-file and plaintext-chunk hashes captured from the same first read.

use coven_crypto::{ContentHash, ContentHasher};
use coven_format::chunks::CHUNK_SIZE;

#[derive(Debug)]
pub(crate) struct FileHashes {
    pub(crate) content: ContentHash,
    chunks: Vec<ContentHash>,
}

pub(crate) struct FileHasher {
    content: ContentHasher,
    chunk: ContentHasher,
    in_chunk: usize,
    chunks: Vec<ContentHash>,
}

impl FileHasher {
    pub(crate) fn new() -> Self {
        Self {
            content: ContentHasher::new(),
            chunk: ContentHasher::new(),
            in_chunk: 0,
            chunks: Vec::new(),
        }
    }

    pub(crate) fn update(&mut self, mut bytes: &[u8]) {
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

    pub(crate) fn finish(mut self) -> FileHashes {
        if self.in_chunk != 0 {
            self.finish_chunk();
        }
        FileHashes {
            content: self.content.finish(),
            chunks: self.chunks,
        }
    }
}

impl FileHashes {
    /// Hashes a first read recorded, as kept with coven's copy.
    pub(crate) fn from_parts(content: ContentHash, chunks: Vec<ContentHash>) -> Self {
        Self { content, chunks }
    }

    /// One hash per 64-KiB plaintext chunk, the last possibly shorter.
    pub(crate) fn chunks(&self) -> &[ContentHash] {
        &self.chunks
    }
}

#[cfg(test)]
#[path = "file_hashes_tests.rs"]
mod tests;
