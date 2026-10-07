//! Whole-file and plaintext-chunk hashes captured from the same first read.

use crate::{sqlite::DatabaseConnection, write_rows::AppKey, DbError};
use coven_crypto::{ContentHash, ContentHasher};
use coven_format::file::{FileHeader, DEFAULT_CHUNK_SIZE};

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
            let length = bytes.len().min(DEFAULT_CHUNK_SIZE as usize - self.in_chunk);
            self.chunk.update(&bytes[..length]);
            self.in_chunk += length;
            bytes = &bytes[length..];
            if self.in_chunk == DEFAULT_CHUNK_SIZE as usize {
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
    pub(crate) fn record(
        &self,
        db: &DatabaseConnection,
        key: &AppKey,
        column: &str,
        identity: &[u8],
        size: u64,
    ) -> Result<(), DbError> {
        if self.chunks.len() as u64 != FileHeader::new(size).chunk_count() {
            return Err(DbError::DamagedDatabase);
        }
        db.internal_execute(
            "DELETE FROM _coven_file_chunks WHERE table_name=?1 AND key=?2 AND column_name=?3",
            (&key.0, &key.1, column),
        )?;
        for (index, hash) in self.chunks.iter().enumerate() {
            db.internal_execute(
                "INSERT INTO _coven_file_chunks(table_name,key,column_name,identity,chunk,hash)
                 VALUES(?1,?2,?3,?4,?5,?6)",
                (
                    &key.0,
                    &key.1,
                    column,
                    identity,
                    index as i64,
                    &hash.as_bytes()[..],
                ),
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "file_hashes_tests.rs"]
mod tests;
