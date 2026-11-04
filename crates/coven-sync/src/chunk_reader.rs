//! Seekable plaintext over canonical sealed chunks already retained locally.

use crate::slot_object::DigestStream;
use coven_crypto::DerivedKeys;
use coven_format::{
    chunks::{chunk_count, chunk_length, chunk_start, CHUNK_SIZE},
    sealed_stream::StreamObjectLayout,
    STORED_CHUNK_OVERHEAD,
};
use std::{
    io::{self, Read, Seek, SeekFrom},
    sync::Arc,
};

/// A section's authenticated coordinates and bounded decrypted cache. Cloning
/// makes another cursor without copying a cached plaintext chunk, and
/// without the digest of a cursor that hashes what it reads.
pub(crate) struct ChunkReader<R> {
    input: R,
    offset: u64,
    length: u64,
    position: u64,
    path: Arc<str>,
    prefix: Arc<[u8]>,
    key: Arc<DerivedKeys>,
    section: u64,
    cached: Option<(u64, zeroize::Zeroizing<Vec<u8>>)>,
    hashing: Option<Hashing>,
}

/// The sealed chunks a cursor has hashed, in order, and the next one it
/// will hash. A cursor that reads past an unhashed chunk drops its digest.
struct Hashing {
    digest: DigestStream,
    next: u64,
}

impl<R: Clone> Clone for ChunkReader<R> {
    fn clone(&self) -> Self {
        Self {
            input: self.input.clone(),
            offset: self.offset,
            length: self.length,
            position: self.position,
            path: self.path.clone(),
            prefix: self.prefix.clone(),
            key: self.key.clone(),
            section: self.section,
            cached: None,
            hashing: None,
        }
    }
}

impl<R> ChunkReader<R> {
    pub(crate) fn new(
        input: R,
        offset: u64,
        length: u64,
        path: Arc<str>,
        prefix: Arc<[u8]>,
        key: Arc<DerivedKeys>,
        section: u64,
    ) -> Self {
        Self {
            input,
            offset,
            length,
            position: 0,
            path,
            prefix,
            key,
            section,
            cached: None,
            hashing: None,
        }
    }
    /// Hash each sealed chunk into `digest` as this cursor first reads it.
    pub(crate) fn hashing(mut self, digest: DigestStream) -> Self {
        self.hashing = Some(Hashing { digest, next: 0 });
        self
    }
    /// The digest of every sealed chunk, once this cursor has read all of
    /// them in order; `None` if it skipped one or wasn't hashing.
    pub(crate) fn digest(&mut self) -> Option<DigestStream> {
        let chunks = chunk_count(self.length);
        self.hashing.take().filter(|hashing| hashing.next == chunks).map(|hashing| hashing.digest)
    }
    pub(crate) fn len(&self) -> u64 {
        self.length
    }
}

impl<R: Read + Seek> Read for ChunkReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.position >= self.length {
            return Ok(0);
        }
        let index = self.position / CHUNK_SIZE as u64;
        if self.cached.as_ref().is_none_or(|(cached, _)| *cached != index) {
            let plain_length = chunk_length(self.length, index);
            self.input.seek(SeekFrom::Start(
                self.offset + index * (CHUNK_SIZE + STORED_CHUNK_OVERHEAD) as u64,
            ))?;
            let mut bytes = vec![0; plain_length + STORED_CHUNK_OVERHEAD];
            self.input.read_exact(&mut bytes)?;
            if let Some(hashing) = &mut self.hashing {
                if index == hashing.next {
                    hashing.digest.update(&bytes);
                    hashing.next += 1;
                } else if index > hashing.next {
                    self.hashing = None;
                }
            }
            let sealed =
                StreamObjectLayout::new().decode_chunk(&bytes).map_err(io::Error::other)?;
            let plain = self
                .key
                .open_object_chunk(&self.path, &self.prefix, self.section, index, sealed)
                .map_err(io::Error::other)?;
            self.cached = Some((index, zeroize::Zeroizing::new(plain)));
        }
        let (_, plain) = self.cached.as_ref().expect("loaded chunk");
        let offset = (self.position - chunk_start(index)) as usize;
        let length = output.len().min(plain.len() - offset);
        output[..length].copy_from_slice(&plain[offset..offset + length]);
        self.position += length as u64;
        Ok(length)
    }
}

impl<R> Seek for ChunkReader<R> {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.position = match from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
            SeekFrom::End(offset) => self.length.checked_add_signed(offset),
        }
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "plaintext position is outside u64")
        })?;
        Ok(self.position)
    }
    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position)
    }
}

pub(crate) type SlotPlaintext = ChunkReader<coven_database::RetainedBytes>;
pub(crate) type PartPlaintext = ChunkReader<SlotPlaintext>;
