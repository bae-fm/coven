//! Seekable plaintext over canonical sealed chunks already retained locally.

use coven_crypto::DerivedKeys;
use coven_format::{chunks::CHUNK_SIZE, sealed_stream::StreamObjectLayout};
use std::{
    io::{self, Read, Seek, SeekFrom},
    sync::Arc,
};

pub(crate) const CHUNK_OVERHEAD: usize = 4 + coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD;

/// A section's authenticated coordinates and bounded decrypted cache. Cloning
/// makes another cursor without copying a cached plaintext chunk.
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
        }
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
        if self
            .cached
            .as_ref()
            .is_none_or(|(cached, _)| *cached != index)
        {
            let plain_length =
                (self.length - index * CHUNK_SIZE as u64).min(CHUNK_SIZE as u64) as usize;
            self.input.seek(SeekFrom::Start(
                self.offset + index * (CHUNK_SIZE + CHUNK_OVERHEAD) as u64,
            ))?;
            let mut bytes = vec![0; plain_length + CHUNK_OVERHEAD];
            self.input.read_exact(&mut bytes)?;
            let sealed = StreamObjectLayout::new()
                .decode_chunk(&bytes)
                .map_err(io::Error::other)?;
            let plain = self
                .key
                .open_object_chunk(&self.path, &self.prefix, self.section, index, sealed)
                .map_err(io::Error::other)?;
            self.cached = Some((index, zeroize::Zeroizing::new(plain)));
        }
        let (_, plain) = self.cached.as_ref().expect("loaded chunk");
        let offset = (self.position % CHUNK_SIZE as u64) as usize;
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
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "plaintext position is outside u64",
            )
        })?;
        Ok(self.position)
    }
    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position)
    }
}

pub(crate) type SlotPlaintext = ChunkReader<coven_database::RetainedBytes>;
pub(crate) type PartPlaintext = ChunkReader<SlotPlaintext>;
