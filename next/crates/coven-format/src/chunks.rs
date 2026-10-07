//! Fixed-size plaintext chunks and bounded reconstruction of individual frames.

use crate::error::{require, Error, Rule};
use crate::{frame_length, FRAME_PREFIX_LEN};

/// Plaintext bytes in a full write-part or snapshot chunk.
pub const CHUNK_SIZE: usize = 64 * 1024;

/// Cut a sequence of frames into fixed-size chunks, retaining at most one frame
/// and one output chunk. Only the last chunk may be shorter than [`CHUNK_SIZE`].
pub struct PlaintextChunks<I> {
    frames: I,
    frame: Vec<u8>,
    offset: usize,
    ended: bool,
}

impl<I: Iterator<Item = Result<Vec<u8>, Error>>> PlaintextChunks<I> {
    /// The frames come from a write-row or snapshot encoder, in stream order.
    pub fn new(frames: I) -> Self {
        Self {
            frames,
            frame: Vec::new(),
            offset: 0,
            ended: false,
        }
    }

    fn chunk(&mut self) -> Result<Vec<u8>, Error> {
        let mut chunk = Vec::new();
        chunk
            .try_reserve_exact(CHUNK_SIZE)
            .map_err(|_| Error::Allocation)?;
        while chunk.len() < CHUNK_SIZE {
            if self.offset == self.frame.len() {
                match self.frames.next() {
                    Some(frame) => {
                        self.frame = frame?;
                        require(
                            frame_length(&self.frame)? == self.frame.len(),
                            "chunk frame",
                            Rule::Chunk,
                        )?;
                        self.offset = 0;
                    }
                    None => {
                        self.ended = true;
                        break;
                    }
                }
            }
            let count = (CHUNK_SIZE - chunk.len()).min(self.frame.len() - self.offset);
            chunk.extend_from_slice(&self.frame[self.offset..self.offset + count]);
            self.offset += count;
        }
        Ok(chunk)
    }
}

impl<I: Iterator<Item = Result<Vec<u8>, Error>>> Iterator for PlaintextChunks<I> {
    type Item = Result<Vec<u8>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        match self.chunk() {
            Ok(chunk) if chunk.is_empty() => None,
            Ok(chunk) => Some(Ok(chunk)),
            Err(error) => {
                self.ended = true;
                Some(Err(error))
            }
        }
    }
}

/// Holds only one unfinished frame. A known remaining length lets the write
/// decoder refuse a frame that crosses its part's end before reserving memory.
pub struct FrameDecoder {
    bytes: Vec<u8>,
    remaining: Option<u64>,
}

impl FrameDecoder {
    /// Bound the complete stream when its length is known.
    pub fn new(remaining: Option<u64>) -> Self {
        Self {
            bytes: Vec::new(),
            remaining,
        }
    }

    /// Consume bytes through one bounded frame, retaining an unfinished frame.
    pub fn next(&mut self, input: &mut &[u8]) -> Result<Option<Vec<u8>>, Error> {
        loop {
            let target = if self.bytes.len() < FRAME_PREFIX_LEN {
                FRAME_PREFIX_LEN
            } else {
                frame_length(&self.bytes)?
            };
            if let Some(remaining) = self.remaining {
                require(
                    (target - self.bytes.len()) as u64 <= remaining,
                    "frame crosses stream end",
                    Rule::StreamLength,
                )?;
            }
            if self.bytes.len() == target {
                // A complete prefix may also be an empty frame.
                let length = frame_length(&self.bytes)?;
                if length == self.bytes.len() {
                    return Ok(Some(std::mem::take(&mut self.bytes)));
                }
                continue;
            }
            if input.is_empty() {
                return Ok(None);
            }
            let count = (target - self.bytes.len()).min(input.len());
            self.bytes
                .try_reserve_exact(count)
                .map_err(|_| Error::Allocation)?;
            self.bytes.extend_from_slice(&input[..count]);
            *input = &input[count..];
            if let Some(remaining) = &mut self.remaining {
                *remaining -= count as u64;
            }
        }
    }

    /// Reject an incomplete frame or a stream shorter than its declared length.
    pub fn finish(&self) -> Result<(), Error> {
        if self.bytes.is_empty() && self.remaining.is_none_or(|n| n == 0) {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }
}

#[cfg(test)]
#[path = "chunks_tests.rs"]
mod tests;
