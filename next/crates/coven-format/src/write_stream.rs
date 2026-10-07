//! Write headers describe independently framed, chunked audience streams (§14.4).

use crate::chunks::{FrameDecoder, PlaintextChunks, CHUNK_SIZE};
use crate::dismissal::WriteFrame;
use crate::error::{require, Error, Rule};
use crate::wire::{decode_frame, wire_struct, Wire};
use crate::write::{WriteHeader, WritePart, WriteRecord};
use crate::{encode_frame, frame_length, FRAME_PREFIX_LEN};
use coven_merge::{Audience, RowId, WriteId};

/// The plaintext stream boundaries of one audience part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartHeader {
    /// Every row in the stream belongs to this audience.
    pub audience: Audience,
    /// Number of change and dismissal frames, without a per-write collection bound.
    pub record_count: u64,
    /// Sum of the complete row frames' lengths, including frame prefixes.
    pub plaintext_length: u64,
}
wire_struct!(PartHeader, audience, record_count, plaintext_length);
impl PartHeader {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        require(self.record_count > 0, "write part rows", Rule::Required)?;
        require(
            self.record_count <= self.plaintext_length / FRAME_PREFIX_LEN as u64,
            "write part length",
            Rule::StreamLength,
        )
    }

    /// Number of sealed chunks required by this part's plaintext length.
    pub fn chunk_count(&self) -> u64 {
        self.plaintext_length.div_ceil(CHUNK_SIZE as u64)
    }
}

/// The kind-1 header frame; changes and dismissals are separate kind-2 and kind-3 frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteHeaderFrame {
    /// Identity, causal positions, timestamp and schema.
    pub header: WriteHeader,
    /// Descriptors in strictly increasing audience order. Empty only for a migration.
    pub parts: Vec<PartHeader>,
}
wire_struct!(WriteHeaderFrame, header, parts);
impl WriteHeaderFrame {
    /// Validate and encode the bounded header frame.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        encode_frame(1, self)
    }

    /// Decode exactly one header, refusing trailing bytes and invalid counts.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, mut input) = decode_frame(bytes)?;
        require(kind == 1, "write header kind", Rule::Kind)?;
        let header = Self::get(&mut input)?;
        input.finish()?;
        header.validate()?;
        Ok(header)
    }

    fn validate(&self) -> Result<(), Error> {
        self.header.validate()?;
        self.header.disposition.validate_parts(self.parts.len())?;
        require(
            self.parts.windows(2).all(|p| p[0].audience < p[1].audience),
            "write parts",
            Rule::Order,
        )?;
        for part in &self.parts {
            part.validate()?;
        }
        Ok(())
    }
}

/// A borrowed write producer. Before emitting, it encodes one row at a time
/// to measure each part's byte length and construct the header;
/// emitting a part retains at most one row frame and one 64-KiB chunk.
pub struct WriteEncoder<'a> {
    record: &'a WriteRecord,
    header: WriteHeaderFrame,
    frame: Vec<u8>,
    length: u64,
}
impl<'a> WriteEncoder<'a> {
    /// Validate the complete in-memory write and measure each part's stream.
    pub fn new(record: &'a WriteRecord) -> Result<Self, Error> {
        record.validate()?;
        let mut parts = Vec::new();
        for part in &record.parts {
            let mut length = 0u64;
            for frame in part.frames() {
                length = add_length(length, frame?.len() as u64)?;
            }
            parts.push(PartHeader {
                audience: part.audience.clone(),
                record_count: (part.rows.len() + part.dismissals.len()) as u64,
                plaintext_length: length,
            });
        }
        let header = WriteHeaderFrame {
            header: record.header.clone(),
            parts,
        };
        let frame = header.encode()?;
        let mut length = frame.len() as u64;
        for part in &header.parts {
            length = add_length(length, part.plaintext_length)?;
        }
        Ok(Self {
            record,
            header,
            frame,
            length,
        })
    }

    /// Metadata that lets a receiver delimit each part without opening it.
    pub fn header(&self) -> &WriteHeaderFrame {
        &self.header
    }

    /// The header's complete kind-1 plaintext frame.
    pub fn header_frame(&self) -> &[u8] {
        &self.frame
    }

    /// The queued plaintext length, for the database's SQLite value limit.
    pub fn plaintext_length(&self) -> u64 {
        self.length
    }

    /// Encode the upload-queue value into an exactly sized buffer. The database
    /// checks [`Self::plaintext_length`] against SQLite's limit before allocating
    /// this buffer. A different buffer length is refused before writing bytes.
    pub fn encode_plaintext(&self, output: &mut [u8]) -> Result<(), Error> {
        require(
            output.len() as u64 == self.length,
            "write plaintext buffer length",
            Rule::StreamLength,
        )?;
        let (header, mut remaining) = output.split_at_mut(self.frame.len());
        header.copy_from_slice(&self.frame);
        for index in 0..self.header.parts.len() {
            for chunk in self.part_chunks(index)? {
                let chunk = chunk?;
                let (piece, rest) = remaining.split_at_mut(chunk.len());
                piece.copy_from_slice(&chunk);
                remaining = rest;
            }
        }
        assert!(remaining.is_empty(), "encoded the measured write length");
        Ok(())
    }

    /// Cut the selected audience's row frames into canonical plaintext chunks.
    pub fn part_chunks(
        &self,
        index: usize,
    ) -> Result<PlaintextChunks<impl Iterator<Item = Result<Vec<u8>, Error>> + '_>, Error> {
        let part = self.record.parts.get(index).ok_or(Error::Invalid {
            field: "write part index",
            rule: Rule::Chunk,
        })?;
        Ok(PlaintextChunks::new(part.frames()))
    }
}

fn add_length(a: u64, b: u64) -> Result<u64, Error> {
    a.checked_add(b).ok_or(Error::Invalid {
        field: "write plaintext length",
        rule: Rule::StreamLength,
    })
}

/// Decode opened chunks of one part in order, retaining only its unfinished
/// frame and previous row identity. The caller stages yielded rows until the
/// whole object and signature have been checked. `finish` checks the declared
/// record count and byte length; frames crossing the part's end are refused
/// before their allocation.
pub struct PartDecoder {
    header: PartHeader,
    frames: FrameDecoder,
    received: u64,
    rows: u64,
    previous: Option<(RowId, Option<(String, WriteId)>)>,
}
impl PartDecoder {
    /// Begin the stream described by the authenticated write header.
    pub fn new(header: PartHeader) -> Result<Self, Error> {
        header.validate()?;
        Ok(Self {
            frames: FrameDecoder::new(Some(header.plaintext_length)),
            header,
            received: 0,
            rows: 0,
            previous: None,
        })
    }

    /// Feed exactly the next chunk. Full chunks are 64 KiB, and the last chunk
    /// has the header's remaining length. Discard the decoder after any error.
    pub fn chunk(&mut self, mut bytes: &[u8]) -> Result<Vec<WriteFrame>, Error> {
        let left = self.header.plaintext_length - self.received;
        require(
            left > 0 && bytes.len() as u64 == left.min(CHUNK_SIZE as u64),
            "write part chunk length",
            Rule::StreamLength,
        )?;
        self.received += bytes.len() as u64;
        let mut rows = Vec::new();
        while let Some(frame) = self.frames.next(&mut bytes)? {
            require(
                self.rows < self.header.record_count,
                "write part row count",
                Rule::StreamLength,
            )?;
            let row = WriteFrame::decode(&frame)?;
            let key = row.key();
            require(
                key.0.audience == self.header.audience,
                "write part audience",
                Rule::Audience,
            )?;
            require(
                self.previous
                    .as_ref()
                    .is_none_or(|previous| *previous < key),
                "write part rows",
                Rule::Order,
            )?;
            self.previous = Some(key);
            self.rows += 1;
            rows.push(row);
            if bytes.is_empty() {
                break;
            }
        }
        Ok(rows)
    }

    /// Refuse an unfinished frame, missing bytes, or a mismatched row count.
    pub fn finish(&self) -> Result<(), Error> {
        self.frames.finish()?;
        require(
            self.received == self.header.plaintext_length && self.rows == self.header.record_count,
            "write part row count/length",
            Rule::StreamLength,
        )
    }
}

/// Decode a plaintext upload-queue value into the database's in-memory write.
/// Stream consumers can instead use [`WriteHeaderFrame`] and [`PartDecoder`].
pub fn decode_plaintext(bytes: &[u8]) -> Result<WriteRecord, Error> {
    let length = frame_length(bytes)?;
    let header = WriteHeaderFrame::decode(bytes.get(..length).ok_or(Error::Truncated)?)?;
    let mut bytes = &bytes[length..];
    let mut parts = Vec::new();
    for part in header.parts {
        let length = usize::try_from(part.plaintext_length).map_err(|_| Error::Truncated)?;
        let stream = bytes.get(..length).ok_or(Error::Truncated)?;
        bytes = &bytes[length..];
        let audience = part.audience.clone();
        let mut decoder = PartDecoder::new(part)?;
        let mut rows = Vec::new();
        let mut dismissals = Vec::new();
        for chunk in stream.chunks(CHUNK_SIZE) {
            for frame in decoder.chunk(chunk)? {
                match frame {
                    WriteFrame::Change(row) => rows.push(row),
                    WriteFrame::Dismissal(dismissal) => dismissals.push(dismissal),
                }
            }
        }
        decoder.finish()?;
        parts.push(WritePart {
            audience,
            rows,
            dismissals,
        });
    }
    if !bytes.is_empty() {
        return Err(Error::TrailingBytes);
    }
    let record = WriteRecord {
        header: header.header,
        parts,
    };
    record.validate()?;
    Ok(record)
}

#[cfg(test)]
#[path = "write_stream_tests.rs"]
mod tests;
