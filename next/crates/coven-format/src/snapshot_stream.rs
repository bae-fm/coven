//! Feed opened snapshot chunks to the frame decoder without retaining rows.

use crate::chunks::FrameDecoder;
use crate::error::{require, Error, Rule};
use crate::snapshot::{SnapshotDecoder, SnapshotHeader, SnapshotRecord};
use coven_merge::{Audience, WriteOracle};

/// Incrementally reconstruct frames and check the snapshot's end marker.
/// Call [`Self::next_record`] repeatedly on each opened chunk, updating the
/// oracle after each applied-write record before asking for the next record.
pub struct SnapshotChunkDecoder {
    audience: Audience,
    frames: FrameDecoder,
    snapshot: Option<SnapshotDecoder>,
}
impl SnapshotChunkDecoder {
    /// The expected audience comes from the sealed snapshot prefix.
    pub fn new(audience: Audience) -> Self {
        Self {
            audience,
            frames: FrameDecoder::new(None),
            snapshot: None,
        }
    }

    /// Header metadata becomes available as soon as its frame is complete.
    pub fn header(&self) -> Option<&SnapshotHeader> {
        self.snapshot.as_ref().map(SnapshotDecoder::header)
    }

    /// Consume bytes through the next complete record, or all available bytes
    /// if its frame is unfinished. `None` means this input has no further record.
    /// Discard the decoder after an error; commit staged rows only after `finish`.
    pub fn next_record(
        &mut self,
        bytes: &mut &[u8],
        oracle: &impl WriteOracle,
    ) -> Result<Option<SnapshotRecord>, Error> {
        while let Some(frame) = self.frames.next(bytes)? {
            if let Some(snapshot) = &mut self.snapshot {
                if let Some(record) = snapshot.frame(&frame, oracle)? {
                    return Ok(Some(record));
                }
            } else {
                let snapshot = SnapshotDecoder::start(&frame)?;
                require(
                    snapshot.header().id.audience == self.audience,
                    "snapshot prefix audience",
                    Rule::Audience,
                )?;
                self.snapshot = Some(snapshot);
            }
        }
        Ok(None)
    }

    /// Refuse EOF without the complete header, declared records and end marker.
    pub fn finish(&self) -> Result<(), Error> {
        self.frames.finish()?;
        self.snapshot.as_ref().ok_or(Error::Truncated)?.finish()
    }
}

#[cfg(test)]
#[path = "snapshot_stream_tests.rs"]
mod tests;
