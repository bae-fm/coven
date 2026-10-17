//! Incremental framing of a writes slot's unbounded plaintext body (D9).
//! Inner writes stay opaque until their own header and parts are opened.

use crate::chunks::FrameDecoder;
use crate::error::{require, Error, Rule};
use crate::path::ObjectPath;
use crate::sequence::SlotHeader;
use crate::write::WriteHeader;

/// Exact crypto identity of an inner write, independent of its containing slot.
pub fn identity(
    store: coven_foundation::id_source::StoreId,
    write: coven_merge::WriteId,
) -> Result<String, Error> {
    crate::value::positive(write.number)?;
    Ok(format!(
        "coven/write/{store}/{}/{}",
        write.device.0, write.number
    ))
}

/// Encodes the body header and each inner length, checking device and order.
/// Feed the returned pieces and the inner bytes to a 64-KiB chunker. No frame
/// or collection encloses the complete batch.
pub struct WritesSlotEncoder {
    header: SlotHeader,
    remaining: u64,
    previous: Option<u64>,
}
impl WritesSlotEncoder {
    /// Emit the slot header and the positive u64 write count.
    pub fn start(
        header: SlotHeader,
        path: &ObjectPath,
        count: u64,
    ) -> Result<(Self, Vec<u8>), Error> {
        crate::value::positive(count)?;
        let mut bytes = header.encode(path)?;
        bytes.extend_from_slice(&count.to_be_bytes());
        Ok((
            Self {
                header,
                remaining: count,
                previous: None,
            },
            bytes,
        ))
    }
    /// Emit an inner object's length after validating its original header.
    /// The caller streams exactly this many fixed inner bytes afterwards.
    pub fn write(&mut self, header: &WriteHeader, length: u64) -> Result<[u8; 8], Error> {
        require(
            self.remaining > 0 && length > 0,
            "batch write count/length",
            Rule::StreamLength,
        )?;
        check_write(&self.header, self.previous, header)?;
        self.previous = Some(header.position.number);
        self.remaining -= 1;
        Ok(length.to_be_bytes())
    }
    /// Require every declared write.
    pub fn finish(self) -> Result<(), Error> {
        require(self.remaining == 0, "batch write count", Rule::StreamLength)
    }
}

/// One piece borrowed from the opened outer stream, never a whole-batch allocation.
#[derive(Debug)]
pub enum BatchPiece<'a> {
    /// Encrypted-header metadata is now available.
    Header(SlotHeader),
    /// Start of one inner write with this exact sealed size.
    Write {
        /// Exact sealed inner-object size, excluding this length field.
        length: u64,
    },
    /// The next bytes of that inner object, in order.
    Bytes(&'a [u8]),
    /// The inner object ended; its opened header must have been checked.
    EndWrite,
}

enum State {
    Header,
    Count,
    Length,
    Bytes(u64),
    EndWrite,
    End,
}

/// Reads the header, u64 lengths and inner bytes with bounded retained state.
/// Call `check_header` once per inner write, after opening its header. The
/// enclosing signature and each inner signature remain the caller's to verify.
pub struct WritesSlotDecoder {
    path: ObjectPath,
    frame: FrameDecoder,
    word: Vec<u8>,
    header: Option<SlotHeader>,
    remaining: u64,
    previous: Option<u64>,
    checked: bool,
    state: State,
}
impl WritesSlotDecoder {
    /// Read one slot body at its canonical path.
    pub fn new(path: ObjectPath) -> Self {
        Self {
            path,
            frame: FrameDecoder::new(Some(47)),
            word: Vec::new(),
            header: None,
            remaining: 0,
            previous: None,
            checked: false,
            state: State::Header,
        }
    }
    /// Advance through one event. A partial scalar or header is retained;
    /// inner bytes are borrowed and can be fed directly to their decoder.
    pub fn next<'a>(&mut self, input: &mut &'a [u8]) -> Result<Option<BatchPiece<'a>>, Error> {
        loop {
            match self.state {
                State::Header => {
                    let Some(frame) = self.frame.next(input)? else {
                        return Ok(None);
                    };
                    let header = SlotHeader::decode(&frame, &self.path)?;
                    self.header = Some(header.clone());
                    self.state = State::Count;
                    return Ok(Some(BatchPiece::Header(header)));
                }
                State::Count | State::Length => {
                    let take = (8 - self.word.len()).min(input.len());
                    self.word.extend_from_slice(&input[..take]);
                    *input = &input[take..];
                    if self.word.len() != 8 {
                        return Ok(None);
                    }
                    let number = u64::from_be_bytes(self.word[..].try_into().expect("eight bytes"));
                    self.word.clear();
                    crate::value::positive(number)?;
                    if matches!(self.state, State::Count) {
                        self.remaining = number;
                        self.state = State::Length;
                    } else {
                        self.state = State::Bytes(number);
                        self.checked = false;
                        return Ok(Some(BatchPiece::Write { length: number }));
                    }
                }
                State::Bytes(left) => {
                    if input.is_empty() {
                        return Ok(None);
                    }
                    let take = left.min(input.len() as u64) as usize;
                    let bytes = &input[..take];
                    *input = &input[take..];
                    self.state = if left == take as u64 {
                        State::EndWrite
                    } else {
                        State::Bytes(left - take as u64)
                    };
                    return Ok(Some(BatchPiece::Bytes(bytes)));
                }
                State::EndWrite => {
                    require(self.checked, "batch inner header", Rule::Required)?;
                    self.remaining -= 1;
                    self.state = if self.remaining == 0 {
                        State::End
                    } else {
                        State::Length
                    };
                    return Ok(Some(BatchPiece::EndWrite));
                }
                State::End => {
                    if !input.is_empty() {
                        return Err(Error::TrailingBytes);
                    }
                    return Ok(None);
                }
            }
        }
    }
    /// Check the authenticated inner header's author, authored view and order.
    /// Queue completeness and historical credentials require the consumer's history.
    pub fn check_header(&mut self, header: &WriteHeader) -> Result<(), Error> {
        require(
            !self.checked && matches!(self.state, State::Bytes(_) | State::EndWrite),
            "batch inner header position",
            Rule::StreamLength,
        )?;
        check_write(
            self.header.as_ref().ok_or(Error::Truncated)?,
            self.previous,
            header,
        )?;
        self.previous = Some(header.position.number);
        self.checked = true;
        Ok(())
    }
    /// Reject an incomplete count, length, object or missing inner-header check.
    pub fn finish(self) -> Result<(), Error> {
        if matches!(self.state, State::End) {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }
}
/// Check one authenticated inner header's device, causal prefix and increasing
/// number. Seekable consumers use the same rule as the incremental decoder.
pub fn check_write(
    slot: &SlotHeader,
    previous: Option<u64>,
    header: &WriteHeader,
) -> Result<(), Error> {
    slot.check_write(header)?;
    require(
        previous.is_none_or(|number| number < header.position.number),
        "batch write order",
        Rule::Order,
    )
}

#[cfg(test)]
#[path = "writes_slot_tests.rs"]
mod tests;
