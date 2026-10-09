//! Permanent log failures carried by signed posted positions (D8).

use crate::error::{require, Rule};
use crate::value::EntryId;
use crate::wire::{Decoder, Encoder, Wire};
use crate::Error;
use coven_foundation::id_source::DeviceId;
use coven_merge::WriteId;

/// The immutable object at which a device stopped reading a log (§19.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogObject {
    /// A write in a device's write log.
    Write(WriteId),
    /// An entry in a device's store log.
    Entry(EntryId),
}

impl LogObject {
    /// The device that owns this log.
    pub fn device(self) -> DeviceId {
        match self {
            Self::Write(id) => id.device,
            Self::Entry(id) => id.device,
        }
    }

    /// This object's positive position within its log.
    pub fn number(self) -> u64 {
        match self {
            Self::Write(id) => id.number,
            Self::Entry(id) => id.number,
        }
    }
}

impl Wire for LogObject {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Write(id) => {
                0u8.put(out)?;
                id.put(out)
            }
            Self::Entry(id) => {
                1u8.put(out)?;
                id.put(out)
            }
        }
    }

    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Write(Wire::get(input)?)),
            1 => Ok(Self::Entry(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "log object",
                tag,
            }),
        }
    }
}

/// The check that permanently refused an immutable log object (§19.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StuckFailure {
    /// The authenticated encryption could not be opened.
    Decryption,
    /// The author's signature did not verify.
    Signature,
    /// The bytes or causal metadata violate their format.
    Parse,
    /// The downloaded write violates the merge or application schema's checks.
    InvalidWrite,
}

impl From<StuckFailure> for u8 {
    fn from(failure: StuckFailure) -> Self {
        match failure {
            StuckFailure::Decryption => 0,
            StuckFailure::Signature => 1,
            StuckFailure::Parse => 2,
            StuckFailure::InvalidWrite => 3,
        }
    }
}

impl TryFrom<u8> for StuckFailure {
    type Error = Error;
    fn try_from(tag: u8) -> Result<Self, Error> {
        match tag {
            0 => Ok(Self::Decryption),
            1 => Ok(Self::Signature),
            2 => Ok(Self::Parse),
            3 => Ok(Self::InvalidWrite),
            tag => Err(Error::UnknownTag {
                field: "stuck failure",
                tag,
            }),
        }
    }
}

impl Wire for StuckFailure {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        u8::from(*self).put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Self::try_from(u8::get(input)?)
    }
}

/// A receiver's judgment, published only by that receiver (D8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StuckRecord {
    /// The object and its log's author.
    pub object: LogObject,
    /// The permanent check failure.
    pub failure: StuckFailure,
}
crate::wire::wire_struct!(StuckRecord, object, failure);

impl StuckRecord {
    /// Whether this judgment stops reading this object in the same log.
    pub fn blocks(&self, object: LogObject) -> bool {
        matches!(
            (self.object, object),
            (LogObject::Write(_), LogObject::Write(_)) | (LogObject::Entry(_), LogObject::Entry(_))
        ) && self.object.device() == object.device()
            && self.object.number() <= object.number()
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        require(self.object.number() > 0, "stuck object", Rule::Required)
    }
}
