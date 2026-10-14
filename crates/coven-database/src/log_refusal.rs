//! Permanent log stops used by local download scheduling and persistence.

use coven_format::pending::RefusalCode;
use coven_format::value::EntryId;
use coven_foundation::id_source::DeviceId;
use coven_merge::WriteId;

/// The immutable object at which reading a log stopped (§19.1).
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

/// A permanent log stop. This local scheduling value has no wire encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogRefusal {
    /// The object and its log's author.
    pub object: LogObject,
    /// The check that refused complete immutable bytes.
    pub failure: RefusalCode,
}

impl LogRefusal {
    /// Whether this judgment stops reading this object in the same log.
    pub fn blocks(&self, object: LogObject) -> bool {
        matches!(
            (self.object, object),
            (LogObject::Write(_), LogObject::Write(_)) | (LogObject::Entry(_), LogObject::Entry(_))
        ) && self.object.device() == object.device()
            && self.object.number() <= object.number()
    }
}
