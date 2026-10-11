//! Permanent log stops used by local download scheduling and persistence.

use coven_format::pending::{PendingReason, PendingReport, PendingSubject, RefusalCode};
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

    /// Select a permanent log refusal from a peer's broader pending report.
    /// Other pending conditions never become permanent local log stops.
    pub fn from_report(report: &PendingReport) -> Option<Self> {
        let PendingReason::Refused(failure) = report.reason else {
            return None;
        };
        let object = match report.subject {
            PendingSubject::Write(id) => LogObject::Write(id),
            PendingSubject::Entry(id) => LogObject::Entry(id),
            _ => return None,
        };
        Some(Self { object, failure })
    }
}

impl From<LogRefusal> for PendingReport {
    fn from(record: LogRefusal) -> Self {
        Self {
            subject: match record.object {
                LogObject::Write(id) => PendingSubject::Write(id),
                LogObject::Entry(id) => PendingSubject::Entry(id),
            },
            reason: PendingReason::Refused(record.failure),
        }
    }
}
