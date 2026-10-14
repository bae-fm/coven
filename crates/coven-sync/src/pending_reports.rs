//! The authenticated D8 subset of local pending work.

use crate::{FileMissingReason, PendingReason, PendingRecord, PendingSubject, Prerequisite};
use coven_format::pending as wire;
use coven_foundation::id_source::DeviceId;

pub(crate) fn publish(record: PendingRecord, own: DeviceId) -> Option<wire::PendingReport> {
    if record.reported_by != own {
        return None;
    }
    let subject = match record.subject {
        PendingSubject::Write(id) => wire::PendingSubject::Write(id),
        PendingSubject::Entry(id) => wire::PendingSubject::Entry(id),
        PendingSubject::KeyCopy {
            audience,
            key,
            member,
            sealing,
        } => wire::PendingSubject::KeyCopy {
            audience,
            key,
            member,
            sealing,
        },
        PendingSubject::Snapshot(id) => wire::PendingSubject::Snapshot(id),
        PendingSubject::Positions(device) => wire::PendingSubject::Positions(device),
        PendingSubject::File { device, file } => wire::PendingSubject::File { device, file },
        _ => return None,
    };
    let reason = match record.reason {
        PendingReason::Refused(failure) => wire::PendingReason::Refused((&failure).into()),
        PendingReason::InvalidPositions(failure) => {
            wire::PendingReason::InvalidPositions((&failure).into())
        }
        PendingReason::Missing { path } => wire::PendingReason::Missing { path },
        PendingReason::Waits(Prerequisite::Object(path)) => {
            wire::PendingReason::Waits(wire::Prerequisite::Object(path))
        }
        PendingReason::Waits(Prerequisite::DeviceRegistration(device)) => {
            wire::PendingReason::Waits(wire::Prerequisite::DeviceRegistration(device))
        }
        PendingReason::KeyUnavailable { audience, key } => {
            wire::PendingReason::KeyUnavailable { audience, key }
        }
        PendingReason::UpdateRequired(update) => wire::PendingReason::UpdateRequired(update),
        PendingReason::FileUnavailable(FileMissingReason::Source(failure)) if matches!(subject, wire::PendingSubject::File { device, .. } if device == own) => {
            wire::PendingReason::FileUnavailable(failure)
        }
        _ => return None,
    };
    Some(wire::PendingReport { subject, reason })
}

pub(crate) fn receive(report: wire::PendingReport, reported_by: DeviceId) -> PendingRecord {
    let subject = match report.subject {
        wire::PendingSubject::Write(id) => PendingSubject::Write(id),
        wire::PendingSubject::Entry(id) => PendingSubject::Entry(id),
        wire::PendingSubject::KeyCopy {
            audience,
            key,
            member,
            sealing,
        } => PendingSubject::KeyCopy {
            audience,
            key,
            member,
            sealing,
        },
        wire::PendingSubject::Snapshot(id) => PendingSubject::Snapshot(id),
        wire::PendingSubject::Positions(device) => PendingSubject::Positions(device),
        wire::PendingSubject::File { device, file } => PendingSubject::File { device, file },
    };
    let reason = match report.reason {
        wire::PendingReason::Refused(failure) => PendingReason::Refused(failure.into()),
        wire::PendingReason::InvalidPositions(failure) => {
            PendingReason::InvalidPositions(failure.into())
        }
        wire::PendingReason::Missing { path } => PendingReason::Missing { path },
        wire::PendingReason::Waits(wire::Prerequisite::Object(path)) => {
            PendingReason::Waits(Prerequisite::Object(path))
        }
        wire::PendingReason::Waits(wire::Prerequisite::DeviceRegistration(device)) => {
            PendingReason::Waits(Prerequisite::DeviceRegistration(device))
        }
        wire::PendingReason::KeyUnavailable { audience, key } => {
            PendingReason::KeyUnavailable { audience, key }
        }
        wire::PendingReason::UpdateRequired(update) => PendingReason::UpdateRequired(update),
        wire::PendingReason::FileUnavailable(failure) => {
            PendingReason::FileUnavailable(FileMissingReason::Source(failure))
        }
    };
    PendingRecord {
        subject,
        reason,
        reported_by,
    }
}
