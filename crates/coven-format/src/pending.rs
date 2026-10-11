//! The peer-actionable subset of pending observations in signed positions (D8).
//!
//! These wire values cannot carry local operation/provider/drop details, native
//! causes, user-file paths, retry metadata or a separate reporting device.
//! The enclosing positions object identifies and authenticates the reporter.

use crate::error::{require, Error, Rule};
use crate::path::ObjectPath;
use crate::store_log::SnapshotId;
use crate::value::{positive, EntryId};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use coven_crypto::MemberId;
use coven_foundation::id_source::{DeviceId, FileId, KeyId};
use coven_merge::{Audience, WriteId};
use std::cmp::Ordering;

/// One local observation published by the enclosing positions object's author.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingReport {
    /// The object whose processing cannot advance.
    pub subject: PendingSubject,
    /// Its first unmet condition, stripped of process-local details.
    pub reason: PendingReason,
}
wire_struct!(PendingReport, subject, reason);

/// Subjects permitted in D8, ordered by tag then fields in D2 byte order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingSubject {
    /// A device's numbered write.
    Write(WriteId),
    /// A device's numbered store-log entry.
    Entry(EntryId),
    /// A sealed copy addressed to the reporting device's member.
    KeyCopy {
        /// The key's audience.
        audience: Audience,
        /// The key's identity.
        key: KeyId,
        /// The copy's recipient.
        member: MemberId,
    },
    /// A file belonging to its uploading device.
    File {
        /// The uploader.
        device: DeviceId,
        /// The fixed file identity.
        file: FileId,
    },
    /// An audience snapshot.
    Snapshot(SnapshotId),
    /// A device's replaceable posted positions.
    Positions(DeviceId),
}

impl PendingSubject {
    fn tag(&self) -> u8 {
        match self {
            Self::Write(_) => 0,
            Self::Entry(_) => 1,
            Self::KeyCopy { .. } => 2,
            Self::File { .. } => 3,
            Self::Snapshot(_) => 4,
            Self::Positions(_) => 5,
        }
    }
}

impl Ord for PendingSubject {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Write(a), Self::Write(b)) => a.cmp(b),
            (Self::Entry(a), Self::Entry(b)) => a.cmp(b),
            (
                Self::KeyCopy {
                    audience: a,
                    key: ak,
                    member: am,
                },
                Self::KeyCopy {
                    audience: b,
                    key: bk,
                    member: bm,
                },
            ) => (a, ak, am).cmp(&(b, bk, bm)),
            (
                Self::File {
                    device: a,
                    file: af,
                },
                Self::File {
                    device: b,
                    file: bf,
                },
            ) => (a, af).cmp(&(b, bf)),
            (Self::Snapshot(a), Self::Snapshot(b)) => {
                (&a.audience, a.device, a.number).cmp(&(&b.audience, b.device, b.number))
            }
            (Self::Positions(a), Self::Positions(b)) => a.cmp(b),
            _ => self.tag().cmp(&other.tag()),
        }
    }
}
impl PartialOrd for PendingSubject {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Wire for PendingSubject {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.tag().put(out)?;
        match self {
            Self::Write(id) => id.put(out),
            Self::Entry(id) => id.put(out),
            Self::KeyCopy {
                audience,
                key,
                member,
            } => {
                audience.put(out)?;
                key.put(out)?;
                member.put(out)
            }
            Self::File { device, file } => {
                device.put(out)?;
                file.put(out)
            }
            Self::Snapshot(id) => id.put(out),
            Self::Positions(device) => device.put(out),
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Write(Wire::get(input)?)),
            1 => Ok(Self::Entry(Wire::get(input)?)),
            2 => Ok(Self::KeyCopy {
                audience: Wire::get(input)?,
                key: Wire::get(input)?,
                member: Wire::get(input)?,
            }),
            3 => Ok(Self::File {
                device: Wire::get(input)?,
                file: Wire::get(input)?,
            }),
            4 => Ok(Self::Snapshot(Wire::get(input)?)),
            5 => Ok(Self::Positions(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "pending subject",
                tag,
            }),
        }
    }
}

/// Only conditions that a peer can act on travel in positions (D8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingReason {
    /// A permanent check failure of complete immutable bytes.
    Refused(RefusalCode),
    /// The named storage object has not arrived.
    Missing {
        /// A canonical storage path, never a local file path.
        path: ObjectPath,
    },
    /// Another object's arrival or a device's registration is required.
    Waits(Prerequisite),
    /// A member cannot obtain this audience key.
    KeyUnavailable {
        /// The key's audience.
        audience: Audience,
        /// The missing key identity.
        key: KeyId,
    },
    /// An app schema or format update is required.
    UpdateRequired(RequiredUpdate),
    /// The poster cannot supply its own file's source.
    FileUnavailable(FileSourceFailure),
    /// Replaceable positions failed a check; discovery remains retryable.
    InvalidPositions(RefusalCode),
}

/// Prerequisites that name something a peer can supply (D8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Prerequisite {
    /// A canonical storage object must arrive.
    Object(ObjectPath),
    /// The named device's registration must arrive.
    DeviceRegistration(DeviceId),
}

/// The version the reader needs, with the format's u16 bound represented here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequiredUpdate {
    /// The application's schema.
    AppSchema {
        /// The required schema version.
        version: u32,
    },
    /// Coven's object format.
    CovenFormat {
        /// The required format version.
        version: u16,
    },
}

/// A file source that its uploading device cannot deliver; no local path travels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileSourceFailure {
    /// The local source is absent.
    Missing,
    /// The source changed after attachment.
    Changed,
    /// The source fails its recorded integrity checks.
    Integrity,
}

/// The D8 code of a refusal, without a native error from a process or higher crate.
/// Sync converts its cause-bearing `Refusal` at this wire boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusalCode {
    /// Authenticated decryption failed.
    Decryption,
    /// The signature failed.
    Signature,
    /// The bytes violate their format.
    Parse,
    /// The write failed merge or application-schema checks.
    InvalidWrite,
    /// The author was not authorized in its recorded view.
    NotAuthorized,
    /// The timestamp or recorded past violates causality.
    InvalidCausality,
    /// The identity differs from the path, author or key.
    WrongIdentity,
    /// File plaintext differs from its content hash.
    ContentHash,
}

macro_rules! wire_code {
    ($ty:ident, $field:literal, $($variant:ident = $tag:literal),+ $(,)?) => {
        impl From<$ty> for u8 {
            fn from(value: $ty) -> Self {
                match value { $($ty::$variant => $tag,)+ }
            }
        }
        impl TryFrom<u8> for $ty {
            type Error = Error;
            fn try_from(tag: u8) -> Result<Self, Error> {
                match tag {
                    $($tag => Ok(Self::$variant),)+
                    tag => Err(Error::UnknownTag { field: $field, tag }),
                }
            }
        }
        impl Wire for $ty {
            fn put(&self, out: &mut Encoder) -> Result<(), Error> { u8::from(*self).put(out) }
            fn get(input: &mut Decoder<'_>) -> Result<Self, Error> { Self::try_from(u8::get(input)?) }
        }
    };
}
wire_code!(
    RefusalCode,
    "refusal",
    Decryption = 0,
    Signature = 1,
    Parse = 2,
    InvalidWrite = 3,
    NotAuthorized = 4,
    InvalidCausality = 5,
    WrongIdentity = 6,
    ContentHash = 7
);
wire_code!(
    FileSourceFailure,
    "file source failure",
    Missing = 0,
    Changed = 1,
    Integrity = 2
);

impl Wire for ObjectPath {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        crate::wire::put_blob(self.as_str().as_bytes(), out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        String::get(input)?.try_into().map_err(|_| Error::Invalid {
            field: "pending object path",
            rule: Rule::Kind,
        })
    }
}

impl Wire for Prerequisite {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Object(path) => {
                0u8.put(out)?;
                path.put(out)
            }
            Self::DeviceRegistration(device) => {
                1u8.put(out)?;
                device.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Object(Wire::get(input)?)),
            1 => Ok(Self::DeviceRegistration(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "pending prerequisite",
                tag,
            }),
        }
    }
}

impl Wire for RequiredUpdate {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::AppSchema { version } => {
                0u8.put(out)?;
                version.put(out)
            }
            Self::CovenFormat { version } => {
                1u8.put(out)?;
                u32::from(*version).put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::AppSchema {
                version: Wire::get(input)?,
            }),
            1 => Ok(Self::CovenFormat {
                version: u16::try_from(u32::get(input)?).map_err(|_| Error::Invalid {
                    field: "required format version",
                    rule: Rule::Kind,
                })?,
            }),
            tag => Err(Error::UnknownTag {
                field: "required update",
                tag,
            }),
        }
    }
}

impl Wire for PendingReason {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Refused(failure) => {
                0u8.put(out)?;
                failure.put(out)
            }
            Self::Missing { path } => {
                1u8.put(out)?;
                path.put(out)
            }
            Self::Waits(prerequisite) => {
                2u8.put(out)?;
                prerequisite.put(out)
            }
            Self::KeyUnavailable { audience, key } => {
                3u8.put(out)?;
                audience.put(out)?;
                key.put(out)
            }
            Self::UpdateRequired(update) => {
                4u8.put(out)?;
                update.put(out)
            }
            Self::FileUnavailable(failure) => {
                5u8.put(out)?;
                failure.put(out)
            }
            Self::InvalidPositions(failure) => {
                6u8.put(out)?;
                failure.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Refused(Wire::get(input)?)),
            1 => Ok(Self::Missing {
                path: Wire::get(input)?,
            }),
            2 => Ok(Self::Waits(Wire::get(input)?)),
            3 => Ok(Self::KeyUnavailable {
                audience: Wire::get(input)?,
                key: Wire::get(input)?,
            }),
            4 => Ok(Self::UpdateRequired(Wire::get(input)?)),
            5 => Ok(Self::FileUnavailable(Wire::get(input)?)),
            6 => Ok(Self::InvalidPositions(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "pending reason",
                tag,
            }),
        }
    }
}

impl PendingReport {
    pub(crate) fn validate(&self, poster: DeviceId) -> Result<(), Error> {
        match &self.subject {
            PendingSubject::Write(id) => positive(id.number)?,
            PendingSubject::Entry(id) => positive(id.number)?,
            PendingSubject::Snapshot(id) => positive(id.number)?,
            _ => (),
        }
        match &self.reason {
            PendingReason::Refused(_) => require(
                !matches!(self.subject, PendingSubject::Positions(_)),
                "refused subject",
                Rule::Kind,
            )?,
            PendingReason::InvalidPositions(_) => require(
                matches!(self.subject, PendingSubject::Positions(_)),
                "invalid positions subject",
                Rule::Kind,
            )?,
            PendingReason::FileUnavailable(_) => require(
                matches!(self.subject, PendingSubject::File { device, .. } if device == poster),
                "file source reporter",
                Rule::Kind,
            )?,
            _ => (),
        }
        if matches!(
            self.reason,
            PendingReason::Refused(RefusalCode::ContentHash)
                | PendingReason::InvalidPositions(RefusalCode::ContentHash)
        ) {
            require(
                matches!(self.subject, PendingSubject::File { .. }),
                "content hash subject",
                Rule::Kind,
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "pending_tests.rs"]
mod tests;
