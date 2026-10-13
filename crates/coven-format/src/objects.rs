//! Join requests, posted positions and clock observations (D8).

use crate::error::{require, Error, Rule};
use crate::pending::{PendingReason, PendingReport, PendingSubject, Prerequisite};
use crate::store_log::MemberPublicKeys;
use crate::value::{name, EntryPositions, WritePositions};
use crate::wire::wire_struct;
use coven_foundation::id_source::{DeviceId, InviteId, StoreId};
use coven_merge::Audience;

/// A device's storage-time probe (§9, D8). Its publication time is provider
/// metadata; the frame asserts no time, positions, fingerprints or reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockObservation {
    /// The publishing device, which must match the clock object's path.
    pub device: DeviceId,
}
wire_struct!(ClockObservation, device);

/// A new person's request, encrypted with the invite-derived key and signed (§12.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinRequest {
    /// The invite this request answers.
    pub invite: InviteId,
    /// The new member's signing and sealed-box public keys.
    pub keys: MemberPublicKeys,
    /// The new device's name shown to the approving admin.
    pub device_name: String,
}
wire_struct!(JoinRequest, invite, keys, device_name => crate::wire::get_name);
impl JoinRequest {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        name(&self.device_name)
    }
}

/// An audience's keyed fingerprint, supplied by the cryptography layer (§19.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fingerprint {
    /// The rows this fingerprint describes.
    pub audience: Audience,
    /// The audience key id used to derive the fingerprint key.
    pub key: coven_foundation::id_source::KeyId,
    /// The 256-bit fingerprint; this crate neither computes nor authenticates it.
    pub bytes: coven_crypto::Fingerprint,
}
wire_struct!(Fingerprint, audience, key, bytes);

/// A device's posted positions, optional fingerprints and local pending reports (§15, §19).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostedPositions {
    /// The device publishing this record.
    pub device: DeviceId,
    /// Applied positions in device write logs.
    pub writes: WritePositions,
    /// Applied positions in the store log, which also affect the merged state.
    pub store_log: EntryPositions,
    /// The app schema under which these fingerprints were computed (D8).
    pub schema_version: u32,
    /// Empty unless fingerprints describe exactly this past; otherwise store first,
    /// followed by the readable circles in increasing audience order.
    pub fingerprints: Vec<Fingerprint>,
    /// Local observations ordered by subject, with one first unmet condition per subject.
    /// The enclosing device is the reporter; received reports must never be forwarded.
    pub pending: Vec<PendingReport>,
}
wire_struct!(
    PostedPositions,
    device,
    writes,
    store_log,
    schema_version,
    fingerprints,
    pending
);
impl PostedPositions {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.writes.validate()?;
        self.store_log.validate()?;
        for record in &self.pending {
            record.validate(self.device)?;
        }
        require(
            self.pending
                .windows(2)
                .all(|pair| pair[0].subject < pair[1].subject),
            "pending subjects",
            Rule::Order,
        )?;
        require(
            self.fingerprints
                .first()
                .is_none_or(|f| f.audience == Audience::Store),
            "store fingerprint",
            Rule::Required,
        )?;
        require(
            self.fingerprints
                .windows(2)
                .all(|f| f[0].audience < f[1].audience),
            "fingerprints",
            Rule::Order,
        )?;
        Ok(())
    }

    /// Check recipients and report paths against the authenticated member and store (D8).
    /// The decoder checks syntax; the caller supplies this context after authentication.
    pub fn validate_reporter(
        &self,
        member: &coven_crypto::MemberId,
        store: StoreId,
    ) -> Result<(), Error> {
        for report in &self.pending {
            match &report.reason {
                PendingReason::Missing { path }
                | PendingReason::Waits(Prerequisite::Object(path)) => {
                    require(path.store() == store, "pending path store", Rule::Store)?
                }
                _ => {}
            }
        }
        require(
            self.pending.iter().all(|report| match &report.subject {
                PendingSubject::KeyCopy {
                    member: recipient, ..
                } => recipient == member,
                _ => true,
            }),
            "key copy reporter",
            Rule::Kind,
        )
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
