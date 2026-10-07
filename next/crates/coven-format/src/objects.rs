//! Join requests and posted positions (§6, §11, §12, §16, §19).

use crate::error::{require, Error, Rule};
use crate::store_log::MemberPublicKeys;
use crate::value::{name, EntryPositions, WritePositions};
use crate::wire::wire_struct;
use coven_foundation::id_source::{DeviceId, InviteId};
use coven_merge::Audience;

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

/// A device's posted positions and fingerprints, made after uploading its writes (§15).
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
    /// One fingerprint per readable audience, in increasing audience order.
    pub fingerprints: Vec<Fingerprint>,
}
wire_struct!(
    PostedPositions,
    device,
    writes,
    store_log,
    schema_version,
    fingerprints
);
impl PostedPositions {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.writes.validate()?;
        self.store_log.validate()?;
        require(
            self.fingerprints
                .first()
                .is_some_and(|f| f.audience == Audience::Store),
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
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
