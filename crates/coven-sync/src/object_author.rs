//! The signing key of a device's owner in an object's own signed store-log
//! view (D9): what verifies a received snapshot or posted positions, and
//! what every send of a fixed object must still hold (§10). The view's
//! positions are lookup hints until the signature they select verifies.

use crate::{PendingReason, Prerequisite, Refusal, SyncError};
use coven_crypto::MemberIdentity;
use coven_crypto::SigningKey;
use coven_database::StoreLog;
use coven_format::value::EntryPositions;
use coven_foundation::id_source::{DeviceId, MemberId, StoreId};

/// Why an object's signer cannot be resolved in its recorded view.
#[derive(Debug)]
pub(crate) enum Unresolved {
    /// An entry the view names, or the device's registration, has not
    /// applied here yet: the object waits for it and is not damaged (§19.1).
    Waits(PendingReason),
    /// The view is present and does not register the device to a member:
    /// its signer can never be found there, so the object is refused.
    Refused(Refusal),
}

/// The member owning `device` and that member's signing key in the replay of
/// `positions`, the object's signed store-log coverage. A key replacement the
/// view does not include leaves its earlier key current there: historical
/// snapshot and positions signatures stay valid, and replacement adds no
/// landing deadline to them (D9). Sending still needs current credentials.
pub(crate) fn owner(
    log: &StoreLog,
    store: StoreId,
    device: DeviceId,
    positions: &EntryPositions,
) -> Result<(MemberId, SigningKey), Unresolved> {
    if let Some(missing) = positions
        .0
        .iter()
        .find(|id| !log.replay.entries.contains_key(id))
    {
        return Err(Unresolved::Waits(PendingReason::Waits(
            Prerequisite::Object(crate::store_log_object::path(store, *missing)),
        )));
    }
    let view = crate::replay::at(log, positions);
    let Some(owner) = view.state.devices.get(&device).map(|device| device.member) else {
        // A device no applied entry registers waits for that registration; one
        // registered only outside the signed view cannot have signed in it.
        return Err(if log.replay.state.devices.contains_key(&device) {
            Unresolved::Refused(Refusal::NotAuthorized)
        } else {
            Unresolved::Waits(PendingReason::Waits(Prerequisite::DeviceRegistration(
                device,
            )))
        });
    };
    // A kept registration names a member added by a kept entry, retained
    // after removal.
    let keys = view
        .state
        .members
        .get(&owner)
        .expect("a registered device's owner is a member")
        .keys;
    Ok((owner, keys.signing))
}

/// The object's fixed signer must be the held signing key: a write or entry
/// verifies with its author's key in its recorded view, and current custody
/// cannot re-sign it under newer keys (§10, D9). `signer` is that recorded key.
pub(crate) fn check_signer(
    member: &MemberIdentity,
    signer: Option<coven_crypto::SigningKey>,
) -> Result<(), SyncError> {
    if signer != Some(member.keys().public_keys().signing) {
        return Err(SyncError::MemberKeysReplaced {
            member: member.member(),
        });
    }
    Ok(())
}

/// The signing key the replay of `positions` binds to `member`, if any.
pub(crate) fn recorded_signer(
    log: &StoreLog,
    positions: &coven_format::value::EntryPositions,
    member: &coven_foundation::id_source::MemberId,
) -> Option<coven_crypto::SigningKey> {
    crate::replay::at(log, positions)
        .state
        .members
        .get(member)
        .map(|held| held.keys.signing)
}

#[cfg(test)]
#[path = "object_author_tests.rs"]
pub(crate) mod tests;
