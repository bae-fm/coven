//! The signing key of a device's owner in the reader's applied store log,
//! including removed devices and members.

use crate::{PendingReason, Prerequisite};
use coven_crypto::SigningKey;
use coven_database::StoreLog;
use coven_foundation::id_source::{DeviceId, MemberId};

/// The member owning `device` and that member's signing key. A device whose registration
/// this reader has not applied yet makes its object wait for that
/// registration; the object is not damaged, and its retained bytes are checked
/// when the entry arrives (§19.1).
///
/// Sentinel: until T105 replays member-key replacements, a member's pair never
/// changes after its addition, so the latest replay holds the key current in
/// every recorded view. T105 resolves the signer in the object's own signed
/// `store_log` view instead (D9).
pub(crate) fn owner(
    log: &StoreLog,
    device: DeviceId,
) -> Result<(MemberId, SigningKey), PendingReason> {
    let state = &log.replay.state;
    let owner = state
        .devices
        .get(&device)
        .map(|device| &device.member)
        .ok_or(PendingReason::Waits(Prerequisite::DeviceRegistration(
            device,
        )))?;
    // A kept registration names a member added by a kept entry, retained
    // after removal.
    let keys = state
        .members
        .get(owner)
        .expect("a registered device's owner is a member")
        .keys;
    Ok((*owner, keys.signing))
}
