//! Device ownership in the reader's applied store log, including removed devices.

use crate::{PendingReason, Prerequisite};
use coven_crypto::MemberId;
use coven_database::StoreLog;
use coven_foundation::id_source::DeviceId;

/// The member owning `device`. A device whose registration this reader has not
/// applied yet makes its object wait for that registration; the object is not
/// damaged, and its retained bytes are checked when the entry arrives (§19.1).
pub(crate) fn member(log: &StoreLog, device: DeviceId) -> Result<&MemberId, PendingReason> {
    log.replay
        .state
        .devices
        .get(&device)
        .map(|device| &device.member)
        .ok_or(PendingReason::Waits(Prerequisite::DeviceRegistration(
            device,
        )))
}
