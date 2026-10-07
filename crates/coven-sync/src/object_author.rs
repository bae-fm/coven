//! Device ownership in the reader's applied store log, including removed devices.

use crate::{
    write_object::{damaged, invalid},
    SyncError,
};
use coven_crypto::MemberId;
use coven_database::StoreLog;
use coven_storage::ObjectPath;

pub(crate) fn member<'a>(log: &'a StoreLog, path: &ObjectPath) -> Result<&'a MemberId, SyncError> {
    path.device()
        .and_then(|id| log.replay.state.devices.get(&id))
        .map(|device| &device.member)
        .ok_or_else(|| {
            damaged(
                path,
                invalid("object's device is absent from the applied store log"),
            )
        })
}
