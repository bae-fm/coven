//! Ending the join requests a device gone for good left waiting (§13.2).
//! No pass of that device will ever read them, and on S3 nothing shared is
//! taken back, so each would wait forever. An active admin's device that
//! applied the stop ends them for it.

use super::{SequenceSync, Session};
use crate::SyncError;
use coven_crypto::MemberIdentity;
use coven_format::{admin::MemberRole, device_gone::DeviceGone};
use coven_foundation::id_source::DeviceId;
use coven_storage::{ObjectPath, ObjectPrefix, StorageFailure};

impl SequenceSync {
    /// For each device gone for good at a slot this registered device
    /// applied, put its gone marker in its join requests' folder, then
    /// delete everything else there: the requests, and its ended invites'
    /// markers. The marker turns away any request sent later, and a request
    /// this listing misses is deleted by the joining device that finds the
    /// marker. Only a device whose member is an active admin does this; one
    /// added after the stop leaves it to the devices that applied it. A
    /// failure fails the pass, and the next pass ends that device's
    /// requests again from the start.
    pub(crate) async fn end_gone_requests(
        &self,
        session: &Session,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let history = &session.history;
        if !history
            .state
            .members
            .get(&member.member())
            .is_some_and(|value| !value.removed && value.role == MemberRole::Admin)
        {
            return Ok(());
        }
        let added = history.added(self.device).expect("a registered device was added");
        let ended = self.database.requests_ended().await?;
        for device in history.state.devices.keys().filter(|device| !ended.contains(device)) {
            match history.gone_for_good(*device) {
                // A reset registers this installation in the same slot that
                // replaces the one it carries on from.
                Some((slot, _)) if slot.0 >= added => {
                    self.end_requests(*device).await?;
                    self.database.record_requests_ended(*device).await?;
                }
                _ => (),
            }
        }
        Ok(())
    }

    /// Create `device`'s gone marker, then list and delete what else is
    /// under its join requests.
    async fn end_requests(&self, device: DeviceId) -> Result<(), SyncError> {
        let marker = ObjectPath::device_gone(self.store(), device);
        match self.storage.create(&marker, &DeviceGone.encode()).await {
            Ok(_) => (),
            // An earlier attempt's marker, or another admin device's.
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                DeviceGone::decode(&self.storage.read(&marker).await?)?;
            }
            Err(error) => return Err(error.into()),
        }
        let folder = ObjectPrefix::device_join_requests(self.store(), device);
        for object in self.storage.list(&folder).await? {
            if object.path == marker {
                continue;
            }
            match self.storage.delete(&object.path).await {
                Ok(()) => (),
                Err(error) if error.failure() == StorageFailure::NotFound => (),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_gone_requests_tests.rs"]
mod tests;
