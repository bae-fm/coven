//! A request sealed for one path, and the storage work that sends it and
//! looks at it, shared by a join's request and a pairing's (§13.2).

use crate::SyncError;
use coven_format::device_gone::DeviceGone;
use coven_foundation::id_source::DeviceId;
use coven_storage::{ObjectPath, Storage, StorageFailure};

/// The request bytes `request` fixed for `path`, which the device named by
/// `showing` answers: an inviting admin's device for a join, the device
/// showing the code for a pairing.
pub(crate) struct RequestAt<'a> {
    pub(crate) path: &'a ObjectPath,
    pub(crate) request: &'a [u8],
    pub(crate) showing: DeviceId,
}

/// What a create of the request found at its path.
pub(crate) enum Placed {
    /// The request is at the path: this create wrote it, or it was there.
    Ours,
    /// Something else is at the path, with these bytes.
    Other(Vec<u8>),
}

impl RequestAt<'_> {
    /// Create the request at the path, create-once. An occupied path is read:
    /// the request itself is placed, anything else is reported. A path found
    /// taken and then empty is the storage's failure, for the caller's retry
    /// to handle.
    pub(crate) async fn place(&self, storage: &dyn Storage) -> Result<Placed, SyncError> {
        match storage.create(self.path, self.request).await {
            Ok(_) => Ok(Placed::Ours),
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                let found = storage.read(self.path).await?;
                Ok(if found == self.request { Placed::Ours } else { Placed::Other(found) })
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Whether the showing device's gone marker is in storage: it is gone for
    /// good, and an admin's device has ended its requests (§13.2).
    pub(crate) async fn showing_gone(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        let marker = ObjectPath::device_gone(self.path.store(), self.showing);
        match storage.read(&marker).await {
            Ok(found) => {
                DeviceGone::decode(&found)?;
                Ok(true)
            }
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Whether this request is still at its path. A request beside the
    /// showing device's gone marker reached storage after an admin's device
    /// listed that device's requests, and no device will list them again, so
    /// it is deleted, and it is gone (§13.2).
    pub(crate) async fn pending(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        let present = match storage.read(self.path).await {
            Ok(found) => found == self.request,
            Err(error) if error.failure() == StorageFailure::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if present && self.showing_gone(storage).await? {
            storage.delete_if_present(self.path).await?;
            return Ok(false);
        }
        Ok(present)
    }
}
