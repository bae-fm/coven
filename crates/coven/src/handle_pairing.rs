//! Adding a device of the person's own by pairing, on the device that shows
//! the code (§13.2, E10).

use super::*;
use coven_storage::{ConnectionCredentials, StorageCredentials};

impl CovenHandle {
    /// Starts a pairing to add another of this member's devices, in a write
    /// of its own, with or without storage, and returns its id and codes at
    /// once (`CovenWrite::add_device`). Any registered device of an active
    /// member may; one that isn't fails with `Rejected(NotAllowed)`. On S3
    /// the QR code carries the key this device connects with, read from its
    /// custody now; one holding no S3 key fails with `InvalidConfiguration`,
    /// and a locked keychain with `KeychainTemporarilyUnavailable`.
    pub async fn add_device(&self) -> Result<AddDevice, SyncError> {
        let key = match self.codes.connection().await? {
            Some(ConnectionCredentials { credentials: StorageCredentials::S3(key), .. }) => {
                Some(key)
            }
            Some(_) | None => None,
        };
        self.write(move |write| write.add_device(key)).await
    }

    /// The pairing's status, from its start until `end_add_device` forgets it;
    /// `None` for a pairing this device never made or has forgotten.
    pub async fn add_device_status(
        &self,
        pairing: AddDeviceId,
    ) -> CovenResult<Option<AddDeviceStatus>> {
        self.database
            .read(move |sql| coven_sync::pairing_status(sql, pairing))
            .await
            .map_err(CovenError::from)
    }

    /// The same internal query, live (E4), for the screen that shows the code.
    pub fn follow_add_device(
        &self,
        pairing: AddDeviceId,
    ) -> LiveQuery<Option<AddDeviceStatus>, CovenError> {
        self.database.subscribe(move |sql| {
            coven_sync::pairing_status(sql, pairing).map_err(CovenError::from)
        })
    }

    /// Approves exactly this checked request, in a write of its own
    /// (`CovenWrite::approve_add_device`).
    pub async fn approve_add_device(&self, request: &AddDeviceRequest) -> Result<(), SyncError> {
        let request = request.clone();
        self.write(move |write| write.approve_add_device(&request)).await
    }

    /// Declines exactly this checked request, in a write of its own
    /// (`CovenWrite::decline_add_device`).
    pub async fn decline_add_device(&self, request: &AddDeviceRequest) -> Result<(), SyncError> {
        let request = request.clone();
        self.write(move |write| write.decline_add_device(&request)).await
    }

    /// The screen showing the pairing's code closed: ends the pairing and
    /// forgets it, in a write of its own (`CovenWrite::end_add_device`).
    pub async fn end_add_device(&self, pairing: AddDeviceId) -> Result<(), SyncError> {
        self.write(move |write| write.end_add_device(pairing)).await
    }
}
