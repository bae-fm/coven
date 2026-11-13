//! Adding a device of the person's own by pairing, on the device that shows
//! the code (§13.2, E10).

use super::*;
use coven_storage::{ConnectionCredentials, StorageCredentials};

impl CovenHandle {
    /// Starts a pairing to add another of this member's devices, in a write
    /// of its own, with or without storage, and returns its id and codes at
    /// once (`CovenWrite::start_pairing`). Any registered device of an active
    /// member may; one that isn't fails with `Rejected(NotAllowed)`. On S3
    /// the QR code carries the key this device connects with, read from its
    /// custody now; one holding no S3 key fails with `InvalidConfiguration`,
    /// and a locked keychain with `KeychainTemporarilyUnavailable`.
    pub async fn start_pairing(&self) -> Result<Pairing, SyncError> {
        let key = match self.codes.connection().await? {
            Some(ConnectionCredentials { credentials: StorageCredentials::S3(key), .. }) => {
                Some(key)
            }
            Some(_) | None => None,
        };
        self.write(move |write| write.start_pairing(key)).await
    }

    /// The pairing's status, from its start until `end_pairing` forgets it;
    /// `None` for a pairing this device never made or has forgotten.
    pub async fn pairing(&self, pairing: PairingId) -> CovenResult<Option<PairingStatus>> {
        self.database
            .read(move |sql| coven_sync::pairing_status(sql, pairing))
            .await
            .map_err(CovenError::from)
    }

    /// The same internal query, live (E4), for the screen that shows the code.
    pub fn subscribe_pairing(
        &self,
        pairing: PairingId,
    ) -> LiveQuery<Option<PairingStatus>, CovenError> {
        self.database.subscribe(move |sql| {
            coven_sync::pairing_status(sql, pairing).map_err(CovenError::from)
        })
    }

    /// Approves exactly this checked request, in a write of its own
    /// (`CovenWrite::approve_pairing`).
    pub async fn approve_pairing(&self, request: &PairingRequest) -> Result<(), SyncError> {
        let request = request.clone();
        self.write(move |write| write.approve_pairing(&request)).await
    }

    /// Declines exactly this checked request, in a write of its own
    /// (`CovenWrite::decline_pairing`).
    pub async fn decline_pairing(&self, request: &PairingRequest) -> Result<(), SyncError> {
        let request = request.clone();
        self.write(move |write| write.decline_pairing(&request)).await
    }

    /// The screen showing the pairing's code closed: ends the pairing and
    /// forgets it, in a write of its own (`CovenWrite::end_pairing`).
    pub async fn end_pairing(&self, pairing: PairingId) -> Result<(), SyncError> {
        self.write(move |write| write.end_pairing(pairing)).await
    }
}
