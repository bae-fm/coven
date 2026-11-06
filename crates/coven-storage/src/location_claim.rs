//! A location every member reaches whole holds one store, which the location
//! marker at its root names (§20.3). The marker is provider metadata,
//! separate from encrypted store paths.

use crate::{ProviderOps, Storage, StorageConnection, StorageError, StorageFailure};
use coven_foundation::id_source::StoreId;

pub(crate) const LOCATION_MARKER: &str = "coven-location";
pub(crate) const LOCATION_MARKER_SIZE: usize = 36;

pub(crate) fn occupied() -> StorageError {
    StorageFailure::OwnLocationRequired.into()
}

/// Refuse a marker whose size isn't a store id's before reading its bytes:
/// it is someone else's object.
pub(crate) fn check_marker_size(size: u64) -> Result<(), StorageError> {
    if size != LOCATION_MARKER_SIZE as u64 {
        return Err(occupied());
    }
    Ok(())
}

impl<P: ProviderOps + ?Sized> StorageConnection<P> {
    /// First setup claims the location after provider validation and before
    /// publishing store state. An occupied marker is accepted only when its
    /// exact bytes name this store. No failure removes or replaces the marker.
    /// A location that holds several stores has no marker.
    pub async fn claim_location(&self, store: StoreId) -> Result<(), StorageError> {
        if !self.config().holds_one_store() {
            return Ok(());
        }
        // A resend that finds the marker occupied reads it, as any occupied
        // marker is read: its exact bytes settle it.
        let created = self
            .resent(move || async move {
                self.cooling()?;
                self.observed(
                    self.send(
                        crate::RequestKind::Create,
                        LOCATION_MARKER_SIZE as u64,
                        self.provider.create_location_marker(store, self.device),
                    )
                    .await,
                )
                .await
            })
            .await;
        match created {
            Ok(()) => Ok(()),
            Err(error) if error.own_failure() == Some(StorageFailure::AlreadyExists) => {
                self.check_location_claim(store).await
            }
            Err(error) => Err(error),
        }
    }

    /// Reconnect, restore and join read the exact location marker without
    /// listing the root or claiming an absent marker. Provider errors survive;
    /// missing, malformed and other-store markers refuse this connection.
    pub async fn check_location_claim(&self, store: StoreId) -> Result<(), StorageError> {
        if !self.config().holds_one_store() {
            return Ok(());
        }
        let read = self
            .resent(move || async move {
                self.cooling()?;
                let read = self
                    .send(crate::RequestKind::Read, 0, self.provider.read_location_marker())
                    .await;
                if let Ok(bytes) = &read {
                    self.received(bytes.len() as u64);
                }
                self.observed(read).await
            })
            .await;
        match read {
            Ok(bytes) if bytes == store.to_string().as_bytes() => Ok(()),
            Ok(_) => Err(occupied()),
            Err(error) if error.failure() == StorageFailure::NotFound => Err(occupied()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
#[path = "location_claim_tests.rs"]
mod tests;
