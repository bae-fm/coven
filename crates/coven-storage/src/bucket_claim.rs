//! Bucket ownership is provider metadata, separate from encrypted store paths.

use crate::{
    OwnBucketReason, ProviderOps, Storage, StorageConfig, StorageConnection, StorageError,
    StorageFailure,
};
use coven_foundation::id_source::StoreId;

pub(crate) const BUCKET_CLAIM: &str = "coven-bucket";
pub(crate) const BUCKET_CLAIM_SIZE: usize = 36;

pub(crate) fn occupied() -> StorageError {
    StorageFailure::OwnBucketRequired {
        reason: OwnBucketReason::Occupied,
    }
    .into()
}

impl<P: ProviderOps + ?Sized> StorageConnection<P> {
    fn needs_bucket_claim(&self) -> Result<bool, StorageError> {
        let config = self.config();
        match config {
            StorageConfig::S3 {
                endpoint, profile, ..
            } => Ok(
                crate::providers::s3_key_scope(endpoint.as_ref(), profile.as_ref())?
                    == crate::providers::S3KeyScope::Bucket,
            ),
            _ => Ok(false),
        }
    }

    /// First setup claims the bucket after provider validation and before
    /// publishing store state. An occupied marker is accepted only when its
    /// exact bytes name this store. No failure removes or replaces the marker.
    /// Locations with prefix-scoped keys, and non-S3 locations, need no marker.
    pub async fn claim_bucket(&self, store: StoreId) -> Result<(), StorageError> {
        if !self.needs_bucket_claim()? {
            return Ok(());
        }
        self.cooling()?;
        let created = self
            .observed(
                self.send(
                    crate::RequestKind::Create,
                    crate::bucket_claim::BUCKET_CLAIM_SIZE as u64,
                    self.provider.create_bucket_claim(store),
                )
                .await,
            )
            .await;
        match created {
            Ok(()) => Ok(()),
            Err(error)
                if error.failure() == StorageFailure::AlreadyExists
                    && !matches!(error, StorageError::Cleanup { .. }) =>
            {
                self.check_bucket(store).await
            }
            Err(error) => Err(error),
        }
    }

    /// Reconnect, restore and join read the exact bucket marker without
    /// listing the root or claiming an absent marker. Provider errors survive;
    /// missing, malformed and other-store markers refuse this connection.
    pub async fn check_bucket(&self, store: StoreId) -> Result<(), StorageError> {
        if !self.needs_bucket_claim()? {
            return Ok(());
        }
        self.cooling()?;
        let read = self
            .send(
                crate::RequestKind::Read,
                0,
                self.provider.read_bucket_claim(),
            )
            .await;
        if let Ok(bytes) = &read {
            self.received(bytes.len() as u64);
        }
        match self.observed(read).await {
            Ok(bytes) if bytes == store.to_string().as_bytes() => Ok(()),
            Ok(_) => Err(occupied()),
            Err(error) if error.failure() == StorageFailure::NotFound => Err(occupied()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
#[path = "bucket_claim_tests.rs"]
mod tests;
