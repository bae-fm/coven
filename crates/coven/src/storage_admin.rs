//! Guided console steps for making an S3 member key by hand (§20.10).

use super::StorageConnections;
use crate::*;

/// What guided console steps need to make a member key by hand (§20.10).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GuidedAccessKey {
    /// The bucket the key must reach.
    pub bucket: String,
    /// The store's prefix within it, `<prefix>/<store>/`.
    pub store_prefix: String,
    /// The IAM-style member policy, filled in for this store.
    pub policy: String,
}

impl StorageConnections {
    pub(crate) async fn guided_access_key(&self) -> Result<GuidedAccessKey, SyncError> {
        let location = self.codes.location().await?.ok_or(SyncError::NoStorage)?;
        let StorageConfig::S3 { bucket, prefix, .. } = &location else {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("only S3 locations have member keys")
                .into());
        };
        Ok(GuidedAccessKey {
            bucket: bucket.clone(),
            store_prefix: prefix.key_prefix_of(&coven_storage::ObjectPrefix::all(self.store())),
            policy: coven_storage::providers::s3_member_policy(&location, self.store())?,
        })
    }
}

#[cfg(test)]
#[path = "storage_admin_tests.rs"]
mod tests;
