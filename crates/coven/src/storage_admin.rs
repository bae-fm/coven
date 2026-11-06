//! Guided console steps for making an S3 member key by hand (§20.10).

use super::StorageConnections;
use crate::*;

/// What guided console steps need to make a member key by hand (§20.10).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GuidedAccessKey {
    /// The bucket the key must reach, which the store has to itself.
    pub bucket: String,
    /// The IAM-style member policy, filled in for this store's bucket.
    pub policy: String,
}

impl StorageConnections {
    pub(crate) async fn guided_access_key(&self) -> Result<GuidedAccessKey, SyncError> {
        let location = self.codes.location().await?.ok_or(SyncError::NoStorage)?;
        let StorageConfig::S3 { bucket, .. } = &location else {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("only S3 locations have member keys")
                .into());
        };
        Ok(GuidedAccessKey {
            bucket: bucket.clone(),
            policy: coven_storage::providers::s3_member_policy(&location)?,
        })
    }
}

#[cfg(test)]
#[path = "storage_admin_tests.rs"]
mod tests;
