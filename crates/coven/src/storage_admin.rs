//! Admin access and guided console steps for S3 member keys (§4).

use super::StorageConnections;
use crate::*;
use std::sync::Arc;

/// What guided console steps need to make a member key by hand (§4).
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
    /// Obtain the storage account's admin access from the source the person
    /// chose, for one operation (§4). Only S3 locations have member keys.
    pub(crate) async fn admin_access(
        &self,
        source: coven_storage::providers::AdminAccessSource,
    ) -> Result<Arc<dyn coven_storage::AccessKeyAdmin>, SyncError> {
        let location = self.codes.location().await?.ok_or(SyncError::NoStorage)?;
        if location.provider() != CloudProvider::S3 {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("only S3 member keys need admin access")
                .into());
        }
        let credential = coven_storage::providers::obtain_admin_access(
            source,
            self.oauth.as_ref(),
            self.clock.clone(),
        )
        .await?;
        Ok(self.connector.s3_key_admin(location, credential)?)
    }

    pub(crate) async fn guided_access_key(&self) -> Result<GuidedAccessKey, SyncError> {
        let location = self.codes.location().await?.ok_or(SyncError::NoStorage)?;
        let StorageConfig::S3 { bucket, prefix, .. } = &location else {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("only S3 locations have member keys")
                .into());
        };
        Ok(GuidedAccessKey {
            bucket: bucket.clone(),
            store_prefix: coven_storage::ObjectPrefix::all(self.store).under(prefix),
            policy: coven_storage::providers::s3_member_policy(&location, self.store)?,
        })
    }
}

#[cfg(test)]
#[path = "storage_admin_tests.rs"]
mod tests;
