//! Development tools outside the sync protocol (E14). No store is opened and
//! nothing about any store is checked or recorded.

use crate::{S3Credentials, StorageConfig, SystemClock, UuidIds};
use coven_storage::providers::{EmptiedS3Prefix, EmptyS3PrefixError, ProviderConnector};
use std::sync::Arc;

/// Delete every object and abort every unfinished upload under an S3
/// location's base prefix, whatever wrote them, and return how many of each
/// went (E14). A location with no base prefix is refused before any request
/// with `EmptyS3PrefixError::WholeBucket`. The first failure stops it;
/// calling again finishes. Retained versions in a versioned bucket stay.
pub async fn empty_s3_prefix(
    location: StorageConfig,
    credentials: S3Credentials,
) -> Result<EmptiedS3Prefix, EmptyS3PrefixError> {
    ProviderConnector::new(Arc::new(SystemClock), Arc::new(UuidIds))
        .empty_s3_prefix(location, credentials)
        .await
}

#[cfg(test)]
#[path = "dev_tools_tests.rs"]
mod tests;
