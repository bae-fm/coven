//! Development tools outside the sync protocol (E14). No store is opened and
//! nothing about any store is checked or recorded.

use crate::{ClockRef, IdSourceRef, S3Credentials, StorageConfig};
use coven_storage::providers::{EmptiedS3Bucket, EmptyS3BucketError, ProviderConnector};

/// Delete every object and abort every unfinished upload in an S3
/// location's bucket, whatever wrote them, and return how many of each went
/// (E14). A bucket whose root holds anything but stores' folders and the
/// location marker is refused with `EmptyS3BucketError::OtherFiles` before
/// anything is removed. The first failure stops it; calling again finishes.
/// Retained versions in a versioned bucket stay. It uses the app's `clock`
/// and `ids`, the ones given to `CovenBuilder::clock` and
/// `CovenBuilder::id_source`, and makes none of its own.
pub async fn empty_s3_bucket(
    location: StorageConfig,
    credentials: S3Credentials,
    clock: ClockRef,
    ids: IdSourceRef,
) -> Result<EmptiedS3Bucket, EmptyS3BucketError> {
    ProviderConnector::new(clock, ids).empty_s3_bucket(location, credentials).await
}

#[cfg(test)]
#[path = "dev_tools_tests.rs"]
mod tests;
