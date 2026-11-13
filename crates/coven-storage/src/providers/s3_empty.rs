//! Emptying an S3 bucket: a development tool outside the sync protocol
//! (api.md E14), and, under one store's folder, the live certification's
//! cleanup of its namespace. It removes what is there whatever wrote it, so
//! no store's layout limits it: current objects, upload temporaries,
//! detection probes and unfinished multipart uploads alike.
#[cfg(feature = "dev-tools")]
use super::own_bucket::RootEntry;
use super::*;

/// What emptying an S3 bucket removed (api.md E14).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmptiedS3Bucket {
    /// Objects deleted.
    pub objects: usize,
    /// Unfinished multipart uploads aborted.
    pub uploads: usize,
}

/// Why an S3 bucket was not emptied (api.md E14).
#[cfg(feature = "dev-tools")]
#[derive(Debug, thiserror::Error)]
pub enum EmptyS3BucketError {
    /// The bucket's root holds something no store wrote: an object other
    /// than the location marker, or a folder not named by a store id. Nothing
    /// was removed.
    #[error("the S3 bucket holds files no coven store wrote")]
    OtherFiles,
    /// The location or key was refused, or a request failed. Whatever was
    /// removed before the failure stays removed; emptying again finishes.
    #[error("emptying the S3 bucket: {0}")]
    Storage(#[source] StorageError),
}

/// Abort every unfinished upload and delete every object in the location's
/// bucket, with the location's own settings, once its root shows it holds
/// only stores' folders and the location marker.
#[cfg(feature = "dev-tools")]
pub(crate) async fn empty_bucket(
    config: StorageConfig,
    credentials: S3Credentials,
    clock: ClockRef,
    ids: IdSourceRef,
    http: HttpClient,
) -> Result<EmptiedS3Bucket, EmptyS3BucketError> {
    crate::ConnectionCredentials {
        location: config.clone(),
        credentials: StorageCredentials::S3(credentials.clone()),
    }
    .validate()
    .map_err(EmptyS3BucketError::Storage)?;
    let storage = S3Storage::new(
        config,
        credentials,
        clock,
        ids,
        std::sync::Arc::new(crate::NoCooldown),
        http,
    )
    .map_err(EmptyS3BucketError::Storage)?;
    let stores_only = storage
        .root_admits(|entry| match entry {
            RootEntry::Object(key) => key == crate::location_claim::LOCATION_MARKER,
            RootEntry::Folder(folder) => folder
                .strip_suffix('/')
                .is_some_and(|name| coven_foundation::id_source::canonical_uuid(name).is_ok()),
        })
        .await
        .map_err(EmptyS3BucketError::Storage)?;
    if !stores_only {
        return Err(EmptyS3BucketError::OtherFiles);
    }
    storage.empty_under("", storage.settings.listing).await.map_err(EmptyS3BucketError::Storage)
}

impl S3Storage {
    /// Abort every unfinished upload whose key starts with `prefix`, then
    /// delete every object whose key does, and confirm that none of either
    /// remains. Everything is listed before anything is removed, so the
    /// listings' markers stay valid. The first failure stops it; removal is
    /// idempotent, so running it again finishes. Retained versions of a
    /// versioned bucket are left: member keys may not delete them.
    pub(super) async fn empty_under(
        &self,
        prefix: &str,
        listing: S3Listing,
    ) -> Result<EmptiedS3Bucket, StorageError> {
        let uploads = self.unfinished_uploads(prefix, None).await?;
        for (key, id) in &uploads {
            self.abort_multipart(key, id, None).await?;
        }
        let keys = self.keys_under(prefix, listing).await?;
        for key in &keys {
            self.delete_key(key).await?;
        }
        // Something still writing here, such as another device's library,
        // would leave the prefix occupied however often it is emptied.
        let objects = self.list_page(listing, prefix, None, None, Some(1)).await?;
        if !objects.contents.is_empty() || !self.unfinished_uploads(prefix, None).await?.is_empty()
        {
            return Err(StorageFailure::Conflict
                .with_source("objects or uploads arrived under the prefix while it was emptied"));
        }
        Ok(EmptiedS3Bucket { objects: keys.len(), uploads: uploads.len() })
    }
}
