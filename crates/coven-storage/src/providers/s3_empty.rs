//! Emptying an S3 prefix: a development tool outside the sync protocol
//! (api.md E14), and the live certification's cleanup of its namespace.
//! It removes what is under the prefix whatever wrote it, so no store's
//! layout limits it: current objects, upload temporaries, detection probes
//! and unfinished multipart uploads alike.
use super::*;

/// What emptying an S3 prefix removed (api.md E14).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmptiedS3Prefix {
    /// Objects deleted.
    pub objects: usize,
    /// Unfinished multipart uploads aborted.
    pub uploads: usize,
}

/// Why an S3 prefix was not emptied (api.md E14).
#[cfg(feature = "dev-tools")]
#[derive(Debug, thiserror::Error)]
pub enum EmptyS3PrefixError {
    /// The location has no base prefix, so emptying it would empty the whole
    /// bucket. Nothing was sent.
    #[error("the S3 location has no base prefix; emptying it would empty the whole bucket")]
    WholeBucket,
    /// The location or key was refused, or a request failed. Whatever was
    /// removed before the failure stays removed; emptying again finishes.
    #[error("emptying the S3 prefix: {0}")]
    Storage(#[source] StorageError),
}

/// Abort every unfinished upload and delete every object under the
/// location's base prefix, with the location's own settings. A location
/// without a base prefix is refused before any request.
#[cfg(feature = "dev-tools")]
pub(crate) async fn empty_prefix(
    config: StorageConfig,
    credentials: S3Credentials,
    clock: ClockRef,
    ids: IdSourceRef,
    http: HttpClient,
) -> Result<EmptiedS3Prefix, EmptyS3PrefixError> {
    let StorageConfig::S3 { prefix, .. } = &config else {
        return Err(EmptyS3PrefixError::Storage(
            StorageFailure::InvalidConfiguration.with_source("expected S3 configuration"),
        ));
    };
    if prefix.is_empty() {
        return Err(EmptyS3PrefixError::WholeBucket);
    }
    // The slash keeps a sibling prefix that merely starts with the same
    // characters, `dev-old` beside `dev`, out of the listing.
    let under = prefix.key_prefix();
    let emptied = async {
        crate::ConnectionCredentials {
            location: config.clone(),
            credentials: StorageCredentials::S3(credentials.clone()),
        }
        .validate()?;
        let storage = S3Storage::new(
            config,
            credentials,
            clock,
            ids,
            std::sync::Arc::new(crate::NoCooldown),
            http,
        )?;
        storage.empty_under(&under, storage.settings.listing).await
    };
    emptied.await.map_err(EmptyS3PrefixError::Storage)
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
    ) -> Result<EmptiedS3Prefix, StorageError> {
        let uploads = self.unfinished_uploads(prefix).await?;
        for (key, id) in &uploads {
            match self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(id)
                .send()
                .await
            {
                Ok(_) => {}
                Err(error) if error.code() == Some("NoSuchUpload") => {
                    tracing::debug!("S3 upload completed or aborted after it was listed");
                }
                Err(error) => return Err(s3_error(error)),
            }
        }
        let keys = self.keys_under(prefix, listing).await?;
        for key in &keys {
            self.delete_key(key).await?;
        }
        // Something still writing here, such as another device's library,
        // would leave the prefix occupied however often it is emptied.
        let objects = self.list_page(listing, prefix, None, None, Some(1)).await?;
        if !objects.contents.is_empty() || !self.unfinished_uploads(prefix).await?.is_empty() {
            return Err(StorageFailure::Conflict
                .with_source("objects or uploads arrived under the prefix while it was emptied"));
        }
        Ok(EmptiedS3Prefix { objects: keys.len(), uploads: uploads.len() })
    }

    /// Every unfinished multipart upload under `prefix`: its key and upload id.
    async fn unfinished_uploads(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let mut key_marker = None;
        let mut upload_marker = None;
        let mut uploads = Vec::new();
        let mut pages = Pagination::new();
        loop {
            let page = self
                .client
                .list_multipart_uploads()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_key_marker(key_marker)
                .set_upload_id_marker(upload_marker)
                .send()
                .await
                .map_err(s3_error)?;
            for upload in page.uploads() {
                let key = metadata::required(upload.key(), "S3 upload listing omitted key")?;
                within(key, prefix)?;
                let id = metadata::required(upload.upload_id(), "S3 upload listing omitted id")?;
                uploads.push((key.to_owned(), id.to_owned()));
            }
            if !page
                .is_truncated()
                .ok_or(StorageFailure::Protocol.with_source("S3 omitted page completion"))?
            {
                return Ok(uploads);
            }
            let key = metadata::required(page.next_key_marker(), "S3 omitted upload key marker")?;
            let id = metadata::required(page.next_upload_id_marker(), "S3 omitted upload marker")?;
            pages.check(&format!("{key}/{id}"))?;
            key_marker = Some(key.to_owned());
            upload_marker = Some(id.to_owned());
        }
    }

    /// Every object key under `prefix`, across all pages.
    async fn keys_under(
        &self,
        prefix: &str,
        listing: S3Listing,
    ) -> Result<Vec<String>, StorageError> {
        let mut marker = None;
        let mut keys = Vec::new();
        let mut pages = Pagination::new();
        loop {
            let page = self.list_page(listing, prefix, None, marker, None).await?;
            for object in page.contents {
                let key = metadata::required(object.key(), "S3 list entry omitted key")?;
                within(key, prefix)?;
                keys.push(key.to_owned());
            }
            let Some(next) = page.next else {
                return Ok(keys);
            };
            pages.check(&next)?;
            marker = Some(next);
        }
    }
}

/// Refuse a listed key outside the requested prefix rather than remove it.
pub(super) fn within(key: &str, prefix: &str) -> Result<(), StorageError> {
    if key.starts_with(prefix) {
        Ok(())
    } else {
        Err(StorageFailure::Protocol.with_source("S3 listed a key outside the requested prefix"))
    }
}
