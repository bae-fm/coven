#![cfg(test)]

use super::*;
use coven_foundation::{
    clock::SystemClock,
    id_source::{StoreId, UuidIds},
};
use std::sync::Arc;

pub(super) struct LiveS3 {
    pub(super) config: StorageConfig,
    credentials: S3Credentials,
}
impl LiveS3 {
    pub(super) fn load() -> Self {
        let mut settings = crate::providers::tests::live_settings("COVEN_LIVE_S3_ENV");
        let endpoint = settings
            .remove("COVEN_LIVE_S3_ENDPOINT")
            .map(|value| value.as_str().parse().expect("valid live endpoint"));
        let mut take = |key| {
            settings
                .remove(key)
                .expect("required live S3 setting missing")
        };
        let config = StorageConfig::S3 {
            bucket: take("COVEN_LIVE_S3_BUCKET").as_str().to_owned(),
            region: take("COVEN_LIVE_S3_REGION").as_str().to_owned(),
            endpoint,
            prefix: "coven-live".into(),
        };
        let credentials = S3Credentials {
            access_key_id: take("COVEN_LIVE_S3_ACCESS_KEY_ID").as_str().to_owned(),
            secret_access_key: take("COVEN_LIVE_S3_SECRET_ACCESS_KEY"),
        };
        config.validate().expect("valid live configuration");
        Self {
            config,
            credentials,
        }
    }
    pub(super) fn connect(&self, profile: Option<S3Profile>) -> StorageConnection<S3Storage> {
        let config = self.config.clone();
        let credentials = self.credentials.clone();
        let mut provider = match profile {
            Some(profile) => S3Storage::with_profile(
                config,
                credentials,
                Arc::new(SystemClock),
                Arc::new(UuidIds),
                profile,
            ),
            None => S3Storage::new(
                config,
                credentials,
                Arc::new(SystemClock),
                Arc::new(UuidIds),
            ),
        }
        .expect("live S3 connection");
        provider.single_request_limit = 1024 * 1024;
        StorageConnection::from_provider(Arc::new(provider))
    }
}

/// Use native listings so completed temporaries and incomplete uploads are both
/// removed, including after an assertion or a provider operation fails.
pub(super) async fn cleanup(storage: &S3Storage, store: StoreId) -> Result<(), StorageError> {
    let prefix = ObjectPrefix::all(store).under(&storage.prefix);
    let mut key_marker = None;
    let mut upload_marker = None;
    let mut uploads = Vec::new();
    let mut pages = Pagination::new();
    loop {
        let page = storage
            .client
            .list_multipart_uploads()
            .bucket(&storage.bucket)
            .prefix(&prefix)
            .set_key_marker(key_marker)
            .set_upload_id_marker(upload_marker)
            .send()
            .await
            .map_err(s3_error)?;
        for upload in page.uploads() {
            uploads.push((
                upload.key().expect("upload key").to_owned(),
                upload.upload_id().expect("upload id").to_owned(),
            ));
        }
        if !page.is_truncated().expect("multipart listing completion") {
            break;
        }
        let key = page
            .next_key_marker()
            .expect("multipart key marker")
            .to_owned();
        let id = page
            .next_upload_id_marker()
            .expect("multipart id marker")
            .to_owned();
        pages.check(&format!("{key}/{id}"))?;
        key_marker = Some(key);
        upload_marker = Some(id);
    }
    let mut cleanup_result = Ok(());
    for (key, id) in uploads {
        assert!(key.starts_with(&prefix));
        let result = storage
            .client
            .abort_multipart_upload()
            .bucket(&storage.bucket)
            .key(key)
            .upload_id(id)
            .send()
            .await
            .map(|_| ())
            .map_err(s3_error);
        cleanup_result = crate::transfer::with_cleanup(cleanup_result, result);
    }
    let mut marker = None;
    let mut keys = Vec::new();
    let mut pages = Pagination::new();
    loop {
        // V1 is probed before writes, so cleanup does not depend on V2 support.
        let page = storage
            .list_page(profile::Listing::V1, &prefix, None, marker, None)
            .await?;
        for object in page.contents {
            keys.push(metadata::required(object.key(), "cleanup listing omitted key")?.to_owned());
        }
        let Some(next) = page.next else {
            break;
        };
        pages.check(&next)?;
        marker = Some(next);
    }
    for key in keys {
        assert!(key.starts_with(&prefix));
        let result = storage
            .client
            .delete_object()
            .bucket(&storage.bucket)
            .key(key)
            .send()
            .await
            .map(|_| ())
            .map_err(s3_error);
        cleanup_result = crate::transfer::with_cleanup(cleanup_result, result);
    }
    cleanup_result?;
    let objects = storage
        .list_page(profile::Listing::V1, &prefix, None, None, Some(1))
        .await?;
    assert!(objects.contents.is_empty());
    let uploads = storage
        .client
        .list_multipart_uploads()
        .bucket(&storage.bucket)
        .prefix(&prefix)
        .send()
        .await
        .map_err(s3_error)?;
    assert!(uploads.uploads().is_empty());
    Ok(())
}
