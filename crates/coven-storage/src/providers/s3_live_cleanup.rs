#![cfg(test)]

//! Cleanup of the certification namespace, including retained versions.
use super::*;
use coven_foundation::id_source::StoreId;

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
            .list_page(S3Listing::V1, &prefix, None, marker, None)
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
    match storage.bucket_versioning().await {
        Ok(S3BucketVersioning::Enabled | S3BucketVersioning::Suspended) => {
            cleanup_versions(storage, &prefix).await?
        }
        Ok(_) => {}
        Err(error) => return Err(error),
    }
    let objects = storage
        .list_page(S3Listing::V1, &prefix, None, None, Some(1))
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

/// Versioned cleanup must remove both historical bytes and deletion markers.
/// List the entire namespace before deleting so pagination markers stay valid.
pub(super) async fn cleanup_versions(
    storage: &S3Storage,
    prefix: &str,
) -> Result<(), StorageError> {
    let mut marker = None;
    let mut version_marker = None;
    let mut pages = Pagination::new();
    let mut versions = Vec::new();
    loop {
        let page = version_page(storage, prefix, marker, version_marker, None).await?;
        for (key, version) in page
            .versions()
            .iter()
            .map(|v| (v.key(), v.version_id()))
            .chain(
                page.delete_markers()
                    .iter()
                    .map(|v| (v.key(), v.version_id())),
            )
        {
            let key = metadata::required(key, "version listing omitted key")?;
            require_prefix(key, prefix)?;
            versions.push((
                key.to_owned(),
                metadata::required(version, "version listing omitted version")?.to_owned(),
            ));
        }
        if !page.is_truncated().ok_or(StorageFailure::Protocol)? {
            break;
        }
        let key =
            metadata::required(page.next_key_marker(), "missing version key marker")?.to_owned();
        let version =
            metadata::required(page.next_version_id_marker(), "missing version marker")?.to_owned();
        pages.check(&format!("{key}/{version}"))?;
        marker = Some(key);
        version_marker = Some(version);
    }
    let mut result = Ok(());
    for (key, version) in versions {
        let deleted = storage
            .client
            .delete_object()
            .bucket(&storage.bucket)
            .key(key)
            .version_id(version)
            .send()
            .await
            .map(|_| ())
            .map_err(s3_error);
        result = crate::transfer::with_cleanup(result, deleted);
    }
    result?;
    let remaining = version_page(storage, prefix, None, None, Some(1)).await?;
    if !remaining.versions().is_empty() || !remaining.delete_markers().is_empty() {
        return Err(
            StorageFailure::Protocol.with_source("certification left object versions behind")
        );
    }
    Ok(())
}

async fn version_page(
    storage: &S3Storage,
    prefix: &str,
    marker: Option<String>,
    version_marker: Option<String>,
    limit: Option<i32>,
) -> Result<aws_sdk_s3::operation::list_object_versions::ListObjectVersionsOutput, StorageError> {
    let google = storage.settings.response_headers == S3ResponseHeaders::Google;
    storage
        .client
        .list_object_versions()
        .bucket(&storage.bucket)
        .prefix(prefix)
        .set_key_marker(marker)
        .set_version_id_marker(version_marker)
        .set_max_keys(limit)
        .customize()
        .mutate_request(move |request| {
            if google {
                request
                    .headers_mut()
                    .insert("x-goog-interop-list-objects-format", "enabled");
            }
        })
        .send()
        .await
        .map_err(s3_error)
}

fn require_prefix(key: &str, prefix: &str) -> Result<(), StorageError> {
    if !key.starts_with(prefix) {
        return Err(
            StorageFailure::Protocol.with_source("version listing escaped certification prefix")
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "s3_live_cleanup_tests.rs"]
mod tests;
