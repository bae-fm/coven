#![cfg(test)]

//! Cleanup of the certification namespace, including retained versions.
use super::*;
use coven_foundation::id_source::StoreId;

/// Use native listings so completed temporaries and incomplete uploads are both
/// removed, including after an assertion or a provider operation fails.
pub(super) async fn cleanup(storage: &S3Storage, store: StoreId) -> Result<(), StorageError> {
    let prefix = storage.prefix.key_prefix_of(&ObjectPrefix::all(store));
    // V1 is probed before writes, so cleanup does not depend on V2 support.
    storage.empty_under(&prefix, S3Listing::V1).await?;
    match storage.bucket_versioning().await? {
        S3BucketVersioning::Enabled | S3BucketVersioning::Suspended => {
            cleanup_versions(storage, &prefix).await
        }
        S3BucketVersioning::Disabled | S3BucketVersioning::Unavailable => Ok(()),
    }
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
            .chain(page.delete_markers().iter().map(|v| (v.key(), v.version_id())))
        {
            let key = metadata::required(key, "version listing omitted key")?;
            empty::within(key, prefix)?;
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
                request.headers_mut().insert("x-goog-interop-list-objects-format", "enabled");
            }
        })
        .send()
        .await
        .map_err(s3_error)
}

#[cfg(test)]
#[path = "s3_live_cleanup_tests.rs"]
mod tests;
