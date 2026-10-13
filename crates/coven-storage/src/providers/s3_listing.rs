//! One page representation for the two S3 listing protocols.
use super::*;
use profile::Listing;

pub(super) struct Page {
    pub(super) contents: Vec<aws_sdk_s3::types::Object>,
    pub(super) folders: Vec<aws_sdk_s3::types::CommonPrefix>,
    pub(super) next: Option<String>,
}
impl S3Storage {
    pub(super) async fn list_page(
        &self,
        protocol: Listing,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<String>,
        max_keys: Option<i32>,
    ) -> Result<Page, StorageError> {
        let (contents, folders, truncated, next) = if protocol == Listing::V1 {
            let page = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_delimiter(delimiter.map(str::to_owned))
                .set_marker(marker)
                .set_max_keys(max_keys)
                .send()
                .await
                .map_err(s3_error)?;
            // V1 only promises NextMarker with a delimiter. Otherwise the
            // final object key is the specified continuation marker.
            let next = if delimiter.is_none() && page.is_truncated == Some(true) {
                page.contents()
                    .last()
                    .and_then(|object| object.key())
                    .map(str::to_owned)
            } else {
                page.next_marker
            };
            (page.contents, page.common_prefixes, page.is_truncated, next)
        } else {
            let page = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_delimiter(delimiter.map(str::to_owned))
                .set_continuation_token(marker)
                .set_max_keys(max_keys)
                .send()
                .await
                .map_err(s3_error)?;
            (
                page.contents,
                page.common_prefixes,
                page.is_truncated,
                page.next_continuation_token,
            )
        };
        let next = if truncated
            .ok_or(StorageFailure::Protocol.with_source("S3 omitted page completion"))?
        {
            Some(
                next.filter(|value| !value.is_empty())
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted continuation token"))?,
            )
        } else {
            None
        };
        Ok(Page {
            contents: contents.unwrap_or_default(),
            folders: folders.unwrap_or_default(),
            next,
        })
    }
}
