//! Exact reads of D10's unsealed location marker at the bucket's root.
use super::*;
use crate::location_claim::{check_marker_size, LOCATION_MARKER, LOCATION_MARKER_SIZE};
use tokio::io::AsyncReadExt;

impl S3Storage {
    /// Read the fixed root object, without listing or accepting a prefix of
    /// its bytes. Bounded reading also refuses an oversized external marker.
    pub(super) async fn location_marker_bytes(&self) -> Result<Vec<u8>, StorageError> {
        let mut request = self.client.get_object().bucket(&self.bucket).key(LOCATION_MARKER);
        if self.settings.request_checksums {
            request = request.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);
        }
        let response = request.send().await.map_err(s3_error)?;
        let size = response
            .content_length()
            .ok_or(StorageFailure::Protocol.with_source("S3 omitted location marker length"))?;
        let size =
            u64::try_from(size).map_err(|error| StorageFailure::Protocol.with_source(error))?;
        check_marker_size(size)?;
        let mut bytes = Vec::with_capacity(LOCATION_MARKER_SIZE);
        response
            .body
            .into_async_read()
            .take((LOCATION_MARKER_SIZE + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(super::read::body_read_error)?;
        Ok(bytes)
    }
}
