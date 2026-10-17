//! Exact reads of D10's unsealed bucket ownership marker.
use super::*;
use crate::bucket_claim::{occupied, BUCKET_CLAIM, BUCKET_CLAIM_SIZE};
use tokio::io::AsyncReadExt;

impl S3Storage {
    /// Read the fixed root object, without listing or accepting a prefix of
    /// its bytes. Bounded reading also refuses an oversized external marker.
    pub(super) async fn bucket_claim_bytes(&self) -> Result<Vec<u8>, StorageError> {
        let mut request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(BUCKET_CLAIM);
        if self.settings.request_checksums {
            request = request.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);
        }
        let response = request.send().await.map_err(s3_error)?;
        let size = response
            .content_length()
            .ok_or(StorageFailure::Protocol.with_source("S3 omitted bucket marker length"))?;
        let size =
            usize::try_from(size).map_err(|error| StorageFailure::Protocol.with_source(error))?;
        if size != BUCKET_CLAIM_SIZE {
            return Err(occupied());
        }
        let mut bytes = Vec::with_capacity(BUCKET_CLAIM_SIZE);
        response
            .body
            .into_async_read()
            .take((BUCKET_CLAIM_SIZE + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(|source| StorageError::Provider {
                provider: CloudProvider::S3,
                failure: StorageFailure::Network,
                source: Box::new(source),
            })?;
        Ok(bytes)
    }
}
