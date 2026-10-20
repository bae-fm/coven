//! Incremental S3 response bodies, including SDK checksum validation at EOF.

use super::*;
use tokio::io::AsyncReadExt;

struct S3Body(std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>);

#[async_trait]
impl ObjectBody for S3Body {
    async fn read(&mut self, output: &mut [u8]) -> Result<usize, StorageError> {
        self.0
            .read(output)
            .await
            .map_err(|source| StorageError::Provider {
                provider: CloudProvider::S3,
                failure: StorageFailure::Network,
                source: Box::new(crate::providers::error_text::WithCauses(source)),
            })
    }
}

impl S3Storage {
    pub(super) async fn get(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectStream<'static>, StorageError> {
        let mut request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path.under(&self.prefix));
        if let Some(range) = range {
            request = request.range(range.header());
        }
        if self.settings.request_checksums {
            request = request.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);
        }
        let response = request.send().await.map_err(s3_error)?;
        let length = response
            .content_length()
            .ok_or(StorageFailure::Protocol.with_source("S3 omitted content length"))?;
        let complete_size = match range {
            Some(range) => crate::providers::http::validate_content_range(
                response.content_range().ok_or(StorageFailure::Protocol)?,
                range,
            )?,
            None => u64::try_from(length).map_err(|e| StorageFailure::Protocol.with_source(e))?,
        };
        let metadata = metadata::get(self, path, &response, complete_size)?;
        let length =
            u64::try_from(length).map_err(|error| StorageFailure::Protocol.with_source(error))?;
        if length != range.map_or(complete_size, ByteRange::len) {
            return Err(
                StorageFailure::Protocol.with_source("S3 content length disagrees with its range")
            );
        }
        ObjectStream::new(
            metadata,
            range,
            S3Body(Box::pin(response.body.into_async_read())),
        )
    }
}

#[cfg(test)]
#[path = "s3_read_tests.rs"]
mod tests;
