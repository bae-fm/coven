//! Incremental HTTP bodies. Provider metadata checks run at EOF when required.

use super::http::{checked, transport, validate_content_range};
use crate::{
    ByteRange, CloudProvider, ObjectBody, ObjectStream, StorageError, StorageFailure, StoredObject,
};
use std::io::{Cursor, Read};

struct HttpBody {
    provider: CloudProvider,
    response: reqwest::Response,
    chunk: Cursor<Vec<u8>>,
}

#[async_trait::async_trait]
impl ObjectBody for HttpBody {
    async fn read(&mut self, output: &mut [u8]) -> Result<usize, StorageError> {
        loop {
            let length = Read::read(&mut self.chunk, output).expect("read from a memory cursor");
            if length != 0 || output.is_empty() {
                return Ok(length);
            }
            match self.response.chunk().await.map_err(|error| transport(self.provider, error))? {
                Some(chunk) => self.chunk = Cursor::new(chunk.to_vec()),
                None => return Ok(0),
            }
        }
    }
}

pub(super) async fn stream(
    provider: CloudProvider,
    response: reqwest::Response,
    metadata: StoredObject,
    range: Option<ByteRange>,
) -> Result<ObjectStream<'static>, StorageError> {
    let response = checked(provider, response).await?;
    if let Some(range) = range {
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(StorageFailure::Protocol.with_source("provider ignored byte range"));
        }
        let header = response
            .headers()
            .get("content-range")
            .ok_or_else(|| StorageFailure::Protocol.with_source("missing Content-Range"))?
            .to_str()
            .map_err(|error| StorageFailure::Protocol.with_source(error))?;
        if validate_content_range(header, range)? != metadata.size {
            return Err(
                StorageFailure::Protocol.with_source("Content-Range differs from object metadata")
            );
        }
    }
    ObjectStream::new(
        metadata,
        range,
        HttpBody { provider, response, chunk: Cursor::new(Vec::new()) },
    )
}

#[cfg(test)]
#[path = "http_body_tests.rs"]
mod tests;
