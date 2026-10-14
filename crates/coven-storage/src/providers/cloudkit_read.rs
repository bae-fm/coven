//! One CloudKit asset part at a time; native fetches remain physical transfers.

use super::*;
use std::io::{Cursor, Read};

enum Body {
    Inline(Vec<u8>),
    Parts(String),
}

#[async_trait::async_trait]
trait AssetParts: Send + Sync {
    async fn part(&self, root: &str, start: u64) -> Result<Vec<u8>, StorageError>;
}

#[async_trait::async_trait]
impl AssetParts for CloudKitStorage {
    async fn part(&self, root: &str, start: u64) -> Result<Vec<u8>, StorageError> {
        let name = record::part_name(root, start);
        let files = self.files.clone();
        native::fetch(
            &self.database,
            &self.record_id(&name),
            &["data"],
            move |record| record.asset_bytes(&files),
        )
        .wait()
        .await
    }
}

struct CloudKitBody<'a> {
    storage: &'a dyn AssetParts,
    root: String,
    size: u64,
    offset: u64,
    end: u64,
    part: Cursor<Vec<u8>>,
}

#[async_trait::async_trait]
impl ObjectBody for CloudKitBody<'_> {
    async fn read(&mut self, output: &mut [u8]) -> Result<usize, StorageError> {
        if output.is_empty() || self.offset == self.end {
            return Ok(0);
        }
        if self.part.position() == self.part.get_ref().len() as u64 {
            let part_start = self.offset / PART_SIZE as u64 * PART_SIZE as u64;
            let bytes = self.storage.part(&self.root, part_start).await?;
            let expected = (self.size - part_start).min(PART_SIZE as u64);
            if bytes.len() as u64 != expected {
                return Err(
                    StorageFailure::Protocol.with_source("CloudKit returned an incomplete asset")
                );
            }
            self.part = Cursor::new(bytes);
            self.part.set_position(self.offset - part_start);
        }
        let length = output
            .len()
            .min((self.end - self.offset).min(usize::MAX as u64) as usize);
        let read =
            Read::read(&mut self.part, &mut output[..length]).expect("read from a memory cursor");
        self.offset += read as u64;
        Ok(read)
    }
}

impl CloudKitStorage {
    pub(super) async fn read_stream(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectStream<'_>, StorageError> {
        let files = self.files.clone();
        let request = native::fetch(
            &self.database,
            &self.record_id(path.as_str()),
            &["path", "size", "upload", "data"],
            move |record| {
                let metadata = record.metadata()?;
                let body = match record.optional_text("upload")? {
                    Some(token) => Body::Parts(token),
                    None => Body::Inline(record.asset_bytes(&files)?),
                };
                Ok((metadata, body))
            },
        );
        let (metadata, body) = request.wait().await?;
        metadata.read_length(path, range)?;
        let (start, end) = range.map_or((0, metadata.size), |range| (range.start(), range.end()));
        match body {
            Body::Inline(mut bytes) => {
                if bytes.len() as u64 != metadata.size {
                    return Err(StorageFailure::Protocol.into());
                }
                bytes.truncate(end as usize);
                let mut body = Cursor::new(bytes);
                body.set_position(start);
                ObjectStream::new(metadata, range, body)
            }
            Body::Parts(token) => {
                let body = CloudKitBody {
                    storage: self,
                    root: record::upload_name(path, &token)?,
                    size: metadata.size,
                    offset: start,
                    end,
                    part: Cursor::new(Vec::new()),
                };
                ObjectStream::new(metadata, range, body)
            }
        }
    }
}

#[cfg(test)]
#[path = "cloudkit_read_tests.rs"]
mod tests;
