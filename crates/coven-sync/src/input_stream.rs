//! Read retained input and its missing suffix through one bounded body response.

use super::{identity, ObjectInput};
use crate::SyncError;
use coven_storage::{ByteRange, ObjectStream, Storage, StorageFailure, StoredObject};
use std::io::{Cursor, Read};

const BUFFER: usize = coven_format::chunks::CHUNK_SIZE;

pub(crate) struct InputStream<'a> {
    input: &'a ObjectInput,
    storage: &'a dyn Storage,
    _receiving: tokio::sync::MutexGuard<'a, ()>,
    response: Option<ObjectStream<'a>>,
    chunk: Cursor<Vec<u8>>,
    position: u64,
    complete: bool,
}

impl ObjectInput {
    pub(crate) async fn stream<'a>(&'a self, storage: &'a dyn Storage) -> InputStream<'a> {
        InputStream {
            input: self,
            storage,
            _receiving: self.retained.receiving().await,
            response: None,
            chunk: Cursor::new(Vec::new()),
            position: 0,
            complete: false,
        }
    }
}

impl InputStream<'_> {
    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    pub(crate) fn object(&self) -> &StoredObject {
        &self.input.object
    }

    pub(crate) async fn keep_metadata(&mut self, bytes: Vec<u8>) -> Result<(), SyncError> {
        self.input.keep_metadata(bytes).await
    }

    pub(crate) async fn read_exact(&mut self, length: usize) -> Result<Vec<u8>, SyncError> {
        if length as u64 > self.input.object.size - self.position {
            return crate::write_object::checked(
                &self.input.object.path,
                Err(coven_format::Error::Truncated),
            );
        }
        let mut bytes = vec![0; length];
        let mut filled = 0;
        while filled < length {
            let read = self
                .chunk
                .read(&mut bytes[filled..])
                .expect("read from a memory cursor");
            if read == 0 {
                self.chunk = Cursor::new(self.next_chunk().await?);
            } else {
                self.position += read as u64;
                filled += read;
            }
        }
        Ok(bytes)
    }

    async fn next_chunk(&mut self) -> Result<Vec<u8>, SyncError> {
        let object = &self.input.object;
        let end = object.size.min(self.position.saturating_add(BUFFER as u64));
        if self.response.is_none() {
            let ranges = self.input.retained.ranges(self.position, end).await?;
            let mut cached = Vec::new();
            for range in ranges {
                if range.offset != self.position + cached.len() as u64 {
                    break;
                }
                cached.extend(range.bytes);
            }
            if !cached.is_empty() {
                return Ok(cached);
            }
            if object.size == 0 && self.input.retained.received().await? {
                self.complete = true;
                return Ok(Vec::new());
            }
            // Released inputs are evicted as a whole. Receiving leaves a
            // contiguous prefix, never cached bytes beyond the missing suffix.
            let range = if self.position == 0 {
                None
            } else {
                Some(ByteRange::new(self.position, object.size)?)
            };
            let stream = self.storage.stream_object(&object.path, range).await?;
            if stream.metadata().path != object.path
                || identity(stream.metadata())? != identity(object)?
            {
                return Err(StorageFailure::Protocol
                    .with_source("stream differs from the retained observation")
                    .into());
            }
            self.response = Some(stream);
        }
        let response = self.response.as_mut().expect("opened response");
        let mut bytes = vec![0; (end - self.position) as usize];
        let mut filled = 0;
        while filled < bytes.len() {
            match response.read(&mut bytes[filled..]).await {
                Ok(0) => {
                    return Err(StorageFailure::Protocol
                        .with_source("stream ended before the object")
                        .into())
                }
                Ok(length) => filled += length,
                Err(error) => {
                    // Preserve the successfully received prefix. The last data
                    // chunk is committed only together with a successful EOF.
                    bytes.truncate(filled);
                    let operation = error.into();
                    return match self.input.retained.keep_range(self.position, bytes).await {
                        Ok(()) => Err(operation),
                        Err(error) => Err(SyncError::Cleanup {
                            operation: Box::new(operation),
                            cleanup: Box::new(error.into()),
                        }),
                    };
                }
            }
        }
        let complete = end == object.size;
        if complete && response.read(&mut [0]).await? != 0 {
            return Err(StorageFailure::Protocol
                .with_source("stream has extra bytes")
                .into());
        }
        self.input
            .retained
            .keep_received_range(self.position, bytes.clone(), complete)
            .await?;
        self.complete = complete;
        Ok(bytes)
    }

    pub(crate) async fn drain(&mut self) -> Result<(), SyncError> {
        while self.position < self.input.object.size {
            let length = (self.input.object.size - self.position).min(BUFFER as u64) as usize;
            self.read_exact(length).await?;
        }
        if !self.complete {
            if self.input.object.size == 0 {
                self.next_chunk().await?;
            } else {
                self.input
                    .retained
                    .keep_received_range(self.position, Vec::new(), true)
                    .await?;
            }
            self.complete = true;
        }
        Ok(())
    }

    /// Complete an already opened response before classifying object contents.
    /// Transport and local I/O failures remain retryable and are never drained
    /// through a replacement response.
    pub(crate) async fn checked<T>(
        &mut self,
        result: Result<T, SyncError>,
    ) -> Result<T, SyncError> {
        if matches!(
            &result,
            Err(SyncError::Damaged(_)
                | SyncError::Crypto(_)
                | SyncError::Format(_)
                | SyncError::KeyUnavailable(_)
                | SyncError::UpdateRequired(_))
        ) {
            self.drain().await?;
        }
        result
    }
}

#[cfg(test)]
#[path = "input_stream_tests.rs"]
mod tests;
