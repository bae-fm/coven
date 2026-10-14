//! A provider response consumed incrementally, with an explicit checked EOF.

use crate::{ByteRange, ObjectPath, ObjectRead, StorageError, StorageFailure, StoredObject};

/// Maximum collected response; larger objects use the incremental reader.
pub const BUFFERED_READ_LIMIT: u64 = 32 * 1024 * 1024;

/// A native response body. Reads never exceed the supplied buffer; zero means
/// transport completed and any replacement-revision check also succeeded.
#[async_trait::async_trait]
pub trait ObjectBody: Send {
    /// Receive the next bytes of this response, without starting another body request.
    async fn read(&mut self, output: &mut [u8]) -> Result<usize, StorageError>;
}

/// Metadata and the single body response it describes. A consumer must read
/// through EOF before treating a complete object as received. Dropping this
/// value cancels the response and establishes no completion verdict.
pub struct ObjectStream<'a> {
    metadata: StoredObject,
    body: Box<dyn ObjectBody + 'a>,
    state: StreamState,
}

enum StreamState {
    Receiving(u64),
    Complete,
    Failed,
}

impl<'a> ObjectStream<'a> {
    /// Observation for the complete object, including on a ranged response.
    pub fn metadata(&self) -> &StoredObject {
        &self.metadata
    }

    /// Wrap a native response, bounding its exact body length by the observation.
    pub fn new(
        metadata: StoredObject,
        range: Option<ByteRange>,
        body: impl ObjectBody + 'a,
    ) -> Result<Self, StorageError> {
        let remaining = metadata.read_length(&metadata.path, range)?;
        Ok(Self {
            metadata,
            body: Box::new(body),
            state: StreamState::Receiving(remaining),
        })
    }

    pub(crate) fn check(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<(), StorageError> {
        let expected = self.metadata.read_length(path, range)?;
        match self.state {
            StreamState::Receiving(remaining) if remaining == expected => Ok(()),
            _ => Err(StorageFailure::Protocol
                .with_source("provider returned an already consumed response")),
        }
    }

    /// Receive bounded bytes and check transport completion and exact framing.
    pub async fn read(&mut self, output: &mut [u8]) -> Result<usize, StorageError> {
        if output.is_empty() {
            return Ok(0);
        }
        let remaining = match self.state {
            StreamState::Receiving(remaining) => remaining,
            StreamState::Complete => return Ok(0),
            StreamState::Failed => {
                return Err(StorageFailure::Protocol.with_source("response already failed"))
            }
        };
        self.state = StreamState::Failed;
        let count = self.body.read(output).await?;
        if count > output.len() || count as u64 > remaining {
            return Err(
                StorageFailure::Protocol.with_source("response exceeds its declared length")
            );
        }
        if count == 0 {
            if remaining != 0 {
                return Err(StorageFailure::Protocol
                    .with_source("response ended before its declared length"));
            }
            self.state = StreamState::Complete;
        } else {
            self.state = StreamState::Receiving(remaining - count as u64);
        }
        Ok(count)
    }

    /// Collect only a caller-bounded response. Objects with streamed plaintext
    /// use `read` instead; the bound is checked before allocating their body.
    pub async fn collect(mut self, maximum: u64) -> Result<ObjectRead, StorageError> {
        let remaining = match self.state {
            StreamState::Receiving(remaining) => remaining,
            StreamState::Complete => 0,
            StreamState::Failed => {
                return Err(StorageFailure::Protocol.with_source("response already failed"))
            }
        };
        if remaining > maximum {
            return Err(StorageFailure::Protocol
                .with_source("object exceeds the consumer's buffered-read bound"));
        }
        let capacity = usize::try_from(remaining)
            .map_err(|error| StorageFailure::Protocol.with_source(error))?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut chunk = [0; 64 * 1024];
        loop {
            let length = self.read(&mut chunk).await?;
            if length == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..length]);
        }
        Ok(ObjectRead {
            metadata: self.metadata,
            bytes,
        })
    }
}

struct CompletingBody<'a, F> {
    body: Box<dyn ObjectBody + 'a>,
    completion: Option<F>,
}

#[async_trait::async_trait]
impl<F: std::future::Future<Output = Result<(), StorageError>> + Send> ObjectBody
    for CompletingBody<'_, F>
{
    async fn read(&mut self, output: &mut [u8]) -> Result<usize, StorageError> {
        let length = self.body.read(output).await?;
        if length == 0 {
            if let Some(completion) = self.completion.take() {
                completion.await?;
            }
        }
        Ok(length)
    }
}

impl<'a> ObjectStream<'a> {
    /// Require a provider check after EOF, before completion is observable.
    pub(crate) fn after(
        mut self,
        completion: impl std::future::Future<Output = Result<(), StorageError>> + Send + 'a,
    ) -> Self {
        self.body = Box::new(CompletingBody {
            body: self.body,
            completion: Some(completion),
        });
        self
    }
}

#[async_trait::async_trait]
impl<T: AsRef<[u8]> + Send> ObjectBody for std::io::Cursor<T> {
    async fn read(&mut self, output: &mut [u8]) -> Result<usize, StorageError> {
        Ok(std::io::Read::read(self, output).expect("read from a memory cursor"))
    }
}

#[cfg(test)]
#[path = "object_stream_tests.rs"]
mod tests;
