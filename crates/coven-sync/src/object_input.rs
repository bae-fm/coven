//! Reuse the exact bytes of a durable observation across consumers and reopening.

use crate::SyncError;
use coven_database::{Database, RetainedInput};
use coven_storage::{ByteRange, ObjectPath, ObjectStream, Storage, StorageFailure, StoredObject};

#[path = "input_stream.rs"]
mod stream;

#[derive(Clone)]
pub(crate) struct ObjectInput {
    retained: RetainedInput,
    object: StoredObject,
}

fn identity(object: &StoredObject) -> Result<Vec<u8>, SyncError> {
    Ok(serde_json::to_vec(&(
        object.size,
        object.stored_at,
        object.path.is_replaceable().then_some(&object.revision),
    ))?)
}

impl ObjectInput {
    pub(crate) async fn files(&self) -> Result<Option<coven_database::FileEvidence>, SyncError> {
        Ok(self.retained.files().await?)
    }
    pub(crate) async fn keep_files(
        &self,
        files: coven_database::FileEvidence,
    ) -> Result<(), SyncError> {
        Ok(self.retained.keep_files(files).await?)
    }
    pub(crate) async fn keep_claim_bytes(&self) -> Result<(), SyncError> {
        Ok(self.retained.keep_claim_bytes().await?)
    }
    pub(crate) async fn release(self) -> Result<(), SyncError> {
        Ok(self.retained.release_ranges().await?)
    }
    pub(crate) async fn receive_prefix(
        database: &Database,
        path: &ObjectPath,
        response: coven_storage::ObjectRead,
    ) -> Result<Self, SyncError> {
        if response.metadata.path != *path || response.bytes.len() != 64 {
            return Err(StorageFailure::Protocol
                .with_source("snapshot prefix response differs from its request")
                .into());
        }
        let input = Self::observe(database, &response.metadata).await?;
        input.retained.keep_range(0, response.bytes).await?;
        Ok(input)
    }
    /// At most two header ranges. If the first buffer cannot expose
    /// every count/length field, extend to the format's maximum header boundary.
    /// Any body bytes in that extension remain in the shared retained input.
    pub(crate) async fn prefix(
        &self,
        storage: &dyn Storage,
        maximum: usize,
        exact_end: fn(&[u8]) -> Result<usize, coven_format::Error>,
    ) -> Result<Vec<u8>, SyncError> {
        let mut bytes = if self.object.size == 0 {
            Vec::new()
        } else {
            self.read(storage, ByteRange::new(0, self.object.size.min(64))?)
                .await?
        };
        let end = match exact_end(&bytes) {
            Ok(end) => end,
            Err(coven_format::Error::Truncated) => maximum,
            Err(error) => return crate::write_object::checked(&self.object.path, Err(error)),
        };
        let end = (end as u64).min(self.object.size);
        if end > bytes.len() as u64 {
            bytes.extend(
                self.read(storage, ByteRange::new(bytes.len() as u64, end)?)
                    .await?,
            );
        }
        let length = crate::write_object::checked(&self.object.path, exact_end(&bytes))?;
        if length > bytes.len() {
            return crate::write_object::checked(
                &self.object.path,
                Err(coven_format::Error::Truncated),
            );
        }
        bytes.truncate(length);
        Ok(bytes)
    }

    /// The observation whose bytes this input retains.
    pub(crate) fn object(&self) -> &StoredObject {
        &self.object
    }
    /// Read received bytes inside a database transaction without provider I/O.
    pub(crate) async fn local_bytes(&self) -> Result<coven_database::RetainedBytes, SyncError> {
        Ok(self.retained.local_bytes(self.object.size).await?)
    }
    /// Retain a whole-object GET's response under the observation it
    /// describes: a next-number hit is the object itself (§6.1). Bytes this
    /// observation already retains are used first, and the response then
    /// supplies none of them.
    pub(crate) async fn receive_response(
        database: &Database,
        storage: &dyn Storage,
        path: &ObjectPath,
        response: ObjectStream<'_>,
    ) -> Result<Self, SyncError> {
        if &response.metadata().path != path {
            return Err(StorageFailure::Protocol
                .with_source("stream names another path")
                .into());
        }
        let input = Self::observe(database, response.metadata()).await?;
        let mut stream = input.stream(storage).await;
        if !input.retains_bytes().await? {
            stream.start_with(response);
        } else {
            // The missing suffix needs its own response and download capacity.
            drop(response);
        }
        stream.drain().await?;
        drop(stream);
        Ok(input)
    }

    /// The observation already retained at `path`, if any, with whatever of
    /// its bytes are retained.
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't convert or decode is DamagedDatabase"
    )]
    pub(crate) async fn retained(
        database: &Database,
        path: &ObjectPath,
    ) -> Result<Option<Self>, SyncError> {
        let Some(observation) = database
            .retained_observation(path.as_str().to_owned())
            .await?
        else {
            return Ok(None);
        };
        let object: StoredObject = serde_json::from_slice(&observation)
            .map_err(|_| coven_database::DbError::DamagedDatabase)?;
        Self::observe(database, &object).await.map(Some)
    }

    /// Checked while holding this observation's receiving guard.
    async fn retains_bytes(&self) -> Result<bool, SyncError> {
        Ok(self.received().await? || !self.retained.ranges(0, self.object.size).await?.is_empty())
    }

    /// Every byte of the object was received and is still retained, so
    /// reading it needs no request.
    pub(crate) async fn held(&self) -> Result<bool, SyncError> {
        Ok(self.received().await? && self.retained.retained_length().await? == self.object.size)
    }

    pub(crate) async fn received(&self) -> Result<bool, SyncError> {
        Ok(self.retained.received().await?)
    }
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't convert or decode is DamagedDatabase"
    )]
    pub(crate) async fn observe(
        database: &Database,
        object: &StoredObject,
    ) -> Result<Self, SyncError> {
        let retained = database
            .observe_input(
                object.path.as_str().to_owned(),
                identity(object)?,
                serde_json::to_vec(object)?,
                object.path.is_replaceable(),
            )
            .await?
            .ok_or_else(|| {
                StorageFailure::Protocol
                    .with_source("immutable object size or publication time changed")
            })?;
        let object = serde_json::from_slice(&retained.observation().await?)
            .map_err(|_| coven_database::DbError::DamagedDatabase)?;
        Ok(Self { retained, object })
    }

    /// Fetch only gaps between retained ranges. Each completed response commits
    /// before its bytes are given to a parser, including a parser that must wait.
    pub(crate) async fn read(
        &self,
        storage: &dyn Storage,
        range: ByteRange,
    ) -> Result<Vec<u8>, SyncError> {
        let _receiving = self.retained.receiving().await;
        if range.end() > self.object.size {
            return Err(StorageFailure::InvalidRange.into());
        }
        let mut bytes = Vec::new();
        let mut offset = range.start();
        for cached in self.retained.ranges(range.start(), range.end()).await? {
            if cached.offset > offset {
                bytes.extend(
                    self.fetch(storage, ByteRange::new(offset, cached.offset)?)
                        .await?,
                );
            }
            offset = cached.offset + cached.bytes.len() as u64;
            bytes.extend(cached.bytes);
        }
        if offset < range.end() {
            bytes.extend(
                self.fetch(storage, ByteRange::new(offset, range.end())?)
                    .await?,
            );
        }
        Ok(bytes)
    }

    /// One stream of the object's bytes from `start` to its end, of the same
    /// observation, for a reader that keeps none of them (§16.2).
    pub(crate) async fn stream_from<'a>(
        &self,
        storage: &'a dyn Storage,
        start: u64,
    ) -> Result<ObjectStream<'a>, SyncError> {
        let stream = storage
            .stream_object(
                &self.object.path,
                Some(ByteRange::new(start, self.object.size)?),
            )
            .await?;
        if stream.metadata().path != self.object.path
            || identity(stream.metadata())? != identity(&self.object)?
        {
            return Err(StorageFailure::Protocol
                .with_source("stream differs from the retained observation")
                .into());
        }
        Ok(stream)
    }

    async fn fetch(&self, storage: &dyn Storage, range: ByteRange) -> Result<Vec<u8>, SyncError> {
        let read = storage.read_object(&self.object.path, Some(range)).await?;
        if read.metadata.path != self.object.path
            || identity(&read.metadata)? != identity(&self.object)?
        {
            return Err(StorageFailure::Protocol
                .with_source("read differs from the retained object observation")
                .into());
        }
        if read.bytes.len() as u64 != range.len() {
            return Err(StorageFailure::Protocol
                .with_source("range response has the wrong length")
                .into());
        }
        self.retained
            .keep_range(range.start(), read.bytes.clone())
            .await?;
        Ok(read.bytes)
    }
}

#[cfg(test)]
#[path = "object_input_tests.rs"]
pub(crate) mod tests;
