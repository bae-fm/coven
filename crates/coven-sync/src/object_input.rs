//! Reuse the exact bytes of a durable observation across consumers and reopening.

use crate::SyncError;
use coven_database::{Database, RetainedInput};
use coven_storage::{ByteRange, Storage, StorageFailure, StoredObject};

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
    pub(crate) async fn metadata(&self) -> Result<Option<Vec<u8>>, SyncError> {
        Ok(self.retained.metadata().await?)
    }
    pub(crate) async fn keep_metadata(&self, bytes: Vec<u8>) -> Result<(), SyncError> {
        Ok(self.retained.keep_metadata(bytes).await?)
    }
    pub(crate) async fn files(
        &self,
    ) -> Result<Option<Option<crate::pass_reads::FileReferences>>, SyncError> {
        Ok(self.retained.files().await?)
    }
    pub(crate) async fn keep_files(
        &self,
        files: Option<crate::pass_reads::FileReferences>,
    ) -> Result<(), SyncError> {
        Ok(self.retained.keep_files(files).await?)
    }
    pub(crate) async fn release_ranges(self) -> Result<(), SyncError> {
        Ok(self.retained.release_ranges().await?)
    }
    pub(crate) async fn whole(&self, storage: &dyn Storage) -> Result<Vec<u8>, SyncError> {
        // An observed zero-length version has no byte ranges to fetch.
        if self.object.size == 0 {
            return Ok(Vec::new());
        }
        self.read(storage, ByteRange::new(0, self.object.size)?)
            .await
    }
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
mod tests;
