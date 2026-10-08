//! Exact ranged responses; callers retain their object-specific failure classification.

use coven_storage::{ByteRange, ObjectPath, Storage, StorageError};

pub(crate) enum ReadError {
    Storage(StorageError),
    Length,
}

pub(crate) async fn read(
    storage: &dyn Storage,
    path: &ObjectPath,
    range: ByteRange,
) -> Result<Vec<u8>, ReadError> {
    let bytes = storage
        .read_range(path, range)
        .await
        .map_err(ReadError::Storage)?;
    if bytes.len() as u64 != range.len() {
        return Err(ReadError::Length);
    }
    Ok(bytes)
}
