use crate::{ObjectPath, Storage, StorageError, StorageFailure};
use std::future::Future;

/// Most bytes a caller collects in memory for one request body, matching
/// [`crate::BUFFERED_READ_LIMIT`] for response bodies. A single request
/// carries its whole body, so no object goes in one request past this, even
/// on a provider accepting more: a large object goes one part at a time and
/// never has to fit in memory (§17.1).
pub const BUFFERED_REQUEST_LIMIT: u64 = 32 * 1024 * 1024;

/// Whether an object of `size` bytes goes up in parts rather than in one
/// request: past the provider's single-request limit or
/// [`BUFFERED_REQUEST_LIMIT`], whichever is less (§20.6). The 64-KiB
/// encryption chunk plays no part. Every upload routes by this alone: a
/// create of complete bytes, and a caller sending a repeatable source, which
/// reads it whole only for one request and otherwise records a session.
pub fn goes_in_parts(storage: &(impl Storage + ?Sized), size: u64) -> bool {
    size > storage.single_request_limit().min(BUFFERED_REQUEST_LIMIT)
}

/// Send complete bytes in the provider's single request or, when
/// [`goes_in_parts`] says so, through its session operations, telling `sent`
/// how far through them the parts reach. Callers needing crash continuation
/// use the session operations directly and persist each returned recording.
pub(crate) async fn upload_bytes(
    storage: &crate::StorageConnection<impl crate::ProviderOps + ?Sized>,
    path: &ObjectPath,
    bytes: &[u8],
    sent: &crate::ByteProgress,
    single_request: impl Future<Output = Result<Option<crate::StoredObject>, StorageError>> + Send,
) -> Result<Option<crate::StoredObject>, StorageError> {
    if !goes_in_parts(storage, bytes.len() as u64) {
        return single_request.await;
    }
    let mut session = storage
        .begin_upload_checked(&crate::UploadTarget::Create(path.clone()), bytes.len() as u64)
        .await?;
    let upload = async {
        while session.confirmed_bytes() < session.total_bytes() {
            let start = usize::try_from(session.confirmed_bytes())
                .map_err(|error| StorageFailure::InvalidPart.with_source(error))?;
            let remaining = bytes.get(start..).ok_or(StorageFailure::InvalidPart)?;
            let length = remaining.len().min(session.part_size());
            let permit = storage.part_permit(&mut session, &remaining[..length])?;
            storage.send_permitted(permit, &mut session, sent).await?;
            if session.confirmed_bytes() <= start as u64 {
                return Err(StorageFailure::Protocol.with_source("upload did not advance"));
            }
        }
        storage.finish_upload_checked(&mut session).await
    }
    .await;
    match upload {
        Ok(()) => Ok(None),
        Err(operation) => {
            with_cleanup(Err(operation), storage.abort_upload_checked(&session).await)
        }
    }
}

pub(crate) fn with_cleanup<T>(
    operation: Result<T, StorageError>,
    cleanup: Result<(), StorageError>,
) -> Result<T, StorageError> {
    match (operation, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(operation), Err(cleanup)) => Err(StorageError::Cleanup {
            operation: Box::new(operation),
            cleanup: Box::new(cleanup),
        }),
    }
}

pub(crate) fn check_single_request(size: u64, limit: u64) -> Result<(), StorageError> {
    if size > limit {
        return Err(StorageFailure::SingleRequestTooLarge { size, limit }.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "transfer_tests.rs"]
mod tests;
