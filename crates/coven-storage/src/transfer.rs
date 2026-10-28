use crate::{ObjectPath, Storage, StorageError, StorageFailure};
use std::future::Future;

/// Most bytes a caller collects in memory for one request body, matching
/// [`crate::BUFFERED_READ_LIMIT`] for response bodies. A single request
/// carries its whole body, so an object assembled from a bounded source — a
/// file read and checked chunk by chunk, a write sealed piece by piece — goes
/// in one request only up to this, and past it through a recorded session one
/// part at a time: a large object never has to fit in memory (§17.1).
pub const BUFFERED_REQUEST_LIMIT: u64 = 32 * 1024 * 1024;

/// The largest assembled object sent in one request: the provider's
/// single-request limit, within [`BUFFERED_REQUEST_LIMIT`]. The 64-KiB
/// encryption chunk plays no part; a larger object needs a session (§17.7).
pub fn buffered_request_limit(storage: &(impl Storage + ?Sized)) -> u64 {
    storage.single_request_limit().min(BUFFERED_REQUEST_LIMIT)
}

/// Route complete bytes through the provider's single-request call or its
/// recorded-session operations, telling `sent` how far through them the
/// parts reach. Callers needing crash continuation use the session
/// operations directly and persist each returned recording.
pub(crate) async fn upload_bytes(
    storage: &crate::StorageConnection<impl crate::ProviderOps + ?Sized>,
    path: &ObjectPath,
    bytes: &[u8],
    sent: &crate::SendProgress,
    single_request: impl Future<Output = Result<Option<crate::StoredObject>, StorageError>> + Send,
) -> Result<Option<crate::StoredObject>, StorageError> {
    if bytes.len() as u64 <= storage.single_request_limit() {
        return single_request.await;
    }
    let mut session = storage
        .begin_upload_checked(
            &crate::UploadTarget::Create(path.clone()),
            bytes.len() as u64,
        )
        .await?;
    let upload = async {
        while session.confirmed_bytes() < session.total_bytes() {
            let start = usize::try_from(session.confirmed_bytes())
                .map_err(|error| StorageFailure::InvalidPart.with_source(error))?;
            let remaining = bytes.get(start..).ok_or(StorageFailure::InvalidPart)?;
            let length = remaining.len().min(session.part_size());
            storage
                .upload_part_checked(&mut session, &remaining[..length], sent)
                .await?;
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
