use crate::{ObjectPath, Storage, StorageError, StorageFailure};
use std::future::Future;

/// Route complete bytes through the provider's single-request call or its
/// recorded-session operations. Callers needing crash continuation use the
/// session operations directly and persist each returned recording.
pub(crate) async fn upload_bytes(
    storage: &crate::StorageConnection<impl crate::ProviderOps + ?Sized>,
    path: &ObjectPath,
    bytes: &[u8],
    single_request: impl Future<Output = Result<(), StorageError>> + Send,
) -> Result<(), StorageError> {
    if path.store_log_position().is_some() {
        // Finality relies on completion-time metadata from one-request entry
        // publication. Never substitute a multipart initiation timestamp.
        check_single_request(
            bytes.len() as u64,
            storage
                .single_request_limit()
                .min(coven_format::store_log::MAX_STORE_LOG_OBJECT as u64),
        )?;
        return single_request.await;
    }
    if bytes.len() as u64 <= storage.single_request_limit() {
        return single_request.await;
    }
    let mut session = storage
        .begin_upload_checked(path, bytes.len() as u64)
        .await?;
    let upload = async {
        while session.confirmed_bytes() < session.total_bytes() {
            let start = usize::try_from(session.confirmed_bytes())
                .map_err(|error| StorageFailure::InvalidPart.with_source(error))?;
            let remaining = bytes.get(start..).ok_or(StorageFailure::InvalidPart)?;
            let length = remaining.len().min(session.part_size());
            storage
                .upload_part_checked(&mut session, &remaining[..length])
                .await?;
            if session.confirmed_bytes() <= start as u64 {
                return Err(StorageFailure::Protocol.with_source("upload did not advance"));
            }
        }
        storage.finish_upload_checked(&mut session).await
    }
    .await;
    match upload {
        Ok(()) => Ok(()),
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
