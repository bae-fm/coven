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
        Err(operation) => match storage.abort_upload_checked(&session).await {
            Ok(()) => Err(operation),
            Err(cleanup) => Err(StorageError::Cleanup {
                operation: Box::new(operation),
                cleanup: Box::new(cleanup),
            }),
        },
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
