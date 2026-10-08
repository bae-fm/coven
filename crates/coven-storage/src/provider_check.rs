use crate::{
    ByteRange, ObjectPath, ObjectPrefix, Storage, StorageCheck, StorageError, StorageFailure,
    StorageSetupError,
};

/// Check a candidate provider before reserving or publishing setup state.
/// The storage owner supplies a fresh path and sealed bytes (at least three).
/// This calls the provider without committing credentials or local settings.
/// Cleanup runs even after a lost create reply; a preexisting path is left intact.
/// The caller must await cleanup even if its app waiter is cancelled.
pub async fn check_provider(
    storage: &dyn Storage,
    path: &ObjectPath,
    encrypted_bytes: &[u8],
) -> Result<(), StorageSetupError> {
    let end = encrypted_bytes
        .len()
        .checked_sub(1)
        .ok_or(StorageError::Failure(StorageFailure::InvalidRange))?;
    let range = ByteRange::new(1, end as u64)?;
    let created = storage.create(path, encrypted_bytes).await;
    if created
        .as_ref()
        .is_err_and(|error| error.failure() == StorageFailure::AlreadyExists)
    {
        return created.map_err(|source| StorageSetupError::ProviderCheck {
            check: StorageCheck::Create,
            source,
        });
    }
    let mut check = StorageCheck::Create;
    let result = async {
        created?;
        check = StorageCheck::CreateOnce;
        match storage.create(path, encrypted_bytes).await {
            Err(error)
                if error.failure() == StorageFailure::AlreadyExists
                    && !matches!(error, StorageError::Cleanup { .. }) => {}
            Err(error) => return Err(error),
            Ok(()) => {
                return Err(
                    StorageFailure::Protocol.with_source("provider accepted a second create")
                )
            }
        }
        check = StorageCheck::Read;
        if storage.read(path).await? != encrypted_bytes {
            return Err(StorageFailure::Protocol.with_source("whole read disagrees with upload"));
        }
        check = StorageCheck::ReadRange;
        if storage.read_range(path, range).await? != encrypted_bytes[1..end] {
            return Err(StorageFailure::Protocol.with_source("range read disagrees with upload"));
        }
        check = StorageCheck::List;
        if !storage
            .list(&ObjectPrefix::all())
            .await?
            .iter()
            .any(|object| &object.path == path && object.size == encrypted_bytes.len() as u64)
        {
            return Err(StorageFailure::Protocol
                .with_source("test object missing or wrong size in listing"));
        }
        Ok(())
    }
    .await;
    let cleanup = async {
        storage.delete(path).await?;
        match storage.read(path).await {
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(()),
            Err(error) => Err(error),
            Ok(_) => {
                Err(StorageFailure::Protocol.with_source("deleted test object is still readable"))
            }
        }
    }
    .await;
    let source = match (result, cleanup) {
        (Ok(()), Ok(())) => return Ok(()),
        (Ok(()), Err(error)) => {
            check = StorageCheck::Delete;
            error
        }
        (Err(error), Ok(()))
            if check == StorageCheck::List && error.failure() == StorageFailure::InvalidPath =>
        {
            return Err(StorageSetupError::LocationOccupied);
        }
        (Err(error), Ok(())) => error,
        (Err(operation), Err(cleanup)) => StorageError::Cleanup {
            operation: Box::new(operation),
            cleanup: Box::new(cleanup),
        },
    };
    Err(StorageSetupError::ProviderCheck { check, source })
}

#[cfg(test)]
#[path = "provider_check_tests.rs"]
mod tests;
