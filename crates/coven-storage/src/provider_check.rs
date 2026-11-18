use crate::{
    ByteRange, ObjectPath, ObjectPrefix, Storage, StorageCheck, StorageCheckError, StorageError,
    StorageFailure,
};

/// How a provider check ended when nothing failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckEnd {
    /// Every step passed.
    Passed,
    /// The caller stopped the check between two of its steps; the test
    /// object, if the check made it, is deleted.
    Stopped,
}

/// Check a candidate provider before reserving or publishing setup state.
/// The storage owner supplies a fresh path and sealed bytes: at least three,
/// and within the single-request limit, because the conditional replacement
/// it checks is one request.
/// This calls the provider without committing credentials or local settings.
/// Cleanup runs even after a lost create reply; a preexisting path is left intact.
/// The caller must await cleanup even if its app waiter is cancelled.
/// During setup the connection sends rate-limited requests again (§20.8),
/// so a refusal fails the check only once the setup call's minute is spent.
/// Before each step the check asks `stopped`, and once it answers true the
/// check sends no further step: it deletes its test object and ends
/// `Stopped`.
pub async fn check_provider(
    storage: &crate::StorageConnection<impl crate::ProviderOps + ?Sized>,
    path: &ObjectPath,
    encrypted_bytes: &[u8],
    stopped: impl Fn() -> bool,
) -> Result<CheckEnd, StorageCheckError> {
    let end = encrypted_bytes
        .len()
        .checked_sub(1)
        .ok_or(StorageError::Failure(StorageFailure::InvalidRange))?;
    let range = ByteRange::new(1, end as u64)?;
    if stopped() {
        return Ok(CheckEnd::Stopped);
    }
    let created = match storage.create(path, encrypted_bytes).await {
        Err(source) if source.failure() == StorageFailure::AlreadyExists => {
            return Err(StorageCheckError::ProviderCheck { check: StorageCheck::Create, source })
        }
        created => created,
    };
    let mut check = StorageCheck::Create;
    let result = async {
        created?;
        if stopped() {
            return Ok(CheckEnd::Stopped);
        }
        check = StorageCheck::CreateOnce;
        match storage.create(path, encrypted_bytes).await {
            Err(error) if error.own_failure() == Some(StorageFailure::AlreadyExists) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(
                    StorageFailure::Protocol.with_source("provider accepted a second create")
                )
            }
        }
        if stopped() {
            return Ok(CheckEnd::Stopped);
        }
        check = StorageCheck::Read;
        if storage.read(path).await? != encrypted_bytes {
            return Err(StorageFailure::Protocol.with_source("whole read disagrees with upload"));
        }
        if stopped() {
            return Ok(CheckEnd::Stopped);
        }
        check = StorageCheck::ReadRange;
        if storage.read_range(path, range).await? != encrypted_bytes[1..end] {
            return Err(StorageFailure::Protocol.with_source("range read disagrees with upload"));
        }
        if stopped() {
            return Ok(CheckEnd::Stopped);
        }
        check = StorageCheck::ConditionalReplace;
        let previous = storage.status(path).await?.ok_or(StorageFailure::NotFound)?;
        let replacement = encrypted_bytes.iter().map(|byte| byte ^ 0x80).collect::<Vec<_>>();
        storage.replace_object(&previous, &replacement).await?;
        match storage.replace_object(&previous, encrypted_bytes).await {
            Err(error) if error.failure() == StorageFailure::Conflict => {}
            Err(error) => return Err(error),
            Ok(()) => {
                return Err(
                    StorageFailure::Protocol.with_source("provider accepted a stale replacement")
                )
            }
        }
        if storage.read(path).await? != replacement {
            return Err(StorageFailure::Protocol
                .with_source("conditional replacement changed the wrong bytes"));
        }
        if stopped() {
            return Ok(CheckEnd::Stopped);
        }
        check = StorageCheck::List;
        if storage
            .list(&ObjectPrefix::all(path.store()))
            .await?
            .iter()
            .any(|object| &object.path == path && object.size != encrypted_bytes.len() as u64)
        {
            return Err(
                StorageFailure::Protocol.with_source("test object has wrong size in listing")
            );
        }
        Ok(CheckEnd::Passed)
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
    // A check that passed or stopped, and failed only to delete its object,
    // failed deleting.
    if result.is_ok() && cleanup.is_err() {
        check = StorageCheck::Delete;
    }
    crate::transfer::with_cleanup(result, cleanup)
        .map_err(|source| StorageCheckError::ProviderCheck { check, source })
}

#[cfg(test)]
#[path = "provider_check_tests.rs"]
mod tests;
