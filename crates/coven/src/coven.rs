//! The calls that create, locate, open and remove device-local stores.

use crate::{CovenBuilder, StoreDeletionError, StoreDir, StoreLayout};
use coven_crypto::custody::{KeyError, Keychain, StoreKeychain};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::unwinding::run_blocking;

/// Creating, opening and deleting stores on this device (E1).
pub struct Coven;

impl Coven {
    /// Read the saved storage location without opening the store or its custody.
    /// None means storage has never been set up; once set up, the location stays.
    pub async fn storage_config(
        store_dir: &StoreDir,
    ) -> Result<Option<crate::StorageConfig>, crate::StorageError> {
        let settings = coven_storage::StorageSettings::new(store_dir.clone());
        run_blocking("read the storage settings", move || settings.read())
            .await
            .map_err(|shutdown| crate::StorageError::from(crate::FileError::from(shutdown)))?
    }

    /// Keep SQLite's temporary tables in `directory`, such as the app's cache
    /// folder, for every store this process opens. An Android app calls this
    /// before opening its first store: SQLite finds no folder there by itself,
    /// and temporary tables that outgrow a connection's cache fail without
    /// one. Elsewhere SQLite uses the system's.
    pub fn set_temporary_directory(
        directory: &std::path::Path,
    ) -> Result<(), crate::TemporaryDirectoryError> {
        coven_database::set_temporary_directory(directory)
    }

    /// Configure creating, opening, restoring or joining a store.
    pub fn builder(layout: StoreLayout) -> CovenBuilder {
        CovenBuilder::new(layout)
    }

    /// Deletes a closed store from this device without opening it: this
    /// device's own keychain entries for it, including every saved host
    /// secret, then its directory. The person's saved restore code and S3 key
    /// stay for their next devices; they go only when the
    /// person leaves the store (§13.1). Unsent writes and files are lost, and
    /// this device stays registered with the store until it is removed; prefer
    /// `CovenHandle::delete_store`. Use this for a store that cannot open, or
    /// to finish a handle's deletion that failed after closing. A store whose
    /// `reset_device` or `restore_device` stopped after its swap is deleted
    /// whole: both installations, the replacement's directory and record,
    /// and this device's entries, which by then may name either device.
    /// Refused while a writer, read-only handle, file stream or a running
    /// replacement remains; storage is untouched. A handle of it in this
    /// process that is closing, or whose last clone was dropped, is waited
    /// for. Retrying finishes a deletion that failed partway.
    pub async fn discard_store(store_dir: &StoreDir) -> Result<(), StoreDeletionError> {
        let store = store_dir.id();
        discard(store_dir, move || Ok(StoreKeychain::new(Keychain::registered()?, store))).await
    }
}

/// Wait for any closing instance of the store in this process, then delete
/// it with this device's entries in the keychain `keys` makes, made on the
/// blocking pool as the deletion runs.
pub(crate) async fn discard(
    store_dir: &StoreDir,
    keys: impl FnOnce() -> Result<StoreKeychain, KeyError> + Send + 'static,
) -> Result<(), StoreDeletionError> {
    store_dir.closed().await?;
    let directory = store_dir.clone();
    run_blocking("discard the store", move || delete(&directory, &keys()?)).await?
}

pub(crate) fn delete(directory: &StoreDir, keys: &StoreKeychain) -> Result<(), StoreDeletionError> {
    let lock = directory.lock_for_deletion()?;
    keys.delete_device_entries()?;
    if let Some(lock) = lock {
        lock.remove_directory()?;
    }
    Ok(())
}

/// Wait for `task`, spawned so the call named `operation` finishes even when
/// its caller stops waiting. A panic in it unwinds into the caller; the
/// runtime dropping it as it shuts down is a `Shutdown` that may have run in
/// part.
pub(crate) async fn completion<T>(
    operation: &'static str,
    task: tokio::task::JoinHandle<T>,
) -> Result<T, crate::task::Shutdown> {
    coven_foundation::unwinding::task(operation, task.await)
}

#[cfg(test)]
#[path = "coven_tests.rs"]
mod tests;
