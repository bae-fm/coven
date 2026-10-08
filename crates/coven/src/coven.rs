//! The calls that create, locate, open and remove device-local stores.

use crate::{
    CovenBuilder, IdSourceRef, StoreCreationError, StoreDeletionError, StoreDir, StoreId,
    StoreLayout,
};
use coven_crypto::custody::{Keychain, StoreKeychain};

/// Creating, opening and deleting stores on this device (E1).
pub struct Coven;

impl Coven {
    /// Makes a new store on this device, named `name`: its directory, its id
    /// and this device's id, both from `ids`. Storage is set up after opening.
    pub async fn create_store(
        layout: &StoreLayout,
        name: &str,
        ids: IdSourceRef,
    ) -> Result<StoreDir, StoreCreationError> {
        let layout = layout.clone();
        let name = name.to_owned();
        blocking(move || {
            let id = StoreId(ids.new_id());
            let keychain = Keychain::registered()
                .map_err(|source| StoreCreationError::Initialization { id, source })?;
            create(&layout, id, &name, ids, StoreKeychain::new(keychain, id))
        })
        .await
    }

    /// Configure opening, restoring or joining a store under this layout.
    pub fn builder(layout: StoreLayout) -> CovenBuilder {
        CovenBuilder::new(layout)
    }

    /// Deletes a closed store from this device: every keychain entry coven
    /// holds for it, including every saved host secret, then its directory.
    /// Does not open the database.
    /// Refused while a writer, read-only handle or file stream remains open;
    /// storage is untouched. Retrying finishes a deletion that failed partway.
    pub async fn delete_store(store_dir: &StoreDir) -> Result<(), StoreDeletionError> {
        let directory = store_dir.clone();
        blocking(move || {
            delete(
                &directory,
                StoreKeychain::new(Keychain::registered()?, directory.id()),
            )
        })
        .await
    }
}

pub(crate) fn create(
    layout: &StoreLayout,
    id: StoreId,
    name: &str,
    ids: IdSourceRef,
    keys: StoreKeychain,
) -> Result<StoreDir, StoreCreationError> {
    let mut initialized = false;
    let result = layout.create_store_dir_with(id, name, ids.as_ref(), |settings| {
        initialized = true;
        keys.set_device_id(settings.device_id)
    });
    match result {
        Err(operation)
            if initialized && !matches!(operation, StoreCreationError::Published { .. }) =>
        {
            match keys.delete_device_id() {
                Ok(()) => Err(operation),
                Err(cleanup) => Err(StoreCreationError::InitializationCleanup {
                    operation: Box::new(operation),
                    cleanup,
                }),
            }
        }
        result => result,
    }
}

pub(crate) fn delete(directory: &StoreDir, keys: StoreKeychain) -> Result<(), StoreDeletionError> {
    let lock = directory.lock_for_deletion()?;
    keys.delete_store_entries()?;
    if let Some(lock) = lock {
        lock.remove_directory()?;
    }
    Ok(())
}

pub(crate) async fn blocking<T: Send + 'static>(run: impl FnOnce() -> T + Send + 'static) -> T {
    completion(tokio::task::spawn_blocking(run)).await
}

pub(crate) async fn completion<T>(task: tokio::task::JoinHandle<T>) -> T {
    match task.await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("store operation task cancelled: {error}"),
    }
}

#[cfg(test)]
#[path = "coven_tests.rs"]
mod tests;
