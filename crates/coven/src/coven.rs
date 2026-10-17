//! The calls that create, locate, open and remove device-local stores.

use crate::{CovenBuilder, StoreDeletionError, StoreDir, StoreLayout};
use coven_crypto::custody::{Keychain, StoreKeychain};

/// Creating, opening and deleting stores on this device (E1).
pub struct Coven;

impl Coven {
    /// Configure creating, opening, restoring or joining a store.
    pub fn builder(layout: StoreLayout) -> CovenBuilder {
        CovenBuilder::new(layout)
    }

    /// Deletes a closed store from this device without opening it: every
    /// keychain entry coven holds for it, including every saved host secret,
    /// then its directory. Unsent writes and files are lost, and this device
    /// stays registered with the store until it is removed; prefer
    /// `CovenHandle::delete_store`. Use this for a store that cannot open, or
    /// to finish a handle's deletion that failed after closing.
    /// Refused while a writer, read-only handle or file stream remains open;
    /// storage is untouched. Retrying finishes a deletion that failed partway.
    pub async fn discard_store(store_dir: &StoreDir) -> Result<(), StoreDeletionError> {
        let directory = store_dir.clone();
        blocking(move || {
            delete(
                &directory,
                &StoreKeychain::new(Keychain::registered()?, directory.id()),
            )
        })
        .await
    }
}

pub(crate) fn delete(directory: &StoreDir, keys: &StoreKeychain) -> Result<(), StoreDeletionError> {
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
