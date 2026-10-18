//! Isolated keychains for application tests using the production store graph.

use crate::*;
use coven_crypto::custody::{Keychain, KeyringCustody, StoreKeychain};
use std::sync::Arc;

/// One simulated installation's keychain, isolated from the OS and other tests.
/// A new instance simulates another device; keep an instance to test reopen.
pub struct TestCoven {
    keychain: Arc<Keychain>,
}

#[cfg(test)]
#[path = "test_utils_tests.rs"]
mod tests;

impl TestCoven {
    /// Make an installation with an empty device-only and synced keychain.
    pub fn new() -> Self {
        Self {
            keychain: Keychain::in_memory("coven-test").expect("valid service name"),
        }
    }

    /// Create through the production builder, then close for a reopening fixture.
    pub async fn create_closed_store(
        &self,
        layout: &StoreLayout,
        name: &str,
        ids: IdSourceRef,
    ) -> Result<StoreDir, StoreCreationError> {
        let handle = self
            .builder(layout.clone())
            .id_source(ids)
            .synced_tables(Vec::new())
            .migrations(Vec::new())
            .create_store(name)
            .await?;
        let directory = layout.store_dir(&handle.store_id());
        handle.close().await?;
        Ok(directory)
    }

    /// Build the production graph with this installation's keychain.
    pub fn builder(&self, layout: StoreLayout) -> CovenBuilder {
        Coven::builder(layout).with_keychain(self.keychain.clone())
    }

    /// List this installation's saved codes through the production discovery path.
    pub async fn saved_restore_codes(&self) -> Result<Vec<SavedRestoreCode>, BootstrapError> {
        let keychain = self.keychain.clone();
        crate::coven::blocking(move || crate::saved_restore::list(keychain)).await
    }

    /// Discard through the same locked directory and keychain path as `Coven`.
    pub async fn discard_store(&self, directory: &StoreDir) -> Result<(), StoreDeletionError> {
        let directory = directory.clone();
        let keychain = self.keychain.clone();
        crate::coven::blocking(move || {
            crate::coven::delete(&directory, &StoreKeychain::new(keychain, directory.id()))
        })
        .await
    }

    /// Seed the simulated OS custody as if this installation had opened keys.
    pub fn keep_store_keys(
        &self,
        directory: &StoreDir,
        keys: &StoreKeyring,
    ) -> Result<(), KeyError> {
        let keychain = Arc::new(StoreKeychain::new(self.keychain.clone(), directory.id()));
        StoreKeyCustody::persist(&KeyringCustody::new(keychain), keys)
    }

    /// This installation's custody for one store, to read what it holds.
    #[cfg(test)]
    pub(crate) fn store_keychain(&self, store: StoreId) -> StoreKeychain {
        StoreKeychain::new(self.keychain.clone(), store)
    }

    /// Refuse the next keychain operation before changing any stored value.
    pub fn fail_next_keychain_operation(&self) {
        self.keychain.fail_next_operation();
    }

    #[cfg(test)]
    pub(crate) fn fail_keychain_operations_after(&self, successes: &[usize]) {
        self.keychain.fail_operations_after(successes);
    }
}
