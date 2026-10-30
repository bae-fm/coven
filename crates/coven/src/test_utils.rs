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
    /// Make an installation with no device-only entries and none of the person's entries.
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

    /// An installation on an Android phone: its person entries behave as
    /// Block Store keeps them, this phone's own, with Block Store's limits
    /// (§13.1).
    pub fn with_block_store() -> Self {
        Self {
            keychain: Keychain::in_memory_block_store("coven-test").expect("valid service name"),
        }
    }

    /// Another device of the same person: an installation with its own empty
    /// device-only keychain. With iCloud Keychain it shares this one's person
    /// entries, as iCloud Keychain carries a restore code and S3 key between
    /// the person's devices; with Block Store it has its own, empty.
    pub fn another_device(&self) -> Self {
        Self {
            keychain: self.keychain.another_device(),
        }
    }

    /// A new phone set up from this one by direct transfer: nothing of this
    /// phone's own, and a copy of its Block Store entries (§13.1). Requires
    /// `with_block_store`.
    pub fn set_up_new_device(&self) -> Self {
        Self {
            keychain: self.keychain.set_up_new_device(),
        }
    }

    /// A new phone set up from this phone's last cloud backup: the Block Store
    /// entries that backup holds (§13.1). Requires `with_block_store`.
    pub fn restore_new_device_from_cloud(&self) -> Self {
        Self {
            keychain: self.keychain.restore_new_device_from_cloud(),
        }
    }

    /// Google's periodic cloud backup of this phone's Block Store: the entries
    /// stored while end-to-end encryption was available. Requires
    /// `with_block_store`.
    pub fn back_up_to_cloud(&self) {
        self.keychain.back_up_to_cloud();
    }

    /// Whether this phone's cloud backup would be end-to-end encrypted: it
    /// has a screen lock. Requires `with_block_store`.
    pub fn set_end_to_end_encryption(&self, available: bool) {
        self.keychain.set_end_to_end_encryption(available);
    }

    /// Build the production graph with this installation's keychain.
    pub fn builder(&self, layout: StoreLayout) -> CovenBuilder {
        Coven::builder(layout).with_keychain(self.keychain.clone())
    }

    /// List this installation's saved codes through the production discovery path.
    pub async fn saved_restore_codes(&self) -> Result<Vec<SavedRestoreEntry>, BootstrapError> {
        let keychain = self.keychain.clone();
        crate::coven::blocking(move || crate::saved_restore::list(keychain)).await
    }

    /// Forget one store's saved code and S3 key through the production path.
    pub async fn forget_saved_restore_code(&self, store: StoreId) -> Result<(), BootstrapError> {
        let keychain = self.keychain.clone();
        crate::coven::blocking(move || crate::saved_restore::forget(keychain, store)).await
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

    /// The person's entries for one store, as this installation sees them.
    #[cfg(test)]
    pub(crate) fn person_entries(&self, store: StoreId) -> coven_crypto::custody::PersonEntries {
        coven_crypto::custody::PersonEntries::new(self.keychain.clone(), store)
    }

    /// While `refuse` holds, refuse this installation's calls on person entries only.
    #[cfg(test)]
    pub(crate) fn refuse_person_entry_operations(&self, refuse: bool) {
        self.keychain.refuse_person_entry_operations(refuse);
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
