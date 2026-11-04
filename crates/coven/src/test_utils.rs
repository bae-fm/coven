//! Installations for application tests: an isolated in-memory keychain and
//! a network the test drives, with the production store graph.

use super::Coven;
use crate::*;
use coven_crypto::custody::{Keychain, KeyringCustody, StoreKeychain};
use coven_foundation::files::StoreDirectories;
use coven_foundation::network_watch::{NetworkSource, TestNetwork};
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(test)]
#[path = "test_utils_tests.rs"]
mod tests;

impl Coven {
    /// An installation for a test: its stores under `app_dir`, a keychain
    /// with no device-only entries and none of the person's entries, isolated
    /// from the OS and other tests, and a usable network that isn't metered,
    /// which the test drives. A new one simulates another device; keep one to
    /// test reopening.
    pub fn in_memory(app_dir: PathBuf) -> Self {
        Self::testing(app_dir, Keychain::in_memory("coven-test").expect("valid service name"))
    }

    /// An installation on an Android phone: its person entries behave as
    /// Block Store keeps them, this phone's own, with Block Store's limits
    /// (§13.1).
    pub fn with_block_store(app_dir: PathBuf) -> Self {
        Self::testing(
            app_dir,
            Keychain::in_memory_block_store("coven-test").expect("valid service name"),
        )
    }

    /// An installation for a test with the keychain `keychain`, such as one
    /// failing on cue.
    #[cfg(test)]
    pub(crate) fn with_keychain(app_dir: PathBuf, keychain: Arc<Keychain>) -> Self {
        Self::testing(app_dir, keychain)
    }

    /// The app directory's stores, for a test that begins a bootstrap or a
    /// replacement itself.
    #[cfg(test)]
    pub(crate) fn directories(&self) -> &StoreDirectories {
        &self.directories
    }

    fn testing(app_dir: PathBuf, keychain: Arc<Keychain>) -> Self {
        Self {
            directories: StoreDirectories::new(app_dir),
            keychain,
            network: NetworkSource::Test(TestNetwork::new(true, false)),
        }
    }

    /// Another device of the same person, its stores under `app_dir`: an
    /// installation with its own empty device-only keychain. With iCloud
    /// Keychain it shares this one's person entries, as iCloud Keychain
    /// carries a restore code and S3 key between the person's devices; with
    /// Block Store it has its own, empty.
    pub fn another_device(&self, app_dir: PathBuf) -> Self {
        Self::testing(app_dir, self.keychain.another_device())
    }

    /// A new phone set up from this one by direct transfer, its stores under
    /// `app_dir`: nothing of this phone's own, and a copy of its Block Store
    /// entries (§13.1). Requires `with_block_store`.
    pub fn set_up_new_device(&self, app_dir: PathBuf) -> Self {
        Self::testing(app_dir, self.keychain.set_up_new_device())
    }

    /// A new phone set up from this phone's last cloud backup, its stores
    /// under `app_dir`: the Block Store entries that backup holds (§13.1).
    /// Requires `with_block_store`.
    pub fn restore_new_device_from_cloud(&self, app_dir: PathBuf) -> Self {
        Self::testing(app_dir, self.keychain.restore_new_device_from_cloud())
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

    /// Create through the production builder, then close for a reopening fixture.
    pub async fn create_closed_store(
        &self,
        name: &str,
        ids: IdSourceRef,
    ) -> Result<StoreDir, StoreCreationError> {
        let handle = self
            .builder()
            .id_source(ids)
            .synced_tables(Vec::new())
            .migrations(Vec::new())
            .create_store(name)
            .await?;
        let directory = self.store_dir(&handle.store_id());
        handle.close().await?;
        Ok(directory)
    }

    /// Publish a directory fixture without a database or member custody.
    pub fn create_store_dir(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<StoreDir, crate::DirectoryCreationError> {
        self.directories.create_store_dir(id, ids)
    }

    /// Change the network this installation's test drives.
    fn drive_network(&self, change: impl FnOnce(&TestNetwork)) {
        match &self.network {
            NetworkSource::Test(network) => change(network),
            NetworkSource::Platform => panic!("an installation made for a test drives its network"),
        }
    }

    /// The device moves to another usable network: a network report, as
    /// the platform's would make (api.md E5).
    pub fn join_network(&self, metered: bool) {
        self.drive_network(|network| network.join(metered));
    }

    /// The device loses its network: no report.
    pub fn lose_network(&self) {
        self.drive_network(TestNetwork::lose);
    }

    /// The platform fails to say what the device's network is: coven can't
    /// tell, and calls it metered, until the test says again.
    pub fn network_cannot_tell(&self) {
        self.drive_network(TestNetwork::cannot_tell);
    }

    /// The device's network turns metered or unmetered: no report.
    pub fn set_network_metered(&self, metered: bool) {
        self.drive_network(|network| network.set_metered(metered));
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

    /// Delete this installation's storage credentials for `store`, as the
    /// person deleting the keychain entry by hand does. The recorded location
    /// stays: a handle opened afterwards reports no storage set up on this
    /// device, and setup connects that location again.
    pub fn lose_storage_credentials(&self, store: StoreId) -> Result<(), KeyError> {
        StoreKeychain::new(self.keychain.clone(), store).delete_storage_credentials()
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
