//! Isolated keychains for application tests using the production store graph.

use crate::*;
use coven_crypto::custody::{Keychain, KeyringCustody, StoreKeychain};
use std::sync::Arc;

/// One simulated installation's keychain, isolated from the OS and other tests.
/// A new instance simulates another device; keep an instance to test reopen.
pub struct TestCoven {
    keychain: Arc<Keychain>,
}

impl TestCoven {
    /// Make an installation with an empty device-only and synced keychain.
    pub fn new() -> Self {
        Self {
            keychain: Keychain::in_memory("coven-test").expect("valid service name"),
        }
    }

    /// Create through the same publication and custody path as `Coven`.
    pub async fn create_store(
        &self,
        layout: &StoreLayout,
        name: &str,
        ids: IdSourceRef,
    ) -> Result<StoreDir, StoreCreationError> {
        let layout = layout.clone();
        let name = name.to_owned();
        let keychain = self.keychain.clone();
        crate::coven::blocking(move || {
            let id = StoreId(ids.new_id());
            crate::coven::create(&layout, id, &name, ids, StoreKeychain::new(keychain, id))
        })
        .await
    }

    /// Build the production graph with this installation's keychain.
    pub fn builder(&self, directory: StoreDir) -> CovenBuilder {
        Coven::builder(directory).with_keychain(self.keychain.clone())
    }

    /// Open an invite or restore code through production bootstrap, with this
    /// installation's isolated keychain and an injected provider connection.
    pub async fn open_code(
        &self,
        code: &str,
        device_name: &str,
        tables: &[SyncedTable],
        migrations: &[Migration],
        layout: &StoreLayout,
        storage: Arc<dyn coven_storage::Storage>,
        clock: ClockRef,
        ids: IdSourceRef,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<Option<StoreDir>, BootstrapError> {
        use crate::bootstrap::BootstrapRequest;
        let request = match decode_code_info(code)?.kind {
            CodeKind::Restore => BootstrapRequest::Restore {
                code: coven_sync::read_restore_code(code)?,
                name: device_name.into(),
            },
            CodeKind::Invite => BootstrapRequest::Join {
                code: coven_sync::read_invite_code(code)?,
                name: device_name.into(),
            },
        };
        crate::bootstrap::bootstrap_device(
            request,
            tables,
            migrations,
            CovenMigrationPolicy::ApplyPending,
            KeyCustody::Keyring,
            IdentityCustody::Keyring,
            None,
            layout,
            Arc::new(OAuthClients::new(None, None, None, clock.clone())),
            None,
            clock,
            ids,
            |_| {},
            cancel,
            self.keychain.clone(),
            Some(storage),
        )
        .await
    }

    /// Delete through the same locked directory and keychain path as `Coven`.
    pub async fn delete_store(
        &self,
        directory: &StoreDir,
        names: &[&str],
    ) -> Result<(), StoreDeletionError> {
        let directory = directory.clone();
        let names = names.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        let keychain = self.keychain.clone();
        crate::coven::blocking(move || {
            crate::coven::delete(
                &directory,
                &names,
                StoreKeychain::new(keychain, directory.id()),
            )
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

    /// Refuse the next keychain operation before changing any stored value.
    pub fn fail_next_keychain_operation(&self) {
        self.keychain.fail_next_operation();
    }
}
