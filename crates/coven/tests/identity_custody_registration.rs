//! Process-global identity-custody contracts. `CovenBuilder::open` must
//! perform no keyring interaction, and `set_identity_custody` follows the
//! same one-time-registration discipline as `set_keyring_service`. Both are
//! process-wide state, so — like `keyring_store.rs` — this runs as one
//! ordered test in its own process rather than racing coven's other
//! integration tests over shared globals.

use std::collections::HashMap;
use std::sync::Arc;

use coven::{Config, Coven, Migration, StoreDir, SyncedTable};

/// A keyring store that panics on any credential-build call. Every keyring
/// operation in this crate — a read, a write, a delete — goes through
/// `keyring_core::Entry::new`, which calls `build()`, so panicking there
/// proves a code path under test performs zero keyring interaction: a silent
/// success against this store would mean it never even tried.
#[derive(Debug)]
struct PanicOnAnyKeyringOpStore;

impl keyring_core::api::CredentialStoreApi for PanicOnAnyKeyringOpStore {
    fn vendor(&self) -> String {
        "panic-on-any-op test store".to_string()
    }

    fn id(&self) -> String {
        "panic-on-any-op".to_string()
    }

    fn build(
        &self,
        service: &str,
        user: &str,
        _modifiers: Option<&HashMap<&str, &str>>,
    ) -> keyring_core::Result<keyring_core::Entry> {
        panic!(
            "unexpected keyring credential build: service={service:?} user={user:?} — the \
             code under test must perform no keyring interaction"
        );
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn notes_migration() -> Migration {
    Migration::sql(
        1,
        "test-schema",
        "CREATE TABLE notes (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
    )
}

#[test]
fn open_performs_no_keyring_interaction_and_identity_custody_registers_once() {
    keyring_core::set_default_store(Arc::new(PanicOnAnyKeyringOpStore));

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new(tmp.path());
    let config = Config::with_defaults(
        "open-no-keys-test".to_string(),
        "device".to_string(),
        dir,
        "Test".to_string(),
    );
    Coven::builder(config)
        .synced_tables(vec![SyncedTable::new("notes")])
        .migrations(vec![notes_migration()])
        .open()
        .expect("open must succeed while performing no keyring credential build");

    // `set_identity_custody` is the process-wide, one-time registration
    // `DeviceIdentityCustody` shares with `set_keyring_service`: the first
    // call registers, a second is a startup contradiction. Neither call
    // touches the panicking store above — `IdentityCustody::Keyring.resolve()`
    // only constructs the trait object; it reads no credential.
    coven::set_identity_custody(coven::IdentityCustody::Keyring)
        .expect("first identity custody registration succeeds");
    let err = coven::set_identity_custody(coven::IdentityCustody::Keyring)
        .expect_err("a second registration is a startup contradiction");
    assert!(
        err.to_string().contains("already registered"),
        "error names the conflict: {err}"
    );
}
