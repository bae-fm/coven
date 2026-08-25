//! `CovenBuilder::open` must perform no keyring interaction: resolving both
//! `key_custody` and `identity_custody` builds their trait objects without
//! ever calling `unlock`. Runs as one ordered test in its own process, like
//! `keyring_store.rs`, since a panicking store installed process-wide would
//! otherwise race coven's other integration tests.

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
        "CREATE TABLE notes (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;
         INSERT INTO notes VALUES ('preferences', '0000000001000-0000-device');",
    )
}

#[test]
fn open_performs_no_keyring_interaction_for_either_custody() {
    keyring_core::set_default_store(Arc::new(PanicOnAnyKeyringOpStore));

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let config = Config::with_defaults(
        "open-no-keys-test".to_string(),
        "device".to_string(),
        "Test".to_string(),
    );
    // Both custodies default to `Keyring`, so `open()` resolves both to a
    // trait object over the panicking store above and must still succeed —
    // resolving a policy is not consulting it.
    Coven::builder(dir, config)
        .synced_tables(vec![SyncedTable::new(
            "notes",
            coven::RowIdentity::SharedKey,
        )])
        .coven_migration_policy(coven::CovenMigrationPolicy::ApplyPending)
        .migrations(vec![notes_migration()])
        .open()
        .expect("open must succeed while performing no keyring credential build");

    let invalid_tmp = tempfile::tempdir().expect("independent UUID temp dir");
    let invalid_dir = StoreDir::new_ephemeral(invalid_tmp.path());
    let invalid_config = Config::with_defaults(
        "open-independent-identity-test".to_string(),
        "device".to_string(),
        "Test".to_string(),
    );
    let invalid = Coven::builder(invalid_dir, invalid_config)
        .synced_tables(vec![SyncedTable::new(
            "notes",
            coven::RowIdentity::IndependentUuid,
        )])
        .coven_migration_policy(coven::CovenMigrationPolicy::ApplyPending)
        .migrations(vec![notes_migration()])
        .open();
    let error = match invalid {
        Ok(_) => panic!("public builder/open must enforce IndependentUuid"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("preferences") && error.contains("IndependentUuid"),
        "public open preserves the declared row identity: {error}",
    );
}
