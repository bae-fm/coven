use coven::{
    Config, Coven, CovenError, HomeStorage, Migration, RowIdentity, StoreDir, SyncedTable,
};

fn config(storage: HomeStorage) -> Config {
    let mut config = Config::with_defaults(
        "storage-scope".to_string(),
        "configuration-device".to_string(),
        "Storage scope configuration".to_string(),
    );
    config.cloud_home.storage = storage;
    config
}

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("settings", RowIdentity::SharedKey),
        SyncedTable::new("documents", RowIdentity::SharedKey).scoped_by("audience"),
    ]
}

fn migrations() -> Vec<Migration> {
    vec![Migration::sql(
        1,
        "storage scope configuration",
        "CREATE TABLE settings (
            id TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            _updated_at TEXT NOT NULL
         ) STRICT;
         CREATE TABLE documents (
            id TEXT PRIMARY KEY,
            audience TEXT,
            _updated_at TEXT NOT NULL
         ) STRICT;",
    )]
}

#[test]
fn builder_rejects_browsable_storage_with_scoped_tables() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let result = Coven::builder(store_dir.clone(), config(HomeStorage::Browsable))
        .synced_tables(tables())
        .coven_migration_policy(coven::CovenMigrationPolicy::ApplyPending)
        .migrations(migrations())
        .open();

    match result {
        Err(CovenError::BrowsableStorageWithScopedTable { table }) => {
            assert_eq!(table, "documents");
        }
        Err(other) => panic!("unexpected browsable/scoped configuration error: {other}"),
        Ok(handle) => {
            drop(handle);
            panic!("browsable storage with a scoped table must be rejected");
        }
    }
    assert!(!store_dir.db_path().exists());
    assert!(!store_dir.config_path().exists());
    assert!(!temp.path().join(".coven-lock").exists());
}

#[test]
fn opaque_storage_accepts_scoped_tables() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let handle = Coven::builder(store_dir, config(HomeStorage::Opaque))
        .synced_tables(tables())
        .coven_migration_policy(coven::CovenMigrationPolicy::ApplyPending)
        .migrations(migrations())
        .open()
        .expect("opaque storage accepts scoped tables");
    drop(handle);
}
