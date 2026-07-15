use coven::{
    Config, Coven, CovenError, HomeStorage, Migration, RowIdentity, StoreDir, SyncedTable,
    WritePolicy,
};
use coven_core::InMemoryCloudHome;

fn config(store_dir: StoreDir, storage: HomeStorage, policy: WritePolicy) -> Config {
    let mut config = Config::with_defaults(
        format!("{policy:?}-storage-scope"),
        "configuration-device".to_string(),
        store_dir,
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

fn assert_browsable_scoped_tables_are_rejected(policy: WritePolicy) {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new(temp.path());
    let cloud = InMemoryCloudHome::new();

    let result = Coven::builder(config(store_dir.clone(), HomeStorage::Browsable, policy))
        .write_policy(policy)
        .synced_tables(tables())
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
    assert!(cloud.is_empty());
    assert!(cloud.appended_keys().is_empty());
}

#[test]
fn merge_builder_rejects_browsable_storage_with_scoped_tables() {
    assert_browsable_scoped_tables_are_rejected(WritePolicy::MergeConcurrent);
}

#[test]
fn serial_builder_rejects_browsable_storage_with_scoped_tables() {
    assert_browsable_scoped_tables_are_rejected(WritePolicy::Serial);
}

#[test]
fn opaque_storage_accepts_scoped_tables_for_both_write_policies() {
    for policy in [WritePolicy::MergeConcurrent, WritePolicy::Serial] {
        let temp = tempfile::tempdir().expect("store directory");
        let store_dir = StoreDir::new(temp.path());
        let handle = Coven::builder(config(store_dir, HomeStorage::Opaque, policy))
            .write_policy(policy)
            .synced_tables(tables())
            .migrations(migrations())
            .open()
            .expect("opaque storage accepts scoped tables");
        drop(handle);
    }
}
