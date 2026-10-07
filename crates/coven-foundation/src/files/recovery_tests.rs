use super::*;
use crate::files::StoreLayout;
use crate::id_source::{StoreId, UuidIds};

#[test]
fn interrupted_archival_and_reload_require_explicit_retry_and_preserve_every_sqlite_file() {
    for moved in 0..=FILES.len() {
        let root = tempfile::tempdir().unwrap();
        let store = StoreLayout::new(root.path().to_owned())
            .create_store_dir(StoreId(uuid::Uuid::from_u128(1)), "Recovery", &UuidIds)
            .unwrap();
        let writer = store.lock_exclusive().unwrap();
        let path = store.database_path();
        let directory = path.parent().unwrap();
        let backup = directory.join("damaged-database-test");
        std::fs::create_dir(&backup).unwrap();
        for file in FILES {
            std::fs::write(directory.join(file), file.as_bytes()).unwrap();
        }
        let journal = Journal {
            backup: "damaged-database-test".into(),
            files: [true; 4],
            phase: Phase::Archiving,
        };
        AtomicFile::new(directory.join(MARKER))
            .replace(&serde_json::to_vec(&journal).unwrap())
            .unwrap();
        for file in FILES.iter().take(moved) {
            std::fs::rename(directory.join(file), backup.join(file)).unwrap();
        }
        assert!(matches!(
            store.check_database_recovery(),
            Err(StoreLockError::RecoveryPending(_))
        ));
        let recovery = store
            .recover_database(&writer, &FileName::new("ignored-on-retry").unwrap())
            .unwrap();
        for file in FILES {
            assert_eq!(std::fs::read(backup.join(file)).unwrap(), file.as_bytes());
        }
        std::fs::write(&path, "unpublished").unwrap();
        drop(recovery);
        assert!(matches!(
            store.check_database_recovery(),
            Err(StoreLockError::RecoveryPending(_))
        ));
        let recovery = store
            .recover_database(&writer, &FileName::new("retry").unwrap())
            .unwrap();
        assert!(!path.exists());
        assert_eq!(
            std::fs::read(recovery.source_database_path()).unwrap(),
            b"store.db"
        );
        std::fs::write(&path, "loading").unwrap();
        let mut recovery = recovery;
        recovery.prepared().unwrap();
        drop(recovery);
        let recovery = store
            .recover_database(&writer, &FileName::new("resume").unwrap())
            .unwrap();
        assert!(!recovery.needs_salvage());
        assert_eq!(std::fs::read(&path).unwrap(), b"loading");
        std::fs::write(&path, "loaded").unwrap();
        recovery.finish().unwrap();
        store.check_database_recovery().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"loaded");
    }
}

#[test]
fn active_readers_prevent_archiving_before_any_marker_or_file_changes() {
    let root = tempfile::tempdir().unwrap();
    let store = StoreLayout::new(root.path().to_owned())
        .create_store_dir(StoreId(uuid::Uuid::from_u128(1)), "Recovery", &UuidIds)
        .unwrap();
    let writer = store.lock_exclusive().unwrap();
    let reader = store.lock_read_only().unwrap();
    std::fs::write(store.database_path(), "damaged").unwrap();
    assert!(matches!(
        store.recover_database(&writer, &FileName::new("test").unwrap()),
        Err(StoreLockError::AlreadyOpen(_))
    ));
    store.check_database_recovery().unwrap();
    assert_eq!(std::fs::read(store.database_path()).unwrap(), b"damaged");
    drop(reader);
    store
        .recover_database(&writer, &FileName::new("test").unwrap())
        .unwrap();
    assert!(!store.database_path().exists());
}

#[test]
fn a_missing_prepared_replacement_fails_without_discarding_its_journal() {
    let root = tempfile::tempdir().unwrap();
    let store = StoreLayout::new(root.path().to_owned())
        .create_store_dir(StoreId(uuid::Uuid::from_u128(1)), "Recovery", &UuidIds)
        .unwrap();
    let writer = store.lock_exclusive().unwrap();
    std::fs::write(store.database_path(), "damaged").unwrap();
    let mut recovery = store
        .recover_database(&writer, &FileName::new("test").unwrap())
        .unwrap();
    std::fs::write(store.database_path(), "replacement").unwrap();
    recovery.prepared().unwrap();
    drop(recovery);
    std::fs::remove_file(store.database_path()).unwrap();
    assert!(store
        .recover_database(&writer, &FileName::new("retry").unwrap())
        .is_err());
    assert!(matches!(
        store.check_database_recovery(),
        Err(StoreLockError::RecoveryPending(_))
    ));
}
