//! Where a Store snapshot restores to, and what a finished or failed
//! attempt is allowed to have left there.

use super::*;

/// The fresh directory a Store snapshot restores into. The temp dir must outlive
/// the restore, and both the database path and the Store dir are read from it.
pub(super) struct RestoreTarget {
    _temp: tempfile::TempDir,
    database_path: std::path::PathBuf,
    store_dir: coven_foundation::store_dir::StoreDir,
}

impl RestoreTarget {
    pub(super) fn new() -> Self {
        let temp = tempfile::tempdir().expect("restore destination");
        Self {
            database_path: temp.path().join("store.db"),
            store_dir: coven_foundation::store_dir::StoreDir::new_ephemeral(temp.path()),
            _temp: temp,
        }
    }

    /// Where the restored database goes. The file is the restore's to create;
    /// a failed attempt leaves nothing at this path.
    pub(super) fn database_path(&self) -> &std::path::Path {
        &self.database_path
    }

    pub(super) fn store_dir(&self) -> &coven_foundation::store_dir::StoreDir {
        &self.store_dir
    }
}

/// Every file a store directory holds, by path, so a test can say exactly what
/// an attempt added or left alone.
pub(super) fn store_files(
    store_dir: &coven_foundation::store_dir::StoreDir,
) -> std::collections::BTreeSet<std::path::PathBuf> {
    fn walk(
        directory: &std::path::Path,
        found: &mut std::collections::BTreeSet<std::path::PathBuf>,
    ) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries {
            let path = entry.expect("read store directory entry").path();
            if path.is_dir() {
                walk(&path, found);
            } else {
                found.insert(path);
            }
        }
    }
    let mut found = std::collections::BTreeSet::new();
    walk(store_dir.as_ref(), &mut found);
    found
}

/// Asserts that a failed restore left the destination exactly as it found it.
///
/// The payloads a restore installs are rows inside the database it installs
/// them into, so "no database, no SQLite sidecars" is the whole statement: there
/// is nothing else beside it for an abandoned attempt to leave behind.
pub(super) fn assert_restore_left_nothing(
    target: &RestoreTarget,
    before: &std::collections::BTreeSet<std::path::PathBuf>,
) {
    for suffix in ["", "-wal", "-shm"] {
        let path =
            std::path::PathBuf::from(format!("{}{suffix}", target.database_path().display()));
        assert!(
            !path.exists(),
            "a failed restore left {} behind",
            path.display()
        );
    }
    assert_eq!(
        store_files(target.store_dir()),
        *before,
        "a failed restore changed the destination store directory"
    );
}

/// Restores the Store snapshot as `restorer` and installs it into `target`. The
/// preparation is expected to verify; the install outcome is the caller's, since
/// the failure cases are exactly what several of these tests assert on.
pub(super) async fn restore_store_snapshot<'a>(
    store: &'a TestStore,
    db: &Database,
    membership: &coven_protocol::membership::MembershipChain,
    restorer: &UserKeypair,
    target: &'a RestoreTarget,
    device_id: &str,
) -> Result<crate::sync::store::RestoringStore<'a>, crate::sync::store::SnapshotError> {
    store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            db.schema_version(),
            target.database_path(),
            restorer,
        )
        .await
        .expect("restore the Store snapshot")
        .install(
            target.store_dir(),
            circle_routing_tables(),
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            device_id.to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &circle_routing_migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            Some(&EncryptionService::from_key([42; 32])),
        )
        .await
}
