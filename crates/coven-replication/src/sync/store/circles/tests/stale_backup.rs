//! A device restored from a file-level backup of its own database.
//!
//! Unlike Owner recovery, this device keeps its registration and therefore its
//! Store and Circle author streams. Its database is simply behind: an older
//! Store tip, an older accepted publication record, and an older Circle
//! frontier. The question these tests answer is whether it can fill an author
//! stream position the live history has already filled.

use super::*;

/// The store directory's root. `StoreDir` names the files it owns rather than
/// the directory holding them, and a backup copies the directory.
fn store_root(dir: &coven_foundation::store_dir::StoreDir) -> std::path::PathBuf {
    dir.db_path()
        .parent()
        .expect("the store database lives inside its store directory")
        .to_path_buf()
}

/// Copy a store directory's whole tree, as a file-level backup of a device
/// takes the database and everything beside it.
fn copy_store_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).expect("create the backup directory");
    for entry in std::fs::read_dir(from).expect("read the store directory") {
        let entry = entry.expect("store directory entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("entry file type").is_dir() {
            copy_store_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy one store file");
        }
    }
}

/// Replace a store directory's tree with a previously taken copy.
fn restore_store_tree(backup: &std::path::Path, into: &std::path::Path) {
    std::fs::remove_dir_all(into).expect("clear the store directory being rolled back");
    copy_store_tree(backup, into);
}

/// A single Owner device with a Circle, a file-level backup of its store
/// directory taken before it published a rename, and the rename published
/// after. Rolling the directory back reproduces a device restored from a stale
/// backup: same registration, same author streams, older everything.
struct BackedUpDevice {
    dir: coven_foundation::store_dir::StoreDir,
    store: std::sync::Arc<TestStore>,
    signer: UserKeypair,
    circle_id: CircleId,
    backup: tempfile::TempDir,
}

impl BackedUpDevice {
    fn open(&self) -> Database {
        open_file_backed(&self.dir)
    }

    async fn store(&self, db: &Database) -> crate::sync::test_helpers::TestDevice {
        self.store
            .bind_device_in(db, self.dir.clone(), &self.signer)
            .await
            .expect("bind the device")
    }

    async fn build(label: &str) -> Self {
        let dir = crate::sync::test_helpers::test_store_dir();
        let signer = UserKeypair::generate();
        let db = open_file_backed(&dir);
        let store = TestStore::create(
            &db,
            dir.clone(),
            label,
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create the Owner's Store");
        let circle_id = {
            let device = store
                .bind_device_in(&db, dir.clone(), &signer)
                .await
                .expect("bind the device before its backup");
            device
                .create_circle("0000000001000-0000-owner", "Household")
                .await
                .expect("publish the founder Circle")
        };
        drop(db);
        let backup = tempfile::tempdir().expect("backup directory");
        copy_store_tree(&store_root(&dir), backup.path());
        Self {
            dir,
            store,
            signer,
            circle_id,
            backup,
        }
    }

    /// Roll the store directory back to the copy taken at `build`.
    fn roll_back(&self) {
        restore_store_tree(self.backup.path(), &store_root(&self.dir));
    }

    /// The Circle's current control and the metadata entry coordinates its
    /// activation carries, as this device's database holds them.
    async fn metadata_positions(
        &self,
        db: &Database,
    ) -> Vec<coven_protocol::circle::CircleMetadataCoord> {
        let database = StoreDatabase::new(db);
        let (current, _) = database
            .circle_authoring_context(self.circle_id, &keys::public_key_hex(&self.signer))
            .await
            .expect("read the Circle's current control");
        let (activation, _) = database
            .verified_circle_activation_context(
                self.store.root().clone(),
                self.circle_id,
                current.control.coord.clone(),
            )
            .await
            .expect("read the current activation")
            .expect("the current control is retained");
        activation
            .reference
            .objects()
            .metadata_entries
            .keys()
            .cloned()
            .collect()
    }
}

fn open_file_backed(dir: &coven_foundation::store_dir::StoreDir) -> Database {
    coven_database::Database::open_with_hlc_in_store_dir_for_test(
        &dir.db_path(),
        dir.clone(),
        crate::sync::test_helpers::test_synced_tables(),
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        std::sync::Arc::new(
            coven_protocol::hlc::Hlc::try_new(
                "stale-backup-device".to_string(),
                std::sync::Arc::new(coven_foundation::clock::SystemClock),
            )
            .expect("create the register clock"),
        ),
        coven_database::CovenMigrationPolicy::ApplyPending,
        &crate::sync::test_helpers::test_migrations(),
    )
    .expect("open the device's file-backed database")
}

/// A device rolled back to a stale backup cannot fill a Circle author-stream
/// position the live history has already filled.
///
/// Its local accepted publication record is behind the live one, and the record
/// only ever advances through a pull that materializes the interval's commits,
/// so the publish path refuses to reach its conditional write until it has
/// installed the newer history. Once it has, its own Circle frontier includes
/// the entry it was about to duplicate, and its next position is fresh.
#[tokio::test]
async fn a_stale_backup_cannot_fill_a_circle_position_the_live_history_filled() {
    let fixture = BackedUpDevice::build("stale-backup-circle-position").await;

    // The live history fills the device's next Circle metadata position.
    let filled = {
        let db = fixture.open();
        fixture
            .store
            .bind_device_in(&db, fixture.dir.clone(), &fixture.signer)
            .await
            .expect("bind the device")
            .rename_circle("0000000002000-0000-owner", fixture.circle_id, "Alpha")
            .await
            .expect("publish the rename that fills the position");
        let positions = fixture.metadata_positions(&db).await;
        drop(db);
        positions
    };
    assert_eq!(filled.len(), 2, "founder plus the rename: {filled:?}");
    let alpha = filled
        .iter()
        .max_by_key(|coord| coord.seq)
        .expect("the rename's metadata coordinate")
        .clone();

    // The device is restored from its backup: it has never seen the rename.
    fixture.roll_back();
    let db = fixture.open();
    let stale = fixture.metadata_positions(&db).await;
    assert_eq!(stale.len(), 1, "the backup predates the rename: {stale:?}");

    // It authors its own rename, which would take the position the live
    // history already filled.
    let outcome = fixture
        .store
        .bind_device_in(&db, fixture.dir.clone(), &fixture.signer)
        .await
        .expect("bind the rolled-back device")
        .rename_circle("0000000003000-0000-owner", fixture.circle_id, "Beta")
        .await;

    // It is refused at publish, by name. Reaching its conditional write means
    // first installing the live accepted record, and that install materializes
    // the commit already standing at its own Store coordinate.
    let refusal = outcome.expect_err("a stale backup cannot publish over live history");
    assert!(
        format!("{refusal}")
            .contains("Store commit coordinate is installed with another exact commit"),
        "{refusal:?}"
    );

    let after = fixture.metadata_positions(&db).await;
    assert!(
        after.contains(&alpha),
        "the position the live history filled still holds its entry: {after:?}"
    );
    let duplicates = after
        .iter()
        .filter(|coord| coord.stream_key() == alpha.stream_key() && coord.seq == alpha.seq)
        .count();
    assert_eq!(
        duplicates, 1,
        "nothing is accepted at the filled position: {after:?}"
    );

    // The forced pull left the device holding the live entry, so it is not
    // conflicted and its next Circle position is fresh.
    assert!(
        StoreDatabase::new(&db)
            .circle_control_conflict_branches(fixture.circle_id)
            .await
            .expect("read the rolled-back device's conflict state")
            .is_none(),
        "the refusal leaves no fork behind"
    );

    // The refusal released the Store publication reservation it held, so the
    // device is not stuck behind a publication that can never happen: its next
    // command publishes. (This Circle's own next command is a separate matter —
    // the refused operation is still its one in-flight operation.)
    fixture
        .store
        .bind_device_in(&db, fixture.dir.clone(), &fixture.signer)
        .await
        .expect("bind the rolled-back device")
        .create_circle("0000000004000-0000-owner", "Allotment")
        .await
        .expect("the device publishes again once the refusal releases its reservation");

    // The coordinate that refused it is also what settles it: accepted history
    // holds a different commit there, so the operation is discardable and the
    // Circle takes commands again.
    let refused_operation = StoreDatabase::new(&db)
        .blocked_circle_operation_ids()
        .await
        .expect("read the blocked Circle operations")
        .into_iter()
        .next()
        .expect("the refused operation is journaled");

    // And the refused operation is still there to report, blocked with why.
    let blocked = StoreDatabase::new(&db)
        .blocked_circle_operations()
        .await
        .expect("read the blocked Circle operations")
        .into_iter()
        .next()
        .expect("the refused operation stays journaled and blocked");
    assert!(
        matches!(
            &blocked,
            coven_protocol::circle::CircleOperationBlock::PublicationRefused { reason }
                if reason.contains("Store commit coordinate is installed with another exact commit")
        ),
        "{blocked:?}"
    );

    fixture
        .store(&db)
        .await
        .circles()
        .discard_circle_operation(&refused_operation)
        .await
        .expect("a candidate whose coordinate is taken is discardable");
    fixture
        .store(&db)
        .await
        .circles()
        .rename_circle("0000000005000-0000-owner", fixture.circle_id, "Delta")
        .await
        .expect("the Circle takes commands again once the refusal is discarded");
}
