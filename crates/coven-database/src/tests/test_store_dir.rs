//! The test fixture's own directory: it belongs to the `StoreDir` the fixture
//! hands back, so a test's payload files outlive every database opened over
//! them and go when the test does.

use crate::synthetic_store::{test_migrations, test_store_dir, test_synced_tables};
use crate::{CovenMigrationPolicy, Database};
use coven_foundation::store_dir::StoreDir;

fn open_over(store_dir: &StoreDir) -> Database {
    Database::open_in_store_dir_for_test(
        &store_dir.db_path(),
        store_dir.clone(),
        test_synced_tables(),
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        CovenMigrationPolicy::ApplyPending,
        &test_migrations(),
    )
    .expect("open a database in the fixture's store directory")
}

/// Closing a database leaves the fixture's directory exactly where it was —
/// the same files a reopen reads back — and only dropping the fixture itself
/// takes the tree, so a test run leaves no store directory behind.
#[test]
fn the_fixture_directory_survives_a_close_and_reopen_and_goes_with_the_fixture() {
    let store_dir = test_store_dir();
    let path = store_dir.to_path_buf();
    let payload = path.join("payload");

    let database = open_over(&store_dir);
    std::fs::write(&payload, b"written before the first close").expect("write under the store");
    drop(database);
    assert!(
        store_dir.db_path().is_file() && payload.is_file(),
        "a closed database leaves the fixture's directory and its files in place",
    );

    let reopened = open_over(&store_dir);
    assert_eq!(
        std::fs::read(&payload).expect("read across the reopen"),
        b"written before the first close",
        "the reopened database reads the same files the first one wrote",
    );
    drop(reopened);
    assert!(path.is_dir(), "the fixture still holds its directory");

    drop(store_dir);
    assert!(
        !path.exists(),
        "dropping the fixture takes the directory and everything under it",
    );
}
