use super::*;
use coven_database::DbError;
use coven_database::{test_utils, DatabaseBuilder};
use coven_foundation::network_watch::{NetworkSource, NetworkWatch, TestNetwork};
use coven_foundation::{
    clock::{ClockRef, SystemClock},
    files::StoreDirectories,
    id_source::{IdSourceRef, StoreId, UuidIds},
};

/// What `coven_network` shows of the network.
async fn shown(database: &Database) -> (Option<bool>, bool) {
    database
        .read(|sql| {
            Ok::<_, DbError>(sql.query_row(
                "SELECT usable,metered FROM coven_network",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
        })
        .await
        .unwrap()
}

/// Each usable network that appears is one report, and what coven's views
/// show follows the watch; losing the network or a metered flip reports
/// nothing.
#[tokio::test]
async fn each_network_that_appears_is_one_report() {
    let root = tempfile::tempdir().unwrap();
    let ids: IdSourceRef = Arc::new(UuidIds);
    let clock: ClockRef = Arc::new(SystemClock);
    let directory =
        StoreDirectories::new(root.path().to_owned(), coven_foundation::task::HeardBugs::shared())
            .create_store_dir(StoreId(uuid::Uuid::from_u128(1)), ids.as_ref())
            .unwrap();
    let database = DatabaseBuilder::new(directory)
        .id_source(ids)
        .clock(clock.clone())
        .synced_tables(test_utils::notes_tables())
        .migrations(test_utils::notes_migrations())
        .open()
        .await
        .unwrap();
    let waits = Arc::new(RetryWaits::open(database.clone(), clock).await.unwrap());
    let network = TestNetwork::new(true, false);
    let watch = NetworkWatch::start(
        &NetworkSource::Test(network.clone()),
        &(Arc::new(coven_foundation::task::LoggedBugs) as coven_foundation::task::BugSinkRef),
    )
    .unwrap();
    let follow = NetworkFollow::start(watch, waits.clone(), database.clone());
    // The network found at the start ends the waits saved before the store
    // opened, and the views show it at once.
    assert_eq!(waits.network_reported(), 1);
    assert_eq!(shown(&database).await, (Some(true), false));
    let mut reports = waits.network_reports();
    // Each change is followed before the next, so the metered flip and the
    // loss are each seen reporting nothing, and only the join reports.
    network.set_metered(true);
    follow.parked_from_now().await;
    network.lose();
    follow.parked_from_now().await;
    assert_eq!(waits.network_reported(), 1);
    network.join(false);
    reports.changed().await.unwrap();
    assert_eq!(waits.network_reported(), 2);
    assert_eq!(shown(&database).await, (Some(true), false));
    follow.close().await;
    network.join(true);
    assert_eq!(waits.network_reported(), 2, "nothing reports once closed");
    database.close().await.unwrap();
}
