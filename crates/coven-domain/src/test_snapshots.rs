//! Publishing the Store snapshot a joining or restoring device installs.
//!
//! A snapshot's coverage is the frontier the image was captured over, and it is
//! what tells the installing device which history the image already holds. A
//! fixture that publishes an image over real history while declaring an empty
//! coverage is publishing a lie: every device that installs it then re-resolves
//! the whole history from the cloud, and no test built on that fixture can see
//! the difference. Both flows publish through here so neither can.

use coven_protocol::store_commit::{CommitFrontier, StoreRootRef};
use coven_replication::sync::test_helpers::TestDevice;

/// The frontier an image captured from this database covers.
pub(crate) async fn captured_coverage(database: &coven_database::StoreDatabase) -> CommitFrontier {
    CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("read the captured materialized frontier"),
    )
    .expect("the materialized frontier is a commit frontier")
}

/// Capture one Store snapshot, publish it over the history it covers, and
/// acknowledge it — the state an installing device finds once the owner's
/// snapshot cadence has run.
pub(crate) async fn publish_owner_snapshot(
    owner_device: &TestDevice,
    database: &coven_database::StoreDatabase,
    root: StoreRootRef,
    snapshot_dir: &std::path::Path,
) {
    let image = database
        .capture_snapshot_image_for_test(root, snapshot_dir.to_path_buf(), None)
        .await
        .expect("capture the Store snapshot image");
    let coverage = captured_coverage(database).await;
    owner_device
        .publish_snapshot(image, coverage.clone())
        .await
        .expect("publish the Store snapshot");
    owner_device
        .publish_acknowledgement(coverage)
        .await
        .expect("publish the Store snapshot acknowledgement");
}
