use crate::*;
use coven_protocol::blob::BLOB_TOMBSTONE_GRACE;

#[tokio::test]
async fn fresh_open_requires_each_make_remote_intent_to_name_retain_pinned() {
    let db = Database::open(
        Path::new(":memory:"),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &[],
    )
    .expect("open database");

    let column = db
        .make_remote_retain_pinned_column_for_test()
        .await
        .expect("read make_remote intent schema")
        .expect("retain_pinned column exists");

    assert_eq!(column.0, 1, "retain_pinned must be NOT NULL");
    assert_eq!(
        column.1, None,
        "retain_pinned must be supplied by every make_remote intent",
    );
}
