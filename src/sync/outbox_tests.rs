//! Tests for outbox delete processing and the shared `cloud_outbox` row shape.
//!
//! The upload drain moved to the blob engine ([`crate::blob::upload`]); its tests
//! live in `src/blob/upload_tests.rs`. What remains here is the delete drain
//! (`process_deletes`) and the row-shape contract both operations share (an upload
//! row decodes to an `Upload` carrying its scope, a delete row to a `Delete`).

use super::outbox::process_deletes;
use crate::database::Database;
use crate::storage::cloud::test_utils::InMemoryCloudHome;

const T0: &str = "2024-06-01T00:00:00Z";

/// A `Database` over an in-memory connection with just the bookkeeping tables.
/// The `cloud_outbox` table both operations share is created by `Database::open`.
fn open_outbox_db() -> Database {
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        Vec::new(),
        "test-device".to_string(),
        |_conn| Ok(()),
    )
    .expect("open outbox database");
    db
}

/// An upload row reads back as an `Upload` carrying its scope; a delete row
/// reads back as a `Delete`. The operation-specific fields live in the variant,
/// so a delete has no scope to be `None` — the flat row's unused columns are
/// simply unread.
#[tokio::test]
async fn upload_carries_scope_delete_carries_no_extra_fields() {
    use crate::blob::BlobScope;
    use crate::db::OutboxOperation;

    let db = open_outbox_db();
    db.enqueue_upload("f1", "k-up", None, BlobScope::Master, T0)
        .await
        .expect("enqueue upload");
    db.enqueue_delete("k-del", T0)
        .await
        .expect("enqueue delete");

    let uploads = db.get_pending_cloud_uploads().await.expect("uploads");
    assert_eq!(uploads.len(), 1);
    assert_eq!(
        uploads[0].operation,
        OutboxOperation::Upload {
            file_id: "f1".to_string(),
            source_path: None,
            scope: BlobScope::Master,
        },
        "an upload entry carries its scope in the variant"
    );

    let deletes = db.get_pending_cloud_deletes().await.expect("deletes");
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].operation, OutboxOperation::Delete);
}

/// A queued blob delete is removed from the cloud on the next drain, with no wait
/// on peers. coven deletes eagerly and relies on a trailing peer pulling the
/// row's removal on its own next cycle (and a consumer tolerating a briefly
/// missing blob) rather than holding the delete until every device has synced
/// past it — which only deferred cleanup while letting a departed device wedge
/// deletion forever.
#[tokio::test]
async fn process_deletes_removes_queued_blobs_immediately() {
    let db = open_outbox_db();
    let cloud = InMemoryCloudHome::new();

    db.enqueue_delete("k-del-1", T0)
        .await
        .expect("enqueue delete 1");
    db.enqueue_delete("k-del-2", T0)
        .await
        .expect("enqueue delete 2");

    let n = process_deletes(&db, &cloud).await.expect("deletes");
    assert_eq!(n, 2, "both queued deletes drain in one pass");

    let mut seen = cloud.deletes_seen();
    seen.sort();
    assert_eq!(seen, vec!["k-del-1".to_string(), "k-del-2".to_string()]);
    assert!(
        db.get_pending_cloud_deletes()
            .await
            .expect("pending")
            .is_empty(),
        "drained deletes are removed from the outbox",
    );
}
