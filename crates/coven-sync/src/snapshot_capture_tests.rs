use crate::sequence_sync::sequence_operations::{
    OperationProgress, OperationStep, SequenceOperation,
};
use crate::sequence_sync::tests::*;
use coven_database::DbError;
use coven_storage::{test_utils::Faults, ObjectPath, StorageFailure};

/// The bytes of the bodies `device` captured to upload, read from
/// `coven_library_bytes`.
pub(crate) async fn captured_bytes(device: &Device) -> i64 {
    device
        .database()
        .read(|sql| {
            Ok::<_, DbError>(sql.query_row(
                "SELECT bytes FROM coven_library_bytes WHERE part='captures'",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap()
}

/// Ana's store snapshot fails on its announcement, so its operation keeps
/// the captured body at the top of `files`. The body counts among the
/// store's own files at its size until the operation ends and deletes it
/// (§17.8).
#[tokio::test]
async fn a_captured_snapshot_counts_until_its_operation_deletes_it() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    assert_eq!(captured_bytes(&ana).await, 0);
    let next = ana.database().applied_slot().await.unwrap() + 1;
    let announcement = ObjectPath::slot(group.store, next.try_into().unwrap());
    group
        .storage
        .set_faults(Faults {
            create_failures: [(announcement, (StorageFailure::ProviderUnavailable, 503))].into(),
            ..Faults::none()
        })
        .await;
    assert!(ana.sync.snapshot(coven_merge::Audience::Store).await.is_err());
    group.storage.set_faults(Faults::none()).await;
    let record = ana.database().operations().await.unwrap().remove(0);
    let work: SequenceOperation = serde_json::from_slice(&record.data).unwrap();
    let OperationProgress::Running(OperationStep::Announcing(artifact)) = work.progress else {
        panic!("the snapshot stopped before its announcement");
    };
    let file = ana.sync.artifact_file(&artifact.capture.name).unwrap();
    assert_eq!(file.open_reader().unwrap().size(), artifact.size);
    assert_eq!(captured_bytes(&ana).await, i64::try_from(artifact.size).unwrap());
    ana.sync.retry_pending_operation(record.id).await.unwrap();
    assert!(file.open_reader().is_err());
    assert_eq!(captured_bytes(&ana).await, 0);
    ana.database().close().await.unwrap();
}
