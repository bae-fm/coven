use super::*;

#[test]
fn judge_rejects_double_success_wrong_bytes_unexpected_failures_and_disjoint_requests() {
    let payloads = vec![b"first".to_vec(), b"other".to_vec()];
    let replies = || {
        vec![
            Reply {
                writer: 0,
                start: Duration::ZERO,
                end: Duration::from_millis(4),
                result: Ok(()),
            },
            Reply {
                writer: 1,
                start: Duration::from_millis(1),
                end: Duration::from_millis(5),
                result: Err(StorageFailure::AlreadyExists.into()),
            },
        ]
    };
    assert!(judge(&replies(), &payloads, b"first"));
    assert!(!judge(&replies(), &payloads, b"other"));
    let mut both = replies();
    both[1].result = Ok(());
    assert!(!judge(&both, &payloads, b"other"));
    let mut neither = replies();
    neither[0].result = Err(StorageFailure::AlreadyExists.into());
    assert!(!judge(&neither, &payloads, b"first"));
    let mut disjoint = replies();
    disjoint[1].start = disjoint[0].end;
    assert!(!judge(&disjoint, &payloads, b"first"));
    let mut network = replies();
    network[1].result = Err(StorageFailure::Network.into());
    assert!(!judge(&network, &payloads, b"first"));
    let mut cleanup = replies();
    cleanup[1].result = Err(StorageError::Cleanup {
        operation: Box::new(StorageFailure::AlreadyExists.into()),
        cleanup: Box::new(StorageFailure::Network.into()),
    });
    assert!(!judge(&cleanup, &payloads, b"first"));
}

#[tokio::test]
async fn lost_replies_are_settled_through_the_storage_connection() {
    let storage = crate::test_utils::MemoryStorage::builder().build().unwrap();
    lost_replies(&storage, &storage, crate::test_utils::STORE_ID)
        .await
        .unwrap();
}
