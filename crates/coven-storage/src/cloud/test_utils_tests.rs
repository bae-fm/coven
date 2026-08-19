use super::*;
use crate::cloud::{
    create_exact_bytes, no_progress, BlobBody, CloudHomeJoinInfo, RevokeOutcome,
    PROGRESS_CHUNK_SIZE,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[tokio::test]
async fn write_then_read_roundtrips() {
    let h = InMemoryCloudHome::new();
    h.write(
        "foo",
        BlobBody::from_bytes(b"hello".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();
    assert_eq!(h.read("foo").await.unwrap(), b"hello");
    assert!(h.exists("foo").await.unwrap());
    assert!(!h.exists("bar").await.unwrap());
}

#[tokio::test]
async fn write_reports_progress_in_chunks_reaching_the_total() {
    let h = InMemoryCloudHome::new();
    // Two-and-a-bit chunks so progress fires more than once and the final
    // value equals the total.
    let len = PROGRESS_CHUNK_SIZE * 2 + 7;
    let last = Arc::new(AtomicU64::new(0));
    let ticks = Arc::new(AtomicU64::new(0));
    let last2 = last.clone();
    let ticks2 = ticks.clone();
    let sink: UploadProgress = Arc::new(move |n: u64| {
        last2.store(n, Ordering::Relaxed);
        ticks2.fetch_add(1, Ordering::Relaxed);
    });
    h.write("big", BlobBody::from_bytes(vec![0u8; len]), &sink)
        .await
        .unwrap();
    assert_eq!(last.load(Ordering::Relaxed), len as u64);
    assert_eq!(ticks.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn read_range_returns_a_slice() {
    let h = InMemoryCloudHome::new();
    h.write(
        "k",
        BlobBody::from_bytes(b"0123456789".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();
    assert_eq!(h.read_range("k", 2, 5).await.unwrap(), b"234");
}

#[tokio::test]
async fn list_filters_by_prefix() {
    let h = InMemoryCloudHome::new();
    h.write("a/x", BlobBody::from_bytes(vec![1]), &no_progress())
        .await
        .unwrap();
    h.write("a/y", BlobBody::from_bytes(vec![2]), &no_progress())
        .await
        .unwrap();
    h.write("b/x", BlobBody::from_bytes(vec![3]), &no_progress())
        .await
        .unwrap();
    let mut got = h.list("a/").await.unwrap();
    got.sort();
    assert_eq!(got, vec!["a/x".to_string(), "a/y".to_string()]);
}

#[tokio::test]
async fn delete_removes_and_records() {
    let h = InMemoryCloudHome::new();
    h.write("k", BlobBody::from_bytes(vec![1]), &no_progress())
        .await
        .unwrap();
    h.delete("k").await.unwrap();
    assert!(matches!(
        h.read("k").await,
        Err(CloudHomeError::NotFound(_))
    ));
    assert_eq!(h.deletes_seen(), vec!["k".to_string()]);
}

#[tokio::test]
async fn arm_write_failures_fails_writes_after_arming() {
    let h = InMemoryCloudHome::new();
    // Writes land before arming.
    h.write("before", BlobBody::from_bytes(vec![1]), &no_progress())
        .await
        .unwrap();

    h.arm_write_failures();
    let err = h
        .write("after", BlobBody::from_bytes(vec![2]), &no_progress())
        .await
        .unwrap_err();
    assert!(matches!(err, CloudHomeError::Transport(_)));
    assert!(err.is_retryable());
    // Nothing was stored for the failed write, and the earlier one survives.
    assert!(h.get("after").is_none());
    assert_eq!(h.get("before"), Some(vec![1]));
}

#[tokio::test]
async fn fail_next_range_reads_fails_the_next_n_then_recovers() {
    let h = InMemoryCloudHome::new();
    h.write(
        "k",
        BlobBody::from_bytes(b"0123456789".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();

    h.fail_next_range_reads(2);
    assert!(matches!(
        h.read_range("k", 0, 4).await,
        Err(CloudHomeError::Transport(_))
    ));
    assert!(matches!(
        h.read_range("k", 0, 4).await,
        Err(CloudHomeError::Transport(_))
    ));
    // The third serves real bytes — the countdown is spent.
    assert_eq!(h.read_range("k", 0, 4).await.unwrap(), b"0123");
}

#[tokio::test]
async fn remove_drops_a_key_out_of_band() {
    let h = InMemoryCloudHome::new();
    h.write("k", BlobBody::from_bytes(vec![1]), &no_progress())
        .await
        .unwrap();

    h.remove("k");
    assert!(matches!(
        h.read("k").await,
        Err(CloudHomeError::NotFound(_))
    ));
    // Out-of-band removal is not a delete, so it leaves no delete record.
    assert!(h.deletes_seen().is_empty());
}

#[tokio::test]
async fn exact_create_is_visible_before_a_lost_response() {
    let h = InMemoryCloudHome::new();
    let slot = h.allocate_slot("store-v1/test/one.json").await.unwrap();
    let (reached, release) = h.pause_after_exact_create_call(1);
    let writer = h.clone();
    let writer_slot = slot.clone();
    let task = tokio::spawn(async move {
        create_exact_bytes(&writer, &writer_slot, b"first", &no_progress()).await
    });

    reached.notified().await;
    assert_eq!(h.exact_create_count(), 1);
    assert_eq!(h.read_at(&slot).await.unwrap(), b"first");
    release.notify_one();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn exact_create_never_overwrites() {
    let h = InMemoryCloudHome::new();
    let slot = h.allocate_slot("store-v1/test/one.json").await.unwrap();
    create_exact_bytes(&h, &slot, b"winner", &no_progress())
        .await
        .unwrap();

    assert!(matches!(
        create_exact_bytes(&h, &slot, b"loser", &no_progress()).await,
        Err(CloudHomeError::SlotCollision(_))
    ));
    assert_eq!(h.read_at(&slot).await.unwrap(), b"winner");
}

#[tokio::test]
async fn exact_slot_observation_identifies_present_bytes_and_absence() {
    let h = InMemoryCloudHome::new();
    let slot = h
        .allocate_slot("store-v1/test/observed.json")
        .await
        .unwrap();
    assert_eq!(h.observe_at(&slot).await.unwrap(), None);

    create_exact_bytes(&h, &slot, b"observed", &no_progress())
        .await
        .unwrap();

    assert_eq!(
        h.observe_at(&slot).await.unwrap(),
        Some(coven_protocol::objects::ExactObjectRef::new(
            slot,
            8,
            coven_protocol::store_commit::ObjectHash::digest(b"observed"),
        ))
    );
}

#[tokio::test]
async fn exact_slot_delete_confirms_present_and_already_absent_slots() {
    let h = InMemoryCloudHome::new();
    let slot = h.allocate_slot("store-v1/test/deleted.json").await.unwrap();
    h.delete_and_verify_absent(&slot).await.unwrap();

    create_exact_bytes(&h, &slot, b"delete-me", &no_progress())
        .await
        .unwrap();
    h.delete_and_verify_absent(&slot).await.unwrap();

    assert!(matches!(
        h.read_at(&slot).await,
        Err(CloudHomeError::NotFound(_))
    ));
}

#[tokio::test]
async fn google_drive_exact_slots_with_one_logical_key_remain_independent() {
    let h = InMemoryCloudHome::new().with_provider_binding(
        coven_protocol::objects::ResolvedProviderBinding {
            store: coven_protocol::objects::StoreProviderBinding::GoogleDrive {
                corpus: coven_protocol::objects::GoogleDriveCorpus::SharedDrive {
                    drive_id: "drive-id".to_string(),
                    folder_id: "folder-id".to_string(),
                },
            },
            device: coven_protocol::objects::ProviderDeviceBinding {
                principal: coven_protocol::objects::ProviderPrincipalId::GoogleDrive {
                    permission_id: "permission-id".to_string(),
                },
            },
        },
    );
    let first = h.allocate_slot("store-v1/test/one.json").await.unwrap();
    let second = h.allocate_slot("store-v1/test/one.json").await.unwrap();
    assert_ne!(first, second);
    create_exact_bytes(&h, &first, b"first", &no_progress())
        .await
        .unwrap();
    create_exact_bytes(&h, &second, b"second", &no_progress())
        .await
        .unwrap();
    assert_eq!(h.read_at(&first).await.unwrap(), b"first");
    assert_eq!(h.read_at(&second).await.unwrap(), b"second");
    h.delete_at(&first).await.unwrap();
    assert!(matches!(
        h.read_at(&first).await,
        Err(CloudHomeError::NotFound(_))
    ));
    assert_eq!(h.read_at(&second).await.unwrap(), b"second");
}

#[tokio::test]
async fn google_drive_exact_slots_with_different_logical_keys_have_distinct_file_ids() {
    let h = InMemoryCloudHome::new().with_provider_binding(
        coven_protocol::objects::ResolvedProviderBinding {
            store: coven_protocol::objects::StoreProviderBinding::GoogleDrive {
                corpus: coven_protocol::objects::GoogleDriveCorpus::SharedDrive {
                    drive_id: "drive-id".to_string(),
                    folder_id: "folder-id".to_string(),
                },
            },
            device: coven_protocol::objects::ProviderDeviceBinding {
                principal: coven_protocol::objects::ProviderPrincipalId::GoogleDrive {
                    permission_id: "permission-id".to_string(),
                },
            },
        },
    );

    let first = h.allocate_slot("store-v1/test/one.json").await.unwrap();
    let second = h.allocate_slot("store-v1/test/two.json").await.unwrap();

    assert_ne!(first.physical(), second.physical());
}

#[tokio::test]
async fn access_matches_the_in_memory_s3_binding() {
    let h = InMemoryCloudHome::new();
    let desired = CloudAccessState::Present {
        member_pubkey: "member".to_string(),
        provider_account_email: None,
    };

    let first = h.set_access(desired.clone()).await.unwrap();
    let second = h.set_access(desired).await.unwrap();
    let expected = CloudAccessOutcome::Present(CloudHomeJoinInfo::S3 {
        bucket: "in-memory".to_string(),
        region: "test".to_string(),
        endpoint: Some("https://in-memory.invalid".to_string()),
        access_key: "in-memory".to_string(),
        secret_key: "in-memory".to_string(),
        key_prefix: None,
    });
    assert_eq!(first, expected);
    assert_eq!(second, expected);
    assert_eq!(
        h.set_access(CloudAccessState::Absent {
            member_pubkey: "member".to_string(),
            provider_account_email: None,
        })
        .await
        .unwrap(),
        CloudAccessOutcome::Absent(RevokeOutcome::Unsupported)
    );
}
