use super::*;
fn config() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "test".into(),
        region: "us-east-1".into(),
        endpoint: None,
        prefix: "store".into(),
    }
}
#[tokio::test]
async fn memory_conforms() {
    Conformance::new(Arc::new(MemoryStorage::new(config()).unwrap()))
        .run()
        .await
        .unwrap();
}
#[tokio::test]
async fn crash_after_accepted_part_and_dropped_part_resumes() {
    let provider = MemoryStorage::new(config()).unwrap();
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xaa; 16],
    )));
    let mut session = provider.begin_upload(&path, 10).await.unwrap();
    provider.upload_part(&mut session, b"abcd").await.unwrap();
    let recorded = session.encode().unwrap();
    provider
        .set_faults(Faults {
            lose_part_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(provider.upload_part(&mut session, b"efgh").await.is_err());
    assert_eq!(session.confirmed_bytes(), 4);
    drop(session);
    let reopened = provider.clone();
    let mut session = UploadSession::decode(recorded.as_bytes()).unwrap();
    reopened.resume_upload(&mut session).await.unwrap();
    assert_eq!(session.confirmed_bytes(), 8);
    reopened
        .set_faults(Faults {
            drop_part: true,
            ..Faults::none()
        })
        .await;
    assert!(reopened.upload_part(&mut session, b"ij").await.is_err());
    reopened.resume_upload(&mut session).await.unwrap();
    assert_eq!(session.confirmed_bytes(), 8);
    reopened.upload_part(&mut session, b"ij").await.unwrap();
    let before_finish = session.encode().unwrap();
    reopened.finish_upload(&mut session).await.unwrap();
    let mut lost_finish = UploadSession::decode(before_finish.as_bytes()).unwrap();
    reopened.finish_upload(&mut lost_finish).await.unwrap();
    assert_eq!(reopened.read(&path).await.unwrap(), b"abcdefghij");
}
#[tokio::test]
async fn setup_refuses_other_store_and_retries_its_own_entry() {
    let provider = MemoryStorage::new(config()).unwrap();
    let path = ObjectPath::store_log(
        coven_foundation::id_source::DeviceId(1),
        std::num::NonZeroU64::MIN,
    );
    provider
        .setup(&path, b"encrypted first entry")
        .await
        .unwrap();
    provider
        .setup(&path, b"encrypted first entry")
        .await
        .unwrap();
    assert_eq!(
        provider
            .setup(&path, b"other store")
            .await
            .unwrap_err()
            .failure(),
        StorageSetupFailure::LocationOccupied
    );
}
#[tokio::test(start_paused = true)]
async fn faults_are_counted_and_delay_is_awaited() {
    let provider = MemoryStorage::new(config()).unwrap();
    provider
        .set_faults(Faults {
            fail_next: 2,
            failure: StorageFailure::QuotaExceeded,
            delay: Duration::from_secs(3),
            ..Faults::none()
        })
        .await;
    let start = tokio::time::Instant::now();
    for _ in 0..2 {
        assert_eq!(
            provider
                .list(&ObjectPrefix::all())
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::QuotaExceeded
        );
    }
    assert!(provider
        .list(&ObjectPrefix::all())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(start.elapsed(), Duration::from_secs(9));
}
