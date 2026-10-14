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
}

#[tokio::test]
async fn gcs_race_prepares_temporaries_then_sends_conditional_publication_copies() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let s = profile_provider(
        &server.url,
        CataloguedProfile::named(S3ProfileName::Gcs).settings,
    );
    let target = path(STORE_ID, 1);
    let first = prepare(&s, &target, b"first").await.unwrap();
    let other = prepare(&s, &target, b"other").await.unwrap();
    assert!(matches!(first, Write::Copy(_)) && matches!(other, Write::Copy(_)));
    {
        let state = state.lock().unwrap();
        assert!(state.uploads.is_empty());
        assert_eq!(state.objects.len(), 2);
        assert!(!state.objects.contains_key(&target.under("store")));
        assert_eq!(state.copies, 0);
    }
    fire(&s.provider, &target, b"first".to_vec(), first)
        .await
        .unwrap();
    assert_eq!(
        fire(&s.provider, &target, b"other".to_vec(), other)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(state.lock().unwrap().copies, 2);
    assert_eq!(s.read(&target).await.unwrap(), b"first");
}
