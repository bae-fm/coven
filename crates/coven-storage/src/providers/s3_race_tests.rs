use super::*;

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
    assert!(matches!(first, Write::Copy { .. }) && matches!(other, Write::Copy { .. }));
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

/// On a profile publishing every object by copy, a single-request writer
/// races as that profile publishes: a staged temporary and its conditional
/// copy, never a conditional PUT, so a mixed race is copy against copy.
#[tokio::test]
async fn every_object_race_stages_single_requests_and_races_only_copies() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let s = profile_provider(
        &server.url,
        CataloguedProfile::named(S3ProfileName::SeaweedFs).settings,
    );
    let target = path(STORE_ID, 1);
    let staged = single(&s.provider, &target, b"single").await.unwrap();
    let assembled = prepare(&s, &target, b"multipart").await.unwrap();
    assert!(matches!(staged, Write::Copy { .. }) && matches!(assembled, Write::Copy { .. }));
    assert_eq!(state.lock().unwrap().copies, 0);
    assert!(s.status(&target).await.unwrap().is_none());
    fire(&s.provider, &target, b"single".to_vec(), staged)
        .await
        .unwrap();
    assert_eq!(
        fire(&s.provider, &target, b"multipart".to_vec(), assembled)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(state.lock().unwrap().copies, 2);
    assert_eq!(s.read(&target).await.unwrap(), b"single");
    let plain = profile_provider(&server.url, profile::PLAIN);
    assert!(matches!(
        single(&plain.provider, &path(STORE_ID, 2), b"put")
            .await
            .unwrap(),
        Write::Put {
            header: "if-none-match",
            ..
        }
    ));
}
