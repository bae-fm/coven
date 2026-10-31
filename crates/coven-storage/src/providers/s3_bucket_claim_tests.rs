use super::*;
use crate::bucket_claim::BUCKET_CLAIM;
use coven_foundation::id_source::StoreId;

const OTHER: StoreId = StoreId(uuid::Uuid::from_bytes([0x20; 16]));

fn occupied(error: StorageError) {
    assert_eq!(
        error.failure(),
        StorageFailure::OwnBucketRequired { reason: OwnBucketReason::Occupied }
    );
}

#[tokio::test]
async fn concurrent_bucket_claims_choose_one_store_and_accept_its_retry() {
    for settings in [
        profile::PLAIN,
        S3Settings {
            copy_publication: S3CopyPublication::Every(S3CopyCondition::IfNoneMatch),
            ..profile::PLAIN
        },
    ] {
        let state = Arc::new(Mutex::new(Remote::default()));
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let arrivals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server = TestServer::new(Router::new().fallback({
            let state = state.clone();
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let (state, barrier, arrivals) = (state.clone(), barrier.clone(), arrivals.clone());
                async move {
                    if method == Method::PUT
                        && uri.path() == "/bucket/coven-bucket"
                        && arrivals.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2
                    {
                        barrier.wait().await;
                    }
                    endpoint(State(state), method, uri, headers, body).await
                }
            }
        }))
        .await;
        let first = profile_provider(&server.url, settings);
        let second = StorageConnection::from_provider(first.provider.clone());
        let (a, b) = tokio::join!(first.claim_bucket(STORE_ID), second.claim_bucket(OTHER));
        let winner = match (a, b) {
            (Ok(()), Err(error)) => {
                occupied(error);
                STORE_ID
            }
            (Err(error), Ok(())) => {
                occupied(error);
                OTHER
            }
            results => panic!("exactly one store must pass: {results:?}"),
        };
        first.claim_bucket(winner).await.unwrap();
        first.check_bucket(winner).await.unwrap();
        let loser = if winner == STORE_ID { OTHER } else { STORE_ID };
        occupied(first.check_bucket(loser).await.unwrap_err());
        let remote = state.lock().unwrap();
        assert_eq!(
            remote.objects,
            BTreeMap::from([(BUCKET_CLAIM.into(), winner.to_string().into_bytes()),])
        );
        assert!(
            remote
                .requests
                .iter()
                .all(|(method, uri)| uri.path() != "/bucket/coven-bucket"
                    || *method != Method::DELETE)
        );
        assert_eq!(
            remote.copies,
            if settings.copy_publication.single_request().is_some() { 3 } else { 0 }
        );
    }
}

#[tokio::test]
async fn absent_or_malformed_marker_refuses_reconnect_without_writing() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    for bytes in [
        None,
        Some(Vec::new()),
        Some(vec![1; 4096]),
        Some(format!("{STORE_ID}\n").into_bytes()),
        Some(OTHER.to_string().into_bytes()),
    ] {
        state.lock().unwrap().objects =
            bytes.into_iter().map(|bytes| (BUCKET_CLAIM.into(), bytes)).collect();
        occupied(storage.check_bucket(STORE_ID).await.unwrap_err());
    }
    assert!(state
        .lock()
        .unwrap()
        .requests
        .iter()
        .all(|(method, uri)| *method == Method::GET && uri.path() == "/bucket/coven-bucket"));
}

#[tokio::test]
async fn a_lost_marker_reply_is_settled_by_retry_and_failures_are_preserved() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    state.lock().unwrap().lose_create_reply = true;
    assert_eq!(
        storage.claim_bucket(STORE_ID).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    storage.claim_bucket(STORE_ID).await.unwrap();
    for (code, status, expected) in [
        ("AccessDenied", 403, StorageFailure::PermissionDenied),
        ("NoSuchBucket", 404, StorageFailure::ContainerNotFound),
        ("ServiceUnavailable", 503, StorageFailure::Network),
    ] {
        state.lock().unwrap().forced_error = Some((code, status));
        assert_eq!(storage.check_bucket(STORE_ID).await.unwrap_err().failure(), expected);
        assert_eq!(storage.claim_bucket(STORE_ID).await.unwrap_err().failure(), expected);
    }
}

#[tokio::test]
#[ignore = "requires COVEN_CREATE_LEAN"]
async fn bucket_claims_match_lean() {
    for copy in [false, true] {
        let state = Arc::new(Mutex::new(Remote::default()));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let settings = S3Settings {
            copy_publication: if copy {
                S3CopyPublication::Every(S3CopyCondition::IfNoneMatch)
            } else {
                S3CopyPublication::Never
            },
            ..profile::PLAIN
        };
        let storage = profile_provider(&server.url, settings);
        let cases = [
            ("reconnect", OTHER),
            ("setup", STORE_ID),
            ("setup", OTHER),
            ("setup", STORE_ID),
            ("reconnect", OTHER),
            ("reconnect", STORE_ID),
        ];
        let input: Vec<_> = cases
            .iter()
            .map(|(op, store)| {
                serde_json::json!({
                    "op": op, "store": store.to_string().into_bytes(),
                })
            })
            .collect();
        let expected =
            crate::providers::tests::create_runner(&serde_json::json!({"bucket": input}));
        let expected = expected.as_array().unwrap();
        assert_eq!(expected.len(), cases.len());
        for ((op, store), expected) in cases.into_iter().zip(expected) {
            let result = match op {
                "setup" => storage.claim_bucket(store).await,
                _ => storage.check_bucket(store).await,
            };
            assert_eq!(expected["accepted"], result.is_ok());
            if let Err(error) = result {
                occupied(error);
            }
            assert_eq!(
                expected["bytes"],
                serde_json::json!(state.lock().unwrap().objects.get(BUCKET_CLAIM))
            );
        }
    }
}

#[tokio::test]
async fn copy_cleanup_failure_cannot_be_hidden_by_a_matching_bucket_marker() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = profile_provider(
        &server.url,
        S3Settings {
            copy_publication: S3CopyPublication::Every(S3CopyCondition::IfNoneMatch),
            ..profile::PLAIN
        },
    );
    storage.claim_bucket(STORE_ID).await.unwrap();
    state.lock().unwrap().refuse_temporary_delete = true;
    let error = storage.claim_bucket(STORE_ID).await.unwrap_err();
    assert!(matches!(error, StorageError::Cleanup { operation, cleanup }
        if operation.failure() == StorageFailure::AlreadyExists
            && cleanup.failure() == StorageFailure::PermissionDenied));
    state.lock().unwrap().refuse_temporary_delete = false;
    storage.claim_bucket(STORE_ID).await.unwrap();
    assert_eq!(state.lock().unwrap().objects.len(), 1);
}
