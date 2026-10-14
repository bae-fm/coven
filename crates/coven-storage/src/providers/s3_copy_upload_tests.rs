use super::*;

#[tokio::test]
#[ignore]
async fn lean_differential_s3_create() {
    for profile in profile::PROFILES {
        for limit in [3, 5 * 1024 * 1024 * 1024] {
            let state = Arc::new(Mutex::new(Remote {
                gcs: profile.settings.signing == S3Signing::GoogleHmac,
                ..Remote::default()
            }));
            let server =
                TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
            let mut storage = profile_provider(&server.url, profile.settings);
            Arc::get_mut(&mut storage.provider)
                .unwrap()
                .single_request_limit = limit;
            // Multipart creates below the limit publish by copy on copy profiles.
            let copy = limit == 3 && profile.settings.multipart.uses_copy();
            crate::providers::tests::compare_create_model(&storage, copy).await;
            assert!(state.lock().unwrap().uploads.is_empty());
            assert_eq!(state.lock().unwrap().objects.len(), 4);
            if copy {
                compare_copy_race(&storage, &state).await;
            }
        }
    }
}

/// Racing sessions on one name, each with its temporary assembled, publish
/// in a chosen order: compared with `CopyPublication.run` (`race_one_winner`).
async fn compare_copy_race(storage: &StorageConnection<S3Storage>, state: &Arc<Mutex<Remote>>) {
    for (number, order) in [(5u64, [1usize, 0, 2]), (6, [2, 1, 0])] {
        let path = ObjectPath::device_log(
            STORE_ID,
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::new(number).unwrap(),
        );
        let payloads: Vec<Vec<u8>> = (0..3u8).map(|i| vec![10 * i + 1; 4]).collect();
        let mut sessions = Vec::new();
        let mut ops = Vec::new();
        for (index, bytes) in payloads.iter().enumerate() {
            let mut session = storage.begin_upload(&path, 4).await.unwrap();
            storage.upload_part(&mut session, bytes).await.unwrap();
            sessions.push(session);
            ops.push(serde_json::json!({"op": "stage", "session": index, "bytes": bytes}));
        }
        let mut outcomes = Vec::new();
        for index in order {
            ops.push(serde_json::json!({"op": "publish", "session": index, "path": number}));
            outcomes.push(match storage.finish_upload(&mut sessions[index]).await {
                Ok(()) => "copied",
                Err(error) if error.failure() == StorageFailure::AlreadyExists => "refused",
                Err(error) => panic!("copy race failed: {error:?}"),
            });
        }
        let expected = crate::providers::tests::create_runner(&serde_json::json!({ "ops": ops }));
        assert_eq!(serde_json::json!(outcomes), expected["outcomes"]);
        let stored = storage.read(&path).await.unwrap();
        assert_eq!(serde_json::json!(stored), expected["objects"][0]["bytes"]);
        let temporaries = state
            .lock()
            .unwrap()
            .objects
            .keys()
            .filter(|key| key.contains(".coven-uploads/"))
            .count();
        assert_eq!(serde_json::json!(temporaries), expected["staged"]);
        for session in &sessions {
            storage.abort_upload(session).await.unwrap();
        }
        assert_eq!(storage.read(&path).await.unwrap(), stored);
    }
}

pub(super) fn gcs_provider(url: &str) -> StorageConnection<S3Storage> {
    let mut storage = S3Storage::new(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "auto".into(),
            endpoint: Some("https://storage.googleapis.com".parse().unwrap()),
            profile: None,
            prefix: "store".into(),
        },
        S3Credentials {
            access_key_id: "access".into(),
            secret_access_key: SecretText::new("secret".into()),
        },
        Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
        Arc::new(SequentialIds::new()),
    )
    .unwrap();
    storage.client = Client::from_conf(
        storage
            .client
            .config()
            .to_builder()
            .endpoint_url(url)
            .build(),
    );
    StorageConnection::from_provider(Arc::new(storage))
}

#[tokio::test]
async fn gcs_create_preserves_an_occupied_path() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = gcs_provider(&server.url);
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    storage.create(&path, b"original").await.unwrap();
    let result = storage.create(&path, b"replacement").await;
    assert!(matches!(result, Err(error) if error.failure() == StorageFailure::AlreadyExists));
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
    check_provider(
        &storage,
        &ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(32)),
        b"sealed check bytes",
    )
    .await
    .unwrap();
}

#[test]
fn profile_uses_the_endpoint_host_exactly() {
    let aws = Some(profile::PROFILES[0].settings);
    let gcs = Some(profile::PROFILES[1].settings);
    assert_eq!(profile::settings(None, None).unwrap(), aws);
    for (url, expected) in [
        ("https://storage.googleapis.com", gcs),
        ("https://STORAGE.GOOGLEAPIS.COM/", gcs),
        ("https://s3.us-east-1.amazonaws.com", aws),
        ("https://storage.googleapis.com.attacker.test", None),
        ("https://other.test/storage.googleapis.com", None),
    ] {
        assert_eq!(
            profile::settings(Some(&url.parse().unwrap()), None).unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn gcs_precondition_status_is_already_exists_without_an_error_code() {
    for body in [
        "",
        "<Error><Code></Code></Error>",
        "<Error><Code>PreconditionFailed</Code></Error>",
    ] {
        let server =
            TestServer::new(Router::new().fallback(move || async move { response(412, body) }))
                .await;
        let path = ObjectPath::device_log(
            STORE_ID,
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::MIN,
        );
        let error = gcs_provider(&server.url)
            .create(&path, b"bytes")
            .await
            .unwrap_err();
        assert_eq!(error.failure(), StorageFailure::AlreadyExists);
        assert!(matches!(error, StorageError::Provider { .. }));
    }
}

#[tokio::test]
async fn gcs_multipart_copy_preserves_occupied_bytes_and_removes_temporaries() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let mut storage = gcs_provider(&server.url);
    Arc::get_mut(&mut storage.provider)
        .unwrap()
        .single_request_limit = 3;
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    storage.create(&path, b"original").await.unwrap();
    let result = storage.create(&path, b"replacement").await;
    assert!(matches!(result, Err(error) if error.failure() == StorageFailure::AlreadyExists));
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
    assert_eq!(
        storage
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap()
            .len(),
        1
    );
    let state = state.lock().unwrap();
    assert_eq!(state.copies, 2);
    assert!(state.uploads.is_empty());
    assert_eq!(
        state.objects.keys().collect::<Vec<_>>(),
        vec![&path.under("store")]
    );
}

#[tokio::test]
async fn gcs_resume_reuses_its_temporary_after_assembly_copy_or_delete_reply_loss() {
    for lost in ["assembly", "copy", "delete_refused", "delete_reply"] {
        let state = Arc::new(Mutex::new(Remote {
            gcs: true,
            ..Remote::default()
        }));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = gcs_provider(&server.url);
        let path = ObjectPath::device_log(
            STORE_ID,
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::MIN,
        );
        let mut session = storage.begin_upload(&path, 4).await.unwrap();
        let recorded = session.encode().unwrap();
        let temporary = storage.provider.session_key(&session).unwrap();
        assert!(temporary.starts_with(&format!("store/{STORE_ID}/.coven-uploads/")));
        assert!(ObjectPath::parse(temporary.strip_prefix("store/").unwrap()).is_err());
        storage.upload_part(&mut session, b"data").await.unwrap();
        {
            let mut state = state.lock().unwrap();
            state.fail_completion_reply = lost == "assembly";
            state.fail_copy_reply = lost == "copy";
            state.refuse_temporary_delete = lost == "delete_refused";
            state.lose_temporary_delete_reply = lost == "delete_reply";
        }
        assert!(storage.finish_upload(&mut session).await.is_err());
        assert!(!session.is_complete());
        assert_eq!(
            state.lock().unwrap().objects.contains_key(&temporary),
            lost != "delete_reply"
        );
        // Listing a store must not treat an unfinished temporary as published bytes.
        let listed = storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
        assert_eq!(listed.len(), usize::from(lost != "assembly"));
        state.lock().unwrap().refuse_temporary_delete = false;
        let mut resumed = UploadSession::decode(recorded.as_bytes()).unwrap();
        assert_eq!(storage.provider.session_key(&resumed).unwrap(), temporary);
        storage.resume_upload(&mut resumed).await.unwrap();
        assert!(resumed.is_complete());
        assert_eq!(resumed.confirmed_bytes(), 4);
        assert_eq!(storage.read(&path).await.unwrap(), b"data");
        assert!(!state.lock().unwrap().objects.contains_key(&temporary));
        let copies = state.lock().unwrap().copies;
        storage.finish_upload(&mut resumed).await.unwrap();
        assert_eq!(state.lock().unwrap().copies, copies);
        // The recording may precede both publication and deletion.
        let mut stale = UploadSession::decode(recorded.as_bytes()).unwrap();
        storage.resume_upload(&mut stale).await.unwrap();
        assert!(stale.is_complete());
    }
}

#[tokio::test]
async fn gcs_abandoned_recording_cleans_assembled_temporary_after_a_crash() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = gcs_provider(&server.url);
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    for assembled in [false, true] {
        let mut session = storage.begin_upload(&path, 4).await.unwrap();
        let recorded = session.encode().unwrap();
        storage.upload_part(&mut session, b"data").await.unwrap();
        if assembled {
            state.lock().unwrap().fail_completion_reply = true;
            assert_eq!(
                storage
                    .finish_upload(&mut session)
                    .await
                    .unwrap_err()
                    .failure(),
                StorageFailure::Network
            );
            assert_eq!(state.lock().unwrap().objects.len(), 1);
        }
        let abandoned = UploadSession::decode(recorded.as_bytes()).unwrap();
        storage.abort_upload(&abandoned).await.unwrap();
        storage.abort_upload(&abandoned).await.unwrap();
        assert!(state.lock().unwrap().uploads.is_empty());
        assert!(state.lock().unwrap().objects.is_empty());
    }
}

#[tokio::test]
async fn gcs_definitive_copy_refusal_deletes_its_temporary() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        refuse_copy: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = gcs_provider(&server.url);
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mut session = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut session, b"data").await.unwrap();
    assert_eq!(
        storage
            .finish_upload(&mut session)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::PermissionDenied
    );
    assert!(!session.is_complete());
    assert!(state.lock().unwrap().objects.is_empty());
    assert!(state.lock().unwrap().uploads.is_empty());
    storage.abort_upload(&session).await.unwrap();
}

#[tokio::test]
async fn gcs_refused_copy_retains_both_failures_and_retry_cleans_the_temporary() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = gcs_provider(&server.url);
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    storage.create(&path, b"original").await.unwrap();
    let mut session = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut session, b"else").await.unwrap();
    state.lock().unwrap().refuse_temporary_delete = true;
    let error = storage.finish_upload(&mut session).await.unwrap_err();
    assert!(matches!(error, StorageError::Cleanup { operation, cleanup }
        if operation.failure() == StorageFailure::AlreadyExists && cleanup.failure() == StorageFailure::PermissionDenied));
    assert!(!session.is_complete());
    state.lock().unwrap().refuse_temporary_delete = false;
    assert_eq!(
        storage
            .resume_upload(&mut session)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(state.lock().unwrap().objects.len(), 1);
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
}

#[tokio::test]
async fn an_occupied_create_does_not_hide_failed_temporary_cleanup() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let mut storage = gcs_provider(&server.url);
    Arc::get_mut(&mut storage.provider)
        .unwrap()
        .single_request_limit = 3;
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    storage.create(&path, b"original").await.unwrap();
    state.lock().unwrap().refuse_temporary_delete = true;
    // A failed cleanup is never a plain occupied path a caller may compare.
    assert!(matches!(
        storage.create(&path, b"replacement").await,
        Err(StorageError::Cleanup { .. })
    ));
}

/// One `CopyObject` copies at most 5 GiB, so an S3 copy profile refuses a
/// larger upload before any request; GCS's copy has no such limit (§4).
#[tokio::test]
async fn s3_copy_profiles_refuse_uploads_above_the_single_copy_limit() {
    let limit = 5 * 1024 * 1024 * 1024;
    for profile in profile::PROFILES {
        let state = Arc::new(Mutex::new(Remote {
            gcs: profile.settings.signing == S3Signing::GoogleHmac,
            ..Remote::default()
        }));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = profile_provider(&server.url, profile.settings);
        let path = ObjectPath::device_log(
            STORE_ID,
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::MIN,
        );
        let result = storage.begin_upload(&path, limit + 1).await;
        let limited = matches!(
            profile.settings.multipart,
            S3Multipart::Copy(
                S3CopyCondition::IfNoneMatch | S3CopyCondition::CloudflareIfNoneMatch
            )
        );
        if limited {
            assert!(matches!(
                result,
                Err(error) if error.failure() == StorageFailure::CopyTooLarge { size: limit + 1, limit }
            ));
            assert!(state.lock().unwrap().requests.is_empty());
        } else {
            let session = result.unwrap();
            storage.abort_upload(&session).await.unwrap();
        }
        assert!(storage.begin_upload(&path, limit).await.is_ok() || !limited);
    }
    assert!(matches!(
        profile::CataloguedProfile::named(S3ProfileName::SeaweedFs)
            .settings
            .multipart,
        S3Multipart::Copy(S3CopyCondition::IfNoneMatch)
    ));
}
