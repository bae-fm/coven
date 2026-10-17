use super::*;

#[tokio::test]
async fn temporary_identity_is_recordable_before_any_provider_request() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let upload = storage.begin_upload(&path(), 4).await.unwrap();
    assert!(state.lock().unwrap().requests.is_empty());
    let recorded = upload.encode().unwrap();
    let restored = UploadSession::decode(recorded.as_bytes()).unwrap();
    assert_eq!(temporary_path(&restored), temporary_path(&upload));
}

#[tokio::test]
async fn lost_initiation_reply_keeps_the_recorded_temporary_for_retry() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
    let recorded = upload.encode().unwrap();
    let temporary = temporary_path(&upload);
    state.lock().unwrap().lose_initiation_reply = true;
    assert_eq!(
        storage
            .upload_part(&mut upload, b"data")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
    assert_eq!(state.lock().unwrap().session_starts, 1);
    let mut restored = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut restored).await.unwrap();
    assert_eq!(temporary_path(&restored), temporary);
    storage.upload_part(&mut restored, b"data").await.unwrap();
    storage.finish_upload(&mut restored).await.unwrap();
    assert_eq!(storage.read(&path()).await.unwrap(), b"data");
    let state = state.lock().unwrap();
    assert!(!state.files.contains_key(&temporary));
    // A lost capability can leave an empty native session until expiry, but
    // every attempt names the recorded temporary and cannot publish a slot.
    assert!(state
        .uploads
        .values()
        .all(|(name, bytes)| name == &temporary && bytes.is_empty()));
}

#[tokio::test]
#[ignore = "requires COVEN_CREATE_LEAN"]
async fn lean_differential_onedrive_records_before_initiation() {
    let expected = crate::providers::tests::create_runner(&json!({"recordings": [
        {"op": "record", "session": 1}, {"op": "initiate", "session": 1}
    ]}));
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
    let recorded = upload.encode().unwrap();
    assert_eq!(expected[0]["recorded"], !recorded.as_bytes().is_empty());
    assert_eq!(
        expected[0]["staged"],
        !state.lock().unwrap().uploads.is_empty()
    );
    state.lock().unwrap().lose_initiation_reply = true;
    assert!(storage.upload_part(&mut upload, b"data").await.is_err());
    let restored = UploadSession::decode(recorded.as_bytes()).unwrap();
    let name = temporary_path(&restored);
    let state = state.lock().unwrap();
    assert_eq!(expected[1]["staged"], !state.uploads.is_empty());
    assert_eq!(
        expected[1]["recorded"],
        state.uploads.values().all(|(path, _)| path == &name)
    );
}

#[tokio::test]
async fn lost_copy_reply_requires_byte_verification() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xff; 16])),
    );
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    let recorded = upload.encode().unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    state.lock().unwrap().fail_copy_reply = true;
    assert!(storage.finish_upload(&mut upload).await.is_err());
    let storage = provider(&server.url);
    let mut upload = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    assert!(!upload.is_complete());
    assert!(matches!(
        storage.upload_part(&mut upload, b"else").await,
        Err(error) if error.failure() == StorageFailure::AlreadyExists
    ));
    assert_eq!(upload.confirmed_bytes(), 0);
    let mut upload = UploadSession::decode(upload.encode().unwrap().as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    assert!(upload.is_complete());
}

fn path() -> ObjectPath {
    ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN)
}
fn temporary_path(upload: &UploadSession) -> String {
    let SessionState::OneDrive { temporary, .. } = &upload.state else {
        panic!("OneDrive session")
    };
    format!("{STORE_ID}/.coven-uploads/{temporary}")
}

#[tokio::test]
async fn resume_keeps_the_temporary_after_final_part_copy_and_cleanup_reply_loss() {
    for (failure, record_error_state) in
        ["part", "copy", "monitor", "delete_refused", "delete_reply"]
            .into_iter()
            .flat_map(|failure| [false, true].map(|record| (failure, record)))
    {
        let state = remote();
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
        let recorded = upload.encode().unwrap();
        let temporary = temporary_path(&upload);
        state.lock().unwrap().fail_reply = failure == "part";
        let part = storage.upload_part(&mut upload, b"data").await;
        if failure == "part" {
            assert_eq!(part.unwrap_err().failure(), StorageFailure::Network);
        } else {
            part.unwrap();
            {
                let mut state = state.lock().unwrap();
                state.fail_copy_reply = failure == "copy";
                state.fail_monitor = failure == "monitor";
                state.refuse_temporary_delete = failure == "delete_refused";
                state.lose_temporary_delete_reply = failure == "delete_reply";
            }
            assert!(storage.finish_upload(&mut upload).await.is_err());
        }
        assert!(!upload.is_complete());
        assert_eq!(
            state.lock().unwrap().files.contains_key(&temporary),
            failure != "delete_reply"
        );
        let listed = storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
        assert_eq!(listed.len(), usize::from(failure != "part"));
        state.lock().unwrap().refuse_temporary_delete = false;
        let latest = upload.encode().unwrap();
        let saved = if record_error_state {
            latest.as_bytes()
        } else {
            recorded.as_bytes()
        };
        let mut resumed = UploadSession::decode(saved).unwrap();
        assert_eq!(temporary_path(&resumed), temporary);
        storage.resume_upload(&mut resumed).await.unwrap();
        if resumed.confirmed_bytes() == 0 {
            storage.upload_part(&mut resumed, b"data").await.unwrap();
        }
        storage.finish_upload(&mut resumed).await.unwrap();
        assert!(resumed.is_complete());
        assert_eq!(storage.read(&path()).await.unwrap(), b"data");
        assert!(!state.lock().unwrap().files.contains_key(&temporary));
        let copies = state.lock().unwrap().copies;
        storage.finish_upload(&mut resumed).await.unwrap();
        assert_eq!(state.lock().unwrap().copies, copies);
        assert_eq!(copies, 1, "resume must not submit another successful copy");
        let mut stale = UploadSession::decode(recorded.as_bytes()).unwrap();
        storage.resume_upload(&mut stale).await.unwrap();
        assert_eq!(stale.confirmed_bytes(), 0);
        storage.upload_part(&mut stale, b"data").await.unwrap();
        assert!(stale.is_complete());
    }
}

#[tokio::test]
async fn abandoned_recording_removes_open_and_assembled_temporaries() {
    for assembled in [false, true] {
        let state = remote();
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        let total = if assembled {
            4
        } else {
            crate::session::ONEDRIVE_PART_SIZE as u64 + 1
        };
        let mut upload = storage.begin_upload(&path(), total).await.unwrap();
        let recorded = upload.encode().unwrap();
        let bytes = vec![7; total.min(upload.part_size() as u64) as usize];
        storage.upload_part(&mut upload, &bytes).await.unwrap();
        assert_eq!(state.lock().unwrap().files.len(), 1);
        assert!(storage.status(&path()).await.unwrap().is_none());
        assert!(storage
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap()
            .is_empty());
        let abandoned = UploadSession::decode(recorded.as_bytes()).unwrap();
        storage.abort_upload(&abandoned).await.unwrap();
        storage.abort_upload(&abandoned).await.unwrap();
        assert!(state.lock().unwrap().files.is_empty());
        // With only the planned record, the native capability was lost. Its
        // unfinished session can expire, but its visible temporary is removed.
        assert!(state
            .lock()
            .unwrap()
            .uploads
            .values()
            .all(|(name, _)| { name == &temporary_path(&abandoned) }));
    }
}

#[tokio::test]
async fn copy_conflict_preserves_destination_and_deletion_failure_stays_visible() {
    for refuse_delete in [false, true] {
        let state = remote();
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        storage.create(&path(), b"keep").await.unwrap();
        let before = storage.status(&path()).await.unwrap().unwrap();
        let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
        let temporary = temporary_path(&upload);
        storage.upload_part(&mut upload, b"else").await.unwrap();
        state.lock().unwrap().refuse_temporary_delete = refuse_delete;
        let error = storage.finish_upload(&mut upload).await.unwrap_err();
        assert_eq!(error.failure(), StorageFailure::AlreadyExists);
        if refuse_delete {
            assert!(matches!(error, StorageError::Cleanup { operation, cleanup }
                if operation.failure() == StorageFailure::AlreadyExists && cleanup.failure() == StorageFailure::PermissionDenied));
        }
        assert!(!upload.is_complete());
        assert_eq!(
            state.lock().unwrap().files.contains_key(&temporary),
            refuse_delete
        );
        assert_eq!(storage.read(&path()).await.unwrap(), b"keep");
        assert_eq!(storage.status(&path()).await.unwrap().unwrap(), before);
        state.lock().unwrap().refuse_temporary_delete = false;
        storage.abort_upload(&upload).await.unwrap();
        assert_eq!(state.lock().unwrap().files.len(), 1);
    }
}

#[tokio::test]
async fn definitive_copy_refusal_deletes_the_temporary() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    state.lock().unwrap().refuse_copy = true;
    assert_eq!(
        storage
            .finish_upload(&mut upload)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::PermissionDenied
    );
    assert!(state.lock().unwrap().files.is_empty());
    assert!(!upload.is_complete());
    storage.abort_upload(&upload).await.unwrap();
}

#[tokio::test]
async fn expired_partial_temporary_is_deleted_before_the_recording_can_be_replaced() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let mut upload = storage
        .begin_upload(&path(), crate::session::ONEDRIVE_PART_SIZE as u64 + 1)
        .await
        .unwrap();
    let bytes = vec![7; upload.part_size()];
    storage.upload_part(&mut upload, &bytes).await.unwrap();
    state.lock().unwrap().uploads.clear();
    state.lock().unwrap().refuse_temporary_delete = true;
    assert_eq!(
        storage
            .resume_upload(&mut upload)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::PermissionDenied
    );
    assert_eq!(state.lock().unwrap().files.len(), 1);
    state.lock().unwrap().refuse_temporary_delete = false;
    assert_eq!(
        storage
            .resume_upload(&mut upload)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::SessionExpired
    );
    assert!(state.lock().unwrap().files.is_empty());
}

#[tokio::test]
async fn immutable_copy_status_listing_and_read_use_the_publication_time() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = provider(&server.url);
    let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    let status = storage.status(&path()).await.unwrap().unwrap();
    assert_eq!(
        status.stored_at,
        http::timestamp(&json!({"time":"2026-10-06T00:10:00Z"}), "time").unwrap()
    );
    assert_eq!(
        storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap(),
        vec![status.clone()]
    );
    assert_eq!(
        storage.read_object(&path(), None).await.unwrap().metadata,
        status
    );
}

#[tokio::test]
#[ignore = "requires COVEN_CREATE_LEAN"]
async fn lean_differential_onedrive_create() {
    for limit in [3, 250_000_000] {
        let state = remote();
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let mut storage = provider(&server.url);
        Arc::get_mut(&mut storage.provider)
            .unwrap()
            .single_request_limit = limit;
        crate::providers::tests::compare_create_model(&storage, false).await;
        assert!(state.lock().unwrap().uploads.is_empty());
        assert_eq!(state.lock().unwrap().files.len(), 4);
    }
}

#[tokio::test]
async fn copy_waits_for_monitor_completion_without_sending_bearer_tokens() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let mut storage = provider(&server.url);
    Arc::get_mut(&mut storage.provider).unwrap().session =
        crate::providers::tests::session_with_clock(
            PROVIDER,
            Arc::new(coven_foundation::clock::ClosureClock(|| {
                std::time::SystemTime::UNIX_EPOCH
            })),
        );
    let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    state.lock().unwrap().monitor_replies.extend([
        (200, json!({"status":"notStarted"})),
        (202, json!({"status":"inProgress"})),
        (200, json!({"status":"completed"})),
    ]);
    storage.finish_upload(&mut upload).await.unwrap();
    assert!(upload.is_complete());
    assert!(state.lock().unwrap().monitor_replies.is_empty());
    assert_eq!(state.lock().unwrap().files.len(), 1);
}

#[tokio::test]
async fn copy_monitor_refusal_retains_its_response_and_deletes_the_temporary() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    let failure = json!({"status":"failed","error":{"code":"nameAlreadyExists"}});
    state
        .lock()
        .unwrap()
        .monitor_replies
        .push_back((200, failure.clone()));
    let error = storage.finish_upload(&mut upload).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::AlreadyExists);
    let StorageError::Provider { source, .. } = error else {
        panic!("native failure discarded")
    };
    let response = source.downcast_ref::<http::ProviderResponse>().unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        serde_json::from_slice::<Value>(response.body()).unwrap(),
        failure
    );
    assert!(!state
        .lock()
        .unwrap()
        .files
        .contains_key(&temporary_path(&upload)));
}

#[tokio::test]
async fn expired_copy_monitor_requires_byte_verification_before_completion() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    state
        .lock()
        .unwrap()
        .monitor_replies
        .push_back((404, json!({})));
    assert_eq!(
        storage
            .finish_upload(&mut upload)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
    assert!(!upload.is_complete());
    assert_eq!(upload.confirmed_bytes(), 0);
    storage.resume_upload(&mut upload).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    assert!(upload.is_complete());
    assert_eq!(state.lock().unwrap().files.len(), 1);
}

#[tokio::test]
async fn a_recorded_copy_refusal_can_restart_after_its_temporary_is_deleted() {
    for (code, expected) in [
        ("accessDenied", StorageFailure::PermissionDenied),
        ("activityLimitReached", StorageFailure::RateLimited),
    ] {
        let state = remote();
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
        storage.upload_part(&mut upload, b"data").await.unwrap();
        let failure = json!({"status":"failed","error":{"code":code}});
        state
            .lock()
            .unwrap()
            .monitor_replies
            .extend([(200, failure.clone()), (200, failure)]);
        assert_eq!(
            storage
                .finish_upload(&mut upload)
                .await
                .unwrap_err()
                .failure(),
            expected
        );
        assert!(state.lock().unwrap().files.is_empty());
        let mut resumed = UploadSession::decode(upload.encode().unwrap().as_bytes()).unwrap();
        assert_eq!(
            storage
                .resume_upload(&mut resumed)
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::SessionExpired
        );
        // Restore access for the new copy; the old job's failure is terminal.
        state.lock().unwrap().monitor_replies.clear();
        let mut restarted = storage.restart_upload(&resumed).await.unwrap();
        storage.upload_part(&mut restarted, b"data").await.unwrap();
        storage.finish_upload(&mut restarted).await.unwrap();
        assert!(restarted.is_complete());
        assert_eq!(storage.read(&path()).await.unwrap(), b"data");
        assert_eq!(state.lock().unwrap().files.len(), 1);
    }
}

#[tokio::test]
async fn an_overlapping_fragment_resumes_the_sessions_confirmed_progress() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let part = crate::session::ONEDRIVE_PART_SIZE;
    let total = part as u64 + 1;
    let mut upload = storage.begin_upload(&path(), total).await.unwrap();
    let first = vec![3; part];
    // The service stores the fragment but its reply is lost.
    state.lock().unwrap().fail_reply = true;
    assert_eq!(
        storage
            .upload_part(&mut upload, &first)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
    assert_eq!(upload.confirmed_bytes(), 0);
    let before = state.lock().unwrap().requests.len();
    // Resending overlaps what the service holds: coven takes its progress.
    storage.upload_part(&mut upload, &first).await.unwrap();
    assert_eq!(upload.confirmed_bytes(), part as u64);
    let requests = state.lock().unwrap().requests[before..].to_vec();
    assert_eq!(
        requests
            .iter()
            .map(|(method, _)| method.clone())
            .collect::<Vec<_>>(),
        [Method::PUT, Method::GET],
        "the refused fragment, then the session's status"
    );
    storage.upload_part(&mut upload, &[4]).await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    let mut expected = first;
    expected.push(4);
    assert_eq!(storage.read(&path()).await.unwrap(), expected);
}

#[tokio::test]
async fn an_invalid_fragment_range_stays_an_error_without_resuming() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    state.lock().unwrap().invalid_range = true;
    let before = state.lock().unwrap().requests.len();
    assert_eq!(
        storage
            .upload_part(&mut upload, b"data")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::InvalidRange
    );
    assert_eq!(state.lock().unwrap().requests.len(), before + 1);
    assert_eq!(upload.confirmed_bytes(), 0);
}

#[tokio::test]
async fn a_locked_copy_waits_for_a_retry_and_keeps_its_temporary() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let mut upload = storage.begin_upload(&path(), 4).await.unwrap();
    let temporary = temporary_path(&upload);
    storage.upload_part(&mut upload, b"data").await.unwrap();
    state.lock().unwrap().lock_copy = true;
    let error = storage.finish_upload(&mut upload).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Conflict);
    assert!(!error.retryable());
    assert_eq!(
        error.retry_after(),
        Some(crate::RetryAfter::Wait(std::time::Duration::from_secs(30)))
    );
    assert!(state.lock().unwrap().files.contains_key(&temporary));
    storage.finish_upload(&mut upload).await.unwrap();
    assert!(upload.is_complete());
    assert_eq!(storage.read(&path()).await.unwrap(), b"data");
}

mod sequence {
    use super::*;

    #[tokio::test]
    async fn floor_replacement_conflicts_and_lost_reply_preserve_the_accepted_revision() {
        let state = remote();
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        let path = ObjectPath::floor(crate::test_utils::STORE_ID);
        storage.create(&path, b"floor one").await.unwrap();
        let first = storage.status(&path).await.unwrap().unwrap();
        storage.replace(&first, b"floor two").await.unwrap();
        assert_eq!(
            storage
                .replace(&first, b"stale floor")
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Conflict
        );
        assert_eq!(storage.read(&path).await.unwrap(), b"floor two");
        let second = storage.status(&path).await.unwrap().unwrap();
        state.lock().unwrap().fail_reply = true;
        assert_eq!(
            storage
                .replace(&second, b"floor three")
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Network
        );
        assert_eq!(
            storage
                .replace(&second, b"stale retry")
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Conflict
        );
        let third = storage.read_object(&path, None).await.unwrap();
        assert_eq!(third.bytes, b"floor three");
        assert_ne!(second.revision, third.metadata.revision);
        assert_ne!(second.replace_token, third.metadata.replace_token);
    }

    #[tokio::test]
    async fn slot_create_once_refuses_equal_and_different_bytes_without_replacement() {
        let state = remote();
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        let path = ObjectPath::slot(crate::test_utils::STORE_ID, std::num::NonZeroU64::MIN);
        storage.create(&path, b"claim one").await.unwrap();
        let first = storage.status(&path).await.unwrap().unwrap();
        for bytes in [b"claim one", b"claim two"] {
            assert_eq!(
                storage.create(&path, bytes).await.unwrap_err().failure(),
                StorageFailure::AlreadyExists
            );
            assert_eq!(storage.read(&path).await.unwrap(), b"claim one");
            assert_eq!(storage.status(&path).await.unwrap().unwrap(), first);
        }
    }
}
