use super::*;

/// ListMultipartUploads: every upload whose key starts with the prefix.
pub(super) fn list_uploads(state: &Remote, q: &BTreeMap<String, String>) -> Response<Body> {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let uploads: String = state
        .uploads
        .iter()
        .filter(|(_, upload)| upload.key.starts_with(&prefix))
        .map(|(id, upload)| {
            format!(
                "<Upload><Key>{}</Key><UploadId>{id}</UploadId><Initiated>{}</Initiated></Upload>",
                upload.key, upload.initiated
            )
        })
        .collect();
    response(
        200,
        format!("<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>{uploads}</ListMultipartUploadsResult>"),
    )
}

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
            Arc::get_mut(storage.provider_mut()).unwrap().single_request_limit = limit;
            // Multipart creates below the limit publish by copy on copy
            // profiles; an every-object profile copies every create.
            let publication = profile.settings.copy_publication;
            let copy = limit == 3 && publication.multipart().is_some()
                || publication.single_request().is_some();
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
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::new(number).unwrap());
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
        },
        S3Credentials {
            access_key_id: "access".into(),
            secret_access_key: SecretText::new("secret".into()),
        },
        Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
        Arc::new(SequentialIds::new()),
        std::sync::Arc::new(crate::NoCooldown),
        crate::providers::tests::http_client(),
    )
    .unwrap();
    storage.client =
        Client::from_conf(storage.client.config().to_builder().endpoint_url(url).build());
    StorageConnection::from_provider(Arc::new(storage))
}

#[tokio::test]
async fn gcs_create_preserves_an_occupied_path() {
    let state = Arc::new(Mutex::new(Remote { gcs: true, ..Remote::default() }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = gcs_provider(&server.url);
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    storage.create(&path, b"original").await.unwrap();
    let result = storage.create(&path, b"replacement").await;
    assert!(matches!(result, Err(error) if error.failure() == StorageFailure::AlreadyExists));
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
    check_provider(&storage, &ObjectPath::floor(STORE_ID), b"sealed check bytes").await.unwrap();
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
        assert_eq!(profile::settings(Some(&url.parse().unwrap()), None).unwrap(), expected);
    }
}

#[tokio::test]
async fn gcs_precondition_status_is_already_exists_without_an_error_code() {
    for body in
        ["", "<Error><Code></Code></Error>", "<Error><Code>PreconditionFailed</Code></Error>"]
    {
        let server =
            TestServer::new(Router::new().fallback(move || async move { response(412, body) }))
                .await;
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
        let error = gcs_provider(&server.url).create(&path, b"bytes").await.unwrap_err();
        assert_eq!(error.failure(), StorageFailure::AlreadyExists);
        assert!(matches!(error, StorageError::Provider { .. }));
    }
}

#[tokio::test]
async fn gcs_multipart_copy_preserves_occupied_bytes_and_removes_temporaries() {
    let state = Arc::new(Mutex::new(Remote { gcs: true, ..Remote::default() }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let mut storage = gcs_provider(&server.url);
    Arc::get_mut(storage.provider_mut()).unwrap().single_request_limit = 3;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    storage.create(&path, b"original").await.unwrap();
    let result = storage.create(&path, b"replacement").await;
    assert!(matches!(result, Err(error) if error.failure() == StorageFailure::AlreadyExists));
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
    assert_eq!(storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap().len(), 1);
    let state = state.lock().unwrap();
    assert_eq!(state.copies, 2);
    assert!(state.uploads.is_empty());
    assert_eq!(state.objects.keys().collect::<Vec<_>>(), vec![path.as_str()]);
}

#[tokio::test]
async fn gcs_resume_reuses_its_temporary_after_assembly_copy_or_delete_reply_loss() {
    for lost in ["assembly", "copy", "delete_refused", "delete_reply"] {
        let state = Arc::new(Mutex::new(Remote { gcs: true, ..Remote::default() }));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = gcs_provider(&server.url);
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
        let mut session = storage.begin_upload(&path, 4).await.unwrap();
        // The caller saves the plan its first part's admission marked.
        let permit = storage.admit_part(&mut session, b"data").unwrap();
        let recorded = session.encode().unwrap();
        let temporary = storage.provider().session_key(&session).unwrap();
        assert!(temporary
            .starts_with(&format!("{STORE_ID}/.coven-uploads/{}/", crate::test_utils::DEVICE.0)));
        assert_eq!(session.device(), crate::test_utils::DEVICE);
        assert!(ObjectPath::parse(&temporary).is_err());
        storage.send_part(permit, &mut session, &ByteProgress::unread()).await.unwrap();
        {
            let mut state = state.lock().unwrap();
            state.fail_completion_reply = lost == "assembly";
            state.fail_copy_reply = lost == "copy";
            state.refuse_temporary_delete = lost == "delete_refused";
            state.lose_temporary_delete_reply = lost == "delete_reply";
        }
        assert!(storage.finish_upload(&mut session).await.is_err());
        assert!(!session.is_complete());
        assert_eq!(state.lock().unwrap().objects.contains_key(&temporary), lost != "delete_reply");
        // Listing a store must not treat an unfinished temporary as published bytes.
        let listed = storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
        assert_eq!(listed.len(), usize::from(lost != "assembly"));
        state.lock().unwrap().refuse_temporary_delete = false;
        let mut resumed = UploadSession::decode(recorded.as_bytes()).unwrap();
        assert_eq!(storage.provider().session_key(&resumed).unwrap(), temporary);
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
    let state = Arc::new(Mutex::new(Remote { gcs: true, ..Remote::default() }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = gcs_provider(&server.url);
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    for assembled in [false, true] {
        let mut session = storage.begin_upload(&path, 4).await.unwrap();
        let permit = storage.admit_part(&mut session, b"data").unwrap();
        let recorded = session.encode().unwrap();
        storage.send_part(permit, &mut session, &ByteProgress::unread()).await.unwrap();
        if assembled {
            state.lock().unwrap().fail_completion_reply = true;
            assert_eq!(
                storage.finish_upload(&mut session).await.unwrap_err().failure(),
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
    let state = Arc::new(Mutex::new(Remote { gcs: true, refuse_copy: true, ..Remote::default() }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = gcs_provider(&server.url);
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let mut session = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut session, b"data").await.unwrap();
    assert_eq!(
        storage.finish_upload(&mut session).await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
    assert!(!session.is_complete());
    assert!(state.lock().unwrap().objects.is_empty());
    assert!(state.lock().unwrap().uploads.is_empty());
    storage.abort_upload(&session).await.unwrap();
}

#[tokio::test]
async fn gcs_refused_copy_retains_both_failures_and_retry_cleans_the_temporary() {
    let state = Arc::new(Mutex::new(Remote { gcs: true, ..Remote::default() }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = gcs_provider(&server.url);
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
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
        storage.resume_upload(&mut session).await.unwrap_err().failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(state.lock().unwrap().objects.len(), 1);
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
}

#[tokio::test]
async fn an_occupied_create_does_not_hide_failed_temporary_cleanup() {
    let state = Arc::new(Mutex::new(Remote { gcs: true, ..Remote::default() }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let mut storage = gcs_provider(&server.url);
    Arc::get_mut(storage.provider_mut()).unwrap().single_request_limit = 3;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    storage.create(&path, b"original").await.unwrap();
    state.lock().unwrap().refuse_temporary_delete = true;
    // A failed cleanup is never a plain occupied path a caller may compare.
    assert!(matches!(
        storage.create(&path, b"replacement").await,
        Err(StorageError::Cleanup { .. })
    ));
}

/// One `CopyObject` copies at most 5 GiB, so an S3 copy profile refuses a
/// larger upload before any request; GCS's copy within one bucket, the only
/// kind coven makes, has no such limit (§20.6).
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
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
        let result = storage.begin_upload(&path, limit + 1).await;
        let limited = matches!(
            profile.settings.copy_publication.multipart(),
            Some(S3CopyCondition::IfNoneMatch | S3CopyCondition::CloudflareIfNoneMatch)
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
    assert_eq!(
        profile::CataloguedProfile::named(S3ProfileName::SeaweedFs).settings.copy_publication,
        S3CopyPublication::Every(S3CopyCondition::IfNoneMatch)
    );
}

async fn every_object_provider() -> (TestServer, Arc<Mutex<Remote>>, StorageConnection<S3Storage>) {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let settings = profile::CataloguedProfile::named(S3ProfileName::SeaweedFs).settings;
    let storage = profile_provider(&server.url, settings);
    (server, state, storage)
}

fn temporaries(state: &Mutex<Remote>) -> usize {
    state.lock().unwrap().objects.keys().filter(|key| key.contains("/.coven-uploads/")).count()
}

/// A profile publishing every object by copy stages even a single-request
/// create and publishes it with one conditional copy, which an occupied name
/// refuses with its bytes unchanged; no temporary outlives the call (§20.6).
#[tokio::test]
async fn every_object_copy_publishes_single_request_creates_by_conditional_copy() {
    let (_server, state, storage) = every_object_provider().await;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    // The copy reply carries a time, but its other metadata isn't HEAD's on a
    // certified copy profile, so exact status supplies the object (E.11).
    assert_eq!(storage.create(&path, b"original").await.unwrap(), None);
    assert_eq!(state.lock().unwrap().copies, 1);
    for bytes in [&b"replacement"[..], b"original"] {
        assert_eq!(
            storage.create(&path, bytes).await.unwrap_err().failure(),
            StorageFailure::AlreadyExists
        );
    }
    assert_eq!(state.lock().unwrap().copies, 3);
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
    assert_eq!(temporaries(&state), 0);
    assert!(state.lock().unwrap().uploads.is_empty());
    // Bounded slots use the same create-once copy condition.
    let entry = ObjectPath::slot(STORE_ID, 2.try_into().unwrap());
    storage.create(&entry, b"entry").await.unwrap();
    assert_eq!(state.lock().unwrap().copies, 4);
    // Floor creation and replacement both use conditional copies.
    let floor = ObjectPath::floor(STORE_ID);
    storage.create(&floor, b"floor").await.unwrap();
    let previous = storage.status(&floor).await.unwrap().unwrap();
    storage.replace(&previous, b"next floor").await.unwrap();
    assert_eq!(state.lock().unwrap().copies, 6);
    assert_eq!(temporaries(&state), 0);
    let listed = storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
    assert_eq!(listed.len(), 3);
}

/// S3 can answer a copy with status 200 and an error document. A publishing
/// copy answered so published nothing: the create fails with what the
/// document names, and its temporary is gone; the next create publishes.
#[tokio::test]
async fn a_publishing_copy_answered_200_with_an_error_fails() {
    let (_server, state, storage) = every_object_provider().await;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    state.lock().unwrap().error_in_ok = Some("InternalError");
    let failed = storage.create(&path, b"original").await.unwrap_err();
    assert_eq!(failed.failure(), StorageFailure::Network, "{failed:?}");
    assert_eq!(storage.status(&path).await.unwrap(), None);
    assert_eq!(temporaries(&state), 0);
    storage.create(&path, b"original").await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
}

/// A lost copy reply: the copy landed, and the create settles by this
/// attempt's marker at the destination, not as another object's occupation.
#[tokio::test]
async fn every_object_copy_settles_a_lost_copy_reply_by_its_marker() {
    let (_server, state, storage) = every_object_provider().await;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    state.lock().unwrap().fail_copy_reply = true;
    storage.create(&path, b"landed").await.unwrap();
    assert_eq!(state.lock().unwrap().copies, 1);
    assert_eq!(storage.read(&path).await.unwrap(), b"landed");
    assert_eq!(temporaries(&state), 0);
}

/// A temporary that cannot be deleted fails the create even when the copy
/// published it. The caller's retry of the same create stages and deletes
/// the same temporary, and finds a plain occupied path holding its own
/// bytes, which it accepts by comparing them (Appendix E.10, §5.1).
#[tokio::test]
async fn every_object_copy_retry_after_a_failed_deletion_removes_the_temporary() {
    let (_server, state, storage) = every_object_provider().await;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let temporary = storage
        .provider()
        .create_temporary_key(&path, b"bytes", crate::test_utils::DEVICE)
        .unwrap();
    state.lock().unwrap().refuse_temporary_delete = true;
    assert_eq!(
        storage.create(&path, b"bytes").await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
    assert_eq!(storage.read(&path).await.unwrap(), b"bytes");
    assert!(state.lock().unwrap().objects.contains_key(&temporary));
    state.lock().unwrap().refuse_temporary_delete = false;
    let retry = storage.create(&path, b"bytes").await.unwrap_err();
    assert!(!matches!(retry, StorageError::Cleanup { .. }));
    assert_eq!(retry.failure(), StorageFailure::AlreadyExists);
    assert_eq!(storage.read(&path).await.unwrap(), b"bytes");
    assert_eq!(temporaries(&state), 0);
}

/// A crash between staging and deletion leaves the create's temporary; the
/// create is still recorded by its caller, and its retry removes it. Another
/// create's temporary has another name (Appendix E.10,
/// `CopyPublication.Recorded.staged_recorded`).
#[tokio::test]
async fn every_object_copy_retry_after_a_crash_removes_the_staged_temporary() {
    let (_server, state, storage) = every_object_provider().await;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let temporary = storage
        .provider()
        .create_temporary_key(&path, b"bytes", crate::test_utils::DEVICE)
        .unwrap();
    assert_ne!(
        temporary,
        storage
            .provider()
            .create_temporary_key(&path, b"other", crate::test_utils::DEVICE)
            .unwrap()
    );
    // The crashed attempt staged its bytes and stopped before its copy.
    state.lock().unwrap().objects.insert(temporary.clone(), b"bytes".to_vec());
    storage.create(&path, b"bytes").await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"bytes");
    assert_eq!(temporaries(&state), 0);
}

/// A copy profile records its session before the native multipart upload
/// exists: the first part initiates it. An initiation whose reply was lost
/// is found under the recording's name and aborted on resume or abort, so
/// no upload outlives a recording that does not name it (Appendix E.10).
#[tokio::test]
async fn a_planned_session_aborts_an_initiation_whose_reply_was_lost() {
    for abandon in [false, true] {
        let (_server, state, storage) = every_object_provider().await;
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
        let mut session = storage.begin_upload(&path, 4).await.unwrap();
        let permit = storage.admit_part(&mut session, b"data").unwrap();
        let recorded = session.encode().unwrap();
        assert!(state.lock().unwrap().requests.is_empty(), "named before any request");
        state.lock().unwrap().lose_initiation_reply = true;
        assert_eq!(
            storage
                .send_part(permit, &mut session, &ByteProgress::unread())
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Network
        );
        assert_eq!(state.lock().unwrap().uploads.len(), 1);
        let mut resumed = UploadSession::decode(recorded.as_bytes()).unwrap();
        if abandon {
            storage.abort_upload(&resumed).await.unwrap();
            assert!(state.lock().unwrap().uploads.is_empty());
            continue;
        }
        storage.resume_upload(&mut resumed).await.unwrap();
        assert_eq!(resumed.confirmed_bytes(), 0);
        assert!(state.lock().unwrap().uploads.is_empty());
        storage.upload_part(&mut resumed, b"data").await.unwrap();
        storage.finish_upload(&mut resumed).await.unwrap();
        assert!(resumed.is_complete());
        assert_eq!(storage.read(&path).await.unwrap(), b"data");
        assert!(state.lock().unwrap().uploads.is_empty());
        assert_eq!(temporaries(&state), 0);
    }
}

/// A copy request the fake holds before serving it: it reports its arrival
/// and waits for release, so a test interleaves other requests with it.
pub(super) struct HeldCopy {
    arrived: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

pub(super) async fn hold_copy(state: &Mutex<Remote>, headers: &HeaderMap) {
    if !headers.contains_key("x-amz-copy-source") {
        return;
    }
    let Some(held) = state.lock().unwrap().held_copy.take() else {
        return;
    };
    held.arrived.send(()).unwrap();
    held.release.await.unwrap();
}

/// Two creators of one path with identical bytes are attempts of one create
/// and share its temporary. The first stages and its copy is held; the
/// second stages over it, copies and deletes it, so the first copy finds no
/// source. It settles by the destination: an occupied path holding the same
/// bytes, which the caller's comparison accepts, or, when the second copy
/// published nothing, a conflict whose repeated create publishes (Appendix E.10,
/// `CopyPublication.Recorded.shared_never_fails`,
/// `shared_occupied_identical`, `retry_publishes`).
#[tokio::test]
async fn every_object_copy_settles_a_copy_whose_shared_temporary_is_gone() {
    for second_publishes in [true, false] {
        let (_server, state, storage) = every_object_provider().await;
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
        let (arrived, arrival) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        state.lock().unwrap().held_copy = Some(HeldCopy { arrived, release: released });
        let (first, second) = tokio::join!(storage.create(&path, b"bytes"), async {
            arrival.await.unwrap();
            state.lock().unwrap().refuse_copy = !second_publishes;
            let second = storage.create(&path, b"bytes").await;
            state.lock().unwrap().refuse_copy = false;
            release.send(()).unwrap();
            second
        });
        assert_eq!(temporaries(&state), 0);
        if second_publishes {
            second.unwrap();
            let first = first.unwrap_err();
            assert!(!matches!(first, StorageError::Cleanup { .. }));
            assert_eq!(first.failure(), StorageFailure::AlreadyExists);
            assert_eq!(storage.read(&path).await.unwrap(), b"bytes");
            continue;
        }
        assert_eq!(second.unwrap_err().failure(), StorageFailure::PermissionDenied);
        let first = first.unwrap_err();
        assert_eq!(first.failure(), StorageFailure::Conflict);
        assert!(!first.retryable());
        assert!(storage.status(&path).await.unwrap().is_none());
        storage.create(&path, b"bytes").await.unwrap();
        assert_eq!(storage.read(&path).await.unwrap(), b"bytes");
        assert_eq!(temporaries(&state), 0);
    }
}

pub(super) fn condition_holds(state: &Remote, key: &str, headers: &HeaderMap) -> bool {
    let expected = if state.gcs {
        let value = headers["x-goog-if-generation-match"].to_str().unwrap();
        if value == "0" {
            return !state.objects.contains_key(key);
        }
        value
    } else if headers.contains_key("if-none-match")
        || headers.contains_key("cf-copy-destination-if-none-match")
    {
        return !state.objects.contains_key(key);
    } else {
        headers
            .get("if-match")
            .or_else(|| headers.get("cf-copy-destination-if-match"))
            .expect("conditional publication")
            .to_str()
            .unwrap()
    };
    let current = if state.gcs { state.revisions.get(key) } else { state.etags.get(key) };
    current.map(String::as_str) == Some(expected)
}

#[tokio::test]
async fn competing_floor_pointer_writes_keep_each_s3_profiles_revision_condition() {
    for profile in profile::PROFILES {
        let state = Arc::new(Mutex::new(Remote {
            gcs: profile.settings.signing == S3Signing::GoogleHmac,
            ..Remote::default()
        }));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        crate::test_utils::Conformance::new(
            Arc::new(profile_provider(&server.url, profile.settings)),
            STORE_ID,
        )
        .conditional_floor_replace()
        .await;
        let state = state.lock().unwrap();
        assert!(state.uploads.is_empty());
        assert!(state.objects.is_empty());
    }
}

#[tokio::test]
async fn a_shared_temporary_marker_cannot_accept_two_floor_replacements() {
    let (_server, state, storage) = every_object_provider().await;
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"old").await.unwrap();
    let previous = storage.status(&path).await.unwrap().unwrap();
    let (arrived_first, first_arrival) = tokio::sync::oneshot::channel();
    let (release_first, first_release) = tokio::sync::oneshot::channel();
    let (arrived_second, second_arrival) = tokio::sync::oneshot::channel();
    let (release_second, second_release) = tokio::sync::oneshot::channel();
    let (finished_first, first_finished) = tokio::sync::oneshot::channel();
    state.lock().unwrap().held_copy =
        Some(HeldCopy { arrived: arrived_first, release: first_release });
    let first = async {
        let result = storage.replace(&previous, b"next").await;
        finished_first.send(()).unwrap();
        result
    };
    let second = async {
        first_arrival.await.unwrap();
        state.lock().unwrap().held_copy =
            Some(HeldCopy { arrived: arrived_second, release: second_release });
        let (result, ()) = tokio::join!(storage.replace(&previous, b"next"), async {
            second_arrival.await.unwrap();
            // The first copy carries the second attempt's staged marker, then
            // deletes their shared temporary before the second copy reaches it.
            release_first.send(()).unwrap();
            first_finished.await.unwrap();
            release_second.send(()).unwrap();
        });
        result
    };
    let (first, second) = tokio::join!(first, second);
    first.unwrap();
    assert_eq!(second.unwrap_err().failure(), StorageFailure::Conflict);
    assert_eq!(storage.read(&path).await.unwrap(), b"next");
    assert_eq!(temporaries(&state), 0);
}

/// A put's and each part's bytes are heard through the SDK as the
/// connection takes them, a part's from where it starts in the object,
/// whether the profile publishes by PUT or by copying a temporary.
#[tokio::test]
async fn put_and_part_bytes_are_heard_through_the_object() {
    for profile in profile::PROFILES {
        let state = Arc::new(Mutex::new(Remote {
            gcs: profile.settings.signing == S3Signing::GoogleHmac,
            ..Remote::default()
        }));
        let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
        let storage = profile_provider(&server.url, profile.settings);
        let heard = Arc::new(Mutex::new(Vec::new()));
        let sent = crate::ByteProgress::new({
            let heard = heard.clone();
            move |count| heard.lock().unwrap().push(count)
        });
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
        storage.create_reporting(&path, &vec![1; 100 * 1024], &sent).await.unwrap();
        assert_eq!(
            std::mem::take(&mut *heard.lock().unwrap()),
            [64 * 1024, 100 * 1024],
            "{profile:?}"
        );
        let path = ObjectPath::file(
            STORE_ID,
            coven_foundation::id_source::DeviceId(31),
            coven_foundation::id_source::ObjectId(uuid::Uuid::from_bytes([0xdd; 16])),
        );
        let total = 12 * 1024 * 1024;
        let mut upload = storage.begin_upload(&path, total).await.unwrap();
        let part = upload.part_size() as u64;
        assert!(part < total);
        while upload.confirmed_bytes() < total {
            let length = part.min(total - upload.confirmed_bytes());
            let bytes = vec![2; length as usize];
            let permit = storage.admit_part(&mut upload, &bytes).unwrap();
            storage.send_part(permit, &mut upload, &sent).await.unwrap();
            assert_eq!(heard.lock().unwrap().last(), Some(&upload.confirmed_bytes()));
        }
        let heard = heard.lock().unwrap().clone();
        assert!(heard.windows(2).all(|pair| pair[0] < pair[1]), "{heard:?}");
        storage.finish_upload(&mut upload).await.unwrap();
    }
}

/// Every temporary a copy profile stages, a single-request create's and a
/// session's alike, sits in the folder of the device whose connection
/// staged it, so whose it is stays known (D10, Appendix E.10).
#[tokio::test]
async fn every_temporary_sits_in_the_staging_devices_folder() {
    let folder = format!("{STORE_ID}/.coven-uploads/{}/", crate::test_utils::DEVICE.0);
    let staged = |state: &Mutex<Remote>| -> Vec<String> {
        let state = state.lock().unwrap();
        state.objects.keys().filter(|key| key.contains("/.coven-uploads/")).cloned().collect()
    };
    let (_server, state, storage) = every_object_provider().await;
    state.lock().unwrap().refuse_temporary_delete = true;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    assert!(storage.create(&path, b"bytes").await.is_err());
    let created = staged(&state);
    assert_eq!(created.len(), 1);
    assert!(created[0].starts_with(&folder), "{created:?}");

    let state = Arc::new(Mutex::new(Remote { gcs: true, ..Remote::default() }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = gcs_provider(&server.url);
    let mut session = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut session, b"data").await.unwrap();
    assert!(state.lock().unwrap().uploads.values().all(|upload| upload.key.starts_with(&folder)));
    state.lock().unwrap().refuse_temporary_delete = true;
    assert!(storage.finish_upload(&mut session).await.is_err());
    let assembled = staged(&state);
    assert_eq!(assembled.len(), 1);
    assert!(assembled[0].starts_with(&folder), "{assembled:?}");
}

/// An upload listing that doesn't say whether more pages follow is a
/// protocol failure, never taken as the last page: uploads left on a later
/// page would stay behind unaborted.
#[tokio::test]
async fn an_upload_listing_without_its_completion_is_a_protocol_failure() {
    let server = TestServer::new(Router::new().fallback(|method: Method, uri: Uri| async move {
        let q = query(&uri);
        match method {
            Method::DELETE => response(204, ""),
            _ if q.contains_key("uploads") => response(
                200,
                "<ListMultipartUploadsResult><Upload><Key>dev/k</Key><UploadId>u1</UploadId></Upload></ListMultipartUploadsResult>",
            ),
            _ => response(404, ""),
        }
    }))
    .await;
    let storage = profile_provider(&server.url, profile::PLAIN);

    let error = storage.provider().abort_private_uploads("dev/k", None).await.unwrap_err();

    assert_eq!(error.failure(), StorageFailure::Protocol);
}

/// A create's temporary is named by what the create is: SHA-256 over the
/// version tag, the path's length as a big-endian u64, the path and the
/// bytes, its first 16 bytes as a UUID, in the staging device's folder.
#[tokio::test]
async fn a_creates_temporary_is_named_by_its_path_and_bytes() {
    let (_server, _state, storage) = every_object_provider().await;
    let device = crate::test_utils::DEVICE;
    let mut names = std::collections::BTreeSet::new();
    for (path, bytes) in [
        (ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN), &b"first bytes"[..]),
        (ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN), b"other bytes"),
        (ObjectPath::slot(STORE_ID, 2.try_into().unwrap()), b"first bytes"),
    ] {
        let mut hasher = coven_crypto::ObjectHasher::new();
        for part in [
            &b"coven/copy-temporary/v1"[..],
            &(path.as_str().len() as u64).to_be_bytes(),
            path.as_str().as_bytes(),
            bytes,
        ] {
            hasher.update(part);
        }
        let digest = hasher.finish();
        let mut id = [0; 16];
        id.copy_from_slice(&digest.as_bytes()[..16]);
        let expected = format!(
            "{}/{}",
            coven_format::path::device_uploads(STORE_ID, device),
            uuid::Uuid::from_bytes(id)
        );
        let named = storage.provider().create_temporary_key(&path, bytes, device).unwrap();
        assert_eq!(named, expected);
        assert!(names.insert(named), "a create's name is its own");
    }
}

/// Two attempts of one create stage over one temporary, and both copies are
/// held: the first copy carries the second's marker and deletes the
/// temporary, so the second copy finds no source and settles by its own
/// marker at the destination. Both answer stored (§20.6, E.10).
#[tokio::test]
async fn two_attempts_staging_one_temporary_both_answer_stored() {
    let (_server, state, storage) = every_object_provider().await;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let (arrived_first, first_arrival) = tokio::sync::oneshot::channel();
    let (release_first, first_release) = tokio::sync::oneshot::channel();
    let (arrived_second, second_arrival) = tokio::sync::oneshot::channel();
    let (release_second, second_release) = tokio::sync::oneshot::channel();
    let (finished_first, first_finished) = tokio::sync::oneshot::channel();
    state.lock().unwrap().held_copy =
        Some(HeldCopy { arrived: arrived_first, release: first_release });
    let first = async {
        let result = storage.create(&path, b"bytes").await;
        finished_first.send(()).unwrap();
        result
    };
    let second = async {
        first_arrival.await.unwrap();
        state.lock().unwrap().held_copy =
            Some(HeldCopy { arrived: arrived_second, release: second_release });
        let (result, ()) = tokio::join!(storage.create(&path, b"bytes"), async {
            second_arrival.await.unwrap();
            release_first.send(()).unwrap();
            first_finished.await.unwrap();
            release_second.send(()).unwrap();
        });
        result
    };
    let (first, second) = tokio::join!(first, second);
    first.unwrap();
    second.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"bytes");
    assert_eq!(temporaries(&state), 0);
}
