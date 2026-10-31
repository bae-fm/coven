use super::*;

pub(super) struct Upload {
    path: String,
    revision: u64,
    bytes: Vec<u8>,
    deferred: bool,
}

pub(super) fn begin(
    state: &mut Remote,
    id: &str,
    value: &Value,
    headers: &HeaderMap,
    host: &str,
) -> Response<Body> {
    let decoded = url::form_urlencoded::parse(id.as_bytes()).next().unwrap().0;
    let path = decoded.strip_prefix("file:").unwrap();
    if !state.files.contains_key(path) {
        return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
    }
    let Some(revision) = state.revisions.get(path).copied() else {
        return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
    };
    assert_eq!(value["item"], json!({"@microsoft.graph.conflictBehavior":"replace"}));
    if headers["if-match"] != revision.to_string() {
        return response(412, json!({"error":{"code":"resourceModified"}}).to_string());
    }
    state.next += 1;
    let id = state.next.to_string();
    state.replacements.insert(
        id.clone(),
        Upload {
            path: path.into(),
            revision,
            bytes: Vec::new(),
            deferred: value["deferCommit"] == true,
        },
    );
    reply(json!({"uploadUrl":format!("http://{host}/replace-session/{id}")}))
}

pub(super) fn transfer(
    state: &mut Remote,
    id: &str,
    method: &Method,
    headers: &HeaderMap,
    body: &[u8],
    host: &str,
) -> Response<Body> {
    assert!(headers.get("authorization").is_none());
    if method == Method::DELETE {
        state.replacements.remove(id);
        return response(204, Vec::new());
    }
    let Some(upload) = state.replacements.get_mut(id) else {
        return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
    };
    if state.revisions.get(&upload.path) != Some(&upload.revision) {
        state.replacements.remove(id);
        return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
    }
    if method == Method::GET {
        return reply(json!({"nextExpectedRanges":[format!("{}-", upload.bytes.len())]}));
    }
    assert_eq!(headers["content-length"], body.len().to_string());
    if method == Method::POST {
        assert!(upload.deferred);
        assert_eq!(headers["if-match"], upload.revision.to_string());
        assert!(body.is_empty());
        assert!(headers.get("content-range").is_none());
    } else {
        assert_eq!(method, Method::PUT);
        assert!(headers.get("if-match").is_none());
        let range = headers["content-range"].to_str().unwrap().strip_prefix("bytes ").unwrap();
        let (bounds, total) = range.split_once('/').unwrap();
        let (start, end) = bounds.split_once('-').unwrap();
        assert_eq!(start.parse::<usize>().unwrap(), upload.bytes.len());
        upload.bytes.extend_from_slice(body);
        assert_eq!(end.parse::<usize>().unwrap() + 1, upload.bytes.len());
        if upload.bytes.len() < total.parse::<usize>().unwrap() || upload.deferred {
            return response(
                202,
                json!({"nextExpectedRanges":[format!("{}-", upload.bytes.len())]}).to_string(),
            );
        }
    }
    let upload = state.replacements.remove(id).unwrap();
    *state.revisions.get_mut(&upload.path).unwrap() += 1;
    let result = file(&upload.path, &upload.bytes, host, state);
    state.files.insert(upload.path, upload.bytes);
    if std::mem::take(&mut state.fail_reply) {
        return response(503, "{}");
    }
    reply(result)
}

#[tokio::test]
async fn simultaneous_floor_replacements_use_the_sessions_revision_condition() {
    // An empty body completes a deferred session with its own request.
    for (first, second) in [(b"writer-a".as_slice(), b"writer-b".as_slice()), (b"", b"writer-b")] {
        let state = remote();
        let gate = Arc::new(tokio::sync::Barrier::new(2));
        let remote_state = state.clone();
        let server = TestServer::new(Router::new().fallback(
            move |method: Method, uri: Uri, mut headers: HeaderMap, body: Bytes| {
                let state = remote_state.clone();
                let gate = gate.clone();
                async move {
                    if method == Method::PUT && headers.contains_key("if-match") {
                        // Graph's direct content PUT admits both requests using the old
                        // revision, then applies both writes without another comparison.
                        gate.wait().await;
                        let remote = state.lock().unwrap();
                        let path = ObjectPath::floor(STORE_ID);
                        let current = remote.revisions[path.as_str()].to_string();
                        headers.insert("if-match", current.parse().unwrap());
                        drop(remote);
                    }
                    let created =
                        method == Method::POST && uri.path().ends_with("/createUploadSession");
                    let response = endpoint(State(state), method, uri, headers, body).await;
                    if created {
                        gate.wait().await;
                    }
                    response
                }
            },
        ))
        .await;
        let storage = provider(&server.url);
        let path = ObjectPath::floor(STORE_ID);
        storage.create(&path, b"original").await.unwrap();
        let before = storage.status(&path).await.unwrap().unwrap();
        let (a, b) =
            tokio::join!(storage.replace(&before, first), storage.replace(&before, second));
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let (loser, winner) = if a.is_ok() { (b, first) } else { (a, second) };
        let loser = loser.unwrap_err();
        assert_eq!(loser.failure(), StorageFailure::Conflict);
        let StorageError::Provider { source, .. } = loser else { panic!("native cause discarded") };
        assert_eq!(source.downcast_ref::<http::ProviderResponse>().unwrap().status(), 404);
        assert_eq!(storage.read(&path).await.unwrap(), winner);
        assert!(state.lock().unwrap().replacements.is_empty());
    }
}

#[tokio::test]
async fn replacement_fragments_leave_the_old_bytes_visible_until_completion() {
    for intervening in [false, true] {
        let state = remote();
        let remote_state = state.clone();
        let server = TestServer::new(Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let state = remote_state.clone();
                async move {
                    let fragment =
                        method == Method::PUT && uri.path().starts_with("/replace-session/");
                    let reply = endpoint(State(state.clone()), method, uri, headers, body).await;
                    if fragment && reply.status().as_u16() == 202 {
                        let path = ObjectPath::floor(STORE_ID);
                        let mut remote = state.lock().unwrap();
                        assert_eq!(remote.files[path.as_str()], b"original");
                        if intervening {
                            remote.files.insert(path.as_str().into(), b"newer".to_vec());
                            *remote.revisions.get_mut(path.as_str()).unwrap() += 1;
                        }
                    }
                    reply
                }
            },
        ))
        .await;
        let storage = provider(&server.url);
        let path = ObjectPath::floor(STORE_ID);
        storage.create(&path, b"original").await.unwrap();
        let previous = storage.status(&path).await.unwrap().unwrap();
        let bytes = vec![19; crate::session::ONEDRIVE_PART_SIZE + 17];
        let result = storage.replace(&previous, &bytes).await;
        if intervening {
            assert_eq!(result.unwrap_err().failure(), StorageFailure::Conflict);
            assert_eq!(storage.read(&path).await.unwrap(), b"newer");
        } else {
            result.unwrap();
            assert_eq!(storage.read(&path).await.unwrap(), bytes);
        }
        assert!(state.lock().unwrap().replacements.is_empty());
    }
}

#[tokio::test]
async fn replacement_refusals_keep_native_causes_and_cancel_without_retrying() {
    for (status, code, failure) in [
        (404, "itemNotFound", StorageFailure::Conflict),
        (409, "resourceModified", StorageFailure::Conflict),
        (412, "preconditionFailed", StorageFailure::Conflict),
        (423, "resourceLocked", StorageFailure::Conflict),
        (403, "accessDenied", StorageFailure::PermissionDenied),
        (416, "invalidRange", StorageFailure::InvalidRange),
        (429, "activityLimitReached", StorageFailure::RateLimited),
        (503, "generalException", StorageFailure::Network),
    ] {
        let state = remote();
        let remote_state = state.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let received = requests.clone();
        let server = TestServer::new(Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let state = remote_state.clone();
                let requests = received.clone();
                async move {
                    if uri.path().starts_with("/replace-session/") {
                        assert!(headers.get("authorization").is_none());
                        requests.lock().unwrap().push(method.clone());
                        if method == Method::PUT {
                            return response(status, json!({"error":{"code":code}}).to_string());
                        }
                    }
                    endpoint(State(state), method, uri, headers, body).await
                }
            },
        ))
        .await;
        let storage = provider(&server.url);
        let path = ObjectPath::floor(STORE_ID);
        storage.create(&path, b"original").await.unwrap();
        let previous = storage.status(&path).await.unwrap().unwrap();
        let error = storage.replace(&previous, b"candidate").await.unwrap_err();
        assert_eq!(error.failure(), failure);
        let StorageError::Provider { source, .. } = error else { panic!("native cause discarded") };
        let native = source.downcast_ref::<http::ProviderResponse>().unwrap();
        assert_eq!(native.status(), status);
        assert_eq!(serde_json::from_slice::<Value>(native.body()).unwrap()["error"]["code"], code);
        assert_eq!(*requests.lock().unwrap(), [Method::PUT, Method::DELETE]);
        assert_eq!(storage.read(&path).await.unwrap(), b"original");
        assert!(state.lock().unwrap().replacements.is_empty());
    }
}

#[tokio::test]
async fn a_pending_final_reply_is_not_success_and_both_cleanup_causes_survive() {
    let state = remote();
    let remote_state = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let state = remote_state.clone();
            async move {
                if uri.path().starts_with("/replace-session/") {
                    if method == Method::PUT {
                        return response(202, json!({"nextExpectedRanges":["0-"]}).to_string());
                    }
                    if method == Method::DELETE {
                        return response(403, json!({"error":{"code":"accessDenied"}}).to_string());
                    }
                }
                endpoint(State(state), method, uri, headers, body).await
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"original").await.unwrap();
    let previous = storage.status(&path).await.unwrap().unwrap();
    let StorageError::Cleanup { operation, cleanup } =
        storage.replace(&previous, b"candidate").await.unwrap_err()
    else {
        panic!("cleanup failure discarded")
    };
    assert_eq!(operation.failure(), StorageFailure::Protocol);
    assert_eq!(cleanup.failure(), StorageFailure::PermissionDenied);
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
}

#[tokio::test]
async fn withdrawn_publication_authority_still_allows_session_cancellation() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Gate(AtomicBool);
    impl PublicationGate for Gate {
        fn check(&self, _: &ObjectPath) -> Result<(), StorageError> {
            if self.0.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(StorageFailure::PermissionDenied.into())
            }
        }
    }
    let gate = Arc::new(Gate(AtomicBool::new(true)));
    let authority = gate.clone();
    let state = remote();
    let remote_state = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let state = remote_state.clone();
            let gate = authority.clone();
            async move {
                let created = uri.path().ends_with("/createUploadSession");
                let reply = endpoint(State(state), method, uri, headers, body).await;
                if created {
                    gate.0.store(false, Ordering::SeqCst);
                }
                reply
            }
        },
    ))
    .await;
    let mut storage = provider(&server.url);
    storage.publications = gate;
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"original").await.unwrap();
    let previous = storage.status(&path).await.unwrap().unwrap();
    assert_eq!(
        storage.replace(&previous, b"candidate").await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
    let state = state.lock().unwrap();
    assert!(state.replacements.is_empty());
    assert!(!state
        .requests
        .iter()
        .any(|(method, uri)| *method == Method::PUT && uri.starts_with("/replace-session/")));
}

#[tokio::test]
async fn a_lost_replacement_reply_is_not_retried_against_a_new_revision() {
    let state = remote();
    let remote_state = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let state = remote_state.clone();
            async move {
                let final_request =
                    method == Method::PUT && uri.path().starts_with("/replace-session/");
                let reply = endpoint(State(state), method, uri, headers, body).await;
                if final_request && reply.status().is_success() {
                    response(503, json!({"error":{"code":"generalException"}}).to_string())
                } else {
                    reply
                }
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"original").await.unwrap();
    let previous = storage.status(&path).await.unwrap().unwrap();
    assert_eq!(
        storage.replace(&previous, b"candidate").await.unwrap_err().failure(),
        StorageFailure::Network
    );
    assert_eq!(
        storage.replace(&previous, b"candidate").await.unwrap_err().failure(),
        StorageFailure::Conflict
    );
    assert_eq!(storage.read(&path).await.unwrap(), b"candidate");
    let state = state.lock().unwrap();
    assert_eq!(
        state
            .requests
            .iter()
            .filter(|(method, uri)| *method == Method::PUT && uri.starts_with("/replace-session/"))
            .count(),
        1
    );
    assert!(state.replacements.is_empty());
}

#[tokio::test]
async fn replies_that_do_not_acknowledge_the_sent_bytes_are_protocol_errors() {
    let part = crate::session::ONEDRIVE_PART_SIZE;
    let floor = ObjectPath::floor(STORE_ID);
    let item = |id: String, name: &str, size: usize, file: bool| {
        let mut item = json!({"id": id, "name": name, "size": size});
        if file {
            item["file"] = json!({});
        }
        item
    };
    let observed = format!("file:{}", floor.as_str());
    let candidate = b"candidate".len();
    for (total, status, reply) in [
        // Before the last byte: a 202 asking for exactly the next byte.
        (part + 1, 202, json!({"nextExpectedRanges": ["0-"]})),
        (part + 1, 202, json!({"nextExpectedRanges": []})),
        (part + 1, 202, json!({})),
        (part + 1, 202, json!({"nextExpectedRanges": [format!("{}-", part + 1)]})),
        (part + 1, 200, json!({"nextExpectedRanges": [format!("{part}-")]})),
        // After it: the observed item, still a file, at the new size.
        (candidate, 200, item("file:other".into(), "floor", candidate, true)),
        (candidate, 200, item(observed.clone(), "other", candidate, true)),
        (candidate, 201, item(observed.clone(), "floor", candidate - 1, true)),
        (candidate, 200, item(observed.clone(), "floor", candidate, false)),
    ] {
        let state = remote();
        let remote_state = state.clone();
        let substitute = reply.clone();
        let server = TestServer::new(Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let state = remote_state.clone();
                let reply = substitute.clone();
                async move {
                    if method == Method::PUT && uri.path().starts_with("/replace-session/") {
                        state.lock().unwrap().requests.push((method, uri.to_string()));
                        return response(status, reply.to_string());
                    }
                    endpoint(State(state), method, uri, headers, body).await
                }
            },
        ))
        .await;
        let storage = provider(&server.url);
        storage.create(&floor, b"original").await.unwrap();
        let previous = storage.status(&floor).await.unwrap().unwrap();
        let bytes = if total == candidate { b"candidate".to_vec() } else { vec![3; total] };
        let error = storage.replace(&previous, &bytes).await.unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Protocol);
        let StorageError::Provider { source, .. } = error else {
            panic!("provider reply discarded")
        };
        let native = source.downcast_ref::<http::ProviderResponse>().unwrap();
        assert_eq!(native.status(), status);
        assert_eq!(serde_json::from_slice::<Value>(native.body()).unwrap(), reply);
        assert_eq!(storage.read(&floor).await.unwrap(), b"original");
        let state = state.lock().unwrap();
        assert_eq!(
            state
                .requests
                .iter()
                .filter(
                    |(method, uri)| *method == Method::PUT && uri.starts_with("/replace-session/")
                )
                .count(),
            1,
            "no fragment follows an unacknowledged one"
        );
        assert!(state.replacements.is_empty(), "the session was cancelled");
    }
}

#[tokio::test]
async fn a_write_after_session_creation_refuses_completion_and_keeps_its_bytes() {
    for bytes in [b"".as_slice(), b"candidate"] {
        let state = remote();
        let remote_state = state.clone();
        let server = TestServer::new(Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let state = remote_state.clone();
                async move {
                    let created = uri.path().ends_with("/createUploadSession");
                    let reply = endpoint(State(state.clone()), method, uri, headers, body).await;
                    if created {
                        let path = ObjectPath::floor(STORE_ID);
                        let mut remote = state.lock().unwrap();
                        remote.files.insert(path.as_str().into(), b"newer".to_vec());
                        *remote.revisions.get_mut(path.as_str()).unwrap() += 1;
                    }
                    reply
                }
            },
        ))
        .await;
        let storage = provider(&server.url);
        let path = ObjectPath::floor(STORE_ID);
        storage.create(&path, b"original").await.unwrap();
        let previous = storage.status(&path).await.unwrap().unwrap();
        let error = storage.replace(&previous, bytes).await.unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Conflict);
        let StorageError::Provider { source, .. } = error else { panic!("native cause discarded") };
        let native = source.downcast_ref::<http::ProviderResponse>().unwrap();
        assert_eq!(native.status(), 404);
        assert_eq!(
            serde_json::from_slice::<Value>(native.body()).unwrap()["error"]["code"],
            "itemNotFound"
        );
        assert_eq!(storage.read(&path).await.unwrap(), b"newer");
        // Only the creation names the item; the floor is never written again.
        let state = state.lock().unwrap();
        assert!(state.replacements.is_empty());
        assert_eq!(
            state
                .requests
                .iter()
                .filter(|(_, uri)| uri.starts_with("/graph/") && uri.contains("floor"))
                .filter(|(method, _)| *method != Method::GET)
                .count(),
            2,
            "one create, one session creation"
        );
    }
}

#[tokio::test]
async fn a_cancellation_refused_by_the_sessions_own_condition_is_not_a_cleanup_failure() {
    // Live: a losing final fragment's 409 `resourceModified`, then the same
    // refusal of its cancellation, since the session is bound to the old eTag.
    let refusal = json!({"error":{"code":"resourceModified"}});
    for (cancel_status, cancel_body, cleanup) in [
        (409, refusal.clone(), None),
        (409, json!({}), Some(StorageFailure::AlreadyExists)),
        (423, json!({"error":{"code":"resourceLocked"}}), Some(StorageFailure::Conflict)),
    ] {
        let state = remote();
        let remote_state = state.clone();
        let refused = refusal.clone();
        let server = TestServer::new(Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let state = remote_state.clone();
                let refused = refused.clone();
                let cancel_body = cancel_body.clone();
                async move {
                    if uri.path().starts_with("/replace-session/") {
                        if method == Method::PUT {
                            return response(409, refused.to_string());
                        }
                        if method == Method::DELETE {
                            return response(cancel_status, cancel_body.to_string());
                        }
                    }
                    endpoint(State(state), method, uri, headers, body).await
                }
            },
        ))
        .await;
        let storage = provider(&server.url);
        let path = ObjectPath::floor(STORE_ID);
        storage.create(&path, b"original").await.unwrap();
        let previous = storage.status(&path).await.unwrap().unwrap();
        let error = storage.replace(&previous, b"candidate").await.unwrap_err();
        match (cleanup, error) {
            (None, StorageError::Provider { failure, source, .. }) => {
                assert_eq!(failure, StorageFailure::Conflict);
                let native = source.downcast_ref::<http::ProviderResponse>().unwrap();
                assert_eq!(native.status(), 409);
            }
            (Some(expected), StorageError::Cleanup { operation, cleanup }) => {
                // The refusal stays a replacement conflict inside the cleanup error.
                assert_eq!(operation.failure(), StorageFailure::Conflict);
                assert_eq!(cleanup.failure(), expected);
            }
            (cleanup, error) => panic!("unexpected {error:?} for expected cleanup {cleanup:?}"),
        }
        assert_eq!(storage.read(&path).await.unwrap(), b"original");
    }
}

#[tokio::test]
async fn recorded_floor_uploads_keep_onedrive_revision_conditions() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    crate::test_utils::Conformance::new(Arc::new(provider(&server.url)), STORE_ID)
        .conditional_floor_upload()
        .await;
}
