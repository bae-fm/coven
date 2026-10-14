use super::*;

#[tokio::test]
async fn creates_real_folders_and_queries_one_exact_name_on_a_warm_miss() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            captured.lock().unwrap().push((method.clone(), uri.clone()));
            let remote = remote.clone();
            async move { respond(&remote, &method, &uri, &headers, &body) }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    storage.create(&path, b"data").await.unwrap();
    let object = storage.status(&path).await.unwrap().unwrap();
    let parent = {
        let remote = state.lock().unwrap();
        let item = &remote.files[&object.object_id].0;
        assert_eq!(item["name"], "1");
        let mut parent = item["parents"][0].as_str().unwrap().to_owned();
        let file_parent = parent.clone();
        for name in ["31", "store-log", &STORE_ID.to_string()] {
            let folder = &remote.files[&parent].0;
            assert_eq!(folder["name"], name);
            assert_eq!(folder["mimeType"], "application/vnd.google-apps.folder");
            parent = folder["parents"][0].as_str().unwrap().to_owned();
        }
        assert_eq!(parent, "folder");
        file_parent
    };
    requests.lock().unwrap().clear();
    let missing = ObjectPath::store_log(STORE_ID, DeviceId(31), 2.try_into().unwrap());
    assert_eq!(storage.status(&missing).await.unwrap(), None);
    let trace = std::mem::take(&mut *requests.lock().unwrap());
    assert_eq!(trace.len(), 1);
    assert_eq!(trace[0].0, Method::GET);
    assert_eq!(
        crate::providers::tests::query(&trace[0].1)["q"],
        format!("'{parent}' in parents and name = '2' and trashed = false")
    );
    storage.create(&missing, b"next").await.unwrap();
    let trace = requests.lock().unwrap();
    assert_eq!(trace.len(), 6); // Preflight, three ancestor refreshes, content, duplicate check.
    for (_, uri) in trace.iter().filter(|(_, uri)| {
        crate::providers::tests::query(uri)
            .get("q")
            .is_some_and(|q| q.contains("name = '2'"))
    }) {
        assert_eq!(
            crate::providers::tests::query(uri)["q"],
            format!("'{parent}' in parents and name = '2' and trashed = false")
        );
    }
}

#[tokio::test]
async fn listings_scope_every_page_and_seed_cached_ids_without_erasing_omitted_objects() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let first = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    let second = ObjectPath::store_log(STORE_ID, DeviceId(31), 2.try_into().unwrap());
    let parent = {
        let mut remote = state.lock().unwrap();
        duplicate(&mut remote, &first, "first", "2026-10-06T00:00:00Z", 31);
        duplicate(&mut remote, &second, "second", "2026-10-06T00:00:01Z", 31);
        remote.directory(&format!("{STORE_ID}/store-log/31"))
    };
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            captured.lock().unwrap().push((method.clone(), uri.clone()));
            let remote = remote.clone();
            async move { respond(&remote, &method, &uri, &headers, &body) }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let prefix = ObjectPrefix::store_log(STORE_ID, DeviceId(31));
    assert_eq!(storage.list(&prefix).await.unwrap().len(), 2);
    let trace = std::mem::take(&mut *requests.lock().unwrap());
    let queries: Vec<_> = trace
        .iter()
        .filter(|(_, uri)| uri.path() == "/drive/files")
        .map(|(_, uri)| crate::providers::tests::query(uri))
        .collect();
    assert_eq!(queries.len(), 5); // Three ancestors, two child pages.
    assert_eq!(
        queries[3]["q"],
        format!("'{parent}' in parents and trashed = false")
    );
    assert_eq!(queries[3]["q"], queries[4]["q"]);
    assert_eq!(queries[4]["pageToken"], "1");
    assert!(queries.iter().all(|query| !query["q"].contains("contains")));
    state.lock().unwrap().hide_listings = true;
    assert!(storage.list(&prefix).await.unwrap().is_empty());
    requests.lock().unwrap().clear();
    assert_eq!(storage.read(&first).await.unwrap(), b"data");
    assert_eq!(storage.status(&second).await.unwrap().unwrap().size, 4);
    let trace = requests.lock().unwrap();
    assert_eq!(trace.len(), 3); // Metadata + body, then exact status.
    assert!(trace.iter().all(|(_, uri)| uri.path() != "/drive/files"));
}

#[tokio::test]
async fn paginated_exact_lookup_waits_for_the_earliest_copy_and_caches_its_id() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    {
        let mut remote = state.lock().unwrap();
        duplicate(&mut remote, &path, "a-later", "2026-10-06T00:00:01Z", 31);
        duplicate(&mut remote, &path, "z-first", "2026-10-06T00:00:00Z", 31);
    }
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            captured.lock().unwrap().push(uri.clone());
            let remote = remote.clone();
            async move { respond(&remote, &method, &uri, &headers, &body) }
        },
    ))
    .await;
    let storage = provider(&server.url);
    assert_eq!(
        storage.status(&path).await.unwrap().unwrap().object_id,
        "z-first"
    );
    let trace = std::mem::take(&mut *requests.lock().unwrap());
    assert_eq!(trace.len(), 7); // Root, three ancestors, two exact pages, metadata.
    let query = crate::providers::tests::query(&trace[4]);
    assert!(query["q"].ends_with("name = '1' and trashed = false"));
    assert_eq!(crate::providers::tests::query(&trace[5])["pageToken"], "1");
    storage.status(&path).await.unwrap();
    let trace = requests.lock().unwrap();
    assert_eq!(trace.len(), 1);
    assert_eq!(trace[0].path(), "/drive/files/z-first");
}

#[tokio::test]
async fn failed_discovery_does_not_cache_objects_or_turn_an_exact_lookup_into_absence() {
    for incomplete in [true, false] {
        let state = Arc::new(Mutex::new(Remote::default()));
        let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
        let other = ObjectPath::store_log(STORE_ID, DeviceId(31), 2.try_into().unwrap());
        {
            let mut remote = state.lock().unwrap();
            duplicate(&mut remote, &path, "first", "2026-10-06T00:00:00Z", 31);
            duplicate(&mut remote, &other, "second", "2026-10-06T00:00:00Z", 31);
        }
        let server = TestServer::new(Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let remote = state.clone();
                async move {
                    let query = crate::providers::tests::query(&uri);
                    if query.contains_key("pageToken") {
                        if incomplete {
                            reply(json!({"files":[],"incompleteSearch":true}))
                        } else {
                            response(503, "failed page")
                        }
                    } else {
                        respond(&remote, &method, &uri, &headers, &body)
                    }
                }
            },
        ))
        .await;
        let storage = provider(&server.url);
        assert!(storage
            .list(&ObjectPrefix::store_logs(STORE_ID))
            .await
            .is_err());
        assert!(storage.provider.object_ids.lock().await.is_empty());
        assert!(!storage
            .provider
            .folder_ids
            .lock()
            .await
            .contains_key(&format!("{STORE_ID}/store-log/31")));
    }
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    state
        .lock()
        .unwrap()
        .directory(&format!("{STORE_ID}/store-log/31"));
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = state.clone();
            async move {
                if crate::providers::tests::query(&uri)
                    .get("q")
                    .is_some_and(|q| q.contains("name = '1'"))
                {
                    reply(json!({"files":[],"incompleteSearch":true}))
                } else {
                    respond(&remote, &method, &uri, &headers, &body)
                }
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    assert_eq!(
        storage.status(&path).await.unwrap_err().failure(),
        StorageFailure::Protocol
    );
    assert_eq!(
        storage.read(&path).await.unwrap_err().failure(),
        StorageFailure::Protocol
    );
    assert_eq!(
        storage.create(&path, b"data").await.unwrap_err().failure(),
        StorageFailure::Protocol
    );
}

#[tokio::test]
async fn missing_directory_is_not_cached_and_an_empty_page_can_have_a_continuation() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = remote.clone();
            async move {
                let query = crate::providers::tests::query(&uri);
                if query.get("q").is_some_and(|q| q.contains("name = '1'")) {
                    if !query.contains_key("pageToken") {
                        return reply(json!({"files":[],"nextPageToken":"later"}));
                    }
                    assert_eq!(query["pageToken"], "later");
                    return reply(json!({"files":[remote.lock().unwrap().files["object"].0]}));
                }
                respond(&remote, &method, &uri, &headers, &body)
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    assert_eq!(storage.status(&path).await.unwrap(), None);
    duplicate(
        &mut state.lock().unwrap(),
        &path,
        "object",
        "2026-10-06T00:00:00Z",
        31,
    );
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn a_listing_omission_cannot_switch_a_known_path_to_a_later_duplicate() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    let parent = {
        let mut remote = state.lock().unwrap();
        duplicate(&mut remote, &path, "first", "2026-10-06T00:00:00Z", 31);
        duplicate(&mut remote, &path, "later", "2026-10-06T00:00:01Z", 31);
        remote.files.get_mut("later").unwrap().1 = b"else".to_vec();
        remote.directory(&format!("{STORE_ID}/store-log/31"))
    };
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = state.clone();
            let parent = parent.clone();
            async move {
                if crate::providers::tests::query(&uri).get("q")
                    == Some(&format!("'{parent}' in parents and trashed = false"))
                {
                    reply(json!({"files":[remote.lock().unwrap().files["later"].0]}))
                } else {
                    respond(&remote, &method, &uri, &headers, &body)
                }
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    assert_eq!(
        storage.status(&path).await.unwrap().unwrap().object_id,
        "first"
    );
    assert_eq!(
        storage
            .list(&ObjectPrefix::store_logs(STORE_ID))
            .await
            .unwrap()[0]
            .object_id,
        "first"
    );
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn repeated_search_entries_do_not_delete_the_copy_being_kept() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    duplicate(
        &mut state.lock().unwrap(),
        &path,
        "first",
        "2026-10-06T00:00:00Z",
        31,
    );
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = remote.clone();
            async move {
                let query = crate::providers::tests::query(&uri);
                if query.get("q").is_some_and(|q| q.contains("name = '1'")) {
                    let mut page = json!({"files":[remote.lock().unwrap().files["first"].0]});
                    if !query.contains_key("pageToken") {
                        page["nextPageToken"] = json!("repeat-entry");
                    }
                    reply(page)
                } else {
                    respond(&remote, &method, &uri, &headers, &body)
                }
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    assert_eq!(
        storage.create(&path, b"data").await.unwrap_err().failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(state.lock().unwrap().object_ids(), ["first"]);
}

#[tokio::test]
async fn warm_replacements_use_the_known_id_and_parallel_creates_share_folders() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            captured.lock().unwrap().push((method.clone(), uri.clone()));
            let remote = remote.clone();
            async move { respond(&remote, &method, &uri, &headers, &body) }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let positions = ObjectPath::positions(STORE_ID, DeviceId(31));
    let clock = ObjectPath::clock(STORE_ID, DeviceId(31));
    let (first, second) = tokio::join!(
        storage.replace(&positions, b"first"),
        storage.replace(&clock, b"time")
    );
    first.unwrap();
    second.unwrap();
    requests.lock().unwrap().clear();
    storage.replace(&positions, b"other").await.unwrap();
    let trace = std::mem::take(&mut *requests.lock().unwrap());
    assert_eq!(trace.len(), 2);
    assert_eq!(trace[0].0, Method::GET);
    assert_eq!(trace[1].0, Method::PATCH);
    assert!(trace.iter().all(|(_, uri)| uri.path() != "/drive/files"));
    let remote = state.lock().unwrap();
    assert_eq!(
        remote
            .files
            .values()
            .filter(|(item, _)| item["name"] == STORE_ID.to_string())
            .count(),
        1
    );
    assert_eq!(remote.object_ids().len(), 2);
}

#[tokio::test]
async fn snapshot_listing_enters_only_the_requested_audience_and_caches_writer_folders() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::snapshot(
        STORE_ID,
        coven_merge::Audience::Store,
        DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let audience = {
        let mut remote = state.lock().unwrap();
        duplicate(&mut remote, &path, "snapshot", "2026-10-06T00:00:00Z", 31);
        let foreign = remote.directory(&format!("{STORE_ID}/files/31"));
        remote.files.insert(
            "unrelated".into(),
            (
                json!({"id":"unrelated","name":"invalid","parents":[foreign]}),
                Vec::new(),
            ),
        );
        remote.directory(&format!("{STORE_ID}/snapshots/store"))
    };
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            captured.lock().unwrap().push(uri.clone());
            let remote = remote.clone();
            async move { respond(&remote, &method, &uri, &headers, &body) }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let listed = storage
        .list(&ObjectPrefix::audience_snapshots(
            STORE_ID,
            &coven_merge::Audience::Store,
        ))
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path, path);
    let trace = std::mem::take(&mut *requests.lock().unwrap());
    assert_eq!(trace.len(), 6); // Root, three ancestors, audience, writer.
    assert_eq!(
        crate::providers::tests::query(&trace[4])["q"],
        format!("'{audience}' in parents and trashed = false")
    );
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
    assert_eq!(requests.lock().unwrap().len(), 2);
}

fn folder(remote: &mut Remote, parent: &str, name: &str, id: &str, created: &str) {
    let mut item = metadata(
        json!({"id":id,"name":name,"parents":[parent],"mimeType":"application/vnd.google-apps.folder"}),
        0,
    );
    item["createdTime"] = json!(created);
    remote.files.insert(id.into(), (item, Vec::new()));
}

fn object(remote: &mut Remote, parent: &str, name: &str, id: &str, created: &str) {
    let mut item = metadata(
        json!({"id":id,"name":name,"parents":[parent],"properties":{"covenDevice":"31"}}),
        4,
    );
    item["createdTime"] = json!(created);
    remote.files.insert(id.into(), (item, b"data".to_vec()));
}

#[tokio::test]
async fn folder_creation_race_keeps_both_folders_and_publishes_under_the_oldest() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = remote.clone();
            async move {
                let reply = respond(&remote, &method, &uri, &headers, &body);
                if method == Method::POST && uri.path() == "/drive/files" {
                    let value: Value = serde_json::from_slice(&body).unwrap();
                    if value["name"] == "positions" {
                        let parent = value["parents"][0].as_str().unwrap();
                        let mut remote = remote.lock().unwrap();
                        folder(
                            &mut remote,
                            parent,
                            "positions",
                            "racing-folder",
                            "2026-10-05T00:00:00Z",
                        );
                        object(
                            &mut remote,
                            "racing-folder",
                            "32",
                            "peer",
                            "2026-10-06T00:00:00Z",
                        );
                    }
                }
                reply
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let path = ObjectPath::positions(STORE_ID, DeviceId(31));
    storage.replace(&path, b"mine").await.unwrap();
    let id = storage.status(&path).await.unwrap().unwrap().object_id;
    assert_eq!(
        state.lock().unwrap().files[&id].0["parents"],
        json!(["racing-folder"])
    );
    assert_eq!(
        storage
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        state
            .lock()
            .unwrap()
            .files
            .values()
            .filter(|(item, _)| item["name"] == "positions")
            .count(),
        2
    );
}

#[tokio::test]
async fn cached_folders_discover_new_siblings_and_devices_choose_the_same_time_then_id() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let store = state.lock().unwrap().directory(&STORE_ID.to_string());
    folder(
        &mut state.lock().unwrap(),
        &store,
        "positions",
        "z-later",
        "2026-10-07T00:00:00Z",
    );
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let first = provider(&server.url);
    let seed = ObjectPath::positions(STORE_ID, DeviceId(30));
    first.replace(&seed, b"seed").await.unwrap();
    {
        let mut remote = state.lock().unwrap();
        folder(
            &mut remote,
            &store,
            "positions",
            "b-older",
            "2026-10-06T01:00:00+01:00",
        );
        folder(
            &mut remote,
            &store,
            "positions",
            "a-older",
            "2026-10-06T00:00:00Z",
        );
        object(&mut remote, "b-older", "33", "peer", "2026-10-06T00:00:00Z");
    }
    // Creation must refresh even before a listing has discovered the siblings.
    let path = ObjectPath::positions(STORE_ID, DeviceId(31));
    first.replace(&path, b"mine").await.unwrap();
    let second = provider_for_device(&server.url, DeviceId(32));
    let other = ObjectPath::positions(STORE_ID, DeviceId(32));
    second.replace(&other, b"peer").await.unwrap();
    for (storage, path) in [(&first, &path), (&second, &other)] {
        let id = storage.status(path).await.unwrap().unwrap().object_id;
        assert_eq!(
            state.lock().unwrap().files[&id].0["parents"],
            json!(["a-older"])
        );
    }
    // The already cached listing prefix must learn another sibling too.
    first.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
    {
        let mut remote = state.lock().unwrap();
        folder(
            &mut remote,
            &store,
            "positions",
            "last",
            "2026-10-08T00:00:00Z",
        );
        object(
            &mut remote,
            "last",
            "34",
            "late-peer",
            "2026-10-08T00:00:00Z",
        );
    }
    let prefix = ObjectPrefix::positions(STORE_ID);
    assert_eq!(first.list(&prefix).await.unwrap().len(), 5);
    assert_eq!(second.list(&prefix).await.unwrap().len(), 5);
}

#[tokio::test]
async fn resumed_upload_and_non_owner_deletion_use_the_objects_actual_folder() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let removals = Arc::new(Mutex::new(Vec::new()));
    let recorded = removals.clone();
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = remote.clone();
            let recorded = recorded.clone();
            async move {
                if let Some(parent) = crate::providers::tests::query(&uri).get("removeParents") {
                    recorded.lock().unwrap().push(parent.clone());
                }
                respond(&remote, &method, &uri, &headers, &body)
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    let recorded = upload.encode().unwrap();
    let parent = {
        let mut remote = state.lock().unwrap();
        let parent = remote.directory(&format!("{STORE_ID}/devices/31"));
        let logs = remote.directory(&format!("{STORE_ID}/devices"));
        folder(
            &mut remote,
            &logs,
            "31",
            "older-folder",
            "2026-10-05T00:00:00Z",
        );
        parent
    };
    storage.upload_part(&mut upload, b"data").await.unwrap();
    let second = provider(&server.url);
    let mut resumed = UploadSession::decode(recorded.as_bytes()).unwrap();
    second.resume_upload(&mut resumed).await.unwrap();
    assert!(resumed.is_complete());
    let id = second.status(&path).await.unwrap().unwrap().object_id;
    state.lock().unwrap().files.get_mut(&id).unwrap().0["ownedByMe"] = json!(false);
    second.delete(&path).await.unwrap();
    assert_eq!(*removals.lock().unwrap(), [parent]);
    assert_eq!(second.status(&path).await.unwrap(), None);
}

#[tokio::test]
async fn a_failed_sibling_page_does_not_publish_a_partial_union() {
    let state = Arc::new(Mutex::new(Remote::default()));
    {
        let mut remote = state.lock().unwrap();
        let store = remote.directory(&STORE_ID.to_string());
        for id in ["first-folder", "second-folder"] {
            folder(&mut remote, &store, "positions", id, "2026-10-06T00:00:00Z");
        }
        object(
            &mut remote,
            "first-folder",
            "31",
            "first",
            "2026-10-06T00:00:00Z",
        );
        object(
            &mut remote,
            "second-folder",
            "32",
            "second",
            "2026-10-06T00:00:00Z",
        );
    }
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = state.clone();
            async move {
                let query = crate::providers::tests::query(&uri);
                if query
                    .get("q")
                    .is_some_and(|q| q.contains("'second-folder' in parents"))
                    && (query.contains_key("pageToken") || query["q"].contains("name = '35'"))
                {
                    reply(json!({"files":[],"incompleteSearch":true}))
                } else {
                    respond(&remote, &method, &uri, &headers, &body)
                }
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    assert_eq!(
        storage
            .list(&ObjectPrefix::positions(STORE_ID))
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Protocol
    );
    assert!(storage.provider.object_ids.lock().await.is_empty());
    let path = ObjectPath::positions(STORE_ID, DeviceId(35));
    assert_eq!(
        storage.status(&path).await.unwrap_err().failure(),
        StorageFailure::Protocol
    );
}

#[tokio::test]
async fn folder_order_requires_a_valid_creation_time() {
    for invalid in [Value::Null, json!("invalid")] {
        let state = Arc::new(Mutex::new(Remote::default()));
        let id = state
            .lock()
            .unwrap()
            .directory(&format!("{STORE_ID}/positions"));
        state.lock().unwrap().files.get_mut(&id).unwrap().0["createdTime"] = invalid;
        let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
        let storage = provider(&server.url);
        assert!(storage
            .list(&ObjectPrefix::positions(STORE_ID))
            .await
            .is_err());
        assert!(storage
            .replace(&ObjectPath::positions(STORE_ID, DeviceId(31)), b"data")
            .await
            .is_err());
    }
}

#[tokio::test]
async fn a_warm_miss_queries_all_sibling_parents_in_one_exact_query() {
    let state = Arc::new(Mutex::new(Remote::default()));
    {
        let mut remote = state.lock().unwrap();
        let store = remote.directory(&STORE_ID.to_string());
        for id in ["first", "second"] {
            folder(&mut remote, &store, "positions", id, "2026-10-06T00:00:00Z");
        }
    }
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = state.clone();
            let captured = captured.clone();
            async move {
                captured.lock().unwrap().push(uri.clone());
                respond(&remote, &method, &uri, &headers, &body)
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    storage
        .list(&ObjectPrefix::positions(STORE_ID))
        .await
        .unwrap();
    requests.lock().unwrap().clear();
    assert_eq!(
        storage
            .status(&ObjectPath::positions(STORE_ID, DeviceId(31)))
            .await
            .unwrap(),
        None
    );
    let trace = requests.lock().unwrap();
    assert_eq!(trace.len(), 1);
    assert_eq!(
        crate::providers::tests::query(&trace[0])["q"],
        "('first' in parents or 'second' in parents) and name = '31' and trashed = false"
    );
}

#[tokio::test]
async fn omitting_a_known_folder_does_not_change_the_creation_target() {
    let state = Arc::new(Mutex::new(Remote::default()));
    {
        let mut remote = state.lock().unwrap();
        let store = remote.directory(&STORE_ID.to_string());
        folder(
            &mut remote,
            &store,
            "positions",
            "oldest",
            "2026-10-05T00:00:00Z",
        );
        folder(
            &mut remote,
            &store,
            "positions",
            "later",
            "2026-10-06T00:00:00Z",
        );
    }
    let omit = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hidden = omit.clone();
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = remote.clone();
            let hidden = hidden.clone();
            async move {
                if hidden.load(std::sync::atomic::Ordering::SeqCst)
                    && crate::providers::tests::query(&uri)
                        .get("q")
                        .is_some_and(|q| q.contains("name = 'positions'"))
                {
                    reply(json!({"files":[remote.lock().unwrap().files["later"].0]}))
                } else {
                    respond(&remote, &method, &uri, &headers, &body)
                }
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    storage
        .list(&ObjectPrefix::positions(STORE_ID))
        .await
        .unwrap();
    omit.store(true, std::sync::atomic::Ordering::SeqCst);
    let path = ObjectPath::positions(STORE_ID, DeviceId(31));
    storage.replace(&path, b"data").await.unwrap();
    let id = storage.status(&path).await.unwrap().unwrap().object_id;
    assert_eq!(
        state.lock().unwrap().files[&id].0["parents"],
        json!(["oldest"])
    );
    assert_eq!(
        storage
            .list(&ObjectPrefix::positions(STORE_ID))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn duplicate_folders_union_children_and_choose_the_oldest_creation_target() {
    let state = Arc::new(Mutex::new(Remote::default()));
    {
        let mut remote = state.lock().unwrap();
        let store = remote.directory(&STORE_ID.to_string());
        folder(
            &mut remote,
            &store,
            "store-log",
            "z-old",
            "2026-10-05T00:00:00Z",
        );
        folder(
            &mut remote,
            &store,
            "store-log",
            "a-new",
            "2026-10-06T00:00:00Z",
        );
        folder(
            &mut remote,
            "z-old",
            "31",
            "old-writer",
            "2026-10-05T00:00:00Z",
        );
        folder(
            &mut remote,
            "a-new",
            "31",
            "new-writer",
            "2026-10-06T00:00:00Z",
        );
        object(
            &mut remote,
            "old-writer",
            "1",
            "first",
            "2026-10-05T00:00:00Z",
        );
        object(
            &mut remote,
            "new-writer",
            "2",
            "second",
            "2026-10-06T00:00:00Z",
        );
        object(
            &mut remote,
            "new-writer",
            "1",
            "retry",
            "2026-10-06T00:00:00Z",
        );
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let first = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    assert_eq!(
        storage.status(&first).await.unwrap().unwrap().object_id,
        "first"
    );
    for prefix in [
        ObjectPrefix::store_logs(STORE_ID),
        ObjectPrefix::store_log(STORE_ID, DeviceId(31)),
    ] {
        let listed = storage.list(&prefix).await.unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|item| item.object_id.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }
    assert_eq!(
        storage.create(&first, b"data").await.unwrap_err().failure(),
        StorageFailure::AlreadyExists
    );
    assert!(!state.lock().unwrap().files.contains_key("retry"));
    let next = ObjectPath::store_log(STORE_ID, DeviceId(31), 3.try_into().unwrap());
    storage.create(&next, b"next").await.unwrap();
    let id = storage.status(&next).await.unwrap().unwrap().object_id;
    assert_eq!(
        state.lock().unwrap().files[&id].0["parents"],
        json!(["old-writer"])
    );
    let another = ObjectPath::store_log(STORE_ID, DeviceId(32), std::num::NonZeroU64::MIN);
    storage.create(&another, b"peer").await.unwrap();
    let id = storage.status(&another).await.unwrap().unwrap().object_id;
    let remote = state.lock().unwrap();
    let parent = remote.files[&id].0["parents"][0].as_str().unwrap();
    assert_eq!(remote.files[parent].0["parents"], json!(["z-old"]));
    for id in ["z-old", "a-new", "old-writer", "new-writer"] {
        assert!(remote.files.contains_key(id));
    }
}

#[tokio::test]
async fn writer_discovery_queries_folder_children_without_entering_history() {
    let server = TestServer::new(Router::new().fallback(|uri: Uri| async move {
        if uri.path().ends_with("/folder") {
            return reply(json!({"id":"folder","mimeType":"application/vnd.google-apps.folder","trashed":false}));
        }
        let q = crate::providers::tests::query(&uri);
        let (id, name, parent, next) = if q["q"].contains(&format!("name = '{STORE_ID}'")) {
            ("store", STORE_ID.to_string(), "folder", false)
        } else if q["q"].contains("name = 'store-log'") {
            ("log", "store-log".into(), "store", false)
        } else {
            assert_eq!(q["q"], "'log' in parents and trashed = false and mimeType = 'application/vnd.google-apps.folder'");
            if q.contains_key("pageToken") {
                assert_eq!(q["pageToken"], "next");
                ("second", "32".into(), "log", false)
            } else { ("first", "31".into(), "log", true) }
        };
        let mut value = json!({"files":[{"id":id,"name":name,"parents":[parent],"mimeType":"application/vnd.google-apps.folder","createdTime":"2026-10-06T00:00:00Z","trashed":false}]});
        if next { value["nextPageToken"] = json!("next"); }
        reply(value)
    })).await;
    assert_eq!(
        provider(&server.url).list_writers(STORE_ID).await.unwrap(),
        [DeviceId(31), DeviceId(32)]
    );
}
