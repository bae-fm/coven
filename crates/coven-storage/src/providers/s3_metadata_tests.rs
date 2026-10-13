use super::*;

#[tokio::test]
async fn gcs_listing_and_exact_status_keep_the_same_publication_time() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = super::gcs_provider(&server.url);
    let path = ObjectPath::store_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    storage.create(&path, b"immutable entry").await.unwrap();
    let exact = storage.status(&path).await.unwrap().unwrap();
    let listed = storage
        .list(&ObjectPrefix::store_logs(STORE_ID))
        .await
        .unwrap();
    assert_eq!(listed, std::slice::from_ref(&exact));
    assert_eq!(
        storage.read_object(&path, None).await.unwrap().metadata,
        exact
    );
}

#[tokio::test]
async fn gcs_checksum_reads_repeated_hash_headers() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let state = state.clone();
            async move {
                let mut response = respond(&state, method, uri, headers, body);
                response
                    .headers_mut()
                    .append("x-goog-hash", "md5=ignored".parse().unwrap());
                response
                    .headers_mut()
                    .append("x-goog-hash", "crc32c=AAAAAA==".parse().unwrap());
                response
            }
        },
    ))
    .await;
    let storage = super::gcs_provider(&server.url);
    let path = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
    storage.replace(&path, b"first").await.unwrap();
    assert_eq!(
        storage.status(&path).await.unwrap().unwrap().checksum,
        Some(ObjectChecksum::FullObject(ChecksumValue::Crc32cBase64(
            "AAAAAA==".into()
        )))
    );
}

#[tokio::test]
async fn native_metadata_keeps_revision_checksum_and_time_separate() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
    storage.create(&path, b"first").await.unwrap();
    let mut first = storage.status(&path).await.unwrap().unwrap();
    assert_eq!(first.checksum, None, "ETag is not a content checksum");
    for bytes in [b"other", b"other"] {
        storage.replace(&path, bytes).await.unwrap();
        let next = storage.read_object(&path, None).await.unwrap();
        assert_eq!(next.bytes, bytes);
        assert_eq!(next.metadata.size, first.size);
        assert_eq!(next.metadata.stored_at, first.stored_at);
        assert_ne!(next.metadata.revision, first.revision);
        assert_eq!(next.metadata, storage.status(&path).await.unwrap().unwrap());
        first = next.metadata;
    }
    for scope in ["FULL_OBJECT", "COMPOSITE"] {
        state.lock().unwrap().response_headers = BTreeMap::from([
            ("x-amz-checksum-sha256".into(), "digest".into()),
            ("x-amz-checksum-type".into(), scope.into()),
            ("x-amz-version-id".into(), "native-version".into()),
        ]);
        let observed = storage.status(&path).await.unwrap().unwrap();
        assert_eq!(observed.revision, "version:native-version");
        let digest = ChecksumValue::Sha256Base64("digest".into());
        assert_eq!(
            observed.checksum,
            Some(if scope == "FULL_OBJECT" {
                ObjectChecksum::FullObject(digest)
            } else {
                ObjectChecksum::Composite(digest)
            })
        );
    }
    state
        .lock()
        .unwrap()
        .response_headers
        .remove("x-amz-checksum-type");
    assert_eq!(
        storage.status(&path).await.unwrap_err().failure(),
        StorageFailure::Protocol
    );
}

#[tokio::test]
async fn exact_status_distinguishes_absence_from_failed_lookup_and_missing_bucket() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
    assert_eq!(storage.status(&path).await.unwrap(), None);
    for (code, status, expected) in [
        ("AccessDenied", 403, StorageFailure::PermissionDenied),
        ("ServiceUnavailable", 503, StorageFailure::Network),
        ("NoSuchBucket", 404, StorageFailure::ContainerNotFound),
    ] {
        state.lock().unwrap().forced_error = Some((code, status));
        assert_eq!(storage.status(&path).await.unwrap_err().failure(), expected);
    }
}

#[tokio::test]
async fn gcs_generation_changes_for_equal_bytes_and_hash_has_full_object_scope() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = super::gcs_provider(&server.url);
    let path = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
    assert_eq!(storage.status(&path).await.unwrap(), None);
    storage.replace(&path, b"first").await.unwrap();
    let first = storage.status(&path).await.unwrap().unwrap();
    storage.replace(&path, b"first").await.unwrap();
    state
        .lock()
        .unwrap()
        .response_headers
        .insert("x-goog-hash".into(), "crc32c=AAAAAA==,md5=ignored".into());
    let next = storage.status(&path).await.unwrap().unwrap();
    assert_eq!(next.stored_at, first.stored_at);
    assert_eq!(next.size, first.size);
    assert_ne!(next.revision, first.revision);
    assert_eq!(
        next.checksum,
        Some(ObjectChecksum::FullObject(ChecksumValue::Crc32cBase64(
            "AAAAAA==".into()
        )))
    );
}
