use super::*;
use profile::*;

#[test]
fn profile_table_records_certification_and_publication_requirements() {
    for profile in PROFILES {
        if let Certification::Passed { date, suite } = profile.certification {
            assert_eq!(date.len(), 10);
            assert!(suite > 0);
            if profile.multipart_timestamp == MultipartTimestamp::Start {
                assert!(
                    profile.multipart.uses_copy(),
                    "a certified start-time service needs copy publication"
                );
            }
        }
    }
    assert_eq!(PROFILES[0].certification, Certification::Uncertified);
}

#[tokio::test]
async fn scope_policy_controls_head_and_get_without_inferring_a_checksum() {
    for (policy, expected) in [
        (Checksums::ScopeStated, false),
        (Checksums::UnstatedIgnored, true),
    ] {
        let state = Arc::new(Mutex::new(Remote {
            response_headers: BTreeMap::from([(
                "x-amz-checksum-sha256".into(),
                "J3CJ2RwL308uaGK6fkoHYFEZQx9dE/cm3TUrBvGyBqk=".into(),
            )]),
            ..Remote::default()
        }));
        let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
        let mut profile = PROFILES[0];
        profile.checksums = policy;
        let storage = profile_provider(&server.url, profile);
        let target = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
        storage.replace(&target, b"bytes").await.unwrap();
        let head = storage.status(&target).await;
        let get = storage.read_object(&target, None).await;
        if expected {
            assert_eq!(head.unwrap().unwrap().checksum, None);
            let read = get.unwrap();
            assert_eq!(read.metadata.checksum, None);
            assert_eq!(read.bytes, b"bytes");
        } else {
            assert_eq!(head.unwrap_err().failure(), StorageFailure::Protocol);
            assert_eq!(get.unwrap_err().failure(), StorageFailure::Protocol);
        }
    }
}

#[tokio::test]
async fn listing_profile_selects_v1_and_follows_key_markers() {
    let keys = [1, 2].map(|n| {
        ObjectPath::device_log(
            STORE_ID,
            coven_foundation::id_source::DeviceId(31),
            n.try_into().unwrap(),
        )
        .under("store")
    });
    let server = TestServer::new(Router::new().fallback(move |uri: Uri| {
        let keys = keys.clone();
        async move {
            let q = query(&uri);
            assert!(!q.contains_key("list-type"));
            let index = usize::from(q.contains_key("marker"));
            if index == 1 { assert_eq!(q["marker"], keys[0]); }
            let key = &keys[index];
            response(200, format!("<ListBucketResult><IsTruncated>{}</IsTruncated><Contents><Key>{key}</Key><Size>4</Size><ETag>etag</ETag><LastModified>2026-10-06T00:00:00Z</LastModified></Contents></ListBucketResult>", index == 0))
        }
    })).await;
    let mut storage = provider(&server.url);
    Arc::get_mut(&mut storage.provider).unwrap().profile.listing = Listing::V1;
    assert_eq!(
        storage
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn unstated_scope_skips_checksum_parsing_but_keeps_required_metadata() {
    let state = Arc::new(Mutex::new(Remote {
        response_headers: BTreeMap::from([("x-amz-checksum-sha256".into(), String::new())]),
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let mut profile = PROFILES[0];
    profile.checksums = Checksums::UnstatedIgnored;
    let storage = profile_provider(&server.url, profile);
    let path = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
    storage.replace(&path, b"data").await.unwrap();
    assert_eq!(storage.status(&path).await.unwrap().unwrap().checksum, None);
    let read = storage.read_object(&path, None).await.unwrap();
    assert_eq!(read.metadata.checksum, None);
    assert_eq!(read.bytes, b"data");
    for (scope, full) in [("FULL_OBJECT", true), ("COMPOSITE", false)] {
        state.lock().unwrap().response_headers = BTreeMap::from([
            ("x-amz-checksum-type".into(), scope.into()),
            ("x-amz-checksum-sha256".into(), "digest".into()),
        ]);
        let digest = ChecksumValue::Sha256Base64("digest".into());
        assert_eq!(
            storage.status(&path).await.unwrap().unwrap().checksum,
            Some(if full {
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

    state
        .lock()
        .unwrap()
        .response_headers
        .insert("etag".into(), String::new());
    let immutable = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    storage.create(&immutable, b"data").await.unwrap();
    assert_eq!(
        storage.status(&immutable).await.unwrap_err().failure(),
        StorageFailure::Protocol
    );
}

fn profile_provider(url: &str, profile: S3Profile) -> StorageConnection<S3Storage> {
    StorageConnection::from_provider(Arc::new(
        S3Storage::with_profile(
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "us-east-1".into(),
                endpoint: Some(url.parse().unwrap()),
                prefix: "store".into(),
            },
            S3Credentials {
                access_key_id: "access".into(),
                secret_access_key: SecretText::new("secret".into()),
            },
            Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
            Arc::new(SequentialIds::new()),
            profile,
        )
        .unwrap(),
    ))
}
