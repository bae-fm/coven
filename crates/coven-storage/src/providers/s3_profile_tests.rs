use super::*;
use profile::*;

#[test]
fn profile_table_records_certification_and_publication_requirements() {
    for profile in PROFILES {
        if let Certification::Passed { date, suite } = profile.certification {
            assert_eq!(date.len(), 10);
            assert!(suite > 0);
            if profile.checksums == Checksums::Absent {
                assert!(
                    !profile.request_checksums,
                    "a service returning no checksum is not asked for one"
                );
            }
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

pub(super) fn profile_provider(url: &str, profile: S3Profile) -> StorageConnection<S3Storage> {
    StorageConnection::from_provider(Arc::new(
        S3Storage::with_profile(
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "us-east-1".into(),
                endpoint: Some(url.parse().unwrap()),
                profile: None,
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

#[test]
fn r2_profile_matches_account_hosts_without_matching_lookalikes() {
    use S3ProfileName::{Aws, R2};
    for (endpoint, name) in [
        ("https://account.r2.cloudflarestorage.com", R2),
        ("https://ACCOUNT.R2.CLOUDFLARESTORAGE.COM/", R2),
        ("https://account.eu.r2.cloudflarestorage.com", R2),
        ("https://r2.cloudflarestorage.com", Aws),
        ("https://account.notr2.cloudflarestorage.com", Aws),
        (
            "https://account.r2.cloudflarestorage.com.attacker.test",
            Aws,
        ),
        ("https://other.test/account.r2.cloudflarestorage.com", Aws),
    ] {
        assert_eq!(
            S3Profile::select(Some(&endpoint.parse().unwrap()), None)
                .unwrap()
                .name,
            name
        );
    }
}

#[tokio::test]
async fn r2_endpoint_ignores_unscoped_checksums_on_status_and_reads() {
    let state = Arc::new(Mutex::new(Remote {
        response_headers: BTreeMap::from([(
            "x-amz-checksum-sha256".into(),
            "J3CJ2RwL308uaGK6fkoHYFEZQx9dE/cm3TUrBvGyBqk=".into(),
        )]),
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let mut storage = S3Storage::new(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "auto".into(),
            endpoint: Some("https://account.r2.cloudflarestorage.com".parse().unwrap()),
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
            .endpoint_url(&server.url)
            .build(),
    );
    let storage = StorageConnection::from_provider(Arc::new(storage));
    let path = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
    storage.replace(&path, b"bytes").await.unwrap();
    let status = storage.status(&path).await.unwrap().unwrap();
    assert_eq!(status.checksum, None);
    for (range, expected) in [
        (None, b"bytes".as_slice()),
        (Some(ByteRange::new(1, 4).unwrap()), b"yte".as_slice()),
    ] {
        let read = storage.read_object(&path, range).await.unwrap();
        assert_eq!(read.metadata.checksum, None);
        assert_eq!(read.metadata.size, 5);
        assert_eq!(read.metadata.revision, status.revision);
        assert_eq!(read.bytes, expected);
    }
}

#[test]
fn explicit_name_selects_its_profile_for_an_uncatalogued_endpoint() {
    let endpoint: url::Url = "http://localhost:3900".parse().unwrap();
    for (name, expected) in [
        (None, S3ProfileName::Aws),
        (Some(S3ProfileName::Aws), S3ProfileName::Aws),
        (Some(S3ProfileName::Gcs), S3ProfileName::Gcs),
        (Some(S3ProfileName::R2), S3ProfileName::R2),
        (Some(S3ProfileName::Garage), S3ProfileName::Garage),
        (Some(S3ProfileName::RustFs), S3ProfileName::RustFs),
        (Some(S3ProfileName::SeaweedFs), S3ProfileName::SeaweedFs),
    ] {
        let profile = S3Profile::select(Some(&endpoint), name).unwrap();
        assert_eq!(profile.name, expected);
        assert_eq!(
            profile,
            *PROFILES.iter().find(|p| p.name == expected).unwrap()
        );
    }
}

#[tokio::test]
async fn explicit_gcs_name_drives_the_adapter_at_an_uncatalogued_endpoint() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = StorageConnection::from_provider(Arc::new(
        S3Storage::new(
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "auto".into(),
                endpoint: Some(server.url.parse().unwrap()),
                profile: Some(S3ProfileName::Gcs),
                prefix: "store".into(),
            },
            S3Credentials {
                access_key_id: "access".into(),
                secret_access_key: SecretText::new("secret".into()),
            },
            Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
            Arc::new(SequentialIds::new()),
        )
        .unwrap(),
    ));
    assert_eq!(storage.provider.profile.name, S3ProfileName::Gcs);
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    storage.create(&path, b"original").await.unwrap();
    assert_eq!(
        storage
            .create(&path, b"replacement")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(storage.read(&path).await.unwrap(), b"original");
}

#[test]
fn self_hosted_profiles_are_selected_only_by_name_at_their_own_endpoint() {
    for name in [
        S3ProfileName::Garage,
        S3ProfileName::RustFs,
        S3ProfileName::SeaweedFs,
    ] {
        let profile = PROFILES.iter().find(|p| p.name == name).unwrap();
        assert_eq!(profile.endpoint, None);
        assert_eq!(
            profile.certification == Certification::Uncertified,
            name == S3ProfileName::Garage
        );
        assert_eq!(
            S3Profile::select(None, Some(name)).unwrap_err().failure(),
            StorageFailure::InvalidConfiguration
        );
        assert_eq!(
            S3Profile::select(
                Some(&"https://storage.googleapis.com".parse().unwrap()),
                Some(name)
            )
            .unwrap_err()
            .failure(),
            StorageFailure::InvalidConfiguration
        );
    }
    for host in [
        "http://localhost:3900",
        "http://localhost:9000",
        "http://localhost:8333",
    ] {
        assert_eq!(
            S3Profile::select(Some(&host.parse().unwrap()), None)
                .unwrap()
                .name,
            S3ProfileName::Aws
        );
    }
}

#[tokio::test]
async fn absent_policy_reads_without_a_checksum_and_refuses_any_returned_one() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let mut profile = PROFILES[0];
    profile.checksums = Checksums::Absent;
    profile.request_checksums = false;
    let storage = profile_provider(&server.url, profile);
    let path = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
    storage.replace(&path, b"bytes").await.unwrap();
    assert_eq!(storage.status(&path).await.unwrap().unwrap().checksum, None);
    let read = storage.read_object(&path, None).await.unwrap();
    assert_eq!(read.metadata.checksum, None);
    assert_eq!(read.bytes, b"bytes");
    for headers in [
        vec![(
            "x-amz-checksum-sha256",
            "J3CJ2RwL308uaGK6fkoHYFEZQx9dE/cm3TUrBvGyBqk=",
        )],
        vec![
            ("x-amz-checksum-type", "FULL_OBJECT"),
            (
                "x-amz-checksum-sha256",
                "J3CJ2RwL308uaGK6fkoHYFEZQx9dE/cm3TUrBvGyBqk=",
            ),
        ],
    ] {
        state.lock().unwrap().response_headers = headers
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();
        assert_eq!(
            storage.status(&path).await.unwrap_err().failure(),
            StorageFailure::Protocol
        );
        assert_eq!(
            storage
                .read_object(&path, None)
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Protocol
        );
    }
}
