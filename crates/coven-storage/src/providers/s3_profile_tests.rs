use super::*;
use profile::*;

#[test]
fn profile_table_records_certification_and_publication_requirements() {
    for profile in PROFILES {
        if let Certification::Passed { date, suite } = profile.certification {
            assert_eq!(date.len(), 10);
            assert!(suite > 0);
        }
        let settings = profile.settings;
        if settings.checksums == S3Checksums::Absent {
            assert!(
                !settings.request_checksums,
                "a service returning no checksum is not asked for one"
            );
        }
        if settings.multipart_timestamp == S3MultipartTimestamp::Start {
            assert!(
                settings.copy_publication.multipart().is_some(),
                "a catalogued start-time service needs copy publication"
            );
        }
    }
    // Only SeaweedFS failed a certification race; setup refuses only it.
    for profile in PROFILES {
        let refused = admit_setup(
            Some(&"http://localhost:8333".parse().unwrap()),
            Some(&S3Profile::Named(profile.name)),
        );
        if profile.name == S3ProfileName::SeaweedFs {
            assert_eq!(profile.setup, Setup::Refused);
            assert_eq!(profile.certification, Certification::Uncertified);
            assert!(matches!(
                refused,
                Err(crate::StorageSetupError::S3ProfileRefused { profile }) if profile == S3ProfileName::SeaweedFs
            ));
        } else {
            assert_eq!(profile.setup, Setup::Admitted);
            assert!(profile.endpoint.is_some() || refused.is_ok());
        }
    }
    assert_eq!(PROFILES[0].name, S3ProfileName::Aws);
    assert_eq!(PROFILES[0].certification, Certification::Uncertified);
    assert_eq!(
        PROFILES[0].settings.copy_publication,
        S3CopyPublication::Multipart(S3CopyCondition::IfNoneMatch)
    );
}

#[tokio::test]
async fn scope_policy_controls_head_and_get_without_inferring_a_checksum() {
    for (policy, expected) in [
        (S3Checksums::ScopeStated, false),
        (S3Checksums::UnstatedIgnored, true),
    ] {
        let state = Arc::new(Mutex::new(Remote {
            response_headers: BTreeMap::from([(
                "x-amz-checksum-sha256".into(),
                "J3CJ2RwL308uaGK6fkoHYFEZQx9dE/cm3TUrBvGyBqk=".into(),
            )]),
            ..Remote::default()
        }));
        let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
        let storage = profile_provider(
            &server.url,
            S3Settings {
                checksums: policy,
                ..PLAIN
            },
        );
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
        .under("")
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
    Arc::get_mut(&mut storage.provider)
        .unwrap()
        .settings
        .listing = S3Listing::V1;
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
    let storage = profile_provider(
        &server.url,
        S3Settings {
            checksums: S3Checksums::UnstatedIgnored,
            ..PLAIN
        },
    );
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

#[test]
fn r2_profile_matches_account_hosts_without_matching_lookalikes() {
    for (endpoint, r2) in [
        ("https://account.r2.cloudflarestorage.com", true),
        ("https://ACCOUNT.R2.CLOUDFLARESTORAGE.COM/", true),
        ("https://account.eu.r2.cloudflarestorage.com", true),
        ("https://r2.cloudflarestorage.com", false),
        ("https://account.notr2.cloudflarestorage.com", false),
        (
            "https://account.r2.cloudflarestorage.com.attacker.test",
            false,
        ),
        ("https://other.test/account.r2.cloudflarestorage.com", false),
    ] {
        let endpoint: url::Url = endpoint.parse().unwrap();
        assert_eq!(
            CataloguedProfile::hosting(&endpoint).map(|profile| profile.name),
            r2.then_some(S3ProfileName::R2)
        );
        // A lookalike is uncatalogued: setup detects it, with no AWS fallback.
        assert_eq!(
            settings(Some(&endpoint), None).unwrap(),
            r2.then_some(CataloguedProfile::named(S3ProfileName::R2).settings)
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
        std::sync::Arc::new(crate::NoCooldown),
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
    let endpoint: url::Url = "http://localhost:9000".parse().unwrap();
    assert_eq!(settings(Some(&endpoint), None).unwrap(), None);
    for profile in PROFILES {
        assert_eq!(
            settings(Some(&endpoint), Some(&S3Profile::Named(profile.name))).unwrap(),
            Some(profile.settings)
        );
    }
}

#[test]
fn custom_settings_need_an_uncatalogued_endpoint() {
    let custom = S3Profile::Custom(S3Settings {
        listing: S3Listing::V1,
        ..PLAIN
    });
    assert_eq!(
        settings(
            Some(&"http://localhost:9000".parse().unwrap()),
            Some(&custom)
        )
        .unwrap(),
        Some(S3Settings {
            listing: S3Listing::V1,
            ..PLAIN
        })
    );
    for endpoint in [
        None,
        Some("https://storage.googleapis.com"),
        Some("https://s3.us-east-1.amazonaws.com"),
        Some("https://account.r2.cloudflarestorage.com"),
    ] {
        let endpoint: Option<url::Url> = endpoint.map(|url| url.parse().unwrap());
        assert_eq!(
            settings(endpoint.as_ref(), Some(&custom))
                .unwrap_err()
                .failure(),
            StorageFailure::InvalidConfiguration
        );
    }
}

#[test]
fn an_uncatalogued_endpoint_without_settings_connects_only_after_detection() {
    let error = S3Storage::new(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://localhost:9000".parse().unwrap()),
            profile: None,
            prefix: "store".into(),
        },
        S3Credentials {
            access_key_id: "access".into(),
            secret_access_key: SecretText::new("secret".into()),
        },
        Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
        Arc::new(SequentialIds::new()),
        std::sync::Arc::new(crate::NoCooldown),
    )
    .err()
    .unwrap();
    assert_eq!(error.failure(), StorageFailure::InvalidConfiguration);
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
                profile: Some(S3Profile::Named(S3ProfileName::Gcs)),
                prefix: "store".into(),
            },
            S3Credentials {
                access_key_id: "access".into(),
                secret_access_key: SecretText::new("secret".into()),
            },
            Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
            Arc::new(SequentialIds::new()),
            std::sync::Arc::new(crate::NoCooldown),
        )
        .unwrap(),
    ));
    assert_eq!(
        storage.provider.settings,
        CataloguedProfile::named(S3ProfileName::Gcs).settings
    );
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
        S3ProfileName::RustFs,
        S3ProfileName::SeaweedFs,
        S3ProfileName::CephRgw,
        S3ProfileName::VersityGw,
    ] {
        let profile = CataloguedProfile::named(name);
        assert_eq!(profile.endpoint, None);
        let named = S3Profile::Named(name);
        assert_eq!(
            settings(None, Some(&named)).unwrap_err().failure(),
            StorageFailure::InvalidConfiguration
        );
        assert_eq!(
            settings(
                Some(&"https://storage.googleapis.com".parse().unwrap()),
                Some(&named)
            )
            .unwrap_err()
            .failure(),
            StorageFailure::InvalidConfiguration
        );
    }
    for host in [
        "http://localhost:9000",
        "http://localhost:8333",
        "http://localhost:7480",
        "http://localhost:10000",
    ] {
        assert_eq!(settings(Some(&host.parse().unwrap()), None).unwrap(), None);
    }
}

#[tokio::test]
async fn absent_policy_reads_without_a_checksum_and_refuses_any_returned_one() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = profile_provider(
        &server.url,
        S3Settings {
            checksums: S3Checksums::Absent,
            request_checksums: false,
            ..PLAIN
        },
    );
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

#[test]
fn only_r2_and_uncatalogued_settings_scope_member_keys_to_a_whole_bucket() {
    for profile in PROFILES {
        let expected = if profile.name == S3ProfileName::R2 {
            S3KeyScope::Bucket
        } else {
            S3KeyScope::Prefix
        };
        assert_eq!(profile.key_scope, expected, "{:?}", profile.name);
    }
    let uncatalogued: url::Url = "http://localhost:9000".parse().unwrap();
    for (profile, expected) in [
        (None, S3KeyScope::Bucket),
        (Some(S3Profile::Custom(PLAIN)), S3KeyScope::Bucket),
        (
            Some(S3Profile::Named(S3ProfileName::R2)),
            S3KeyScope::Bucket,
        ),
        (
            Some(S3Profile::Named(S3ProfileName::RustFs)),
            S3KeyScope::Prefix,
        ),
    ] {
        assert_eq!(
            select(Some(&uncatalogued), profile.as_ref())
                .unwrap()
                .key_scope(),
            expected
        );
    }
    assert_eq!(select(None, None).unwrap().key_scope(), S3KeyScope::Prefix);
}
