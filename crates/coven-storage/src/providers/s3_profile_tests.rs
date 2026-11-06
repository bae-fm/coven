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
    assert_eq!(PROFILES[0].name, S3ProfileName::Aws);
    assert_eq!(PROFILES[0].certification, Certification::Uncertified);
    assert_eq!(
        PROFILES[0].settings.copy_publication,
        S3CopyPublication::Multipart(S3CopyCondition::IfNoneMatch)
    );
}

#[tokio::test]
async fn scope_policy_controls_head_and_get_without_inferring_a_checksum() {
    for (policy, expected) in
        [(S3Checksums::ScopeStated, false), (S3Checksums::UnstatedIgnored, true)]
    {
        let state = Arc::new(Mutex::new(Remote {
            response_headers: BTreeMap::from([(
                "x-amz-checksum-sha256".into(),
                "J3CJ2RwL308uaGK6fkoHYFEZQx9dE/cm3TUrBvGyBqk=".into(),
            )]),
            ..Remote::default()
        }));
        let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
        let storage = profile_provider(&server.url, S3Settings { checksums: policy, ..PLAIN });
        let target = ObjectPath::floor(STORE_ID);
        storage.create(&target, b"bytes").await.unwrap();
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
    let keys =
        [1, 2].map(|n| ObjectPath::slot(STORE_ID, n.try_into().unwrap()).as_str().to_owned());
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
    Arc::get_mut(storage.provider_mut()).unwrap().settings.listing = S3Listing::V1;
    assert_eq!(storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap().len(), 2);
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
        S3Settings { checksums: S3Checksums::UnstatedIgnored, ..PLAIN },
    );
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"data").await.unwrap();
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
    state.lock().unwrap().response_headers.remove("x-amz-checksum-type");

    state.lock().unwrap().response_headers.insert("etag".into(), String::new());
    let immutable = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    storage.create(&immutable, b"data").await.unwrap();
    assert_eq!(storage.status(&immutable).await.unwrap_err().failure(), StorageFailure::Protocol);
}

#[test]
fn r2_profile_matches_account_hosts_without_matching_lookalikes() {
    for (endpoint, r2) in [
        ("https://account.r2.cloudflarestorage.com", true),
        ("https://ACCOUNT.R2.CLOUDFLARESTORAGE.COM/", true),
        ("https://account.eu.r2.cloudflarestorage.com", true),
        ("https://r2.cloudflarestorage.com", false),
        ("https://account.notr2.cloudflarestorage.com", false),
        ("https://account.r2.cloudflarestorage.com.attacker.test", false),
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
        Client::from_conf(storage.client.config().to_builder().endpoint_url(&server.url).build());
    let storage = StorageConnection::from_provider(Arc::new(storage));
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"bytes").await.unwrap();
    let status = storage.status(&path).await.unwrap().unwrap();
    assert_eq!(status.checksum, None);
    for (range, expected) in
        [(None, b"bytes".as_slice()), (Some(ByteRange::new(1, 4).unwrap()), b"yte".as_slice())]
    {
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
    let custom = S3Profile::Custom(S3Settings { listing: S3Listing::V1, ..PLAIN });
    assert_eq!(
        settings(Some(&"http://localhost:9000".parse().unwrap()), Some(&custom)).unwrap(),
        Some(S3Settings { listing: S3Listing::V1, ..PLAIN })
    );
    for endpoint in [
        None,
        Some("https://storage.googleapis.com"),
        Some("https://s3.us-east-1.amazonaws.com"),
        Some("https://account.r2.cloudflarestorage.com"),
    ] {
        let endpoint: Option<url::Url> = endpoint.map(|url| url.parse().unwrap());
        assert_eq!(
            settings(endpoint.as_ref(), Some(&custom)).unwrap_err().failure(),
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
    .err()
    .unwrap();
    assert_eq!(error.failure(), StorageFailure::InvalidConfiguration);
}

#[tokio::test]
async fn explicit_gcs_name_drives_the_adapter_at_an_uncatalogued_endpoint() {
    let state = Arc::new(Mutex::new(Remote { gcs: true, ..Remote::default() }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = StorageConnection::from_provider(Arc::new(
        S3Storage::new(
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "auto".into(),
                endpoint: Some(server.url.parse().unwrap()),
                profile: Some(S3Profile::Named(S3ProfileName::Gcs)),
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
        .unwrap(),
    ));
    assert_eq!(storage.provider().settings, CataloguedProfile::named(S3ProfileName::Gcs).settings);
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    storage.create(&path, b"original").await.unwrap();
    assert_eq!(
        storage.create(&path, b"replacement").await.unwrap_err().failure(),
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
            settings(Some(&"https://storage.googleapis.com".parse().unwrap()), Some(&named))
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
        S3Settings { checksums: S3Checksums::Absent, request_checksums: false, ..PLAIN },
    );
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"bytes").await.unwrap();
    assert_eq!(storage.status(&path).await.unwrap().unwrap().checksum, None);
    let read = storage.read_object(&path, None).await.unwrap();
    assert_eq!(read.metadata.checksum, None);
    assert_eq!(read.bytes, b"bytes");
    for headers in [
        vec![("x-amz-checksum-sha256", "J3CJ2RwL308uaGK6fkoHYFEZQx9dE/cm3TUrBvGyBqk=")],
        vec![
            ("x-amz-checksum-type", "FULL_OBJECT"),
            ("x-amz-checksum-sha256", "J3CJ2RwL308uaGK6fkoHYFEZQx9dE/cm3TUrBvGyBqk="),
        ],
    ] {
        state.lock().unwrap().response_headers =
            headers.into_iter().map(|(name, value)| (name.into(), value.into())).collect();
        assert_eq!(storage.status(&path).await.unwrap_err().failure(), StorageFailure::Protocol);
        assert_eq!(
            storage.read_object(&path, None).await.unwrap_err().failure(),
            StorageFailure::Protocol
        );
    }
}

/// Each profile's entry has the behaviours and the certification its row of
/// the spec's table of certified profiles states (Appendix E.4).
#[test]
fn each_profile_entry_has_the_behaviours_its_table_row_states() {
    use S3ProfileName::*;
    let multipart = |condition| S3CopyPublication::Multipart(condition);
    // name, signing, single PUT, copy publication, multipart time, checksums,
    // revision, requests checksums, response headers, certified, versioned too
    let rows = [
        (
            Aws,
            S3Signing::AwsSigV4,
            S3SinglePut::IfNoneMatch,
            multipart(S3CopyCondition::IfNoneMatch),
            S3MultipartTimestamp::Start,
            S3Checksums::ScopeStated,
            S3Revision::ETag,
            true,
            S3ResponseHeaders::S3,
            false,
            false,
        ),
        (
            Gcs,
            S3Signing::GoogleHmac,
            S3SinglePut::GenerationMatch,
            multipart(S3CopyCondition::GenerationMatch),
            S3MultipartTimestamp::Completion,
            S3Checksums::ScopeStated,
            S3Revision::Generation,
            false,
            S3ResponseHeaders::Google,
            true,
            false,
        ),
        (
            R2,
            S3Signing::AwsSigV4,
            S3SinglePut::IfNoneMatch,
            S3CopyPublication::Never,
            S3MultipartTimestamp::Completion,
            S3Checksums::UnstatedIgnored,
            S3Revision::ETag,
            true,
            S3ResponseHeaders::S3,
            true,
            false,
        ),
        (
            RustFs,
            S3Signing::AwsSigV4,
            S3SinglePut::IfNoneMatch,
            S3CopyPublication::Never,
            S3MultipartTimestamp::Completion,
            S3Checksums::Absent,
            S3Revision::ETag,
            false,
            S3ResponseHeaders::S3,
            true,
            true,
        ),
        (
            SeaweedFs,
            S3Signing::AwsSigV4,
            S3SinglePut::IfNoneMatch,
            S3CopyPublication::Every(S3CopyCondition::IfNoneMatch),
            S3MultipartTimestamp::Completion,
            S3Checksums::Absent,
            S3Revision::ETag,
            false,
            S3ResponseHeaders::S3,
            true,
            true,
        ),
        (
            CephRgw,
            S3Signing::AwsSigV4,
            S3SinglePut::IfNoneMatch,
            S3CopyPublication::Never,
            S3MultipartTimestamp::Completion,
            S3Checksums::Absent,
            S3Revision::ETag,
            false,
            S3ResponseHeaders::S3,
            true,
            false,
        ),
        (
            VersityGw,
            S3Signing::AwsSigV4,
            S3SinglePut::IfNoneMatch,
            S3CopyPublication::Never,
            S3MultipartTimestamp::Completion,
            S3Checksums::ScopeStated,
            S3Revision::ETag,
            true,
            S3ResponseHeaders::S3,
            true,
            true,
        ),
    ];
    assert_eq!(rows.len(), PROFILES.len());
    for (
        name,
        signing,
        single_put,
        copy,
        time,
        checksums,
        revision,
        asks,
        headers,
        certified,
        versioned,
    ) in rows
    {
        let entry =
            PROFILES.iter().find(|profile| profile.name == name).expect("a catalogued name");
        let settings = entry.settings;
        assert_eq!(settings.signing, signing, "{name:?}");
        assert_eq!(settings.single_put, single_put, "{name:?}");
        assert_eq!(settings.copy_publication, copy, "{name:?}");
        assert_eq!(settings.multipart_timestamp, time, "{name:?}");
        assert_eq!(settings.checksums, checksums, "{name:?}");
        assert_eq!(settings.listing, S3Listing::V2, "{name:?}");
        assert_eq!(settings.revision, revision, "{name:?}");
        assert_eq!(settings.request_checksums, asks, "{name:?}");
        assert_eq!(settings.response_headers, headers, "{name:?}");
        assert_eq!(
            matches!(entry.certification, Certification::Passed { .. }),
            certified,
            "{name:?}"
        );
        assert_eq!(
            matches!(entry.versioned_buckets, Certification::Passed { .. }),
            versioned,
            "{name:?}"
        );
    }
}
