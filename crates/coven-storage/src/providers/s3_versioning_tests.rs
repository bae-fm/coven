use super::versioning::*;
use super::*;
use crate::providers::tests::{query, response, TestServer};
use axum::{
    http::{HeaderMap, Method, Uri},
    Router,
};

#[tokio::test]
async fn xml_versioning_states_are_read_with_each_signer() {
    for signing in [S3Signing::AwsSigV4, S3Signing::GoogleHmac] {
        for (status, expected) in [
            ("", S3BucketVersioning::Disabled),
            ("<Status>Enabled</Status>", S3BucketVersioning::Enabled),
            ("<Status>Suspended</Status>", S3BucketVersioning::Suspended),
        ] {
            let server = TestServer::new(Router::new().fallback(
                move |method: Method, uri: Uri, headers: HeaderMap| async move {
                    assert_eq!(method, Method::GET);
                    assert_eq!(uri.path(), "/bucket/");
                    assert!(query(&uri).contains_key("versioning"));
                    assert_eq!(
                        headers["authorization"]
                            .to_str()
                            .unwrap()
                            .starts_with("GOOG4-HMAC-SHA256"),
                        signing == S3Signing::GoogleHmac
                    );
                    response(
                        200,
                        format!("<VersioningConfiguration>{status}</VersioningConfiguration>"),
                    )
                },
            ))
            .await;
            let s = profile_provider(
                &server.url,
                S3Settings {
                    signing,
                    ..profile::PLAIN
                },
            );
            assert_eq!(s.provider.bucket_versioning().await.unwrap(), expected);
        }
    }
}

#[tokio::test]
async fn refused_unknown_and_malformed_reads_never_mean_disabled() {
    for (status, body, failure) in [
        (
            403,
            "<Error><Code>AccessDenied</Code></Error>",
            StorageFailure::PermissionDenied,
        ),
        (
            503,
            "<Error><Code>ServiceUnavailable</Code></Error>",
            StorageFailure::Network,
        ),
        (
            200,
            "<VersioningConfiguration><Status>FutureState</Status></VersioningConfiguration>",
            StorageFailure::Protocol,
        ),
        (200, "malformed", StorageFailure::Protocol),
        (501, "malformed", StorageFailure::Protocol),
    ] {
        let server =
            TestServer::new(Router::new().fallback(move || async move { response(status, body) }))
                .await;
        let s = profile_provider(&server.url, profile::PLAIN);
        let error = s.provider.bucket_versioning().await.unwrap_err();
        assert_eq!(error.failure(), failure);
        if failure == StorageFailure::Protocol {
            assert!(!unknown(&error), "malformed replies fail detection");
        }
    }
}

#[tokio::test]
async fn only_a_hosted_r2_refusal_establishes_that_versioning_is_unavailable() {
    for (status, body, failure) in [
        (
            403,
            "<Error><Code>AccessDenied</Code></Error>",
            StorageFailure::PermissionDenied,
        ),
        (
            501,
            "<Error><Code>NotImplemented</Code></Error>",
            StorageFailure::Network,
        ),
        (501, "malformed", StorageFailure::Protocol),
    ] {
        let server =
            TestServer::new(Router::new().fallback(move || async move { response(status, body) }))
                .await;
        for endpoint in [
            server.url.as_str(),
            "https://account.r2.cloudflarestorage.com",
        ] {
            let mut connection = profile_provider(
                &server.url,
                profile::CataloguedProfile::named(S3ProfileName::R2).settings,
            );
            let storage = Arc::get_mut(&mut connection.provider).unwrap();
            if let StorageConfig::S3 {
                endpoint: recorded,
                profile,
                ..
            } = &mut storage.config
            {
                *recorded = Some(endpoint.parse().unwrap());
                *profile = Some(S3Profile::Named(S3ProfileName::R2));
            }
            let result = storage.bucket_versioning().await;
            if endpoint == server.url || failure == StorageFailure::Protocol {
                assert_eq!(result.unwrap_err().failure(), failure);
            } else {
                assert_eq!(result.unwrap(), S3BucketVersioning::Unavailable);
            }
        }
    }
}

#[test]
fn uncertified_profiles_refuse_enabled_suspended_and_unknown_buckets() {
    let endpoint = "http://localhost:9000".parse().unwrap();
    let profiles = profile::PROFILES
        .iter()
        .map(|entry| Some(S3Profile::Named(entry.name)))
        .chain([None, Some(S3Profile::Custom(profile::PLAIN))]);
    for selected in profiles {
        for state in [S3BucketVersioning::Enabled, S3BucketVersioning::Suspended] {
            assert!(
                matches!(check(Some(&endpoint), selected.as_ref(), &Ok(state)), Err(StorageSetupError::S3VersioningUnsupported { state: observed }) if observed == state)
            );
        }
        let cause = Arc::new(
            StorageFailure::PermissionDenied.with_source("member lacks bucket metadata permission"),
        );
        match check(Some(&endpoint), selected.as_ref(), &Err(cause.clone())).unwrap_err() {
            StorageSetupError::S3VersioningUnknown { source } => {
                assert!(Arc::ptr_eq(&source, &cause))
            }
            other => panic!("{other}"),
        }
        check(
            Some(&endpoint),
            selected.as_ref(),
            &Ok(S3BucketVersioning::Disabled),
        )
        .unwrap();
    }
}
