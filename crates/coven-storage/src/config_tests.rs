use super::*;
use crate::ProviderSignOut;

#[test]
fn sign_out_instructions_depend_only_on_the_provider() {
    assert_eq!(
        CloudProvider::S3.sign_out(),
        ProviderSignOut::ReplaceAccessKey
    );
    assert_eq!(
        CloudProvider::CloudKit.sign_out(),
        ProviderSignOut::RemoveFromAppleAccount
    );
    for provider in [
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
    ] {
        assert_eq!(
            provider.sign_out(),
            ProviderSignOut::RemoveAppAccess { provider }
        );
    }
}

#[test]
fn endpoints_allow_http_but_refuse_credentials_queries_and_fragments() {
    for (url, accepted) in [
        ("http://localhost:9000", true),
        ("https://storage.example/base", true),
        ("https://user@storage.example", false),
        ("https://user:secret@storage.example", false),
        ("https://storage.example?query", false),
        ("https://storage.example#fragment", false),
        ("file:///storage", false),
    ] {
        let config = StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "region".into(),
            prefix: "store".into(),
            endpoint: Some(url.parse().unwrap()),
            profile: None,
        };
        assert_eq!(config.validate().is_ok(), accepted, "{url}");
    }
}

#[test]
fn catalogued_hosts_require_https_and_their_own_profile_name() {
    use S3ProfileName::{Aws, Gcs, R2};
    for (endpoint, profile, accepted) in [
        (None, None, true),
        (None, Some(Aws), true),
        (None, Some(Gcs), false),
        (None, Some(R2), false),
        (Some("http://s3.us-east-1.amazonaws.com"), None, false),
        (Some("http://storage.googleapis.com"), Some(Gcs), false),
        (Some("http://account.r2.cloudflarestorage.com"), None, false),
        (Some("https://s3.us-east-1.amazonaws.com"), Some(Aws), true),
        (Some("https://s3.us-east-1.amazonaws.com"), Some(R2), false),
        (Some("https://storage.googleapis.com"), Some(Gcs), true),
        (Some("https://storage.googleapis.com"), Some(Aws), false),
        (
            Some("https://account.r2.cloudflarestorage.com"),
            Some(R2),
            true,
        ),
        (
            Some("https://account.r2.cloudflarestorage.com"),
            Some(Gcs),
            false,
        ),
        (Some("http://localhost:3900"), None, true),
        (Some("http://localhost:3900"), Some(Aws), true),
        (
            Some("https://storage-private.p.googleapis.com"),
            Some(Gcs),
            true,
        ),
        (Some("http://192.168.1.20:9000"), Some(R2), true),
    ] {
        let config = StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "region".into(),
            endpoint: endpoint.map(|url| url.parse().unwrap()),
            profile,
            prefix: "store".into(),
        };
        let result = config.validate();
        assert_eq!(result.is_ok(), accepted, "{endpoint:?} {profile:?}");
        if let Err(error) = result {
            assert_eq!(error.failure(), crate::StorageFailure::InvalidConfiguration);
        }
    }
}
