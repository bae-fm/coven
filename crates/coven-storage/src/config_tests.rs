use super::*;
use crate::*;

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
    let custom = Some(S3Profile::Custom(S3Settings {
        signing: S3Signing::AwsSigV4,
        single_put: S3SinglePut::IfNoneMatch,
        multipart: S3Multipart::ConditionalCompletion,
        multipart_timestamp: S3MultipartTimestamp::Completion,
        checksums: S3Checksums::Absent,
        listing: S3Listing::V1,
        revision: S3Revision::ETag,
        bucket_check: S3BucketCheck::Head,
        response_headers: S3ResponseHeaders::S3,
        request_checksums: false,
    }));
    let named = |name| Some(S3Profile::Named(name));
    for (endpoint, profile, accepted) in [
        (None, None, true),
        (None, named(Aws), true),
        (None, named(Gcs), false),
        (None, named(R2), false),
        (Some("http://s3.us-east-1.amazonaws.com"), None, false),
        (Some("http://storage.googleapis.com"), named(Gcs), false),
        (Some("http://account.r2.cloudflarestorage.com"), None, false),
        (Some("https://s3.us-east-1.amazonaws.com"), named(Aws), true),
        (Some("https://s3.us-east-1.amazonaws.com"), named(R2), false),
        (Some("https://storage.googleapis.com"), named(Gcs), true),
        (Some("https://storage.googleapis.com"), named(Aws), false),
        (
            Some("https://account.r2.cloudflarestorage.com"),
            named(R2),
            true,
        ),
        (
            Some("https://account.r2.cloudflarestorage.com"),
            named(Gcs),
            false,
        ),
        (Some("http://localhost:3900"), None, true),
        (Some("http://localhost:3900"), named(Aws), true),
        (
            Some("https://storage-private.p.googleapis.com"),
            named(Gcs),
            true,
        ),
        (Some("http://192.168.1.20:9000"), named(R2), true),
        (Some("http://192.168.1.20:9000"), custom, true),
        (Some("https://storage.googleapis.com"), custom, false),
        (None, custom, false),
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

#[test]
fn locations_select_the_required_initial_access() {
    use crate::{DropboxAccess, DropboxLocation, GoogleDriveAccess, OAuthAccess};
    for (config, expected) in [
        (
            StorageConfig::GoogleDrive {
                folder_id: "folder".into(),
            },
            Some(OAuthAccess::GoogleDrive(GoogleDriveAccess::File)),
        ),
        (
            StorageConfig::Dropbox {
                location: DropboxLocation::AppFolder,
            },
            Some(OAuthAccess::Dropbox(DropboxAccess::AppFolder)),
        ),
        (
            StorageConfig::Dropbox {
                location: DropboxLocation::SharedFolder {
                    namespace_id: "namespace".into(),
                },
            },
            Some(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)),
        ),
        (
            StorageConfig::OneDrive {
                drive_id: "drive".into(),
                folder_id: "folder".into(),
            },
            Some(OAuthAccess::OneDrive),
        ),
        (
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "region".into(),
                endpoint: None,
                profile: None,
                prefix: String::new(),
            },
            None,
        ),
        (
            StorageConfig::CloudKit {
                container: "container".into(),
                owner: "owner".into(),
                zone: "zone".into(),
            },
            None,
        ),
    ] {
        config.validate().unwrap();
        assert_eq!(config.oauth_access(), expected);
        if let Some(access) = expected {
            assert_eq!(config.provider(), access.provider());
        }
    }
}

#[test]
fn every_native_location_identifier_is_required() {
    use serde_json::json;
    for (location, field) in [
        (
            json!({"S3":{"bucket":"bucket","region":"region","endpoint":null,"prefix":""}}),
            "bucket",
        ),
        (
            json!({"S3":{"bucket":"bucket","region":"region","endpoint":null,"prefix":""}}),
            "region",
        ),
        (json!({"GoogleDrive":{"folder_id":"folder"}}), "folder_id"),
        (
            json!({"OneDrive":{"drive_id":"drive","folder_id":"folder"}}),
            "drive_id",
        ),
        (
            json!({"OneDrive":{"drive_id":"drive","folder_id":"folder"}}),
            "folder_id",
        ),
        (
            json!({"CloudKit":{"container":"container","owner":"owner","zone":"zone"}}),
            "container",
        ),
        (
            json!({"CloudKit":{"container":"container","owner":"owner","zone":"zone"}}),
            "owner",
        ),
        (
            json!({"CloudKit":{"container":"container","owner":"owner","zone":"zone"}}),
            "zone",
        ),
    ] {
        let config: StorageConfig = serde_json::from_value(location.clone()).unwrap();
        config.validate().unwrap();
        for invalid in ["", "  ", "identifier\n"] {
            let mut location = location.clone();
            let fields = location
                .as_object_mut()
                .unwrap()
                .values_mut()
                .next()
                .unwrap();
            fields[field] = json!(invalid);
            let config: StorageConfig = serde_json::from_value(location).unwrap();
            assert_eq!(
                config.validate().unwrap_err().failure(),
                crate::StorageFailure::InvalidConfiguration
            );
        }
    }
    for invalid in ["", "  ", "namespace\n"] {
        let config = StorageConfig::Dropbox {
            location: crate::DropboxLocation::SharedFolder {
                namespace_id: invalid.into(),
            },
        };
        assert_eq!(
            config.validate().unwrap_err().failure(),
            crate::StorageFailure::InvalidConfiguration
        );
    }
}

#[test]
fn s3_base_prefixes_have_no_leading_or_trailing_slash() {
    for (prefix, valid) in [
        ("", true),
        ("base", true),
        ("base/nested", true),
        ("/", false),
        ("/base", false),
        ("base/", false),
    ] {
        let config = StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "region".into(),
            endpoint: None,
            profile: None,
            prefix: prefix.into(),
        };
        assert_eq!(config.validate().is_ok(), valid, "{prefix}");
    }
}
