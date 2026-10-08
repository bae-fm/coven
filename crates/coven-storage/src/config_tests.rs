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
        };
        assert_eq!(config.validate().is_ok(), accepted, "{url}");
    }
}
