use super::*;
use crate::{S3Profile, S3ProfileName};
use coven_foundation::id_source::StoreId;
use coven_foundation::{clock::SystemClock, id_source::UuidIds};

fn location(profile: Option<S3Profile>, endpoint: Option<&str>, prefix: &str) -> StorageConfig {
    StorageConfig::S3 {
        bucket: "bucket".into(),
        region: "us-east-1".into(),
        endpoint: endpoint.map(|url| Url::parse(url).unwrap()),
        profile,
        prefix: prefix.into(),
    }
}

fn admin(config: StorageConfig) -> Result<std::sync::Arc<dyn AccessKeyAdmin>, StorageError> {
    key_admin(
        config,
        AdminCredential::BearerToken(SecretText::new("token".into())),
        AdminEndpoints::production(),
        std::sync::Arc::new(SystemClock),
        std::sync::Arc::new(UuidIds),
        crate::providers::tests::http_client(),
    )
}

#[test]
fn the_member_policy_reaches_only_the_store_prefix_and_denies_nothing() {
    let store = StoreId(uuid::Uuid::from_u128(5));
    for (prefix, scope) in [
        ("base", format!("base/{store}/")),
        ("", format!("{store}/")),
    ] {
        let policy: serde_json::Value =
            serde_json::from_str(&member_policy(&location(None, None, prefix), store).unwrap())
                .unwrap();
        let statements = policy["Statement"].as_array().unwrap();
        assert!(statements.iter().all(|s| s["Effect"] == "Allow"));
        assert_eq!(
            statements[0]["Resource"],
            format!("arn:aws:s3:::bucket/{scope}*")
        );
        assert_eq!(statements[1]["Resource"], "arn:aws:s3:::bucket");
        assert_eq!(
            statements[1]["Condition"]["StringLike"]["s3:prefix"],
            format!("{scope}*")
        );
        assert!(statements[0]["Action"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("s3:ListMultipartUploadParts")));
        // Bucket-level reads AWS cannot limit to a prefix, unconditioned.
        assert_eq!(statements[2]["Resource"], "arn:aws:s3:::bucket");
        assert!(statements[2].get("Condition").is_none());
        assert_eq!(
            statements[2]["Action"],
            serde_json::json!(["s3:GetBucketVersioning", "s3:ListBucketMultipartUploads"])
        );
    }
}

#[test]
fn custom_profiles_take_console_steps() {
    let custom = location(
        Some(S3Profile::Custom(profile::PLAIN)),
        Some("http://127.0.0.1:9000"),
        "",
    );
    assert_eq!(
        admin(custom).err().unwrap().failure(),
        StorageFailure::InvalidConfiguration
    );
    assert!(admin(location(None, None, "base")).is_ok());
    assert!(admin(location(
        Some(S3Profile::Named(S3ProfileName::SeaweedFs)),
        Some("http://127.0.0.1:8333"),
        "base"
    ))
    .is_ok());
}

#[test]
fn only_versity_admin_access_names_a_separate_iam_endpoint() {
    let access = |iam_endpoint: Option<&str>| AdminCredential::AccessKey {
        access_key_id: "admin".into(),
        secret_access_key: SecretText::new("admin-secret".into()),
        session_token: None,
        iam_endpoint: iam_endpoint.map(|url| Url::parse(url).unwrap()),
    };
    let admin = |name: Option<S3ProfileName>, credential: AdminCredential| {
        key_admin(
            location(
                name.map(S3Profile::Named),
                name.map(|_| "http://127.0.0.1:10000"),
                "base",
            ),
            credential,
            AdminEndpoints::production(),
            std::sync::Arc::new(SystemClock),
            std::sync::Arc::new(UuidIds),
            crate::providers::tests::http_client(),
        )
    };
    let versity = Some(S3ProfileName::VersityGw);
    assert!(admin(versity, access(Some("http://127.0.0.1:7076"))).is_ok());
    for credential in [
        access(None),
        AdminCredential::BearerToken(SecretText::new("token".into())),
        access(Some("ftp://127.0.0.1:7076")),
        access(Some("http://127.0.0.1:7076/?Action=ListUsers")),
    ] {
        assert_eq!(
            admin(versity, credential).err().unwrap().failure(),
            StorageFailure::InvalidConfiguration
        );
    }
    // Elsewhere IAM is global or at the location's endpoint.
    for name in [
        None,
        Some(S3ProfileName::SeaweedFs),
        Some(S3ProfileName::CephRgw),
        Some(S3ProfileName::RustFs),
    ] {
        assert!(admin(name, access(None)).is_ok());
        assert_eq!(
            admin(name, access(Some("http://127.0.0.1:7076")))
                .err()
                .unwrap()
                .failure(),
            StorageFailure::InvalidConfiguration
        );
    }
}
