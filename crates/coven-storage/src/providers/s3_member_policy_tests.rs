use super::*;

fn location(prefix: &str) -> StorageConfig {
    StorageConfig::S3 {
        bucket: "bucket".into(),
        region: "us-east-1".into(),
        endpoint: None,
        profile: None,
        prefix: crate::S3Prefix::parse(prefix).unwrap(),
    }
}

#[test]
fn the_member_policy_reaches_only_the_store_prefix_and_denies_nothing() {
    let store = StoreId(uuid::Uuid::from_u128(5));
    for (prefix, scope) in [("base", format!("base/{store}/")), ("", format!("{store}/"))] {
        let policy: serde_json::Value =
            serde_json::from_str(&member_policy(&location(prefix), store).unwrap()).unwrap();
        let statements = policy["Statement"].as_array().unwrap();
        assert!(statements.iter().all(|s| s["Effect"] == "Allow"));
        assert_eq!(statements[0]["Resource"], format!("arn:aws:s3:::bucket/{scope}*"));
        assert_eq!(statements[1]["Resource"], "arn:aws:s3:::bucket");
        assert_eq!(statements[1]["Condition"]["StringLike"]["s3:prefix"], format!("{scope}*"));
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
