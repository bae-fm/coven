use super::*;

#[test]
fn the_member_policy_reaches_the_whole_bucket_and_denies_nothing() {
    let location = StorageConfig::S3 {
        bucket: "bucket".into(),
        region: "us-east-1".into(),
        endpoint: None,
        profile: None,
    };
    let policy: serde_json::Value =
        serde_json::from_str(&member_policy(&location).unwrap()).unwrap();
    let statements = policy["Statement"].as_array().unwrap();
    assert_eq!(statements.len(), 2);
    assert!(statements.iter().all(|s| s["Effect"] == "Allow" && s.get("Condition").is_none()));
    assert_eq!(statements[0]["Resource"], "arn:aws:s3:::bucket/*");
    assert!(statements[0]["Action"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("s3:ListMultipartUploadParts")));
    assert_eq!(statements[1]["Resource"], "arn:aws:s3:::bucket");
    assert_eq!(
        statements[1]["Action"],
        serde_json::json!([
            "s3:ListBucket",
            "s3:GetBucketVersioning",
            "s3:ListBucketMultipartUploads"
        ])
    );
}
