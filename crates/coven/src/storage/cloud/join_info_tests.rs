use super::*;

/// The wire shape is the compact `{"t": "<short-tag>", ...}` form, not the
/// derive default's `{"VariantName": {...}}` — invite and restore codes
/// both wrap this type and rely on it staying compact.
#[test]
fn wire_shape_uses_short_t_tags() {
    let cases = [
        (
            CloudHomeJoinInfo::S3 {
                bucket: "b".to_string(),
                region: "r".to_string(),
                endpoint: None,
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
                key_prefix: None,
            },
            "s3",
        ),
        (
            CloudHomeJoinInfo::GoogleDrive {
                folder_id: "f".to_string(),
            },
            "gd",
        ),
        (
            CloudHomeJoinInfo::Dropbox {
                folder_path: "/p".to_string(),
            },
            "db",
        ),
        (
            CloudHomeJoinInfo::OneDrive {
                drive_id: "d".to_string(),
                folder_id: "f".to_string(),
            },
            "od",
        ),
        (CloudHomeJoinInfo::CloudKit, "ck"),
        (
            CloudHomeJoinInfo::CloudKitShare {
                share_url: "https://share.example".to_string(),
                owner_name: "owner".to_string(),
                zone_name: "zone".to_string(),
            },
            "cks",
        ),
    ];
    for (info, tag) in cases {
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["t"], tag, "{info:?} must tag as {tag:?}: {json}");
    }
}

#[test]
fn debug_redacts_s3_secret_key() {
    let info = CloudHomeJoinInfo::S3 {
        bucket: "my-bucket".to_string(),
        region: "us-east-1".to_string(),
        endpoint: None,
        access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
        secret_key: "s3-secret-value-do-not-print".to_string(),
        key_prefix: None,
    };
    let debug = format!("{info:?}");

    assert!(debug.contains("<redacted>"), "{debug}");
    assert!(debug.contains("my-bucket"), "{debug}");
    assert!(debug.contains("AKIAIOSFODNN7EXAMPLE"), "{debug}");
    assert!(
        !debug.contains("s3-secret-value-do-not-print"),
        "S3 secret key leaked: {debug}"
    );
}

/// The join info is what a joining device receives. `s3_exact_slots` and
/// `strong_reads` describe how THIS device talks to the bucket, so neither
/// belongs on the shared wire — a joining device decides them for itself.
#[test]
fn the_shared_wire_carries_no_local_only_s3_settings() {
    let join_info = CloudHomeJoinInfo::S3 {
        bucket: "bucket".to_string(),
        region: "us-east-1".to_string(),
        endpoint: Some("https://objects.example".to_string()),
        access_key: "access".to_string(),
        secret_key: "secret".to_string(),
        key_prefix: None,
    };

    let shared_wire = serde_json::to_string(&join_info).expect("serialize shared join info");

    assert!(!shared_wire.contains("s3_exact_slots"));
    assert!(!shared_wire.contains("strong_reads"));
}
