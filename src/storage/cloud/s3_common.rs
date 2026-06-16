//! Pure helpers shared by both S3 backends.
//!
//! The native backend (`super::s3`, driving `aws-sdk-s3`) and the browser
//! backend (`super::s3_wasm`, signing and sending over `fetch`) must compute the
//! same object keys and produce the same join info, so a library created on one
//! platform opens unchanged on the other. These free functions are that shared
//! definition; both backends call them rather than each holding its own copy.

use super::CloudHomeJoinInfo;

/// Prepend an optional prefix to a key, normalizing trailing slashes on the
/// prefix. With no prefix the key is returned unchanged. Both backends route
/// every operation's key through this so they address identical objects.
pub fn apply_prefix(prefix: Option<&str>, key: &str) -> String {
    match prefix {
        Some(p) => format!("{}/{}", p.trim_end_matches('/'), key),
        None => key.to_string(),
    }
}

/// Build the join info both backends hand back from `grant_access`. S3 access is
/// managed externally (IAM / pre-shared credentials), so this carries the
/// owner's bucket coordinates and credentials to embed in the invite code; there
/// is no per-member grant to make.
pub fn s3_join_info(
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    key_prefix: Option<String>,
) -> CloudHomeJoinInfo {
    CloudHomeJoinInfo::S3 {
        bucket,
        region,
        endpoint,
        access_key,
        secret_key,
        key_prefix,
    }
}
