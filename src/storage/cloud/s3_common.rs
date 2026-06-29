//! Pure helpers shared by both S3 backends.
//!
//! The native backend (`super::s3`, driving `aws-sdk-s3`) and the browser
//! backend (`super::s3_wasm`, signing and sending over `fetch`) must compute the
//! same object keys and produce the same join info, so a library created on one
//! platform opens unchanged on the other. These free functions are that shared
//! definition; both backends call them rather than each holding its own copy.

use super::{CloudHomeError, CloudHomeJoinInfo};

/// Normalize a configured key prefix ONCE, at construction: trim any trailing
/// slash and drop an empty prefix. Both S3 backends store the normalized form, so
/// neither re-trims in `full_key` or `list` — the divergence where the native
/// backend left the prefix un-normalized and re-trimmed everywhere is fixed here.
pub fn normalize_prefix(prefix: Option<String>) -> Option<String> {
    prefix
        .map(|p| p.trim_end_matches('/').to_string())
        .filter(|p| !p.is_empty())
}

/// Prepend an already-normalized prefix to a key. With no prefix the key is
/// returned unchanged. Both backends route every operation's key through this so
/// they address identical objects.
pub fn apply_prefix(prefix: Option<&str>, key: &str) -> String {
    match prefix {
        Some(p) => format!("{p}/{key}"),
        None => key.to_string(),
    }
}

/// The string a `list` strips from each returned key to undo `apply_prefix`:
/// `"{prefix}/"`, or `None` when there is no prefix. The normalized prefix has no
/// trailing slash, so this appends exactly one.
pub fn list_strip_prefix(prefix: Option<&str>) -> Option<String> {
    prefix.map(|p| format!("{p}/"))
}

/// Whether an S3 error code/marker means "absent key" — `NoSuchKey` (GCS's S3 XML
/// API) or `NotFound` (AWS HeadObject). The shared not-found rule both backends'
/// `delete` (404 is success) and `exists` (404 is false) apply, instead of each
/// matching on a Display string.
pub fn is_not_found_code(code: Option<&str>) -> bool {
    matches!(code, Some("NoSuchKey") | Some("NotFound"))
}

/// Map a failed `probe` (HeadBucket) to a `CloudHomeError` from its HTTP status
/// and optional S3 error code: 404 → the bucket doesn't exist; 403 or an
/// auth-signalling code → credentials rejected; anything else → a generic probe
/// failure. Shared so the native (typed SDK error) and wasm (raw status) backends
/// classify a probe failure identically.
pub fn probe_error(status: u16, code: Option<&str>, bucket: &str) -> CloudHomeError {
    if status == 404 || code == Some("NoSuchBucket") {
        return CloudHomeError::Storage(format!("bucket {bucket:?} does not exist"));
    }
    let is_auth = status == 403
        || matches!(
            code,
            Some("SignatureDoesNotMatch") | Some("InvalidAccessKeyId")
        );
    if is_auth {
        CloudHomeError::Storage(format!(
            "S3 credentials rejected (status {status}, code {code:?})"
        ))
    } else {
        CloudHomeError::Storage(format!("S3 probe failed (status {status}, code {code:?})"))
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
