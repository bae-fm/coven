//! Pure helpers for S3-compatible storage and related HTTP transports.
//!
//! These free functions keep key normalization, error classification, and ranged
//! response validation in one definition.

use super::CloudHomeError;

/// Normalize a configured key prefix ONCE, at construction: trim any trailing
/// slash and drop an empty prefix. The S3 backend stores the normalized form, so
/// neither `full_key` nor `list` re-trims it.
pub(crate) fn normalize_prefix(prefix: Option<String>) -> Option<String> {
    prefix
        .map(|p| p.trim_end_matches('/').to_string())
        .filter(|p| !p.is_empty())
}

/// Prepend an already-normalized prefix to a key. With no prefix the key is
/// returned unchanged. Both backends route every operation's key through this so
/// they address identical objects.
pub(crate) fn apply_prefix(prefix: Option<&str>, key: &str) -> String {
    match prefix {
        Some(p) => format!("{p}/{key}"),
        None => key.to_string(),
    }
}

/// Strip a configured key prefix from a key returned by ListObjectsV2.
///
/// With no configured prefix, the listed key is already in coven's local key
/// space. With a prefix, the configured root is removed from the returned key so
/// the caller sees the same keyspace as a no-prefix backend. In both cases the
/// listed key must also sit under the exact ListObjectsV2 query prefix; an
/// out-of-prefix key is skipped by the caller after logging the provider
/// mismatch.
pub(crate) fn strip_listed_key_prefix<'a>(
    configured_prefix: Option<&str>,
    queried_prefix: &str,
    key: &'a str,
) -> Option<&'a str> {
    if !key.starts_with(queried_prefix) {
        return None;
    }
    match configured_prefix {
        Some(prefix) => key.strip_prefix(prefix)?.strip_prefix('/'),
        None => Some(key),
    }
}

/// Whether an S3 error code/marker means "absent key" — `NoSuchKey` (GCS's S3 XML
/// API) or `NotFound` (AWS HeadObject). The shared not-found rule both backends'
/// `delete` (404 is success) and `exists` (404 is false) apply, instead of each
/// matching on a Display string.
pub(crate) fn is_not_found_code(code: Option<&str>) -> bool {
    matches!(code, Some("NoSuchKey") | Some("NotFound"))
}

/// Map a failed `probe` (HeadBucket) to a `CloudHomeError` from its HTTP status
/// and optional S3 error code: 404 → the bucket doesn't exist; 403 or an
/// auth-signalling code → credentials rejected; anything else → a generic probe
/// failure.
pub(crate) fn probe_error(status: u16, code: Option<&str>, bucket: &str) -> CloudHomeError {
    if status == 404 || code == Some("NoSuchBucket") {
        return CloudHomeError::Configuration(format!("bucket {bucket:?} does not exist"));
    }
    let is_auth = status == 403
        || matches!(
            code,
            Some("SignatureDoesNotMatch") | Some("InvalidAccessKeyId")
        );
    if is_auth {
        CloudHomeError::Configuration(format!(
            "S3 credentials rejected (status {status}, code {code:?})"
        ))
    } else {
        CloudHomeError::Transport(format!("S3 probe failed (status {status}, code {code:?})"))
    }
}

/// Whether a ranged GET response is a real partial read: HTTP 206 Partial
/// Content, never 200. A 200 to a `Range` request means the server ignored the
/// range and returned the whole object from byte 0, so serving that body as the
/// requested range is silent corruption. OAuth REST transports gate on the status
/// directly with this. The AWS SDK S3 backend verifies the equivalent invariant
/// by asserting the body length equals the requested byte count.
pub(crate) fn is_range_success(status: u16) -> bool {
    status == 206
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_listed_key_prefix_no_configured_prefix_keeps_key() {
        assert_eq!(
            strip_listed_key_prefix(None, "objects/", "objects/dev1.json"),
            Some("objects/dev1.json")
        );
    }

    #[test]
    fn strip_listed_key_prefix_removes_configured_prefix() {
        assert_eq!(
            strip_listed_key_prefix(
                Some("libs/abc"),
                "libs/abc/objects/",
                "libs/abc/objects/dev1.json"
            ),
            Some("objects/dev1.json")
        );
    }

    #[test]
    fn strip_listed_key_prefix_skips_key_outside_configured_prefix() {
        assert_eq!(
            strip_listed_key_prefix(
                Some("libs/abc"),
                "libs/abc/objects/",
                "other/objects/dev1.json"
            ),
            None
        );
    }

    #[test]
    fn strip_listed_key_prefix_skips_key_outside_queried_prefix() {
        assert_eq!(
            strip_listed_key_prefix(None, "objects/", "membership/owner/1.enc"),
            None
        );
        assert_eq!(
            strip_listed_key_prefix(
                Some("libs/abc"),
                "libs/abc/objects/",
                "libs/abc/membership/owner/1.enc"
            ),
            None
        );
    }

    #[test]
    fn range_status_requires_partial_content() {
        assert!(is_range_success(206));
        assert!(!is_range_success(200));
    }
}
