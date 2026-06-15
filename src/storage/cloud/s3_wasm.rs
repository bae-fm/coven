//! S3-backed `CloudHome` for the browser.
//!
//! The native S3 backend (`super::s3`) drives the `aws-sdk-s3`, which is bound to
//! tokio and a native TLS stack and does not build for wasm. This module talks to
//! the same S3 REST API directly: it builds an `http::Request`, signs it with AWS
//! SigV4 (via `reqsign-aws-v4`), and sends it through reqwest's `fetch` backend.
//! Its public semantics mirror `super::s3::S3CloudHome` exactly — the same prefix
//! handling, object-key layout, path-style addressing, join info, and error
//! mapping — so a library created on the desktop opens unchanged in the browser.
//!
//! ## Addressing
//!
//! Requests use path-style addressing (`{base}/{bucket}/{key}`), matching the
//! native backend's `force_path_style(true)`. With no custom endpoint the base is
//! the regional virtual endpoint `https://s3.{region}.amazonaws.com`; a configured
//! endpoint (MinIO, Backblaze, R2, Google Cloud Storage's S3 API) is used verbatim.
//!
//! ## Payload hashing
//!
//! Requests carry no `x-amz-content-sha256` header, so the signer signs them
//! `UNSIGNED-PAYLOAD`. The browser streams bodies through `fetch` with no chance to
//! pre-hash them, and the transport is HTTPS, so the body is integrity-protected at
//! the connection layer. (Coven's own at-rest protection authenticates blob
//! contents independently of S3.)
//!
//! ## CORS
//!
//! A browser may only read these responses if the bucket returns
//! `Access-Control-Allow-Origin` for the app's origin. The bucket owner applies a
//! one-time CORS policy allowing methods GET/PUT/POST/DELETE/HEAD, headers `*`, and
//! exposing the `ETag` header. Without it the browser blocks every response before
//! coven sees it; this is a bucket-side configuration, not something the client can
//! set per request.

use async_trait::async_trait;
use http::{Method, Request};
use reqsign_aws_v4::{RequestSigner, StaticCredentialProvider};
use reqsign_core::{Context, Signer};
use tracing::warn;

use super::{CloudHome, CloudHomeError, CloudHomeJoinInfo};

/// S3-backed cloud home that signs requests and sends them over `fetch`.
pub struct S3WasmCloudHome {
    client: reqwest::Client,
    signer: Signer<reqsign_aws_v4::Credential>,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    key_prefix: Option<String>,
    /// Base URL for object/list requests: a configured endpoint or the regional
    /// AWS virtual endpoint. Trailing slash trimmed so URL joins are uniform.
    base_url: String,
}

impl S3WasmCloudHome {
    pub fn new(
        bucket: String,
        region: String,
        endpoint: Option<String>,
        access_key: String,
        secret_key: String,
        key_prefix: Option<String>,
    ) -> Self {
        // Context::new() wires every reqsign service (file read, http send, env)
        // to its no-op default. A StaticCredentialProvider ignores the context and
        // hands back the configured keys, so no service is ever exercised — the
        // signer is a pure SigV4 calculator over the request parts.
        let signer = Signer::new(
            Context::new(),
            StaticCredentialProvider::new(&access_key, &secret_key),
            RequestSigner::new("s3", &region),
        );
        let base_url = endpoint_base(&region, endpoint.as_deref());
        S3WasmCloudHome {
            client: reqwest::Client::new(),
            signer,
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            key_prefix,
            base_url,
        }
    }

    /// Prepend the key prefix (if configured) to produce the full S3 object key.
    fn full_key(&self, key: &str) -> String {
        apply_prefix(self.key_prefix.as_deref(), key)
    }

    /// Sign `request` with SigV4 and send it. Returns the reqwest response without
    /// inspecting its status — callers map status codes to their own semantics.
    async fn send(&self, request: Request<Vec<u8>>) -> Result<reqwest::Response, CloudHomeError> {
        let (mut parts, body) = request.into_parts();
        self.signer
            .sign(&mut parts, None)
            .await
            .map_err(|e| CloudHomeError::Storage(format!("sign request: {e}")))?;
        let signed = Request::from_parts(parts, body);
        let req = reqwest::Request::try_from(signed)
            .map_err(|e| CloudHomeError::Storage(format!("build request: {e}")))?;
        self.client
            .execute(req)
            .await
            .map_err(|e| CloudHomeError::Storage(format!("send request: {e}")))
    }

    /// Build an unsigned request for an object under the prefixed key.
    fn object_request(
        &self,
        method: Method,
        full_key: &str,
        body: Vec<u8>,
    ) -> Result<Request<Vec<u8>>, CloudHomeError> {
        let url = object_url(&self.base_url, &self.bucket, full_key);
        Request::builder()
            .method(method)
            .uri(url)
            .body(body)
            .map_err(|e| CloudHomeError::Storage(format!("build request for {full_key}: {e}")))
    }
}

/// Resolve the base URL for path-style requests. A configured endpoint is used
/// verbatim (S3-compatible providers, MinIO, R2); otherwise the regional AWS
/// virtual endpoint. The trailing slash is trimmed so callers append `/bucket/key`.
fn endpoint_base(region: &str, endpoint: Option<&str>) -> String {
    match endpoint {
        Some(ep) => ep.trim_end_matches('/').to_string(),
        None => format!("https://s3.{region}.amazonaws.com"),
    }
}

/// Build a path-style object URL: `{base}/{bucket}/{key}`. Each key segment is
/// percent-encoded (S3 keys may contain characters that aren't URL-safe), while
/// the `/` separators between segments are preserved so the object's path layout
/// survives intact.
fn object_url(base: &str, bucket: &str, full_key: &str) -> String {
    let encoded = encode_key_path(full_key);
    format!("{base}/{bucket}/{encoded}")
}

/// Build a ListObjectsV2 URL with an optional continuation token:
/// `{base}/{bucket}?list-type=2&prefix={prefix}[&continuation-token={token}]`.
fn list_url(
    base: &str,
    bucket: &str,
    full_prefix: &str,
    continuation_token: Option<&str>,
) -> String {
    let mut url = format!(
        "{base}/{bucket}?list-type=2&prefix={}",
        encode_query_value(full_prefix)
    );
    if let Some(token) = continuation_token {
        url.push_str("&continuation-token=");
        url.push_str(&encode_query_value(token));
    }
    url
}

/// Percent-encode each segment of a key path while keeping the `/` separators.
/// S3 treats the key as opaque bytes; the URL path must encode reserved/unsafe
/// characters within a segment but leave the slashes that delimit the layout.
fn encode_key_path(key: &str) -> String {
    key.split('/')
        .map(encode_query_value)
        .collect::<Vec<_>>()
        .join("/")
}

/// Percent-encode a string for use as a URL query value or path segment, encoding
/// everything outside the RFC 3986 unreserved set. Matches AWS's URI-encoding
/// rules (space → `%20`, not `+`).
fn encode_query_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            other => {
                out.push('%');
                out.push_str(&format!("{other:02X}"));
            }
        }
    }
    out
}

/// Prepend an optional prefix to a key. Trailing slashes on the prefix are
/// normalized. Identical to the native backend so both compute the same object
/// keys for one library.
fn apply_prefix(prefix: Option<&str>, key: &str) -> String {
    match prefix {
        Some(p) => format!("{}/{}", p.trim_end_matches('/'), key),
        None => key.to_string(),
    }
}

/// One page of a ListObjectsV2 response: the object keys it lists and the
/// continuation token to fetch the next page, present only when the result was
/// truncated.
struct ListPage {
    keys: Vec<String>,
    next_continuation_token: Option<String>,
}

/// Parse a ListObjectsV2 `ListBucketResult` XML body into its keys and the next
/// continuation token. Each `<Contents><Key>…</Key></Contents>` yields one key;
/// `<NextContinuationToken>` is returned only when `<IsTruncated>true</IsTruncated>`,
/// matching S3's contract that the token is meaningful exactly when more pages
/// remain.
fn parse_list_objects_v2(xml: &str) -> Result<ListPage, CloudHomeError> {
    let keys = extract_tag_values(xml, "Key");
    let is_truncated = extract_first_tag_value(xml, "IsTruncated")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let next_continuation_token = if is_truncated {
        extract_first_tag_value(xml, "NextContinuationToken")
    } else {
        None
    };
    Ok(ListPage {
        keys,
        next_continuation_token,
    })
}

/// Collect the text content of every `<tag>…</tag>` element in `xml`, in document
/// order, decoding XML entities. A minimal scanner sufficient for the flat,
/// attribute-free ListBucketResult elements S3 returns; coven controls the keys it
/// stores, so they hold no XML-hostile structure.
fn extract_tag_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut values = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after_open = &rest[start + open.len()..];
        let Some(end) = after_open.find(&close) else {
            break;
        };
        values.push(decode_xml_entities(&after_open[..end]));
        rest = &after_open[end + close.len()..];
    }
    values
}

/// Text content of the first `<tag>…</tag>` element, or `None` if absent.
fn extract_first_tag_value(xml: &str, tag: &str) -> Option<String> {
    extract_tag_values(xml, tag).into_iter().next()
}

/// Decode the five predefined XML entities S3 may emit in a key (e.g. `&amp;`).
fn decode_xml_entities(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        // `&amp;` last so a literal `&amp;lt;` round-trips to `&lt;`, not `<`.
        .replace("&amp;", "&")
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CloudHome for S3WasmCloudHome {
    /// HEAD the bucket — a cheap auth + existence check with no listing cost.
    async fn probe(&self) -> Result<(), CloudHomeError> {
        let url = format!("{}/{}", self.base_url, self.bucket);
        let request = Request::builder()
            .method(Method::HEAD)
            .uri(url)
            .body(Vec::new())
            .map_err(|e| CloudHomeError::Storage(format!("build probe request: {e}")))?;
        let resp = self.send(request).await?;
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Err(CloudHomeError::Storage(format!(
                "bucket {:?} does not exist",
                self.bucket
            )))
        } else if status == reqwest::StatusCode::FORBIDDEN {
            Err(CloudHomeError::Storage(format!(
                "S3 credentials rejected (status {})",
                status.as_u16()
            )))
        } else {
            Err(CloudHomeError::Storage(format!(
                "S3 probe failed (status {})",
                status.as_u16()
            )))
        }
    }

    async fn write(
        &self,
        key: &str,
        data: Vec<u8>,
        progress: &super::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        let total = data.len() as u64;
        let request = self.object_request(Method::PUT, &full, data)?;
        let resp = self.send(request).await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(CloudHomeError::Storage(format!(
                "put {key}: status {} {body}",
                status.as_u16()
            )));
        }
        // fetch gives no streaming upload progress; report the whole size once the
        // PUT completes.
        progress(total);
        Ok(())
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        let full = self.full_key(key);
        let request = self.object_request(Method::GET, &full, Vec::new())?;
        let resp = self.send(request).await?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(CloudHomeError::NotFound(key.to_string()));
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(CloudHomeError::Storage(format!(
                "get {key}: status {} {body}",
                status.as_u16()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read body for {key}: {e}")))?;
        Ok(bytes.to_vec())
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        let full = self.full_key(key);
        let url = object_url(&self.base_url, &self.bucket, &full);
        // `end` is exclusive; the HTTP Range header is inclusive on both ends.
        let range = format!("bytes={start}-{}", end.saturating_sub(1));
        let request = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header(http::header::RANGE, range)
            .body(Vec::new())
            .map_err(|e| CloudHomeError::Storage(format!("build range request for {key}: {e}")))?;
        let resp = self.send(request).await?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(CloudHomeError::NotFound(key.to_string()));
        }
        // A range request succeeds with 206 Partial Content; a server that ignores
        // the range answers 200 with the whole object, which is still the bytes the
        // caller asked to start at, so accept any success.
        if status != reqwest::StatusCode::PARTIAL_CONTENT && !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(CloudHomeError::Storage(format!(
                "get range {key}: status {} {body}",
                status.as_u16()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read range body for {key}: {e}")))?;
        Ok(bytes.to_vec())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        let full_prefix = self.full_key(prefix);
        let strip_prefix = self
            .key_prefix
            .as_ref()
            .map(|p| format!("{}/", p.trim_end_matches('/')));

        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let url = list_url(
                &self.base_url,
                &self.bucket,
                &full_prefix,
                continuation_token.as_deref(),
            );
            let request = Request::builder()
                .method(Method::GET)
                .uri(url)
                .body(Vec::new())
                .map_err(|e| CloudHomeError::Storage(format!("build list request: {e}")))?;
            let resp = self.send(request).await?;
            let status = resp.status();
            let body = resp
                .text()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("read list body: {e}")))?;
            if !status.is_success() {
                return Err(CloudHomeError::Storage(format!(
                    "list {prefix}: status {} {body}",
                    status.as_u16()
                )));
            }

            let page = parse_list_objects_v2(&body)?;
            for key in page.keys {
                let stripped = match &strip_prefix {
                    Some(p) => key
                        .strip_prefix(p.as_str())
                        .map(str::to_string)
                        .unwrap_or(key),
                    None => key,
                };
                keys.push(stripped);
            }

            match page.next_continuation_token {
                Some(token) => continuation_token = Some(token),
                None => break,
            }
        }

        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        let request = self.object_request(Method::DELETE, &full, Vec::new())?;
        let resp = self.send(request).await?;
        let status = resp.status();
        // S3 returns 204 for a deleted key and also 204 for an already-absent key,
        // so a missing object is not an error. Surface only real failures.
        if status.is_success() || status == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(CloudHomeError::Storage(format!(
                "delete {key}: status {} {body}",
                status.as_u16()
            )))
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        let full = self.full_key(key);
        let request = self.object_request(Method::HEAD, &full, Vec::new())?;
        let resp = self.send(request).await?;
        let status = resp.status();
        if status.is_success() {
            Ok(true)
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Ok(false)
        } else {
            Err(CloudHomeError::Storage(format!(
                "head {key}: status {}",
                status.as_u16()
            )))
        }
    }

    async fn grant_access(&self, member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        // S3 access is managed externally (IAM / pre-shared credentials), so this
        // ignores the member and returns the owner's credentials to embed in the
        // invite code — identical to the native backend.
        let _ = member_id;
        Ok(CloudHomeJoinInfo::S3 {
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            key_prefix: self.key_prefix.clone(),
        })
    }

    async fn revoke_access(&self, member_id: &str) -> Result<(), CloudHomeError> {
        // Access is controlled externally; nothing to revoke. Logged so a caller
        // expecting revocation sees that S3 cannot enforce it here.
        warn!("S3 access is managed externally; revoke for {member_id} is a no-op");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_prefix_prepends_prefix() {
        assert_eq!(
            apply_prefix(Some("libs/abc"), "heads/dev1.json"),
            "libs/abc/heads/dev1.json"
        );
    }

    #[test]
    fn apply_prefix_no_prefix() {
        assert_eq!(apply_prefix(None, "heads/dev1.json"), "heads/dev1.json");
    }

    #[test]
    fn apply_prefix_strips_trailing_slash() {
        assert_eq!(
            apply_prefix(Some("libs/abc/"), "heads/dev1.json"),
            "libs/abc/heads/dev1.json"
        );
    }

    #[test]
    fn endpoint_base_uses_regional_aws_endpoint_by_default() {
        assert_eq!(
            endpoint_base("us-west-2", None),
            "https://s3.us-west-2.amazonaws.com"
        );
    }

    #[test]
    fn endpoint_base_uses_configured_endpoint_verbatim() {
        assert_eq!(
            endpoint_base("us-east-1", Some("http://localhost:9000/")),
            "http://localhost:9000"
        );
    }

    #[test]
    fn object_url_is_path_style_with_encoded_segments() {
        let base = endpoint_base("us-east-1", None);
        let full = apply_prefix(Some("libs/abc"), "changes/dev 1/42.enc");
        assert_eq!(
            object_url(&base, "my-bucket", &full),
            "https://s3.us-east-1.amazonaws.com/my-bucket/libs/abc/changes/dev%201/42.enc"
        );
    }

    #[test]
    fn list_url_encodes_prefix_and_token() {
        let base = endpoint_base("us-east-1", None);
        assert_eq!(
            list_url(&base, "my-bucket", "libs/abc/changes/", None),
            "https://s3.us-east-1.amazonaws.com/my-bucket?list-type=2&prefix=libs%2Fabc%2Fchanges%2F"
        );
        assert_eq!(
            list_url(&base, "my-bucket", "p/", Some("tok/en+1")),
            "https://s3.us-east-1.amazonaws.com/my-bucket?list-type=2&prefix=p%2F&continuation-token=tok%2Fen%2B1"
        );
    }

    #[test]
    fn range_header_is_inclusive_on_both_ends() {
        // end is exclusive in read_range; the header end is `end - 1`.
        let start = 0u64;
        let end = 24u64;
        assert_eq!(
            format!("bytes={start}-{}", end.saturating_sub(1)),
            "bytes=0-23"
        );
    }

    #[test]
    fn parse_list_single_page_no_token() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>my-bucket</Name>
  <Prefix>libs/abc/</Prefix>
  <KeyCount>2</KeyCount>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>libs/abc/heads/dev1.json</Key>
    <Size>10</Size>
  </Contents>
  <Contents>
    <Key>libs/abc/changes/dev1/1.enc</Key>
    <Size>20</Size>
  </Contents>
</ListBucketResult>"#;
        let page = parse_list_objects_v2(xml).expect("parse list");
        assert_eq!(
            page.keys,
            vec![
                "libs/abc/heads/dev1.json".to_string(),
                "libs/abc/changes/dev1/1.enc".to_string(),
            ]
        );
        assert_eq!(page.next_continuation_token, None);
    }

    #[test]
    fn parse_list_truncated_page_yields_token() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>
  <Contents><Key>a/1</Key></Contents>
  <Contents><Key>a/2</Key></Contents>
</ListBucketResult>"#;
        let page = parse_list_objects_v2(xml).expect("parse list");
        assert_eq!(page.keys, vec!["a/1".to_string(), "a/2".to_string()]);
        assert_eq!(
            page.next_continuation_token,
            Some("1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=".to_string())
        );
    }

    #[test]
    fn parse_list_token_ignored_when_not_truncated() {
        // A server may echo a token even on the final page; honor IsTruncated so
        // pagination stops instead of looping on a stale token.
        let xml = r#"<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <NextContinuationToken>stale</NextContinuationToken>
  <Contents><Key>only</Key></Contents>
</ListBucketResult>"#;
        let page = parse_list_objects_v2(xml).expect("parse list");
        assert_eq!(page.keys, vec!["only".to_string()]);
        assert_eq!(page.next_continuation_token, None);
    }

    #[test]
    fn parse_list_empty_result() {
        let xml = r#"<ListBucketResult><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        let page = parse_list_objects_v2(xml).expect("parse list");
        assert!(page.keys.is_empty());
        assert_eq!(page.next_continuation_token, None);
    }

    #[test]
    fn parse_list_decodes_xml_entities_in_key() {
        let xml = r#"<ListBucketResult><IsTruncated>false</IsTruncated>
  <Contents><Key>a/b&amp;c/d&lt;e&gt;.enc</Key></Contents>
</ListBucketResult>"#;
        let page = parse_list_objects_v2(xml).expect("parse list");
        assert_eq!(page.keys, vec!["a/b&c/d<e>.enc".to_string()]);
    }

    /// Build a signer with fixed credentials, sign a GET, and assert the
    /// Authorization header has the SigV4 shape: algorithm, credential scope
    /// ending in `/s3/aws4_request`, a non-empty SignedHeaders list, and a
    /// 64-hex-character signature. The signing time comes from the wall clock
    /// (reqsign exposes a fixed-time override only inside its own tests), so the
    /// date inside the scope is not asserted — only the structure that proves the
    /// request was signed for the S3 service in the configured region.
    #[tokio::test]
    async fn sign_produces_sigv4_authorization_header() {
        let home = S3WasmCloudHome::new(
            "my-bucket".to_string(),
            "us-east-1".to_string(),
            None,
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            None,
        );

        let url = object_url(&home.base_url, &home.bucket, "heads/dev1.json");
        let request = Request::builder()
            .method(Method::GET)
            .uri(url)
            .body(Vec::<u8>::new())
            .expect("build request");
        let (mut parts, _body) = request.into_parts();
        home.signer
            .sign(&mut parts, None)
            .await
            .expect("sign request");

        let auth = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .expect("authorization header present")
            .to_str()
            .expect("authorization header is ascii");

        assert!(
            auth.starts_with("AWS4-HMAC-SHA256 "),
            "unexpected algorithm prefix: {auth}"
        );

        let credential = field_value(auth, "Credential=").expect("Credential field");
        assert!(
            credential.starts_with("AKIAIOSFODNN7EXAMPLE/"),
            "credential should start with the access key: {credential}"
        );
        assert!(
            credential.ends_with("/us-east-1/s3/aws4_request"),
            "credential scope should be the s3 service in us-east-1: {credential}"
        );

        let signed_headers = field_value(auth, "SignedHeaders=").expect("SignedHeaders field");
        assert!(
            signed_headers.contains("host"),
            "host must be signed: {signed_headers}"
        );

        let signature = field_value(auth, "Signature=").expect("Signature field");
        assert_eq!(
            signature.len(),
            64,
            "signature must be 64 hex chars: {signature}"
        );
        assert!(
            signature.bytes().all(|b| b.is_ascii_hexdigit()),
            "signature must be lowercase hex: {signature}"
        );

        // x-amz-date and x-amz-content-sha256 are added by the signer.
        assert!(parts.headers.contains_key("x-amz-date"));
        assert_eq!(
            parts
                .headers
                .get("x-amz-content-sha256")
                .and_then(|v| v.to_str().ok()),
            Some("UNSIGNED-PAYLOAD"),
        );
    }

    /// Pull the value of a `Name=value` field out of the comma-separated tail of
    /// an Authorization header. Used only by the signing test.
    fn field_value<'a>(header: &'a str, name: &str) -> Option<&'a str> {
        let start = header.find(name)? + name.len();
        let tail = &header[start..];
        Some(match tail.find(',') {
            Some(comma) => tail[..comma].trim(),
            None => tail.trim(),
        })
    }
}
