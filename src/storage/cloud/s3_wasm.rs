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
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use quick_xml::events::Event;
use quick_xml::Reader;
use reqsign_aws_v4::{RequestSigner, StaticCredentialProvider};
use reqsign_core::{Context, Signer};
use reqwest::Response;
use tracing::warn;

use super::s3_common::{
    apply_prefix, list_strip_prefix, normalize_prefix, probe_error, range_header, s3_join_info,
};
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
            // Normalize once at the source (shared with the native backend), so
            // neither full_key nor list re-trims.
            key_prefix: normalize_prefix(key_prefix),
            base_url,
        }
    }

    /// Prepend the key prefix (if configured) to produce the full S3 object key.
    fn full_key(&self, key: &str) -> String {
        apply_prefix(self.key_prefix.as_deref(), key)
    }

    /// Sign `request`'s parts with SigV4, returning the request with the
    /// `Authorization`, `x-amz-date`, and `x-amz-content-sha256` headers added.
    /// The body is carried through untouched: it is signed `UNSIGNED-PAYLOAD`
    /// because no `x-amz-content-sha256` header is set going in (see the module
    /// docs). Split from `send` so the signing step is exercised directly by the
    /// signature test on the same code path requests take in flight.
    async fn signed_request(
        &self,
        request: Request<Vec<u8>>,
    ) -> Result<Request<Vec<u8>>, CloudHomeError> {
        let (mut parts, body) = request.into_parts();
        self.signer
            .sign(&mut parts, None)
            .await
            .map_err(|e| CloudHomeError::Storage(format!("sign request: {e}")))?;
        Ok(Request::from_parts(parts, body))
    }

    /// Sign `request` with SigV4 and send it. Returns the reqwest response without
    /// inspecting its status — callers map status codes to their own semantics.
    async fn send(&self, request: Request<Vec<u8>>) -> Result<Response, CloudHomeError> {
        let signed = self.signed_request(request).await?;
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

/// Files at or below this size go up as one PUT; larger ones stream through a
/// multipart upload. The threshold equals the part size, so the smallest
/// multipart upload is two parts. S3 requires every non-final part to be ≥ 5 MiB;
/// 8 MiB keeps the part count reasonable for large audio files.
const MULTIPART_THRESHOLD: usize = 8 * 1024 * 1024;
const MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;

/// A [`super::PartSink`] over an S3 multipart upload driven by the wasm signing
/// path: each `send_part` is a signed `PUT ...?partNumber=N&uploadId=ID` whose
/// `ETag` is kept, and `finish` is the `POST ...?uploadId=ID` completion. On any
/// failure the upload is aborted (best-effort) so the bucket holds no orphan parts.
struct WasmS3PartSink<'a> {
    home: &'a S3WasmCloudHome,
    full_key: String,
    upload_id: String,
    /// `(partNumber, ETag)` pairs in send order, replayed in the completion XML.
    parts: Vec<(i32, String)>,
    next_part_number: i32,
}

impl WasmS3PartSink<'_> {
    /// Best-effort abort (`DELETE ...?uploadId=ID`) so a failed upload leaves no
    /// orphaned parts.
    async fn abort(&self) {
        let url = format!(
            "{}?uploadId={}",
            object_url(&self.home.base_url, &self.home.bucket, &self.full_key),
            encode_query_value(&self.upload_id)
        );
        let request = match Request::builder()
            .method(Method::DELETE)
            .uri(url)
            .body(Vec::new())
        {
            Ok(r) => r,
            Err(e) => {
                warn!("could not build multipart abort for {}: {e}", self.full_key);
                return;
            }
        };
        match self.home.send(request).await {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => warn!(
                "multipart abort for {} returned status {}",
                self.full_key,
                resp.status().as_u16()
            ),
            Err(e) => warn!("multipart abort for {} failed: {e}", self.full_key),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl super::PartSink for WasmS3PartSink<'_> {
    fn part_size(&self) -> usize {
        MULTIPART_PART_SIZE
    }

    async fn send_part(
        &mut self,
        part: bytes::Bytes,
        _offset: u64,
        _is_last: bool,
    ) -> Result<(), CloudHomeError> {
        let part_number = self.next_part_number;
        self.next_part_number += 1;
        let url = format!(
            "{}?partNumber={part_number}&uploadId={}",
            object_url(&self.home.base_url, &self.home.bucket, &self.full_key),
            encode_query_value(&self.upload_id)
        );
        let request = Request::builder()
            .method(Method::PUT)
            .uri(url)
            .body(part.to_vec())
            .map_err(|e| {
                CloudHomeError::Storage(format!(
                    "build multipart part {part_number} {}: {e}",
                    self.full_key
                ))
            })?;
        let resp = match self.home.send(request).await {
            Ok(resp) => resp,
            Err(e) => {
                self.abort().await;
                return Err(e);
            }
        };
        if !resp.status().is_success() {
            let err = status_error(
                &format!("multipart part {part_number} {}", self.full_key),
                resp,
            )
            .await;
            self.abort().await;
            return Err(err);
        }
        let etag = resp
            .headers()
            .get(http::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| {
                CloudHomeError::Storage(format!(
                    "multipart part {part_number} {}: no ETag returned",
                    self.full_key
                ))
            });
        let etag = match etag {
            Ok(etag) => etag,
            Err(e) => {
                self.abort().await;
                return Err(e);
            }
        };
        self.parts.push((part_number, etag));
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        let body = complete_multipart_xml(&self.parts);
        let url = format!(
            "{}?uploadId={}",
            object_url(&self.home.base_url, &self.home.bucket, &self.full_key),
            encode_query_value(&self.upload_id)
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri(url)
            .body(body.into_bytes())
            .map_err(|e| {
                CloudHomeError::Storage(format!("build multipart complete {}: {e}", self.full_key))
            })?;
        let resp = match self.home.send(request).await {
            Ok(resp) => resp,
            Err(e) => {
                self.abort().await;
                return Err(e);
            }
        };
        // S3 can return a 200 carrying an `<Error>` body for a completion failure,
        // but a non-2xx status is the common failure; treat non-success as an error.
        if !resp.status().is_success() {
            let err = status_error(&format!("multipart complete {}", self.full_key), resp).await;
            self.abort().await;
            return Err(err);
        }
        Ok(())
    }
}

/// Build the `CompleteMultipartUpload` request body from the collected
/// `(partNumber, ETag)` pairs. The ETag is XML-escaped (S3 returns it quoted).
fn complete_multipart_xml(parts: &[(i32, String)]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (n, etag) in parts {
        let etag = etag.replace('&', "&amp;").replace('<', "&lt;");
        xml.push_str(&format!(
            "<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag></Part>"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

/// Extract `<UploadId>` from an `InitiateMultipartUploadResult` body. The body is
/// an untrusted S3 response, so it goes through quick-xml like the list parser.
fn parse_upload_id(xml: &str) -> Result<String, CloudHomeError> {
    let mut reader = Reader::from_str(xml);
    let mut in_upload_id = false;
    let mut value = String::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) => {
                in_upload_id = start.local_name().as_ref() == b"UploadId";
            }
            Ok(Event::Text(chunk)) if in_upload_id => {
                let piece = chunk
                    .decode()
                    .map_err(|e| CloudHomeError::Storage(format!("parse upload id: {e}")))?;
                value.push_str(&piece);
            }
            Ok(Event::End(_)) => in_upload_id = false,
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(CloudHomeError::Storage(format!("parse upload id: {e}"))),
        }
    }
    if value.is_empty() {
        return Err(CloudHomeError::Storage(
            "multipart create: no UploadId in response".to_string(),
        ));
    }
    Ok(value)
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

/// Map a non-success response to a storage error, including the response body so
/// the S3 error code/message is visible. `op` names the operation for the message
/// (e.g. `"put heads/dev1.json"`). Reading the body can itself fail (a dropped
/// connection mid-error); fall back to the empty string so the status still
/// surfaces.
async fn status_error(op: &str, resp: Response) -> CloudHomeError {
    let status = resp.status().as_u16();
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            warn!("could not read S3 error-response body for {op}: {e}");
            String::new()
        }
    };
    CloudHomeError::Storage(format!("{op}: status {status} {body}"))
}

/// Read a successful response's body into bytes. `ctx` names what is being read
/// for the error message (e.g. `"read body for heads/dev1.json"`).
async fn resp_bytes(resp: Response, ctx: &str) -> Result<Vec<u8>, CloudHomeError> {
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| CloudHomeError::Storage(format!("{ctx}: {e}")))?;
    Ok(bytes.to_vec())
}

/// Everything outside the RFC 3986 unreserved set (`A-Za-z0-9-_.~`) is
/// percent-encoded. `NON_ALPHANUMERIC` encodes every byte except `A-Za-z0-9`, so
/// the four unreserved punctuation bytes are removed from it. This is AWS's
/// URI-encoding rule (space → `%20`, never `+`); used for both path segments and
/// query values, which S3 path-style addressing encodes identically.
const S3_URI_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Percent-encode each segment of a key path while keeping the `/` separators.
/// S3 treats the key as opaque bytes; the URL path must encode reserved/unsafe
/// characters within a segment but leave the slashes that delimit the layout.
fn encode_key_path(key: &str) -> String {
    key.split('/')
        .map(|segment| utf8_percent_encode(segment, S3_URI_ENCODE).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

/// Percent-encode a string for use as a URL query value, encoding everything
/// outside the RFC 3986 unreserved set (so `/` and `+` in continuation tokens
/// become `%2F` / `%2B`).
fn encode_query_value(value: &str) -> String {
    utf8_percent_encode(value, S3_URI_ENCODE).to_string()
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
///
/// The body is an untrusted S3 response, so parsing goes through quick-xml: it
/// expands entities, tolerates namespaces and attributes, and reports malformed
/// input as an error instead of mis-scanning it. The element names matched here
/// are matched on their local name, so a namespace-prefixed response parses the
/// same as a bare one.
fn parse_list_objects_v2(xml: &str) -> Result<ListPage, CloudHomeError> {
    /// Which leaf element's text the reader is currently inside, if any. Only the
    /// elements whose text this parser consumes are tracked.
    enum In {
        Key,
        IsTruncated,
        NextContinuationToken,
        Other,
    }

    let mut reader = Reader::from_str(xml);
    let mut keys = Vec::new();
    let mut is_truncated = false;
    let mut next_continuation_token: Option<String> = None;
    let mut current = In::Other;
    // An element's text can arrive as several `Text` events — quick-xml splits the
    // run at each entity reference (`a&amp;b` → `a`, `&`, `b`). Accumulate the
    // pieces and consume the whole on the matching `End`.
    let mut text = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) => {
                current = match start.local_name().as_ref() {
                    b"Key" => In::Key,
                    b"IsTruncated" => In::IsTruncated,
                    b"NextContinuationToken" => In::NextContinuationToken,
                    _ => In::Other,
                };
                text.clear();
            }
            Ok(Event::Text(chunk)) => {
                // quick-xml 0.40 emits text runs verbatim (no entities inside) and
                // reports each `&entity;` as a separate `GeneralRef` event, so the
                // text is plain — decode the bytes, no unescaping here.
                let piece = chunk
                    .decode()
                    .map_err(|e| CloudHomeError::Storage(format!("parse list XML: {e}")))?;
                text.push_str(&piece);
            }
            Ok(Event::GeneralRef(reference)) => {
                let resolved = if let Some(ch) = reference
                    .resolve_char_ref()
                    .map_err(|e| CloudHomeError::Storage(format!("parse list XML: {e}")))?
                {
                    // A numeric character reference (`&#49;` / `&#x31;`).
                    ch.to_string()
                } else {
                    // A named reference; S3 emits only the five XML predefined
                    // entities. Anything else is malformed S3 output.
                    let name = reference
                        .decode()
                        .map_err(|e| CloudHomeError::Storage(format!("parse list XML: {e}")))?;
                    match quick_xml::escape::resolve_predefined_entity(&name) {
                        Some(value) => value.to_string(),
                        None => {
                            return Err(CloudHomeError::Storage(format!(
                                "parse list XML: unknown entity &{name};"
                            )));
                        }
                    }
                };
                text.push_str(&resolved);
            }
            Ok(Event::End(_)) => {
                match current {
                    In::Key => keys.push(std::mem::take(&mut text)),
                    In::IsTruncated => is_truncated = text.trim().eq_ignore_ascii_case("true"),
                    In::NextContinuationToken => {
                        next_continuation_token = Some(std::mem::take(&mut text))
                    }
                    In::Other => {}
                }
                current = In::Other;
                text.clear();
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => {
                return Err(CloudHomeError::Storage(format!("parse list XML: {e}")));
            }
        }
    }

    // The token is meaningful only when the result is truncated; a server may
    // echo a stale token on the final page.
    if !is_truncated {
        next_continuation_token = None;
    }

    Ok(ListPage {
        keys,
        next_continuation_token,
    })
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
        } else {
            // The shared 404→missing / 403→creds-rejected classification (no S3
            // error code on this path).
            Err(probe_error(status.as_u16(), None, &self.bucket))
        }
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        let request = self.object_request(Method::PUT, &full, data)?;
        let resp = self.send(request).await?;
        if !resp.status().is_success() {
            return Err(status_error(&format!("put {key}"), resp).await);
        }
        Ok(())
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        _total_len: u64,
    ) -> Result<super::BoxPartSink<'a>, CloudHomeError> {
        let full = self.full_key(key);
        // POST {bucket}/{key}?uploads opens the multipart upload; the response XML
        // carries the UploadId every later part and the completion reference.
        let url = format!(
            "{}?uploads",
            object_url(&self.base_url, &self.bucket, &full)
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri(url)
            .body(Vec::new())
            .map_err(|e| CloudHomeError::Storage(format!("build multipart create {key}: {e}")))?;
        let resp = self.send(request).await?;
        if !resp.status().is_success() {
            return Err(status_error(&format!("multipart create {key}"), resp).await);
        }
        let body = resp
            .text()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read multipart create {key}: {e}")))?;
        let upload_id = parse_upload_id(&body)?;
        Ok(Box::new(WasmS3PartSink {
            home: self,
            full_key: full,
            upload_id,
            parts: Vec::new(),
            next_part_number: 1,
        }))
    }

    fn multipart_threshold(&self) -> u64 {
        MULTIPART_THRESHOLD as u64
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
            return Err(status_error(&format!("get {key}"), resp).await);
        }
        resp_bytes(resp, &format!("read body for {key}")).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        let full = self.full_key(key);
        let url = object_url(&self.base_url, &self.bucket, &full);
        let request = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header(http::header::RANGE, range_header(start, end))
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
            return Err(status_error(&format!("get range {key}"), resp).await);
        }
        resp_bytes(resp, &format!("read range body for {key}")).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        let full_prefix = self.full_key(prefix);
        let strip_prefix = list_strip_prefix(self.key_prefix.as_deref());

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
                match strip_prefix.as_deref() {
                    // We queried S3 with this prefix, so every key should carry it;
                    // a key that doesn't is anomalous — skip it loudly rather than
                    // return a wrongly-prefixed key the caller can't resolve.
                    Some(p) => match key.strip_prefix(p) {
                        Some(stripped) => keys.push(stripped.to_string()),
                        None => {
                            warn!("S3 returned key {key:?} outside queried prefix {p:?}; skipping")
                        }
                    },
                    None => keys.push(key),
                }
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
            Err(status_error(&format!("delete {key}"), resp).await)
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

    async fn grant_access(&self, _member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        Ok(s3_join_info(
            self.bucket.clone(),
            self.region.clone(),
            self.endpoint.clone(),
            self.access_key.clone(),
            self.secret_key.clone(),
            self.key_prefix.clone(),
        ))
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
    fn normalized_prefix_drops_trailing_slash() {
        let prefix = normalize_prefix(Some("libs/abc/".to_string()));
        assert_eq!(
            apply_prefix(prefix.as_deref(), "heads/dev1.json"),
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
        assert_eq!(range_header(0, 24), "bytes=0-23");
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

    /// Build a request, run it through the real `signed_request` step `send` uses,
    /// and assert the Authorization header has the SigV4 shape: algorithm,
    /// credential scope ending in `/s3/aws4_request`, a non-empty SignedHeaders
    /// list, and a 64-hex-character signature. The signing time comes from the
    /// wall clock (reqsign exposes a fixed-time override only inside its own
    /// tests), so the date inside the scope is not asserted — only the structure
    /// that proves the request was signed for the S3 service in the configured
    /// region.
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
        let signed = home.signed_request(request).await.expect("sign request");
        let headers = signed.headers();

        let auth = headers
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
        assert!(headers.contains_key("x-amz-date"));
        assert_eq!(
            headers
                .get("x-amz-content-sha256")
                .map(|v| v.to_str().expect("x-amz-content-sha256 is valid ASCII")),
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
