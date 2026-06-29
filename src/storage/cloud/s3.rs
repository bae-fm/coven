//! S3-backed `CloudHome` implementation.
//!
//! Wraps `aws-sdk-s3` to provide raw storage operations against any
//! S3-compatible endpoint.

use async_trait::async_trait;
use aws_config::stalled_stream_protection::StalledStreamProtectionConfig;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::ResponseChecksumValidation;
use aws_sdk_s3::Client;
use tracing::warn;

use super::s3_common::{apply_prefix, s3_join_info};
use super::{CloudHome, CloudHomeError, CloudHomeJoinInfo};

/// S3-backed cloud home.
pub struct S3CloudHome {
    client: Client,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    key_prefix: Option<String>,
}

impl S3CloudHome {
    pub async fn new(
        bucket: String,
        region: String,
        endpoint: Option<String>,
        access_key: String,
        secret_key: String,
        key_prefix: Option<String>,
    ) -> Result<Self, CloudHomeError> {
        let credentials =
            Credentials::new(&access_key, &secret_key, None, None, "coven-cloud-home");

        // aws-config has default-features disabled, so the SDK won't auto-bundle
        // an HTTP client. Plug in the rustls-ring smithy client explicitly.
        let http_client = aws_smithy_http_client::Builder::new()
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
            ))
            .build_https();

        let mut builder = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .credentials_provider(credentials)
            .http_client(http_client)
            // The SDK default stalled-stream protection aborts any body
            // transfer that stays under 1 B/s for 5 seconds — on slow or
            // briefly-stalling links that kills large legitimate downloads
            // (a pinned release's full-object GETs) with "minimum throughput
            // was specified at 1 B/s, but throughput of 0 B/s was observed".
            // Keep the protection (a truly dead stream should still error;
            // uploads retry from the durable outbox) but give real-world
            // stalls a 60-second grace window.
            .stalled_stream_protection(
                StalledStreamProtectionConfig::enabled()
                    .grace_period(std::time::Duration::from_secs(60))
                    .build(),
            );

        if let Some(ref ep) = endpoint {
            builder = builder.endpoint_url(ep.trim_end_matches('/'));
        }

        let aws_config = builder.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .force_path_style(true)
            // Coven's S3 backend is intentionally S3-compatible, not AWS-only.
            //
            // The AWS SDK default is ResponseChecksumValidation::WhenSupported.
            // For GetObject that default mutates the request to checksum-mode=ENABLED,
            // then validates any returned x-amz-checksum-* header against the response
            // body. That is correct for AWS S3's modeled checksum behavior, but it is
            // not a portable integrity layer for S3-compatible providers.
            //
            // Google Cloud Storage's S3-compatible API returns
            // x-amz-checksum-crc32c on ranged GetObject responses with the checksum of
            // the whole object. A Range: bytes=0-23 response legitimately contains only
            // those 24 bytes, so validating that partial body against the full-object
            // checksum fails with a checksum mismatch before playback can read the
            // encrypted nonce header.
            //
            // Do not use provider checksum headers as coven's generic byte-integrity
            // contract. Managed encrypted blobs are authenticated by their AEAD tags
            // during decrypt; plaintext cloud integrity needs coven-owned metadata or
            // chunk hashes, not provider-specific response-header semantics.
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .build();
        let client = Client::from_conf(s3_config);

        Ok(S3CloudHome {
            client,
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            key_prefix,
        })
    }

    /// Prepend the key prefix (if configured) to produce the full S3 object key.
    fn full_key(&self, key: &str) -> String {
        apply_prefix(self.key_prefix.as_deref(), key)
    }

    /// Upload `data` as a multipart upload, reporting cumulative bytes as each
    /// part lands. `full` is the prefixed object key; `key` is the unprefixed
    /// key used only for error messages. On any failure the in-progress upload
    /// is aborted (best-effort) so the bucket isn't left holding orphaned parts
    /// that accrue storage charges.
    async fn write_multipart(
        &self,
        key: &str,
        full: &str,
        data: Vec<u8>,
        progress: &super::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(full)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("multipart create {key}: {e}")))?;
        let upload_id = create.upload_id().ok_or_else(|| {
            CloudHomeError::Storage(format!("multipart create {key}: no upload id returned"))
        })?;

        match self
            .upload_parts(key, full, upload_id, data, progress)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                // Best-effort cleanup; surface the original write failure, not
                // any abort failure.
                if let Err(abort_err) = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(full)
                    .upload_id(upload_id)
                    .send()
                    .await
                {
                    warn!("Failed to abort multipart upload for {key}: {abort_err}");
                }
                Err(e)
            }
        }
    }

    /// Upload every part for an in-progress multipart upload and complete it.
    /// Split out from `write_multipart` so a failure anywhere here triggers the
    /// caller's abort/cleanup.
    async fn upload_parts(
        &self,
        key: &str,
        full: &str,
        upload_id: &str,
        data: Vec<u8>,
        progress: &super::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        let mut completed = Vec::new();
        let mut sent: u64 = 0;
        // Part numbers are 1-based.
        for (i, chunk) in data.chunks(MULTIPART_PART_SIZE).enumerate() {
            let part_number = (i + 1) as i32;
            let part = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(full)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(chunk.to_vec().into())
                .send()
                .await
                .map_err(|e| {
                    CloudHomeError::Storage(format!("multipart part {part_number} {key}: {e}"))
                })?;
            completed.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(part_number)
                    .set_e_tag(part.e_tag().map(str::to_string))
                    .build(),
            );
            sent += chunk.len() as u64;
            progress(sent);
        }

        let completed_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(full)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("multipart complete {key}: {e}")))?;
        Ok(())
    }
}

/// Files at or below this size go up as a single PUT; larger files use a
/// multipart upload so progress advances per part. The threshold equals the
/// part size, so the smallest multipart upload is two parts.
const MULTIPART_THRESHOLD: usize = 8 * 1024 * 1024;

/// Multipart part size. S3 requires every part except the last to be at least
/// 5 MiB; 8 MiB keeps the part count (and request count) reasonable for large
/// audio files while still giving several progress ticks.
const MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;

fn body_read_error<E>(context: &str, key: &str, err: E) -> CloudHomeError
where
    E: std::error::Error + std::fmt::Debug,
{
    let mut msg = format!("{context} for {key}: {err}");
    let mut source = err.source();
    while let Some(err) = source {
        msg.push_str(&format!("; caused by: {err}"));
        source = err.source();
    }
    CloudHomeError::Storage(msg)
}

/// Map a GetObject failure to a `CloudHomeError`, surfacing the S3 error code and
/// message (e.g. `AccessDenied`, `PermanentRedirect`, `SignatureDoesNotMatch`)
/// rather than the opaque "service error". `NoSuchKey` becomes `NotFound`;
/// non-service failures (timeouts, connection errors) fall back to their own
/// description. Generic over the response type so it serves both `read` and
/// `read_range` without naming the smithy HTTP type.
fn get_object_error<R>(
    key: &str,
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError, R>,
) -> CloudHomeError {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    match err.code() {
        Some("NoSuchKey") => CloudHomeError::NotFound(key.to_string()),
        Some(code) => CloudHomeError::Storage(match err.message() {
            Some(msg) => format!("get {key}: S3 {code}: {msg}"),
            None => format!("get {key}: S3 {code} (no message provided)"),
        }),
        // Not a service error (timeout / connection / dispatch) — its own
        // Display carries the detail.
        None => CloudHomeError::Storage(format!("get {key}: {err}")),
    }
}

/// Map a PutObject failure to a `CloudHomeError`. The common failure modes
/// each name the cause and the recovery the user can take:
///
/// - `AccessDenied` — bucket policy or IAM rejects writes. User fixes via
///   sync settings.
/// - `NoSuchBucket` — bucket was renamed/deleted out from under us.
/// - `OverQuota` / `QuotaExceeded` — non-AWS S3 providers (Backblaze, MinIO)
///   signal quota exhaustion through these codes.
///
/// Other service errors keep the raw code + message so they're debuggable;
/// transport failures surface their own Display.
fn put_object_error(
    key: &str,
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
) -> CloudHomeError {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    match err.code() {
        Some("AccessDenied") => CloudHomeError::Storage(
            "Your S3 credentials don't have permission to write to this bucket. Check the access policy in sync settings."
                .to_string(),
        ),
        Some("NoSuchBucket") => CloudHomeError::Storage(
            "The S3 bucket no longer exists. Check the bucket name in sync settings.".to_string(),
        ),
        Some("OverQuota" | "QuotaExceeded") => CloudHomeError::Storage(
            "Your S3 storage quota is exceeded. Free up space or expand the quota.".to_string(),
        ),
        Some(code) => CloudHomeError::Storage(match err.message() {
            Some(msg) => format!("put {key}: S3 {code}: {msg}"),
            None => format!("put {key}: S3 {code} (no message provided)"),
        }),
        None => CloudHomeError::Storage(format!("put {key}: {err}")),
    }
}

#[async_trait]
impl CloudHome for S3CloudHome {
    /// HeadBucket — cheap auth + existence check, no listing cost.
    async fn probe(&self) -> Result<(), CloudHomeError> {
        use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
        use aws_sdk_s3::operation::head_bucket::HeadBucketError;

        match self.client.head_bucket().bucket(&self.bucket).send().await {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError(svc)) => {
                let status = svc.raw().status().as_u16();
                let code: Option<String> = svc.err().code().map(str::to_string);
                let bucket = self.bucket.clone();
                match svc.into_err() {
                    HeadBucketError::NotFound(_) => Err(CloudHomeError::Storage(format!(
                        "bucket {bucket:?} does not exist"
                    ))),
                    other => {
                        let is_auth = status == 403
                            || matches!(
                                code.as_deref(),
                                Some("SignatureDoesNotMatch") | Some("InvalidAccessKeyId")
                            );
                        if is_auth {
                            Err(CloudHomeError::Storage(format!(
                                "S3 credentials rejected (status {status}, code {code:?})"
                            )))
                        } else {
                            Err(CloudHomeError::Storage(format!(
                                "S3 probe failed (status {status}, code {code:?}): {other}"
                            )))
                        }
                    }
                }
            }
            Err(e) => Err(CloudHomeError::Storage(format!("S3 probe failed: {e}"))),
        }
    }

    async fn write(
        &self,
        key: &str,
        data: Vec<u8>,
        progress: &super::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        // Small files: one PUT, no sub-file progress to report — signal the
        // whole size once on success. Larger files go through multipart so the
        // caller sees progress advance per part.
        if data.len() <= MULTIPART_THRESHOLD {
            let total = data.len() as u64;
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&full)
                .body(data.into())
                .send()
                .await
                .map_err(|e| put_object_error(key, e))?;
            progress(total);
            return Ok(());
        }
        self.write_multipart(key, &full, data, progress).await
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        let full = self.full_key(key);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full)
            .send()
            .await
            .map_err(|e| get_object_error(key, e))?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| body_read_error("read body", key, e))?
            .into_bytes()
            .to_vec();

        Ok(bytes)
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        let full = self.full_key(key);
        let range = format!("bytes={start}-{}", end.saturating_sub(1));
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full)
            .range(range)
            .send()
            .await
            .map_err(|e| get_object_error(key, e))?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| body_read_error("read range body", key, e))?
            .into_bytes()
            .to_vec();

        Ok(bytes)
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
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token.take() {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("list {prefix}: {e}")))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    let stripped = match &strip_prefix {
                        Some(p) => key.strip_prefix(p.as_str()).unwrap_or(key),
                        None => key,
                    };
                    keys.push(stripped.to_string());
                }
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        use aws_sdk_s3::error::ProvideErrorMetadata;
        let full = self.full_key(key);
        if let Err(e) = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(&full)
            .send()
            .await
        {
            // Delete is idempotent: AWS S3 returns 204 for an already-absent key,
            // but GCS's S3 XML API returns 404 `NoSuchKey`. A missing object is not
            // a failure — `cancel_tombstone` deletes the tombstone after every
            // upload and relies on the no-tombstone case being a no-op — so swallow
            // not-found and surface only real errors. The wasm S3 backend already
            // treats 404 as success; this matches it on the native path.
            if e.code() != Some("NoSuchKey") {
                return Err(CloudHomeError::Storage(format!("delete {key}: {e}")));
            }
        }
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        let full = self.full_key(key);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&full)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let msg = format!("{e}");
                if msg.contains("NotFound")
                    || msg.contains("not found")
                    || msg.contains("404")
                    || msg.contains("NoSuchKey")
                {
                    Ok(false)
                } else {
                    Err(CloudHomeError::Storage(format!("head {key}: {e}")))
                }
            }
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

    async fn revoke_access(&self, _member_id: &str) -> Result<(), CloudHomeError> {
        // S3 access is managed externally (IAM/pre-shared credentials).
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE};
    use axum::http::{HeaderMap, Method, Response, StatusCode, Uri};
    use axum::Router;
    use std::sync::Arc;

    #[test]
    fn full_key_prepends_prefix() {
        let key = apply_prefix(Some("libs/abc"), "heads/dev1.json");
        assert_eq!(key, "libs/abc/heads/dev1.json");
    }

    #[test]
    fn full_key_no_prefix() {
        let key = apply_prefix(None, "heads/dev1.json");
        assert_eq!(key, "heads/dev1.json");
    }

    #[test]
    fn full_key_strips_trailing_slash() {
        let key = apply_prefix(Some("libs/abc/"), "heads/dev1.json");
        assert_eq!(key, "libs/abc/heads/dev1.json");
    }

    #[derive(Clone)]
    struct FakeRangeObject {
        bucket: String,
        key: String,
        range_body: Vec<u8>,
        object_len: u64,
        whole_object_crc32c: &'static str,
    }

    async fn fake_s3_range_endpoint(
        State(object): State<Arc<FakeRangeObject>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
    ) -> Response<Body> {
        let expected_path = format!("/{}/{}", object.bucket, object.key);
        let range = headers.get(RANGE).and_then(|v| v.to_str().ok());

        if method != Method::GET || uri.path() != expected_path || range != Some("bytes=0-23") {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!(
                    "unexpected request: method={method}, path={}, range={range:?}",
                    uri.path()
                )))
                .expect("build bad-request response");
        }

        Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(CONTENT_RANGE, format!("bytes 0-23/{}", object.object_len))
            .header(CONTENT_LENGTH, object.range_body.len().to_string())
            .header("x-amz-checksum-crc32c", object.whole_object_crc32c)
            .body(Body::from(object.range_body.clone()))
            .expect("build fake range response")
    }

    async fn spawn_fake_s3_endpoint(
        object: FakeRangeObject,
    ) -> (String, tokio::sync::oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake S3 endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let app = Router::new()
            .fallback(fake_s3_range_endpoint)
            .with_state(Arc::new(object));

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("fake S3 endpoint failed");
        });

        (endpoint, shutdown_tx)
    }

    #[tokio::test]
    async fn read_range_accepts_s3_compatible_full_object_checksum_header() {
        let range_body = b"abcdefghijklmnopqrstuvwx".to_vec();
        let key = "storage/audio-object".to_string();
        let bucket = "coven-s3-compatible-test".to_string();
        let (endpoint, shutdown) = spawn_fake_s3_endpoint(FakeRangeObject {
            bucket: bucket.clone(),
            key: key.clone(),
            range_body: range_body.clone(),
            object_len: 96,
            whole_object_crc32c: "sNqCyA==",
        })
        .await;

        let home = S3CloudHome::new(
            bucket,
            "us-central1".to_string(),
            Some(endpoint),
            "access-key".to_string(),
            "secret-key".to_string(),
            None,
        )
        .await
        .expect("construct S3CloudHome");

        let bytes = home
            .read_range(&key, 0, range_body.len() as u64)
            .await
            .expect("read range");

        assert_eq!(bytes, range_body);
        let _ = shutdown.send(());
    }

    // ── probe() against a real S3 endpoint ──────────────────────────────
    //
    // These tests require a minio (or any S3-compatible server) reachable at
    // `COVEN_TEST_S3_URL` (default http://localhost:19000) with credentials
    // `COVEN_TEST_S3_KEY` / `COVEN_TEST_S3_SECRET` (default minioadmin / minioadmin).
    // Marked `#[ignore]` so `cargo test` skips them; run with
    // `cargo test -- --ignored` when an endpoint is available.

    /// Read a test env var with a default fallback. `NotPresent` silently uses
    /// the default (the intended path); `NotUnicode` panics so a misconfigured
    /// env var fails loudly instead of silently substituting bytes-as-default.
    fn test_env(name: &str, default: &str) -> String {
        match std::env::var(name) {
            Ok(s) => s,
            Err(std::env::VarError::NotPresent) => default.to_string(),
            Err(std::env::VarError::NotUnicode(raw)) => {
                panic!("test env var {name} is non-utf8: {raw:?}");
            }
        }
    }

    struct TestCreds {
        endpoint: String,
        access_key: String,
        secret_key: String,
    }

    fn test_creds() -> TestCreds {
        TestCreds {
            endpoint: test_env("COVEN_TEST_S3_URL", "http://localhost:19000"),
            access_key: test_env("COVEN_TEST_S3_KEY", "coventest"),
            secret_key: test_env("COVEN_TEST_S3_SECRET", "coventestpass"),
        }
    }

    fn required_test_env(name: &str) -> String {
        match std::env::var(name) {
            Ok(s) => s,
            Err(std::env::VarError::NotPresent) => {
                panic!("test env var {name} must be set for this test");
            }
            Err(std::env::VarError::NotUnicode(raw)) => {
                panic!("test env var {name} is non-utf8: {raw:?}");
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn read_range_succeeds_against_existing_s3_object() {
        let creds = test_creds();
        let bucket = required_test_env("COVEN_TEST_S3_BUCKET");
        let region = test_env("COVEN_TEST_S3_REGION", "us-east-1");
        let key = required_test_env("COVEN_TEST_S3_EXISTING_KEY");
        let start: u64 = test_env("COVEN_TEST_S3_RANGE_START", "0")
            .parse()
            .expect("COVEN_TEST_S3_RANGE_START must be a u64");
        let end: u64 = test_env("COVEN_TEST_S3_RANGE_END", "24")
            .parse()
            .expect("COVEN_TEST_S3_RANGE_END must be a u64");

        let home = S3CloudHome::new(
            bucket,
            region,
            Some(creds.endpoint),
            creds.access_key,
            creds.secret_key,
            None,
        )
        .await
        .expect("construct S3CloudHome");

        eprintln!("reading {key} range {start}..{end}");
        let bytes = home
            .read_range(&key, start, end)
            .await
            .unwrap_or_else(|e| panic!("read_range failed: {e:?}"));

        assert_eq!(bytes.len() as u64, end - start);
    }

    /// Provision the bucket configured on `home`.
    async fn provision_test_bucket(home: &S3CloudHome) {
        home.client
            .create_bucket()
            .bucket(&home.bucket)
            .send()
            .await
            .expect("create test bucket");
    }

    #[tokio::test]
    #[ignore]
    async fn probe_succeeds_against_existing_bucket() {
        let creds = test_creds();
        let bucket = format!("coven-probe-ok-{}", uuid::Uuid::new_v4());
        let home = S3CloudHome::new(
            bucket,
            "us-east-1".to_string(),
            Some(creds.endpoint),
            creds.access_key,
            creds.secret_key,
            None,
        )
        .await
        .expect("construct S3CloudHome");
        provision_test_bucket(&home).await;
        home.probe().await.expect("probe should succeed");
    }

    #[tokio::test]
    #[ignore]
    async fn probe_fails_for_missing_bucket() {
        let creds = test_creds();
        let bucket = format!("coven-probe-missing-{}", uuid::Uuid::new_v4());
        let home = S3CloudHome::new(
            bucket.clone(),
            "us-east-1".to_string(),
            Some(creds.endpoint),
            creds.access_key,
            creds.secret_key,
            None,
        )
        .await
        .expect("construct S3CloudHome");
        // Deliberately do NOT create the bucket.
        let err = home
            .probe()
            .await
            .expect_err("probe should fail for a missing bucket");
        let msg = format!("{err}");
        assert!(
            msg.contains("does not exist") || msg.contains("NoSuchBucket") || msg.contains("404"),
            "expected missing-bucket error, got: {msg}",
        );
    }

    #[tokio::test]
    #[ignore]
    async fn probe_fails_for_bad_secret_key() {
        let creds = test_creds();
        let bucket = format!("coven-probe-badkey-{}", uuid::Uuid::new_v4());
        // Provision the bucket with the good creds so the only difference is the bad secret.
        let good = S3CloudHome::new(
            bucket.clone(),
            "us-east-1".to_string(),
            Some(creds.endpoint.clone()),
            creds.access_key.clone(),
            creds.secret_key,
            None,
        )
        .await
        .expect("construct good S3CloudHome");
        provision_test_bucket(&good).await;

        let bad = S3CloudHome::new(
            bucket,
            "us-east-1".to_string(),
            Some(creds.endpoint),
            creds.access_key,
            "wrong-secret".to_string(),
            None,
        )
        .await
        .expect("construct bad S3CloudHome");
        let err = bad
            .probe()
            .await
            .expect_err("probe should fail for bad credentials");
        let msg = format!("{err}");
        assert!(
            msg.contains("rejected")
                || msg.contains("403")
                || msg.contains("SignatureDoesNotMatch"),
            "expected credentials error, got: {msg}",
        );
    }
}
