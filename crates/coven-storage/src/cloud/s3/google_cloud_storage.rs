use bytes::Bytes;
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use super::s3_backend_failure;
use crate::cloud::{
    CloudHomeError, CloudObjectVersion, CloudVersionedObject, ConditionalWriteOutcome,
    UploadControl,
};
use coven_protocol::objects::StorageBackendFailure;

/// How much of the request body is read, and reported, at a time.
///
/// Small enough that a large blob reports hundreds of times over a slow link,
/// large enough that neither the reads nor the reporting is the bottleneck.
const BODY_CHUNK: usize = 256 * 1024;

#[derive(Clone)]
pub(super) struct GoogleCloudStorageXml {
    client: reqwest::Client,
}

pub(super) enum GoogleUploadSource {
    Bytes(Vec<u8>),
    File(std::path::PathBuf),
}

impl GoogleUploadSource {
    /// The request body, reporting bytes into `progress` as the HTTP client
    /// takes them.
    ///
    /// Google Cloud Storage's create-only precondition
    /// (`x-goog-if-generation-match: 0`) is a header on a single request, so an
    /// exact object goes up as one streaming PUT instead of through the
    /// multipart path the other S3 endpoints take. There are no part
    /// acknowledgements here to count, so what the body counts is bytes the
    /// client has pulled from the source and handed to the connection. The
    /// client pulls the next chunk only when it has room to write, so the count
    /// follows the network rather than the disk, and it can lead what the socket
    /// has actually flushed by at most one chunk — which the exact total the
    /// caller reports after a successful response then settles. Without this a
    /// multi-hundred-megabyte blob reported nothing at all until the whole PUT
    /// returned.
    async fn into_body(self, control: UploadControl) -> Result<reqwest::Body, CloudHomeError> {
        let source = match self {
            Self::Bytes(bytes) => ChunkSource::Bytes {
                bytes: Bytes::from(bytes),
                offset: 0,
            },
            Self::File(path) => {
                ChunkSource::File(tokio::fs::File::open(&path).await.map_err(|error| {
                    CloudHomeError::Local(coven_foundation::atomic_file::FileError::at(
                        "open exact Google Cloud Storage upload source",
                        path,
                        error,
                    ))
                })?)
            }
        };
        Ok(reqwest::Body::wrap_stream(futures_util::stream::unfold(
            ReportedBody {
                source: Some(source),
                sent: 0,
                control,
            },
            |mut body| async move { body.next_chunk().await.map(|chunk| (chunk, body)) },
        )))
    }
}

enum ChunkSource {
    Bytes { bytes: Bytes, offset: usize },
    File(tokio::fs::File),
}

struct ReportedBody {
    /// Taken on the last chunk, on end of file, and on a read failure, so the
    /// stream reports its end exactly once however it finished.
    source: Option<ChunkSource>,
    sent: u64,
    control: UploadControl,
}

impl ReportedBody {
    async fn next_chunk(&mut self) -> Option<std::io::Result<Bytes>> {
        self.control.wait_until_resumed().await;
        let chunk = match self.source.take()? {
            ChunkSource::Bytes { bytes, offset } if offset == bytes.len() => return None,
            ChunkSource::Bytes { bytes, offset } => {
                let end = offset.saturating_add(BODY_CHUNK).min(bytes.len());
                self.source = Some(ChunkSource::Bytes {
                    bytes: bytes.clone(),
                    offset: end,
                });
                Ok(bytes.slice(offset..end))
            }
            ChunkSource::File(mut file) => {
                let mut buffer = vec![0u8; BODY_CHUNK];
                match file.read(&mut buffer).await {
                    Ok(0) => return None,
                    Ok(read) => {
                        buffer.truncate(read);
                        self.source = Some(ChunkSource::File(file));
                        Ok(Bytes::from(buffer))
                    }
                    Err(error) => Err(error),
                }
            }
        };
        if let Ok(bytes) = &chunk {
            self.sent = self.sent.saturating_add(bytes.len() as u64);
            self.control.report(self.sent);
        }
        Some(chunk)
    }
}

impl GoogleCloudStorageXml {
    pub(super) fn for_endpoint(endpoint: Option<&str>) -> Result<Option<Self>, CloudHomeError> {
        if !endpoint.is_some_and(is_google_cloud_storage_endpoint) {
            return Ok(None);
        }
        let client = reqwest::Client::builder().build().map_err(|error| {
            CloudHomeError::configuration("build Google Cloud Storage XML client", error)
        })?;
        Ok(Some(Self { client }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn create_only(
        &self,
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        key: &str,
        source: GoogleUploadSource,
        size: u64,
        payload_hash: &str,
        now: DateTime<Utc>,
        control: UploadControl,
    ) -> Result<(), CloudHomeError> {
        let signed = SignedGooglePut::new(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            key,
            payload_hash,
            "0",
            now,
        )?;
        let response = self
            .client
            .put(signed.url)
            .header(reqwest::header::AUTHORIZATION, signed.authorization)
            .header(reqwest::header::HOST, signed.host)
            .header("x-goog-content-sha256", payload_hash)
            .header("x-goog-date", signed.timestamp)
            .header("x-goog-if-generation-match", "0")
            .header(reqwest::header::CONTENT_LENGTH, size)
            .body(source.into_body(control).await?)
            .send()
            .await
            .map_err(|error| {
                CloudHomeError::backend(
                    StorageBackendFailure::Transport,
                    format!("put {key}"),
                    error,
                )
            })?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        if status == reqwest::StatusCode::PRECONDITION_FAILED {
            return Err(CloudHomeError::AlreadyExists(key.to_string()));
        }
        let body = response.text().await.map_err(|error| {
            CloudHomeError::backend(
                StorageBackendFailure::Transport,
                format!("read failed Google Cloud Storage PUT response for {key}"),
                error,
            )
        })?;
        let error = GoogleXmlResponseError {
            status: status.as_u16(),
            code: xml_value(&body, "Code").map(str::to_string),
            message: xml_value(&body, "Message").map(str::to_string),
        };
        Err(CloudHomeError::backend(
            s3_backend_failure(error.code.as_deref(), Some(error.status)),
            format!("put {key}"),
            error,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn read_versioned(
        &self,
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        key: &str,
        now: DateTime<Utc>,
    ) -> Result<CloudVersionedObject, CloudHomeError> {
        let payload_hash = sha256_hex(b"");
        let signed = SignedGoogleGet::new(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            key,
            &payload_hash,
            now,
        )?;
        let response = self
            .client
            .get(signed.url)
            .header(reqwest::header::AUTHORIZATION, signed.authorization)
            .header(reqwest::header::HOST, signed.host)
            .header("x-goog-content-sha256", payload_hash)
            .header("x-goog-date", signed.timestamp)
            .send()
            .await
            .map_err(|error| {
                CloudHomeError::backend(
                    StorageBackendFailure::Transport,
                    format!("read versioned {key}"),
                    error,
                )
            })?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(CloudHomeError::NotFound(key.to_string()));
        }
        if !status.is_success() {
            return Err(google_response_error(response, "read versioned", key).await);
        }
        let version = response
            .headers()
            .get("x-goog-generation")
            .ok_or_else(|| {
                CloudHomeError::Transport(format!(
                    "read versioned {key}: Google Cloud Storage returned no generation"
                ))
            })?
            .to_str()
            .map_err(|error| {
                CloudHomeError::transport(
                    format!("read versioned {key}: invalid generation"),
                    error,
                )
            })?
            .to_string();
        let bytes = response.bytes().await.map_err(|error| {
            CloudHomeError::backend(
                StorageBackendFailure::Transport,
                format!("read versioned body {key}"),
                error,
            )
        })?;
        Ok(CloudVersionedObject {
            bytes: bytes.to_vec(),
            version: CloudObjectVersion::from_provider(version)?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn replace_if_generation(
        &self,
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        key: &str,
        expected: &CloudObjectVersion,
        data: Vec<u8>,
        now: DateTime<Utc>,
    ) -> Result<ConditionalWriteOutcome, CloudHomeError> {
        let payload_hash = sha256_hex(&data);
        let signed = SignedGooglePut::new(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            key,
            &payload_hash,
            expected.as_provider(),
            now,
        )?;
        let response = self
            .client
            .put(signed.url)
            .header(reqwest::header::AUTHORIZATION, signed.authorization)
            .header(reqwest::header::HOST, signed.host)
            .header("x-goog-content-sha256", payload_hash)
            .header("x-goog-date", signed.timestamp)
            .header("x-goog-if-generation-match", expected.as_provider())
            .header(reqwest::header::CONTENT_LENGTH, data.len())
            .body(data)
            .send()
            .await
            .map_err(|error| {
                CloudHomeError::backend(
                    StorageBackendFailure::Transport,
                    format!("replace versioned {key}"),
                    error,
                )
            })?;
        let status = response.status();
        if status == reqwest::StatusCode::PRECONDITION_FAILED {
            return Ok(ConditionalWriteOutcome::VersionChanged);
        }
        if !status.is_success() {
            return Err(google_response_error(response, "replace versioned", key).await);
        }
        let version = response
            .headers()
            .get("x-goog-generation")
            .ok_or_else(|| {
                CloudHomeError::Transport(format!(
                    "replace versioned {key}: Google Cloud Storage returned no generation"
                ))
            })?
            .to_str()
            .map_err(|error| {
                CloudHomeError::transport(
                    format!("replace versioned {key}: invalid generation"),
                    error,
                )
            })?;
        Ok(ConditionalWriteOutcome::Replaced(
            CloudObjectVersion::from_provider(version.to_string())?,
        ))
    }
}

async fn google_response_error(
    response: reqwest::Response,
    operation: &str,
    key: &str,
) -> CloudHomeError {
    let status = response.status();
    let body = match response.text().await {
        Ok(body) => body,
        Err(error) => {
            return CloudHomeError::backend(
                StorageBackendFailure::Transport,
                format!("read failed Google Cloud Storage response for {operation} {key}"),
                error,
            )
        }
    };
    let error = GoogleXmlResponseError {
        status: status.as_u16(),
        code: xml_value(&body, "Code").map(str::to_string),
        message: xml_value(&body, "Message").map(str::to_string),
    };
    CloudHomeError::backend(
        s3_backend_failure(error.code.as_deref(), Some(error.status)),
        format!("{operation} {key}"),
        error,
    )
}

fn is_google_cloud_storage_endpoint(endpoint: &str) -> bool {
    let authority = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, authority)| authority);
    let host = authority
        .split('/')
        .next()
        .unwrap_or(authority)
        .split(':')
        .next()
        .unwrap_or(authority);
    host == "storage.googleapis.com"
        || host == "storage.mtls.googleapis.com"
        || host
            .strip_suffix("-storage.googleapis.com")
            .is_some_and(|location| !location.is_empty())
        || host
            .strip_prefix("storage.")
            .and_then(|suffix| suffix.strip_suffix(".rep.googleapis.com"))
            .is_some_and(|location| !location.is_empty())
}

struct SignedGooglePut {
    url: url::Url,
    host: String,
    timestamp: String,
    authorization: String,
}

impl SignedGooglePut {
    #[allow(clippy::too_many_arguments)]
    fn new(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        key: &str,
        payload_hash: &str,
        generation: &str,
        now: DateTime<Utc>,
    ) -> Result<Self, CloudHomeError> {
        let (url, host) = google_object_url(endpoint, bucket, key)?;
        let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let signed_headers = "host;x-goog-content-sha256;x-goog-date;x-goog-if-generation-match";
        let canonical_headers = format!(
            "host:{host}\nx-goog-content-sha256:{payload_hash}\nx-goog-date:{timestamp}\nx-goog-if-generation-match:{generation}\n"
        );
        let canonical_request = format!(
            "PUT\n{}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
            url.path()
        );
        let scope = format!("{date}/{region}/storage/goog4_request");
        let string_to_sign = format!(
            "GOOG4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let date_key = hmac_sha256(format!("GOOG4{secret_key}").as_bytes(), date.as_bytes());
        let region_key = hmac_sha256(&date_key, region.as_bytes());
        let service_key = hmac_sha256(&region_key, b"storage");
        let signing_key = hmac_sha256(&service_key, b"goog4_request");
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "GOOG4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders={signed_headers},Signature={signature}"
        );
        Ok(Self {
            url,
            host,
            timestamp,
            authorization,
        })
    }
}

struct SignedGoogleGet {
    url: url::Url,
    host: String,
    timestamp: String,
    authorization: String,
}

impl SignedGoogleGet {
    #[allow(clippy::too_many_arguments)]
    fn new(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        key: &str,
        payload_hash: &str,
        now: DateTime<Utc>,
    ) -> Result<Self, CloudHomeError> {
        let (url, host) = google_object_url(endpoint, bucket, key)?;
        let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let signed_headers = "host;x-goog-content-sha256;x-goog-date";
        let canonical_headers =
            format!("host:{host}\nx-goog-content-sha256:{payload_hash}\nx-goog-date:{timestamp}\n");
        let canonical_request = format!(
            "GET\n{}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
            url.path()
        );
        let scope = format!("{date}/{region}/storage/goog4_request");
        let string_to_sign = format!(
            "GOOG4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let date_key = hmac_sha256(format!("GOOG4{secret_key}").as_bytes(), date.as_bytes());
        let region_key = hmac_sha256(&date_key, region.as_bytes());
        let service_key = hmac_sha256(&region_key, b"storage");
        let signing_key = hmac_sha256(&service_key, b"goog4_request");
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "GOOG4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders={signed_headers},Signature={signature}"
        );
        Ok(Self {
            url,
            host,
            timestamp,
            authorization,
        })
    }
}

fn google_object_url(
    endpoint: &str,
    bucket: &str,
    key: &str,
) -> Result<(url::Url, String), CloudHomeError> {
    let mut url = url::Url::parse(endpoint).map_err(|error| {
        CloudHomeError::configuration("parse Google Cloud Storage endpoint", error)
    })?;
    {
        let mut segments = url.path_segments_mut().map_err(|()| {
            CloudHomeError::Configuration(
                "Google Cloud Storage endpoint cannot carry object paths".to_string(),
            )
        })?;
        segments.pop_if_empty().push(bucket);
        for segment in key.split('/') {
            segments.push(segment);
        }
    }
    let host_name = url.host_str().ok_or_else(|| {
        CloudHomeError::Configuration("Google Cloud Storage endpoint has no host".to_string())
    })?;
    let host = match url.port() {
        Some(port) => format!("{host_name}:{port}"),
        None => host_name.to_string(),
    };
    Ok((url, host))
}

fn hmac_sha256(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn xml_value<'body>(body: &'body str, name: &str) -> Option<&'body str> {
    let start_tag = format!("<{name}>");
    let end_tag = format!("</{name}>");
    let value = body.split_once(&start_tag)?.1.split_once(&end_tag)?.0;
    Some(value)
}

#[derive(Debug, thiserror::Error)]
#[error(
    "Google Cloud Storage XML error {status}: {code}: {message}",
    code = code.as_deref().unwrap_or("unknown code"),
    message = message.as_deref().unwrap_or("no message")
)]
struct GoogleXmlResponseError {
    status: u16,
    code: Option<String>,
    message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{HeaderMap, Response, StatusCode, Uri};
    use axum::Router;
    use bytes::Bytes;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct CreateOnlyState {
        stored: Arc<Mutex<Option<Vec<u8>>>>,
    }

    async fn create_only_endpoint(
        State(state): State<CreateOnlyState>,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response<Body> {
        assert_eq!(uri.path(), "/travel-maps/maps/new%20york.png");
        assert_eq!(headers["x-goog-if-generation-match"], "0");
        assert_eq!(headers["x-goog-content-sha256"], sha256_hex(body.as_ref()));
        assert!(headers[reqwest::header::AUTHORIZATION]
            .to_str()
            .expect("authorization is text")
            .starts_with("GOOG4-HMAC-SHA256 Credential=GOOG-ACCESS-ID/"));

        let mut stored = state.stored.lock().expect("lock fake object");
        if stored.is_some() {
            return Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .body(Body::from(
                    "<Error><Code>PreconditionFailed</Code><Message>object exists</Message></Error>",
                ))
                .expect("build occupied response");
        }
        *stored = Some(body.to_vec());
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .expect("build success response")
    }

    #[derive(Clone)]
    struct ConditionalState {
        bytes: Arc<Mutex<Vec<u8>>>,
        generation: Arc<Mutex<String>>,
    }

    async fn conditional_endpoint(
        State(state): State<ConditionalState>,
        uri: Uri,
        headers: HeaderMap,
        method: axum::http::Method,
        body: Bytes,
    ) -> Response<Body> {
        assert_eq!(uri.path(), "/publication-store/protocol/current.json");
        if method == axum::http::Method::GET {
            let generation = state.generation.lock().expect("lock generation").clone();
            let bytes = state.bytes.lock().expect("lock bytes").clone();
            return Response::builder()
                .status(StatusCode::OK)
                .header("x-goog-generation", generation)
                .body(Body::from(bytes))
                .expect("build generation read response");
        }
        assert_eq!(method, axum::http::Method::PUT);
        let supplied = headers
            .get("x-goog-if-generation-match")
            .and_then(|value| value.to_str().ok());
        let mut generation = state.generation.lock().expect("lock generation");
        if supplied != Some(generation.as_str()) {
            return Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .body(Body::from(
                    "<Error><Code>PreconditionFailed</Code><Message>generation changed</Message></Error>",
                ))
                .expect("build generation conflict response");
        }
        *state.bytes.lock().expect("lock bytes") = body.to_vec();
        *generation = "22".to_string();
        Response::builder()
            .status(StatusCode::OK)
            .header("x-goog-generation", generation.as_str())
            .body(Body::empty())
            .expect("build generation replacement response")
    }

    #[test]
    fn recognizes_only_google_cloud_storage_xml_endpoints() {
        assert!(is_google_cloud_storage_endpoint(
            "https://storage.googleapis.com"
        ));
        assert!(is_google_cloud_storage_endpoint(
            "https://us-east1-storage.googleapis.com"
        ));
        assert!(is_google_cloud_storage_endpoint(
            "https://storage.us-east1.rep.googleapis.com"
        ));
        assert!(!is_google_cloud_storage_endpoint(
            "https://s3.us-east-1.amazonaws.com"
        ));
        assert!(!is_google_cloud_storage_endpoint(
            "https://storage.googleapis.com.example.test"
        ));
    }

    #[test]
    fn signed_put_uses_google_headers_and_credential_scope() {
        let now = DateTime::parse_from_rfc3339("2019-11-02T04:35:30Z")
            .expect("fixed timestamp")
            .with_timezone(&Utc);
        let signed = SignedGooglePut::new(
            "https://storage.googleapis.com",
            "travel-maps",
            "us-central1",
            "GOOG-ACCESS-ID",
            "secret",
            "maps/new york.png",
            &sha256_hex(b"map"),
            "0",
            now,
        )
        .expect("sign PUT");

        assert_eq!(
            signed.url.as_str(),
            "https://storage.googleapis.com/travel-maps/maps/new%20york.png"
        );
        assert_eq!(signed.timestamp, "20191102T043530Z");
        assert!(signed.authorization.starts_with(
            "GOOG4-HMAC-SHA256 Credential=GOOG-ACCESS-ID/20191102/us-central1/storage/goog4_request,"
        ));
        assert!(signed
            .authorization
            .contains("SignedHeaders=host;x-goog-content-sha256;x-goog-date;x-goog-if-generation-match,Signature="));
    }

    #[tokio::test]
    async fn conditional_replacement_uses_generation_from_the_exact_read() {
        let state = ConditionalState {
            bytes: Arc::new(Mutex::new(b"first".to_vec())),
            generation: Arc::new(Mutex::new("21".to_string())),
        };
        let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
            Router::new()
                .fallback(conditional_endpoint)
                .with_state(state),
        )
        .await;
        let storage = GoogleCloudStorageXml {
            client: reqwest::Client::new(),
        };
        let now = DateTime::parse_from_rfc3339("2019-11-02T04:35:30Z")
            .expect("fixed timestamp")
            .with_timezone(&Utc);
        let observed = storage
            .read_versioned(
                &endpoint,
                "publication-store",
                "us-central1",
                "GOOG-ACCESS-ID",
                "secret",
                "protocol/current.json",
                now,
            )
            .await
            .expect("read generation");
        assert_eq!(observed.bytes, b"first");
        assert_eq!(observed.version.as_provider(), "21");

        let replaced = storage
            .replace_if_generation(
                &endpoint,
                "publication-store",
                "us-central1",
                "GOOG-ACCESS-ID",
                "secret",
                "protocol/current.json",
                &observed.version,
                b"second".to_vec(),
                now,
            )
            .await
            .expect("replace matching generation");
        assert!(matches!(replaced, ConditionalWriteOutcome::Replaced(_)));
        let stale = storage
            .replace_if_generation(
                &endpoint,
                "publication-store",
                "us-central1",
                "GOOG-ACCESS-ID",
                "secret",
                "protocol/current.json",
                &observed.version,
                b"third".to_vec(),
                now,
            )
            .await
            .expect("settle stale generation");
        assert_eq!(stale, ConditionalWriteOutcome::VersionChanged);
        shutdown
            .send(())
            .expect("shut down Google Cloud Storage endpoint");
    }

    /// A file body large enough to span several chunks reports as it streams,
    /// not once at the end: this is the only signal Google Cloud Storage's
    /// single-PUT create-only path can give, and without it a several-hundred-
    /// megabyte blob showed zero bytes uploaded for its whole transfer.
    #[tokio::test]
    async fn a_streamed_file_body_reports_bytes_as_the_request_consumes_them() {
        let stored = Arc::new(Mutex::new(None));
        let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
            Router::new()
                .fallback(create_only_endpoint)
                .with_state(CreateOnlyState {
                    stored: stored.clone(),
                }),
        )
        .await;
        let source = tempfile::NamedTempFile::new().expect("upload source file");
        let bytes: Vec<u8> = (0..BODY_CHUNK * 3 + 17).map(|index| index as u8).collect();
        std::fs::write(source.path(), &bytes).expect("write upload source");
        let reported = Arc::new(Mutex::new(Vec::<u64>::new()));
        let recorder = Arc::clone(&reported);
        let progress: crate::cloud::UploadProgress =
            Arc::new(move |sent| recorder.lock().expect("lock reports").push(sent));

        GoogleCloudStorageXml {
            client: reqwest::Client::new(),
        }
        .create_only(
            &endpoint,
            "travel-maps",
            "us-central1",
            "GOOG-ACCESS-ID",
            "secret",
            "maps/new york.png",
            GoogleUploadSource::File(source.path().to_path_buf()),
            bytes.len() as u64,
            &sha256_hex(&bytes),
            DateTime::parse_from_rfc3339("2019-11-02T04:35:30Z")
                .expect("fixed timestamp")
                .with_timezone(&Utc),
            UploadControl::running(progress),
        )
        .await
        .expect("create absent object");

        let reports = reported.lock().expect("lock reports").clone();
        assert!(
            reports.len() > 1,
            "a multi-chunk body reports more than once, got {reports:?}"
        );
        assert!(
            reports.windows(2).all(|pair| pair[0] < pair[1]),
            "reports are cumulative and advancing, got {reports:?}"
        );
        assert_eq!(reports.last().copied(), Some(bytes.len() as u64));
        assert_eq!(
            stored.lock().expect("lock stored object").as_deref(),
            Some(bytes.as_slice()),
            "the streamed body arrives whole and unmodified"
        );
        shutdown.send(()).expect("stop fake Google endpoint");
    }

    #[tokio::test]
    async fn google_xml_request_body_stops_and_resumes_at_the_next_chunk() {
        let bytes = Bytes::from(vec![7; BODY_CHUNK * 2 + 17]);
        let (pause_sender, pause) = tokio::sync::watch::channel(false);
        let mut body = ReportedBody {
            source: Some(ChunkSource::Bytes {
                bytes: bytes.clone(),
                offset: 0,
            }),
            sent: 0,
            control: UploadControl::pausable(crate::cloud::no_progress(), pause),
        };

        let first = body
            .next_chunk()
            .await
            .expect("first request chunk")
            .expect("read first request chunk");
        assert_eq!(first.len(), BODY_CHUNK);

        pause_sender.send_replace(true);
        let second = {
            let next = body.next_chunk();
            tokio::pin!(next);
            assert!(
                matches!(futures_util::poll!(&mut next), std::task::Poll::Pending),
                "paused request body yielded another chunk",
            );

            pause_sender.send_replace(false);
            next.as_mut()
                .await
                .expect("second request chunk")
                .expect("read second request chunk")
        };
        assert_eq!(second.len(), BODY_CHUNK);

        let last = body
            .next_chunk()
            .await
            .expect("last request chunk")
            .expect("read last request chunk");
        assert_eq!(last.len(), 17);
        assert!(body.next_chunk().await.is_none());
    }

    #[tokio::test]
    async fn create_only_sends_the_signed_payload_hash_and_rejects_replacement() {
        let stored = Arc::new(Mutex::new(None));
        let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
            Router::new()
                .fallback(create_only_endpoint)
                .with_state(CreateOnlyState {
                    stored: stored.clone(),
                }),
        )
        .await;
        let client = GoogleCloudStorageXml {
            client: reqwest::Client::new(),
        };
        let now = DateTime::parse_from_rfc3339("2019-11-02T04:35:30Z")
            .expect("fixed timestamp")
            .with_timezone(&Utc);
        let bytes = b"map".to_vec();
        let hash = sha256_hex(&bytes);

        client
            .create_only(
                &endpoint,
                "travel-maps",
                "us-central1",
                "GOOG-ACCESS-ID",
                "secret",
                "maps/new york.png",
                GoogleUploadSource::Bytes(bytes.clone()),
                bytes.len() as u64,
                &hash,
                now,
                UploadControl::running(crate::cloud::no_progress()),
            )
            .await
            .expect("create absent object");
        let occupied = client
            .create_only(
                &endpoint,
                "travel-maps",
                "us-central1",
                "GOOG-ACCESS-ID",
                "secret",
                "maps/new york.png",
                GoogleUploadSource::Bytes(bytes.clone()),
                bytes.len() as u64,
                &hash,
                now,
                UploadControl::running(crate::cloud::no_progress()),
            )
            .await
            .expect_err("create-only request cannot replace the object");

        assert!(
            matches!(occupied, CloudHomeError::AlreadyExists(key) if key == "maps/new york.png")
        );
        assert_eq!(
            stored.lock().expect("lock stored object").as_deref(),
            Some(bytes.as_slice())
        );
        shutdown.send(()).expect("stop fake Google endpoint");
    }
}
