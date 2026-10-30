//! The S3 SDK sends through coven's `HttpClient`, as Dropbox and OneDrive do. So S3 checks server certificates with the
//! platform's verifier, as every other provider does: what the OS trusts on
//! Android, iOS, macOS, Windows and Linux. The SDK's own client instead read a
//! root list found by `openssl-probe`, which on Android looks only at Termux's
//! path and not at `/system/etc/security/cacerts`, so it trusted nothing
//! there. S3 also shares the stall window (§6.7).

use crate::providers::http_client::{HttpClient, SendError};
use crate::ByteProgress;
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_runtime_api::client::{
    http::{
        HttpClient as SdkHttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings,
        SharedHttpConnector,
    },
    orchestrator::{HttpRequest, HttpResponse},
    result::ConnectorError,
    runtime_components::RuntimeComponents,
};

/// The SDK's HTTP client, sending every request through one `HttpClient`.
#[derive(Debug)]
pub(super) struct S3HttpClient(SharedHttpConnector);

impl S3HttpClient {
    /// Send through `client`.
    pub(super) fn new(client: HttpClient) -> Self {
        Self(SharedHttpConnector::new(S3HttpConnector(client)))
    }
}

impl SdkHttpClient for S3HttpClient {
    /// The S3 configuration turns the SDK's timeouts and stalled-stream
    /// protection off, so `settings` carries none: the stall window applies,
    /// as for every provider.
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        self.0.clone()
    }
}

#[derive(Debug)]
struct S3HttpConnector(HttpClient);

impl HttpConnector for S3HttpConnector {
    fn call(&self, mut request: HttpRequest) -> HttpConnectorFuture {
        // HTTP/1.1 sends no `Content-Length` for an empty body, and GCS refuses
        // a POST or PUT without one (HTTP 411): CreateMultipartUpload and
        // CopyObject have empty bodies. RFC 9110 §8.6 asks for the length
        // wherever the method gives a body meaning.
        if matches!(request.method(), "POST" | "PUT" | "PATCH")
            && !request.headers().contains_key("content-length")
        {
            if let Some(length) = request.body().content_length() {
                request
                    .headers_mut()
                    .insert("content-length", length.to_string());
            }
        }
        // An operation that sends an object's bytes names who hears them go
        // (`S3Storage::put`, `upload_part`); reqwest keeps no extension.
        let mut sent = ByteProgress::unread();
        let request = match request
            .try_into_http1x()
            .map_err(|error| ConnectorError::user(error.into()))
            .and_then(|request| {
                if let Some(progress) = request.extensions().get::<ByteProgress>() {
                    sent = progress.clone();
                }
                reqwest::Request::try_from(request.map(reqwest::Body::wrap))
                    .map_err(connector_error)
            }) {
            Ok(request) => request,
            Err(error) => return HttpConnectorFuture::ready(Err(error)),
        };
        let client = self.0.clone();
        HttpConnectorFuture::new(async move {
            let response = client
                .execute(request, &sent)
                .await
                .map_err(|error| match error {
                    SendError::Http(error) => connector_error(error),
                    SendError::Stalled(stalled) => ConnectorError::timeout(stalled.into()),
                })?;
            let response =
                ::http::Response::<reqwest::Body>::from(response).map(SdkBody::from_body_1_x);
            HttpResponse::try_from(response)
                .map_err(|error| ConnectorError::other(error.into(), None))
        })
    }
}

/// Classify a reqwest failure for the SDK's retry decision. A failure after
/// the request was built, connecting or exchanging bytes, is transient I/O,
/// as the SDK's own client treats a closed or interrupted connection.
/// The URL, signed query and all, is cut to its scheme and host first.
fn connector_error(error: reqwest::Error) -> ConnectorError {
    let error = crate::providers::error_text::origin_only(error);
    if error.is_timeout() {
        ConnectorError::timeout(error.into())
    } else if error.is_builder() {
        ConnectorError::user(error.into())
    } else if error.is_connect() || error.is_request() || error.is_body() {
        ConnectorError::io(error.into())
    } else {
        ConnectorError::other(error.into(), None)
    }
}

#[cfg(test)]
#[path = "s3_http_tests.rs"]
mod tests;
