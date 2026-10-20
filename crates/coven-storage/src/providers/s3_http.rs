//! The S3 SDK sends through a reqwest client from `http::client`, the builder
//! Dropbox, OneDrive, key minting and admin sign-in use. So S3 checks
//! server certificates with the platform's verifier, as every other provider
//! does: what the OS trusts on Android, iOS, macOS, Windows and Linux. The
//! SDK's own client instead read a root list from the OS, which Android does
//! not expose to native code, so it trusted nothing there.

use crate::providers::http;
use crate::{CloudProvider, StorageError};
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_runtime_api::client::{
    http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    },
    orchestrator::{HttpRequest, HttpResponse},
    result::ConnectorError,
    runtime_components::RuntimeComponents,
};

/// The SDK's HTTP client, sending every request through one reqwest client.
#[derive(Debug)]
pub(super) struct S3HttpClient(SharedHttpConnector);

impl S3HttpClient {
    /// Build the client. A TLS setup failure, such as a platform verifier that
    /// cannot start, is `InvalidConfiguration`, never a panic.
    pub(super) fn new() -> Result<Self, StorageError> {
        let client = http::client().map_err(|error| http::transport(CloudProvider::S3, error))?;
        Ok(Self(SharedHttpConnector::new(S3HttpConnector(client))))
    }
}

impl HttpClient for S3HttpClient {
    /// The S3 configuration turns the SDK's timeouts off, so `settings` carries
    /// none: the reqwest client's own request limit applies, as for every
    /// provider.
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        self.0.clone()
    }
}

#[derive(Debug)]
struct S3HttpConnector(reqwest::Client);

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
        let request = match request
            .try_into_http1x()
            .map_err(|error| ConnectorError::user(error.into()))
            .and_then(|request| {
                reqwest::Request::try_from(request.map(reqwest::Body::wrap))
                    .map_err(connector_error)
            }) {
            Ok(request) => request,
            Err(error) => return HttpConnectorFuture::ready(Err(error)),
        };
        let client = self.0.clone();
        HttpConnectorFuture::new(async move {
            let response = client.execute(request).await.map_err(connector_error)?;
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
fn connector_error(error: reqwest::Error) -> ConnectorError {
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
