//! Shared HTTP plumbing for the OAuth REST cloud backends (Google Drive,
//! Dropbox, OneDrive).
//!
//! The send-and-check, body-read, and range-header logic was copied ~30× across
//! those three backends (the `<body read failed>` literal alone appeared as many
//! times). These helpers are that logic in one place.

use reqwest::{Response, StatusCode};
use serde::de::DeserializeOwned;

use super::CloudHomeError;

/// How a provider signals "absent key" on a non-success response.
pub enum NotFound<'a> {
    /// HTTP 404 (Google Drive, OneDrive, S3).
    Status,
    /// A specific status whose body contains a marker — Dropbox returns 409 with
    /// `not_found` in the body.
    BodyContains { status: StatusCode, needle: &'a str },
}

/// Send-and-check: return the response on 2xx, else a `CloudHomeError`. A response
/// matching `not_found` becomes [`CloudHomeError::NotFound`]; any other non-2xx
/// becomes a [`CloudHomeError::Storage`] carrying the status and body. `ctx` names
/// the operation (e.g. `"read heads/dev1.json"`). The one definition of the
/// `let status = resp.status(); if !status.is_success() { … }` block that was
/// copied ~30× across the OAuth backends.
pub async fn ensure_ok(
    resp: Response,
    ctx: &str,
    not_found: NotFound<'_>,
) -> Result<Response, CloudHomeError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    match not_found {
        NotFound::Status if status == StatusCode::NOT_FOUND => {
            return Err(CloudHomeError::NotFound(ctx.to_string()));
        }
        NotFound::BodyContains { status: nf, needle } if status == nf => {
            let body = body_text(resp).await;
            return if body.contains(needle) {
                Err(CloudHomeError::NotFound(ctx.to_string()))
            } else {
                Err(CloudHomeError::Storage(format!(
                    "{ctx} (HTTP {status}): {body}"
                )))
            };
        }
        _ => {}
    }
    let body = body_text(resp).await;
    Err(CloudHomeError::Storage(format!(
        "{ctx} (HTTP {status}): {body}"
    )))
}

/// Read a successful response's body into bytes. `ctx` names what is being read.
pub async fn ok_bytes(resp: Response, ctx: &str) -> Result<Vec<u8>, CloudHomeError> {
    resp.bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| CloudHomeError::Storage(format!("{ctx}: {e}")))
}

/// Read a successful response's body and parse it as JSON. `ctx` names what is
/// being parsed.
pub async fn ok_json<T: DeserializeOwned>(resp: Response, ctx: &str) -> Result<T, CloudHomeError> {
    let body = resp
        .text()
        .await
        .map_err(|e| CloudHomeError::Storage(format!("{ctx}: read body: {e}")))?;
    serde_json::from_str(&body).map_err(|e| CloudHomeError::Storage(format!("{ctx}: parse: {e}")))
}

/// Parse an error-response body as JSON and pull a provider-specific reason out of
/// it with `extract`. Non-JSON bodies are a common skip (proxy 500s, captive
/// portals) — logged at debug so the bail-out is visible without spamming a normal
/// session, then `None`. This is the one definition of the skip-non-JSON +
/// debug-log + extract structure each backend's `parse_*_error` repeated; the
/// caller supplies only where its provider keeps the reason.
pub fn error_reason(
    body: &str,
    extract: impl Fn(&serde_json::Value) -> Option<String>,
) -> Option<String> {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(v) => extract(&v),
        Err(e) => {
            tracing::debug!("non-JSON error body, skipping reason extraction: {e}");
            None
        }
    }
}

/// Read a response body to text, never failing: a dropped connection mid-body
/// folds into a placeholder so the status still surfaces. This is the single
/// definition of the `<body read failed>` fallback that was copied across every
/// backend's error path.
pub async fn body_text(resp: Response) -> String {
    resp.text()
        .await
        .unwrap_or_else(|e| format!("<body read failed: {e}>"))
}

/// Resolve a sent existence-probe response to a bool: 2xx → present, the
/// provider's `not_found` signal → absent, any other non-2xx → error. The one
/// definition of the `match ensure_ok(..) { Ok => true, NotFound => false, Err =>
/// err }` three-arm match each `exists` repeated. The caller still builds and
/// sends its own (GET vs metadata POST) request.
pub async fn exists_from_response(
    resp: Response,
    ctx: &str,
    not_found: NotFound<'_>,
) -> Result<bool, CloudHomeError> {
    match ensure_ok(resp, ctx, not_found).await {
        Ok(_) => Ok(true),
        Err(CloudHomeError::NotFound(_)) => Ok(false),
        Err(e) => Err(e),
    }
}

/// The `Content-Range` header value for one resumable-upload part:
/// `bytes {start}-{end}/{total}` (both bounds inclusive). The single definition of
/// the string Google Drive and OneDrive each formatted by hand in their upload
/// loops.
pub fn range_content_header(start: u64, end: u64, total: u64) -> String {
    format!("bytes {start}-{end}/{total}")
}
