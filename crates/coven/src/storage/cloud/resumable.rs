//! The resumable-session [`PartSink`] shared by Google Drive and OneDrive.
//!
//! Both upload a large file by PUTting fixed-size windows to a pre-authenticated
//! session URL with a `Content-Range: bytes {start}-{end}/{total}` header, where
//! every non-final window returns an "incomplete" status (Drive 308, OneDrive 202)
//! and the final window returns 200/201. The only differences are that accepted
//! intermediate status, the session URL, the part size, and how a failure body
//! maps to a friendly error — so one sink serves both.

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::StatusCode;

use super::http::range_content_header;
use super::CloudHomeError;

/// Maps a non-success upload response `(status, body)` to a `CloudHomeError` —
/// the per-provider quota/error classifier.
pub type ClassifyWrite = Box<dyn Fn(StatusCode, &str) -> CloudHomeError + Send + Sync>;

/// A [`PartSink`](super::PartSink) over a resumable upload session: each part is a
/// `Content-Range` PUT to the pre-authenticated `session_url` (no bearer token),
/// the last of which commits the file. `finish` is a no-op.
pub struct RangePutSink {
    client: reqwest::Client,
    session_url: String,
    /// The accepted non-final status (Drive `308`, OneDrive `202`).
    intermediate_status: u16,
    total: u64,
    part_size: usize,
    /// The blob key, for error messages.
    key: String,
    classify: ClassifyWrite,
}

impl RangePutSink {
    pub fn new(
        client: reqwest::Client,
        session_url: String,
        intermediate_status: u16,
        total: u64,
        part_size: usize,
        key: String,
        classify: ClassifyWrite,
    ) -> Self {
        RangePutSink {
            client,
            session_url,
            intermediate_status,
            total,
            part_size,
            key,
            classify,
        }
    }
}

#[async_trait]
impl super::PartSink for RangePutSink {
    fn part_size(&self) -> usize {
        self.part_size
    }

    async fn send_part(
        &mut self,
        part: Bytes,
        offset: u64,
        _is_last: bool,
    ) -> Result<(), CloudHomeError> {
        let end = offset + part.len() as u64 - 1;
        // The session URL is already a signed one-time URL, so the part PUTs carry
        // no bearer token.
        let resp = self
            .client
            .put(&self.session_url)
            .header("Content-Length", part.len())
            .header(
                "Content-Range",
                range_content_header(offset, end, self.total),
            )
            .body(part)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("upload chunk {}: {e}", self.key)))?;
        let status = resp.status();
        // Intermediate parts return the provider's "incomplete" status; the final
        // part returns 200/201. Anything else is a failure.
        if status.is_success() || status.as_u16() == self.intermediate_status {
            Ok(())
        } else {
            let body = super::http::body_text(resp).await;
            Err((self.classify)(status, &body))
        }
    }

    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        Ok(())
    }
}
