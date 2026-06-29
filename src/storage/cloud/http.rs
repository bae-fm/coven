//! Shared HTTP plumbing for the OAuth REST cloud backends (Google Drive,
//! Dropbox, OneDrive).
//!
//! The send-and-check, body-read, and range-header logic was copied ~30× across
//! those three backends (the `<body read failed>` literal alone appeared as many
//! times). These helpers are that logic in one place.

use reqwest::Response;

/// Read a response body to text, never failing: a dropped connection mid-body
/// folds into a placeholder so the status still surfaces. This is the single
/// definition of the `<body read failed>` fallback that was copied across every
/// backend's error path.
pub async fn body_text(resp: Response) -> String {
    resp.text()
        .await
        .unwrap_or_else(|e| format!("<body read failed: {e}>"))
}

/// The HTTP `Range` header value for a `read_range`. `start` is inclusive and
/// `end` is exclusive (the `CloudHome` contract); the header is inclusive on both
/// ends, so the upper bound is `end - 1`. The single definition of the
/// `bytes={start}-{end}` string the backends each formatted by hand.
pub fn range_header(start: u64, end: u64) -> String {
    format!("bytes={start}-{}", end.saturating_sub(1))
}

/// The `Content-Range` header value for one resumable-upload part:
/// `bytes {start}-{end}/{total}` (both bounds inclusive). The single definition of
/// the string Google Drive and OneDrive each formatted by hand in their upload
/// loops.
pub fn range_content_header(start: u64, end: u64, total: u64) -> String {
    format!("bytes {start}-{end}/{total}")
}
