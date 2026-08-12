//! The shared `CloudHome` read surface for the OAuth REST backends.
//!
//! Google Drive, Dropbox, and OneDrive each re-implemented `read`, `read_range`,
//! `list` (pagination), `delete`, and `exists` in the same shape — differing only
//! in the endpoints, the request verb (Dropbox POSTs `/files/download`; the others
//! GET), the pagination field names, and the not-found rule. [`OAuthRestHome`]
//! supplies only those differences; the five methods are implemented once here
//! over it, and each backend's `CloudHome` impl forwards to these.

use async_trait::async_trait;
use futures_util::TryStreamExt;
use std::collections::HashSet;
use std::path::Path;

use super::http::{body_text, ensure_ok, ok_bytes, NotFound};
use super::s3_common::is_range_success;
use super::{CloudFileReadError, CloudHomeError};

/// One page of a listing: the keys it yielded (already decoded and prefix-filtered)
/// and the cursor to fetch the next page, present only when more pages remain.
pub(crate) struct ListPage {
    pub keys: Vec<String>,
    pub next: Option<String>,
}

pub(crate) struct PageTokenTracker {
    scan: &'static str,
    seen: HashSet<String>,
}

impl PageTokenTracker {
    pub(crate) fn new(scan: &'static str) -> Self {
        Self {
            scan,
            seen: HashSet::new(),
        }
    }

    pub(crate) fn record(&mut self, token: &str) -> Result<String, CloudHomeError> {
        if token.is_empty() {
            return Err(CloudHomeError::Transport(format!(
                "{} returned an empty page token",
                self.scan
            )));
        }
        if !self.seen.insert(token.to_string()) {
            return Err(CloudHomeError::Transport(format!(
                "{} returned a repeated page token",
                self.scan
            )));
        }
        Ok(token.to_string())
    }
}

/// The provider-specific differences the shared OAuth read methods need. Each
/// `send_*` issues its request through the [`OAuthSession`] (so token refresh and
/// the 401 retry happen in one place) and returns the raw response for the shared
/// status handling.
#[async_trait]
pub(crate) trait OAuthRestHome: Send + Sync {
    /// How this provider signals an absent key (HTTP 404, or Dropbox's 409 +
    /// `not_found` body).
    fn not_found(&self) -> NotFound;

    /// Download `key`, optionally a byte range. A provider whose download needs a
    /// prior lookup (Google Drive resolves a flat name to a file id) does it here
    /// and returns [`CloudHomeError::NotFound`] when the key is absent.
    async fn send_read(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<reqwest::Response, CloudHomeError>;

    async fn send_delete(&self, key: &str) -> Result<reqwest::Response, CloudHomeError>;

    /// One listing page for `cursor` (`None` = the first page). `prefix` lets a
    /// provider that filters server-side (Google Drive's `name contains`) build the
    /// query; providers that list everything and filter client-side ignore it.
    async fn send_list_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
    ) -> Result<reqwest::Response, CloudHomeError>;

    /// Parse a listing page body into its keys (decoded and filtered to `prefix`)
    /// and the next cursor.
    fn parse_list_page(&self, body: &str, prefix: &str) -> Result<ListPage, CloudHomeError>;
}

/// Read the full contents of `key`.
pub(crate) async fn rest_read<T: OAuthRestHome + ?Sized>(
    home: &T,
    key: &str,
) -> Result<Vec<u8>, CloudHomeError> {
    let resp = home.send_read(key, None).await?;
    let resp = ensure_ok(resp, &format!("read {key}"), home.not_found()).await?;
    ok_bytes(resp, &format!("read body for {key}")).await
}

pub(crate) async fn response_to_file(
    response: reqwest::Response,
    destination: &Path,
    context: &str,
) -> Result<(), CloudFileReadError> {
    let context = context.to_string();
    let stream = response.bytes_stream().map_err(move |error| {
        CloudHomeError::transport(format!("{context}: stream response"), error)
    });
    super::write_cloud_object_stream(destination, Box::pin(stream)).await?;
    Ok(())
}

/// Read the `[start, end)` byte range of `key`.
pub(crate) async fn rest_read_range<T: OAuthRestHome + ?Sized>(
    home: &T,
    key: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, CloudHomeError> {
    let context = format!("read range {key}");
    let resp = home.send_read(key, Some((start, end))).await?;
    let resp = ensure_ok(resp, &context, home.not_found()).await?;
    validated_range_bytes(resp, &context, start, end).await
}

pub(crate) async fn validated_range_bytes(
    resp: reqwest::Response,
    context: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, CloudHomeError> {
    let expected_len = end
        .checked_sub(start)
        .filter(|length| *length > 0)
        .ok_or_else(|| {
            CloudHomeError::Transport(format!(
                "{context}: invalid half-open range [{start}, {end})"
            ))
        })?;
    // A ranged GET is honored only with 206 Partial Content. A 200 means the
    // provider ignored `Range` (or OneDrive's 302-to-download-URL redirect
    // dropped it) and returned the whole object from byte 0; serving those
    // offset-0 bytes as the requested range is silent corruption on a plaintext
    // home, so reject it here rather than downstream.
    let status = resp.status();
    if !is_range_success(status.as_u16()) {
        return Err(CloudHomeError::Transport(format!(
            "{context}: expected 206 Partial Content, got HTTP {status}"
        )));
    }
    let content_range = resp
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .ok_or_else(|| {
            CloudHomeError::Transport(format!("{context}: 206 response omitted Content-Range"))
        })?
        .to_str()
        .map_err(|error| {
            CloudHomeError::transport(format!("{context}: invalid Content-Range header"), error)
        })?;
    let expected_last = end - 1;
    let expected_prefix = format!("bytes {start}-{expected_last}/");
    let total = content_range
        .strip_prefix(&expected_prefix)
        .and_then(|total| total.parse::<u64>().ok())
        .filter(|total| *total >= end)
        .ok_or_else(|| {
            CloudHomeError::Transport(format!(
                "{context}: expected Content-Range {expected_prefix}<total >= {end}>, got {content_range:?}"
            ))
        })?;
    let bytes = ok_bytes(resp, &format!("{context} body")).await?;
    if bytes.len() as u64 != expected_len {
        return Err(CloudHomeError::Transport(format!(
            "{context}: Content-Range declared {expected_len} bytes of {total}, body contained {}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

/// Delete `key`; an absent key is success.
pub(crate) async fn rest_delete<T: OAuthRestHome + ?Sized>(
    home: &T,
    key: &str,
) -> Result<(), CloudHomeError> {
    let resp = home.send_delete(key).await?;
    match ensure_ok(resp, &format!("delete {key}"), home.not_found()).await {
        Ok(_) => Ok(()),
        Err(CloudHomeError::NotFound(_)) => {
            tracing::debug!("delete of {key} found nothing; treating as already-deleted");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// List every key under `prefix`, following pagination. A not-found on the first
/// page (Dropbox returns it when the folder doesn't exist yet) is an empty list.
/// A not-found on a continuation page is a truncated listing — an expired cursor
/// or a transient 404 mid-pagination — and propagates as an error, since the keys
/// collected so far are not the complete result the caller would read them as.
pub(crate) async fn rest_list<T: OAuthRestHome + ?Sized>(
    home: &T,
    prefix: &str,
) -> Result<Vec<String>, CloudHomeError> {
    let mut keys = Vec::new();
    let mut cursor: Option<String> = None;
    let mut page_tokens = PageTokenTracker::new("OAuth REST listing");
    loop {
        let resp = home.send_list_page(prefix, cursor.as_deref()).await?;
        let resp = match ensure_ok(resp, &format!("list {prefix}"), home.not_found()).await {
            Ok(resp) => resp,
            // An absent listing root — only decidable on the first page — is an
            // empty result. On a continuation page the root existed, so a 404 is a
            // truncated listing and must fail rather than return a partial result.
            Err(CloudHomeError::NotFound(_)) if cursor.is_none() => {
                tracing::debug!("list root for {prefix} absent; returning empty listing");
                return Ok(keys);
            }
            Err(e) => return Err(e),
        };
        let body = body_text(resp).await;
        let page = home.parse_list_page(&body, prefix)?;
        keys.extend(page.keys);
        match page.next {
            Some(token) => cursor = Some(page_tokens.record(&token)?),
            None => break,
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Response as HttpResponse;

    /// Drives `rest_list`'s pagination against scripted responses. The first page
    /// returns one key and a continuation cursor; every continuation page returns
    /// 404 — the not-found cases are what these tests exercise.
    struct MockListHome {
        first_page_status: u16,
    }

    fn response(status: u16, body: &str) -> reqwest::Response {
        reqwest::Response::from(
            HttpResponse::builder()
                .status(status)
                .body(body.to_string())
                .unwrap(),
        )
    }

    #[async_trait]
    impl OAuthRestHome for MockListHome {
        fn not_found(&self) -> NotFound {
            NotFound::Status
        }

        async fn send_read(
            &self,
            _key: &str,
            _range: Option<(u64, u64)>,
        ) -> Result<reqwest::Response, CloudHomeError> {
            unimplemented!("read is not exercised by the list tests")
        }

        async fn send_delete(&self, _key: &str) -> Result<reqwest::Response, CloudHomeError> {
            unimplemented!("delete is not exercised by the list tests")
        }

        async fn send_list_page(
            &self,
            _prefix: &str,
            cursor: Option<&str>,
        ) -> Result<reqwest::Response, CloudHomeError> {
            match cursor {
                None => Ok(response(self.first_page_status, "PAGE1")),
                Some(_) => Ok(response(404, "")),
            }
        }

        fn parse_list_page(&self, body: &str, _prefix: &str) -> Result<ListPage, CloudHomeError> {
            assert_eq!(body, "PAGE1", "only the first page's body is ever parsed");
            Ok(ListPage {
                keys: vec!["objects/dev1.json".to_string()],
                next: Some("page2".to_string()),
            })
        }
    }

    /// A 404 mid-pagination is a truncated listing, not a complete one: the call
    /// must error rather than return the keys collected before the failure.
    #[tokio::test]
    async fn list_errors_on_not_found_continuation_page() {
        let home = MockListHome {
            first_page_status: 200,
        };
        let err = rest_list(&home, "objects/")
            .await
            .expect_err("a 404 on a continuation page must fail the listing");
        assert!(matches!(err, CloudHomeError::NotFound(_)), "got {err:?}");
    }

    /// A 404 on the first page means the listing root doesn't exist yet, which is
    /// an empty listing, not an error.
    #[tokio::test]
    async fn list_returns_empty_on_not_found_first_page() {
        let home = MockListHome {
            first_page_status: 404,
        };
        let keys = rest_list(&home, "objects/")
            .await
            .expect("a 404 on the first page yields an empty listing");
        assert!(keys.is_empty(), "expected empty, got {keys:?}");
    }

    /// Answers a ranged read with a caller-chosen status and body, to pin how
    /// `rest_read_range` treats a full-body 200 versus a real 206.
    struct MockRangeHome {
        status: u16,
        body: &'static str,
        content_range: Option<&'static str>,
    }

    #[async_trait]
    impl OAuthRestHome for MockRangeHome {
        fn not_found(&self) -> NotFound {
            NotFound::Status
        }

        async fn send_read(
            &self,
            _key: &str,
            range: Option<(u64, u64)>,
        ) -> Result<reqwest::Response, CloudHomeError> {
            assert!(range.is_some(), "a range read must carry a range");
            let mut builder = HttpResponse::builder().status(self.status);
            if let Some(content_range) = self.content_range {
                builder = builder.header(reqwest::header::CONTENT_RANGE, content_range);
            }
            Ok(reqwest::Response::from(
                builder.body(self.body.to_string()).unwrap(),
            ))
        }

        async fn send_delete(&self, _key: &str) -> Result<reqwest::Response, CloudHomeError> {
            unimplemented!("delete is not exercised by the range tests")
        }

        async fn send_list_page(
            &self,
            _prefix: &str,
            _cursor: Option<&str>,
        ) -> Result<reqwest::Response, CloudHomeError> {
            unimplemented!("list is not exercised by the range tests")
        }

        fn parse_list_page(&self, _body: &str, _prefix: &str) -> Result<ListPage, CloudHomeError> {
            unimplemented!("list is not exercised by the range tests")
        }
    }

    /// A provider that ignores `Range` and returns 200 with the whole object
    /// serves offset-0 bytes where the caller asked for a mid-file range — silent
    /// corruption on a plaintext home. The range read must reject it, not return
    /// the wrong bytes.
    #[tokio::test]
    async fn read_range_rejects_full_body_200_response() {
        let home = MockRangeHome {
            status: 200,
            body: "WHOLE-OBJECT-FROM-BYTE-0",
            content_range: None,
        };
        let err = rest_read_range(&home, "storage/audio", 8, 16)
            .await
            .expect_err("a 200 full-body response to a range request must error");
        assert!(matches!(err, CloudHomeError::Transport(_)), "got {err:?}");
    }

    /// A real 206 Partial Content is the honored range and yields its body.
    #[tokio::test]
    async fn read_range_accepts_partial_content_206() {
        let home = MockRangeHome {
            status: 206,
            body: "RANGE",
            content_range: Some("bytes 8-12/20"),
        };
        let bytes = rest_read_range(&home, "storage/audio", 8, 13)
            .await
            .expect("a 206 Partial Content response is the honored range");
        assert_eq!(bytes, b"RANGE");
    }

    #[tokio::test]
    async fn read_range_rejects_missing_content_range() {
        let home = MockRangeHome {
            status: 206,
            body: "RANGE",
            content_range: None,
        };

        let error = rest_read_range(&home, "storage/audio", 8, 13)
            .await
            .expect_err("a range response must identify the returned byte interval");

        assert!(
            matches!(error, CloudHomeError::Transport(_)),
            "got {error:?}"
        );
    }

    #[tokio::test]
    async fn read_range_rejects_mismatched_content_range() {
        let home = MockRangeHome {
            status: 206,
            body: "RANGE",
            content_range: Some("bytes 0-4/20"),
        };

        let error = rest_read_range(&home, "storage/audio", 8, 13)
            .await
            .expect_err("a response for another range must be rejected");

        assert!(
            matches!(error, CloudHomeError::Transport(_)),
            "got {error:?}"
        );
    }

    #[tokio::test]
    async fn read_range_rejects_short_body() {
        let home = MockRangeHome {
            status: 206,
            body: "RANG",
            content_range: Some("bytes 8-12/20"),
        };

        let error = rest_read_range(&home, "storage/audio", 8, 13)
            .await
            .expect_err("a partial range body must be rejected");

        assert!(
            matches!(error, CloudHomeError::Transport(_)),
            "got {error:?}"
        );
    }
}
