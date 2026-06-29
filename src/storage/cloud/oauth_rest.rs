//! The shared `CloudHome` read surface for the OAuth REST backends.
//!
//! Google Drive, Dropbox, and OneDrive each re-implemented `read`, `read_range`,
//! `list` (pagination), `delete`, and `exists` in the same shape — differing only
//! in the endpoints, the request verb (Dropbox POSTs `/files/download`; the others
//! GET), the pagination field names, and the not-found rule. [`OAuthRestHome`]
//! supplies only those differences; the five methods are implemented once here
//! over it, and each backend's `CloudHome` impl forwards to these.

use async_trait::async_trait;

use super::http::{body_text, ensure_ok, ok_bytes, NotFound};
use super::CloudHomeError;

/// One page of a listing: the keys it yielded (already decoded and prefix-filtered)
/// and the cursor to fetch the next page, present only when more pages remain.
pub struct ListPage {
    pub keys: Vec<String>,
    pub next: Option<String>,
}

/// The provider-specific differences the shared OAuth read methods need. Each
/// `send_*` issues its request through the [`OAuthSession`] (so token refresh and
/// the 401 retry happen in one place) and returns the raw response for the shared
/// status handling.
#[async_trait]
pub trait OAuthRestHome: Send + Sync {
    /// How this provider signals an absent key (HTTP 404, or Dropbox's 409 +
    /// `not_found` body).
    fn not_found(&self) -> NotFound<'_>;

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
pub async fn rest_read<T: OAuthRestHome + ?Sized>(
    home: &T,
    key: &str,
) -> Result<Vec<u8>, CloudHomeError> {
    let resp = home.send_read(key, None).await?;
    let resp = ensure_ok(resp, &format!("read {key}"), home.not_found()).await?;
    ok_bytes(resp, &format!("read body for {key}")).await
}

/// Read the `[start, end)` byte range of `key`.
pub async fn rest_read_range<T: OAuthRestHome + ?Sized>(
    home: &T,
    key: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, CloudHomeError> {
    let resp = home.send_read(key, Some((start, end))).await?;
    let resp = ensure_ok(resp, &format!("read range {key}"), home.not_found()).await?;
    ok_bytes(resp, &format!("read range body for {key}")).await
}

/// Delete `key`; an absent key is success.
pub async fn rest_delete<T: OAuthRestHome + ?Sized>(
    home: &T,
    key: &str,
) -> Result<(), CloudHomeError> {
    let resp = home.send_delete(key).await?;
    match ensure_ok(resp, &format!("delete {key}"), home.not_found()).await {
        Ok(_) => Ok(()),
        Err(CloudHomeError::NotFound(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

/// List every key under `prefix`, following pagination. A not-found on the first
/// page (Dropbox returns it when the folder doesn't exist yet) is an empty list.
pub async fn rest_list<T: OAuthRestHome + ?Sized>(
    home: &T,
    prefix: &str,
) -> Result<Vec<String>, CloudHomeError> {
    let mut keys = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let resp = home.send_list_page(prefix, cursor.as_deref()).await?;
        let resp = match ensure_ok(resp, &format!("list {prefix}"), home.not_found()).await {
            Ok(resp) => resp,
            // An absent listing root is an empty result, not an error.
            Err(CloudHomeError::NotFound(_)) => return Ok(keys),
            Err(e) => return Err(e),
        };
        let body = body_text(resp).await;
        let page = home.parse_list_page(&body, prefix)?;
        keys.extend(page.keys);
        match page.next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    Ok(keys)
}
