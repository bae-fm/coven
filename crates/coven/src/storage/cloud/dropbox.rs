//! Dropbox `CloudHome` implementation.
//!
//! Uses the Dropbox HTTP API v2 with OAuth 2.0 (PKCE) tokens. Files live under a
//! folder using native path-based access — no filename encoding. The
//! `read`/`read_range`/`list`/`delete` methods are the shared [`OAuthRestHome`]
//! implementations; this file supplies only the Dropbox request shapes (POST with
//! a `Dropbox-API-Arg` header), the page parser, the upload session, and sharing.

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::StatusCode;

use super::http::{self, ensure_ok, exists_from_response, NotFound};
use super::oauth_rest::{
    rest_delete, rest_list, rest_read, rest_read_range, ListPage, OAuthRestHome,
};
use super::oauth_session::OAuthSession;
use super::{
    BoxPartSink, CloudAccessGrant, CloudAccessRevoke, CloudHome, CloudHomeError, CloudHomeJoinInfo,
};
use crate::clock::ClockRef;
use crate::keys::KeyService;
use crate::oauth::{OAuthConfig, OAuthTokens};

const API_BASE: &str = "https://api.dropboxapi.com/2";
const CONTENT_BASE: &str = "https://content.dropboxapi.com/2";

/// Dropbox cloud home backend.
pub struct DropboxCloudHome {
    /// Folder path in Dropbox, e.g. "/Apps/your-app/my-library"
    folder_path: String,
    session: OAuthSession,
}

impl DropboxCloudHome {
    pub fn new(
        folder_path: String,
        tokens: OAuthTokens,
        key_service: KeyService,
        clock: ClockRef,
    ) -> Self {
        Self {
            folder_path,
            session: OAuthSession::new(tokens, key_service, clock, Self::oauth_config(), "Dropbox"),
        }
    }

    pub fn oauth_config() -> OAuthConfig {
        let creds = crate::oauth::oauth_client_creds("dropbox");
        OAuthConfig {
            client_id: creds.client_id,
            client_secret: creds.client_secret,
            auth_url: "https://www.dropbox.com/oauth2/authorize".to_string(),
            token_url: "https://api.dropboxapi.com/oauth2/token".to_string(),
            // Empty on purpose: passing a `scope` param requests ONLY the listed
            // scopes, which would narrow away the file scopes the app relies on.
            // With no `scope` param Dropbox grants the app's console-configured
            // default scope set. Fetching the account email (commit 2) needs the
            // `account_info.read` scope; that dependency lives in the Dropbox app
            // registration's default scope set, not here.
            scopes: vec![],
            redirect_port: 19284,
            extra_auth_params: vec![("token_access_type".to_string(), "offline".to_string())],
        }
    }

    fn client(&self) -> &reqwest::Client {
        self.session.client()
    }

    /// Build the full Dropbox path for a key.
    /// `changes/dev1/42.enc` -> `/Apps/your-app/my-library/changes/dev1/42.enc`
    fn full_path(&self, key: &str) -> String {
        format!("{}/{}", self.folder_path, key)
    }

    /// Call `share_folder` and resolve the shared_folder_id, handling both
    /// immediate and async_job_id responses.
    async fn get_or_create_shared_folder_id(&self) -> Result<String, CloudHomeError> {
        let share_body = serde_json::json!({ "path": self.folder_path });
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/sharing/share_folder", API_BASE))
                    .bearer_auth(token)
                    .json(&share_body)
            })
            .await?;

        let status = resp.status();
        let resp_body = http::body_text(resp).await;
        let json: serde_json::Value = serde_json::from_str(&resp_body).map_err(|e| {
            CloudHomeError::Storage(format!(
                "share folder (HTTP {status}): unparseable response: {e}: {resp_body}"
            ))
        })?;

        // Immediate: {".tag": "complete", "shared_folder_id": "..."}
        if let Some(id) = json["shared_folder_id"].as_str() {
            return Ok(id.to_string());
        }
        // Already shared: error payload contains the shared_folder_metadata
        if let Some(id) = json["error"]["shared_folder_metadata"]["shared_folder_id"].as_str() {
            return Ok(id.to_string());
        }
        // Async: {".tag": "async_job_id", "async_job_id": "..."}
        if let Some(job_id) = json["async_job_id"].as_str() {
            return self.poll_share_job(job_id).await;
        }
        if !status.is_success() {
            return Err(CloudHomeError::Storage(format!(
                "share folder (HTTP {status}): {resp_body}"
            )));
        }
        Err(CloudHomeError::Storage(
            "could not determine shared_folder_id".to_string(),
        ))
    }

    /// Poll `check_share_job_status` until the share operation completes.
    async fn poll_share_job(&self, job_id: &str) -> Result<String, CloudHomeError> {
        let body = serde_json::json!({ "async_job_id": job_id });
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let resp = self
                .session
                .api_call(|token| {
                    self.client()
                        .post(format!("{}/sharing/check_share_job_status", API_BASE))
                        .bearer_auth(token)
                        .json(&body)
                })
                .await?;
            let resp_body = http::body_text(resp).await;
            let json: serde_json::Value = serde_json::from_str(&resp_body).map_err(|e| {
                CloudHomeError::Storage(format!(
                    "share job status: unparseable response: {e}: {resp_body}"
                ))
            })?;
            match json[".tag"].as_str() {
                Some("complete") => {
                    if let Some(id) = json["shared_folder_id"].as_str() {
                        return Ok(id.to_string());
                    }
                    return Err(CloudHomeError::Storage(
                        "share job completed but no shared_folder_id".to_string(),
                    ));
                }
                Some("failed") => {
                    return Err(CloudHomeError::Storage(format!(
                        "share folder job failed: {resp_body}"
                    )));
                }
                _ => continue, // "in_progress" — keep polling
            }
        }
        Err(CloudHomeError::Storage(
            "share folder timed out after 30 seconds".to_string(),
        ))
    }
}

/// A [`PartSink`](super::PartSink) over a Dropbox upload session: `append_v2` adds
/// each non-final part at its byte offset; the final part is committed to the
/// destination path via `upload_session/finish` (overwrite mode). The session was
/// opened by `open_multipart`; `finish` is a no-op (the last part committed).
struct DropboxSessionSink<'a> {
    home: &'a DropboxCloudHome,
    session_id: String,
    key: String,
}

#[async_trait]
impl super::PartSink for DropboxSessionSink<'_> {
    fn part_size(&self) -> usize {
        DROPBOX_CHUNK_SIZE
    }

    async fn send_part(
        &mut self,
        part: bytes::Bytes,
        offset: u64,
        is_last: bool,
    ) -> Result<(), CloudHomeError> {
        let resp = if is_last {
            // The final part commits the file at the destination path.
            let path = self.home.full_path(&self.key);
            let arg = serde_json::json!({
                "cursor": { "session_id": self.session_id, "offset": offset },
                "commit": {
                    "path": path,
                    "mode": { ".tag": "overwrite" },
                    "autorename": false,
                    "mute": true,
                },
            })
            .to_string();
            self.home
                .session
                .api_call(|token| {
                    self.home
                        .client()
                        .post(format!("{}/files/upload_session/finish", CONTENT_BASE))
                        .bearer_auth(token)
                        .header("Dropbox-API-Arg", &arg)
                        .header("Content-Type", "application/octet-stream")
                        .body(part.clone())
                })
                .await?
        } else {
            let arg = serde_json::json!({
                "cursor": { "session_id": self.session_id, "offset": offset },
                "close": false,
            })
            .to_string();
            self.home
                .session
                .api_call(|token| {
                    self.home
                        .client()
                        .post(format!("{}/files/upload_session/append_v2", CONTENT_BASE))
                        .bearer_auth(token)
                        .header("Dropbox-API-Arg", &arg)
                        .header("Content-Type", "application/octet-stream")
                        .body(part.clone())
                })
                .await?
        };
        let status = resp.status();
        if !status.is_success() {
            return Err(classify_write_error(
                status,
                &http::body_text(resp).await,
                &self.key,
            ));
        }
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        Ok(())
    }
}

/// `error_summary` field from a Dropbox API error body (the chained tag string),
/// or `None` if the body isn't Dropbox JSON.
fn parse_dropbox_error_summary(body: &str) -> Option<String> {
    http::error_reason(body, |v| v.get("error_summary")?.as_str().map(String::from))
}

/// Map a Dropbox write failure to a `CloudHomeError`. The `insufficient_space`
/// path error gets a message naming the provider and the recovery step; everything
/// else keeps the raw HTTP status + body for debugging.
fn classify_write_error(status: reqwest::StatusCode, body: &str, key: &str) -> CloudHomeError {
    if let Some(summary) = parse_dropbox_error_summary(body) {
        if summary.starts_with("path/insufficient_space") {
            return CloudHomeError::Storage(
                "Your Dropbox storage is full. Free up space at dropbox.com to keep syncing."
                    .to_string(),
            );
        }
    }
    CloudHomeError::Storage(format!("write {key} (HTTP {status}): {body}"))
}

/// Files at or below this size go up in one `files/upload` call; larger files use
/// an upload session. Dropbox requires the single-shot endpoint for payloads up to
/// 150 MB; this stays well under it.
const DROPBOX_SIMPLE_UPLOAD_MAX: usize = 4 * 1024 * 1024;

/// Upload-session part size. Dropbox imposes no alignment requirement; 8 MiB keeps
/// the request count low.
const DROPBOX_CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl OAuthRestHome for DropboxCloudHome {
    fn not_found(&self) -> NotFound {
        // Dropbox signals an absent path with 409 + `not_found` in the body.
        NotFound::BodyContains {
            status: StatusCode::CONFLICT,
            needle: "not_found",
        }
    }

    async fn send_read(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        let arg = serde_json::json!({ "path": self.full_path(key) }).to_string();
        let range = range.map(|(start, end)| super::range_header(start, end));
        self.session
            .api_call(|token| {
                let mut req = self
                    .client()
                    .post(format!("{}/files/download", CONTENT_BASE))
                    .bearer_auth(token)
                    .header("Dropbox-API-Arg", &arg);
                if let Some(ref range) = range {
                    req = req.header("Range", range);
                }
                req
            })
            .await
    }

    async fn send_delete(&self, key: &str) -> Result<reqwest::Response, CloudHomeError> {
        let body = serde_json::json!({ "path": self.full_path(key) });
        self.session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/files/delete_v2", API_BASE))
                    .bearer_auth(token)
                    .json(&body)
            })
            .await
    }

    async fn send_list_page(
        &self,
        _prefix: &str,
        cursor: Option<&str>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        // list_folder needs a folder path, not a prefix, so always list the root
        // recursively and filter client-side; `continue` follows pagination.
        match cursor {
            Some(cur) => {
                let body = serde_json::json!({ "cursor": cur });
                self.session
                    .api_call(|token| {
                        self.client()
                            .post(format!("{}/files/list_folder/continue", API_BASE))
                            .bearer_auth(token)
                            .json(&body)
                    })
                    .await
            }
            None => {
                let body = serde_json::json!({
                    "path": self.folder_path,
                    "recursive": true,
                    "limit": 2000,
                });
                self.session
                    .api_call(|token| {
                        self.client()
                            .post(format!("{}/files/list_folder", API_BASE))
                            .bearer_auth(token)
                            .json(&body)
                    })
                    .await
            }
        }
    }

    fn parse_list_page(&self, body: &str, prefix: &str) -> Result<ListPage, CloudHomeError> {
        let json: serde_json::Value = serde_json::from_str(body)
            .map_err(|e| CloudHomeError::Storage(format!("parse list: {e}")))?;
        let folder_lower = self.folder_path.to_lowercase();
        let lower_prefix = format!("{folder_lower}/");
        let mut keys = Vec::new();
        if let Some(entries) = json["entries"].as_array() {
            for entry in entries {
                if entry[".tag"].as_str() != Some("file") {
                    continue;
                }
                // path_lower for reliable prefix stripping (path_display has
                // inconsistent casing); path_display for the actual key value.
                if let (Some(path_lower), Some(path_display)) =
                    (entry["path_lower"].as_str(), entry["path_display"].as_str())
                {
                    if path_lower.starts_with(&lower_prefix) {
                        let key = &path_display[lower_prefix.len()..];
                        if key.starts_with(prefix) {
                            keys.push(key.to_string());
                        }
                    }
                }
            }
        }
        let next = if json["has_more"].as_bool() == Some(true) {
            Some(
                json["cursor"]
                    .as_str()
                    .ok_or_else(|| {
                        CloudHomeError::Storage(
                            "Dropbox list response has_more without cursor".to_string(),
                        )
                    })?
                    .to_string(),
            )
        } else {
            None
        };
        Ok(ListPage { keys, next })
    }
}

#[async_trait]
impl CloudHome for DropboxCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let body = Bytes::from(data);
        let api_arg = serde_json::json!({
            "path": self.full_path(key),
            "mode": { ".tag": "overwrite" },
            "autorename": false,
            "mute": true,
        })
        .to_string();
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/files/upload", CONTENT_BASE))
                    .bearer_auth(token)
                    .header("Dropbox-API-Arg", &api_arg)
                    .header("Content-Type", "application/octet-stream")
                    .body(body.clone())
            })
            .await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(classify_write_error(
                status,
                &http::body_text(resp).await,
                key,
            ));
        }
        Ok(())
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        _total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        // Open an empty session; the parts are sent via append/finish.
        let start = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/files/upload_session/start", CONTENT_BASE))
                    .bearer_auth(token)
                    .header("Dropbox-API-Arg", r#"{"close":false}"#)
                    .header("Content-Type", "application/octet-stream")
                    .body(Vec::new())
            })
            .await?;
        let status = start.status();
        let start_body = http::body_text(start).await;
        if !status.is_success() {
            return Err(classify_write_error(status, &start_body, key));
        }
        let start_json: serde_json::Value = serde_json::from_str(&start_body)
            .map_err(|e| CloudHomeError::Storage(format!("parse upload session {key}: {e}")))?;
        let session_id = start_json["session_id"]
            .as_str()
            .ok_or_else(|| {
                CloudHomeError::Storage(format!("upload session {key}: no session_id returned"))
            })?
            .to_string();
        Ok(Box::new(DropboxSessionSink {
            home: self,
            session_id,
            key: key.to_string(),
        }))
    }

    fn multipart_threshold(&self) -> u64 {
        DROPBOX_SIMPLE_UPLOAD_MAX as u64
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        rest_read(self, key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        rest_read_range(self, key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        rest_list(self, prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        rest_delete(self, key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        let body = serde_json::json!({ "path": self.full_path(key) });
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/files/get_metadata", API_BASE))
                    .bearer_auth(token)
                    .json(&body)
            })
            .await?;
        exists_from_response(resp, &format!("exists {key}"), self.not_found()).await
    }

    async fn grant_access(
        &self,
        grant: CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        let email = grant.require_provider_email("Dropbox")?;
        let shared_folder_id = self.get_or_create_shared_folder_id().await?;
        let add_body = serde_json::json!({
            "shared_folder_id": shared_folder_id,
            "members": [{
                "member": { ".tag": "email", "email": email },
                "access_level": { ".tag": "editor" },
            }],
            "quiet": false,
        });
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/sharing/add_folder_member", API_BASE))
                    .bearer_auth(token)
                    .json(&add_body)
            })
            .await?;
        ensure_ok(resp, &format!("grant access to {email}"), self.not_found()).await?;
        Ok(CloudHomeJoinInfo::Dropbox { shared_folder_id })
    }

    async fn revoke_access(&self, revoke: CloudAccessRevoke) -> Result<(), CloudHomeError> {
        let email = revoke.require_provider_email("Dropbox")?;
        let shared_folder_id = self.get_or_create_shared_folder_id().await?;
        let remove_body = serde_json::json!({
            "shared_folder_id": shared_folder_id,
            "member": { ".tag": "email", "email": email },
            "leave_a_copy": false,
        });
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/sharing/remove_folder_member", API_BASE))
                    .bearer_auth(token)
                    .json(&remove_body)
            })
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = http::body_text(resp).await;
            // A member who isn't there is already revoked.
            if body.contains("not_found") || body.contains("member_error") {
                return Ok(());
            }
            return Err(CloudHomeError::Storage(format!(
                "revoke access for {email} (HTTP {status}): {body}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn home() -> DropboxCloudHome {
        DropboxCloudHome::new(
            "/Apps/your-app/my-library".to_string(),
            OAuthTokens {
                access_token: String::new(),
                refresh_token: None,
                expires_at: None,
            },
            KeyService::new("test".to_string()),
            Arc::new(crate::clock::SystemClock),
        )
    }

    #[test]
    fn full_path_joins_correctly() {
        assert_eq!(
            home().full_path("changes/dev1/42.enc"),
            "/Apps/your-app/my-library/changes/dev1/42.enc"
        );
    }

    #[test]
    fn oauth_config_uses_dropbox_urls() {
        let config = DropboxCloudHome::oauth_config();
        assert_eq!(config.auth_url, "https://www.dropbox.com/oauth2/authorize");
        assert_eq!(config.token_url, "https://api.dropboxapi.com/oauth2/token");
        assert!(config.client_secret.is_none());
        assert!(config.scopes.is_empty());
    }

    #[test]
    fn parse_dropbox_error_summary_extracts_insufficient_space() {
        let body = r#"{"error_summary":"path/insufficient_space/.tag","error":{".tag":"path"}}"#;
        assert_eq!(
            parse_dropbox_error_summary(body).as_deref(),
            Some("path/insufficient_space/.tag"),
        );
    }

    #[test]
    fn classify_write_error_quota_message_names_provider_and_recovery() {
        let body = r#"{"error_summary":"path/insufficient_space/..","error":{}}"#;
        let err =
            classify_write_error(reqwest::StatusCode::INSUFFICIENT_STORAGE, body, "changes/1");
        let msg = err.to_string();
        assert!(msg.contains("Dropbox storage is full"), "{msg}");
        assert!(msg.contains("Free up space"), "{msg}");
    }

    #[test]
    fn classify_write_error_keeps_raw_for_non_quota_errors() {
        let body = r#"{"error_summary":"path/conflict/file","error":{}}"#;
        let err = classify_write_error(reqwest::StatusCode::CONFLICT, body, "changes/dev1/1.enc");
        let msg = err.to_string();
        assert!(msg.contains("HTTP 409"), "{msg}");
        assert!(msg.contains("changes/dev1/1.enc"), "{msg}");
        assert!(!msg.contains("storage is full"), "{msg}");
    }

    #[test]
    fn parse_list_page_rejects_has_more_without_cursor() {
        let body = r#"{"entries":[],"has_more":true}"#;
        match home().parse_list_page(body, "") {
            Ok(_) => panic!("has_more without cursor must fail"),
            Err(err) => assert!(err.to_string().contains("cursor"), "{err}"),
        }
    }
}
