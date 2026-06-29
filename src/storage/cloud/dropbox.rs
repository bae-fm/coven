//! Dropbox `CloudHome` implementation.
//!
//! Uses the Dropbox HTTP API v2 with OAuth 2.0 (PKCE) tokens.
//! Files are stored in a folder (e.g. `/Apps/your-app/{library_name}`) using native
//! path-based access -- no filename encoding needed unlike Google Drive.

use async_trait::async_trait;

use super::oauth_session::OAuthSession;
use super::{http, BoxPartSink, CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::clock::ClockRef;
use crate::keys::KeyService;
use crate::oauth::{OAuthConfig, OAuthTokens};

const API_BASE: &str = "https://api.dropboxapi.com/2";
const CONTENT_BASE: &str = "https://content.dropboxapi.com/2";

/// Dropbox cloud home backend.
pub struct DropboxCloudHome {
    client: reqwest::Client,
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
            client: reqwest::Client::new(),
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
            scopes: vec![],
            redirect_port: 19284,
            extra_auth_params: vec![("token_access_type".to_string(), "offline".to_string())],
        }
    }

    /// Build the full Dropbox path for a key.
    /// `changes/dev1/42.enc` -> `/Apps/your-app/my-library/changes/dev1/42.enc`
    fn full_path(&self, key: &str) -> String {
        format!("{}/{}", self.folder_path, key)
    }

    /// Make an API call with automatic token refresh on 401.
    async fn api_call(
        &self,
        build_request: impl Fn(&str) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, CloudHomeError> {
        self.session.api_call(build_request).await
    }

    /// Call `share_folder` and resolve the shared_folder_id, handling both
    /// immediate and async_job_id responses.
    async fn get_or_create_shared_folder_id(&self) -> Result<String, CloudHomeError> {
        let share_body = serde_json::json!({ "path": self.folder_path });

        let resp = self
            .api_call(|token| {
                self.client
                    .post(format!("{}/sharing/share_folder", API_BASE))
                    .bearer_auth(token)
                    .json(&share_body)
            })
            .await?;

        let status = resp.status();
        let resp_body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<body read failed: {e}>"));
        let json: serde_json::Value = serde_json::from_str(&resp_body).unwrap_or_default();

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
                .api_call(|token| {
                    self.client
                        .post(format!("{}/sharing/check_share_job_status", API_BASE))
                        .bearer_auth(token)
                        .json(&body)
                })
                .await?;

            let resp_body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            let json: serde_json::Value = serde_json::from_str(&resp_body).unwrap_or_default();

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
                .api_call(|token| {
                    self.home
                        .client
                        .post(format!("{}/files/upload_session/finish", CONTENT_BASE))
                        .bearer_auth(token)
                        .header("Dropbox-API-Arg", &arg)
                        .header("Content-Type", "application/octet-stream")
                        .body(part.to_vec())
                })
                .await?
        } else {
            let arg = serde_json::json!({
                "cursor": { "session_id": self.session_id, "offset": offset },
                "close": false,
            })
            .to_string();
            self.home
                .api_call(|token| {
                    self.home
                        .client
                        .post(format!("{}/files/upload_session/append_v2", CONTENT_BASE))
                        .bearer_auth(token)
                        .header("Dropbox-API-Arg", &arg)
                        .header("Content-Type", "application/octet-stream")
                        .body(part.to_vec())
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

/// `error_summary` field from a Dropbox API error body (the chained tag string,
/// e.g. `"path/insufficient_space/..."`), or `None` if the body isn't Dropbox
/// JSON. Non-JSON bodies are a common skip (proxy 500s, captive portals) — log
/// at debug so the bail-out is visible without spamming a normal session.
fn parse_dropbox_error_summary(body: &str) -> Option<String> {
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!("non-JSON Dropbox error body, skipping summary extraction: {e}");
            return None;
        }
    };
    v.get("error_summary")?.as_str().map(String::from)
}

/// Map a Dropbox write failure to a `CloudHomeError`. The `insufficient_space`
/// path error gets a message naming the provider and the recovery step;
/// everything else keeps the raw HTTP status + body for debugging.
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

/// Files at or below this size go up in one `files/upload` call; larger files
/// use an upload session so progress advances per chunk. Dropbox requires the
/// single-shot `files/upload` endpoint for payloads up to 150 MB; this stays
/// well under it.
const DROPBOX_SIMPLE_UPLOAD_MAX: usize = 4 * 1024 * 1024;

/// Upload-session chunk size. Dropbox imposes no alignment requirement on
/// session chunks (only a 150 MB per-call ceiling); 8 MiB keeps the request
/// count low while giving several progress ticks.
const DROPBOX_CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl CloudHome for DropboxCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let path = self.full_path(key);
        let api_arg = serde_json::json!({
            "path": path,
            "mode": { ".tag": "overwrite" },
            "autorename": false,
            "mute": true,
        })
        .to_string();

        let resp = self
            .api_call(|token| {
                self.client
                    .post(format!("{}/files/upload", CONTENT_BASE))
                    .bearer_auth(token)
                    .header("Dropbox-API-Arg", &api_arg)
                    .header("Content-Type", "application/octet-stream")
                    .body(data.clone())
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
            .api_call(|token| {
                self.client
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
        let path = self.full_path(key);
        let api_arg = serde_json::json!({ "path": path });
        let api_arg_str = api_arg.to_string();

        let resp = self
            .api_call(|token| {
                self.client
                    .post(format!("{}/files/download", CONTENT_BASE))
                    .bearer_auth(token)
                    .header("Dropbox-API-Arg", &api_arg_str)
            })
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::CONFLICT {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            if body.contains("not_found") {
                return Err(CloudHomeError::NotFound(key.to_string()));
            }
            return Err(CloudHomeError::Storage(format!(
                "read {key} (HTTP {status}): {body}"
            )));
        }
        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            return Err(CloudHomeError::Storage(format!(
                "read {key} (HTTP {status}): {body}"
            )));
        }

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read body for {key}: {e}")))?;

        Ok(bytes.to_vec())
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        let path = self.full_path(key);
        let api_arg = serde_json::json!({ "path": path });
        let api_arg_str = api_arg.to_string();
        let range = http::range_header(start, end);

        let resp = self
            .api_call(|token| {
                self.client
                    .post(format!("{}/files/download", CONTENT_BASE))
                    .bearer_auth(token)
                    .header("Dropbox-API-Arg", &api_arg_str)
                    .header("Range", &range)
            })
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::CONFLICT {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            if body.contains("not_found") {
                return Err(CloudHomeError::NotFound(key.to_string()));
            }
            return Err(CloudHomeError::Storage(format!(
                "read range {key} (HTTP {status}): {body}"
            )));
        }
        if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            return Err(CloudHomeError::Storage(format!(
                "read range {key} (HTTP {status}): {body}"
            )));
        }

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read range body for {key}: {e}")))?;

        Ok(bytes.to_vec())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        // List from the root folder_path with recursive=true, then filter by prefix.
        // Dropbox list_folder needs a folder path, not a prefix, so we always
        // start from the root and filter results.
        let search_path = self.folder_path.clone();

        let mut all_keys = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let resp = if let Some(ref cur) = cursor {
                let body = serde_json::json!({ "cursor": cur });
                self.api_call(|token| {
                    self.client
                        .post(format!("{}/files/list_folder/continue", API_BASE))
                        .bearer_auth(token)
                        .json(&body)
                })
                .await?
            } else {
                let body = serde_json::json!({
                    "path": search_path,
                    "recursive": true,
                    "limit": 2000,
                });
                self.api_call(|token| {
                    self.client
                        .post(format!("{}/files/list_folder", API_BASE))
                        .bearer_auth(token)
                        .json(&body)
                })
                .await?
            };

            let status = resp.status();

            // If the folder doesn't exist, return empty list
            if status == reqwest::StatusCode::CONFLICT {
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|e| format!("<body read failed: {e}>"));
                if body.contains("not_found") {
                    return Ok(Vec::new());
                }
                return Err(CloudHomeError::Storage(format!(
                    "list {prefix} (HTTP {status}): {body}"
                )));
            }

            if !status.is_success() {
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|e| format!("<body read failed: {e}>"));
                return Err(CloudHomeError::Storage(format!(
                    "list {prefix} (HTTP {status}): {body}"
                )));
            }

            let body = resp
                .text()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("read body: {e}")))?;
            let json: serde_json::Value = serde_json::from_str(&body)
                .map_err(|e| CloudHomeError::Storage(format!("parse list: {e}")))?;

            let folder_lower = self.folder_path.to_lowercase();

            if let Some(entries) = json["entries"].as_array() {
                for entry in entries {
                    // Only include files, not folders
                    if entry[".tag"].as_str() != Some("file") {
                        continue;
                    }
                    // Use path_lower for reliable prefix stripping (path_display
                    // has inconsistent casing), then use path_display for the
                    // actual key value to preserve original casing.
                    if let (Some(path_lower), Some(path_display)) =
                        (entry["path_lower"].as_str(), entry["path_display"].as_str())
                    {
                        let lower_prefix = format!("{}/", folder_lower);
                        if path_lower.starts_with(&lower_prefix) {
                            // Extract key from path_display at the same offset
                            let key = &path_display[lower_prefix.len()..];
                            if key.starts_with(prefix) {
                                all_keys.push(key.to_string());
                            }
                        }
                    }
                }
            }

            let has_more = json["has_more"].as_bool().unwrap_or(false);
            if has_more {
                cursor = json["cursor"].as_str().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(all_keys)
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        let path = self.full_path(key);
        let body = serde_json::json!({ "path": path });

        let resp = self
            .api_call(|token| {
                self.client
                    .post(format!("{}/files/delete_v2", API_BASE))
                    .bearer_auth(token)
                    .json(&body)
            })
            .await?;

        let status = resp.status();

        // 409 with path_lookup/not_found means already deleted -- treat as success
        if status == reqwest::StatusCode::CONFLICT {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            if body.contains("not_found") {
                return Ok(());
            }
            return Err(CloudHomeError::Storage(format!(
                "delete {key} (HTTP {status}): {body}"
            )));
        }

        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            return Err(CloudHomeError::Storage(format!(
                "delete {key} (HTTP {status}): {body}"
            )));
        }

        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        let path = self.full_path(key);
        let body = serde_json::json!({ "path": path });

        let resp = self
            .api_call(|token| {
                self.client
                    .post(format!("{}/files/get_metadata", API_BASE))
                    .bearer_auth(token)
                    .json(&body)
            })
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::CONFLICT {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            if body.contains("not_found") {
                return Ok(false);
            }
            return Err(CloudHomeError::Storage(format!(
                "exists {key} (HTTP {status}): {body}"
            )));
        }

        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            return Err(CloudHomeError::Storage(format!(
                "exists {key} (HTTP {status}): {body}"
            )));
        }

        Ok(true)
    }

    async fn grant_access(&self, member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        let shared_folder_id = self.get_or_create_shared_folder_id().await?;

        // Now add the member
        let add_body = serde_json::json!({
            "shared_folder_id": shared_folder_id,
            "members": [{
                "member": {
                    ".tag": "email",
                    "email": member_id,
                },
                "access_level": { ".tag": "editor" },
            }],
            "quiet": false,
        });

        let resp = self
            .api_call(|token| {
                self.client
                    .post(format!("{}/sharing/add_folder_member", API_BASE))
                    .bearer_auth(token)
                    .json(&add_body)
            })
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            return Err(CloudHomeError::Storage(format!(
                "grant access to {member_id} (HTTP {status}): {body}"
            )));
        }

        Ok(CloudHomeJoinInfo::Dropbox { shared_folder_id })
    }

    async fn revoke_access(&self, member_id: &str) -> Result<(), CloudHomeError> {
        let shared_folder_id = self.get_or_create_shared_folder_id().await?;

        // Remove the member
        let remove_body = serde_json::json!({
            "shared_folder_id": shared_folder_id,
            "member": {
                ".tag": "email",
                "email": member_id,
            },
            "leave_a_copy": false,
        });

        let resp = self
            .api_call(|token| {
                self.client
                    .post(format!("{}/sharing/remove_folder_member", API_BASE))
                    .bearer_auth(token)
                    .json(&remove_body)
            })
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));

            // If the member is not found, treat as success
            if body.contains("not_found") || body.contains("member_error") {
                return Ok(());
            }

            return Err(CloudHomeError::Storage(format!(
                "revoke access for {member_id} (HTTP {status}): {body}"
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn full_path_joins_correctly() {
        let home = DropboxCloudHome::new(
            "/Apps/your-app/my-library".to_string(),
            OAuthTokens {
                access_token: String::new(),
                refresh_token: None,
                expires_at: None,
            },
            KeyService::new("test".to_string()),
            Arc::new(crate::clock::SystemClock),
        );

        assert_eq!(
            home.full_path("changes/dev1/42.enc"),
            "/Apps/your-app/my-library/changes/dev1/42.enc"
        );
        assert_eq!(
            home.full_path("snapshot/abc/0.db.enc"),
            "/Apps/your-app/my-library/snapshot/abc/0.db.enc"
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
        let body = r#"{"error_summary":"path/insufficient_space/.tag","error":{".tag":"path","reason":{".tag":"insufficient_space"}}}"#;
        assert_eq!(
            parse_dropbox_error_summary(body).as_deref(),
            Some("path/insufficient_space/.tag"),
        );
    }

    #[test]
    fn parse_dropbox_error_summary_returns_none_for_non_matching_body() {
        assert!(parse_dropbox_error_summary("<html>500</html>").is_none());
        assert!(parse_dropbox_error_summary("{}").is_none());
        assert!(parse_dropbox_error_summary(r#"{"other":"field"}"#).is_none());
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
}
