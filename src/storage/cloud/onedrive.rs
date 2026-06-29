//! OneDrive `CloudHome` implementation.
//!
//! Uses the Microsoft Graph API. Files are stored flat in a single folder --
//! path separators are encoded as `__` (same as Google Drive) to avoid
//! sub-folder creation.

use async_trait::async_trait;

use super::oauth_session::OAuthSession;
use super::resumable::RangePutSink;
use super::{http, BoxPartSink, CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::clock::ClockRef;
use crate::keys::KeyService;
use crate::oauth::{OAuthConfig, OAuthTokens};

const GRAPH_API: &str = "https://graph.microsoft.com/v1.0";

/// OneDrive cloud home backend.
pub struct OneDriveCloudHome {
    client: reqwest::Client,
    drive_id: String,
    folder_id: String,
    session: OAuthSession,
}

impl OneDriveCloudHome {
    pub fn new(
        drive_id: String,
        folder_id: String,
        tokens: OAuthTokens,
        key_service: KeyService,
        clock: ClockRef,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            drive_id,
            folder_id,
            session: OAuthSession::new(
                tokens,
                key_service,
                clock,
                Self::oauth_config(),
                "OneDrive",
            ),
        }
    }

    pub fn oauth_config() -> OAuthConfig {
        let creds = crate::oauth::oauth_client_creds("onedrive");
        OAuthConfig {
            client_id: creds.client_id,
            client_secret: creds.client_secret,
            auth_url: "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize"
                .to_string(),
            token_url: "https://login.microsoftonline.com/consumers/oauth2/v2.0/token".to_string(),
            scopes: vec!["Files.ReadWrite".to_string(), "offline_access".to_string()],
            redirect_port: 19284,
            extra_auth_params: vec![],
        }
    }

    /// Encode a CloudHome key to a flat OneDrive filename.
    /// `changes/dev1/42.enc` -> `changes__dev1__42.enc`
    fn encode_key(key: &str) -> String {
        key.replace('/', "__")
    }

    /// Decode a flat filename back to a CloudHome key.
    /// `changes__dev1__42.enc` -> `changes/dev1/42.enc`
    fn decode_key(filename: &str) -> String {
        filename.replace("__", "/")
    }

    /// Build the Graph API URL for a file by encoded name within the app folder.
    fn item_path_url(&self, key: &str) -> String {
        let encoded = Self::encode_key(key);
        format!(
            "{}/drives/{}/items/{}:/{}:",
            GRAPH_API, self.drive_id, self.folder_id, encoded
        )
    }

    /// Build the Graph API URL for the folder's children endpoint.
    fn children_url(&self) -> String {
        format!(
            "{}/drives/{}/items/{}/children",
            GRAPH_API, self.drive_id, self.folder_id
        )
    }

    /// Make an API call with automatic token refresh on 401.
    async fn api_call(
        &self,
        build_request: impl Fn(&str) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, CloudHomeError> {
        self.session.api_call(build_request).await
    }
}

/// `error.code` from a Microsoft Graph error body (e.g. `"quotaLimitReached"`,
/// `"itemNotFound"`), or `None` if the body isn't Graph JSON. Non-JSON bodies
/// are a common skip (proxy 500s, captive portals) — log at debug so the
/// bail-out is visible without spamming a normal session.
fn parse_onedrive_error_code(body: &str) -> Option<String> {
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!("non-JSON OneDrive error body, skipping code extraction: {e}");
            return None;
        }
    };
    v.get("error")?.get("code")?.as_str().map(String::from)
}

/// Map a OneDrive write failure to a `CloudHomeError`. The `quotaLimitReached`
/// code gets a message naming the provider and the recovery step; everything
/// else keeps the raw HTTP status + body for debugging.
fn classify_write_error(status: reqwest::StatusCode, body: &str, key: &str) -> CloudHomeError {
    if parse_onedrive_error_code(body).as_deref() == Some("quotaLimitReached") {
        return CloudHomeError::Storage(
            "Your OneDrive storage is full. Free up space at onedrive.live.com to keep syncing."
                .to_string(),
        );
    }
    CloudHomeError::Storage(format!("write {key} (HTTP {status}): {body}"))
}

/// Files at or below this size go up as a single PUT (the smallest payload that
/// still warrants the round-trip of opening a resumable session). Larger files
/// use an upload session so progress advances per chunk. Microsoft Graph caps a
/// simple PUT at 250 MiB; this stays well under it.
const ONEDRIVE_SIMPLE_PUT_MAX: usize = 4 * 1024 * 1024;

/// Upload-session chunk size. Graph requires every chunk except the last to be
/// a multiple of 320 KiB; 7.5 MiB (24 × 320 KiB) keeps the request count low on
/// a large audio file while giving several progress ticks.
const ONEDRIVE_CHUNK_SIZE: usize = 24 * 320 * 1024;

#[async_trait]
impl CloudHome for OneDriveCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let url = format!("{}/content", self.item_path_url(key));
        let resp = self
            .api_call(|token| {
                self.client
                    .put(&url)
                    .bearer_auth(token)
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
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        let session_url = format!("{}/createUploadSession", self.item_path_url(key));
        let body = serde_json::json!({
            "item": { "@microsoft.graph.conflictBehavior": "replace" }
        });
        let resp = self
            .api_call(|token| {
                self.client
                    .post(&session_url)
                    .bearer_auth(token)
                    .json(&body)
            })
            .await?;
        let status = resp.status();
        let resp_body = http::body_text(resp).await;
        if !status.is_success() {
            return Err(classify_write_error(status, &resp_body, key));
        }
        let json: serde_json::Value = serde_json::from_str(&resp_body)
            .map_err(|e| CloudHomeError::Storage(format!("parse upload session {key}: {e}")))?;
        let upload_url = json["uploadUrl"]
            .as_str()
            .ok_or_else(|| {
                CloudHomeError::Storage(format!("upload session {key}: no uploadUrl returned"))
            })?
            .to_string();
        let key_owned = key.to_string();
        let classify =
            Box::new(move |status, body: &str| classify_write_error(status, body, &key_owned));
        // OneDrive returns 202 Accepted for every non-final part.
        Ok(Box::new(RangePutSink::new(
            self.client.clone(),
            upload_url,
            202,
            total_len,
            ONEDRIVE_CHUNK_SIZE,
            key.to_string(),
            classify,
        )))
    }

    fn multipart_threshold(&self) -> u64 {
        ONEDRIVE_SIMPLE_PUT_MAX as u64
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        let url = format!("{}/content", self.item_path_url(key));

        let resp = self
            .api_call(|token| self.client.get(&url).bearer_auth(token))
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(CloudHomeError::NotFound(key.to_string()));
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
        let url = format!("{}/content", self.item_path_url(key));
        let range = http::range_header(start, end);

        let resp = self
            .api_call(|token| {
                self.client
                    .get(&url)
                    .bearer_auth(token)
                    .header("Range", &range)
            })
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(CloudHomeError::NotFound(key.to_string()));
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
        // All files are stored flat with encoded names. Fetch all children
        // and filter client-side after decoding.
        let mut all_keys = Vec::new();
        let initial_url = format!("{}?$select=name", self.children_url());
        let mut next_url: Option<String> = Some(initial_url);
        let encoded_prefix = Self::encode_key(prefix);

        while let Some(url) = next_url.take() {
            let resp = self
                .api_call(|token| self.client.get(&url).bearer_auth(token))
                .await?;

            let status = resp.status();
            let body = resp
                .text()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("read body: {e}")))?;

            if !status.is_success() {
                return Err(CloudHomeError::Storage(format!(
                    "list {prefix} (HTTP {status}): {body}"
                )));
            }

            let json: serde_json::Value = serde_json::from_str(&body)
                .map_err(|e| CloudHomeError::Storage(format!("parse list: {e}")))?;

            if let Some(items) = json["value"].as_array() {
                for item in items {
                    if let Some(name) = item["name"].as_str() {
                        if name.starts_with(&encoded_prefix) {
                            all_keys.push(Self::decode_key(name));
                        }
                    }
                }
            }

            // @odata.nextLink is a full URL with all params included
            next_url = json["@odata.nextLink"].as_str().map(|s| s.to_string());
        }

        Ok(all_keys)
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        let url = self.item_path_url(key);

        let resp = self
            .api_call(|token| self.client.delete(&url).bearer_auth(token))
            .await?;

        let status = resp.status();
        // 204 No Content is success, 404 is OK (already deleted)
        if !status.is_success() && status != reqwest::StatusCode::NOT_FOUND {
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
        let url = self.item_path_url(key);

        let resp = self
            .api_call(|token| self.client.get(&url).bearer_auth(token))
            .await?;

        match resp.status() {
            s if s.is_success() => Ok(true),
            reqwest::StatusCode::NOT_FOUND => Ok(false),
            status => {
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|e| format!("<body read failed: {e}>"));
                Err(CloudHomeError::Storage(format!(
                    "exists {key} (HTTP {status}): {body}"
                )))
            }
        }
    }

    async fn grant_access(&self, member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        let url = format!(
            "{}/drives/{}/items/{}/invite",
            GRAPH_API, self.drive_id, self.folder_id
        );

        let invite = serde_json::json!({
            "recipients": [{"email": member_id}],
            "roles": ["write"],
            "requireSignIn": true,
        });

        let resp = self
            .api_call(|token| self.client.post(&url).bearer_auth(token).json(&invite))
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

        Ok(CloudHomeJoinInfo::OneDrive {
            drive_id: self.drive_id.clone(),
            folder_id: self.folder_id.clone(),
        })
    }

    async fn revoke_access(&self, member_id: &str) -> Result<(), CloudHomeError> {
        // First, list permissions on the folder to find the one matching member_id
        let perms_url = format!(
            "{}/drives/{}/items/{}/permissions",
            GRAPH_API, self.drive_id, self.folder_id
        );

        let resp = self
            .api_call(|token| self.client.get(&perms_url).bearer_auth(token))
            .await?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read body: {e}")))?;

        if !status.is_success() {
            return Err(CloudHomeError::Storage(format!(
                "list permissions (HTTP {status}): {body}"
            )));
        }

        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| CloudHomeError::Storage(format!("parse permissions: {e}")))?;

        // Find the permission entry whose grantedTo or grantedToV2 email matches member_id
        let permission_id = json["value"]
            .as_array()
            .and_then(|perms| {
                perms.iter().find_map(|p| {
                    let email = p["grantedToV2"]["user"]["email"]
                        .as_str()
                        .or_else(|| p["grantedTo"]["user"]["email"].as_str());
                    if email.map(|e| e.eq_ignore_ascii_case(member_id)) == Some(true) {
                        p["id"].as_str().map(|s| s.to_string())
                    } else {
                        None
                    }
                })
            })
            .ok_or_else(|| {
                CloudHomeError::Storage(format!("no permission found for {member_id}"))
            })?;

        // Delete the permission
        let delete_url = format!("{}/{}", perms_url, permission_id);

        let resp = self
            .api_call(|token| self.client.delete(&delete_url).bearer_auth(token))
            .await?;

        let status = resp.status();
        if !status.is_success() && status != reqwest::StatusCode::NOT_FOUND {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
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
    fn item_path_url_encodes_key() {
        let home = OneDriveCloudHome::new(
            "drive123".to_string(),
            "folder456".to_string(),
            OAuthTokens {
                access_token: "test".to_string(),
                refresh_token: None,
                expires_at: None,
            },
            KeyService::new("test".to_string()),
            Arc::new(crate::clock::SystemClock),
        );

        // Keys with slashes are encoded to flat filenames
        assert_eq!(
            home.item_path_url("changes/dev1/42.enc"),
            "https://graph.microsoft.com/v1.0/drives/drive123/items/folder456:/changes__dev1__42.enc:"
        );
    }

    #[test]
    fn children_url_format() {
        let home = OneDriveCloudHome::new(
            "drive123".to_string(),
            "folder456".to_string(),
            OAuthTokens {
                access_token: "test".to_string(),
                refresh_token: None,
                expires_at: None,
            },
            KeyService::new("test".to_string()),
            Arc::new(crate::clock::SystemClock),
        );

        assert_eq!(
            home.children_url(),
            "https://graph.microsoft.com/v1.0/drives/drive123/items/folder456/children"
        );
    }

    #[test]
    fn oauth_config_uses_consumers_endpoint() {
        let config = OneDriveCloudHome::oauth_config();
        assert!(config.auth_url.contains("/consumers/"));
        assert!(config.token_url.contains("/consumers/"));
        assert!(config.scopes.contains(&"Files.ReadWrite".to_string()));
        assert!(config.scopes.contains(&"offline_access".to_string()));
    }

    #[test]
    fn parse_onedrive_error_code_extracts_quota_limit_reached() {
        let body = r#"{"error":{"code":"quotaLimitReached","message":"Insufficient Storage"}}"#;
        assert_eq!(
            parse_onedrive_error_code(body).as_deref(),
            Some("quotaLimitReached"),
        );
    }

    #[test]
    fn parse_onedrive_error_code_returns_none_for_non_matching_body() {
        assert!(parse_onedrive_error_code("<html>500</html>").is_none());
        assert!(parse_onedrive_error_code("{}").is_none());
        assert!(parse_onedrive_error_code(r#"{"error":"flat"}"#).is_none());
    }

    #[test]
    fn classify_write_error_quota_message_names_provider_and_recovery() {
        let body = r#"{"error":{"code":"quotaLimitReached"}}"#;
        let err =
            classify_write_error(reqwest::StatusCode::INSUFFICIENT_STORAGE, body, "changes/1");
        let msg = err.to_string();
        assert!(msg.contains("OneDrive storage is full"), "{msg}");
        assert!(msg.contains("Free up space"), "{msg}");
    }

    #[test]
    fn classify_write_error_keeps_raw_for_non_quota_errors() {
        let body = r#"{"error":{"code":"itemNotFound","message":"..."}}"#;
        let err = classify_write_error(reqwest::StatusCode::NOT_FOUND, body, "changes/dev1/1.enc");
        let msg = err.to_string();
        assert!(msg.contains("HTTP 404"), "{msg}");
        assert!(msg.contains("changes/dev1/1.enc"), "{msg}");
        assert!(!msg.contains("storage is full"), "{msg}");
    }
}
