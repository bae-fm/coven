//! OneDrive `CloudHome` implementation.
//!
//! Uses the Microsoft Graph API. Files are stored flat in a single folder — path
//! separators are encoded as `__` (see [`super::key_encoding`]). The
//! `read`/`read_range`/`list`/`delete` methods are the shared [`OAuthRestHome`]
//! implementations; this file supplies only the Graph request shapes, the page
//! parser, the upload paths, and sharing.

use async_trait::async_trait;
use bytes::Bytes;

use super::http::{self, ensure_ok, exists_from_response, NotFound};
use super::key_encoding::{decode_key, encode_key};
use super::oauth_rest::{
    rest_delete, rest_list, rest_read, rest_read_range, ListPage, OAuthRestHome,
};
use super::oauth_session::OAuthSession;
use super::resumable::RangePutSink;
use super::{
    sharing, BoxPartSink, CloudAccessGrant, CloudAccessRevoke, CloudHome, CloudHomeError,
    CloudHomeJoinInfo, CloudObjectState, CloudObjectVersion, ConditionalDelete,
};
use crate::clock::ClockRef;
use crate::keys::KeyService;
use crate::oauth::{OAuthConfig, OAuthTokens};

const GRAPH_API: &str = "https://graph.microsoft.com/v1.0";

/// OneDrive cloud home backend.
pub struct OneDriveCloudHome {
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
            scopes: vec![
                "Files.ReadWrite".to_string(),
                "offline_access".to_string(),
                // Lets the joiner fetch its account email for OAuth folder sharing.
                "User.Read".to_string(),
            ],
            redirect_port: 19284,
            extra_auth_params: vec![],
        }
    }

    fn client(&self) -> &reqwest::Client {
        self.session.client()
    }

    /// Build the Graph API URL for a file by encoded name within the app folder.
    fn item_path_url(&self, key: &str) -> String {
        format!(
            "{}/drives/{}/items/{}:/{}:",
            GRAPH_API,
            self.drive_id,
            self.folder_id,
            encode_key(key)
        )
    }

    /// Build the Graph API URL for the folder's children endpoint.
    fn children_url(&self) -> String {
        format!(
            "{}/drives/{}/items/{}/children",
            GRAPH_API, self.drive_id, self.folder_id
        )
    }
}

/// `error.code` from a Microsoft Graph error body (e.g. `"quotaLimitReached"`),
/// or `None` if the body isn't Graph JSON.
fn parse_onedrive_error_code(body: &str) -> Option<String> {
    http::error_reason(body, |v| {
        v.get("error")?.get("code")?.as_str().map(String::from)
    })
}

/// Map a OneDrive write failure to a `CloudHomeError`. The `quotaLimitReached`
/// code gets a message naming the provider and the recovery step; everything else
/// keeps the raw HTTP status + body for debugging.
fn classify_write_error(status: reqwest::StatusCode, body: &str, key: &str) -> CloudHomeError {
    if parse_onedrive_error_code(body).as_deref() == Some("quotaLimitReached") {
        return CloudHomeError::Storage(
            "Your OneDrive storage is full. Free up space at onedrive.live.com to keep syncing."
                .to_string(),
        );
    }
    CloudHomeError::Storage(format!("write {key} (HTTP {status}): {body}"))
}

/// Files at or below this size go up as a single PUT; larger files use a resumable
/// session. Microsoft Graph caps a simple PUT at 250 MiB.
const ONEDRIVE_SIMPLE_PUT_MAX: usize = 4 * 1024 * 1024;

/// Resumable-session part size. Graph requires every part except the last to be a
/// multiple of 320 KiB; 7.5 MiB (24 × 320 KiB) keeps the request count low.
const ONEDRIVE_CHUNK_SIZE: usize = 24 * 320 * 1024;

#[async_trait]
impl OAuthRestHome for OneDriveCloudHome {
    fn not_found(&self) -> NotFound {
        NotFound::Status
    }

    async fn send_read(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        let url = format!("{}/content", self.item_path_url(key));
        let range = range.map(|(start, end)| super::range_header(start, end));
        self.session
            .api_call(|token| {
                let mut req = self.client().get(&url).bearer_auth(token);
                if let Some(ref range) = range {
                    req = req.header("Range", range);
                }
                req
            })
            .await
    }

    async fn send_delete(&self, key: &str) -> Result<reqwest::Response, CloudHomeError> {
        let url = self.item_path_url(key);
        self.session
            .api_call(|token| self.client().delete(&url).bearer_auth(token))
            .await
    }

    async fn send_list_page(
        &self,
        _prefix: &str,
        cursor: Option<&str>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        // `@odata.nextLink` is a full URL with all params; the first page is the
        // children endpoint selecting only names.
        let url = match cursor {
            Some(next) => next.to_string(),
            None => format!("{}?$select=name", self.children_url()),
        };
        self.session
            .api_call(|token| self.client().get(&url).bearer_auth(token))
            .await
    }

    fn parse_list_page(&self, body: &str, prefix: &str) -> Result<ListPage, CloudHomeError> {
        let json: serde_json::Value = serde_json::from_str(body)
            .map_err(|e| CloudHomeError::Storage(format!("parse list: {e}")))?;
        let encoded_prefix = encode_key(prefix);
        let mut keys = Vec::new();
        if let Some(items) = json["value"].as_array() {
            for item in items {
                if let Some(name) = item["name"].as_str() {
                    if name.starts_with(&encoded_prefix) {
                        keys.push(decode_key(name));
                    }
                }
            }
        }
        Ok(ListPage {
            keys,
            next: json["@odata.nextLink"].as_str().map(String::from),
        })
    }
}

#[async_trait]
impl CloudHome for OneDriveCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let body = Bytes::from(data);
        let url = format!("{}/content", self.item_path_url(key));
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .put(&url)
                    .bearer_auth(token)
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
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        let session_url = format!("{}/createUploadSession", self.item_path_url(key));
        let body = serde_json::json!({
            "item": { "@microsoft.graph.conflictBehavior": "replace" }
        });
        let resp = self
            .session
            .api_call(|token| {
                self.client()
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
            self.client().clone(),
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
        let url = self.item_path_url(key);
        let resp = self
            .session
            .api_call(|token| self.client().get(&url).bearer_auth(token))
            .await?;
        exists_from_response(resp, &format!("exists {key}"), NotFound::Status).await
    }

    async fn object_state(&self, key: &str) -> Result<CloudObjectState, CloudHomeError> {
        let url = format!("{}?$select=eTag", self.item_path_url(key));
        let resp = self
            .session
            .api_call(|token| self.client().get(&url).bearer_auth(token))
            .await?;
        let status = resp.status();
        let body = http::body_text(resp).await;
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(CloudObjectState::Absent);
        }
        if !status.is_success() {
            return Err(CloudHomeError::Storage(format!(
                "metadata {key} (HTTP {status}): {body}"
            )));
        }
        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| CloudHomeError::Storage(format!("parse metadata {key}: {e}")))?;
        match json["eTag"].as_str() {
            Some(etag) => Ok(CloudObjectState::Present(CloudObjectVersion::new(etag))),
            None => Ok(CloudObjectState::VersionUnavailable),
        }
    }

    async fn delete_if_version(
        &self,
        key: &str,
        version: &CloudObjectVersion,
    ) -> Result<ConditionalDelete, CloudHomeError> {
        let url = self.item_path_url(key);
        let etag = version.as_str().to_string();
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .delete(&url)
                    .bearer_auth(token)
                    .header("If-Match", &etag)
            })
            .await?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(ConditionalDelete::NotFound);
        }
        if status == reqwest::StatusCode::PRECONDITION_FAILED {
            return Ok(ConditionalDelete::Changed);
        }
        if !status.is_success() {
            return Err(CloudHomeError::Storage(format!(
                "delete {key} (HTTP {status}): {}",
                http::body_text(resp).await
            )));
        }
        Ok(ConditionalDelete::Deleted)
    }

    async fn grant_access(
        &self,
        grant: CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        let email = grant.require_provider_email("OneDrive")?;
        let url = format!(
            "{}/drives/{}/items/{}/invite",
            GRAPH_API, self.drive_id, self.folder_id
        );
        let invite = serde_json::json!({
            "recipients": [{"email": email}],
            "roles": ["write"],
            "requireSignIn": true,
        });
        let resp = self
            .session
            .api_call(|token| self.client().post(&url).bearer_auth(token).json(&invite))
            .await?;
        ensure_ok(resp, &format!("grant access to {email}"), NotFound::Status).await?;
        Ok(CloudHomeJoinInfo::OneDrive {
            drive_id: self.drive_id.clone(),
            folder_id: self.folder_id.clone(),
        })
    }

    async fn revoke_access(&self, revoke: CloudAccessRevoke) -> Result<(), CloudHomeError> {
        let email = revoke.require_provider_email("OneDrive")?;
        let perms_url = format!(
            "{}/drives/{}/items/{}/permissions",
            GRAPH_API, self.drive_id, self.folder_id
        );
        let delete_base = perms_url.clone();
        sharing::revoke_by_email(
            &self.session,
            email,
            &perms_url,
            "value",
            |p| {
                p["grantedToV2"]["user"]["email"]
                    .as_str()
                    .or_else(|| p["grantedTo"]["user"]["email"].as_str())
                    .map(String::from)
            },
            |perm_id| format!("{delete_base}/{perm_id}"),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn home() -> OneDriveCloudHome {
        OneDriveCloudHome::new(
            "drive123".to_string(),
            "folder456".to_string(),
            OAuthTokens {
                access_token: "test".to_string(),
                refresh_token: None,
                expires_at: None,
            },
            KeyService::new("test".to_string()),
            Arc::new(crate::clock::SystemClock),
        )
    }

    #[test]
    fn item_path_url_encodes_key() {
        assert_eq!(
            home().item_path_url("changes/dev1/42.enc"),
            "https://graph.microsoft.com/v1.0/drives/drive123/items/folder456:/changes__dev1__42.enc:"
        );
    }

    #[test]
    fn children_url_format() {
        assert_eq!(
            home().children_url(),
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
