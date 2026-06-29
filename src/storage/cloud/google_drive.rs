//! Google Drive `CloudHome` implementation.
//!
//! Uses the Google Drive REST API v3 with OAuth 2.0 tokens.
//! Files are stored flat in a single folder -- path separators are encoded as `__`.

use async_trait::async_trait;

use super::oauth_session::OAuthSession;
use super::resumable::RangePutSink;
use super::{http, BoxPartSink, CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::clock::ClockRef;
use crate::keys::KeyService;
use crate::oauth::{OAuthConfig, OAuthTokens};

const DRIVE_API: &str = "https://www.googleapis.com/drive/v3";
const UPLOAD_API: &str = "https://www.googleapis.com/upload/drive/v3";

/// Google Drive cloud home backend.
pub struct GoogleDriveCloudHome {
    client: reqwest::Client,
    folder_id: String,
    session: OAuthSession,
}

impl GoogleDriveCloudHome {
    pub fn new(
        folder_id: String,
        tokens: OAuthTokens,
        key_service: KeyService,
        clock: ClockRef,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            folder_id,
            session: OAuthSession::new(
                tokens,
                key_service,
                clock,
                Self::oauth_config(),
                "Google Drive",
            ),
        }
    }

    pub fn oauth_config() -> OAuthConfig {
        let creds = crate::oauth::oauth_client_creds("google_drive");
        OAuthConfig {
            client_id: creds.client_id,
            client_secret: creds.client_secret,
            auth_url: "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
            token_url: "https://oauth2.googleapis.com/token".to_string(),
            scopes: vec!["https://www.googleapis.com/auth/drive.file".to_string()],
            redirect_port: 19284,
            extra_auth_params: vec![("access_type".to_string(), "offline".to_string())],
        }
    }

    /// Encode a CloudHome key to a flat Google Drive filename.
    /// `changes/dev1/42.enc` -> `changes__dev1__42.enc`
    fn encode_key(key: &str) -> String {
        key.replace('/', "__")
    }

    /// Decode a flat filename back to a CloudHome key.
    /// `changes__dev1__42.enc` -> `changes/dev1/42.enc`
    fn decode_key(filename: &str) -> String {
        filename.replace("__", "/")
    }

    /// Encode a prefix for Google Drive query matching.
    /// `changes/dev1/` -> `changes__dev1__`
    fn encode_prefix(prefix: &str) -> String {
        prefix.replace('/', "__")
    }

    /// Make an API call with automatic token refresh on 401.
    async fn api_call(
        &self,
        build_request: impl Fn(&str) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, CloudHomeError> {
        self.session.api_call(build_request).await
    }

    /// Find a file's Google Drive ID by name within our folder.
    async fn find_file_id(&self, encoded_name: &str) -> Result<Option<String>, CloudHomeError> {
        let query = format!(
            "'{}' in parents and name = '{}' and trashed = false",
            self.folder_id, encoded_name
        );

        let resp = self
            .api_call(|token| {
                self.client
                    .get(format!("{}/files", DRIVE_API))
                    .bearer_auth(token)
                    .query(&[
                        ("q", query.as_str()),
                        ("fields", "files(id)"),
                        ("pageSize", "1"),
                    ])
            })
            .await?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("read body: {e}")))?;

        if !status.is_success() {
            return Err(CloudHomeError::Storage(format!(
                "list files (HTTP {status}): {body}"
            )));
        }

        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| CloudHomeError::Storage(format!("parse list response: {e}")))?;

        if let Some(files) = json["files"].as_array() {
            if let Some(first) = files.first() {
                if let Some(id) = first["id"].as_str() {
                    return Ok(Some(id.to_string()));
                }
            }
        }

        Ok(None)
    }

    /// Open a resumable upload session and return its session URL (the
    /// `Location` header Google returns). `existing` selects update (PATCH an
    /// existing file id) vs create (POST with metadata).
    async fn open_resumable_session(
        &self,
        key: &str,
        encoded: &str,
        existing: Option<&str>,
    ) -> Result<String, CloudHomeError> {
        let resp = match existing {
            Some(file_id) => {
                let url = format!("{}/files/{}?uploadType=resumable", UPLOAD_API, file_id);
                self.api_call(|token| {
                    self.client
                        .patch(&url)
                        .bearer_auth(token)
                        .header("Content-Type", "application/json; charset=UTF-8")
                        .body("{}")
                })
                .await?
            }
            None => {
                let metadata = serde_json::json!({
                    "name": encoded,
                    "parents": [self.folder_id],
                })
                .to_string();
                self.api_call(|token| {
                    self.client
                        .post(format!("{}/files?uploadType=resumable", UPLOAD_API))
                        .bearer_auth(token)
                        .header("Content-Type", "application/json; charset=UTF-8")
                        .body(metadata.clone())
                })
                .await?
            }
        };

        let status = resp.status();
        if !status.is_success() {
            let op = if existing.is_some() {
                "update"
            } else {
                "create"
            };
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            return Err(classify_write_error(status, &body, key, op));
        }
        resp.headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
            .ok_or_else(|| {
                CloudHomeError::Storage(format!(
                    "resumable session {key}: no Location header returned"
                ))
            })
    }
}

/// First `error.errors[].reason` in a Google API error body (the shape Drive,
/// Sheets, and other googleapis.com endpoints share), or `None` if the body
/// isn't that JSON (HTML error page, empty, malformed). Non-JSON bodies are a
/// common skip (proxy 500s, captive portals) — log at debug so the bail-out
/// is visible without spamming a normal session.
fn parse_google_api_error_reason(body: &str) -> Option<String> {
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!("non-JSON error body, skipping reason extraction: {e}");
            return None;
        }
    };
    v.get("error")?
        .get("errors")?
        .as_array()?
        .first()?
        .get("reason")?
        .as_str()
        .map(String::from)
}

/// Map a Drive write failure to a `CloudHomeError`. The `storageQuotaExceeded`
/// reason gets a message naming the provider and the recovery step; everything
/// else keeps the raw HTTP status + body so transient failures stay debuggable.
fn classify_write_error(
    status: reqwest::StatusCode,
    body: &str,
    key: &str,
    op: &str,
) -> CloudHomeError {
    if status == reqwest::StatusCode::FORBIDDEN
        && parse_google_api_error_reason(body).as_deref() == Some("storageQuotaExceeded")
    {
        return CloudHomeError::Storage(
            "Your Google Drive storage is full. Free up space at drive.google.com to keep syncing."
                .to_string(),
        );
    }
    CloudHomeError::Storage(format!("{op} {key} (HTTP {status}): {body}"))
}

/// Files at or below this size go up as a single media/multipart PUT; larger
/// files use a resumable session so progress advances per chunk. Drive accepts
/// a simple upload up to 5 MB.
const GDRIVE_SIMPLE_UPLOAD_MAX: usize = 4 * 1024 * 1024;

/// Resumable-session chunk size. Drive requires every chunk except the last to
/// be a multiple of 256 KiB; 8 MiB (32 × 256 KiB) keeps the request count low
/// while giving several progress ticks.
const GDRIVE_CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl CloudHome for GoogleDriveCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let encoded = Self::encode_key(key);
        if let Some(file_id) = self.find_file_id(&encoded).await? {
            // Update existing file content.
            let resp = self
                .api_call(|token| {
                    self.client
                        .patch(format!("{}/files/{}?uploadType=media", UPLOAD_API, file_id))
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
                    "update",
                ));
            }
        } else {
            // Create a new file (multipart: metadata + content).
            let metadata = serde_json::json!({
                "name": encoded,
                "parents": [self.folder_id],
            });
            let boundary = "coven_multipart_boundary";
            let mut body = Vec::new();
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            body.extend_from_slice(b"Content-Type: application/json; charset=UTF-8\r\n\r\n");
            body.extend_from_slice(metadata.to_string().as_bytes());
            body.extend_from_slice(b"\r\n");
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
            body.extend_from_slice(&data);
            body.extend_from_slice(b"\r\n");
            body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

            let resp = self
                .api_call(|token| {
                    self.client
                        .post(format!("{}/files?uploadType=multipart", UPLOAD_API))
                        .bearer_auth(token)
                        .header(
                            "Content-Type",
                            format!("multipart/related; boundary={boundary}"),
                        )
                        .body(body.clone())
                })
                .await?;
            let status = resp.status();
            if !status.is_success() {
                return Err(classify_write_error(
                    status,
                    &http::body_text(resp).await,
                    key,
                    "create",
                ));
            }
        }
        Ok(())
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        let encoded = Self::encode_key(key);
        let existing = self.find_file_id(&encoded).await?;
        let op = if existing.is_some() {
            "update"
        } else {
            "create"
        };
        let session_url = self
            .open_resumable_session(key, &encoded, existing.as_deref())
            .await?;
        let key_owned = key.to_string();
        let classify =
            Box::new(move |status, body: &str| classify_write_error(status, body, &key_owned, op));
        // Drive returns 308 Resume Incomplete for every non-final part.
        Ok(Box::new(RangePutSink::new(
            self.client.clone(),
            session_url,
            308,
            total_len,
            GDRIVE_CHUNK_SIZE,
            key.to_string(),
            classify,
        )))
    }

    fn multipart_threshold(&self) -> u64 {
        GDRIVE_SIMPLE_UPLOAD_MAX as u64
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        let encoded = Self::encode_key(key);
        let file_id = self
            .find_file_id(&encoded)
            .await?
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;

        let resp = self
            .api_call(|token| {
                self.client
                    .get(format!("{}/files/{}", DRIVE_API, file_id))
                    .bearer_auth(token)
                    .query(&[("alt", "media")])
            })
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
        let encoded = Self::encode_key(key);
        let file_id = self
            .find_file_id(&encoded)
            .await?
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;

        let range = http::range_header(start, end);

        let resp = self
            .api_call(|token| {
                self.client
                    .get(format!("{}/files/{}", DRIVE_API, file_id))
                    .bearer_auth(token)
                    .query(&[("alt", "media")])
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
        let encoded_prefix = Self::encode_prefix(prefix);

        // Google Drive query: files in our folder whose name starts with the encoded prefix
        let query = format!(
            "'{}' in parents and name contains '{}' and trashed = false",
            self.folder_id, encoded_prefix
        );

        let mut all_keys = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let query_ref = query.clone();
            let page_ref = page_token.clone();

            let resp = self
                .api_call(|token| {
                    let mut req = self
                        .client
                        .get(format!("{}/files", DRIVE_API))
                        .bearer_auth(token)
                        .query(&[
                            ("q", query_ref.as_str()),
                            ("fields", "nextPageToken,files(name)"),
                            ("pageSize", "1000"),
                        ]);
                    if let Some(ref pt) = page_ref {
                        req = req.query(&[("pageToken", pt.as_str())]);
                    }
                    req
                })
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

            if let Some(files) = json["files"].as_array() {
                for file in files {
                    if let Some(name) = file["name"].as_str() {
                        let decoded = Self::decode_key(name);
                        // The `contains` query may match mid-string, so filter to actual prefix
                        if decoded.starts_with(prefix) {
                            all_keys.push(decoded);
                        }
                    }
                }
            }

            if let Some(next) = json["nextPageToken"].as_str() {
                page_token = Some(next.to_string());
            } else {
                break;
            }
        }

        Ok(all_keys)
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        let encoded = Self::encode_key(key);

        if let Some(file_id) = self.find_file_id(&encoded).await? {
            let resp = self
                .api_call(|token| {
                    self.client
                        .delete(format!("{}/files/{}", DRIVE_API, file_id))
                        .bearer_auth(token)
                })
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
        }

        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        let encoded = Self::encode_key(key);
        Ok(self.find_file_id(&encoded).await?.is_some())
    }

    async fn grant_access(&self, member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        // Share the folder with the member's Google account
        let permission = serde_json::json!({
            "type": "user",
            "role": "writer",
            "emailAddress": member_id,
        });

        let resp = self
            .api_call(|token| {
                self.client
                    .post(format!(
                        "{}/files/{}/permissions",
                        DRIVE_API, self.folder_id
                    ))
                    .bearer_auth(token)
                    .json(&permission)
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

        Ok(CloudHomeJoinInfo::GoogleDrive {
            folder_id: self.folder_id.clone(),
        })
    }

    async fn revoke_access(&self, member_id: &str) -> Result<(), CloudHomeError> {
        // First, find the permission ID for this member
        let resp = self
            .api_call(|token| {
                self.client
                    .get(format!(
                        "{}/files/{}/permissions",
                        DRIVE_API, self.folder_id
                    ))
                    .bearer_auth(token)
                    .query(&[("fields", "permissions(id,emailAddress)")])
            })
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

        let permission_id = json["permissions"]
            .as_array()
            .and_then(|perms| {
                perms.iter().find_map(|p| {
                    if p["emailAddress"].as_str() == Some(member_id) {
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
        let resp = self
            .api_call(|token| {
                self.client
                    .delete(format!(
                        "{}/files/{}/permissions/{}",
                        DRIVE_API, self.folder_id, permission_id
                    ))
                    .bearer_auth(token)
            })
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

    #[test]
    fn encode_key_replaces_slashes() {
        assert_eq!(
            GoogleDriveCloudHome::encode_key("changes/dev1/42.enc"),
            "changes__dev1__42.enc"
        );
    }

    #[test]
    fn decode_key_restores_slashes() {
        assert_eq!(
            GoogleDriveCloudHome::decode_key("changes__dev1__42.enc"),
            "changes/dev1/42.enc"
        );
    }

    #[test]
    fn encode_decode_roundtrip() {
        let keys = [
            "snapshot/abc/0.db.enc",
            "snapshot/current.json.enc",
            "changes/device-abc/1.enc",
            "heads/device-abc.json.enc",
            "images/cover.jpg",
        ];
        for key in keys {
            let encoded = GoogleDriveCloudHome::encode_key(key);
            let decoded = GoogleDriveCloudHome::decode_key(&encoded);
            assert_eq!(decoded, key);
        }
    }

    #[test]
    fn encode_prefix_for_query() {
        assert_eq!(
            GoogleDriveCloudHome::encode_prefix("changes/dev1/"),
            "changes__dev1__"
        );
    }

    #[test]
    fn parse_google_api_error_reason_extracts_storage_quota() {
        let body = r#"{"error":{"code":403,"message":"quota","errors":[{"domain":"usageLimits","reason":"storageQuotaExceeded","message":"full"}]}}"#;
        assert_eq!(
            parse_google_api_error_reason(body).as_deref(),
            Some("storageQuotaExceeded"),
        );
    }

    #[test]
    fn parse_google_api_error_reason_returns_none_for_non_drive_body() {
        assert!(parse_google_api_error_reason("<html>500</html>").is_none());
        assert!(parse_google_api_error_reason("{}").is_none());
        assert!(parse_google_api_error_reason(r#"{"error":"flat"}"#).is_none());
    }

    #[test]
    fn classify_write_error_quota_message_names_provider_and_recovery() {
        let body = r#"{"error":{"code":403,"errors":[{"reason":"storageQuotaExceeded"}]}}"#;
        let err = classify_write_error(reqwest::StatusCode::FORBIDDEN, body, "k", "create");
        let msg = err.to_string();
        assert!(
            msg.contains("Google Drive storage is full"),
            "missing provider+state: {msg}",
        );
        assert!(
            msg.contains("Free up space"),
            "missing recovery step: {msg}"
        );
    }

    #[test]
    fn classify_write_error_keeps_raw_for_non_quota_errors() {
        let body = r#"{"error":{"code":500,"message":"server error"}}"#;
        let err = classify_write_error(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            body,
            "blobs/aa/bb/cc",
            "create",
        );
        let msg = err.to_string();
        assert!(msg.contains("HTTP 500"), "missing HTTP status: {msg}");
        assert!(msg.contains("blobs/aa/bb/cc"), "missing key: {msg}");
        assert!(
            !msg.contains("storage is full"),
            "should not match the quota message: {msg}",
        );
    }
}
