//! Google Drive `CloudHome` implementation.
//!
//! Uses the Google Drive REST API v3 with OAuth 2.0 tokens. Files are stored flat
//! in a single folder — path separators are encoded as `__` (see
//! [`super::key_encoding`]). The `read`/`read_range`/`list`/`delete` methods are
//! the shared [`OAuthRestHome`] implementations; this file supplies only the Drive
//! request shapes, the page parser, the upload paths, and sharing.

use async_trait::async_trait;

use super::http::{self, ensure_ok, ok_json, NotFound};
use super::key_encoding::{decode_key, encode_key};
use super::oauth_rest::{
    rest_delete, rest_list, rest_read, rest_read_range, ListPage, OAuthRestHome,
};
use super::oauth_session::OAuthSession;
use super::resumable::RangePutSink;
use super::{
    sharing, BoxPartSink, CloudAccessGrant, CloudAccessRevoke, CloudHome, CloudHomeError,
    CloudHomeJoinInfo,
};
use crate::clock::ClockRef;
use crate::keys::KeyService;
use crate::oauth::{OAuthConfig, OAuthTokens};

const DRIVE_API: &str = "https://www.googleapis.com/drive/v3";
const UPLOAD_API: &str = "https://www.googleapis.com/upload/drive/v3";

/// Google Drive cloud home backend.
pub struct GoogleDriveCloudHome {
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
            scopes: vec![
                "https://www.googleapis.com/auth/drive.file".to_string(),
                // Lets the joiner fetch its account email for OAuth folder sharing.
                "https://www.googleapis.com/auth/userinfo.email".to_string(),
            ],
            redirect_port: 19284,
            extra_auth_params: vec![("access_type".to_string(), "offline".to_string())],
        }
    }

    /// The Drive HTTP client (shared, owned by the session).
    fn client(&self) -> &reqwest::Client {
        self.session.client()
    }

    /// Find a file's Google Drive ID by name within our folder.
    async fn find_file_id(&self, encoded_name: &str) -> Result<Option<String>, CloudHomeError> {
        let query = format!(
            "'{}' in parents and name = '{}' and trashed = false",
            self.folder_id, encoded_name
        );
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!("{}/files", DRIVE_API))
                    .bearer_auth(token)
                    .query(&[
                        ("q", query.as_str()),
                        ("fields", "files(id)"),
                        ("pageSize", "1"),
                    ])
            })
            .await?;
        let resp = ensure_ok(resp, "list files", NotFound::Status).await?;
        let json: serde_json::Value = ok_json(resp, "parse list response").await?;
        Ok(json["files"]
            .as_array()
            .and_then(|files| files.first())
            .and_then(|first| first["id"].as_str())
            .map(String::from))
    }

    /// Open a resumable upload session and return its session URL (the `Location`
    /// header Google returns). `existing` selects update (PATCH an existing file
    /// id) vs create (POST with metadata).
    async fn open_resumable_session(
        &self,
        key: &str,
        encoded: &str,
        existing: Option<&str>,
    ) -> Result<String, CloudHomeError> {
        let resp = match existing {
            Some(file_id) => {
                let url = format!("{}/files/{}?uploadType=resumable", UPLOAD_API, file_id);
                self.session
                    .api_call(|token| {
                        self.client()
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
                self.session
                    .api_call(|token| {
                        self.client()
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
            return Err(classify_write_error(
                status,
                &http::body_text(resp).await,
                key,
                op,
            ));
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
/// Sheets, and other googleapis.com endpoints share), or `None` if the body isn't
/// that JSON.
fn parse_google_api_error_reason(body: &str) -> Option<String> {
    http::error_reason(body, |v| {
        v.get("error")?
            .get("errors")?
            .as_array()?
            .first()?
            .get("reason")?
            .as_str()
            .map(String::from)
    })
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

/// Files at or below this size go up as a single media/multipart PUT; larger files
/// use a resumable session. Drive accepts a simple upload up to 5 MB.
const GDRIVE_SIMPLE_UPLOAD_MAX: usize = 4 * 1024 * 1024;

/// Resumable-session part size. Drive requires every part except the last to be a
/// multiple of 256 KiB; 8 MiB (32 × 256 KiB) keeps the request count low.
const GDRIVE_CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl OAuthRestHome for GoogleDriveCloudHome {
    fn not_found(&self) -> NotFound<'_> {
        NotFound::Status
    }

    async fn send_read(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        let file_id = self
            .find_file_id(&encode_key(key))
            .await?
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
        let range = range.map(|(start, end)| super::range_header(start, end));
        self.session
            .api_call(|token| {
                let mut req = self
                    .client()
                    .get(format!("{}/files/{}", DRIVE_API, file_id))
                    .bearer_auth(token)
                    .query(&[("alt", "media")]);
                if let Some(ref range) = range {
                    req = req.header("Range", range);
                }
                req
            })
            .await
    }

    async fn send_delete(&self, key: &str) -> Result<reqwest::Response, CloudHomeError> {
        // No file id ⇒ already absent; surface as not-found so `rest_delete` treats
        // it as success.
        let file_id = self
            .find_file_id(&encode_key(key))
            .await?
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
        self.session
            .api_call(|token| {
                self.client()
                    .delete(format!("{}/files/{}", DRIVE_API, file_id))
                    .bearer_auth(token)
            })
            .await
    }

    async fn send_list_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        let query = format!(
            "'{}' in parents and name contains '{}' and trashed = false",
            self.folder_id,
            encode_key(prefix)
        );
        let page = cursor.map(str::to_string);
        self.session
            .api_call(|token| {
                let mut req = self
                    .client()
                    .get(format!("{}/files", DRIVE_API))
                    .bearer_auth(token)
                    .query(&[
                        ("q", query.as_str()),
                        ("fields", "nextPageToken,files(name)"),
                        ("pageSize", "1000"),
                    ]);
                if let Some(ref pt) = page {
                    req = req.query(&[("pageToken", pt.as_str())]);
                }
                req
            })
            .await
    }

    fn parse_list_page(&self, body: &str, prefix: &str) -> Result<ListPage, CloudHomeError> {
        let json: serde_json::Value = serde_json::from_str(body)
            .map_err(|e| CloudHomeError::Storage(format!("parse list: {e}")))?;
        let mut keys = Vec::new();
        if let Some(files) = json["files"].as_array() {
            for file in files {
                if let Some(name) = file["name"].as_str() {
                    let decoded = decode_key(name);
                    // The `contains` query may match mid-string, so filter to the
                    // actual prefix.
                    if decoded.starts_with(prefix) {
                        keys.push(decoded);
                    }
                }
            }
        }
        Ok(ListPage {
            keys,
            next: json["nextPageToken"].as_str().map(String::from),
        })
    }
}

#[async_trait]
impl CloudHome for GoogleDriveCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let encoded = encode_key(key);
        if let Some(file_id) = self.find_file_id(&encoded).await? {
            // Update existing file content.
            let resp = self
                .session
                .api_call(|token| {
                    self.client()
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
                .session
                .api_call(|token| {
                    self.client()
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
        let encoded = encode_key(key);
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
            self.client().clone(),
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
        // A name query confirms existence in one request; the generic 2xx/404 rule
        // doesn't fit (the query returns 200 with an empty array for an absent key).
        Ok(self.find_file_id(&encode_key(key)).await?.is_some())
    }

    async fn grant_access(
        &self,
        grant: CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        let email = grant.require_provider_email("Google Drive")?;
        // Share the folder with the member's Google account.
        let permission = serde_json::json!({
            "type": "user",
            "role": "writer",
            "emailAddress": email,
        });
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!(
                        "{}/files/{}/permissions",
                        DRIVE_API, self.folder_id
                    ))
                    .bearer_auth(token)
                    .json(&permission)
            })
            .await?;
        ensure_ok(resp, &format!("grant access to {email}"), NotFound::Status).await?;
        Ok(CloudHomeJoinInfo::GoogleDrive {
            folder_id: self.folder_id.clone(),
        })
    }

    async fn revoke_access(&self, revoke: CloudAccessRevoke) -> Result<(), CloudHomeError> {
        let email = revoke.require_provider_email("Google Drive")?;
        let list_url = format!(
            "{}/files/{}/permissions?fields=permissions(id,emailAddress)",
            DRIVE_API, self.folder_id
        );
        let folder_id = self.folder_id.clone();
        sharing::revoke_by_email(
            &self.session,
            email,
            &list_url,
            "permissions",
            |p| p["emailAddress"].as_str().map(String::from),
            |perm_id| format!("{}/files/{}/permissions/{}", DRIVE_API, folder_id, perm_id),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
