//! Dropbox `CloudHome` implementation.
//!
//! Uses the Dropbox HTTP API v2 with OAuth 2.0 (PKCE) tokens. Files live under a
//! folder using path-based access — no filename encoding. The
//! `read`/`read_range`/`list`/`delete` methods are the shared [`OAuthRestHome`]
//! implementations; this file supplies only the Dropbox request shapes (POST with
//! a `Dropbox-API-Arg` header), the page parser, the upload session, and sharing.

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::StatusCode;
use std::fmt::Write as _;

use super::http::{self, ensure_ok, exists_from_response, NotFound};
use super::oauth_rest::{
    rest_delete, rest_list, rest_read, rest_read_range, rest_read_to_file, ListPage, OAuthRestHome,
};
use super::oauth_session::OAuthSession;
use super::{
    BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeError,
    CloudHomeJoinInfo, RevokeOutcome,
};
use crate::clock::ClockRef;
use crate::keys::StoreKeys;
use crate::oauth::{OAuthConfig, OAuthTokens};
use tracing::warn;

const API_BASE: &str = "https://api.dropboxapi.com/2";
const CONTENT_BASE: &str = "https://content.dropboxapi.com/2";

/// Dropbox cloud home backend.
pub(crate) struct DropboxCloudHome {
    /// Folder path in Dropbox, e.g. "/Apps/your-app/my-store"
    folder_path: String,
    session: OAuthSession,
}

impl DropboxCloudHome {
    pub(crate) fn new(
        folder_path: String,
        tokens: OAuthTokens,
        key_service: StoreKeys,
        clock: ClockRef,
    ) -> Result<Self, CloudHomeError> {
        let config =
            Self::oauth_config().map_err(|e| CloudHomeError::Configuration(e.to_string()))?;
        Ok(Self {
            folder_path,
            session: OAuthSession::new(tokens, key_service, clock, config, "Dropbox"),
        })
    }

    /// The join info a device needs to reach this same folder: the path files
    /// are read and written under — never the Dropbox sharing API's
    /// `shared_folder_id` (a member-management handle for that API, unrelated
    /// to locating a file).
    fn join_info(&self) -> CloudHomeJoinInfo {
        CloudHomeJoinInfo::Dropbox {
            folder_path: self.folder_path.clone(),
        }
    }

    pub(crate) fn oauth_config() -> Result<OAuthConfig, crate::oauth::OAuthClientCredsError> {
        let creds = crate::oauth::oauth_client_creds("dropbox")?;
        Ok(OAuthConfig {
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
        })
    }

    fn client(&self) -> &reqwest::Client {
        self.session.client()
    }

    /// Build the full Dropbox path for a key.
    /// `objects/dev1/42.enc` -> `/Apps/your-app/my-store/objects/dev1/42.enc`
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
            CloudHomeError::Transport(format!(
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
            return Err(CloudHomeError::Transport(format!(
                "share folder (HTTP {status}): {resp_body}"
            )));
        }
        Err(CloudHomeError::Transport(
            "could not determine shared_folder_id".to_string(),
        ))
    }

    /// Poll `check_share_job_status` until the share operation completes.
    async fn poll_share_job(&self, job_id: &str) -> Result<String, CloudHomeError> {
        self.poll_dropbox_job(
            job_id,
            "sharing/check_share_job_status",
            "share folder",
            |json| {
                json["shared_folder_id"]
                    .as_str()
                    .map(String::from)
                    .ok_or_else(|| {
                        CloudHomeError::Transport(
                            "share job completed but no shared_folder_id".to_string(),
                        )
                    })
            },
        )
        .await
    }

    async fn poll_remove_member_job(&self, job_id: &str) -> Result<(), CloudHomeError> {
        self.poll_dropbox_job(
            job_id,
            "sharing/check_remove_member_job_status",
            "remove folder member",
            |_| Ok(()),
        )
        .await
    }

    async fn poll_dropbox_job<T>(
        &self,
        job_id: &str,
        endpoint: &str,
        operation: &str,
        complete: impl Fn(&serde_json::Value) -> Result<T, CloudHomeError>,
    ) -> Result<T, CloudHomeError> {
        let request_body = serde_json::json!({ "async_job_id": job_id });
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let resp = self
                .session
                .api_call(|token| {
                    self.client()
                        .post(format!("{API_BASE}/{endpoint}"))
                        .bearer_auth(token)
                        .json(&request_body)
                })
                .await?;
            let status = resp.status();
            let resp_body = http::body_text(resp).await;
            if !status.is_success() {
                return Err(CloudHomeError::Transport(format!(
                    "{operation} job status (HTTP {status}): {resp_body}"
                )));
            }
            let json: serde_json::Value = serde_json::from_str(&resp_body).map_err(|e| {
                CloudHomeError::Transport(format!(
                    "{operation} job status: unparseable response: {e}: {resp_body}"
                ))
            })?;
            match json[".tag"].as_str() {
                Some("complete") => return complete(&json),
                Some("failed") => {
                    return Err(CloudHomeError::Transport(format!(
                        "{operation} job failed: {resp_body}"
                    )));
                }
                Some("in_progress") => continue,
                _ => {
                    return Err(CloudHomeError::Transport(format!(
                        "{operation} job status returned an unexpected tag: {resp_body}"
                    )));
                }
            }
        }
        Err(CloudHomeError::Transport(format!(
            "{operation} timed out after 30 seconds"
        )))
    }

    async fn folder_member_access(
        &self,
        shared_folder_id: &str,
        email: &str,
    ) -> Result<Option<String>, CloudHomeError> {
        let mut endpoint = "sharing/list_folder_members";
        let mut request = serde_json::json!({
            "shared_folder_id": shared_folder_id,
            "include_inherited": false,
            "limit": 1000,
        });
        loop {
            let resp = self
                .session
                .api_call(|token| {
                    self.client()
                        .post(format!("{API_BASE}/{endpoint}"))
                        .bearer_auth(token)
                        .json(&request)
                })
                .await?;
            let resp = ensure_ok(resp, "list folder members", self.not_found()).await?;
            let body: serde_json::Value = http::ok_json(resp, "parse folder members").await?;
            for (array, identity_field) in [("users", "user"), ("invitees", "invitee")] {
                if let Some(access) = body[array].as_array().and_then(|members| {
                    members.iter().find_map(|member| {
                        let member_email = member[identity_field]["email"].as_str()?;
                        member_email
                            .eq_ignore_ascii_case(email)
                            .then(|| member["access_type"][".tag"].as_str().map(str::to_string))?
                    })
                }) {
                    return Ok(Some(access));
                }
            }
            let Some(cursor) = body["cursor"].as_str() else {
                return Ok(None);
            };
            endpoint = "sharing/list_folder_members/continue";
            request = serde_json::json!({ "cursor": cursor });
        }
    }

    async fn remove_folder_member(
        &self,
        shared_folder_id: &str,
        email: &str,
    ) -> Result<(), CloudHomeError> {
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
        let body = http::body_text(resp).await;
        if !status.is_success() {
            if dropbox_revoke_error_is_already_absent(&body) {
                return Ok(());
            }
            return Err(CloudHomeError::Transport(format!(
                "revoke access for {email} (HTTP {status}): {body}"
            )));
        }
        match parse_dropbox_revoke_launch(&body)? {
            DropboxRevokeLaunch::Complete => Ok(()),
            DropboxRevokeLaunch::AsyncJob(job_id) => self.poll_remove_member_job(&job_id).await,
        }
    }
}

enum DropboxRevokeLaunch {
    Complete,
    AsyncJob(String),
}

fn parse_dropbox_revoke_launch(body: &str) -> Result<DropboxRevokeLaunch, CloudHomeError> {
    let json: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        CloudHomeError::Transport(format!("revoke access: unparseable response: {e}: {body}"))
    })?;
    match json[".tag"].as_str() {
        Some("complete") => Ok(DropboxRevokeLaunch::Complete),
        Some("async_job_id") => json["async_job_id"]
            .as_str()
            .map(|id| DropboxRevokeLaunch::AsyncJob(id.to_string()))
            .ok_or_else(|| {
                CloudHomeError::Transport(format!(
                    "revoke access: async_job_id response missing async_job_id: {body}"
                ))
            }),
        _ => Err(CloudHomeError::Transport(format!(
            "revoke access: unexpected launch response: {body}"
        ))),
    }
}

fn dropbox_api_arg(value: &serde_json::Value) -> String {
    let json = value.to_string();
    let mut arg = String::with_capacity(json.len());
    for c in json.chars() {
        if c.is_ascii() {
            arg.push(c);
        } else {
            let mut units = [0u16; 2];
            for unit in c.encode_utf16(&mut units) {
                write!(&mut arg, "\\u{unit:04x}").expect("writing to a String cannot fail");
            }
        }
    }
    arg
}

fn strip_dropbox_folder_prefix<'a>(path_display: &'a str, folder_path: &str) -> Option<&'a str> {
    let folder_prefix = path_display.get(..folder_path.len())?;
    if !folder_prefix.eq_ignore_ascii_case(folder_path) {
        return None;
    }
    path_display.get(folder_path.len()..)?.strip_prefix('/')
}

fn dropbox_revoke_error_is_already_absent(body: &str) -> bool {
    parse_dropbox_error_summary(body)
        .as_deref()
        .is_some_and(|summary| summary.starts_with("member_error/not_a_member"))
}

/// A [`PartSink`](super::PartSink) over a Dropbox upload session: `append_v2` adds
/// each non-final part at its byte offset; the final part is committed to the
/// destination path via `upload_session/finish` (overwrite mode). The session was
/// opened by `open_multipart`; `finish` is a no-op (the last part committed).
///
/// Parts go through `api_call_no_transient_retry`: a successful `append_v2`
/// advances the session's expected offset, so a blind re-send after a lost
/// response would collide with that offset. A failed part instead fails the whole
/// upload, which the blob engine re-runs from the source.
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
            let arg = dropbox_api_arg(&serde_json::json!({
                "cursor": { "session_id": self.session_id, "offset": offset },
                "commit": {
                    "path": path,
                    "mode": { ".tag": "overwrite" },
                    "autorename": false,
                    "mute": true,
                },
            }));
            self.home
                .session
                .api_call_no_transient_retry(|token| {
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
            let arg = dropbox_api_arg(&serde_json::json!({
                "cursor": { "session_id": self.session_id, "offset": offset },
                "close": false,
            }));
            self.home
                .session
                .api_call_no_transient_retry(|token| {
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
            return CloudHomeError::Transport(
                "Your Dropbox storage is full. Free up space at dropbox.com to keep syncing."
                    .to_string(),
            );
        }
    }
    CloudHomeError::Transport(format!("write {key} (HTTP {status}): {body}"))
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
        let arg = dropbox_api_arg(&serde_json::json!({ "path": self.full_path(key) }));
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
            .map_err(|e| CloudHomeError::Transport(format!("parse list: {e}")))?;
        let mut keys = Vec::new();
        if let Some(entries) = json["entries"].as_array() {
            for entry in entries {
                if entry[".tag"].as_str() != Some("file") {
                    continue;
                }
                let Some(path_display) = entry["path_display"].as_str() else {
                    warn!("skipping Dropbox file entry without path_display: {entry}");
                    continue;
                };
                let Some(key) = strip_dropbox_folder_prefix(path_display, &self.folder_path) else {
                    warn!(
                        folder_path = %self.folder_path,
                        path_display,
                        "skipping Dropbox file entry outside the configured folder"
                    );
                    continue;
                };
                if key.starts_with(prefix) {
                    keys.push(key.to_string());
                }
            }
        }
        let next = if json["has_more"].as_bool() == Some(true) {
            Some(
                json["cursor"]
                    .as_str()
                    .ok_or_else(|| {
                        CloudHomeError::Transport(
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
        let api_arg = dropbox_api_arg(&serde_json::json!({
            "path": self.full_path(key),
            "mode": { ".tag": "overwrite" },
            "autorename": false,
            "mute": true,
        }));
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
            .map_err(|e| CloudHomeError::Transport(format!("parse upload session {key}: {e}")))?;
        let session_id = start_json["session_id"]
            .as_str()
            .ok_or_else(|| {
                CloudHomeError::Transport(format!("upload session {key}: no session_id returned"))
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

    async fn read_appended_to_file(
        &self,
        object: &super::AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        rest_read_to_file(self, object.logical_key(), destination).await
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

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        let email = desired.require_provider_email("Dropbox")?.to_string();
        let shared_folder_id = self.get_or_create_shared_folder_id().await?;
        match desired {
            CloudAccessState::Present { .. } => {
                let current = self.folder_member_access(&shared_folder_id, &email).await?;
                if current.as_deref() != Some("editor") {
                    if current.is_some() {
                        self.remove_folder_member(&shared_folder_id, &email).await?;
                    }
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
                }
                if self
                    .folder_member_access(&shared_folder_id, &email)
                    .await?
                    .as_deref()
                    != Some("editor")
                {
                    return Err(CloudHomeError::Transport(format!(
                        "editor access for {email} is not visible after update"
                    )));
                }
                Ok(CloudAccessOutcome::Present(self.join_info()))
            }
            CloudAccessState::Absent { .. } => {
                if self
                    .folder_member_access(&shared_folder_id, &email)
                    .await?
                    .is_some()
                {
                    self.remove_folder_member(&shared_folder_id, &email).await?;
                }
                if self
                    .folder_member_access(&shared_folder_id, &email)
                    .await?
                    .is_some()
                {
                    return Err(CloudHomeError::Transport(format!(
                        "access for {email} remains after removal"
                    )));
                }
                Ok(CloudAccessOutcome::Absent(RevokeOutcome::Revoked))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn home() -> DropboxCloudHome {
        home_with_folder("/Apps/your-app/my-store")
    }

    fn home_with_folder(folder_path: &str) -> DropboxCloudHome {
        crate::oauth::install_test_client_creds();
        DropboxCloudHome::new(
            folder_path.to_string(),
            OAuthTokens {
                access_token: String::new(),
                refresh_token: None,
                expires_at: None,
            },
            StoreKeys::new("test".to_string()),
            Arc::new(crate::clock::SystemClock),
        )
        .expect("build test Dropbox home")
    }

    /// `join_info()` (what `grant_access` returns to a newly-added member) must
    /// carry the folder *path* files are read/written under — the same value
    /// passed to `DropboxCloudHome::new` — not Dropbox's own sharing-API
    /// `shared_folder_id`, which is an unrelated member-management handle a
    /// joiner's `full_path` could never resolve against.
    #[test]
    fn join_info_carries_the_folder_path() {
        let home = home_with_folder("/Apps/your-app/my-store");
        match home.join_info() {
            CloudHomeJoinInfo::Dropbox { folder_path } => {
                assert_eq!(folder_path, "/Apps/your-app/my-store");
            }
            other => panic!("expected Dropbox join info, got {other:?}"),
        }
    }

    #[test]
    fn full_path_joins_correctly() {
        assert_eq!(
            home().full_path("objects/dev1/42.enc"),
            "/Apps/your-app/my-store/objects/dev1/42.enc"
        );
    }

    #[test]
    fn dropbox_api_arg_escapes_non_ascii_for_headers() {
        let path = "/Apps/your-app/Folderé/Object🧪.enc";
        let arg = dropbox_api_arg(&serde_json::json!({ "path": path }));

        assert!(arg.is_ascii(), "Dropbox-API-Arg must be ASCII: {arg}");
        assert!(arg.contains(r"\u00e9"), "missing BMP escape: {arg}");
        assert!(
            arg.contains(r"\ud83e\uddea"),
            "missing surrogate-pair escape: {arg}",
        );
        assert!(reqwest::header::HeaderValue::from_str(&arg).is_ok());

        let parsed: serde_json::Value = serde_json::from_str(&arg).expect("parse escaped JSON");
        assert_eq!(parsed["path"].as_str(), Some(path));
    }

    #[test]
    fn oauth_config_uses_dropbox_urls() {
        crate::oauth::install_test_client_creds();
        let config = DropboxCloudHome::oauth_config().expect("build Dropbox oauth config");
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
            classify_write_error(reqwest::StatusCode::INSUFFICIENT_STORAGE, body, "objects/1");
        let msg = err.to_string();
        assert!(msg.contains("Dropbox storage is full"), "{msg}");
        assert!(msg.contains("Free up space"), "{msg}");
    }

    #[test]
    fn classify_write_error_keeps_raw_for_non_quota_errors() {
        let body = r#"{"error_summary":"path/conflict/file","error":{}}"#;
        let err = classify_write_error(reqwest::StatusCode::CONFLICT, body, "objects/dev1/1.enc");
        let msg = err.to_string();
        assert!(msg.contains("HTTP 409"), "{msg}");
        assert!(msg.contains("objects/dev1/1.enc"), "{msg}");
        assert!(!msg.contains("storage is full"), "{msg}");
    }

    #[test]
    fn revoke_error_accepts_only_not_a_member_as_absent() {
        let absent = r#"{
            "error_summary": "member_error/not_a_member/...",
            "error": {
                ".tag": "member_error",
                "member_error": { ".tag": "not_a_member" }
            }
        }"#;
        assert!(dropbox_revoke_error_is_already_absent(absent));

        let ambiguous = r#"{
            "error_summary": "member_error/invalid_dropbox_id/...",
            "error": {
                ".tag": "member_error",
                "member_error": { ".tag": "invalid_dropbox_id" }
            }
        }"#;
        assert!(!dropbox_revoke_error_is_already_absent(ambiguous));
    }

    #[test]
    fn revoke_launch_response_requires_completion_or_polling() {
        assert!(matches!(
            parse_dropbox_revoke_launch(r#"{".tag":"complete"}"#).expect("parse complete launch"),
            DropboxRevokeLaunch::Complete,
        ));

        match parse_dropbox_revoke_launch(r#"{".tag":"async_job_id","async_job_id":"job-123"}"#)
            .expect("parse async launch")
        {
            DropboxRevokeLaunch::AsyncJob(job_id) => assert_eq!(job_id, "job-123"),
            DropboxRevokeLaunch::Complete => panic!("async launch must carry the job id"),
        }

        assert!(parse_dropbox_revoke_launch(r#"{".tag":"async_job_id"}"#).is_err(),);
    }

    #[test]
    fn parse_list_page_rejects_has_more_without_cursor() {
        let body = r#"{"entries":[],"has_more":true}"#;
        match home().parse_list_page(body, "") {
            Ok(_) => panic!("has_more without cursor must fail"),
            Err(err) => assert!(err.to_string().contains("cursor"), "{err}"),
        }
    }

    #[test]
    fn parse_list_page_strips_non_ascii_folder_prefix() {
        assert_list_page_strips_folder_prefix(
            "/Apps/your-app/Folderé",
            "/apps/your-app/folderé/objects/dev1/1.enc",
            "/Apps/your-app/Folderé/objects/dev1/1.enc",
        );
    }

    #[test]
    fn parse_list_page_strips_prefix_without_lowercase_length_drift() {
        assert_list_page_strips_folder_prefix(
            "/Apps/your-app/İlib",
            "/apps/your-app/i̇lib/objects/dev1/1.enc",
            "/Apps/your-app/İlib/objects/dev1/1.enc",
        );
    }

    fn assert_list_page_strips_folder_prefix(
        folder_path: &str,
        path_lower: &str,
        path_display: &str,
    ) {
        let home = home_with_folder(folder_path);
        let body = serde_json::json!({
            "entries": [{
                ".tag": "file",
                "path_lower": path_lower,
                "path_display": path_display,
            }],
            "has_more": false,
        })
        .to_string();
        let page = home
            .parse_list_page(&body, "objects/")
            .expect("parse list page");

        assert_eq!(page.keys, vec!["objects/dev1/1.enc"]);
    }
}
