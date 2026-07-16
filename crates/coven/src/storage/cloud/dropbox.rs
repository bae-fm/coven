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
use std::collections::HashMap;
use std::fmt::Write as _;

use super::http::{self, ensure_ok, exists_from_response, NotFound};
use super::oauth_rest::{
    rest_delete, rest_list, rest_read, rest_read_range, ListPage, OAuthRestHome, PageTokenTracker,
};
use super::oauth_session::OAuthSession;
use super::{
    AppendedListing, AppendedObject, BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState,
    CloudHome, CloudHomeError, CloudHomeJoinInfo, ImmutableCopyStorage, ListingCoverage,
    RevokeOutcome, UploadProgress,
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
    api_base: String,
    content_base: String,
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
            api_base: API_BASE.to_string(),
            content_base: CONTENT_BASE.to_string(),
            session: OAuthSession::new(tokens, key_service, clock, config, "Dropbox"),
        })
    }

    #[cfg(test)]
    fn with_endpoints(mut self, api_base: String, content_base: String) -> Self {
        self.api_base = api_base;
        self.content_base = content_base;
        self
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

    async fn start_upload_session(&self, key: &str) -> Result<String, CloudHomeError> {
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/files/upload_session/start", self.content_base))
                    .bearer_auth(token)
                    .header("Dropbox-API-Arg", r#"{"close":false}"#)
                    .header("Content-Type", "application/octet-stream")
                    .body(Vec::new())
            })
            .await?;
        let status = response.status();
        let body = http::body_text(response).await;
        if !status.is_success() {
            return Err(classify_write_error(status, &body, key));
        }
        let json: serde_json::Value = serde_json::from_str(&body).map_err(|error| {
            CloudHomeError::Transport(format!("parse upload session {key}: {error}"))
        })?;
        json["session_id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                CloudHomeError::Transport(format!("upload session {key}: no session_id returned"))
            })
    }

    async fn append_small(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<AppendedObject, CloudHomeError> {
        let body = Bytes::from(data);
        let api_arg = dropbox_api_arg(&serde_json::json!({
            "path": self.full_path(key),
                    "mode": { ".tag": "add" },
                    "autorename": false,
                    "strict_conflict": true,
                    "mute": true,
        }));
        let response = self
            .session
            .api_call_no_transient_retry(|token| {
                self.client()
                    .post(format!("{}/files/upload", self.content_base))
                    .bearer_auth(token)
                    .header("Dropbox-API-Arg", &api_arg)
                    .header("Content-Type", "application/octet-stream")
                    .body(body.clone())
            })
            .await?;
        self.appended_object_from_response(key, response).await
    }

    async fn appended_object_from_response(
        &self,
        key: &str,
        response: reqwest::Response,
    ) -> Result<AppendedObject, CloudHomeError> {
        let status = response.status();
        let body = http::body_text(response).await;
        if !status.is_success() {
            return Err(classify_write_error(status, &body, key));
        }
        let json: serde_json::Value = serde_json::from_str(&body).map_err(|error| {
            CloudHomeError::Transport(format!("append {key}: parse response: {error}"))
        })?;
        let id = json["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport(format!("append {key}: response missing file id"))
            })?;
        AppendedObject::from_provider(key.to_string(), id.to_string())
    }

    async fn send_exact_read(
        &self,
        object: &AppendedObject,
    ) -> Result<reqwest::Response, CloudHomeError> {
        let provider_id = object.opaque_provider_id();
        let api_arg = dropbox_api_arg(&serde_json::json!({"path": provider_id}));
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/files/download", self.content_base))
                    .bearer_auth(token)
                    .header("Dropbox-API-Arg", &api_arg)
            })
            .await?;
        let response = ensure_ok(response, "read exact Dropbox file", self.not_found()).await?;
        let metadata = response
            .headers()
            .get("Dropbox-API-Result")
            .ok_or_else(|| {
                CloudHomeError::Transport(format!(
                    "exact Dropbox read for {} omitted Dropbox-API-Result",
                    object.logical_key()
                ))
            })?
            .to_str()
            .map_err(|error| {
                CloudHomeError::Transport(format!(
                    "exact Dropbox read for {} returned invalid metadata: {error}",
                    object.logical_key()
                ))
            })?;
        let metadata: serde_json::Value = serde_json::from_str(metadata).map_err(|error| {
            CloudHomeError::Transport(format!(
                "exact Dropbox read for {} returned malformed metadata: {error}",
                object.logical_key()
            ))
        })?;
        self.validate_appended_metadata(object, &metadata)?;
        Ok(response)
    }

    fn validate_appended_metadata(
        &self,
        object: &AppendedObject,
        metadata: &serde_json::Value,
    ) -> Result<(), CloudHomeError> {
        let expected_path = self.full_path(object.logical_key());
        let matches = metadata[".tag"].as_str() == Some("file")
            && metadata["id"].as_str() == Some(object.opaque_provider_id())
            && metadata["path_display"].as_str() == Some(expected_path.as_str());
        if !matches {
            return Err(CloudHomeError::Transport(format!(
                "exact Dropbox locator for {} does not identify {} at {expected_path}",
                object.logical_key(),
                object.opaque_provider_id()
            )));
        }
        Ok(())
    }

    async fn verify_appended_object(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        let body = serde_json::json!({"path": object.opaque_provider_id()});
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/files/get_metadata", self.api_base))
                    .bearer_auth(token)
                    .json(&body)
            })
            .await?;
        let response = ensure_ok(response, "verify exact Dropbox file", self.not_found()).await?;
        let metadata: serde_json::Value =
            http::ok_json(response, "parse exact Dropbox metadata").await?;
        self.validate_appended_metadata(object, &metadata)
    }

    /// Call `share_folder` and resolve the shared_folder_id, handling both
    /// immediate and async_job_id responses.
    async fn get_or_create_shared_folder_id(&self) -> Result<String, CloudHomeError> {
        let share_body = serde_json::json!({ "path": self.folder_path });
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/sharing/share_folder", self.api_base))
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
                        .post(format!("{}/{endpoint}", self.api_base))
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
                        .post(format!("{}/{endpoint}", self.api_base))
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
                    .post(format!("{}/sharing/remove_folder_member", self.api_base))
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

#[derive(Default)]
struct DropboxAppendedState {
    objects_by_id: HashMap<String, AppendedObject>,
    id_paths: HashMap<String, String>,
    path_ids: HashMap<String, String>,
}

fn apply_dropbox_appended_page(
    page: &serde_json::Value,
    folder_path: &str,
    prefix: &str,
    state: &mut DropboxAppendedState,
) -> Result<(), CloudHomeError> {
    let entries = page["entries"].as_array().ok_or_else(|| {
        CloudHomeError::Transport("Dropbox appended listing omitted entries".to_string())
    })?;
    for entry in entries {
        match entry[".tag"].as_str() {
            Some("file") => {
                let id = entry["id"]
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| {
                        CloudHomeError::Transport(
                            "Dropbox appended file entry omitted id".to_string(),
                        )
                    })?;
                let path_display = entry["path_display"].as_str().ok_or_else(|| {
                    CloudHomeError::Transport(
                        "Dropbox appended file entry omitted path_display".to_string(),
                    )
                })?;
                let path_lower = entry["path_lower"].as_str().ok_or_else(|| {
                    CloudHomeError::Transport(
                        "Dropbox appended file entry omitted path_lower".to_string(),
                    )
                })?;
                let logical_key = strip_dropbox_folder_prefix(path_display, folder_path)
                    .ok_or_else(|| {
                        CloudHomeError::Transport(format!(
                            "Dropbox appended file {path_display:?} is outside configured folder {folder_path:?}"
                        ))
                    })?;
                if let Some(old_path) = state.id_paths.remove(id) {
                    state.path_ids.remove(&old_path);
                }
                state.objects_by_id.remove(id);
                if logical_key.starts_with(prefix) {
                    if let Some(displaced_id) = state
                        .path_ids
                        .insert(path_lower.to_string(), id.to_string())
                    {
                        state.id_paths.remove(&displaced_id);
                        state.objects_by_id.remove(&displaced_id);
                    }
                    state
                        .id_paths
                        .insert(id.to_string(), path_lower.to_string());
                    state.objects_by_id.insert(
                        id.to_string(),
                        AppendedObject::from_provider(logical_key.to_string(), id.to_string())?,
                    );
                }
            }
            Some("deleted") => {
                let path_lower = entry["path_lower"].as_str().ok_or_else(|| {
                    CloudHomeError::Transport(
                        "Dropbox appended deleted entry omitted path_lower".to_string(),
                    )
                })?;
                if let Some(id) = state.path_ids.remove(path_lower) {
                    state.id_paths.remove(&id);
                    state.objects_by_id.remove(&id);
                }
            }
            Some("folder") => {}
            Some(tag) => {
                return Err(CloudHomeError::Transport(format!(
                    "Dropbox appended listing returned unknown entry tag {tag:?}"
                )))
            }
            None => {
                return Err(CloudHomeError::Transport(
                    "Dropbox appended listing entry omitted its tag".to_string(),
                ))
            }
        }
    }
    Ok(())
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
                        .post(format!(
                            "{}/files/upload_session/finish",
                            self.home.content_base
                        ))
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
                        .post(format!(
                            "{}/files/upload_session/append_v2",
                            self.home.content_base
                        ))
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

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
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
        if summary.starts_with("path/conflict/file") {
            return CloudHomeError::AlreadyExists(key.to_string());
        }
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
                    .post(format!("{}/files/download", self.content_base))
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
                    .post(format!("{}/files/delete_v2", self.api_base))
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
                            .post(format!("{}/files/list_folder/continue", self.api_base))
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
                            .post(format!("{}/files/list_folder", self.api_base))
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

impl DropboxCloudHome {
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
                    .post(format!("{}/files/upload", self.content_base))
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
        let session_id = self.start_upload_session(key).await?;
        Ok(Box::new(DropboxSessionSink {
            home: self,
            session_id,
            key: key.to_string(),
        }))
    }

    fn multipart_threshold(&self) -> u64 {
        DROPBOX_SIMPLE_UPLOAD_MAX as u64
    }

    async fn append_object(
        &self,
        full_logical_key: &str,
        mut body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<AppendedObject, CloudHomeError> {
        if body.len() <= self.multipart_threshold() {
            let data = body.collect().await?;
            let length = data.len() as u64;
            let object = self.append_small(full_logical_key, data).await?;
            progress(length);
            return Ok(object);
        }

        let session_id = self.start_upload_session(full_logical_key).await?;
        let total = body.len();
        let mut offset = 0;
        while let Some(part) = body.next_part(DROPBOX_CHUNK_SIZE).await? {
            let length = part.len() as u64;
            let is_last = offset + length >= total;
            let response = if is_last {
                let api_arg = dropbox_api_arg(&serde_json::json!({
                    "cursor": { "session_id": session_id, "offset": offset },
                    "commit": {
                        "path": self.full_path(full_logical_key),
                        "mode": { ".tag": "add" },
                        "autorename": false,
                        "strict_conflict": true,
                        "mute": true,
                    },
                }));
                self.session
                    .api_call_no_transient_retry(|token| {
                        self.client()
                            .post(format!("{}/files/upload_session/finish", self.content_base))
                            .bearer_auth(token)
                            .header("Dropbox-API-Arg", &api_arg)
                            .header("Content-Type", "application/octet-stream")
                            .body(part.clone())
                    })
                    .await?
            } else {
                let api_arg = dropbox_api_arg(&serde_json::json!({
                    "cursor": { "session_id": session_id, "offset": offset },
                    "close": false,
                }));
                let response = self
                    .session
                    .api_call_no_transient_retry(|token| {
                        self.client()
                            .post(format!(
                                "{}/files/upload_session/append_v2",
                                self.content_base
                            ))
                            .bearer_auth(token)
                            .header("Dropbox-API-Arg", &api_arg)
                            .header("Content-Type", "application/octet-stream")
                            .body(part.clone())
                    })
                    .await?;
                let status = response.status();
                if !status.is_success() {
                    return Err(classify_write_error(
                        status,
                        &http::body_text(response).await,
                        full_logical_key,
                    ));
                }
                offset += length;
                progress(offset);
                continue;
            };
            let object = self
                .appended_object_from_response(full_logical_key, response)
                .await?;
            offset += length;
            progress(offset);
            return Ok(object);
        }
        Err(CloudHomeError::Transport(format!(
            "append {full_logical_key}: upload body ended before the final part"
        )))
    }

    async fn list_appended(&self, prefix: &str) -> Result<AppendedListing, CloudHomeError> {
        let mut cursor: Option<String> = None;
        let mut page_tokens = PageTokenTracker::new("Dropbox appended listing");
        let mut state = DropboxAppendedState::default();
        loop {
            let response = match cursor.as_deref() {
                Some(cursor) => {
                    let body = serde_json::json!({"cursor": cursor});
                    self.session
                        .api_call(|token| {
                            self.client()
                                .post(format!("{}/files/list_folder/continue", self.api_base))
                                .bearer_auth(token)
                                .json(&body)
                        })
                        .await?
                }
                None => {
                    let body = serde_json::json!({
                        "path": self.folder_path,
                        "recursive": true,
                        "include_deleted": true,
                        "limit": 2000,
                    });
                    self.session
                        .api_call(|token| {
                            self.client()
                                .post(format!("{}/files/list_folder", self.api_base))
                                .bearer_auth(token)
                                .json(&body)
                        })
                        .await?
                }
            };
            let response =
                ensure_ok(response, "list appended Dropbox files", self.not_found()).await?;
            let page: serde_json::Value =
                http::ok_json(response, "parse appended Dropbox list").await?;
            apply_dropbox_appended_page(&page, &self.folder_path, prefix, &mut state)?;
            let next_cursor = page["cursor"]
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    CloudHomeError::Transport(
                        "Dropbox appended listing omitted its cursor".to_string(),
                    )
                })?
                .to_string();
            match page["has_more"].as_bool() {
                Some(true) => cursor = Some(page_tokens.record(&next_cursor)?),
                Some(false) => break,
                None => {
                    return Err(CloudHomeError::Transport(
                        "Dropbox appended listing omitted has_more".to_string(),
                    ))
                }
            }
        }
        let mut objects: Vec<_> = state.objects_by_id.into_values().collect();
        objects.sort_by(|left, right| {
            left.logical_key()
                .cmp(right.logical_key())
                .then_with(|| left.opaque_provider_id().cmp(right.opaque_provider_id()))
        });
        Ok(AppendedListing {
            objects,
            coverage: ListingCoverage::CompleteAtScan,
        })
    }

    async fn read_appended(&self, object: &AppendedObject) -> Result<Vec<u8>, CloudHomeError> {
        let response = self.send_exact_read(object).await?;
        http::ok_bytes(response, "read exact Dropbox body").await
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        rest_read(self, key).await
    }

    async fn read_appended_to_file(
        &self,
        object: &super::AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        let response = self.send_exact_read(object).await?;
        super::oauth_rest::response_to_file(response, destination, "read exact Dropbox body").await
    }

    async fn delete_appended(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        self.verify_appended_object(object).await?;
        let body = serde_json::json!({"path": object.opaque_provider_id()});
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/files/delete_v2", self.api_base))
                    .bearer_auth(token)
                    .json(&body)
            })
            .await?;
        match ensure_ok(response, "delete exact Dropbox file", self.not_found()).await {
            Ok(_) | Err(CloudHomeError::NotFound(_)) => Ok(()),
            Err(error) => Err(error),
        }
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
                    .post(format!("{}/files/get_metadata", self.api_base))
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
                                .post(format!("{}/sharing/add_folder_member", self.api_base))
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

#[async_trait]
impl CloudHome for DropboxCloudHome {
    fn immutable_copy_storage(
        self: std::sync::Arc<Self>,
    ) -> Option<std::sync::Arc<dyn ImmutableCopyStorage>> {
        Some(self)
    }
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        DropboxCloudHome::put_object(self, key, data).await
    }
    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        DropboxCloudHome::open_multipart(self, key, total_len).await
    }
    fn multipart_threshold(&self) -> u64 {
        DropboxCloudHome::multipart_threshold(self)
    }
    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        DropboxCloudHome::read(self, key).await
    }
    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        DropboxCloudHome::read_range(self, key, start, end).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        DropboxCloudHome::list(self, prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        DropboxCloudHome::delete(self, key).await
    }
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        DropboxCloudHome::exists(self, key).await
    }
    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        DropboxCloudHome::set_access(self, desired).await
    }
}

#[async_trait]
impl ImmutableCopyStorage for DropboxCloudHome {
    async fn append_object(
        &self,
        key: &str,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<AppendedObject, CloudHomeError> {
        DropboxCloudHome::append_object(self, key, body, progress).await
    }
    async fn list_appended(&self, prefix: &str) -> Result<AppendedListing, CloudHomeError> {
        DropboxCloudHome::list_appended(self, prefix).await
    }
    async fn read_appended(&self, object: &AppendedObject) -> Result<Vec<u8>, CloudHomeError> {
        DropboxCloudHome::read_appended(self, object).await
    }
    async fn read_appended_to_file(
        &self,
        object: &AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        DropboxCloudHome::read_appended_to_file(self, object, destination).await
    }
    async fn delete_appended(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        DropboxCloudHome::delete_appended(self, object).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::extract::State;
    use axum::http::{Request, Response, StatusCode};
    use axum::Router;
    use std::sync::{Arc, Mutex};

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

    #[derive(Clone, Debug)]
    struct RecordedRequest {
        path: String,
        api_arg: Option<String>,
        body: Vec<u8>,
    }

    async fn immutable_copy_endpoint(
        State(requests): State<Arc<Mutex<Vec<RecordedRequest>>>>,
        request: Request<Body>,
    ) -> Response<Body> {
        let path = request.uri().path().to_string();
        let api_arg = request.headers().get("Dropbox-API-Arg").map(|value| {
            value
                .to_str()
                .expect("Dropbox-API-Arg is UTF-8")
                .to_string()
        });
        let body = to_bytes(request.into_body(), usize::MAX)
            .await
            .expect("read request body")
            .to_vec();
        requests
            .lock()
            .expect("lock requests")
            .push(RecordedRequest {
                path: path.clone(),
                api_arg,
                body,
            });

        match path.as_str() {
            "/files/upload" => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"id:copy"}"#))
                .expect("build upload response"),
            "/files/download" => Response::builder()
                .status(StatusCode::OK)
                .header(
                    "Dropbox-API-Result",
                    serde_json::json!({
                        ".tag": "file",
                        "id": "id:copy",
                        "path_display": "/Apps/your-app/my-store/protocol/copy",
                    })
                    .to_string(),
                )
                .body(Body::from("copy-bytes"))
                .expect("build download response"),
            "/files/get_metadata" => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        ".tag": "file",
                        "id": "id:copy",
                        "path_display": "/Apps/your-app/my-store/protocol/copy",
                    })
                    .to_string(),
                ))
                .expect("build metadata response"),
            "/files/delete_v2" => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .expect("build delete response"),
            _ => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(format!("unexpected path: {path}")))
                .expect("build unexpected response"),
        }
    }

    async fn immutable_copy_test_home() -> (
        DropboxCloudHome,
        Arc<Mutex<Vec<RecordedRequest>>>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Dropbox endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let app = Router::new()
            .fallback(immutable_copy_endpoint)
            .with_state(requests.clone());
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx
                        .await
                        .expect("receive Dropbox endpoint shutdown");
                })
                .await
                .expect("Dropbox endpoint failed");
        });
        (
            home().with_endpoints(endpoint.clone(), endpoint),
            requests,
            shutdown_tx,
        )
    }

    #[tokio::test]
    async fn immutable_copy_requests_use_create_only_and_exact_provider_ids() {
        let (home, requests, shutdown) = immutable_copy_test_home().await;
        let object = home
            .append_object(
                "protocol/copy",
                BlobBody::from_bytes(b"copy-bytes".to_vec()),
                &super::super::no_progress(),
            )
            .await
            .expect("append Dropbox copy");
        assert_eq!(object.opaque_provider_id(), "id:copy");
        assert_eq!(
            home.read_appended(&object).await.expect("read exact copy"),
            b"copy-bytes"
        );
        home.delete_appended(&object)
            .await
            .expect("delete exact copy");

        let requests = requests.lock().expect("lock requests");
        assert_eq!(requests.len(), 4, "{requests:?}");
        let upload_arg: serde_json::Value = serde_json::from_str(
            requests[0]
                .api_arg
                .as_deref()
                .expect("upload carries Dropbox-API-Arg"),
        )
        .expect("parse upload arg");
        assert_eq!(requests[0].path, "/files/upload");
        assert_eq!(upload_arg["mode"][".tag"], "add");
        assert_eq!(upload_arg["autorename"], false);
        assert_eq!(upload_arg["strict_conflict"], true);
        assert_eq!(requests[0].body, b"copy-bytes");
        let read_arg: serde_json::Value = serde_json::from_str(
            requests[1]
                .api_arg
                .as_deref()
                .expect("read carries Dropbox-API-Arg"),
        )
        .expect("parse read arg");
        assert_eq!(read_arg["path"], "id:copy");
        let metadata_body: serde_json::Value =
            serde_json::from_slice(&requests[2].body).expect("parse metadata body");
        assert_eq!(requests[2].path, "/files/get_metadata");
        assert_eq!(metadata_body["path"], "id:copy");
        let delete_body: serde_json::Value =
            serde_json::from_slice(&requests[3].body).expect("parse delete body");
        assert_eq!(delete_body["path"], "id:copy");
        drop(requests);
        shutdown.send(()).expect("shut down Dropbox endpoint");
    }

    #[tokio::test]
    async fn exact_operations_reject_a_dropbox_id_bound_to_another_logical_key() {
        let (home, requests, shutdown) = immutable_copy_test_home().await;
        let object =
            AppendedObject::from_provider("protocol/other".to_string(), "id:copy".to_string())
                .expect("build mismatched Dropbox locator");

        let read_error = home
            .read_appended(&object)
            .await
            .expect_err("mismatched Dropbox read must fail");
        assert!(
            read_error.to_string().contains("does not identify"),
            "{read_error}"
        );
        let delete_error = home
            .delete_appended(&object)
            .await
            .expect_err("mismatched Dropbox delete must fail");
        assert!(
            delete_error.to_string().contains("does not identify"),
            "{delete_error}"
        );

        let requests = requests.lock().expect("lock requests");
        assert_eq!(requests.len(), 2, "{requests:?}");
        assert_eq!(requests[0].path, "/files/download");
        assert_eq!(requests[1].path, "/files/get_metadata");
        drop(requests);
        shutdown.send(()).expect("shut down Dropbox endpoint");
    }

    async fn repeated_cursor_endpoint() -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"entries":[],"cursor":"same","has_more":true}"#,
            ))
            .expect("build repeated cursor response")
    }

    #[tokio::test]
    async fn authoritative_listing_rejects_a_repeated_cursor() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Dropbox endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().fallback(repeated_cursor_endpoint))
                .await
                .expect("Dropbox endpoint failed");
        });
        let home = home().with_endpoints(endpoint.clone(), endpoint);

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            home.list_appended("protocol/"),
        )
        .await
        .expect("listing must terminate on a repeated cursor")
        .expect_err("repeated cursor must refuse authoritative coverage");

        assert!(result.to_string().contains("repeated"), "{result}");
        server.abort();
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
    fn classify_write_error_preserves_file_conflict_as_already_exists() {
        let body = r#"{"error_summary":"path/conflict/file","error":{}}"#;
        let err = classify_write_error(reqwest::StatusCode::CONFLICT, body, "objects/dev1/1.enc");
        assert!(
            matches!(err, CloudHomeError::AlreadyExists(ref key) if key == "objects/dev1/1.enc")
        );
        assert!(!err.is_retryable());
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
    fn appended_page_preserves_ids_and_applies_deleted_entries() {
        let mut state = DropboxAppendedState::default();
        let first = serde_json::json!({
            "entries": [
                {".tag":"file", "id":"id:first", "path_display":"/Apps/your-app/my-store/protocol/first", "path_lower":"/apps/your-app/my-store/protocol/first"},
                {".tag":"file", "id":"id:removed", "path_display":"/Apps/your-app/my-store/protocol/removed", "path_lower":"/apps/your-app/my-store/protocol/removed"}
            ]
        });
        apply_dropbox_appended_page(&first, "/Apps/your-app/my-store", "protocol/", &mut state)
            .expect("apply first page");
        let second = serde_json::json!({
            "entries": [
                {".tag":"deleted", "path_lower":"/apps/your-app/my-store/protocol/removed"},
                {".tag":"file", "id":"id:second", "path_display":"/Apps/your-app/my-store/protocol/second", "path_lower":"/apps/your-app/my-store/protocol/second"}
            ]
        });
        apply_dropbox_appended_page(&second, "/Apps/your-app/my-store", "protocol/", &mut state)
            .expect("apply continuation page");

        let mut ids: Vec<_> = state
            .objects_by_id
            .values()
            .map(|object| object.opaque_provider_id())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["id:first", "id:second"]);
    }

    #[test]
    fn appended_page_refuses_file_without_exact_id() {
        let page = serde_json::json!({
            "entries": [{".tag":"file", "path_display":"/Apps/your-app/my-store/protocol/first", "path_lower":"/apps/your-app/my-store/protocol/first"}]
        });
        let error = apply_dropbox_appended_page(
            &page,
            "/Apps/your-app/my-store",
            "protocol/",
            &mut DropboxAppendedState::default(),
        )
        .expect_err("missing id must refuse authoritative coverage");
        assert!(error.to_string().contains("omitted id"), "{error}");
    }

    #[test]
    fn appended_page_replaces_an_ids_old_path_and_removes_moves_out_of_prefix() {
        let mut state = DropboxAppendedState::default();
        for page in [
            serde_json::json!({"entries":[
                {".tag":"file", "id":"id:stable", "path_display":"/Apps/your-app/my-store/protocol/old", "path_lower":"/apps/your-app/my-store/protocol/old"}
            ]}),
            serde_json::json!({"entries":[
                {".tag":"file", "id":"id:stable", "path_display":"/Apps/your-app/my-store/protocol/new", "path_lower":"/apps/your-app/my-store/protocol/new"}
            ]}),
        ] {
            apply_dropbox_appended_page(&page, "/Apps/your-app/my-store", "protocol/", &mut state)
                .expect("apply rename event");
        }
        assert_eq!(
            state.objects_by_id["id:stable"].logical_key(),
            "protocol/new"
        );
        assert!(!state
            .path_ids
            .contains_key("/apps/your-app/my-store/protocol/old"));

        let moved_out = serde_json::json!({"entries":[
            {".tag":"file", "id":"id:stable", "path_display":"/Apps/your-app/my-store/other/new", "path_lower":"/apps/your-app/my-store/other/new"}
        ]});
        apply_dropbox_appended_page(
            &moved_out,
            "/Apps/your-app/my-store",
            "protocol/",
            &mut state,
        )
        .expect("apply move out");
        assert!(state.objects_by_id.is_empty());
    }

    #[test]
    fn appended_page_keeps_distinct_ids_with_distinct_paths() {
        let mut state = DropboxAppendedState::default();
        let page = serde_json::json!({"entries":[
            {".tag":"file", "id":"id:a", "path_display":"/Apps/your-app/my-store/protocol/a", "path_lower":"/apps/your-app/my-store/protocol/a"},
            {".tag":"file", "id":"id:b", "path_display":"/Apps/your-app/my-store/protocol/b", "path_lower":"/apps/your-app/my-store/protocol/b"}
        ]});
        apply_dropbox_appended_page(&page, "/Apps/your-app/my-store", "protocol/", &mut state)
            .expect("apply distinct files");
        assert_eq!(state.objects_by_id.len(), 2);
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
