//! Dropbox `CloudHome` implementation.
//!
//! Uses the Dropbox HTTP API v2 with OAuth 2.0 (PKCE) tokens. Files live under a
//! folder using path-based access — no filename encoding. The
//! `read`/`read_range`/`list`/`delete` methods are the shared `OAuthRestHome`
//! implementations; this file supplies only the Dropbox request shapes (POST with
//! a `Dropbox-API-Arg` header), the page parser, the upload session, and sharing.

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::StatusCode;
use std::fmt::Write as _;
use std::sync::OnceLock;

#[path = "dropbox/content_hash.rs"]
mod content_hash;

use super::http::{self, ensure_ok, exists_from_response, NotFound};
use super::oauth_rest::{
    rest_delete, rest_list, rest_read, rest_read_range, ListPage, OAuthRestHome,
};
use super::oauth_session::OAuthSession;
use super::{
    combine_cleanup_failure, BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHome,
    CloudHomeError, CloudHomeJoinInfo, ExactSlotStorage, RevokeOutcome, UploadProgress,
};
use crate::oauth::OAuthConfig;
use coven_protocol::objects::ObjectSlot;
use tracing::warn;

#[derive(Clone, Debug, PartialEq, Eq)]
struct DropboxExactMetadata {
    size: u64,
    content_hash: String,
}

const API_BASE: &str = "https://api.dropboxapi.com/2";
const CONTENT_BASE: &str = "https://content.dropboxapi.com/2";

/// Dropbox cloud home backend.
pub struct DropboxCloudHome {
    /// Folder path in Dropbox, e.g. "/Apps/your-app/my-store"
    folder_path: String,
    api_base: String,
    content_base: String,
    session: OAuthSession,
    namespace_id: OnceLock<String>,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
}

impl DropboxCloudHome {
    pub fn new(
        folder_path: String,
        session: OAuthSession,
        exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    ) -> Self {
        Self {
            folder_path,
            api_base: API_BASE.to_string(),
            content_base: CONTENT_BASE.to_string(),
            session,
            namespace_id: OnceLock::new(),
            exact_upload_verification,
        }
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

    pub(crate) fn oauth_config(creds: crate::oauth::OAuthClientCreds) -> OAuthConfig {
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

    fn namespace_path(key: &str) -> String {
        format!("/{key}")
    }

    fn path_root_header(namespace_id: &str) -> String {
        serde_json::json!({
            ".tag": "namespace_id",
            "namespace_id": namespace_id,
        })
        .to_string()
    }

    fn scoped_request(
        request: reqwest::RequestBuilder,
        path_root: &str,
    ) -> reqwest::RequestBuilder {
        request.header("Dropbox-API-Path-Root", path_root)
    }

    fn remember_namespace(&self, namespace_id: String) -> Result<String, CloudHomeError> {
        if namespace_id.is_empty() {
            return Err(CloudHomeError::Transport(
                "Dropbox shared namespace id is empty".to_string(),
            ));
        }
        if let Some(current) = self.namespace_id.get() {
            if current != &namespace_id {
                return Err(CloudHomeError::Transport(
                    "Dropbox shared namespace changed during one adapter session".to_string(),
                ));
            }
            return Ok(current.clone());
        }
        self.namespace_id
            .set(namespace_id.clone())
            .map_err(|actual| {
                CloudHomeError::Transport(format!(
                    "Dropbox shared namespace changed while being resolved: {actual}"
                ))
            })?;
        Ok(namespace_id)
    }

    async fn start_upload_session(&self, key: &str) -> Result<String, CloudHomeError> {
        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        let response = self
            .session
            .api_call(|oauth| {
                Self::scoped_request(
                    oauth.post(format!("{}/files/upload_session/start", self.content_base)),
                    &path_root,
                )
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

    async fn append_small(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        let body = Bytes::from(data);
        let api_arg = dropbox_api_arg(&serde_json::json!({
            "path": Self::namespace_path(key),
                    "mode": { ".tag": "add" },
                    "autorename": false,
                    "strict_conflict": true,
                    "mute": true,
        }));
        let response = self
            .session
            .api_call_no_transient_retry(|oauth| {
                Self::scoped_request(
                    oauth.post(format!("{}/files/upload", self.content_base)),
                    &path_root,
                )
                .header("Dropbox-API-Arg", &api_arg)
                .header("Content-Type", "application/octet-stream")
                .body(body.clone())
            })
            .await?;
        self.validate_create_response(key, response).await.map(drop)
    }

    async fn validate_create_response(
        &self,
        key: &str,
        response: reqwest::Response,
    ) -> Result<serde_json::Value, CloudHomeError> {
        let status = response.status();
        let body = http::body_text(response).await;
        if !status.is_success() {
            return Err(classify_write_error(status, &body, key));
        }
        let json: serde_json::Value = serde_json::from_str(&body).map_err(|error| {
            CloudHomeError::Transport(format!("append {key}: parse response: {error}"))
        })?;
        json["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport(format!("append {key}: response missing file id"))
            })?;
        if json["path_display"].as_str() != Some(Self::namespace_path(key).as_str()) {
            return Err(CloudHomeError::Transport(format!(
                "append {key}: response identifies another Dropbox path"
            )));
        }
        Ok(json)
    }

    async fn send_exact_read(
        &self,
        slot: &ObjectSlot,
    ) -> Result<reqwest::Response, CloudHomeError> {
        slot.require_logical_key_for("Dropbox")?;
        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        let path = Self::namespace_path(slot.logical_key());
        let api_arg = dropbox_api_arg(&serde_json::json!({"path": path}));
        let response = self
            .session
            .api_call(|oauth| {
                Self::scoped_request(
                    oauth.post(format!("{}/files/download", self.content_base)),
                    &path_root,
                )
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
                    slot.logical_key()
                ))
            })?
            .to_str()
            .map_err(|error| {
                CloudHomeError::Transport(format!(
                    "exact Dropbox read for {} returned invalid metadata: {error}",
                    slot.logical_key()
                ))
            })?;
        let metadata: serde_json::Value = serde_json::from_str(metadata).map_err(|error| {
            CloudHomeError::Transport(format!(
                "exact Dropbox read for {} returned malformed metadata: {error}",
                slot.logical_key()
            ))
        })?;
        self.exact_metadata_from_json(slot, &metadata)?;
        Ok(response)
    }

    fn exact_metadata_from_json(
        &self,
        slot: &ObjectSlot,
        metadata: &serde_json::Value,
    ) -> Result<DropboxExactMetadata, CloudHomeError> {
        slot.require_logical_key_for("Dropbox")?;
        let expected_path = Self::namespace_path(slot.logical_key());
        let matches = metadata[".tag"].as_str() == Some("file")
            && metadata["id"].as_str().is_some_and(|id| !id.is_empty())
            && metadata["path_display"].as_str() == Some(expected_path.as_str());
        if !matches {
            return Err(CloudHomeError::Transport(format!(
                "exact Dropbox slot for {} does not identify {expected_path}",
                slot.logical_key(),
            )));
        }
        let size = metadata["size"].as_u64().ok_or_else(|| {
            CloudHomeError::Transport(format!(
                "exact Dropbox metadata for {} omitted size",
                slot.logical_key()
            ))
        })?;
        let content_hash = metadata["content_hash"]
            .as_str()
            .filter(|hash| !hash.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport(format!(
                    "exact Dropbox metadata for {} omitted content_hash",
                    slot.logical_key()
                ))
            })?
            .to_string();
        Ok(DropboxExactMetadata { size, content_hash })
    }

    async fn exact_metadata(
        &self,
        slot: &ObjectSlot,
    ) -> Result<DropboxExactMetadata, CloudHomeError> {
        slot.require_logical_key_for("Dropbox")?;
        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        let body = serde_json::json!({"path": Self::namespace_path(slot.logical_key())});
        let response = self
            .session
            .api_call(|oauth| {
                Self::scoped_request(
                    oauth.post(format!("{}/files/get_metadata", self.api_base)),
                    &path_root,
                )
                .json(&body)
            })
            .await?;
        let response = ensure_ok(response, "verify exact Dropbox file", self.not_found()).await?;
        let metadata: serde_json::Value =
            http::ok_json(response, "parse exact Dropbox metadata").await?;
        self.exact_metadata_from_json(slot, &metadata)
    }

    async fn verify_slot(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        self.exact_metadata(slot).await.map(drop)
    }

    async fn verify_exact_upload(
        &self,
        upload: &super::ExactUpload<'_>,
        created_response_was_observed: bool,
    ) -> Result<(), CloudHomeError> {
        use coven_foundation::config::ExactUploadVerification;

        match self.exact_upload_verification {
            ExactUploadVerification::UploadChecksum => Err(CloudHomeError::Configuration(
                "Dropbox does not accept a caller-supplied upload checksum".to_string(),
            )),
            ExactUploadVerification::MetadataHash => {
                let metadata = self.exact_metadata(upload.object().slot()).await?;
                let expected_hash = content_hash::for_upload(upload).await?;
                if metadata.size != upload.object().stored_size()
                    || metadata.content_hash != expected_hash
                {
                    return Err(CloudHomeError::SlotCollision(
                        upload.object().slot().logical_key().to_string(),
                    ));
                }
                Ok(())
            }
            ExactUploadVerification::Readback => {
                let bytes = self.read_at(upload.object().slot()).await?;
                upload.verify_stored_bytes(&bytes)
            }
            ExactUploadVerification::Unchecked => {
                super::exact_upload::accept_unchecked_create_response(
                    created_response_was_observed,
                    upload.object(),
                )
            }
        }
    }

    /// Call `share_folder` and resolve the shared_folder_id, handling both
    /// immediate and async_job_id responses.
    async fn get_or_create_shared_folder_id(&self) -> Result<String, CloudHomeError> {
        if let Some(namespace_id) = self.namespace_id.get() {
            return Ok(namespace_id.clone());
        }
        let share_body = serde_json::json!({ "path": self.folder_path });
        let resp = self
            .session
            .api_call(|oauth| {
                oauth
                    .post(format!("{}/sharing/share_folder", self.api_base))
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
            return self.remember_namespace(id.to_string());
        }
        // Already shared: error payload contains the shared_folder_metadata
        if let Some(id) = json["error"]["shared_folder_metadata"]["shared_folder_id"].as_str() {
            return self.remember_namespace(id.to_string());
        }
        // Async: {".tag": "async_job_id", "async_job_id": "..."}
        if let Some(job_id) = json["async_job_id"].as_str() {
            let namespace_id = self.poll_share_job(job_id).await?;
            return self.remember_namespace(namespace_id);
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
            None,
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

    async fn poll_remove_member_job(
        &self,
        job_id: &str,
        shared_folder_id: &str,
    ) -> Result<(), CloudHomeError> {
        let path_root = Self::path_root_header(shared_folder_id);
        self.poll_dropbox_job(
            job_id,
            "sharing/check_remove_member_job_status",
            "remove folder member",
            Some(&path_root),
            |_| Ok(()),
        )
        .await
    }

    async fn poll_dropbox_job<T>(
        &self,
        job_id: &str,
        endpoint: &str,
        operation: &str,
        path_root: Option<&str>,
        complete: impl Fn(&serde_json::Value) -> Result<T, CloudHomeError>,
    ) -> Result<T, CloudHomeError> {
        let request_body = serde_json::json!({ "async_job_id": job_id });
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let resp = self
                .session
                .api_call(|oauth| {
                    let request = oauth
                        .post(format!("{}/{endpoint}", self.api_base))
                        .json(&request_body);
                    match path_root {
                        Some(path_root) => Self::scoped_request(request, path_root),
                        None => request,
                    }
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
        let path_root = Self::path_root_header(shared_folder_id);
        let mut endpoint = "sharing/list_folder_members";
        let mut request = serde_json::json!({
            "shared_folder_id": shared_folder_id,
            "include_inherited": false,
            "limit": 1000,
        });
        loop {
            let resp = self
                .session
                .api_call(|oauth| {
                    Self::scoped_request(
                        oauth.post(format!("{}/{endpoint}", self.api_base)),
                        &path_root,
                    )
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
        let path_root = Self::path_root_header(shared_folder_id);
        let remove_body = serde_json::json!({
            "shared_folder_id": shared_folder_id,
            "member": { ".tag": "email", "email": email },
            "leave_a_copy": false,
        });
        let resp = self
            .session
            .api_call(|oauth| {
                Self::scoped_request(
                    oauth.post(format!("{}/sharing/remove_folder_member", self.api_base)),
                    &path_root,
                )
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
            DropboxRevokeLaunch::AsyncJob(job_id) => {
                self.poll_remove_member_job(&job_id, shared_folder_id).await
            }
        }
    }

    #[cfg(test)]
    fn with_endpoints(mut self, api_base: String, content_base: String) -> Self {
        self.api_base = api_base;
        self.content_base = content_base;
        self
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
    completion: DropboxSessionCompletion,
    confirmed_offset: u64,
    settled: bool,
}

#[derive(Clone, Copy)]
enum DropboxSessionCompletion {
    Overwrite,
    CreateOnly,
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
        let length = part.len() as u64;
        let namespace_id = self.home.get_or_create_shared_folder_id().await?;
        let path_root = DropboxCloudHome::path_root_header(&namespace_id);
        let resp = if is_last {
            // The final part commits the file at the destination path.
            let path = DropboxCloudHome::namespace_path(&self.key);
            let mode = match self.completion {
                DropboxSessionCompletion::Overwrite => serde_json::json!({ ".tag": "overwrite" }),
                DropboxSessionCompletion::CreateOnly => serde_json::json!({ ".tag": "add" }),
            };
            let arg = dropbox_api_arg(&serde_json::json!({
                "cursor": { "session_id": self.session_id, "offset": offset },
                "commit": {
                    "path": path,
                    "mode": mode,
                    "autorename": false,
                    "strict_conflict": matches!(self.completion, DropboxSessionCompletion::CreateOnly),
                    "mute": true,
                },
            }));
            self.home
                .session
                .api_call_no_transient_retry(|oauth| {
                    DropboxCloudHome::scoped_request(
                        oauth.post(format!(
                            "{}/files/upload_session/finish",
                            self.home.content_base
                        )),
                        &path_root,
                    )
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
                .api_call_no_transient_retry(|oauth| {
                    DropboxCloudHome::scoped_request(
                        oauth.post(format!(
                            "{}/files/upload_session/append_v2",
                            self.home.content_base
                        )),
                        &path_root,
                    )
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
        self.confirmed_offset = offset.checked_add(length).ok_or_else(|| {
            CloudHomeError::Transport(format!("Dropbox upload offset overflow for {}", self.key))
        })?;
        if is_last {
            self.settled = true;
            if matches!(self.completion, DropboxSessionCompletion::CreateOnly) {
                self.home.validate_create_response(&self.key, resp).await?;
            }
        }
        Ok(())
    }

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        if self.settled {
            return Ok(());
        }
        let arg = dropbox_api_arg(&serde_json::json!({
            "cursor": {
                "session_id": self.session_id,
                "offset": self.confirmed_offset,
            },
            "close": true,
        }));
        let namespace_id = self.home.get_or_create_shared_folder_id().await?;
        let path_root = DropboxCloudHome::path_root_header(&namespace_id);
        let response = self
            .home
            .session
            .api_call_no_transient_retry(|oauth| {
                DropboxCloudHome::scoped_request(
                    oauth.post(format!(
                        "{}/files/upload_session/append_v2",
                        self.home.content_base
                    )),
                    &path_root,
                )
                .header("Dropbox-API-Arg", &arg)
                .header("Content-Type", "application/octet-stream")
                .body(Vec::new())
            })
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(classify_write_error(
                status,
                &http::body_text(response).await,
                &self.key,
            ));
        }
        self.settled = true;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<(), CloudHomeError> {
        if self.settled {
            return Ok(());
        }
        let operation = CloudHomeError::Transport(format!(
            "Dropbox upload session for {} reached finish without committing its final part",
            self.key
        ));
        let cleanup = self.abort().await;
        Err(combine_cleanup_failure(operation, cleanup))
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
        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        let arg = dropbox_api_arg(&serde_json::json!({ "path": Self::namespace_path(key) }));
        let range = range.map(|(start, end)| super::range_header(start, end));
        self.session
            .api_call(|oauth| {
                let mut req = Self::scoped_request(
                    oauth.post(format!("{}/files/download", self.content_base)),
                    &path_root,
                )
                .header("Dropbox-API-Arg", &arg);
                if let Some(ref range) = range {
                    req = req.header("Range", range);
                }
                req
            })
            .await
    }

    async fn send_delete(&self, key: &str) -> Result<reqwest::Response, CloudHomeError> {
        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        let body = serde_json::json!({ "path": Self::namespace_path(key) });
        self.session
            .api_call(|oauth| {
                Self::scoped_request(
                    oauth.post(format!("{}/files/delete_v2", self.api_base)),
                    &path_root,
                )
                .json(&body)
            })
            .await
    }

    async fn send_list_page(
        &self,
        _prefix: &str,
        cursor: Option<&str>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        // list_folder needs a folder path, not a prefix, so always list the root
        // recursively and filter client-side; `continue` follows pagination.
        match cursor {
            Some(cur) => {
                let body = serde_json::json!({ "cursor": cur });
                self.session
                    .api_call(|oauth| {
                        Self::scoped_request(
                            oauth.post(format!("{}/files/list_folder/continue", self.api_base)),
                            &path_root,
                        )
                        .json(&body)
                    })
                    .await
            }
            None => {
                let body = serde_json::json!({
                    "path": "",
                    "recursive": true,
                    "limit": 2000,
                });
                self.session
                    .api_call(|oauth| {
                        Self::scoped_request(
                            oauth.post(format!("{}/files/list_folder", self.api_base)),
                            &path_root,
                        )
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
                let Some(key) = path_display.strip_prefix('/') else {
                    warn!(
                        path_display,
                        "skipping Dropbox file entry outside the shared namespace"
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
        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        let body = Bytes::from(data);
        let api_arg = dropbox_api_arg(&serde_json::json!({
            "path": Self::namespace_path(key),
            "mode": { ".tag": "overwrite" },
            "autorename": false,
            "mute": true,
        }));
        let resp = self
            .session
            .api_call(|oauth| {
                Self::scoped_request(
                    oauth.post(format!("{}/files/upload", self.content_base)),
                    &path_root,
                )
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
            completion: DropboxSessionCompletion::Overwrite,
            confirmed_offset: 0,
            settled: false,
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
        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        let body = serde_json::json!({ "path": Self::namespace_path(key) });
        let resp = self
            .session
            .api_call(|oauth| {
                Self::scoped_request(
                    oauth.post(format!("{}/files/get_metadata", self.api_base)),
                    &path_root,
                )
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
        let path_root = Self::path_root_header(&shared_folder_id);
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
                        .api_call(|oauth| {
                            Self::scoped_request(
                                oauth.post(format!("{}/sharing/add_folder_member", self.api_base)),
                                &path_root,
                            )
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
impl ExactSlotStorage for DropboxCloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<coven_protocol::objects::ResolvedProviderBinding, CloudHomeError> {
        use coven_protocol::objects::{
            ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
            StoreProviderBinding,
        };

        if self.folder_path.is_empty() {
            return Err(CloudHomeError::Configuration(
                "Dropbox provider binding has an empty folder path".to_string(),
            ));
        }
        let metadata_body = serde_json::json!({
            "path": self.folder_path,
            "include_deleted": false,
        });
        let metadata_response = self
            .session
            .api_call(|oauth| {
                oauth
                    .post(format!("{}/files/get_metadata", self.api_base))
                    .json(&metadata_body)
            })
            .await?;
        let metadata_response = ensure_ok(
            metadata_response,
            "resolve Dropbox folder binding",
            self.not_found(),
        )
        .await?;
        let metadata: serde_json::Value =
            http::ok_json(metadata_response, "parse Dropbox folder binding").await?;
        let folder_id = metadata["id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport(
                    "Dropbox folder metadata omitted the stable folder id".to_string(),
                )
            })?
            .to_string();
        let path_lower = metadata["path_lower"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport("Dropbox folder metadata omitted path_lower".to_string())
            })?
            .to_string();
        if metadata[".tag"].as_str() != Some("folder")
            || !path_lower.eq_ignore_ascii_case(&self.folder_path)
        {
            return Err(CloudHomeError::Transport(
                "Dropbox folder lookup returned a different folder binding".to_string(),
            ));
        }

        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        let account_response = self
            .session
            .api_call(|oauth| {
                Self::scoped_request(
                    oauth.post(format!("{}/users/get_current_account", self.api_base)),
                    &path_root,
                )
                .header("Content-Type", "application/json")
                .body("null")
            })
            .await?;
        let account_response = ensure_ok(
            account_response,
            "resolve Dropbox principal",
            NotFound::Status,
        )
        .await?;
        let account: serde_json::Value =
            http::ok_json(account_response, "parse Dropbox principal").await?;
        let account_id = account["account_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport(
                    "Dropbox current-account response omitted account_id".to_string(),
                )
            })?
            .to_string();

        if namespace_id.is_empty() || folder_id.is_empty() || path_lower.is_empty() {
            return Err(CloudHomeError::Transport(
                "Dropbox shared namespace resolution returned an empty identifier".to_string(),
            ));
        }
        Ok(ResolvedProviderBinding {
            store: StoreProviderBinding::Dropbox { namespace_id },
            device: ProviderDeviceBinding {
                principal: ProviderPrincipalId::Dropbox { account_id },
            },
        })
    }

    async fn create_at(
        &self,
        upload: &super::ExactUpload<'_>,
        progress: &UploadProgress<'_>,
    ) -> Result<super::ExactCreateOutcome, CloudHomeError> {
        if matches!(
            self.exact_upload_verification,
            coven_foundation::config::ExactUploadVerification::UploadChecksum
        ) {
            return Err(CloudHomeError::Configuration(
                "Dropbox does not expose upload-checksum enforcement for this endpoint".to_string(),
            ));
        }
        let slot = upload.object().slot();
        let body = upload.body().await?;
        slot.require_logical_key_for("Dropbox")?;
        let key = slot.logical_key();
        if body.len() <= self.multipart_threshold() {
            let data = body.collect().await?;
            let length = data.len() as u64;
            let operation = self.append_small(key, data).await;
            if operation.is_ok() {
                progress(length);
            }
            return super::exact_upload::settle_exact_create(operation, |observed| {
                self.verify_exact_upload(upload, observed)
            })
            .await;
        }

        let session_id = self.start_upload_session(key).await?;
        let sink = DropboxSessionSink {
            home: self,
            session_id,
            key: key.to_string(),
            completion: DropboxSessionCompletion::CreateOnly,
            confirmed_offset: 0,
            settled: false,
        };
        let operation = super::blob_body::MultipartUpload::new(key, body, Box::new(sink), progress)
            .run()
            .await;
        super::exact_upload::settle_exact_create(operation, |observed| {
            self.verify_exact_upload(upload, observed)
        })
        .await
    }

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        let response = self.send_exact_read(slot).await?;
        http::ok_bytes(response, "read exact Dropbox body").await
    }

    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        let bytes = self.read_at(slot).await?;
        let start = usize::try_from(start)
            .map_err(|_| CloudHomeError::Configuration("range start is too large".to_string()))?;
        let end = usize::try_from(end)
            .map_err(|_| CloudHomeError::Configuration("range end is too large".to_string()))?;
        bytes.get(start..end).map(<[u8]>::to_vec).ok_or_else(|| {
            CloudHomeError::Configuration(format!(
                "invalid range {start}..{end} for {} bytes",
                bytes.len()
            ))
        })
    }

    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        let response = self.send_exact_read(slot).await?;
        super::oauth_rest::response_to_file(response, destination, "read exact Dropbox body").await
    }

    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        match self.verify_slot(slot).await {
            Ok(()) => {}
            Err(CloudHomeError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error),
        }
        let namespace_id = self.get_or_create_shared_folder_id().await?;
        let path_root = Self::path_root_header(&namespace_id);
        let body = serde_json::json!({"path": Self::namespace_path(slot.logical_key())});
        let response = self
            .session
            .api_call(|oauth| {
                Self::scoped_request(
                    oauth.post(format!("{}/files/delete_v2", self.api_base)),
                    &path_root,
                )
                .json(&body)
            })
            .await?;
        match ensure_ok(response, "delete exact Dropbox file", self.not_found()).await {
            Ok(_) | Err(CloudHomeError::NotFound(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
#[path = "dropbox_tests.rs"]
mod tests;
