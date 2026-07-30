//! OneDrive `CloudHome` implementation.
//!
//! Uses the Microsoft Graph API. Files are stored flat in a single folder — path
//! separators are escaped by [`super::key_encoding`]. The
//! `read`/`read_range`/`list`/`delete` methods are the shared [`OAuthRestHome`]
//! implementations; this file supplies only the Graph request shapes, the page
//! parser, the upload paths, and sharing.

use async_trait::async_trait;
use bytes::Bytes;

use super::http::{self, ensure_ok, exists_from_response, NotFound};
use super::key_encoding::{decode_listed_key, encode_key};
use super::oauth_rest::{
    rest_delete, rest_list, rest_read, rest_read_range, ListPage, OAuthRestHome,
};
use super::oauth_session::OAuthSession;
use super::resumable::{RangePutSink, RangePutUploader};
use super::{
    combine_cleanup_failure, require_logical_slot, sharing, BlobBody, BoxPartSink,
    CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeError, CloudHomeJoinInfo,
    ExactSlotStorage, ObjectSlot, RevokeOutcome, UploadProgress,
};
use crate::clock::ClockRef;
use crate::keys::StoreKeys;
use crate::oauth::{OAuthConfig, OAuthTokens};

const GRAPH_API: &str = "https://graph.microsoft.com/v1.0";

fn onedrive_upload_cancellation_succeeded(status: reqwest::StatusCode) -> bool {
    status.is_success() || status == reqwest::StatusCode::NOT_FOUND
}

/// OneDrive cloud home backend.
pub(crate) struct OneDriveCloudHome {
    drive_id: String,
    folder_id: String,
    graph_api: String,
    session: OAuthSession,
}

enum UploadSessionCompletion {
    Automatic,
    DeferredPersonal,
}

impl OneDriveCloudHome {
    pub(crate) fn new(
        drive_id: String,
        folder_id: String,
        tokens: OAuthTokens,
        key_service: StoreKeys,
        clock: ClockRef,
    ) -> Result<Self, CloudHomeError> {
        let config =
            Self::oauth_config().map_err(|e| CloudHomeError::Configuration(e.to_string()))?;
        Ok(Self {
            drive_id,
            folder_id,
            graph_api: GRAPH_API.to_string(),
            session: OAuthSession::new(tokens, key_service, clock, config, "OneDrive"),
        })
    }

    #[cfg(test)]
    fn with_graph_api(mut self, graph_api: String) -> Self {
        self.graph_api = graph_api;
        self
    }

    pub(crate) fn oauth_config() -> Result<OAuthConfig, crate::oauth::OAuthClientCredsError> {
        let creds = crate::oauth::oauth_client_creds("onedrive")?;
        Ok(OAuthConfig {
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
        })
    }

    fn client(&self) -> &reqwest::Client {
        self.session.client()
    }

    /// Build the Graph API URL for a file by encoded name within the app folder.
    fn item_path_url(&self, key: &str) -> String {
        format!(
            "{}/drives/{}/items/{}:/{}:",
            self.graph_api,
            self.drive_id,
            self.folder_id,
            encode_key(key)
        )
    }

    /// Build the Graph API URL for the folder's children endpoint.
    fn children_url(&self) -> String {
        format!(
            "{}/drives/{}/items/{}/children",
            self.graph_api, self.drive_id, self.folder_id
        )
    }

    async fn create_upload_session(
        &self,
        key: &str,
        conflict_behavior: &str,
        completion: UploadSessionCompletion,
    ) -> Result<String, CloudHomeError> {
        let session_url = format!("{}/createUploadSession", self.item_path_url(key));
        let body = serde_json::json!({
            "item": { "@microsoft.graph.conflictBehavior": conflict_behavior },
            "deferCommit": matches!(completion, UploadSessionCompletion::DeferredPersonal),
        });
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .post(&session_url)
                    .bearer_auth(token)
                    .json(&body)
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
        json["uploadUrl"]
            .as_str()
            .filter(|url| !url.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                CloudHomeError::Transport(format!("upload session {key}: no uploadUrl returned"))
            })
    }

    async fn commit_deferred_append(
        &self,
        key: &str,
        upload_url: &str,
    ) -> Result<(), CloudHomeError> {
        let body = serde_json::json!({
            "name": encode_key(key),
            "@microsoft.graph.conflictBehavior": "fail",
            "@microsoft.graph.sourceUrl": upload_url,
        });
        let response = match self
            .session
            .api_call(|token| {
                self.client()
                    .put(self.item_path_url(key))
                    .bearer_auth(token)
                    .json(&body)
            })
            .await
        {
            Ok(response) => response,
            Err(operation) => return Err(operation),
        };
        let status = response.status();
        if !status.is_success() {
            return Err(classify_write_error(
                status,
                &http::body_text(response).await,
                key,
            ));
        }
        let response_body = match response.bytes().await {
            Ok(body) => body,
            Err(operation) => {
                return Err(CloudHomeError::Transport(format!(
                    "commit create {key}: read response: {operation}"
                )))
            }
        };
        let _: serde_json::Value = serde_json::from_slice(&response_body).map_err(|error| {
            CloudHomeError::Transport(format!("commit create {key}: parse response: {error}"))
        })?;
        Ok(())
    }

    async fn verify_slot(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        require_logical_slot(slot, "OneDrive")?;
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(self.item_path_url(slot.logical_key()))
                    .bearer_auth(token)
                    .query(&[("$select", "id,name,parentReference,deleted,file")])
            })
            .await?;
        let response = ensure_ok(response, "verify exact OneDrive item", NotFound::Status).await?;
        let metadata: serde_json::Value =
            http::ok_json(response, "parse exact OneDrive item metadata").await?;
        let expected_name = encode_key(slot.logical_key());
        let matches = metadata["id"].as_str().is_some_and(|id| !id.is_empty())
            && metadata["name"].as_str() == Some(expected_name.as_str())
            && metadata["parentReference"]["id"].as_str() == Some(self.folder_id.as_str())
            && metadata["deleted"].is_null()
            && metadata["file"].is_object();
        if !matches {
            return Err(CloudHomeError::Transport(format!(
                "exact OneDrive slot for {} does not identify {expected_name} in folder {}",
                slot.logical_key(),
                self.folder_id
            )));
        }
        Ok(())
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
    let code = parse_onedrive_error_code(body);
    if code.as_deref() == Some("nameAlreadyExists") {
        return CloudHomeError::AlreadyExists(key.to_string());
    }
    if code.as_deref() == Some("quotaLimitReached") {
        return CloudHomeError::Transport(
            "Your OneDrive storage is full. Free up space at onedrive.live.com to keep syncing."
                .to_string(),
        );
    }
    CloudHomeError::Transport(format!("write {key} (HTTP {status}): {body}"))
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
            .map_err(|e| CloudHomeError::Transport(format!("parse list: {e}")))?;
        let encoded_prefix = encode_key(prefix);
        let mut keys = Vec::new();
        if let Some(items) = json["value"].as_array() {
            for item in items {
                if let Some(name) = item["name"].as_str() {
                    if name.starts_with(&encoded_prefix) {
                        let Some(decoded) = decode_listed_key("OneDrive", name) else {
                            continue;
                        };
                        keys.push(decoded)
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

impl OneDriveCloudHome {
    async fn create_at_slot(
        &self,
        slot: &ObjectSlot,
        mut body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        require_logical_slot(slot, "OneDrive")?;
        let full_logical_key = slot.logical_key();
        let upload_url = self
            .create_upload_session(
                full_logical_key,
                "fail",
                UploadSessionCompletion::DeferredPersonal,
            )
            .await?;
        let key = full_logical_key.to_string();
        let classify =
            Box::new(move |status, response: &str| classify_write_error(status, response, &key));
        let mut uploader = RangePutUploader::new(
            self.client().clone(),
            upload_url.clone(),
            202,
            body.len(),
            ONEDRIVE_CHUNK_SIZE,
            full_logical_key.to_string(),
            classify,
            onedrive_upload_cancellation_succeeded,
        );
        let total = body.len();
        let mut offset = 0;
        loop {
            let part = match body.next_part(uploader.part_size()).await {
                Ok(Some(part)) => part,
                Ok(None) if offset == total => break,
                Ok(None) => {
                    let operation = CloudHomeError::Transport(format!(
                        "append {full_logical_key}: upload body ended before the final part"
                    ));
                    let cleanup = uploader.abort().await;
                    return Err(combine_cleanup_failure(operation, cleanup));
                }
                Err(operation) => {
                    let cleanup = uploader.abort().await;
                    return Err(combine_cleanup_failure(operation, cleanup));
                }
            };
            let length = part.len() as u64;
            let is_last = offset + length >= total;
            let completion = uploader.send_deferred_part(part, offset, is_last).await?;
            offset += length;
            progress(offset);
            if let Some(response) = completion {
                response.bytes().await.map_err(|error| {
                    CloudHomeError::Transport(format!(
                        "append {full_logical_key}: read unexpected completion: {error}"
                    ))
                })?;
                let operation = CloudHomeError::Transport(format!(
                    "append {full_logical_key}: deferred upload published before explicit commit"
                ));
                let cleanup = self.delete_at_slot(slot).await;
                return Err(combine_cleanup_failure(operation, cleanup));
            }
        }
        let result = self
            .commit_deferred_append(full_logical_key, &upload_url)
            .await;
        match result {
            Ok(()) => {
                uploader.mark_completed();
                Ok(())
            }
            Err(operation) => {
                let cleanup = uploader.abort().await;
                Err(combine_cleanup_failure(operation, cleanup))
            }
        }
    }

    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        self.verify_slot(slot).await?;
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!(
                        "{}/content",
                        self.item_path_url(slot.logical_key())
                    ))
                    .bearer_auth(token)
            })
            .await?;
        let response = ensure_ok(response, "read exact OneDrive item", NotFound::Status).await?;
        super::oauth_rest::response_to_file(response, destination, "read exact OneDrive item body")
            .await
    }

    async fn delete_at_slot(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        require_logical_slot(slot, "OneDrive")?;
        match self.verify_slot(slot).await {
            Ok(()) => {}
            Err(CloudHomeError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error),
        }
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .delete(self.item_path_url(slot.logical_key()))
                    .bearer_auth(token)
            })
            .await?;
        let status = response.status();
        if status.is_success() || status == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(CloudHomeError::Transport(format!(
                "delete exact OneDrive item (HTTP {status}): {}",
                http::body_text(response).await
            )))
        }
    }
}

#[async_trait]
impl CloudHome for OneDriveCloudHome {
    fn exact_slot_storage(
        self: std::sync::Arc<Self>,
    ) -> Option<std::sync::Arc<dyn ExactSlotStorage>> {
        Some(self)
    }

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
        let upload_url = self
            .create_upload_session(key, "replace", UploadSessionCompletion::Automatic)
            .await?;
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
            onedrive_upload_cancellation_succeeded,
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

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        let email = desired.require_provider_email("OneDrive")?.to_string();
        let perms_url = format!(
            "{}/drives/{}/items/{}/permissions",
            self.graph_api, self.drive_id, self.folder_id
        );
        let email_of = |permission: &serde_json::Value| {
            permission["grantedToV2"]["user"]["email"]
                .as_str()
                .or_else(|| permission["grantedTo"]["user"]["email"].as_str())
                .map(String::from)
        };
        let next_page = |page: &serde_json::Value| {
            Ok(page["@odata.nextLink"]
                .as_str()
                .map(std::string::ToString::to_string))
        };
        let delete_url = |permission_id: &str| format!("{perms_url}/{permission_id}");
        let can_write = |permission: &serde_json::Value| {
            permission["roles"]
                .as_array()
                .is_some_and(|roles| roles.iter().any(|role| role.as_str() == Some("write")))
        };
        match desired {
            CloudAccessState::Present { .. } => {
                let current = sharing::permission_by_email(
                    &self.session,
                    &email,
                    &perms_url,
                    "value",
                    &email_of,
                    &next_page,
                )
                .await?;
                if current.as_ref().is_some_and(can_write) {
                    return Ok(CloudAccessOutcome::Present(CloudHomeJoinInfo::OneDrive {
                        drive_id: self.drive_id.clone(),
                        folder_id: self.folder_id.clone(),
                    }));
                }
                if current.is_some() {
                    sharing::ensure_absent_by_email(
                        &self.session,
                        &email,
                        &perms_url,
                        "value",
                        &email_of,
                        &delete_url,
                        &next_page,
                    )
                    .await?;
                }
                let url = format!(
                    "{}/drives/{}/items/{}/invite",
                    self.graph_api, self.drive_id, self.folder_id
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
                let verified = sharing::permission_by_email(
                    &self.session,
                    &email,
                    &perms_url,
                    "value",
                    &email_of,
                    &next_page,
                )
                .await?;
                if !verified.as_ref().is_some_and(can_write) {
                    return Err(CloudHomeError::Transport(format!(
                        "write permission for {email} is not visible after creation"
                    )));
                }
                Ok(CloudAccessOutcome::Present(CloudHomeJoinInfo::OneDrive {
                    drive_id: self.drive_id.clone(),
                    folder_id: self.folder_id.clone(),
                }))
            }
            CloudAccessState::Absent { .. } => {
                sharing::ensure_absent_by_email(
                    &self.session,
                    &email,
                    &perms_url,
                    "value",
                    &email_of,
                    &delete_url,
                    &next_page,
                )
                .await?;
                Ok(CloudAccessOutcome::Absent(RevokeOutcome::Revoked))
            }
        }
    }
}

#[async_trait]
impl ExactSlotStorage for OneDriveCloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<crate::storage::ResolvedProviderBinding, CloudHomeError> {
        use crate::storage::{
            ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
            StoreProviderBinding,
        };

        if self.drive_id.is_empty() || self.folder_id.is_empty() {
            return Err(CloudHomeError::Configuration(
                "OneDrive provider binding has an empty drive or folder id".to_string(),
            ));
        }
        let user_response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!("{}/me", self.graph_api))
                    .bearer_auth(token)
                    .query(&[("$select", "id")])
            })
            .await?;
        let user_response = ensure_ok(
            user_response,
            "resolve OneDrive principal",
            NotFound::Status,
        )
        .await?;
        let user: serde_json::Value =
            http::ok_json(user_response, "parse OneDrive principal").await?;
        let user_id = user["id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport(
                    "OneDrive /me response omitted the stable user id".to_string(),
                )
            })?
            .to_string();

        let folder_response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!(
                        "{}/drives/{}/items/{}",
                        self.graph_api, self.drive_id, self.folder_id
                    ))
                    .bearer_auth(token)
                    .query(&[("$select", "id,parentReference,folder")])
            })
            .await?;
        let folder_response = ensure_ok(
            folder_response,
            "resolve OneDrive folder binding",
            NotFound::Status,
        )
        .await?;
        let folder: serde_json::Value =
            http::ok_json(folder_response, "parse OneDrive folder binding").await?;
        if folder["id"].as_str() != Some(self.folder_id.as_str())
            || folder["parentReference"]["driveId"].as_str() != Some(self.drive_id.as_str())
            || !folder["folder"].is_object()
        {
            return Err(CloudHomeError::Transport(
                "OneDrive folder lookup returned a different drive/folder binding".to_string(),
            ));
        }

        Ok(ResolvedProviderBinding {
            store: StoreProviderBinding::OneDrive {
                drive_id: self.drive_id.clone(),
                folder_id: self.folder_id.clone(),
            },
            device: ProviderDeviceBinding {
                principal: ProviderPrincipalId::OneDrive { user_id },
            },
        })
    }

    async fn create_at(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        OneDriveCloudHome::create_at_slot(self, slot, body, progress).await
    }
    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        self.verify_slot(slot).await?;
        OneDriveCloudHome::read(self, slot.logical_key()).await
    }
    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        self.verify_slot(slot).await?;
        OneDriveCloudHome::read_range(self, slot.logical_key(), start, end).await
    }
    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        OneDriveCloudHome::read_at_to_file(self, slot, destination).await
    }
    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        OneDriveCloudHome::delete_at_slot(self, slot).await
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

    #[test]
    fn onedrive_does_not_accept_drive_cancellation_status() {
        let drive_canceled = StatusCode::from_u16(499).expect("valid Drive cancellation status");
        assert!(!onedrive_upload_cancellation_succeeded(drive_canceled));
    }

    fn home() -> OneDriveCloudHome {
        crate::oauth::install_test_client_creds();
        OneDriveCloudHome::new(
            "drive123".to_string(),
            "folder456".to_string(),
            OAuthTokens {
                access_token: "test".to_string(),
                refresh_token: None,
                expires_at: None,
            },
            StoreKeys::new("test".to_string()),
            Arc::new(crate::clock::SystemClock),
        )
        .expect("build test OneDrive home")
    }

    #[derive(Clone, Debug)]
    struct RecordedRequest {
        method: String,
        path: String,
        body: Vec<u8>,
    }

    #[derive(Clone)]
    struct ExactCreateEndpointState {
        endpoint: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    async fn exact_create_endpoint(
        State(state): State<ExactCreateEndpointState>,
        request: Request<Body>,
    ) -> Response<Body> {
        let method = request.method().to_string();
        let path = request.uri().path().to_string();
        let body = to_bytes(request.into_body(), usize::MAX)
            .await
            .expect("read request body")
            .to_vec();
        state
            .requests
            .lock()
            .expect("lock requests")
            .push(RecordedRequest {
                method: method.clone(),
                path: path.clone(),
                body,
            });

        if method == "POST" && path.ends_with("/createUploadSession") {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"uploadUrl":"{}/upload/session"}}"#,
                    state.endpoint
                )))
                .expect("build session response");
        }
        if method == "PUT" && path == "/upload/session" {
            return Response::builder()
                .status(StatusCode::ACCEPTED)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"nextExpectedRanges":[]}"#))
                .expect("build deferred upload response");
        }
        if method == "PUT" && path.contains("/items/folder456:/") {
            return Response::builder()
                .status(StatusCode::CREATED)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"item-1"}"#))
                .expect("build commit response");
        }
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!("unexpected request: {method} {path}")))
            .expect("build unexpected response")
    }

    async fn exact_create_test_home() -> (
        OneDriveCloudHome,
        Arc<Mutex<Vec<RecordedRequest>>>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind OneDrive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = ExactCreateEndpointState {
            endpoint: endpoint.clone(),
            requests: requests.clone(),
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .fallback(exact_create_endpoint)
                    .with_state(state),
            )
            .with_graceful_shutdown(async {
                shutdown_rx
                    .await
                    .expect("receive OneDrive endpoint shutdown");
            })
            .await
            .expect("OneDrive endpoint failed");
        });
        (home().with_graph_api(endpoint), requests, shutdown_tx)
    }

    #[tokio::test]
    async fn exact_create_defers_publication_then_commits_the_destination() {
        let (home, requests, shutdown) = exact_create_test_home().await;
        let slot = home
            .allocate_slot("protocol/copy")
            .await
            .expect("allocate OneDrive slot");
        home.create_at(
            &slot,
            BlobBody::from_bytes(b"copy-bytes".to_vec()),
            &super::super::no_progress(),
        )
        .await
        .expect("create exact OneDrive object");
        assert_eq!(
            slot,
            ObjectSlot::logical("protocol/copy".to_string()).expect("logical OneDrive slot")
        );

        let requests = requests.lock().expect("lock requests");
        assert_eq!(requests.len(), 3, "{requests:?}");
        let session: serde_json::Value =
            serde_json::from_slice(&requests[0].body).expect("parse session body");
        assert_eq!(requests[0].method, "POST");
        assert!(requests[0].path.ends_with("/createUploadSession"));
        assert_eq!(session["item"]["@microsoft.graph.conflictBehavior"], "fail");
        assert_eq!(session["deferCommit"], true);
        assert_eq!(requests[1].path, "/upload/session");
        assert_eq!(requests[1].body, b"copy-bytes");
        let commit: serde_json::Value =
            serde_json::from_slice(&requests[2].body).expect("parse commit body");
        assert_eq!(requests[2].method, "PUT");
        assert_eq!(commit["name"], encode_key("protocol/copy"));
        assert_eq!(commit["@microsoft.graph.conflictBehavior"], "fail");
        assert!(commit["@microsoft.graph.sourceUrl"]
            .as_str()
            .is_some_and(|url| url.ends_with("/upload/session")));
        drop(requests);
        shutdown.send(()).expect("shut down OneDrive endpoint");
    }

    #[tokio::test]
    async fn exact_operations_reject_an_opaque_onedrive_locator() {
        let home = home();
        let slot = ObjectSlot::opaque("protocol/other".to_string(), "item-1".to_string())
            .expect("build opaque OneDrive slot");

        let read_error = home
            .read_at(&slot)
            .await
            .expect_err("opaque OneDrive read must fail");
        assert!(
            read_error.to_string().contains("must use its logical key"),
            "{read_error}"
        );
        let delete_error = home
            .delete_at(&slot)
            .await
            .expect_err("opaque OneDrive delete must fail");
        assert!(
            delete_error
                .to_string()
                .contains("must use its logical key"),
            "{delete_error}"
        );
    }

    async fn ambiguous_commit_endpoint(
        State(endpoint): State<String>,
        request: Request<Body>,
    ) -> Response<Body> {
        let method = request.method().as_str();
        let path = request.uri().path();
        if method == "POST" && path.ends_with("/createUploadSession") {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"uploadUrl":"{endpoint}/upload/session"}}"#
                )))
                .expect("build upload session response");
        }
        if method == "PUT" && path == "/upload/session" {
            return Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Body::from(r#"{"nextExpectedRanges":[]}"#))
                .expect("build upload response");
        }
        if method == "PUT" && path.contains("/items/folder456:/") {
            let stream = futures_util::stream::iter([
                Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from_static(b"{")),
                Err(std::io::Error::other("commit response interrupted")),
            ]);
            return Response::builder()
                .status(StatusCode::CREATED)
                .body(Body::from_stream(stream))
                .expect("build interrupted commit response");
        }
        if method == "GET" && path.contains("/items/folder456:/") {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"pre-existing-item"}"#))
                .expect("build occupant response");
        }
        if method == "DELETE" && path == "/upload/session" {
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("build cancellation response");
        }
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!("unexpected request: {method} {path}")))
            .expect("build unexpected response")
    }

    #[tokio::test]
    async fn ambiguous_commit_does_not_adopt_the_current_path_occupant() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind OneDrive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let app = Router::new()
            .fallback(ambiguous_commit_endpoint)
            .with_state(endpoint.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("OneDrive endpoint failed");
        });
        let home = home().with_graph_api(endpoint);

        let slot = home
            .allocate_slot("protocol/copy")
            .await
            .expect("allocate OneDrive slot");
        let error = home
            .create_at(
                &slot,
                BlobBody::from_bytes(b"copy-bytes".to_vec()),
                &super::super::no_progress(),
            )
            .await
            .expect_err("ambiguous commit must remain unresolved");

        assert!(matches!(error, CloudHomeError::Transport(_)), "{error}");
        server.abort();
    }

    #[test]
    fn item_path_url_encodes_key() {
        assert_eq!(
            home().item_path_url("objects/dev1/42.enc"),
            "https://graph.microsoft.com/v1.0/drives/drive123/items/folder456:/6f626a656374732f646576312f34322e656e63:"
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
    fn parse_list_page_skips_malformed_flat_names() {
        let valid = encode_key("objects/dev1/1.enc");
        let malformed = format!("{}zz", encode_key("objects/"));
        let body = format!(
            r#"{{"value":[{{"name":"{valid}"}},{{"name":"{malformed}"}},{{"name":"{}"}}]}}"#,
            encode_key("snapshots/dev1.json.enc"),
        );

        let page = home()
            .parse_list_page(&body, "objects/")
            .expect("parse list page");

        assert_eq!(page.keys, vec!["objects/dev1/1.enc"]);
    }

    #[test]
    fn oauth_config_uses_consumers_endpoint() {
        crate::oauth::install_test_client_creds();
        let config = OneDriveCloudHome::oauth_config().expect("build OneDrive oauth config");
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
            classify_write_error(reqwest::StatusCode::INSUFFICIENT_STORAGE, body, "objects/1");
        let msg = err.to_string();
        assert!(msg.contains("OneDrive storage is full"), "{msg}");
        assert!(msg.contains("Free up space"), "{msg}");
    }

    #[test]
    fn classify_write_error_keeps_raw_for_non_quota_errors() {
        let body = r#"{"error":{"code":"itemNotFound","message":"..."}}"#;
        let err = classify_write_error(reqwest::StatusCode::NOT_FOUND, body, "objects/dev1/1.enc");
        let msg = err.to_string();
        assert!(msg.contains("HTTP 404"), "{msg}");
        assert!(msg.contains("objects/dev1/1.enc"), "{msg}");
        assert!(!msg.contains("storage is full"), "{msg}");
    }
}
