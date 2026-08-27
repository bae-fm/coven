//! OneDrive `CloudHome` implementation.
//!
//! Uses the Microsoft Graph API. Files are stored flat in a single folder — path
//! separators are escaped by the `key_encoding` helpers. The
//! `read`/`read_range`/`list`/`delete` methods are the shared `OAuthRestHome`
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
use super::{
    combine_cleanup_failure, sharing, BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState,
    CloudHome, CloudHomeError, CloudHomeJoinInfo, ExactSlotStorage, RevokeOutcome,
};
use crate::oauth::OAuthConfig;
use coven_protocol::objects::ObjectSlot;

#[path = "onedrive/content_hash.rs"]
mod content_hash;

#[derive(Clone, Debug, PartialEq, Eq)]
struct OneDriveExactMetadata {
    size: u64,
    sha1_hash: String,
}

const GRAPH_API: &str = "https://graph.microsoft.com/v1.0";

fn onedrive_upload_cancellation_succeeded(status: reqwest::StatusCode) -> bool {
    status.is_success() || status == reqwest::StatusCode::NOT_FOUND
}

/// OneDrive cloud home backend.
pub struct OneDriveCloudHome {
    drive_id: String,
    folder_id: String,
    graph_api: String,
    session: OAuthSession,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
}

enum UploadSessionCompletion {
    Automatic,
    DeferredPersonal,
}

impl OneDriveCloudHome {
    pub fn new(
        drive_id: String,
        folder_id: String,
        session: OAuthSession,
        exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    ) -> Self {
        Self {
            drive_id,
            folder_id,
            graph_api: GRAPH_API.to_string(),
            session,
            exact_upload_verification,
        }
    }

    pub(crate) fn oauth_config(creds: crate::oauth::OAuthClientCreds) -> OAuthConfig {
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
            .api_call(|oauth| oauth.post(&session_url).json(&body))
            .await?;
        let status = response.status();
        let body = http::body_text(response).await;
        if !status.is_success() {
            return Err(classify_write_error(status, &body, key));
        }
        let json: serde_json::Value = serde_json::from_str(&body).map_err(|error| {
            CloudHomeError::transport(format!("parse upload session {key}"), error)
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
            .api_call(|oauth| oauth.put(self.item_path_url(key)).json(&body))
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
            CloudHomeError::transport(format!("commit create {key}: parse response"), error)
        })?;
        Ok(())
    }

    async fn exact_metadata(
        &self,
        slot: &ObjectSlot,
    ) -> Result<OneDriveExactMetadata, CloudHomeError> {
        slot.require_logical_key_for("OneDrive")?;
        let response = self
            .session
            .api_call(|oauth| {
                oauth
                    .get(self.item_path_url(slot.logical_key()))
                    .query(&[("$select", "id,name,parentReference,deleted,file,size")])
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
        let size = metadata["size"].as_u64().ok_or_else(|| {
            CloudHomeError::Transport(format!(
                "exact OneDrive metadata for {} omitted size",
                slot.logical_key()
            ))
        })?;
        let sha1_hash = metadata["file"]["hashes"]["sha1Hash"]
            .as_str()
            .filter(|hash| !hash.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport(format!(
                    "exact OneDrive metadata for {} omitted file.hashes.sha1Hash",
                    slot.logical_key()
                ))
            })?
            .to_string();
        Ok(OneDriveExactMetadata { size, sha1_hash })
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
                "OneDrive does not expose upload-checksum enforcement for this endpoint"
                    .to_string(),
            )),
            ExactUploadVerification::MetadataHash => {
                let metadata = self.exact_metadata(upload.object().slot()).await?;
                let expected_sha1 = content_hash::sha1(upload).await?;
                if metadata.size != upload.object().stored_size()
                    || !metadata.sha1_hash.eq_ignore_ascii_case(&expected_sha1)
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

    async fn create_at_slot(
        &self,
        slot: &ObjectSlot,
        mut body: BlobBody,
        control: &super::UploadControl,
    ) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("OneDrive")?;
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
        let mut uploader = self.session.range_put_uploader(
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
            let completion = uploader
                .send_deferred_part(part, offset, is_last, control)
                .await?;
            offset += length;
            control.report(offset);
            if let Some(response) = completion {
                response.bytes().await.map_err(|error| {
                    CloudHomeError::transport(
                        format!("read unexpected append completion for {full_logical_key}"),
                        error,
                    )
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
        progress: super::DownloadProgress,
    ) -> Result<(), super::CloudFileReadError> {
        self.verify_slot(slot).await?;
        let response = self
            .session
            .api_call(|oauth| {
                oauth.get(format!(
                    "{}/content",
                    self.item_path_url(slot.logical_key())
                ))
            })
            .await?;
        let response = ensure_ok(response, "read exact OneDrive item", NotFound::Status).await?;
        super::oauth_rest::response_to_file(
            response,
            destination,
            "read exact OneDrive item body",
            progress,
        )
        .await
    }

    async fn delete_at_slot(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("OneDrive")?;
        match self.verify_slot(slot).await {
            Ok(()) => {}
            Err(CloudHomeError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error),
        }
        let response = self
            .session
            .api_call(|oauth| oauth.delete(self.item_path_url(slot.logical_key())))
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

    #[cfg(test)]
    fn with_graph_api(mut self, graph_api: String) -> Self {
        self.graph_api = graph_api;
        self
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
            .api_call(|oauth| {
                let mut req = oauth.get(&url);
                if let Some(ref range) = range {
                    req = req.header("Range", range);
                }
                req
            })
            .await
    }

    async fn send_delete(&self, key: &str) -> Result<reqwest::Response, CloudHomeError> {
        let url = self.item_path_url(key);
        self.session.api_call(|oauth| oauth.delete(&url)).await
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
        self.session.api_call(|oauth| oauth.get(&url)).await
    }

    fn parse_list_page(&self, body: &str, prefix: &str) -> Result<ListPage, CloudHomeError> {
        let json: serde_json::Value = serde_json::from_str(body)
            .map_err(|e| CloudHomeError::transport("parse list".to_string(), e))?;
        let encoded_prefix = encode_key(prefix);
        let mut slots = Vec::new();
        if let Some(items) = json["value"].as_array() {
            for item in items {
                if let Some(name) = item["name"].as_str() {
                    if name.starts_with(&encoded_prefix) {
                        let Some(decoded) = decode_listed_key("OneDrive", name) else {
                            continue;
                        };
                        slots.push(ObjectSlot::logical(decoded)?)
                    }
                }
            }
        }
        Ok(ListPage {
            slots,
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
            .api_call(|oauth| {
                oauth
                    .put(&url)
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
        Ok(Box::new(self.session.range_put_sink(
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
        let resp = self.session.api_call(|oauth| oauth.get(&url)).await?;
        exists_from_response(resp, &format!("exists {key}"), NotFound::Status).await
    }

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        let email = desired.require_provider_email("OneDrive")?;
        let perms_url = format!(
            "{}/drives/{}/items/{}/permissions",
            self.graph_api, self.drive_id, self.folder_id
        );
        let access = sharing::SharedFolderAccess::new(
            &self.session,
            perms_url.clone(),
            "value",
            |permission: &serde_json::Value| {
                permission["grantedToV2"]["user"]["email"]
                    .as_str()
                    .or_else(|| permission["grantedTo"]["user"]["email"].as_str())
                    .map(String::from)
            },
            |page: &serde_json::Value| {
                Ok(page["@odata.nextLink"]
                    .as_str()
                    .map(std::string::ToString::to_string))
            },
            |permission_id: &str| format!("{perms_url}/{permission_id}"),
            |permission: &serde_json::Value| {
                permission["roles"]
                    .as_array()
                    .is_some_and(|roles| roles.iter().any(|role| role.as_str() == Some("write")))
            },
            "write",
            format!(
                "{}/drives/{}/items/{}/invite",
                self.graph_api, self.drive_id, self.folder_id
            ),
            serde_json::json!({
                "recipients": [{"email": email}],
                "roles": ["write"],
                "requireSignIn": true,
            }),
        );
        match desired {
            CloudAccessState::Present { .. } => {
                access.ensure_present(email).await?;
                Ok(CloudAccessOutcome::Present(CloudHomeJoinInfo::OneDrive {
                    drive_id: self.drive_id.clone(),
                    folder_id: self.folder_id.clone(),
                }))
            }
            CloudAccessState::Absent { .. } => {
                access.ensure_absent(email).await?;
                Ok(CloudAccessOutcome::Absent(RevokeOutcome::Revoked))
            }
        }
    }
}

#[async_trait]
impl ExactSlotStorage for OneDriveCloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<coven_protocol::objects::ResolvedProviderBinding, CloudHomeError> {
        use coven_protocol::objects::{
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
            .api_call(|oauth| {
                oauth
                    .get(format!("{}/me", self.graph_api))
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
            .api_call(|oauth| {
                oauth
                    .get(format!(
                        "{}/drives/{}/items/{}",
                        self.graph_api, self.drive_id, self.folder_id
                    ))
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
        upload: &super::ExactUpload<'_>,
        control: &super::UploadControl,
    ) -> Result<super::ExactCreateOutcome, CloudHomeError> {
        if matches!(
            self.exact_upload_verification,
            coven_foundation::config::ExactUploadVerification::UploadChecksum
        ) {
            return Err(CloudHomeError::Configuration(
                "OneDrive does not expose upload-checksum enforcement for this endpoint"
                    .to_string(),
            ));
        }
        let operation = OneDriveCloudHome::create_at_slot(
            self,
            upload.object().slot(),
            upload.body().await?,
            control,
        )
        .await;
        super::exact_upload::settle_exact_create(operation, |observed| {
            self.verify_exact_upload(upload, observed)
        })
        .await
    }
    async fn list_slots(&self, prefix: &str) -> Result<Vec<ObjectSlot>, CloudHomeError> {
        crate::cloud::logical_slots(CloudHome::list(self, prefix).await?)
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
        progress: super::DownloadProgress,
    ) -> Result<(), super::CloudFileReadError> {
        OneDriveCloudHome::read_at_to_file(self, slot, destination, progress).await
    }
    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        OneDriveCloudHome::delete_at_slot(self, slot).await
    }
}

#[cfg(test)]
#[path = "onedrive_tests.rs"]
mod tests;
