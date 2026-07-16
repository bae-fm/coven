//! OneDrive `CloudHome` implementation.
//!
//! Uses the Microsoft Graph API. Files are stored flat in a single folder — path
//! separators are escaped by [`super::key_encoding`]. The
//! `read`/`read_range`/`list`/`delete` methods are the shared [`OAuthRestHome`]
//! implementations; this file supplies only the Graph request shapes, the page
//! parser, the upload paths, and sharing.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;

use super::http::{self, ensure_ok, exists_from_response, NotFound};
use super::key_encoding::{decode_listed_key, encode_key};
use super::oauth_rest::{
    rest_delete, rest_list, rest_read, rest_read_range, ListPage, OAuthRestHome, PageTokenTracker,
};
use super::oauth_session::OAuthSession;
use super::resumable::{RangePutSink, RangePutUploader};
use super::{
    sharing, AppendedListing, AppendedObject, BlobBody, BoxPartSink, CloudAccessOutcome,
    CloudAccessState, CloudHome, CloudHomeError, CloudHomeJoinInfo, ImmutableCopyStorage,
    ListingCoverage, RevokeOutcome, UploadProgress,
};
use crate::clock::ClockRef;
use crate::keys::StoreKeys;
use crate::oauth::{OAuthConfig, OAuthTokens};

const GRAPH_API: &str = "https://graph.microsoft.com/v1.0";

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

    fn item_id_url(&self, item_id: &str) -> String {
        format!(
            "{}/drives/{}/items/{item_id}",
            self.graph_api, self.drive_id
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

    async fn resolve_item_by_path(
        &self,
        key: &str,
    ) -> Result<Option<AppendedObject>, CloudHomeError> {
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(self.item_path_url(key))
                    .bearer_auth(token)
                    .query(&[("$select", "id")])
            })
            .await?;
        match ensure_ok(
            response,
            "resolve committed OneDrive item",
            NotFound::Status,
        )
        .await
        {
            Ok(response) => {
                let body = http::ok_bytes(response, "read committed OneDrive item").await?;
                parse_onedrive_appended_object(key, &body).map(Some)
            }
            Err(CloudHomeError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn commit_deferred_append(
        &self,
        key: &str,
        upload_url: &str,
    ) -> Result<AppendedObject, CloudHomeError> {
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
            Err(operation) => {
                return match self.resolve_item_by_path(key).await {
                    Ok(Some(_)) => Err(CloudHomeError::AlreadyExists(key.to_string())),
                    Ok(None) => Err(operation),
                    Err(cleanup) => Err(CloudHomeError::CleanupFailed {
                        operation: Box::new(operation),
                        cleanup: Box::new(cleanup),
                    }),
                }
            }
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
                let operation = CloudHomeError::Transport(format!(
                    "commit append {key}: read response: {operation}"
                ));
                return match self.resolve_item_by_path(key).await {
                    Ok(Some(_)) => Err(CloudHomeError::AlreadyExists(key.to_string())),
                    Ok(None) => Err(operation),
                    Err(cleanup) => Err(CloudHomeError::CleanupFailed {
                        operation: Box::new(operation),
                        cleanup: Box::new(cleanup),
                    }),
                };
            }
        };
        parse_onedrive_appended_object(key, &response_body)
    }

    async fn verify_appended_object(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        let item_id = object.opaque_provider_id();
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(self.item_id_url(item_id))
                    .bearer_auth(token)
                    .query(&[("$select", "id,name,parentReference,deleted,file")])
            })
            .await?;
        let response = ensure_ok(response, "verify exact OneDrive item", NotFound::Status).await?;
        let metadata: serde_json::Value =
            http::ok_json(response, "parse exact OneDrive item metadata").await?;
        let expected_name = encode_key(object.logical_key());
        let matches = metadata["id"].as_str() == Some(item_id)
            && metadata["name"].as_str() == Some(expected_name.as_str())
            && metadata["parentReference"]["id"].as_str() == Some(self.folder_id.as_str())
            && metadata["deleted"].is_null()
            && metadata["file"].is_object();
        if !matches {
            return Err(CloudHomeError::Transport(format!(
                "exact OneDrive locator for {} does not identify item {item_id} named {expected_name} in folder {}",
                object.logical_key(), self.folder_id
            )));
        }
        Ok(())
    }

    async fn list_appended_objects(
        &self,
        prefix: &str,
    ) -> Result<Vec<AppendedObject>, CloudHomeError> {
        let mut next = Some(format!(
            "{}/drives/{}/items/{}/delta?$select=id,name,parentReference,deleted,file",
            self.graph_api, self.drive_id, self.folder_id
        ));
        let mut page_tokens = PageTokenTracker::new("OneDrive delta listing");
        let mut objects = HashMap::new();
        while let Some(url) = next.take() {
            let response = self
                .session
                .api_call(|token| self.client().get(&url).bearer_auth(token))
                .await?;
            let response = ensure_ok(response, "list OneDrive delta", NotFound::Status).await?;
            let page: serde_json::Value = http::ok_json(response, "parse OneDrive delta").await?;
            apply_onedrive_delta_page(&page, &self.folder_id, prefix, &mut objects)?;
            let next_link = page["@odata.nextLink"].as_str();
            let delta_link = page["@odata.deltaLink"].as_str();
            match (next_link, delta_link) {
                (Some(next_link), None) if !next_link.is_empty() => {
                    next = Some(page_tokens.record(next_link)?)
                }
                (None, Some(delta_link)) if !delta_link.is_empty() => break,
                _ => return Err(CloudHomeError::Transport(
                    "OneDrive delta page must contain exactly one non-empty nextLink or deltaLink"
                        .to_string(),
                )),
            }
        }
        let mut objects: Vec<_> = objects.into_values().collect();
        objects.sort_by(|left, right| {
            left.logical_key()
                .cmp(right.logical_key())
                .then_with(|| left.opaque_provider_id().cmp(right.opaque_provider_id()))
        });
        Ok(objects)
    }
}

fn apply_onedrive_delta_page(
    page: &serde_json::Value,
    folder_id: &str,
    prefix: &str,
    objects: &mut HashMap<String, AppendedObject>,
) -> Result<(), CloudHomeError> {
    let items = page["value"].as_array().ok_or_else(|| {
        CloudHomeError::Transport("OneDrive delta page omitted value".to_string())
    })?;
    for item in items {
        let id = item["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport("OneDrive delta item omitted id".to_string())
            })?;
        if item.get("deleted").is_some() {
            objects.remove(id);
            continue;
        }
        if !item["file"].is_object() {
            continue;
        }
        let name = item["name"].as_str().ok_or_else(|| {
            CloudHomeError::Transport(format!("OneDrive delta file {id} omitted name"))
        })?;
        let parent_id = item["parentReference"]["id"].as_str().ok_or_else(|| {
            CloudHomeError::Transport(format!(
                "OneDrive delta file {id} omitted parentReference.id"
            ))
        })?;
        if parent_id != folder_id {
            objects.remove(id);
            continue;
        }
        let Some(logical_key) = decode_listed_key("OneDrive", name) else {
            objects.remove(id);
            continue;
        };
        if logical_key.starts_with(prefix) {
            objects.insert(
                id.to_string(),
                AppendedObject::from_provider(logical_key, id.to_string())?,
            );
        } else {
            objects.remove(id);
        }
    }
    Ok(())
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

fn combine_onedrive_cleanup_failure(
    operation: CloudHomeError,
    cleanup: Result<(), CloudHomeError>,
) -> CloudHomeError {
    match cleanup {
        Ok(()) => operation,
        Err(cleanup) => CloudHomeError::CleanupFailed {
            operation: Box::new(operation),
            cleanup: Box::new(cleanup),
        },
    }
}

fn parse_onedrive_appended_object(
    key: &str,
    body: &[u8],
) -> Result<AppendedObject, CloudHomeError> {
    let json: serde_json::Value = serde_json::from_slice(body).map_err(|error| {
        CloudHomeError::Transport(format!("append {key}: parse response: {error}"))
    })?;
    let id = json["id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| {
            CloudHomeError::Transport(format!("append {key}: response missing item id"))
        })?;
    AppendedObject::from_provider(key.to_string(), id.to_string())
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
        )))
    }

    fn multipart_threshold(&self) -> u64 {
        ONEDRIVE_SIMPLE_PUT_MAX as u64
    }

    async fn append_object(
        &self,
        full_logical_key: &str,
        mut body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<AppendedObject, CloudHomeError> {
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
                    return Err(combine_onedrive_cleanup_failure(operation, cleanup));
                }
                Err(operation) => {
                    let cleanup = uploader.abort().await;
                    return Err(combine_onedrive_cleanup_failure(operation, cleanup));
                }
            };
            let length = part.len() as u64;
            let is_last = offset + length >= total;
            let completion = uploader.send_deferred_part(part, offset, is_last).await?;
            offset += length;
            progress(offset);
            if let Some(response) = completion {
                let completion = response.bytes().await.map_err(|error| {
                    CloudHomeError::Transport(format!(
                        "append {full_logical_key}: read unexpected completion: {error}"
                    ))
                })?;
                let object = parse_onedrive_appended_object(full_logical_key, &completion)?;
                let operation = CloudHomeError::Transport(format!(
                    "append {full_logical_key}: deferred upload published before explicit commit"
                ));
                let cleanup = self.delete_appended(&object).await;
                return Err(combine_onedrive_cleanup_failure(operation, cleanup));
            }
        }
        let result = self
            .commit_deferred_append(full_logical_key, &upload_url)
            .await;
        match result {
            Ok(object) => {
                uploader.mark_completed();
                Ok(object)
            }
            Err(operation) => {
                let cleanup = uploader.abort().await;
                Err(combine_onedrive_cleanup_failure(operation, cleanup))
            }
        }
    }

    async fn list_appended(&self, prefix: &str) -> Result<AppendedListing, CloudHomeError> {
        Ok(AppendedListing {
            objects: self.list_appended_objects(prefix).await?,
            coverage: ListingCoverage::CompleteAtScan,
        })
    }

    async fn read_appended(&self, object: &AppendedObject) -> Result<Vec<u8>, CloudHomeError> {
        self.verify_appended_object(object).await?;
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!(
                        "{}/content",
                        self.item_id_url(object.opaque_provider_id())
                    ))
                    .bearer_auth(token)
            })
            .await?;
        let response = ensure_ok(response, "read exact OneDrive item", NotFound::Status).await?;
        Ok(http::ok_bytes(response, "read exact OneDrive body")
            .await?
            .to_vec())
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        rest_read(self, key).await
    }

    async fn read_appended_to_file(
        &self,
        object: &super::AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        self.verify_appended_object(object).await?;
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!(
                        "{}/content",
                        self.item_id_url(object.opaque_provider_id())
                    ))
                    .bearer_auth(token)
            })
            .await?;
        let response = ensure_ok(response, "read exact OneDrive item", NotFound::Status).await?;
        super::oauth_rest::response_to_file(response, destination, "read exact OneDrive item body")
            .await
    }

    async fn delete_appended(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        self.verify_appended_object(object).await?;
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .delete(self.item_id_url(object.opaque_provider_id()))
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
                if current.as_ref().is_some_and(|permission| {
                    permission["roles"].as_array().is_some_and(|roles| {
                        roles.iter().any(|role| role.as_str() == Some("write"))
                    })
                }) {
                    return Ok(CloudAccessOutcome::Present(CloudHomeJoinInfo::OneDrive {
                        drive_id: self.drive_id.clone(),
                        folder_id: self.folder_id.clone(),
                    }));
                }
                if current.is_some() {
                    let delete_base = perms_url.clone();
                    sharing::ensure_absent_by_email(
                        &self.session,
                        &email,
                        &perms_url,
                        "value",
                        &email_of,
                        |permission_id| format!("{delete_base}/{permission_id}"),
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
                if !verified.as_ref().is_some_and(|permission| {
                    permission["roles"].as_array().is_some_and(|roles| {
                        roles.iter().any(|role| role.as_str() == Some("write"))
                    })
                }) {
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
                let delete_base = perms_url.clone();
                sharing::ensure_absent_by_email(
                    &self.session,
                    &email,
                    &perms_url,
                    "value",
                    &email_of,
                    |permission_id| format!("{delete_base}/{permission_id}"),
                    &next_page,
                )
                .await?;
                Ok(CloudAccessOutcome::Absent(RevokeOutcome::Revoked))
            }
        }
    }
}

#[async_trait]
impl CloudHome for OneDriveCloudHome {
    fn immutable_copy_storage(
        self: std::sync::Arc<Self>,
    ) -> Option<std::sync::Arc<dyn ImmutableCopyStorage>> {
        Some(self)
    }
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        OneDriveCloudHome::put_object(self, key, data).await
    }
    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        OneDriveCloudHome::open_multipart(self, key, total_len).await
    }
    fn multipart_threshold(&self) -> u64 {
        OneDriveCloudHome::multipart_threshold(self)
    }
    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        OneDriveCloudHome::read(self, key).await
    }
    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        OneDriveCloudHome::read_range(self, key, start, end).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        OneDriveCloudHome::list(self, prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        OneDriveCloudHome::delete(self, key).await
    }
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        OneDriveCloudHome::exists(self, key).await
    }
    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        OneDriveCloudHome::set_access(self, desired).await
    }
}

#[async_trait]
impl ImmutableCopyStorage for OneDriveCloudHome {
    async fn append_object(
        &self,
        key: &str,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<AppendedObject, CloudHomeError> {
        OneDriveCloudHome::append_object(self, key, body, progress).await
    }
    async fn list_appended(&self, prefix: &str) -> Result<AppendedListing, CloudHomeError> {
        OneDriveCloudHome::list_appended(self, prefix).await
    }
    async fn read_appended(&self, object: &AppendedObject) -> Result<Vec<u8>, CloudHomeError> {
        OneDriveCloudHome::read_appended(self, object).await
    }
    async fn read_appended_to_file(
        &self,
        object: &AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        OneDriveCloudHome::read_appended_to_file(self, object, destination).await
    }
    async fn delete_appended(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        OneDriveCloudHome::delete_appended(self, object).await
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
    struct AppendEndpointState {
        endpoint: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    async fn append_endpoint(
        State(state): State<AppendEndpointState>,
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

    async fn append_test_home() -> (
        OneDriveCloudHome,
        Arc<Mutex<Vec<RecordedRequest>>>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind OneDrive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = AppendEndpointState {
            endpoint: endpoint.clone(),
            requests: requests.clone(),
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(append_endpoint).with_state(state),
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
    async fn append_defers_publication_then_commits_the_exact_destination() {
        let (home, requests, shutdown) = append_test_home().await;
        let object = home
            .append_object(
                "protocol/copy",
                BlobBody::from_bytes(b"copy-bytes".to_vec()),
                &super::super::no_progress(),
            )
            .await
            .expect("append OneDrive copy");
        assert_eq!(object.opaque_provider_id(), "item-1");

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

    async fn exact_item_endpoint(request: Request<Body>) -> Response<Body> {
        let method = request.method().as_str();
        let path = request.uri().path();
        if method == "GET" && path.ends_with("/items/item-1") {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"id":"item-1","name":"{}","parentReference":{{"id":"folder456"}},"file":{{}}}}"#,
                    encode_key("protocol/copy")
                )))
                .expect("build exact metadata response");
        }
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!("unexpected request: {method} {path}")))
            .expect("build unexpected response")
    }

    #[tokio::test]
    async fn exact_operations_reject_a_onedrive_id_bound_to_another_logical_key() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind OneDrive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().fallback(exact_item_endpoint))
                .await
                .expect("OneDrive endpoint failed");
        });
        let home = home().with_graph_api(endpoint);
        let object =
            AppendedObject::from_provider("protocol/other".to_string(), "item-1".to_string())
                .expect("build mismatched OneDrive locator");

        let read_error = home
            .read_appended(&object)
            .await
            .expect_err("mismatched OneDrive read must fail");
        assert!(
            read_error.to_string().contains("does not identify"),
            "{read_error}"
        );
        let delete_error = home
            .delete_appended(&object)
            .await
            .expect_err("mismatched OneDrive delete must fail");
        assert!(
            delete_error.to_string().contains("does not identify"),
            "{delete_error}"
        );
        server.abort();
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

        let error = home
            .append_object(
                "protocol/copy",
                BlobBody::from_bytes(b"copy-bytes".to_vec()),
                &super::super::no_progress(),
            )
            .await
            .expect_err("ambiguous commit must not adopt a path occupant");

        assert!(matches!(error, CloudHomeError::AlreadyExists(key) if key == "protocol/copy"));
        server.abort();
    }

    async fn repeated_delta_link_endpoint(request: Request<Body>) -> Response<Body> {
        let authority = request
            .headers()
            .get("host")
            .expect("host header")
            .to_str()
            .expect("host header is UTF-8");
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(format!(
                r#"{{"value":[],"@odata.nextLink":"http://{authority}/same"}}"#
            )))
            .expect("build repeated delta link response")
    }

    #[tokio::test]
    async fn authoritative_listing_rejects_a_repeated_next_link() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind OneDrive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(repeated_delta_link_endpoint),
            )
            .await
            .expect("OneDrive endpoint failed");
        });
        let home = home().with_graph_api(endpoint);

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            home.list_appended("protocol/"),
        )
        .await
        .expect("listing must terminate on a repeated next link")
        .expect_err("repeated next link must refuse authoritative coverage");

        assert!(result.to_string().contains("repeated"), "{result}");
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

    #[test]
    fn delta_pages_preserve_duplicate_names_by_exact_item_id_and_apply_deletes() {
        let mut objects = HashMap::new();
        let page = serde_json::json!({"value":[
            {"id":"item-a", "name":encode_key("protocol/copy"), "parentReference":{"id":"folder456"}, "file":{}},
            {"id":"item-b", "name":encode_key("protocol/copy"), "parentReference":{"id":"folder456"}, "file":{}}
        ]});
        apply_onedrive_delta_page(&page, "folder456", "protocol/", &mut objects)
            .expect("apply baseline delta");
        assert_eq!(objects.len(), 2);

        let changes = serde_json::json!({"value":[
            {"id":"item-a", "deleted":{}},
            {"id":"item-b", "name":encode_key("elsewhere/copy"), "parentReference":{"id":"folder456"}, "file":{}}
        ]});
        apply_onedrive_delta_page(&changes, "folder456", "protocol/", &mut objects)
            .expect("apply terminal delta");
        assert!(objects.is_empty());
    }

    #[test]
    fn delta_page_refuses_file_without_exact_item_id() {
        let page = serde_json::json!({"value":[
            {"name":encode_key("protocol/copy"), "parentReference":{"id":"folder456"}, "file":{}}
        ]});
        let error = apply_onedrive_delta_page(&page, "folder456", "protocol/", &mut HashMap::new())
            .expect_err("missing id must refuse authoritative coverage");
        assert!(error.to_string().contains("omitted id"), "{error}");
    }

    #[test]
    fn appended_response_requires_and_preserves_item_id() {
        let object = parse_onedrive_appended_object("protocol/copy", br#"{"id":"item-1"}"#)
            .expect("parse append response");
        assert_eq!(object.logical_key(), "protocol/copy");
        assert_eq!(object.opaque_provider_id(), "item-1");
        assert!(parse_onedrive_appended_object("protocol/copy", b"{}").is_err());
    }
}
