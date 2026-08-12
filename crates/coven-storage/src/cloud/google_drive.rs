//! Google Drive `CloudHome` implementation.
//!
//! Uses the Google Drive REST API v3 with OAuth 2.0 tokens. Files are stored flat
//! in a single folder — path separators are escaped by
//! the `key_encoding` helpers). The `read`/`read_range`/`list`/`delete` methods are
//! the shared `OAuthRestHome` implementations; this file supplies only the Drive
//! request shapes, the page parser, the upload paths, and sharing.

use async_trait::async_trait;
use bytes::Bytes;

use super::exact_upload::settle_exact_create;
use super::http::{self, ensure_ok, ok_bytes, ok_json, NotFound};
use super::key_encoding::{decode_listed_key, encode_key};
use super::oauth_rest::{
    response_to_file, rest_delete, rest_list, rest_read, rest_read_range, validated_range_bytes,
    ListPage, OAuthRestHome, PageTokenTracker,
};
use super::oauth_session::OAuthSession;
use super::resumable::RangePutSink;
use super::{
    sharing, BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHome,
    CloudHomeError, CloudHomeJoinInfo, ExactSlotStorage, RevokeOutcome, UploadProgress,
};
use crate::oauth::OAuthConfig;
use coven_foundation::id_provider::{IdRef, UuidProvider};
use coven_protocol::objects::{ObjectSlot, PhysicalObjectLocator};

const DRIVE_API: &str = "https://www.googleapis.com/drive/v3";
const UPLOAD_API: &str = "https://www.googleapis.com/upload/drive/v3";
const CREATE_TOKEN_PROPERTY: &str = "covenCreateToken";
const LOGICAL_KEY_PROPERTY: &str = "covenLogicalKey";
const DRIVE_FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";

mod content_hash;
mod storage_impl;
use storage_impl::*;

pub(crate) fn supports_all_drives(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request.query(&[("supportsAllDrives", "true")])
}

fn drive_upload_cancellation_succeeded(status: reqwest::StatusCode) -> bool {
    status.is_success() || status == reqwest::StatusCode::NOT_FOUND || status.as_u16() == 499
}

fn escape_drive_query_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

enum DriveNameMatch {
    Equals,
    Contains,
}

impl DriveNameMatch {
    fn operator(&self) -> &'static str {
        match self {
            Self::Equals => "=",
            Self::Contains => "contains",
        }
    }
}

fn drive_file_query(
    folder_id: Option<&str>,
    name_match: DriveNameMatch,
    name_value: &str,
    extra_predicate: Option<&str>,
) -> String {
    let mut predicates = Vec::new();
    if let Some(folder_id) = folder_id {
        let folder_id = escape_drive_query_value(folder_id);
        predicates.push(format!("'{folder_id}' in parents"));
    }

    let name_value = escape_drive_query_value(name_value);
    predicates.push(format!("name {} '{name_value}'", name_match.operator()));

    if let Some(extra_predicate) = extra_predicate {
        predicates.push(extra_predicate.to_string());
    }
    predicates.push("trashed = false".to_string());
    predicates.join(" and ")
}

fn drive_app_property_predicate(key: &str, value: &str) -> String {
    let key = escape_drive_query_value(key);
    let value = escape_drive_query_value(value);
    format!("appProperties has {{ key='{key}' and value='{value}' }}")
}

fn find_file_query(folder_id: &str, encoded_name: &str) -> String {
    drive_file_query(Some(folder_id), DriveNameMatch::Equals, encoded_name, None)
}

fn find_created_file_query(folder_id: &str, encoded_name: &str, create_token: &str) -> String {
    let app_property = drive_app_property_predicate(CREATE_TOKEN_PROPERTY, create_token);
    drive_file_query(
        Some(folder_id),
        DriveNameMatch::Equals,
        encoded_name,
        Some(&app_property),
    )
}

fn list_file_query(folder_id: &str, prefix: &str) -> String {
    let encoded_prefix = encode_key(prefix);
    drive_file_query(
        Some(folder_id),
        DriveNameMatch::Contains,
        &encoded_prefix,
        None,
    )
}

pub(crate) fn folder_search_query(folder_name: &str) -> String {
    drive_file_query(
        None,
        DriveNameMatch::Equals,
        folder_name,
        Some(&format!("mimeType = '{DRIVE_FOLDER_MIME_TYPE}'")),
    )
}

/// Google Drive cloud home backend.
pub struct GoogleDriveCloudHome {
    folder_id: String,
    drive_api: String,
    upload_api: String,
    ids: IdRef,
    session: OAuthSession,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
}

/// One Drive file named by the provider id it was given and the create token
/// this device stamped on it, whether the name came back from a create response
/// or from listing the folder.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DriveFileIdentity {
    id: String,
    create_token: String,
}

struct DriveAppendAttempt {
    file_id: String,
    create_token: String,
}

enum DriveAppendAttemptState {
    Absent,
    Owned,
    Foreign,
}

enum DriveSlotState {
    Absent,
    Exact(DriveExactMetadata),
    Foreign,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DriveExactMetadata {
    size: u64,
    md5_checksum: String,
}

impl GoogleDriveCloudHome {
    pub fn new(
        folder_id: String,
        session: OAuthSession,
        exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    ) -> Self {
        Self {
            folder_id,
            drive_api: DRIVE_API.to_string(),
            upload_api: UPLOAD_API.to_string(),
            ids: std::sync::Arc::new(UuidProvider),
            session,
            exact_upload_verification,
        }
    }

    pub(crate) fn oauth_config(creds: crate::oauth::OAuthClientCreds) -> OAuthConfig {
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

    /// Find a file's Google Drive ID by name within our folder.
    async fn find_file_id(&self, encoded_name: &str) -> Result<Option<String>, CloudHomeError> {
        let files = self.list_file_identities(encoded_name).await?;
        Ok(select_drive_file(&files).map(|file| file.id.clone()))
    }

    async fn list_file_identities(
        &self,
        encoded_name: &str,
    ) -> Result<Vec<DriveFileIdentity>, CloudHomeError> {
        let query = find_file_query(&self.folder_id, encoded_name);
        let mut page_token: Option<String> = None;
        let mut page_tokens = PageTokenTracker::new("Google Drive file identity listing");
        let mut files = Vec::new();

        loop {
            let page = page_token.clone();
            let resp =
                self.session
                    .api_call(|oauth| {
                        let mut req =
                            supports_all_drives(oauth.get(format!("{}/files", self.drive_api)))
                                .query(&[
                                    ("q", query.as_str()),
                                    ("fields", "nextPageToken,files(id,appProperties)"),
                                    ("pageSize", "1000"),
                                    ("includeItemsFromAllDrives", "true"),
                                ]);
                        if let Some(ref page) = page {
                            req = req.query(&[("pageToken", page.as_str())]);
                        }
                        req
                    })
                    .await?;
            let resp = ensure_ok(resp, "list files", NotFound::Status).await?;
            let json: serde_json::Value = ok_json(resp, "parse list response").await?;
            files.extend(parse_drive_file_identities(&json)?);

            match json["nextPageToken"].as_str() {
                Some(next) => page_token = Some(page_tokens.record(next)?),
                None => break,
            }
        }

        Ok(files)
    }

    async fn create_file_metadata(
        &self,
        key: &str,
        encoded: &str,
    ) -> Result<DriveFileIdentity, CloudHomeError> {
        let create_token = self.ids.new_id();
        let metadata = create_file_metadata_body(encoded, &self.folder_id, &create_token);
        let resp = self
            .session
            .api_call(|oauth| {
                supports_all_drives(oauth.post(format!("{}/files", self.drive_api)))
                    .query(&[("fields", "id")])
                    .header("Content-Type", "application/json; charset=UTF-8")
                    .body(metadata.clone())
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
        let id_error = match resp.text().await {
            Ok(body) => match parse_create_file_id(&body, key) {
                Ok(id) => return Ok(DriveFileIdentity { id, create_token }),
                Err(error) => error,
            },
            Err(error) => CloudHomeError::transport(format!("create {key}: read response"), error),
        };
        match self.find_created_file_id(encoded, &create_token).await {
            Ok(Some(file_id)) => match self.delete_created_file(key, &file_id).await {
                Ok(()) => Err(id_error),
                Err(delete_error) => Err(CloudHomeError::Transport(format!(
                    "create {key}: metadata response id failure: {id_error}; rollback delete failed: {delete_error}"
                ))),
            },
            Ok(None) => Err(id_error),
            Err(lookup_error) => Err(CloudHomeError::Transport(format!(
                "create {key}: metadata response id failure: {id_error}; rollback lookup failed: {lookup_error}"
            ))),
        }
    }

    async fn create_file_for_key(
        &self,
        key: &str,
        encoded: &str,
    ) -> Result<String, CloudHomeError> {
        let created = self.create_file_metadata(key, encoded).await?;
        self.reconcile_created_file(key, encoded, created).await
    }

    async fn reconcile_created_file(
        &self,
        key: &str,
        encoded: &str,
        created: DriveFileIdentity,
    ) -> Result<String, CloudHomeError> {
        let files = self.list_file_identities(encoded).await?;
        if !files.contains(&created) {
            return Err(CloudHomeError::Transport(format!(
                "create {key}: created file {} with token {} was not returned by duplicate check",
                created.id, created.create_token
            )));
        }
        let Some(winner) = select_drive_file(&files) else {
            return Err(CloudHomeError::Transport(format!(
                "create {key}: created file {} was not returned by duplicate check",
                created.id
            )));
        };
        let winner_id = winner.id.clone();

        for file in files {
            if file.id != winner_id {
                self.delete_created_file(key, &file.id).await?;
            }
        }

        Ok(winner_id)
    }

    async fn find_created_file_id(
        &self,
        encoded_name: &str,
        create_token: &str,
    ) -> Result<Option<String>, CloudHomeError> {
        let query = find_created_file_query(&self.folder_id, encoded_name, create_token);
        let resp = self
            .session
            .api_call(|oauth| {
                supports_all_drives(oauth.get(format!("{}/files", self.drive_api))).query(&[
                    ("q", query.as_str()),
                    ("fields", "files(id)"),
                    ("pageSize", "1"),
                    ("includeItemsFromAllDrives", "true"),
                ])
            })
            .await?;
        let resp = ensure_ok(resp, "list created files", NotFound::Status).await?;
        let json: serde_json::Value = ok_json(resp, "parse created file list response").await?;
        Ok(json["files"]
            .as_array()
            .and_then(|files| files.first())
            .and_then(|first| first["id"].as_str())
            .map(String::from))
    }

    async fn upload_file_media(
        &self,
        key: &str,
        file_id: &str,
        body: Bytes,
        op: &str,
    ) -> Result<(), CloudHomeError> {
        let resp = self
            .session
            .api_call(|oauth| {
                supports_all_drives(oauth.patch(format!(
                    "{}/files/{}?uploadType=media",
                    self.upload_api, file_id
                )))
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
                op,
            ));
        }
        Ok(())
    }

    async fn create_file_with_media(
        &self,
        key: &str,
        encoded: &str,
        media_body: Bytes,
    ) -> Result<(), CloudHomeError> {
        let file_id = self.create_file_for_key(key, encoded).await?;
        let upload_error = match self
            .upload_file_media(key, &file_id, media_body, "create")
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        match self.delete_created_file(key, &file_id).await {
            Ok(()) => Err(upload_error),
            Err(delete_error) => Err(CloudHomeError::Transport(format!(
                "create {key}: media upload failed after metadata create: {upload_error}; rollback delete failed: {delete_error}"
            ))),
        }
    }

    async fn delete_created_file(&self, key: &str, file_id: &str) -> Result<(), CloudHomeError> {
        let resp = self
            .session
            .api_call(|oauth| {
                supports_all_drives(oauth.delete(format!("{}/files/{}", self.drive_api, file_id)))
            })
            .await?;
        let status = resp.status();
        if status.is_success() || status == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        Err(CloudHomeError::Transport(format!(
            "delete created file {key} (HTTP {status}): {}",
            http::body_text(resp).await
        )))
    }

    async fn generate_file_id(&self, key: &str) -> Result<String, CloudHomeError> {
        let response = self
            .session
            .api_call(|oauth| {
                oauth
                    .get(format!("{}/files/generateIds", self.drive_api))
                    .query(&[("count", "1"), ("space", "drive"), ("type", "files")])
            })
            .await?;
        let response =
            ensure_ok(response, "generate Drive append file id", NotFound::Status).await?;
        let json: serde_json::Value = ok_json(response, "parse generated Drive file id").await?;
        parse_generated_file_id(&json, key)
    }

    async fn new_append_attempt(&self, key: &str) -> Result<DriveAppendAttempt, CloudHomeError> {
        let file_id = self.generate_file_id(key).await?;
        let create_token = self.ids.new_id();
        if create_token == file_id {
            return Err(CloudHomeError::Transport(format!(
                "generate append id {key}: create token equals the provider file id"
            )));
        }
        Ok(DriveAppendAttempt {
            file_id,
            create_token,
        })
    }

    async fn inspect_append_attempt(
        &self,
        key: &str,
        attempt: &DriveAppendAttempt,
    ) -> Result<DriveAppendAttemptState, CloudHomeError> {
        let response = self
            .session
            .api_call(|oauth| {
                supports_all_drives(
                    oauth.get(format!("{}/files/{}", self.drive_api, attempt.file_id)),
                )
                .query(&[("fields", "id,appProperties,trashed")])
            })
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(DriveAppendAttemptState::Absent);
        }
        let response = ensure_ok(response, "inspect failed Drive append", NotFound::Status).await?;
        let json: serde_json::Value = ok_json(response, "parse failed Drive append").await?;
        if json["id"].as_str() != Some(attempt.file_id.as_str()) {
            return Err(CloudHomeError::Transport(format!(
                "inspect append {key}: exact file response did not identify {}",
                attempt.file_id
            )));
        }
        Ok(
            if json["appProperties"][CREATE_TOKEN_PROPERTY].as_str()
                == Some(attempt.create_token.as_str())
            {
                DriveAppendAttemptState::Owned
            } else {
                DriveAppendAttemptState::Foreign
            },
        )
    }

    async fn resolve_failed_append(
        &self,
        key: &str,
        attempt: &DriveAppendAttempt,
        operation: CloudHomeError,
        may_have_committed: bool,
    ) -> Result<String, CloudHomeError> {
        match self.inspect_append_attempt(key, attempt).await {
            Ok(DriveAppendAttemptState::Absent) => Err(operation),
            Ok(DriveAppendAttemptState::Foreign) => {
                Err(CloudHomeError::AlreadyExists(key.to_string()))
            }
            Ok(DriveAppendAttemptState::Owned) if may_have_committed => Ok(attempt.file_id.clone()),
            Ok(DriveAppendAttemptState::Owned) => {
                match self.delete_created_file(key, &attempt.file_id).await {
                    Ok(()) => Err(operation),
                    Err(cleanup) => Err(CloudHomeError::CleanupFailed {
                        operation: Box::new(operation),
                        cleanup: Box::new(cleanup),
                    }),
                }
            }
            Err(verification) => Err(CloudHomeError::CleanupFailed {
                operation: Box::new(operation),
                cleanup: Box::new(verification),
            }),
        }
    }

    fn validate_slot<'a>(&self, slot: &'a ObjectSlot) -> Result<&'a str, CloudHomeError> {
        slot.validate()?;
        match slot.physical() {
            PhysicalObjectLocator::Opaque(file_id) => Ok(file_id),
            PhysicalObjectLocator::LogicalKey => Err(CloudHomeError::Configuration(format!(
                "Google Drive slot for {} requires an opaque file id",
                slot.logical_key()
            ))),
        }
    }

    async fn inspect_slot(&self, slot: &ObjectSlot) -> Result<DriveSlotState, CloudHomeError> {
        let file_id = self.validate_slot(slot)?;
        let response =
            self.session
                .api_call(|oauth| {
                    supports_all_drives(oauth.get(format!("{}/files/{file_id}", self.drive_api)))
                        .query(&[(
                            "fields",
                            "id,name,parents,trashed,appProperties,size,md5Checksum",
                        )])
                })
                .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(DriveSlotState::Absent);
        }
        let response = ensure_ok(
            response,
            &format!("inspect exact {}", slot.logical_key()),
            NotFound::Status,
        )
        .await?;
        let metadata: serde_json::Value =
            ok_json(response, "parse exact Drive file metadata").await?;
        let expected_name = encode_key(slot.logical_key());
        let id_matches = metadata["id"].as_str() == Some(file_id);
        let name_matches = metadata["name"].as_str() == Some(expected_name.as_str());
        let parent_matches = metadata["parents"].as_array().is_some_and(|parents| {
            parents
                .iter()
                .any(|parent| parent.as_str() == Some(&self.folder_id))
        });
        let logical_key_matches =
            metadata["appProperties"][LOGICAL_KEY_PROPERTY].as_str() == Some(slot.logical_key());
        let is_live = metadata["trashed"].as_bool() == Some(false);
        if id_matches && name_matches && parent_matches && logical_key_matches && is_live {
            let size = metadata["size"]
                .as_str()
                .and_then(|size| size.parse::<u64>().ok())
                .ok_or_else(|| {
                    CloudHomeError::Transport(format!(
                        "exact Drive metadata for {} omitted size",
                        slot.logical_key()
                    ))
                })?;
            let md5_checksum = metadata["md5Checksum"]
                .as_str()
                .filter(|hash| !hash.is_empty())
                .ok_or_else(|| {
                    CloudHomeError::Transport(format!(
                        "exact Drive metadata for {} omitted md5Checksum",
                        slot.logical_key()
                    ))
                })?
                .to_string();
            Ok(DriveSlotState::Exact(DriveExactMetadata {
                size,
                md5_checksum,
            }))
        } else {
            Ok(DriveSlotState::Foreign)
        }
    }

    async fn verify_slot(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        match self.inspect_slot(slot).await? {
            DriveSlotState::Exact(_) => Ok(()),
            DriveSlotState::Absent => Err(CloudHomeError::NotFound(slot.logical_key().to_string())),
            DriveSlotState::Foreign => Err(CloudHomeError::Transport(format!(
                "exact Drive slot for {} does not identify its allocated file in folder {}",
                slot.logical_key(),
                self.folder_id
            ))),
        }
    }

    async fn verify_exact_upload(
        &self,
        upload: &super::ExactUpload<'_>,
        created_response_was_observed: bool,
    ) -> Result<(), CloudHomeError> {
        use coven_foundation::config::ExactUploadVerification;

        match self.exact_upload_verification {
            ExactUploadVerification::UploadChecksum => Err(CloudHomeError::Configuration(
                "Google Drive does not accept a caller-supplied upload checksum".to_string(),
            )),
            ExactUploadVerification::MetadataHash => {
                let metadata = match self.inspect_slot(upload.object().slot()).await? {
                    DriveSlotState::Absent => {
                        return Err(CloudHomeError::NotFound(
                            upload.object().slot().logical_key().to_string(),
                        ));
                    }
                    DriveSlotState::Foreign => {
                        return Err(CloudHomeError::SlotCollision(
                            upload.object().slot().logical_key().to_string(),
                        ));
                    }
                    DriveSlotState::Exact(metadata) => metadata,
                };
                let expected_md5 = content_hash::md5(upload).await?;
                if metadata.size != upload.object().stored_size()
                    || metadata.md5_checksum != expected_md5
                {
                    return Err(CloudHomeError::SlotCollision(
                        upload.object().slot().logical_key().to_string(),
                    ));
                }
                Ok(())
            }
            ExactUploadVerification::Readback => {
                let bytes = self.read_at_slot(upload.object().slot()).await?;
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

    async fn create_small_at(
        &self,
        slot: &ObjectSlot,
        data: Vec<u8>,
    ) -> Result<(), CloudHomeError> {
        use sha2::{Digest, Sha256};

        let file_id = self.validate_slot(slot)?;
        let boundary = format!(
            "coven-exact-{}",
            hex::encode(Sha256::digest(slot.logical_key().as_bytes()))
        );
        let metadata = serde_json::json!({
            "id": file_id,
            "name": encode_key(slot.logical_key()),
            "parents": [self.folder_id],
            "appProperties": { (LOGICAL_KEY_PROPERTY): slot.logical_key() },
        })
        .to_string();
        let mut body = Vec::with_capacity(metadata.len() + data.len() + boundary.len() * 3 + 128);
        body.extend_from_slice(format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{metadata}\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes());
        body.extend_from_slice(&data);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let response = match self
            .session
            .api_call(|oauth| {
                supports_all_drives(oauth.post(format!(
                    "{}/files?uploadType=multipart&fields=id",
                    self.upload_api
                )))
                .header(
                    "Content-Type",
                    format!("multipart/related; boundary={boundary}"),
                )
                .body(body.clone())
            })
            .await
        {
            Ok(response) => response,
            Err(operation) => return Err(operation),
        };
        let status = response.status();
        if !status.is_success() {
            if status == reqwest::StatusCode::CONFLICT {
                return Err(CloudHomeError::AlreadyExists(
                    slot.logical_key().to_string(),
                ));
            }
            let operation = classify_write_error(
                status,
                &http::body_text(response).await,
                slot.logical_key(),
                "create exact",
            );
            return Err(operation);
        }
        Ok(())
    }

    /// Open a resumable upload session for an existing Drive file and return its
    /// session URL (the `Location` header Google returns).
    async fn open_resumable_update_session(
        &self,
        key: &str,
        file_id: &str,
    ) -> Result<String, CloudHomeError> {
        let url = format!("{}/files/{}?uploadType=resumable", self.upload_api, file_id);
        let resp = self
            .session
            .api_call(|oauth| {
                supports_all_drives(oauth.patch(&url))
                    .header("Content-Type", "application/json; charset=UTF-8")
                    .body("{}")
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
        resp.headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
            .ok_or_else(|| {
                CloudHomeError::Transport(format!(
                    "resumable session {key}: no Location header returned"
                ))
            })
    }

    async fn open_resumable_create_session(
        &self,
        key: &str,
        attempt: &DriveAppendAttempt,
    ) -> Result<String, CloudHomeError> {
        let metadata = serde_json::json!({
            "id": attempt.file_id,
            "name": encode_key(key),
            "parents": [self.folder_id],
            "appProperties": {
                (CREATE_TOKEN_PROPERTY): attempt.create_token,
                (LOGICAL_KEY_PROPERTY): key,
            },
        });
        let response = self
            .session
            .api_call(|oauth| {
                supports_all_drives(oauth.post(format!(
                    "{}/files?uploadType=resumable&fields=id",
                    self.upload_api
                )))
                .json(&metadata)
            })
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(classify_write_error(
                status,
                &http::body_text(response).await,
                key,
                "append resumable create",
            ));
        }
        let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
            return Err(CloudHomeError::Transport(format!(
                "append resumable create {key}: no Location header returned"
            )));
        };
        let location = location.to_str().map_err(|error| {
            CloudHomeError::transport(
                format!("read append resumable create Location header for {key}"),
                error,
            )
        })?;
        if location.is_empty() {
            return Err(CloudHomeError::Transport(format!(
                "append resumable create {key}: empty Location header returned"
            )));
        }
        Ok(location.to_string())
    }

    async fn create_at_slot(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        let file_id = self.validate_slot(slot)?.to_string();
        if body.len() <= self.multipart_threshold() {
            let bytes = body.collect().await?;
            let length = bytes.len() as u64;
            self.create_small_at(slot, bytes).await?;
            progress(length);
            return Ok(());
        }

        let attempt = DriveAppendAttempt {
            file_id,
            create_token: format!("exact:{}", slot.logical_key()),
        };
        let session_url = self
            .open_resumable_create_session(slot.logical_key(), &attempt)
            .await?;
        let key = slot.logical_key().to_string();
        let classify = Box::new(move |status, response: &str| {
            classify_write_error(status, response, &key, "create exact")
        });
        let sink = self.session.range_put_sink(
            session_url,
            308,
            body.len(),
            GDRIVE_CHUNK_SIZE,
            slot.logical_key().to_string(),
            classify,
            drive_upload_cancellation_succeeded,
        );
        super::blob_body::MultipartUpload::new(slot.logical_key(), body, Box::new(sink), progress)
            .run()
            .await
    }

    /// Verify the slot, issue the exact-read GET (`alt=media`, optionally with a
    /// `Range` header), and check its status — the shared preamble of the three
    /// exact-read paths, which diverge only in what they do with the response
    /// body. The Dropbox backend factors its equivalent the same way.
    async fn send_exact_read(
        &self,
        slot: &ObjectSlot,
        range: Option<&str>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        self.verify_slot(slot).await?;
        let file_id = self.validate_slot(slot)?.to_string();
        let response = self
            .session
            .api_call(|oauth| {
                let request =
                    supports_all_drives(oauth.get(format!("{}/files/{file_id}", self.drive_api)))
                        .query(&[("alt", "media")]);
                match range {
                    Some(range) => request.header("Range", range),
                    None => request,
                }
            })
            .await?;
        ensure_ok(
            response,
            &format!("read exact {}", slot.logical_key()),
            NotFound::Status,
        )
        .await
    }

    async fn read_at_slot(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        let response = self.send_exact_read(slot, None).await?;
        ok_bytes(
            response,
            &format!("read exact body for {}", slot.logical_key()),
        )
        .await
    }

    async fn read_at_slot_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        let response = self.send_exact_read(slot, None).await?;
        response_to_file(
            response,
            destination,
            &format!("read exact body for {}", slot.logical_key()),
        )
        .await
    }

    async fn delete_at_slot(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        let file_id = self.validate_slot(slot)?.to_string();
        match self.verify_slot(slot).await {
            Ok(()) => self.delete_created_file(slot.logical_key(), &file_id).await,
            Err(CloudHomeError::NotFound(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    fn with_endpoints(mut self, drive_api: String, upload_api: String) -> Self {
        self.drive_api = drive_api;
        self.upload_api = upload_api;
        self
    }

    #[cfg(test)]
    fn with_ids(mut self, ids: IdRef) -> Self {
        self.ids = ids;
        self
    }
}

#[cfg(test)]
mod tests;
