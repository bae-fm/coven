//! Google Drive `CloudHome` implementation.
//!
//! Uses the Google Drive REST API v3 with OAuth 2.0 tokens. Files are stored flat
//! in a single folder — path separators are escaped by
//! [`super::key_encoding`]). The `read`/`read_range`/`list`/`delete` methods are
//! the shared [`OAuthRestHome`] implementations; this file supplies only the Drive
//! request shapes, the page parser, the upload paths, and sharing.

use async_trait::async_trait;
use bytes::Bytes;

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
    CloudHomeError, CloudHomeJoinInfo, ExactSlotStorage, ObjectSlot, PhysicalObjectLocator,
    RevokeOutcome, UploadProgress,
};
use crate::id_provider::{IdRef, UuidProvider};
use crate::oauth::OAuthConfig;

const DRIVE_API: &str = "https://www.googleapis.com/drive/v3";
const UPLOAD_API: &str = "https://www.googleapis.com/upload/drive/v3";
const CREATE_TOKEN_PROPERTY: &str = "covenCreateToken";
const LOGICAL_KEY_PROPERTY: &str = "covenLogicalKey";
const DRIVE_FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";

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
pub(crate) struct GoogleDriveCloudHome {
    folder_id: String,
    drive_api: String,
    upload_api: String,
    ids: IdRef,
    session: OAuthSession,
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
    Exact,
    Foreign,
}

impl GoogleDriveCloudHome {
    pub(crate) fn new(folder_id: String, session: OAuthSession) -> Self {
        Self {
            folder_id,
            drive_api: DRIVE_API.to_string(),
            upload_api: UPLOAD_API.to_string(),
            ids: std::sync::Arc::new(UuidProvider),
            session,
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
        let create_token = uuid::Uuid::new_v4().to_string();
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
            Err(error) => {
                CloudHomeError::Transport(format!("create {key}: read response: {error}"))
            }
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
        let response = self
            .session
            .api_call(|oauth| {
                supports_all_drives(oauth.get(format!("{}/files/{file_id}", self.drive_api)))
                    .query(&[("fields", "id,name,parents,trashed,appProperties")])
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
            Ok(DriveSlotState::Exact)
        } else {
            Ok(DriveSlotState::Foreign)
        }
    }

    async fn verify_slot(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        match self.inspect_slot(slot).await? {
            DriveSlotState::Exact => Ok(()),
            DriveSlotState::Absent => Err(CloudHomeError::NotFound(slot.logical_key().to_string())),
            DriveSlotState::Foreign => Err(CloudHomeError::Transport(format!(
                "exact Drive slot for {} does not identify its allocated file in folder {}",
                slot.logical_key(),
                self.folder_id
            ))),
        }
    }

    async fn resolve_failed_exact_create(
        &self,
        slot: &ObjectSlot,
        operation: CloudHomeError,
    ) -> Result<(), CloudHomeError> {
        match self.inspect_slot(slot).await {
            Ok(DriveSlotState::Exact) => Ok(()),
            Ok(DriveSlotState::Absent) => Err(operation),
            Ok(DriveSlotState::Foreign) => Err(CloudHomeError::AlreadyExists(
                slot.logical_key().to_string(),
            )),
            Err(verification) => Err(CloudHomeError::CleanupFailed {
                operation: Box::new(operation),
                cleanup: Box::new(verification),
            }),
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
            Err(operation) => return self.resolve_failed_exact_create(slot, operation).await,
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
            if status.is_server_error() {
                return self.resolve_failed_exact_create(slot, operation).await;
            }
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
            CloudHomeError::Transport(format!(
                "append resumable create {key}: invalid Location header: {error}"
            ))
        })?;
        if location.is_empty() {
            return Err(CloudHomeError::Transport(format!(
                "append resumable create {key}: empty Location header returned"
            )));
        }
        Ok(location.to_string())
    }
}

fn parse_drive_file_identities(
    page: &serde_json::Value,
) -> Result<Vec<DriveFileIdentity>, CloudHomeError> {
    let files = page["files"].as_array().ok_or_else(|| {
        CloudHomeError::Transport("Drive file identity response omitted files".to_string())
    })?;
    files
        .iter()
        .map(|file| {
            let id = file["id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    CloudHomeError::Transport(
                        "Drive file identity response omitted a file id".to_string(),
                    )
                })?
                .to_string();
            let create_token = file["appProperties"][CREATE_TOKEN_PROPERTY]
                .as_str()
                .filter(|token| !token.is_empty())
                .ok_or_else(|| {
                    CloudHomeError::Transport(format!(
                        "Drive file {id} omitted its Coven create token"
                    ))
                })?
                .to_string();
            Ok(DriveFileIdentity { id, create_token })
        })
        .collect()
}

fn select_drive_file(files: &[DriveFileIdentity]) -> Option<&DriveFileIdentity> {
    files.iter().min_by(|left, right| {
        left.create_token
            .cmp(&right.create_token)
            .then_with(|| left.id.cmp(&right.id))
    })
}

fn parse_create_file_id(body: &str, key: &str) -> Result<String, CloudHomeError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| CloudHomeError::Transport(format!("create {key}: parse response: {e}")))?;
    match json.get("id").and_then(|id| id.as_str()) {
        Some(id) if !id.is_empty() => Ok(id.to_string()),
        _ => Err(CloudHomeError::Transport(format!(
            "create {key}: response missing id"
        ))),
    }
}

fn parse_generated_file_id(
    response: &serde_json::Value,
    key: &str,
) -> Result<String, CloudHomeError> {
    match response["ids"].as_array() {
        Some(ids) if ids.len() == 1 => match ids[0].as_str() {
            Some(id) if !id.is_empty() => Ok(id.to_string()),
            _ => Err(CloudHomeError::Transport(format!(
                "generate append id {key}: generated id is empty"
            ))),
        },
        _ => Err(CloudHomeError::Transport(format!(
            "generate append id {key}: expected exactly one generated id"
        ))),
    }
}

fn create_file_metadata_body(encoded_name: &str, folder_id: &str, create_token: &str) -> String {
    let mut app_properties = serde_json::Map::new();
    app_properties.insert(
        CREATE_TOKEN_PROPERTY.to_string(),
        serde_json::Value::String(create_token.to_string()),
    );
    serde_json::json!({
        "name": encoded_name,
        "parents": [folder_id],
        "appProperties": app_properties,
    })
    .to_string()
}

/// A Drive resumable sink. New objects use a resumable-create session, which
/// keeps the file absent until the final part commits; `finish` then resolves
/// concurrent same-name creates by the create token. Existing objects use a
/// resumable-update session and require no post-commit reconciliation.
struct DriveMultipartSink<'a> {
    home: &'a GoogleDriveCloudHome,
    inner: RangePutSink,
    key: String,
    encoded: String,
    created: Option<DriveFileIdentity>,
}

#[async_trait]
impl super::PartSink for DriveMultipartSink<'_> {
    fn part_size(&self) -> usize {
        self.inner.part_size()
    }

    async fn send_part(
        &mut self,
        part: Bytes,
        offset: u64,
        is_last: bool,
    ) -> Result<(), CloudHomeError> {
        self.inner.send_part(part, offset, is_last).await
    }

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        self.inner.abort().await
    }

    async fn finish(mut self: Box<Self>) -> Result<(), CloudHomeError> {
        Box::new(self.inner).finish().await?;
        if let Some(created) = self.created.take() {
            self.home
                .reconcile_created_file(&self.key, &self.encoded, created)
                .await?;
        }
        Ok(())
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
        return CloudHomeError::Transport(
            "Your Google Drive storage is full. Free up space at drive.google.com to keep syncing."
                .to_string(),
        );
    }
    CloudHomeError::Transport(format!("{op} {key} (HTTP {status}): {body}"))
}

/// Files at or below this size go up through simple Drive requests; larger files
/// use a resumable session. Drive accepts a simple upload up to 5 MB.
const GDRIVE_SIMPLE_UPLOAD_MAX: usize = 4 * 1024 * 1024;

/// Resumable-session part size. Drive requires every part except the last to be a
/// multiple of 256 KiB; 8 MiB (32 × 256 KiB) keeps the request count low.
const GDRIVE_CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl OAuthRestHome for GoogleDriveCloudHome {
    fn not_found(&self) -> NotFound {
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
            .api_call(|oauth| {
                let mut req =
                    supports_all_drives(oauth.get(format!("{}/files/{}", self.drive_api, file_id)))
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
            .api_call(|oauth| {
                supports_all_drives(oauth.delete(format!("{}/files/{}", self.drive_api, file_id)))
            })
            .await
    }

    async fn send_list_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
    ) -> Result<reqwest::Response, CloudHomeError> {
        let query = list_file_query(&self.folder_id, prefix);
        let page = cursor.map(str::to_string);
        self.session
            .api_call(|oauth| {
                let mut req = supports_all_drives(oauth.get(format!("{}/files", self.drive_api)))
                    .query(&[
                        ("q", query.as_str()),
                        ("fields", "nextPageToken,files(name)"),
                        ("pageSize", "1000"),
                        ("includeItemsFromAllDrives", "true"),
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
            .map_err(|e| CloudHomeError::Transport(format!("parse list: {e}")))?;
        let mut keys = Vec::new();
        if let Some(files) = json["files"].as_array() {
            for file in files {
                if let Some(name) = file["name"].as_str() {
                    let Some(decoded) = decode_listed_key("Google Drive", name) else {
                        continue;
                    };
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

impl GoogleDriveCloudHome {
    async fn create_at_slot(
        &self,
        slot: &ObjectSlot,
        mut body: BlobBody,
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
        let session_url = match self
            .open_resumable_create_session(slot.logical_key(), &attempt)
            .await
        {
            Ok(session_url) => session_url,
            Err(operation) => return Err(operation),
        };
        let key = slot.logical_key().to_string();
        let classify = Box::new(move |status, response: &str| {
            classify_write_error(status, response, &key, "create exact")
        });
        let mut uploader = self.session.range_put_uploader(
            session_url,
            308,
            body.len(),
            GDRIVE_CHUNK_SIZE,
            slot.logical_key().to_string(),
            classify,
            drive_upload_cancellation_succeeded,
        );
        let total = body.len();
        let mut offset = 0u64;
        loop {
            let part = match body.next_part(uploader.part_size()).await {
                Ok(Some(part)) => part,
                Ok(None) => {
                    let operation = CloudHomeError::Transport(format!(
                        "create {:?}: upload body ended before the final part",
                        slot.logical_key()
                    ));
                    return Err(match uploader.abort().await {
                        Ok(()) => operation,
                        Err(cleanup) => CloudHomeError::CleanupFailed {
                            operation: Box::new(operation),
                            cleanup: Box::new(cleanup),
                        },
                    });
                }
                Err(operation) => {
                    return Err(match uploader.abort().await {
                        Ok(()) => operation,
                        Err(cleanup) => CloudHomeError::CleanupFailed {
                            operation: Box::new(operation),
                            cleanup: Box::new(cleanup),
                        },
                    });
                }
            };
            let length = part.len() as u64;
            let is_last = offset + length >= total;
            let completion = match uploader.send_part(part, offset, is_last).await {
                Ok(completion) => completion,
                Err(operation) => return Err(operation),
            };
            offset += length;
            progress(offset);
            if completion.is_some() {
                return Ok(());
            }
        }
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
}

#[async_trait]
impl CloudHome for GoogleDriveCloudHome {
    fn exact_slot_storage(
        self: std::sync::Arc<Self>,
    ) -> Option<std::sync::Arc<dyn ExactSlotStorage>> {
        Some(self)
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let media_body = Bytes::from(data);
        let encoded = encode_key(key);
        if let Some(file_id) = self.find_file_id(&encoded).await? {
            self.upload_file_media(key, &file_id, media_body, "update")
                .await?;
        } else {
            self.create_file_with_media(key, &encoded, media_body)
                .await?;
        }
        Ok(())
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        let encoded = encode_key(key);
        let (session_url, created, op) = match self.find_file_id(&encoded).await? {
            Some(file_id) => (
                self.open_resumable_update_session(key, &file_id).await?,
                None,
                "update",
            ),
            None => {
                let attempt = self.new_append_attempt(key).await?;
                let session_url = match self.open_resumable_create_session(key, &attempt).await {
                    Ok(session_url) => session_url,
                    Err(operation) => {
                        return match self
                            .resolve_failed_append(key, &attempt, operation, false)
                            .await
                        {
                            Err(error) => Err(error),
                            Ok(file_id) => Err(CloudHomeError::Transport(format!(
                            "open mutable Drive upload {key}: uncommitted session resolved as {}",
                            file_id
                        ))),
                        }
                    }
                };
                (
                    session_url,
                    Some(DriveFileIdentity {
                        id: attempt.file_id,
                        create_token: attempt.create_token,
                    }),
                    "create",
                )
            }
        };
        let key_owned = key.to_string();
        let classify =
            Box::new(move |status, body: &str| classify_write_error(status, body, &key_owned, op));
        // Drive returns 308 Resume Incomplete for every non-final part.
        let inner = self.session.range_put_sink(
            session_url,
            308,
            total_len,
            GDRIVE_CHUNK_SIZE,
            key.to_string(),
            classify,
            drive_upload_cancellation_succeeded,
        );
        Ok(Box::new(DriveMultipartSink {
            home: self,
            inner,
            key: key.to_string(),
            encoded,
            created,
        }))
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

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        let email = desired.require_provider_email("Google Drive")?;
        let list_url = format!(
            "{}/files/{}/permissions?fields=permissions(id,emailAddress,role),nextPageToken&supportsAllDrives=true",
            self.drive_api, self.folder_id
        );
        let email_of =
            |permission: &serde_json::Value| permission["emailAddress"].as_str().map(String::from);
        let next_page = |page: &serde_json::Value| drive_permissions_next_page_url(&list_url, page);
        let delete_url = |permission_id: &str| {
            format!(
                "{}/files/{}/permissions/{}?supportsAllDrives=true",
                self.drive_api, self.folder_id, permission_id
            )
        };
        let is_writer =
            |permission: &serde_json::Value| permission["role"].as_str() == Some("writer");
        match desired {
            CloudAccessState::Present { .. } => {
                let current = sharing::permission_by_email(
                    &self.session,
                    email,
                    &list_url,
                    "permissions",
                    &email_of,
                    &next_page,
                )
                .await?;
                if current.as_ref().is_some_and(is_writer) {
                    return Ok(CloudAccessOutcome::Present(
                        CloudHomeJoinInfo::GoogleDrive {
                            folder_id: self.folder_id.clone(),
                        },
                    ));
                }
                if current.is_some() {
                    sharing::ensure_absent_by_email(
                        &self.session,
                        email,
                        &list_url,
                        "permissions",
                        &email_of,
                        &delete_url,
                        &next_page,
                    )
                    .await?;
                }
                let permission = serde_json::json!({
                    "type": "user",
                    "role": "writer",
                    "emailAddress": email,
                });
                let resp = self
                    .session
                    .api_call(|oauth| {
                        supports_all_drives(oauth.post(format!(
                            "{}/files/{}/permissions",
                            self.drive_api, self.folder_id
                        )))
                        .json(&permission)
                    })
                    .await?;
                ensure_ok(resp, &format!("grant access to {email}"), NotFound::Status).await?;
                let verified = sharing::permission_by_email(
                    &self.session,
                    email,
                    &list_url,
                    "permissions",
                    &email_of,
                    &next_page,
                )
                .await?;
                if !verified.as_ref().is_some_and(is_writer) {
                    return Err(CloudHomeError::Transport(format!(
                        "writer permission for {email} is not visible after creation"
                    )));
                }
                Ok(CloudAccessOutcome::Present(
                    CloudHomeJoinInfo::GoogleDrive {
                        folder_id: self.folder_id.clone(),
                    },
                ))
            }
            CloudAccessState::Absent { .. } => {
                sharing::ensure_absent_by_email(
                    &self.session,
                    email,
                    &list_url,
                    "permissions",
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
impl ExactSlotStorage for GoogleDriveCloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<crate::storage::ResolvedProviderBinding, CloudHomeError> {
        use crate::storage::{
            GoogleDriveCorpus, ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding,
            StoreProviderBinding,
        };

        if self.folder_id.is_empty() {
            return Err(CloudHomeError::Configuration(
                "Google Drive provider binding has an empty folder id".to_string(),
            ));
        }
        let folder_response = self
            .session
            .api_call(|oauth| {
                supports_all_drives(
                    oauth.get(format!("{}/files/{}", self.drive_api, self.folder_id)),
                )
                .query(&[("fields", "id,driveId")])
            })
            .await?;
        let folder_response = ensure_ok(
            folder_response,
            "resolve Google Drive corpus",
            NotFound::Status,
        )
        .await?;
        let folder: serde_json::Value =
            ok_json(folder_response, "parse Google Drive corpus").await?;
        if folder["id"].as_str() != Some(self.folder_id.as_str()) {
            return Err(CloudHomeError::Transport(
                "Google Drive folder lookup returned a different folder id".to_string(),
            ));
        }
        let corpus = match folder.get("driveId") {
            None | Some(serde_json::Value::Null) => GoogleDriveCorpus::MyDrive {
                folder_id: self.folder_id.clone(),
            },
            Some(value) => GoogleDriveCorpus::SharedDrive {
                drive_id: value
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        CloudHomeError::Transport(
                            "Google Drive folder returned a malformed drive id".to_string(),
                        )
                    })?
                    .to_string(),
                folder_id: self.folder_id.clone(),
            },
        };

        let about_response = self
            .session
            .api_call(|oauth| {
                oauth
                    .get(format!("{}/about", self.drive_api))
                    .query(&[("fields", "user(permissionId)")])
            })
            .await?;
        let about_response = ensure_ok(
            about_response,
            "resolve Google Drive principal",
            NotFound::Status,
        )
        .await?;
        let about: serde_json::Value =
            ok_json(about_response, "parse Google Drive principal").await?;
        let permission_id = about["user"]["permissionId"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport(
                    "Google Drive about response omitted the stable permission id".to_string(),
                )
            })?
            .to_string();

        Ok(ResolvedProviderBinding {
            store: StoreProviderBinding::GoogleDrive { corpus },
            device: ProviderDeviceBinding {
                principal: ProviderPrincipalId::GoogleDrive { permission_id },
            },
        })
    }

    async fn allocate_slot(&self, logical_key: &str) -> Result<ObjectSlot, CloudHomeError> {
        ObjectSlot::opaque(
            logical_key.to_string(),
            self.generate_file_id(logical_key).await?,
        )
    }

    async fn create_at(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        GoogleDriveCloudHome::create_at_slot(self, slot, body, progress).await
    }
    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        GoogleDriveCloudHome::read_at_slot(self, slot).await
    }
    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        let range = super::range_header(start, end);
        let response = self.send_exact_read(slot, Some(&range)).await?;
        validated_range_bytes(response, "read exact Drive range", start, end).await
    }
    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        GoogleDriveCloudHome::read_at_slot_to_file(self, slot, destination).await
    }
    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        GoogleDriveCloudHome::delete_at_slot(self, slot).await
    }
}

fn drive_permissions_next_page_url(
    list_url: &str,
    page: &serde_json::Value,
) -> Result<Option<String>, CloudHomeError> {
    let Some(token) = page["nextPageToken"].as_str() else {
        return Ok(None);
    };
    let query = serde_urlencoded::to_string([("pageToken", token)])
        .map_err(|e| CloudHomeError::Transport(format!("encode Drive page token: {e}")))?;
    let separator = if list_url.contains('?') { '&' } else { '?' };
    Ok(Some(format!("{list_url}{separator}{query}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::extract::State;
    use axum::http::{Request, Response, StatusCode};
    use axum::Router;
    use std::sync::{Arc, Mutex};

    use crate::keys::StoreKeys;
    use crate::oauth::OAuthTokens;

    fn home() -> GoogleDriveCloudHome {
        let config = crate::oauth::OAuthClients::for_tests()
            .config_for(crate::config::CloudProvider::GoogleDrive)
            .expect("Google Drive test client");
        let session = OAuthSession::new(
            OAuthTokens {
                access_token: "test".to_string(),
                refresh_token: None,
                expires_at: None,
            },
            StoreKeys::bind("test".to_string()),
            Arc::new(crate::clock::SystemClock),
            config,
            "Google Drive",
        );
        GoogleDriveCloudHome::new("folder123".to_string(), session)
    }

    #[derive(Clone, Debug)]
    struct RecordedRequest {
        method: String,
        path: String,
        query: Option<String>,
        body: Vec<u8>,
    }

    async fn immutable_copy_endpoint(
        State(requests): State<Arc<Mutex<Vec<RecordedRequest>>>>,
        request: Request<Body>,
    ) -> Response<Body> {
        let method = request.method().to_string();
        let path = request.uri().path().to_string();
        let query = request.uri().query().map(str::to_string);
        let body = to_bytes(request.into_body(), usize::MAX)
            .await
            .expect("read request body")
            .to_vec();
        requests
            .lock()
            .expect("lock requests")
            .push(RecordedRequest {
                method: method.clone(),
                path: path.clone(),
                query: query.clone(),
                body,
            });

        if method == "GET" && path == "/files/generateIds" {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"ids":["generated-id"]}"#))
                .expect("build generated id response");
        }
        if method == "POST"
            && path == "/files"
            && query
                .as_deref()
                .is_some_and(|query| query.contains("uploadType=multipart"))
        {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"ignored-response-id"}"#))
                .expect("build append response");
        }
        if method == "GET"
            && path == "/files/generated-id"
            && query
                .as_deref()
                .is_some_and(|query| query.contains("fields="))
        {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"id":"generated-id","name":"{}","parents":["folder123"],"trashed":false,"appProperties":{{"covenLogicalKey":"protocol/copy"}}}}"#,
                    encode_key("protocol/copy"),
                )))
                .expect("build metadata response");
        }
        if method == "GET" && path == "/files/generated-id" {
            return Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("copy-bytes"))
                .expect("build read response");
        }
        if method == "DELETE" && path == "/files/generated-id" {
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("build delete response");
        }
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!(
                "unexpected request: {method} {path} {query:?}"
            )))
            .expect("build unexpected response")
    }

    async fn immutable_copy_test_home() -> (
        GoogleDriveCloudHome,
        Arc<Mutex<Vec<RecordedRequest>>>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .fallback(immutable_copy_endpoint)
            .with_state(requests.clone());
        let (endpoint, shutdown_tx) =
            crate::storage::cloud::test_server::spawn_test_server(app).await;
        (
            home()
                .with_endpoints(endpoint.clone(), endpoint)
                .with_ids(Arc::new(crate::id_provider::SequentialIdProvider::new(
                    "drive-create",
                ))),
            requests,
            shutdown_tx,
        )
    }

    #[tokio::test]
    async fn immutable_copy_uses_preallocated_id_for_create_read_and_delete() {
        let (home, requests, shutdown) = immutable_copy_test_home().await;
        let slot = home
            .allocate_slot("protocol/copy")
            .await
            .expect("allocate Drive slot");
        home.create_at(
            &slot,
            BlobBody::from_bytes(b"copy-bytes".to_vec()),
            &crate::storage::cloud::no_progress(),
        )
        .await
        .expect("create exact Drive object");
        assert_eq!(
            slot,
            ObjectSlot::opaque("protocol/copy".to_string(), "generated-id".to_string())
                .expect("opaque Drive slot")
        );
        assert_eq!(
            home.read_at(&slot).await.expect("read Drive copy"),
            b"copy-bytes"
        );
        home.delete_at(&slot).await.expect("delete Drive copy");

        let requests = requests.lock().expect("lock requests");
        assert_eq!(requests.len(), 6, "{requests:?}");
        assert_eq!(requests[0].path, "/files/generateIds");
        let upload = String::from_utf8(requests[1].body.clone()).expect("multipart body is UTF-8");
        assert_eq!(requests[1].method, "POST");
        assert!(requests[1]
            .query
            .as_deref()
            .is_some_and(|query| query.contains("uploadType=multipart")));
        assert!(upload.contains(r#""id":"generated-id""#), "{upload}");
        assert!(
            upload.contains(r#""covenLogicalKey":"protocol/copy""#),
            "{upload}"
        );
        assert!(!upload.contains(CREATE_TOKEN_PROPERTY), "{upload}");
        assert!(upload.contains(&encode_key("protocol/copy")), "{upload}");
        assert!(upload.contains("copy-bytes"), "{upload}");
        assert_eq!(requests[2].method, "GET");
        assert_eq!(requests[2].path, "/files/generated-id");
        assert!(requests[2]
            .query
            .as_deref()
            .is_some_and(|query| query.contains("fields=")));
        assert_eq!(requests[3].method, "GET");
        assert_eq!(requests[3].path, "/files/generated-id");
        assert_eq!(requests[4].method, "GET");
        assert_eq!(requests[4].path, "/files/generated-id");
        assert_eq!(requests[5].method, "DELETE");
        assert_eq!(requests[5].path, "/files/generated-id");
        for request in requests.iter().skip(1) {
            let query = request.query.as_deref().expect("Drive file request query");
            assert!(query.contains("supportsAllDrives=true"), "{request:?}");
        }
        drop(requests);
        shutdown.send(()).expect("shut down Drive endpoint");
    }

    #[tokio::test]
    async fn exact_operations_reject_a_drive_id_bound_to_another_logical_key() {
        let (home, requests, shutdown) = immutable_copy_test_home().await;
        let slot = ObjectSlot::opaque("protocol/other".to_string(), "generated-id".to_string())
            .expect("build mismatched Drive slot");

        let read_error = home
            .read_at(&slot)
            .await
            .expect_err("mismatched Drive read must fail");
        assert!(
            read_error.to_string().contains("does not identify"),
            "{read_error}"
        );
        let delete_error = home
            .delete_at(&slot)
            .await
            .expect_err("mismatched Drive delete must fail");
        assert!(
            delete_error.to_string().contains("does not identify"),
            "{delete_error}"
        );

        let requests = requests.lock().expect("lock requests");
        assert_eq!(requests.len(), 2, "{requests:?}");
        assert!(requests.iter().all(|request| request.method == "GET"));
        drop(requests);
        shutdown.send(()).expect("shut down Drive endpoint");
    }

    async fn malformed_location_endpoint() -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .header(
                reqwest::header::LOCATION,
                reqwest::header::HeaderValue::from_bytes(&[0xff])
                    .expect("build non-UTF-8 Location header"),
            )
            .body(Body::empty())
            .expect("build malformed Location response")
    }

    #[tokio::test]
    async fn resumable_create_rejects_a_non_utf8_location_header() {
        let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
            Router::new().fallback(malformed_location_endpoint),
        )
        .await;
        let home = home().with_endpoints(endpoint.clone(), endpoint);
        let attempt = DriveAppendAttempt {
            file_id: "generated-id".to_string(),
            create_token: "create-token".to_string(),
        };

        let error = home
            .open_resumable_create_session("protocol/copy", &attempt)
            .await
            .expect_err("non-UTF-8 Location must fail");

        assert!(
            error.to_string().contains("invalid Location header"),
            "{error}"
        );
        shutdown.send(()).expect("shut down test endpoint");
    }

    struct FailingMultipartBodyReader {
        emitted: bool,
    }

    #[async_trait]
    impl crate::storage::local_file::PlaintextChunkReader for FailingMultipartBodyReader {
        async fn next_chunk(
            &mut self,
            _max: usize,
        ) -> Result<Vec<u8>, crate::storage::local_file::PlaintextChunkError> {
            if !self.emitted {
                self.emitted = true;
                return Ok(vec![7; GDRIVE_CHUNK_SIZE]);
            }
            Err(crate::storage::local_file::PlaintextChunkError::Local(
                "injected Drive body failure".to_string(),
            ))
        }
    }

    struct EarlyEofMultipartBodyReader {
        emitted: bool,
    }

    #[async_trait]
    impl crate::storage::local_file::PlaintextChunkReader for EarlyEofMultipartBodyReader {
        async fn next_chunk(
            &mut self,
            _max: usize,
        ) -> Result<Vec<u8>, crate::storage::local_file::PlaintextChunkError> {
            if self.emitted {
                return Ok(Vec::new());
            }
            self.emitted = true;
            Ok(vec![7; GDRIVE_CHUNK_SIZE])
        }
    }

    struct PausedMultipartBodyReader {
        emitted: bool,
        waiting: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl crate::storage::local_file::PlaintextChunkReader for PausedMultipartBodyReader {
        async fn next_chunk(
            &mut self,
            _max: usize,
        ) -> Result<Vec<u8>, crate::storage::local_file::PlaintextChunkError> {
            if !self.emitted {
                self.emitted = true;
                return Ok(vec![7; GDRIVE_CHUNK_SIZE]);
            }
            self.waiting.notify_one();
            std::future::pending().await
        }
    }

    #[derive(Clone)]
    struct MutableCreateEndpointState {
        endpoint: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        deletes: Arc<std::sync::atomic::AtomicUsize>,
        first_delete_status: StatusCode,
    }

    async fn mutable_create_endpoint(
        State(state): State<MutableCreateEndpointState>,
        request: Request<Body>,
    ) -> Response<Body> {
        let method = request.method().to_string();
        let path = request.uri().path().to_string();
        let query = request.uri().query().map(str::to_string);
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
                query: query.clone(),
                body,
            });
        if method == "GET" && path == "/files" {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"files":[]}"#))
                .expect("build absent file response");
        }
        if method == "GET" && path == "/files/generateIds" {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"ids":["generated-id"]}"#))
                .expect("build generated id response");
        }
        if method == "POST"
            && path == "/files"
            && query
                .as_deref()
                .is_some_and(|query| query.contains("uploadType=resumable"))
        {
            return Response::builder()
                .status(StatusCode::OK)
                .header(
                    reqwest::header::LOCATION,
                    format!("{}/session", state.endpoint),
                )
                .body(Body::empty())
                .expect("build resumable session response");
        }
        if method == "PUT" && path == "/session" {
            return Response::builder()
                .status(StatusCode::PERMANENT_REDIRECT)
                .body(Body::empty())
                .expect("build incomplete upload response");
        }
        if method == "DELETE" && path == "/session" {
            let delete_index = state
                .deletes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Response::builder()
                .status(if delete_index == 0 {
                    state.first_delete_status
                } else {
                    StatusCode::NO_CONTENT
                })
                .body(Body::empty())
                .expect("build session cancellation response");
        }
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!(
                "unexpected request: {method} {path} {query:?}"
            )))
            .expect("build unexpected response")
    }

    async fn mutable_create_test_home() -> (
        GoogleDriveCloudHome,
        Arc<Mutex<Vec<RecordedRequest>>>,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        mutable_create_test_home_with_delete_status(StatusCode::NO_CONTENT).await
    }

    async fn mutable_create_test_home_with_delete_status(
        first_delete_status: StatusCode,
    ) -> (
        GoogleDriveCloudHome,
        Arc<Mutex<Vec<RecordedRequest>>>,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Drive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let deletes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let state = MutableCreateEndpointState {
            endpoint: endpoint.clone(),
            requests: requests.clone(),
            deletes: deletes.clone(),
            first_delete_status,
        };
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .fallback(mutable_create_endpoint)
                    .with_state(state),
            )
            .await
            .expect("Drive endpoint failed");
        });
        (
            home()
                .with_endpoints(endpoint.clone(), endpoint)
                .with_ids(Arc::new(crate::id_provider::SequentialIdProvider::new(
                    "drive-create",
                ))),
            requests,
            deletes,
            server,
        )
    }

    #[derive(Clone, Copy)]
    struct CreateEndpointBehavior {
        metadata_status: StatusCode,
        metadata_body: &'static str,
        rollback_lookup_status: StatusCode,
        upload_status: StatusCode,
        delete_status: StatusCode,
    }

    #[derive(Clone)]
    struct CreateEndpointState {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        create_token: Arc<Mutex<Option<String>>>,
        behavior: CreateEndpointBehavior,
    }

    async fn create_endpoint(
        State(state): State<CreateEndpointState>,
        request: Request<Body>,
    ) -> Response<Body> {
        let method = request.method().to_string();
        let path = request.uri().path().to_string();
        let query = request.uri().query().map(str::to_string);
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
                query,
                body: body.clone(),
            });

        match (method.as_str(), path.as_str()) {
            ("GET", "/files") => {
                let token = state
                    .create_token
                    .lock()
                    .expect("lock create token")
                    .clone();
                if token.is_some() && !state.behavior.rollback_lookup_status.is_success() {
                    return Response::builder()
                        .status(state.behavior.rollback_lookup_status)
                        .body(Body::from("lookup failed"))
                        .expect("build lookup failure");
                }
                let response = match token {
                    Some(token) => serde_json::json!({
                        "files": [{
                            "id": "created-file-id",
                            "appProperties": {
                                (CREATE_TOKEN_PROPERTY): token,
                            },
                        }],
                    }),
                    None => serde_json::json!({"files": []}),
                };
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(response.to_string()))
                    .expect("build file list response")
            }
            ("POST", "/files") if state.behavior.metadata_status.is_success() => {
                let metadata: serde_json::Value =
                    serde_json::from_slice(&body).expect("parse create metadata");
                let token = metadata["appProperties"][CREATE_TOKEN_PROPERTY]
                    .as_str()
                    .expect("create token")
                    .to_string();
                *state.create_token.lock().expect("lock create token") = Some(token);
                Response::builder()
                    .status(state.behavior.metadata_status)
                    .header("content-type", "application/json")
                    .body(Body::from(state.behavior.metadata_body))
                    .expect("build metadata response")
            }
            ("POST", "/files") => Response::builder()
                .status(state.behavior.metadata_status)
                .body(Body::from("metadata failed"))
                .expect("build metadata failure"),
            ("PATCH", "/files/created-file-id") => Response::builder()
                .status(state.behavior.upload_status)
                .body(Body::from("media upload failed"))
                .expect("build media response"),
            ("DELETE", "/files/created-file-id") => Response::builder()
                .status(state.behavior.delete_status)
                .body(Body::from("delete failed"))
                .expect("build delete response"),
            _ => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(format!("unexpected request: {method} {path}")))
                .expect("build unexpected response"),
        }
    }

    async fn create_test_home(
        behavior: CreateEndpointBehavior,
    ) -> (
        GoogleDriveCloudHome,
        Arc<Mutex<Vec<RecordedRequest>>>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = CreateEndpointState {
            requests: requests.clone(),
            create_token: Arc::new(Mutex::new(None)),
            behavior,
        };
        let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
            Router::new().fallback(create_endpoint).with_state(state),
        )
        .await;
        (
            home().with_endpoints(endpoint.clone(), endpoint),
            requests,
            shutdown,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn absent_mutable_multipart_body_failure_cancels_the_create_session() {
        let (home, requests, deletes, server) = mutable_create_test_home().await;
        let reader = crate::storage::local_file::PlaintextReader::from_test_reader(
            FailingMultipartBodyReader { emitted: false },
        );
        let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);

        let error = home
            .write("mutable/copy", body, &crate::storage::cloud::no_progress())
            .await
            .expect_err("Drive body failure must cancel its create session");

        assert!(
            error.to_string().contains("injected Drive body failure"),
            "{error}"
        );
        assert_eq!(deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
        let requests = requests.lock().expect("lock requests");
        let file_posts: Vec<_> = requests
            .iter()
            .filter(|request| request.method == "POST" && request.path == "/files")
            .collect();
        assert_eq!(file_posts.len(), 1, "{requests:?}");
        assert!(file_posts[0]
            .query
            .as_deref()
            .is_some_and(|query| query.contains("uploadType=resumable")));
        drop(requests);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn absent_mutable_multipart_early_eof_cancels_the_create_session() {
        let (home, _requests, deletes, server) = mutable_create_test_home().await;
        let reader = crate::storage::local_file::PlaintextReader::from_test_reader(
            EarlyEofMultipartBodyReader { emitted: false },
        );
        let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);

        let error = home
            .write("mutable/short", body, &crate::storage::cloud::no_progress())
            .await
            .expect_err("Drive must reject an incomplete upload body");

        assert!(error.to_string().contains("ended after"), "{error}");
        assert_eq!(deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drive_treats_499_as_successful_session_cancellation() {
        let (home, _requests, deletes, server) = mutable_create_test_home_with_delete_status(
            StatusCode::from_u16(499).expect("valid Drive cancellation status"),
        )
        .await;
        let reader = crate::storage::local_file::PlaintextReader::from_test_reader(
            FailingMultipartBodyReader { emitted: false },
        );
        let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);

        let error = home
            .write(
                "mutable/cancel-499",
                body,
                &crate::storage::cloud::no_progress(),
            )
            .await
            .expect_err("the body failure must surface");

        assert!(
            error.to_string().contains("injected Drive body failure"),
            "{error}"
        );
        assert!(error.cleanup_causes().is_none(), "{error}");
        assert_eq!(deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canceling_absent_mutable_multipart_cancels_the_create_session() {
        let (home, _requests, deletes, server) = mutable_create_test_home().await;
        let waiting = Arc::new(tokio::sync::Notify::new());
        let reader = crate::storage::local_file::PlaintextReader::from_test_reader(
            PausedMultipartBodyReader {
                emitted: false,
                waiting: waiting.clone(),
            },
        );
        let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);
        let write = tokio::spawn(async move {
            home.write("mutable/copy", body, &crate::storage::cloud::no_progress())
                .await
        });
        waiting.notified().await;

        write.abort();
        assert!(write.await.expect_err("write task canceled").is_cancelled());
        assert_eq!(deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
        server.abort();
    }

    async fn repeated_file_identity_page_endpoint(request: Request<Body>) -> Response<Body> {
        let path = request.uri().path();
        let body = if path == "/files" {
            r#"{"files":[],"nextPageToken":"same"}"#
        } else {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(format!("unexpected path: {path}")))
                .expect("build unexpected response");
        };
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("build repeated page response")
    }

    #[tokio::test]
    async fn file_identity_listing_rejects_a_repeated_page_token() {
        let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
            Router::new().fallback(repeated_file_identity_page_endpoint),
        )
        .await;
        let home = home().with_endpoints(endpoint.clone(), endpoint);

        let error = home
            .list_file_identities(&encode_key("protocol/copy"))
            .await
            .expect_err("repeated identity page token must fail");

        assert!(error.to_string().contains("repeated"), "{error}");
        shutdown.send(()).expect("shut down test endpoint");
    }

    async fn shared_drive_listing_endpoint(
        State(requests): State<Arc<Mutex<Vec<RecordedRequest>>>>,
        request: Request<Body>,
    ) -> Response<Body> {
        let path = request.uri().path().to_string();
        let query = request.uri().query().map(str::to_string);
        requests
            .lock()
            .expect("lock requests")
            .push(RecordedRequest {
                method: request.method().to_string(),
                path: path.clone(),
                query,
                body: Vec::new(),
            });
        let body = match path.as_str() {
            "/files" => r#"{"files":[]}"#,
            _ => {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from(format!("unexpected path: {path}")))
                    .expect("build unexpected response")
            }
        };
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("build listing response")
    }

    #[tokio::test]
    async fn exact_slot_identity_lookup_includes_shared_drives() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
            Router::new()
                .fallback(shared_drive_listing_endpoint)
                .with_state(requests.clone()),
        )
        .await;
        let home = home().with_endpoints(endpoint.clone(), endpoint);

        home.list_file_identities(&encode_key("protocol/copy"))
            .await
            .expect("look up shared Drive exact-slot identity");

        let requests = requests.lock().expect("lock requests");
        assert_eq!(requests.len(), 1, "{requests:?}");
        let query = requests[0].query.as_deref().expect("listing query");
        assert!(query.contains("supportsAllDrives=true"), "{query}");
        assert!(query.contains("includeItemsFromAllDrives=true"), "{query}");
        assert!(!query.contains("restrictToMyDrive"), "{query}");
        drop(requests);
        shutdown.send(()).expect("shut down test endpoint");
    }

    async fn generated_id_collision_endpoint(
        State(requests): State<Arc<Mutex<Vec<String>>>>,
        request: Request<Body>,
    ) -> Response<Body> {
        let method = request.method().to_string();
        let path = request.uri().path().to_string();
        requests
            .lock()
            .expect("lock requests")
            .push(format!("{method} {path}"));
        match (method.as_str(), path.as_str()) {
            ("GET", "/files/generateIds") => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"ids":["generated-id"]}"#))
                .expect("build generated id response"),
            ("POST", "/files") => Response::builder()
                .status(StatusCode::CONFLICT)
                .body(Body::from("generated id already exists"))
                .expect("build collision response"),
            _ => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("unexpected request"))
                .expect("build unexpected response"),
        }
    }

    #[tokio::test]
    async fn generated_id_collision_preserves_the_pre_existing_file() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
            Router::new()
                .fallback(generated_id_collision_endpoint)
                .with_state(requests.clone()),
        )
        .await;
        let home = home().with_endpoints(endpoint.clone(), endpoint);

        let slot = home
            .allocate_slot("protocol/collision")
            .await
            .expect("allocate collision slot");
        let error = home
            .create_at(
                &slot,
                BlobBody::from_bytes(b"new bytes".to_vec()),
                &crate::storage::cloud::no_progress(),
            )
            .await
            .expect_err("generated-id collision must fail");

        assert!(matches!(error, CloudHomeError::AlreadyExists(_)), "{error}");
        assert!(
            !requests
                .lock()
                .expect("lock requests")
                .iter()
                .any(|request| request.starts_with("DELETE ")),
            "collision deleted a pre-existing file"
        );
        shutdown.send(()).expect("shut down test endpoint");
    }

    #[derive(Clone, Default)]
    struct AmbiguousCreateState {
        committed: Arc<Mutex<bool>>,
        requests: Arc<Mutex<Vec<String>>>,
    }

    async fn ambiguous_create_endpoint(
        State(state): State<AmbiguousCreateState>,
        request: Request<Body>,
    ) -> Response<Body> {
        let method = request.method().to_string();
        let path = request.uri().path().to_string();
        let body = to_bytes(request.into_body(), usize::MAX)
            .await
            .expect("read request body");
        state
            .requests
            .lock()
            .expect("lock requests")
            .push(format!("{method} {path}"));
        match (method.as_str(), path.as_str()) {
            ("GET", "/files/generateIds") => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"ids":["generated-id"]}"#))
                .expect("build generated id response"),
            ("POST", "/files") => {
                let body = String::from_utf8(body.to_vec()).expect("multipart body is UTF-8");
                assert!(body.contains(r#""id":"generated-id""#), "{body}");
                assert!(
                    body.contains(r#""covenLogicalKey":"protocol/ambiguous""#),
                    "{body}"
                );
                assert!(!body.contains(CREATE_TOKEN_PROPERTY), "{body}");
                *state.committed.lock().expect("lock commit state") = true;
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("response lost after commit"))
                    .expect("build ambiguous response")
            }
            ("GET", "/files/generated-id") => {
                assert!(*state.committed.lock().expect("lock commit state"));
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": "generated-id",
                            "name": encode_key("protocol/ambiguous"),
                            "parents": ["folder123"],
                            "trashed": false,
                            "appProperties": {
                                (LOGICAL_KEY_PROPERTY): "protocol/ambiguous",
                            },
                        })
                        .to_string(),
                    ))
                    .expect("build owned metadata response")
            }
            ("DELETE", "/files/generated-id") => Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("build delete response"),
            _ => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("unexpected request"))
                .expect("build unexpected response"),
        }
    }

    #[tokio::test]
    async fn ambiguous_exact_create_preserves_the_logical_key_matched_commit() {
        let state = AmbiguousCreateState::default();
        let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
            Router::new()
                .fallback(ambiguous_create_endpoint)
                .with_state(state.clone()),
        )
        .await;
        let home = home().with_endpoints(endpoint.clone(), endpoint);

        let slot = home
            .allocate_slot("protocol/ambiguous")
            .await
            .expect("allocate ambiguous slot");
        home.create_at(
            &slot,
            BlobBody::from_bytes(b"committed bytes".to_vec()),
            &crate::storage::cloud::no_progress(),
        )
        .await
        .expect("logical-key-matched commit resolves ambiguous create");

        assert_eq!(
            slot,
            ObjectSlot::opaque("protocol/ambiguous".to_string(), "generated-id".to_string(),)
                .expect("opaque Drive slot")
        );
        assert!(
            !state
                .requests
                .lock()
                .expect("lock requests")
                .iter()
                .any(|request| request.starts_with("DELETE ")),
            "ambiguous committed file was deleted"
        );
        shutdown.send(()).expect("shut down test endpoint");
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
    fn find_file_query_escapes_encoded_name_and_folder() {
        let query = find_file_query("folder'1", "encoded'name");

        assert!(query.contains("'folder\\'1' in parents"));
        assert!(query.contains("name = 'encoded\\'name'"));
        assert!(!query.contains("encoded'name"));
    }

    #[test]
    fn find_created_file_query_escapes_name_and_create_token() {
        let query = find_created_file_query("folder-id", "object'1", r"token\2");

        assert!(query.contains("name = 'object\\'1'"));
        assert!(query.contains(r"value='token\\2'"));
    }

    #[test]
    fn list_file_query_escapes_encoded_prefix() {
        let query = list_file_query("folder-id", "artist's-live/");

        assert!(query.contains("name contains '61727469737427732d6c6976652f'"));
        assert!(!query.contains("artist's-live"));
    }

    #[test]
    fn drive_permissions_next_page_url_appends_encoded_page_token() {
        let page = serde_json::json!({"nextPageToken": "tok/en+1"});

        assert_eq!(
            drive_permissions_next_page_url(
                "https://www.googleapis.com/drive/v3/files/folder/permissions?fields=permissions(id,emailAddress),nextPageToken",
                &page,
            )
            .expect("encode next page")
            .as_deref(),
            Some("https://www.googleapis.com/drive/v3/files/folder/permissions?fields=permissions(id,emailAddress),nextPageToken&pageToken=tok%2Fen%2B1")
        );
    }

    #[test]
    fn parse_drive_file_identities_requires_create_tokens() {
        let page = serde_json::json!({
            "files": [
                {"id": "file-a", "appProperties": {"covenCreateToken": "token-b"}},
                {"id": "file-b", "appProperties": {"other": "ignored"}}
            ]
        });

        let error = parse_drive_file_identities(&page)
            .expect_err("a Drive file without its create token must fail");
        assert!(error.to_string().contains("file-b"), "{error}");
    }

    #[test]
    fn select_drive_file_uses_deterministic_create_token_tiebreak() {
        let files = vec![
            DriveFileIdentity {
                id: "local-loser".to_string(),
                create_token: "token-z".to_string(),
            },
            DriveFileIdentity {
                id: "peer-winner".to_string(),
                create_token: "token-a".to_string(),
            },
        ];

        assert_eq!(
            select_drive_file(&files).map(|file| file.id.as_str()),
            Some("peer-winner")
        );
    }

    #[test]
    fn parse_drive_file_identities_rejects_missing_ids() {
        let page = serde_json::json!({
            "files": [
                {"appProperties": {"covenCreateToken": "token-a"}}
            ]
        });

        assert!(parse_drive_file_identities(&page).is_err());
    }

    #[test]
    fn parse_list_page_skips_malformed_flat_names() {
        let valid = encode_key("objects/dev1/1.enc");
        let other_prefix = encode_key("snapshots/dev1.json.enc");
        let body = format!(
            r#"{{"files":[{{"name":"{valid}"}},{{"name":"not-hex"}},{{"name":"{other_prefix}"}}]}}"#
        );

        let page = home()
            .parse_list_page(&body, "objects/")
            .expect("parse list page");

        assert_eq!(page.keys, vec!["objects/dev1/1.enc"]);
    }

    #[test]
    fn folder_search_query_escapes_folder_name() {
        let query = folder_search_query("your-app - artist's live");

        assert!(query.contains("name = 'your-app - artist\\'s live'"));
        assert!(query.contains("mimeType = 'application/vnd.google-apps.folder'"));
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

    #[test]
    fn parse_create_file_id_extracts_id() {
        assert_eq!(
            parse_create_file_id(r#"{"id":"drive-file-1"}"#, "objects/a")
                .expect("parse created file id"),
            "drive-file-1",
        );
    }

    #[test]
    fn parse_create_file_id_errors_when_id_is_missing() {
        let err = parse_create_file_id(r#"{"name":"encoded-object-name"}"#, "objects/a")
            .expect_err("missing created file id");
        let msg = err.to_string();

        assert!(
            msg.contains("create objects/a"),
            "missing create path: {msg}"
        );
        assert!(msg.contains("missing id"), "missing id reason: {msg}");
    }

    #[test]
    fn generated_append_id_requires_one_nonempty_id() {
        assert_eq!(
            parse_generated_file_id(&serde_json::json!({"ids": ["drive-id-1"]}), "objects/a")
                .expect("parse generated file id"),
            "drive-id-1",
        );
        assert!(parse_generated_file_id(&serde_json::json!({"ids": []}), "objects/a").is_err());
        assert!(parse_generated_file_id(
            &serde_json::json!({"ids": ["drive-id-1", "drive-id-2"]}),
            "objects/a"
        )
        .is_err());
        assert!(parse_generated_file_id(&serde_json::json!({"ids": [""]}), "objects/a").is_err());
    }

    #[test]
    fn create_file_metadata_body_carries_rollback_token() {
        let body = create_file_metadata_body("encoded-object-name", "folder-1", "create-token-1");
        let json: serde_json::Value = serde_json::from_str(&body).expect("metadata json");

        assert_eq!(json["name"].as_str(), Some("encoded-object-name"));
        assert_eq!(json["parents"][0].as_str(), Some("folder-1"));
        assert_eq!(
            json["appProperties"][CREATE_TOKEN_PROPERTY].as_str(),
            Some("create-token-1"),
        );
    }

    #[tokio::test]
    async fn create_metadata_id_error_rolls_back_token_matched_file() {
        let (home, requests, shutdown) = create_test_home(CreateEndpointBehavior {
            metadata_status: StatusCode::OK,
            metadata_body: r#"{"name":"encoded-object-name"}"#,
            rollback_lookup_status: StatusCode::OK,
            upload_status: StatusCode::OK,
            delete_status: StatusCode::NO_CONTENT,
        })
        .await;
        let err = home
            .put_object("objects/a", b"media".to_vec())
            .await
            .expect_err("create id error");
        let msg = err.to_string();

        assert!(
            requests
                .lock()
                .expect("lock requests")
                .iter()
                .any(|request| {
                    request.method == "DELETE" && request.path == "/files/created-file-id"
                }),
            "token-matched created file was not deleted"
        );
        assert!(
            msg.contains("response missing id"),
            "missing create id failure: {msg}"
        );
        shutdown.send(()).expect("shut down test endpoint");
    }

    #[tokio::test]
    async fn create_metadata_id_error_reports_token_lookup_failure() {
        let (home, requests, shutdown) = create_test_home(CreateEndpointBehavior {
            metadata_status: StatusCode::OK,
            metadata_body: r#"{"name":"encoded-object-name"}"#,
            rollback_lookup_status: StatusCode::BAD_REQUEST,
            upload_status: StatusCode::OK,
            delete_status: StatusCode::NO_CONTENT,
        })
        .await;
        let err = home
            .put_object("objects/a", b"media".to_vec())
            .await
            .expect_err("lookup failure");
        let msg = err.to_string();

        assert!(
            !requests
                .lock()
                .expect("lock requests")
                .iter()
                .any(|request| request.method == "DELETE"),
            "delete ran without a resolved created file"
        );
        assert!(
            msg.contains("response missing id"),
            "missing create id failure: {msg}"
        );
        assert!(
            msg.contains("lookup failed"),
            "missing lookup failure: {msg}"
        );
        shutdown.send(()).expect("shut down test endpoint");
    }

    #[tokio::test]
    async fn create_metadata_id_error_reports_token_delete_failure() {
        let (home, requests, shutdown) = create_test_home(CreateEndpointBehavior {
            metadata_status: StatusCode::OK,
            metadata_body: r#"{"name":"encoded-object-name"}"#,
            rollback_lookup_status: StatusCode::OK,
            upload_status: StatusCode::OK,
            delete_status: StatusCode::BAD_REQUEST,
        })
        .await;
        let err = home
            .put_object("objects/a", b"media".to_vec())
            .await
            .expect_err("delete failure");
        let msg = err.to_string();

        assert!(
            requests
                .lock()
                .expect("lock requests")
                .iter()
                .any(|request| {
                    request.method == "DELETE" && request.path == "/files/created-file-id"
                }),
            "rollback delete was not attempted"
        );
        assert!(
            msg.contains("response missing id"),
            "missing create id failure: {msg}"
        );
        assert!(
            msg.contains("delete failed"),
            "missing delete failure: {msg}"
        );
        shutdown.send(()).expect("shut down test endpoint");
    }

    #[tokio::test]
    async fn create_file_with_media_does_not_delete_on_metadata_failure() {
        let (home, requests, shutdown) = create_test_home(CreateEndpointBehavior {
            metadata_status: StatusCode::BAD_REQUEST,
            metadata_body: r#"{"id":"created-file-id"}"#,
            rollback_lookup_status: StatusCode::OK,
            upload_status: StatusCode::OK,
            delete_status: StatusCode::NO_CONTENT,
        })
        .await;
        let err = home
            .put_object("objects/a", b"media".to_vec())
            .await
            .expect_err("metadata failure");
        let msg = err.to_string();

        assert!(
            !requests
                .lock()
                .expect("lock requests")
                .iter()
                .any(|request| request.method == "DELETE"),
            "delete ran without a created file id"
        );
        assert!(
            msg.contains("metadata failed"),
            "missing metadata failure: {msg}"
        );
        shutdown.send(()).expect("shut down test endpoint");
    }

    #[tokio::test]
    async fn create_file_with_media_deletes_created_id_on_media_failure() {
        let (home, requests, shutdown) = create_test_home(CreateEndpointBehavior {
            metadata_status: StatusCode::OK,
            metadata_body: r#"{"id":"created-file-id"}"#,
            rollback_lookup_status: StatusCode::OK,
            upload_status: StatusCode::BAD_REQUEST,
            delete_status: StatusCode::NO_CONTENT,
        })
        .await;
        let err = home
            .put_object("objects/a", b"media".to_vec())
            .await
            .expect_err("media failure");
        let msg = err.to_string();

        assert!(
            requests
                .lock()
                .expect("lock requests")
                .iter()
                .any(|request| request.method == "DELETE"
                    && request.path == "/files/created-file-id"),
            "created file was not deleted"
        );
        assert!(
            msg.contains("media upload failed"),
            "missing media failure: {msg}"
        );
        shutdown.send(()).expect("shut down test endpoint");
    }

    #[tokio::test]
    async fn create_file_with_media_reports_upload_and_rollback_failures() {
        let (home, _requests, shutdown) = create_test_home(CreateEndpointBehavior {
            metadata_status: StatusCode::OK,
            metadata_body: r#"{"id":"created-file-id"}"#,
            rollback_lookup_status: StatusCode::OK,
            upload_status: StatusCode::BAD_REQUEST,
            delete_status: StatusCode::BAD_REQUEST,
        })
        .await;
        let err = home
            .put_object("objects/a", b"media".to_vec())
            .await
            .expect_err("upload and rollback error");
        let msg = err.to_string();

        assert!(
            msg.contains("media upload failed"),
            "missing upload failure: {msg}"
        );
        assert!(
            msg.contains("delete failed"),
            "missing delete failure: {msg}"
        );
        shutdown.send(()).expect("shut down test endpoint");
    }
}
