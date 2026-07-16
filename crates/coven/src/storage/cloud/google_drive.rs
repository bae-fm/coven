//! Google Drive `CloudHome` implementation.
//!
//! Uses the Google Drive REST API v3 with OAuth 2.0 tokens. Files are stored flat
//! in a single folder — path separators are escaped by
//! [`super::key_encoding`]). The `read`/`read_range`/`list`/`delete` methods are
//! the shared [`OAuthRestHome`] implementations; this file supplies only the Drive
//! request shapes, the page parser, the upload paths, and sharing.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;

use super::http::{self, ensure_ok, ok_bytes, ok_json, NotFound};
use super::key_encoding::{decode_listed_key, encode_key};
use super::oauth_rest::{
    response_to_file, rest_delete, rest_list, rest_read, rest_read_range, ListPage, OAuthRestHome,
    PageTokenTracker,
};
use super::oauth_session::OAuthSession;
use super::resumable::{RangePutSink, RangePutUploader};
use super::{
    sharing, AppendedListing, AppendedObject, BlobBody, BoxPartSink, CloudAccessOutcome,
    CloudAccessState, CloudHome, CloudHomeError, CloudHomeJoinInfo, ImmutableCopyStorage,
    ListingCoverage, RevokeOutcome, UploadProgress,
};
use crate::clock::ClockRef;
use crate::id_provider::{IdRef, UuidProvider};
use crate::keys::StoreKeys;
use crate::oauth::{OAuthConfig, OAuthTokens};

const DRIVE_API: &str = "https://www.googleapis.com/drive/v3";
const UPLOAD_API: &str = "https://www.googleapis.com/upload/drive/v3";
const CREATE_TOKEN_PROPERTY: &str = "covenCreateToken";
const DRIVE_FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";

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

pub(super) fn folder_search_query(folder_name: &str) -> String {
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct DriveFileIdentity {
    id: String,
    create_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CreatedDriveFile {
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

impl GoogleDriveCloudHome {
    pub(crate) fn new(
        folder_id: String,
        tokens: OAuthTokens,
        key_service: StoreKeys,
        clock: ClockRef,
    ) -> Result<Self, CloudHomeError> {
        let config =
            Self::oauth_config().map_err(|e| CloudHomeError::Configuration(e.to_string()))?;
        Ok(Self {
            folder_id,
            drive_api: DRIVE_API.to_string(),
            upload_api: UPLOAD_API.to_string(),
            ids: std::sync::Arc::new(UuidProvider),
            session: OAuthSession::new(tokens, key_service, clock, config, "Google Drive"),
        })
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

    pub(crate) fn oauth_config() -> Result<OAuthConfig, crate::oauth::OAuthClientCredsError> {
        let creds = crate::oauth::oauth_client_creds("google_drive")?;
        Ok(OAuthConfig {
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
        })
    }

    /// The Drive HTTP client (shared, owned by the session).
    fn client(&self) -> &reqwest::Client {
        self.session.client()
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
            let resp = self
                .session
                .api_call(|token| {
                    let mut req = self
                        .client()
                        .get(format!("{}/files", self.drive_api))
                        .bearer_auth(token)
                        .query(&[
                            ("q", query.as_str()),
                            ("fields", "nextPageToken,files(id,appProperties)"),
                            ("pageSize", "1000"),
                        ]);
                    if let Some(ref page) = page {
                        req = req.query(&[("pageToken", page.as_str())]);
                    }
                    req
                })
                .await?;
            let resp = ensure_ok(resp, "list files", NotFound::Status).await?;
            let json: serde_json::Value = ok_json(resp, "parse list response").await?;
            files.extend(parse_drive_file_identities(&json));

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
    ) -> Result<CreatedDriveFile, CloudHomeError> {
        let create_token = uuid::Uuid::new_v4().to_string();
        let metadata = create_file_metadata_body(encoded, &self.folder_id, &create_token);
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!("{}/files", self.drive_api))
                    .bearer_auth(token)
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
        let id_result = match resp.text().await {
            Ok(body) => parse_create_file_id(&body, key),
            Err(e) => Err(CloudHomeError::Transport(format!(
                "create {key}: read response: {e}"
            ))),
        };
        finish_create_metadata_response(
            key,
            id_result,
            || self.find_created_file_id(encoded, &create_token),
            |file_id| async move { self.delete_created_file(key, &file_id).await },
        )
        .await
        .map(|id| CreatedDriveFile { id, create_token })
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
        created: CreatedDriveFile,
    ) -> Result<String, CloudHomeError> {
        let files = self.list_file_identities(encoded).await?;
        if !files.iter().any(|file| {
            file.id == created.id && file.create_token.as_deref() == Some(&created.create_token)
        }) {
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
            .api_call(|token| {
                self.client()
                    .get(format!("{}/files", self.drive_api))
                    .bearer_auth(token)
                    .query(&[
                        ("q", query.as_str()),
                        ("fields", "files(id)"),
                        ("pageSize", "1"),
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
            .api_call(|token| {
                self.client()
                    .patch(format!(
                        "{}/files/{}?uploadType=media",
                        self.upload_api, file_id
                    ))
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
                op,
            ));
        }
        Ok(())
    }

    async fn delete_created_file(&self, key: &str, file_id: &str) -> Result<(), CloudHomeError> {
        let resp = self
            .session
            .api_call(|token| {
                self.client()
                    .delete(format!("{}/files/{}", self.drive_api, file_id))
                    .bearer_auth(token)
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

    async fn new_append_attempt(&self, key: &str) -> Result<DriveAppendAttempt, CloudHomeError> {
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!("{}/files/generateIds", self.drive_api))
                    .bearer_auth(token)
                    .query(&[("count", "1"), ("space", "drive"), ("type", "files")])
            })
            .await?;
        let response =
            ensure_ok(response, "generate Drive append file id", NotFound::Status).await?;
        let json: serde_json::Value = ok_json(response, "parse generated Drive file id").await?;
        let file_id = parse_generated_file_id(&json, key)?;
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
            .api_call(|token| {
                self.client()
                    .get(format!("{}/files/{}", self.drive_api, attempt.file_id))
                    .bearer_auth(token)
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
    ) -> Result<AppendedObject, CloudHomeError> {
        match self.inspect_append_attempt(key, attempt).await {
            Ok(DriveAppendAttemptState::Absent) => Err(operation),
            Ok(DriveAppendAttemptState::Foreign) => {
                Err(CloudHomeError::AlreadyExists(key.to_string()))
            }
            Ok(DriveAppendAttemptState::Owned) if may_have_committed => {
                AppendedObject::from_provider(key.to_string(), attempt.file_id.clone())
            }
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

    async fn verify_appended_object(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        let file_id = object.opaque_provider_id();
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!("{}/files/{file_id}", self.drive_api))
                    .bearer_auth(token)
                    .query(&[("fields", "id,name,parents,trashed")])
            })
            .await?;
        let response = ensure_ok(
            response,
            &format!("verify appended {}", object.logical_key()),
            NotFound::Status,
        )
        .await?;
        let metadata: serde_json::Value =
            ok_json(response, "parse exact Drive file metadata").await?;
        let expected_name = encode_key(object.logical_key());
        let id_matches = metadata["id"].as_str() == Some(file_id);
        let name_matches = metadata["name"].as_str() == Some(expected_name.as_str());
        let parent_matches = metadata["parents"].as_array().is_some_and(|parents| {
            parents
                .iter()
                .any(|parent| parent.as_str() == Some(&self.folder_id))
        });
        let is_live = metadata["trashed"].as_bool() == Some(false);
        if !(id_matches && name_matches && parent_matches && is_live) {
            return Err(CloudHomeError::Transport(format!(
                "exact Drive locator for {} does not identify file {file_id} named {expected_name} in folder {}",
                object.logical_key(), self.folder_id
            )));
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
            .api_call(|token| {
                self.client()
                    .patch(&url)
                    .bearer_auth(token)
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

    /// Create a complete file in one multipart/related request. Drive does not
    /// expose the file until this request has accepted both metadata and media.
    async fn append_small_media(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<AppendedObject, CloudHomeError> {
        use sha2::{Digest, Sha256};

        let attempt = self.new_append_attempt(key).await?;
        let encoded = encode_key(key);
        let boundary = format!(
            "coven-append-{}",
            hex::encode(Sha256::digest(key.as_bytes()))
        );
        let metadata = serde_json::json!({
            "id": attempt.file_id,
            "name": encoded,
            "parents": [self.folder_id],
            "appProperties": { (CREATE_TOKEN_PROPERTY): attempt.create_token },
        })
        .to_string();
        let mut body = Vec::with_capacity(metadata.len() + data.len() + boundary.len() * 3 + 128);
        body.extend_from_slice(format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{metadata}\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes());
        body.extend_from_slice(&data);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let resp = match self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!(
                        "{}/files?uploadType=multipart&fields=id",
                        self.upload_api
                    ))
                    .bearer_auth(token)
                    .header(
                        "Content-Type",
                        format!("multipart/related; boundary={boundary}"),
                    )
                    .body(body.clone())
            })
            .await
        {
            Ok(response) => response,
            Err(operation) => {
                return self
                    .resolve_failed_append(key, &attempt, operation, true)
                    .await
            }
        };
        let status = resp.status();
        if !status.is_success() {
            let operation =
                classify_write_error(status, &http::body_text(resp).await, key, "append");
            return self
                .resolve_failed_append(key, &attempt, operation, true)
                .await;
        }
        AppendedObject::from_provider(key.to_string(), attempt.file_id)
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
            "appProperties": { (CREATE_TOKEN_PROPERTY): attempt.create_token },
        });
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .post(format!(
                        "{}/files?uploadType=resumable&fields=id",
                        self.upload_api
                    ))
                    .bearer_auth(token)
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

    async fn list_appended_objects(
        &self,
        prefix: &str,
    ) -> Result<Vec<AppendedObject>, CloudHomeError> {
        let start_token = self.appended_listing_start_token().await?;
        let query = list_file_query(&self.folder_id, prefix);
        let mut page_token: Option<String> = None;
        let mut page_tokens = PageTokenTracker::new("Google Drive appended baseline listing");
        let mut objects = HashMap::new();
        loop {
            let page = page_token.clone();
            let resp = self
                .session
                .api_call(|token| {
                    let mut request = self
                        .client()
                        .get(format!("{}/files", self.drive_api))
                        .bearer_auth(token)
                        .query(&[
                            ("q", query.as_str()),
                            ("fields", "incompleteSearch,nextPageToken,files(id,name)"),
                            ("pageSize", "1000"),
                            ("includeItemsFromAllDrives", "true"),
                            ("supportsAllDrives", "true"),
                        ]);
                    if let Some(ref page) = page {
                        request = request.query(&[("pageToken", page.as_str())]);
                    }
                    request
                })
                .await?;
            let resp = ensure_ok(resp, "list appended files", NotFound::Status).await?;
            let json: serde_json::Value = ok_json(resp, "parse appended file list").await?;
            if json["incompleteSearch"].as_bool() == Some(true) {
                return Err(CloudHomeError::Transport(
                    "Google Drive appended listing reported incompleteSearch".to_string(),
                ));
            }
            for file in json["files"].as_array().into_iter().flatten() {
                let id = file["id"]
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| {
                        CloudHomeError::Transport(
                            "Google Drive appended file entry omitted id".to_string(),
                        )
                    })?;
                let name = file["name"].as_str().ok_or_else(|| {
                    CloudHomeError::Transport(format!(
                        "Google Drive appended file {id} omitted name"
                    ))
                })?;
                let Some(logical_key) = decode_listed_key("Google Drive", name) else {
                    continue;
                };
                if logical_key.starts_with(prefix) {
                    objects.insert(
                        id.to_string(),
                        AppendedObject::from_provider(logical_key, id.to_string())?,
                    );
                }
            }
            match json["nextPageToken"].as_str() {
                Some(token) => page_token = Some(page_tokens.record(token)?),
                None => break,
            }
        }
        self.apply_appended_changes(prefix, start_token, &mut objects)
            .await?;
        let mut objects: Vec<_> = objects.into_values().collect();
        objects.sort_by(|left, right| {
            left.logical_key()
                .cmp(right.logical_key())
                .then_with(|| left.opaque_provider_id().cmp(right.opaque_provider_id()))
        });
        Ok(objects)
    }

    async fn appended_listing_start_token(&self) -> Result<String, CloudHomeError> {
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!("{}/changes/startPageToken", self.drive_api))
                    .bearer_auth(token)
                    .query(&[("fields", "startPageToken"), ("supportsAllDrives", "true")])
            })
            .await?;
        let response = ensure_ok(response, "get Drive change token", NotFound::Status).await?;
        let json: serde_json::Value = ok_json(response, "parse Drive change token").await?;
        json["startPageToken"]
            .as_str()
            .filter(|token| !token.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                CloudHomeError::Transport(
                    "Google Drive start-page-token response omitted startPageToken".to_string(),
                )
            })
    }

    async fn apply_appended_changes(
        &self,
        prefix: &str,
        start_token: String,
        objects: &mut HashMap<String, AppendedObject>,
    ) -> Result<(), CloudHomeError> {
        let mut page_token = start_token;
        let mut page_tokens = PageTokenTracker::new("Google Drive appended change listing");
        page_tokens.record(&page_token)?;
        loop {
            let response = self
                .session
                .api_call(|token| {
                    self.client()
                        .get(format!("{}/changes", self.drive_api))
                        .bearer_auth(token)
                        .query(&[
                            ("pageToken", page_token.as_str()),
                            ("pageSize", "1000"),
                            ("includeRemoved", "true"),
                            ("includeItemsFromAllDrives", "true"),
                            ("supportsAllDrives", "true"),
                            ("spaces", "drive"),
                            (
                                "fields",
                                "nextPageToken,newStartPageToken,changes(fileId,removed,file(id,name,parents,trashed))",
                            ),
                        ])
                })
                .await?;
            let response = ensure_ok(response, "list Drive changes", NotFound::Status).await?;
            let json: serde_json::Value = ok_json(response, "parse Drive changes").await?;
            apply_drive_changes(&json, &self.folder_id, prefix, objects)?;

            if let Some(next) = json["nextPageToken"]
                .as_str()
                .filter(|token| !token.is_empty())
            {
                page_token = page_tokens.record(next)?;
                continue;
            }
            if json["newStartPageToken"]
                .as_str()
                .is_some_and(|token| !token.is_empty())
            {
                return Ok(());
            }
            return Err(CloudHomeError::Transport(
                "Google Drive changes page omitted both nextPageToken and newStartPageToken"
                    .to_string(),
            ));
        }
    }
}

fn apply_drive_changes(
    page: &serde_json::Value,
    folder_id: &str,
    prefix: &str,
    objects: &mut HashMap<String, AppendedObject>,
) -> Result<(), CloudHomeError> {
    for change in page["changes"].as_array().into_iter().flatten() {
        let file_id = change["fileId"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                CloudHomeError::Transport("Google Drive change omitted fileId".to_string())
            })?;
        let file = &change["file"];
        let removed =
            change["removed"].as_bool() == Some(true) || file["trashed"].as_bool() == Some(true);
        if removed {
            objects.remove(file_id);
            continue;
        }
        let name = file["name"].as_str().ok_or_else(|| {
            CloudHomeError::Transport(format!("Google Drive change {file_id} omitted file.name"))
        })?;
        let parents = file["parents"].as_array().ok_or_else(|| {
            CloudHomeError::Transport(format!(
                "Google Drive change {file_id} omitted file.parents"
            ))
        })?;
        if !parents
            .iter()
            .any(|parent| parent.as_str() == Some(folder_id))
        {
            objects.remove(file_id);
            continue;
        }
        let Some(logical_key) = decode_listed_key("Google Drive", name) else {
            objects.remove(file_id);
            continue;
        };
        if logical_key.starts_with(prefix) {
            objects.insert(
                file_id.to_string(),
                AppendedObject::from_provider(logical_key, file_id.to_string())?,
            );
        } else {
            objects.remove(file_id);
        }
    }
    Ok(())
}

fn parse_drive_file_identities(page: &serde_json::Value) -> Vec<DriveFileIdentity> {
    page["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|file| {
            let id = file["id"].as_str()?.to_string();
            let create_token = file["appProperties"][CREATE_TOKEN_PROPERTY]
                .as_str()
                .map(String::from);
            Some(DriveFileIdentity { id, create_token })
        })
        .collect()
}

fn select_drive_file(files: &[DriveFileIdentity]) -> Option<&DriveFileIdentity> {
    files.iter().min_by(|left, right| {
        let left_token = left.create_token.as_deref().unwrap_or(left.id.as_str());
        let right_token = right.create_token.as_deref().unwrap_or(right.id.as_str());
        left_token
            .cmp(right_token)
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

async fn finish_create_metadata_response<F, FFut, D, DFut>(
    key: &str,
    id_result: Result<String, CloudHomeError>,
    find_created_file: F,
    delete_created_file: D,
) -> Result<String, CloudHomeError>
where
    F: FnOnce() -> FFut,
    FFut: std::future::Future<Output = Result<Option<String>, CloudHomeError>>,
    D: FnOnce(String) -> DFut,
    DFut: std::future::Future<Output = Result<(), CloudHomeError>>,
{
    let id_error = match id_result {
        Ok(file_id) => return Ok(file_id),
        Err(e) => e,
    };
    match find_created_file().await {
        Ok(Some(file_id)) => match delete_created_file(file_id).await {
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

async fn finish_create_media_upload<F, Fut>(
    key: &str,
    upload_result: Result<(), CloudHomeError>,
    delete_created_file: F,
) -> Result<(), CloudHomeError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), CloudHomeError>>,
{
    let upload_error = match upload_result {
        Ok(()) => return Ok(()),
        Err(e) => e,
    };
    match delete_created_file().await {
        Ok(()) => Err(upload_error),
        Err(delete_error) => Err(CloudHomeError::Transport(format!(
            "create {key}: media upload failed after metadata create: {upload_error}; rollback delete failed: {delete_error}"
        ))),
    }
}

async fn create_file_with_media<Create, CreateFut, Upload, UploadFut, Delete, DeleteFut>(
    key: &str,
    create_metadata: Create,
    upload_media: Upload,
    delete_created_file: Delete,
) -> Result<(), CloudHomeError>
where
    Create: FnOnce() -> CreateFut,
    CreateFut: std::future::Future<Output = Result<String, CloudHomeError>>,
    Upload: FnOnce(String) -> UploadFut,
    UploadFut: std::future::Future<Output = Result<(), CloudHomeError>>,
    Delete: FnOnce(String) -> DeleteFut,
    DeleteFut: std::future::Future<Output = Result<(), CloudHomeError>>,
{
    let file_id = create_metadata().await?;
    let upload_result = upload_media(file_id.clone()).await;
    finish_create_media_upload(key, upload_result, || delete_created_file(file_id)).await
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
    created: Option<CreatedDriveFile>,
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
            .api_call(|token| {
                let mut req = self
                    .client()
                    .get(format!("{}/files/{}", self.drive_api, file_id))
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
                    .delete(format!("{}/files/{}", self.drive_api, file_id))
                    .bearer_auth(token)
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
            .api_call(|token| {
                let mut req = self
                    .client()
                    .get(format!("{}/files", self.drive_api))
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
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let media_body = Bytes::from(data);
        let encoded = encode_key(key);
        if let Some(file_id) = self.find_file_id(&encoded).await? {
            self.upload_file_media(key, &file_id, media_body, "update")
                .await?;
        } else {
            create_file_with_media(
                key,
                || self.create_file_for_key(key, &encoded),
                |file_id| {
                    let body = media_body.clone();
                    async move { self.upload_file_media(key, &file_id, body, "create").await }
                },
                |file_id| async move { self.delete_created_file(key, &file_id).await },
            )
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
                            Ok(object) => Err(CloudHomeError::Transport(format!(
                            "open mutable Drive upload {key}: uncommitted session resolved as {}",
                            object.opaque_provider_id()
                        ))),
                        }
                    }
                };
                (
                    session_url,
                    Some(CreatedDriveFile {
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
        let inner = RangePutSink::new(
            self.client().clone(),
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

    async fn append_object(
        &self,
        full_logical_key: &str,
        mut body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<AppendedObject, CloudHomeError> {
        if body.len() <= self.multipart_threshold() {
            let bytes = body.collect().await?;
            let length = bytes.len() as u64;
            let object = self.append_small_media(full_logical_key, bytes).await?;
            progress(length);
            return Ok(object);
        }

        let attempt = self.new_append_attempt(full_logical_key).await?;
        let session_url = match self
            .open_resumable_create_session(full_logical_key, &attempt)
            .await
        {
            Ok(session_url) => session_url,
            Err(operation) => {
                return self
                    .resolve_failed_append(full_logical_key, &attempt, operation, false)
                    .await
            }
        };
        let key = full_logical_key.to_string();
        let classify = Box::new(move |status, response: &str| {
            classify_write_error(status, response, &key, "append")
        });
        let mut uploader = RangePutUploader::new(
            self.client().clone(),
            session_url,
            308,
            body.len(),
            GDRIVE_CHUNK_SIZE,
            full_logical_key.to_string(),
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
                        "append {full_logical_key}: upload body ended before the final part"
                    ));
                    let operation = match uploader.abort().await {
                        Ok(()) => operation,
                        Err(cleanup) => CloudHomeError::CleanupFailed {
                            operation: Box::new(operation),
                            cleanup: Box::new(cleanup),
                        },
                    };
                    return self
                        .resolve_failed_append(full_logical_key, &attempt, operation, false)
                        .await;
                }
                Err(operation) => {
                    let operation = match uploader.abort().await {
                        Ok(()) => operation,
                        Err(cleanup) => CloudHomeError::CleanupFailed {
                            operation: Box::new(operation),
                            cleanup: Box::new(cleanup),
                        },
                    };
                    return self
                        .resolve_failed_append(full_logical_key, &attempt, operation, false)
                        .await;
                }
            };
            let length = part.len() as u64;
            let is_last = offset + length >= total;
            let completion = match uploader.send_part(part, offset, is_last).await {
                Ok(completion) => completion,
                Err(operation) => {
                    return self
                        .resolve_failed_append(full_logical_key, &attempt, operation, is_last)
                        .await
                }
            };
            offset += length;
            progress(offset);
            if completion.is_some() {
                return AppendedObject::from_provider(
                    full_logical_key.to_string(),
                    attempt.file_id,
                );
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
        let file_id = object.opaque_provider_id().to_string();
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!("{}/files/{file_id}", self.drive_api))
                    .bearer_auth(token)
                    .query(&[("alt", "media")])
            })
            .await?;
        let response = ensure_ok(
            response,
            &format!("read appended {}", object.logical_key()),
            NotFound::Status,
        )
        .await?;
        ok_bytes(
            response,
            &format!("read appended body for {}", object.logical_key()),
        )
        .await
    }

    async fn read_appended_to_file(
        &self,
        object: &AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        self.verify_appended_object(object).await?;
        let file_id = object.opaque_provider_id().to_string();
        let response = self
            .session
            .api_call(|token| {
                self.client()
                    .get(format!("{}/files/{file_id}", self.drive_api))
                    .bearer_auth(token)
                    .query(&[("alt", "media")])
            })
            .await?;
        let response = ensure_ok(
            response,
            &format!("read appended {}", object.logical_key()),
            NotFound::Status,
        )
        .await?;
        response_to_file(
            response,
            destination,
            &format!("read appended body for {}", object.logical_key()),
        )
        .await
    }

    async fn delete_appended(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        self.verify_appended_object(object).await?;
        self.delete_created_file(object.logical_key(), object.opaque_provider_id())
            .await
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
            "{}/files/{}/permissions?fields=permissions(id,emailAddress,role),nextPageToken",
            self.drive_api, self.folder_id
        );
        match desired {
            CloudAccessState::Present { .. } => {
                let list_url_for_next = list_url.clone();
                let current = sharing::permission_by_email(
                    &self.session,
                    email,
                    &list_url,
                    "permissions",
                    &|permission| permission["emailAddress"].as_str().map(String::from),
                    &move |page| drive_permissions_next_page_url(&list_url_for_next, page),
                )
                .await?;
                if current
                    .as_ref()
                    .is_some_and(|permission| permission["role"].as_str() == Some("writer"))
                {
                    return Ok(CloudAccessOutcome::Present(
                        CloudHomeJoinInfo::GoogleDrive {
                            folder_id: self.folder_id.clone(),
                        },
                    ));
                }
                if current.is_some() {
                    let next_base = list_url.clone();
                    let folder_id = self.folder_id.clone();
                    sharing::ensure_absent_by_email(
                        &self.session,
                        email,
                        &list_url,
                        "permissions",
                        |permission| permission["emailAddress"].as_str().map(String::from),
                        |permission_id| {
                            format!(
                                "{}/files/{}/permissions/{}",
                                self.drive_api, folder_id, permission_id
                            )
                        },
                        move |page| drive_permissions_next_page_url(&next_base, page),
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
                    .api_call(|token| {
                        self.client()
                            .post(format!(
                                "{}/files/{}/permissions",
                                self.drive_api, self.folder_id
                            ))
                            .bearer_auth(token)
                            .json(&permission)
                    })
                    .await?;
                ensure_ok(resp, &format!("grant access to {email}"), NotFound::Status).await?;
                let next_base = list_url.clone();
                let verified = sharing::permission_by_email(
                    &self.session,
                    email,
                    &list_url,
                    "permissions",
                    &|permission| permission["emailAddress"].as_str().map(String::from),
                    &move |page| drive_permissions_next_page_url(&next_base, page),
                )
                .await?;
                if verified
                    .as_ref()
                    .is_none_or(|permission| permission["role"].as_str() != Some("writer"))
                {
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
                let next_base = list_url.clone();
                let folder_id = self.folder_id.clone();
                sharing::ensure_absent_by_email(
                    &self.session,
                    email,
                    &list_url,
                    "permissions",
                    |permission| permission["emailAddress"].as_str().map(String::from),
                    |permission_id| {
                        format!(
                            "{}/files/{}/permissions/{}",
                            self.drive_api, folder_id, permission_id
                        )
                    },
                    move |page| drive_permissions_next_page_url(&next_base, page),
                )
                .await?;
                Ok(CloudAccessOutcome::Absent(RevokeOutcome::Revoked))
            }
        }
    }
}

#[async_trait]
impl CloudHome for GoogleDriveCloudHome {
    fn immutable_copy_storage(
        self: std::sync::Arc<Self>,
    ) -> Option<std::sync::Arc<dyn ImmutableCopyStorage>> {
        Some(self)
    }
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        GoogleDriveCloudHome::put_object(self, key, data).await
    }
    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        GoogleDriveCloudHome::open_multipart(self, key, total_len).await
    }
    fn multipart_threshold(&self) -> u64 {
        GoogleDriveCloudHome::multipart_threshold(self)
    }
    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        GoogleDriveCloudHome::read(self, key).await
    }
    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        GoogleDriveCloudHome::read_range(self, key, start, end).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        GoogleDriveCloudHome::list(self, prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        GoogleDriveCloudHome::delete(self, key).await
    }
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        GoogleDriveCloudHome::exists(self, key).await
    }
    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        GoogleDriveCloudHome::set_access(self, desired).await
    }
}

#[async_trait]
impl ImmutableCopyStorage for GoogleDriveCloudHome {
    async fn append_object(
        &self,
        key: &str,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<AppendedObject, CloudHomeError> {
        GoogleDriveCloudHome::append_object(self, key, body, progress).await
    }
    async fn list_appended(&self, prefix: &str) -> Result<AppendedListing, CloudHomeError> {
        GoogleDriveCloudHome::list_appended(self, prefix).await
    }
    async fn read_appended(&self, object: &AppendedObject) -> Result<Vec<u8>, CloudHomeError> {
        GoogleDriveCloudHome::read_appended(self, object).await
    }
    async fn read_appended_to_file(
        &self,
        object: &AppendedObject,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        GoogleDriveCloudHome::read_appended_to_file(self, object, destination).await
    }
    async fn delete_appended(&self, object: &AppendedObject) -> Result<(), CloudHomeError> {
        GoogleDriveCloudHome::delete_appended(self, object).await
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

    fn home() -> GoogleDriveCloudHome {
        crate::oauth::install_test_client_creds();
        GoogleDriveCloudHome::new(
            "folder123".to_string(),
            OAuthTokens {
                access_token: "test".to_string(),
                refresh_token: None,
                expires_at: None,
            },
            StoreKeys::new("test".to_string()),
            Arc::new(crate::clock::SystemClock),
        )
        .expect("build test Google Drive home")
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
                    r#"{{"id":"generated-id","name":"{}","parents":["folder123"],"trashed":false}}"#,
                    encode_key("protocol/copy")
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Drive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .fallback(immutable_copy_endpoint)
            .with_state(requests.clone());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.expect("receive Drive endpoint shutdown");
                })
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
            shutdown_tx,
        )
    }

    #[tokio::test]
    async fn immutable_copy_uses_preallocated_id_for_create_read_and_delete() {
        let (home, requests, shutdown) = immutable_copy_test_home().await;
        let object = home
            .append_object(
                "protocol/copy",
                BlobBody::from_bytes(b"copy-bytes".to_vec()),
                &super::super::no_progress(),
            )
            .await
            .expect("append Drive copy");
        assert_eq!(object.opaque_provider_id(), "generated-id");
        assert_eq!(
            home.read_appended(&object).await.expect("read Drive copy"),
            b"copy-bytes"
        );
        home.delete_appended(&object)
            .await
            .expect("delete Drive copy");

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
            upload.contains(r#""covenCreateToken":"drive-create-0""#),
            "{upload}"
        );
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
        drop(requests);
        shutdown.send(()).expect("shut down Drive endpoint");
    }

    #[tokio::test]
    async fn exact_operations_reject_a_drive_id_bound_to_another_logical_key() {
        let (home, requests, shutdown) = immutable_copy_test_home().await;
        let object =
            AppendedObject::from_provider("protocol/other".to_string(), "generated-id".to_string())
                .expect("build mismatched Drive locator");

        let read_error = home
            .read_appended(&object)
            .await
            .expect_err("mismatched Drive read must fail");
        assert!(
            read_error.to_string().contains("does not identify"),
            "{read_error}"
        );
        let delete_error = home
            .delete_appended(&object)
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Drive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(malformed_location_endpoint),
            )
            .await
            .expect("Drive endpoint failed");
        });
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
        server.abort();
    }

    struct FailingMultipartBodyReader {
        emitted: bool,
    }

    #[async_trait]
    impl crate::local_blob::PlaintextChunkReader for FailingMultipartBodyReader {
        async fn next_chunk(
            &mut self,
            _max: usize,
        ) -> Result<Vec<u8>, crate::local_blob::PlaintextChunkError> {
            if !self.emitted {
                self.emitted = true;
                return Ok(vec![7; GDRIVE_CHUNK_SIZE]);
            }
            Err(crate::local_blob::PlaintextChunkError::Local(
                "injected Drive body failure".to_string(),
            ))
        }
    }

    struct EarlyEofMultipartBodyReader {
        emitted: bool,
    }

    #[async_trait]
    impl crate::local_blob::PlaintextChunkReader for EarlyEofMultipartBodyReader {
        async fn next_chunk(
            &mut self,
            _max: usize,
        ) -> Result<Vec<u8>, crate::local_blob::PlaintextChunkError> {
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
    impl crate::local_blob::PlaintextChunkReader for PausedMultipartBodyReader {
        async fn next_chunk(
            &mut self,
            _max: usize,
        ) -> Result<Vec<u8>, crate::local_blob::PlaintextChunkError> {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn absent_mutable_multipart_body_failure_cancels_the_create_session() {
        let (home, requests, deletes, server) = mutable_create_test_home().await;
        let reader =
            crate::local_blob::PlaintextReader::from_test_reader(FailingMultipartBodyReader {
                emitted: false,
            });
        let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);

        let error = home
            .write("mutable/copy", body, &super::super::no_progress())
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
        let reader =
            crate::local_blob::PlaintextReader::from_test_reader(EarlyEofMultipartBodyReader {
                emitted: false,
            });
        let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);

        let error = home
            .write("mutable/short", body, &super::super::no_progress())
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
        let reader =
            crate::local_blob::PlaintextReader::from_test_reader(FailingMultipartBodyReader {
                emitted: false,
            });
        let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);

        let error = home
            .write("mutable/cancel-499", body, &super::super::no_progress())
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
        let reader =
            crate::local_blob::PlaintextReader::from_test_reader(PausedMultipartBodyReader {
                emitted: false,
                waiting: waiting.clone(),
            });
        let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);
        let write = tokio::spawn(async move {
            home.write("mutable/copy", body, &super::super::no_progress())
                .await
        });
        waiting.notified().await;

        write.abort();
        assert!(write.await.expect_err("write task canceled").is_cancelled());
        assert_eq!(deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
        server.abort();
    }

    async fn repeated_drive_page_endpoint(request: Request<Body>) -> Response<Body> {
        let path = request.uri().path();
        let body = match path {
            "/changes/startPageToken" => r#"{"startPageToken":"start"}"#,
            "/files" => r#"{"files":[]}"#,
            "/changes" => r#"{"changes":[],"nextPageToken":"start"}"#,
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
            .expect("build repeated page response")
    }

    #[tokio::test]
    async fn authoritative_listing_rejects_a_repeated_change_token() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Drive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(repeated_drive_page_endpoint),
            )
            .await
            .expect("Drive endpoint failed");
        });
        let home = home().with_endpoints(endpoint.clone(), endpoint);

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            home.list_appended("protocol/"),
        )
        .await
        .expect("listing must terminate on a repeated page token")
        .expect_err("repeated page token must refuse authoritative coverage");

        assert!(result.to_string().contains("repeated"), "{result}");
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Drive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(repeated_file_identity_page_endpoint),
            )
            .await
            .expect("Drive endpoint failed");
        });
        let home = home().with_endpoints(endpoint.clone(), endpoint);

        let error = home
            .list_file_identities(&encode_key("protocol/copy"))
            .await
            .expect_err("repeated identity page token must fail");

        assert!(error.to_string().contains("repeated"), "{error}");
        server.abort();
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
            "/changes/startPageToken" => r#"{"startPageToken":"start"}"#,
            "/files" => r#"{"files":[]}"#,
            "/changes" => r#"{"changes":[],"newStartPageToken":"end"}"#,
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
    async fn authoritative_listing_includes_shared_drive_changes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Drive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .fallback(shared_drive_listing_endpoint)
            .with_state(requests.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("Drive endpoint failed");
        });
        let home = home().with_endpoints(endpoint.clone(), endpoint);

        home.list_appended("protocol/")
            .await
            .expect("list shared Drive files");

        let requests = requests.lock().expect("lock requests");
        assert_eq!(requests.len(), 3, "{requests:?}");
        for request in &*requests {
            let query = request.query.as_deref().expect("listing query");
            assert!(query.contains("supportsAllDrives=true"), "{query}");
        }
        for request in requests
            .iter()
            .filter(|request| request.path != "/changes/startPageToken")
        {
            let query = request.query.as_deref().expect("listing query");
            assert!(query.contains("includeItemsFromAllDrives=true"), "{query}");
            assert!(!query.contains("restrictToMyDrive"), "{query}");
        }
        drop(requests);
        server.abort();
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
            ("GET", "/files/generated-id") => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"id":"generated-id","appProperties":{"covenCreateToken":"different-invocation"}}"#,
                ))
                .expect("build existing metadata response"),
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
    async fn generated_id_collision_preserves_the_pre_existing_file() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Drive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = requests.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .fallback(generated_id_collision_endpoint)
                    .with_state(state),
            )
            .await
            .expect("Drive endpoint failed");
        });
        let home = home().with_endpoints(endpoint.clone(), endpoint);

        let error = home
            .append_object(
                "protocol/collision",
                BlobBody::from_bytes(b"new bytes".to_vec()),
                &super::super::no_progress(),
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
        server.abort();
    }

    #[derive(Clone, Default)]
    struct AmbiguousCreateState {
        create_token: Arc<Mutex<Option<String>>>,
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
                let marker = format!(r#""{}":""#, CREATE_TOKEN_PROPERTY);
                let token = body
                    .split_once(&marker)
                    .and_then(|(_, tail)| tail.split_once('"'))
                    .map(|(token, _)| token.to_string())
                    .expect("append metadata carries its create token");
                state
                    .create_token
                    .lock()
                    .expect("lock create token")
                    .replace(token);
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("response lost after commit"))
                    .expect("build ambiguous response")
            }
            ("GET", "/files/generated-id") => {
                let token = state
                    .create_token
                    .lock()
                    .expect("lock create token")
                    .clone()
                    .expect("create token recorded");
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": "generated-id",
                            "appProperties": { (CREATE_TOKEN_PROPERTY): token },
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
    async fn ambiguous_create_preserves_and_returns_the_token_matched_commit() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Drive endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let state = AmbiguousCreateState::default();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .fallback(ambiguous_create_endpoint)
                    .with_state(server_state),
            )
            .await
            .expect("Drive endpoint failed");
        });
        let home = home().with_endpoints(endpoint.clone(), endpoint);

        let object = home
            .append_object(
                "protocol/ambiguous",
                BlobBody::from_bytes(b"committed bytes".to_vec()),
                &super::super::no_progress(),
            )
            .await
            .expect("token-matched commit resolves ambiguous create");

        assert_eq!(object.opaque_provider_id(), "generated-id");
        assert!(
            !state
                .requests
                .lock()
                .expect("lock requests")
                .iter()
                .any(|request| request.starts_with("DELETE ")),
            "ambiguous committed file was deleted"
        );
        server.abort();
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
    fn parse_drive_file_identities_reads_create_tokens() {
        let page = serde_json::json!({
            "files": [
                {"id": "file-a", "appProperties": {"covenCreateToken": "token-b"}},
                {"id": "file-b", "appProperties": {"other": "ignored"}},
                {"name": "missing-id"}
            ]
        });

        assert_eq!(
            parse_drive_file_identities(&page),
            vec![
                DriveFileIdentity {
                    id: "file-a".to_string(),
                    create_token: Some("token-b".to_string()),
                },
                DriveFileIdentity {
                    id: "file-b".to_string(),
                    create_token: None,
                },
            ]
        );
    }

    #[test]
    fn select_drive_file_uses_deterministic_create_token_tiebreak() {
        let files = vec![
            DriveFileIdentity {
                id: "local-loser".to_string(),
                create_token: Some("token-z".to_string()),
            },
            DriveFileIdentity {
                id: "peer-winner".to_string(),
                create_token: Some("token-a".to_string()),
            },
        ];

        assert_eq!(
            select_drive_file(&files).map(|file| file.id.as_str()),
            Some("peer-winner")
        );
    }

    #[test]
    fn select_drive_file_is_stable_for_files_without_tokens() {
        let files = vec![
            DriveFileIdentity {
                id: "file-b".to_string(),
                create_token: None,
            },
            DriveFileIdentity {
                id: "file-a".to_string(),
                create_token: None,
            },
        ];

        assert_eq!(
            select_drive_file(&files).map(|file| file.id.as_str()),
            Some("file-a")
        );
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
    fn change_replay_moves_removes_and_adds_exact_provider_ids() {
        let mut objects = HashMap::from([
            (
                "moved".to_string(),
                AppendedObject::from_provider("protocol/old".to_string(), "moved".to_string())
                    .unwrap(),
            ),
            (
                "deleted".to_string(),
                AppendedObject::from_provider(
                    "protocol/deleted".to_string(),
                    "deleted".to_string(),
                )
                .unwrap(),
            ),
        ]);
        let page = serde_json::json!({
            "changes": [
                {
                    "fileId": "moved",
                    "file": {"id": "moved", "name": encode_key("elsewhere/object"), "parents": ["folder"], "trashed": false}
                },
                {"fileId": "deleted", "removed": true},
                {
                    "fileId": "created",
                    "file": {"id": "created", "name": encode_key("protocol/new"), "parents": ["folder"], "trashed": false}
                },
                {
                    "fileId": "other-folder",
                    "file": {"id": "other-folder", "name": encode_key("protocol/other"), "parents": ["different"], "trashed": false}
                }
            ]
        });

        apply_drive_changes(&page, "folder", "protocol/", &mut objects)
            .expect("apply Drive change page");

        assert_eq!(objects.len(), 1);
        let created = objects.get("created").expect("created change is present");
        assert_eq!(created.logical_key(), "protocol/new");
        assert_eq!(created.opaque_provider_id(), "created");
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
        let delete_called_with = std::cell::RefCell::new(None);
        let id_error = CloudHomeError::Transport("response missing id".to_string());
        let err = finish_create_metadata_response(
            "objects/a",
            Err(id_error),
            || async { Ok(Some("created-file-id".to_string())) },
            |file_id| async {
                delete_called_with.replace(Some(file_id));
                Ok(())
            },
        )
        .await
        .expect_err("create id error");
        let msg = err.to_string();

        assert_eq!(
            delete_called_with.into_inner().as_deref(),
            Some("created-file-id"),
        );
        assert!(
            msg.contains("response missing id"),
            "missing create id failure: {msg}"
        );
    }

    #[tokio::test]
    async fn create_metadata_id_error_reports_token_lookup_failure() {
        let id_error = CloudHomeError::Transport("response missing id".to_string());
        let err = finish_create_metadata_response(
            "objects/a",
            Err(id_error),
            || async { Err(CloudHomeError::Transport("lookup failed".to_string())) },
            |_| async { Ok(()) },
        )
        .await
        .expect_err("lookup failure");
        let msg = err.to_string();

        assert!(
            msg.contains("response missing id"),
            "missing create id failure: {msg}"
        );
        assert!(
            msg.contains("lookup failed"),
            "missing lookup failure: {msg}"
        );
    }

    #[tokio::test]
    async fn create_metadata_id_error_reports_token_delete_failure() {
        let id_error = CloudHomeError::Transport("response missing id".to_string());
        let err = finish_create_metadata_response(
            "objects/a",
            Err(id_error),
            || async { Ok(Some("created-file-id".to_string())) },
            |_| async { Err(CloudHomeError::Transport("delete failed".to_string())) },
        )
        .await
        .expect_err("delete failure");
        let msg = err.to_string();

        assert!(
            msg.contains("response missing id"),
            "missing create id failure: {msg}"
        );
        assert!(
            msg.contains("delete failed"),
            "missing delete failure: {msg}"
        );
    }

    #[tokio::test]
    async fn create_file_with_media_does_not_delete_on_metadata_failure() {
        let delete_called = std::cell::Cell::new(false);
        let err = create_file_with_media(
            "objects/a",
            || async { Err(CloudHomeError::Transport("metadata failed".to_string())) },
            |_| async { Ok(()) },
            |_| async {
                delete_called.set(true);
                Ok(())
            },
        )
        .await
        .expect_err("metadata failure");
        let msg = err.to_string();

        assert!(!delete_called.get(), "delete ran without a created file id");
        assert!(
            msg.contains("metadata failed"),
            "missing metadata failure: {msg}"
        );
    }

    #[tokio::test]
    async fn create_file_with_media_deletes_created_id_on_media_failure() {
        let deleted_id = std::cell::RefCell::new(None);
        let err = create_file_with_media(
            "objects/a",
            || async { Ok("created-file-id".to_string()) },
            |file_id| async move {
                assert_eq!(file_id, "created-file-id");
                Err(CloudHomeError::Transport("media upload failed".to_string()))
            },
            |file_id| async {
                deleted_id.replace(Some(file_id));
                Ok(())
            },
        )
        .await
        .expect_err("media failure");
        let msg = err.to_string();

        assert_eq!(deleted_id.into_inner().as_deref(), Some("created-file-id"));
        assert!(
            msg.contains("media upload failed"),
            "missing media failure: {msg}"
        );
    }

    #[tokio::test]
    async fn create_media_upload_error_rolls_back_created_file() {
        let rollback_called = std::cell::Cell::new(false);
        let upload_error = CloudHomeError::Transport("media upload failed".to_string());
        let err = finish_create_media_upload("objects/a", Err(upload_error), || async {
            rollback_called.set(true);
            Ok(())
        })
        .await
        .expect_err("upload error");
        let msg = err.to_string();

        assert!(rollback_called.get(), "rollback did not run");
        assert!(
            msg.contains("media upload failed"),
            "missing upload failure: {msg}"
        );
    }

    #[tokio::test]
    async fn create_media_upload_error_reports_upload_and_rollback_failures() {
        let upload_error = CloudHomeError::Transport("media upload failed".to_string());
        let err = finish_create_media_upload("objects/a", Err(upload_error), || async {
            Err(CloudHomeError::Transport("delete failed".to_string()))
        })
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
    }
}
