//! Google Drive `CloudHome` implementation.
//!
//! Uses the Google Drive REST API v3 with OAuth 2.0 tokens. Files are stored flat
//! in a single folder — path separators are escaped by
//! [`super::key_encoding`]). The `read`/`read_range`/`list`/`delete` methods are
//! the shared [`OAuthRestHome`] implementations; this file supplies only the Drive
//! request shapes, the page parser, the upload paths, and sharing.

use async_trait::async_trait;
use bytes::Bytes;

use super::http::{self, ensure_ok, ok_json, NotFound};
use super::key_encoding::{decode_listed_key, encode_key};
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
const CREATE_TOKEN_PROPERTY: &str = "covenCreateToken";
const DRIVE_FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";

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
pub struct GoogleDriveCloudHome {
    folder_id: String,
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

impl GoogleDriveCloudHome {
    pub fn new(
        folder_id: String,
        tokens: OAuthTokens,
        key_service: KeyService,
        clock: ClockRef,
    ) -> Result<Self, CloudHomeError> {
        let config = Self::oauth_config().map_err(|e| CloudHomeError::Storage(e.to_string()))?;
        Ok(Self {
            folder_id,
            session: OAuthSession::new(tokens, key_service, clock, config, "Google Drive"),
        })
    }

    pub fn oauth_config() -> Result<OAuthConfig, crate::oauth::OAuthClientCredsError> {
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
        let mut files = Vec::new();

        loop {
            let page = page_token.clone();
            let resp = self
                .session
                .api_call(|token| {
                    let mut req = self
                        .client()
                        .get(format!("{}/files", DRIVE_API))
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
                Some(next) => page_token = Some(next.to_string()),
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
                    .post(format!("{}/files", DRIVE_API))
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
            Err(e) => Err(CloudHomeError::Storage(format!(
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
            return Err(CloudHomeError::Storage(format!(
                "create {key}: created file {} with token {} was not returned by duplicate check",
                created.id, created.create_token
            )));
        }
        let Some(winner) = select_drive_file(&files) else {
            return Err(CloudHomeError::Storage(format!(
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
                    .get(format!("{}/files", DRIVE_API))
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
                    .patch(format!("{}/files/{}?uploadType=media", UPLOAD_API, file_id))
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
                    .delete(format!("{}/files/{}", DRIVE_API, file_id))
                    .bearer_auth(token)
            })
            .await?;
        let status = resp.status();
        if status.is_success() || status == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        Err(CloudHomeError::Storage(format!(
            "delete created file {key} (HTTP {status}): {}",
            http::body_text(resp).await
        )))
    }

    /// Open a resumable upload session for an existing Drive file and return its
    /// session URL (the `Location` header Google returns).
    async fn open_resumable_update_session(
        &self,
        key: &str,
        file_id: &str,
    ) -> Result<String, CloudHomeError> {
        let url = format!("{}/files/{}?uploadType=resumable", UPLOAD_API, file_id);
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
                CloudHomeError::Storage(format!(
                    "resumable session {key}: no Location header returned"
                ))
            })
    }
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
        .map_err(|e| CloudHomeError::Storage(format!("create {key}: parse response: {e}")))?;
    match json.get("id").and_then(|id| id.as_str()) {
        Some(id) if !id.is_empty() => Ok(id.to_string()),
        _ => Err(CloudHomeError::Storage(format!(
            "create {key}: response missing id"
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
            Err(delete_error) => Err(CloudHomeError::Storage(format!(
                "create {key}: metadata response id failure: {id_error}; rollback delete failed: {delete_error}"
            ))),
        },
        Ok(None) => Err(id_error),
        Err(lookup_error) => Err(CloudHomeError::Storage(format!(
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
        Err(delete_error) => Err(CloudHomeError::Storage(format!(
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
        let query = list_file_query(&self.folder_id, prefix);
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

#[async_trait]
impl CloudHome for GoogleDriveCloudHome {
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
        let existing = self.find_file_id(&encoded).await?;
        let (file_id, op) = if let Some(file_id) = existing {
            (file_id, "update")
        } else {
            (self.create_file_for_key(key, &encoded).await?, "create")
        };
        let session_url = self.open_resumable_update_session(key, &file_id).await?;
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
            "{}/files/{}/permissions?fields=permissions(id,emailAddress),nextPageToken",
            DRIVE_API, self.folder_id
        );
        let list_url_for_next = list_url.clone();
        let folder_id = self.folder_id.clone();
        sharing::revoke_by_email(
            &self.session,
            email,
            &list_url,
            "permissions",
            |p| p["emailAddress"].as_str().map(String::from),
            |perm_id| format!("{}/files/{}/permissions/{}", DRIVE_API, folder_id, perm_id),
            move |page| drive_permissions_next_page_url(&list_url_for_next, page),
        )
        .await
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
        .map_err(|e| CloudHomeError::Storage(format!("encode Drive page token: {e}")))?;
    let separator = if list_url.contains('?') { '&' } else { '?' };
    Ok(Some(format!("{list_url}{separator}{query}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn home() -> GoogleDriveCloudHome {
        crate::oauth::install_test_client_creds();
        GoogleDriveCloudHome::new(
            "folder123".to_string(),
            OAuthTokens {
                access_token: "test".to_string(),
                refresh_token: None,
                expires_at: None,
            },
            KeyService::new("test".to_string()),
            Arc::new(crate::clock::SystemClock),
        )
        .expect("build test Google Drive home")
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
        let valid = encode_key("changes/dev1/1.enc");
        let other_prefix = encode_key("heads/dev1.json.enc");
        let body = format!(
            r#"{{"files":[{{"name":"{valid}"}},{{"name":"not-hex"}},{{"name":"{other_prefix}"}}]}}"#
        );

        let page = home()
            .parse_list_page(&body, "changes/")
            .expect("parse list page");

        assert_eq!(page.keys, vec!["changes/dev1/1.enc"]);
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
        let id_error = CloudHomeError::Storage("response missing id".to_string());
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
        let id_error = CloudHomeError::Storage("response missing id".to_string());
        let err = finish_create_metadata_response(
            "objects/a",
            Err(id_error),
            || async { Err(CloudHomeError::Storage("lookup failed".to_string())) },
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
        let id_error = CloudHomeError::Storage("response missing id".to_string());
        let err = finish_create_metadata_response(
            "objects/a",
            Err(id_error),
            || async { Ok(Some("created-file-id".to_string())) },
            |_| async { Err(CloudHomeError::Storage("delete failed".to_string())) },
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
            || async { Err(CloudHomeError::Storage("metadata failed".to_string())) },
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
                Err(CloudHomeError::Storage("media upload failed".to_string()))
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
        let upload_error = CloudHomeError::Storage("media upload failed".to_string());
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
        let upload_error = CloudHomeError::Storage("media upload failed".to_string());
        let err = finish_create_media_upload("objects/a", Err(upload_error), || async {
            Err(CloudHomeError::Storage("delete failed".to_string()))
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
