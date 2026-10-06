use super::http::{self, Body, OAuthSession};
use crate::session::SessionState;
use crate::*;
use async_trait::async_trait;
use coven_crypto::SecretText;
use coven_foundation::id_source::DeviceId;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::BTreeSet;
const PROVIDER: CloudProvider = CloudProvider::GoogleDrive;

/// Encrypted objects named by their full coven path in a shared Drive folder.
pub struct GoogleDriveStorage {
    config: StorageConfig,
    folder: String,
    device: DeviceId,
    session: OAuthSession,
    api: String,
    upload_api: String,
    create_lock: tokio::sync::Mutex<()>,
}
impl GoogleDriveStorage {
    /// Construct with this device's own Google sign-in and upload identity.
    pub fn new(
        config: StorageConfig,
        device: DeviceId,
        session: OAuthSession,
    ) -> Result<Self, StorageError> {
        config.validate()?;
        let StorageConfig::GoogleDrive { folder_id } = &config else {
            return Err(StorageError::InvalidConfiguration(
                "expected Google Drive location",
            ));
        };
        if session.provider() != PROVIDER {
            return Err(StorageError::InvalidConfiguration("wrong OAuth provider"));
        }
        Ok(Self {
            folder: folder_id.clone(),
            config,
            device,
            session,
            api: "https://www.googleapis.com/drive/v3".into(),
            upload_api: "https://www.googleapis.com/upload/drive/v3".into(),
            create_lock: tokio::sync::Mutex::new(()),
        })
    }
    async fn send(
        &self,
        method: Method,
        url: &str,
        body: Body,
    ) -> Result<reqwest::Response, StorageError> {
        self.session.send(method, url, &[], body, true).await
    }
    fn url(&self, segments: &[&str], query: &[(&str, &str)]) -> Result<String, StorageError> {
        let mut query = query.to_vec();
        query.push(("supportsAllDrives", "true"));
        http::endpoint(&self.api, segments, &query)
    }
    async fn create_request(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        let metadata = json!({"name": path.as_str(), "parents": [&self.folder], "properties": {"covenDevice": self.device.0.to_string()}});
        let (content_type, body) = multipart_content(&metadata, bytes)?;
        let url = http::endpoint(
            &self.upload_api,
            &["files"],
            &[("uploadType", "multipart"), ("supportsAllDrives", "true")],
        )?;
        let item = http::json(
            PROVIDER,
            self.session
                .send(
                    Method::POST,
                    &url,
                    &[("Content-Type", content_type)],
                    Body::Bytes(body),
                    true,
                )
                .await?,
        )
        .await?;
        self.ensure_unique(path, http::string(&item, "id")?).await
    }
    async fn copies(&self, path: &ObjectPath) -> Result<Vec<Value>, StorageError> {
        let query = format!(
            "'{}' in parents and name = '{}' and trashed = false",
            escape(&self.folder),
            escape(path.as_str())
        );
        let mut copies = Vec::new();
        for item in self.pages(&query).await? {
            let stored = http::timestamp(&item, "createdTime")?;
            let id = http::string(&item, "id")?.to_owned();
            copies.push(((stored, id), item));
        }
        copies.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(copies.into_iter().map(|(_, item)| item).collect())
    }
    async fn find(&self, path: &ObjectPath) -> Result<Option<Value>, StorageError> {
        Ok(self.copies(path).await?.into_iter().next())
    }
    async fn remove_own_duplicates(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<Value>, StorageError> {
        let copies = self.copies(path).await?;
        let device = self.device.0.to_string();
        let mut own = copies
            .iter()
            .filter(|item| item["properties"]["covenDevice"].as_str() == Some(device.as_str()));
        // Only this path's writer retries it. A lost reply may leave several
        // copies; deleting later copies is itself safe to retry after a lost reply.
        own.next();
        for duplicate in own {
            let response = self
                .send(
                    Method::DELETE,
                    &self.url(&["files", http::string(duplicate, "id")?], &[])?,
                    Body::Empty,
                )
                .await?;
            if response.status().as_u16() != 404 {
                http::checked(PROVIDER, response).await?;
            }
        }
        Ok(copies.into_iter().next())
    }
    async fn pages(&self, query: &str) -> Result<Vec<Value>, StorageError> {
        let mut token = None::<String>;
        let mut seen = BTreeSet::new();
        let mut files = Vec::new();
        loop {
            let mut parameters = vec![("q", query), ("fields", "nextPageToken,files(id,name,size,createdTime,parents,properties,ownedByMe,capabilities(canDelete,canRemoveMyDriveParent))"), ("includeItemsFromAllDrives", "true"), ("pageSize", "1000")];
            if let Some(token) = &token {
                parameters.push(("pageToken", token));
            }
            let value = http::json(
                PROVIDER,
                self.send(
                    Method::GET,
                    &self.url(&["files"], &parameters)?,
                    Body::Empty,
                )
                .await?,
            )
            .await?;
            files.extend(http::array(&value, "files")?.iter().cloned());
            match value.get("nextPageToken") {
                None => break,
                Some(value) => {
                    let next = value
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .ok_or(StorageError::Protocol("invalid Drive page token"))?
                        .to_owned();
                    if !seen.insert(next.clone()) {
                        return Err(StorageError::Protocol("repeated Drive page token"));
                    }
                    token = Some(next);
                }
            }
        }
        Ok(files)
    }
    async fn read_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, StorageError> {
        let item = self.find(path).await?.ok_or(StorageError::NotFound)?;
        let url = self.url(&["files", http::string(&item, "id")?], &[("alt", "media")])?;
        let headers = range
            .map(|range| vec![("Range", range.header())])
            .unwrap_or_default();
        http::bytes(
            PROVIDER,
            self.session
                .send(Method::GET, &url, &headers, Body::Empty, true)
                .await?,
            range,
        )
        .await
    }
    async fn verify_completed(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        let SessionState::GoogleDrive { file_id, .. } = &session.state else {
            return Err(StorageError::SessionMismatch);
        };
        let item = http::json(
            PROVIDER,
            self.send(
                Method::GET,
                &self.url(
                    &["files", file_id.as_str()],
                    &[("fields", "id,name,size,parents")],
                )?,
                Body::Empty,
            )
            .await?,
        )
        .await;
        let item = match item {
            Ok(item) => item,
            Err(error) if error.failure() == StorageFailure::NotFound => {
                return Err(StorageError::SessionExpired)
            }
            Err(error) => return Err(error),
        };
        if http::string(&item, "name")? != session.path.as_str()
            || http::string(&item, "size")?
                .parse::<u64>()
                .map_err(|_| StorageError::Protocol("invalid Drive size"))?
                != session.total
            || !http::array(&item, "parents")?
                .iter()
                .any(|p| p.as_str() == Some(&self.folder))
        {
            return Err(StorageError::Protocol("Drive completed another object"));
        }
        self.ensure_unique(&session.path, file_id.as_str()).await?;
        session.confirmed = session.total;
        session.state = SessionState::Complete;
        Ok(())
    }
    async fn ensure_unique(&self, path: &ObjectPath, file_id: &str) -> Result<(), StorageError> {
        match self.remove_own_duplicates(path).await {
            Ok(Some(found)) if http::string(&found, "id")? == file_id => {}
            Ok(Some(_)) => return Err(StorageError::AlreadyExists),
            Err(error) => return Err(error),
            _ => {
                return Err(StorageError::Protocol(
                    "Drive upload is absent from its path",
                ))
            }
        }
        Ok(())
    }
    fn upload_url<'a>(&self, session: &'a UploadSession) -> Result<&'a str, StorageError> {
        session.check(&self.config)?;
        let SessionState::GoogleDrive { url, .. } = &session.state else {
            return Err(StorageError::SessionMismatch);
        };
        http::same_origin(&self.upload_api, url.as_str())?;
        Ok(url.as_str())
    }
    async fn progress(
        &self,
        session: &mut UploadSession,
        response: reqwest::Response,
    ) -> Result<(), StorageError> {
        if response.status().as_u16() == 308 {
            let confirmed = match response.headers().get("Range") {
                None => 0,
                Some(value) => value
                    .to_str()
                    .map_err(|_| StorageError::Protocol("invalid Drive range"))?
                    .strip_prefix("bytes=0-")
                    .ok_or(StorageError::Protocol("Drive range is not contiguous"))?
                    .parse::<u64>()
                    .map_err(|_| StorageError::Protocol("invalid Drive range"))?
                    .checked_add(1)
                    .ok_or(StorageError::InvalidPart)?,
            };
            if confirmed < session.confirmed || confirmed > session.total {
                return Err(StorageError::Protocol("Drive lost confirmed bytes"));
            }
            session.confirmed = confirmed;
            Ok(())
        } else if response.status().is_success() || response.status().as_u16() == 404 {
            self.verify_completed(session).await
        } else {
            Err(http::response_error(PROVIDER, response).await)
        }
    }
    async fn permissions(&self, email: &str) -> Result<Vec<Value>, StorageError> {
        let mut token = None::<String>;
        let mut seen = BTreeSet::new();
        let mut found = Vec::new();
        loop {
            let mut query = vec![(
                "fields",
                "nextPageToken,permissions(id,emailAddress,role,type)",
            )];
            if let Some(token) = &token {
                query.push(("pageToken", token));
            }
            let value = http::json(
                PROVIDER,
                self.send(
                    Method::GET,
                    &self.url(&["files", &self.folder, "permissions"], &query)?,
                    Body::Empty,
                )
                .await?,
            )
            .await?;
            for permission in http::array(&value, "permissions")? {
                if permission["type"].as_str() == Some("user")
                    && permission["emailAddress"]
                        .as_str()
                        .is_some_and(|e| e.eq_ignore_ascii_case(email))
                {
                    found.push(permission.clone());
                }
            }
            let Some(next) = value.get("nextPageToken") else {
                break;
            };
            let next = next
                .as_str()
                .ok_or(StorageError::Protocol("invalid permission page token"))?
                .to_owned();
            if !seen.insert(next.clone()) {
                return Err(StorageError::Protocol("repeated permission page token"));
            }
            token = Some(next);
        }
        Ok(found)
    }
}
fn multipart_content(metadata: &Value, bytes: &[u8]) -> Result<(String, Vec<u8>), StorageError> {
    let metadata =
        serde_json::to_vec(metadata).map_err(|error| StorageError::Encoding(Box::new(error)))?;
    let mut candidate = 0u64;
    let mut boundary = format!("coven-upload-{candidate:x}");
    while bytes
        .windows(boundary.len())
        .any(|part| part == boundary.as_bytes())
        || metadata
            .windows(boundary.len())
            .any(|part| part == boundary.as_bytes())
    {
        candidate = candidate
            .checked_add(1)
            .ok_or(StorageError::Protocol("multipart boundary exhausted"))?;
        boundary = format!("coven-upload-{candidate:x}");
    }
    let mut body = format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n")
        .into_bytes();
    body.extend_from_slice(&metadata);
    body.extend_from_slice(
        format!("\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    Ok((format!("multipart/related; boundary={boundary}"), body))
}

fn can_delete(item: &Value) -> Result<bool, StorageError> {
    let allowed = item["capabilities"]["canDelete"]
        .as_bool()
        .ok_or(StorageError::Protocol("missing Drive deletion right"))?;
    // Shared-drive files have no individual owner. The store's deletion policy
    // never turns a broader provider permission into authority over another
    // uploader's objects.
    Ok(allowed && item["ownedByMe"].as_bool() == Some(true))
}
fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

#[async_trait]
impl Storage for GoogleDriveStorage {
    fn config(&self) -> StorageConfig {
        self.config.clone()
    }
    async fn set_oauth_tokens(&self, tokens: OAuthTokens) -> Result<(), StorageError> {
        self.session.set_tokens(tokens).await;
        Ok(())
    }
    fn single_request_limit(&self) -> u64 {
        5 * 1024 * 1024
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        let _guard = self.create_lock.lock().await;
        crate::transfer::upload_bytes(self, path, bytes, async {
            if self.remove_own_duplicates(path).await?.is_some() {
                return Err(StorageError::AlreadyExists);
            }
            self.create_request(path, bytes).await
        })
        .await
    }

    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        if !path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
        crate::transfer::check_single_request(bytes.len() as u64, self.single_request_limit())?;
        let Some(item) = self.remove_own_duplicates(path).await? else {
            return self.create_request(path, bytes).await;
        };
        let url = http::endpoint(
            &self.upload_api,
            &["files", http::string(&item, "id")?],
            &[("uploadType", "media"), ("supportsAllDrives", "true")],
        )?;
        http::checked(
            PROVIDER,
            self.session
                .send(
                    Method::PATCH,
                    &url,
                    &[("Content-Type", "application/octet-stream".into())],
                    Body::Bytes(bytes.to_vec()),
                    true,
                )
                .await?,
        )
        .await?;
        Ok(())
    }
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        self.read_object(path, None).await
    }
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        self.read_object(path, Some(range)).await
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectPath>, StorageError> {
        let query = format!("'{}' in parents and trashed = false", escape(&self.folder));
        let mut paths = BTreeSet::new();
        for item in self.pages(&query).await? {
            let path = ObjectPath::parse(http::string(&item, "name")?)?;
            if prefix.contains(&path) {
                paths.insert(path);
            }
        }
        Ok(paths.into_iter().collect())
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        let Some(item) = self.find(path).await? else {
            tracing::debug!("Drive object already absent");
            return Ok(());
        };
        let id = http::string(&item, "id")?;
        let response = if can_delete(&item)? {
            self.send(Method::DELETE, &self.url(&["files", id], &[])?, Body::Empty)
                .await?
        } else if item["capabilities"]["canRemoveMyDriveParent"].as_bool() == Some(true) {
            self.send(
                Method::PATCH,
                &self.url(&["files", id], &[("removeParents", &self.folder)])?,
                Body::Json(json!({})),
            )
            .await?
        } else {
            return Err(StorageError::Provider {
                provider: PROVIDER,
                failure: StorageFailure::PermissionDenied,
                source: Box::new(StorageError::Protocol(
                    "Drive account cannot delete or remove this file",
                )),
            });
        };
        if response.status().as_u16() != 404 {
            http::checked(PROVIDER, response).await?;
        }
        Ok(())
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        let permissions = self.permissions(account).await?;
        if permissions
            .iter()
            .any(|p| matches!(p["role"].as_str(), Some("writer" | "owner")))
        {
            return Ok(AccessGrant::Granted);
        }
        for permission in permissions {
            let url = self.url(
                &[
                    "files",
                    &self.folder,
                    "permissions",
                    http::string(&permission, "id")?,
                ],
                &[],
            )?;
            http::checked(
                PROVIDER,
                self.send(Method::PATCH, &url, Body::Json(json!({"role":"writer"})))
                    .await?,
            )
            .await?;
        }
        if self.permissions(account).await?.is_empty() {
            let url = self.url(&["files", &self.folder, "permissions"], &[])?;
            http::checked(
                PROVIDER,
                self.send(
                    Method::POST,
                    &url,
                    Body::Json(json!({"type":"user","role":"writer","emailAddress":account})),
                )
                .await?,
            )
            .await?;
        }
        if !self
            .permissions(account)
            .await?
            .iter()
            .any(|p| matches!(p["role"].as_str(), Some("writer" | "owner")))
        {
            return Err(StorageError::Protocol("Drive did not grant write access"));
        }
        Ok(AccessGrant::Granted)
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        let MemberAccess::ProviderAccount(email) = member else {
            return Err(StorageError::InvalidConfiguration(
                "Drive requires an account",
            ));
        };
        for permission in self.permissions(email).await? {
            let response = self
                .send(
                    Method::DELETE,
                    &self.url(
                        &[
                            "files",
                            &self.folder,
                            "permissions",
                            http::string(&permission, "id")?,
                        ],
                        &[],
                    )?,
                    Body::Empty,
                )
                .await?;
            if response.status().as_u16() != 404 {
                http::checked(PROVIDER, response).await?;
            }
        }
        if !self.permissions(email).await?.is_empty() {
            return Err(StorageError::Protocol(
                "Drive access remains after revocation",
            ));
        }
        Ok(MemberRemoval::Revoked)
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        if path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
        if total == 0 {
            return Err(StorageError::InvalidPart);
        }
        if self.remove_own_duplicates(path).await?.is_some() {
            return Err(StorageError::AlreadyExists);
        }
        let generated = http::json(
            PROVIDER,
            self.send(
                Method::GET,
                &self.url(
                    &["files", "generateIds"],
                    &[("count", "1"), ("space", "drive"), ("type", "files")],
                )?,
                Body::Empty,
            )
            .await?,
        )
        .await?;
        let file_id = http::array(&generated, "ids")?
            .first()
            .and_then(Value::as_str)
            .ok_or(StorageError::Protocol("Drive omitted generated id"))?
            .to_owned();
        let body = json!({"id":file_id,"name":path.as_str(),"parents":[&self.folder],"properties":{"covenDevice":self.device.0.to_string()}});
        let url = http::endpoint(
            &self.upload_api,
            &["files"],
            &[("uploadType", "resumable"), ("supportsAllDrives", "true")],
        )?;
        let response = http::checked(
            PROVIDER,
            self.session
                .send(
                    Method::POST,
                    &url,
                    &[
                        ("X-Upload-Content-Length", total.to_string()),
                        ("X-Upload-Content-Type", "application/octet-stream".into()),
                    ],
                    Body::Json(body),
                    true,
                )
                .await?,
        )
        .await?;
        let url = response
            .headers()
            .get("Location")
            .and_then(|h| h.to_str().ok())
            .ok_or(StorageError::Protocol("Drive omitted upload URL"))?
            .to_owned();
        http::same_origin(&self.upload_api, &url)?;
        Ok(UploadSession {
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size: crate::session::GOOGLE_DRIVE_PART_SIZE,
            state: SessionState::GoogleDrive {
                url: SecretText::new(url),
                file_id: SecretText::new(file_id),
            },
        })
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if session.is_complete() {
            return Ok(());
        }
        let response = self
            .session
            .send(
                Method::PUT,
                self.upload_url(session)?,
                &[
                    ("Content-Range", format!("bytes */{}", session.total)),
                    ("Content-Length", "0".into()),
                ],
                Body::Bytes(Vec::new()),
                true,
            )
            .await?;
        self.progress(session, response).await
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let end = session.end_of_part(bytes.len())?;
        let response = self
            .session
            .send(
                Method::PUT,
                self.upload_url(session)?,
                &[
                    (
                        "Content-Range",
                        format!("bytes {}-{}/{}", session.confirmed, end - 1, session.total),
                    ),
                    ("Content-Type", "application/octet-stream".into()),
                ],
                Body::Bytes(bytes.to_vec()),
                true,
            )
            .await?;
        self.progress(session, response).await?;
        if session.confirmed != end {
            return Err(StorageError::Protocol("Drive did not store the whole part"));
        }
        Ok(())
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.resume_upload(session).await?;
        if !session.is_complete() {
            return Err(StorageError::InvalidPart);
        }
        Ok(())
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if session.is_complete() {
            return Ok(());
        }
        let response = self
            .session
            .send(
                Method::DELETE,
                self.upload_url(session)?,
                &[],
                Body::Empty,
                true,
            )
            .await?;
        if !matches!(response.status().as_u16(), 404 | 499) {
            http::checked(PROVIDER, response).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "google_drive_tests.rs"]
mod tests;
